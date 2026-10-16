package lambda

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/aws/s3io"
	"github.com/nrndev/pogi/internal/core"
)

// Mapping is an event source mapping (SQS, DynamoDB Streams, Kinesis or self-managed Kafka → function).
type Mapping struct {
	UUID              string            `json:"uuid"`
	Account           string            `json:"account"`
	Region            string            `json:"region"`
	FunctionArn       string            `json:"functionArn"`
	SourceArn         string            `json:"sourceArn"`
	BatchSize         int               `json:"batchSize"`
	Window            int               `json:"window"`
	StartingPosition  string            `json:"startingPosition,omitempty"`
	StartingTimestamp float64           `json:"startingTimestamp,omitempty"`
	Enabled           bool              `json:"enabled"`
	Filters           []string          `json:"filters,omitempty"`
	ResponseTypes     []string          `json:"responseTypes,omitempty"`
	MaxRetry          int               `json:"maxRetry"`
	Bisect            bool              `json:"bisect,omitempty"`
	MaxConcurrency    int               `json:"maxConcurrency,omitempty"` // ScalingConfig.MaximumConcurrency, kept but not used to limit polling
	OnFailure         string            `json:"onFailure,omitempty"`
	Topics            []string          `json:"topics,omitempty"`        // self-managed Kafka
	Endpoints         []string          `json:"endpoints,omitempty"`     // its KAFKA_BOOTSTRAP_SERVERS
	Access            []accessConfig    `json:"access,omitempty"`        // its SourceAccessConfigurations
	ConsumerGroup     string            `json:"consumerGroup,omitempty"` // its consumer group id
	Modified          time.Time         `json:"modified"`
	LastResult        string            `json:"lastResult"`
	Tags              map[string]string `json:"tags,omitempty"`
}

func (m *Mapping) state() string {
	if m.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

func (m *Mapping) json() map[string]any {
	out := map[string]any{
		"UUID": m.UUID, "FunctionArn": m.FunctionArn, "BatchSize": m.BatchSize,
		"MaximumBatchingWindowInSeconds": m.Window, "State": m.state(), "StateTransitionReason": "USER_INITIATED",
		"LastModified": core.Unix(m.Modified), "LastProcessingResult": or(m.LastResult, "No records processed"),
		"EventSourceMappingArn": core.ARN("lambda", m.Region, m.Account, "event-source-mapping:"+m.UUID),
		"FunctionResponseTypes": orList(m.ResponseTypes, []string{}),
	}
	if m.kafka() {
		sources := []map[string]string{}
		for _, a := range m.Access {
			sources = append(sources, map[string]string{"Type": a.Type, "URI": a.URI})
		}
		out["Topics"], out["StartingPosition"], out["SourceAccessConfigurations"] = m.Topics, m.StartingPosition, sources
		if m.SourceArn != "" { // Amazon MSK
			out["EventSourceArn"] = m.SourceArn
			out["AmazonManagedKafkaEventSourceConfig"] = map[string]string{"ConsumerGroupId": m.ConsumerGroup}
		} else {
			out["SelfManagedEventSource"] = map[string]any{"Endpoints": map[string][]string{"KAFKA_BOOTSTRAP_SERVERS": m.Endpoints}}
			out["SelfManagedKafkaEventSourceConfig"] = map[string]string{"ConsumerGroupId": m.ConsumerGroup}
		}
	} else {
		out["EventSourceArn"] = m.SourceArn
	}
	if m.StartingPosition != "" && !m.kafka() {
		out["StartingPosition"] = m.StartingPosition
		out["MaximumRetryAttempts"], out["MaximumRecordAgeInSeconds"], out["ParallelizationFactor"] = m.MaxRetry, -1, 1
		out["BisectBatchOnFunctionError"], out["TumblingWindowInSeconds"] = m.Bisect, 0
		dc := map[string]any{"OnFailure": map[string]any{}}
		if m.OnFailure != "" {
			dc["OnFailure"] = map[string]string{"Destination": m.OnFailure}
		}
		out["DestinationConfig"] = dc
	}
	if m.MaxConcurrency > 0 {
		out["ScalingConfig"] = map[string]int{"MaximumConcurrency": m.MaxConcurrency}
	}
	if len(m.Filters) > 0 {
		var fs []map[string]string
		for _, f := range m.Filters {
			fs = append(fs, map[string]string{"Pattern": f})
		}
		out["FilterCriteria"] = map[string]any{"Filters": fs}
	}
	return out
}

type mappingInput struct {
	EventSourceArn                 string
	FunctionName                   string
	StartingPosition               string
	StartingPositionTimestamp      float64
	BatchSize                      *int
	MaximumBatchingWindowInSeconds *int
	Enabled                        *bool
	FilterCriteria                 *struct{ Filters []struct{ Pattern string } }
	FunctionResponseTypes          []string
	ScalingConfig                  *struct{ MaximumConcurrency *int }
	MaximumRetryAttempts           *int
	BisectBatchOnFunctionError     *bool
	DestinationConfig              *struct{ OnFailure struct{ Destination string } }
	Tags                           map[string]string

	Topics                              []string
	SelfManagedEventSource              *struct{ Endpoints map[string][]string }
	SourceAccessConfigurations          []struct{ Type, URI string }
	SelfManagedKafkaEventSourceConfig   *struct{ ConsumerGroupId string }
	AmazonManagedKafkaEventSourceConfig *struct{ ConsumerGroupId string }
}

// apply validates and copies the set fields; the lock must be held.
func (s *Service) apply(c *core.Ctx, m *Mapping, in *mappingInput) error {
	if in.FunctionName != "" {
		f, q, err := s.function(c, in.FunctionName)
		if err != nil {
			return err
		}
		m.FunctionArn = f.arn()
		if q != "" {
			m.FunctionArn += ":" + q
		}
	}
	if in.BatchSize != nil {
		m.BatchSize = *in.BatchSize
	}
	if in.MaximumBatchingWindowInSeconds != nil {
		m.Window = *in.MaximumBatchingWindowInSeconds
	}
	if in.Enabled != nil {
		m.Enabled = *in.Enabled
	}
	if in.FilterCriteria != nil {
		m.Filters = nil
		for _, f := range in.FilterCriteria.Filters {
			if _, err := pattern.Compile(f.Pattern); err != nil {
				return invalid("Invalid filter pattern definition: %v", err)
			}
			m.Filters = append(m.Filters, f.Pattern)
		}
	}
	if in.FunctionResponseTypes != nil {
		m.ResponseTypes = in.FunctionResponseTypes
	}
	if sc := in.ScalingConfig; sc != nil {
		m.MaxConcurrency = 0 // an empty ScalingConfig clears it
		if sc.MaximumConcurrency != nil {
			m.MaxConcurrency = *sc.MaximumConcurrency
		}
	}
	if in.MaximumRetryAttempts != nil {
		m.MaxRetry = *in.MaximumRetryAttempts
	}
	if in.BisectBatchOnFunctionError != nil {
		m.Bisect = *in.BisectBatchOnFunctionError
	}
	if in.DestinationConfig != nil {
		m.OnFailure = in.DestinationConfig.OnFailure.Destination
	}
	if in.SourceAccessConfigurations != nil {
		m.Access = nil
		for _, a := range in.SourceAccessConfigurations {
			m.Access = append(m.Access, accessConfig{a.Type, a.URI})
		}
	}
	m.Modified = time.Now().UTC()
	return nil
}

func (s *Service) createMapping(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in mappingInput
	if err := decode(r, &in); err != nil {
		return err
	}
	if in.EventSourceArn == "" && in.SelfManagedEventSource != nil {
		return s.createKafkaMapping(w, c, &in)
	}
	svc, _, _, res, ok := core.ParseARN(in.EventSourceArn)
	if ok && svc == "kafka" && strings.HasPrefix(res, "cluster/") {
		return s.createKafkaMapping(w, c, &in)
	}
	if !ok || svc != "sqs" && svc != "dynamodb" && svc != "kinesis" {
		return invalid("Unsupported event source %q (pogi polls SQS, DynamoDB Streams, Kinesis, Amazon MSK and self-managed Kafka)", in.EventSourceArn)
	}
	if svc != "sqs" && in.StartingPosition == "" {
		return invalid("1 validation error detected: Value null at 'startingPosition' failed to satisfy constraint: Member must not be null")
	}
	m := &Mapping{UUID: core.UUID(), Account: c.Account, Region: c.Region, SourceArn: in.EventSourceArn, Enabled: true, MaxRetry: -1,
		StartingPosition: in.StartingPosition, StartingTimestamp: in.StartingPositionTimestamp, Tags: in.Tags, BatchSize: 100}
	if svc == "sqs" {
		m.BatchSize = 10
	}
	s.mu.Lock()
	err := s.apply(c, m, &in)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if m.StartingPosition == "LATEST" {
		s.esm.open(m) // LATEST means "from now": take the position before replying, and before the poller sees the mapping
	}
	s.mu.Lock()
	s.st.Mappings[m.UUID] = m
	out := m.json()
	s.mu.Unlock()
	core.RESTJSON(w, c, 202, out)
	return nil
}

// createKafkaMapping makes a mapping that reads a topic of an Amazon MSK cluster (EventSourceArn) or of
// self-managed Kafka brokers. An MSK mapping looks up the cluster's broker each time it polls.
func (s *Service) createKafkaMapping(w http.ResponseWriter, c *core.Ctx, in *mappingInput) error {
	var servers []string
	if in.SelfManagedEventSource != nil {
		servers = in.SelfManagedEventSource.Endpoints["KAFKA_BOOTSTRAP_SERVERS"]
	}
	switch {
	case in.EventSourceArn == "" && len(servers) == 0:
		return invalid("SelfManagedEventSource needs Endpoints with KAFKA_BOOTSTRAP_SERVERS.")
	case len(in.Topics) != 1 || in.Topics[0] == "":
		return invalid("A Kafka event source mapping reads exactly one topic: set Topics to one name.")
	case in.BatchSize != nil && (*in.BatchSize < 1 || *in.BatchSize > 10000):
		return invalid("1 validation error detected: Value '%d' at 'batchSize' failed to satisfy constraint: Member must have value between 1 and 10000", *in.BatchSize)
	case !slices.Contains([]string{"", "LATEST", "TRIM_HORIZON", "AT_TIMESTAMP"}, in.StartingPosition):
		return invalid("1 validation error detected: Value '%s' at 'startingPosition' failed to satisfy constraint: Member must satisfy enum value set: [AT_TIMESTAMP, LATEST, TRIM_HORIZON]", in.StartingPosition)
	case in.StartingPosition == "AT_TIMESTAMP" && in.StartingPositionTimestamp == 0:
		return invalid("StartingPositionTimestamp is required when StartingPosition is AT_TIMESTAMP.")
	}
	m := &Mapping{UUID: core.UUID(), Account: c.Account, Region: c.Region, Enabled: true, MaxRetry: -1, BatchSize: 100, Topics: in.Topics, Endpoints: servers,
		SourceArn: in.EventSourceArn, StartingPosition: or(in.StartingPosition, "LATEST"), StartingTimestamp: in.StartingPositionTimestamp, Tags: in.Tags}
	m.ConsumerGroup = m.UUID
	if cfg := in.SelfManagedKafkaEventSourceConfig; cfg != nil && cfg.ConsumerGroupId != "" {
		m.ConsumerGroup = cfg.ConsumerGroupId
	}
	if cfg := in.AmazonManagedKafkaEventSourceConfig; cfg != nil && cfg.ConsumerGroupId != "" {
		m.ConsumerGroup = cfg.ConsumerGroupId
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.apply(c, m, in); err != nil {
		return err
	}
	s.st.Mappings[m.UUID] = m
	core.RESTJSON(w, c, 202, m.json())
	return nil
}

func (s *Service) mapping(id string) (*Mapping, error) {
	if m := s.st.Mappings[id]; m != nil {
		return m, nil
	}
	return nil, core.Errorf(404, "ResourceNotFoundException", "The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: %s)", core.UUID())
}

func (s *Service) getMapping(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.mapping(r.PathValue("uuid"))
	if err != nil {
		return err
	}
	core.RESTJSON(w, c, 200, m.json())
	return nil
}

func (s *Service) updateMapping(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in mappingInput
	if err := decode(r, &in); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.mapping(r.PathValue("uuid"))
	if err != nil {
		return err
	}
	if err := s.apply(c, m, &in); err != nil {
		return err
	}
	core.RESTJSON(w, c, 202, m.json())
	return nil
}

func (s *Service) deleteMapping(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.mapping(r.PathValue("uuid"))
	if err != nil {
		return err
	}
	delete(s.st.Mappings, m.UUID)
	s.esm.forget(m.UUID)
	out := m.json()
	out["State"] = "Deleting"
	core.RESTJSON(w, c, 202, out)
	return nil
}

func (s *Service) listMappings(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	fnArn := ""
	if n := q.Get("FunctionName"); n != "" {
		f, _, err := s.function(c, n)
		if err != nil {
			return err
		}
		fnArn = f.arn()
	}
	list := []any{}
	for _, id := range core.SortedKeys(s.st.Mappings) {
		m := s.st.Mappings[id]
		if m.Account != c.Account || m.Region != c.Region || fnArn != "" && !strings.HasPrefix(m.FunctionArn, fnArn) ||
			q.Get("EventSourceArn") != "" && m.SourceArn != q.Get("EventSourceArn") {
			continue
		}
		list = append(list, m.json())
	}
	core.RESTJSON(w, c, 200, map[string]any{"EventSourceMappings": list})
	return nil
}

// ---- pollers ----

type pollers struct {
	s       *Service
	quit    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	busy    map[string]bool
	cursors map[string]*cursor
	since   map[string]time.Time // SQS mapping → when its current batching window began
	kafka   map[string]*kafkaConsumer
}

// cursor remembers the stream positions of one mapping.
type cursor struct {
	shards map[string]*shardState
}

// shardState is one shard's place: the iterator for the next read, and the
// batches already read but not yet done (the first one is being tried).
type shardState struct {
	iter    string
	queue   [][]map[string]any
	retries int // failed tries of the first batch
}

func startPollers(s *Service) *pollers {
	p := &pollers{s: s, quit: make(chan struct{}), busy: map[string]bool{}, cursors: map[string]*cursor{}, since: map[string]time.Time{}, kafka: map[string]*kafkaConsumer{}}
	core.Go("lambda.startPollers", func() {
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-p.quit:
				return
			case <-t.C:
				func() { defer core.Recover("lambda.pollers"); p.tick() }() // a panic skips one tick
			}
		}
	})
	return p
}

func (p *pollers) stop() {
	p.once.Do(func() { close(p.quit) })
	p.mu.Lock()
	ids := make([]string, 0, len(p.kafka))
	for id := range p.kafka {
		ids = append(ids, id)
	}
	p.mu.Unlock()
	for _, id := range ids {
		p.closeKafka(id)
	}
}

func (p *pollers) tick() {
	p.s.mu.Lock()
	var due []Mapping
	for _, m := range p.s.st.Mappings {
		if m.Enabled {
			due = append(due, *m)
		}
	}
	p.s.mu.Unlock()
	p.mu.Lock()
	var idle []string // Kafka consumers whose mapping is disabled or gone leave their group
	for id := range p.kafka {
		if !slices.ContainsFunc(due, func(m Mapping) bool { return m.UUID == id }) && !p.busy[id] {
			idle = append(idle, id)
		}
	}
	p.mu.Unlock()
	for _, id := range idle {
		p.closeKafka(id)
	}
	for _, m := range due {
		p.mu.Lock()
		if p.busy[m.UUID] {
			p.mu.Unlock()
			continue
		}
		p.busy[m.UUID] = true
		p.mu.Unlock()
		m := m
		core.Go("lambda.poll", func() {
			result := ""
			defer func() { // runs on a panic too, so the mapping is polled again later
				p.mu.Lock()
				delete(p.busy, m.UUID)
				p.mu.Unlock()
				if result != "" {
					p.s.mu.Lock()
					if cur := p.s.st.Mappings[m.UUID]; cur != nil {
						cur.LastResult = result
					}
					p.s.mu.Unlock()
				}
			}()
			result = p.poll(&m)
		})
	}
}

// poll runs one cycle for a mapping and returns its LastProcessingResult ("" if idle).
func (p *pollers) poll(m *Mapping) string {
	if m.kafka() {
		return p.pollKafka(m)
	}
	svc, region, account, res, _ := core.ParseARN(m.SourceArn)
	switch svc {
	case "sqs":
		return p.pollSQS(m, account, region, res)
	case "dynamodb":
		return p.pollStream(m, account, region, "DynamoDBStreams_20120810")
	case "kinesis":
		return p.pollStream(m, account, region, "Kinesis_20131202")
	}
	return ""
}

func (p *pollers) invoke(m *Mapping, records []map[string]any, trace string) (outcome, error) {
	t, err := p.s.target(ctxFor(m.FunctionArn), m.FunctionArn, "")
	if err != nil {
		return outcome{}, err
	}
	payload, _ := json.Marshal(map[string]any{"Records": records})
	return p.s.runQueued(t, payload, origin{trace: trace, from: m.SourceArn, span: true}, func() bool { // a throttled batch waits while its mapping is enabled
		p.s.mu.Lock()
		defer p.s.mu.Unlock()
		cur := p.s.st.Mappings[m.UUID]
		return cur != nil && cur.Enabled
	})
}

// keep applies filter criteria; records without a match are dropped.
func keep(m *Mapping, rec map[string]any, dataKey string) bool {
	if len(m.Filters) == 0 {
		return true
	}
	view := map[string]any{}
	for k, v := range rec {
		view[k] = v
	}
	if raw, ok := rec[dataKey].(string); ok {
		b := []byte(raw)
		if dataKey == "data" {
			b, _ = base64.StdEncoding.DecodeString(raw)
		}
		var parsed any
		if json.Unmarshal(b, &parsed) == nil {
			view[dataKey] = parsed
		}
	}
	if k, ok := rec["kinesis"].(map[string]any); ok {
		kv := map[string]any{}
		for key, v := range k {
			kv[key] = v
		}
		if raw, ok := k["data"].(string); ok {
			var parsed any
			if b, err := base64.StdEncoding.DecodeString(raw); err == nil && json.Unmarshal(b, &parsed) == nil {
				kv["data"] = parsed
			}
		}
		view["kinesis"] = kv
	}
	for _, f := range m.Filters {
		if pt, err := pattern.Compile(f); err == nil && pt.Match(view) {
			return true
		}
	}
	return false
}

func (p *pollers) pollSQS(m *Mapping, account, region, queue string) string {
	hub := p.s.hub
	queueURL := "http://localhost/" + account + "/" + queue
	if m.Window > 0 && !p.windowDone(m, account, region, queueURL) {
		return ""
	}
	var out struct {
		Messages []struct {
			MessageId, ReceiptHandle, MD5OfBody, Body string
			Attributes                                map[string]string
			MessageAttributes                         map[string]struct {
				DataType, StringValue string
				BinaryValue           []byte
			}
		}
	}
	err := hub.Call(account, region, "AmazonSQS", "ReceiveMessage", map[string]any{"QueueUrl": queueURL,
		"MaxNumberOfMessages": min(max(m.BatchSize, 1), 10), "AttributeNames": []string{"All"}, "MessageAttributeNames": []string{"All"}}, &out)
	if err != nil {
		return "PROBLEM: " + err.Error()
	}
	if len(out.Messages) == 0 {
		return ""
	}
	var records []map[string]any
	var dropped []string
	handles := map[string]string{}
	for _, msg := range out.Messages {
		attrs := map[string]any{}
		for k, a := range msg.MessageAttributes {
			v := map[string]any{"dataType": a.DataType, "stringListValues": []string{}, "binaryListValues": []string{}}
			if a.StringValue != "" {
				v["stringValue"] = a.StringValue
			}
			if a.BinaryValue != nil {
				v["binaryValue"] = base64.StdEncoding.EncodeToString(a.BinaryValue)
			}
			attrs[k] = v
		}
		rec := map[string]any{"messageId": msg.MessageId, "receiptHandle": msg.ReceiptHandle, "body": msg.Body, "attributes": msg.Attributes,
			"messageAttributes": attrs, "md5OfBody": msg.MD5OfBody, "eventSource": "aws:sqs", "eventSourceARN": m.SourceArn, "awsRegion": region}
		if keep(m, rec, "body") {
			handles[msg.MessageId] = msg.ReceiptHandle
			records = append(records, rec)
		} else {
			dropped = append(dropped, msg.ReceiptHandle)
		}
	}
	del := func(receipts []string) {
		for i := 0; i < len(receipts); i += 10 {
			var entries []map[string]string
			for j, h := range receipts[i:min(i+10, len(receipts))] {
				entries = append(entries, map[string]string{"Id": strconv.Itoa(j), "ReceiptHandle": h})
			}
			hub.Call(account, region, "AmazonSQS", "DeleteMessageBatch", map[string]any{"QueueUrl": queueURL, "Entries": entries}, nil)
		}
	}
	del(dropped) // filtered-out SQS messages are deleted, as AWS does
	if len(records) == 0 {
		return "OK"
	}
	trace := ""
	for _, msg := range out.Messages { // ponytail: a batch runs under its first message's trace; one invoke per batch
		if trace = core.TraceRoot(msg.Attributes["AWSTraceHeader"]); trace != "" {
			break
		}
	}
	res, err := p.invoke(m, records, trace)
	if err != nil || res.errType != "" {
		return "PROBLEM: Function call failed" // messages reappear after the visibility timeout
	}
	failed := map[string]bool{}
	if contains(m.ResponseTypes, "ReportBatchItemFailures") {
		ids, bad := failedItems(res.body)
		for _, id := range ids {
			if _, ok := handles[id]; !ok {
				bad = true // an id that is not in the batch
			}
			failed[id] = true
		}
		if bad {
			return "PROBLEM: Invalid batchItemFailures response" // AWS fails the whole batch
		}
	}
	var done []string
	for _, rec := range records {
		if id := rec["messageId"].(string); !failed[id] {
			done = append(done, handles[id])
		}
	}
	del(done)
	return "OK"
}

// windowDone says whether an SQS mapping with a MaximumBatchingWindowInSeconds should read now:
// when a full batch is waiting, or the window, which starts at the first waiting message, is over.
func (p *pollers) windowDone(m *Mapping, account, region, queueURL string) bool {
	var out struct{ Attributes map[string]string }
	err := p.s.hub.Call(account, region, "AmazonSQS", "GetQueueAttributes", map[string]any{"QueueUrl": queueURL, "AttributeNames": []string{"ApproximateNumberOfMessages"}}, &out)
	waiting, _ := strconv.Atoi(out.Attributes["ApproximateNumberOfMessages"])
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil || waiting == 0 {
		delete(p.since, m.UUID)
		return err != nil // on an error, let the read report it
	}
	if p.since[m.UUID].IsZero() {
		p.since[m.UUID] = time.Now()
	}
	if waiting >= min(max(m.BatchSize, 1), 10) || time.Since(p.since[m.UUID]) >= time.Duration(m.Window)*time.Second {
		delete(p.since, m.UUID)
		return true
	}
	return false
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// failedItems reads the batchItemFailures of a function reply. A missing, null
// or empty list means success. bad is true when the list is malformed (not a
// list, or an entry without a non-empty itemIdentifier): AWS then fails the batch.
func failedItems(body []byte) (ids []string, bad bool) {
	var reply map[string]json.RawMessage
	if json.Unmarshal(body, &reply) != nil {
		return nil, false
	}
	raw := reply["batchItemFailures"]
	if len(raw) == 0 || string(raw) == "null" {
		return nil, false
	}
	var items []map[string]any
	if json.Unmarshal(raw, &items) != nil {
		return nil, true
	}
	for _, it := range items {
		id, _ := it["itemIdentifier"].(string)
		if id == "" {
			return nil, true
		}
		ids = append(ids, id)
	}
	return ids, false
}

// unfinished returns the part of a stream batch that must run again after the
// function reported failed items: from the lowest failed record on, as AWS does.
// bad is true when the reply is malformed or names a record not in the batch.
func unfinished(batch []map[string]any, body []byte, kinesis bool) (rest []map[string]any, bad bool) {
	ids, bad := failedItems(body)
	if bad || len(ids) == 0 {
		return nil, bad
	}
	first := len(batch)
	for _, id := range ids {
		i := slices.IndexFunc(batch, func(rec map[string]any) bool { return seqOf(rec, kinesis) == id })
		if i < 0 {
			return nil, true
		}
		first = min(first, i)
	}
	return batch[first:], false
}

// seqOf is the sequence number of a stream record.
func seqOf(rec map[string]any, kinesis bool) string {
	key, field := "dynamodb", "SequenceNumber"
	if kinesis {
		key, field = "kinesis", "sequenceNumber"
	}
	sub, _ := rec[key].(map[string]any)
	seq, _ := sub[field].(string)
	return seq
}

// open makes the cursor of a stream mapping and takes a shard iterator for every
// shard, if it has none. It returns the cursor and a problem text ("" if fine).
func (p *pollers) open(m *Mapping) (*cursor, string) {
	svc, region, account, _, _ := core.ParseARN(m.SourceArn)
	target := "DynamoDBStreams_20120810"
	if svc == "kinesis" {
		target = "Kinesis_20131202"
	}
	hub := p.s.hub
	kinesis := svc == "kinesis"
	p.mu.Lock()
	cur := p.cursors[m.UUID]
	if cur == nil {
		cur = &cursor{shards: map[string]*shardState{}}
		p.cursors[m.UUID] = cur
	}
	p.mu.Unlock()
	if len(cur.shards) > 0 {
		return cur, ""
	}
	var shards []string
	if kinesis {
		var out struct{ Shards []struct{ ShardId string } }
		if err := hub.Call(account, region, target, "ListShards", map[string]any{"StreamARN": m.SourceArn}, &out); err != nil {
			return cur, "PROBLEM: " + err.Error()
		}
		for _, sh := range out.Shards {
			shards = append(shards, sh.ShardId)
		}
	} else {
		var out struct {
			StreamDescription struct{ Shards []struct{ ShardId string } }
		}
		if err := hub.Call(account, region, target, "DescribeStream", map[string]any{"StreamArn": m.SourceArn}, &out); err != nil {
			return cur, "PROBLEM: " + err.Error()
		}
		for _, sh := range out.StreamDescription.Shards {
			shards = append(shards, sh.ShardId)
		}
	}
	for _, sh := range shards {
		in := map[string]any{"ShardId": sh, "ShardIteratorType": m.StartingPosition}
		if kinesis {
			in["StreamARN"] = m.SourceArn
			if m.StartingPosition == "AT_TIMESTAMP" {
				in["Timestamp"] = m.StartingTimestamp
			}
		} else {
			in["StreamArn"] = m.SourceArn
		}
		var out struct{ ShardIterator string }
		if err := hub.Call(account, region, target, "GetShardIterator", in, &out); err == nil && out.ShardIterator != "" {
			cur.shards[sh] = &shardState{iter: out.ShardIterator}
		}
	}
	return cur, ""
}

// forget drops the stream positions of a deleted mapping.
func (p *pollers) forget(uuid string) {
	p.mu.Lock()
	delete(p.cursors, uuid)
	delete(p.since, uuid)
	p.mu.Unlock()
	core.Go("lambda.closeKafka", func() { p.closeKafka(uuid) }) // leaving the group talks to the brokers; the caller may hold the service lock
}

// sendFailure sends the note about an exhausted batch to the OnFailure destination:
// an S3 bucket gets an object, anything else is a message.
func (p *pollers) sendFailure(m *Mapping, shard, info string) {
	if bucket, ok := strings.CutPrefix(m.OnFailure, "arn:aws:s3:::"); ok {
		bucket, _, _ = strings.Cut(bucket, "/")
		now := time.Now().UTC()
		key := "aws/lambda/" + m.UUID + "/" + shard + "/" + now.Format("2006-01-02/2006-01-02T15.04.05") + "-" + core.UUID()
		if err := (s3io.Client{Hub: p.s.hub, Account: m.Account, Region: m.Region}).Put(bucket, key, []byte(info)); err != nil {
			p.s.hub.Log.Warn("lambda OnFailure to S3 failed", "mapping", m.UUID, "err", err)
		}
		return
	}
	p.s.hub.Deliver(m.OnFailure, &core.Message{Body: info})
}

// pollStream reads DynamoDB Streams or Kinesis shards in order. A failed batch
// is retried until MaximumRetryAttempts, then sent to OnFailure. With
// BisectBatchOnFunctionError a failed batch is split in two first (this does not
// use up a retry). With ReportBatchItemFailures a retry starts at the lowest failed record.
func (p *pollers) pollStream(m *Mapping, account, region, target string) string {
	hub := p.s.hub
	kinesis := target == "Kinesis_20131202"
	cur, problem := p.open(m)
	if problem != "" {
		return problem
	}
	result := ""
	for shard, sh := range cur.shards {
		if len(sh.queue) == 0 {
			var out struct {
				Records           []map[string]any
				NextShardIterator string
			}
			if err := hub.Call(account, region, target, "GetRecords", map[string]any{"ShardIterator": sh.iter, "Limit": m.BatchSize}, &out); err != nil {
				delete(cur.shards, shard) // expired or trimmed: start over on the next tick
				continue
			}
			var records []map[string]any
			for _, r := range out.Records {
				rec := r
				if kinesis {
					seq, _ := r["SequenceNumber"].(string)
					rec = map[string]any{"kinesis": map[string]any{"kinesisSchemaVersion": "1.0", "partitionKey": r["PartitionKey"], "sequenceNumber": seq,
						"data": r["Data"], "approximateArrivalTimestamp": r["ApproximateArrivalTimestamp"]},
						"eventSource": "aws:kinesis", "eventVersion": "1.0", "eventID": shard + ":" + seq, "eventName": "aws:kinesis:record",
						"invokeIdentityArn": core.GlobalARN("iam", region, account, "role/lambda-role"), "awsRegion": region}
				}
				rec["eventSourceARN"] = m.SourceArn
				if keep(m, rec, "data") {
					records = append(records, rec)
				}
			}
			if len(records) > 0 {
				sh.queue = [][]map[string]any{records}
			}
			if out.NextShardIterator != "" {
				sh.iter = out.NextShardIterator
			}
		}
		if len(sh.queue) == 0 {
			continue
		}
		batch := sh.queue[0]
		res, err := p.invoke(m, batch, "")
		whole := err != nil || res.errType != "" // the whole batch failed
		redo := batch                            // what to run again
		partial := false                         // some records failed
		if !whole && contains(m.ResponseTypes, "ReportBatchItemFailures") {
			var rest []map[string]any
			if rest, whole = unfinished(batch, res.body, kinesis); len(rest) > 0 {
				redo, partial = rest, true
			}
		}
		failed := whole || partial
		switch {
		case !failed:
			sh.queue, sh.retries = sh.queue[1:], 0
			result = "OK"
		case whole && m.Bisect && len(batch) > 1:
			half := len(batch) / 2
			sh.queue = append([][]map[string]any{batch[:half], batch[half:]}, sh.queue[1:]...)
			result = "PROBLEM: Function call failed"
		default:
			sh.retries++
			sh.queue[0] = redo
			if m.MaxRetry < 0 || sh.retries <= m.MaxRetry {
				result = "PROBLEM: Function call failed"
				break // retry the same batch next tick
			}
			if m.OnFailure != "" {
				info, _ := json.Marshal(map[string]any{"requestContext": map[string]any{"functionArn": m.FunctionArn, "condition": "RetryAttemptsExhausted",
					"approximateInvokeCount": sh.retries}, "version": "1.0", "timestamp": time.Now().UTC().Format(core.ISO), "records": len(redo)})
				p.sendFailure(m, shard, string(info))
			}
			sh.queue, sh.retries = sh.queue[1:], 0
			result = "OK"
		}
	}
	return result
}
