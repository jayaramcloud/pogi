package pipes

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/core"
)

// source is what the poller reads of SourceParameters.
type source struct {
	FilterCriteria *struct{ Filters []struct{ Pattern string } }

	SqsQueueParameters, KinesisStreamParameters, DynamoDBStreamParameters *batching
}

type batching struct {
	BatchSize, MaximumBatchingWindowInSeconds int
	StartingPosition                          string
	StartingPositionTimestamp                 float64
}

// of returns the batching of a source service, with AWS's default sizes.
func (sp *source) of(svc string) batching {
	b, size := sp.SqsQueueParameters, 10
	switch svc {
	case "kinesis":
		b, size = sp.KinesisStreamParameters, 100
	case "dynamodb":
		b, size = sp.DynamoDBStreamParameters, 100
	}
	var out batching
	if b != nil {
		out = *b
	}
	out.BatchSize = cmp.Or(out.BatchSize, size)
	return out
}

// keep applies the filter criteria: an event passes when any pattern matches.
func (sp *source) keep(rec json.RawMessage) bool {
	if sp.FilterCriteria == nil || len(sp.FilterCriteria.Filters) == 0 {
		return true
	}
	var ev map[string]any
	json.Unmarshal(view(rec), &ev)
	for _, f := range sp.FilterCriteria.Filters {
		if p, err := pattern.Compile(f.Pattern); err == nil && p.Match(ev) {
			return true
		}
	}
	return false
}

// cursor is a pipe's place in its source; it lives as long as the pipe, so a
// stopped pipe resumes where it was.
// ponytail: kept in memory, so a restarted pogi reads streams again from StartingPosition; persist shard sequence numbers if that matters.
type cursor struct {
	busy   bool              // a poll runs (s.mu)
	shards map[string]*shard // a stream's shards, once open
	since  time.Time         // when the batch now waiting saw its first record
}

// shard is where a stream shard is read next, and the last record delivered.
type shard struct{ iter, seq string }

// due reports whether n records waiting at the source go now: the batch is
// full, or its batching window has passed since the first of them arrived.
func (c *cursor) due(n int, b batching) bool {
	switch {
	case n == 0:
		c.since = time.Time{}
		return false
	case c.since.IsZero():
		c.since = time.Now()
	}
	if n < b.BatchSize && time.Since(c.since) < time.Duration(b.MaximumBatchingWindowInSeconds)*time.Second {
		return false
	}
	c.since = time.Time{}
	return true
}

func streamsAPI(svc string) string {
	if svc == "kinesis" {
		return "Kinesis_20131202"
	}
	return "DynamoDBStreams_20120810"
}

func queueURL(account, queue string) string { return "http://localhost/" + account + "/" + queue }

// open checks that an SQS source exists, or finds a stream's shards and
// their starting iterators once. A pipe reports RUNNING only after, so a
// LATEST pipe misses no record put once it runs.
func (s *Service) open(p *Pipe, cur *cursor) error {
	svc, region, account, res, _ := core.ParseARN(p.Source)
	switch {
	case svc == "sqs" && p.CurrentState == "RUNNING", svc != "sqs" && cur.shards != nil:
		return nil
	case svc == "sqs":
		return s.hub.Call(account, region, "AmazonSQS", "GetQueueAttributes", map[string]string{"QueueUrl": queueURL(account, res)}, nil)
	}
	var sp source
	p.SourceParameters.decode(&sp)
	var out struct {
		StreamDescription struct{ Shards []struct{ ShardId string } }
	}
	if err := s.hub.Call(account, region, streamsAPI(svc), "DescribeStream", map[string]string{"StreamArn": p.Source}, &out); err != nil {
		return err
	}
	shards := map[string]*shard{}
	for _, sh := range out.StreamDescription.Shards {
		shards[sh.ShardId] = &shard{}
		if err := s.iterate(p, sp.of(svc), sh.ShardId, shards[sh.ShardId]); err != nil {
			return err
		}
	}
	cur.shards = shards
	return nil
}

// iterate points a shard at the starting position, or just past the last
// record delivered. Kinesis (StreamARN, StartingSequenceNumber) and DynamoDB
// Streams (StreamArn, SequenceNumber) take their own names of each.
func (s *Service) iterate(p *Pipe, b batching, id string, sh *shard) error {
	svc, region, account, _, _ := core.ParseARN(p.Source)
	in := map[string]any{"StreamArn": p.Source, "ShardId": id, "ShardIteratorType": b.StartingPosition}
	switch {
	case sh.seq != "":
		in["ShardIteratorType"], in["SequenceNumber"], in["StartingSequenceNumber"] = "AFTER_SEQUENCE_NUMBER", sh.seq, sh.seq
	case b.StartingPosition == "AT_TIMESTAMP":
		in["Timestamp"] = b.StartingPositionTimestamp
	}
	var out struct{ ShardIterator string }
	err := s.hub.Call(account, region, streamsAPI(svc), "GetShardIterator", in, &out)
	sh.iter = out.ShardIterator
	return err
}

// pull moves what is due at the source through the pipe.
func (s *Service) pull(p *Pipe, cur *cursor) error {
	var sp source
	p.SourceParameters.decode(&sp)
	svc, region, account, res, _ := core.ParseARN(p.Source)
	if svc == "sqs" {
		return s.pullSQS(p, cur, &sp, account, region, queueURL(account, res))
	}
	return s.pullStream(p, cur, &sp, svc, account, region)
}

type sqsMessage struct {
	MessageId, ReceiptHandle, MD5OfBody, Body string
	Attributes                                map[string]string
	MessageAttributes                         map[string]struct {
		DataType, StringValue string
		BinaryValue           []byte
	}
}

// pullSQS receives a batch once it is due. Messages are deleted when the
// target took them, or when the filters drop them; a failed batch comes
// back after its visibility timeout.
func (s *Service) pullSQS(p *Pipe, cur *cursor, sp *source, account, region, url string) error {
	b := sp.of("sqs")
	var attrs struct{ Attributes map[string]string }
	if err := s.hub.Call(account, region, "AmazonSQS", "GetQueueAttributes", map[string]any{"QueueUrl": url,
		"AttributeNames": []string{"ApproximateNumberOfMessages"}}, &attrs); err != nil {
		return err
	}
	if n, _ := strconv.Atoi(attrs.Attributes["ApproximateNumberOfMessages"]); !cur.due(n, b) {
		return nil
	}
	var batch []json.RawMessage
	var done, dropped []string
	for got := 0; got < b.BatchSize; {
		var out struct{ Messages []sqsMessage }
		if err := s.hub.Call(account, region, "AmazonSQS", "ReceiveMessage", map[string]any{"QueueUrl": url, "MaxNumberOfMessages": min(10, b.BatchSize-got),
			"AttributeNames": []string{"All"}, "MessageAttributeNames": []string{"All"}}, &out); err != nil || len(out.Messages) == 0 {
			break
		}
		for _, m := range out.Messages {
			got++
			if rec := m.event(p.Source, region); sp.keep(rec) {
				batch, done = append(batch, rec), append(done, m.ReceiptHandle)
			} else {
				dropped = append(dropped, m.ReceiptHandle)
			}
		}
	}
	s.deleteMessages(account, region, url, dropped)
	if err := s.send(p, batch); err != nil {
		return err
	}
	s.deleteMessages(account, region, url, done)
	return nil
}

// event is the message in the shape Lambda and Pipes give SQS records.
func (m *sqsMessage) event(arn, region string) json.RawMessage {
	attrs := map[string]any{}
	for k, a := range m.MessageAttributes {
		v := map[string]any{"dataType": a.DataType, "stringListValues": []string{}, "binaryListValues": []string{}}
		if a.StringValue != "" {
			v["stringValue"] = a.StringValue
		}
		if a.BinaryValue != nil {
			v["binaryValue"] = a.BinaryValue
		}
		attrs[k] = v
	}
	return marshal(map[string]any{"messageId": m.MessageId, "receiptHandle": m.ReceiptHandle, "body": m.Body, "attributes": m.Attributes,
		"messageAttributes": attrs, "md5OfBody": m.MD5OfBody, "eventSource": "aws:sqs", "eventSourceARN": arn, "awsRegion": region})
}

func (s *Service) deleteMessages(account, region, url string, handles []string) {
	for i := 0; i < len(handles); i += 10 {
		var entries []map[string]string
		for j, h := range handles[i:min(i+10, len(handles))] {
			entries = append(entries, map[string]string{"Id": strconv.Itoa(j), "ReceiptHandle": h})
		}
		s.hub.Call(account, region, "AmazonSQS", "DeleteMessageBatch", map[string]any{"QueueUrl": url, "Entries": entries}, nil)
	}
}

// pullStream reads each shard from where it stands and, once the batch is
// due, sends each shard's records in order. A shard moves on only past what
// the target took, so a failed batch is read and sent again.
// ponytail: it retries forever; MaximumRetryAttempts, DeadLetterConfig and partial batch failures are stored, not acted on.
func (s *Service) pullStream(p *Pipe, cur *cursor, sp *source, svc, account, region string) error {
	b := sp.of(svc)
	type read struct {
		sh         *shard
		events     []json.RawMessage
		next, last string
	}
	var reads []read
	n := 0
	for id, sh := range cur.shards {
		if sh.iter == "" {
			if err := s.iterate(p, b, id, sh); err != nil {
				return err
			}
		}
		var out struct {
			Records           []map[string]any
			NextShardIterator string
		}
		if err := s.hub.Call(account, region, streamsAPI(svc), "GetRecords", map[string]any{"ShardIterator": sh.iter, "Limit": b.BatchSize}, &out); err != nil {
			sh.iter = "" // expired: reopen past the last record delivered
			continue
		}
		if len(out.Records) == 0 {
			sh.iter = out.NextShardIterator
			continue
		}
		rd := read{sh: sh, next: out.NextShardIterator}
		for _, r := range out.Records {
			var ev json.RawMessage
			if ev, rd.last = streamEvent(p, r, svc, id, region); sp.keep(ev) {
				rd.events = append(rd.events, ev)
			}
		}
		n += len(out.Records)
		reads = append(reads, rd)
	}
	if !cur.due(n, b) {
		return nil
	}
	for _, rd := range reads {
		if err := s.send(p, rd.events); err != nil {
			return err
		}
		rd.sh.iter, rd.sh.seq = rd.next, rd.last
	}
	return nil
}

// streamEvent shapes a stream record as pipes deliver it (Kinesis records
// flat, DynamoDB ones as they come) and returns its sequence number.
func streamEvent(p *Pipe, r map[string]any, svc, shard, region string) (json.RawMessage, string) {
	if svc != "kinesis" {
		ddb, _ := r["dynamodb"].(map[string]any)
		seq, _ := ddb["SequenceNumber"].(string)
		r["eventSourceARN"] = p.Source
		return marshal(r), seq
	}
	seq, _ := r["SequenceNumber"].(string)
	return marshal(map[string]any{"kinesisSchemaVersion": "1.0", "partitionKey": r["PartitionKey"], "sequenceNumber": seq,
		"data": r["Data"], "approximateArrivalTimestamp": r["ApproximateArrivalTimestamp"], "eventSource": "aws:kinesis",
		"eventVersion": "1.0", "eventID": shard + ":" + seq, "eventName": "aws:kinesis:record", "invokeIdentityArn": p.RoleArn,
		"awsRegion": region, "eventSourceARN": p.Source}), seq
}

// ---- enrichment and targets ----

// send runs a batch through the enrichment to the target; an error leaves
// the batch at the source.
func (s *Service) send(p *Pipe, batch []json.RawMessage) error {
	if len(batch) == 0 {
		return nil
	}
	if p.Enrichment != "" {
		var err error
		if batch, err = s.enrich(p, transform(p, p.EnrichmentParameters, batch)); err != nil || len(batch) == 0 {
			return err // an enrichment answering nothing drops the batch
		}
	}
	return s.deliver(p, batch)
}

// enrich calls the enrichment synchronously: Lambda and Step Functions
// take the batch as a JSON array, API Gateway and API destinations one
// event at a time. The answer is the new batch.
func (s *Service) enrich(p *Pipe, batch []json.RawMessage) ([]json.RawMessage, error) {
	var out []byte
	var err error
	switch svc, _, _, _, _ := core.ParseARN(p.Enrichment); svc {
	case "lambda":
		out, err = s.hub.Invoke(p.Enrichment, array(batch))
	case "states":
		out, err = s.express(p.Enrichment, array(batch))
	default:
		var all []json.RawMessage
		for _, ev := range batch {
			b, err := s.request(p.Enrichment, ev)
			if err != nil {
				return nil, err
			}
			all = append(all, records(b)...)
		}
		return all, nil
	}
	return records(out), err
}

// targetParams are the TargetParameters pogi acts on.
type targetParams struct {
	LambdaFunctionParameters, StepFunctionStateMachineParameters *struct{ InvocationType string }
	SqsQueueParameters                                           *struct{ MessageGroupId, MessageDeduplicationId string }
	KinesisStreamParameters                                      *struct{ PartitionKey string }
	EventBridgeEventBusParameters                                *struct {
		DetailType, Source string
		Resources          []string
	}
}

// deliver hands the batch, rendered by the target's InputTemplate, to the
// target. Lambda and Step Functions take it whole, and wait for the result
// unless InvocationType is FIRE_AND_FORGET; API Gateway takes one event per
// call; every other target gets one hub delivery per event, the dispatch
// EventBridge rules use.
func (s *Service) deliver(p *Pipe, batch []json.RawMessage) error {
	var tp targetParams
	p.TargetParameters.decode(&tp)
	svc, _, _, res, _ := core.ParseARN(p.Target)
	inputs := transform(p, p.TargetParameters, batch)
	async := func(x *struct{ InvocationType string }) bool {
		return x != nil && x.InvocationType == "FIRE_AND_FORGET"
	}
	switch {
	case svc == "lambda" && async(tp.LambdaFunctionParameters), svc == "states" && async(tp.StepFunctionStateMachineParameters):
		return s.hub.Deliver(p.Target, &core.Message{Body: string(array(inputs))})
	case svc == "lambda":
		_, err := s.hub.Invoke(p.Target, array(inputs))
		return err
	case svc == "states":
		_, err := s.express(p.Target, array(inputs))
		return err
	}
	for i, in := range inputs {
		var err error
		if svc == "execute-api" {
			_, err = s.request(p.Target, in)
		} else {
			err = s.hub.Deliver(p.Target, message(p, &tp, svc, res, batch[i], in))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// message is the input of one event for a hub target, with the target's
// parameters; a value starting with $. is a JSON path into the event.
func message(p *Pipe, tp *targetParams, svc, res string, ev, input json.RawMessage) *core.Message {
	v := string(view(ev))
	at := func(s string) string {
		if strings.HasPrefix(s, "$.") {
			x, _ := events.JSONPath(v, s)
			return events.Text(x)
		}
		return s
	}
	m := &core.Message{Body: events.Text(input)}
	switch {
	case tp.SqsQueueParameters != nil:
		m.GroupID, m.DedupID = at(tp.SqsQueueParameters.MessageGroupId), at(tp.SqsQueueParameters.MessageDeduplicationId)
	case tp.KinesisStreamParameters != nil: // core.Message has no partition key; GroupID carries it
		m.GroupID = at(tp.KinesisStreamParameters.PartitionKey)
	case svc == "events" && strings.HasPrefix(res, "event-bus/"):
		src, _, _, _, _ := core.ParseARN(p.Source)
		m.Source, m.DetailType = "Pipe "+p.Name, "Event from aws:"+src
		if eb := tp.EventBridgeEventBusParameters; eb != nil {
			m.Source, m.DetailType, m.Resources = cmp.Or(at(eb.Source), m.Source), cmp.Or(at(eb.DetailType), m.DetailType), eb.Resources
		}
	}
	return m
}

// express runs a state machine with StartSyncExecution and returns its output.
func (s *Service) express(arn string, input []byte) ([]byte, error) {
	_, region, account, _, _ := core.ParseARN(arn)
	var out struct{ Status, Output, Error, Cause string }
	if err := s.hub.Call(account, region, "AWSStepFunctions", "StartSyncExecution", map[string]string{"stateMachineArn": arn, "input": string(input)}, &out); err != nil {
		return nil, err
	}
	if out.Status != "SUCCEEDED" {
		return nil, fmt.Errorf("%s: execution %s: %s %s", arn, out.Status, out.Error, out.Cause)
	}
	return []byte(out.Output), nil
}

// request sends one event to an API destination, or to an API Gateway route
// (arn:…:execute-api:region:account:api/stage/METHOD/path), and returns the answer.
// ponytail: HttpParameters (headers, query, path values) are stored, not sent.
func (s *Service) request(arn string, ev json.RawMessage) ([]byte, error) {
	svc, region, account, res, _ := core.ParseARN(arn)
	if svc == "events" {
		e, ok := s.hub.Service("events").(*events.Service)
		if !ok {
			return nil, errors.New("events is not enabled, cannot call " + arn)
		}
		return e.Invoke(arn, events.Text(ev))
	}
	api, rest, _ := strings.Cut(res, "/")
	stage, rest, _ := strings.Cut(rest, "/")
	method, path, _ := strings.Cut(rest, "/")
	if method == "*" || method == "ANY" {
		method = http.MethodPost
	}
	status, _, body := s.hub.Do(account, region, "execute-api", method, "/_aws/execute-api/"+api+"/"+stage+"/"+path,
		http.Header{"Content-Type": {"application/json"}}, []byte(events.Text(ev)))
	if status >= 300 {
		return nil, fmt.Errorf("%s answered %d: %s", arn, status, body)
	}
	return body, nil
}

// ---- events ----

// transform applies an InputTemplate (of EnrichmentParameters or
// TargetParameters) to each event: <$.json.path> and the <aws.pipes.*>
// variables, as EventBridge renders input transformers. A template that
// renders no JSON makes a string event.
func transform(p *Pipe, params params, batch []json.RawMessage) []json.RawMessage {
	var tpl string
	if json.Unmarshal(params["InputTemplate"], &tpl); tpl == "" {
		return batch
	}
	vars := map[string]string{"aws.pipes.pipe-arn": p.Arn, "aws.pipes.pipe-name": p.Name, "aws.pipes.source-arn": p.Source,
		"aws.pipes.enrichment-arn": p.Enrichment, "aws.pipes.target-arn": p.Target,
		"aws.pipes.event.ingestion-time": time.Now().UTC().Format(time.RFC3339)}
	out := make([]json.RawMessage, len(batch))
	for i, ev := range batch {
		v := view(ev)
		res := events.Render(tpl, func(name string) (json.RawMessage, bool) {
			switch {
			case name == "aws.pipes.event":
				return ev, true
			case name == "aws.pipes.event.json":
				return v, true
			case strings.HasPrefix(name, "$"):
				x, _ := events.JSONPath(string(v), name)
				return x, true
			}
			s, ok := vars[name]
			return marshal(s), ok
		})
		if out[i] = json.RawMessage(res); !json.Valid(out[i]) {
			out[i] = marshal(res)
		}
	}
	return out
}

// view is an event as filters and templates see it: an SQS body or a
// base64 Kinesis data blob that holds JSON is parsed in place.
func view(ev json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(ev, &m) != nil || m == nil {
		return ev
	}
	var body, data string
	if json.Unmarshal(m["body"], &body) == nil && json.Valid([]byte(body)) {
		m["body"] = json.RawMessage(body)
	}
	if json.Unmarshal(m["data"], &data) == nil {
		if b, err := base64.StdEncoding.DecodeString(data); err == nil && json.Valid(b) {
			m["data"] = b
		}
	}
	return marshal(m)
}

// array is a batch as the JSON array Lambda and Step Functions get.
func array(batch []json.RawMessage) []byte {
	if batch == nil {
		batch = []json.RawMessage{}
	}
	return marshal(batch)
}

// records splits an enrichment's answer into events: an array's items,
// nothing for null or an empty array, else the one value (text becomes a string).
func records(b []byte) []json.RawMessage {
	b = bytes.TrimSpace(b)
	var list []json.RawMessage
	switch {
	case len(b) == 0 || string(b) == "null":
		return nil
	case json.Unmarshal(b, &list) == nil:
		return list
	case json.Valid(b):
		return []json.RawMessage{b}
	}
	return []json.RawMessage{marshal(string(b))}
}

// marshal encodes JSON without Go's HTML escaping, as AWS does.
func marshal(v any) json.RawMessage {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	e.Encode(v)
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}
