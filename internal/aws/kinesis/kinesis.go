// Package kinesis emulates Amazon Kinesis Data Streams over JSON 1.1: streams
// and shards, records routed by the MD5 of their partition key, sequence
// numbers, shard iterators, retention, resharding, consumers with
// SubscribeToShard event streams, tags and resource policies. Every operation accepts StreamName or StreamARN.
//
// The Java SDK (and so the KCL) talks legacy CBOR (application/x-amz-cbor-1.1)
// to Kinesis by default; core.RPC decodes it. If a Java client misreads a
// response, disable CBOR: -Daws.cborEnabled=false (SDK v2).
package kinesis

import (
	"cmp"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Kinesis Data Streams emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Streams map[string]*Stream `json:"streams"` // account/region/name
	Seq     uint64             `json:"seq"`     // last sequence number issued, shared by all shards
}

// Stream is one data stream.
type Stream struct {
	Name      string            `json:"name"`
	Account   string            `json:"account"`
	Region    string            `json:"region"`
	Created   time.Time         `json:"created"`
	Mode      string            `json:"mode"`      // PROVISIONED | ON_DEMAND
	Retention int               `json:"retention"` // hours
	Shards    []*Shard          `json:"shards"`
	NextShard int               `json:"nextShard"`
	Metrics   []string          `json:"metrics,omitempty"` // enhanced shard-level metrics
	KeyID     string            `json:"keyId,omitempty"`   // KMS key while encrypted
	Tags      map[string]string `json:"tags,omitempty"`
	Policy    string            `json:"policy,omitempty"`
	Consumers []*Consumer       `json:"consumers,omitempty"`
}

// Shard owns an inclusive hash key range; resharding closes it (EndSeq set).
type Shard struct {
	ID       string    `json:"id"`
	Parent   string    `json:"parent,omitempty"`
	Adjacent string    `json:"adjacent,omitempty"`
	Start    string    `json:"start"` // decimal hash keys
	End      string    `json:"end"`
	StartSeq string    `json:"startSeq"`
	EndSeq   string    `json:"endSeq,omitempty"`
	Created  time.Time `json:"created"`
	Closed   time.Time `json:"closed,omitzero"`
	Records  []*Record `json:"records,omitempty"`
}

// Record is one stored record.
type Record struct {
	Seq  string    `json:"seq"`
	Key  string    `json:"key"`
	Data []byte    `json:"data"`
	At   time.Time `json:"at"`
}

// Consumer is a registered enhanced fan-out consumer.
type Consumer struct {
	Name    string            `json:"name"`
	ARN     string            `json:"arn"`
	Created time.Time         `json:"created"`
	Tags    map[string]string `json:"tags,omitempty"`
	Policy  string            `json:"policy,omitempty"`
}

const shardLimit = 500 // per account and region, as in us-east-1

// New creates the Kinesis service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateStream":                  core.H(s.createStream),
		"DescribeStream":                core.H(s.describeStream),
		"DescribeStreamSummary":         core.H(s.describeStreamSummary),
		"ListStreams":                   core.H(s.listStreams),
		"DeleteStream":                  core.H(s.deleteStream),
		"PutRecord":                     core.H(s.putRecord),
		"PutRecords":                    core.H(s.putRecords),
		"GetShardIterator":              core.H(s.getShardIterator),
		"GetRecords":                    core.H(s.getRecords),
		"ListShards":                    core.H(s.listShards),
		"UpdateShardCount":              core.H(s.updateShardCount),
		"UpdateStreamMode":              core.H(s.updateStreamMode),
		"IncreaseStreamRetentionPeriod": core.H(s.retention),
		"DecreaseStreamRetentionPeriod": core.H(s.retention),
		"EnableEnhancedMonitoring":      core.H(s.monitoring),
		"DisableEnhancedMonitoring":     core.H(s.monitoring),
		"StartStreamEncryption":         core.H(s.encryption),
		"StopStreamEncryption":          core.H(s.encryption),
		"AddTagsToStream":               core.H(s.tags),
		"RemoveTagsFromStream":          core.H(s.tags),
		"ListTagsForStream":             core.H(s.tags),
		"TagResource":                   core.H(s.tags),
		"UntagResource":                 core.H(s.tags),
		"ListTagsForResource":           core.H(s.tags),
		"RegisterStreamConsumer":        core.H(s.registerConsumer),
		"DescribeStreamConsumer":        core.H(s.describeConsumer),
		"ListStreamConsumers":           core.H(s.listConsumers),
		"DeregisterStreamConsumer":      core.H(s.deregisterConsumer),
		"PutResourcePolicy":             core.H(s.policy),
		"GetResourcePolicy":             core.H(s.policy),
		"DeleteResourcePolicy":          core.H(s.policy),
		"DescribeLimits":                core.H(s.describeLimits),
	}}
	if hub != nil {
		hub.OnDeliver("kinesis", s.deliver)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "kinesis", Cloud: core.AWS, Title: "Kinesis Data Streams", Targets: []string{"Kinesis_20131202"}, Signing: []string{"kinesis"}, Protocol: "json"}
}

// ServeHTTP serves JSON and, through core.RPC, the Java SDK's legacy CBOR.
// SubscribeToShard answers with an event stream, which RPC cannot send.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".SubscribeToShard") {
		s.subscribe(w, r)
		return
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Streams: map[string]*Stream{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Streams == nil {
		st.Streams = map[string]*Stream{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidArgumentException", format, a...)
}

func validation(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func (st *Stream) arn() string { return core.ARN("kinesis", st.Region, st.Account, "stream/"+st.Name) }

func (st *Stream) encType() string {
	if st.KeyID != "" {
		return "KMS"
	}
	return "NONE"
}

func (st *Stream) open() []*Shard {
	var out []*Shard
	for _, sh := range st.Shards {
		if sh.EndSeq == "" {
			out = append(out, sh)
		}
	}
	return out
}

func (st *Stream) shard(id string) (*Shard, error) {
	for _, sh := range st.Shards {
		if sh.ID == id {
			return sh, nil
		}
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "Shard %s in stream %s under account %s does not exist", id, st.Name, st.Account)
}

// seqNum formats sequence numbers like AWS: 56 digits starting 4959. Fixed
// width keeps them ordered as strings.
func seqNum(n uint64) string { return fmt.Sprintf("4959%052d", n) }

// cmpSeq orders decimal sequence numbers; "" sorts first.
func cmpSeq(a, b string) int { return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b)) }

// page keeps the sorted items whose id is after `after`, at most limit (0: all),
// and reports whether more remain.
func page[T any](items []T, id func(T) string, after string, limit int) ([]T, bool) {
	out := []T{}
	for _, it := range items {
		if id(it) <= after {
			continue
		}
		if limit > 0 && len(out) == limit {
			return out, true
		}
		out = append(out, it)
	}
	return out, false
}

type ref struct {
	StreamName string
	StreamARN  string
}

// stream resolves a StreamName (caller's account and region) or StreamARN; lock held.
func (s *Service) stream(c *core.Ctx, r ref) (*Stream, error) {
	account, region, name := c.Account, c.Region, r.StreamName
	if r.StreamARN != "" {
		_, rg, acct, res, ok := core.ParseARN(r.StreamARN)
		if !ok || !strings.HasPrefix(res, "stream/") {
			return nil, invalid("Invalid StreamARN %s", r.StreamARN)
		}
		account, region, name = acct, rg, strings.TrimPrefix(res, "stream/")
	}
	if name == "" {
		return nil, invalid("Either StreamName or StreamARN must be provided.")
	}
	if st := s.st.Streams[key(account, region, name)]; st != nil {
		return st, nil
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "Stream %s under account %s not found.", name, account)
}

// consumer finds a consumer by ARN, or by stream ARN and name; lock held.
func (s *Service) consumer(c *core.Ctx, streamARN, name, arn string) (*Stream, *Consumer, error) {
	if arn != "" {
		streamARN, _, _ = strings.Cut(arn, "/consumer/")
	}
	st, err := s.stream(c, ref{StreamARN: streamARN})
	if err != nil {
		return nil, nil, err
	}
	for _, con := range st.Consumers {
		if arn != "" && con.ARN == arn || arn == "" && con.Name == name {
			return st, con, nil
		}
	}
	return nil, nil, core.Errorf(400, "ResourceNotFoundException", "Consumer %s under stream %s not found.", cmp.Or(arn, name), st.Name)
}

// resource resolves a stream or consumer ARN, or else the stream in r; lock held.
func (s *Service) resource(c *core.Ctx, r ref, arn string) (*Stream, *Consumer, error) {
	if strings.Contains(arn, "/consumer/") {
		return s.consumer(c, "", "", arn)
	}
	if arn != "" {
		r = ref{StreamARN: arn}
	}
	st, err := s.stream(c, r)
	return st, nil, err
}

// usage counts open shards and on-demand streams in an account and region; lock held.
func (s *Service) usage(account, region string) (open, onDemand int) {
	for _, st := range s.st.Streams {
		if st.Account == account && st.Region == region {
			open += len(st.open())
			if st.Mode == "ON_DEMAND" {
				onDemand++
			}
		}
	}
	return open, onDemand
}

// ---- shards and hash keys ----

var hashSpace = new(big.Int).Lsh(big.NewInt(1), 128)

// addShards opens n shards splitting the hash space evenly. Each new shard's
// parents are the replaced shards owning the two ends of its range; lock held.
// ponytail: a new shard spanning three old ones records only two parents (AWS
// reshards by split/merge chains); fine unless KCL ordering across reshards matters.
func (s *Service) addShards(st *Stream, n int, replaced []*Shard) {
	now := time.Now().UTC()
	bound := func(i int) *big.Int {
		return new(big.Int).Div(new(big.Int).Mul(hashSpace, big.NewInt(int64(i))), big.NewInt(int64(n)))
	}
	for i := range n {
		lo, hi := bound(i), bound(i+1)
		hi.Sub(hi, big.NewInt(1))
		sh := &Shard{ID: fmt.Sprintf("shardId-%012d", st.NextShard), Start: lo.String(), End: hi.String(), StartSeq: seqNum(s.st.Seq), Created: now}
		st.NextShard++
		if p := owner(replaced, lo); p != nil {
			sh.Parent = p.ID
		}
		if a := owner(replaced, hi); a != nil && a.ID != sh.Parent {
			sh.Adjacent = a.ID
		}
		st.Shards = append(st.Shards, sh)
	}
}

// owner returns the shard whose hash key range holds h.
func owner(shards []*Shard, h *big.Int) *Shard {
	for _, sh := range shards {
		lo, _ := new(big.Int).SetString(sh.Start, 10)
		hi, _ := new(big.Int).SetString(sh.End, 10)
		if h.Cmp(lo) >= 0 && h.Cmp(hi) <= 0 {
			return sh
		}
	}
	return nil
}

// hashKey validates a record and returns its 128-bit hash key: the explicit
// one, or the MD5 of the partition key read as a big-endian integer.
func hashKey(partitionKey, explicit string, data []byte) (*big.Int, error) {
	if n := utf8.RuneCountInString(partitionKey); n < 1 || n > 256 {
		return nil, validation("1 validation error detected: Value '%s' at 'partitionKey' failed to satisfy constraint: Member must have length between 1 and 256", partitionKey)
	}
	if data == nil { // Data is required; an empty blob ("") decodes to a non-nil slice
		return nil, validation("1 validation error detected: Value null at 'data' failed to satisfy constraint: Member must not be null")
	}
	if len(data) > 1<<20 {
		return nil, validation("1 validation error detected: Value at 'data' failed to satisfy constraint: Member must have length less than or equal to 1048576")
	}
	if explicit == "" {
		sum := md5.Sum([]byte(partitionKey))
		return new(big.Int).SetBytes(sum[:]), nil
	}
	h, ok := new(big.Int).SetString(explicit, 10)
	if !ok || h.Sign() < 0 || h.Cmp(hashSpace) >= 0 {
		return nil, invalid("Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. Specified value was %s", explicit)
	}
	return h, nil
}

// store appends a validated record to the open shard owning h; lock held.
func (s *Service) store(st *Stream, h *big.Int, partitionKey string, data []byte) (*Shard, *Record) {
	sh := owner(st.open(), h)
	st.trim(sh)
	if data == nil {
		data = []byte{}
	}
	s.st.Seq++
	r := &Record{Seq: seqNum(s.st.Seq), Key: partitionKey, Data: data, At: time.Now().UTC()}
	sh.Records = append(sh.Records, r)
	return sh, r
}

// trim drops records older than the retention period; lock held.
func (st *Stream) trim(sh *Shard) {
	cut := time.Now().Add(-time.Duration(st.Retention) * time.Hour)
	i := sort.Search(len(sh.Records), func(i int) bool { return sh.Records[i].At.After(cut) })
	clear(sh.Records[:i])
	sh.Records = sh.Records[i:]
}

type hashRange struct{ StartingHashKey, EndingHashKey string }

type shardOut struct {
	ShardId               string
	ParentShardId         string `json:",omitempty"`
	AdjacentParentShardId string `json:",omitempty"`
	HashKeyRange          hashRange
	SequenceNumberRange   struct {
		StartingSequenceNumber string
		EndingSequenceNumber   string `json:",omitempty"`
	}
}

func outShards(shards []*Shard) []shardOut {
	out := []shardOut{}
	for _, sh := range shards {
		o := shardOut{ShardId: sh.ID, ParentShardId: sh.Parent, AdjacentParentShardId: sh.Adjacent, HashKeyRange: hashRange{sh.Start, sh.End}}
		o.SequenceNumberRange.StartingSequenceNumber, o.SequenceNumberRange.EndingSequenceNumber = sh.StartSeq, sh.EndSeq
		out = append(out, o)
	}
	return out
}

func shardID(sh *Shard) string { return sh.ID }

// ---- streams ----

var streamName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)

type modeDetails struct{ StreamMode string }

func (s *Service) createStream(c *core.Ctx, in *struct {
	StreamName        string
	ShardCount        *int
	StreamModeDetails *modeDetails
	Tags              map[string]string
}) (any, error) {
	if !streamName.MatchString(in.StreamName) {
		return nil, validation("1 validation error detected: Value '%s' at 'streamName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", in.StreamName)
	}
	n, mode := 4, "ON_DEMAND" // on-demand streams start with four shards
	if in.ShardCount != nil && (in.StreamModeDetails == nil || in.StreamModeDetails.StreamMode != "ON_DEMAND") {
		n, mode = *in.ShardCount, "PROVISIONED"
	}
	if n < 1 {
		return nil, validation("1 validation error detected: Value '%d' at 'shardCount' failed to satisfy constraint: Member must have value greater than or equal to 1", n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.StreamName)
	if s.st.Streams[k] != nil {
		return nil, core.Errorf(400, "ResourceInUseException", "Stream %s under account %s already exists.", in.StreamName, c.Account)
	}
	if open, _ := s.usage(c.Account, c.Region); open+n > shardLimit {
		return nil, core.Errorf(400, "LimitExceededException", "This request would exceed the shard limit for the account %s in %s. Current shard count for the account: %d. Limit: %d. Number of additional shards that would have resulted from this request: %d.", c.Account, c.Region, open, shardLimit, n)
	}
	st := &Stream{Name: in.StreamName, Account: c.Account, Region: c.Region, Created: time.Now().UTC(), Mode: mode, Retention: 24, Tags: in.Tags}
	s.addShards(st, n, nil)
	s.st.Streams[k] = st
	return nil, nil
}

// describe is what DescribeStream and DescribeStreamSummary share.
func (st *Stream) describe() map[string]any {
	d := map[string]any{
		"StreamName": st.Name, "StreamARN": st.arn(), "StreamStatus": "ACTIVE", "StreamModeDetails": modeDetails{st.Mode},
		"RetentionPeriodHours": st.Retention, "StreamCreationTimestamp": core.Unix(st.Created), "EncryptionType": st.encType(),
		"EnhancedMonitoring": []any{map[string]any{"ShardLevelMetrics": append([]string{}, st.Metrics...)}},
	}
	if st.KeyID != "" {
		d["KeyId"] = st.KeyID
	}
	return d
}

func (s *Service) describeStream(c *core.Ctx, in *struct {
	ref
	ExclusiveStartShardId string
	Limit                 int
}) (any, error) {
	limit := cmp.Or(in.Limit, 100)
	if limit < 1 || limit > 10000 {
		return nil, invalid("Limit must be between 1 and 10000.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	shards, more := page(st.Shards, shardID, in.ExclusiveStartShardId, limit)
	d := st.describe()
	d["Shards"], d["HasMoreShards"] = outShards(shards), more
	return map[string]any{"StreamDescription": d}, nil
}

func (s *Service) describeStreamSummary(c *core.Ctx, in *ref) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, *in)
	if err != nil {
		return nil, err
	}
	d := st.describe()
	d["OpenShardCount"], d["ConsumerCount"] = len(st.open()), len(st.Consumers)
	return map[string]any{"StreamDescriptionSummary": d}, nil
}

func (s *Service) listStreams(c *core.Ctx, in *struct {
	ExclusiveStartStreamName string
	Limit                    int
	NextToken                string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var mine []*Stream
	for _, k := range core.SortedKeys(s.st.Streams) {
		if st := s.st.Streams[k]; st.Account == c.Account && st.Region == c.Region {
			mine = append(mine, st)
		}
	}
	got, more := page(mine, func(st *Stream) string { return st.Name }, cmp.Or(in.NextToken, in.ExclusiveStartStreamName), min(cmp.Or(in.Limit, 100), 100))
	names, sums := []string{}, []map[string]any{}
	for _, st := range got {
		names = append(names, st.Name)
		sums = append(sums, map[string]any{"StreamName": st.Name, "StreamARN": st.arn(), "StreamStatus": "ACTIVE",
			"StreamModeDetails": modeDetails{st.Mode}, "StreamCreationTimestamp": core.Unix(st.Created)})
	}
	out := map[string]any{"StreamNames": names, "StreamSummaries": sums, "HasMoreStreams": more}
	if more {
		out["NextToken"] = names[len(names)-1]
	}
	return out, nil
}

func (s *Service) deleteStream(c *core.Ctx, in *struct {
	ref
	EnforceConsumerDeletion bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	if len(st.Consumers) > 0 && !in.EnforceConsumerDeletion {
		return nil, core.Errorf(400, "ResourceInUseException", "Stream %s under account %s has registered consumers.", st.Name, st.Account)
	}
	delete(s.st.Streams, key(st.Account, st.Region, st.Name))
	return nil, nil
}

// ---- records ----

type putEntry struct {
	PartitionKey    string
	ExplicitHashKey string
	Data            []byte
}

func (s *Service) putRecord(c *core.Ctx, in *struct {
	ref
	putEntry
}) (any, error) {
	h, err := hashKey(in.PartitionKey, in.ExplicitHashKey, in.Data)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	sh, r := s.store(st, h, in.PartitionKey, in.Data)
	return map[string]string{"ShardId": sh.ID, "SequenceNumber": r.Seq, "EncryptionType": st.encType()}, nil
}

func (s *Service) putRecords(c *core.Ctx, in *struct {
	ref
	Records []putEntry
}) (any, error) {
	if len(in.Records) < 1 || len(in.Records) > 500 {
		return nil, validation("1 validation error detected: Value at 'records' failed to satisfy constraint: Member must have length between 1 and 500")
	}
	hs := make([]*big.Int, len(in.Records))
	for i, r := range in.Records { // a bad record fails the whole call, before anything is stored
		var err error
		if hs[i], err = hashKey(r.PartitionKey, r.ExplicitHashKey, r.Data); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	res := []map[string]string{}
	for i, r := range in.Records {
		sh, rec := s.store(st, hs[i], r.PartitionKey, r.Data)
		res = append(res, map[string]string{"ShardId": sh.ID, "SequenceNumber": rec.Seq})
	}
	return map[string]any{"FailedRecordCount": 0, "Records": res, "EncryptionType": st.encType()}, nil
}

// deliver puts a record from another service (EventBridge, SNS, Pipes…) onto a stream.
func (s *Service) deliver(arn string, m *core.Message) error {
	pk := cmp.Or(m.GroupID, core.UUID())
	h, err := hashKey(pk, "", []byte(m.Body))
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(&core.Ctx{}, ref{StreamARN: arn})
	if err != nil {
		return err
	}
	s.store(st, h, pk, []byte(m.Body))
	return nil
}

// iterator is the decoded form of an opaque shard iterator.
type iterator struct {
	Stream string `json:"s"` // stream ARN
	Shard  string `json:"h"`
	After  string `json:"a,omitempty"` // read records with a greater sequence number…
	At     bool   `json:"i,omitempty"` // …or an equal one (AT_SEQUENCE_NUMBER)
	Issued int64  `json:"t"`           // unix millis; GetRecords rejects it after 5 minutes
}

// encode issues the iterator now.
func (it iterator) encode() string {
	it.Issued = time.Now().UnixMilli()
	b, _ := json.Marshal(it)
	return base64.StdEncoding.EncodeToString(b)
}

// millis reads an epoch-seconds timestamp (JSON number) to the millisecond.
func millis(sec float64) int64 { return int64(math.Round(sec * 1000)) }

func (s *Service) getShardIterator(c *core.Ctx, in *struct {
	ref
	ShardId                string
	ShardIteratorType      string
	StartingSequenceNumber string
	Timestamp              *float64
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	sh, err := st.shard(in.ShardId)
	if err != nil {
		return nil, err
	}
	it, err := st.position(sh, in.ShardIteratorType, in.StartingSequenceNumber, in.Timestamp)
	if err != nil {
		return nil, err
	}
	return map[string]string{"ShardIterator": it.encode()}, nil
}

// position is the iterator for a GetShardIterator or SubscribeToShard
// starting position; lock held.
func (st *Stream) position(sh *Shard, typ, seq string, ts *float64) (iterator, error) {
	st.trim(sh)
	it := iterator{Stream: st.arn(), Shard: sh.ID}
	switch typ {
	case "TRIM_HORIZON":
	case "LATEST":
		if n := len(sh.Records); n > 0 {
			it.After = sh.Records[n-1].Seq
		}
	case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
		if seq == "" || strings.Trim(seq, "0123456789") != "" {
			return it, invalid("StartingSequenceNumber %s used in GetShardIterator on shard %s in stream %s under account %s is invalid.", seq, sh.ID, st.Name, st.Account)
		}
		it.After, it.At = seq, typ == "AT_SEQUENCE_NUMBER"
	case "AT_TIMESTAMP":
		if ts == nil {
			return it, invalid("Must specify timestampInMillis parameter for iterator of type AT_TIMESTAMP. Current request has no timestamp parameter.")
		}
		for _, r := range sh.Records {
			if r.At.UnixMilli() >= millis(*ts) {
				break
			}
			it.After = r.Seq
		}
	default:
		return it, validation("1 validation error detected: Value '%s' at 'shardIteratorType' failed to satisfy constraint: Member must satisfy enum value set: [AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST, AT_TIMESTAMP]", typ)
	}
	return it, nil
}

// after returns the records the iterator has not read yet.
func (sh *Shard) after(it iterator) []*Record {
	i := sort.Search(len(sh.Records), func(i int) bool {
		d := cmpSeq(sh.Records[i].Seq, it.After)
		return d > 0 || d == 0 && it.At
	})
	return sh.Records[i:]
}

type outRecord struct {
	SequenceNumber              string
	ApproximateArrivalTimestamp float64
	Data                        []byte
	PartitionKey                string
	EncryptionType              string `json:",omitempty"`
}

func (s *Service) getRecords(c *core.Ctx, in *struct {
	ShardIterator string
	Limit         int
}) (any, error) {
	limit := cmp.Or(in.Limit, 10000)
	if limit < 1 || limit > 10000 {
		return nil, invalid("Limit must be between 1 and 10000.")
	}
	var it iterator
	b, err := base64.StdEncoding.DecodeString(in.ShardIterator)
	if err != nil || json.Unmarshal(b, &it) != nil || it.Stream == "" {
		return nil, invalid("Invalid ShardIterator.")
	}
	if issued := time.UnixMilli(it.Issued); time.Since(issued) > 5*time.Minute {
		return nil, core.Errorf(400, "ExpiredIteratorException", "Iterator expired. The iterator was created at time %s while right now it is %s which is further in the future than the tolerated delay of 300000 milliseconds.",
			issued.UTC().Format(time.UnixDate), time.Now().UTC().Format(time.UnixDate))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, ref{StreamARN: it.Stream})
	if err != nil {
		return nil, err
	}
	sh, err := st.shard(it.Shard)
	if err != nil {
		return nil, err
	}
	st.trim(sh)
	unread := sh.after(it)
	batch := unread[:min(limit, len(unread))]
	rest := unread[len(batch):]
	enc := ""
	if st.KeyID != "" {
		enc = "KMS"
	}
	recs := []outRecord{}
	for _, r := range batch {
		recs = append(recs, outRecord{r.Seq, core.Unix(r.At), r.Data, r.Key, enc})
	}
	out := map[string]any{"Records": recs, "MillisBehindLatest": 0}
	if len(batch) > 0 {
		it.After, it.At = batch[len(batch)-1].Seq, false
	}
	if len(rest) > 0 {
		out["MillisBehindLatest"] = time.Since(rest[0].At).Milliseconds()
	} else if sh.EndSeq != "" { // a closed shard read to its end: point at its children
		out["ChildShards"] = st.children(sh)
		return out, nil
	}
	out["NextShardIterator"] = it.encode()
	return out, nil
}

// children lists the shards that replaced sh.
func (st *Stream) children(sh *Shard) []map[string]any {
	out := []map[string]any{}
	for _, ch := range st.Shards {
		if ch.Parent == sh.ID || ch.Adjacent == sh.ID {
			parents := slices.DeleteFunc([]string{ch.Parent, ch.Adjacent}, func(p string) bool { return p == "" })
			out = append(out, map[string]any{"ShardId": ch.ID, "ParentShards": parents, "HashKeyRange": hashRange{ch.Start, ch.End}})
		}
	}
	return out
}

// ---- shards ----

type shardFilter struct {
	Type      string
	ShardId   string
	Timestamp *float64
}

// keep applies a ListShards ShardFilter. Shards never expire here, so the
// trim horizon is the stream's original shards.
func (f *shardFilter) keep(sh *Shard) bool {
	if f == nil {
		return true
	}
	var t time.Time
	if f.Timestamp != nil {
		t = time.UnixMilli(millis(*f.Timestamp))
	}
	openAt := sh.EndSeq == "" || sh.Closed.After(t)
	switch f.Type {
	case "AFTER_SHARD_ID":
		return sh.ID > f.ShardId
	case "AT_TRIM_HORIZON":
		return sh.Parent == ""
	case "AT_LATEST":
		return sh.EndSeq == ""
	case "AT_TIMESTAMP":
		return openAt && (!sh.Created.After(t) || sh.Parent == "")
	case "FROM_TIMESTAMP":
		return openAt
	}
	return true // FROM_TRIM_HORIZON
}

func (s *Service) listShards(c *core.Ctx, in *struct {
	ref
	NextToken             string
	ExclusiveStartShardId string
	MaxResults            int
	ShardFilter           *shardFilter
}) (any, error) {
	after := in.ExclusiveStartShardId
	if in.NextToken != "" {
		if in.StreamName != "" {
			return nil, invalid("NextToken and StreamName cannot be provided together.")
		}
		b, _ := base64.StdEncoding.DecodeString(in.NextToken)
		arn, last, ok := strings.Cut(string(b), "|")
		if !ok {
			return nil, invalid("Invalid NextToken.")
		}
		in.StreamARN, after = arn, last
	}
	limit := cmp.Or(in.MaxResults, 1000)
	if limit < 1 || limit > 10000 {
		return nil, invalid("MaxResults must be between 1 and 10000.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	shards, more := page(slices.DeleteFunc(slices.Clone(st.Shards), func(sh *Shard) bool { return !in.ShardFilter.keep(sh) }), shardID, after, limit)
	out := map[string]any{"Shards": outShards(shards)}
	if more {
		out["NextToken"] = base64.StdEncoding.EncodeToString([]byte(st.arn() + "|" + shards[len(shards)-1].ID))
	}
	return out, nil
}

func (s *Service) updateShardCount(c *core.Ctx, in *struct {
	ref
	TargetShardCount int
	ScalingType      string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	open, n := st.open(), in.TargetShardCount
	switch total, _ := s.usage(st.Account, st.Region); {
	case n < 1:
		return nil, validation("1 validation error detected: Value '%d' at 'targetShardCount' failed to satisfy constraint: Member must have value greater than or equal to 1", n)
	case n > 2*len(open):
		return nil, invalid("UpdateShardCount cannot scale up over double your current open shard count. Current open shard count: %d Target shard count: %d", len(open), n)
	case 2*n < len(open):
		return nil, invalid("UpdateShardCount cannot scale down below half your current open shard count. Current open shard count: %d Target shard count: %d", len(open), n)
	case total-len(open)+n > shardLimit:
		return nil, core.Errorf(400, "LimitExceededException", "This request would exceed the shard limit for the account %s in %s. Limit: %d.", st.Account, st.Region, shardLimit)
	}
	if n != len(open) {
		now := time.Now().UTC()
		for _, sh := range open {
			sh.EndSeq, sh.Closed = seqNum(s.st.Seq), now
		}
		s.addShards(st, n, open)
	}
	return map[string]any{"StreamName": st.Name, "StreamARN": st.arn(), "CurrentShardCount": len(open), "TargetShardCount": n}, nil
}

func (s *Service) updateStreamMode(c *core.Ctx, in *struct {
	ref
	StreamModeDetails modeDetails
}) (any, error) {
	if m := in.StreamModeDetails.StreamMode; m != "PROVISIONED" && m != "ON_DEMAND" {
		return nil, validation("1 validation error detected: Value '%s' at 'streamModeDetails.streamMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, ON_DEMAND]", m)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	st.Mode = in.StreamModeDetails.StreamMode
	return nil, nil
}

// ---- settings: retention, monitoring, encryption ----

func (s *Service) retention(c *core.Ctx, in *struct {
	ref
	RetentionPeriodHours int
}) (any, error) {
	h := in.RetentionPeriodHours
	if h < 24 || h > 8760 {
		return nil, invalid("Retention period must be between 24 and 8760 hours. Requested retention period (%d hours) is invalid.", h)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	switch up := c.Op == "IncreaseStreamRetentionPeriod"; {
	case up && h < st.Retention:
		return nil, invalid("Requested retention period (%d hours) for stream %s can not be shorter than existing retention period (%d hours). Use DecreaseRetentionPeriod API.", h, st.Name, st.Retention)
	case !up && h > st.Retention:
		return nil, invalid("Requested retention period (%d hours) for stream %s can not be longer than existing retention period (%d hours). Use IncreaseRetentionPeriod API.", h, st.Name, st.Retention)
	}
	st.Retention = h
	return nil, nil
}

var allMetrics = []string{"IncomingBytes", "IncomingRecords", "OutgoingBytes", "OutgoingRecords",
	"WriteProvisionedThroughputExceeded", "ReadProvisionedThroughputExceeded", "IteratorAgeMilliseconds"}

// monitoring records enhanced shard-level metrics; pogi emits none.
func (s *Service) monitoring(c *core.Ctx, in *struct {
	ref
	ShardLevelMetrics []string
}) (any, error) {
	want := in.ShardLevelMetrics
	if slices.Contains(want, "ALL") {
		want = allMetrics
	}
	for _, m := range want {
		if !slices.Contains(allMetrics, m) {
			return nil, validation("1 validation error detected: Value '%s' at 'shardLevelMetrics' failed to satisfy constraint: Member must satisfy enum value set: [%s, ALL]", m, strings.Join(allMetrics, ", "))
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	before := append([]string{}, st.Metrics...)
	on := map[string]bool{}
	for _, m := range st.Metrics {
		on[m] = true
	}
	for _, m := range want {
		on[m] = c.Op == "EnableEnhancedMonitoring"
	}
	st.Metrics = nil
	for _, m := range allMetrics {
		if on[m] {
			st.Metrics = append(st.Metrics, m)
		}
	}
	return map[string]any{"StreamName": st.Name, "StreamARN": st.arn(), "CurrentShardLevelMetrics": before,
		"DesiredShardLevelMetrics": append([]string{}, st.Metrics...)}, nil
}

// encryption records server-side encryption; records are stored as sent.
func (s *Service) encryption(c *core.Ctx, in *struct {
	ref
	EncryptionType string
	KeyId          string
}) (any, error) {
	start := c.Op == "StartStreamEncryption"
	if in.EncryptionType != "KMS" || start && in.KeyId == "" {
		return nil, invalid("EncryptionType must be KMS and KeyId is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, in.ref)
	if err != nil {
		return nil, err
	}
	st.KeyID = ""
	if start {
		st.KeyID = in.KeyId
	}
	return nil, nil
}

// ---- tags, consumers, policies, limits ----

// tags serves the stream (…Stream) and ARN-addressed (…Resource) tag operations.
func (s *Service) tags(c *core.Ctx, in *struct {
	ref
	ResourceARN          string
	Tags                 map[string]string
	TagKeys              []string
	ExclusiveStartTagKey string
	Limit                int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, con, err := s.resource(c, in.ref, in.ResourceARN)
	if err != nil {
		return nil, err
	}
	tags := &st.Tags
	if con != nil {
		tags = &con.Tags
	}
	switch c.Op {
	case "AddTagsToStream", "TagResource":
		if *tags == nil {
			*tags = map[string]string{}
		}
		maps.Copy(*tags, in.Tags)
		return nil, nil
	case "RemoveTagsFromStream", "UntagResource":
		for _, k := range in.TagKeys {
			delete(*tags, k)
		}
		return nil, nil
	}
	keys, more := page(core.SortedKeys(*tags), func(k string) string { return k }, in.ExclusiveStartTagKey, in.Limit)
	list := []map[string]string{}
	for _, k := range keys {
		list = append(list, map[string]string{"Key": k, "Value": (*tags)[k]})
	}
	if c.Op == "ListTagsForResource" {
		return map[string]any{"Tags": list}, nil
	}
	return map[string]any{"Tags": list, "HasMoreTags": more}, nil
}

type consumerInput struct {
	StreamARN    string
	ConsumerName string
	ConsumerARN  string
	Tags         map[string]string
}

func (con *Consumer) out() map[string]any {
	return map[string]any{"ConsumerName": con.Name, "ConsumerARN": con.ARN, "ConsumerStatus": "ACTIVE", "ConsumerCreationTimestamp": core.Unix(con.Created)}
}

func (s *Service) registerConsumer(c *core.Ctx, in *consumerInput) (any, error) {
	if !streamName.MatchString(in.ConsumerName) {
		return nil, validation("1 validation error detected: Value '%s' at 'consumerName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", in.ConsumerName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, ref{StreamARN: in.StreamARN})
	if err != nil {
		return nil, err
	}
	if slices.ContainsFunc(st.Consumers, func(x *Consumer) bool { return x.Name == in.ConsumerName }) {
		return nil, core.Errorf(400, "ResourceInUseException", "Consumer %s under stream %s already exists.", in.ConsumerName, st.Name)
	}
	now := time.Now().UTC()
	con := &Consumer{Name: in.ConsumerName, ARN: fmt.Sprintf("%s/consumer/%s:%d", st.arn(), in.ConsumerName, now.Unix()), Created: now, Tags: in.Tags}
	st.Consumers = append(st.Consumers, con)
	return map[string]any{"Consumer": con.out()}, nil
}

func (s *Service) describeConsumer(c *core.Ctx, in *consumerInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, con, err := s.consumer(c, in.StreamARN, in.ConsumerName, in.ConsumerARN)
	if err != nil {
		return nil, err
	}
	d := con.out()
	d["StreamARN"] = st.arn()
	return map[string]any{"ConsumerDescription": d}, nil
}

// listConsumers returns every consumer in one page.
func (s *Service) listConsumers(c *core.Ctx, in *consumerInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(c, ref{StreamARN: in.StreamARN})
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, con := range st.Consumers {
		list = append(list, con.out())
	}
	return map[string]any{"Consumers": list}, nil
}

func (s *Service) deregisterConsumer(c *core.Ctx, in *consumerInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, con, err := s.consumer(c, in.StreamARN, in.ConsumerName, in.ConsumerARN)
	if err != nil {
		return nil, err
	}
	st.Consumers = slices.DeleteFunc(st.Consumers, func(x *Consumer) bool { return x == con })
	return nil, nil
}

// policy stores resource policies on streams and consumers; nothing enforces them.
func (s *Service) policy(c *core.Ctx, in *struct {
	ResourceARN string
	Policy      string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, con, err := s.resource(c, ref{}, in.ResourceARN)
	if err != nil {
		return nil, err
	}
	p := &st.Policy
	if con != nil {
		p = &con.Policy
	}
	switch c.Op {
	case "PutResourcePolicy":
		if !json.Valid([]byte(in.Policy)) {
			return nil, invalid("Policy is not a valid JSON document.")
		}
		*p = in.Policy
	case "DeleteResourcePolicy":
		*p = ""
	default:
		return map[string]string{"Policy": cmp.Or(*p, "{}")}, nil
	}
	return nil, nil
}

func (s *Service) describeLimits(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	open, onDemand := s.usage(c.Account, c.Region)
	return map[string]int{"ShardLimit": shardLimit, "OpenShardCount": open, "OnDemandStreamCount": onDemand, "OnDemandStreamCountLimit": 50}, nil
}

// ---- console ----

// Resources lists streams for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Streams", Columns: []string{"Name", "Region", "Account", "Shards", "Records", "Mode"}}
	for _, k := range core.SortedKeys(s.st.Streams) {
		st := s.st.Streams[k]
		n := 0
		for _, sh := range st.Shards {
			n += len(sh.Records)
		}
		t.Rows = append(t.Rows, []string{st.Name, st.Region, st.Account, strconv.Itoa(len(st.open())), strconv.Itoa(n), st.Mode})
	}
	return []core.Table{t}
}

// Inspect returns a stream's description, shards with record counts, tags and consumers.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Streams) {
		st := s.st.Streams[k]
		if st.Name != id {
			continue
		}
		d := st.describe()
		var shards []map[string]any
		for i, o := range outShards(st.Shards) {
			shards = append(shards, map[string]any{"Shard": o, "Records": len(st.Shards[i].Records)})
		}
		var consumers []map[string]any
		for _, con := range st.Consumers {
			consumers = append(consumers, con.out())
		}
		d["Shards"], d["Tags"], d["Consumers"] = shards, st.Tags, consumers
		return d
	}
	return nil
}
