package dynamodb

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Stream is a table's change stream: one shard with the latest records.
type Stream struct {
	Arn     string    `json:"arn"`
	Label   string    `json:"label"`
	Table   string    `json:"table"`
	Account string    `json:"account"`
	Region  string    `json:"region"`
	View    string    `json:"view"`
	Enabled bool      `json:"enabled"`
	Created time.Time `json:"created"`
	Keys    []keyElem `json:"keys"`
	Shard   string    `json:"shard"`
	Seq     int64     `json:"seq"` // last sequence number handed out
	Records []*Record `json:"records"`
}

// Record is a stream record in the DynamoDB Streams (and Lambda event) shape.
type Record struct {
	EventID      string       `json:"eventID"`
	EventName    string       `json:"eventName"`
	EventVersion string       `json:"eventVersion"`
	EventSource  string       `json:"eventSource"`
	AwsRegion    string       `json:"awsRegion"`
	Dynamodb     streamRecord `json:"dynamodb"`
	UserIdentity *identity    `json:"userIdentity,omitempty"`
}

type streamRecord struct {
	ApproximateCreationDateTime float64
	Keys                        Item
	NewImage                    Item `json:",omitempty"`
	OldImage                    Item `json:",omitempty"`
	SequenceNumber              string
	SizeBytes                   int
	StreamViewType              string
}

type identity struct {
	PrincipalId string
	Type        string
}

const maxRecords = 10000

// Sequence numbers are "1" plus 20 digits: increasing as numbers and as strings.
func seqStr(n int64) string { return fmt.Sprintf("1%020d", n) }

func parseSeq(s string) (int64, bool) {
	if len(s) != 21 || s[0] != '1' {
		return 0, false
	}
	n, err := strconv.ParseInt(s[1:], 10, 64)
	return n, err == nil
}

// first is the sequence number of the oldest record kept, or of the next one.
func (st *Stream) first() int64 {
	if len(st.Records) == 0 {
		return st.Seq + 1
	}
	n, _ := parseSeq(st.Records[0].Dynamodb.SequenceNumber)
	return n
}

// enableStream starts a new stream for t; lock held.
func (s *Service) enableStream(t *Table, view string) {
	now := time.Now().UTC()
	for s.st.Streams[t.arn()+"/stream/"+now.Format("2006-01-02T15:04:05.000")] != nil {
		now = now.Add(time.Millisecond)
	}
	label := now.Format("2006-01-02T15:04:05.000")
	st := &Stream{Arn: t.arn() + "/stream/" + label, Label: label, Table: t.Name, Account: t.Account, Region: t.Region,
		View: view, Enabled: true, Created: now, Keys: slices.Clone(t.Keys), Shard: fmt.Sprintf("shardId-%020d-%s", now.UnixMilli(), core.RandHex(4))}
	s.st.Streams[st.Arn] = st
	s.j.Put("s/"+st.Arn, st)
	t.StreamArn = st.Arn
}

// record appends the stream record of one item change; lock held. Writes
// that change nothing leave no record, as in DynamoDB.
func (s *Service) record(t *Table, old, new Item, who *identity) {
	st := s.st.Streams[t.StreamArn]
	if st == nil || !st.Enabled || equalItems(old, new) {
		return
	}
	r := &Record{EventID: core.NewID(), EventName: "MODIFY", EventVersion: "1.1", EventSource: "aws:dynamodb", AwsRegion: t.Region, UserIdentity: who}
	src := new
	switch {
	case old == nil:
		r.EventName = "INSERT"
	case new == nil:
		r.EventName, src = "REMOVE", old
	}
	d := &r.Dynamodb
	d.Keys = pick(src, keyNames(t.Keys)...)
	if st.View == "NEW_IMAGE" || st.View == "NEW_AND_OLD_IMAGES" {
		d.NewImage = new
	}
	if st.View == "OLD_IMAGE" || st.View == "NEW_AND_OLD_IMAGES" {
		d.OldImage = old
	}
	st.Seq++
	d.ApproximateCreationDateTime = float64(time.Now().Unix())
	d.SequenceNumber = seqStr(st.Seq)
	d.SizeBytes = itemSize(d.Keys) + itemSize(d.NewImage) + itemSize(d.OldImage)
	d.StreamViewType = st.View
	st.add(r)
	s.j.Put("r/"+st.Arn, r)
}

// add appends a record and trims the oldest.
func (st *Stream) add(r *Record) {
	st.Records = append(st.Records, r)
	if len(st.Records) > maxRecords+maxRecords/10 { // trim in batches; readers may hold the old slice
		st.Records = slices.Clone(st.Records[len(st.Records)-maxRecords:])
	}
}

func (s *Service) stream(arn string) (*Stream, error) {
	if st := s.st.Streams[arn]; st != nil {
		return st, nil
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "Requested resource not found: Stream: %s not found", arn)
}

func (s *Service) listStreams(c *core.Ctx, in *struct {
	TableName               string
	Limit                   int
	ExclusiveStartStreamArn string
}) (any, error) {
	limit := in.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]string{}
	for _, arn := range core.SortedKeys(s.st.Streams) {
		st := s.st.Streams[arn]
		if st.Account == c.Account && st.Region == c.Region && (in.TableName == "" || st.Table == in.TableName) && arn > in.ExclusiveStartStreamArn {
			list = append(list, map[string]string{"StreamArn": arn, "TableName": st.Table, "StreamLabel": st.Label})
		}
	}
	out := map[string]any{"Streams": list}
	if len(list) > limit {
		out["Streams"], out["LastEvaluatedStreamArn"] = list[:limit], list[limit-1]["StreamArn"]
	}
	return out, nil
}

func (s *Service) describeStream(c *core.Ctx, in *struct {
	StreamArn             string
	Limit                 int
	ExclusiveStartShardId string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(in.StreamArn)
	if err != nil {
		return nil, err
	}
	status, rng := "ENABLED", map[string]string{"StartingSequenceNumber": seqStr(st.first())}
	if !st.Enabled {
		status, rng["EndingSequenceNumber"] = "DISABLED", seqStr(st.Seq)
	}
	shards := []map[string]any{}
	if in.ExclusiveStartShardId != st.Shard {
		shards = append(shards, map[string]any{"ShardId": st.Shard, "SequenceNumberRange": rng})
	}
	return map[string]any{"StreamDescription": map[string]any{
		"StreamArn": st.Arn, "StreamLabel": st.Label, "StreamStatus": status, "StreamViewType": st.View,
		"CreationRequestDateTime": core.Unix(st.Created), "TableName": st.Table, "KeySchema": st.Keys, "Shards": shards,
	}}, nil
}

// iterator is opaque to clients: stream ARN, next sequence number, creation time.
func iterator(st *Stream, pos int64) string {
	return base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%s|%d|%d", st.Arn, pos, time.Now().Unix()))
}

func (s *Service) getShardIterator(c *core.Ctx, in *struct {
	StreamArn         string
	ShardId           string
	ShardIteratorType string
	SequenceNumber    string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(in.StreamArn)
	if err != nil {
		return nil, err
	}
	if in.ShardId != st.Shard {
		return nil, core.Errorf(400, "ResourceNotFoundException", "Requested resource not found: Shard does not exist")
	}
	var pos int64
	switch in.ShardIteratorType {
	case "TRIM_HORIZON":
		pos = st.first()
	case "LATEST":
		pos = st.Seq + 1
	case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
		n, ok := parseSeq(in.SequenceNumber)
		if !ok {
			return nil, errVal("Invalid SequenceNumber: %s", in.SequenceNumber)
		}
		if pos = n; in.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" {
			pos++
		}
		if pos < st.first() {
			return nil, core.Errorf(400, "TrimmedDataAccessException", "The sequence number %s is beyond the trim horizon of the shard", in.SequenceNumber)
		}
	default:
		return nil, errVal("1 validation error detected: Value '%s' at 'shardIteratorType' failed to satisfy constraint: Member must satisfy enum value set: [AFTER_SEQUENCE_NUMBER, LATEST, AT_SEQUENCE_NUMBER, TRIM_HORIZON]", in.ShardIteratorType)
	}
	return map[string]string{"ShardIterator": iterator(st, pos)}, nil
}

func (s *Service) getRecords(c *core.Ctx, in *struct {
	ShardIterator string
	Limit         int
}) (any, error) {
	raw, _ := base64.RawURLEncoding.DecodeString(in.ShardIterator)
	parts := strings.Split(string(raw), "|")
	if len(parts) != 3 {
		return nil, errVal("Invalid ShardIterator")
	}
	pos, err1 := strconv.ParseInt(parts[1], 10, 64)
	at, err2 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil {
		return nil, errVal("Invalid ShardIterator")
	}
	if created := time.Unix(at, 0); time.Since(created) > 15*time.Minute {
		return nil, core.Errorf(400, "ExpiredIteratorException", "Iterator expired. The iterator was created at time %s while right now it is %s.",
			created.UTC().Format(time.RFC1123), time.Now().UTC().Format(time.RFC1123))
	}
	limit := in.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.stream(parts[0])
	if err != nil {
		return nil, err
	}
	first := st.first()
	if pos < first {
		return nil, core.Errorf(400, "TrimmedDataAccessException", "Attempted to access data that has been trimmed")
	}
	recs, size := []*Record{}, 0
	for i := int(min(pos-first, int64(len(st.Records)))); i < len(st.Records) && len(recs) < limit && size < 1<<20; i++ {
		recs = append(recs, st.Records[i])
		size += st.Records[i].Dynamodb.SizeBytes
	}
	out := map[string]any{"Records": recs}
	if next := pos + int64(len(recs)); st.Enabled || next <= st.Seq { // a closed shard ends once read
		out["NextShardIterator"] = iterator(st, next)
	}
	return out, nil
}
