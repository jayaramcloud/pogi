// Package firehose emulates Amazon Data Firehose over JSON 1.1. Direct PUT
// and Kinesis-sourced streams buffer records by size and interval, then
// deliver them to S3 (concatenated as-is, optionally gzipped) or to HTTP
// endpoints. Other destinations (Redshift, OpenSearch, Splunk, Snowflake,
// Iceberg…) are stored and described, but their records are dropped.
package firehose

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Data Firehose emulator.
type Service struct {
	stop func()
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex // guards st; never held while delivering
	work sync.Mutex // serializes polling and delivery, so Flush returns once data is out
	st   state
}

type state struct {
	Streams map[string]*Stream `json:"streams"` // account/region/name
}

// Stream is one Firehose stream and its unflushed buffer.
type Stream struct {
	Name       string            `json:"name"`
	Account    string            `json:"account"`
	Region     string            `json:"region"`
	Type       string            `json:"type"` // DirectPut | KinesisStreamAsSource
	Source     *source           `json:"source,omitempty"`
	Kind       string            `json:"kind"` // destination: S3, ExtendedS3, HttpEndpoint, Redshift…
	Dest       map[string]any    `json:"dest"` // its configuration as sent, plus defaults
	Version    int               `json:"version"`
	Created    time.Time         `json:"created"`
	Updated    time.Time         `json:"updated,omitzero"`
	Tags       map[string]string `json:"tags,omitempty"`
	KeyType    string            `json:"keyType,omitempty"` // set while server-side encryption is on
	KeyARN     string            `json:"keyArn,omitempty"`
	Buf        [][]byte          `json:"buf,omitempty"`
	BufSize    int               `json:"bufSize,omitempty"`
	BufAt      time.Time         `json:"bufAt,omitzero"`       // arrival of the oldest buffered record
	Checkpoint map[string]string `json:"checkpoint,omitempty"` // Kinesis source: shard → last sequence number read
}

type source struct {
	KinesisStreamARN string
	RoleARN          string `json:",omitempty"`
}

type tag struct {
	Key   string
	Value string
}

type encryption struct{ KeyARN, KeyType string }

// New creates the Firehose service and starts its delivery loop.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateDeliveryStream":          core.H(s.createDeliveryStream),
		"DescribeDeliveryStream":        core.H(s.describeDeliveryStream),
		"ListDeliveryStreams":           core.H(s.listDeliveryStreams),
		"DeleteDeliveryStream":          core.H(s.deleteDeliveryStream),
		"UpdateDestination":             core.H(s.updateDestination),
		"PutRecord":                     core.H(s.putRecord),
		"PutRecordBatch":                core.H(s.putRecordBatch),
		"TagDeliveryStream":             core.H(s.tags),
		"UntagDeliveryStream":           core.H(s.tags),
		"ListTagsForDeliveryStream":     core.H(s.tags),
		"StartDeliveryStreamEncryption": core.H(s.encryption),
		"StopDeliveryStreamEncryption":  core.H(s.encryption),
	}}
	hub.OnDeliver("firehose", s.deliver)
	s.stop = core.Every(time.Second, func(time.Time) { s.work.Lock(); s.poll(); s.flush(false); s.work.Unlock() })
	return s
}

// Close stops the delivery loop and delivers what is still buffered.
func (s *Service) Close() error {
	s.stop()
	s.Flush()
	return nil
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "firehose", Cloud: core.AWS, Title: "Data Firehose", Targets: []string{"Firehose_20150804"}, Signing: []string{"firehose"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

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

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func (ds *Stream) key() string { return key(ds.Account, ds.Region, ds.Name) }

func (ds *Stream) arn() string {
	return core.ARN("firehose", ds.Region, ds.Account, "deliverystream/"+ds.Name)
}

// stream finds the caller's stream; lock held.
func (s *Service) stream(c *core.Ctx, name string) (*Stream, error) {
	if ds := s.st.Streams[key(c.Account, c.Region, name)]; ds != nil {
		return ds, nil
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "Firehose stream %s under account %s not found.", name, c.Account)
}

func (ds *Stream) encrypt(e *encryption) {
	ds.KeyType, ds.KeyARN = "", ""
	if e != nil {
		ds.KeyType, ds.KeyARN = cmp.Or(e.KeyType, "AWS_OWNED_CMK"), e.KeyARN
	}
}

// add buffers records; lock held.
func (ds *Stream) add(recs [][]byte) {
	if len(recs) == 0 {
		return
	}
	if len(ds.Buf) == 0 {
		ds.BufAt = time.Now().UTC()
	}
	for _, r := range recs {
		ds.Buf = append(ds.Buf, r)
		ds.BufSize += len(r)
	}
}

// target is what delivery needs from a destination configuration.
type target struct {
	BucketARN             string
	Prefix                string
	CompressionFormat     string
	FileExtension         string
	BufferingHints        struct{ SizeInMBs, IntervalInSeconds int }
	EndpointConfiguration struct{ Url, AccessKey string }
}

func parse(cfg map[string]any) (t target) {
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &t)
	return t
}

func bucket(arn string) string { return arn[strings.LastIndex(arn, ":")+1:] }

// ---- destinations ----

// streamInput is the CreateDeliveryStream and UpdateDestination input. raw
// keeps every member so the one destination (…Configuration or …Update) can
// be stored generically.
type streamInput struct {
	DeliveryStreamName                         string
	DeliveryStreamType                         string
	KinesisStreamSourceConfiguration           *source
	DeliveryStreamEncryptionConfigurationInput *encryption
	Tags                                       []tag
	CurrentDeliveryStreamVersionId             string
	raw                                        map[string]any
}

func (in *streamInput) UnmarshalJSON(b []byte) error {
	type plain streamInput
	if err := json.Unmarshal(b, (*plain)(in)); err != nil {
		return err
	}
	return json.Unmarshal(b, &in.raw)
}

// destination finds the request's single "<Kind><suffix>" member.
func destination(raw map[string]any, suffix string) (string, map[string]any, error) {
	kind, cfg := "", map[string]any(nil)
	for k, v := range raw {
		if m, ok := v.(map[string]any); ok && strings.HasSuffix(k, suffix) {
			if cfg != nil {
				return "", nil, invalid("Only one destination can be configured.")
			}
			kind, cfg = strings.TrimSuffix(k, suffix), m
		}
	}
	if cfg == nil {
		return "", nil, invalid("Exactly one destination %s is required.", suffix)
	}
	return kind, cfg, nil
}

// prepare checks what delivery needs and fills AWS's defaults (5 MiB, 300 s).
// Only GZIP compression is applied; other formats are stored as given.
func prepare(kind string, cfg map[string]any) error {
	t := parse(cfg)
	switch kind {
	case "S3", "ExtendedS3":
		if !strings.HasPrefix(t.BucketARN, "arn:") {
			return invalid("BucketARN must be an S3 bucket ARN, got %q.", t.BucketARN)
		}
		if cfg["CompressionFormat"] == nil {
			cfg["CompressionFormat"] = "UNCOMPRESSED"
		}
	case "HttpEndpoint":
		if t.EndpointConfiguration.Url == "" {
			return invalid("EndpointConfiguration.Url is required.")
		}
	default:
		return nil
	}
	bh, _ := cfg["BufferingHints"].(map[string]any)
	if bh == nil {
		bh = map[string]any{}
		cfg["BufferingHints"] = bh
	}
	if bh["SizeInMBs"] == nil {
		bh["SizeInMBs"] = 5
	}
	if bh["IntervalInSeconds"] == nil {
		bh["IntervalInSeconds"] = 300
	}
	return nil
}

// merge copies src into dst recursively, renaming …Update members to the
// stored names (S3Update → S3Configuration).
func merge(dst, src map[string]any) {
	for k, v := range src {
		switch k {
		case "S3Update":
			k = "S3Configuration"
		case "S3BackupUpdate":
			k = "S3BackupConfiguration"
		}
		m, ok := v.(map[string]any)
		if !ok {
			dst[k] = v
			continue
		}
		d, ok := dst[k].(map[string]any)
		if !ok {
			d = map[string]any{}
			dst[k] = d
		}
		merge(d, m)
	}
}

// asDescription turns a stored configuration into its …Description shape,
// leaving out secrets as DescribeDeliveryStream does.
func asDescription(cfg map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range cfg {
		switch k {
		case "AccessKey", "Password", "PrivateKey", "KeyPassphrase":
			continue
		case "S3Configuration":
			k = "S3DestinationDescription"
		case "S3BackupConfiguration":
			k = "S3BackupDescription"
		}
		if m, ok := v.(map[string]any); ok {
			v = asDescription(m)
		}
		out[k] = v
	}
	return out
}

func (ds *Stream) describe() map[string]any {
	desc := asDescription(ds.Dest)
	dest := map[string]any{"DestinationId": "destinationId-000000000001", ds.Kind + "DestinationDescription": desc}
	if ds.Kind == "S3" || ds.Kind == "ExtendedS3" { // AWS reports S3 destinations in both shapes
		ext := maps.Clone(desc)
		if _, set := ext["S3BackupMode"]; !set && ds.Kind == "ExtendedS3" {
			ext["S3BackupMode"] = "Disabled"
		}
		dest["S3DestinationDescription"], dest["ExtendedS3DestinationDescription"] = desc, ext
	}
	enc := map[string]any{"Status": "DISABLED"}
	if ds.KeyType != "" {
		enc = map[string]any{"Status": "ENABLED", "KeyType": ds.KeyType}
		if ds.KeyARN != "" {
			enc["KeyARN"] = ds.KeyARN
		}
	}
	d := map[string]any{
		"DeliveryStreamName": ds.Name, "DeliveryStreamARN": ds.arn(), "DeliveryStreamStatus": "ACTIVE", "DeliveryStreamType": ds.Type,
		"VersionId": strconv.Itoa(ds.Version), "CreateTimestamp": core.Unix(ds.Created), "Destinations": []any{dest},
		"HasMoreDestinations": false, "DeliveryStreamEncryptionConfiguration": enc,
	}
	if !ds.Updated.IsZero() {
		d["LastUpdateTimestamp"] = core.Unix(ds.Updated)
	}
	if ds.Source != nil {
		d["Source"] = map[string]any{"KinesisStreamSourceDescription": map[string]any{
			"KinesisStreamARN": ds.Source.KinesisStreamARN, "RoleARN": ds.Source.RoleARN, "DeliveryStartTimestamp": core.Unix(ds.Created)}}
	}
	return d
}

// ---- operations ----

var streamName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

func (s *Service) createDeliveryStream(c *core.Ctx, in *streamInput) (any, error) {
	if !streamName.MatchString(in.DeliveryStreamName) {
		return nil, invalid("Firehose stream name %q must match [a-zA-Z0-9_.-]{1,64}.", in.DeliveryStreamName)
	}
	kind, cfg, err := destination(in.raw, "DestinationConfiguration")
	if err != nil {
		return nil, err
	}
	if err := prepare(kind, cfg); err != nil {
		return nil, err
	}
	ds := &Stream{Name: in.DeliveryStreamName, Account: c.Account, Region: c.Region, Type: cmp.Or(in.DeliveryStreamType, "DirectPut"),
		Kind: kind, Dest: cfg, Version: 1, Created: time.Now().UTC(), Tags: map[string]string{}}
	if ds.Type == "KinesisStreamAsSource" {
		if in.KinesisStreamSourceConfiguration == nil || in.KinesisStreamSourceConfiguration.KinesisStreamARN == "" {
			return nil, invalid("KinesisStreamSourceConfiguration.KinesisStreamARN is required for KinesisStreamAsSource.")
		}
		ds.Source = in.KinesisStreamSourceConfiguration
	}
	ds.encrypt(in.DeliveryStreamEncryptionConfigurationInput)
	for _, t := range in.Tags {
		ds.Tags[t.Key] = t.Value
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Streams[ds.key()] != nil {
		return nil, core.Errorf(400, "ResourceInUseException", "Firehose stream %s under account %s already exists.", ds.Name, ds.Account)
	}
	s.st.Streams[ds.key()] = ds
	return map[string]string{"DeliveryStreamARN": ds.arn()}, nil
}

func (s *Service) describeDeliveryStream(c *core.Ctx, in *struct{ DeliveryStreamName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, err := s.stream(c, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"DeliveryStreamDescription": ds.describe()}, nil
}

func (s *Service) listDeliveryStreams(c *core.Ctx, in *struct {
	DeliveryStreamType               string
	ExclusiveStartDeliveryStreamName string
	Limit                            int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for _, ds := range s.st.Streams {
		if ds.Account == c.Account && ds.Region == c.Region && ds.Name > in.ExclusiveStartDeliveryStreamName &&
			(in.DeliveryStreamType == "" || in.DeliveryStreamType == ds.Type) {
			names = append(names, ds.Name)
		}
	}
	sort.Strings(names)
	limit := cmp.Or(in.Limit, 10)
	if limit < 1 {
		return nil, invalid("Limit must be at least 1.")
	}
	more := len(names) > limit
	return map[string]any{"DeliveryStreamNames": names[:min(len(names), limit)], "HasMoreDeliveryStreams": more}, nil
}

func (s *Service) deleteDeliveryStream(c *core.Ctx, in *struct{ DeliveryStreamName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, err := s.stream(c, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}
	delete(s.st.Streams, ds.key()) // unflushed records go with it, as AWS gives no guarantee
	return nil, nil
}

// updateDestination merges into the current destination when the type is the
// same (S3 and ExtendedS3 count as one) and replaces it otherwise.
func (s *Service) updateDestination(c *core.Ctx, in *streamInput) (any, error) {
	kind, upd, err := destination(in.raw, "DestinationUpdate")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, err := s.stream(c, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}
	if in.CurrentDeliveryStreamVersionId != strconv.Itoa(ds.Version) {
		return nil, core.Errorf(400, "ConcurrentModificationException", "Version %q is not the current version %d of Firehose stream %s.", in.CurrentDeliveryStreamVersionId, ds.Version, ds.Name)
	}
	isS3 := func(k string) bool { return k == "S3" || k == "ExtendedS3" }
	dest := map[string]any{}
	if kind == ds.Kind || isS3(kind) && isS3(ds.Kind) {
		merge(dest, ds.Dest) // deep copy, so a failed update leaves the stream alone
		if kind != ds.Kind {
			kind = "ExtendedS3"
		}
	}
	merge(dest, upd)
	if err := prepare(kind, dest); err != nil {
		return nil, err
	}
	ds.Kind, ds.Dest = kind, dest
	ds.Version++
	ds.Updated = time.Now().UTC()
	return nil, nil
}

func (s *Service) putRecord(c *core.Ctx, in *struct {
	DeliveryStreamName string
	Record             struct{ Data []byte }
}) (any, error) {
	ids, enc, err := s.put(c, in.DeliveryStreamName, [][]byte{in.Record.Data})
	if err != nil {
		return nil, err
	}
	return map[string]any{"RecordId": ids[0], "Encrypted": enc}, nil
}

func (s *Service) putRecordBatch(c *core.Ctx, in *struct {
	DeliveryStreamName string
	Records            []struct{ Data []byte }
}) (any, error) {
	recs := make([][]byte, len(in.Records))
	for i, r := range in.Records {
		recs[i] = r.Data
	}
	ids, enc, err := s.put(c, in.DeliveryStreamName, recs)
	if err != nil {
		return nil, err
	}
	res := make([]map[string]string, len(ids))
	for i, id := range ids {
		res[i] = map[string]string{"RecordId": id}
	}
	return map[string]any{"FailedPutCount": 0, "Encrypted": enc, "RequestResponses": res}, nil
}

// put buffers records on a Direct PUT stream.
func (s *Service) put(c *core.Ctx, name string, recs [][]byte) (ids []string, encrypted bool, err error) {
	if len(recs) < 1 || len(recs) > 500 {
		return nil, false, invalid("A batch must have between 1 and 500 records, got %d.", len(recs))
	}
	for _, r := range recs {
		if len(r) > 1000<<10 {
			return nil, false, invalid("Record size %d exceeds the 1000 KiB limit.", len(r))
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, err := s.stream(c, name)
	if err != nil {
		return nil, false, err
	}
	if ds.Type == "KinesisStreamAsSource" {
		return nil, false, invalid("This operation is not permitted on KinesisStreamAsSource delivery stream type.")
	}
	ds.add(recs)
	for range recs {
		ids = append(ids, core.NewID())
	}
	return ids, ds.KeyType != "", nil
}

// deliver takes one record from another service (EventBridge, SNS, IoT…).
func (s *Service) deliver(arn string, m *core.Message) error {
	_, region, account, res, _ := core.ParseARN(arn)
	_, _, err := s.put(&core.Ctx{Account: account, Region: region}, strings.TrimPrefix(res, "deliverystream/"), [][]byte{[]byte(m.Body)})
	return err
}

func (s *Service) tags(c *core.Ctx, in *struct {
	DeliveryStreamName   string
	Tags                 []tag
	TagKeys              []string
	ExclusiveStartTagKey string
	Limit                int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, err := s.stream(c, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}
	switch c.Op {
	case "TagDeliveryStream":
		if ds.Tags == nil {
			ds.Tags = map[string]string{}
		}
		for _, t := range in.Tags {
			ds.Tags[t.Key] = t.Value
		}
		return nil, nil
	case "UntagDeliveryStream":
		for _, k := range in.TagKeys {
			delete(ds.Tags, k)
		}
		return nil, nil
	}
	list := []tag{}
	for _, k := range core.SortedKeys(ds.Tags) {
		if k > in.ExclusiveStartTagKey {
			list = append(list, tag{k, ds.Tags[k]})
		}
	}
	limit := cmp.Or(in.Limit, 50)
	if limit < 1 {
		return nil, invalid("Limit must be at least 1.")
	}
	return map[string]any{"Tags": list[:min(len(list), limit)], "HasMoreTags": len(list) > limit}, nil
}

// encryption records server-side encryption; records are stored as sent.
func (s *Service) encryption(c *core.Ctx, in *struct {
	DeliveryStreamName                         string
	DeliveryStreamEncryptionConfigurationInput *encryption
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, err := s.stream(c, in.DeliveryStreamName)
	if err != nil {
		return nil, err
	}
	if c.Op == "StopDeliveryStreamEncryption" {
		ds.encrypt(nil)
	} else {
		ds.encrypt(cmp.Or(in.DeliveryStreamEncryptionConfigurationInput, &encryption{}))
	}
	return nil, nil
}

// ---- delivery ----

// Flush reads what Kinesis sources have pending and delivers every buffer
// now, whatever its buffering hints say. It returns once delivery is done.
func (s *Service) Flush() {
	s.work.Lock()
	defer s.work.Unlock()
	s.poll()
	s.flush(true)
}

type batch struct {
	ds      *Stream
	name    string
	arn     string
	kind    string
	version int
	t       target
	recs    [][]byte
	size    int
	at      time.Time
}

// flush delivers the buffers that are due (all of them when force) without
// holding s.mu; a failed batch goes back to the front of its buffer.
func (s *Service) flush(force bool) {
	var due []*batch
	s.mu.Lock()
	for _, ds := range s.st.Streams {
		if len(ds.Buf) == 0 {
			continue
		}
		t := parse(ds.Dest)
		if !force && ds.BufSize < t.BufferingHints.SizeInMBs<<20 && time.Since(ds.BufAt) < time.Duration(t.BufferingHints.IntervalInSeconds)*time.Second {
			continue
		}
		due = append(due, &batch{ds, ds.Name, ds.arn(), ds.Kind, ds.Version, t, ds.Buf, ds.BufSize, ds.BufAt})
		ds.Buf, ds.BufSize, ds.BufAt = nil, 0, time.Time{}
	}
	s.mu.Unlock()
	for _, b := range due {
		err := s.send(b)
		if err == nil {
			continue
		}
		s.hub.Log.Warn("firehose delivery failed, will retry", "stream", b.name, "records", len(b.recs), "err", err)
		s.mu.Lock()
		if s.st.Streams[b.ds.key()] == b.ds {
			b.ds.Buf = append(b.recs, b.ds.Buf...)
			b.ds.BufSize += b.size
			b.ds.BufAt = time.Now().UTC() // retry after another buffering interval
		}
		s.mu.Unlock()
	}
}

func (s *Service) send(b *batch) error {
	switch b.kind {
	case "S3", "ExtendedS3":
		return s.toS3(b)
	case "HttpEndpoint":
		return toHTTP(b)
	}
	s.hub.Log.Debug("firehose destination not emulated, records dropped", "stream", b.name, "destination", b.kind, "records", len(b.recs))
	return nil
}

// javaTime turns the Java date pattern of !{timestamp:…} into a Go layout (the letters Firehose docs use).
var javaTime = strings.NewReplacer("yyyy", "2006", "yy", "06", "MM", "01", "dd", "02", "HH", "15", "mm", "04", "ss", "05")

var prefixExpr = regexp.MustCompile(`!\{(timestamp:[^}]*|firehose:random-string)\}`)

// expandPrefix fills in !{timestamp:yyyy/MM/dd} and !{firehose:random-string}. A prefix that uses an
// expression gets no automatic YYYY/MM/DD/HH/ after it, as in AWS.
// ponytail: partitionKeyFromQuery and other expressions are left as written.
func expandPrefix(prefix string, at time.Time) (string, bool) {
	if !prefixExpr.MatchString(prefix) {
		return prefix, false
	}
	return prefixExpr.ReplaceAllStringFunc(prefix, func(e string) string {
		if layout, ok := strings.CutPrefix(e[2:len(e)-1], "timestamp:"); ok {
			return at.Format(javaTime.Replace(layout))
		}
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		r := make([]byte, 11)
		for i := range r {
			r[i] = alphabet[rand.IntN(len(alphabet))]
		}
		return string(r)
	}), true
}

// toS3 writes one object: Prefix + YYYY/MM/DD/HH/ + name-version-timestamp-uuid.
func (s *Service) toS3(b *batch) error {
	s3svc, _ := s.hub.Service("s3").(*s3.Service)
	if s3svc == nil {
		return errors.New("s3 is not enabled")
	}
	data, ext := bytes.Join(b.recs, nil), b.t.FileExtension
	if b.t.CompressionFormat == "GZIP" {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		zw.Write(data)
		zw.Close()
		data, ext = buf.Bytes(), cmp.Or(ext, ".gz")
	}
	at := b.at.UTC()
	prefix, custom := expandPrefix(b.t.Prefix, at)
	dated := at.Format("2006/01/02/15/")
	if custom {
		dated = ""
	}
	key := fmt.Sprintf("%s%s%s-%d-%s-%s%s", prefix, dated, b.name, b.version, at.Format("2006-01-02-15-04-05"), core.UUID(), ext)
	return s3svc.Put(bucket(b.t.BucketARN), key, "application/octet-stream", data)
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

// toHTTP posts one request in the Firehose HTTP endpoint delivery format.
func toHTTP(b *batch) error {
	recs := make([]map[string][]byte, len(b.recs))
	for i, r := range b.recs {
		recs[i] = map[string][]byte{"data": r}
	}
	id := core.UUID()
	body, _ := json.Marshal(map[string]any{"requestId": id, "timestamp": time.Now().UnixMilli(), "records": recs})
	req, err := http.NewRequest(http.MethodPost, b.t.EndpointConfiguration.Url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Firehose-Protocol-Version", "1.0")
	req.Header.Set("X-Amz-Firehose-Request-Id", id)
	req.Header.Set("X-Amz-Firehose-Source-Arn", b.arn)
	if k := b.t.EndpointConfiguration.AccessKey; k != "" {
		req.Header.Set("X-Amz-Firehose-Access-Key", k)
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("%s answered %s", b.t.EndpointConfiguration.Url, res.Status)
	}
	return nil
}

// poll moves new records of Kinesis-sourced streams into their buffers,
// reading each shard after its checkpoint (or from the stream's creation).
func (s *Service) poll() {
	type src struct {
		ds      *Stream
		arn     string
		start   time.Time
		checkpt map[string]string
	}
	var srcs []src
	s.mu.Lock()
	for _, ds := range s.st.Streams {
		if ds.Source != nil {
			srcs = append(srcs, src{ds, ds.Source.KinesisStreamARN, ds.Created, maps.Clone(ds.Checkpoint)})
		}
	}
	s.mu.Unlock()
	if len(srcs) == 0 || s.hub.Service("kinesis") == nil {
		return
	}
	for _, x := range srcs {
		recs, checkpt, err := s.read(x.arn, x.start, x.checkpt)
		if err != nil {
			s.hub.Log.Debug("firehose cannot read its Kinesis source", "source", x.arn, "err", err)
		}
		s.mu.Lock()
		if s.st.Streams[x.ds.key()] == x.ds {
			x.ds.add(recs)
			x.ds.Checkpoint = checkpt
		}
		s.mu.Unlock()
	}
}

// read fetches records after each shard's checkpoint through the hub, and
// returns them with the advanced checkpoints.
func (s *Service) read(streamARN string, start time.Time, checkpt map[string]string) ([][]byte, map[string]string, error) {
	_, region, account, _, _ := core.ParseARN(streamARN)
	call := func(op string, in, out any) error {
		return s.hub.Call(account, region, "Kinesis_20131202", op, in, out)
	}
	if checkpt == nil {
		checkpt = map[string]string{}
	}
	var shards struct{ Shards []struct{ ShardId string } }
	if err := call("ListShards", map[string]any{"StreamARN": streamARN}, &shards); err != nil {
		return nil, checkpt, err
	}
	var data [][]byte
	for _, sh := range shards.Shards {
		in := map[string]any{"StreamARN": streamARN, "ShardId": sh.ShardId, "ShardIteratorType": "AT_TIMESTAMP", "Timestamp": core.Unix(start)}
		if seq := checkpt[sh.ShardId]; seq != "" {
			in = map[string]any{"StreamARN": streamARN, "ShardId": sh.ShardId, "ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "StartingSequenceNumber": seq}
		}
		var it struct{ ShardIterator string }
		if err := call("GetShardIterator", in, &it); err != nil {
			return data, checkpt, err
		}
		for iter := it.ShardIterator; iter != ""; {
			var out struct {
				Records []struct {
					Data           []byte
					SequenceNumber string
				}
				NextShardIterator string
			}
			if err := call("GetRecords", map[string]any{"ShardIterator": iter}, &out); err != nil {
				return data, checkpt, err
			}
			if len(out.Records) == 0 {
				break
			}
			for _, r := range out.Records {
				data = append(data, r.Data)
				checkpt[sh.ShardId] = r.SequenceNumber
			}
			iter = out.NextShardIterator
		}
	}
	return data, checkpt, nil
}

// ---- console ----

// Resources lists Firehose streams for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "DeliveryStreams", Columns: []string{"Name", "Region", "Account", "Destination", "Buffered"}}
	for _, k := range core.SortedKeys(s.st.Streams) {
		ds := s.st.Streams[k]
		where, tg := ds.Kind, parse(ds.Dest)
		switch {
		case ds.Kind == "S3" || ds.Kind == "ExtendedS3":
			where = "s3://" + bucket(tg.BucketARN) + "/" + tg.Prefix
		case tg.EndpointConfiguration.Url != "":
			where = tg.EndpointConfiguration.Url
		}
		t.Rows = append(t.Rows, []string{ds.Name, ds.Region, ds.Account, where, strconv.Itoa(len(ds.Buf))})
	}
	return []core.Table{t}
}

// Inspect returns a stream's description, tags and buffer size.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Streams) {
		if ds := s.st.Streams[k]; ds.Name == id {
			return map[string]any{"DeliveryStreamDescription": ds.describe(), "Tags": ds.Tags, "BufferedRecords": len(ds.Buf), "BufferedBytes": ds.BufSize}
		}
	}
	return nil
}
