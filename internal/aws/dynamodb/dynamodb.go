// Package dynamodb emulates Amazon DynamoDB and DynamoDB Streams over JSON 1.0:
// tables with global and local secondary indexes, every expression type,
// conditional writes, batches, transactions, TTL expiry and change streams.
package dynamodb

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"hash/crc32"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the DynamoDB and DynamoDB Streams emulator.
type Service struct {
	hub  *core.Hub
	ops  map[string]core.Handler
	mu   sync.Mutex
	st   state
	j    *core.Journal // wal mode
	stop func()
}

type state struct {
	Tables  map[string]*Table  `json:"tables"`            // account/region/name
	Streams map[string]*Stream `json:"streams"`           // stream ARN
	Tokens  map[string]token   `json:"tokens,omitempty"`  // account/ClientRequestToken of TransactWriteItems
	Backups map[string]*Backup `json:"backups,omitempty"` // backup ARN
}

// Table is one table and its items.
type Table struct {
	Name       string            `json:"name"`
	Account    string            `json:"account"`
	Region     string            `json:"region"`
	ID         string            `json:"id"`
	Created    time.Time         `json:"created"`
	Attrs      []attrDef         `json:"attrs"`
	Keys       []keyElem         `json:"keys"`
	GSIs       []*index          `json:"gsis,omitempty"`
	LSIs       []*index          `json:"lsis,omitempty"`
	Billing    string            `json:"billing"`
	Throughput *throughput       `json:"throughput,omitempty"`
	StreamArn  string            `json:"streamArn,omitempty"` // latest stream, possibly disabled
	SSE        *sseSpec          `json:"sse,omitempty"`
	Class      string            `json:"class"`
	Protected  bool              `json:"protected,omitempty"`
	TTL        string            `json:"ttl,omitempty"` // TTL attribute while enabled
	PITR       bool              `json:"pitr,omitempty"`
	Replicas   []string          `json:"replicas,omitempty"` // other regions of a global table
	Restore    *restoreSummary   `json:"restore,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	Items      map[string]Item   `json:"items"` // keyOf(primary key) → item
}

type attrDef struct {
	AttributeName string
	AttributeType string
}

type keyElem struct {
	AttributeName string
	KeyType       string
}

type projection struct {
	ProjectionType   string
	NonKeyAttributes []string `json:",omitempty"`
}

type throughput struct {
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

type index struct {
	IndexName             string
	KeySchema             []keyElem
	Projection            projection
	ProvisionedThroughput *throughput `json:",omitempty"`
}

type sseSpec struct {
	Enabled        bool
	SSEType        string `json:",omitempty"`
	KMSMasterKeyId string `json:",omitempty"`
}

type streamSpec struct {
	StreamEnabled  bool
	StreamViewType string `json:",omitempty"`
}

type tag struct {
	Key   string
	Value string
}

type token struct {
	Hash string    `json:"hash"`
	At   time.Time `json:"at"`
}

// New creates the service and starts its TTL sweeper.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.ops = map[string]core.Handler{
		"CreateTable":                         core.H(s.createTable),
		"DescribeTable":                       core.H(s.describeTable),
		"ListTables":                          core.H(s.listTables),
		"DeleteTable":                         core.H(s.deleteTable),
		"UpdateTable":                         core.H(s.updateTable),
		"TagResource":                         core.H(s.tagResource),
		"UntagResource":                       core.H(s.untagResource),
		"ListTagsOfResource":                  core.H(s.listTags),
		"UpdateTimeToLive":                    core.H(s.updateTTL),
		"DescribeTimeToLive":                  core.H(s.describeTTL),
		"DescribeContinuousBackups":           core.H(s.describeBackups),
		"UpdateContinuousBackups":             core.H(s.updateBackups),
		"CreateBackup":                        core.H(s.createBackup),
		"ListBackups":                         core.H(s.listBackups),
		"DescribeBackup":                      core.H(s.describeBackup),
		"DeleteBackup":                        core.H(s.deleteBackup),
		"RestoreTableFromBackup":              core.H(s.restoreFromBackup),
		"RestoreTableToPointInTime":           core.H(s.restoreToPointInTime),
		"DescribeEndpoints":                   core.H(s.describeEndpoints),
		"DescribeLimits":                      core.H(s.describeLimits),
		"DescribeKinesisStreamingDestination": core.H(s.describeKinesis),
		"PutItem":                             core.H(s.putItem),
		"GetItem":                             core.H(s.getItem),
		"UpdateItem":                          core.H(s.updateItem),
		"DeleteItem":                          core.H(s.deleteItem),
		"Query":                               core.H(s.query),
		"Scan":                                core.H(s.scan),
		"BatchGetItem":                        core.H(s.batchGet),
		"BatchWriteItem":                      core.H(s.batchWrite),
		"TransactWriteItems":                  core.H(s.transactWrite),
		"TransactGetItems":                    core.H(s.transactGet),
		// DynamoDBStreams_20120810
		"ListStreams":      core.H(s.listStreams),
		"DescribeStream":   core.H(s.describeStream),
		"GetShardIterator": core.H(s.getShardIterator),
		"GetRecords":       core.H(s.getRecords),
	}
	s.stop = core.Every(5*time.Second, func(now time.Time) { s.ExpireTTL(now) })
	return s
}

// Close stops the TTL sweeper.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Info() core.Info {
	return core.Info{Name: "dynamodb", Cloud: core.AWS, Title: "DynamoDB", Protocol: "json",
		Targets: []string{"DynamoDB_20120810", "DynamoDBStreams_20120810"}, Signing: []string{"dynamodb"}}
}

// apiError is an error whose body carries members besides the message
// (CancellationReasons, Item), which core.Error cannot express.
type apiError struct {
	err   *core.Error
	extra map[string]any
}

func (e *apiError) Error() string { return e.err.Error() }
func (e *apiError) Unwrap() error { return e.err }

// ServeHTTP is core.RPC's JSON path plus apiError members and the
// X-Amz-Crc32 header, which DynamoDB SDKs verify.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	_, c.Op, _ = strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	out, err := s.call(c, core.Body(r))
	status := http.StatusOK
	if err != nil {
		e := core.AsError(err)
		body := map[string]any{"__type": e.Code, "message": e.Msg}
		var ae *apiError
		if errors.As(err, &ae) {
			maps.Copy(body, ae.extra)
		}
		c.ErrCode = e.Code
		w.Header().Set("X-Amzn-ErrorType", e.Code)
		out, status = body, e.Status
	}
	b := []byte("{}")
	if out != nil {
		b, _ = json.Marshal(out)
	}
	h := w.Header()
	h.Set("Content-Type", "application/x-amz-json-1.0")
	h.Set("x-amzn-RequestId", c.RequestID)
	h.Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(b)), 10))
	w.WriteHeader(status)
	w.Write(b)
}

func (s *Service) call(c *core.Ctx, body []byte) (any, error) {
	if err := c.BodyErr(); err != nil {
		return nil, err
	}
	h := s.ops[c.Op]
	if h == nil {
		return nil, core.Errorf(400, "UnknownOperationException", "pogi does not implement %s.%s yet", c.Service, c.Op)
	}
	if err := c.Intercept(); err != nil {
		return nil, err
	}
	return h(c, func(v any) error {
		if len(body) == 0 {
			return nil
		}
		return json.Unmarshal(body, v)
	})
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Tables: map[string]*Table{}, Streams: map[string]*Stream{}, Tokens: map[string]token{}, Backups: map[string]*Backup{}}
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
	s.Reset()
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.Copy(s.st.Tables, st.Tables)
	maps.Copy(s.st.Streams, st.Streams)
	maps.Copy(s.st.Tokens, st.Tokens)
	maps.Copy(s.st.Backups, st.Backups)
	for _, t := range s.st.Tables {
		if t.Items == nil {
			t.Items = map[string]Item{}
		}
	}
	return nil
}

// SetJournal logs every change of tables, items and streams (wal mode).
func (s *Service) SetJournal(j *core.Journal) {
	s.mu.Lock()
	s.j = j
	s.mu.Unlock()
}

// Apply replays a journal record: t/ tables (without items), i/ items,
// s/ streams, r/ stream records, x/ transaction tokens and b/ backups.
func (s *Service) Apply(key string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kind, k, _ := strings.Cut(key, "/")
	switch kind {
	case "t":
		items := map[string]Item{}
		if old := s.st.Tables[k]; old != nil {
			items = old.Items // items have records of their own
		}
		if err := core.Replay(s.st.Tables, k, v); err != nil || v == nil {
			return err
		}
		s.st.Tables[k].Items = items
	case "i":
		p := strings.SplitN(k, "/", 4) // account, region, table, item key
		if len(p) < 4 {
			return errors.New("bad item key")
		}
		if t := s.st.Tables[tkey(p[0], p[1], p[2])]; t != nil { // nil: the table went later
			return core.Replay(t.Items, p[3], v)
		}
	case "s":
		return core.Replay(s.st.Streams, k, v)
	case "r":
		var r Record
		st := s.st.Streams[k]
		if err := json.Unmarshal(v, &r); err != nil || st == nil {
			return err
		}
		if n, _ := parseSeq(r.Dynamodb.SequenceNumber); n > st.Seq {
			st.Seq = n
			st.add(&r)
		}
	case "x":
		return core.Replay(s.st.Tokens, k, v)
	case "b":
		return core.Replay(s.st.Backups, k, v)
	}
	return nil
}

// journalTable logs a table without its items; lock held.
func (s *Service) journalTable(t *Table) {
	def := *t
	def.Items = nil
	s.j.Put("t/"+tkey(t.Account, t.Region, t.Name), &def)
}

// ikey is the journal key of the item with primary key k.
func ikey(t *Table, k string) string { return "i/" + tkey(t.Account, t.Region, t.Name) + "/" + k }

// ---- tables ----

func tkey(account, region, name string) string { return account + "/" + region + "/" + name }

func errNoTable(name string) error {
	return core.Errorf(400, "ResourceNotFoundException", "Requested resource not found: Table: %s not found", name)
}

// table finds a table by name or ARN for the caller; lock held.
func (s *Service) table(c *core.Ctx, name string) (*Table, error) {
	account, region := c.Account, c.Region
	if _, r, a, res, ok := core.ParseARN(name); ok {
		account, region, name = a, r, strings.TrimPrefix(res, "table/")
	}
	if t := s.st.Tables[tkey(account, region, name)]; t != nil {
		return t, nil
	}
	return nil, errNoTable(name)
}

func (t *Table) arn() string { return core.ARN("dynamodb", t.Region, t.Account, "table/"+t.Name) }

// keyNames lists a key schema's attributes, partition keys first.
func keyNames(ks []keyElem) []string {
	var hash, rng []string
	for _, k := range ks {
		if k.KeyType == "HASH" {
			hash = append(hash, k.AttributeName)
		} else {
			rng = append(rng, k.AttributeName)
		}
	}
	return append(hash, rng...)
}

func (t *Table) attrType(name string) string {
	for _, a := range t.Attrs {
		if a.AttributeName == name {
			return a.AttributeType
		}
	}
	return ""
}

// keyOf encodes the named attributes of an item as a map key.
func keyOf(it Item, names ...string) string {
	var b strings.Builder
	for _, n := range names {
		v := it[n]
		s := scalar(v)
		if v.B != nil {
			s = base64.StdEncoding.EncodeToString(v.B) // keeps state JSON valid UTF-8
		}
		b.WriteString(v.kind() + ":" + s + "\x00")
	}
	return b.String()
}

func (t *Table) key(it Item) string { return keyOf(it, keyNames(t.Keys)...) }

// index returns the named GSI or LSI.
func (t *Table) index(name string) (ix *index, global bool) {
	for _, x := range t.GSIs {
		if x.IndexName == name {
			return x, true
		}
	}
	for _, x := range t.LSIs {
		if x.IndexName == name {
			return x, false
		}
	}
	return nil, false
}

// inIndex reports whether an item has the index's key attributes.
func inIndex(ix *index, it Item) bool {
	for _, k := range ix.KeySchema {
		if it[k.AttributeName] == nil {
			return false
		}
	}
	return true
}

// indexView is an item as the index stores it.
func (t *Table) indexView(ix *index, it Item) Item {
	if ix.Projection.ProjectionType == "ALL" {
		return it
	}
	return pick(it, slices.Concat(keyNames(t.Keys), keyNames(ix.KeySchema), ix.Projection.NonKeyAttributes)...)
}

var errBadKey = errVal("The provided key element does not match the schema")

// keyValue refuses an empty or oversized key value: a partition key may hold
// 2048 bytes, a sort key 1024.
func keyValue(v *Value, name string, hash bool) error {
	if n := len(scalar(v)); n > 0 {
		switch {
		case v.N != nil:
		case hash && n > 2048:
			return errVal("One or more parameter values were invalid: Size of hashkey has exceeded the maximum size limit of2048 bytes")
		case !hash && n > 1024:
			return errVal("One or more parameter values were invalid: Aggregated size of all range keys has exceeded the size limit of 1024 bytes")
		}
		return nil
	}
	kind := map[string]string{"S": "string", "B": "binary"}[v.kind()]
	return errVal("One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty %s value. Key: %s", kind, name)
}

// checkKey validates a primary key given in a request.
func (t *Table) checkKey(key Item) error {
	if err := checkItem(key); err != nil {
		return err
	}
	if len(key) != len(t.Keys) {
		return errBadKey
	}
	for i, k := range t.Keys {
		v := key[k.AttributeName]
		if v == nil || v.kind() != t.attrType(k.AttributeName) {
			return errBadKey
		}
		if err := keyValue(v, k.AttributeName, i == 0); err != nil {
			return err
		}
	}
	return nil
}

// fits validates a whole item against the table: key attributes, index key
// types and the 400 KB limit. Values must already be checked.
func (t *Table) fits(it Item) error {
	for i, k := range t.Keys {
		v, want := it[k.AttributeName], t.attrType(k.AttributeName)
		switch {
		case v == nil:
			return errVal("One or more parameter values were invalid: Missing the key %s in the item", k.AttributeName)
		case v.kind() != want:
			return errVal("One or more parameter values were invalid: Type mismatch for key %s expected: %s actual: %s", k.AttributeName, want, v.kind())
		}
		if err := keyValue(v, k.AttributeName, i == 0); err != nil {
			return err
		}
	}
	for _, ix := range slices.Concat(t.GSIs, t.LSIs) {
		for _, k := range ix.KeySchema {
			v, want := it[k.AttributeName], t.attrType(k.AttributeName)
			switch {
			case v == nil:
			case v.kind() != want:
				return errVal("One or more parameter values were invalid: Type mismatch for Index Key %s Expected: %s Actual: %s IndexName: %s", k.AttributeName, want, v.kind(), ix.IndexName)
			case scalar(v) == "":
				return errVal("One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: %s, IndexKey: %s", ix.IndexName, k.AttributeName)
			}
		}
	}
	if itemSize(it) > 400<<10 {
		return errVal("Item size has exceeded the maximum allowed size")
	}
	return nil
}

var tableName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,255}$`)

// validate checks a new or updated table definition.
func (t *Table) validate() error {
	bad := func(format string, a ...any) error {
		return errVal("One or more parameter values were invalid: "+format, a...)
	}
	if !tableName.MatchString(t.Name) {
		return errVal("1 validation error detected: Value '%s' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+ and have length between 3 and 255", t.Name)
	}
	used := map[string]bool{}
	// multi lets a global secondary index have up to four HASH and four RANGE attributes.
	checkKeys := func(ks []keyElem, what string, multi bool) error {
		most, nh := 1, hashCount(ks)
		if multi {
			most = 4
		}
		if nh == 0 || nh > most || len(ks)-nh > most || slices.ContainsFunc(ks[:nh], func(k keyElem) bool { return k.KeyType != "HASH" }) ||
			slices.ContainsFunc(ks[nh:], func(k keyElem) bool { return k.KeyType != "RANGE" }) {
			return bad("Invalid KeySchema for %s: it needs HASH key attributes, optionally followed by RANGE key attributes (at most %d of each)", what, most)
		}
		if len(uniq(keyNames(ks))) != len(ks) {
			return bad("Invalid KeySchema for %s: an attribute is listed twice", what)
		}
		for _, k := range ks {
			if typ := t.attrType(k.AttributeName); typ != "S" && typ != "N" && typ != "B" {
				var defs []string
				for _, a := range t.Attrs {
					defs = append(defs, a.AttributeName)
				}
				return bad("Some index key attributes are not defined in AttributeDefinitions. Keys: [%s], AttributeDefinitions: [%s]",
					strings.Join(keyNames(ks), ", "), strings.Join(defs, ", "))
			}
			used[k.AttributeName] = true
		}
		return nil
	}
	if err := checkKeys(t.Keys, "table "+t.Name, false); err != nil {
		return err
	}
	names := map[string]bool{}
	for _, ix := range slices.Concat(t.GSIs, t.LSIs) {
		if names[ix.IndexName] {
			return bad("Duplicate index name: %s", ix.IndexName)
		}
		names[ix.IndexName] = true
		if err := checkKeys(ix.KeySchema, "index "+ix.IndexName, slices.Contains(t.GSIs, ix)); err != nil {
			return err
		}
		switch p := ix.Projection; {
		case p.ProjectionType != "ALL" && p.ProjectionType != "KEYS_ONLY" && p.ProjectionType != "INCLUDE":
			return bad("Unknown ProjectionType: %s", p.ProjectionType)
		case p.ProjectionType != "INCLUDE" && len(p.NonKeyAttributes) > 0:
			return bad("ProjectionType is %s, but NonKeyAttributes is specified", p.ProjectionType)
		}
	}
	for _, ix := range t.LSIs {
		if len(t.Keys) < 2 {
			return bad("Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex")
		}
		if len(ix.KeySchema) != 2 || ix.KeySchema[0].AttributeName != keyNames(t.Keys)[0] {
			return bad("Index KeySchema of %s must share the table's hash key and have a range key", ix.IndexName)
		}
	}
	if len(used) != len(t.Attrs) {
		return bad("Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions")
	}
	switch t.Billing {
	case "PROVISIONED":
		if t.Throughput == nil {
			return bad("ReadCapacityUnits and WriteCapacityUnits must both be specified when BillingMode is PROVISIONED")
		}
		for _, ix := range t.GSIs {
			if ix.ProvisionedThroughput == nil {
				return bad("ProvisionedThroughput is not specified for index: %s", ix.IndexName)
			}
		}
	case "PAY_PER_REQUEST":
		if t.Throughput != nil {
			return bad("Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST")
		}
	default:
		return errVal("1 validation error detected: Value '%s' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]", t.Billing)
	}
	return nil
}

var views = []string{"NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES", "KEYS_ONLY"}

func checkView(sp *streamSpec) error {
	if sp.StreamEnabled && !slices.Contains(views, sp.StreamViewType) {
		return errVal("One or more parameter values were invalid: StreamViewType must be one of NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES, KEYS_ONLY when StreamEnabled is true")
	}
	return nil
}

type createTableInput struct {
	TableName                 string
	AttributeDefinitions      []attrDef
	KeySchema                 []keyElem
	GlobalSecondaryIndexes    []*index
	LocalSecondaryIndexes     []*index
	BillingMode               string
	ProvisionedThroughput     *throughput
	StreamSpecification       *streamSpec
	SSESpecification          *sseSpec
	Tags                      []tag
	TableClass                string
	DeletionProtectionEnabled bool
}

func (s *Service) createTable(c *core.Ctx, in *createTableInput) (any, error) {
	t := &Table{Name: in.TableName, Account: c.Account, Region: c.Region, ID: core.UUID(), Created: time.Now().UTC(),
		Attrs: in.AttributeDefinitions, Keys: in.KeySchema, GSIs: in.GlobalSecondaryIndexes, LSIs: in.LocalSecondaryIndexes,
		Billing: cmp.Or(in.BillingMode, "PROVISIONED"), Throughput: in.ProvisionedThroughput, SSE: in.SSESpecification,
		Class: cmp.Or(in.TableClass, "STANDARD"), Protected: in.DeletionProtectionEnabled, Items: map[string]Item{}}
	if len(in.Tags) > 0 {
		t.Tags = map[string]string{}
		for _, tg := range in.Tags {
			t.Tags[tg.Key] = tg.Value
		}
	}
	if err := t.validate(); err != nil {
		return nil, err
	}
	sp := cmp.Or(in.StreamSpecification, &streamSpec{})
	if err := checkView(sp); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tkey(c.Account, c.Region, in.TableName)
	if s.st.Tables[k] != nil {
		return nil, core.Errorf(400, "ResourceInUseException", "Table already exists: %s", in.TableName)
	}
	if sp.StreamEnabled {
		s.enableStream(t, sp.StreamViewType)
	}
	s.st.Tables[k] = t
	s.journalTable(t)
	return map[string]any{"TableDescription": s.describe(t)}, nil
}

func throughputDesc(tp *throughput) map[string]any {
	tp = cmp.Or(tp, &throughput{})
	return map[string]any{"NumberOfDecreasesToday": 0, "ReadCapacityUnits": tp.ReadCapacityUnits, "WriteCapacityUnits": tp.WriteCapacityUnits}
}

// describe renders a TableDescription; lock held. It copies what it shares.
// warm is the default warm throughput AWS reports for on-demand tables and indexes.
var warm = map[string]any{"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000, "Status": "ACTIVE"}

func (s *Service) describe(t *Table) map[string]any {
	size := 0
	for _, it := range t.Items {
		size += itemSize(it)
	}
	d := map[string]any{
		"TableName": t.Name, "TableArn": t.arn(), "TableId": t.ID, "TableStatus": "ACTIVE",
		"AttributeDefinitions": slices.Clone(t.Attrs), "KeySchema": slices.Clone(t.Keys),
		"CreationDateTime": core.Unix(t.Created), "ItemCount": len(t.Items), "TableSizeBytes": size,
		"ProvisionedThroughput": throughputDesc(t.Throughput), "DeletionProtectionEnabled": t.Protected,
		"TableClassSummary": map[string]string{"TableClass": t.Class},
		"WarmThroughput":    warm, // Terraform's AWS provider waits for its status
	}
	if t.Restore != nil {
		rs := map[string]any{"SourceTableArn": t.Restore.SourceArn, "RestoreDateTime": core.Unix(t.Restore.At), "RestoreInProgress": false}
		if t.Restore.BackupArn != "" {
			rs["SourceBackupArn"] = t.Restore.BackupArn
		}
		d["RestoreSummary"] = rs
	}
	if len(t.Replicas) > 0 {
		d["Replicas"], d["GlobalTableVersion"] = replicaDescriptions(t), "2019.11.21"
	}
	if t.Billing == "PAY_PER_REQUEST" {
		d["BillingModeSummary"] = map[string]any{"BillingMode": t.Billing, "LastUpdateToPayPerRequestDateTime": core.Unix(t.Created)}
	}
	var gsis, lsis []map[string]any
	for _, ix := range slices.Concat(t.GSIs, t.LSIs) {
		n, sz := 0, 0
		for _, it := range t.Items {
			if inIndex(ix, it) {
				n, sz = n+1, sz+itemSize(t.indexView(ix, it))
			}
		}
		x := map[string]any{"IndexName": ix.IndexName, "KeySchema": slices.Clone(ix.KeySchema), "Projection": ix.Projection,
			"IndexSizeBytes": sz, "ItemCount": n, "IndexArn": t.arn() + "/index/" + ix.IndexName}
		if _, global := t.index(ix.IndexName); global {
			x["IndexStatus"] = "ACTIVE"
			x["ProvisionedThroughput"] = throughputDesc(ix.ProvisionedThroughput)
			x["WarmThroughput"] = warm
			gsis = append(gsis, x)
		} else {
			lsis = append(lsis, x)
		}
	}
	if gsis != nil {
		d["GlobalSecondaryIndexes"] = gsis
	}
	if lsis != nil {
		d["LocalSecondaryIndexes"] = lsis
	}
	if st := s.st.Streams[t.StreamArn]; st != nil {
		spec := map[string]any{"StreamEnabled": st.Enabled}
		if st.Enabled {
			spec["StreamViewType"] = st.View
		}
		d["StreamSpecification"], d["LatestStreamArn"], d["LatestStreamLabel"] = spec, st.Arn, st.Label
	}
	if t.SSE != nil && t.SSE.Enabled {
		key := cmp.Or(t.SSE.KMSMasterKeyId, "alias/aws/dynamodb")
		if !strings.HasPrefix(key, "arn:") {
			key = core.ARN("kms", t.Region, t.Account, key)
		}
		d["SSEDescription"] = map[string]any{"Status": "ENABLED", "SSEType": cmp.Or(t.SSE.SSEType, "KMS"), "KMSMasterKeyArn": key}
	}
	return d
}

type tableInput struct{ TableName string }

func (s *Service) describeTable(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Table": s.describe(t)}, nil
}

func (s *Service) listTables(c *core.Ctx, in *struct {
	ExclusiveStartTableName string
	Limit                   int
}) (any, error) {
	limit := in.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for _, t := range s.st.Tables {
		if t.Account == c.Account && t.Region == c.Region && t.Name > in.ExclusiveStartTableName {
			names = append(names, t.Name)
		}
	}
	slices.Sort(names)
	out := map[string]any{"TableNames": names}
	if len(names) > limit {
		out["TableNames"], out["LastEvaluatedTableName"] = names[:limit], names[limit-1]
	}
	return out, nil
}

func (s *Service) deleteTable(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	if t.Protected {
		return nil, errVal("Resource cannot be deleted as it is currently protected against manual deletion")
	}
	d := s.describe(t)
	d["TableStatus"] = "DELETING"
	if st := s.st.Streams[t.StreamArn]; st != nil {
		st.Enabled = false
		s.j.Put("s/"+st.Arn, st)
	}
	for _, reg := range t.Replicas { // the other regions keep their copies
		if o := s.replicaTable(t, reg); o != nil {
			o.Replicas = slices.DeleteFunc(slices.Clone(o.Replicas), func(x string) bool { return x == t.Region })
			s.journalTable(o)
		}
	}
	delete(s.st.Tables, tkey(t.Account, t.Region, t.Name))
	s.j.Delete("t/" + tkey(t.Account, t.Region, t.Name))
	return map[string]any{"TableDescription": d}, nil
}

func (s *Service) updateTable(c *core.Ctx, in *struct {
	TableName                   string
	AttributeDefinitions        []attrDef
	BillingMode                 string
	ProvisionedThroughput       *throughput
	GlobalSecondaryIndexUpdates []struct {
		Create *index
		Update *index
		Delete *index
	}
	StreamSpecification       *streamSpec
	SSESpecification          *sseSpec
	TableClass                string
	DeletionProtectionEnabled *bool
	ReplicaUpdates            []replicaUpdate
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	if len(in.ReplicaUpdates) > 0 {
		view := ""
		if st := s.st.Streams[t.StreamArn]; st != nil && st.Enabled {
			view = st.View
		}
		if sp := in.StreamSpecification; sp != nil && sp.StreamEnabled {
			view = sp.StreamViewType
		}
		if err := s.checkReplicas(t, in.ReplicaUpdates, view); err != nil {
			return nil, err
		}
	}
	n := *t // validated before it replaces t
	n.Attrs = slices.Clone(t.Attrs)
	for _, a := range in.AttributeDefinitions {
		if n.attrType(a.AttributeName) == "" {
			n.Attrs = append(n.Attrs, a)
		}
	}
	n.Billing = cmp.Or(in.BillingMode, n.Billing)
	n.Throughput = cmp.Or(in.ProvisionedThroughput, n.Throughput)
	n.GSIs = slices.Clone(t.GSIs)
	if in.BillingMode == "PAY_PER_REQUEST" && in.ProvisionedThroughput == nil {
		n.Throughput = nil
		for i, ix := range n.GSIs {
			cp := *ix
			cp.ProvisionedThroughput, n.GSIs[i] = nil, &cp
		}
	}
	changes := 0
	for i, u := range in.GlobalSecondaryIndexUpdates {
		for kind, ix := range map[string]*index{"create": u.Create, "update": u.Update, "delete": u.Delete} {
			if ix != nil && len(ix.IndexName) < 3 {
				return nil, errVal("1 validation error detected: Value '%s' at 'globalSecondaryIndexUpdates.%d.member.%s.indexName' failed to satisfy constraint: Member must have length greater than or equal to 3", ix.IndexName, i+1, kind)
			}
		}
		if u.Create != nil || u.Delete != nil {
			changes++
		}
	}
	if changes > 1 { // one index may be created or deleted per UpdateTable
		return nil, core.Errorf(400, "LimitExceededException", "Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table")
	}
	for _, u := range in.GlobalSecondaryIndexUpdates {
		i := -1
		for j, ix := range n.GSIs {
			if u.Create != nil && ix.IndexName == u.Create.IndexName || u.Update != nil && ix.IndexName == u.Update.IndexName || u.Delete != nil && ix.IndexName == u.Delete.IndexName {
				i = j
			}
		}
		switch {
		case u.Create != nil && i >= 0:
			return nil, errVal("One or more parameter values were invalid: Index with name %s already exists", u.Create.IndexName)
		case u.Create != nil:
			n.GSIs = append(n.GSIs, u.Create)
		case i < 0:
			return nil, core.Errorf(400, "ResourceNotFoundException", "Requested resource not found: Requested index does not exist")
		case u.Delete != nil:
			n.GSIs = slices.Delete(n.GSIs, i, i+1)
		default:
			cp := *n.GSIs[i]
			cp.ProvisionedThroughput, n.GSIs[i] = u.Update.ProvisionedThroughput, &cp
		}
	}
	keys := slices.Concat(n.Keys)
	for _, ix := range slices.Concat(n.GSIs, n.LSIs) {
		keys = append(keys, ix.KeySchema...)
	}
	n.Attrs = slices.DeleteFunc(n.Attrs, func(a attrDef) bool { // drop definitions only deleted indexes used
		return !slices.ContainsFunc(keys, func(k keyElem) bool { return k.AttributeName == a.AttributeName })
	})
	n.Class = cmp.Or(in.TableClass, n.Class)
	n.SSE = cmp.Or(in.SSESpecification, n.SSE)
	if in.DeletionProtectionEnabled != nil {
		n.Protected = *in.DeletionProtectionEnabled
	}
	if err := n.validate(); err != nil {
		return nil, err
	}
	if sp := in.StreamSpecification; sp != nil {
		cur := s.st.Streams[t.StreamArn]
		on := cur != nil && cur.Enabled
		switch err := checkView(sp); {
		case err != nil:
			return nil, err
		case sp.StreamEnabled && on:
			return nil, errVal("Table already has an enabled stream: %s", cur.Arn)
		case !sp.StreamEnabled && !on:
			return nil, errVal("One or more parameter values were invalid: Table does not have an enabled stream to disable")
		case sp.StreamEnabled:
			s.enableStream(&n, sp.StreamViewType)
		default:
			cur.Enabled = false
			s.j.Put("s/"+cur.Arn, cur)
		}
	}
	*t = n
	s.journalTable(t)
	s.applyReplicas(t, in.ReplicaUpdates)
	return map[string]any{"TableDescription": s.describe(t)}, nil
}

func (s *Service) updateTTL(c *core.Ctx, in *struct {
	TableName               string
	TimeToLiveSpecification struct {
		AttributeName string
		Enabled       bool
	}
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	switch spec := in.TimeToLiveSpecification; {
	case spec.Enabled && t.TTL != "":
		return nil, errVal("TimeToLive is already enabled")
	case !spec.Enabled && t.TTL == "":
		return nil, errVal("TimeToLive is already disabled")
	case spec.Enabled && spec.AttributeName == "":
		return nil, errVal("TimeToLiveSpecification.AttributeName must be specified")
	case spec.Enabled:
		t.TTL = spec.AttributeName
	default:
		t.TTL = ""
	}
	s.journalTable(t)
	return map[string]any{"TimeToLiveSpecification": in.TimeToLiveSpecification}, nil
}

func (s *Service) describeTTL(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	d := map[string]string{"TimeToLiveStatus": "DISABLED"}
	if t.TTL != "" {
		d = map[string]string{"TimeToLiveStatus": "ENABLED", "AttributeName": t.TTL}
	}
	return map[string]any{"TimeToLiveDescription": d}, nil
}

var ttlIdentity = &identity{PrincipalId: "dynamodb.amazonaws.com", Type: "Service"}

// ExpireTTL deletes the items whose TTL attribute (epoch seconds) is at or
// before now, as the background sweeper does every few seconds. Like
// DynamoDB, it ignores TTLs more than five years in the past.
func (s *Service) ExpireTTL(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	floor := float64(now.AddDate(-5, 0, 0).Unix())
	for _, t := range s.st.Tables {
		if t.TTL == "" {
			continue
		}
		for k, it := range t.Items {
			v := it[t.TTL]
			if v == nil || v.N == nil {
				continue
			}
			if exp, err := strconv.ParseFloat(*v.N, 64); err == nil && exp <= float64(now.Unix()) && exp >= floor {
				delete(t.Items, k)
				s.j.Delete(ikey(t, k))
				s.record(t, it, nil, ttlIdentity)
			}
		}
	}
}

func backups(t *Table) any {
	pitr := map[string]any{"PointInTimeRecoveryStatus": "DISABLED"}
	if t.PITR {
		pitr = map[string]any{"PointInTimeRecoveryStatus": "ENABLED", "RecoveryPeriodInDays": 35,
			"EarliestRestorableDateTime": core.Unix(t.Created), "LatestRestorableDateTime": core.Unix(time.Now())}
	}
	return map[string]any{"ContinuousBackupsDescription": map[string]any{"ContinuousBackupsStatus": "ENABLED", "PointInTimeRecoveryDescription": pitr}}
}

func (s *Service) describeBackups(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, core.Errorf(400, "TableNotFoundException", "Table not found: %s", in.TableName)
	}
	return backups(t), nil
}

func (s *Service) updateBackups(c *core.Ctx, in *struct {
	TableName                        string
	PointInTimeRecoverySpecification struct{ PointInTimeRecoveryEnabled bool }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, core.Errorf(400, "TableNotFoundException", "Table not found: %s", in.TableName)
	}
	t.PITR = in.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled
	s.journalTable(t)
	return backups(t), nil
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        []tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := maps.Clone(t.Tags) // ListTagsOfResource output may still be encoding the old map
	if tags == nil {
		tags = map[string]string{}
	}
	for _, tg := range in.Tags {
		tags[tg.Key] = tg.Value
	}
	t.Tags = tags
	s.journalTable(t)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := maps.Clone(t.Tags)
	for _, k := range in.TagKeys {
		delete(tags, k)
	}
	t.Tags = tags
	s.journalTable(t)
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := []tag{}
	for _, k := range core.SortedKeys(t.Tags) {
		tags = append(tags, tag{k, t.Tags[k]})
	}
	return map[string]any{"Tags": tags}, nil
}

func (s *Service) describeEndpoints(c *core.Ctx, in *struct{}) (any, error) {
	return map[string]any{"Endpoints": []map[string]any{{"Address": cmp.Or(c.Host, "localhost:7644"), "CachePeriodInMinutes": 1440}}}, nil
}

func (s *Service) describeLimits(c *core.Ctx, in *struct{}) (any, error) {
	return map[string]any{"AccountMaxReadCapacityUnits": 80000, "AccountMaxWriteCapacityUnits": 80000,
		"TableMaxReadCapacityUnits": 40000, "TableMaxWriteCapacityUnits": 40000}, nil
}

func (s *Service) describeKinesis(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"TableName": t.Name, "KinesisDataStreamDestinations": []any{}}, nil
}

// ---- console ----

// TaggedResources lists tables (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, t := range s.st.Tables {
		if t.Account == account && t.Region == region {
			out[t.arn()] = maps.Clone(t.Tags)
		}
	}
	return out
}

// Resources lists tables for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := core.Table{Name: "Tables", Columns: []string{"Name", "Region", "Account", "Items", "Size", "Keys", "Stream"}}
	for _, k := range core.SortedKeys(s.st.Tables) {
		t := s.st.Tables[k]
		size := 0
		for _, it := range t.Items {
			size += itemSize(it)
		}
		var keys []string
		for _, n := range keyNames(t.Keys) {
			keys = append(keys, n+" ("+t.attrType(n)+")")
		}
		stream := ""
		if st := s.st.Streams[t.StreamArn]; st != nil && st.Enabled {
			stream = st.View
		}
		out.Rows = append(out.Rows, []string{t.Name, t.Region, t.Account, strconv.Itoa(len(t.Items)), strconv.Itoa(size), strings.Join(keys, ", "), stream})
	}
	return []core.Table{out}
}

// Inspect returns a table's description and up to 20 of its items.
// ponytail: tables are found by name alone; the first account/region wins.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Tables) {
		if t := s.st.Tables[k]; table == "Tables" && t.Name == id {
			items := t.sorted()
			return map[string]any{"Table": s.describe(t), "Items": items[:min(len(items), 20)]}
		}
	}
	return nil
}

// sorted returns the items in primary key order; lock held.
func (t *Table) sorted() []Item {
	items := slices.AppendSeq([]Item{}, maps.Values(t.Items))
	slices.SortFunc(items, byKeys(keyNames(t.Keys), false))
	return items
}

// byKeys orders items by the given attributes, which all of them have.
func byKeys(names []string, desc bool) func(a, b Item) int {
	return func(a, b Item) int {
		for _, n := range names {
			if c, _ := compare(a[n], b[n]); c != 0 {
				if desc {
					return -c
				}
				return c
			}
		}
		return 0
	}
}

// Items returns up to limit items (all when limit <= 0) in DynamoDB JSON,
// e.g. {"id":{"S":"1"}}, for the console.
func (s *Service) Items(account, region, table string, limit int) ([]map[string]any, error) {
	s.mu.Lock()
	t := s.st.Tables[tkey(account, region, table)]
	if t == nil {
		s.mu.Unlock()
		return nil, errNoTable(table)
	}
	items := t.sorted()
	s.mu.Unlock()
	if limit > 0 {
		items = items[:min(len(items), limit)]
	}
	var out []map[string]any
	return out, remarshal(items, &out)
}

// PutItemJSON stores an item given in DynamoDB JSON, for the console.
func (s *Service) PutItemJSON(account, region, table string, item map[string]any) error {
	in := &writeInput{TableName: table}
	if err := remarshal(item, &in.Item); err != nil {
		return err
	}
	_, err := s.putItem(&core.Ctx{Account: account, Region: region}, in)
	return err
}

// DeleteItemJSON deletes the item with a key given in DynamoDB JSON, for the console.
func (s *Service) DeleteItemJSON(account, region, table string, key map[string]any) error {
	in := &writeInput{TableName: table}
	if err := remarshal(key, &in.Key); err != nil {
		return err
	}
	_, err := s.deleteItem(&core.Ctx{Account: account, Region: region}, in)
	return err
}

func remarshal(from, to any) error {
	b, err := json.Marshal(from)
	if err == nil {
		err = json.Unmarshal(b, to)
	}
	return err
}
