// Package table emulates Azure Table Storage on Azurite's port (10002):
// tables, entities with typed (EDM) properties, insert, update, merge,
// upsert and delete with ETags, OData $filter/$select/$top queries with
// continuation tokens, and entity group transactions ($batch). Connect with
// a connection string whose TableEndpoint is
// http://127.0.0.1:10002/devstoreaccount1. Shared Key and SAS are accepted
// without signature checks.
package table

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Table Storage emulator.
type Service struct {
	hub  *core.Hub
	mu   sync.Mutex
	st   state
	last time.Time // newest Timestamp handed out; keeps ETags unique
}

type state struct {
	Accounts map[string]*Account `json:"accounts"`
	ARM      armres.Store        `json:"arm,omitempty"` // service settings the resource manager holds
}

// Account is a storage account (devstoreaccount1 by default).
type Account struct {
	Tables map[string]*Table `json:"tables"`          // by lower-case name
	Props  string            `json:"props,omitempty"` // service properties XML
}

// Table holds entities keyed by PartitionKey+"\x00"+RowKey (keys can't contain
// control characters), which also sorts them the way the service does.
// ponytail: queries sort the keys each time; keep a sorted index if tables get big.
type Table struct {
	Name     string             `json:"name"`
	ACL      string             `json:"acl,omitempty"` // SignedIdentifiers XML
	Entities map[string]*Entity `json:"entities"`
}

// Entity is a stored entity. Writes replace the pointer, so batches can roll back.
type Entity struct {
	PartitionKey string          `json:"pk"`
	RowKey       string          `json:"rk"`
	Timestamp    time.Time       `json:"ts"`
	Props        map[string]Prop `json:"props,omitempty"`
}

// Prop is a property: its EDM type and its JSON value as the service writes it.
type Prop struct {
	Type  string          `json:"t"`
	Value json.RawMessage `json:"v"`
}

// Port is Azurite's table port.
const Port = 10002

// dateFmt is how Edm.DateTime values are written (100ns ticks, UTC).
const dateFmt = "2006-01-02T15:04:05.9999999Z"

// New creates the Table Storage service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "aztable", Cloud: core.Azure, Title: "Table Storage", Protocol: "azure-json", Match: armres.HostMatch("table")}
}

func (s *Service) Port() int { return Port }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Accounts: map[string]*Account{}}
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
	if st.Accounts == nil {
		st.Accounts = map[string]*Account{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

var (
	errInput     = func() error { return core.Errorf(400, "InvalidInput", "One of the request inputs is not valid.") }
	errNoTable   = func() error { return core.Errorf(404, "TableNotFound", "The table specified does not exist.") }
	errNoEntity  = func() error { return core.Errorf(404, "ResourceNotFound", "The specified resource does not exist.") }
	errCondition = func() error {
		return core.Errorf(412, "UpdateConditionNotSatisfied", "The update condition specified in the request was not satisfied.")
	}
)

// writeError writes the OData JSON error with x-ms-error-code; prefix is a batch's "index:".
func writeError(w http.ResponseWriter, c *core.Ctx, err error, prefix string) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("x-ms-error-code", e.Code)
	w.Header().Set("Content-Type", "application/json;odata=minimalmetadata;streaming=true;charset=utf-8")
	w.WriteHeader(e.Status)
	msg := prefix + e.Msg + "\nRequestId:" + c.RequestID + "\nTime:" + time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")
	json.NewEncoder(w).Encode(map[string]any{"odata.error": map[string]any{"code": e.Code, "message": map[string]string{"lang": "en-US", "value": msg}}})
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = armres.AccountPath(r, "table")
	c := core.C(r)
	account, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	h := w.Header()
	h.Set("x-ms-request-id", c.RequestID)
	h.Set("x-ms-version", or(r.Header.Get("x-ms-version"), "2019-02-02"))
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if id := r.Header.Get("x-ms-client-request-id"); id != "" {
		h.Set("x-ms-client-request-id", id)
	}
	op, fn := s.route(w, r, c, account, rest)
	c.Op = op
	if fn == nil {
		writeError(w, c, core.Errorf(400, "UnsupportedHttpVerb", "pogi does not implement %s %s", r.Method, r.URL.RequestURI()), "")
		return
	}
	if err := c.Intercept(); err != nil {
		writeError(w, c, err, "")
		return
	}
	if err := fn(); err != nil {
		writeError(w, c, err, "")
	}
}

func (s *Service) route(w http.ResponseWriter, r *http.Request, c *core.Ctx, account, rest string) (string, func() error) {
	q := r.URL.Query()
	m := method(r)
	name, args, _ := strings.Cut(rest, "(")
	switch {
	case rest == "":
		if q.Get("restype") != "service" {
			break
		}
		switch comp := q.Get("comp"); {
		case comp == "properties" && m == http.MethodGet:
			return "GetServiceProperties", func() error { return s.serviceProps(w, account, nil) }
		case comp == "properties" && m == http.MethodPut:
			return "SetServiceProperties", func() error { return s.serviceProps(w, account, core.Body(r)) }
		case comp == "stats":
			return "GetServiceStats", func() error {
				w.Header().Set("Content-Type", "application/xml")
				io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><StorageServiceStats><GeoReplication><Status>live</Status><LastSyncTime>`+
					time.Now().UTC().Format(http.TimeFormat)+`</LastSyncTime></GeoReplication></StorageServiceStats>`)
				return nil
			}
		}
	case rest == "$batch" && m == http.MethodPost:
		return "Batch", func() error { return s.batch(w, r, c, account) }
	case strings.EqualFold(name, "Tables"):
		switch m {
		case http.MethodPost:
			return "CreateTable", func() error { return s.createTable(w, r, account) }
		case http.MethodGet:
			return "QueryTables", func() error { return s.queryTables(w, r, account) }
		case http.MethodDelete:
			return "DeleteTable", func() error { return s.deleteTable(w, account, rest) }
		}
	case q.Get("comp") == "acl" && name == rest && (m == http.MethodGet || m == http.MethodPut):
		return map[string]string{http.MethodGet: "GetTableACL", http.MethodPut: "SetTableACL"}[m], func() error {
			return s.acl(w, r, account, rest, m == http.MethodPut)
		}
	default:
		op := map[string]string{http.MethodGet: "QueryEntities", http.MethodPost: "InsertEntity"}[m]
		if strings.Trim(args, ") ") != "" {
			op = map[string]string{http.MethodGet: "GetEntity", http.MethodPut: "UpdateEntity", "MERGE": "MergeEntity",
				http.MethodPatch: "MergeEntity", http.MethodDelete: "DeleteEntity"}[m]
		}
		if op == "" {
			break
		}
		return op, func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.entity(w, r, account, rest)
		}
	}
	return "", nil
}

// method honours X-HTTP-Method tunnelling (POST with X-HTTP-Method: MERGE).
func method(r *http.Request) string {
	if m := r.Header.Get("X-HTTP-Method"); m != "" && r.Method == http.MethodPost {
		return m
	}
	return r.Method
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// target splits "name", "name()" or "name(args)", where args are 'quoted'
// (” escapes) and named (PartitionKey='p',RowKey='r') or positional ('t').
func target(rest string) (string, map[string]string, error) {
	name, args, _ := strings.Cut(rest, "(")
	args = strings.TrimSpace(strings.TrimSuffix(args, ")"))
	out := map[string]string{}
	for args != "" {
		k, v := "", args
		if args[0] != '\'' {
			k, v, _ = strings.Cut(args, "=")
			v = strings.TrimSpace(v)
		}
		if !strings.HasPrefix(v, "'") {
			return "", nil, errInput()
		}
		end := 1
		for end < len(v) && (v[end] != '\'' || end+1 < len(v) && v[end+1] == '\'') {
			if v[end] == '\'' {
				end++
			}
			end++
		}
		if end >= len(v) {
			return "", nil, errInput()
		}
		out[strings.TrimSpace(k)] = strings.ReplaceAll(v[1:end], "''", "'")
		args = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v[end+1:]), ","))
	}
	return name, out, nil
}

func (s *Service) account(name string) *Account {
	a := s.st.Accounts[name]
	if a == nil {
		a = &Account{Tables: map[string]*Table{}}
		s.st.Accounts[name] = a
	}
	return a
}

func (s *Service) table(account, name string) *Table {
	return s.account(account).Tables[strings.ToLower(name)]
}

// stamp returns a new entity Timestamp, strictly increasing so ETags differ; s.mu is held.
func (s *Service) stamp() time.Time {
	t := time.Now().UTC().Truncate(100 * time.Nanosecond)
	if !t.After(s.last) {
		t = s.last.Add(100 * time.Nanosecond)
	}
	s.last = t
	return t
}

func (e *Entity) etag() string {
	return `W/"datetime'` + strings.ReplaceAll(e.Timestamp.Format(dateFmt), ":", "%3A") + `'"`
}

// ---- rendering ----

// view is how a response renders: metadata level (the odata= value of
// Accept or $format), the account's base URL and $select.
type view struct {
	md, base, account string
	sel               []string
}

func newView(r *http.Request, account string) view {
	v := view{md: "minimalmetadata", base: "http://" + r.Host + "/" + account, account: account}
	switch f := or(r.URL.Query().Get("$format"), r.Header.Get("Accept")); {
	case strings.Contains(f, "nometadata"):
		v.md = "nometadata"
	case strings.Contains(f, "fullmetadata"):
		v.md = "fullmetadata"
	}
	if sel := r.URL.Query().Get("$select"); sel != "" && sel != "*" {
		for _, n := range strings.Split(sel, ",") {
			v.sel = append(v.sel, strings.TrimSpace(n))
		}
	}
	return v
}

// obj writes a JSON object with keys in insertion order (OData puts metadata first).
type obj struct{ bytes.Buffer }

func (o *obj) add(k string, v any) {
	if o.Len() == 0 {
		o.WriteByte('{')
	} else {
		o.WriteByte(',')
	}
	kb, _ := json.Marshal(k)
	o.Write(kb)
	o.WriteByte(':')
	if raw, ok := v.(json.RawMessage); ok {
		o.Write(raw)
		return
	}
	vb, _ := json.Marshal(v)
	o.Write(vb)
}

func (o *obj) close() []byte {
	if o.Len() == 0 {
		o.WriteByte('{')
	}
	o.WriteByte('}')
	return o.Bytes()
}

// annotated reports whether a value's type can't be inferred from its JSON,
// so responses with metadata carry its @odata.type.
func annotated(p Prop) bool {
	switch p.Type {
	case "Edm.Int64", "Edm.DateTime", "Edm.Guid", "Edm.Binary":
		return true
	}
	return p.Type == "Edm.Double" && p.Value[0] == '"' // NaN, Infinity
}

// entity renders e; meta is the odata.metadata fragment ("" inside a list).
func (v view) entity(e *Entity, table, meta string) []byte {
	var o obj
	if v.md != "nometadata" && meta != "" {
		o.add("odata.metadata", v.base+"/$metadata#"+meta)
	}
	key := func(k string) string { return url.PathEscape(strings.ReplaceAll(k, "'", "''")) }
	edit := table + "(PartitionKey='" + key(e.PartitionKey) + "',RowKey='" + key(e.RowKey) + "')"
	if v.md == "fullmetadata" {
		o.add("odata.type", v.account+"."+table)
		o.add("odata.id", v.base+"/"+edit)
	}
	if v.md != "nometadata" {
		o.add("odata.etag", e.etag())
	}
	if v.md == "fullmetadata" {
		o.add("odata.editLink", edit)
	}
	want := func(n string) bool { return v.sel == nil || slices.Contains(v.sel, n) }
	if want("PartitionKey") {
		o.add("PartitionKey", e.PartitionKey)
	}
	if want("RowKey") {
		o.add("RowKey", e.RowKey)
	}
	if want("Timestamp") {
		if v.md == "fullmetadata" {
			o.add("Timestamp@odata.type", "Edm.DateTime")
		}
		o.add("Timestamp", e.Timestamp.Format(dateFmt))
	}
	names := v.sel
	if names == nil {
		names = core.SortedKeys(e.Props)
	}
	for _, n := range names {
		p, ok := e.Props[n]
		switch {
		case n == "PartitionKey" || n == "RowKey" || n == "Timestamp":
		case !ok:
			o.add(n, nil) // selected but absent
		default:
			if v.md != "nometadata" && annotated(p) {
				o.add(n+"@odata.type", p.Type)
			}
			o.add(n, p.Value)
		}
	}
	return o.close()
}

func (v view) table(t *Table, meta string) []byte {
	var o obj
	if v.md != "nometadata" && meta != "" {
		o.add("odata.metadata", v.base+"/$metadata#"+meta)
	}
	if v.md == "fullmetadata" {
		edit := "Tables('" + t.Name + "')"
		o.add("odata.type", v.account+".Tables")
		o.add("odata.id", v.base+"/"+edit)
		o.add("odata.editLink", edit)
	}
	o.add("TableName", t.Name)
	return o.close()
}

func (v view) list(meta string, items [][]byte) []byte {
	var o obj
	if v.md != "nometadata" {
		o.add("odata.metadata", v.base+"/$metadata#"+meta)
	}
	o.add("value", json.RawMessage("["+string(bytes.Join(items, []byte(",")))+"]"))
	return o.close()
}

func writeJSON(w http.ResponseWriter, status int, md string, body []byte) {
	w.Header().Set("Content-Type", "application/json;odata="+md+";streaming=true;charset=utf-8")
	w.WriteHeader(status)
	w.Write(body)
}

// noContent honours Prefer: return-no-content.
func noContent(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Prefer") != "return-no-content" {
		return false
	}
	w.Header().Set("Preference-Applied", "return-no-content")
	w.WriteHeader(http.StatusNoContent)
	return true
}

func topParam(q url.Values) (int, error) {
	if q.Get("$top") == "" {
		return 1000, nil
	}
	n, err := strconv.Atoi(q.Get("$top"))
	if err != nil || n < 1 || n > 1000 {
		return 0, errInput()
	}
	return n, nil
}

// ---- tables ----

var tableName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{2,62}$`)

func (s *Service) createTable(w http.ResponseWriter, r *http.Request, account string) error {
	var in struct{ TableName string }
	if json.Unmarshal(core.Body(r), &in) != nil {
		return errInput()
	}
	if !tableName.MatchString(in.TableName) || strings.EqualFold(in.TableName, "tables") {
		return core.Errorf(400, "InvalidResourceName", "The specified resource name contains invalid characters.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	k := strings.ToLower(in.TableName)
	if a.Tables[k] != nil {
		return core.Errorf(409, "TableAlreadyExists", "The table specified already exists.")
	}
	t := &Table{Name: in.TableName, Entities: map[string]*Entity{}}
	a.Tables[k] = t
	if noContent(w, r) {
		return nil
	}
	v := newView(r, account)
	writeJSON(w, http.StatusCreated, v.md, v.table(t, "Tables/@Element"))
	return nil
}

func (s *Service) queryTables(w http.ResponseWriter, r *http.Request, account string) error {
	q := r.URL.Query()
	match, err := parseFilter(q.Get("$filter"))
	if err != nil {
		return err
	}
	top, err := topParam(q)
	if err != nil {
		return err
	}
	next := strings.ToLower(q.Get("NextTableName"))
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	v := newView(r, account)
	var items [][]byte
	for _, k := range core.SortedKeys(a.Tables) {
		t := a.Tables[k]
		if k < next || !match(func(n string) (val, bool) { return val{kind: 's', s: t.Name}, n == "TableName" }) {
			continue
		}
		if len(items) == top {
			w.Header().Set("x-ms-continuation-NextTableName", k)
			break
		}
		items = append(items, v.table(t, ""))
	}
	writeJSON(w, http.StatusOK, v.md, v.list("Tables", items))
	return nil
}

func (s *Service) deleteTable(w http.ResponseWriter, account, rest string) error {
	_, args, err := target(rest)
	name, ok := args[""]
	if err != nil || !ok {
		return errInput()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Tables[strings.ToLower(name)] == nil {
		return errNoEntity()
	}
	delete(a.Tables, strings.ToLower(name))
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// checkACL enforces the service limits: at most 5 identifiers of up to 64 characters.
func checkACL(body []byte) error {
	var in struct {
		IDs []string `xml:"SignedIdentifier>Id"`
	}
	if len(bytes.TrimSpace(body)) > 0 && xml.Unmarshal(body, &in) != nil || len(in.IDs) > 5 ||
		slices.ContainsFunc(in.IDs, func(id string) bool { return len(id) > 64 }) {
		return core.Errorf(400, "InvalidXmlDocument", "XML specified is not syntactically valid.")
	}
	return nil
}

func (s *Service) acl(w http.ResponseWriter, r *http.Request, account, name string, set bool) error {
	body := core.Body(r)
	if set {
		if err := checkACL(body); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.table(account, name)
	switch {
	case t == nil:
		return errNoTable()
	case set:
		t.ACL = string(body)
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, or(t.ACL, `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers />`))
	return nil
}

func (s *Service) serviceProps(w http.ResponseWriter, account string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if body != nil {
		a.Props = string(body)
		w.WriteHeader(http.StatusAccepted)
		return nil
	}
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, or(a.Props, `<?xml version="1.0" encoding="utf-8"?><StorageServiceProperties><Logging><Version>1.0</Version><Delete>false</Delete><Read>false</Read><Write>false</Write><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging><HourMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics><MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics><Cors /></StorageServiceProperties>`))
	return nil
}

// ---- entities ----

// entity runs one entity operation for a request or a batch part; s.mu is held.
func (s *Service) entity(w http.ResponseWriter, r *http.Request, account, rest string) error {
	name, args, err := target(rest)
	if err != nil {
		return err
	}
	t := s.table(account, name)
	if t == nil {
		return errNoTable()
	}
	pk, okp := args["PartitionKey"]
	rk, okr := args["RowKey"]
	switch m := method(r); {
	case len(args) == 0 && m == http.MethodPost:
		return s.insert(w, r, account, t)
	case len(args) == 0 && m == http.MethodGet:
		return s.query(w, r, account, t)
	case !okp || !okr || len(args) != 2:
	case m == http.MethodGet:
		return s.get(w, r, account, t, pk, rk)
	case m == http.MethodPut || m == http.MethodPatch || m == "MERGE":
		return s.write(w, r, t, pk, rk, m != http.MethodPut)
	case m == http.MethodDelete:
		return s.remove(w, r, t, pk, rk)
	}
	return errInput()
}

// checkKeys rejects the characters the service forbids in PartitionKey and RowKey.
func checkKeys(keys ...string) error {
	for _, k := range keys {
		if len(k) > 1024 || strings.ContainsFunc(k, func(c rune) bool { return c < 0x20 || c >= 0x7f && c <= 0x9f || strings.ContainsRune(`/\#?`, c) }) {
			return core.Errorf(400, "OutOfRangeInput", "The PartitionKey or RowKey is too long or contains a forbidden character.")
		}
	}
	return nil
}

// parseEntity reads a JSON entity: its keys (nil when absent) and typed properties.
func parseEntity(body []byte) (pk, rk *string, props map[string]Prop, err error) {
	var raw map[string]json.RawMessage
	if json.Unmarshal(body, &raw) != nil {
		return nil, nil, nil, errInput()
	}
	props = map[string]Prop{}
	for k, v := range raw {
		switch {
		case k == "PartitionKey" || k == "RowKey":
			var s string
			if json.Unmarshal(v, &s) != nil {
				return nil, nil, nil, errInput()
			}
			if k == "PartitionKey" {
				pk = &s
			} else {
				rk = &s
			}
		case k == "Timestamp" || strings.HasPrefix(k, "odata.") || strings.Contains(k, "@odata."):
			// set by the service, or metadata
		default:
			var typ string
			if a, ok := raw[k+"@odata.type"]; ok && json.Unmarshal(a, &typ) != nil {
				return nil, nil, nil, errInput()
			}
			p, err := typed(typ, v)
			if err != nil {
				return nil, nil, nil, err
			}
			if p.Type != "" { // nulls aren't stored
				props[k] = p
			}
		}
	}
	return pk, rk, props, nil
}

// typed checks a value against its EDM type (inferred when unannotated) and
// returns it the way the service writes it back.
func typed(t string, v json.RawMessage) (Prop, error) {
	var s string
	isStr := json.Unmarshal(v, &s) == nil
	num := string(v)
	if isStr {
		num = s
	}
	switch t {
	case "":
		switch {
		case string(v) == "null":
			return Prop{}, nil
		case isStr:
			return Prop{"Edm.String", v}, nil
		case string(v) == "true" || string(v) == "false":
			return Prop{"Edm.Boolean", v}, nil
		case v[0] == '{' || v[0] == '[':
			return Prop{}, errInput()
		}
		if _, err := strconv.ParseInt(num, 10, 32); err == nil {
			return Prop{"Edm.Int32", v}, nil
		}
		return typed("Edm.Double", v)
	case "Edm.String":
		if isStr {
			return Prop{t, v}, nil
		}
	case "Edm.Boolean":
		if string(v) == "true" || string(v) == "false" {
			return Prop{t, v}, nil
		}
	case "Edm.Int32", "Edm.Int64":
		if n, err := strconv.ParseInt(num, 10, map[string]int{"Edm.Int32": 32, "Edm.Int64": 64}[t]); err == nil {
			out := strconv.FormatInt(n, 10)
			if t == "Edm.Int64" {
				out = strconv.Quote(out) // Int64 travels as a string
			}
			return Prop{t, json.RawMessage(out)}, nil
		}
	case "Edm.Double":
		if s == "NaN" || s == "Infinity" || s == "-Infinity" {
			return Prop{t, v}, nil
		}
		if f, err := strconv.ParseFloat(num, 64); err == nil {
			if b, err := json.Marshal(f); err == nil {
				if !bytes.ContainsAny(b, ".eE") {
					b = append(b, ".0"...) // keep whole doubles from reading back as Int32
				}
				return Prop{t, b}, nil
			}
		}
	case "Edm.DateTime":
		if tm, err := parseTime(s); isStr && err == nil {
			return Prop{t, json.RawMessage(strconv.Quote(tm.UTC().Format(dateFmt)))}, nil
		}
	case "Edm.Guid":
		if isStr {
			return Prop{t, v}, nil
		}
	case "Edm.Binary":
		if _, err := base64.StdEncoding.DecodeString(s); isStr && err == nil {
			return Prop{t, v}, nil
		}
	}
	return Prop{}, core.Errorf(400, "InvalidInput", "A property value is not a valid %s.", or(t, "JSON value"))
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05.999999999", s) // no zone: UTC
	}
	return t, err
}

func (s *Service) insert(w http.ResponseWriter, r *http.Request, account string, t *Table) error {
	pk, rk, props, err := parseEntity(core.Body(r))
	if err != nil {
		return err
	}
	if pk == nil || rk == nil {
		return core.Errorf(400, "PropertiesNeedValue", "The values are not specified for all properties in the entity.")
	}
	if err := checkKeys(*pk, *rk); err != nil {
		return err
	}
	k := *pk + "\x00" + *rk
	if t.Entities[k] != nil {
		return core.Errorf(409, "EntityAlreadyExists", "The specified entity already exists.")
	}
	e := &Entity{PartitionKey: *pk, RowKey: *rk, Timestamp: s.stamp(), Props: props}
	t.Entities[k] = e
	w.Header().Set("ETag", e.etag())
	if noContent(w, r) {
		return nil
	}
	v := newView(r, account)
	writeJSON(w, http.StatusCreated, v.md, v.entity(e, t.Name, t.Name+"/@Element"))
	return nil
}

func (s *Service) get(w http.ResponseWriter, r *http.Request, account string, t *Table, pk, rk string) error {
	e := t.Entities[pk+"\x00"+rk]
	if e == nil {
		return errNoEntity()
	}
	v := newView(r, account)
	w.Header().Set("ETag", e.etag())
	writeJSON(w, http.StatusOK, v.md, v.entity(e, t.Name, t.Name+"/@Element"))
	return nil
}

// write replaces (PUT) or merges (MERGE, PATCH) an entity; without If-Match it upserts.
func (s *Service) write(w http.ResponseWriter, r *http.Request, t *Table, pk, rk string, merge bool) error {
	_, _, props, err := parseEntity(core.Body(r))
	if err != nil {
		return err
	}
	if err := checkKeys(pk, rk); err != nil {
		return err
	}
	k := pk + "\x00" + rk
	cur := t.Entities[k]
	switch im := r.Header.Get("If-Match"); {
	case im == "":
	case cur == nil:
		return errNoEntity()
	case im != "*" && im != cur.etag():
		return errCondition()
	}
	if merge && cur != nil {
		for n, p := range cur.Props {
			if _, ok := props[n]; !ok {
				props[n] = p
			}
		}
	}
	e := &Entity{PartitionKey: pk, RowKey: rk, Timestamp: s.stamp(), Props: props}
	t.Entities[k] = e
	w.Header().Set("ETag", e.etag())
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) remove(w http.ResponseWriter, r *http.Request, t *Table, pk, rk string) error {
	im := r.Header.Get("If-Match")
	if im == "" {
		return core.Errorf(400, "MissingRequiredHeader", "An HTTP header that's mandatory for this request is not specified.")
	}
	k := pk + "\x00" + rk
	switch cur := t.Entities[k]; {
	case cur == nil:
		return errNoEntity()
	case im != "*" && im != cur.etag():
		return errCondition()
	}
	delete(t.Entities, k)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// Continuation tokens are opaque: "1!" + base64(key), never empty.
func encodeToken(s string) string { return "1!" + base64.RawURLEncoding.EncodeToString([]byte(s)) }

func decodeToken(s string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(s, "1!"))
	if err != nil {
		return "", errInput()
	}
	return string(b), nil
}

func (s *Service) query(w http.ResponseWriter, r *http.Request, account string, t *Table) error {
	q := r.URL.Query()
	match, err := parseFilter(q.Get("$filter"))
	if err != nil {
		return err
	}
	top, err := topParam(q)
	if err != nil {
		return err
	}
	npk, err := decodeToken(q.Get("NextPartitionKey"))
	if err != nil {
		return err
	}
	nrk, err := decodeToken(q.Get("NextRowKey"))
	if err != nil {
		return err
	}
	from := npk + "\x00" + nrk
	v := newView(r, account)
	var items [][]byte
	for _, k := range core.SortedKeys(t.Entities) {
		e := t.Entities[k]
		if k < from || !match(e.get) {
			continue
		}
		if len(items) == top {
			w.Header().Set("x-ms-continuation-NextPartitionKey", encodeToken(e.PartitionKey))
			w.Header().Set("x-ms-continuation-NextRowKey", encodeToken(e.RowKey))
			break
		}
		items = append(items, v.entity(e, t.Name, ""))
	}
	writeJSON(w, http.StatusOK, v.md, v.list(t.Name, items))
	return nil
}

// ---- entity group transactions ----

// part is one operation of a batch changeset.
type part struct {
	req  *http.Request
	rest string // path after the account
	body []byte
	cid  string // Content-ID, echoed back
}

// recorder buffers one operation's response inside a batch.
type recorder struct {
	h      http.Header
	status int
	body   bytes.Buffer
	cid    string
}

func (r *recorder) Header() http.Header { return r.h }

func (r *recorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}

func (r *recorder) Write(b []byte) (int, error) {
	r.WriteHeader(http.StatusOK)
	return r.body.Write(b)
}

// readBatch parses multipart/mixed: one changeset of application/http requests.
func readBatch(r *http.Request) ([]part, error) {
	bad := func() error { return core.Errorf(400, "InvalidInput", "The batch request body is malformed.") }
	boundary := func(ct string) string {
		_, p, _ := mime.ParseMediaType(ct)
		return p["boundary"]
	}
	b := boundary(r.Header.Get("Content-Type"))
	if b == "" {
		return nil, bad()
	}
	cs, err := multipart.NewReader(bytes.NewReader(core.Body(r)), b).NextPart()
	if err != nil || boundary(cs.Header.Get("Content-Type")) == "" {
		return nil, bad() // ponytail: only changesets; a lone query part in a batch isn't supported
	}
	inner := multipart.NewReader(cs, boundary(cs.Header.Get("Content-Type")))
	var parts []part
	for {
		p, err := inner.NextPart()
		if err == io.EOF && len(parts) > 0 {
			return parts, nil
		}
		if err != nil {
			return nil, bad()
		}
		raw, err := io.ReadAll(p)
		if err != nil {
			return nil, bad()
		}
		head, body, ok := bytes.Cut(raw, []byte("\r\n\r\n"))
		if !ok {
			head, body, _ = bytes.Cut(raw, []byte("\n\n"))
		}
		// The body is everything after the headers, Content-Length or not.
		req, err := http.ReadRequest(bufio.NewReader(io.MultiReader(bytes.NewReader(head), strings.NewReader("\r\n\r\n"))))
		if err != nil {
			return nil, bad()
		}
		_, rest, _ := strings.Cut(strings.TrimPrefix(req.URL.Path, "/"), "/")
		parts = append(parts, part{req, rest, bytes.TrimSpace(body), or(p.Header.Get("Content-ID"), req.Header.Get("Content-ID"))})
	}
}

// keys returns the table and entity keys a batch part works on.
func (p part) keys() (name, pk, rk string, err error) {
	name, args, err := target(p.rest)
	if err != nil || len(args) > 0 {
		return name, args["PartitionKey"], args["RowKey"], err
	}
	var e struct{ PartitionKey, RowKey string }
	json.Unmarshal(p.body, &e) // a bad body fails in insert
	return name, e.PartitionKey, e.RowKey, nil
}

// changeset runs the parts all-or-nothing; s.mu is held. On failure it puts
// back every entity it touched and returns the failing part's index.
func (s *Service) changeset(parts []part, account string) ([]*recorder, int, error) {
	var (
		t            *Table
		table0, pk0  string
		recs         []*recorder
		prev         = map[string]*Entity{} // entity before the batch, nil when absent
		rows         = map[string]bool{}
		errPartition = func() error {
			return core.Errorf(400, "CommandsInBatchActOnDifferentPartitions", "All commands in a batch must operate on the same table and partition.")
		}
	)
	for i, p := range parts {
		name, pk, rk, err := p.keys()
		switch {
		case err != nil:
		case i == 100:
			err = core.Errorf(400, "InvalidInput", "The batch request contains more than 100 operations.")
		case method(p.req) == http.MethodGet:
			err = core.Errorf(400, "InvalidInput", "A changeset can only contain inserts, updates, merges and deletes.")
		case i == 0:
			table0, pk0 = name, pk
			if t = s.table(account, name); t == nil {
				err = errNoTable()
			}
		case !strings.EqualFold(name, table0) || pk != pk0:
			err = errPartition()
		case rows[rk]:
			err = core.Errorf(400, "InvalidDuplicateRow", "The batch request contains multiple changes with same row key. An entity can appear only once in a batch request.")
		}
		if err == nil {
			rows[rk] = true
			k := pk + "\x00" + rk
			prev[k] = t.Entities[k]
			rec := &recorder{h: http.Header{}, cid: p.cid}
			p.req.Body = io.NopCloser(bytes.NewReader(p.body))
			if err = s.entity(rec, p.req, account, p.rest); err == nil {
				recs = append(recs, rec)
				continue
			}
		}
		for k, e := range prev {
			if e == nil {
				delete(t.Entities, k)
			} else {
				t.Entities[k] = e
			}
		}
		return nil, i, err
	}
	return recs, 0, nil
}

// batch answers 202 with a multipart/mixed changeset response: every
// operation's response, or only the failing one's error ("index:message").
func (s *Service) batch(w http.ResponseWriter, r *http.Request, c *core.Ctx, account string) error {
	parts, err := readBatch(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	recs, failed, err := s.changeset(parts, account)
	s.mu.Unlock()
	if err != nil {
		rec := &recorder{h: http.Header{}, cid: parts[failed].cid}
		writeError(rec, c, err, strconv.Itoa(failed)+":")
		recs = []*recorder{rec}
		w.Header().Set("x-ms-error-code", rec.h.Get("x-ms-error-code")) // for SDKs that only read the outer response
	}
	bb, cb := "batchresponse_"+core.UUID(), "changesetresponse_"+core.UUID()
	var b bytes.Buffer
	fmt.Fprintf(&b, "--%s\r\nContent-Type: multipart/mixed; boundary=%s\r\n\r\n", bb, cb)
	for _, rec := range recs {
		fmt.Fprintf(&b, "--%s\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\nHTTP/1.1 %d %s\r\n", cb, rec.status, http.StatusText(rec.status))
		if rec.cid != "" {
			rec.h.Set("Content-ID", rec.cid)
		}
		rec.h.Write(&b)
		b.WriteString("\r\n")
		b.Write(rec.body.Bytes())
		b.WriteString("\r\n")
	}
	fmt.Fprintf(&b, "--%s--\r\n--%s--\r\n", cb, bb)
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+bb)
	w.WriteHeader(http.StatusAccepted)
	w.Write(b.Bytes())
	return nil
}

// ---- OData $filter ----

// val is a typed value: an entity property or a $filter literal.
type val struct {
	kind  byte // 's'tring, 'n'umber, 'b'oolean, 'd'atetime, 'g'uid, 'x' binary
	s     string
	f     float64
	i     int64
	isInt bool
	t     time.Time
}

type (
	getter func(name string) (val, bool)
	pred   func(getter) bool
	term   func(getter) (val, bool)
)

func number(text string) (val, bool) {
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return val{kind: 'n', i: n, isInt: true, f: float64(n)}, true
	}
	f, err := strconv.ParseFloat(text, 64)
	return val{kind: 'n', f: f}, err == nil
}

func (p Prop) val() (val, bool) {
	var s string
	json.Unmarshal(p.Value, &s) // strings, Int64 and special doubles; numbers leave s empty
	switch p.Type {
	case "Edm.Int32", "Edm.Int64", "Edm.Double":
		return number(or(s, string(p.Value)))
	case "Edm.Boolean":
		return val{kind: 'b', s: string(p.Value)}, true
	case "Edm.DateTime":
		t, err := parseTime(s)
		return val{kind: 'd', t: t}, err == nil
	case "Edm.Guid":
		return val{kind: 'g', s: strings.ToLower(s)}, true
	case "Edm.Binary":
		b, err := base64.StdEncoding.DecodeString(s)
		return val{kind: 'x', s: string(b)}, err == nil
	}
	return val{kind: 's', s: s}, true
}

func (e *Entity) get(name string) (val, bool) {
	switch name {
	case "PartitionKey":
		return val{kind: 's', s: e.PartitionKey}, true
	case "RowKey":
		return val{kind: 's', s: e.RowKey}, true
	case "Timestamp":
		return val{kind: 'd', t: e.Timestamp}, true
	}
	p, ok := e.Props[name]
	if !ok {
		return val{}, false
	}
	return p.val()
}

// compare orders two values of one kind (strings ordinally); Int32, Int64
// and Double compare as numbers. Different kinds never match.
func compare(a, b val) (int, bool) {
	switch {
	case a.kind != b.kind:
		return 0, false
	case a.kind == 'n' && a.isInt && b.isInt:
		return cmp.Compare(a.i, b.i), true
	case a.kind == 'n':
		return cmp.Compare(a.f, b.f), true
	case a.kind == 'd':
		return a.t.Compare(b.t), true
	}
	return strings.Compare(a.s, b.s), true
}

var errFilter = func() error { return core.Errorf(400, "InvalidInput", "The $filter expression is not valid.") }

// lex splits a filter into "(", ")", 'quoted' literals (maybe prefixed, as
// in datetime'…') and words.
func lex(s string) ([]string, error) {
	const space = " \t\r\n"
	var toks []string
	for s = strings.TrimLeft(s, space); s != ""; s = strings.TrimLeft(s, space) {
		i := 1
		if s[0] != '(' && s[0] != ')' {
			if i = strings.IndexAny(s, space+"()'"); i < 0 {
				i = len(s)
			}
			if i < len(s) && s[i] == '\'' {
				closed := false
				for i++; i < len(s) && !closed; i++ {
					if s[i] == '\'' {
						if i+1 < len(s) && s[i+1] == '\'' {
							i++
						} else {
							closed = true
						}
					}
				}
				if !closed {
					return nil, errFilter()
				}
			}
		}
		toks = append(toks, s[:i])
		s = s[i:]
	}
	return toks, nil
}

type parser struct{ toks []string }

func (p *parser) next() string {
	if len(p.toks) == 0 {
		return ""
	}
	t := p.toks[0]
	p.toks = p.toks[1:]
	return t
}

// parseFilter compiles $filter: eq ne gt ge lt le, and or not, parentheses,
// and string, number (L for Int64), bool, datetime, guid and binary literals.
// An empty filter matches everything.
func parseFilter(s string) (pred, error) {
	toks, err := lex(s)
	if err != nil || len(toks) == 0 {
		return func(getter) bool { return true }, err
	}
	p := &parser{toks}
	f, err := p.chain("or")
	if err == nil && len(p.toks) > 0 {
		err = errFilter()
	}
	return f, err
}

// chain parses operands joined by "or" (operands: and-chains) or "and" (operands: unary).
func (p *parser) chain(op string) (pred, error) {
	operand := p.unary
	if op == "or" {
		operand = func() (pred, error) { return p.chain("and") }
	}
	l, err := operand()
	for err == nil && len(p.toks) > 0 && strings.EqualFold(p.toks[0], op) {
		p.next()
		var r pred
		if r, err = operand(); err == nil {
			a := l
			if op == "and" {
				l = func(g getter) bool { return a(g) && r(g) }
			} else {
				l = func(g getter) bool { return a(g) || r(g) }
			}
		}
	}
	return l, err
}

func (p *parser) unary() (pred, error) {
	switch t := p.next(); {
	case strings.EqualFold(t, "not"):
		f, err := p.unary()
		return func(g getter) bool { return !f(g) }, err
	case t == "(":
		f, err := p.chain("or")
		if err == nil && p.next() != ")" {
			err = errFilter()
		}
		return f, err
	case t == "" || t == ")":
		return nil, errFilter()
	default:
		return p.comparison(t)
	}
}

var ops = map[string]func(int) bool{
	"eq": func(c int) bool { return c == 0 }, "ne": func(c int) bool { return c != 0 },
	"gt": func(c int) bool { return c > 0 }, "ge": func(c int) bool { return c >= 0 },
	"lt": func(c int) bool { return c < 0 }, "le": func(c int) bool { return c <= 0 },
}

// comparison parses "left op right"; a missing property or a type mismatch never matches.
func (p *parser) comparison(left string) (pred, error) {
	op, right := ops[strings.ToLower(p.next())], p.next()
	l, err1 := operand(left)
	r, err2 := operand(right)
	if op == nil || err1 != nil || err2 != nil {
		return nil, errFilter()
	}
	return func(g getter) bool {
		a, ok1 := l(g)
		b, ok2 := r(g)
		c, ok := compare(a, b)
		return ok1 && ok2 && ok && op(c)
	}, nil
}

func lit(v val) term { return func(getter) (val, bool) { return v, true } }

// operand compiles a literal or a property reference.
func operand(tok string) (term, error) {
	if tok == "" || tok == "(" || tok == ")" {
		return nil, errFilter()
	}
	if q := strings.IndexByte(tok, '\''); q >= 0 {
		body := strings.ReplaceAll(tok[q+1:len(tok)-1], "''", "'")
		v, err := val{kind: 's', s: body}, error(nil)
		switch strings.ToLower(tok[:q]) {
		case "":
		case "datetime":
			v = val{kind: 'd'}
			v.t, err = parseTime(body)
		case "guid":
			v = val{kind: 'g', s: strings.ToLower(body)}
		case "x", "binary":
			var b []byte
			b, err = hex.DecodeString(body)
			v = val{kind: 'x', s: string(b)}
		default:
			err = errFilter()
		}
		return lit(v), err
	}
	switch low := strings.ToLower(tok); {
	case low == "true" || low == "false":
		return lit(val{kind: 'b', s: low}), nil
	case strings.ContainsRune("+-.0123456789", rune(tok[0])):
		v, ok := number(strings.TrimRight(tok, "LlDdFfMm"))
		if !ok {
			return nil, errFilter()
		}
		return lit(v), nil
	}
	return func(g getter) (val, bool) { return g(tok) }, nil
}

// ---- console ----

// Resources lists tables across accounts.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Tables", Columns: []string{"Name", "Account", "Entities"}}
	for _, acct := range core.SortedKeys(s.st.Accounts) {
		a := s.st.Accounts[acct]
		for _, k := range core.SortedKeys(a.Tables) {
			t.Rows = append(t.Rows, []string{a.Tables[k].Name, acct, strconv.Itoa(len(a.Tables[k].Entities))})
		}
	}
	return []core.Table{t}
}

// Inspect returns a table and its first 50 entities as plain JSON.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, acct := range core.SortedKeys(s.st.Accounts) {
		if t := s.st.Accounts[acct].Tables[strings.ToLower(id)]; t != nil {
			return map[string]any{"account": acct, "name": t.Name, "count": len(t.Entities), "entities": plain(t, 50)}
		}
	}
	return nil
}

// Entities returns up to limit entities (all when limit <= 0) as plain JSON
// values, ordered by PartitionKey and RowKey.
func (s *Service) Entities(account, table string, limit int) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[account]; a != nil && a.Tables[strings.ToLower(table)] != nil {
		return plain(a.Tables[strings.ToLower(table)], limit)
	}
	return nil
}

func plain(t *Table, limit int) []map[string]any {
	out := []map[string]any{}
	for _, k := range core.SortedKeys(t.Entities) {
		if limit > 0 && len(out) == limit {
			break
		}
		e := t.Entities[k]
		m := map[string]any{"PartitionKey": e.PartitionKey, "RowKey": e.RowKey, "Timestamp": e.Timestamp.Format(dateFmt)}
		for n, p := range e.Props {
			var v any
			json.Unmarshal(p.Value, &v)
			m[n] = v
		}
		out = append(out, m)
	}
	return out
}
