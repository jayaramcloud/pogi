// Package appconfig emulates the Azure App Configuration data plane (REST,
// api-version 1.0 to 2023-10-01): key-values with labels, tags, content types,
// ETag conditions, locks, history (revisions and Accept-Datetime), snapshots
// and feature flags (key-values under .appconfig.featureflag/), plus a
// store registry with access keys and connection strings.
//
// Stores are named by host (<store>.azconfig.localhost) or by path
// (/_azure/appconfig/<store>/kv/...), which is the form connection strings
// from ConnectionString use, since it needs no name resolution. A request
// with an HMAC-SHA256 Authorization header on an App Configuration path is
// also ours: its credential names the store. HMAC signatures are checked when
// Config.ValidateSigs is on and the access key is known; bearer tokens
// (Entra ID) are accepted unchecked. Unless signatures are validated, an
// unknown store is created on first use.
package appconfig

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

var (
	storeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{0,49}$`)
	hmacSep   = regexp.MustCompile(`[&,]\s*`)
)

const (
	pathPrefix = "/_azure/appconfig/"
	pageSize   = 100
	timeFormat = "2006-01-02T15:04:05+00:00"
	maxSkew    = 15 * time.Minute // HMAC requests older (or newer) than this are refused
)

// Service is the App Configuration emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
}

type state struct {
	Stores map[string]*store `json:"stores"`
	ARM    armres.Store      `json:"arm,omitempty"` // stores as the resource manager has them
}

type store struct {
	Name      string               `json:"name"`
	Created   time.Time            `json:"created"`
	Keys      []AccessKey          `json:"keys"`
	Seq       int64                `json:"seq"`                 // every write; the sync token's sequence number
	Cur       map[string]*setting  `json:"settings"`            // by key + "\x00" + label
	History   []rev                `json:"history"`             // every version of every setting, oldest first
	Snapshots map[string]*snapshot `json:"snapshots,omitempty"` // by name
}

// AccessKey is one of a store's access keys; its connection string is
// Endpoint=...;Id=<ID>;Secret=<Secret>.
type AccessKey struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Secret   string `json:"secret"` // base64
	ReadOnly bool   `json:"readOnly,omitempty"`
}

type setting struct {
	Key         string            `json:"key"`
	Label       string            `json:"label,omitempty"` // "" is the null label
	Value       *string           `json:"value"`
	ContentType *string           `json:"contentType"`
	Tags        map[string]string `json:"tags,omitempty"`
	ETag        string            `json:"etag"`
	Locked      bool              `json:"locked,omitempty"`
	Modified    time.Time         `json:"modified"`
}

// rev is one version of a setting; Deleted marks the removal, so that
// Accept-Datetime can tell a key was gone.
type rev struct {
	setting
	Deleted bool `json:"deleted,omitempty"`
}

type filter struct {
	Key   *string `json:"key,omitempty"`
	Label *string `json:"label,omitempty"`
}

type snapshot struct {
	Name        string            `json:"name"`
	Status      string            `json:"status"` // provisioning, ready, archived, failed
	Filters     []filter          `json:"filters"`
	Composition string            `json:"compositionType"`
	Retention   int64             `json:"retentionPeriod"`
	Tags        map[string]string `json:"tags,omitempty"`
	Created     time.Time         `json:"created"`
	Expires     *time.Time        `json:"expires,omitempty"`
	ETag        string            `json:"etag"`
	Items       []setting         `json:"items"`
	Error       string            `json:"error,omitempty"`
}

func newStore(name string) *store {
	st := &store{Name: name, Created: time.Now().UTC(), Cur: map[string]*setting{}, Snapshots: map[string]*snapshot{}}
	for _, k := range []struct {
		name string
		ro   bool
	}{{"Primary", false}, {"Secondary", false}, {"Primary Read Only", true}, {"Secondary Read Only", true}} {
		sec := make([]byte, 32)
		rand.Read(sec)
		st.Keys = append(st.Keys, AccessKey{Name: k.name, ID: randString(4) + "-l0-s0:" + randString(20), Secret: base64.StdEncoding.EncodeToString(sec), ReadOnly: k.ro})
	}
	return st
}

func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// New creates the App Configuration service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azappconfig", Cloud: core.Azure, Title: "App Configuration", Match: match, Protocol: "azure-json"}
}

// ---- routing ----

// route is the store a request names and the API path after it. A
// request with an HMAC Authorization header on an API path names no store:
// its credential does.
func route(r *http.Request) (name, path string, ok bool) {
	if rest, found := strings.CutPrefix(r.URL.Path, pathPrefix); found {
		name, path, _ = strings.Cut(rest, "/")
		return name, "/" + path, name != ""
	}
	host, _, _ := strings.Cut(r.Host, ":")
	if parts := strings.Split(host, "."); len(parts) >= 3 && parts[1] == "azconfig" {
		return parts[0], r.URL.Path, true
	}
	api, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return "", r.URL.Path, strings.HasPrefix(r.Header.Get("Authorization"), "HMAC-SHA256 ") &&
		slices.Contains([]string{"kv", "keys", "labels", "revisions", "locks", "snapshots", "operations"}, api)
}

func match(r *http.Request) bool { _, _, ok := route(r); return ok }

// hmacAuth is a parsed "HMAC-SHA256 Credential=..&SignedHeaders=..&Signature=.." header;
// the Go SDK separates the fields with ", ".
type hmacAuth struct{ cred, signed, sig string }

func parseHMAC(h string) (a hmacAuth, ok bool) {
	rest, ok := strings.CutPrefix(h, "HMAC-SHA256 ")
	if !ok {
		return a, false
	}
	for _, f := range hmacSep.Split(rest, -1) {
		k, v, _ := strings.Cut(f, "=")
		switch strings.TrimSpace(k) {
		case "Credential":
			a.cred = v
		case "SignedHeaders":
			a.signed = v
		case "Signature":
			a.sig = v
		}
	}
	return a, a.cred != "" && a.signed != "" && a.sig != ""
}

// verify checks the HMAC signature of r made with the key's secret:
// HMAC-SHA256 over METHOD \n path-and-query \n signed header values joined by ";".
func verify(r *http.Request, a hmacAuth, k AccessKey, body []byte) string {
	secret, err := base64.StdEncoding.DecodeString(k.Secret)
	if err != nil {
		return "the access key is not valid"
	}
	var vals []string
	var date string
	for _, h := range strings.Split(a.signed, ";") {
		v := r.Header.Get(h)
		switch strings.ToLower(h) {
		case "host":
			v = r.Host
		case "date", "x-ms-date":
			date = v
		}
		vals = append(vals, v)
	}
	if t, err := http.ParseTime(date); err != nil || time.Since(t).Abs() > maxSkew {
		return "the request date is missing, malformed or more than 15 minutes off"
	}
	sum := sha256.Sum256(body)
	if r.Header.Get("x-ms-content-sha256") != base64.StdEncoding.EncodeToString(sum[:]) {
		return "x-ms-content-sha256 does not match the request body"
	}
	path := r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%s\n%s\n%s", r.Method, path, strings.Join(vals, ";"))
	if subtle.ConstantTimeCompare([]byte(base64.StdEncoding.EncodeToString(mac.Sum(nil))), []byte(a.sig)) != 1 {
		return "the signature does not match"
	}
	return ""
}

// authorize finds the store of a request and checks its credentials. The
// store is the named one, or the one whose access key signed the request.
// Lock held.
func (s *Service) authorize(r *http.Request, name string) (*store, *apiError) {
	auth := r.Header.Get("Authorization")
	h, signed := parseHMAC(auth)
	st := s.st.Stores[name]
	if st == nil && signed {
		for _, x := range s.st.Stores {
			if x.key(h.cred) != nil {
				st = x
			}
		}
	}
	validate := s.hub.Cfg.ValidateSigs
	if st == nil {
		if validate {
			return nil, unauthorized("the store does not exist or the access key is unknown")
		}
		st = s.create(cmpOr(name, "default"))
	}
	var key *AccessKey
	switch {
	case signed:
		key = st.key(h.cred)
		if validate && key == nil {
			return nil, unauthorized("the access key is not known to this store")
		}
		if validate {
			if msg := verify(r, h, *key, core.Body(r)); msg != "" {
				return nil, unauthorized(msg)
			}
		}
	case strings.HasPrefix(auth, "Bearer "): // Entra ID: any token
	case validate:
		return nil, unauthorized("no credentials: send an HMAC-SHA256 or Bearer Authorization header")
	}
	if key != nil && key.ReadOnly && r.Method != http.MethodGet && r.Method != http.MethodHead {
		return nil, &apiError{http.StatusForbidden, "forbidden", "Forbidden", "The access key is read-only."}
	}
	return st, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (st *store) key(id string) *AccessKey {
	for i := range st.Keys {
		if st.Keys[i].ID == id {
			return &st.Keys[i]
		}
	}
	return nil
}

// ---- errors and responses ----

type apiError struct {
	status     int
	typ, title string
	detail     string
}

func (e *apiError) Error() string { return e.title + ": " + e.detail }

func unauthorized(msg string) *apiError {
	return &apiError{http.StatusUnauthorized, "invalid-token", "Unauthorized", msg}
}

func problem(status int, typ, title, detail string) *apiError {
	return &apiError{status, typ, title, detail}
}

var (
	errPrecondition = problem(http.StatusPreconditionFailed, "precondition-failed", "Precondition failed", "The If-Match or If-None-Match condition did not hold.")
	errLocked       = func(key string) *apiError {
		return problem(http.StatusConflict, "key-locked", "Modifing key '"+key+"' is not allowed", "")
	}
)

func (e *apiError) write(w http.ResponseWriter, c *core.Ctx) {
	c.ErrCode = e.typ
	if e.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `HMAC-SHA256 error="invalid_token" error_description="`+strings.ReplaceAll(e.detail, `"`, "'")+`"`)
	}
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(e.status)
	json.NewEncoder(w).Encode(map[string]any{"type": "https://azconfig.io/errors/" + e.typ, "title": e.title, "name": "", "detail": e.detail, "status": e.status})
}

// reply is what an operation answers with: a body (nil: no body), its media
// type and extra headers.
type reply struct {
	status int
	media  string
	body   any
	header http.Header
}

func ok(media string, body any) *reply {
	return &reply{status: http.StatusOK, media: media, body: body, header: http.Header{}}
}

const (
	mediaKV       = "application/vnd.microsoft.appconfig.kv+json"
	mediaKVSet    = "application/vnd.microsoft.appconfig.kvset+json"
	mediaKeys     = "application/vnd.microsoft.appconfig.keyset+json"
	mediaLabels   = "application/vnd.microsoft.appconfig.labelset+json"
	mediaSnapshot = "application/vnd.microsoft.appconfig.snapshot+json"
	mediaSnaps    = "application/vnd.microsoft.appconfig.snapshotset+json"
)

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	name, path, _ := route(r)
	api, rest, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	c.Op = r.Method + " /" + api
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		(&apiError{e.Status, e.Code, e.Code, e.Msg}).write(w, c)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, aerr := s.authorize(r, name)
	if aerr != nil {
		aerr.write(w, c)
		return
	}
	rep, aerr := s.dispatch(st, r, api, rest)
	w.Header().Set("Sync-Token", fmt.Sprintf("%s=pogi;sn=%d", st.Name, st.Seq))
	if aerr != nil {
		aerr.write(w, c)
		return
	}
	for k, v := range rep.header {
		w.Header()[k] = v
	}
	if rep.body == nil {
		w.WriteHeader(rep.status)
		return
	}
	w.Header().Set("Content-Type", rep.media+"; charset=utf-8")
	w.WriteHeader(rep.status)
	json.NewEncoder(w).Encode(rep.body)
}

func (s *Service) dispatch(st *store, r *http.Request, api, rest string) (*reply, *apiError) {
	get := r.Method == http.MethodGet || r.Method == http.MethodHead
	switch {
	case api == "kv" && rest == "" && get:
		return st.listKV(r)
	case api == "kv" && rest != "" && get:
		return st.getKV(r, rest)
	case api == "kv" && rest != "" && r.Method == http.MethodPut:
		return st.putKV(r, rest)
	case api == "kv" && rest != "" && r.Method == http.MethodDelete:
		return st.deleteKV(r, rest)
	case api == "locks" && rest != "" && (r.Method == http.MethodPut || r.Method == http.MethodDelete):
		return st.lock(r, rest, r.Method == http.MethodPut)
	case api == "keys" && get:
		return st.listNames(r, mediaKeys, false)
	case api == "labels" && get:
		return st.listNames(r, mediaLabels, true)
	case api == "revisions" && get:
		return st.revisions(r)
	case api == "snapshots" && rest == "" && get:
		return st.listSnapshots(r)
	case api == "snapshots" && rest != "" && get:
		return st.getSnapshot(r, rest)
	case api == "snapshots" && rest != "" && r.Method == http.MethodPut:
		return st.createSnapshot(r, rest)
	case api == "snapshots" && rest != "" && r.Method == http.MethodPatch:
		return st.updateSnapshot(r, rest)
	case api == "operations" && get:
		return st.operation(r)
	}
	return nil, problem(http.StatusNotFound, "not-found", "Not found", "pogi does not implement "+r.Method+" "+r.URL.Path)
}

// ---- request helpers ----

// param reads a query parameter by name, ignoring case ($Select, After).
func param(r *http.Request, name string) (string, bool) {
	for k, v := range r.URL.Query() {
		if strings.EqualFold(k, name) {
			return v[0], true
		}
	}
	return "", false
}

// at is the Accept-Datetime of a request: its header, or an After parameter
// that is a time (the Go SDK sends its selector's AcceptDateTime as After).
func at(r *http.Request) (time.Time, bool, *apiError) {
	v := r.Header.Get("Accept-Datetime")
	if a, _ := param(r, "after"); v == "" && !strings.HasPrefix(a, "pogi:") {
		v = a
	}
	if v == "" {
		return time.Time{}, false, nil
	}
	for _, f := range []string{time.RFC1123, time.RFC3339Nano, http.TimeFormat} {
		if t, err := time.Parse(f, v); err == nil {
			return t.UTC(), true, nil
		}
	}
	return time.Time{}, false, problem(http.StatusBadRequest, "invalid-argument", "Invalid argument", "Accept-Datetime is not a date: "+v)
}

func newETag() string {
	b := make([]byte, 18)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// etagIn reports whether the If-Match or If-None-Match header names etag
// ("*" names any setting that exists).
func etagIn(header string, etag string, exists bool) bool {
	for _, e := range strings.Split(header, ",") {
		e = strings.Trim(strings.TrimPrefix(strings.TrimSpace(e), "W/"), `"`)
		if e == "*" && exists || e == etag && exists {
			return true
		}
	}
	return false
}

func skey(key, label string) string { return key + "\x00" + label }

// labelOf is the label of a /kv/{key} request: "\0" and nothing are the null label.
func labelOf(r *http.Request) string {
	l, _ := param(r, "label")
	if l == `\0` {
		return ""
	}
	return l
}

func (st *store) bump() { st.Seq++ }

func (st *setting) clone() setting {
	c := *st
	c.Tags = maps.Clone(st.Tags)
	return c
}

// view is a setting as the API shows it, limited to the fields $select names.
func (st *setting) view(sel map[string]bool) map[string]any {
	var label any
	if st.Label != "" {
		label = st.Label
	}
	tags := map[string]string{}
	for k, v := range st.Tags {
		tags[k] = v
	}
	m := map[string]any{"etag": st.ETag, "key": st.Key, "label": label, "content_type": st.ContentType, "value": st.Value,
		"tags": tags, "locked": st.Locked, "last_modified": st.Modified.UTC().Format(timeFormat)}
	if len(sel) > 0 {
		for k := range m {
			if !sel[k] {
				delete(m, k)
			}
		}
	}
	return m
}

func selectOf(r *http.Request) map[string]bool {
	v, _ := param(r, "$select")
	sel := map[string]bool{}
	for _, f := range strings.Split(v, ",") {
		if f = strings.TrimSpace(f); f != "" {
			sel[f] = true
		}
	}
	return sel
}

// ---- filters ----

// matcher compiles a key or label filter: comma-separated alternatives of
// literal text, with "*" for any characters and "\" escaping "*", "," and
// "\". For labels, "\0" alone is the null label. No filter matches all.
func matcher(f string) func(string) bool {
	if f == "" {
		return func(string) bool { return true }
	}
	var alts []string
	var raw, re strings.Builder
	flush := func() {
		if raw.String() != `\0` { // \0 is the null label: an empty alternative
			alts = append(alts, re.String())
		} else {
			alts = append(alts, "")
		}
		raw.Reset()
		re.Reset()
	}
	for i := 0; i < len(f); i++ {
		switch {
		case f[i] == ',':
			flush()
			continue
		case f[i] == '\\' && i+1 < len(f):
			raw.WriteByte(f[i])
			i++
			re.WriteString(regexp.QuoteMeta(string(f[i])))
		case f[i] == '*':
			re.WriteString(".*")
		default:
			re.WriteString(regexp.QuoteMeta(string(f[i])))
		}
		raw.WriteByte(f[i])
	}
	flush()
	return regexp.MustCompile("(?s)^(?:" + strings.Join(alts, "|") + ")$").MatchString
}

// ---- key-values ----

// settings returns the settings as of t (the current ones when !hist), sorted by key and label.
func (st *store) settings(t time.Time, hist bool) []setting {
	var out []setting
	if !hist {
		for _, v := range st.Cur {
			out = append(out, v.clone())
		}
	} else {
		last := map[string]rev{}
		for _, rv := range st.History {
			if !rv.Modified.After(t) {
				last[skey(rv.Key, rv.Label)] = rv
			}
		}
		for _, rv := range last {
			if !rv.Deleted {
				out = append(out, rv.clone())
			}
		}
	}
	slices.SortFunc(out, func(a, b setting) int { return strings.Compare(skey(a.Key, a.Label), skey(b.Key, b.Label)) })
	return out
}

// paged cuts one page from items and, when there is more, the nextLink
// that continues it: the request's own query with after set to the offset.
func paged[T any](r *http.Request, items []T, path string) ([]T, string) {
	start := 0
	if a, _ := param(r, "after"); strings.HasPrefix(a, "pogi:") {
		start, _ = strconv.Atoi(strings.TrimPrefix(a, "pogi:"))
	}
	start = min(max(start, 0), len(items))
	end := min(start+pageSize, len(items))
	if end == len(items) {
		return items[start:end], ""
	}
	q := url.Values{}
	for k, v := range r.URL.Query() {
		if !strings.EqualFold(k, "after") {
			q[k] = v
		}
	}
	q.Set("after", "pogi:"+strconv.Itoa(end))
	return items[start:end], path + "?" + q.Encode()
}

func (st *store) listKV(r *http.Request) (*reply, *apiError) {
	var items []setting
	if name, _ := param(r, "snapshot"); name != "" {
		sn := st.Snapshots[name]
		if sn == nil {
			return nil, notFound()
		}
		items = sn.Items
	} else {
		t, hist, err := at(r)
		if err != nil {
			return nil, err
		}
		items = st.settings(t, hist)
	}
	k, _ := param(r, "key")
	l, _ := param(r, "label")
	km, lm := matcher(k), matcher(l)
	var hit []setting
	for _, x := range items {
		if km(x.Key) && lm(x.Label) {
			hit = append(hit, x)
		}
	}
	return st.list(r, hit, "/kv")
}

// list answers a key-value list: the page, an ETag over its settings and
// 304 when If-None-Match has it.
func (st *store) list(r *http.Request, items []setting, path string) (*reply, *apiError) {
	page, next := paged(r, items, path)
	sel := selectOf(r)
	views := make([]map[string]any, 0, len(page))
	h := sha256.New()
	for _, x := range page {
		views = append(views, x.view(sel))
		h.Write([]byte(x.ETag))
	}
	etag := base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:18])
	body := map[string]any{"items": views}
	if next != "" {
		body["@nextLink"] = next
	}
	rep := ok(mediaKVSet, body)
	rep.header.Set("ETag", `"`+etag+`"`)
	if t, hist, _ := at(r); hist {
		rep.header.Set("Memento-Datetime", t.Format(http.TimeFormat))
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagIn(inm, etag, true) {
		return &reply{status: http.StatusNotModified, header: rep.header}, nil
	}
	if im := r.Header.Get("If-Match"); im != "" && !etagIn(im, etag, true) {
		return nil, errPrecondition
	}
	return rep, nil
}

func notFound() *apiError {
	return &apiError{status: http.StatusNotFound, typ: "not-found", title: "Not found"}
}

func (st *store) getKV(r *http.Request, key string) (*reply, *apiError) {
	label := labelOf(r)
	var cur *setting
	t, hist, err := at(r)
	if err != nil {
		return nil, err
	}
	if hist {
		for _, x := range st.settings(t, true) {
			if x.Key == key && x.Label == label {
				cur = &x
			}
		}
	} else {
		cur = st.Cur[skey(key, label)]
	}
	if cur == nil {
		return nil, notFound()
	}
	rep := ok(mediaKV, cur.view(selectOf(r)))
	rep.header.Set("ETag", `"`+cur.ETag+`"`)
	rep.header.Set("Last-Modified", cur.Modified.UTC().Format(http.TimeFormat))
	if hist {
		rep.header.Set("Memento-Datetime", t.Format(http.TimeFormat))
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagIn(inm, cur.ETag, true) {
		return &reply{status: http.StatusNotModified, header: rep.header}, nil
	}
	if im := r.Header.Get("If-Match"); im != "" && !etagIn(im, cur.ETag, true) {
		return nil, errPrecondition
	}
	return rep, nil
}

// conditions checks If-Match and If-None-Match of a write against cur (nil: none).
func conditions(r *http.Request, cur *setting) *apiError {
	etag := ""
	if cur != nil {
		etag = cur.ETag
	}
	if im := r.Header.Get("If-Match"); im != "" && !etagIn(im, etag, cur != nil) {
		return errPrecondition
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagIn(inm, etag, cur != nil) {
		return errPrecondition
	}
	return nil
}

func validKey(key string) *apiError {
	if key == "" || key == "." || key == ".." || len(key) > 10240 {
		return problem(http.StatusBadRequest, "invalid-argument", "Invalid argument", "The key is not valid.")
	}
	return nil
}

// write stores a new version of a setting. Lock held by the caller.
func (st *store) write(x *setting, deleted bool) {
	x.ETag, x.Modified = newETag(), time.Now().UTC()
	st.bump()
	st.History = append(st.History, rev{setting: x.clone(), Deleted: deleted})
}

func (st *store) putKV(r *http.Request, key string) (*reply, *apiError) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	var in struct {
		Value       *string            `json:"value"`
		ContentType *string            `json:"content_type"`
		Tags        map[string]*string `json:"tags"`
	}
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, &in) != nil {
		return nil, problem(http.StatusBadRequest, "invalid-argument", "Invalid argument", "The body is not a key-value.")
	}
	label := labelOf(r)
	cur := st.Cur[skey(key, label)]
	if err := conditions(r, cur); err != nil {
		return nil, err
	}
	if cur != nil && cur.Locked {
		return nil, errLocked(key)
	}
	x := &setting{Key: key, Label: label, Value: in.Value, ContentType: in.ContentType, Tags: map[string]string{}}
	for k, v := range in.Tags {
		if v != nil {
			x.Tags[k] = *v
		}
	}
	st.write(x, false)
	st.Cur[skey(key, label)] = x
	return st.one(x), nil
}

func (st *store) one(x *setting) *reply {
	rep := ok(mediaKV, x.view(nil))
	rep.header.Set("ETag", `"`+x.ETag+`"`)
	rep.header.Set("Last-Modified", x.Modified.UTC().Format(http.TimeFormat))
	return rep
}

func (st *store) deleteKV(r *http.Request, key string) (*reply, *apiError) {
	label := labelOf(r)
	cur := st.Cur[skey(key, label)]
	if cur == nil {
		if r.Header.Get("If-Match") != "" {
			return nil, errPrecondition
		}
		return &reply{status: http.StatusNoContent, header: http.Header{}}, nil
	}
	if err := conditions(r, cur); err != nil {
		return nil, err
	}
	if cur.Locked {
		return nil, errLocked(key)
	}
	gone := cur.clone()
	delete(st.Cur, skey(key, label))
	del := gone.clone()
	st.write(&del, true)
	return st.one(&gone), nil
}

func (st *store) lock(r *http.Request, key string, lock bool) (*reply, *apiError) {
	cur := st.Cur[skey(key, labelOf(r))]
	if cur == nil {
		return nil, notFound()
	}
	if err := conditions(r, cur); err != nil {
		return nil, err
	}
	cur.Locked = lock
	st.write(cur, false)
	return st.one(cur), nil
}

// ---- keys, labels, revisions ----

func (st *store) listNames(r *http.Request, media string, labels bool) (*reply, *apiError) {
	t, hist, err := at(r)
	if err != nil {
		return nil, err
	}
	f, _ := param(r, "name")
	m := matcher(f)
	seen := map[string]bool{}
	var names []map[string]string
	for _, x := range st.settings(t, hist) {
		n := x.Key
		if labels {
			n = x.Label
		}
		if (!labels || n != "") && !seen[n] && m(n) {
			seen[n] = true
			names = append(names, map[string]string{"name": n})
		}
	}
	if labels {
		slices.SortFunc(names, func(a, b map[string]string) int { return strings.Compare(a["name"], b["name"]) })
	}
	page, next := paged(r, names, map[bool]string{false: "/keys", true: "/labels"}[labels])
	body := map[string]any{"items": page}
	if page == nil {
		body["items"] = []map[string]string{}
	}
	if next != "" {
		body["@nextLink"] = next
	}
	return ok(media, body), nil
}

// revisions lists every version of the matching settings, oldest first per setting.
// ponytail: real App Configuration may order differently; sorted by key, label, then time
func (st *store) revisions(r *http.Request) (*reply, *apiError) {
	until, hist, err := at(r)
	if err != nil {
		return nil, err
	}
	k, _ := param(r, "key")
	l, _ := param(r, "label")
	km, lm := matcher(k), matcher(l)
	var hit []setting
	for _, rv := range st.History {
		if !rv.Deleted && km(rv.Key) && lm(rv.Label) && (!hist || !rv.Modified.After(until)) {
			hit = append(hit, rv.clone())
		}
	}
	slices.SortStableFunc(hit, func(a, b setting) int { return strings.Compare(skey(a.Key, a.Label), skey(b.Key, b.Label)) })
	rep, err := st.list(r, hit, "/revisions")
	return rep, err
}

// ---- snapshots ----

func (sn *snapshot) view(sel map[string]bool) map[string]any {
	var size int64
	for _, x := range sn.Items {
		size += int64(len(x.Key) + len(x.Label) + len(deref(x.Value)) + len(deref(x.ContentType)))
	}
	filters := []map[string]any{}
	for _, f := range sn.Filters {
		filters = append(filters, map[string]any{"key": f.Key, "label": f.Label})
	}
	var exp any
	if sn.Expires != nil {
		exp = sn.Expires.UTC().Format(timeFormat)
	}
	tags := sn.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	m := map[string]any{"etag": sn.ETag, "name": sn.Name, "status": sn.Status, "filters": filters, "composition_type": sn.Composition,
		"created": sn.Created.UTC().Format(timeFormat), "expires": exp, "size": size, "items_count": len(sn.Items),
		"tags": tags, "retention_period": sn.Retention}
	if len(sel) > 0 {
		for k := range m {
			if !sel[k] {
				delete(m, k)
			}
		}
	}
	return m
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (st *store) snapshotReply(sn *snapshot, r *http.Request, status int) *reply {
	rep := ok(mediaSnapshot, sn.view(selectOf(r)))
	rep.status = status
	rep.header.Set("ETag", `"`+sn.ETag+`"`)
	return rep
}

func (st *store) getSnapshot(r *http.Request, name string) (*reply, *apiError) {
	sn := st.Snapshots[name]
	if sn == nil {
		return nil, notFound()
	}
	rep := st.snapshotReply(sn, r, http.StatusOK)
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagIn(inm, sn.ETag, true) {
		return &reply{status: http.StatusNotModified, header: rep.header}, nil
	}
	if im := r.Header.Get("If-Match"); im != "" && !etagIn(im, sn.ETag, true) {
		return nil, errPrecondition
	}
	return rep, nil
}

func (st *store) listSnapshots(r *http.Request) (*reply, *apiError) {
	f, _ := param(r, "name")
	m := matcher(f)
	status, _ := param(r, "status")
	var all []*snapshot
	for _, n := range core.SortedKeys(st.Snapshots) {
		if sn := st.Snapshots[n]; m(n) && (status == "" || slices.Contains(strings.Split(status, ","), sn.Status)) {
			all = append(all, sn)
		}
	}
	page, next := paged(r, all, "/snapshots")
	sel := selectOf(r)
	views := make([]map[string]any, 0, len(page))
	for _, sn := range page {
		views = append(views, sn.view(sel))
	}
	body := map[string]any{"items": views}
	if next != "" {
		body["@nextLink"] = next
	}
	return ok(mediaSnaps, body), nil
}

func (st *store) createSnapshot(r *http.Request, name string) (*reply, *apiError) {
	var in struct {
		Filters     []filter           `json:"filters"`
		Composition string             `json:"composition_type"`
		Retention   int64              `json:"retention_period"`
		Tags        map[string]*string `json:"tags"`
	}
	if json.Unmarshal(core.Body(r), &in) != nil || len(in.Filters) == 0 || len(in.Filters) > 3 {
		return nil, problem(http.StatusBadRequest, "invalid-argument", "Invalid argument", "A snapshot needs one to three filters.")
	}
	if in.Composition == "" {
		in.Composition = "key"
	}
	if in.Composition != "key" && in.Composition != "key_label" {
		return nil, problem(http.StatusBadRequest, "invalid-argument", "Invalid argument", "composition_type is key or key_label.")
	}
	if st.Snapshots[name] != nil {
		return nil, problem(http.StatusConflict, "already-exists", "Already exists", "The snapshot '"+name+"' already exists.")
	}
	if in.Retention == 0 {
		in.Retention = 30 * 24 * 3600
	}
	sn := &snapshot{Name: name, Status: "ready", Filters: in.Filters, Composition: in.Composition, Retention: in.Retention,
		Created: time.Now().UTC(), ETag: newETag(), Tags: map[string]string{}}
	for k, v := range in.Tags {
		if v != nil {
			sn.Tags[k] = *v
		}
	}
	seen := map[string]bool{}
	for _, f := range in.Filters {
		km, lm := matcher(deref(f.Key)), matcher(deref(f.Label))
		for _, x := range st.settings(time.Time{}, false) {
			id := skey(x.Key, x.Label)
			if in.Composition == "key" {
				id = x.Key
			}
			if !km(x.Key) || !lm(x.Label) {
				continue
			}
			if seen[id] {
				sn.Status, sn.Error = "failed", "The filters match key '"+x.Key+"' more than once: use composition_type key_label."
				continue
			}
			seen[id] = true
			sn.Items = append(sn.Items, x)
		}
	}
	slices.SortFunc(sn.Items, func(a, b setting) int { return strings.Compare(skey(a.Key, a.Label), skey(b.Key, b.Label)) })
	st.Snapshots[name] = sn
	st.bump()
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	base := scheme + "://" + r.Host
	if strings.HasPrefix(r.URL.Path, pathPrefix) {
		base += pathPrefix + st.Name
	}
	rep := st.snapshotReply(sn, r, http.StatusCreated)
	rep.body.(map[string]any)["status"] = "provisioning" // the operation below is already done
	rep.header.Set("Operation-Location", base+"/operations?snapshot="+url.QueryEscape(name)+"&api-version=2023-10-01")
	return rep, nil
}

func (st *store) updateSnapshot(r *http.Request, name string) (*reply, *apiError) {
	sn := st.Snapshots[name]
	if sn == nil {
		return nil, notFound()
	}
	var in struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(core.Body(r), &in) != nil || (in.Status != "archived" && in.Status != "ready") {
		return nil, problem(http.StatusBadRequest, "invalid-argument", "Invalid argument", "status is archived or ready.")
	}
	if im := r.Header.Get("If-Match"); im != "" && !etagIn(im, sn.ETag, true) {
		return nil, errPrecondition
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagIn(inm, sn.ETag, true) {
		return nil, errPrecondition
	}
	switch {
	case in.Status == "archived" && sn.Status == "ready":
		exp := time.Now().UTC().Add(time.Duration(sn.Retention) * time.Second)
		sn.Status, sn.Expires = "archived", &exp
	case in.Status == "ready" && sn.Status == "archived":
		sn.Status, sn.Expires = "ready", nil
	default:
		return nil, problem(http.StatusConflict, "invalid-state", "Invalid state", "The snapshot is "+sn.Status+" and cannot become "+in.Status+".")
	}
	sn.ETag = newETag()
	st.bump()
	return st.snapshotReply(sn, r, http.StatusOK), nil
}

func (st *store) operation(r *http.Request) (*reply, *apiError) {
	name, _ := param(r, "snapshot")
	sn := st.Snapshots[name]
	if sn == nil {
		return nil, notFound()
	}
	body := map[string]any{"id": name, "status": "Succeeded"}
	if sn.Status == "failed" {
		body["status"], body["error"] = "Failed", map[string]any{"code": "InvalidArgument", "message": sn.Error}
	}
	return ok("application/json", body), nil
}

// ---- stores (for the resource manager, seeds and tests) ----

// create makes a store, or returns the one that exists. Lock held.
func (s *Service) create(name string) *store {
	if st := s.st.Stores[name]; st != nil {
		return st
	}
	st := newStore(name)
	s.st.Stores[name] = st
	return st
}

// CreateStore creates a store with fresh access keys; one that exists is left as it is.
func (s *Service) CreateStore(name string) error {
	if !storeName.MatchString(name) {
		return errors.New("a store name is 1-50 letters, digits and hyphens")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.create(name)
	return nil
}

// DeleteStore removes a store and everything in it; it reports whether there was one.
func (s *Service) DeleteStore(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, had := s.st.Stores[name]
	delete(s.st.Stores, name)
	return had
}

// Stores lists the store names.
func (s *Service) Stores() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return core.SortedKeys(s.st.Stores)
}

// AccessKeys returns a store's access keys: two read-write and two read-only.
func (s *Service) AccessKeys(name string) ([]AccessKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.st.Stores[name]
	if st == nil {
		return nil, fmt.Errorf("App Configuration store %q does not exist", name)
	}
	return slices.Clone(st.Keys), nil
}

// Endpoint is where clients reach a store: the path form of pogi's address,
// which needs no name resolution. <store>.azconfig.localhost:<port> reaches
// it too wherever *.localhost resolves.
func (s *Service) Endpoint(name string) string {
	return strings.TrimSuffix(s.hub.URL, "/") + pathPrefix + name
}

// ConnectionString is Endpoint=...;Id=...;Secret=... with a store's first
// read-write key.
func (s *Service) ConnectionString(name string) (string, error) {
	keys, err := s.AccessKeys(name)
	if err != nil {
		return "", err
	}
	return "Endpoint=" + s.Endpoint(name) + ";Id=" + keys[0].ID + ";Secret=" + keys[0].Secret, nil
}

// SetKeyValue writes a key-value (the seed hook); an existing one is left alone.
func (s *Service) SetKeyValue(storeName, key, label string, value, contentType *string, tags map[string]string) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.create(storeName)
	if st.Cur[skey(key, label)] != nil {
		return nil
	}
	x := &setting{Key: key, Label: label, Value: value, ContentType: contentType, Tags: tags}
	st.write(x, false)
	st.Cur[skey(key, label)] = x
	return nil
}

// ---- persistence and console ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Stores == nil {
		st.Stores = map[string]*store{}
	}
	for _, x := range st.Stores {
		if x.Cur == nil {
			x.Cur = map[string]*setting{}
		}
		if x.Snapshots == nil {
			x.Snapshots = map[string]*snapshot{}
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Resources lists stores and their key-values for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	stores := core.Table{Name: "Stores", Columns: []string{"Name", "Key-values", "Snapshots", "Revisions", "Created"}}
	kvs := core.Table{Name: "Key-values", Columns: []string{"Setting", "Store", "Key", "Label", "Content type", "Value", "Locked", "Last modified"}}
	for _, n := range core.SortedKeys(s.st.Stores) {
		st := s.st.Stores[n]
		stores.Rows = append(stores.Rows, []string{n, strconv.Itoa(len(st.Cur)), strconv.Itoa(len(st.Snapshots)), strconv.Itoa(len(st.History)), st.Created.Format(time.DateTime)})
		for _, x := range st.settings(time.Time{}, false) {
			kvs.Rows = append(kvs.Rows, []string{settingID(n, x), n, x.Key, x.Label, deref(x.ContentType), deref(x.Value), strconv.FormatBool(x.Locked), x.Modified.Format(time.DateTime)})
		}
	}
	return []core.Table{stores, kvs}
}

func settingID(store string, x setting) string {
	id := store + "/" + x.Key
	if x.Label != "" {
		id += "?label=" + x.Label
	}
	return id
}

// Inspect returns a store with its access keys and connection string, or a
// key-value with its revisions.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if table == "Stores" {
		st := s.st.Stores[id]
		if st == nil {
			return nil
		}
		cs := "Endpoint=" + s.Endpoint(id) + ";Id=" + st.Keys[0].ID + ";Secret=" + st.Keys[0].Secret
		return map[string]any{"name": id, "created": st.Created, "endpoint": s.Endpoint(id), "connectionString": cs, "accessKeys": st.Keys,
			"snapshots": core.SortedKeys(st.Snapshots), "revisions": len(st.History)}
	}
	name, rest, _ := strings.Cut(id, "/")
	key, label, _ := strings.Cut(rest, "?label=")
	st := s.st.Stores[name]
	if st == nil || st.Cur[skey(key, label)] == nil {
		return nil
	}
	var revs []map[string]any
	for _, rv := range st.History {
		if rv.Key == key && rv.Label == label {
			v := rv.view(nil)
			v["deleted"] = rv.Deleted
			revs = append(revs, v)
		}
	}
	return map[string]any{"keyValue": st.Cur[skey(key, label)].view(nil), "revisions": revs}
}
