// Package generic serves Google Cloud REST APIs from their discovery documents, the way
// internal/aws/generic serves AWS services from their models. apis.txt lists the APIs; each one
// is a service that stores the resources its create, patch and delete methods make and reads them
// back with get and list. Nothing runs behind a resource: this is the Thin depth.
//
// What it does for every listed API:
//   - create (POST on a collection: the id is the <x>Id query parameter, or the body's name for
//     the older style), get, list (pageSize or maxResults, pageToken, filter), patch with
//     updateMask, and delete;
//   - a method that returns an Operation answers with a finished one that the shared operations
//     service serves at projects/p/locations/l/operations/<id>;
//   - the output-only fields the document names: name, createTime, updateTime, a state at its ready
//     value, etag, uid, selfLink, kind and a numeric id.
//
// A method with a custom verb (:restore) answers UNIMPLEMENTED, naming it, unless the API's file
// (dns.go) overrides it. Fields are not checked against the schema; a client may send what a newer
// API has.
package generic

import (
	"bytes"
	"cmp"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
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

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Def is one generated service, for the registry.
type Def struct {
	Name string
	New  func(*core.Hub) core.Service
}

func init() {
	for _, e := range entries() {
		core.RegisterGoogleAPI(e.svc, e.label)
	}
}

// Services lists the services of apis.txt.
func Services() []Def {
	var out []Def
	for _, e := range entries() {
		out = append(out, Def{e.svc, func(h *core.Hub) core.Service { return newAPI(h, e) }})
	}
	return out
}

// Endpoints maps the Terraform google provider's custom endpoint argument of each listed API to its
// base path on pogi, for "pogi tf --cloud gcp".
func Endpoints() map[string]string {
	out := map[string]string{}
	for _, e := range entries() {
		for _, tf := range strings.Split(e.tf, ",") { // an API can have several arguments: container_aws,container_azure
			out[tf] = "/" + e.label + "/" + e.version + "/"
		}
	}
	return out
}

// Hook methods: a method's whole behavior, by its discovery id ("dns.changes.create").
type hook func(c *call) (any, error)

// hooks are one API's overrides of the engine, where the generic behavior is not enough.
type hooks struct {
	methods  map[string]hook
	required map[string][]string                                // schema name -> fields a create must set
	created  map[string]func(c *call, obj map[string]any)       // fills computed fields of a new resource, by schema name
	check    map[string]func(c *call, obj map[string]any) error // refuses a create or an update, by schema name
	newID    map[string]func() string                           // schemas whose id the server makes, not the client
	ownName  map[string]bool                                    // schemas whose "name" is a label the client sets, not the resource path
	ownOrg   bool                                               // organizations/<id> is a resource of this API (Apigee), so a missing one is a 404
	noEtag   []string                                           // schemas that have an etag field but get none (Terraform would see drift)
	implicit func(name string) bool                             // parents that exist without a create call of their own (Chronicle's instance, IAP's tunnel location)
}

// method is a discovery method ready to match requests.
type method struct {
	id, httpMethod, verb string
	segs                 []seg
	lits                 int
	query                map[string]bool
	req, resp            string // schema names
	aip                  bool   // names come from the path ({+name}), not from the body
}

type seg struct{ lit, param string } // a literal or a {parameter}

type api struct {
	e     *entry
	hub   *core.Hub
	ops   *gapi.Ops
	hooks hooks

	compiled sync.Once // meth and base are built on the API's first request, not at start
	meth     []*method
	base     string // https://<host>/<prefix>/<version>/, for selfLink

	mu   sync.Mutex
	objs map[string]map[string]any // the path after the version -> resource
	seq  int                       // ids of changes and operations of the older style
}

func newAPI(h *core.Hub, e *entry) *api {
	a := &api{e: e, hub: h, ops: gapi.Operations(h)}
	a.hooks = apiHooks[e.name]
	a.Reset()
	return a
}

// build compiles the API's methods from its document, once. Every request goes through find, which
// calls it, so pogi starts without decoding any document.
func (a *api) build() {
	a.compiled.Do(func() {
		e, d := a.e, a.e.doc()
		a.base = d.RootURL + e.version + "/"
		d.walk(func(dm *dmethod) {
			if first, _, _ := strings.Cut(cmp.Or(dm.FlatPath, dm.Path), "/"); !gapiVersion(first) {
				a.base = d.RootURL + first + "/" + e.version + "/" // dns.googleapis.com/dns/v1/
			}
			if m := compile(e, dm); m != nil {
				a.meth = append(a.meth, m)
			}
		})
		slices.SortStableFunc(a.meth, func(x, y *method) int { return y.lits - x.lits }) // the most specific path first
	})
}

// compile turns a discovery method into a matcher. Its path loses the API's own prefix ("dns/"),
// which core takes off every request, so what is left starts with the version.
func compile(e *entry, dm *dmethod) *method {
	p := cmp.Or(dm.FlatPath, dm.Path)
	if strings.Contains(p, "{+") || strings.Contains(dm.ID, ".operations.") {
		return nil // operations are the shared service's
	}
	parts := strings.Split(p, "/")
	if !gapiVersion(parts[0]) && len(parts) > 1 {
		parts = parts[1:]
	}
	m := &method{id: dm.ID, httpMethod: dm.HTTPMethod, query: map[string]bool{}}
	if last := parts[len(parts)-1]; strings.Contains(last, ":") {
		i := strings.LastIndex(last, ":")
		parts[len(parts)-1], m.verb = last[:i], last[i+1:]
	}
	for _, s := range parts {
		if strings.HasPrefix(s, "{") {
			m.segs = append(m.segs, seg{param: strings.Trim(s, "{}")})
		} else {
			m.segs = append(m.segs, seg{lit: s})
			m.lits++
		}
	}
	for n, pr := range dm.Parameters {
		m.query[n] = pr.Location == "query"
	}
	if dm.Request != nil {
		m.req = dm.Request.Ref
	}
	if dm.Response != nil {
		m.resp = dm.Response.Ref
	}
	m.aip = strings.Contains(dm.Path, "{+")
	return m
}

func gapiVersion(s string) bool { return core.APIVersionPath("/" + s) }

func (a *api) Info() core.Info {
	return core.Info{Name: a.e.svc, Cloud: core.GCP, Title: a.e.title, Match: func(r *http.Request) bool {
		m, _ := a.find(r)
		return m != nil
	}}
}

// find returns the method a request is for and the path after the version, without a custom verb.
func (a *api) find(r *http.Request) (*method, []string) {
	segs := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(segs) < 2 || segs[0] != a.e.version {
		return nil, nil
	}
	a.build()
	segs = segs[1:]
	verb := ""
	if last := segs[len(segs)-1]; strings.Contains(last, ":") {
		i := strings.LastIndex(last, ":")
		segs[len(segs)-1], verb = last[:i], last[i+1:]
	}
	for _, m := range a.meth {
		if m.httpMethod != r.Method || m.verb != verb || len(m.segs) != len(segs)+1 {
			continue
		}
		ok := true
		for i, s := range m.segs[1:] {
			if segs[i] == "" || s.lit != "" && s.lit != segs[i] {
				ok = false
				break
			}
		}
		if ok {
			return m, segs
		}
	}
	return nil, nil
}

// call is one request.
type call struct {
	a    *api
	m    *method
	r    *http.Request
	segs []string // the path after the version
	q    url.Values
	body map[string]any
}

// param reads a query parameter by its documented name, or in snake case: Google's JSON mapping
// takes both, and some Terraform resources send "data_exchange_id".
func (c *call) param(name string) string {
	if v := c.q.Get(name); v != "" {
		return v
	}
	var b strings.Builder
	for _, r := range name {
		if r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}
	return c.q.Get(b.String())
}

// camelQuery adds the camelCase name of every snake_case query parameter: the real APIs take both
// (the Terraform provider sends instance_id, the discovery document says instanceId).
func camelQuery(q url.Values) url.Values {
	for k, v := range q {
		if !strings.Contains(k, "_") {
			continue
		}
		parts := strings.Split(k, "_")
		for i := 1; i < len(parts); i++ {
			if parts[i] != "" {
				parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
			}
		}
		if c := strings.Join(parts, ""); q[c] == nil {
			q[c] = v
		}
	}
	return q
}

func (c *call) name() string { return strings.Join(c.segs, "/") }

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := core.C(r)
	m, segs := a.find(r)
	if m == nil {
		gapi.Err(w, ctx, status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	ctx.Op = m.id
	if err := ctx.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, ctx, e.Status, e.Code, e.Msg)
		return
	}
	c := &call{a: a, m: m, r: r, segs: segs, q: camelQuery(r.URL.Query())}
	if m.req != "" {
		var err error
		if c.body, err = gapi.ParseBody(core.Body(r), ""); err != nil {
			gapi.Err(w, ctx, err)
			return
		}
	}
	a.mu.Lock()
	out, err := a.dispatch(c)
	a.mu.Unlock()
	if err != nil {
		gapi.Err(w, ctx, err)
		return
	}
	gapi.JSON(w, out)
}

func (a *api) dispatch(c *call) (any, error) {
	if h := a.hooks.methods[c.m.id]; h != nil {
		return h(c)
	}
	odd := c.m.segs[len(c.m.segs)-1].lit != "" // a collection: the path ends in a name, not in an id (iap_tunnel/.../destGroups has an even count)
	switch {
	case c.m.verb != "":
	case c.m.httpMethod == "POST" && odd:
		return c.create()
	case c.m.httpMethod == "GET" && odd:
		return c.list()
	case c.m.httpMethod == "GET":
		return c.get()
	case (c.m.httpMethod == "PATCH" || c.m.httpMethod == "PUT") && !odd:
		return c.update()
	case c.m.httpMethod == "DELETE" && !odd:
		return c.delete()
	}
	return nil, status.Errorf(codes.Unimplemented, "pogi does not implement %s (%s %s)", c.m.id, c.r.Method, c.r.URL.Path)
}

// ---- schemas ----

func (a *api) schema(n string) *schema {
	if s := a.e.doc().Schemas[n]; s != nil {
		return s
	}
	return &schema{}
}

// lro says whether a schema is a long-running operation: name and done.
func (a *api) lro(n string) bool {
	p := a.schema(n).Properties
	return p["done"] != nil && p["name"] != nil
}

// ---- helpers ----

var idRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

func now() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") }

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	return decode(b)
}

func decode(b []byte) map[string]any {
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	d.Decode(&m)
	return m
}

// lookup reads a dotted path of nested objects.
func lookup(m map[string]any, path string) (any, bool) {
	for {
		k, rest, more := strings.Cut(path, ".")
		v, ok := m[k]
		if !more || !ok {
			return v, ok
		}
		if m, ok = v.(map[string]any); !ok {
			return nil, false
		}
		path = rest
	}
}

// store sets a dotted path, making the objects on the way.
func store(m map[string]any, path string, v any) {
	k, rest, more := strings.Cut(path, ".")
	if !more {
		m[k] = v
		return
	}
	child, _ := m[k].(map[string]any)
	if child == nil {
		child = map[string]any{}
		m[k] = child
	}
	store(child, rest, v)
}

func (a *api) etag(obj map[string]any) string {
	e := obj["etag"]
	delete(obj, "etag")
	b, _ := json.Marshal(obj)
	if e != nil {
		obj["etag"] = e
	}
	h := sha1.Sum(b)
	return hex.EncodeToString(h[:8])
}

func (a *api) numericID() string {
	n, _ := strconv.ParseUint(core.RandHex(8)[:15], 16, 64)
	return strconv.FormatUint(n, 10)
}

// fill sets the output-only fields the schema has. A field the client sent stays.
func (c *call) fill(obj map[string]any, schemaName, name string) {
	a, props := c.a, c.a.schema(schemaName).Properties
	set := func(k string, v any) {
		if _, ok := obj[k]; ok || props[k] == nil {
			return
		}
		obj[k] = v
	}
	if !a.hooks.ownName[schemaName] && (c.m.aip || a.hooks.newID[schemaName] != nil && props["name"] != nil) { // a server-made id: the older style APIs name it too
		obj["name"] = name
	}
	if p := props["kind"]; p != nil && p.Default != nil {
		obj["kind"] = p.Default
	}
	if p := props["id"]; p != nil && p.Type == "string" && (p.Format == "uint64" || p.Format == "int64") {
		set("id", a.numericID())
	}
	set("createTime", now())
	set("updateTime", now())
	set("uid", core.UUID())
	set("uuid", core.UUID())     // Composer
	set("uniqueId", core.UUID()) // Network Connectivity
	set("selfLink", a.base+name)
	if p := props["state"]; p != nil {
		for _, ready := range []string{"READY", "ACTIVE", "SUCCEEDED", "RUNNING"} {
			if slices.Contains(p.Enum, ready) {
				set("state", ready)
				break
			}
		}
	}
	if props["etag"] != nil && !slices.Contains(a.hooks.noEtag, schemaName) {
		obj["etag"] = a.etag(obj)
	}
}

// writable drops the output-only fields a client sent; the engine sets them.
func (a *api) writable(schemaName string, body map[string]any) map[string]any {
	out := map[string]any{}
	props := a.schema(schemaName).Properties
	for k, v := range body {
		if p := props[k]; p == nil || !p.ReadOnly {
			out[k] = v
		}
	}
	return out
}

// checkParent refuses a collection whose parent resource does not exist. The project and the
// location are never checked.
func (c *call) checkParent(parent []string) error {
	n := 0
	if len(parent) >= 2 && (parent[0] == "projects" || parent[0] == "folders" || parent[0] == "organizations" || parent[0] == "billingAccounts") {
		n = 2
		if parent[0] == "organizations" && c.a.hooks.ownOrg {
			n = 0
		}
		if len(parent) >= 4 && (parent[2] == "locations" || parent[0] == "projects" && parent[2] == "regions") { // any root with locations; regions: Dataproc's older style
			n = 4
		}
	}
	if len(parent) > n && c.a.objs[strings.Join(parent, "/")] == nil && (c.a.hooks.implicit == nil || !c.a.hooks.implicit(strings.Join(parent, "/"))) {
		return gapi.NotFound("The resource '%s' was not found", strings.Join(parent, "/"))
	}
	return nil
}

// ---- the five methods ----

// idField is the body field that names a resource of the older style: name, or <thing>Name.
func (a *api) idField(schemaName string) string {
	props := a.schema(schemaName).Properties
	if props["name"] != nil {
		return "name"
	}
	for _, k := range slices.Sorted(maps.Keys(props)) {
		if strings.HasSuffix(k, "Name") && k != "dnsName" {
			return k
		}
	}
	return "name"
}

func (c *call) create() (any, error) {
	a, n := c.a, len(c.segs)
	parent := c.segs[:n-1]
	if err := c.checkParent(parent); err != nil {
		return nil, err
	}
	obj := clone(a.writable(c.m.req, c.body))
	id := ""
	gen := a.hooks.newID[c.m.req]
	param := "" // the query parameter that names the new resource, when the method has one
	if c.m.aip {
		for k, isQuery := range c.m.query {
			if isQuery && strings.HasSuffix(k, "Id") && k != "clientOperationId" && k != "requestId" && k != "projectId" {
				param = k
			}
		}
	}
	switch {
	case param != "":
		if id = c.param(param); id == "" && gen == nil {
			return nil, gapi.Invalid("The query parameter %s is required.", param)
		}
	case gen == nil:
		field := a.idField(c.m.req)
		id, _ = c.body[field].(string) // the body, not obj: an output-only name (Sensitive Data Protection) is dropped from obj
		if id == "" {
			return nil, gapi.Invalid("Missing required field %q in %s.", field, c.m.req)
		}
		if strings.Contains(id, "/") { // the body may hold the whole name, but only under this collection
			if id = strings.TrimPrefix(id, c.name()+"/"); strings.Contains(id, "/") {
				return nil, gapi.Invalid("Invalid id: field %q is %q, which is not under %s.", field, c.body[field], c.name())
			}
		}
	}
	if id == "" {
		id = gen()
	}
	if !idRE.MatchString(id) {
		return nil, gapi.Invalid("Invalid id %q: 1 to 63 letters, digits, '-', '_' and '.', starting with a letter or digit", id)
	}
	for _, f := range a.hooks.required[c.m.req] {
		if v, ok := obj[f]; !ok || v == nil || v == "" {
			return nil, gapi.Invalid("Missing required field %q in %s.", f, c.m.req)
		}
	}
	if gen != nil && a.schema(c.m.req).Properties["id"] != nil {
		obj["id"] = id
	}
	return c.put(obj, c.name()+"/"+id)
}

// put stores a new resource under name: the end of a create, for a hook that works out the name itself.
func (c *call) put(obj map[string]any, name string) (any, error) {
	a := c.a
	if a.objs[name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists", name)
	}
	c.fill(obj, c.m.req, name)
	if f := a.hooks.check[c.m.req]; f != nil {
		if err := f(c, obj); err != nil {
			return nil, err
		}
	}
	if f := a.hooks.created[c.m.req]; f != nil {
		f(c, obj)
	}
	a.objs[name] = obj
	return c.answer(obj, c.m.req, "create", name)
}

// answer shapes a resource into the method's response: a finished operation, the resource, or
// the object that wraps it.
func (c *call) answer(obj map[string]any, schemaName, verb, name string) (any, error) {
	a, resp := c.a, c.m.resp
	switch {
	case a.lro(resp):
		return c.lro(clone(obj), schemaName, verb, name), nil
	case resp == schemaName || resp == "":
		return clone(obj), nil
	}
	for k, p := range a.schema(resp).Properties {
		if p.Ref == schemaName {
			return map[string]any{k: clone(obj)}, nil
		}
	}
	return clone(obj), nil
}

// lro records a finished operation for the resource, at projects/p/locations/l/operations/<id>.
func (c *call) lro(res map[string]any, schemaName, verb, name string) map[string]any {
	parent := ""
	switch s := c.segs; {
	case strings.HasPrefix(name, "apps/"): // App Engine's: apps/a/operations/o
		parent = strings.Join(strings.SplitN(name, "/", 3)[:2], "/")
	case len(s) >= 4 && s[0] == "projects" && (s[2] == "locations" || s[2] == "regions"):
		parent = strings.Join(s[:4], "/")
	case len(s) >= 2 && (s[0] == "projects" || s[0] == "organizations"): // organizations: Apigee
		parent = strings.Join(s[:2], "/")
	}
	pkg := "type.googleapis.com/google.cloud." + c.a.e.label + "." + c.a.e.version + "."
	meta := map[string]any{"@type": pkg + "OperationMetadata", "target": name, "verb": verb,
		"createTime": now(), "endTime": now(), "apiVersion": c.a.e.version}
	if verb == "delete" {
		res = map[string]any{"@type": "type.googleapis.com/google.protobuf.Empty"}
	} else {
		res["@type"] = pkg + schemaName
	}
	return c.a.ops.DoneJSON(parent, meta, res)
}

func (c *call) notFound() error {
	return gapi.NotFound("The resource '%s' was not found", c.name())
}

func (c *call) get() (any, error) {
	obj := c.a.objs[c.name()]
	if obj == nil {
		return nil, c.notFound()
	}
	return c.answer(obj, c.m.resp, "get", c.name())
}

func (c *call) delete() (any, error) {
	a, name := c.a, c.name()
	if a.objs[name] == nil {
		return nil, c.notFound()
	}
	delete(a.objs, name)
	for k := range a.objs {
		if strings.HasPrefix(k, name+"/") {
			delete(a.objs, k)
		}
	}
	if a.lro(c.m.resp) {
		return c.lro(nil, "", "delete", name), nil
	}
	return map[string]any{}, nil
}

// update changes the fields the updateMask names, or the fields sent when it is empty.
func (c *call) update() (any, error) {
	obj, err := c.apply()
	if err != nil {
		return nil, err
	}
	return c.answer(obj, c.m.req, "update", c.name())
}

func (c *call) apply() (map[string]any, error) {
	a, name := c.a, c.name()
	obj := a.objs[name]
	if obj == nil {
		return nil, c.notFound()
	}
	props := a.schema(c.m.req).Properties
	body := c.body
	var paths []string
	if mask := c.param("updateMask"); mask != "" && mask != "*" {
		paths = strings.Split(mask, ",")
		for _, p := range paths {
			if props[strings.Split(p, ".")[0]] == nil {
				return nil, gapi.Invalid("Invalid update mask: %q is not a field of %s.", p, c.m.req)
			}
		}
	} else {
		for k := range body {
			paths = append(paths, k)
		}
	}
	next := clone(obj)
	for _, p := range paths {
		if first := props[strings.Split(p, ".")[0]]; first != nil && first.ReadOnly || p == "name" && !a.hooks.ownName[c.m.req] {
			continue
		}
		if v, ok := lookup(body, p); ok {
			store(next, p, v)
		} else if !strings.Contains(p, ".") {
			delete(next, p)
		}
	}
	if f := a.hooks.check[c.m.req]; f != nil {
		if err := f(c, next); err != nil {
			return nil, err
		}
	}
	if props["updateTime"] != nil {
		next["updateTime"] = now()
	}
	if props["etag"] != nil && !slices.Contains(a.hooks.noEtag, c.m.req) {
		next["etag"] = a.etag(next)
	}
	a.objs[name] = next
	return next, nil
}

// list answers a collection: its resources sorted by name, one page.
func (c *call) list() (any, error) {
	a, n := c.a, len(c.segs)
	if err := c.checkParent(c.segs[:n-1]); err != nil {
		return nil, err
	}
	var f *gapi.Filter
	if c.m.query["filter"] {
		var err error
		if f, err = gapi.ParseFilter(c.q.Get("filter")); err != nil {
			return nil, gapi.Invalid("%v", err)
		}
	}
	prefix := c.name() + "/"
	var items []map[string]any
	for _, k := range slices.Sorted(maps.Keys(a.objs)) {
		if rest, ok := strings.CutPrefix(k, prefix); ok && !strings.Contains(rest, "/") && matches(f, a.objs[k]) {
			items = append(items, a.objs[k])
		}
	}
	return c.page(items)
}

// matches evaluates a filter against a resource: field = value, field != value and field:value,
// on dotted paths (labels.env), and a bare word anywhere in the resource.
func matches(f *gapi.Filter, obj map[string]any) bool {
	if f == nil {
		return true
	}
	text := func(s string) bool {
		b, _ := json.Marshal(obj)
		return strings.Contains(strings.ToLower(string(b)), strings.ToLower(s))
	}
	return f.Match(func(field, op, value string) bool {
		v, ok := lookup(obj, field)
		got := fmt.Sprint(v)
		switch op {
		case "=":
			return ok && got == value
		case "!=":
			return !ok || got != value
		case ":":
			return ok && (got == value || strings.Contains(strings.ToLower(got), strings.ToLower(value)) || value == "*")
		}
		return false
	}, text)
}

// page cuts one page and shapes the list response: the array of resources, nextPageToken and kind.
func (c *call) page(items []map[string]any) (any, error) {
	size := int64(0)
	for _, k := range []string{"pageSize", "maxResults"} {
		if v := c.q.Get(k); v != "" && c.m.query[k] {
			var err error
			if size, err = strconv.ParseInt(v, 10, 32); err != nil || size < 0 {
				return nil, gapi.Invalid("Invalid value at '%s': %q", k, v)
			}
		}
	}
	page, next := gapi.Page(items, int32(size), c.q.Get("pageToken"))
	props := c.a.schema(c.m.resp).Properties
	out := map[string]any{}
	for k, p := range props {
		if p.Type == "array" && k != "unreachable" {
			list := make([]any, 0, len(page))
			for _, it := range page {
				list = append(list, clone(it))
			}
			out[k] = list
		}
	}
	if p := props["kind"]; p != nil && p.Default != nil {
		out["kind"] = p.Default
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out, nil
}

// ---- state ----

func (a *api) Reset() {
	a.mu.Lock()
	a.objs, a.seq = map[string]map[string]any{}, 0
	a.mu.Unlock()
}

type saved struct {
	Objs map[string]map[string]any
	Seq  int
}

func (a *api) Save() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Marshal(saved{a.objs, a.seq})
}

func (a *api) Load(b []byte) error {
	var in saved
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&in); err != nil {
		return err
	}
	if in.Objs == nil {
		in.Objs = map[string]map[string]any{}
	}
	a.mu.Lock()
	a.objs, a.seq = in.Objs, in.Seq
	a.mu.Unlock()
	return nil
}

func newToken() string { return core.RandHex(16) }
