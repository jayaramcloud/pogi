// Package compute emulates Compute Engine: the REST API at /compute/v1/projects/<p>/... that
// google.golang.org/api/compute/v1, cloud.google.com/go/compute/apiv1 (REST only: it has no gRPC) and
// gcloud speak, and the metadata server at /computeMetadata/v1/.
//
// Zones, regions, machine types, disk types and images are static lists. Instances, disks, networks,
// subnetworks, firewalls and addresses are stored. Every operation is DONE when it is returned, because
// the work (booting a container) has finished by then.
//
// With a container engine an instance runs as a container of its boot disk's image (see catalog.go),
// with the startup-script metadata run at every boot, and GCE_METADATA_HOST pointing at a metadata
// listener of its own, so that Google client libraries inside find the instance's metadata and tokens.
// Without an engine the instance is RUNNING at once, as metadata only.
//
// ponytail: no live migration, snapshots, instance templates, groups, load balancing or custom
// images. Networks do not carry traffic, and firewall rules are only stored.
package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// api is how Compute Engine writes links; pogi writes them the same way.
const api = "https://www.googleapis.com/compute/v1/"

var nameRE = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)

type state struct {
	Res   map[string]map[string]any    `json:"res"`   // by path: projects/p/zones/z/instances/n
	Ops   map[string]map[string]any    `json:"ops"`   // by path
	Proj  map[string]map[string]any    `json:"proj"`  // project -> its settings: commonInstanceMetadata
	Guest map[string]map[string]string `json:"guest"` // instance path -> "namespace/key" -> value
	Ctr   map[string]string            `json:"ctr"`   // instance path -> container name; absent: metadata only
	IP    int                          `json:"ip"`    // addresses handed out
}

// Service is the Compute Engine emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
	eng sync.Mutex // one container-engine call at a time, taken without mu

	md map[string]*mdListener // per-instance metadata listeners by instance path; guarded by mu
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "compute", Cloud: core.GCP, Title: "Compute Engine", Match: func(r *http.Request) bool {
		p := r.URL.Path
		return strings.HasPrefix(p, "/compute/v1/") || strings.HasPrefix(p, "/compute/beta/") || isMetadata(p)
	}}
}

// ---- requests ----

type req struct {
	s       *Service
	w       http.ResponseWriter
	r       *http.Request
	c       *core.Ctx
	project string
	seg     []string // after projects/<p>
	body    map[string]any
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	if isMetadata(r.URL.Path) {
		s.metadata(w, r, "")
		return
	}
	seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // compute v1 projects <p> ...
	if len(seg) < 4 || seg[2] != "projects" {
		fail(w, c, core.Errorf(http.StatusNotFound, "notFound", "Not Found"))
		return
	}
	q := &req{s: s, w: w, r: r, c: c, project: seg[3], seg: seg[4:]}
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, &q.body) != nil {
		fail(w, c, core.Errorf(http.StatusBadRequest, "invalid", "Invalid JSON payload received."))
		return
	}
	if q.body == nil {
		q.body = map[string]any{}
	}
	c.Op = q.op()
	err := c.Intercept()
	if err == nil && !gapi.PageSizeOK(q.r.URL.Query().Get("maxResults")) {
		err = invalid("Invalid value for field 'maxResults': must be a whole number, 0 or more")
	}
	var out any
	if err == nil {
		out, err = s.route(q)
	}
	if err != nil {
		fail(w, c, err)
		return
	}
	gapi.JSON(w, out)
}

// op names the request for the log and chaos rules: instances.insert, instances.stop, operations.get, ...
func (q *req) op() string {
	seg := q.seg
	if len(seg) == 2 && seg[0] == "aggregated" {
		return seg[1] + ".aggregatedList"
	}
	if len(seg) > 0 && (seg[0] == "zones" || seg[0] == "regions") && len(seg) > 2 {
		seg = seg[2:]
	} else if len(seg) > 1 && seg[0] == "global" {
		seg = seg[1:]
	}
	coll, verb := "projects", map[string]string{http.MethodGet: "get", http.MethodPost: "insert", http.MethodDelete: "delete"}[q.r.Method]
	if len(seg) > 0 {
		coll = seg[0]
	}
	switch {
	case len(seg) >= 3:
		verb = seg[2]
	case len(seg) == 1 && q.r.Method == http.MethodGet:
		verb = "list"
	case len(seg) == 2 && (q.r.Method == http.MethodPatch || q.r.Method == http.MethodPut):
		verb = "update"
	}
	return coll + "." + verb
}

// fail writes Compute Engine's error shape; the code of the error is the reason.
func fail(w http.ResponseWriter, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	status := core.GoogleStatus(e.Status)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": e.Status, "message": e.Msg, "status": status,
		"errors": []any{map[string]any{"message": e.Msg, "domain": "global", "reason": e.Code}}}})
}

func notFound(path string) error {
	return core.Errorf(http.StatusNotFound, "notFound", "The resource '%s' was not found", path)
}

func invalid(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "invalid", format, a...)
}

func str(v any) string { s, _ := v.(string); return s }

func obj(m map[string]any, k string) map[string]any { o, _ := m[k].(map[string]any); return o }

func arr(m map[string]any, k string) []any { a, _ := m[k].([]any); return a }

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	return out
}

func now() string { return time.Now().Format("2006-01-02T15:04:05.000-07:00") }

func newID() string {
	return strconv.FormatUint(uint64(time.Now().UnixNano())&0x3fffffffffff<<8|uint64(core.RandHex(1)[0]), 10)
}

// fingerprint is what setMetadata and setLabels must send back.
func fingerprint() string { return core.RandHex(6) }

// ref turns a reference to a resource (a URL, projects/p/..., or a bare name) into its path, and a bare name into
// the path under prefix.
func ref(in, prefix string) string {
	if i := strings.Index(in, "projects/"); i >= 0 {
		return in[i:]
	}
	if strings.Contains(in, "/") { // zones/z/machineTypes/t, global/networks/n: in the prefix's project
		if p := strings.SplitN(prefix, "/", 3); len(p) == 3 {
			return p[0] + "/" + p[1] + "/" + strings.TrimLeft(in, "/")
		}
		return strings.TrimLeft(in, "/")
	}
	return prefix + "/" + in
}

// ---- routing ----

func (s *Service) route(q *req) (any, error) {
	seg, m := q.seg, q.r.Method
	s.ensure(q.project)
	switch {
	case len(seg) == 0 && m == http.MethodGet:
		return s.projectDoc(q.project), nil
	case len(seg) == 1 && seg[0] == "setCommonInstanceMetadata" && m == http.MethodPost:
		return s.setCommon(q)
	case len(seg) == 2 && seg[0] == "aggregated" && m == http.MethodGet:
		return s.aggregated(q, seg[1])
	case (seg0(seg) == "zones" || seg0(seg) == "regions") && len(seg) <= 2:
		return staticScope(q, seg)
	case seg0(seg) == "zones" || seg0(seg) == "regions":
		if seg[0] == "zones" && !validZone(seg[1]) || seg[0] == "regions" && !validRegion(seg[1]) {
			return nil, notFound("projects/" + q.project + "/" + seg[0] + "/" + seg[1])
		}
		return s.collection(q, seg[0], seg[1], seg[2:])
	case seg0(seg) == "global" && len(seg) > 1:
		return s.collection(q, "global", "", seg[1:])
	}
	return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
}

func seg0(seg []string) string {
	if len(seg) == 0 {
		return ""
	}
	return seg[0]
}

// staticScope serves zones and regions, and their lists.
func staticScope(q *req, seg []string) (any, error) {
	if q.r.Method != http.MethodGet {
		return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
	}
	kind, names, doc := "region", regionNames, regionDoc
	if seg[0] == "zones" {
		kind, names, doc = "zone", nil, zoneDoc
		for _, r := range regionNames {
			names = append(names, zonesOf(r)...)
		}
	}
	if len(seg) == 2 {
		if !slices.Contains(names, seg[1]) {
			return nil, notFound("projects/" + q.project + "/" + seg[0] + "/" + seg[1])
		}
		return withProject(doc(seg[1]), q.project), nil
	}
	var items []map[string]any
	for _, n := range names {
		items = append(items, withProject(doc(n), q.project))
	}
	return q.list(kind+"List", "projects/"+q.project+"/"+seg[0], items), nil
}

// withProject puts the project into the links of a static document.
func withProject(d map[string]any, project string) map[string]any {
	for k, v := range d {
		if s, ok := v.(string); ok {
			d[k] = strings.Replace(s, "projects/-/", "projects/"+project+"/", 1)
		}
	}
	return d
}

// list is a paged list response: filter, maxResults and pageToken apply.
func (q *req) list(kind, selfPath string, items []map[string]any) any {
	query := q.r.URL.Query()
	if f, err := gapi.ParseFilter(query.Get("filter")); err == nil {
		out := items[:0:0]
		for _, it := range items {
			if f.Match(func(field, op, value string) bool { return term(it, field, op, value) }, func(string) bool { return true }) {
				out = append(out, it)
			}
		}
		items = out
	}
	n, _ := strconv.Atoi(query.Get("maxResults"))
	page, next := gapi.Page(items, int32(n), query.Get("pageToken"))
	out := map[string]any{"kind": "compute#" + kind, "id": selfPath, "items": page, "selfLink": api + selfPath}
	if page == nil {
		out["items"] = []any{}
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out
}

// term evaluates "field op value" on a document; a dotted field walks into objects (labels.env).
func term(doc map[string]any, field, op, value string) bool {
	var v any = doc
	for _, p := range strings.Split(field, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			v = nil
			break
		}
		v = m[p]
	}
	s := ""
	switch x := v.(type) {
	case string:
		s = x
	case nil:
	default:
		b, _ := json.Marshal(x)
		s = string(b)
	}
	switch op {
	case "=":
		return s == value
	case "!=":
		return s != value
	case ":":
		return strings.Contains(s, value)
	case "=~":
		re, err := regexp.Compile(value)
		return err == nil && re.MatchString(s)
	case "!~":
		re, err := regexp.Compile(value)
		return err == nil && !re.MatchString(s)
	}
	return false
}

// ---- projects ----

// ensure makes the project's default network the first time anybody uses the project.
func (s *Service) ensure(project string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Proj[project] != nil {
		return
	}
	s.st.Proj[project] = map[string]any{"commonInstanceMetadata": map[string]any{"kind": "compute#metadata", "fingerprint": fingerprint(), "items": []any{}}}
	path := "projects/" + project + "/global/networks/default"
	s.st.Res[path] = map[string]any{"kind": "compute#network", "id": newID(), "creationTimestamp": now(), "name": "default", "autoCreateSubnetworks": true,
		"routingConfig": map[string]any{"routingMode": "REGIONAL"}, "mtu": 1460, "selfLink": api + path, "subnetworks": []any{}}
}

func (s *Service) projectDoc(project string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"kind": "compute#project", "id": strconv.FormatInt(numeric(project), 10), "name": project, "creationTimestamp": now(),
		"commonInstanceMetadata": s.st.Proj[project]["commonInstanceMetadata"], "selfLink": api + "projects/" + project, "defaultNetworkTier": "PREMIUM"}
}

// numeric is the project number of a project id: stable, 12 digits.
func numeric(project string) int64 {
	var h int64 = 7
	for _, c := range project {
		h = (h*31 + int64(c)) % 900000000000
	}
	return 100000000000 + h%900000000000
}

func (s *Service) setCommon(q *req) (any, error) {
	s.mu.Lock()
	cur := obj(s.st.Proj[q.project], "commonInstanceMetadata")
	if err := checkFingerprint(q.body["fingerprint"], cur, "fingerprint", "metadata"); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.st.Proj[q.project]["commonInstanceMetadata"] = metadataDoc(q.body)
	s.mu.Unlock()
	return s.op(q, "global", "", "setCommonInstanceMetadata", "projects/"+q.project, nil, nil), nil
}

// checkFingerprint is the optimistic lock of setMetadata and setLabels.
func checkFingerprint(given any, cur map[string]any, field, what string) error {
	switch {
	case str(given) == "":
		return invalid("Required field '%s' not specified", field)
	case str(given) != str(cur[field]):
		return core.Errorf(http.StatusPreconditionFailed, "conditionNotMet", "Supplied fingerprint does not match current %s fingerprint.", what)
	}
	return nil
}

func metadataDoc(body map[string]any) map[string]any {
	items := []any{}
	for _, it := range arr(body, "items") {
		if m, ok := it.(map[string]any); ok {
			items = append(items, map[string]any{"key": str(m["key"]), "value": str(m["value"])})
		}
	}
	return map[string]any{"kind": "compute#metadata", "fingerprint": fingerprint(), "items": items}
}

// ---- operations ----

// op records an operation, DONE, and returns it. err is the failure of the work it did.
func (s *Service) op(q *req, scope, sn, typ, target string, doc map[string]any, err error) map[string]any {
	name := "operation-" + core.RandHex(8)
	base := "projects/" + q.project + "/global"
	o := map[string]any{"kind": "compute#operation", "id": newID(), "name": name, "operationType": typ, "targetLink": api + target, "status": "DONE",
		"user": "pogi@" + q.project + ".iam.gserviceaccount.com", "progress": 100, "insertTime": now(), "startTime": now(), "endTime": now()}
	if doc != nil {
		o["targetId"] = doc["id"]
	}
	switch scope {
	case "zones":
		base = "projects/" + q.project + "/zones/" + sn
		o["zone"] = api + base
	case "regions":
		base = "projects/" + q.project + "/regions/" + sn
		o["region"] = api + base
	}
	if err != nil {
		e := core.AsError(err)
		o["error"] = map[string]any{"errors": []any{map[string]any{"code": e.Code, "message": e.Msg}}}
		o["httpErrorStatusCode"], o["httpErrorMessage"] = e.Status, http.StatusText(e.Status)
	}
	o["selfLink"] = api + base + "/operations/" + name
	s.mu.Lock()
	s.st.Ops[base+"/operations/"+name] = o
	s.mu.Unlock()
	return o
}

// operations serves zoneOperations, regionOperations and globalOperations.
func (s *Service) operations(q *req, scope, sn string, rest []string) (any, error) {
	base := "projects/" + q.project + "/global"
	if scope != "global" {
		base = "projects/" + q.project + "/" + scope + "/" + sn
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(rest) == 1 && q.r.Method == http.MethodGet {
		var items []map[string]any
		for _, k := range core.SortedKeys(s.st.Ops) {
			if strings.HasPrefix(k, base+"/operations/") {
				items = append(items, s.st.Ops[k])
			}
		}
		return q.list("operationList", base+"/operations", items), nil
	}
	if len(rest) < 2 {
		return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
	}
	path := base + "/operations/" + rest[1]
	o := s.st.Ops[path]
	if o == nil {
		return nil, notFound(path)
	}
	switch {
	case len(rest) == 2 && q.r.Method == http.MethodGet, len(rest) == 3 && rest[2] == "wait" && q.r.Method == http.MethodPost:
		return o, nil
	case len(rest) == 2 && q.r.Method == http.MethodDelete:
		delete(s.st.Ops, path)
		return nil, nil
	}
	return nil, core.Errorf(http.StatusNotFound, "notFound", "Not Found")
}

// ---- persistence and console ----

func (s *Service) Reset() { s.Load([]byte("{}")) }

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load replaces the state. Instances that were running in containers boot again (the old containers are
// removed first), and the containers of the old state are removed.
func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Res == nil {
		st.Res = map[string]map[string]any{}
	}
	if st.Ops == nil {
		st.Ops = map[string]map[string]any{}
	}
	if st.Proj == nil {
		st.Proj = map[string]map[string]any{}
	}
	if st.Guest == nil {
		st.Guest = map[string]map[string]string{}
	}
	if st.Ctr == nil {
		st.Ctr = map[string]string{}
	}
	s.mu.Lock()
	old, oldMD := s.st, s.md
	s.st, s.md = st, map[string]*mdListener{}
	var boot []string
	for path, name := range st.Ctr {
		if i := st.Res[path]; i != nil && (str(i["status"]) == "RUNNING" || str(i["status"]) == "STAGING") && name != "" {
			boot = append(boot, path)
		}
	}
	s.mu.Unlock()
	for _, l := range oldMD {
		l.close()
	}
	if len(old.Ctr) > 0 && s.hub.Docker().Available() {
		s.eng.Lock()
		for _, name := range old.Ctr {
			s.hub.Docker().Remove(context.Background(), name)
		}
		s.eng.Unlock()
	}
	for _, path := range boot {
		core.Go("compute.boot", func() { s.boot(path, "run") })
	}
	return nil
}

// Close stops the metadata listeners.
func (s *Service) Close() error {
	s.mu.Lock()
	md := s.md
	s.md = map[string]*mdListener{}
	s.mu.Unlock()
	for _, l := range md {
		l.close()
	}
	return nil
}

var tables = []struct{ name, coll string }{{"Instances", "instances"}, {"Disks", "disks"}, {"Networks", "networks"},
	{"Subnetworks", "subnetworks"}, {"Firewalls", "firewalls"}, {"Addresses", "addresses"}}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Table
	for _, t := range tables {
		tb := core.Table{Name: t.name, Columns: []string{"Path", "Status", "Detail"}}
		for _, path := range core.SortedKeys(s.st.Res) {
			d := s.st.Res[path]
			if p := strings.Split(path, "/"); p[len(p)-2] != t.coll {
				continue
			}
			detail := str(d["machineType"])
			if i := strings.LastIndexByte(detail, '/'); i >= 0 {
				detail = detail[i+1:]
			}
			tb.Rows = append(tb.Rows, []string{path, str(d["status"]), firstOf(detail, str(d["ipCidrRange"]), str(d["address"]), str(d["sizeGb"]))})
		}
		out = append(out, tb)
	}
	return out
}

func firstOf(l ...string) string {
	for _, s := range l {
		if s != "" {
			return s
		}
	}
	return ""
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.Res[id]
}
