// Package bigquery emulates BigQuery's REST API (v2): datasets, tables with
// nested schemas, views, streaming inserts, and jobs that query, load from
// and extract to Cloud Storage, and copy.
//
// Rows live in pogi's state. Queries run on DuckDB in a container (the
// engine Athena uses): the tables a query names are loaded into a fresh
// in-memory database, the GoogleSQL is translated to DuckDB's dialect
// (see sql.go), and a statement that changes a table has its result stored
// back. Without Docker everything but queries works.
//
// The Go client takes the endpoint with its version path:
//
//	bigquery.NewClient(ctx, project, option.WithEndpoint(pogi+"/bigquery/v2/"), option.WithoutAuthentication())
package bigquery

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/duckdb"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// Service is the BigQuery emulator.
type Service struct {
	hub *core.Hub
	eng *duckdb.Engine
	mu  sync.Mutex
	st  state
}

type state struct {
	datasets map[string]*dataset // by project/dataset
	jobs     map[string]*job     // by project/id
	jobOrder []string
}

func newState() state { return state{datasets: map[string]*dataset{}, jobs: map[string]*job{}} }

// New creates the BigQuery service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, st: newState()}
	s.eng = &duckdb.Engine{Hub: h, Name: "BigQuery", Service: "bigquery", Script: server}
	h.OnDeliver("bigquery", s.deliver)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "bigquery", Cloud: core.GCP, Title: "BigQuery", Match: match}
}

func match(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/bigquery/v2/") || strings.HasPrefix(r.URL.Path, "/upload/bigquery/v2/")
}

// Close stops the engine.
func (s *Service) Close() error { s.eng.Close(); return nil }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func (d *dataset) etag() string {
	h := sha1.Sum([]byte(d.Ref.DatasetID + strconv.FormatInt(d.Modified, 10)))
	return hex.EncodeToString(h[:8])
}

// ---- routing ----

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/bigquery/v2/"), "/")
	if strings.HasPrefix(r.URL.Path, "/upload/") {
		c.Op = "upload"
		writeErr(w, c, errf(501, "notImplemented", "pogi does not take media uploads: load from Cloud Storage"))
		return
	}
	seg := strings.Split(path, "/")
	op, h := s.route(r.Method, seg)
	c.Op = op
	if h == nil {
		writeErr(w, c, errf(501, "notImplemented", "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeErr(w, c, &apiErr{e.Status, e.Code, e.Msg})
		return
	}
	q := r.URL.Query()
	if !gapi.PageSizeOK(q.Get("maxResults")) || !gapi.PageSizeOK(q.Get("startIndex")) {
		writeErr(w, c, errf(400, "invalid", "Invalid value for maxResults or startIndex: must be a whole number, 0 or more"))
		return
	}
	out, status, err := h(r, seg)
	if err != nil {
		writeErr(w, c, err)
		return
	}
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return
	}
	writeJSON(w, out)
}

type handler func(r *http.Request, seg []string) (any, int, error)

func ok(v any, err error) (any, int, error) { return v, http.StatusOK, err }

// route finds the handler of a call by the shape of its path.
func (s *Service) route(method string, seg []string) (string, handler) {
	if seg[0] != "projects" {
		return "", nil
	}
	n := len(seg)
	if n == 1 && method == "GET" {
		return "projects.list", func(r *http.Request, _ []string) (any, int, error) { return ok(s.listProjects(), nil) }
	}
	if n < 3 {
		return "", nil
	}
	pick := func(m map[string]handler, op string) (string, handler) { return op, m[method] }
	body := func(f func(r *http.Request, seg []string) (any, error)) handler {
		return func(r *http.Request, seg []string) (any, int, error) { return ok(f(r, seg)) }
	}
	switch seg[2] {
	case "datasets":
		switch {
		case n == 3:
			return pick(map[string]handler{"GET": body(s.listDatasets), "POST": body(s.insertDataset)}, "datasets."+map[string]string{"GET": "list", "POST": "insert"}[method])
		case n == 4:
			return pick(map[string]handler{"GET": body(s.getDataset), "PATCH": body(s.patchDataset), "PUT": body(s.patchDataset),
				"DELETE": s.deleteDataset}, "datasets."+map[string]string{"GET": "get", "PATCH": "patch", "PUT": "update", "DELETE": "delete"}[method])
		case n == 5 && seg[4] == "tables":
			return pick(map[string]handler{"GET": body(s.listTables), "POST": body(s.insertTable)}, "tables."+map[string]string{"GET": "list", "POST": "insert"}[method])
		case n == 6 && seg[4] == "tables":
			return pick(map[string]handler{"GET": body(s.getTable), "PATCH": body(s.patchTable), "PUT": body(s.patchTable),
				"DELETE": s.deleteTable}, "tables."+map[string]string{"GET": "get", "PATCH": "patch", "PUT": "update", "DELETE": "delete"}[method])
		case n == 7 && seg[4] == "tables" && seg[6] == "insertAll":
			return pick(map[string]handler{"POST": body(s.insertAll)}, "tabledata.insertAll")
		case n == 7 && seg[4] == "tables" && seg[6] == "data":
			return pick(map[string]handler{"GET": body(s.tableData)}, "tabledata.list")
		}
	case "jobs":
		switch {
		case n == 3:
			return pick(map[string]handler{"GET": body(s.listJobs), "POST": body(s.insertJob)}, "jobs."+map[string]string{"GET": "list", "POST": "insert"}[method])
		case n == 4:
			return pick(map[string]handler{"GET": body(s.getJob)}, "jobs.get")
		case n == 5 && seg[4] == "cancel":
			return pick(map[string]handler{"POST": body(s.cancelJob)}, "jobs.cancel")
		}
	case "queries":
		switch {
		case n == 3:
			return pick(map[string]handler{"POST": body(s.query)}, "jobs.query")
		case n == 4:
			return pick(map[string]handler{"GET": body(s.queryResults)}, "jobs.getQueryResults")
		}
	case "serviceAccount":
		return pick(map[string]handler{"GET": body(func(*http.Request, []string) (any, error) {
			return map[string]string{"kind": "bigquery#getServiceAccountResponse", "email": "bq-" + seg[1] + "@bigquery-encryption.iam.gserviceaccount.com"}, nil
		})}, "projects.getServiceAccount")
	}
	return "", nil
}

func (s *Service) listProjects() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var projects []any
	for _, k := range core.SortedKeys(s.st.datasets) {
		if p := s.st.datasets[k].Ref.ProjectID; !seen[p] {
			seen[p] = true
			projects = append(projects, map[string]any{"kind": "bigquery#project", "id": p, "numericId": "1", "projectReference": map[string]string{"projectId": p}, "friendlyName": p})
		}
	}
	return map[string]any{"kind": "bigquery#projectList", "projects": projects, "totalItems": len(projects)}
}

// ---- datasets ----

func (d *dataset) api() map[string]any {
	m := map[string]any{"kind": "bigquery#dataset", "etag": d.etag(), "id": d.Ref.ProjectID + ":" + d.Ref.DatasetID,
		"selfLink":         "https://bigquery.googleapis.com/bigquery/v2/projects/" + d.Ref.ProjectID + "/datasets/" + d.Ref.DatasetID,
		"datasetReference": d.Ref, "location": d.Location, "creationTime": strconv.FormatInt(d.Created, 10), "lastModifiedTime": strconv.FormatInt(d.Modified, 10)}
	if d.FriendlyName != "" {
		m["friendlyName"] = d.FriendlyName
	}
	if d.Description != "" {
		m["description"] = d.Description
	}
	if len(d.Labels) > 0 {
		m["labels"] = d.Labels
	}
	if d.DefaultTTLms != "" {
		m["defaultTableExpirationMs"] = d.DefaultTTLms
	}
	if len(d.Access) > 0 {
		m["access"] = d.Access
	}
	return m
}

func decodeBody(r *http.Request, v any) error {
	b := core.Body(r)
	if len(strings.TrimSpace(string(b))) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, v); err != nil {
		return invalid("Invalid JSON payload received. %s", core.PlainJSON(err))
	}
	return nil
}

func (s *Service) getDS(project, id string) (*dataset, error) {
	d := s.st.datasets[project+"/"+id]
	if d == nil {
		return nil, notFound("Not found: Dataset %s:%s", project, id)
	}
	return d, nil
}

func (s *Service) insertDataset(r *http.Request, seg []string) (any, error) {
	var in dataset
	if err := decodeBody(r, &in); err != nil {
		return nil, err
	}
	id := in.Ref.DatasetID
	if !datasetIDRE.MatchString(id) || len(id) > 1024 {
		return nil, invalid("Invalid dataset ID %q. Dataset IDs must be alphanumeric (plus underscores) and must be at most 1024 characters long.", id)
	}
	in.Ref.ProjectID = seg[1]
	if in.Location == "" {
		in.Location = "US"
	}
	in.Created, in.Modified = ms(time.Now()), ms(time.Now())
	in.tables = map[string]*table{}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.datasets[seg[1]+"/"+id]; ok {
		return nil, errf(409, "duplicate", "Already Exists: Dataset %s:%s", seg[1], id)
	}
	s.st.datasets[seg[1]+"/"+id] = &in
	return in.api(), nil
}

func (s *Service) getDataset(_ *http.Request, seg []string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.getDS(seg[1], seg[3])
	if err != nil {
		return nil, err
	}
	return d.api(), nil
}

func (s *Service) listDatasets(r *http.Request, seg []string) (any, error) {
	q := r.URL.Query()
	var want [][2]string // labels.key:value filters
	for _, f := range strings.Fields(q.Get("filter")) {
		if k, v, ok := strings.Cut(strings.TrimPrefix(f, "labels."), ":"); ok && strings.HasPrefix(f, "labels.") {
			want = append(want, [2]string{k, v})
		}
	}
	s.mu.Lock()
	var items []map[string]any
	for _, k := range core.SortedKeys(s.st.datasets) {
		d := s.st.datasets[k]
		if d.Ref.ProjectID != seg[1] {
			continue
		}
		hit := true
		for _, w := range want {
			hit = hit && d.Labels[w[0]] == w[1]
		}
		if hit {
			m := map[string]any{"kind": "bigquery#dataset", "id": d.Ref.ProjectID + ":" + d.Ref.DatasetID, "datasetReference": d.Ref, "location": d.Location}
			if len(d.Labels) > 0 {
				m["labels"] = d.Labels
			}
			if d.FriendlyName != "" {
				m["friendlyName"] = d.FriendlyName
			}
			items = append(items, m)
		}
	}
	s.mu.Unlock()
	page, next := pageOf(items, q.Get("maxResults"), q.Get("pageToken"))
	out := map[string]any{"kind": "bigquery#datasetList", "etag": "pogi", "datasets": page}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out, nil
}

// pageOf cuts a page by maxResults and an offset token.
func pageOf[T any](items []T, maxResults, token string) ([]T, string) {
	start := min(max(atoi(token, 0), 0), len(items))
	end := len(items)
	if n := atoi(maxResults, 0); n > 0 {
		end = min(end, start+n)
	}
	if end < len(items) {
		return items[start:end], strconv.Itoa(end)
	}
	return items[start:end], ""
}

// patchDataset changes the fields of the body (patch and update).
func (s *Service) patchDataset(r *http.Request, seg []string) (any, error) {
	var in map[string]json.RawMessage
	if err := decodeBody(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.getDS(seg[1], seg[3])
	if err != nil {
		return nil, err
	}
	for k, raw := range in {
		var err error
		switch k {
		case "friendlyName":
			err = json.Unmarshal(raw, &d.FriendlyName)
		case "description":
			err = json.Unmarshal(raw, &d.Description)
		case "defaultTableExpirationMs":
			err = json.Unmarshal(raw, &d.DefaultTTLms)
		case "access":
			d.Access = raw
		case "labels":
			d.Labels, err = mergeLabels(d.Labels, raw)
		}
		if err != nil {
			return nil, invalid("Invalid value for %s: %v", k, err)
		}
	}
	d.Modified = ms(time.Now())
	return d.api(), nil
}

// mergeLabels applies a labels object where a null value removes the label.
func mergeLabels(cur map[string]string, raw json.RawMessage) (map[string]string, error) {
	var in map[string]*string
	if err := json.Unmarshal(raw, &in); err != nil {
		return cur, err
	}
	if cur == nil {
		cur = map[string]string{}
	}
	for k, v := range in {
		if v == nil {
			delete(cur, k)
		} else {
			cur[k] = *v
		}
	}
	return cur, nil
}

func (s *Service) deleteDataset(r *http.Request, seg []string) (any, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.getDS(seg[1], seg[3])
	if err != nil {
		return nil, 0, err
	}
	if len(d.tables) > 0 && r.URL.Query().Get("deleteContents") != "true" {
		return nil, 0, errf(400, "resourceInUse", "Dataset %s:%s is still in use", seg[1], seg[3])
	}
	delete(s.st.datasets, seg[1]+"/"+seg[3])
	return nil, http.StatusNoContent, nil
}

// ---- tables ----

func (t *table) api() map[string]any {
	c := *t
	c.Rows = nil
	b, _ := json.Marshal(c)
	var m map[string]any
	json.Unmarshal(b, &m)
	m["kind"], m["id"], m["etag"] = "bigquery#table", t.Ref.ProjectID+":"+t.Ref.DatasetID+"."+t.Ref.TableID, "pogi"+strconv.FormatInt(t.Modified, 36)
	m["selfLink"] = "https://bigquery.googleapis.com/bigquery/v2/projects/" + t.Ref.ProjectID + "/datasets/" + t.Ref.DatasetID + "/tables/" + t.Ref.TableID
	m["location"] = "US"
	if t.Type == "TABLE" {
		m["numRows"] = strconv.Itoa(len(t.Rows))
		m["numBytes"] = strconv.Itoa(len(t.Rows) * max(len(t.fields()), 1) * 8) // ponytail: an estimate
		m["numLongTermBytes"] = "0"
	}
	return m
}

func tableName(id string) string {
	name, _, _ := strings.Cut(id, "$") // a partition decorator
	return name
}

func (s *Service) getTbl(project, ds, id string) (*dataset, *table, error) {
	d, err := s.getDS(project, ds)
	if err != nil {
		return nil, nil, err
	}
	t := d.tables[tableName(id)]
	if t == nil {
		return nil, nil, notFound("Not found: Table %s:%s.%s", project, ds, id)
	}
	return d, t, nil
}

func (s *Service) insertTable(r *http.Request, seg []string) (any, error) {
	var in table
	if err := decodeBody(r, &in); err != nil {
		return nil, err
	}
	id := in.Ref.TableID
	if !tableIDRE.MatchString(id) || len(id) > 1024 {
		return nil, invalid("Invalid table ID %q. Table IDs must be alphanumeric (plus underscores) and must be at most 1024 characters long.", id)
	}
	in.Ref = tableRef{seg[1], seg[3], id}
	in.Rows = nil
	if in.View != nil {
		in.Type = "VIEW"
	} else {
		in.Type = "TABLE"
	}
	if in.Schema != nil {
		if err := checkSchema(in.Schema.Fields); err != nil {
			return nil, err
		}
	}
	in.Created, in.Modified = ms(time.Now()), ms(time.Now())
	if in.View != nil {
		if in.View.UseLegacySQL {
			return nil, invalid("Legacy SQL views are not supported by pogi")
		}
		if err := s.checkView(seg[1], in.View.Query); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.getDS(seg[1], seg[3])
	if err != nil {
		return nil, err
	}
	if d.tables[id] != nil {
		return nil, errf(409, "duplicate", "Already Exists: Table %s", in.Ref)
	}
	if in.ExpirationTime == "" && d.DefaultTTLms != "" {
		in.ExpirationTime = strconv.FormatInt(in.Created+int64(atoi(d.DefaultTTLms, 0)), 10)
	}
	d.tables[id] = &in
	s.mu.Unlock()
	if in.View != nil {
		s.fillViewSchema(r, in.Ref)
	}
	s.mu.Lock()
	return in.api(), nil
}

// fillViewSchema gives a view the schema of its query, when the engine can tell.
func (s *Service) fillViewSchema(r *http.Request, ref tableRef) {
	res, err := s.runSQL(r.Context(), ref.ProjectID, "", "SELECT * FROM `"+ref.ProjectID+"."+ref.DatasetID+"."+ref.TableID+"`", nil, true)
	if err != nil || res.fields == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := s.st.datasets[ref.ProjectID+"/"+ref.DatasetID]; d != nil && d.tables[ref.TableID] != nil {
		d.tables[ref.TableID].Schema = &schema{Fields: res.fields}
	}
}

// checkView validates a view's query: it must translate, and name tables that exist.
func (s *Service) checkView(project, query string) error {
	tr, err := translate(query, tctx{project, ""}, nil)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range tr.refs {
		if _, _, err := s.getTbl(r.project, r.dataset, r.table); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) getTable(_ *http.Request, seg []string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, err := s.getTbl(seg[1], seg[3], seg[5])
	if err != nil {
		return nil, err
	}
	return t.api(), nil
}

func (s *Service) listTables(r *http.Request, seg []string) (any, error) {
	s.mu.Lock()
	d, err := s.getDS(seg[1], seg[3])
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var items []map[string]any
	for _, id := range core.SortedKeys(d.tables) {
		t := d.tables[id]
		m := map[string]any{"kind": "bigquery#table", "id": t.Ref.ProjectID + ":" + t.Ref.DatasetID + "." + id, "tableReference": t.Ref, "type": t.Type,
			"creationTime": strconv.FormatInt(t.Created, 10)}
		if len(t.Labels) > 0 {
			m["labels"] = t.Labels
		}
		if t.TimePartitioning != nil {
			m["timePartitioning"] = t.TimePartitioning
		}
		if t.View != nil {
			m["view"] = map[string]bool{"useLegacySql": false}
		}
		items = append(items, m)
	}
	s.mu.Unlock()
	page, next := pageOf(items, r.URL.Query().Get("maxResults"), r.URL.Query().Get("pageToken"))
	out := map[string]any{"kind": "bigquery#tableList", "etag": "pogi", "tables": page, "totalItems": len(items)}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out, nil
}

// patchTable changes the fields of the body (patch and update).
func (s *Service) patchTable(r *http.Request, seg []string) (any, error) {
	var in map[string]json.RawMessage
	if err := decodeBody(r, &in); err != nil {
		return nil, err
	}
	if raw, ok := in["view"]; ok {
		var v view
		if json.Unmarshal(raw, &v) == nil {
			if err := s.checkView(seg[1], v.Query); err != nil {
				return nil, err
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, err := s.getTbl(seg[1], seg[3], seg[5])
	if err != nil {
		return nil, err
	}
	for k, raw := range in {
		var err error
		switch k {
		case "description":
			err = json.Unmarshal(raw, &t.Description)
		case "friendlyName":
			err = json.Unmarshal(raw, &t.FriendlyName)
		case "expirationTime":
			err = json.Unmarshal(raw, &t.ExpirationTime)
		case "labels":
			t.Labels, err = mergeLabels(t.Labels, raw)
		case "clustering":
			err = json.Unmarshal(raw, &t.Clustering)
		case "timePartitioning":
			var tp timePartitioning
			if err = json.Unmarshal(raw, &tp); err == nil && t.TimePartitioning != nil { // only the expiration and the filter flag change
				t.TimePartitioning.ExpirationMs, t.TimePartitioning.RequirePartitionFilter = tp.ExpirationMs, tp.RequirePartitionFilter
			}
		case "view":
			if t.View == nil {
				return nil, invalid("Table %s is not a view", t.Ref)
			}
			err = json.Unmarshal(raw, t.View)
		case "schema":
			var sc schema
			if err = json.Unmarshal(raw, &sc); err == nil {
				err = s.setSchema(t, sc.Fields)
			}
		}
		if err != nil {
			if _, isAPI := err.(*apiErr); isAPI {
				return nil, err
			}
			return nil, invalid("Invalid value for %s: %v", k, err)
		}
	}
	t.Modified = ms(time.Now())
	out := t.api()
	if _, viewChanged := in["view"]; viewChanged {
		s.mu.Unlock()
		s.fillViewSchema(r, t.Ref)
		s.mu.Lock()
		out = t.api()
	}
	return out, nil
}

// setSchema replaces a table's schema: columns can be added and relaxed to
// NULLABLE, not dropped or retyped. Lock held.
func (s *Service) setSchema(t *table, fs []*Field) error {
	if err := checkSchema(fs); err != nil {
		return err
	}
	for _, old := range t.fields() {
		i := slices.IndexFunc(fs, func(f *Field) bool { return f.Name == old.Name })
		switch {
		case i < 0:
			return invalid("Provided Schema does not match Table %s. Field %s is missing in new schema", t.Ref, old.Name)
		case fs[i].Type != old.Type || fs[i].Mode != old.Mode && !(old.Mode == "REQUIRED" && fs[i].Mode == "NULLABLE"):
			return invalid("Provided Schema does not match Table %s. Field %s has changed type from %s to %s", t.Ref, old.Name, old.Type, fs[i].Type)
		}
	}
	for _, f := range fs {
		if !slices.ContainsFunc(t.fields(), func(o *Field) bool { return o.Name == f.Name }) && f.Mode == "REQUIRED" && len(t.Rows) > 0 {
			return invalid("Provided Schema does not match Table %s. Cannot add required field %s to a table with rows", t.Ref, f.Name)
		}
	}
	t.Schema = &schema{Fields: fs}
	return nil
}

func (s *Service) deleteTable(_ *http.Request, seg []string) (any, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, t, err := s.getTbl(seg[1], seg[3], seg[5])
	if err != nil {
		return nil, 0, err
	}
	delete(d.tables, t.Ref.TableID)
	return nil, http.StatusNoContent, nil
}

// ---- table data ----

func (s *Service) insertAll(r *http.Request, seg []string) (any, error) {
	var in struct {
		Rows []struct {
			InsertID string         `json:"insertId"`
			JSON     map[string]any `json:"json"`
		} `json:"rows"`
		SkipInvalidRows     bool `json:"skipInvalidRows"`
		IgnoreUnknownValues bool `json:"ignoreUnknownValues"`
	}
	d := jsonDecoder(core.Body(r))
	if err := d.Decode(&in); err != nil {
		return nil, invalid("Invalid JSON payload received. %s", core.PlainJSON(err))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, err := s.getTbl(seg[1], seg[3], seg[5])
	if err != nil {
		return nil, err
	}
	if t.Type != "TABLE" {
		return nil, invalid("Cannot insert into %s %s", strings.ToLower(t.Type), t.Ref)
	}
	if t.Schema == nil {
		return nil, invalid("Table %s has no schema: streaming inserts need one", t.Ref)
	}
	rec := &Field{Name: "row", Type: "RECORD", Fields: t.fields()}
	type bad struct {
		index int
		msg   string
	}
	var bads []bad
	rows := make([]row, len(in.Rows))
	for i, rw := range in.Rows {
		v, err := scalar(rec, rw.JSON, in.IgnoreUnknownValues)
		if err != nil {
			bads = append(bads, bad{i, err.Error()})
			continue
		}
		rows[i], _ = v.(map[string]any)
	}
	var errs []map[string]any
	for _, b := range bads {
		errs = append(errs, map[string]any{"index": b.index, "errors": []map[string]string{{"reason": "invalid", "message": b.msg}}})
	}
	if len(bads) > 0 && !in.SkipInvalidRows { // nothing is inserted: the rows that were fine are "stopped"
		for i := range in.Rows {
			if !slices.ContainsFunc(bads, func(b bad) bool { return b.index == i }) {
				errs = append(errs, map[string]any{"index": i, "errors": []map[string]string{{"reason": "stopped", "message": "stopped"}}})
			}
		}
	} else {
		if t.insertIDs == nil {
			t.insertIDs = map[string]bool{}
		}
		for i, rw := range in.Rows {
			if rows[i] == nil || rw.InsertID != "" && t.insertIDs[rw.InsertID] {
				continue
			}
			if rw.InsertID != "" {
				t.insertIDs[rw.InsertID] = true
			}
			t.Rows = append(t.Rows, rows[i])
		}
		t.Modified = ms(time.Now())
	}
	out := map[string]any{"kind": "bigquery#tableDataInsertAllResponse"}
	if len(errs) > 0 {
		out["insertErrors"] = errs
	}
	return out, nil
}

func jsonDecoder(b []byte) *json.Decoder {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	return d
}

func (s *Service) tableData(r *http.Request, seg []string) (any, error) {
	s.mu.Lock()
	_, t, err := s.getTbl(seg[1], seg[3], seg[5])
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	fs, rows := t.fields(), t.Rows[:len(t.Rows):len(t.Rows)]
	isView := t.Type == "VIEW"
	s.mu.Unlock()
	if isView { // a view is read by running its query
		res, err := s.runSQL(r.Context(), seg[1], "", "SELECT * FROM `"+seg[1]+"."+seg[3]+"."+seg[5]+"`", nil, false)
		if err != nil {
			return nil, err
		}
		fs, rows = res.fields, res.rows
	}
	q := r.URL.Query()
	if sel := q.Get("selectedFields"); sel != "" {
		names := strings.Split(sel, ",")
		fs = slices.DeleteFunc(slices.Clone(fs), func(f *Field) bool { return !slices.Contains(names, f.Name) })
	}
	start := 0
	if q.Get("pageToken") != "" {
		start = atoi(q.Get("pageToken"), 0)
	} else if q.Get("startIndex") != "" {
		start = atoi(q.Get("startIndex"), 0)
	}
	page, next := pageOf(rows[min(start, len(rows)):], q.Get("maxResults"), "")
	out := map[string]any{"kind": "bigquery#tableDataList", "etag": "pogi", "totalRows": strconv.Itoa(len(rows)), "rows": wireRows(fs, page, q.Get("formatOptions.useInt64Timestamp") == "true")}
	if next != "" {
		out["pageToken"] = strconv.Itoa(min(start, len(rows)) + atoi(next, 0))
	}
	return out, nil
}

func wireRows(fs []*Field, rows []row, i64 bool) []any {
	out := make([]any, len(rows))
	for i, rw := range rows {
		out[i] = wireRow(fs, rw, i64)
	}
	return out
}

// ---- Hub.Deliver ----

// logSchema is the table of a Cloud Logging sink.
var logSchema = []*Field{{Name: "logName", Type: "STRING"}, {Name: "timestamp", Type: "TIMESTAMP"}, {Name: "receiveTimestamp", Type: "TIMESTAMP"},
	{Name: "severity", Type: "STRING"}, {Name: "insertId", Type: "STRING"}, {Name: "textPayload", Type: "STRING"}, {Name: "jsonPayload", Type: "JSON"},
	{Name: "resource", Type: "JSON"}, {Name: "labels", Type: "JSON"}, {Name: "httpRequest", Type: "JSON"}, {Name: "trace", Type: "STRING"}, {Name: "spanId", Type: "STRING"}}

// deliver stores a message in a table: the target is projects/<p>/datasets/<d>,
// Subject names the table, Body is a JSON object whose keys are columns. A
// table that does not exist is made with the schema of a log entry.
func (s *Service) deliver(target string, m *core.Message) error {
	parts := strings.Split(target, "/")
	if len(parts) < 4 || parts[0] != "projects" || parts[2] != "datasets" {
		return errf(400, "invalid", "invalid BigQuery target %q: want projects/<p>/datasets/<d>", target)
	}
	if m.Subject == "" {
		return errf(400, "invalid", "a delivery to BigQuery names its table in Subject")
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(m.Body), &obj); err != nil {
		return errf(400, "invalid", "the body must be a JSON object: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.getDS(parts[1], parts[3])
	if err != nil {
		return err
	}
	t := d.tables[m.Subject]
	if t == nil {
		t = &table{Ref: tableRef{parts[1], parts[3], m.Subject}, Type: "TABLE", Schema: &schema{Fields: logSchema}, Created: ms(time.Now()), Modified: ms(time.Now())}
		d.tables[m.Subject] = t
	}
	rec := &Field{Name: "row", Type: "RECORD", Fields: t.fields()}
	for _, f := range rec.Fields { // JSON columns take objects as their text
		if f.Type == "JSON" {
			if v, ok := obj[f.Name]; ok {
				if _, isString := v.(string); !isString {
					b, _ := json.Marshal(v)
					obj[f.Name] = string(b)
				}
			}
		}
	}
	v, err := scalar(rec, obj, true)
	if err != nil {
		return errf(400, "invalid", "%v", err)
	}
	t.Rows = append(t.Rows, v.(map[string]any))
	return nil
}
