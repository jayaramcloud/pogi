// Package monitor emulates Azure Monitor: Log Analytics workspaces (with
// custom tables), data collection endpoints and rules, the Logs Ingestion
// API, the Log Analytics query API on a KQL subset (see kql.go), Application
// Insights components with telemetry ingestion, metrics, and stored action
// groups and alert rules.
//
// Management plane: ARM providers registered with arm.Register. Data planes
// route on the main port:
//
//	POST <dce>.ingest.monitor.localhost/dataCollectionRules/<immutableId>/streams/<stream>
//	POST /_azure/monitor/ingest/dataCollectionRules/...                 (no name resolution needed)
//	POST api.loganalytics.localhost/v1/workspaces/<customerId>/query
//	POST /_azure/monitor/query/v1/workspaces/<customerId>/query
//	POST /_azure/monitor/appinsights/v2/track      (also /v2/track and /v2.1/track)
//	GET  <resourceId>/providers/Microsoft.Insights/metrics | metricDefinitions
//
// Any bearer token is accepted. Rows live in memory per workspace (and are
// saved with the state), nothing expires.
//
// ponytail: no legacy HTTP Data Collector API (SharedKey), no metrics
// recording (series are zeros), no retention, no resource-centric query.
package monitor

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

const (
	typWorkspace = "Microsoft.OperationalInsights/workspaces"
	typTable     = "Microsoft.OperationalInsights/workspaces/tables"
	typDCE       = "Microsoft.Insights/dataCollectionEndpoints"
	typDCR       = "Microsoft.Insights/dataCollectionRules"
	typComponent = "Microsoft.Insights/components"
	prefix       = "/_azure/monitor/"
)

// Service is the Azure Monitor emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
}

type state struct {
	Res  map[string]map[string]any    `json:"resources"` // ARM resources by lower-cased ID
	Data map[string]map[string]*table `json:"data"`      // rows: workspace key -> lower-cased table name -> table
}

// table is a Log Analytics table. Rows hold JSON-form values, aligned with
// Cols (a row may be shorter when columns were added later).
type table struct {
	Name   string  `json:"name"`
	Cols   []col   `json:"cols"`
	Rows   [][]any `json:"rows"`
	Strict bool    `json:"strict,omitempty"` // the schema was declared: unknown columns are dropped
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	for _, k := range []struct {
		typ, loc string
		fill     func(id arm.ID, old, props map[string]any) error
	}{
		{typWorkspace, "eastus", s.fillWorkspace},
		{typTable, "eastus", s.fillTable},
		{typDCE, "eastus", s.fillDCE},
		{typDCR, "eastus", s.fillDCR},
		{typComponent, "eastus", s.fillComponent},
		{"Microsoft.Insights/actionGroups", "global", nil},
		{"Microsoft.Insights/metricAlerts", "global", nil},
		{"Microsoft.Insights/scheduledQueryRules", "eastus", nil},
		{"Microsoft.Insights/activityLogAlerts", "global", nil},
	} {
		p := s.provider(k.typ, k.loc, k.fill)
		if k.typ == typWorkspace {
			keys := func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				res, err := p.Get(c, id)
				if err != nil {
					return nil, err
				}
				cid, _ := res["properties"].(map[string]any)["customerId"].(string)
				sum := func(n string) string {
					h := sha256.Sum256([]byte(cid + n))
					return base64.StdEncoding.EncodeToString(h[:])
				}
				return map[string]any{"primarySharedKey": sum("primary"), "secondarySharedKey": sum("secondary")}, nil
			}
			p.Actions = map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": keys, "sharedKeys": keys}
		}
		arm.Register(h, p)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azmonitor", Cloud: core.Azure, Title: "Azure Monitor", Protocol: "azure-json", Match: func(r *http.Request) bool {
		k, _ := classify(r)
		return k != ""
	}}
}

var metricsPath = regexp.MustCompile(`(?i)^/subscriptions/[^/]+/.+/providers/microsoft\.insights/(metrics|metricdefinitions)$`)

// classify says which data plane a request is for and the path within it.
// Hosts and paths are specific enough never to claim another service's traffic.
func classify(r *http.Request) (kind, path string) {
	p := r.URL.Path
	if rest, ok := strings.CutPrefix(p, prefix); ok {
		k, rest, _ := strings.Cut(rest, "/")
		switch k {
		case "ingest", "query":
			return k, "/" + rest
		case "appinsights":
			return "track", "/" + rest
		}
		return "", p
	}
	host, _, _ := strings.Cut(r.Host, ":")
	switch {
	case strings.HasSuffix(host, ".localhost") && strings.Contains(host, ".ingest.monitor."):
		return "ingest", p
	case host == "api.loganalytics.localhost" || host == "api.applicationinsights.localhost":
		return "query", p
	case p == "/v2/track" || p == "/v2.1/track":
		return "track", p
	case r.Method == http.MethodGet && metricsPath.MatchString(p):
		return "metrics", p
	}
	return "", p
}

// ---- helpers ----

func guid() string {
	h := core.NewID()
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	json.Unmarshal(b, &out)
	return out
}

func sub(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	if v == nil {
		v = map[string]any{}
		m[key] = v
	}
	return v
}

func def(m map[string]any, key string, v any) {
	if _, ok := m[key]; !ok {
		m[key] = v
	}
}

func str(m map[string]any, key string) string { s, _ := m[key].(string); return s }

func keyOfID(id arm.ID) string { return strings.ToLower(id.String()) }

// hostURL is a *.localhost address of pogi: https://<sub>.localhost:<port>. It is
// https because Azure SDKs refuse to send bearer tokens over plain HTTP, and
// pogi serves TLS on its one port.
func (s *Service) hostURL(sub string) string {
	port := "7644"
	if u, err := url.Parse(s.hub.URL); err == nil && u.Port() != "" {
		port = u.Port()
	}
	return "https://" + sub + ".localhost:" + port
}

func (s *Service) pathURL(p string) string { return strings.TrimSuffix(s.hub.URL, "/") + prefix + p }

// ---- ARM providers ----

func (s *Service) provider(typ, loc string, fill func(arm.ID, map[string]any, map[string]any) error) arm.Provider {
	return arm.Provider{Type: typ,
		Put: func(_ *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			old := s.st.Res[keyOfID(id)]
			res := clone(body)
			if res == nil {
				res = map[string]any{}
			}
			props := sub(res, "properties")
			var oldProps map[string]any
			if old != nil {
				oldProps, _ = old["properties"].(map[string]any)
			}
			res["id"], res["name"], res["type"] = id.String(), id.Name(), id.Type()
			if str(res, "location") == "" {
				res["location"] = loc
				if old != nil {
					res["location"] = old["location"]
				}
			}
			def(res, "tags", map[string]any{})
			props["provisioningState"] = "Succeeded"
			if fill != nil {
				if err := fill(id, oldProps, props); err != nil {
					return nil, err
				}
			}
			s.st.Res[keyOfID(id)] = res
			return clone(res), nil
		},
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			res := s.st.Res[keyOfID(id)]
			if res == nil {
				return nil, arm.NotFound(id)
			}
			return clone(res), nil
		},
		Delete: func(_ *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			k := keyOfID(id)
			delete(s.st.Res, k)
			for rk := range s.st.Res { // children
				if strings.HasPrefix(rk, k+"/") {
					delete(s.st.Res, rk)
				}
			}
			switch typ {
			case typWorkspace:
				delete(s.st.Data, k)
			case typTable:
				delete(s.st.Data[strings.ToLower(id.Parent().String())], strings.ToLower(id.Name()))
			}
			return nil
		},
		List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			pre := strings.ToLower(parent.String()) + "/"
			var out []map[string]any
			for _, k := range core.SortedKeys(s.st.Res) {
				if res := s.st.Res[k]; strings.HasPrefix(k, pre) && strings.EqualFold(str(res, "type"), typ) {
					out = append(out, clone(res))
				}
			}
			return out, nil
		},
	}
}

func (s *Service) fillWorkspace(id arm.ID, old, props map[string]any) error {
	if cid := str(old, "customerId"); cid != "" {
		props["customerId"] = cid
		props["createdDate"] = old["createdDate"]
	} else {
		props["customerId"] = guid()
		props["createdDate"] = time.Now().UTC().Format(time.RFC3339)
	}
	props["modifiedDate"] = time.Now().UTC().Format(time.RFC3339)
	def(props, "sku", map[string]any{"name": "PerGB2018"})
	def(props, "retentionInDays", float64(30))
	def(props, "features", map[string]any{"enableLogAccessUsingOnlyResourcePermissions": true})
	def(props, "workspaceCapping", map[string]any{"dailyQuotaGb": float64(-1)})
	def(props, "publicNetworkAccessForIngestion", "Enabled")
	def(props, "publicNetworkAccessForQuery", "Enabled")
	return nil
}

// schemaCols reads a table resource's schema.columns.
func schemaCols(props map[string]any) []col {
	var out []col
	list, _ := sub(props, "schema")["columns"].([]any)
	for _, c := range list {
		if m, ok := c.(map[string]any); ok && str(m, "name") != "" {
			t := strings.ToLower(str(m, "type"))
			if t == "boolean" {
				t = "bool"
			}
			out = append(out, col{str(m, "name"), cmpOr(t, "string")})
		}
	}
	return out
}

func (s *Service) fillTable(id arm.ID, old, props map[string]any) error {
	def(props, "plan", "Analytics")
	def(props, "retentionInDays", float64(-1))
	def(props, "totalRetentionInDays", float64(-1))
	sch := sub(props, "schema")
	sch["name"] = id.Name()
	def(sch, "tableType", "CustomLog")
	if cols := schemaCols(props); len(cols) > 0 {
		t := s.tableData(strings.ToLower(id.Parent().String()), id.Name(), "TimeGenerated")
		t.Strict = true
		for _, c := range cols {
			if t.index(c.Name) < 0 {
				t.Cols = append(t.Cols, c)
			}
		}
	}
	return nil
}

func (s *Service) fillDCE(id arm.ID, old, props map[string]any) error {
	host := id.Name() + ".ingest.monitor"
	sub(props, "logsIngestion")["endpoint"] = s.hostURL(host)
	sub(props, "metricsIngestion")["endpoint"] = s.hostURL(host)
	sub(props, "configurationAccess")["endpoint"] = s.hostURL(id.Name() + ".handler.control.monitor")
	def(props, "immutableId", "dce-"+core.NewID())
	if v := str(old, "immutableId"); v != "" {
		props["immutableId"] = v
	}
	return nil
}

func (s *Service) fillDCR(id arm.ID, old, props map[string]any) error {
	props["immutableId"] = cmpOr(str(old, "immutableId"), "dcr-"+core.NewID())
	if las, ok := sub(props, "destinations")["logAnalytics"].([]any); ok {
		for _, d := range las {
			if m, ok := d.(map[string]any); ok && str(m, "workspaceId") == "" {
				if ws := s.st.Res[strings.ToLower(str(m, "workspaceResourceId"))]; ws != nil {
					m["workspaceId"] = ws["properties"].(map[string]any)["customerId"]
				}
			}
		}
	}
	flows, _ := props["dataFlows"].([]any)
	for _, f := range flows {
		if fm, ok := f.(map[string]any); ok && str(fm, "transformKql") != "" {
			if _, err := parseKQL(str(fm, "transformKql")); err != nil {
				return core.Errorf(http.StatusBadRequest, "InvalidPayload", "dataFlows transformKql: %v", err)
			}
		}
	}
	return nil
}

func (s *Service) fillComponent(id arm.ID, old, props map[string]any) error {
	props["AppId"] = cmpOr(str(old, "AppId"), guid())
	ikey := cmpOr(str(old, "InstrumentationKey"), guid())
	props["InstrumentationKey"] = ikey
	props["ConnectionString"] = "InstrumentationKey=" + ikey + ";IngestionEndpoint=" + s.pathURL("appinsights/") + ";LiveEndpoint=" + s.pathURL("appinsights/")
	props["Name"] = id.Name()
	def(props, "Application_Type", "web")
	def(props, "Flow_Type", "Bluefield")
	def(props, "IngestionMode", "LogAnalytics")
	props["CreationDate"] = cmpOr(str(old, "CreationDate"), time.Now().UTC().Format(time.RFC3339))
	return nil
}

// ---- tables ----

func (t *table) index(name string) int {
	for i, c := range t.Cols {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// tableData finds or creates a table whose time column is timeCol; lock held.
func (s *Service) tableData(wkey, name, timeCol string) *table {
	if s.st.Data[wkey] == nil {
		s.st.Data[wkey] = map[string]*table{}
	}
	t := s.st.Data[wkey][strings.ToLower(name)]
	if t == nil {
		t = &table{Name: name, Cols: []col{{timeCol, "datetime"}}}
		s.st.Data[wkey][strings.ToLower(name)] = t
	}
	return t
}

// coerce converts a JSON-form or typed value to a column type; nil when it doesn't fit.
func coerce(v any, typ string) any {
	if v == nil {
		return nil
	}
	switch typ {
	case "string", "guid":
		return tostr(v)
	case "int", "long":
		return toLong(v)
	case "real":
		return toReal(v)
	case "bool":
		return toBool(v)
	case "datetime":
		return toTime(v)
	case "timespan":
		return toSpan(v)
	}
	return v
}

// stored is the JSON form of a typed value.
func stored(v any) any {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return int64(x)
	}
	return jsonVal(v)
}

// appendRows writes a transform's output into a workspace table.
func (s *Service) appendRows(wkey, name, timeCol string, out *rel, now time.Time) {
	t := s.tableData(wkey, name, timeCol)
	tg := -1
	for i, c := range out.cols {
		if c.Name == timeCol {
			tg = i
		} else if t.index(c.Name) < 0 && !t.Strict {
			t.Cols = append(t.Cols, c)
		}
	}
	for _, row := range out.rows {
		nr := make([]any, len(t.Cols))
		for i, tc := range t.Cols {
			for j, oc := range out.cols {
				if oc.Name == tc.Name {
					nr[i] = stored(coerce(row[j], tc.Type))
				}
			}
			if tc.Name == timeCol && (tg < 0 || nr[i] == nil) {
				nr[i] = stored(now)
			}
		}
		t.Rows = append(t.Rows, nr)
	}
}

// rel converts a stored table to typed values, keeping rows in the time range.
func (t *table) rel(from, to time.Time) *rel {
	r := &rel{cols: append([]col(nil), t.Cols...)}
	ti := -1
	for i, c := range t.Cols {
		if (c.Name == "TimeGenerated" || c.Name == "timestamp") && c.Type == "datetime" {
			ti = i
		}
	}
	for _, row := range t.Rows {
		nr := make([]any, len(t.Cols))
		for i, c := range t.Cols {
			if i < len(row) {
				nr[i] = coerce(row[i], c.Type)
			}
		}
		if ti >= 0 && (!from.IsZero() || !to.IsZero()) {
			ts, _ := nr[ti].(time.Time)
			if !from.IsZero() && ts.Before(from) || !to.IsZero() && !ts.Before(to) {
				continue
			}
		}
		r.rows = append(r.rows, nr)
	}
	return r
}

// ---- HTTP ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}

func fail(w http.ResponseWriter, c *core.Ctx, status int, code, msg string) {
	c.ErrCode = code
	writeJSON(w, status, map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

func failQuery(w http.ResponseWriter, c *core.Ctx, err error) {
	if ke, ok := err.(*kqlError); ok {
		c.ErrCode = ke.kind
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "BadArgumentError",
			"message": "The request had some invalid properties", "innererror": map[string]any{"code": ke.kind, "message": ke.msg}}})
		return
	}
	e := core.AsError(err)
	fail(w, c, e.Status, e.Code, e.Msg)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	kind, p := classify(r)
	c.Op = map[string]string{"ingest": "Upload", "query": "Query", "track": "Track", "metrics": "Metrics"}[kind]
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		fail(w, c, e.Status, e.Code, e.Msg)
		return
	}
	switch kind {
	case "ingest":
		s.serveIngest(w, r, c, p)
	case "query":
		s.serveQuery(w, r, c, p)
	case "track":
		s.serveTrack(w, r, c)
	default:
		s.serveMetrics(w, r, c, p)
	}
}

// body returns the request body, gunzipped when it was sent compressed.
func body(r *http.Request) ([]byte, error) {
	b := core.Body(r)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		// Read one byte past the cap so a gzip bomb is refused, not unpacked.
		out, err := io.ReadAll(io.LimitReader(zr, maxUnzipped+1))
		if err == nil && len(out) > maxUnzipped {
			err = errors.New("the unzipped body is over the 64 MiB limit")
		}
		return out, err
	}
	return b, nil
}

// maxUnzipped caps a gunzipped request body (64 MiB).
const maxUnzipped = 64 << 20

// ---- Logs Ingestion API ----

func (s *Service) serveIngest(w http.ResponseWriter, r *http.Request, c *core.Ctx, p string) {
	seg := strings.Split(strings.Trim(p, "/"), "/")
	if r.Method != http.MethodPost || len(seg) != 4 || seg[0] != "dataCollectionRules" || seg[2] != "streams" {
		fail(w, c, http.StatusNotFound, "NotFound", "pogi serves POST /dataCollectionRules/<immutableId>/streams/<stream>")
		return
	}
	raw, err := body(r)
	var rows []map[string]any
	if err != nil || json.Unmarshal(raw, &rows) != nil {
		fail(w, c, http.StatusBadRequest, "InvalidPayload", "The request body must be a JSON array of objects.")
		return
	}
	if e := s.ingest(seg[1], seg[3], rows); e != nil {
		fail(w, c, e.Status, e.Code, e.Msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ingest runs rows through the rule's data flow for a stream and stores the result.
func (s *Service) ingest(rule, stream string, rows []map[string]any) *core.Error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dcr map[string]any
	for _, res := range s.st.Res {
		if strings.EqualFold(str(res, "type"), typDCR) && str(res["properties"].(map[string]any), "immutableId") == rule {
			dcr = res["properties"].(map[string]any)
		}
	}
	if dcr == nil {
		return core.Errorf(http.StatusNotFound, "NotFound", "The data collection rule with immutable id '%s' was not found.", rule)
	}
	var flow map[string]any
	flows, _ := dcr["dataFlows"].([]any)
	for _, f := range flows {
		fm, _ := f.(map[string]any)
		ss, _ := fm["streams"].([]any)
		for _, x := range ss {
			if strings.EqualFold(fmt.Sprint(x), stream) && flow == nil {
				flow = fm
			}
		}
	}
	if flow == nil {
		return core.Errorf(http.StatusBadRequest, "InvalidStream", "The stream '%s' is not accepted by data collection rule '%s'.", stream, rule)
	}
	// the source relation: the declared columns, or what the rows have
	var src *rel
	if decl, ok := dcr["streamDeclarations"].(map[string]any); ok {
		for name, d := range decl {
			if dm, ok := d.(map[string]any); ok && strings.EqualFold(name, stream) {
				src = &rel{cols: schemaCols(map[string]any{"schema": dm})}
			}
		}
	}
	if src == nil {
		src = inferRel(rows)
	} else {
		for _, row := range rows {
			nr := make([]any, len(src.cols))
			for i, cl := range src.cols {
				nr[i] = coerce(row[cl.Name], cl.Type)
			}
			src.rows = append(src.rows, nr)
		}
	}
	q, err := parseKQL(cmpOr(str(flow, "transformKql"), "source"))
	if err != nil {
		return core.Errorf(http.StatusBadRequest, "InvalidTransform", "%v", err)
	}
	now := time.Now().UTC()
	out, err := q.run(&env{now: now, tables: func(n string) (*rel, bool) { return src, n == "source" }})
	if err != nil {
		return core.Errorf(http.StatusBadRequest, "InvalidTransform", "%v", err)
	}
	tname := cmpOr(str(flow, "outputStream"), stream) // Custom-X_CL is table X_CL, Microsoft-Syslog is Syslog
	for _, pre := range []string{"Custom-", "Microsoft-Table-", "Microsoft-"} {
		if t, ok := strings.CutPrefix(tname, pre); ok {
			tname = t
			break
		}
	}
	dests, _ := flow["destinations"].([]any)
	las, _ := sub(dcr, "destinations")["logAnalytics"].([]any)
	for _, dn := range dests {
		for _, d := range las {
			dm, _ := d.(map[string]any)
			if str(dm, "name") != fmt.Sprint(dn) {
				continue
			}
			s.appendRows(s.workspaceKey(str(dm, "workspaceResourceId"), str(dm, "workspaceId")), tname, "TimeGenerated", out, now)
		}
	}
	return nil
}

// workspaceKey is the data key of a destination: the workspace's lower-cased resource ID.
func (s *Service) workspaceKey(rid, customerID string) string {
	if rid != "" {
		return strings.ToLower(rid)
	}
	k, _ := s.findWorkspace(customerID)
	return k
}

// findWorkspace resolves a workspace by customer ID, resource ID or name.
func (s *Service) findWorkspace(id string) (string, bool) {
	if id == "" {
		return "", false
	}
	for k, res := range s.st.Res {
		if strings.EqualFold(str(res, "type"), typWorkspace) &&
			(strings.EqualFold(str(res["properties"].(map[string]any), "customerId"), id) || k == strings.ToLower(id) || strings.EqualFold(str(res, "name"), id)) {
			return k, true
		}
	}
	return "", false
}

// inferRel types a stream with no declaration from its JSON rows.
func inferRel(rows []map[string]any) *rel {
	r := &rel{}
	idx := map[string]int{}
	for _, row := range rows {
		for _, k := range core.SortedKeys(row) {
			v := row[k]
			t := "dynamic"
			switch x := v.(type) {
			case nil:
				t = ""
			case string:
				t = "string"
			case bool:
				t = "bool"
			case float64:
				t = "real"
				if x == float64(int64(x)) {
					t = "long"
				}
			}
			i, seen := idx[k]
			if !seen {
				idx[k] = len(r.cols)
				r.cols = append(r.cols, col{k, t})
			} else if c := &r.cols[i]; c.Type == "" || c.Type == "long" && t == "real" {
				c.Type = t
			}
		}
	}
	for i := range r.cols {
		r.cols[i].Type = cmpOr(r.cols[i].Type, "string")
	}
	for _, row := range rows {
		nr := make([]any, len(r.cols))
		for i, c := range r.cols {
			nr[i] = coerce(row[c.Name], c.Type)
		}
		r.rows = append(r.rows, nr)
	}
	return r
}

// ---- query API ----

var isoDur = regexp.MustCompile(`^P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

func parseISODuration(s string) (time.Duration, bool) {
	m := isoDur.FindStringSubmatch(s)
	if m == nil || s == "P" {
		return 0, false
	}
	var d time.Duration
	for i, unit := range []time.Duration{7 * 24 * time.Hour, 24 * time.Hour, time.Hour, time.Minute, time.Second} {
		if m[i+1] != "" {
			f, _ := strconv.ParseFloat(m[i+1], 64)
			d += time.Duration(f * float64(unit))
		}
	}
	return d, true
}

// parseTimespan reads an ISO 8601 duration (the last d before now) or interval (start/end,
// start/duration, duration/end); both zero means no limit.
func parseTimespan(s string, now time.Time) (from, to time.Time, ok bool) {
	if s == "" {
		return from, to, true
	}
	a, b, interval := strings.Cut(s, "/")
	if !interval {
		d, ok := parseISODuration(a)
		return now.Add(-d), now, ok
	}
	ta, errA := time.Parse(time.RFC3339Nano, a)
	tb, errB := time.Parse(time.RFC3339Nano, b)
	da, okA := parseISODuration(a)
	db, okB := parseISODuration(b)
	switch {
	case errA == nil && errB == nil:
		return ta, tb, true
	case errA == nil && okB:
		return ta, ta.Add(db), true
	case okA && errB == nil:
		return tb.Add(-da), tb, true
	}
	return from, to, false
}

type queryBody struct {
	Query    string `json:"query"`
	Timespan string `json:"timespan"`
}

func (s *Service) serveQuery(w http.ResponseWriter, r *http.Request, c *core.Ctx, p string) {
	seg := strings.Split(strings.Trim(p, "/"), "/")
	if len(seg) == 2 && seg[0] == "v1" && seg[1] == "$batch" && r.Method == http.MethodPost {
		s.serveBatch(w, r, c)
		return
	}
	if len(seg) != 4 || seg[0] != "v1" || seg[3] != "query" || seg[1] != "workspaces" && seg[1] != "apps" {
		fail(w, c, http.StatusNotFound, "PathNotFoundError", "The requested path does not exist")
		return
	}
	var qb queryBody
	switch r.Method {
	case http.MethodPost:
		raw, err := body(r)
		if err != nil || json.Unmarshal(raw, &qb) != nil {
			fail(w, c, http.StatusBadRequest, "BadArgumentError", "The request had some invalid properties")
			return
		}
	case http.MethodGet:
		qb = queryBody{r.URL.Query().Get("query"), r.URL.Query().Get("timespan")}
	default:
		fail(w, c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Use POST")
		return
	}
	res, status, err := s.query(seg[1], seg[2], qb)
	if err != nil {
		if status == http.StatusNotFound {
			fail(w, c, status, "PathNotFoundError", err.Error())
		} else {
			failQuery(w, c, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Service) serveBatch(w http.ResponseWriter, r *http.Request, c *core.Ctx) {
	var in struct {
		Requests []struct {
			ID        string    `json:"id"`
			Body      queryBody `json:"body"`
			Workspace string    `json:"workspace"`
		} `json:"requests"`
	}
	raw, err := body(r)
	if err != nil || json.Unmarshal(raw, &in) != nil {
		fail(w, c, http.StatusBadRequest, "BadArgumentError", "The request had some invalid properties")
		return
	}
	var out []map[string]any
	for _, q := range in.Requests {
		res, status, err := s.query("workspaces", q.Workspace, q.Body)
		entry := map[string]any{"id": q.ID, "status": 200, "body": res}
		if err != nil {
			entry["status"] = max(status, 400)
			entry["body"] = map[string]any{"error": map[string]any{"code": "BadArgumentError", "message": err.Error()}}
		}
		out = append(out, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"responses": out})
}

// query runs KQL against a workspace (or an Application Insights app) and returns the
// api.loganalytics.io response. status is 404 when the workspace is unknown.
func (s *Service) query(scope, id string, qb queryBody) (map[string]any, int, error) {
	s.mu.Lock()
	wkey, ok := "", false
	if scope == "apps" {
		for _, res := range s.st.Res {
			if strings.EqualFold(str(res, "type"), typComponent) && strings.EqualFold(str(res["properties"].(map[string]any), "AppId"), id) {
				wkey, ok = s.componentKey(res), true
			}
		}
	} else {
		wkey, ok = s.findWorkspace(id)
	}
	s.mu.Unlock()
	if !ok {
		return nil, http.StatusNotFound, fmt.Errorf("The workspace '%s' could not be found", id)
	}
	now := time.Now().UTC()
	from, to, tsOK := parseTimespan(qb.Timespan, now)
	if !tsOK {
		return nil, 400, syntaxf("The timespan '%s' is not a valid ISO 8601 duration or interval", qb.Timespan)
	}
	q, err := parseKQL(qb.Query)
	if err != nil {
		return nil, 400, err
	}
	rl, err := q.run(&env{now: now, tables: func(name string) (*rel, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		t := s.st.Data[wkey][strings.ToLower(name)]
		if t == nil {
			return nil, false
		}
		return t.rel(from, to), true
	}})
	if err != nil {
		return nil, 400, err
	}
	cols := make([]map[string]any, len(rl.cols))
	for i, cl := range rl.cols {
		cols[i] = map[string]any{"name": cl.Name, "type": cl.Type}
	}
	rows := make([][]any, len(rl.rows))
	for i, row := range rl.rows {
		rows[i] = make([]any, len(row))
		for j, v := range row {
			if v == nil && rl.cols[j].Type == "string" {
				v = ""
			}
			rows[i][j] = jsonVal(v)
		}
	}
	return map[string]any{"tables": []any{map[string]any{"name": "PrimaryResult", "columns": cols, "rows": rows}}}, 200, nil
}

// ---- Application Insights ----

// componentKey is where a component's telemetry lives: its workspace, or
// itself. Lock held.
func (s *Service) componentKey(res map[string]any) string {
	if ws := str(res["properties"].(map[string]any), "WorkspaceResourceId"); ws != "" {
		return strings.ToLower(ws)
	}
	return "app:" + strings.ToLower(str(res, "id"))
}

func (s *Service) serveTrack(w http.ResponseWriter, r *http.Request, c *core.Ctx) {
	raw, err := body(r)
	if err != nil {
		fail(w, c, http.StatusBadRequest, "BadRequest", "bad body")
		return
	}
	var items []map[string]any
	if t := strings.TrimSpace(string(raw)); strings.HasPrefix(t, "[") {
		json.Unmarshal([]byte(t), &items)
	} else {
		dec := json.NewDecoder(strings.NewReader(t))
		for {
			var m map[string]any
			if dec.Decode(&m) != nil {
				break
			}
			items = append(items, m)
		}
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []map[string]any
	accepted := 0
	for i, it := range items {
		ikey := str(it, "iKey")
		var comp map[string]any
		for _, res := range s.st.Res {
			if strings.EqualFold(str(res, "type"), typComponent) && str(res["properties"].(map[string]any), "InstrumentationKey") == ikey {
				comp = res
			}
		}
		if comp == nil {
			errs = append(errs, map[string]any{"index": i, "statusCode": 400, "message": "Invalid instrumentation key"})
			continue
		}
		accepted++
		data, _ := it["data"].(map[string]any)
		base, _ := data["baseData"].(map[string]any)
		tags, _ := it["tags"].(map[string]any)
		ts, ok := parseTime(str(it, "time"))
		if !ok {
			ts = now
		}
		dims, _ := base["properties"].(map[string]any)
		common := func(r *rel) {
			r.cols = append(r.cols, col{"timestamp", "datetime"})
			r.rows = [][]any{{ts}}
		}
		out := &rel{}
		common(out)
		add := func(n, t string, v any) {
			out.cols = append(out.cols, col{n, t})
			out.rows[0] = append(out.rows[0], v)
		}
		var name string
		switch str(data, "baseType") {
		case "RequestData":
			name = "requests"
			add("id", "string", str(base, "id"))
			add("name", "string", str(base, "name"))
			add("url", "string", str(base, "url"))
			add("success", "bool", base["success"])
			add("resultCode", "string", str(base, "responseCode"))
			add("duration", "real", durationMs(str(base, "duration")))
		case "MessageData":
			name = "traces"
			add("message", "string", str(base, "message"))
			add("severityLevel", "int", toLong(base["severityLevel"]))
		default:
			continue // ponytail: only requests and traces are stored; dependencies, exceptions, events and metrics are accepted and dropped
		}
		add("itemCount", "int", int64(1))
		add("operation_Id", "string", str(tags, "ai.operation.id"))
		add("cloud_RoleName", "string", str(tags, "ai.cloud.role"))
		add("customDimensions", "dynamic", dims)
		s.appendRows(s.componentKey(comp), name, "timestamp", out, now)
	}
	status := http.StatusOK
	if len(errs) > 0 {
		status = http.StatusPartialContent
	}
	if errs == nil {
		errs = []map[string]any{}
	}
	writeJSON(w, status, map[string]any{"itemsReceived": len(items), "itemsAccepted": accepted, "errors": errs})
}

// durationMs reads App Insights' "d.hh:mm:ss.fffffff" timespan as milliseconds.
func durationMs(s string) any {
	days := 0.0
	if d, rest, ok := strings.Cut(s, "."); ok && !strings.Contains(d, ":") {
		days, _ = strconv.ParseFloat(d, 64)
		s = rest
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return nil
	}
	h, _ := strconv.ParseFloat(parts[0], 64)
	m, _ := strconv.ParseFloat(parts[1], 64)
	sec, _ := strconv.ParseFloat(parts[2], 64)
	return ((days*24+h)*60+m)*60*1000 + sec*1000
}

// ---- metrics ----

// serveMetrics answers metrics and metricDefinitions for any resource. pogi
// records no metrics, so series are zeros in the right shape.
func (s *Service) serveMetrics(w http.ResponseWriter, r *http.Request, c *core.Ctx, p string) {
	i := strings.LastIndex(strings.ToLower(p), "/providers/microsoft.insights/")
	resource, what := p[:i], strings.ToLower(p[i+len("/providers/microsoft.insights/"):])
	if what == "metricdefinitions" {
		writeJSON(w, http.StatusOK, map[string]any{"value": []any{}})
		return
	}
	q := r.URL.Query()
	now := time.Now().UTC().Truncate(time.Minute)
	from, to, ok := parseTimespan(q.Get("timespan"), now)
	if !ok || from.IsZero() {
		from, to = now.Add(-time.Hour), now
	}
	step := time.Minute
	if iv := q.Get("interval"); iv != "" {
		if d, ok := parseISODuration(iv); ok && d > 0 {
			step = d
		}
	}
	var aggs []string
	for _, a := range strings.Split(cmpOr(q.Get("aggregation"), "Average"), ",") {
		aggs = append(aggs, strings.ToLower(strings.TrimSpace(a)))
	}
	var points []any
	for t := from; t.Before(to) && len(points) < 1440; t = t.Add(step) {
		pt := map[string]any{"timeStamp": t.UTC().Format("2006-01-02T15:04:05Z")}
		for _, a := range aggs {
			pt[a] = 0
		}
		points = append(points, pt)
	}
	ns := ""
	if id, ok := arm.ParseID(resource); ok {
		ns = id.Type()
	}
	var value []any
	for _, n := range strings.Split(q.Get("metricnames"), ",") {
		if n = strings.TrimSpace(n); n == "" {
			continue
		}
		value = append(value, map[string]any{"id": resource + "/providers/Microsoft.Insights/metrics/" + n, "type": "Microsoft.Insights/metrics",
			"name": map[string]any{"value": n, "localizedValue": n}, "displayDescription": "", "unit": "Count", "errorCode": "Success",
			"timeseries": []any{map[string]any{"metadatavalues": []any{}, "data": points}}})
	}
	if value == nil {
		value = []any{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"cost": 0, "timespan": from.UTC().Format(time.RFC3339) + "/" + to.UTC().Format(time.RFC3339),
		"interval": cmpOr(q.Get("interval"), "PT1M"), "namespace": ns, "resourceregion": "eastus", "value": value})
}

// ---- state and console ----

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
	if st.Res == nil {
		st.Res = map[string]map[string]any{}
	}
	if st.Data == nil {
		st.Data = map[string]map[string]*table{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := core.Table{Name: "Workspaces", Columns: []string{"Name", "Customer ID", "Location", "Tables", "Rows"}}
	tb := core.Table{Name: "Tables", Columns: []string{"Table", "Workspace", "Columns", "Rows"}}
	other := core.Table{Name: "Resources", Columns: []string{"ID", "Type", "Name"}}
	for _, k := range core.SortedKeys(s.st.Res) {
		res := s.st.Res[k]
		switch typ := str(res, "type"); {
		case strings.EqualFold(typ, typWorkspace):
			n := 0
			for _, t := range s.st.Data[k] {
				n += len(t.Rows)
			}
			ws.Rows = append(ws.Rows, []string{str(res, "name"), str(res["properties"].(map[string]any), "customerId"), str(res, "location"),
				strconv.Itoa(len(s.st.Data[k])), strconv.Itoa(n)})
		case !strings.EqualFold(typ, typTable):
			other.Rows = append(other.Rows, []string{str(res, "id"), typ, str(res, "name")})
		}
	}
	for _, k := range core.SortedKeys(s.st.Data) {
		for _, tn := range core.SortedKeys(s.st.Data[k]) {
			t := s.st.Data[k][tn]
			tb.Rows = append(tb.Rows, []string{k + "|" + t.Name, k, strconv.Itoa(len(t.Cols)), strconv.Itoa(len(t.Rows))})
		}
	}
	return []core.Table{ws, tb, other}
}

func (s *Service) Inspect(tbl, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch tbl {
	case "Tables":
		k, name, _ := strings.Cut(id, "|")
		if t := s.st.Data[k][strings.ToLower(name)]; t != nil {
			rows := t.Rows
			if len(rows) > 100 {
				rows = rows[len(rows)-100:]
			}
			return map[string]any{"name": t.Name, "columns": t.Cols, "rowCount": len(t.Rows), "lastRows": rows}
		}
	default:
		for k, res := range s.st.Res {
			if str(res, "id") == id || k == strings.ToLower(id) || strings.EqualFold(str(res, "name"), id) {
				return res
			}
		}
	}
	return nil
}
