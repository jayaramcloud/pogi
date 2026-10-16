package generic

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/amp"
	"github.com/nrndev/pogi/internal/core"
)

// Managed Prometheus: the workspaces, rule groups and alert manager definitions are the engine's. The data
// plane is package amp: a signed (service "aps") request to /workspaces/<id>/api/v1/remote_write stores
// samples, and /api/v1/query, query_range, labels, label/<name>/values and series answer from them.
// A workspace's prometheusEndpoint is that URL on pogi's own address.
//
// ponytail: Prometheus clients that do not sign their requests are not routed here; sign them with
// SigV4 for "aps", as they must for the real service.
func init() {
	Raw("amp", ampServe)
	arnTemplates["amp/RuleGroupsNamespace"] = "rulegroupsnamespace/{workspaceId}/{name}"
	arnTemplates["amp/AnomalyDetector"] = "anomalydetector/{workspaceId}/{anomalyDetectorId}"
	OnClose("amp", func(s *Service) { ampStores.Delete(s) })

	// every operation on a workspace's children needs the workspace
	for _, op := range []string{"CreateAlertManagerDefinition", "DescribeAlertManagerDefinition", "PutAlertManagerDefinition", "DeleteAlertManagerDefinition",
		"CreateRuleGroupsNamespace", "DescribeRuleGroupsNamespace", "PutRuleGroupsNamespace", "DeleteRuleGroupsNamespace", "ListRuleGroupsNamespaces",
		"CreateLoggingConfiguration", "DescribeLoggingConfiguration", "UpdateLoggingConfiguration", "DeleteLoggingConfiguration",
		"CreateQueryLoggingConfiguration", "DescribeQueryLoggingConfiguration", "UpdateQueryLoggingConfiguration", "DeleteQueryLoggingConfiguration",
		"CreateAnomalyDetector", "DescribeAnomalyDetector", "PutAnomalyDetector", "DeleteAnomalyDetector", "ListAnomalyDetectors",
		"PutResourcePolicy", "DescribeResourcePolicy", "DeleteResourcePolicy", "DescribeWorkspaceConfiguration", "UpdateWorkspaceConfiguration"} {
		Override("amp", op, func(c *Call) (map[string]any, error) {
			if c.Get("Workspace", str(c.In["workspaceId"])) == nil {
				return nil, c.NotFound("Workspace %s not found", str(c.In["workspaceId"]))
			}
			return c.Default()
		})
	}
	Override("amp", "CreateWorkspace", func(c *Call) (map[string]any, error) {
		c.In["workspaceId"] = "ws-" + core.UUID()
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		out["status"] = map[string]any{"statusCode": "CREATING"}
		return out, nil
	})
	Override("amp", "DescribeWorkspace", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		if w := s2Map(out["workspace"]); c.Core != nil {
			w["prometheusEndpoint"] = "http://" + c.Core.Host + "/workspaces/" + str(w["workspaceId"]) + "/"
		}
		return out, nil
	})
	Override("amp", "UpdateWorkspaceAlias", func(c *Call) (map[string]any, error) {
		r := c.Get("Workspace", str(c.In["workspaceId"]))
		if r == nil {
			return nil, c.NotFound("Workspace %s not found", str(c.In["workspaceId"]))
		}
		r.Data["alias"] = c.In["alias"]
		c.Put(r)
		return map[string]any{}, nil
	})
	Hook("amp", "DeleteWorkspace", func(c *Call) {
		id := c.Res.IDs["workspaceId"]
		for _, r := range c.Svc.children(c.Account, c.Region, "workspaceId", id) { // rule groups, alert manager, logging, ...
			c.Svc.Delete(r.Account, r.Region, r.Type, r.Key)
		}
		ampStore(c.Svc).Delete(id)
	})
}

// children are the resources of an account whose identifier member is id, other than the one that owns it.
func (s *Service) children(account, region, member, id string) []*Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Resource
	for _, r := range s.st.Res {
		if r.Account == account && r.Region == s.region(region) && r.IDs[member] == id && r.Type != "Workspace" {
			out = append(out, r.clone())
		}
	}
	return out
}

var ampStores sync.Map // *Service → *amp.Store

func ampStore(s *Service) *amp.Store {
	v, _ := ampStores.LoadOrStore(s, amp.New())
	return v.(*amp.Store)
}

var ampPath = regexp.MustCompile(`^/workspaces/([^/]+)/api/v1/(remote_write|query|query_range|labels|series|label/([^/]+)/values)$`)

// ampServe answers the Prometheus API of a workspace.
func ampServe(s *Service, w http.ResponseWriter, r *http.Request) bool {
	m := ampPath.FindStringSubmatch(r.URL.Path)
	if m == nil {
		return false
	}
	c := core.C(r)
	c.Op = map[string]string{"remote_write": "RemoteWrite", "query": "QueryMetrics", "query_range": "QueryMetrics", "labels": "GetLabels", "series": "GetSeries"}[m[2]]
	if c.Op == "" {
		c.Op = "GetLabels"
	}
	if err := c.Intercept(); err != nil {
		e, _ := err.(*core.Error)
		ampError(w, e.Status, e.Code, e.Msg)
		return true
	}
	ws := m[1]
	if s.Get(c.Account, c.Region, "Workspace", ws) == nil {
		ampError(w, http.StatusNotFound, "not_found", "workspace "+ws+" not found")
		return true
	}
	st := ampStore(s)
	if m[2] == "remote_write" {
		if r.Method != http.MethodPost {
			ampError(w, http.StatusMethodNotAllowed, "bad_data", "remote write is a POST")
			return true
		}
		if _, err := st.Write(ws, core.Body(r)); err != nil {
			ampError(w, http.StatusBadRequest, "bad_data", err.Error())
			return true
		}
		w.WriteHeader(http.StatusOK)
		return true
	}
	r.ParseForm() // the query string, or the form of a POST
	q := r.Form
	var data any
	switch m[2] {
	case "query":
		t, err := ampTime(q.Get("time"), time.Now())
		if err != nil {
			ampError(w, http.StatusBadRequest, "bad_data", err.Error())
			return true
		}
		res, matrix, err := st.Query(ws, q.Get("query"), t)
		if err != nil {
			ampError(w, http.StatusBadRequest, "bad_data", err.Error())
			return true
		}
		data = map[string]any{"resultType": map[bool]string{false: "vector", true: "matrix"}[matrix], "result": ampResults(res, matrix)}
	case "query_range":
		now := time.Now()
		start, e1 := ampTime(q.Get("start"), now)
		end, e2 := ampTime(q.Get("end"), now)
		step, e3 := ampStep(q.Get("step"))
		if err := firstErr(e1, e2, e3); err != nil {
			ampError(w, http.StatusBadRequest, "bad_data", err.Error())
			return true
		}
		res, err := st.QueryRange(ws, q.Get("query"), start, end, step)
		if err != nil {
			ampError(w, http.StatusBadRequest, "bad_data", err.Error())
			return true
		}
		data = map[string]any{"resultType": "matrix", "result": ampResults(res, true)}
	case "labels":
		data = append([]string{}, st.Labels(ws, "")...)
	case "series":
		series, err := st.SeriesOf(ws, q.Get("match[]"))
		if err != nil {
			ampError(w, http.StatusBadRequest, "bad_data", err.Error())
			return true
		}
		data = series
	default:
		data = append([]string{}, st.Labels(ws, m[3])...)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": data})
	return true
}

func ampError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"status": "error", "errorType": typ, "error": msg})
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// ampTime reads a Prometheus time: unix seconds (fractions allowed) or RFC 3339; ms.
func ampTime(s string, def time.Time) (int64, error) {
	if s == "" {
		return def.UnixMilli(), nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f * 1000), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	return t.UnixMilli(), err
}

func ampStep(s string) (int64, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f * 1000), nil
	}
	d, err := time.ParseDuration(s)
	return d.Milliseconds(), err
}

func ampResults(res []amp.Result, matrix bool) []any {
	out := make([]any, 0, len(res))
	for _, r := range res {
		item := map[string]any{"metric": r.Metric}
		pt := func(p amp.Point) []any { return []any{float64(p.T) / 1000, strconv.FormatFloat(p.V, 'f', -1, 64)} }
		if matrix {
			vals := make([]any, 0, len(r.Points))
			for _, p := range r.Points {
				vals = append(vals, pt(p))
			}
			item["values"] = vals
		} else {
			item["value"] = pt(r.Points[0])
		}
		out = append(out, item)
	}
	return out
}
