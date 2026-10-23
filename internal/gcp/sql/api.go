package sql

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// req is one call: the project, the instance and the collection item the path names.
type req struct {
	r       *http.Request
	project string
	inst    string // instances/<inst>
	item    string // .../<collection>/<item>
	body    []byte
}

// base is the URL of the API as the client reached it, for selfLink and targetLink.
func (q *req) base() string {
	scheme := "http"
	if q.r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + q.r.Host + "/sql/v1beta4"
}

// into decodes the JSON body into v (an empty body leaves it as it is).
func (q *req) into(v any) error {
	if len(strings.TrimSpace(string(q.body))) == 0 {
		return nil
	}
	if err := json.Unmarshal(q.body, v); err != nil {
		return fail(400, "invalidRequest", "Invalid JSON payload received. %s", core.PlainJSON(err))
	}
	return nil
}

func (q *req) key() string { return q.project + "/" + q.inst }

type handler func(*Service, *req) (any, error)

// routes is keyed "VERB shape[:verb]": the path after projects/<p>/ with every id replaced by "*".
var routes = map[string]handler{
	"GET instances":                          (*Service).list,
	"POST instances":                         (*Service).insert,
	"GET instances/*":                        (*Service).get,
	"PATCH instances/*":                      (*Service).patch,
	"PUT instances/*":                        (*Service).update,
	"DELETE instances/*":                     (*Service).delete,
	"POST instances/*/restart":               (*Service).restart,
	"POST instances/*/clone":                 (*Service).cloneInstance,
	"POST instances/*/export":                (*Service).export,
	"POST instances/*/import":                (*Service).importSQL,
	"GET instances/*/connectSettings":        (*Service).connectSettings,
	"POST instances/*:generateEphemeralCert": (*Service).generateEphemeralCert,
	"POST instances/*/createEphemeral":       (*Service).createEphemeral,
	"GET instances/*/databases":              (*Service).listDatabases,
	"POST instances/*/databases":             (*Service).insertDatabase,
	"GET instances/*/databases/*":            (*Service).getDatabase,
	"PATCH instances/*/databases/*":          (*Service).updateDatabase,
	"PUT instances/*/databases/*":            (*Service).updateDatabase,
	"DELETE instances/*/databases/*":         (*Service).deleteDatabase,
	"GET instances/*/users":                  (*Service).listUsers,
	"POST instances/*/users":                 (*Service).insertUser,
	"PUT instances/*/users":                  (*Service).updateUser,
	"DELETE instances/*/users":               (*Service).deleteUser,
	"GET instances/*/users/*":                (*Service).getUser,
	"GET instances/*/sslCerts":               (*Service).listCerts,
	"POST instances/*/sslCerts":              (*Service).insertCert,
	"GET instances/*/sslCerts/*":             (*Service).getCert,
	"DELETE instances/*/sslCerts/*":          (*Service).deleteCert,
	"GET operations":                         (*Service).listOps,
	"GET operations/*":                       (*Service).getOp,
	"GET tiers":                              (*Service).tiers,
	"GET flags":                              (*Service).flags,
}

// trim returns the path after the API version, and whether the version prefix
// is Cloud SQL's own: v1 clients send /v1/projects/..., which other APIs share.
func trim(p string) (rest string, own, ok bool) {
	for _, pre := range []string{"/sql/v1beta4/", "/sql/v1/"} {
		if r, ok := strings.CutPrefix(p, pre); ok {
			return r, true, true
		}
	}
	r, ok := strings.CutPrefix(p, "/v1/")
	return r, false, ok
}

func (s *Service) match(r *http.Request) bool {
	rest, own, ok := trim(r.URL.Path)
	if !ok {
		return false
	}
	if own {
		return true
	}
	p := strings.Split(rest, "/")
	if len(p) < 3 || p[0] != "projects" {
		return false
	}
	switch c, _, _ := strings.Cut(p[2], ":"); c {
	case "instances", "tiers":
		return true
	case "operations": // the shared operations API polls the others'
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(p) == 4 && s.st.Ops[p[1]+"/"+p[3]] != nil
	}
	return false
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	rest, _, _ := trim(r.URL.Path)
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	q := &req{r: r, body: core.Body(r)}
	if parts[0] == "projects" && len(parts) >= 2 {
		q.project, parts = parts[1], parts[2:]
	}
	verb := ""
	if n := len(parts); n > 0 {
		if last, v, cut := strings.Cut(parts[n-1], ":"); cut {
			parts[n-1], verb = last, ":"+v
		}
	}
	shape := slices.Clone(parts)
	for i := 1; i < len(shape); i += 2 {
		shape[i] = "*"
	}
	if parts[0] == "instances" && len(parts) >= 2 {
		q.inst = parts[1]
	}
	if parts[0] == "instances" && len(parts) >= 4 {
		q.item = parts[3]
	}
	if parts[0] == "operations" && len(parts) >= 2 {
		q.item = parts[1]
	}
	h := routes[r.Method+" "+strings.Join(shape, "/")+verb]
	c.Op = r.Method + ":" + strings.Join(shape, "/") + verb
	if h == nil || q.project == "" && parts[0] != "flags" {
		writeErr(w, c, fail(501, "notImplemented", "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeErr(w, c, fail(e.Status, e.Code, "%s", e.Msg))
		return
	}
	if !gapi.PageSizeOK(r.URL.Query().Get("maxResults")) {
		writeErr(w, c, fail(400, "invalid", "Invalid value for maxResults: must be a whole number, 0 or more"))
		return
	}
	out, err := h(s, q)
	if err != nil {
		var ae *apiError
		if !asAPI(err, &ae) {
			ae = fail(500, "backendError", "%v", err)
		}
		writeErr(w, c, ae)
		return
	}
	s.hub.MarkDirty()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	json.NewEncoder(w).Encode(out)
}

func asAPI(err error, target **apiError) bool {
	e, ok := err.(*apiError)
	if ok {
		*target = e
	}
	return ok
}
