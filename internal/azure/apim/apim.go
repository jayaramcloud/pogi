// Package apim emulates Azure API Management: the management plane (services, APIs with operations, policies and
// schemas, backends, products with their APIs and policies, subscriptions with keys, named values, and the service-level
// policy) as resource-manager Providers, and the gateway that serves the APIs.
//
// The gateway of a service is https://<service>.azure-api.localhost:7644/<api path>/..., or on any host under
// /_azure/apim/<service>/<api path>/.... A request finds its API by path and its operation by method and URL template;
// the subscription key (the Ocp-Apim-Subscription-Key header or the subscription-key query parameter, or the names the API
// sets) is checked when the API requires one; the policies of the service, the product of the key, the API and the operation
// run in that order, honouring <base/>; the request is forwarded to the API's serviceUrl (or what set-backend-service chose).
// Policies: set-header, set-query-parameter, rewrite-uri, set-backend-service, rate-limit, rate-limit-by-key, validate-jwt,
// return-response, mock-response, cors and set-body (see policy.go for what the expressions can do).
// ponytail: APIs are not imported from OpenAPI or WSDL documents (create operations by hand), on-error sections do not run,
// revisions and versions sets are plain APIs, and the other policies are skipped.
package apim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

const ns = "Microsoft.ApiManagement/service"

// Service is the API Management emulator.
type Service struct {
	h      *core.Hub
	mu     sync.Mutex
	st     state
	limits map[string]*window // rate-limit counters, not persisted
}

type state struct {
	ARM armres.Store `json:"arm,omitempty"` // the resources as the resource manager has them
}

// New creates the API Management service.
func New(h *core.Hub) *Service {
	s := &Service{h: h}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "apim", Cloud: core.Azure, Title: "API Management", Protocol: "azure-json", Match: func(r *http.Request) bool {
		_, _, ok := target(r)
		return ok
	}}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st, s.limits = state{}, map[string]*window{}
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
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// Resources lists the services, APIs and products; a row is identified by its resource ID.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Table
	for _, t := range []struct{ name, typ, col string }{{"Services", ns, "Gateway"}, {"APIs", ns + "/apis", "Path"}, {"Products", ns + "/products", "State"}} {
		tab := core.Table{Name: t.name, Columns: []string{"ID", "Name", t.col}}
		for _, k := range core.SortedKeys(s.st.ARM) {
			r := s.st.ARM[k]
			if id, ok := arm.ParseID(k); ok && strings.EqualFold(id.Type(), t.typ) {
				p := armres.Props(r)
				v := armres.Str(p, map[string]string{"Services": "gatewayUrl", "APIs": "path", "Products": "state"}[t.name])
				if t.name == "Services" {
					v = "https://" + id.Name() + ".azure-api.localhost"
				}
				tab.Rows = append(tab.Rows, []string{armres.Str(r, "id"), armres.Str(r, "name"), v})
			}
		}
		out = append(out, tab)
	}
	return out
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if aid, ok := arm.ParseID(id); ok {
		r, _ := s.st.ARM.Get(aid)
		return r
	}
	return nil
}

// ---- lookups ----

// get returns a stored resource, or nil.
func (s *Service) get(id arm.ID) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, _ := s.st.ARM.Get(id)
	return r
}

// children are the resources of a type directly under parent; typ is what follows the service's type.
func (s *Service) children(parent arm.ID, typ string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.ARM.List(parent, ns+typ)
}

// service finds a service by name.
func (s *Service) service(name string) (arm.ID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.st.ARM {
		if id, ok := arm.ParseID(k); ok && strings.EqualFold(id.Type(), ns) && strings.EqualFold(id.Name(), name) {
			return id, true
		}
	}
	return arm.ID{}, false
}

// policy is the parsed policy of an entity (id is the entity), or nil.
func (s *Service) policy(entity arm.ID, scope string) sections {
	p := armres.Props(s.get(entity.Child("policies", "policy")))
	doc := armres.Str(p, "value")
	if strings.HasSuffix(armres.Str(p, "format"), "-link") || doc == "" { // ponytail: policies by link are not fetched
		return nil
	}
	sec, _ := parsePolicy(doc, scope)
	return sec
}

// namedValue is the value of a named value, found by its display name or its name.
func (s *Service) namedValue(svc, name string) (string, bool) {
	id, _ := arm.ParseID(svc)
	for _, r := range s.children(id, "/namedValues") {
		p := armres.Props(r)
		if strings.EqualFold(armres.Str(p, "displayName"), name) || strings.EqualFold(armres.Str(r, "name"), name) {
			return armres.Str(p, "value"), true
		}
	}
	return "", false
}

// backend is the properties of a backend entity, or nil.
func (s *Service) backend(svc, name string) map[string]any {
	id, _ := arm.ParseID(svc)
	if r := s.get(id.Child("backends", name)); r != nil {
		return armres.Props(r)
	}
	return nil
}

// ---- the gateway ----

// target finds the service a request is for: the host <name>.azure-api.localhost, or the path /_azure/apim/<name>; path is
// what remains of the URL path.
func target(r *http.Request) (name, path string, ok bool) {
	if rest, ok := strings.CutPrefix(r.URL.Path, "/_azure/apim/"); ok {
		name, path, _ = strings.Cut(rest, "/")
		return name, "/" + path, name != ""
	}
	label, base := core.SplitHost(r.Host, "")
	ok = label != "" && strings.HasPrefix(base, "azure-api.") && strings.HasSuffix(base, "localhost")
	return label, r.URL.Path, ok
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "Invoke"
	name, path, _ := target(r)
	rep := s.gateway(c, r, name, path)
	if rep.status >= 400 {
		c.ErrCode = strings.ReplaceAll(http.StatusText(rep.status), " ", "")
	}
	for k, v := range rep.hdr {
		w.Header()[k] = v
	}
	w.WriteHeader(rep.status)
	w.Write(rep.body)
}

func str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

func boolOr(m map[string]any, k string, def bool) bool {
	if b, ok := m[k].(bool); ok {
		return b
	}
	return def
}

func (s *Service) gateway(c *core.Ctx, r *http.Request, name, path string) *reply {
	if err := c.Intercept(); err != nil {
		return errReply(core.AsError(err).Status, err.Error())
	}
	notFound := errReply(http.StatusNotFound, "Resource not found")
	svc, ok := s.service(name)
	if !ok {
		return notFound
	}
	apiID, api, rest := s.matchAPI(svc, path)
	if api == nil {
		return notFound
	}
	opID, op, params, keepQ := s.matchOp(apiID, rest, r.Method)

	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	e := &exec{s: s, svc: svc.String(), r: r, method: r.Method, path: rest, query: r.URL.Query(), hdr: r.Header.Clone(), body: core.Body(r),
		params: params, keepQ: keepQ, op: armres.Props(op), ip: ip, backend: str(api, "serviceUrl")}
	global := s.policy(svc, "service")
	apiPol := s.policy(apiID, "api/"+apiID.Name())

	if op == nil { // a preflight has no operation; the cors policy of the API answers it
		if r.Method == http.MethodOptions {
			var levels []sections
			for _, p := range []sections{global, apiPol} {
				if p != nil {
					levels = append(levels, p)
				}
			}
			for _, n := range compose(levels, len(levels)-1, "inbound") {
				if n.name == "cors" {
					if rep := e.corsInbound(n); rep != nil {
						return e.finish(rep)
					}
				}
			}
		}
		return notFound
	}
	c.Op = apiID.Name() + "/" + opID.Name()

	sub, product, rep := s.authorize(svc, apiID, api, r)
	if rep != nil {
		return rep
	}
	e.sub = sub

	levels := []sections{defaultPolicy}
	for _, p := range []sections{global, s.policy(svc.Child("products", product), "product/"+product), apiPol, s.policy(opID, "operation/"+opID.Name())} {
		if p != nil {
			levels = append(levels, p)
		}
	}
	top := len(levels) - 1
	if rep, _ := e.inbound(compose(levels, top, "inbound")); rep != nil {
		return e.finish(rep)
	}
	forward := false
	for _, n := range compose(levels, top, "backend") {
		forward = forward || n.name == "forward-request"
	}
	if !forward {
		e.status, e.rhdr = http.StatusOK, http.Header{}
	} else if err := e.forward(); err != nil {
		return e.finish(errReply(http.StatusInternalServerError, err.Error()))
	}
	if rep, _ := e.outbound(compose(levels, top, "outbound")); rep != nil {
		return e.finish(rep)
	}
	return e.finish(&reply{e.status, e.rhdr, e.rbody})
}

// defaultPolicy is what a service's policies stand on: Azure gives every service a global policy that forwards the request.
var defaultPolicy = sections{"backend": {{name: "forward-request"}}}

// finish adds what the policies promised every response: the cors headers, and the names of the policies pogi skipped.
func (e *exec) finish(rep *reply) *reply {
	if rep.hdr == nil {
		rep.hdr = http.Header{}
	}
	e.corsHeaders(rep.hdr)
	if len(e.ignored) > 0 {
		rep.hdr.Set("X-Pogi-Apim-Ignored", strings.Join(e.ignored, ","))
	}
	return rep
}

// matchAPI finds the API with the longest path that starts the request path; rest is what follows it.
func (s *Service) matchAPI(svc arm.ID, path string) (id arm.ID, props map[string]any, rest string) {
	p, best := strings.TrimPrefix(path, "/"), -1
	for _, a := range s.children(svc, "/apis") {
		ap := strings.Trim(str(armres.Props(a), "path"), "/")
		var r string
		switch {
		case ap == "":
			r = p
		case strings.EqualFold(p, ap):
		case len(p) > len(ap) && strings.EqualFold(p[:len(ap)+1], ap+"/"):
			r = p[len(ap)+1:]
		default:
			continue
		}
		if len(ap) > best {
			best, id, props, rest = len(ap), svc.Child("apis", str(a, "name")), armres.Props(a), "/"+r
		}
	}
	return
}

// matchOp finds the operation of an API by method and URL template; the one with the most literal segments wins.
func (s *Service) matchOp(api arm.ID, path, method string) (id arm.ID, op map[string]any, params map[string]string, keepQ map[string]bool) {
	best := -1
	for _, o := range s.children(api, "/apis/operations") {
		p := armres.Props(o)
		if !strings.EqualFold(str(p, "method"), method) {
			continue
		}
		pr, lit, ok := matchTemplate(str(p, "urlTemplate"), path)
		if ok && lit > best {
			best, id, op, params = lit, api.Child("operations", str(o, "name")), o, pr
			keepQ = map[string]bool{}
			if _, q, _ := strings.Cut(str(p, "urlTemplate"), "?"); q != "" {
				for _, kv := range strings.Split(q, "&") {
					k, _, _ := strings.Cut(kv, "=")
					keepQ[k] = true
				}
			}
		}
	}
	return
}

// matchTemplate matches a URL template (/items/{id}, a trailing * or {*rest} for the remainder) with a path.
func matchTemplate(tmpl, path string) (params map[string]string, literals int, ok bool) {
	tmpl, _, _ = strings.Cut(tmpl, "?")
	tp, pp := strings.Split(strings.Trim(tmpl, "/"), "/"), strings.Split(strings.Trim(path, "/"), "/")
	params = map[string]string{}
	for i, seg := range tp {
		switch {
		case seg == "*" || strings.HasPrefix(seg, "{*") && strings.HasSuffix(seg, "}") && i == len(tp)-1:
			if i > len(pp) {
				return nil, 0, false
			}
			params[strings.Trim(seg, "{*}")] = strings.Join(pp[i:], "/")
			return params, literals, true
		case i >= len(pp):
			return nil, 0, false
		case strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}"):
			if pp[i] == "" {
				return nil, 0, false
			}
			params[seg[1:len(seg)-1]] = pp[i]
		case strings.EqualFold(seg, pp[i]):
			literals++
		default:
			return nil, 0, false
		}
	}
	return params, literals, len(tp) == len(pp)
}

// authorize checks the subscription key of a request to an API. It returns the subscription's name and, when it is a
// product subscription, the product, or a reply that ends the request.
func (s *Service) authorize(svc, apiID arm.ID, api map[string]any, r *http.Request) (sub, product string, rep *reply) {
	hdr, query := "Ocp-Apim-Subscription-Key", "subscription-key"
	if n, ok := api["subscriptionKeyParameterNames"].(map[string]any); ok {
		hdr, query = armres.Str(n, "header"), armres.Str(n, "query")
	}
	key := r.Header.Get(hdr)
	if key == "" {
		key = r.URL.Query().Get(query)
	}
	if key != "" {
		for _, sr := range s.children(svc, "/subscriptions") {
			p := armres.Props(sr)
			if st := str(p, "state"); st != "" && st != "active" || key != str(p, "primaryKey") && key != str(p, "secondaryKey") {
				continue
			}
			if ok, prod := s.covers(svc, apiID, str(p, "scope")); ok {
				return str(sr, "name"), prod, nil
			}
		}
	}
	if !boolOr(api, "subscriptionRequired", true) {
		return "", "", nil
	}
	if key == "" {
		return "", "", errReply(http.StatusUnauthorized, "Access denied due to missing subscription key. Make sure to include subscription key when making requests to an API.")
	}
	return "", "", errReply(http.StatusUnauthorized, "Access denied due to invalid subscription key. Make sure to provide a valid key for an active subscription.")
}

// covers reports whether a subscription's scope includes the API: the whole service ("/" or ""), all APIs ("/apis"), the
// API, or a product that has it. A product's name comes back too.
func (s *Service) covers(svc, api arm.ID, scope string) (bool, string) {
	if i := strings.Index(strings.ToLower(scope), "/service/"+strings.ToLower(svc.Name())); i >= 0 {
		scope = scope[i+len("/service/"+svc.Name()):]
	}
	parts := strings.Split(strings.Trim(scope, "/"), "/")
	switch {
	case parts[0] == "" || len(parts) == 1 && strings.EqualFold(parts[0], "apis"):
		return true, ""
	case len(parts) == 2 && strings.EqualFold(parts[0], "apis"):
		return strings.EqualFold(parts[1], api.Name()), ""
	case len(parts) == 2 && strings.EqualFold(parts[0], "products"):
		return s.get(svc.Child("products", parts[1]).Child("apis", api.Name())) != nil, parts[1]
	}
	return false, ""
}

var hop = map[string]bool{"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Content-Length": true}

var backendClient = &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// forward sends the request to the backend and keeps its response.
func (e *exec) forward() error {
	if e.backend == "" {
		return errors.New("the API has no serviceUrl and no policy chose a backend")
	}
	u, err := url.Parse(e.backend)
	if err != nil {
		return err
	}
	hdr := e.hdr.Clone()
	if h, _ := e.creds["header"].(map[string]any); h != nil { // a backend entity's credentials
		for k, v := range h {
			hdr.Del(k)
			vs, _ := v.([]any)
			for _, x := range vs {
				hdr.Add(k, fmt.Sprint(x))
			}
		}
	}
	if a, _ := e.creds["authorization"].(map[string]any); a != nil {
		hdr.Set("Authorization", str(a, "scheme")+" "+str(a, "parameter"))
	}
	if q, _ := e.creds["query"].(map[string]any); q != nil {
		for k, v := range q {
			e.query.Del(k)
			vs, _ := v.([]any)
			for _, x := range vs {
				e.query.Add(k, fmt.Sprint(x))
			}
		}
	}
	u.Path, u.RawPath, u.RawQuery = strings.TrimRight(u.Path, "/")+e.path, "", e.query.Encode()
	req, err := http.NewRequestWithContext(e.r.Context(), e.method, u.String(), bytes.NewReader(e.body))
	if err != nil {
		return err
	}
	for k, v := range hdr {
		if !hop[k] {
			req.Header[k] = v
		}
	}
	res, err := backendClient.Do(req)
	if err != nil {
		return errors.New("the backend could not be reached: " + err.Error())
	}
	defer res.Body.Close()
	e.status, e.rhdr = res.StatusCode, http.Header{}
	for k, v := range res.Header {
		if !hop[k] {
			e.rhdr[k] = v
		}
	}
	e.rbody, err = io.ReadAll(res.Body)
	return err
}
