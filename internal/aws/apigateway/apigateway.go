// Package apigateway emulates Amazon API Gateway: the v1 (REST APIs) and v2
// (HTTP and WebSocket APIs) management APIs over REST-JSON, and the
// execute-api data plane that serves deployed APIs from Lambda functions,
// HTTP backends and mocks.
package apigateway

import (
	"encoding/json"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the API Gateway emulator.
type Service struct {
	hub   *core.Hub
	mux   *http.ServeMux
	mu    sync.Mutex
	st    state
	conns map[string]*wsConn // open WebSocket connections by id
}

type state struct {
	Rest     map[string]*restAPI   `json:"rest"`     // v1 REST APIs by id
	HTTP     map[string]*httpAPI   `json:"http"`     // v2 HTTP and WebSocket APIs by id
	Keys     map[string]*ApiKey    `json:"keys"`     // API keys by id
	Plans    map[string]*UsagePlan `json:"plans"`    // usage plans by id
	PlanKeys map[string][]string   `json:"planKeys"` // usage plan id → API key ids

	Domains     map[string]*domain     `json:"domains"`     // v1 custom domain names by name
	HTTPDomains map[string]*httpDomain `json:"httpDomains"` // v2 custom domain names by name
}

func newState() state {
	return state{Rest: map[string]*restAPI{}, HTTP: map[string]*httpAPI{}, Keys: map[string]*ApiKey{},
		Plans: map[string]*UsagePlan{}, PlanKeys: map[string][]string{},
		Domains: map[string]*domain{}, HTTPDomains: map[string]*httpDomain{}}
}

// New creates the API Gateway service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: newState(), conns: map[string]*wsConn{}}
	s.restRoutes()
	s.httpRoutes()
	s.domainRoutes()
	s.httpDomainRoutes()
	s.route("/", "Unknown", func(r *http.Request, c *core.Ctx) (int, any, error) {
		return 0, nil, core.Errorf(501, "NotImplemented", "pogi does not implement API Gateway %s %s", r.Method, r.URL.Path)
	})
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "apigateway", Cloud: core.AWS, Title: "API Gateway", Signing: []string{"apigateway"},
		Protocol: "rest-json", Match: s.isInvoke}
}

// isInvoke recognises execute-api traffic: the <id>.execute-api.<host> invoke
// URLs and LocalStack's path-style /_aws/execute-api/<id>/… and legacy
// /restapis/<id>/<stage>/_user_request_/… URLs, plus the @connections API
// on pogi's own endpoint. Management calls also use /restapis/… but never
// contain _user_request_.
func isInvoke(r *http.Request) bool {
	p := r.URL.Path
	_, base := core.SplitHost(r.Host, "")
	return strings.HasPrefix(base, "execute-api.") || strings.HasPrefix(p, "/_aws/execute-api/") ||
		strings.HasPrefix(p, "/restapis/") && strings.Contains(p, "/_user_request_") || strings.HasPrefix(p, "/@connections/")
}

// isInvoke is isInvoke(r) or a request to a custom domain name.
func (s *Service) isInvoke(r *http.Request) bool { return isInvoke(r) || s.customHost(r.Host) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.isInvoke(r) {
		s.invoke(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// handler is one management operation. It runs under the service lock and
// returns the status and the output to encode.
type handler func(r *http.Request, c *core.Ctx) (int, any, error)

func (s *Service) route(pattern, op string, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		if err := c.Intercept(); err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		core.Body(r) // read the body before taking the lock
		s.mu.Lock()
		status, out, err := h(r, c)
		var b []byte
		if err == nil && out != nil {
			b, err = json.Marshal(out) // outputs alias the state: encode under the lock
		}
		s.mu.Unlock()
		switch {
		case err != nil:
			core.RESTJSONError(w, c, err)
		case b == nil:
			core.RESTJSON(w, c, status, nil)
		default:
			core.RESTJSON(w, c, status, json.RawMessage(b))
		}
	})
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 {
		if err := json.Unmarshal(b, v); err != nil {
			return badRequest("Invalid request body: %v", err)
		}
	}
	return nil
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "NotFoundException", format, a...)
}
func conflict(format string, a ...any) error {
	return core.Errorf(409, "ConflictException", format, a...)
}
func badRequest(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

// newID returns an API Gateway style id: 10 lowercase alphanumerics.
func newID() string { return core.RandString(10, "abcdefghijklmnopqrstuvwxyz0123456789") }

// find returns m[id] or a NotFoundException naming what.
func find[T any](m map[string]*T, id, what string) (*T, error) {
	if v := m[id]; v != nil {
		return v, nil
	}
	return nil, notFound("Invalid %s identifier specified", what)
}

// items lists m in key order under key ("item" in v1, "items" in v2).
func items[T any](m map[string]*T, key string) map[string]any {
	list := make([]*T, 0, len(m))
	for _, k := range core.SortedKeys(m) {
		list = append(list, m[k])
	}
	return map[string]any{key: list}
}

// nest stores v as m[outer][id], making the maps it needs.
func nest[T any](m map[string]map[string]*T, outer, id string, v *T) map[string]map[string]*T {
	if m == nil {
		m = map[string]map[string]*T{}
	}
	if m[outer] == nil {
		m[outer] = map[string]*T{}
	}
	m[outer][id] = v
	return m
}

// clone deep-copies v so the data plane can use it without the lock.
func clone[T any](v *T) *T {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	out := new(T)
	json.Unmarshal(b, out)
	return out
}

// patchOp is one v1 patch operation; values are always strings on the wire.
type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

// patch applies the request's v1 patchOperations to v, a pointer to a wire
// shape, on its JSON form. Values are converted to the type of the field the
// path names; a path into a string list (/binaryMediaTypes/image~1png) adds,
// removes or replaces that element.
func patch(r *http.Request, v any) error {
	var in struct {
		PatchOperations []patchOp `json:"patchOperations"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	t := reflect.TypeOf(v)
	if st, ok := v.(*Stage); ok {
		in.PatchOperations = stageOps(st, in.PatchOperations)
	}
	b, _ := json.Marshal(v)
	var doc map[string]any
	json.Unmarshal(b, &doc)
	unescape := strings.NewReplacer("~1", "/", "~0", "~")
	for _, op := range in.PatchOperations {
		keys := strings.Split(strings.TrimPrefix(op.Path, "/"), "/")
		for i, k := range keys {
			keys[i] = unescape.Replace(k)
		}
		last := len(keys) - 1
		inList := false
		if pt := typeAt(t, keys[:last]); last > 0 && pt != nil && pt.Kind() == reflect.Slice {
			inList, last = true, last-1
		}
		node := doc
		for _, k := range keys[:last] {
			next, ok := node[k].(map[string]any)
			if !ok {
				next = map[string]any{}
				node[k] = next
			}
			node = next
		}
		k := keys[last]
		if inList {
			elem := keys[last+1]
			out := slices.DeleteFunc(slices.Clone(list(node[k])), func(e any) bool { return e == elem })
			switch op.Op {
			case "add":
				node[k] = append(out, elem)
			case "replace":
				node[k] = append(out, op.Value)
			case "remove":
				node[k] = out
			}
			continue
		}
		switch op.Op {
		case "add", "replace":
			node[k] = coerce(typeAt(t, keys), op.Value)
		case "remove":
			delete(node, k)
		}
	}
	b, _ = json.Marshal(doc)
	fresh := reflect.New(t.Elem())
	if err := json.Unmarshal(b, fresh.Interface()); err != nil {
		return badRequest("Invalid patch operation: %v", err)
	}
	reflect.ValueOf(v).Elem().Set(fresh.Elem())
	return nil
}

// methodSettingFields maps the "<group>/<name>" end of a stage patch path to
// the field of a method setting.
var methodSettingFields = map[string]string{
	"metrics/enabled": "metricsEnabled", "logging/loglevel": "loggingLevel", "logging/dataTrace": "dataTraceEnabled",
	"throttling/burstLimit": "throttlingBurstLimit", "throttling/rateLimit": "throttlingRateLimit",
	"caching/enabled": "cachingEnabled", "caching/ttlInSeconds": "cacheTtlInSeconds", "caching/dataEncrypted": "cacheDataEncrypted",
	"caching/requireAuthorizationForCacheControl":    "requireAuthorizationForCacheControl",
	"caching/unauthorizedCacheControlHeaderStrategy": "unauthorizedCacheControlHeaderStrategy",
}

// methodSettingDefaults is what GetStage shows for a method setting nobody has set.
const methodSettingDefaults = `{"metricsEnabled":false,"loggingLevel":"OFF","dataTraceEnabled":false,"throttlingBurstLimit":-1,
"throttlingRateLimit":-1,"cachingEnabled":false,"cacheTtlInSeconds":300,"cacheDataEncrypted":false,
"requireAuthorizationForCacheControl":true,"unauthorizedCacheControlHeaderStrategy":"SUCCEED_WITH_RESPONSE_HEADER"}`

// stageOps rewrites stage patch paths that name a method, like
// /~1pets/GET/metrics/enabled or /*/*/metrics/enabled, into paths under
// /methodSettings. The method key is the resource path (~1 is a slash, ~0 a
// tilde) and the verb, without a leading slash: "pets/GET", or "*/*" for all.
// A setting nobody has set starts from AWS's defaults. A "remove" of a path
// that is not a stage field removes that method's settings.
func stageOps(st *Stage, ops []patchOp) []patchOp {
	unescape := strings.NewReplacer("~1", "/", "~0", "~")
	escape := strings.NewReplacer("~", "~0", "/", "~1")
	var out []patchOp
	seeded := map[string]bool{}
	for _, op := range ops {
		segs := strings.Split(strings.TrimPrefix(op.Path, "/"), "/")
		if typeAt(reflect.TypeOf(st), segs[:1]) != nil {
			out = append(out, op)
			continue
		}
		for i, k := range segs {
			segs[i] = unescape.Replace(k)
		}
		n := len(segs)
		field := ""
		if n >= 3 {
			field = methodSettingFields[segs[n-2]+"/"+segs[n-1]]
		}
		switch {
		case field != "":
			key := strings.TrimPrefix(strings.Join(segs[:n-2], "/"), "/")
			if _, set := st.MethodSettings[key]; !set && !seeded[key] && op.Op != "remove" {
				seeded[key] = true
				out = append(out, patchOp{Op: "add", Path: "/methodSettings/" + escape.Replace(key), Value: methodSettingDefaults})
			}
			op.Path = "/methodSettings/" + escape.Replace(key) + "/" + field
		case op.Op == "remove":
			op.Path = "/methodSettings/" + escape.Replace(strings.TrimPrefix(strings.Join(segs, "/"), "/"))
		}
		out = append(out, op)
	}
	return out
}

// typeAt returns the Go type found at a JSON path inside t, or nil.
func typeAt(t reflect.Type, keys []string) reflect.Type {
	for _, k := range keys {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		switch t.Kind() {
		case reflect.Map, reflect.Slice:
			t = t.Elem()
		case reflect.Struct:
			found := false
			for i := range t.NumField() {
				if name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ","); strings.EqualFold(name, k) {
					t, found = t.Field(i).Type, true
					break
				}
			}
			if !found {
				return nil
			}
		default:
			return nil
		}
	}
	return t
}

// coerce converts a patch value to the JSON value a field of type t expects.
func coerce(t reflect.Type, s string) any {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t != nil {
		switch t.Kind() {
		case reflect.String:
			return s
		case reflect.Bool:
			if b, err := strconv.ParseBool(s); err == nil {
				return b
			}
		case reflect.Int, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
	}
	var v any
	if json.Unmarshal([]byte(s), &v) == nil {
		return v
	}
	return s
}

// tagged finds the tags of a v1 or v2 resource ARN; lock held.
func (s *Service) tagged(arn string) (*map[string]string, error) {
	_, _, _, res, _ := core.ParseARN(arn)
	p := strings.Split(strings.Trim(res, "/"), "/")
	if len(p) >= 2 {
		switch p[0] {
		case "restapis":
			if a := s.st.Rest[p[1]]; a != nil && len(p) == 2 {
				return &a.Tags, nil
			} else if a != nil && len(p) == 4 && p[2] == "stages" && a.Stages[p[3]] != nil {
				return &a.Stages[p[3]].Tags, nil
			}
		case "apis":
			if a := s.st.HTTP[p[1]]; a != nil && len(p) == 2 {
				return &a.Tags, nil
			} else if a != nil && len(p) == 4 && p[2] == "stages" && a.Stages[p[3]] != nil {
				return &a.Stages[p[3]].Tags, nil
			}
		case "domainnames":
			if d := s.st.Domains[p[1]]; d != nil {
				return &d.Tags, nil
			} else if d := s.st.HTTPDomains[p[1]]; d != nil {
				return &d.Tags, nil
			}
		case "apikeys":
			if k := s.st.Keys[p[1]]; k != nil {
				return &k.Tags, nil
			}
		case "usageplans":
			if u := s.st.Plans[p[1]]; u != nil {
				return &u.Tags, nil
			}
		}
	}
	return nil, notFound("Invalid resource ARN specified: %s", arn)
}

// tagRoutes serves TagResource, UntagResource and GetTags under prefix
// (/tags in v1, /v2/tags in v2); tagged is TagResource's status code.
func (s *Service) tagRoutes(prefix, tagMethod string, tagged int) {
	s.route(tagMethod+" "+prefix+"/{arn...}", "TagResource", func(r *http.Request, c *core.Ctx) (int, any, error) {
		var in struct {
			Tags map[string]string `json:"tags"`
		}
		if err := decode(r, &in); err != nil {
			return 0, nil, err
		}
		tags, err := s.tagged(r.PathValue("arn"))
		if err != nil {
			return 0, nil, err
		}
		if *tags == nil {
			*tags = map[string]string{}
		}
		for k, v := range in.Tags {
			(*tags)[k] = v
		}
		if tagged == http.StatusNoContent {
			return tagged, nil, nil
		}
		return tagged, map[string]any{}, nil
	})
	s.route("DELETE "+prefix+"/{arn...}", "UntagResource", func(r *http.Request, c *core.Ctx) (int, any, error) {
		tags, err := s.tagged(r.PathValue("arn"))
		if err != nil {
			return 0, nil, err
		}
		for _, k := range r.URL.Query()["tagKeys"] {
			delete(*tags, k)
		}
		return http.StatusNoContent, nil, nil
	})
	s.route("GET "+prefix+"/{arn...}", "GetTags", func(r *http.Request, c *core.Ctx) (int, any, error) {
		tags, err := s.tagged(r.PathValue("arn"))
		if err != nil {
			return 0, nil, err
		}
		out := map[string]string{}
		for k, v := range *tags {
			out[k] = v
		}
		return 200, map[string]any{"tags": out}, nil
	})
}

// endpoint is an API's execute-api base URL for a client that reached pogi at host.
func endpoint(host, id string) string {
	port := ""
	if i := strings.LastIndex(host, ":"); i >= 0 && !strings.Contains(host[i:], "]") {
		port = host[i:]
	}
	return "http://" + id + ".execute-api.localhost" + port
}

func (s *Service) host() string {
	if s.hub != nil && s.hub.URL != "" {
		return core.Hostport(s.hub.URL)
	}
	return "localhost:7644"
}

// Resources lists REST APIs and HTTP/WebSocket APIs.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	rest := core.Table{Name: "REST APIs", Columns: []string{"Id", "Name", "Region", "Account", "Stages", "Invoke URL"}}
	for _, id := range core.SortedKeys(s.st.Rest) {
		a := s.st.Rest[id]
		stages, url := core.SortedKeys(a.Stages), ""
		if len(stages) > 0 {
			url = endpoint(s.host(), id) + "/" + stages[0] + "/"
		}
		rest.Rows = append(rest.Rows, []string{id, a.Name, a.Region, a.Account, strings.Join(stages, ", "), url})
	}
	apis := core.Table{Name: "HTTP APIs", Columns: []string{"Id", "Name", "Protocol", "Routes", "Endpoint"}}
	for _, id := range core.SortedKeys(s.st.HTTP) {
		a := s.st.HTTP[id]
		apis.Rows = append(apis.Rows, []string{id, a.Name, a.ProtocolType, strconv.Itoa(len(a.Routes)), a.url(s.host())})
	}
	return []core.Table{rest, apis}
}

// Inspect returns an API with its resources, routes, integrations and stages.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b []byte
	switch {
	case table == "REST APIs" && s.st.Rest[id] != nil:
		b, _ = json.Marshal(s.st.Rest[id])
	case table == "HTTP APIs" && s.st.HTTP[id] != nil:
		b, _ = json.Marshal(s.st.HTTP[id])
	default:
		return nil
	}
	return json.RawMessage(b)
}

// TaggedResources lists API keys (ARN → tags) for the tagging API.
// ponytail: keys are kept without a region, so every region lists them; REST and HTTP APIs and usage plans are not listed yet.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for id, k := range s.st.Keys {
		out[core.ARN("apigateway", region, "", "/apikeys/"+id)] = maps.Clone(k.Tags)
	}
	return out
}
