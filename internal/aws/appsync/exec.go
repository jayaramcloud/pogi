package appsync

import (
	"bytes"
	"cmp"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
	"github.com/vektah/gqlparser/v2/validator/rules"
)

// appsyncPrelude declares the scalars and directives AppSync schemas use
// without declaring them.
const appsyncPrelude = `scalar AWSDate
scalar AWSTime
scalar AWSDateTime
scalar AWSTimestamp
scalar AWSEmail
scalar AWSJSON
scalar AWSURL
scalar AWSPhone
scalar AWSIPAddress
directive @aws_subscribe(mutations: [String]) on FIELD_DEFINITION
directive @aws_publish(subscriptions: [String]) on FIELD_DEFINITION
directive @aws_auth(cognito_groups: [String]) on FIELD_DEFINITION
directive @aws_api_key on FIELD_DEFINITION | OBJECT
directive @aws_iam on FIELD_DEFINITION | OBJECT
directive @aws_oidc on FIELD_DEFINITION | OBJECT
directive @aws_lambda on FIELD_DEFINITION | OBJECT
directive @aws_cognito_user_pools(cognito_groups: [String]) on FIELD_DEFINITION | OBJECT`

// loadSchema parses an SDL schema on top of the prelude entries it does not declare itself.
func loadSchema(sdl string) (*ast.Schema, error) {
	doc, err := parser.ParseSchema(&ast.Source{Name: "schema.graphql", Input: sdl})
	if err != nil {
		return nil, err
	}
	var prelude []string
	for _, line := range strings.Split(appsyncPrelude, "\n") {
		name := strings.Fields(line)[1]
		if d, ok := strings.CutPrefix(name, "@"); ok {
			if d, _, _ = strings.Cut(d, "("); doc.Directives.ForName(d) != nil {
				continue
			}
		} else if doc.Definitions.ForName(name) != nil {
			continue
		}
		prelude = append(prelude, line)
	}
	return gqlparser.LoadSchema(&ast.Source{Name: "appsync.graphql", Input: strings.Join(prelude, "\n"), BuiltIn: true},
		&ast.Source{Name: "schema.graphql", Input: sdl})
}

// gqlRequest is one GraphQL request, with what auth and resolvers see of it.
type gqlRequest struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
	h             header
	c             *core.Ctx // nil on the real-time endpoint
	ip            string
}

// header holds request headers by lowercase name.
type header map[string]string

func httpHeader(h http.Header) header {
	out := header{}
	for k, v := range h {
		out[strings.ToLower(k)] = strings.Join(v, ",")
	}
	return out
}

func (h header) get(k string) string { return h[strings.ToLower(k)] }

// target returns the API id of a data-plane request and whether it is
// real-time: the <id>.appsync-api and <id>.appsync-realtime-api hosts, and
// LocalStack's path-style /appsync-api/<id>/graphql and /appsync-realtime-api/<id>/graphql.
func target(r *http.Request) (id string, realtime, ok bool) {
	if p := strings.Split(r.URL.Path, "/"); len(p) == 4 && p[3] == "graphql" && (p[1] == "appsync-api" || p[1] == "appsync-realtime-api") {
		return p[2], p[1] == "appsync-realtime-api", true
	}
	id, base := core.SplitHost(r.Host, "")
	realtime = strings.HasPrefix(base, "appsync-realtime-api.")
	return id, realtime, realtime || strings.HasPrefix(base, "appsync-api.")
}

// cors answers like AppSync: any origin, any request header. It reports
// whether r was a preflight, which it answered.
func cors(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Origin") == "" {
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method != http.MethodOptions {
		return false
	}
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
	w.Header().Set("Access-Control-Max-Age", "86400")
	w.WriteHeader(http.StatusOK)
	return true
}

// fail answers a data-plane request with an AppSync error.
func fail(w http.ResponseWriter, c *core.Ctx, status int, code, msg string) {
	c.ErrCode = code
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-ErrorType", code)
	w.WriteHeader(status)
	io.WriteString(w, toJSON(map[string]any{"errors": []any{map[string]any{"errorType": code, "message": msg}}}))
}

func (s *Service) serveGraphQL(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "GraphQL"
	if cors(w, r) {
		return
	}
	id, realtime, _ := target(r)
	a := s.snapshot(id)
	if a == nil {
		fail(w, c, 404, "NotFoundException", "GraphQL API "+id+" not found.")
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		fail(w, c, e.Status, e.Code, e.Msg)
		return
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if realtime {
		s.realtime(w, r, a.ID, ip)
		return
	}
	q := &gqlRequest{h: httpHeader(r.Header), c: c, ip: ip}
	if r.Method != http.MethodPost || json.Unmarshal(core.Body(r), q) != nil || q.Query == "" {
		fail(w, c, 400, "MalformedHttpRequestException", "Unable to parse GraphQL query.")
		return
	}
	au, msg := s.authenticate(a, q)
	if au == nil {
		fail(w, c, 401, "UnauthorizedException", msg)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, toJSON(s.execute(a, au, q)))
}

// snapshot copies an API so a request runs without the lock.
func (s *Service) snapshot(id string) *API {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.APIs[id]
	if a == nil {
		return nil
	}
	a.schema()
	cp := *a
	cp.Keys, cp.Sources, cp.Functions, cp.Resolvers = maps.Clone(a.Keys), maps.Clone(a.Sources), maps.Clone(a.Functions), maps.Clone(a.Resolvers)
	return &cp
}

// gqlErr is an entry of a response's errors.
type gqlErr struct {
	Path      []any  `json:"path"`
	Data      any    `json:"data"`
	ErrorType string `json:"errorType,omitempty"`
	ErrorInfo any    `json:"errorInfo"`
	Locations []loc  `json:"locations,omitempty"`
	Message   string `json:"message"`
}

type loc struct {
	Line       int `json:"line"`
	Column     int `json:"column"`
	SourceName any `json:"sourceName"`
}

// resolveErr is an error a resolver raised ($util.error) or a data source returned.
type resolveErr struct {
	Message string `json:"message"`
	Type    string `json:"errorType,omitempty"`
	Data    any    `json:"data,omitempty"`
	Info    any    `json:"errorInfo,omitempty"`
}

func (e *resolveErr) Error() string { return e.Message }

func newErr(f *ast.Field, path []any, err error) *gqlErr {
	g := &gqlErr{Path: path, Message: err.Error()}
	if f.Position != nil {
		g.Locations = []loc{{Line: f.Position.Line, Column: f.Position.Column}}
	}
	var re *resolveErr
	if errors.As(err, &re) {
		g.ErrorType, g.Data, g.ErrorInfo = re.Type, re.Data, re.Info
	}
	return g
}

func failed(msg string, errs ...*gqlerror.Error) map[string]any {
	var out []*gqlErr
	for _, e := range errs {
		g := &gqlErr{Message: e.Message}
		if e.Rule != "" {
			g.Message = "Validation error of type " + e.Rule + ": " + e.Message
		}
		for _, l := range e.Locations {
			g.Locations = append(g.Locations, loc{Line: l.Line, Column: l.Column})
		}
		out = append(out, g)
	}
	if msg != "" {
		out = append(out, &gqlErr{Message: msg})
	}
	return map[string]any{"data": nil, "errors": out}
}

// parse validates a request against the schema and picks its operation.
func parse(sch *ast.Schema, q *gqlRequest) (*ast.OperationDefinition, map[string]any, map[string]any) {
	if sch == nil {
		return nil, nil, failed("No schema has been created for this API.")
	}
	doc, errs := gqlparser.LoadQueryWithRules(sch, q.Query, rules.NewDefaultRules())
	if len(errs) > 0 {
		return nil, nil, failed("", errs...)
	}
	op := doc.Operations.ForName(q.OperationName)
	if op == nil {
		return nil, nil, failed(fmt.Sprintf("Unknown operation named '%s'.", q.OperationName))
	}
	vars, err := validator.VariableValues(sch, op, q.Variables)
	if err != nil {
		var ge *gqlerror.Error
		errors.As(err, &ge)
		return nil, nil, failed("", cmp.Or(ge, &gqlerror.Error{Message: err.Error()}))
	}
	return op, vars, nil
}

// execute runs a query or mutation, then publishes mutation results to subscribers.
func (s *Service) execute(a *API, au *auth, q *gqlRequest) map[string]any {
	op, vars, bad := parse(a.parsed, q)
	if bad != nil {
		return bad
	}
	x := &exec{s: s, api: a, schema: a.parsed, auth: au, vars: vars, req: q}
	root := x.schema.Query
	switch op.Operation {
	case ast.Mutation:
		root = x.schema.Mutation
	case ast.Subscription:
		return failed("Subscriptions are served by the real-time endpoint.")
	}
	out := map[string]any{"data": nil}
	data, _ := x.object(op.SelectionSet, root, nil, nil)
	if data != nil {
		out["data"] = data
	}
	if len(x.errs) > 0 {
		out["errors"] = x.errs
	}
	if op.Operation == ast.Mutation && data != nil {
		for _, f := range x.collect(op.SelectionSet, root) {
			if v := data.vals[f.Alias]; v != nil {
				s.publish(a.ID, f.Name, v)
			}
		}
	}
	return out
}

// object is a JSON object that keeps the order of its selection set.
type object struct {
	keys []string
	vals map[string]any
}

func (o *object) set(k string, v any) {
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k)
	}
	o.vals[k] = v
}

func (o *object) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(toJSON(k) + ":" + toJSON(o.vals[k]))
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// exec executes one operation.
type exec struct {
	s      *Service
	api    *API
	schema *ast.Schema
	auth   *auth
	vars   map[string]any
	req    *gqlRequest
	plain  bool // subscription payloads: fields read their parent, resolvers do not run
	errs   []*gqlErr
}

// collect flattens a selection set for an object type: the fragments that
// apply, @skip and @include, and fields of one response key merged.
func (x *exec) collect(sel ast.SelectionSet, t *ast.Definition) []*ast.Field {
	var out []*ast.Field
	idx := map[string]int{}
	var walk func(ast.SelectionSet)
	walk = func(sel ast.SelectionSet) {
		for _, s := range sel {
			switch s := s.(type) {
			case *ast.Field:
				if !x.included(s.Directives) {
					continue
				}
				if i, ok := idx[s.Alias]; ok {
					f := *out[i]
					f.SelectionSet = append(slices.Clip(f.SelectionSet), s.SelectionSet...)
					out[i] = &f
					continue
				}
				idx[s.Alias] = len(out)
				out = append(out, s)
			case *ast.InlineFragment:
				if x.included(s.Directives) && x.applies(s.TypeCondition, t) {
					walk(s.SelectionSet)
				}
			case *ast.FragmentSpread:
				if x.included(s.Directives) && s.Definition != nil && x.applies(s.Definition.TypeCondition, t) {
					walk(s.Definition.SelectionSet)
				}
			}
		}
	}
	walk(sel)
	return out
}

func (x *exec) included(ds ast.DirectiveList) bool {
	if d := ds.ForName("skip"); d != nil && d.ArgumentMap(x.vars)["if"] == true {
		return false
	}
	d := ds.ForName("include")
	return d == nil || d.ArgumentMap(x.vars)["if"] != false
}

func (x *exec) applies(cond string, t *ast.Definition) bool {
	return cond == "" || cond == t.Name || slices.Contains(x.schema.PossibleTypes[cond], t)
}

// object resolves a selection set on a value of object type t. A null
// non-null field makes the whole object null (null is true).
// ponytail: sibling fields resolve one at a time, AppSync runs them in parallel; go concurrent if slow Lambdas add up.
func (x *exec) object(sel ast.SelectionSet, t *ast.Definition, source any, path []any) (*object, bool) {
	o := &object{vals: map[string]any{}}
	for _, f := range x.collect(sel, t) {
		v, null := x.field(f, t, source, append(slices.Clip(path), f.Alias))
		if null {
			return nil, true
		}
		o.set(f.Alias, v)
	}
	return o, false
}

func (x *exec) field(f *ast.Field, t *ast.Definition, source any, path []any) (any, bool) {
	var v any
	switch {
	case f.Name == "__typename":
		return t.Name, false
	case (f.Name == "__schema" || f.Name == "__type") && x.api.Config.IntrospectionConfig == "DISABLED":
		return x.fail(f, path, &resolveErr{Message: "Introspection is disabled for this API.", Type: "Unauthorized"})
	case f.Name == "__schema":
		v = introspect(x.schema)
	case f.Name == "__type":
		if d := x.schema.Types[fmt.Sprint(f.ArgumentMap(x.vars)["name"])]; d != nil {
			v = introspectType(x.schema, d)
		}
	case !x.allowed(t, f.Definition):
		return x.fail(f, path, &resolveErr{Message: fmt.Sprintf("Not Authorized to access %s on type %s", f.Name, t.Name), Type: "Unauthorized"})
	default:
		if r := x.api.Resolvers[t.Name+"."+f.Name]; r != nil && !x.plain {
			var err error
			if v, err = x.resolve(r, f, t, source, path); err != nil {
				return x.fail(f, path, err)
			}
		} else {
			v = prop(source, f.Name)
		}
	}
	return x.complete(f.Definition.Type, f, v, path)
}

// fail records a field error. The field is null, which a non-null field passes to its parent.
func (x *exec) fail(f *ast.Field, path []any, err error) (any, bool) {
	x.errs = append(x.errs, newErr(f, path, err))
	return nil, f.Definition.Type.NonNull
}

// complete shapes a resolved value to the field's type and selection set.
func (x *exec) complete(t *ast.Type, f *ast.Field, v any, path []any) (any, bool) {
	out, null := x.completeValue(t, f, v, path)
	if out == nil && t.NonNull {
		if !null {
			x.errs = append(x.errs, newErr(f, path, fmt.Errorf("Cannot return null for non-nullable type: '%s' within parent '%s' (/%s)",
				t.Name(), f.ObjectDefinition.Name, joinPath(path))))
		}
		return nil, true
	}
	return out, false
}

func (x *exec) completeValue(t *ast.Type, f *ast.Field, v any, path []any) (any, bool) {
	if v == nil {
		return nil, false
	}
	if t.Elem != nil {
		items, ok := v.([]any)
		if !ok {
			x.errs = append(x.errs, newErr(f, path, fmt.Errorf("Can't resolve value (/%s) : type mismatch error, expected type LIST", joinPath(path))))
			return nil, false
		}
		out := make([]any, len(items))
		for i, e := range items {
			ev, null := x.complete(t.Elem, f, e, append(slices.Clip(path), i))
			if null {
				return nil, true
			}
			out[i] = ev
		}
		return out, false
	}
	d := x.schema.Types[t.NamedType]
	switch d.Kind {
	case ast.Scalar, ast.Enum:
		return scalar(d.Name, v), false
	case ast.Interface, ast.Union:
		name, _ := prop(v, "__typename").(string)
		pts := x.schema.PossibleTypes[d.Name]
		if i := slices.IndexFunc(pts, func(p *ast.Definition) bool { return p.Name == name }); i >= 0 {
			d = pts[i]
		} else if len(pts) == 1 {
			d = pts[0]
		} else {
			x.errs = append(x.errs, newErr(f, path, fmt.Errorf("Could not determine the exact type of %s. Missing __typename key on value.", d.Name)))
			return nil, false
		}
	}
	o, null := x.object(f.SelectionSet, d, v, path)
	if null {
		return nil, true
	}
	return o, false
}

// scalar coerces a resolved leaf value the way AppSync serializes it.
func scalar(name string, v any) any {
	switch name {
	case "Int":
		if f, ok := v.(float64); ok && f == math.Trunc(f) {
			return int64(f)
		}
	case "String", "ID":
		switch v.(type) {
		case float64, int64, bool:
			return fmt.Sprint(v)
		}
	case "AWSJSON":
		if _, ok := v.(string); !ok {
			return toJSON(v)
		}
	}
	return v
}

func joinPath(path []any) string {
	var p []string
	for _, e := range path {
		p = append(p, fmt.Sprint(e))
	}
	return strings.Join(p, "/")
}

// prop is the default resolver: the parent's property of the field's name.
func prop(v any, k string) any {
	switch m := v.(type) {
	case map[string]any:
		return m[k]
	case *object:
		return m.vals[k]
	}
	return nil
}

func (x *exec) resolve(r *Resolver, f *ast.Field, t *ast.Definition, source any, path []any) (any, error) {
	rc := &rctx{args: f.ArgumentMap(x.vars), source: source, identity: x.auth.identity, info: x.info(f, t), headers: x.req.h, stash: map[string]any{}}
	v, err := x.run(r, rc)
	for _, e := range rc.errors {
		x.errs = append(x.errs, newErr(f, path, e))
	}
	return v, err
}

// info is $ctx.info.
func (x *exec) info(f *ast.Field, t *ast.Definition) map[string]any {
	list := []string{}
	var walk func(sel ast.SelectionSet, typ, prefix string)
	walk = func(sel ast.SelectionSet, typ, prefix string) {
		d := x.schema.Types[typ]
		if d == nil {
			return
		}
		for _, c := range x.collect(sel, d) {
			if c.Name == "__typename" {
				continue
			}
			list = append(list, prefix+c.Alias)
			if len(c.SelectionSet) > 0 && c.Definition != nil {
				walk(c.SelectionSet, c.Definition.Type.Name(), prefix+c.Alias+"/")
			}
		}
	}
	walk(f.SelectionSet, f.Definition.Type.Name(), "")
	var b strings.Builder
	if len(f.SelectionSet) > 0 {
		formatter.NewFormatter(&b, formatter.WithIndent("  ")).(interface{ FormatSelectionSet(ast.SelectionSet) }).FormatSelectionSet(f.SelectionSet)
	}
	return map[string]any{"fieldName": f.Name, "parentTypeName": t.Name, "variables": x.vars, "selectionSetList": list,
		"selectionSetGraphQL": strings.TrimSpace(b.String())}
}

// ---- resolvers ----

// rctx is a resolver's context: $ctx in VTL, ctx in JS.
type rctx struct {
	args     map[string]any
	source   any
	identity any
	info     map[string]any
	headers  header
	stash    map[string]any
	prev     any // the result of the previous pipeline step
	result   any
	err      any           // the data source's error: {message, type}
	errors   []*resolveErr // $util.appendError
	logs     []string
}

func (rc *rctx) ctx() map[string]any {
	return map[string]any{"arguments": rc.args, "args": rc.args, "source": rc.source, "identity": rc.identity, "info": rc.info,
		"request": map[string]any{"headers": rc.headers, "domainName": nil}, "stash": rc.stash,
		"prev": map[string]any{"result": rc.prev}, "result": rc.result, "error": rc.err}
}

// run executes a unit resolver's data source, or a pipeline's before step,
// functions and after step.
func (x *exec) run(r *Resolver, rc *rctx) (any, error) {
	if r.Kind != "PIPELINE" {
		return x.unit(r.DataSourceName, r.Runtime, r.Code, r.RequestMappingTemplate, r.ResponseMappingTemplate, rc)
	}
	v, early, err := step(r.Runtime, r.Code, r.RequestMappingTemplate, "request", rc)
	if err != nil || early {
		return v, err
	}
	rc.prev = v
	for _, id := range r.PipelineConfig.Functions {
		fn := x.api.Functions[id]
		if fn == nil {
			return nil, &resolveErr{Message: "Function " + id + " not found.", Type: "MappingTemplate"}
		}
		if rc.prev, err = x.unit(fn.DataSourceName, fn.Runtime, fn.Code, fn.RequestMappingTemplate, fn.ResponseMappingTemplate, rc); err != nil {
			return nil, err
		}
		rc.result, rc.err = nil, nil
	}
	rc.result = rc.prev
	v, _, err = step(r.Runtime, r.Code, r.ResponseMappingTemplate, "response", rc)
	return v, err
}

// unit calls a data source between a request and a response step. A Lambda
// data source without a request step gets the whole context (a direct Lambda resolver).
func (x *exec) unit(name string, rt *runtime, code, req, resp string, rc *rctx) (any, error) {
	ds := x.api.Sources[name]
	if ds == nil {
		return nil, &resolveErr{Message: "No data source found named " + name + ".", Type: "MappingTemplate"}
	}
	direct := !rt.js() && req == "" && ds.Type == "AWS_LAMBDA"
	var in any
	if !direct {
		v, early, err := step(rt, code, req, "request", rc)
		if err != nil || early {
			return v, err
		}
		in = v
	}
	rc.result, rc.err = x.invoke(ds, in, direct, rc)
	v, _, err := step(rt, code, resp, "response", rc)
	return v, err
}

// step runs the request or response half of a resolver or function: JS
// code, a VTL template, or neither (the request is {} and the response is the result).
func step(rt *runtime, code, tmpl, fn string, rc *rctx) (any, bool, error) {
	switch {
	case rt.js():
		return runJS(code, fn, rc)
	case tmpl != "":
		return runVTL(tmpl, rc)
	case fn == "request":
		return map[string]any{}, false, nil
	case rc.err != nil:
		e, _ := rc.err.(map[string]any)
		return nil, false, &resolveErr{Message: str(e["message"]), Type: str(e["type"])}
	}
	return rc.result, false, nil
}

// ---- auth ----

// auth is how a request authenticated: the mode, and $ctx.identity.
type auth struct {
	mode     string
	identity map[string]any
	groups   []string // Cognito groups
	denied   []string // fields a Lambda authorizer denied
}

const unauthorized = "You are not authorized to make this call."

// authenticate finds the authorization mode the request's credentials are
// for, among the API's, and checks them. It returns nil and a message to deny.
func (s *Service) authenticate(a *API, q *gqlRequest) (*auth, string) {
	modes := map[string][]provider{}
	for _, p := range a.Config.providers() {
		modes[p.AuthenticationType] = append(modes[p.AuthenticationType], p)
	}
	if key := q.h.get("x-api-key"); key != "" {
		k := a.Keys[key]
		if modes["API_KEY"] == nil || k == nil || time.Now().Unix() >= k.Expires {
			return nil, unauthorized
		}
		return &auth{mode: "API_KEY"}, ""
	}
	tok := q.h.get("authorization")
	switch {
	case tok == "":
		return nil, "Valid authorization header not provided."
	case strings.HasPrefix(tok, "AWS4-HMAC-SHA256"):
		// The gateway checked the signature (Config.ValidateSigs) before the request got here.
		if modes["AWS_IAM"] == nil {
			return nil, unauthorized
		}
		return &auth{mode: "AWS_IAM", identity: iamIdentity(a, q)}, ""
	}
	jwt := strings.TrimPrefix(tok, "Bearer ")
	for _, p := range modes["AMAZON_COGNITO_USER_POOLS"] {
		if claims := s.cognitoClaims(p.UserPoolConfig, jwt); claims != nil {
			groups := strs(claims["cognito:groups"])
			return &auth{mode: p.AuthenticationType, groups: groups, identity: map[string]any{"sub": claims["sub"], "issuer": claims["iss"],
				"username": cmp.Or(str(claims["cognito:username"]), str(claims["username"])), "claims": claims, "sourceIp": []string{q.ip},
				"defaultAuthStrategy": p.UserPoolConfig.DefaultAction, "groups": claims["cognito:groups"]}}, ""
		}
	}
	for _, p := range modes["OPENID_CONNECT"] {
		if claims := oidcClaims(p.OpenIDConnectConfig, jwt); claims != nil {
			return &auth{mode: p.AuthenticationType, identity: map[string]any{"sub": claims["sub"], "issuer": claims["iss"], "claims": claims}}, ""
		}
	}
	if ps := modes["AWS_LAMBDA"]; ps != nil {
		return s.authorizer(a, ps[0].LambdaAuthorizerConfig, tok, q)
	}
	return nil, unauthorized
}

// iamIdentity is $ctx.identity of a signed request.
// ponytail: real-time connections are not signature-checked, their identity is the API's account.
func iamIdentity(a *API, q *gqlRequest) map[string]any {
	id := map[string]any{"accountId": a.Account, "userArn": core.GlobalARN("iam", a.Region, a.Account, "root"), "username": "",
		"sourceIp": []string{q.ip}, "cognitoIdentityPoolId": nil, "cognitoIdentityId": nil, "cognitoIdentityAuthType": nil,
		"cognitoIdentityAuthProvider": nil}
	if c := q.c; c != nil {
		id["accountId"], id["userArn"], id["username"] = c.Account, c.Principal, c.AccessKey
	}
	return id
}

// cognitoClaims verifies a token of the pool against the pool's JWKS.
func (s *Service) cognitoClaims(cfg *userPool, tok string) map[string]any {
	claims := peek(tok)
	if claims == nil || !strings.HasSuffix(str(claims["iss"]), "/"+cfg.UserPoolId) || !clientMatches(cfg.AppIdClientRegex, claims) {
		return nil
	}
	req, _ := http.NewRequest(http.MethodGet, "/"+cfg.UserPoolId+"/.well-known/jwks.json", nil)
	if status, jwks := s.hub.DoService("cognito-idp", req); status == http.StatusOK {
		return verifyJWT(tok, jwks)
	}
	return nil
}

var client = &http.Client{Timeout: 30 * time.Second}

// oidcClaims verifies a token of the issuer against the JWKS its discovery document names.
// ponytail: the JWKS is fetched per request, cache it if a remote issuer gets slow.
func oidcClaims(cfg *oidcConfig, tok string) map[string]any {
	iss := strings.TrimSuffix(cfg.Issuer, "/")
	claims := peek(tok)
	if claims == nil || strings.TrimSuffix(str(claims["iss"]), "/") != iss || !clientMatches(cfg.ClientId, claims) {
		return nil
	}
	now := time.Now().UnixMilli()
	if cfg.IatTTL > 0 && now-int64(num(claims["iat"]))*1000 > cfg.IatTTL || cfg.AuthTTL > 0 && now-int64(num(claims["auth_time"]))*1000 > cfg.AuthTTL {
		return nil
	}
	var doc struct {
		JwksURI string `json:"jwks_uri"`
	}
	if json.Unmarshal(fetch(iss+"/.well-known/openid-configuration"), &doc) != nil {
		return nil
	}
	return verifyJWT(tok, fetch(doc.JwksURI))
}

func fetch(u string) []byte {
	resp, err := client.Get(u)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b
}

var b64 = base64.RawURLEncoding

// peek decodes a JWT's claims without checking it.
func peek(tok string) map[string]any {
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		return nil
	}
	body, _ := b64.DecodeString(p[1])
	var claims map[string]any
	json.Unmarshal(body, &claims)
	return claims
}

// verifyJWT checks an RS256 token's signature against a JWKS, and its expiry.
func verifyJWT(tok string, jwks []byte) map[string]any {
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		return nil
	}
	var hdr struct{ Kid, Alg string }
	var set struct{ Keys []struct{ Kid, N, E string } }
	h, _ := b64.DecodeString(p[0])
	if json.Unmarshal(h, &hdr) != nil || hdr.Alg != "RS256" || json.Unmarshal(jwks, &set) != nil {
		return nil
	}
	for _, k := range set.Keys {
		if k.Kid != hdr.Kid {
			continue
		}
		n, _ := b64.DecodeString(k.N)
		e, _ := b64.DecodeString(k.E)
		sig, _ := b64.DecodeString(p[2])
		sum := sha256.Sum256([]byte(p[0] + "." + p[1]))
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		claims := peek(tok)
		if rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig) != nil || time.Now().Unix() >= int64(num(claims["exp"])) {
			return nil
		}
		return claims
	}
	return nil
}

// clientMatches applies an app client id regex to the token's audience.
func clientMatches(expr string, claims map[string]any) bool {
	if expr == "" {
		return true
	}
	re, err := regexp.Compile("^(?:" + expr + ")$")
	if err != nil {
		return false
	}
	ids := append(strs(claims["aud"]), str(claims["aud"]), str(claims["azp"]), str(claims["client_id"]))
	return slices.ContainsFunc(ids, func(id string) bool { return id != "" && re.MatchString(id) })
}

// authorizer runs the API's Lambda authorizer.
// ponytail: authorizer results are not cached, so authorizerResultTtlInSeconds is ignored.
func (s *Service) authorizer(a *API, cfg *lambdaAuth, tok string, q *gqlRequest) (*auth, string) {
	if e := cfg.IdentityValidationExpression; e != "" {
		if re, err := regexp.Compile("^(?:" + e + ")$"); err != nil || !re.MatchString(tok) {
			return nil, unauthorized
		}
	}
	ev := map[string]any{"authorizationToken": tok, "requestHeaders": q.h, "requestContext": map[string]any{"apiId": a.ID,
		"accountId": a.Account, "requestId": core.UUID(), "queryString": q.Query, "operationName": q.OperationName, "variables": q.Variables}}
	out, err := s.hub.Invoke(cfg.AuthorizerUri, []byte(toJSON(ev)))
	var res struct {
		IsAuthorized    bool           `json:"isAuthorized"`
		ResolverContext map[string]any `json:"resolverContext"`
		DeniedFields    []string       `json:"deniedFields"`
	}
	if err != nil || json.Unmarshal(out, &res) != nil || !res.IsAuthorized {
		return nil, unauthorized
	}
	return &auth{mode: "AWS_LAMBDA", identity: map[string]any{"resolverContext": res.ResolverContext}, denied: res.DeniedFields}, ""
}

var directiveModes = map[string]string{"aws_api_key": "API_KEY", "aws_iam": "AWS_IAM", "aws_oidc": "OPENID_CONNECT",
	"aws_lambda": "AWS_LAMBDA", "aws_cognito_user_pools": "AMAZON_COGNITO_USER_POOLS", "aws_auth": "AMAZON_COGNITO_USER_POOLS"}

// allowed applies the auth directives: the field's own, else its type's, else
// only the default mode may read it. cognito_groups narrow a Cognito directive.
func (x *exec) allowed(t *ast.Definition, f *ast.FieldDefinition) bool {
	a := x.auth
	if strings.HasPrefix(t.Name, "__") {
		return true
	}
	if a.mode == "AWS_LAMBDA" && slices.ContainsFunc(a.denied, func(d string) bool {
		return d == t.Name+"."+f.Name || strings.HasSuffix(d, "/types/"+t.Name+"/fields/"+f.Name)
	}) {
		return false
	}
	dirs := authDirectives(f.Directives)
	if dirs == nil {
		dirs = authDirectives(t.Directives)
	}
	if dirs == nil {
		cfg := x.api.Config
		return a.mode == cfg.AuthenticationType && !(a.mode == "AMAZON_COGNITO_USER_POOLS" && cfg.UserPoolConfig.DefaultAction == "DENY")
	}
	for _, d := range dirs {
		if directiveModes[d.Name] != a.mode {
			continue
		}
		g := d.Arguments.ForName("cognito_groups")
		if g == nil {
			return true
		}
		for _, v := range g.Value.Children {
			if slices.Contains(a.groups, v.Value.Raw) {
				return true
			}
		}
	}
	return false
}

func authDirectives(ds ast.DirectiveList) []*ast.Directive {
	var out []*ast.Directive
	for _, d := range ds {
		if directiveModes[d.Name] != "" {
			out = append(out, d)
		}
	}
	return out
}

// ---- introspection ----

// introspect is a schema's __schema as plain values, which the default
// resolver walks. Type references carry kind, name and ofType only.
func introspect(s *ast.Schema) map[string]any {
	types := []any{}
	for _, n := range core.SortedKeys(s.Types) {
		types = append(types, introspectType(s, s.Types[n]))
	}
	dirs := []any{}
	for _, n := range core.SortedKeys(s.Directives) {
		d := s.Directives[n]
		locs := []any{}
		for _, l := range d.Locations {
			locs = append(locs, string(l))
		}
		dirs = append(dirs, map[string]any{"name": d.Name, "description": orNil(d.Description), "locations": locs,
			"args": args(s, d.Arguments), "isRepeatable": d.IsRepeatable})
	}
	root := func(d *ast.Definition) any {
		if d == nil {
			return nil
		}
		return typeRef(s, ast.NamedType(d.Name, nil))
	}
	return map[string]any{"description": nil, "queryType": root(s.Query), "mutationType": root(s.Mutation),
		"subscriptionType": root(s.Subscription), "types": types, "directives": dirs}
}

func introspectType(s *ast.Schema, d *ast.Definition) map[string]any {
	t := map[string]any{"kind": string(d.Kind), "name": d.Name, "description": orNil(d.Description), "specifiedByURL": nil,
		"fields": nil, "inputFields": nil, "interfaces": nil, "enumValues": nil, "possibleTypes": nil}
	switch d.Kind {
	case ast.Object, ast.Interface:
		fields, ifaces := []any{}, []any{}
		for _, f := range d.Fields {
			if !strings.HasPrefix(f.Name, "__") {
				dep := f.Directives.ForName("deprecated")
				fields = append(fields, map[string]any{"name": f.Name, "description": orNil(f.Description), "args": args(s, f.Arguments),
					"type": typeRef(s, f.Type), "isDeprecated": dep != nil, "deprecationReason": reason(dep)})
			}
		}
		for _, i := range d.Interfaces {
			ifaces = append(ifaces, typeRef(s, ast.NamedType(i, nil)))
		}
		t["fields"], t["interfaces"] = fields, ifaces
	case ast.InputObject:
		in := []any{}
		for _, f := range d.Fields {
			in = append(in, inputValue(s, f.Name, f.Description, f.Type, f.DefaultValue))
		}
		t["inputFields"] = in
	case ast.Enum:
		vals := []any{}
		for _, v := range d.EnumValues {
			dep := v.Directives.ForName("deprecated")
			vals = append(vals, map[string]any{"name": v.Name, "description": orNil(v.Description), "isDeprecated": dep != nil, "deprecationReason": reason(dep)})
		}
		t["enumValues"] = vals
	}
	if d.IsAbstractType() {
		pts := []any{}
		for _, p := range s.PossibleTypes[d.Name] {
			pts = append(pts, typeRef(s, ast.NamedType(p.Name, nil)))
		}
		t["possibleTypes"] = pts
	}
	return t
}

func args(s *ast.Schema, defs ast.ArgumentDefinitionList) []any {
	out := []any{}
	for _, a := range defs {
		out = append(out, inputValue(s, a.Name, a.Description, a.Type, a.DefaultValue))
	}
	return out
}

func inputValue(s *ast.Schema, name, desc string, t *ast.Type, def *ast.Value) map[string]any {
	v := map[string]any{"name": name, "description": orNil(desc), "type": typeRef(s, t), "defaultValue": nil, "isDeprecated": false, "deprecationReason": nil}
	if def != nil {
		v["defaultValue"] = def.String()
	}
	return v
}

func typeRef(s *ast.Schema, t *ast.Type) map[string]any {
	switch {
	case t.NonNull:
		inner := *t
		inner.NonNull = false
		return map[string]any{"kind": "NON_NULL", "name": nil, "ofType": typeRef(s, &inner)}
	case t.Elem != nil:
		return map[string]any{"kind": "LIST", "name": nil, "ofType": typeRef(s, t.Elem)}
	}
	kind := "SCALAR"
	if d := s.Types[t.NamedType]; d != nil {
		kind = string(d.Kind)
	}
	return map[string]any{"kind": kind, "name": t.NamedType, "ofType": nil}
}

func reason(dep *ast.Directive) any {
	if dep == nil {
		return nil
	}
	if r := dep.Arguments.ForName("reason"); r != nil {
		return r.Value.Raw
	}
	return "No longer supported"
}

// ---- small helpers ----

func orNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// toJSON encodes v the way AppSync prints JSON: no HTML escaping.
func toJSON(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(v) != nil {
		return "null"
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func num(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	}
	return 0
}

func strs(v any) []string {
	var out []string
	for _, e := range asList(v) {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	switch l := v.(type) {
	case []any:
		return l
	case *[]any:
		return *l
	}
	return nil
}
