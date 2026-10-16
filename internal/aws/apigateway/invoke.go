package apigateway

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/core"
)

// Data-plane errors, answered as {"message": …} like execute-api does.
var (
	errMissingToken = core.Errorf(403, "MissingAuthenticationTokenException", "Missing Authentication Token")
	errForbidden    = core.Errorf(403, "ForbiddenException", "Forbidden")
	errDenied       = core.Errorf(403, "AccessDeniedException", "User is not authorized to access this resource")
	errNotFound     = core.Errorf(404, "NotFoundException", "Not Found")
	errUnauthorized = core.Errorf(401, "UnauthorizedException", "Unauthorized")
	errMediaType    = core.Errorf(415, "UnsupportedMediaTypeException", "Unsupported Media Type")
	errInternal     = core.Errorf(500, "InternalServerErrorException", "Internal server error")
	errAuthorizer   = core.Errorf(500, "AuthorizerConfigurationException", "Internal server error")
	errBadGateway   = core.Errorf(502, "BadGatewayException", "Internal server error")
	errTimeout      = core.Errorf(504, "IntegrationTimeoutException", "Endpoint request timed out")
)

// call is one execute-api request. Everything it holds was copied out of the
// state, so integrations and authorizers run without the lock.
type call struct {
	r        *http.Request
	c        *core.Ctx
	id       string // request id
	apiID    string
	account  string
	region   string
	stage    string
	vars     map[string]string // stage variables
	full     string            // path as the API's own host sees it, stage included: /dev/items/42
	path     string            // path after the stage: /items/42
	resource string            // matched resource or route path: /items/{id}
	resID    string            // v1 resource id
	routeKey string            // v2 route key
	params   map[string]string // path parameters
	auth     map[string]any    // requestContext.authorizer
	keyID    string            // v1 API key that passed the usage plan check
	binary   []string          // v1 binary media types

	method *Method     // v1
	authz  *Authorizer // v1
	route  *Route      // v2
	integ  *HTTPIntegration
	hauthz *HTTPAuthorizer
	cors   *Cors

	conn   *wsConn                             // WebSocket
	twoWay bool                                // the route has a route response
	irs    map[string]*HTTPIntegrationResponse // the integration's responses
}

// invoke serves a request to a deployed API (see isInvoke for the URL forms).
func (s *Service) invoke(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "Invoke"
	x := &call{r: r, c: c, id: core.UUID()}
	x.apiID, x.full = invokeTarget(r)
	if _, conn, ok := strings.Cut(x.full, "/@connections/"); ok {
		s.manage(w, x, conn)
		return
	}
	w.Header().Set("x-amzn-RequestId", x.id)
	s.mu.Lock()
	rest, api := s.st.Rest[x.apiID], s.st.HTTP[x.apiID]
	ws := api != nil && api.ProtocolType == "WEBSOCKET"
	var err error
	if x.apiID == "" { // a custom domain name
		if err = s.viaDomain(x); err == nil {
			rest, api = s.st.Rest[x.apiID], s.st.HTTP[x.apiID]
			ws = api != nil && api.ProtocolType == "WEBSOCKET"
		}
	}
	switch {
	case err != nil:
	case rest != nil:
		err = s.resolveRest(rest, x)
	case ws:
		err = api.resolveWS(x)
	case api != nil:
		err = api.resolve(x)
	default:
		err = core.Errorf(404, "NotFoundException", "The API id '%s' does not correspond to a deployed API Gateway API", x.apiID)
	}
	s.mu.Unlock()
	if err == nil {
		err = c.Intercept()
	}
	switch {
	case err != nil:
	case rest != nil:
		err = s.runRest(w, x)
	case ws:
		err = s.connect(w, x)
	default:
		err = s.runHTTP(w, x)
	}
	if err != nil {
		fail(w, c, err)
	}
}

// invokeTarget splits an invoke URL into the API id and the path as the API's
// own host would see it.
func invokeTarget(r *http.Request) (id, full string) {
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/_aws/execute-api/"):
		id, rest, _ := strings.Cut(strings.TrimPrefix(p, "/_aws/execute-api/"), "/")
		return id, "/" + rest
	case strings.HasPrefix(p, "/restapis/") && strings.Contains(p, "/_user_request_"):
		id, rest, _ := strings.Cut(strings.TrimPrefix(p, "/restapis/"), "/")
		stage, p, _ := strings.Cut(rest, "/_user_request_")
		return id, "/" + stage + p
	}
	if id, base := core.SplitHost(r.Host, ""); strings.HasPrefix(base, "execute-api.") {
		return id, p
	}
	return "", p // pogi's own endpoint: the @connections API
}

// fail answers a data-plane error the way execute-api does.
func fail(w http.ResponseWriter, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-ErrorType", e.Code)
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]string{"message": e.Msg})
}

// match reports whether a resource or route path such as /items/{id} or
// /{proxy+} matches path, with the path parameters and a score: literal
// segments rank above {params}, which rank above a greedy {name+}, so the
// highest score (compared as strings) is the most specific match.
func match(tmpl, path string) (params map[string]string, score string, ok bool) {
	ts, ps := segments(tmpl), segments(path)
	params = map[string]string{}
	var b strings.Builder
	for i, t := range ts {
		switch {
		case i >= len(ps):
			return nil, "", false
		case strings.HasPrefix(t, "{") && strings.HasSuffix(t, "+}"):
			params[t[1:len(t)-2]] = strings.Join(ps[i:], "/")
			return params, b.String() + "0", true
		case strings.HasPrefix(t, "{") && strings.HasSuffix(t, "}"):
			params[t[1:len(t)-1]] = ps[i]
			b.WriteByte('1')
		case t == ps[i]:
			b.WriteByte('2')
		default:
			return nil, "", false
		}
	}
	return params, b.String(), len(ts) == len(ps)
}

func segments(p string) []string { return strings.FieldsFunc(p, func(r rune) bool { return r == '/' }) }

// resolveRest picks the stage, resource and method of a REST API call; lock held.
// ponytail: calls run against the live resources, not a deployment snapshot.
func (s *Service) resolveRest(a *restAPI, x *call) error {
	stage, rest, _ := strings.Cut(strings.TrimPrefix(x.full, "/"), "/")
	st := a.Stages[stage]
	if st == nil {
		return errForbidden
	}
	x.account, x.region, x.stage, x.path = a.Account, a.Region, stage, "/"+rest
	x.vars, x.binary = maps.Clone(st.Variables), slices.Clone(a.BinaryMediaTypes)
	best := ""
	for _, res := range a.Resources {
		m, rank := res.ResourceMethods[x.r.Method], "1" // an exact method beats ANY
		if m == nil {
			m, rank = res.ResourceMethods["ANY"], "0"
		}
		if m == nil {
			continue
		}
		if params, score, ok := match(res.Path, x.path); ok && score+rank > best {
			best, x.method, x.resource, x.resID, x.params = score+rank, m, res.Path, res.ID, params
		}
	}
	if x.method == nil {
		return errMissingToken
	}
	x.method = clone(x.method)
	if a.Policy != "" && x.policyDenies(a.Policy) {
		return errDenied
	}
	if t := x.method.AuthorizationType; t == "CUSTOM" || t == "COGNITO_USER_POOLS" {
		if x.authz = clone(a.Authorizers[x.method.AuthorizerID]); x.authz == nil {
			return errAuthorizer
		}
	}
	if x.method.AuthorizationType == "AWS_IAM" && !signed(x.r) {
		return errMissingToken
	}
	if x.method.ApiKeyRequired {
		if x.keyID = s.validKey(x.r.Header.Get("X-Api-Key"), a.ID, stage); x.keyID == "" {
			return errForbidden
		}
	}
	return nil
}

// signed reports whether the request carries a SigV4 signature, in the
// Authorization header or the query string (a presigned URL).
// ponytail: only the presence is checked, not the signature or the caller's
// IAM permissions; add them when a test needs to tell callers apart.
func signed(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") || r.URL.Query().Get("X-Amz-Algorithm") == "AWS4-HMAC-SHA256"
}

// policyDenies applies the API's resource policy: true when a Deny statement
// covers this call. The short form "execute-api:/stage/VERB/path" in a Resource
// means this API's ARN. Allow statements and implicit denies are not applied.
func (x *call) policyDenies(policy string) bool {
	arn := core.ARN("execute-api", x.region, x.account, x.apiID+"/"+x.stage+"/"+x.r.Method+x.path)
	pre := core.ARN("execute-api", x.region, x.account, x.apiID)
	policy = strings.ReplaceAll(policy, `"execute-api:/`, `"`+pre+`/`)
	ip, _, _ := net.SplitHostPort(x.r.RemoteAddr)
	return iam.ResourcePolicyDenies(policy, x.c.Principal, "execute-api:Invoke", arn, map[string]string{"aws:sourceip": ip})
}

// validKey returns the id of the enabled API key with this value in a usage plan of the API stage, or ""; lock held.
func (s *Service) validKey(value, api, stage string) string {
	for id, p := range s.st.Plans {
		if !slices.ContainsFunc(p.ApiStages, func(as ApiStage) bool { return as.ApiID == api && as.Stage == stage }) {
			continue
		}
		for _, k := range s.st.PlanKeys[id] {
			if key := s.st.Keys[k]; key != nil && key.Enabled && value != "" && key.Value == value {
				return key.ID
			}
		}
	}
	return ""
}

// resolve picks the stage and route of an HTTP API call; lock held. The
// stage segment is optional when the API has a $default stage.
func (a *httpAPI) resolve(x *call) error {
	x.account, x.region, x.cors = a.Account, a.Region, clone(a.CorsConfiguration)
	first, rest, _ := strings.Cut(strings.TrimPrefix(x.full, "/"), "/")
	st := a.Stages[first]
	x.stage, x.path = first, "/"+rest
	if st == nil {
		st, x.stage, x.path = a.Stages["$default"], "$default", x.full
	}
	if st == nil {
		return errNotFound
	}
	x.vars = maps.Clone(st.StageVariables)
	if x.preflight() {
		return nil
	}
	var def *Route
	best := ""
	for _, rt := range a.Routes {
		method, tmpl, _ := strings.Cut(rt.RouteKey, " ")
		rank := "1" // an exact method beats ANY
		switch {
		case rt.RouteKey == "$default":
			def = rt
			continue
		case method == "ANY":
			rank = "0"
		case method != x.r.Method:
			continue
		}
		if params, score, ok := match(tmpl, x.path); ok && score+rank > best {
			best, x.route, x.resource, x.params = score+rank, rt, tmpl, params
		}
	}
	if x.route = cmp.Or(x.route, def); x.route == nil {
		return errNotFound
	}
	x.route, x.routeKey = clone(x.route), x.route.RouteKey
	if x.integ = clone(a.Integrations[strings.TrimPrefix(x.route.Target, "integrations/")]); x.integ == nil {
		return errNotFound
	}
	if x.route.AuthorizationType == "AWS_IAM" && !signed(x.r) {
		return errForbidden
	}
	if t := x.route.AuthorizationType; t == "JWT" || t == "CUSTOM" {
		if x.hauthz = clone(a.Authorizers[x.route.AuthorizerID]); x.hauthz == nil {
			return errAuthorizer
		}
	}
	return nil
}

func (x *call) preflight() bool {
	return x.cors != nil && x.r.Method == http.MethodOptions && x.r.Header.Get("Origin") != "" &&
		x.r.Header.Get("Access-Control-Request-Method") != ""
}

func (s *Service) runRest(w http.ResponseWriter, x *call) error {
	if x.authz != nil {
		if err := s.authorizeRest(x); err != nil {
			return err
		}
	}
	// Like AWS, never pass a SigV4 signature (or any Authorization header on an
	// AWS_IAM method) on to the integration.
	if h := x.r.Header.Get("Authorization"); x.method.AuthorizationType == "AWS_IAM" || strings.HasPrefix(h, "AWS4-HMAC-SHA256") {
		x.r.Header.Del("Authorization")
	}
	switch in := x.method.MethodIntegration; {
	case in == nil:
		return errInternal
	case in.Type == "AWS_PROXY":
		return s.proxy(w, x, in.URI, restEvent(x, ""), false)
	case in.Type == "HTTP_PROXY":
		return x.forward(w, in.HTTPMethod, in.URI)
	default:
		return s.transform(w, x, in)
	}
}

func (s *Service) runHTTP(w http.ResponseWriter, x *call) error {
	if x.cors != nil {
		x.cors.write(w.Header(), x.r.Header.Get("Origin"), x.preflight())
		if x.preflight() {
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
	}
	if x.hauthz != nil {
		if err := s.authorizeHTTP(x); err != nil {
			return err
		}
	}
	switch in := x.integ; in.IntegrationType {
	case "AWS_PROXY":
		if in.PayloadFormatVersion == "2.0" {
			return s.proxy(w, x, in.IntegrationURI, httpEvent(x), true)
		}
		return s.proxy(w, x, in.IntegrationURI, restEvent(x, "1.0"), false)
	case "HTTP_PROXY":
		return x.forward(w, in.IntegrationMethod, in.IntegrationURI)
	case "MOCK":
		w.WriteHeader(http.StatusOK)
		return nil
	default:
		s.hub.Log.Warn("api gateway: unsupported HTTP API integration", "type", in.IntegrationType)
		return errInternal
	}
}

// write adds the CORS response headers for an allowed origin.
func (c *Cors) write(h http.Header, origin string, preflight bool) {
	switch {
	case origin == "":
		return
	case slices.Contains(c.AllowOrigins, "*") && !c.AllowCredentials:
		h.Set("Access-Control-Allow-Origin", "*")
	case slices.Contains(c.AllowOrigins, "*") || slices.Contains(c.AllowOrigins, origin):
		h.Set("Access-Control-Allow-Origin", origin)
	default:
		return
	}
	if c.AllowCredentials {
		h.Set("Access-Control-Allow-Credentials", "true")
	}
	if len(c.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(c.ExposeHeaders, ","))
	}
	if !preflight {
		return
	}
	if len(c.AllowMethods) > 0 {
		h.Set("Access-Control-Allow-Methods", strings.Join(c.AllowMethods, ","))
	}
	if len(c.AllowHeaders) > 0 {
		h.Set("Access-Control-Allow-Headers", strings.Join(c.AllowHeaders, ","))
	}
	if c.MaxAge != nil {
		h.Set("Access-Control-Max-Age", strconv.Itoa(*c.MaxAge))
	}
}

// ---- Lambda ----

var stageVarRef = regexp.MustCompile(`\$\{stageVariables\.([A-Za-z0-9_.:-]+)\}`)

func stageVars(s string, vars map[string]string) string {
	return stageVarRef.ReplaceAllStringFunc(s, func(m string) string { return vars[stageVarRef.FindStringSubmatch(m)[1]] })
}

// functionARN extracts the function from a Lambda integration URI
// (arn:<partition>:apigateway:<region>:lambda:path/2015-03-31/functions/<arn>/invocations);
// any other URI is taken to be the function ARN itself.
func functionARN(uri string, vars map[string]string) string {
	uri = stageVars(uri, vars)
	if _, fn, ok := strings.Cut(uri, "/functions/"); ok && strings.Contains(uri, ":lambda:path/") {
		return strings.TrimSuffix(fn, "/invocations")
	}
	return uri
}

// proxy runs a Lambda proxy integration and maps the function's result.
func (s *Service) proxy(w http.ResponseWriter, x *call, uri string, event []byte, v2 bool) error {
	out, err := s.hub.InvokeFrom(x.c.Trace, x.from(), functionARN(uri, x.vars), event)
	if err != nil {
		return s.invokeErr(uri, err)
	}
	if v2 {
		lambda.WriteHTTPResult(w, out)
		return nil
	}
	return writeProxy(w, out)
}

func (s *Service) invokeErr(uri string, err error) error {
	var fe *core.FunctionError
	if errors.As(err, &fe) {
		return errBadGateway
	}
	s.hub.Log.Warn("api gateway: lambda integration failed", "uri", uri, "err", err)
	return errInternal
}

// writeProxy maps a payload 1.0 Lambda result. Anything but an object of
// statusCode, headers, multiValueHeaders, body (a string) and isBase64Encoded is a 502.
func writeProxy(w http.ResponseWriter, out []byte) error {
	var keys map[string]json.RawMessage
	var res struct {
		StatusCode        int                 `json:"statusCode"`
		Headers           map[string]string   `json:"headers"`
		MultiValueHeaders map[string][]string `json:"multiValueHeaders"`
		Body              *string             `json:"body"`
		IsBase64Encoded   bool                `json:"isBase64Encoded"`
	}
	if json.Unmarshal(out, &keys) != nil || json.Unmarshal(out, &res) != nil {
		return errBadGateway
	}
	for k := range keys {
		if !slices.Contains([]string{"statusCode", "headers", "multiValueHeaders", "body", "isBase64Encoded"}, k) {
			return errBadGateway
		}
	}
	var body []byte
	if res.Body != nil {
		body = []byte(*res.Body)
		if res.IsBase64Encoded {
			b, err := base64.StdEncoding.DecodeString(*res.Body)
			if err != nil {
				return errBadGateway
			}
			body = b
		}
	}
	h := w.Header()
	for k, v := range res.Headers {
		h.Set(k, v)
	}
	for k, vs := range res.MultiValueHeaders {
		h[http.CanonicalHeaderKey(k)] = vs
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/json")
	}
	w.WriteHeader(cmp.Or(res.StatusCode, http.StatusOK))
	w.Write(body)
	return nil
}

// requestParts are the headers and query string of a payload 1.0 event.
func requestParts(r *http.Request) map[string]any {
	headers, multi := map[string]string{"Host": r.Host}, map[string][]string{"Host": {r.Host}}
	for k, v := range r.Header {
		headers[k], multi[k] = v[len(v)-1], v
	}
	ev := map[string]any{"headers": headers, "multiValueHeaders": multi, "queryStringParameters": nil, "multiValueQueryStringParameters": nil}
	if q := r.URL.Query(); len(q) > 0 {
		single := map[string]string{}
		for k, v := range q {
			single[k] = v[len(v)-1]
		}
		ev["queryStringParameters"], ev["multiValueQueryStringParameters"] = single, q
	}
	return ev
}

// restEvent is the REST API proxy event (payload 1.0); version is set for HTTP APIs.
func restEvent(x *call, version string) []byte {
	r := x.r
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	now := time.Now().UTC()
	rc := map[string]any{"resourceId": x.resID, "resourcePath": x.resource, "httpMethod": r.Method, "path": x.full,
		"stage": x.stage, "requestId": x.id, "extendedRequestId": x.id, "accountId": x.account, "apiId": x.apiID,
		"domainName": r.Host, "domainPrefix": x.apiID, "protocol": r.Proto, "requestTimeEpoch": now.UnixMilli(),
		"requestTime": now.Format("02/Jan/2006:15:04:05 -0700"), "identity": map[string]any{"sourceIp": ip, "userAgent": r.UserAgent()}}
	if x.auth != nil {
		rc["authorizer"] = x.auth
	}
	if x.keyID != "" {
		id := rc["identity"].(map[string]any)
		id["apiKey"], id["apiKeyId"] = r.Header.Get("X-Api-Key"), x.keyID
	}
	ev := requestParts(r)
	maps.Copy(ev, map[string]any{"resource": x.resource, "path": x.path, "httpMethod": r.Method, "pathParameters": orNil(x.params),
		"stageVariables": orNil(x.vars), "requestContext": rc, "body": nil, "isBase64Encoded": false})
	if version != "" {
		ev["version"] = version
	}
	if b := core.Body(r); len(b) > 0 {
		ev["body"] = string(b)
		if slices.ContainsFunc(x.binary, func(t string) bool { ok, _ := path.Match(t, mediaType(r.Header.Get("Content-Type"))); return ok }) {
			ev["body"], ev["isBase64Encoded"] = base64.StdEncoding.EncodeToString(b), true
		}
	}
	b, _ := json.Marshal(ev)
	return b
}

func orNil(m map[string]string) any {
	if len(m) == 0 {
		return nil
	}
	return m
}

// httpEvent is the HTTP API payload 2.0 event: the function URL event plus
// the stage, account, path parameters, stage variables and authorizer.
func httpEvent(x *call) []byte {
	r := x.r.WithContext(x.r.Context())
	u := *x.r.URL
	u.Path, u.RawPath = x.full, ""
	r.URL = &u
	var ev map[string]any
	json.Unmarshal(lambda.HTTPEvent(r, x.apiID, x.routeKey), &ev)
	rc := ev["requestContext"].(map[string]any)
	rc["stage"], rc["accountId"], rc["requestId"] = x.stage, x.account, x.id
	if x.auth != nil {
		rc["authorizer"] = x.auth
	}
	if len(x.params) > 0 {
		ev["pathParameters"] = x.params
	}
	if len(x.vars) > 0 {
		ev["stageVariables"] = x.vars
	}
	b, _ := json.Marshal(ev)
	return b
}

// ---- HTTP ----

var backend = &http.Client{Timeout: 30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// forward proxies the call to an HTTP backend and streams the answer back.
func (x *call) forward(w http.ResponseWriter, method, uri string) error {
	resp, err := x.send(method, uri, core.Body(x.r))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	maps.Copy(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return nil
}

// send calls an HTTP integration with the request's headers and query string.
func (x *call) send(method, uri string, body []byte) (*http.Response, error) {
	u, err := url.Parse(x.expand(uri))
	if err != nil {
		return nil, errInternal
	}
	q := u.Query()
	for k, vs := range x.r.URL.Query() {
		q[k] = append(q[k], vs...)
	}
	u.RawQuery = q.Encode()
	if method == "" || method == "ANY" {
		method = x.r.Method
	}
	req, err := http.NewRequestWithContext(x.r.Context(), method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errInternal
	}
	req.Header = x.r.Header.Clone()
	resp, err := backend.Do(req)
	if ne := net.Error(nil); errors.As(err, &ne) && ne.Timeout() {
		return nil, errTimeout
	} else if err != nil {
		x.c.Hub.Log.Warn("api gateway: HTTP integration failed", "uri", u.String(), "err", err)
		return nil, errInternal
	}
	return resp, nil
}

// expand substitutes path parameters ({id}, {proxy+}) and ${stageVariables.x} in an integration URI.
func (x *call) expand(uri string) string {
	uri = stageVars(uri, x.vars)
	for k, v := range x.params {
		v = strings.ReplaceAll(url.PathEscape(v), "%2F", "/")
		uri = strings.NewReplacer("{"+k+"}", v, "{"+k+"+}", v).Replace(uri)
	}
	return uri
}

// ---- non-proxy integrations ----

// transform runs an AWS (Lambda or another service), HTTP or MOCK integration: render the
// request template, call the backend, pick the integration response by
// selectionPattern and render its template.
func (s *Service) transform(w http.ResponseWriter, x *call, in *Integration) error {
	body, err := x.request(in)
	if err != nil {
		return err
	}
	var out []byte
	selector, backendHeader := "", http.Header{}
	switch in.Type {
	case "MOCK": // the request template says which response: {"statusCode": 200}
		var m struct {
			StatusCode json.Number `json:"statusCode"`
		}
		json.Unmarshal(body, &m)
		selector = cmp.Or(m.StatusCode.String(), "200")
	case "AWS":
		if !strings.Contains(in.URI, ":lambda:path/") { // another service: patterns match the status code
			var status int
			if out, status, backendHeader, err = s.awsService(x, in, body); err != nil {
				return err
			}
			selector = strconv.Itoa(status)
			break
		}
		if len(body) == 0 {
			body = []byte("{}")
		}
		out, err = s.hub.InvokeFrom(x.c.Trace, x.from(), functionARN(in.URI, x.vars), body)
		var fe *core.FunctionError
		if errors.As(err, &fe) { // selection patterns match the error message
			var e struct {
				ErrorMessage string `json:"errorMessage"`
			}
			json.Unmarshal(fe.Payload, &e)
			out, selector = fe.Payload, e.ErrorMessage
		} else if err != nil {
			return s.invokeErr(in.URI, err)
		}
	case "HTTP": // selection patterns match the status code
		resp, err := x.send(in.HTTPMethod, in.URI, body)
		if err != nil {
			return err
		}
		out, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		selector, backendHeader = strconv.Itoa(resp.StatusCode), resp.Header
	}
	ir := pick(in.IntegrationResponses, selector)
	if ir == nil {
		s.hub.Log.Warn("api gateway: no integration response matches and there is no default", "selector", selector)
		return errInternal
	}
	h := w.Header()
	for k, v := range ir.ResponseParameters {
		name, ok := strings.CutPrefix(k, "method.response.header.")
		switch {
		case !ok:
		case strings.HasPrefix(v, "'"):
			h.Set(name, strings.Trim(v, "'"))
		case strings.HasPrefix(v, "integration.response.header."):
			h.Set(name, backendHeader.Get(strings.TrimPrefix(v, "integration.response.header.")))
		}
	}
	if ct, tmpl, ok := template(ir.ResponseTemplates, x.r.Header.Get("Accept")); ok {
		out = []byte(x.render(tmpl, out))
		h.Set("Content-Type", cmp.Or(h.Get("Content-Type"), ct))
	}
	h.Set("Content-Type", cmp.Or(h.Get("Content-Type"), "application/json"))
	status, _ := strconv.Atoi(ir.StatusCode)
	w.WriteHeader(cmp.Or(status, http.StatusOK))
	w.Write(out)
	return nil
}

// request renders the integration request from the template for the
// request's content type, or passes the body through.
func (x *call) request(in *Integration) ([]byte, error) {
	body := core.Body(x.r)
	if t, ok := in.RequestTemplates[mediaType(x.r.Header.Get("Content-Type"))]; ok {
		return []byte(x.render(t, body)), nil
	}
	if in.PassthroughBehavior == "NEVER" || in.PassthroughBehavior == "WHEN_NO_TEMPLATES" && len(in.RequestTemplates) > 0 {
		return nil, errMediaType
	}
	return body, nil
}

func mediaType(ct string) string {
	ct, _, _ = strings.Cut(ct, ",")
	ct, _, _ = strings.Cut(ct, ";")
	return cmp.Or(strings.ToLower(strings.TrimSpace(ct)), "application/json")
}

// template picks a response template: the accepted type, else application/json, else the only one.
func template(templates map[string]string, accept string) (ct, tmpl string, ok bool) {
	for _, ct := range []string{mediaType(accept), "application/json"} {
		if t, ok := templates[ct]; ok {
			return ct, t, true
		}
	}
	for ct, t := range templates {
		return ct, t, len(templates) == 1
	}
	return "", "", false
}

// pick returns the integration response whose selectionPattern matches s, else the default one.
func pick(rs map[string]*IntegrationResponse, s string) *IntegrationResponse {
	var def *IntegrationResponse
	for _, code := range core.SortedKeys(rs) {
		ir := rs[code]
		if ir.SelectionPattern == "" {
			def = cmp.Or(def, ir)
		} else if re, err := regexp.Compile("^(?:" + ir.SelectionPattern + ")$"); err == nil && re.MatchString(s) {
			return ir
		}
	}
	return def
}

// render runs a mapping template (see vtl.go) over the request body.
// An invalid template is logged and passed through as it is.
func (x *call) render(tmpl string, body []byte) string {
	vars := map[string]any{"input": &vinput{x: x, body: body}, "util": vutil{}, "stageVariables": fromGo(x.vars)}
	if strings.Contains(tmpl, "context") {
		vars["context"] = x.vcontext()
	}
	out, err := vtlRender(tmpl, vars)
	if err != nil {
		x.c.Hub.Log.Warn("api gateway: invalid mapping template", "err", err)
		return tmpl
	}
	return out
}

// jsonPath resolves a simple JSONPath ($ or $.a.b) in a JSON document.
func jsonPath(doc []byte, p string) (any, bool) {
	var v any
	if json.Unmarshal(doc, &v) != nil {
		return nil, false
	}
	for _, k := range strings.Split(strings.TrimPrefix(p, "$"), ".") {
		if k == "" {
			continue
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		v = m[k]
	}
	return v, true
}

// param is $input.params('name'): a path parameter, query parameter or header.
func (x *call) param(name string) string {
	if v, ok := x.params[name]; ok {
		return v
	}
	if v := x.r.URL.Query().Get(name); v != "" {
		return v
	}
	return x.r.Header.Get(name)
}

// context is $context.<k>.
func (x *call) context(k string) string {
	if p, ok := strings.CutPrefix(k, "authorizer."); ok {
		var v any = x.auth
		for _, part := range strings.Split(p, ".") {
			m, _ := v.(map[string]any)
			v = m[part]
		}
		if s, ok := v.(string); ok || v == nil {
			return s
		}
		b, _ := json.Marshal(v)
		return string(b)
	}
	ip, _, _ := net.SplitHostPort(x.r.RemoteAddr)
	conn := ""
	if x.conn != nil {
		conn = x.conn.id
	}
	return map[string]string{"requestId": x.id, "extendedRequestId": x.id, "stage": x.stage, "httpMethod": x.r.Method,
		"resourcePath": x.resource, "resourceId": x.resID, "apiId": x.apiID, "accountId": x.account, "path": x.full,
		"domainName": x.r.Host, "protocol": x.r.Proto, "routeKey": x.routeKey, "identity.sourceIp": ip,
		"identity.userAgent": x.r.UserAgent(), "connectionId": conn}[k]
}

// ---- authorizers ----

// identity resolves an identity source: method.request.header.X,
// $request.querystring.x, route.request.header.X (WebSocket),
// stageVariables.x, $context.x …
func (x *call) identity(src string) string {
	src = strings.TrimPrefix(strings.TrimSpace(src), "$")
	src = strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(src, "method."), "route."), "request.")
	kind, k, _ := strings.Cut(src, ".")
	switch kind {
	case "header":
		return x.r.Header.Get(k)
	case "querystring":
		return x.r.URL.Query().Get(k)
	case "stageVariables":
		return x.vars[k]
	case "context":
		return x.context(k)
	}
	return ""
}

// from names the API as a resource, for the span log and the map.
func (x *call) from() string {
	return core.ARN("apigateway", x.region, "", "/apis/"+x.apiID)
}

// arn is the call's execute-api ARN, as authorizer policies name it.
func (x *call) arn() string {
	return core.ARN("execute-api", x.region, x.account, x.apiID+"/"+x.stage+"/"+x.r.Method+x.path)
}

// authorizeRest runs a v1 TOKEN, REQUEST or COGNITO_USER_POOLS authorizer and fills x.auth.
func (s *Service) authorizeRest(x *call) error {
	a := x.authz
	if a.Type == "COGNITO_USER_POOLS" {
		claims, ok := jwtClaims(x.identity(cmp.Or(a.IdentitySource, "method.request.header.Authorization")))
		iss, _ := claims["iss"].(string)
		if !ok || !slices.ContainsFunc(a.ProviderARNs, func(arn string) bool {
			_, pool, _ := strings.Cut(arn, "userpool/")
			return pool != "" && strings.HasSuffix(iss, "/"+pool)
		}) {
			return errUnauthorized
		}
		x.auth = map[string]any{"claims": claims}
		return nil
	}
	event := map[string]any{"type": a.Type, "methodArn": x.arn()}
	if a.Type == "TOKEN" {
		token := x.identity(a.IdentitySource)
		if token == "" {
			return errUnauthorized
		}
		if re := a.IdentityValidationExpression; re != "" {
			if ok, _ := regexp.MatchString("^(?:"+re+")$", token); !ok {
				return errUnauthorized
			}
		}
		event["authorizationToken"] = token
	} else {
		for _, src := range strings.Split(a.IdentitySource, ",") {
			if strings.TrimSpace(src) != "" && x.identity(src) == "" {
				return errUnauthorized
			}
		}
		json.Unmarshal(restEvent(x, ""), &event) // adds the request to type and methodArn
		delete(event, "body")
		delete(event, "isBase64Encoded")
	}
	res, err := s.authorizer(x, a.AuthorizerURI, event)
	if err != nil {
		return err
	}
	if !allows(res["policyDocument"], x.arn()) {
		return errDenied
	}
	ctx := map[string]any{}
	if m, ok := res["context"].(map[string]any); ok {
		maps.Copy(ctx, m)
	}
	ctx["principalId"] = res["principalId"]
	x.auth = ctx
	return nil
}

// authorizeHTTP runs a v2 JWT or REQUEST Lambda authorizer and fills x.auth.
func (s *Service) authorizeHTTP(x *call) error {
	a := x.hauthz
	if a.AuthorizerType == "JWT" {
		src := "$request.header.Authorization"
		if len(a.IdentitySource) > 0 {
			src = a.IdentitySource[0]
		}
		claims, ok := jwtClaims(x.identity(src))
		cfg := a.JwtConfiguration
		if !ok || cfg == nil || claims["iss"] != cfg.Issuer || !audience(claims, cfg.Audience) {
			return errUnauthorized
		}
		scope, _ := claims["scope"].(string)
		scopes := strings.Fields(scope)
		if want := x.route.AuthorizationScopes; len(want) > 0 && !slices.ContainsFunc(want, func(s string) bool { return slices.Contains(scopes, s) }) {
			return errForbidden
		}
		x.auth = map[string]any{"jwt": map[string]any{"claims": claims, "scopes": scopes}}
		return nil
	}
	var ids []string
	for _, src := range a.IdentitySource {
		v := x.identity(src)
		if v == "" {
			return errUnauthorized
		}
		ids = append(ids, v)
	}
	event := map[string]any{}
	if a.AuthorizerPayloadFormatVersion == "2.0" {
		json.Unmarshal(httpEvent(x), &event)
		event["routeArn"], event["identitySource"] = x.arn(), ids
	} else {
		json.Unmarshal(restEvent(x, "1.0"), &event)
		event["methodArn"], event["identitySource"] = x.arn(), strings.Join(ids, ",")
	}
	event["type"] = "REQUEST"
	delete(event, "body")
	delete(event, "isBase64Encoded")
	res, err := s.authorizer(x, a.AuthorizerURI, event)
	if err != nil {
		return err
	}
	if a.EnableSimpleResponses {
		if ok, _ := res["isAuthorized"].(bool); !ok {
			return errForbidden
		}
	} else if !allows(res["policyDocument"], x.arn()) {
		return errForbidden
	}
	x.auth = map[string]any{"lambda": res["context"]}
	return nil
}

// authorizer invokes a Lambda authorizer; a function failing with "Unauthorized" means 401.
func (s *Service) authorizer(x *call, uri string, event map[string]any) (map[string]any, error) {
	b, _ := json.Marshal(event)
	out, err := s.hub.InvokeFrom(x.c.Trace, x.from(), functionARN(uri, x.vars), b)
	var fe *core.FunctionError
	var res map[string]any
	switch {
	case errors.As(err, &fe):
		var e struct {
			ErrorMessage string `json:"errorMessage"`
		}
		json.Unmarshal(fe.Payload, &e)
		if e.ErrorMessage == "Unauthorized" {
			return nil, errUnauthorized
		}
		return nil, errAuthorizer
	case err != nil:
		s.hub.Log.Warn("api gateway: authorizer failed", "uri", uri, "err", err)
		return nil, errAuthorizer
	case json.Unmarshal(out, &res) != nil:
		return nil, errAuthorizer
	}
	return res, nil
}

// allows evaluates a Lambda authorizer's IAM policy for arn: an explicit Deny wins over Allow.
func allows(policy any, arn string) bool {
	doc, _ := policy.(map[string]any)
	allowed := false
	for _, st := range list(doc["Statement"]) {
		st, _ := st.(map[string]any)
		hit := slices.ContainsFunc(list(st["Resource"]), func(r any) bool { p, _ := r.(string); return glob(p, arn) })
		switch {
		case !hit:
		case st["Effect"] == "Deny":
			return false
		case st["Effect"] == "Allow":
			allowed = true
		}
	}
	return allowed
}

// list reads a JSON value that may be one item or a list of them.
func list(v any) []any {
	if l, ok := v.([]any); ok || v == nil {
		return l
	}
	return []any{v}
}

// glob matches an IAM resource pattern with * and ? wildcards.
func glob(pattern, s string) bool {
	re := strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(pattern))
	ok, _ := regexp.MatchString("^"+re+"$", s)
	return ok
}

// jwtClaims decodes a bearer JWT's claims and checks exp and nbf.
// ponytail: the signature is not verified; check it against the issuer's JWKS when tests need forged tokens rejected.
func jwtClaims(token string) (map[string]any, bool) {
	token = strings.TrimSpace(token)
	if len(token) > 7 && strings.EqualFold(token[:7], "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	p := strings.Split(token, ".")
	if len(p) != 3 {
		return nil, false
	}
	var c map[string]any
	if b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(p[1], "=")); err != nil || json.Unmarshal(b, &c) != nil {
		return nil, false
	}
	now := float64(time.Now().Unix())
	exp, ok := c["exp"].(float64)
	if nbf, set := c["nbf"].(float64); !ok || now >= exp || set && now < nbf {
		return nil, false
	}
	return c, true
}

// audience reports whether a JWT's aud (or Cognito client_id) is one of want.
func audience(claims map[string]any, want []string) bool {
	return slices.ContainsFunc(append(list(claims["aud"]), claims["client_id"]), func(v any) bool {
		s, _ := v.(string)
		return s != "" && slices.Contains(want, s)
	})
}
