package cloudformation

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// SAM's API Gateway side: AWS::Serverless::Api (and the implicit
// ServerlessRestApi) become a RestApi built from a Swagger document, a
// deployment and a stage. AWS::Serverless::HttpApi (and ServerlessHttpApi)
// become an HTTP API with one integration and one route per event.

const (
	samImplicitREST = "ServerlessRestApi"
	samImplicitHTTP = "ServerlessHttpApi"
)

const samRestProps = "Name StageName DefinitionBody DefinitionUri Description BinaryMediaTypes EndpointConfiguration MethodSettings Variables " +
	"TracingEnabled CacheClusterEnabled CacheClusterSize AccessLogSetting Tags Cors Auth OpenApiVersion Mode AlwaysDeploy"

type samRoute struct {
	path, method string
	integ        map[string]any
	auth         any
}

type samRest struct {
	id       string
	src      *samSrc
	props    map[string]any
	explicit bool
	routes   []samRoute
}

type samHTTP struct {
	id       string
	src      *samSrc
	props    map[string]any
	explicit bool
}

var samBraces = regexp.MustCompile(`\{[^}]*\}`)

// samStar writes a path the way an execute-api ARN does: {id} becomes *.
func samStar(path string) string { return samBraces.ReplaceAllString(path, "*") }

func samMethodSeg(method string) string {
	if method == "" || strings.EqualFold(method, "ANY") {
		return "*"
	}
	return strings.ToUpper(method)
}

// samExecArn is the execute-api ARN that lets API Gateway call a function.
func samExecArn(api, method, path string) any {
	return samSub("arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/${__Stage__}/"+samMethodSeg(method)+samStar(path),
		map[string]any{"__ApiId__": samRef(api), "__Stage__": "*"})
}

// restOf finds the API an event belongs to, making the implicit one when it has no RestApiId.
func (x *samExp) restOf(what string, p map[string]any) *samRest {
	if v := p["RestApiId"]; v != nil {
		ref, _ := v.(map[string]any)
		r := x.rest[str(ref["Ref"])]
		if r == nil {
			x.fail("%s: RestApiId must be a !Ref to an AWS::Serverless::Api in this template", what)
		}
		return r
	}
	r := x.rest[samImplicitREST]
	if r == nil {
		props, _ := samMerge(x.globals["Api"], nil).(map[string]any)
		if props == nil {
			props = map[string]any{}
		}
		r = &samRest{id: samImplicitREST, src: &samSrc{id: samImplicitREST, attrs: map[string]any{}}, props: props}
		x.rest[r.id] = r
	}
	return r
}

func (x *samExp) httpOf(what string, p map[string]any) *samHTTP {
	if v := p["ApiId"]; v != nil {
		ref, _ := v.(map[string]any)
		h := x.http[str(ref["Ref"])]
		if h == nil {
			x.fail("%s: ApiId must be a !Ref to an AWS::Serverless::HttpApi in this template", what)
		}
		return h
	}
	h := x.http[samImplicitHTTP]
	if h == nil {
		props, _ := samMerge(x.globals["HttpApi"], nil).(map[string]any)
		if props == nil {
			props = map[string]any{}
		}
		h = &samHTTP{id: samImplicitHTTP, src: &samSrc{id: samImplicitHTTP, attrs: map[string]any{}}, props: props}
		x.http[h.id] = h
	}
	return h
}

func (x *samExp) restRoute(what string, r *samRest, path, method string, integ map[string]any, auth any) {
	if !strings.HasPrefix(path, "/") {
		x.fail("%s: Path must start with /", what)
		return
	}
	if !slices.Contains(strings.Fields("get post put delete head patch options any"), method) {
		x.fail("%s: Method %s is not valid", what, method)
		return
	}
	if a, ok := auth.(map[string]any); ok {
		x.check(what+" Auth", a, "Authorizer ApiKeyRequired AuthorizationScopes")
	}
	r.routes = append(r.routes, samRoute{path, method, integ, auth})
}

// samKeyCase makes the first letter of every key upper case (jwt "issuer" -> "Issuer").
func samKeyCase(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := map[string]any{}
	for k, val := range m {
		out[strings.ToUpper(k[:1])+k[1:]] = val
	}
	return out
}

// restAuth reads Auth: the Swagger security definitions of its authorizers.
func (x *samExp) restAuth(r *samRest, auth map[string]any) (defs map[string]any, scopes map[string]any) {
	defs, scopes = map[string]any{}, map[string]any{}
	x.check("Auth of AWS::Serverless::Api "+r.id, auth, "DefaultAuthorizer Authorizers ApiKeyRequired AddDefaultAuthorizerToCorsPreflight")
	authorizers, _ := auth["Authorizers"].(map[string]any)
	for _, name := range core.SortedKeys(authorizers) {
		a, _ := authorizers[name].(map[string]any)
		what := fmt.Sprintf("authorizer %s of %s", name, r.id)
		x.check(what, a, "UserPoolArn FunctionArn FunctionPayloadType FunctionInvokeRole Identity AuthorizationScopes")
		id, _ := a["Identity"].(map[string]any)
		x.check(what+" Identity", id, "Header ValidationExpression ReauthorizeEvery QueryStrings Context StageVariables")
		header := "Authorization"
		if h := id["Header"]; h != nil {
			header = str(h)
		}
		az := map[string]any{}
		def := map[string]any{"type": "apiKey", "name": header, "in": "header"}
		if id["ValidationExpression"] != nil {
			az["identityValidationExpression"] = id["ValidationExpression"]
		}
		if id["ReauthorizeEvery"] != nil {
			az["authorizerResultTtlInSeconds"] = id["ReauthorizeEvery"]
		}
		switch {
		case a["UserPoolArn"] != nil:
			arns := a["UserPoolArn"]
			if _, ok := arns.([]any); !ok {
				arns = []any{arns}
			}
			az["type"], az["providerARNs"] = "cognito_user_pools", arns
			def["x-amazon-apigateway-authtype"] = "cognito_user_pools"
		case a["FunctionArn"] != nil:
			typ := strings.ToLower(or(str(a["FunctionPayloadType"]), "TOKEN"))
			if typ != "token" && typ != "request" {
				x.fail("%s: FunctionPayloadType must be TOKEN or REQUEST", what)
				continue
			}
			az["type"] = typ
			az["authorizerUri"] = samSub("arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${__FunctionArn__}/invocations", map[string]any{"__FunctionArn__": a["FunctionArn"]})
			if a["FunctionInvokeRole"] != nil {
				az["authorizerCredentials"] = a["FunctionInvokeRole"]
			}
			def["x-amazon-apigateway-authtype"] = "custom"
			if typ == "request" {
				var src []string
				if id["Header"] != nil {
					src = append(src, "method.request.header."+header)
				}
				for _, kp := range [][2]string{{"QueryStrings", "method.request.querystring."}, {"StageVariables", "stageVariables."}, {"Context", "context."}} {
					for _, n := range list(id[kp[0]]) {
						src = append(src, kp[1]+n)
					}
				}
				if len(src) == 0 {
					x.fail("%s: a REQUEST authorizer needs at least one Identity source", what)
					continue
				}
				az["identitySource"] = strings.Join(src, ",")
				def["name"] = "Unused"
			}
			x.add(r.src, r.id+name+"AuthorizerPermission", "AWS::Lambda::Permission", map[string]any{"Action": "lambda:InvokeFunction", "FunctionName": a["FunctionArn"],
				"Principal": "apigateway.amazonaws.com", "SourceArn": samSub("arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/authorizers/*", map[string]any{"__ApiId__": samRef(r.id)})})
		default:
			x.fail("%s needs UserPoolArn (Cognito) or FunctionArn (Lambda)", what)
			continue
		}
		def["x-amazon-apigateway-authorizer"] = az
		defs[name] = def
		scopes[name] = a["AuthorizationScopes"]
	}
	return defs, scopes
}

var samVerbs = []string{"delete", "get", "head", "options", "patch", "post", "put"}

func (x *samExp) finishRest(r *samRest) {
	id := r.id
	what := "AWS::Serverless::Api " + id
	x.check(what, r.props, samRestProps)
	if x.err != nil {
		return
	}
	stage := "Prod"
	if s, ok := r.props["StageName"].(string); ok && s != "" {
		stage = s
	} else if r.explicit {
		x.fail("%s: StageName is required and must be a plain string", what)
		return
	}
	title := any(samRef("AWS::StackName"))
	if r.props["Name"] != nil {
		title = r.props["Name"]
	}

	// the Swagger document: the given one, or a new one
	var body map[string]any
	var raw any
	switch {
	case r.props["DefinitionBody"] != nil:
		if m, ok := samClone(r.props["DefinitionBody"]).(map[string]any); ok && !samIntrinsic(m) {
			body = m
		} else {
			raw = r.props["DefinitionBody"]
		}
	case r.props["DefinitionUri"] != nil:
		text, ok := x.samS3Doc(what+" DefinitionUri", r.props["DefinitionUri"])
		if !ok {
			return
		}
		doc, err := parseDoc(text)
		m, _ := doc.(map[string]any)
		if err != nil || m == nil {
			x.fail("%s: DefinitionUri is not a valid Swagger or OpenAPI document", what)
			return
		}
		body = m
	default:
		body = map[string]any{"swagger": "2.0", "info": map[string]any{"version": "1.0", "title": title}}
		if r.props["Description"] != nil {
			body["info"].(map[string]any)["description"] = r.props["Description"]
		}
	}
	var bodyOut = any(raw)
	if body != nil {
		x.swagger(r, body)
		bodyOut = body
	} else if len(r.routes) > 0 {
		x.fail("%s: events cannot be added to a DefinitionBody that is not a literal document", what)
	}
	if x.err != nil {
		return
	}

	ap := map[string]any{"Body": bodyOut}
	for _, k := range []string{"Name", "Description", "BinaryMediaTypes"} {
		if r.props[k] != nil {
			ap[k] = r.props[k]
		}
	}
	switch e := r.props["EndpointConfiguration"].(type) {
	case string:
		ap["EndpointConfiguration"] = map[string]any{"Types": []any{e}}
	case map[string]any:
		ec := map[string]any{"Types": []any{e["Type"]}}
		if e["VPCEndpointIds"] != nil {
			ec["VpcEndpointIds"] = e["VPCEndpointIds"]
		}
		ap["EndpointConfiguration"] = ec
	}
	if r.props["Tags"] != nil {
		ap["Tags"] = samTags(r.props["Tags"], "lambda")
	}
	x.addMain(r.src, id, "AWS::ApiGateway::RestApi", ap)

	did, sid := id+"Deployment"+samHash(bodyOut), id+samAlnum(stage)+"Stage"
	x.add(r.src, did, "AWS::ApiGateway::Deployment", map[string]any{"RestApiId": samRef(id), "Description": "RestApi deployment id: " + samHash(bodyOut)})
	sp := map[string]any{"RestApiId": samRef(id), "DeploymentId": samRef(did), "StageName": stage}
	for _, k := range strings.Fields("Variables MethodSettings TracingEnabled CacheClusterEnabled CacheClusterSize AccessLogSetting") {
		if r.props[k] != nil {
			sp[k] = r.props[k]
		}
	}
	x.add(r.src, sid, "AWS::ApiGateway::Stage", sp)
	x.rename[id+".Stage"], x.rename[id+".Deployment"] = sid, did
}

// swagger puts the routes, security definitions and CORS methods into body.
func (x *samExp) swagger(r *samRest, body map[string]any) {
	auth, _ := r.props["Auth"].(map[string]any)
	defs, scopes := x.restAuth(r, auth)
	defaultAuth := str(auth["DefaultAuthorizer"])
	paths, _ := body["paths"].(map[string]any)
	if paths == nil {
		paths = map[string]any{}
		body["paths"] = paths
	}
	for _, rt := range r.routes {
		a, _ := rt.auth.(map[string]any)
		name := defaultAuth
		if a["Authorizer"] != nil {
			name = str(a["Authorizer"])
		}
		sec := map[string]any{}
		switch name {
		case "", "NONE":
		case "AWS_IAM":
			defs["AWS_IAM"] = map[string]any{"type": "apiKey", "name": "Authorization", "in": "header", "x-amazon-apigateway-authtype": "awsSigv4"}
			sec["AWS_IAM"] = []any{}
		default:
			if defs[name] == nil {
				x.fail("Unable to set Authorizer %s on API %s because it wasn't defined in the Auth.Authorizers", name, r.id)
				return
			}
			sc := scopes[name]
			if a["AuthorizationScopes"] != nil {
				sc = a["AuthorizationScopes"]
			}
			if sc == nil {
				sc = []any{}
			}
			sec[name] = sc
		}
		keyRequired := auth["ApiKeyRequired"] == true
		if a["ApiKeyRequired"] != nil {
			keyRequired = a["ApiKeyRequired"] == true
		}
		if keyRequired {
			defs["api_key"] = map[string]any{"type": "apiKey", "name": "x-api-key", "in": "header"}
			sec["api_key"] = []any{}
		}
		op := map[string]any{"x-amazon-apigateway-integration": rt.integ}
		if len(sec) > 0 {
			op["security"] = []any{sec}
		}
		pm, _ := paths[rt.path].(map[string]any)
		if pm == nil {
			pm = map[string]any{}
			paths[rt.path] = pm
		}
		verb := rt.method
		if verb == "any" {
			verb = "x-amazon-apigateway-any-method"
		}
		if old, ok := pm[verb].(map[string]any); ok { // the document had the operation: add to it
			for k, v := range op {
				old[k] = v
			}
		} else {
			op["responses"] = map[string]any{}
			pm[verb] = op
		}
	}
	if len(defs) > 0 {
		sd, _ := body["securityDefinitions"].(map[string]any)
		if sd == nil {
			sd = map[string]any{}
			body["securityDefinitions"] = sd
		}
		for k, v := range defs {
			sd[k] = v
		}
	}
	if c := r.props["Cors"]; c != nil {
		x.cors(r, paths, c)
	}
}

// cors adds an OPTIONS method that answers the CORS preflight to every path.
func (x *samExp) cors(r *samRest, paths map[string]any, c any) {
	m, ok := c.(map[string]any)
	if !ok {
		m = map[string]any{"AllowOrigin": c}
	}
	x.check("Cors of AWS::Serverless::Api "+r.id, m, "AllowOrigin AllowMethods AllowHeaders MaxAge AllowCredentials")
	if m["AllowOrigin"] == nil {
		x.fail("Cors of AWS::Serverless::Api %s needs AllowOrigin", r.id)
		return
	}
	quote := func(v any) any {
		if s, ok := v.(string); ok || v == nil {
			return s
		}
		if samIntrinsic(v) {
			return v
		}
		return "'" + str(v) + "'"
	}
	for _, path := range core.SortedKeys(paths) {
		pm, _ := paths[path].(map[string]any)
		if pm == nil || pm["options"] != nil {
			continue
		}
		methods := m["AllowMethods"]
		if methods == nil {
			var vs []string
			for _, k := range core.SortedKeys(pm) {
				switch {
				case k == "x-amazon-apigateway-any-method":
					vs = append(vs, "DELETE", "GET", "HEAD", "PATCH", "POST", "PUT")
				case slices.Contains(samVerbs, k):
					vs = append(vs, strings.ToUpper(k))
				}
			}
			vs = append(vs, "OPTIONS")
			slices.Sort(vs)
			methods = "'" + strings.Join(slices.Compact(vs), ",") + "'"
		}
		headers := m["AllowHeaders"]
		if headers == nil {
			headers = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
		}
		rp := map[string]any{
			"method.response.header.Access-Control-Allow-Origin":  quote(m["AllowOrigin"]),
			"method.response.header.Access-Control-Allow-Headers": quote(headers),
			"method.response.header.Access-Control-Allow-Methods": quote(methods),
		}
		hdrs := map[string]any{"Access-Control-Allow-Origin": map[string]any{"type": "string"}, "Access-Control-Allow-Headers": map[string]any{"type": "string"},
			"Access-Control-Allow-Methods": map[string]any{"type": "string"}}
		if m["MaxAge"] != nil {
			rp["method.response.header.Access-Control-Max-Age"] = quote(m["MaxAge"])
			hdrs["Access-Control-Max-Age"] = map[string]any{"type": "string"}
		}
		if m["AllowCredentials"] == true {
			rp["method.response.header.Access-Control-Allow-Credentials"] = "'true'"
			hdrs["Access-Control-Allow-Credentials"] = map[string]any{"type": "string"}
		}
		pm["options"] = map[string]any{
			"summary": "CORS support", "consumes": []any{"application/json"}, "produces": []any{"application/json"},
			"responses": map[string]any{"200": map[string]any{"description": "Default response for CORS method", "headers": hdrs}},
			"x-amazon-apigateway-integration": map[string]any{
				"type":             "mock",
				"requestTemplates": map[string]any{"application/json": "{\n  \"statusCode\" : 200\n}\n"},
				"responses": map[string]any{"default": map[string]any{"statusCode": "200", "responseParameters": rp,
					"responseTemplates": map[string]any{"application/json": "{}\n"}}},
			},
		}
	}
}

// ---- HTTP APIs ----

const samHTTPProps = "Description StageName StageVariables AccessLogSettings DefaultRouteSettings RouteSettings CorsConfiguration Auth Tags " +
	"DisableExecuteApiEndpoint FailOnWarnings"

// httpRoute adds the integration and route of one HttpApi event.
func (x *samExp) httpRoute(src *samSrc, what string, h *samHTTP, base, key string, integ map[string]any, evAuth any) {
	integ["ApiId"] = samRef(h.id)
	x.add(src, base+"Integration", "AWS::ApiGatewayV2::Integration", integ)
	rp := map[string]any{"ApiId": samRef(h.id), "RouteKey": key, "Target": samSub("integrations/${"+base+"Integration}", nil)}
	auth, _ := h.props["Auth"].(map[string]any)
	authorizers, _ := auth["Authorizers"].(map[string]any)
	a, _ := evAuth.(map[string]any)
	x.check(what+" Auth", a, "Authorizer AuthorizationScopes")
	name := str(auth["DefaultAuthorizer"])
	if a["Authorizer"] != nil {
		name = str(a["Authorizer"])
	}
	switch name {
	case "", "NONE":
	case "AWS_IAM":
		rp["AuthorizationType"] = "AWS_IAM"
	default:
		az, _ := authorizers[name].(map[string]any)
		if az == nil {
			x.fail("%s: authorizer %s is not defined in Auth.Authorizers of %s", what, name, h.id)
			return
		}
		rp["AuthorizationType"] = "CUSTOM"
		if az["JwtConfiguration"] != nil {
			rp["AuthorizationType"] = "JWT"
		}
		rp["AuthorizerId"] = samRef(h.id + name + "Authorizer")
		if sc := a["AuthorizationScopes"]; sc != nil {
			rp["AuthorizationScopes"] = sc
		} else if sc := az["AuthorizationScopes"]; sc != nil {
			rp["AuthorizationScopes"] = sc
		}
	}
	x.add(src, base+"Route", "AWS::ApiGatewayV2::Route", rp)
}

func (x *samExp) finishHTTP(h *samHTTP) {
	id := h.id
	what := "AWS::Serverless::HttpApi " + id
	x.check(what, h.props, samHTTPProps)
	if x.err != nil {
		return
	}
	ap := map[string]any{"Name": samRef("AWS::StackName"), "ProtocolType": "HTTP"}
	if h.props["Description"] != nil {
		ap["Description"] = h.props["Description"]
	}
	switch c := h.props["CorsConfiguration"].(type) {
	case bool:
		if c {
			ap["CorsConfiguration"] = map[string]any{"AllowOrigins": []any{"*"}, "AllowHeaders": []any{"*"}, "AllowMethods": []any{"*"}}
		}
	case map[string]any:
		ap["CorsConfiguration"] = c
	}
	tags := map[string]any{"httpapi:createdBy": "SAM"}
	if t, ok := h.props["Tags"].(map[string]any); ok {
		for k, v := range t {
			tags[k] = v
		}
	}
	ap["Tags"] = tags
	x.addMain(h.src, id, "AWS::ApiGatewayV2::Api", ap)

	auth, _ := h.props["Auth"].(map[string]any)
	x.check("Auth of "+what, auth, "Authorizers DefaultAuthorizer EnableIamAuthorizer")
	authorizers, _ := auth["Authorizers"].(map[string]any)
	for _, name := range core.SortedKeys(authorizers) {
		a, _ := authorizers[name].(map[string]any)
		aw := fmt.Sprintf("authorizer %s of %s", name, id)
		x.check(aw, a, "AuthorizationScopes JwtConfiguration IdentitySource FunctionArn FunctionInvokeRole AuthorizerPayloadFormatVersion EnableSimpleResponses Identity")
		zp := map[string]any{"ApiId": samRef(id), "Name": name}
		switch {
		case a["JwtConfiguration"] != nil:
			zp["AuthorizerType"], zp["JwtConfiguration"] = "JWT", samKeyCase(a["JwtConfiguration"])
			zp["IdentitySource"] = []any{or(str(a["IdentitySource"]), "$request.header.Authorization")}
		case a["FunctionArn"] != nil:
			zp["AuthorizerType"] = "REQUEST"
			zp["AuthorizerUri"] = samSub("arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${__FunctionArn__}/invocations", map[string]any{"__FunctionArn__": a["FunctionArn"]})
			zp["AuthorizerPayloadFormatVersion"] = or(str(a["AuthorizerPayloadFormatVersion"]), "2.0")
			for from, to := range map[string]string{"EnableSimpleResponses": "EnableSimpleResponses", "FunctionInvokeRole": "AuthorizerCredentialsArn"} {
				if a[from] != nil {
					zp[to] = a[from]
				}
			}
			idn, _ := a["Identity"].(map[string]any)
			x.check(aw+" Identity", idn, "Headers QueryStrings StageVariables Context ReauthorizeEvery")
			src := []any{}
			for _, kp := range [][2]string{{"Headers", "$request.header."}, {"QueryStrings", "$request.querystring."}, {"StageVariables", "$stageVariables."}, {"Context", "$context."}} {
				for _, n := range list(idn[kp[0]]) {
					src = append(src, kp[1]+n)
				}
			}
			zp["IdentitySource"] = src
			if idn["ReauthorizeEvery"] != nil {
				zp["AuthorizerResultTtlInSeconds"] = idn["ReauthorizeEvery"]
			}
			x.add(h.src, id+name+"AuthorizerPermission", "AWS::Lambda::Permission", map[string]any{"Action": "lambda:InvokeFunction", "FunctionName": a["FunctionArn"],
				"Principal": "apigateway.amazonaws.com", "SourceArn": samSub("arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/authorizers/*", map[string]any{"__ApiId__": samRef(id)})})
		default:
			x.fail("%s needs JwtConfiguration (OAuth2) or FunctionArn (Lambda)", aw)
			return
		}
		x.add(h.src, id+name+"Authorizer", "AWS::ApiGatewayV2::Authorizer", zp)
	}

	stage, sid := "$default", id+"ApiGatewayDefaultStage"
	if s, ok := h.props["StageName"].(string); ok && s != "" {
		stage, sid = s, id+samAlnum(s)+"Stage"
	}
	sp := map[string]any{"ApiId": samRef(id), "StageName": stage, "AutoDeploy": true, "Tags": tags}
	for _, k := range []string{"StageVariables", "AccessLogSettings", "DefaultRouteSettings", "RouteSettings"} {
		if h.props[k] != nil {
			sp[k] = h.props[k]
		}
	}
	x.add(h.src, sid, "AWS::ApiGatewayV2::Stage", sp)
	x.rename[id+".Stage"] = sid
}
