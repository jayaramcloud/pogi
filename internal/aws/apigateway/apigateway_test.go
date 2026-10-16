package apigateway_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	v2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	ltypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	apigw "github.com/nrndev/pogi/internal/aws/apigateway"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	hub  *core.Hub
	gw   *apigateway.Client
	v2   *apigatewayv2.Client
	fn   *lambda.Client
	url  string // http://127.0.0.1:<port>
	port string
}

func setup(t *testing.T) *env {
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return apigw.New(h) },
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	h.Cfg.CORSOrigins = []string{"https://app.example"} // the front door lets the API's own CORS origin in first
	cfg := awstest.Config(t, url)
	return &env{hub: h, gw: apigateway.NewFromConfig(cfg), v2: apigatewayv2.NewFromConfig(cfg), fn: lambda.NewFromConfig(cfg),
		url: url, port: url[strings.LastIndex(url, ":")+1:]}
}

func need(t *testing.T, bin string) {
	if _, err := exec.LookPath(bin); err != nil {
		t.Skipf("%s not installed", bin)
	}
}

const app = `
import json

def echo(event, context):
    return {"statusCode": 200, "headers": {"Content-Type": "application/json", "X-Echo": "yes"}, "body": json.dumps(event)}

def hello(event, context):
    return {"hello": event["pathParameters"]["name"], "stage": event["requestContext"]["stage"], "version": event["version"]}

def auth(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    ok = headers.get("x-token") == "letmein"
    if "methodArn" in event:
        return {"principalId": "me", "context": {"user": "bob"}, "policyDocument": {"Version": "2012-10-17",
                "Statement": [{"Action": "execute-api:Invoke", "Effect": "Allow" if ok else "Deny", "Resource": event["methodArn"]}]}}
    return {"isAuthorized": ok, "context": {"user": "bob"}}
`

// function deploys a handler of app and returns its ARN.
func (e *env) function(name string) string {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("app.py")
	io.WriteString(w, app)
	zw.Close()
	out := must(e.fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: ltypes.RuntimePython312,
		Handler: aws.String("app." + name), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &ltypes.FunctionCode{ZipFile: buf.Bytes()}}))
	return *out.FunctionArn
}

func lambdaURI(arn string) string {
	return "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/" + arn + "/invocations"
}

// call sends a plain unsigned request (to host, if set) and returns status, body and headers.
func call(t *testing.T, method, url, host string, header map[string]string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	if host != "" {
		req.Host = host
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b), res.Header
}

type event struct {
	Version               string
	Resource, Path        string
	HTTPMethod            string `json:"httpMethod"`
	RawPath               string
	PathParameters        map[string]string
	QueryStringParameters map[string]string
	StageVariables        map[string]string
	RequestContext        struct {
		Stage, APIID, ResourcePath, Path string
		Authorizer                       map[string]any
	}
}

func decodeEvent(t *testing.T, body string) event {
	t.Helper()
	var ev event
	if err := json.Unmarshal([]byte(body), &ev); err != nil {
		t.Fatalf("not an event: %s", body)
	}
	return ev
}

func (e *env) method(api, res *string, verb string, in types.IntegrationType, uri string) {
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api, ResourceId: res, HttpMethod: aws.String(verb), AuthorizationType: aws.String("NONE")}))
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api, ResourceId: res, HttpMethod: aws.String(verb),
		Type: in, IntegrationHttpMethod: aws.String("POST"), Uri: aws.String(uri)}))
}

func TestRestAPI(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	echo := e.function("echo")
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("shop")}))
	if len(*api.Id) != 10 || api.RootResourceId == nil || api.CreatedDate == nil {
		t.Fatalf("api = %+v", api)
	}
	items := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("items")}))
	item := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: items.Id, PathPart: aws.String("{id}")}))
	if *item.Path != "/items/{id}" {
		t.Fatalf("path = %s", *item.Path)
	}
	e.method(api.Id, item.Id, "GET", types.IntegrationTypeAwsProxy, lambdaURI(echo))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("dev"), Variables: map[string]string{"color": "blue"}}))

	// Path-style, host-style and legacy invoke URLs all work unsigned.
	for _, u := range []struct{ url, host string }{
		{e.url + "/_aws/execute-api/" + *api.Id + "/dev/items/42?q=1", ""},
		{e.url + "/dev/items/42?q=1", *api.Id + ".execute-api.localhost:" + e.port},
		{e.url + "/restapis/" + *api.Id + "/dev/_user_request_/items/42?q=1", ""},
	} {
		status, body, hdr := call(t, "GET", u.url, u.host, nil)
		ev := decodeEvent(t, body)
		if status != 200 || hdr.Get("X-Echo") != "yes" || ev.Resource != "/items/{id}" || ev.Path != "/items/42" || ev.HTTPMethod != "GET" ||
			ev.PathParameters["id"] != "42" || ev.QueryStringParameters["q"] != "1" || ev.StageVariables["color"] != "blue" ||
			ev.RequestContext.Stage != "dev" || ev.RequestContext.APIID != *api.Id || ev.RequestContext.Path != "/dev/items/42" {
			t.Fatalf("%s (host %q) = %d %s", u.url, u.host, status, body)
		}
	}
	base := e.url + "/_aws/execute-api/" + *api.Id + "/dev"
	if status, body, _ := call(t, "GET", base+"/nope", "", nil); status != 403 || !strings.Contains(body, "Missing Authentication Token") {
		t.Fatalf("missing route = %d %s", status, body)
	}
	if status, _, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*api.Id+"/prod/items/42", "", nil); status != 403 {
		t.Fatalf("unknown stage = %d", status)
	}

	// A greedy {proxy+} catches the rest; the more specific /items/{id} still wins.
	proxy := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("{proxy+}")}))
	e.method(api.Id, proxy.Id, "ANY", types.IntegrationTypeAwsProxy, lambdaURI(echo))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("dev")}))
	if _, body, _ := call(t, "POST", base+"/a/b/c", "", nil); decodeEvent(t, body).PathParameters["proxy"] != "a/b/c" || decodeEvent(t, body).Resource != "/{proxy+}" {
		t.Fatalf("greedy = %s", body)
	}
	if _, body, _ := call(t, "GET", base+"/items/7", "", nil); decodeEvent(t, body).Resource != "/items/{id}" {
		t.Fatalf("specific = %s", body)
	}

	// MOCK: the request template picks the integration response, whose template is the body.
	mock := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("mock")}))
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: mock.Id, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE")}))
	must(e.gw.PutMethodResponse(ctx, &apigateway.PutMethodResponseInput{RestApiId: api.Id, ResourceId: mock.Id, HttpMethod: aws.String("GET"), StatusCode: aws.String("200")}))
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: mock.Id, HttpMethod: aws.String("GET"), Type: types.IntegrationTypeMock,
		RequestTemplates: map[string]string{"application/json": `{"statusCode": 200}`}}))
	must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: mock.Id, HttpMethod: aws.String("GET"), StatusCode: aws.String("200"),
		ResponseParameters: map[string]string{"method.response.header.X-Mock": "'yes'"},
		ResponseTemplates:  map[string]string{"application/json": `{"mocked": true, "stage": "$context.stage", "color": "$stageVariables.color"}`}}))
	if status, body, hdr := call(t, "GET", base+"/mock", "", nil); status != 200 || hdr.Get("X-Mock") != "yes" || body != `{"mocked": true, "stage": "dev", "color": "blue"}` {
		t.Fatalf("mock = %d %s %v", status, body, hdr)
	}

	got := must(e.gw.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: api.Id}))
	var paths []string
	for _, r := range got.Items {
		paths = append(paths, *r.Path)
	}
	if strings.Join(paths, " ") != "/ /items /items/{id} /mock /{proxy+}" || got.Items[2].ResourceMethods["GET"].MethodIntegration == nil {
		t.Fatalf("resources = %v", paths)
	}
	if tables := e.hub.Resources("apigateway")["apigateway"]; len(tables[0].Rows) != 1 || !strings.Contains(tables[0].Rows[0][5], *api.Id+".execute-api.localhost:"+e.port+"/dev/") {
		t.Fatalf("console tables = %+v", tables)
	}

	// AWS (non-proxy) Lambda integrations run mapping templates both ways; a
	// function error picks the integration response by its selectionPattern.
	for part, fn := range map[string]string{"legacy": echo, "fail": e.function("hello")} {
		r := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String(part)}))
		must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: r.Id, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE")}))
		must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: r.Id, HttpMethod: aws.String("GET"), Type: types.IntegrationTypeAws,
			IntegrationHttpMethod: aws.String("POST"), Uri: aws.String(lambdaURI(fn)),
			RequestTemplates: map[string]string{"application/json": `{"q": "$input.params('q')", "body": $input.json('$')}`}}))
		must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: r.Id, HttpMethod: aws.String("GET"), StatusCode: aws.String("200"),
			ResponseTemplates: map[string]string{"application/json": "$input.path('$.body')"}}))
		must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: r.Id, HttpMethod: aws.String("GET"), StatusCode: aws.String("400"),
			SelectionPattern: aws.String(".*pathParameters.*"), ResponseTemplates: map[string]string{"application/json": `{"error": "$input.path('$.errorMessage')"}`}}))
	}
	if status, body, _ := call(t, "GET", base+"/legacy?q=hi", "", nil); status != 200 || body != `{"q": "hi", "body": {}}` {
		t.Fatalf("aws integration = %d %s", status, body)
	}
	if status, body, _ := call(t, "GET", base+"/fail", "", nil); status != 400 || body != `{"error": "'pathParameters'"}` {
		t.Fatalf("aws integration error = %d %s", status, body)
	}
}

func TestHTTPAPI(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	hello := e.function("hello")
	api := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("web"), ProtocolType: v2types.ProtocolTypeHttp, Target: aws.String(hello),
		CorsConfiguration: &v2types.Cors{AllowOrigins: []string{"https://app.example"}, AllowMethods: []string{"GET"}}}))
	if *api.ApiEndpoint != "http://"+*api.ApiId+".execute-api.localhost:"+e.port || api.CreatedDate == nil {
		t.Fatalf("api = %+v", api)
	}
	integs := must(e.v2.GetIntegrations(ctx, &apigatewayv2.GetIntegrationsInput{ApiId: api.ApiId}))
	stages := must(e.v2.GetStages(ctx, &apigatewayv2.GetStagesInput{ApiId: api.ApiId}))
	if len(integs.Items) != 1 || *integs.Items[0].PayloadFormatVersion != "2.0" || len(stages.Items) != 1 || *stages.Items[0].StageName != "$default" {
		t.Fatalf("quick create = %+v %+v", integs.Items, stages.Items)
	}
	must(e.v2.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: api.ApiId, RouteKey: aws.String("GET /hello/{name}"),
		Target: aws.String("integrations/" + *integs.Items[0].IntegrationId)}))

	// $default stage: no stage segment; a plain JSON result is the body (payload 2.0).
	status, body, hdr := call(t, "GET", e.url+"/_aws/execute-api/"+*api.ApiId+"/hello/bob", "", map[string]string{"Origin": "https://app.example"})
	if status != 200 || body != `{"hello": "bob", "stage": "$default", "version": "2.0"}` || hdr.Get("Access-Control-Allow-Origin") != "https://app.example" {
		t.Fatalf("hello = %d %s %v", status, body, hdr)
	}
	if _, body, _ := call(t, "GET", e.url+"/hello/ann", *api.ApiId+".execute-api.localhost:"+e.port, nil); !strings.Contains(body, `"ann"`) {
		t.Fatalf("host style = %s", body)
	}
	// The quick-create $default route catches the rest; the function fails without path parameters.
	if status, body, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*api.ApiId+"/other", "", nil); status != 502 || !strings.Contains(body, "Internal server error") {
		t.Fatalf("function error = %d %s", status, body)
	}
	status, _, hdr = call(t, "OPTIONS", e.url+"/_aws/execute-api/"+*api.ApiId+"/hello/bob", "", map[string]string{"Origin": "https://app.example", "Access-Control-Request-Method": "GET"})
	if status != 204 || hdr.Get("Access-Control-Allow-Methods") != "GET" {
		t.Fatalf("preflight = %d %v", status, hdr)
	}
}

func TestHTTPProxy(t *testing.T) {
	e := setup(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Backend", "yes")
		w.WriteHeader(201)
		fmt.Fprintf(w, "%s %s %s %s", r.Method, r.URL.RequestURI(), r.Header.Get("X-Custom"), b)
	}))
	defer backend.Close()
	api := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("proxy"), ProtocolType: v2types.ProtocolTypeHttp}))
	in := must(e.v2.CreateIntegration(ctx, &apigatewayv2.CreateIntegrationInput{ApiId: api.ApiId, IntegrationType: v2types.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String(backend.URL + "/base/{proxy}"), IntegrationMethod: aws.String("ANY"), PayloadFormatVersion: aws.String("1.0")}))
	must(e.v2.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: api.ApiId, RouteKey: aws.String("ANY /svc/{proxy+}"), Target: aws.String("integrations/" + *in.IntegrationId)}))
	must(e.v2.CreateStage(ctx, &apigatewayv2.CreateStageInput{ApiId: api.ApiId, StageName: aws.String("dev"), AutoDeploy: aws.Bool(true)}))

	req, _ := http.NewRequest("POST", e.url+"/_aws/execute-api/"+*api.ApiId+"/dev/svc/a/b?x=1", strings.NewReader("payload"))
	req.Header.Set("X-Custom", "hi")
	res := must(http.DefaultClient.Do(req))
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 201 || string(b) != "POST /base/a/b?x=1 hi payload" || res.Header.Get("X-Backend") != "yes" {
		t.Fatalf("proxied = %d %q %v", res.StatusCode, b, res.Header)
	}
	// Without a $default stage the stage segment is required.
	if status, body, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*api.ApiId+"/svc/a", "", nil); status != 404 || body != "{\"message\":\"Not Found\"}\n" {
		t.Fatalf("no stage = %d %q", status, body)
	}

	arn := aws.String("arn:aws:apigateway:us-east-1::/apis/" + *api.ApiId)
	must(e.v2.TagResource(ctx, &apigatewayv2.TagResourceInput{ResourceArn: arn, Tags: map[string]string{"team": "web", "tmp": "x"}}))
	must(e.v2.UntagResource(ctx, &apigatewayv2.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"tmp"}}))
	if tags := must(e.v2.GetTags(ctx, &apigatewayv2.GetTagsInput{ResourceArn: arn})); len(tags.Tags) != 1 || tags.Tags["team"] != "web" {
		t.Fatalf("v2 tags = %v", tags.Tags)
	}
}

// jwt builds an unsigned token; pogi checks the claims, not the signature.
func jwt(claims map[string]any) string {
	b, _ := json.Marshal(claims)
	return "eyJhbGciOiJub25lIn0." + base64.RawURLEncoding.EncodeToString(b) + ".sig"
}

func TestAuthorizers(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	echo, auth := e.function("echo"), e.function("auth")

	// v1 REQUEST authorizer.
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("secure")}))
	au := must(e.gw.CreateAuthorizer(ctx, &apigateway.CreateAuthorizerInput{RestApiId: api.Id, Name: aws.String("req"), Type: types.AuthorizerTypeRequest,
		AuthorizerUri: aws.String(lambdaURI(auth)), IdentitySource: aws.String("method.request.header.X-Token")}))
	res := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("secret")}))
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"),
		AuthorizationType: aws.String("CUSTOM"), AuthorizerId: au.Id}))
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"),
		Type: types.IntegrationTypeAwsProxy, IntegrationHttpMethod: aws.String("POST"), Uri: aws.String(lambdaURI(echo))}))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("dev")}))
	secret := e.url + "/_aws/execute-api/" + *api.Id + "/dev/secret"
	if status, _, _ := call(t, "GET", secret, "", map[string]string{"X-Token": "nope"}); status != 403 {
		t.Fatalf("denied = %d", status)
	}
	if status, _, _ := call(t, "GET", secret, "", nil); status != 401 {
		t.Fatalf("no identity = %d", status)
	}
	status, body, _ := call(t, "GET", secret, "", map[string]string{"X-Token": "letmein"})
	if a := decodeEvent(t, body).RequestContext.Authorizer; status != 200 || a["user"] != "bob" || a["principalId"] != "me" {
		t.Fatalf("allowed = %d %s", status, body)
	}

	// v2 REQUEST authorizer with simple responses, and a JWT authorizer.
	h := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("h"), ProtocolType: v2types.ProtocolTypeHttp, Target: aws.String(echo)}))
	lam := must(e.v2.CreateAuthorizer(ctx, &apigatewayv2.CreateAuthorizerInput{ApiId: h.ApiId, Name: aws.String("lam"), AuthorizerType: v2types.AuthorizerTypeRequest,
		AuthorizerUri: aws.String(lambdaURI(auth)), IdentitySource: []string{"$request.header.X-Token"}, AuthorizerPayloadFormatVersion: aws.String("2.0"),
		EnableSimpleResponses: aws.Bool(true)}))
	routes := must(e.v2.GetRoutes(ctx, &apigatewayv2.GetRoutesInput{ApiId: h.ApiId}))
	must(e.v2.UpdateRoute(ctx, &apigatewayv2.UpdateRouteInput{ApiId: h.ApiId, RouteId: routes.Items[0].RouteId,
		AuthorizationType: v2types.AuthorizationTypeCustom, AuthorizerId: lam.AuthorizerId}))
	anything := e.url + "/_aws/execute-api/" + *h.ApiId + "/anything"
	if status, _, _ := call(t, "GET", anything, "", map[string]string{"X-Token": "nope"}); status != 403 {
		t.Fatalf("v2 denied = %d", status)
	}
	status, body, _ = call(t, "GET", anything, "", map[string]string{"X-Token": "letmein"})
	if a := decodeEvent(t, body).RequestContext.Authorizer; status != 200 || a["lambda"].(map[string]any)["user"] != "bob" {
		t.Fatalf("v2 allowed = %d %s", status, body)
	}

	jwtAuth := must(e.v2.CreateAuthorizer(ctx, &apigatewayv2.CreateAuthorizerInput{ApiId: h.ApiId, Name: aws.String("jwt"), AuthorizerType: v2types.AuthorizerTypeJwt,
		IdentitySource: []string{"$request.header.Authorization"}, JwtConfiguration: &v2types.JWTConfiguration{Issuer: aws.String("https://issuer.example"), Audience: []string{"app"}}}))
	must(e.v2.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: h.ApiId, RouteKey: aws.String("GET /me"), Target: routes.Items[0].Target,
		AuthorizationType: v2types.AuthorizationTypeJwt, AuthorizerId: jwtAuth.AuthorizerId}))
	exp := time.Now().Add(time.Hour).Unix()
	good := jwt(map[string]any{"iss": "https://issuer.example", "aud": "app", "sub": "u1", "exp": exp})
	status, body, _ = call(t, "GET", e.url+"/_aws/execute-api/"+*h.ApiId+"/me", "", map[string]string{"Authorization": "Bearer " + good})
	if a := decodeEvent(t, body).RequestContext.Authorizer; status != 200 || a["jwt"].(map[string]any)["claims"].(map[string]any)["sub"] != "u1" {
		t.Fatalf("jwt = %d %s", status, body)
	}
	for _, bad := range []string{
		jwt(map[string]any{"iss": "https://issuer.example", "aud": "other", "exp": exp}),
		jwt(map[string]any{"iss": "https://issuer.example", "aud": "app", "exp": time.Now().Add(-time.Hour).Unix()}),
	} {
		if status, _, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*h.ApiId+"/me", "", map[string]string{"Authorization": "Bearer " + bad}); status != 401 {
			t.Fatalf("bad jwt = %d", status)
		}
	}
}

func TestImportPatchAndErrors(t *testing.T) {
	e := setup(t)
	doc := `{"openapi": "3.0.1", "info": {"title": "imported", "version": "1"}, "paths": {"/pets/{petId}": {"get": {
		"responses": {"200": {"description": "ok"}},
		"x-amazon-apigateway-integration": {"type": "mock", "requestTemplates": {"application/json": "{\"statusCode\": 200}"},
			"responses": {"default": {"statusCode": "200", "responseTemplates": {"application/json": "{\"pet\": \"$input.params('petId')\"}"}}}}}}}}`
	api := must(e.gw.ImportRestApi(ctx, &apigateway.ImportRestApiInput{Body: []byte(doc)}))
	if *api.Name != "imported" {
		t.Fatalf("imported = %+v", api)
	}
	m := must(e.gw.GetMethod(ctx, &apigateway.GetMethodInput{RestApiId: api.Id, ResourceId: resourceID(t, e, api.Id, "/pets/{petId}"), HttpMethod: aws.String("GET")}))
	if m.MethodIntegration.Type != types.IntegrationTypeMock || m.MethodResponses["200"].StatusCode == nil {
		t.Fatalf("method = %+v", m)
	}
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("v1")}))
	pet := e.url + "/_aws/execute-api/" + *api.Id + "/v1/pets/7"
	if status, body, _ := call(t, "GET", pet, "", nil); status != 200 || body != `{"pet": "7"}` {
		t.Fatalf("pet = %d %s", status, body)
	}

	// patchOperations
	upd := must(e.gw.UpdateRestApi(ctx, &apigateway.UpdateRestApiInput{RestApiId: api.Id, PatchOperations: []types.PatchOperation{
		{Op: types.OpReplace, Path: aws.String("/name"), Value: aws.String("renamed")},
		{Op: types.OpAdd, Path: aws.String("/binaryMediaTypes/image~1png")},
		{Op: types.OpReplace, Path: aws.String("/endpointConfiguration/types/EDGE"), Value: aws.String("REGIONAL")}}}))
	if *upd.Name != "renamed" || strings.Join(upd.BinaryMediaTypes, ",") != "image/png" || upd.EndpointConfiguration.Types[0] != types.EndpointTypeRegional {
		t.Fatalf("updated api = %+v", upd)
	}
	st := must(e.gw.UpdateStage(ctx, &apigateway.UpdateStageInput{RestApiId: api.Id, StageName: aws.String("v1"), PatchOperations: []types.PatchOperation{
		{Op: types.OpReplace, Path: aws.String("/variables/color"), Value: aws.String("red")},
		{Op: types.OpReplace, Path: aws.String("/tracingEnabled"), Value: aws.String("true")}}}))
	if st.Variables["color"] != "red" || !st.TracingEnabled {
		t.Fatalf("updated stage = %+v", st)
	}

	// apiKeyRequired: only a key in a usage plan of the stage gets through.
	must(e.gw.UpdateMethod(ctx, &apigateway.UpdateMethodInput{RestApiId: api.Id, ResourceId: resourceID(t, e, api.Id, "/pets/{petId}"), HttpMethod: aws.String("GET"),
		PatchOperations: []types.PatchOperation{{Op: types.OpReplace, Path: aws.String("/apiKeyRequired"), Value: aws.String("true")}}}))
	if status, _, _ := call(t, "GET", pet, "", nil); status != 403 {
		t.Fatalf("no key = %d", status)
	}
	key := must(e.gw.CreateApiKey(ctx, &apigateway.CreateApiKeyInput{Name: aws.String("k"), Enabled: true}))
	plan := must(e.gw.CreateUsagePlan(ctx, &apigateway.CreateUsagePlanInput{Name: aws.String("p"), ApiStages: []types.ApiStage{{ApiId: api.Id, Stage: aws.String("v1")}}}))
	must(e.gw.CreateUsagePlanKey(ctx, &apigateway.CreateUsagePlanKeyInput{UsagePlanId: plan.Id, KeyId: key.Id, KeyType: aws.String("API_KEY")}))
	if status, _, _ := call(t, "GET", pet, "", map[string]string{"x-api-key": *key.Value}); status != 200 {
		t.Fatalf("with key = %d", status)
	}

	arn := "arn:aws:apigateway:us-east-1::/restapis/" + *api.Id
	must(e.gw.TagResource(ctx, &apigateway.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"env": "test"}}))
	if tags := must(e.gw.GetTags(ctx, &apigateway.GetTagsInput{ResourceArn: aws.String(arn)})); tags.Tags["env"] != "test" {
		t.Fatalf("tags = %v", tags.Tags)
	}
	if c := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("c"), Tags: map[string]string{"_custom_id_": "myid"}})); *c.Id != "myid" {
		t.Fatalf("custom id = %s", *c.Id)
	}

	// Error codes SDKs branch on.
	var nf *types.NotFoundException
	var cf *types.ConflictException
	var br *types.BadRequestException
	var nf2 *v2types.NotFoundException
	if _, err := e.gw.GetRestApi(ctx, &apigateway.GetRestApiInput{RestApiId: aws.String("missing")}); !errors.As(err, &nf) {
		t.Fatalf("GetRestApi missing: %v", err)
	}
	if _, err := e.gw.GetMethod(ctx, &apigateway.GetMethodInput{RestApiId: api.Id, ResourceId: api.RootResourceId, HttpMethod: aws.String("POST")}); !errors.As(err, &nf) {
		t.Fatalf("GetMethod missing: %v", err)
	}
	if _, err := e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("pets")}); !errors.As(err, &cf) {
		t.Fatalf("duplicate resource: %v", err)
	}
	empty := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("empty")}))
	if _, err := e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: empty.Id, StageName: aws.String("dev")}); !errors.As(err, &br) {
		t.Fatalf("deploy without methods: %v", err)
	}
	if _, err := e.v2.GetApi(ctx, &apigatewayv2.GetApiInput{ApiId: aws.String("missing")}); !errors.As(err, &nf2) {
		t.Fatalf("GetApi missing: %v", err)
	}
	if _, err := e.v2.GetRoute(ctx, &apigatewayv2.GetRouteInput{ApiId: aws.String("missing"), RouteId: aws.String("r")}); !errors.As(err, &nf2) {
		t.Fatalf("GetRoute missing: %v", err)
	}

	// Snapshots round-trip the state.
	snap := must(e.hub.Snapshot())
	e.hub.Reset("apigateway")
	if _, err := e.gw.GetRestApi(ctx, &apigateway.GetRestApiInput{RestApiId: api.Id}); !errors.As(err, &nf) {
		t.Fatalf("after reset: %v", err)
	}
	if err := e.hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if status, body, _ := call(t, "GET", pet, "", map[string]string{"x-api-key": *key.Value}); status != 200 || body != `{"pet": "7"}` {
		t.Fatalf("after restore = %d %s", status, body)
	}
	must(e.gw.DeleteRestApi(ctx, &apigateway.DeleteRestApiInput{RestApiId: api.Id}))
	if _, err := e.gw.GetRestApi(ctx, &apigateway.GetRestApiInput{RestApiId: api.Id}); !errors.As(err, &nf) {
		t.Fatalf("after delete: %v", err)
	}
}

func resourceID(t *testing.T, e *env, api *string, path string) *string {
	for _, r := range must(e.gw.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: api})).Items {
		if *r.Path == path {
			return r.Id
		}
	}
	t.Fatalf("no resource %s", path)
	return nil
}

// A mock GET /ping method on a new REST API; policy is its resource policy ("" for none).
func mockPing(t *testing.T, e *env, policy string) *apigateway.CreateRestApiOutput {
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("ping"), Policy: aws.String(policy)}))
	res := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("ping")}))
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE")}))
	must(e.gw.PutMethodResponse(ctx, &apigateway.PutMethodResponseInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), StatusCode: aws.String("200")}))
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), Type: types.IntegrationTypeMock,
		RequestTemplates: map[string]string{"application/json": `{"statusCode": 200}`}}))
	must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), StatusCode: aws.String("200")}))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("test")}))
	return api
}

func TestRestImportAndPolicyFixes(t *testing.T) {
	e := setup(t)

	// floci #2804: a new API is AVAILABLE, so Terraform stops waiting.
	api := mockPing(t, e, "")
	if got := must(e.gw.GetRestApi(ctx, &apigateway.GetRestApiInput{RestApiId: api.Id})); api.ApiStatus != types.ApiStatusAvailable || got.ApiStatus != types.ApiStatusAvailable {
		t.Fatalf("apiStatus = %q, %q", api.ApiStatus, got.ApiStatus)
	}
	// floci #3507: a resource policy that denies everyone is enforced on invoke.
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"execute-api:Invoke","Resource":"execute-api:/*"}]}`
	if status, _, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*api.Id+"/test/ping", "", nil); status != 200 {
		t.Fatalf("no policy = %d", status)
	}
	denied := mockPing(t, e, deny)
	if status, _, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*denied.Id+"/test/ping", "", nil); status != 403 {
		t.Fatalf("Deny policy = %d, want 403", status)
	}
	// An Allow-only policy, or a Deny for another IP, lets the call through.
	other := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"execute-api:Invoke","Resource":"execute-api:/*","Condition":{"IpAddress":{"aws:SourceIp":"203.0.113.0/24"}}}]}`
	ok := mockPing(t, e, other)
	if status, _, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*ok.Id+"/test/ping", "", nil); status != 200 {
		t.Fatalf("Deny for another IP = %d, want 200", status)
	}

	// floci #5388, #4546: OpenAPI import sets apiKeyRequired from the security list
	// and fills in ${AWS::...} variables.
	spec := `{"openapi":"3.0.1","info":{"title":"k","version":"1"},"security":[{"api_key":[]}],"paths":{
	"/keyed":{"post":{"responses":{"200":{"description":"ok"}},"x-amazon-apigateway-integration":{"type":"mock"}}},
	"/open":{"get":{"security":[],"responses":{"200":{"description":"ok"}},"x-amazon-apigateway-integration":{"type":"mock"}}},
	"/vars":{"get":{"security":[{"api_key":[]}],"responses":{"200":{"description":"ok"}},"x-amazon-apigateway-integration":{"type":"aws","httpMethod":"POST",
	  "uri":"arn:${AWS::Partition}:apigateway:${AWS::Region}:states:action/StartSyncExecution","credentials":"arn:${AWS::Partition}:iam::${AWS::AccountId}:role/r"}}}},
	"components":{"securitySchemes":{"api_key":{"type":"apiKey","name":"x-api-key","in":"header"}}}}`
	imp := must(e.gw.ImportRestApi(ctx, &apigateway.ImportRestApiInput{Body: []byte(spec)}))
	for path, verb := range map[string]string{"/keyed": "POST", "/open": "GET", "/vars": "GET"} {
		m := must(e.gw.GetMethod(ctx, &apigateway.GetMethodInput{RestApiId: imp.Id, ResourceId: resourceID(t, e, imp.Id, path), HttpMethod: aws.String(verb)}))
		if want := path != "/open"; aws.ToBool(m.ApiKeyRequired) != want {
			t.Errorf("%s apiKeyRequired = %v, want %v", path, m.ApiKeyRequired, want)
		}
		if path == "/vars" {
			in := m.MethodIntegration
			if *in.Uri != "arn:aws:apigateway:us-east-1:states:action/StartSyncExecution" || *in.Credentials != "arn:aws:iam::000000000000:role/r" {
				t.Errorf("variables not filled in: %s %s", *in.Uri, *in.Credentials)
			}
		}
	}

	// floci #4492: a malformed security list is a warning, and an error with failOnWarnings.
	bad := `{"openapi":"3.0.1","info":{"title":"w","version":"1"},"paths":{"/p":{"get":{"security":"nope","responses":{"200":{"description":"ok"}}}}}}`
	var br *types.BadRequestException
	if _, err := e.gw.ImportRestApi(ctx, &apigateway.ImportRestApiInput{Body: []byte(bad), FailOnWarnings: true}); !errors.As(err, &br) {
		t.Fatalf("failOnWarnings: %v", err)
	}
	if w := must(e.gw.ImportRestApi(ctx, &apigateway.ImportRestApiInput{Body: []byte(bad)})); len(w.Warnings) != 1 {
		t.Fatalf("warnings = %v", w.Warnings)
	}
}

func TestSigV4HeaderAndIAMRoutes(t *testing.T) {
	if os.Getenv("POGI_TEST_VALIDATE_SIGNATURES") == "1" {
		t.Skip("uses a made-up signature, which pogi rightly refuses when it checks signatures")
	}
	e := setup(t)
	got := make(chan string, 2)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r.Header.Get("Authorization") }))
	defer backend.Close()
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("sig")}))
	res := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("x")}))
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE")}))
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), Type: types.IntegrationTypeHttpProxy,
		IntegrationHttpMethod: aws.String("GET"), Uri: aws.String(backend.URL)}))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("dev")}))
	url := e.url + "/_aws/execute-api/" + *api.Id + "/dev/x"
	// floci #4566: a SigV4 signature is not passed on; a Bearer token is.
	sig := "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/execute-api/aws4_request, SignedHeaders=host, Signature=abc"
	for header, want := range map[string]string{sig: "", "Bearer t": "Bearer t"} {
		call(t, "GET", url, "", map[string]string{"Authorization": header})
		if a := <-got; a != want {
			t.Errorf("backend got Authorization %q, want %q", a, want)
		}
	}

	// floci #2810: an AWS_IAM route needs a signed request (only the header is checked).
	h := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("i"), ProtocolType: v2types.ProtocolTypeHttp}))
	in := must(e.v2.CreateIntegration(ctx, &apigatewayv2.CreateIntegrationInput{ApiId: h.ApiId, IntegrationType: v2types.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String(backend.URL), IntegrationMethod: aws.String("GET"), PayloadFormatVersion: aws.String("1.0")}))
	must(e.v2.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: h.ApiId, RouteKey: aws.String("GET /secure"), Target: aws.String("integrations/" + *in.IntegrationId),
		AuthorizationType: v2types.AuthorizationTypeAwsIam}))
	must(e.v2.CreateStage(ctx, &apigatewayv2.CreateStageInput{ApiId: h.ApiId, StageName: aws.String("$default"), AutoDeploy: aws.Bool(true)}))
	secure := e.url + "/_aws/execute-api/" + *h.ApiId + "/secure"
	if status, _, _ := call(t, "GET", secure, "", nil); status != 403 {
		t.Errorf("unsigned AWS_IAM route = %d, want 403", status)
	}
	if status, _, _ := call(t, "GET", secure, "", map[string]string{"Authorization": sig}); status != 200 {
		t.Errorf("signed AWS_IAM route = %d, want 200", status)
	}
	<-got
}

// ---- parity features ----

// mockAPI makes a REST API with a GET method on path ("" is the root) that is
// a MOCK integration answering 200 with the response template, and deploys it to stage.
func (e *env) mockAPI(t *testing.T, name, path, authType, stage, tmpl string) *string {
	t.Helper()
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String(name)}))
	res := api.RootResourceId
	if path != "" {
		res = must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String(path)})).Id
	}
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("GET"), AuthorizationType: aws.String(authType)}))
	must(e.gw.PutMethodResponse(ctx, &apigateway.PutMethodResponseInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("GET"), StatusCode: aws.String("200")}))
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("GET"), Type: types.IntegrationTypeMock,
		RequestTemplates: map[string]string{"application/json": `{"statusCode": 200}`}}))
	must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("GET"), StatusCode: aws.String("200"),
		ResponseTemplates: map[string]string{"application/json": tmpl}}))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String(stage)}))
	return api.Id
}

// floci #4090, #4827: custom domain names and base path mappings, for REST
// and HTTP APIs; the domain is a Host header or a *.localhost name, and a
// signed request for an AWS_IAM method works through it.
func TestCustomDomains(t *testing.T) {
	e := setup(t)
	pets := e.mockAPI(t, "pets", "pets", "AWS_IAM", "prod", `{"ok":true,"host":"$context.domainName"}`)
	root := e.mockAPI(t, "root", "", "NONE", "dev", `{"root":true}`)

	d := must(e.gw.CreateDomainName(ctx, &apigateway.CreateDomainNameInput{DomainName: aws.String("API.example.com"),
		RegionalCertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/x"),
		EndpointConfiguration:  &types.EndpointConfiguration{Types: []types.EndpointType{types.EndpointTypeRegional}},
		Tags:                   map[string]string{"env": "test"}}))
	if *d.DomainName != "api.example.com" || d.RegionalDomainName == nil || d.DomainNameStatus != types.DomainNameStatusAvailable || d.RegionalHostedZoneId == nil {
		t.Fatalf("domain = %+v", d)
	}
	if _, err := e.gw.CreateDomainName(ctx, &apigateway.CreateDomainNameInput{DomainName: aws.String("api.example.com")}); err == nil {
		t.Fatal("a second CreateDomainName of the same name should conflict")
	}
	must(e.gw.CreateBasePathMapping(ctx, &apigateway.CreateBasePathMappingInput{DomainName: d.DomainName, RestApiId: pets, Stage: aws.String("prod"), BasePath: aws.String("v1")}))
	must(e.gw.CreateBasePathMapping(ctx, &apigateway.CreateBasePathMappingInput{DomainName: d.DomainName, RestApiId: root, Stage: aws.String("dev")}))
	if _, err := e.gw.CreateBasePathMapping(ctx, &apigateway.CreateBasePathMappingInput{DomainName: d.DomainName, RestApiId: root, Stage: aws.String("dev"), BasePath: aws.String("v1")}); err == nil {
		t.Fatal("a second mapping of the same base path should conflict")
	}
	maps := must(e.gw.GetBasePathMappings(ctx, &apigateway.GetBasePathMappingsInput{DomainName: d.DomainName}))
	none := must(e.gw.GetBasePathMapping(ctx, &apigateway.GetBasePathMappingInput{DomainName: d.DomainName, BasePath: aws.String("(none)")}))
	if len(maps.Items) != 2 || *none.RestApiId != *root || *none.BasePath != "(none)" {
		t.Fatalf("mappings = %+v, root mapping = %+v", maps.Items, none)
	}

	host := "api.example.com:" + e.port
	if status, body, _ := call(t, "GET", e.url+"/", host, nil); status != 200 || body != `{"root":true}` {
		t.Fatalf("root mapping = %d %s", status, body)
	}
	// An AWS_IAM method needs a signature, which a signed request through the domain has.
	if status, _, _ := call(t, "GET", e.url+"/v1/pets", host, nil); status != 403 {
		t.Fatalf("unsigned AWS_IAM request = %d", status)
	}
	signed := func(path string) (int, string) {
		req, _ := http.NewRequest("GET", e.url+path, nil)
		req.Host = host
		awstest.Sign(t, req, "execute-api")
		res := must(http.DefaultClient.Do(req))
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	if status, body := signed("/v1/pets"); status != 200 || body != `{"ok":true,"host":"api.example.com:`+e.port+`"}` {
		t.Fatalf("signed request through the domain = %d %s", status, body)
	}
	if status, _, _ := call(t, "GET", e.url+"/v2/pets", host, nil); status != 403 { // no such mapping: the root API has no /v2/pets
		t.Fatalf("unmapped path = %d", status)
	}

	// Update, tags, delete: a mapping moves to another API, and a deleted domain stops answering.
	must(e.gw.UpdateBasePathMapping(ctx, &apigateway.UpdateBasePathMappingInput{DomainName: d.DomainName, BasePath: aws.String("v1"), PatchOperations: []types.PatchOperation{
		{Op: types.OpReplace, Path: aws.String("/basePath"), Value: aws.String("v9")}}}))
	if status, _ := signed("/v9/pets"); status != 200 {
		t.Fatalf("moved mapping = %d", status)
	}
	must(e.gw.UpdateDomainName(ctx, &apigateway.UpdateDomainNameInput{DomainName: d.DomainName, PatchOperations: []types.PatchOperation{
		{Op: types.OpReplace, Path: aws.String("/securityPolicy"), Value: aws.String("TLS_1_0")}}}))
	arn := "arn:aws:apigateway:us-east-1::/domainnames/api.example.com"
	must(e.gw.TagResource(ctx, &apigateway.TagResourceInput{ResourceArn: &arn, Tags: map[string]string{"more": "1"}}))
	got := must(e.gw.GetDomainName(ctx, &apigateway.GetDomainNameInput{DomainName: aws.String("api.example.com")}))
	tags := must(e.gw.GetTags(ctx, &apigateway.GetTagsInput{ResourceArn: &arn}))
	if got.SecurityPolicy != types.SecurityPolicyTls10 || tags.Tags["env"] != "test" || tags.Tags["more"] != "1" {
		t.Fatalf("after update: %+v tags %v", got, tags.Tags)
	}
	must(e.gw.DeleteBasePathMapping(ctx, &apigateway.DeleteBasePathMappingInput{DomainName: d.DomainName, BasePath: aws.String("v9")}))
	must(e.gw.DeleteDomainName(ctx, &apigateway.DeleteDomainNameInput{DomainName: d.DomainName}))
	if _, err := e.gw.GetDomainName(ctx, &apigateway.GetDomainNameInput{DomainName: d.DomainName}); err == nil {
		t.Fatal("deleted domain still there")
	}
	if status, body, _ := call(t, "GET", e.url+"/", host, nil); status == 200 || strings.Contains(body, "root") {
		t.Fatalf("a deleted domain answered %d %s", status, body)
	}

	// A *.localhost name works the same way (the S3 front door must not take it).
	must(e.gw.CreateDomainName(ctx, &apigateway.CreateDomainNameInput{DomainName: aws.String("myapi.localhost")}))
	must(e.gw.CreateBasePathMapping(ctx, &apigateway.CreateBasePathMappingInput{DomainName: aws.String("myapi.localhost"), RestApiId: root, Stage: aws.String("dev")}))
	if status, body, _ := call(t, "GET", e.url+"/", "myapi.localhost:"+e.port, nil); status != 200 || body != `{"root":true}` {
		t.Fatalf("*.localhost domain = %d %s", status, body)
	}
}

func TestCustomDomainsHTTP(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	api := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("web"), ProtocolType: v2types.ProtocolTypeHttp, Target: aws.String(e.function("echo"))}))
	d := must(e.v2.CreateDomainName(ctx, &apigatewayv2.CreateDomainNameInput{DomainName: aws.String("http.example.com"),
		DomainNameConfigurations: []v2types.DomainNameConfiguration{{CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/x"), EndpointType: v2types.EndpointTypeRegional}}}))
	if *d.ApiMappingSelectionExpression != "$request.basepath" || len(d.DomainNameConfigurations) != 1 || d.DomainNameConfigurations[0].ApiGatewayDomainName == nil {
		t.Fatalf("domain = %+v", d)
	}
	m := must(e.v2.CreateApiMapping(ctx, &apigatewayv2.CreateApiMappingInput{DomainName: d.DomainName, ApiId: api.ApiId, Stage: aws.String("$default"), ApiMappingKey: aws.String("web")}))
	if list := must(e.v2.GetApiMappings(ctx, &apigatewayv2.GetApiMappingsInput{DomainName: d.DomainName})); len(list.Items) != 1 || *list.Items[0].ApiMappingId != *m.ApiMappingId {
		t.Fatalf("mappings = %+v", list.Items)
	}
	status, body, _ := call(t, "GET", e.url+"/web/items/1", "http.example.com:"+e.port, nil)
	if status != 200 || !strings.Contains(body, `"rawPath": "/items/1"`) && !strings.Contains(body, `"rawPath":"/items/1"`) {
		t.Fatalf("through the v2 domain = %d %s", status, body)
	}
	must(e.v2.DeleteApiMapping(ctx, &apigatewayv2.DeleteApiMappingInput{DomainName: d.DomainName, ApiMappingId: m.ApiMappingId}))
	must(e.v2.DeleteDomainName(ctx, &apigatewayv2.DeleteDomainNameInput{DomainName: d.DomainName}))
	if _, err := e.v2.GetDomainName(ctx, &apigatewayv2.GetDomainNameInput{DomainName: d.DomainName}); err == nil {
		t.Fatal("deleted domain still there")
	}
}

// floci #3439: $input.params().header.get(), #foreach over keySet() and $util functions.
func TestVTLFunctions(t *testing.T) {
	e := setup(t)
	id := e.mockAPI(t, "vtl", "v", "NONE", "dev", `#set($h = $input.params().header)
{"v": "$h.get('x-repro-header')", "keys": [#foreach($k in $h.keySet())"$k"#if($foreach.hasNext),#end#end], "q": "$util.urlEncode($input.params('q'))"}`)
	status, body, _ := call(t, "GET", e.url+"/_aws/execute-api/"+*id+"/dev/v?q=a%20b", "", map[string]string{"X-Repro-Header": "hello"})
	var got struct {
		V    string
		Keys []string
		Q    string
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil || status != 200 || got.V != "hello" || got.Q != "a+b" || !strings.Contains(strings.ToLower(strings.Join(got.Keys, ",")), "x-repro-header") {
		t.Fatalf("%d %s (%v)", status, body, err)
	}
}

// floci #3519: stage method settings patched with ~1 and ~0 escapes in the path.
func TestStageMethodSettings(t *testing.T) {
	e := setup(t)
	id := e.mockAPI(t, "ms", "pets", "NONE", "dev", `{}`)
	must(e.gw.UpdateStage(ctx, &apigateway.UpdateStageInput{RestApiId: id, StageName: aws.String("dev"), PatchOperations: []types.PatchOperation{
		{Op: types.OpReplace, Path: aws.String("/~1pets/GET/metrics/enabled"), Value: aws.String("true")},
		{Op: types.OpReplace, Path: aws.String("/~1pets~0promo/GET/metrics/enabled"), Value: aws.String("true")},
		{Op: types.OpReplace, Path: aws.String("/*/*/throttling/burstLimit"), Value: aws.String("20")},
		{Op: types.OpReplace, Path: aws.String("/*/*/logging/loglevel"), Value: aws.String("INFO")}}}))
	st := must(e.gw.GetStage(ctx, &apigateway.GetStageInput{RestApiId: id, StageName: aws.String("dev")}))
	if !st.MethodSettings["pets/GET"].MetricsEnabled || !st.MethodSettings["pets~promo/GET"].MetricsEnabled ||
		st.MethodSettings["*/*"].ThrottlingBurstLimit != 20 ||
		*st.MethodSettings["*/*"].LoggingLevel != "INFO" || *st.MethodSettings["pets/GET"].LoggingLevel != "OFF" {
		t.Fatalf("methodSettings = %+v", st.MethodSettings)
	}
	// Terraform removes a method's settings with the whole key as one escaped segment.
	must(e.gw.UpdateStage(ctx, &apigateway.UpdateStageInput{RestApiId: id, StageName: aws.String("dev"), PatchOperations: []types.PatchOperation{
		{Op: types.OpRemove, Path: aws.String("/pets~1GET")}}}))
	st = must(e.gw.GetStage(ctx, &apigateway.GetStageInput{RestApiId: id, StageName: aws.String("dev")}))
	if _, there := st.MethodSettings["pets/GET"]; there || len(st.MethodSettings) != 2 {
		t.Fatalf("after remove: %+v", st.MethodSettings)
	}
}

// restToSQS makes POST /<path> an AWS integration with the URI, the integration
// request parameters and a request template, answering a 200 response and a
// 400 response picked by the pattern.
func (e *env) restToSQS(t *testing.T, path, uri string, params map[string]string, tmpl string) (base string) {
	t.Helper()
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String(path)}))
	res := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String(path)})).Id
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("POST"), AuthorizationType: aws.String("NONE")}))
	for _, c := range []string{"200", "400"} {
		must(e.gw.PutMethodResponse(ctx, &apigateway.PutMethodResponseInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("POST"), StatusCode: aws.String(c)}))
	}
	must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("POST"), Type: types.IntegrationTypeAws,
		IntegrationHttpMethod: aws.String("POST"), Uri: aws.String(uri), Credentials: aws.String("arn:aws:iam::000000000000:role/r"),
		RequestParameters: params, RequestTemplates: map[string]string{"application/json": tmpl}}))
	must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("POST"), StatusCode: aws.String("200")}))
	must(e.gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: res, HttpMethod: aws.String("POST"), StatusCode: aws.String("400"), SelectionPattern: aws.String(`4\d{2}`)}))
	must(e.gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("dev")}))
	return e.url + "/_aws/execute-api/" + *api.Id + "/dev/" + path
}

// floci #1384, #5418: an AWS integration to SQS, with a path-style URI and a
// form-encoded template, and with the action form; selectionPattern matches the
// status code of the backend.
func TestAWSIntegrationSQS(t *testing.T) {
	e := setup(t)
	sq := sqs.NewFromConfig(awstest.Config(t, e.url))
	q := must(sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("gwq")}))
	form := map[string]string{"integration.request.header.Content-Type": "'application/x-www-form-urlencoded'"}
	post := func(url, body string) (int, string) {
		res := must(http.Post(url, "application/json", strings.NewReader(body)))
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	received := func() []string {
		var got []string
		for range 2 {
			out := must(sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10}))
			for _, m := range out.Messages {
				got = append(got, *m.Body)
				must(sq.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: m.ReceiptHandle}))
			}
		}
		return got
	}

	path := e.restToSQS(t, "send", "arn:aws:apigateway:us-east-1:sqs:path/000000000000/gwq", form, "Action=SendMessage&MessageBody=$util.urlEncode($input.body)")
	if status, body := post(path, "hello world"); status != 200 || !strings.Contains(body, "SendMessageResponse") {
		t.Fatalf("path-style send = %d %s", status, body)
	}
	if got := received(); len(got) != 1 || got[0] != "hello world" {
		t.Fatalf("messages = %q", got)
	}

	action := e.restToSQS(t, "act", "arn:aws:apigateway:us-east-1:sqs:action/SendMessage", map[string]string{
		"integration.request.querystring.QueueUrl": "'" + *q.QueueUrl + "'", "integration.request.querystring.MessageBody": "method.request.querystring.m"}, "")
	if status, body := post(action+"?m=via-action", ""); status != 200 {
		t.Fatalf("action-style send = %d %s", status, body)
	}
	if got := received(); len(got) != 1 || got[0] != "via-action" {
		t.Fatalf("messages = %q", got)
	}

	// The backend answers 400 for a queue that is not there; the 4xx pattern picks that response.
	missing := e.restToSQS(t, "gone", "arn:aws:apigateway:us-east-1:sqs:path/000000000000/does-not-exist", form, "Action=SendMessage&MessageBody=hi")
	if status, body := post(missing, "{}"); status != 400 {
		t.Fatalf("missing queue = %d %s, want the 400 response", status, body)
	}
}

// OpenAPI import makes authorizers from x-amazon-apigateway-authorizer schemes
// (as SAM's Auth does) and a method's security list picks one.
func TestImportAuthorizers(t *testing.T) {
	e := setup(t)
	spec := `{"swagger":"2.0","info":{"title":"a","version":"1"},"paths":{
	"/in":{"get":{"security":[{"Cog":["email"]}],"x-amazon-apigateway-integration":{"type":"mock"}}},
	"/lam":{"get":{"security":[{"Lam":[]}],"x-amazon-apigateway-integration":{"type":"mock"}}},
	"/iam":{"get":{"security":[{"AWS_IAM":[]}],"x-amazon-apigateway-integration":{"type":"mock"}}},
	"/open":{"get":{"x-amazon-apigateway-integration":{"type":"mock"}}}},
	"securityDefinitions":{
	  "Cog":{"type":"apiKey","name":"Authorization","in":"header","x-amazon-apigateway-authtype":"cognito_user_pools","x-amazon-apigateway-authorizer":{"type":"cognito_user_pools","providerARNs":["arn:aws:cognito-idp:us-east-1:000000000000:userpool/p"]}},
	  "Lam":{"type":"apiKey","name":"Authorization","in":"header","x-amazon-apigateway-authtype":"custom","x-amazon-apigateway-authorizer":{"type":"token","authorizerUri":"arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:f/invocations","authorizerResultTtlInSeconds":0}},
	  "AWS_IAM":{"type":"apiKey","name":"Authorization","in":"header","x-amazon-apigateway-authtype":"awsSigv4"}}}`
	api := must(e.gw.ImportRestApi(ctx, &apigateway.ImportRestApiInput{Body: []byte(spec)}))
	azs := must(e.gw.GetAuthorizers(ctx, &apigateway.GetAuthorizersInput{RestApiId: api.Id}))
	ids := map[string]string{}
	for _, a := range azs.Items {
		ids[*a.Name] = *a.Id
	}
	if len(azs.Items) != 2 || ids["Cog"] == "" || ids["Lam"] == "" {
		t.Fatalf("authorizers = %+v", azs.Items)
	}
	for path, want := range map[string][2]string{"/in": {"COGNITO_USER_POOLS", ids["Cog"]}, "/lam": {"CUSTOM", ids["Lam"]}, "/iam": {"AWS_IAM", ""}, "/open": {"NONE", ""}} {
		m := must(e.gw.GetMethod(ctx, &apigateway.GetMethodInput{RestApiId: api.Id, ResourceId: resourceID(t, e, api.Id, path), HttpMethod: aws.String("GET")}))
		if aws.ToString(m.AuthorizationType) != want[0] || aws.ToString(m.AuthorizerId) != want[1] {
			t.Errorf("%s: %s %s, want %v", path, aws.ToString(m.AuthorizationType), aws.ToString(m.AuthorizerId), want)
		}
	}
	for _, a := range azs.Items {
		if *a.Name == "Lam" && (aws.ToString(a.IdentitySource) != "method.request.header.Authorization" || strings.Contains(*a.AuthorizerUri, "${")) {
			t.Errorf("lambda authorizer = %+v", a)
		}
	}
}

// floci #4758: PutIntegration answers cacheKeyParameters as [] (not missing).
// floci #4072: an authorizer TTL is between 0 and 3600 seconds.
func TestIntegrationDefaultsAndAuthorizerTTL(t *testing.T) {
	e := setup(t)
	api := must(e.gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("ttl")}))
	res := must(e.gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("p")}))
	must(e.gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE")}))
	in := must(e.gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"), Type: types.IntegrationTypeMock}))
	if in.CacheKeyParameters == nil || len(in.CacheKeyParameters) != 0 {
		t.Fatalf("cacheKeyParameters = %#v", in.CacheKeyParameters)
	}

	for _, ttl := range []int32{-1, 3601} {
		if _, err := e.gw.CreateAuthorizer(ctx, &apigateway.CreateAuthorizerInput{RestApiId: api.Id, Name: aws.String("a"), Type: types.AuthorizerTypeToken,
			AuthorizerUri: aws.String(lambdaURI("arn:aws:lambda:us-east-1:000000000000:function:f")), IdentitySource: aws.String("method.request.header.x"),
			AuthorizerResultTtlInSeconds: aws.Int32(ttl)}); err == nil {
			t.Fatalf("REST authorizer accepted TTL %d", ttl)
		}
	}
	h := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("h"), ProtocolType: v2types.ProtocolTypeHttp}))
	for ttl, ok := range map[int32]bool{99999: false, 3600: true} {
		_, err := e.v2.CreateAuthorizer(ctx, &apigatewayv2.CreateAuthorizerInput{ApiId: h.ApiId, Name: aws.String("x"), AuthorizerType: v2types.AuthorizerTypeRequest,
			AuthorizerUri: aws.String(lambdaURI("arn:aws:lambda:us-east-1:000000000000:function:f")), IdentitySource: []string{"$request.header.x"},
			AuthorizerResultTtlInSeconds: aws.Int32(ttl), AuthorizerPayloadFormatVersion: aws.String("2.0")})
		if (err == nil) != ok {
			t.Fatalf("HTTP authorizer TTL %d: err = %v", ttl, err)
		}
	}
}
