package apigateway_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	mgmttypes "github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi/types"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	v2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	ltypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/coder/websocket"
	"github.com/nrndev/pogi/internal/awstest"
)

// wsApp's handlers post the events they get to $HOOK; echo answers through
// @connections on $AWS_ENDPOINT_URL, the request the apigatewaymanagementapi
// client makes there (boto3 is not in the native runtime).
const wsApp = `
import json, os, urllib.request

def post(url, body):
    urllib.request.urlopen(urllib.request.Request(url, data=body, method="POST")).read()

def connect(event, context):
    post(os.environ["HOOK"], json.dumps(event).encode())
    return {"statusCode": 403 if "deny" in (event.get("queryStringParameters") or {}) else 200}

def disconnect(event, context):
    post(os.environ["HOOK"], json.dumps(event).encode())
    return {"statusCode": 200}

def echo(event, context):
    post(os.environ["HOOK"], json.dumps(event).encode())
    data = json.loads(event["body"])["data"]
    post(os.environ["AWS_ENDPOINT_URL"] + "/@connections/" + event["requestContext"]["connectionId"], json.dumps({"echo": data}).encode())
    return {"statusCode": 200}

def fallback(event, context):
    return {"statusCode": 200, "body": "default:" + event["body"]}

def authorize(event, context):
    ok = (event.get("queryStringParameters") or {}).get("token") == "good"
    return {"principalId": "me", "context": {"user": "bob"}, "policyDocument": {"Version": "2012-10-17",
            "Statement": [{"Action": "execute-api:Invoke", "Effect": "Allow" if ok else "Deny", "Resource": event["methodArn"]}]}}
`

// wsEvents records what the functions post to their hook.
func wsEvents(t *testing.T) (string, func() map[string]any) {
	ch := make(chan map[string]any, 20)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev map[string]any
		json.NewDecoder(r.Body).Decode(&ev)
		ch <- ev
	}))
	t.Cleanup(hook.Close)
	return hook.URL, func() map[string]any {
		t.Helper()
		select {
		case ev := <-ch:
			return ev
		case <-time.After(15 * time.Second):
			t.Fatal("no event")
			return nil
		}
	}
}

// wsFunction deploys a handler of wsApp and returns its ARN.
func (e *env) wsFunction(handler, hook string) string {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("app.py")
	io.WriteString(w, wsApp)
	zw.Close()
	out := must(e.fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(handler), Runtime: ltypes.RuntimePython312,
		Handler: aws.String("app." + handler), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &ltypes.FunctionCode{ZipFile: buf.Bytes()},
		Environment: &ltypes.Environment{Variables: map[string]string{"HOOK": hook}}}))
	return *out.FunctionArn
}

// wsRoute adds a route to an integration; a two-way route gets a route response.
func (e *env) wsRoute(api *string, key string, in *apigatewayv2.CreateIntegrationInput, twoWay bool) *apigatewayv2.CreateRouteOutput {
	in.ApiId = api
	integ := must(e.v2.CreateIntegration(ctx, in))
	rt := must(e.v2.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: api, RouteKey: aws.String(key), Target: aws.String("integrations/" + *integ.IntegrationId)}))
	if twoWay {
		must(e.v2.CreateRouteResponse(ctx, &apigatewayv2.CreateRouteResponseInput{ApiId: api, RouteId: rt.RouteId, RouteResponseKey: aws.String("$default")}))
	}
	return rt
}

func (e *env) wsAPI(name string) *apigatewayv2.CreateApiOutput {
	api := must(e.v2.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String(name), ProtocolType: v2types.ProtocolTypeWebsocket,
		RouteSelectionExpression: aws.String("$request.body.action")}))
	must(e.v2.CreateStage(ctx, &apigatewayv2.CreateStageInput{ApiId: api.ApiId, StageName: aws.String("dev"), AutoDeploy: aws.Bool(true)}))
	return api
}

func proxy(fn string) *apigatewayv2.CreateIntegrationInput {
	return &apigatewayv2.CreateIntegrationInput{IntegrationType: v2types.IntegrationTypeAwsProxy, IntegrationUri: aws.String(lambdaURI(fn))}
}

func read(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, b, err := c.Read(rctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestWebSocketAPI(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	hook, next := wsEvents(t)
	api := e.wsAPI("chat")
	if *api.ApiEndpoint != "ws://"+*api.ApiId+".execute-api.localhost:"+e.port || *api.RouteSelectionExpression != "$request.body.action" {
		t.Fatalf("api = %+v", api)
	}
	e.wsRoute(api.ApiId, "$connect", proxy(e.wsFunction("connect", hook)), false)
	e.wsRoute(api.ApiId, "$disconnect", proxy(e.wsFunction("disconnect", hook)), false)
	e.wsRoute(api.ApiId, "echo", proxy(e.wsFunction("echo", hook)), false)
	e.wsRoute(api.ApiId, "$default", proxy(e.wsFunction("fallback", hook)), true)
	host := *api.ApiId + ".execute-api.localhost:" + e.port

	// $connect gets the headers and the query string.
	conn, _, err := websocket.Dial(ctx, "ws://127.0.0.1:"+e.port+"/dev?user=ann", &websocket.DialOptions{Host: host, HTTPHeader: http.Header{"X-Team": {"blue"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	ev := next()
	rc := ev["requestContext"].(map[string]any)
	id, _ := rc["connectionId"].(string)
	if rc["eventType"] != "CONNECT" || rc["routeKey"] != "$connect" || id == "" || rc["stage"] != "dev" || rc["apiId"] != *api.ApiId ||
		rc["domainName"] != host || rc["connectedAt"] == nil || rc["requestId"] == nil ||
		ev["queryStringParameters"].(map[string]any)["user"] != "ann" || ev["headers"].(map[string]any)["X-Team"] != "blue" {
		t.Fatalf("connect event = %v", ev)
	}

	// A message routes by its action; echo answers through @connections.
	conn.Write(ctx, websocket.MessageText, []byte(`{"action":"echo","data":"hi"}`))
	ev = next()
	rc = ev["requestContext"].(map[string]any)
	if rc["eventType"] != "MESSAGE" || rc["routeKey"] != "echo" || rc["connectionId"] != id || rc["messageId"] == nil ||
		ev["body"] != `{"action":"echo","data":"hi"}` || ev["isBase64Encoded"] != false {
		t.Fatalf("message event = %v", ev)
	}
	if got := read(t, conn); got != `{"echo": "hi"}` {
		t.Fatalf("echo = %s", got)
	}
	// Anything else takes $default, two-way; binary arrives base64-encoded.
	conn.Write(ctx, websocket.MessageText, []byte("plain"))
	if got := read(t, conn); got != "default:plain" {
		t.Fatalf("default = %s", got)
	}
	conn.Write(ctx, websocket.MessageBinary, []byte{0xff, 0})
	if got := read(t, conn); got != "default:/wA=" {
		t.Fatalf("binary = %s", got)
	}

	// The management API, at pogi's endpoint and at the stage's.
	mgmt := apigatewaymanagementapi.NewFromConfig(awstest.Config(t, e.url))
	staged := apigatewaymanagementapi.NewFromConfig(awstest.Config(t, e.url+"/_aws/execute-api/"+*api.ApiId+"/dev"))
	info := must(mgmt.GetConnection(ctx, &apigatewaymanagementapi.GetConnectionInput{ConnectionId: &id}))
	if info.ConnectedAt == nil || info.LastActiveAt == nil || *info.Identity.SourceIp != "127.0.0.1" || info.Identity.UserAgent == nil {
		t.Fatalf("connection = %+v", info)
	}
	must(mgmt.PostToConnection(ctx, &apigatewaymanagementapi.PostToConnectionInput{ConnectionId: &id, Data: []byte("pushed")}))
	must(staged.PostToConnection(ctx, &apigatewaymanagementapi.PostToConnectionInput{ConnectionId: &id, Data: []byte("staged")}))
	if a, b := read(t, conn), read(t, conn); a != "pushed" || b != "staged" {
		t.Fatalf("posted = %s %s", a, b)
	}

	// DELETE closes the connection, $disconnect fires and the connection is gone.
	must(mgmt.DeleteConnection(ctx, &apigatewaymanagementapi.DeleteConnectionInput{ConnectionId: &id}))
	if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("after delete: %v", err)
	}
	if rc = next()["requestContext"].(map[string]any); rc["eventType"] != "DISCONNECT" || rc["routeKey"] != "$disconnect" || rc["connectionId"] != id {
		t.Fatalf("disconnect event = %v", rc)
	}
	var gone *mgmttypes.GoneException
	if _, err := mgmt.PostToConnection(ctx, &apigatewaymanagementapi.PostToConnectionInput{ConnectionId: &id, Data: []byte("x")}); !errors.As(err, &gone) {
		t.Fatalf("post after delete: %v", err)
	}

	// A client closing fires $disconnect with its status (path-style URL).
	base := "ws://127.0.0.1:" + e.port + "/_aws/execute-api/" + *api.ApiId + "/dev"
	other, _, err := websocket.Dial(ctx, base, nil)
	if err != nil {
		t.Fatal(err)
	}
	id2 := next()["requestContext"].(map[string]any)["connectionId"].(string)
	other.Close(websocket.StatusNormalClosure, "bye")
	if rc = next()["requestContext"].(map[string]any); rc["connectionId"] != id2 || rc["disconnectStatusCode"] != 1000.0 || rc["disconnectReason"] != "bye" {
		t.Fatalf("client close = %v", rc)
	}
	if _, err := staged.GetConnection(ctx, &apigatewaymanagementapi.GetConnectionInput{ConnectionId: &id2}); !errors.As(err, &gone) {
		t.Fatalf("get after close: %v", err)
	}

	// $connect refuses with a non-2xx status.
	if _, res, err := websocket.Dial(ctx, base+"?deny=1", nil); err == nil || res == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("denied connect = %v %v", res, err)
	}
	next()
}

func TestWebSocketAuthorizer(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	hook, next := wsEvents(t)
	api := e.wsAPI("secure")
	au := must(e.v2.CreateAuthorizer(ctx, &apigatewayv2.CreateAuthorizerInput{ApiId: api.ApiId, Name: aws.String("auth"),
		AuthorizerType: v2types.AuthorizerTypeRequest, AuthorizerUri: aws.String(lambdaURI(e.wsFunction("authorize", hook))),
		IdentitySource: []string{"route.request.querystring.token"}}))
	rt := e.wsRoute(api.ApiId, "$connect", proxy(e.wsFunction("connect", hook)), false)
	must(e.v2.UpdateRoute(ctx, &apigatewayv2.UpdateRouteInput{ApiId: api.ApiId, RouteId: rt.RouteId,
		AuthorizationType: v2types.AuthorizationTypeCustom, AuthorizerId: au.AuthorizerId}))
	base := "ws://127.0.0.1:" + e.port + "/_aws/execute-api/" + *api.ApiId + "/dev"
	for query, status := range map[string]int{"": http.StatusUnauthorized, "?token=bad": http.StatusForbidden} {
		if _, res, err := websocket.Dial(ctx, base+query, nil); err == nil || res == nil || res.StatusCode != status {
			t.Fatalf("connect%s = %v %v", query, res, err)
		}
	}
	conn, _, err := websocket.Dial(ctx, base+"?token=good", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if a := next()["requestContext"].(map[string]any)["authorizer"].(map[string]any); a["principalId"] != "me" || a["user"] != "bob" {
		t.Fatalf("authorizer context = %v", a)
	}
}

func TestWebSocketMockAndHTTP(t *testing.T) {
	e := setup(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "relayed:%s", b)
	}))
	defer backend.Close()
	api := e.wsAPI("mocked")
	mock := &apigatewayv2.CreateIntegrationInput{IntegrationType: v2types.IntegrationTypeMock, TemplateSelectionExpression: aws.String(`\$default`),
		RequestTemplates: map[string]string{"$default": `{"statusCode": 200}`}}
	ping := e.wsRoute(api.ApiId, "ping", mock, true)
	integ := strings.TrimPrefix(*ping.Target, "integrations/")
	must(e.v2.CreateIntegrationResponse(ctx, &apigatewayv2.CreateIntegrationResponseInput{ApiId: api.ApiId, IntegrationId: &integ,
		IntegrationResponseKey: aws.String("$default"), ResponseTemplates: map[string]string{"$default": `{"pong": "$context.connectionId"}`}}))
	e.wsRoute(api.ApiId, "quiet", &apigatewayv2.CreateIntegrationInput{IntegrationType: v2types.IntegrationTypeMock}, false)
	e.wsRoute(api.ApiId, "relay", &apigatewayv2.CreateIntegrationInput{IntegrationType: v2types.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String(backend.URL), IntegrationMethod: aws.String("POST")}, true)
	if rr := must(e.v2.GetRouteResponses(ctx, &apigatewayv2.GetRouteResponsesInput{ApiId: api.ApiId, RouteId: ping.RouteId})); len(rr.Items) != 1 || *rr.Items[0].RouteResponseKey != "$default" {
		t.Fatalf("route responses = %+v", rr.Items)
	}
	if ir := must(e.v2.GetIntegrationResponses(ctx, &apigatewayv2.GetIntegrationResponsesInput{ApiId: api.ApiId, IntegrationId: &integ})); len(ir.Items) != 1 {
		t.Fatalf("integration responses = %+v", ir.Items)
	}

	base := "ws://127.0.0.1:" + e.port + "/_aws/execute-api/" + *api.ApiId
	if _, res, err := websocket.Dial(ctx, base+"/prod", nil); err == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("unknown stage = %v %v", res, err)
	}
	if status, _, _ := call(t, "GET", strings.Replace(base, "ws:", "http:", 1)+"/dev", "", nil); status != http.StatusUpgradeRequired {
		t.Fatalf("plain GET = %d", status)
	}
	conn, _, err := websocket.Dial(ctx, base+"/dev", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	for _, step := range []struct {
		send []string
		want string
	}{
		{[]string{`{"action":"quiet"}`, `{"action":"ping"}`}, `{"pong": "`}, // quiet is one-way: the first answer is ping's
		{[]string{`{"action":"relay","n":1}`}, `relayed:{"action":"relay","n":1}`},
		{[]string{`{"action":"nope"}`}, `"message":"Forbidden"`}, // no route and no $default
	} {
		for _, m := range step.send {
			conn.Write(ctx, websocket.MessageText, []byte(m))
		}
		if got := read(t, conn); !strings.Contains(got, step.want) {
			t.Fatalf("%v = %s", step.send, got)
		}
	}
}
