package cloudformation

import (
	"encoding/json"
	"strings"
	"testing"
)

// expand runs the SAM transform on a template and returns its Resources.
func expand(t *testing.T, body string) map[string]map[string]any {
	t.Helper()
	tmpl, err := (&Service{}).template(body)
	if err != nil {
		t.Fatal(err)
	}
	if tmpl["Transform"] != nil || tmpl["Globals"] != nil {
		t.Fatalf("Transform or Globals left in the processed template: %v", tmpl)
	}
	out := map[string]map[string]any{}
	for id, r := range tmpl["Resources"].(map[string]any) {
		out[id] = r.(map[string]any)
	}
	return out
}

func dump(t *testing.T, v any) {
	b, _ := json.MarshalIndent(v, "", " ")
	t.Log(string(b))
}

const samBig = `
Transform: AWS::Serverless-2016-10-31
Globals:
  Function:
    Runtime: python3.12
    Timeout: 10
    Environment:
      Variables: {A: "1"}
    Layers: [!Ref Lib]
  Api:
    Cors: "'*'"
Resources:
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.handler
      CodeUri: s3://code/fn.zip
      AutoPublishAlias: live
      Environment:
        Variables: {B: "2"}
      Policies:
        - AmazonS3ReadOnlyAccess
        - SQSPollerPolicy: {QueueName: !GetAtt Q.QueueName}
        - Statement: [{Effect: Allow, Action: "s3:*", Resource: "*"}]
      Events:
        Get: {Type: Api, Properties: {Path: "/hello/{id}", Method: get}}
        Any: {Type: Api, Properties: {Path: /x, Method: any}}
        Poll: {Type: SQS, Properties: {Queue: !GetAtt Q.Arn, BatchSize: 5}}
        Tick: {Type: Schedule, Properties: {Schedule: rate(5 minutes)}}
        Tock: {Type: ScheduleV2, Properties: {ScheduleExpression: rate(1 hour)}}
        Notify: {Type: SNS, Properties: {Topic: !Ref T}}
        Up: {Type: S3, Properties: {Bucket: !Ref B, Events: s3:ObjectCreated:*}}
        Rows: {Type: DynamoDB, Properties: {Stream: !GetAtt Tbl.StreamArn, StartingPosition: LATEST}}
        Rule: {Type: EventBridgeRule, Properties: {Pattern: {source: [a.b]}}}
  Lib:
    Type: AWS::Serverless::LayerVersion
    Properties: {ContentUri: s3://code/lib.zip}
  Tbl: {Type: AWS::Serverless::SimpleTable}
  Q: {Type: AWS::SQS::Queue}
  T: {Type: AWS::SNS::Topic}
  B: {Type: AWS::S3::Bucket}
Outputs:
  Url: {Value: !Sub "https://${ServerlessRestApi}.execute-api.${AWS::Region}.amazonaws.com/Prod/"}
  Alias: {Value: !Ref Fn.Alias}
`

func TestSAMExpand(t *testing.T) {
	r := expand(t, samBig)
	want := map[string]string{
		"Fn": "AWS::Lambda::Function", "FnRole": "AWS::IAM::Role", "FnAliaslive": "AWS::Lambda::Alias",
		"FnGetPermissionProd": "AWS::Lambda::Permission", "FnPoll": "AWS::Lambda::EventSourceMapping",
		"FnTick": "AWS::Events::Rule", "FnTickPermission": "AWS::Lambda::Permission", "FnTock": "AWS::Scheduler::Schedule",
		"FnTockScheduleRole": "AWS::IAM::Role", "FnNotify": "AWS::SNS::Subscription", "FnUpPermission": "AWS::Lambda::Permission",
		"FnRows": "AWS::Lambda::EventSourceMapping", "FnRule": "AWS::Events::Rule",
		"ServerlessRestApi": "AWS::ApiGateway::RestApi", "ServerlessRestApiProdStage": "AWS::ApiGateway::Stage",
		"Tbl": "AWS::DynamoDB::Table", "Q": "AWS::SQS::Queue",
	}
	for id, typ := range want {
		if r[id] == nil || r[id]["Type"] != typ {
			t.Errorf("%s: want %s, have %v", id, typ, r[id])
		}
	}
	var layer, version string
	for id, res := range r {
		switch {
		case strings.HasPrefix(id, "Lib") && res["Type"] == "AWS::Lambda::LayerVersion":
			layer = id
		case strings.HasPrefix(id, "FnVersion"):
			version = id
		}
	}
	if layer == "" || version == "" {
		t.Fatalf("no layer (%q) or version (%q) made", layer, version)
	}
	// !Ref Lib in Layers follows the renamed layer
	fn := r["Fn"]["Properties"].(map[string]any)
	if got := fn["Layers"].([]any)[0].(map[string]any)["Ref"]; got != layer {
		t.Errorf("Layers ref %v, want %s", got, layer)
	}
	if env := fn["Environment"].(map[string]any)["Variables"].(map[string]any); env["A"] != "1" || env["B"] != "2" || fn["Timeout"].(float64) != 10 {
		t.Errorf("globals not merged: %v", fn)
	}
	// the bucket got the notification and waits for the permission
	b := r["B"]
	if b["DependsOn"].([]any)[0] != "FnUpPermission" || b["Properties"].(map[string]any)["NotificationConfiguration"] == nil {
		t.Errorf("bucket not wired: %v", b)
	}
	if t.Failed() {
		dump(t, r)
	}
}

func TestSAMErrors(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"no code":     {`{"Transform":"AWS::Serverless-2016-10-31","Resources":{"F":{"Type":"AWS::Serverless::Function","Properties":{"Handler":"h","Runtime":"python3.12"}}}}`, "CodeUri or InlineCode"},
		"local path":  {`{"Transform":"AWS::Serverless-2016-10-31","Resources":{"F":{"Type":"AWS::Serverless::Function","Properties":{"Handler":"h","Runtime":"python3.12","CodeUri":"./src"}}}}`, "local path"},
		"bad prop":    {`{"Transform":"AWS::Serverless-2016-10-31","Resources":{"F":{"Type":"AWS::Serverless::Function","Properties":{"InlineCode":"x","Handler":"h","Runtime":"python3.12","DeploymentPreference":{}}}}}`, "DeploymentPreference"},
		"bad event":   {`{"Transform":"AWS::Serverless-2016-10-31","Resources":{"F":{"Type":"AWS::Serverless::Function","Properties":{"InlineCode":"x","Handler":"h","Runtime":"python3.12","Events":{"E":{"Type":"MSK"}}}}}}`, "MSK"},
		"bad policy":  {`{"Transform":"AWS::Serverless-2016-10-31","Resources":{"F":{"Type":"AWS::Serverless::Function","Properties":{"InlineCode":"x","Handler":"h","Runtime":"python3.12","Policies":[{"NoSuchPolicy":{}}]}}}}`, "NoSuchPolicy"},
		"unsupported": {`{"Transform":"AWS::Serverless-2016-10-31","Resources":{"A":{"Type":"AWS::Serverless::Application","Properties":{}}}}`, "AWS::Serverless::Application"},
	} {
		_, err := (&Service{}).template(tc.body)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: want an error with %q, got %v", name, tc.want, err)
		}
	}
}

func TestSAMHttpApiAndStateMachine(t *testing.T) {
	r := expand(t, `
Transform: AWS::Serverless-2016-10-31
Resources:
  Web:
    Type: AWS::Serverless::HttpApi
    Properties:
      StageName: v1
      CorsConfiguration: true
      Auth:
        DefaultAuthorizer: Jwt
        Authorizers:
          Jwt: {JwtConfiguration: {issuer: "https://idp/", audience: [a]}}
          Lam: {FunctionArn: !GetAtt AuthFn.Arn, Identity: {Headers: [Authorization]}}
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: h
      Runtime: nodejs20.x
      InlineCode: "x"
      Events:
        Root: {Type: HttpApi, Properties: {ApiId: !Ref Web, Path: "$default"}}
        Item: {Type: HttpApi, Properties: {ApiId: !Ref Web, Path: "/items/{id}", Method: any, Auth: {Authorizer: Lam}}}
        Open: {Type: HttpApi, Properties: {ApiId: !Ref Web, Path: /open, Method: get, Auth: {Authorizer: NONE}}}
  AuthFn: {Type: AWS::Lambda::Function, Properties: {Code: {ZipFile: x}, Handler: h, Runtime: nodejs20.x, Role: arn}}
  Flow:
    Type: AWS::Serverless::StateMachine
    Properties:
      DefinitionSubstitutions: {Fn: !GetAtt Fn.Arn}
      Definition: {StartAt: A, States: {A: {Type: Pass, End: true}}}
      Events:
        Start: {Type: Api, Properties: {Path: /start, Method: post}}
        Daily: {Type: ScheduleV2, Properties: {ScheduleExpression: rate(1 day)}}
`)
	route := func(id string) map[string]any { return r[id]["Properties"].(map[string]any) }
	if route("FnRootRoute")["RouteKey"] != "$default" || route("FnRootRoute")["AuthorizationType"] != "JWT" {
		t.Errorf("default route: %v", route("FnRootRoute"))
	}
	if p := route("FnItemRoute"); p["RouteKey"] != "ANY /items/{id}" || p["AuthorizationType"] != "CUSTOM" || p["AuthorizerId"].(map[string]any)["Ref"] != "WebLamAuthorizer" {
		t.Errorf("item route: %v", p)
	}
	if p := route("FnOpenRoute"); p["AuthorizationType"] != nil {
		t.Errorf("open route has auth: %v", p)
	}
	if jwt := route("WebJwtAuthorizer")["JwtConfiguration"].(map[string]any); jwt["Issuer"] != "https://idp/" || jwt["Audience"] == nil {
		t.Errorf("jwt: %v", jwt)
	}
	if r["WebLamAuthorizerPermission"] == nil || r["Webv1Stage"] == nil || r["ServerlessHttpApi"] != nil {
		t.Errorf("authorizer permission, stage v1 or an implicit api that should not be there: %v", idsOf(r))
	}
	// the state machine: the role, the Api event's integration and the schedule role
	if r["FlowRole"] == nil || r["FlowDailyRole"] == nil || r["FlowDaily"]["Type"] != "AWS::Scheduler::Schedule" {
		t.Errorf("state machine pieces: %v", idsOf(r))
	}
	body := route("ServerlessRestApi")["Body"].(map[string]any)["paths"].(map[string]any)["/start"].(map[string]any)["post"].(map[string]any)["x-amazon-apigateway-integration"].(map[string]any)
	if body["type"] != "aws" || body["credentials"] == nil {
		t.Errorf("state machine api integration: %v", body)
	}
}

func idsOf(r map[string]map[string]any) []string {
	var ids []string
	for id := range r {
		ids = append(ids, id)
	}
	return ids
}

func TestSAMExplicitApiMergesIntoBody(t *testing.T) {
	r := expand(t, `
Transform: [AWS::Serverless-2016-10-31]
Resources:
  Api:
    Type: AWS::Serverless::Api
    Properties:
      StageName: dev
      Name: mine
      DefinitionBody:
        swagger: "2.0"
        info: {title: mine, version: "1"}
        paths:
          /a: {get: {operationId: getA, responses: {"200": {description: ok}}}}
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: h
      Runtime: nodejs20.x
      InlineCode: "x"
      Events:
        A: {Type: Api, Properties: {RestApiId: !Ref Api, Path: /a, Method: get, Auth: {ApiKeyRequired: true}}}
`)
	paths := r["Api"]["Properties"].(map[string]any)["Body"].(map[string]any)["paths"].(map[string]any)
	get := paths["/a"].(map[string]any)["get"].(map[string]any)
	if get["operationId"] != "getA" || get["x-amazon-apigateway-integration"] == nil || get["security"] == nil {
		t.Errorf("the operation in the body was replaced or not extended: %v", get)
	}
	if r["ApidevStage"] == nil || r["FnAPermissiondev"] == nil || r["ServerlessRestApi"] != nil {
		t.Errorf("ids: %v", idsOf(r))
	}
}
