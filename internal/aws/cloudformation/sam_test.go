package cloudformation_test

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

const samApp = `
Transform: AWS::Serverless-2016-10-31
Globals:
  Function: {Runtime: python3.12, Timeout: 5}
Resources:
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      InlineCode: "def handler(e, c): return {'statusCode': 200}"
      AutoPublishAlias: live
      Environment: {Variables: {TABLE: !Ref Tbl}}
      Policies:
        - DynamoDBCrudPolicy: {TableName: !Ref Tbl}
      Events:
        Hello: {Type: Api, Properties: {Path: /hello, Method: get}}
        Jobs: {Type: SQS, Properties: {Queue: !GetAtt Jobs.Arn, BatchSize: 3}}
        Web: {Type: HttpApi, Properties: {Path: /web, Method: get}}
        Tick: {Type: Schedule, Properties: {Schedule: rate(5 minutes)}}
        Tock: {Type: ScheduleV2, Properties: {ScheduleExpression: rate(1 hour)}}
        Notify: {Type: SNS, Properties: {Topic: !Ref Topic}}
        Upload: {Type: S3, Properties: {Bucket: !Ref Bkt, Events: "s3:ObjectCreated:*"}}
        Rule: {Type: EventBridgeRule, Properties: {Pattern: {source: [app.sam]}}}
      Layers: [!Ref Lib]
      FunctionUrlConfig: {AuthType: NONE}
  Lib:
    Type: AWS::Serverless::LayerVersion
    Properties: {ContentUri: {Bucket: code, Key: lib.zip}}
  Topic: {Type: AWS::SNS::Topic}
  Bkt: {Type: AWS::S3::Bucket}
  Tbl: {Type: AWS::Serverless::SimpleTable}
  Jobs: {Type: AWS::SQS::Queue}
  Flow:
    Type: AWS::Serverless::StateMachine
    Properties:
      Definition: {StartAt: Done, States: {Done: {Type: Pass, End: true}}}
      Policies: [{LambdaInvokePolicy: {FunctionName: !Ref Fn}}]
      Events:
        Daily: {Type: Schedule, Properties: {Schedule: rate(1 day)}}
Outputs:
  Api: {Value: !Ref ServerlessRestApi}
  Stage: {Value: !Ref ServerlessRestApiProdStage}
  HttpApi: {Value: !Ref ServerlessHttpApi}
  AliasArn: {Value: !Ref Fn.Alias}
  Machine: {Value: !Ref Flow}
  Fn: {Value: !Ref Fn}
`

// A small SAM app (function, Api and SQS events, table, state machine) deploys and every resource is real.
func TestSAMApp(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	// the layer's content, as `sam package` would have uploaded it
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("code")}))
	var zipped bytes.Buffer
	zw := zip.NewWriter(&zipped)
	must(zw.Create("python/lib.py")).Write([]byte("X = 1\n"))
	zw.Close()
	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("code"), Key: aws.String("lib.zip"), Body: bytes.NewReader(zipped.Bytes())}))
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("sam"), TemplateBody: aws.String(samApp),
		Capabilities: []types.Capability{types.CapabilityCapabilityIam}}))
	st := settle(t, c, "sam")
	if st.StackStatus != types.StackStatusCreateComplete {
		for _, e := range must(c.DescribeStackEvents(ctx, &cfn.DescribeStackEventsInput{StackName: aws.String("sam")})).StackEvents {
			if strings.HasSuffix(string(e.ResourceStatus), "_FAILED") {
				t.Logf("%s: %s", aws.ToString(e.LogicalResourceId), aws.ToString(e.ResourceStatusReason))
			}
		}
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	out := outputs(st)

	// the logical ids SAM makes
	res := must(c.ListStackResources(ctx, &cfn.ListStackResourcesInput{StackName: aws.String("sam")}))
	have := map[string]string{}
	for _, r := range res.StackResourceSummaries {
		have[aws.ToString(r.LogicalResourceId)] = aws.ToString(r.ResourceType)
	}
	for id, typ := range map[string]string{"Fn": "AWS::Lambda::Function", "FnRole": "AWS::IAM::Role", "FnAliaslive": "AWS::Lambda::Alias",
		"FnHelloPermissionProd": "AWS::Lambda::Permission", "FnJobs": "AWS::Lambda::EventSourceMapping", "ServerlessRestApi": "AWS::ApiGateway::RestApi",
		"ServerlessRestApiProdStage": "AWS::ApiGateway::Stage", "ServerlessHttpApi": "AWS::ApiGatewayV2::Api", "Tbl": "AWS::DynamoDB::Table",
		"Flow": "AWS::StepFunctions::StateMachine", "FlowRole": "AWS::IAM::Role", "FnTick": "AWS::Events::Rule", "FnTock": "AWS::Scheduler::Schedule",
		"FnNotify": "AWS::SNS::Subscription", "FnUploadPermission": "AWS::Lambda::Permission", "FnRule": "AWS::Events::Rule", "FnUrl": "AWS::Lambda::Url",
		"FlowDaily": "AWS::Events::Rule", "FlowDailyRole": "AWS::IAM::Role"} {
		if have[id] != typ {
			t.Errorf("resource %s: want %s, have %q", id, typ, have[id])
		}
	}

	lc := lambda.NewFromConfig(cfg)
	if a := must(lc.GetAlias(ctx, &lambda.GetAliasInput{FunctionName: aws.String(out["Fn"]), Name: aws.String("live")})); aws.ToString(a.AliasArn) != out["AliasArn"] {
		t.Errorf("alias %s, output %s", aws.ToString(a.AliasArn), out["AliasArn"])
	}
	fn := must(lc.GetFunctionConfiguration(ctx, &lambda.GetFunctionConfigurationInput{FunctionName: aws.String(out["Fn"])}))
	if fn.Environment.Variables["TABLE"] == "" || fn.Timeout == nil || *fn.Timeout != 5 {
		t.Errorf("function configuration: %+v", fn)
	}
	maps := must(lc.ListEventSourceMappings(ctx, &lambda.ListEventSourceMappingsInput{FunctionName: aws.String(out["Fn"])}))
	if len(maps.EventSourceMappings) != 1 || *maps.EventSourceMappings[0].BatchSize != 3 {
		t.Errorf("event source mappings: %+v", maps.EventSourceMappings)
	}
	if _, err := dynamodb.NewFromConfig(cfg).DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(fn.Environment.Variables["TABLE"])}); err != nil {
		t.Errorf("table: %v", err)
	}

	// the ScheduleV2 event made a real schedule that calls the alias
	sch := must(scheduler.NewFromConfig(cfg).ListSchedules(ctx, &scheduler.ListSchedulesInput{}))
	if len(sch.Schedules) != 1 || !strings.HasSuffix(aws.ToString(sch.Schedules[0].Target.Arn), ":live") {
		t.Errorf("schedules: %+v", sch.Schedules)
	}

	// the REST API has the route with a Lambda proxy integration, and the stage is deployed
	ag := apigateway.NewFromConfig(cfg)
	rs := must(ag.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: aws.String(out["Api"])}))
	found := false
	for _, r := range rs.Items {
		if aws.ToString(r.Path) == "/hello" {
			m := must(ag.GetIntegration(ctx, &apigateway.GetIntegrationInput{RestApiId: aws.String(out["Api"]), ResourceId: r.Id, HttpMethod: aws.String("GET")}))
			found = strings.Contains(aws.ToString(m.Uri), "functions/arn:aws:lambda:") && strings.HasSuffix(aws.ToString(m.Uri), ":live/invocations")
		}
	}
	if !found {
		t.Errorf("no GET /hello integration to the alias in %+v", rs.Items)
	}
	if s := must(ag.GetStage(ctx, &apigateway.GetStageInput{RestApiId: aws.String(out["Api"]), StageName: aws.String("Prod")})); s.DeploymentId == nil {
		t.Errorf("stage Prod has no deployment")
	}
	routes := must(apigatewayv2.NewFromConfig(cfg).GetRoutes(ctx, &apigatewayv2.GetRoutesInput{ApiId: aws.String(out["HttpApi"])}))
	if len(routes.Items) != 1 || aws.ToString(routes.Items[0].RouteKey) != "GET /web" {
		t.Errorf("http api routes: %+v", routes.Items)
	}
	if _, err := sfn.NewFromConfig(cfg).DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(out["Machine"])}); err != nil {
		t.Errorf("state machine: %v", err)
	}

	// the processed template is the expanded one
	g := must(c.GetTemplate(ctx, &cfn.GetTemplateInput{StackName: aws.String("sam"), TemplateStage: types.TemplateStageProcessed}))
	if body := aws.ToString(g.TemplateBody); strings.Contains(body, "Serverless::") || !strings.Contains(body, "ServerlessRestApiProdStage") {
		t.Errorf("processed template: %.300s", body)
	}

	// new code makes a new version, and the alias moves to it
	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("sam"), TemplateBody: aws.String(strings.Replace(samApp, "'statusCode': 200", "'statusCode': 201", 1)),
		Capabilities: []types.Capability{types.CapabilityCapabilityIam}}))
	if st := settle(t, c, "sam"); st.StackStatus != types.StackStatusUpdateComplete {
		t.Fatalf("update: %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	if a := must(lc.GetAlias(ctx, &lambda.GetAliasInput{FunctionName: aws.String(out["Fn"]), Name: aws.String("live")})); aws.ToString(a.FunctionVersion) != "2" {
		t.Errorf("alias points at version %s after the update, want 2", aws.ToString(a.FunctionVersion))
	}

	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String("sam")}))
	if st := settle(t, c, *st.StackId); st.StackStatus != types.StackStatusDeleteComplete {
		t.Errorf("delete: %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
}

const samAuth = `
Transform: AWS::Serverless-2016-10-31
Resources:
  Pool: {Type: AWS::Cognito::UserPool}
  Api:
    Type: AWS::Serverless::Api
    Properties:
      StageName: dev
      Cors: "'*'"
      Auth:
        DefaultAuthorizer: Cog
        Authorizers:
          Cog: {UserPoolArn: !GetAtt Pool.Arn}
  Fn:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
      Runtime: python3.12
      InlineCode: "def handler(e, c): pass"
      Events:
        Private: {Type: Api, Properties: {RestApiId: !Ref Api, Path: /private, Method: get}}
        Public: {Type: Api, Properties: {RestApiId: !Ref Api, Path: /public, Method: get, Auth: {Authorizer: NONE}}}
Outputs:
  Api: {Value: !Ref Api}
  Stage: {Value: !Ref Api.Stage}
`

// The Cognito authorizer of a SAM API is created, the default applies to the routes, and Authorizer NONE opts out.
func TestSAMCognitoAuthorizer(t *testing.T) {
	cfg := start(t)
	c, ag := cfn.NewFromConfig(cfg), apigateway.NewFromConfig(cfg)
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("samauth"), TemplateBody: aws.String(samAuth),
		Capabilities: []types.Capability{types.CapabilityCapabilityIam}}))
	st := settle(t, c, "samauth")
	if st.StackStatus != types.StackStatusCreateComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	out := outputs(st)
	if out["Stage"] == "" {
		t.Errorf("!Ref Api.Stage gave nothing")
	}
	azs := must(ag.GetAuthorizers(ctx, &apigateway.GetAuthorizersInput{RestApiId: aws.String(out["Api"])}))
	if len(azs.Items) != 1 || azs.Items[0].Type != "COGNITO_USER_POOLS" || len(azs.Items[0].ProviderARNs) != 1 {
		t.Fatalf("authorizers: %+v", azs.Items)
	}
	auth := map[string]string{}
	for _, r := range must(ag.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: aws.String(out["Api"]), Embed: []string{"methods"}})).Items {
		for verb, m := range r.ResourceMethods {
			auth[aws.ToString(r.Path)+" "+verb] = aws.ToString(m.AuthorizationType) + "/" + aws.ToString(m.AuthorizerId)
		}
	}
	id := aws.ToString(azs.Items[0].Id)
	if auth["/private GET"] != "COGNITO_USER_POOLS/"+id || auth["/public GET"] != "NONE/" || auth["/private OPTIONS"] != "NONE/" {
		t.Errorf("method authorization: %v", auth)
	}
}
