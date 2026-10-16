package cloudformation_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

var ctx = context.Background()

// start runs every service on a port that also speaks TLS, like `pogi start`.
func start(t *testing.T) aws.Config {
	return awstest.Config(t, pogi.Start(t).URL)
}

// settle waits for a stack to leave its *_IN_PROGRESS states.
func settle(t *testing.T, c *cfn.Client, stack string) types.Stack {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		out, err := c.DescribeStacks(ctx, &cfn.DescribeStacksInput{StackName: aws.String(stack)})
		if err != nil {
			t.Fatal(err)
		}
		st := out.Stacks[0]
		if !strings.HasSuffix(string(st.StackStatus), "_IN_PROGRESS") {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("stack %s stuck in %s", stack, st.StackStatus)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// failed reports whether a resource has a *_FAILED event (whose reason contains want, if given).
func failed(t *testing.T, c *cfn.Client, stack, logical string, want ...string) bool {
	t.Helper()
	for _, e := range must(c.DescribeStackEvents(ctx, &cfn.DescribeStackEventsInput{StackName: aws.String(stack)})).StackEvents {
		if *e.LogicalResourceId == logical && strings.HasSuffix(string(e.ResourceStatus), "_FAILED") &&
			(len(want) == 0 || strings.Contains(aws.ToString(e.ResourceStatusReason), want[0])) {
			return true
		}
	}
	return false
}

func outputs(st types.Stack) map[string]string {
	m := map[string]string{}
	for _, o := range st.Outputs {
		m[*o.OutputKey] = *o.OutputValue
	}
	return m
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

const appTemplate = `
AWSTemplateFormatVersion: "2010-09-09"
Description: pogi test stack
Parameters:
  Env:
    Type: AWS::SSM::Parameter::Value<String>
    Default: /test/env
  Retention:
    Type: Number
    Default: 345600
    MinValue: 60
Conditions:
  IsDev: !Equals [!Ref Env, dev]
  IsProd: !Not [!Condition IsDev]
Mappings:
  Delays:
    dev: {Seconds: 1}
    prod: {Seconds: 5}
Resources:
  DLQ:
    Type: AWS::SQS::Queue
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-work"
      DelaySeconds: !FindInMap [Delays, !Ref Env, Seconds]
      MessageRetentionPeriod: !Ref Retention
      RedrivePolicy:
        deadLetterTargetArn: !GetAtt DLQ.Arn
        maxReceiveCount: 3
  Topic:
    Type: AWS::SNS::Topic
  Sub:
    Type: AWS::SNS::Subscription
    Properties:
      TopicArn: !Ref Topic
      Protocol: sqs
      Endpoint: !GetAtt Queue.Arn
      RawMessageDelivery: true
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      VersioningConfiguration: {Status: Enabled}
  QueueArnParam:
    Type: AWS::SSM::Parameter
    Properties:
      Name: /test/queue-arn
      Type: String
      Value: !GetAtt Queue.Arn
  ProdOnly:
    Type: AWS::SQS::Queue
    Condition: IsProd
Outputs:
  QueueUrl:
    Value: !Ref Queue
  QueueArn:
    Value: !GetAtt Queue.Arn
    Export: {Name: !Sub "${AWS::StackName}-QueueArn"}
  BucketName:
    Value: !Ref Bucket
  Suffix:
    Value: !Select [1, !Split ["-", !Ref "AWS::StackName"]]
  Where:
    Value: !Join [":", [!Ref "AWS::Region", !If [IsDev, dev, prod]]]
`

func TestStackLifecycle(t *testing.T) {
	cfg := start(t)
	c, sq, sm, s3c := cfn.NewFromConfig(cfg), sqs.NewFromConfig(cfg), ssm.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	must(sm.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/test/env"), Value: aws.String("dev"), Type: "String"}))

	created := must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("app-one"), TemplateBody: aws.String(appTemplate)}))
	st := settle(t, c, "app-one")
	if st.StackStatus != types.StackStatusCreateComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	out := outputs(st)
	if !strings.HasSuffix(out["QueueUrl"], "/app-one-work") || !strings.HasSuffix(out["QueueArn"], ":app-one-work") ||
		out["Suffix"] != "one" || out["Where"] != "us-east-1:dev" || out["BucketName"] == "" {
		t.Fatalf("outputs %v", out)
	}
	if p := st.Parameters; len(p) != 2 || *p[0].ParameterKey != "Env" || aws.ToString(p[0].ResolvedValue) != "dev" {
		t.Fatalf("parameters %+v", p)
	}
	attrs := must(sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(out["QueueUrl"]), AttributeNames: []sqstypes.QueueAttributeName{"All"}})).Attributes
	if attrs["DelaySeconds"] != "1" || attrs["MessageRetentionPeriod"] != "345600" || !strings.Contains(attrs["RedrivePolicy"], "maxReceiveCount") {
		t.Fatalf("queue attributes %v", attrs)
	}
	if v := must(sm.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/test/queue-arn")})).Parameter.Value; *v != out["QueueArn"] {
		t.Fatalf("ssm value %s", *v)
	}
	if ex := must(c.ListExports(ctx, &cfn.ListExportsInput{})).Exports; len(ex) != 1 || *ex[0].Name != "app-one-QueueArn" {
		t.Fatalf("exports %+v", ex)
	}
	if rs := must(c.ListStackResources(ctx, &cfn.ListStackResourcesInput{StackName: aws.String("app-one")})).StackResourceSummaries; len(rs) != 6 {
		t.Fatalf("want 6 resources (ProdOnly is conditional), got %d", len(rs))
	}

	// a change set that adds, modifies and removes
	next := strings.Replace(appTemplate, "      Value: !GetAtt Queue.Arn\n", "      Value: !Sub \"${Queue.Arn}-v2\"\n  Extra:\n    Type: AWS::SQS::Queue\n    Properties: {QueueName: extra}\n", 1)
	next = strings.Replace(next, "  Bucket:\n    Type: AWS::S3::Bucket\n    Properties:\n      VersioningConfiguration: {Status: Enabled}\n", "", 1)
	next = strings.Replace(next, "  BucketName:\n    Value: !Ref Bucket\n", "", 1)
	must(c.CreateChangeSet(ctx, &cfn.CreateChangeSetInput{StackName: aws.String("app-one"), ChangeSetName: aws.String("v2"), TemplateBody: aws.String(next)}))
	cs := must(c.DescribeChangeSet(ctx, &cfn.DescribeChangeSetInput{StackName: aws.String("app-one"), ChangeSetName: aws.String("v2")}))
	var changes []string
	for _, ch := range cs.Changes {
		changes = append(changes, string(ch.ResourceChange.Action)+" "+*ch.ResourceChange.LogicalResourceId)
	}
	if cs.Status != types.ChangeSetStatusCreateComplete || strings.Join(changes, ",") != "Add Extra,Modify QueueArnParam,Remove Bucket" {
		t.Fatalf("change set %s %v", cs.Status, changes)
	}
	must(c.ExecuteChangeSet(ctx, &cfn.ExecuteChangeSetInput{StackName: aws.String("app-one"), ChangeSetName: aws.String("v2")}))
	if st = settle(t, c, "app-one"); st.StackStatus != types.StackStatusUpdateComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	if _, err := s3c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(out["BucketName"])}); err == nil {
		t.Fatal("removed bucket still exists")
	}
	if v := must(sm.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/test/queue-arn")})).Parameter.Value; *v != out["QueueArn"]+"-v2" {
		t.Fatalf("ssm value after update %s", *v)
	}
	must(sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("extra")}))

	// nothing to do
	if _, err := c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("app-one"), UsePreviousTemplate: aws.Bool(true)}); err == nil || !strings.Contains(err.Error(), "No updates are to be performed") {
		t.Fatalf("want no-op error, got %v", err)
	}

	// a failing update rolls back to the previous template
	bad := strings.Replace(next, "Properties: {QueueName: extra}", "Properties: {QueueName: \"not valid!\"}", 1)
	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("app-one"), TemplateBody: aws.String(bad)}))
	if st = settle(t, c, "app-one"); st.StackStatus != types.StackStatusUpdateRollbackComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	if body := must(c.GetTemplate(ctx, &cfn.GetTemplateInput{StackName: aws.String("app-one")})).TemplateBody; *body != next {
		t.Fatal("rollback did not restore the template")
	}
	must(sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("extra")}))

	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String("app-one")}))
	if st = settle(t, c, *created.StackId); st.StackStatus != types.StackStatusDeleteComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	if _, err := sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("app-one-work")}); err == nil {
		t.Fatal("queue survived stack deletion")
	}
	if _, err := sm.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/test/queue-arn")}); err == nil {
		t.Fatal("parameter survived stack deletion")
	}
	if ex := must(c.ListExports(ctx, &cfn.ListExportsInput{})).Exports; len(ex) != 0 {
		t.Fatalf("exports after delete %+v", ex)
	}
}

func TestValidationAndRollback(t *testing.T) {
	cfg := start(t)
	c, sq, sm := cfn.NewFromConfig(cfg), sqs.NewFromConfig(cfg), ssm.NewFromConfig(cfg)

	// CDK's bootstrap check: a failing rule rejects the call
	cdk := `
Parameters:
  BootstrapVersion:
    Type: AWS::SSM::Parameter::Value<String>
    Default: /cdk-bootstrap/hnb659fds/version
Rules:
  CheckBootstrapVersion:
    Assertions:
      - Assert: !Not [!Contains [["1", "2", "3", "4", "5"], !Ref BootstrapVersion]]
        AssertDescription: CDK bootstrap stack version 6 required.
Resources:
  Q: {Type: AWS::SQS::Queue}
`
	if _, err := c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("cdk"), TemplateBody: aws.String(cdk)}); err == nil || !strings.Contains(err.Error(), "Unable to fetch parameters") {
		t.Fatalf("missing SSM parameter: %v", err)
	}
	must(sm.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/cdk-bootstrap/hnb659fds/version"), Value: aws.String("3"), Type: "String"}))
	if _, err := c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("cdk"), TemplateBody: aws.String(cdk)}); err == nil || !strings.Contains(err.Error(), "version 6 required") {
		t.Fatalf("rule: %v", err)
	}
	must(sm.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/cdk-bootstrap/hnb659fds/version"), Value: aws.String("30"), Type: "String", Overwrite: aws.Bool(true)}))
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("cdk"), TemplateBody: aws.String(cdk)}))
	if st := settle(t, c, "cdk"); st.StackStatus != types.StackStatusCreateComplete {
		t.Fatalf("status %s", st.StackStatus)
	}

	// IAM resources need a capability
	role := `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`
	if _, err := c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("iam"), TemplateBody: aws.String(role)}); err == nil || !strings.Contains(err.Error(), "CAPABILITY_IAM") {
		t.Fatalf("capabilities: %v", err)
	}

	// a failed create deletes what it made
	broken := `
Resources:
  Good:
    Type: AWS::SQS::Queue
    Properties: {QueueName: good-one}
  Bad:
    Type: AWS::SQS::Queue
    DependsOn: Good
    Properties: {QueueName: "bad name!"}
`
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("broken"), TemplateBody: aws.String(broken)}))
	st := settle(t, c, "broken")
	if st.StackStatus != types.StackStatusRollbackComplete || !failed(t, c, "broken", "Bad") {
		t.Fatalf("status %s", st.StackStatus)
	}
	if _, err := sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("good-one")}); err == nil {
		t.Fatal("rollback left the queue behind")
	}
	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: st.StackId}))
	if st = settle(t, c, *st.StackId); st.StackStatus != types.StackStatusDeleteComplete {
		t.Fatalf("delete after rollback: %s", st.StackStatus)
	}
}

func TestNestedStack(t *testing.T) {
	cfg := start(t)
	c, sq, s3c := cfn.NewFromConfig(cfg), sqs.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	child := `
Parameters:
  Prefix: {Type: String}
Resources:
  Q:
    Type: AWS::SQS::Queue
    Properties: {QueueName: !Sub "${Prefix}-child"}
Outputs:
  QueueName: {Value: !GetAtt Q.QueueName}
`
	must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("templates")}))
	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("templates"), Key: aws.String("child.yaml"), Body: strings.NewReader(child)}))
	parent := `
Resources:
  Child:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: https://templates.s3.amazonaws.com/child.yaml
      Parameters: {Prefix: nest}
Outputs:
  ChildQueue: {Value: !GetAtt Child.Outputs.QueueName}
`
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("parent"), TemplateBody: aws.String(parent)}))
	st := settle(t, c, "parent")
	if st.StackStatus != types.StackStatusCreateComplete || outputs(st)["ChildQueue"] != "nest-child" {
		t.Fatalf("status %s %v: %s", st.StackStatus, outputs(st), aws.ToString(st.StackStatusReason))
	}
	must(sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("nest-child")}))
	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String("parent")}))
	if st = settle(t, c, *st.StackId); st.StackStatus != types.StackStatusDeleteComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	if _, err := sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("nest-child")}); err == nil {
		t.Fatal("nested stack's queue survived")
	}
}

const customTemplate = `
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement: [{Effect: Allow, Principal: {Service: lambda.amazonaws.com}, Action: "sts:AssumeRole"}]
  PyFn:
    Type: AWS::Lambda::Function
    Properties:
      Runtime: python3.12
      Handler: index.handler
      Role: !GetAtt Role.Arn
      Code:
        ZipFile: |
          import json, urllib.request
          def handler(event, context):
              props = event["ResourceProperties"]
              body = {"Status": "SUCCESS", "PhysicalResourceId": "greeter-" + props["Name"], "StackId": event["StackId"],
                      "RequestId": event["RequestId"], "LogicalResourceId": event["LogicalResourceId"],
                      "Data": {"Greeting": "hello " + props["Name"]}}
              if props.get("Fail") == "yes" and event["RequestType"] != "Delete":
                  body["Status"], body["Reason"] = "FAILED", "asked to fail"
              req = urllib.request.Request(event["ResponseURL"], data=json.dumps(body).encode(), method="PUT")
              urllib.request.urlopen(req)
  NodeFn:
    Type: AWS::Lambda::Function
    Properties:
      Runtime: nodejs22.x
      Handler: index.handler
      Role: !GetAtt Role.Arn
      Code:
        ZipFile: |
          // the CDK provider framework's way: https.request with no port
          const https = require("https");
          exports.handler = async (event) => {
            const body = JSON.stringify({ Status: "SUCCESS", PhysicalResourceId: "node-" + event.LogicalResourceId,
              StackId: event.StackId, RequestId: event.RequestId, LogicalResourceId: event.LogicalResourceId, Data: { Via: "https" } });
            const u = new URL(event.ResponseURL);
            await new Promise((resolve, reject) => {
              const req = https.request({ hostname: u.hostname, path: u.pathname, method: "PUT",
                headers: { "content-type": "", "content-length": Buffer.byteLength(body) } }, resolve);
              req.on("error", reject);
              req.end(body);
            });
          };
  Greeter:
    Type: Custom::Greeter
    Properties:
      ServiceToken: !GetAtt PyFn.Arn
      Name: pogi
  ViaHttps:
    Type: AWS::CloudFormation::CustomResource
    Properties:
      ServiceToken: !GetAtt NodeFn.Arn
Outputs:
  Greeting: {Value: !GetAtt Greeter.Greeting}
  GreeterId: {Value: !Ref Greeter}
  Via: {Value: !GetAtt ViaHttps.Via}
`

func TestCustomResources(t *testing.T) {
	for _, bin := range []string{"python3", "node"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skip(bin + " not installed")
		}
	}
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	caps := []types.Capability{types.CapabilityCapabilityIam}
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("custom"), TemplateBody: aws.String(customTemplate), Capabilities: caps}))
	st := settle(t, c, "custom")
	if out := outputs(st); st.StackStatus != types.StackStatusCreateComplete || out["Greeting"] != "hello pogi" || out["GreeterId"] != "greeter-pogi" || out["Via"] != "https" {
		t.Fatalf("status %s %v: %s", st.StackStatus, out, aws.ToString(st.StackStatusReason))
	}

	failing := customTemplate + "  Broken:\n    Type: Custom::Greeter\n    Properties: {ServiceToken: !GetAtt PyFn.Arn, Name: x, Fail: \"yes\"}\n"
	failing = strings.Replace(failing, "Outputs:\n", "", 1)
	failing = strings.Replace(failing, "  Greeting: {Value: !GetAtt Greeter.Greeting}\n  GreeterId: {Value: !Ref Greeter}\n  Via: {Value: !GetAtt ViaHttps.Via}\n", "", 1)
	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("custom"), TemplateBody: aws.String(failing), Capabilities: caps}))
	if st = settle(t, c, "custom"); st.StackStatus != types.StackStatusUpdateRollbackComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	if !failed(t, c, "custom", "Broken", "asked to fail") {
		t.Fatal("the handler's failure reason is not in the stack events")
	}

	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String("custom")}))
	if st = settle(t, c, *st.StackId); st.StackStatus != types.StackStatusDeleteComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
}

// floci #5677: a failed update puts in-place changes back. Also (found by testing):
// a replaced resource is kept until the update works, then the old one is deleted.
func TestUpdateRollbackRestoresOldState(t *testing.T) {
	cfg := start(t)
	c, sm, db := cfn.NewFromConfig(cfg), ssm.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg)
	table := func(key string) string {
		return `"Tbl":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PAY_PER_REQUEST","AttributeDefinitions":[{"AttributeName":"` + key + `","AttributeType":"S"}],"KeySchema":[{"AttributeName":"` + key + `","KeyType":"HASH"}]}}`
	}
	param := func(v string) string {
		return `"P":{"Type":"AWS::SSM::Parameter","Properties":{"Name":"/rb","Type":"String","Value":"` + v + `"}}`
	}
	bad := `"Bad":{"Type":"AWS::SQS::Queue","DependsOn":["P","Tbl"],"Properties":{"QueueName":"bad name!"}}`
	tpl := func(v, key string, extra string) string {
		return `{"Resources":{` + param(v) + `,` + table(key) + extra + `}}`
	}
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("rb"), TemplateBody: aws.String(tpl("before", "id", ""))}))
	settle(t, c, "rb")
	first := *must(c.DescribeStackResource(ctx, &cfn.DescribeStackResourceInput{StackName: aws.String("rb"), LogicalResourceId: aws.String("Tbl")})).StackResourceDetail.PhysicalResourceId

	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("rb"), TemplateBody: aws.String(tpl("after", "pk", ","+bad))}))
	if st := settle(t, c, "rb"); st.StackStatus != types.StackStatusUpdateRollbackComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	if v := must(sm.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/rb")})).Parameter.Value; *v != "before" {
		t.Errorf("parameter after rollback: %s", *v)
	}
	if _, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(first)}); err != nil {
		t.Errorf("the original table is gone after rollback: %v", err)
	}
	if n := len(must(db.ListTables(ctx, &dynamodb.ListTablesInput{})).TableNames); n != 1 {
		t.Errorf("%d tables after rollback, want 1", n)
	}

	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("rb"), TemplateBody: aws.String(tpl("after", "pk", ""))}))
	if st := settle(t, c, "rb"); st.StackStatus != types.StackStatusUpdateComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	if _, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(first)}); err == nil {
		t.Errorf("the replaced table survived a good update")
	}
}

// RootResourceId must come back (found by testing), and changing a method's settings
// must update it in place instead of failing with "already exists".
func TestRestApiRootAndMethodUpdate(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := func(op string) string {
		return `{"Resources":{"Api":{"Type":"AWS::ApiGateway::RestApi","Properties":{"Name":"a"}},
"M":{"Type":"AWS::ApiGateway::Method","Properties":{"RestApiId":{"Ref":"Api"},"ResourceId":{"Fn::GetAtt":["Api","RootResourceId"]},"HttpMethod":"GET","AuthorizationType":"NONE","Integration":{"Type":"MOCK"},"OperationName":"` + op + `"}}},
"Outputs":{"R":{"Value":{"Fn::GetAtt":["Api","RootResourceId"]}}}}`
	}
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("gw"), TemplateBody: aws.String(tpl("a"))}))
	st := settle(t, c, "gw")
	if st.StackStatus != types.StackStatusCreateComplete || outputs(st)["R"] == "" {
		t.Fatalf("status %s, outputs %v: %s", st.StackStatus, outputs(st), aws.ToString(st.StackStatusReason))
	}
	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("gw"), TemplateBody: aws.String(tpl("b"))}))
	if st = settle(t, c, "gw"); st.StackStatus != types.StackStatusUpdateComplete {
		t.Fatalf("method update: %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
}

// floci #4939: when a later resource fails, a changed custom resource gets a second
// Update with the old properties (and the new ones as OldResourceProperties).
func TestRollbackUpdatesCustomResource(t *testing.T) {
	cfg := start(t)
	c, sn, sq := cfn.NewFromConfig(cfg), sns.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	// the handler is a topic that feeds a queue; the test reads the requests from it
	topic := must(sn.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("handler")}))
	q := must(sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("handler-q")}))
	arn := must(sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"}})).Attributes["QueueArn"]
	must(sn.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(arn), Attributes: map[string]string{"RawMessageDelivery": "true"}}))
	var seen []map[string]any
	var done sync.Mutex
	stop := make(chan struct{})
	defer close(stop)
	go func() { // answer every request with SUCCESS
		for {
			select {
			case <-stop:
				return
			default:
			}
			out, err := sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
			if err != nil {
				return
			}
			for _, m := range out.Messages {
				var ev map[string]any
				if json.Unmarshal([]byte(*m.Body), &ev) != nil {
					continue
				}
				done.Lock()
				seen = append(seen, ev)
				done.Unlock()
				body, _ := json.Marshal(map[string]any{"Status": "SUCCESS", "PhysicalResourceId": "p", "StackId": ev["StackId"], "RequestId": ev["RequestId"], "LogicalResourceId": ev["LogicalResourceId"]})
				req, _ := http.NewRequest("PUT", ev["ResponseURL"].(string), strings.NewReader(string(body)))
				if resp, err := http.DefaultClient.Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}
	}()
	tpl := func(v, extra string) string {
		return `{"Resources":{"C":{"Type":"Custom::Thing","Properties":{"ServiceToken":"` + *topic.TopicArn + `","V":"` + v + `"}}` + extra + `}}`
	}
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("cr"), TemplateBody: aws.String(tpl("old", ""))}))
	settle(t, c, "cr")
	bad := `,"Bad":{"Type":"AWS::SQS::Queue","DependsOn":"C","Properties":{"QueueName":"bad name!"}}`
	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("cr"), TemplateBody: aws.String(tpl("new", bad))}))
	if st := settle(t, c, "cr"); st.StackStatus != types.StackStatusUpdateRollbackComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	time.Sleep(time.Second) // the last answer may still be in flight
	done.Lock()
	defer done.Unlock()
	var updates []map[string]any
	for _, ev := range seen {
		if ev["RequestType"] == "Update" {
			updates = append(updates, ev)
		}
	}
	if len(updates) != 2 {
		t.Fatalf("handler got %d Update requests, want 2 (forward and rollback)", len(updates))
	}
	rp, _ := updates[1]["ResourceProperties"].(map[string]any)
	op, _ := updates[1]["OldResourceProperties"].(map[string]any)
	if rp["V"] != "old" || op["V"] != "new" {
		t.Errorf("rollback Update: ResourceProperties=%v OldResourceProperties=%v", rp, op)
	}
}

// floci #3097: a stack in ROLLBACK_COMPLETE can only be deleted, not updated.
func TestUpdateRefusedAfterFailedCreate(t *testing.T) {
	c := cfn.NewFromConfig(start(t))
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("rbc"), TemplateBody: aws.String(`{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"bad name!"}}}}`)}))
	if st := settle(t, c, "rbc"); st.StackStatus != types.StackStatusRollbackComplete {
		t.Fatalf("status %s", st.StackStatus)
	}
	_, err := c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("rbc"), TemplateBody: aws.String(`{"Resources":{"T":{"Type":"AWS::SNS::Topic"}}}`)})
	if err == nil || !strings.Contains(err.Error(), "ROLLBACK_COMPLETE") {
		t.Errorf("UpdateStack: %v", err)
	}
	if _, err := c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String("rbc")}); err != nil {
		t.Errorf("DeleteStack: %v", err)
	}
}

// floci #2884: Fn::Transform AWS::Include pulls the snippet in, and the processed template has no Transform node.
func TestIncludeTransform(t *testing.T) {
	cfg := start(t)
	c, sq, s3c := cfn.NewFromConfig(cfg), sqs.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("snippets")}))
	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("snippets"), Key: aws.String("q.json"), Body: strings.NewReader(`{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"included"}}}`)}))
	tpl := `{"Resources":{"Fn::Transform":{"Name":"AWS::Include","Parameters":{"Location":"s3://snippets/q.json"}},"T":{"Type":"AWS::SNS::Topic"}}}`
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("inc"), TemplateBody: aws.String(tpl)}))
	if st := settle(t, c, "inc"); st.StackStatus != types.StackStatusCreateComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	must(sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("included")}))
	g := must(c.GetTemplate(ctx, &cfn.GetTemplateInput{StackName: aws.String("inc"), TemplateStage: types.TemplateStageProcessed}))
	if body := aws.ToString(g.TemplateBody); strings.Contains(body, "Fn::Transform") || !strings.Contains(body, "included") {
		t.Errorf("processed template: %s", body)
	}
}

// floci #5265: a user gets its PermissionsBoundary, and removing it from a role or user on update removes it.
func TestPermissionsBoundary(t *testing.T) {
	cfg := start(t)
	c, i := cfn.NewFromConfig(cfg), iam.NewFromConfig(cfg)
	pol := must(i.CreatePolicy(ctx, &iam.CreatePolicyInput{PolicyName: aws.String("boundary"), PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)}))
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	tpl := func(boundary string) string {
		if boundary != "" {
			boundary = `,"PermissionsBoundary":"` + boundary + `"`
		}
		return `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"r1","AssumeRolePolicyDocument":` + trust + boundary + `}},
"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"u1"` + boundary + `}}}}`
	}
	caps := []types.Capability{types.CapabilityCapabilityNamedIam}
	has := func() (role, user bool) {
		r := must(i.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String("r1")}))
		u := must(i.GetUser(ctx, &iam.GetUserInput{UserName: aws.String("u1")}))
		return r.Role.PermissionsBoundary != nil, u.User.PermissionsBoundary != nil
	}
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("pb"), TemplateBody: aws.String(tpl(*pol.Policy.Arn)), Capabilities: caps}))
	settle(t, c, "pb")
	if r, u := has(); !r || !u {
		t.Fatalf("after create: role boundary %v, user boundary %v", r, u)
	}
	must(c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String("pb"), TemplateBody: aws.String(tpl("")), Capabilities: caps}))
	if st := settle(t, c, "pb"); st.StackStatus != types.StackStatusUpdateComplete {
		t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
	if r, u := has(); r || u {
		t.Errorf("after removal: role boundary %v, user boundary %v", r, u)
	}
}

// floci #1966: DeleteStack treats a queue that is already gone as deleted.
func TestDeleteStackWithQueueGone(t *testing.T) {
	cfg := start(t)
	c, sq := cfn.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("qg"), TemplateBody: aws.String(`{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"gone-q"}}}}`)}))
	st := settle(t, c, "qg")
	u := must(sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("gone-q")}))
	must(sq.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: u.QueueUrl}))
	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String("qg")}))
	if st = settle(t, c, *st.StackId); st.StackStatus != types.StackStatusDeleteComplete {
		t.Errorf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
}

// floci #5675 and #3947: a bucket's OwnershipControls and LifecycleConfiguration are applied
// on create and update, and removed when the template drops them.
func TestBucketOwnershipAndLifecycle(t *testing.T) {
	cfg := start(t)
	c, s3c := cfn.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	tpl := func(own string, days int) string {
		props := `"BucketName":"own-bkt"`
		if own != "" {
			props += `,"OwnershipControls":{"Rules":[{"ObjectOwnership":"` + own + `"}]}`
		}
		if days > 0 {
			props += `,"LifecycleConfiguration":{"Rules":[{"Id":"r","Status":"Enabled","ExpirationInDays":` + strconv.Itoa(days) + `,"Transitions":[{"StorageClass":"GLACIER","TransitionInDays":1}]}]}`
		}
		return `{"Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{` + props + `}}}}`
	}
	ownership := func() string {
		g, err := s3c.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("own-bkt")})
		if err != nil {
			return err.Error()
		}
		return string(g.OwnershipControls.Rules[0].ObjectOwnership)
	}
	lifecycleDays := func() int32 {
		g, err := s3c.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("own-bkt")})
		if err != nil {
			return -1
		}
		return aws.ToInt32(g.Rules[0].Expiration.Days)
	}
	step := func(own string, days int) {
		t.Helper()
		in := &cfn.UpdateStackInput{StackName: aws.String("own"), TemplateBody: aws.String(tpl(own, days))}
		must(c.UpdateStack(ctx, in))
		if st := settle(t, c, "own"); st.StackStatus != types.StackStatusUpdateComplete {
			t.Fatalf("status %s: %s", st.StackStatus, aws.ToString(st.StackStatusReason))
		}
	}
	must(c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("own"), TemplateBody: aws.String(tpl("BucketOwnerPreferred", 7))}))
	settle(t, c, "own")
	if o, d := ownership(), lifecycleDays(); o != "BucketOwnerPreferred" || d != 7 {
		t.Fatalf("after create: ownership %s, expiration days %d", o, d)
	}
	step("ObjectWriter", 9)
	if o, d := ownership(), lifecycleDays(); o != "ObjectWriter" || d != 9 {
		t.Errorf("after update: ownership %s, expiration days %d", o, d)
	}
	step("", 0)
	if o, d := ownership(), lifecycleDays(); !strings.Contains(o, "OwnershipControlsNotFoundError") || d != -1 {
		t.Errorf("after removal: ownership %s, expiration days %d", o, d)
	}
}

// floci #5670 and #4950: a DynamoDB table's TimeToLiveSpecification must be an object (else the create fails and
// leaves no table), and renaming the attribute while TTL is on fails the update, which rolls back.
func TestTableTimeToLive(t *testing.T) {
	cfg := start(t)
	c, db := cfn.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg)
	tpl := func(name, ttl string) string {
		return `{"Resources":{"T":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"` + name + `","BillingMode":"PAY_PER_REQUEST",` +
			`"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"TimeToLiveSpecification":` + ttl + `}}}}`
	}
	for i, bad := range []string{"false", `"no"`, "[]", "5"} {
		name := "bad" + strconv.Itoa(i)
		if st := deploy(t, c, name, tpl(name, bad)); st.StackStatus != types.StackStatusRollbackComplete {
			t.Errorf("TimeToLiveSpecification %s: stack %s, want ROLLBACK_COMPLETE", bad, st.StackStatus)
		}
		if _, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}); err == nil {
			t.Errorf("TimeToLiveSpecification %s left a table behind", bad)
		}
	}
	attr := func() string {
		d := must(db.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String("ttl")})).TimeToLiveDescription
		return string(d.TimeToLiveStatus) + " " + aws.ToString(d.AttributeName)
	}
	wantStatus(t, c, deploy(t, c, "ttl", tpl("ttl", `{"AttributeName":"exp","Enabled":true}`)), types.StackStatusCreateComplete)
	if got := attr(); got != "ENABLED exp" {
		t.Errorf("after create: %s", got)
	}
	wantStatus(t, c, redeploy(t, c, "ttl", tpl("ttl", `{"AttributeName":"exp2","Enabled":true}`)), types.StackStatusUpdateRollbackComplete)
	if got := attr(); got != "ENABLED exp" {
		t.Errorf("after refused rename: %s", got)
	}
	wantStatus(t, c, redeploy(t, c, "ttl", tpl("ttl", `{"AttributeName":"exp","Enabled":false}`)), types.StackStatusUpdateComplete)
	if got := attr(); got != "DISABLED " {
		t.Errorf("after Enabled false: %s", got)
	}
}
