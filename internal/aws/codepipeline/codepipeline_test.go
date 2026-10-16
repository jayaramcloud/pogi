package codepipeline_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	cfnsvc "github.com/nrndev/pogi/internal/aws/cloudformation"
	codebuildsvc "github.com/nrndev/pogi/internal/aws/codebuild"
	codepipelinesvc "github.com/nrndev/pogi/internal/aws/codepipeline"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	logssvc "github.com/nrndev/pogi/internal/aws/logs"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	snssvc "github.com/nrndev/pogi/internal/aws/sns"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func zipOf(files map[string]string) []byte {
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		w, _ := zw.Create(name)
		io.WriteString(w, files[name])
	}
	zw.Close()
	return b.Bytes()
}

var role = aws.String("arn:aws:iam::000000000000:role/pipeline")

func act(name, category, provider string, config map[string]string, in, out string) types.ActionDeclaration {
	a := types.ActionDeclaration{Name: aws.String(name), Configuration: config, ActionTypeId: &types.ActionTypeId{
		Category: types.ActionCategory(category), Owner: types.ActionOwnerAws, Provider: aws.String(provider), Version: aws.String("1")}}
	if in != "" {
		a.InputArtifacts = []types.InputArtifact{{Name: aws.String(in)}}
	}
	if out != "" {
		a.OutputArtifacts = []types.OutputArtifact{{Name: aws.String(out)}}
	}
	return a
}

func stage(name string, actions ...types.ActionDeclaration) types.StageDeclaration {
	return types.StageDeclaration{Name: aws.String(name), Actions: actions}
}

func source() types.StageDeclaration {
	a := act("Fetch", "Source", "S3", map[string]string{"S3Bucket": "source", "S3ObjectKey": "app.zip", "PollForSourceChanges": "false"}, "", "SourceOutput")
	a.Namespace = aws.String("SourceVariables")
	return stage("Source", a)
}

// wait polls an execution until it ends.
func wait(t *testing.T, c *codepipeline.Client, pipeline, id string, limit time.Duration) *types.PipelineExecution {
	t.Helper()
	for end := time.Now().Add(limit); ; time.Sleep(200 * time.Millisecond) {
		x := must(c.GetPipelineExecution(ctx, &codepipeline.GetPipelineExecutionInput{PipelineName: aws.String(pipeline),
			PipelineExecutionId: aws.String(id)})).PipelineExecution
		if x.Status != types.PipelineExecutionStatusInProgress && x.Status != types.PipelineExecutionStatusStopping {
			return x
		}
		if time.Now().After(end) {
			t.Fatalf("execution %s still %s", id, x.Status)
		}
	}
}

// token waits for the approval action's token.
func token(t *testing.T, c *codepipeline.Client, pipeline string) string {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		for _, st := range must(c.GetPipelineState(ctx, &codepipeline.GetPipelineStateInput{Name: aws.String(pipeline)})).StageStates {
			for _, a := range st.ActionStates {
				if x := a.LatestExecution; *a.ActionName == "Gate" && x != nil && x.Status == types.ActionExecutionStatusInProgress && x.Token != nil {
					return *x.Token
				}
			}
		}
	}
	t.Fatal("no approval waits")
	return ""
}

// notify is a CodePipeline job worker: it reports success with output
// variables, or failure when its user parameters say so.
const notify = `import json, os, urllib.request

def call(op, body):
    urllib.request.urlopen(urllib.request.Request(os.environ["AWS_ENDPOINT_URL"], data=json.dumps(body).encode(), headers={
        "X-Amz-Target": "CodePipeline_20150709." + op, "Content-Type": "application/x-amz-json-1.1"})).read()

def handler(event, context):
    job = event["CodePipeline.job"]
    params = job["data"]["actionConfiguration"]["configuration"]["UserParameters"]
    if params == "fail":
        call("PutJobFailureResult", {"jobId": job["id"], "failureDetails": {"type": "JobFailed", "message": "asked to fail"}})
        return
    key = job["data"]["inputArtifacts"][0]["location"]["s3Location"]["objectKey"]
    call("PutJobSuccessResult", {"jobId": job["id"], "outputVariables": {"greeting": params, "artifact": key}})
`

// TestApprovalAndLambda runs Source S3 → Manual approval → Invoke Lambda,
// without Docker.
func TestApprovalAndLambda(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return codepipelinesvc.New(h) })
	cfg := awstest.Config(t, url)
	c, s3c, fn := codepipeline.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), lambda.NewFromConfig(cfg)
	for _, b := range []string{"artifacts", "source"} {
		must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("source"), Key: aws.String("app.zip"), Body: bytes.NewReader(zipOf(map[string]string{"main.txt": "v1"}))}))
	must(fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("notify"), Runtime: lambdatypes.RuntimePython312, Handler: aws.String("index.handler"),
		Role: role, Code: &lambdatypes.FunctionCode{ZipFile: zipOf(map[string]string{"index.py": notify})}}))

	invoke := act("Invoke", "Invoke", "Lambda", map[string]string{"FunctionName": "notify", "UserParameters": "hello #{SourceVariables.ObjectKey}"}, "SourceOutput", "")
	invoke.Namespace = aws.String("Notified")
	decl := &types.PipelineDeclaration{Name: aws.String("release"), RoleArn: role,
		ArtifactStore: &types.ArtifactStore{Type: types.ArtifactStoreTypeS3, Location: aws.String("artifacts")},
		Stages:        []types.StageDeclaration{source(), stage("Approve", act("Gate", "Approval", "Manual", nil, "", "")), stage("Notify", invoke)}}
	created := must(c.CreatePipeline(ctx, &codepipeline.CreatePipelineInput{Pipeline: decl, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}}}))
	if *created.Pipeline.Version != 1 || *created.Pipeline.Stages[1].Actions[0].RunOrder != 1 || created.Pipeline.PipelineType != types.PipelineTypeV1 {
		t.Fatalf("created %+v", created.Pipeline)
	}
	_, err := c.CreatePipeline(ctx, &codepipeline.CreatePipelineInput{Pipeline: decl})
	if dup := (*types.PipelineNameInUseException)(nil); !errors.As(err, &dup) {
		t.Fatalf("want PipelineNameInUseException, got %v", err)
	}
	got := must(c.GetPipeline(ctx, &codepipeline.GetPipelineInput{Name: aws.String("release")}))
	if *got.Metadata.PipelineArn != "arn:aws:codepipeline:us-east-1:000000000000:release" || got.Pipeline.Stages[2].Actions[0].Configuration["FunctionName"] != "notify" {
		t.Fatalf("get %+v %+v", got.Metadata, got.Pipeline)
	}
	if tags := must(c.ListTagsForResource(ctx, &codepipeline.ListTagsForResourceInput{ResourceArn: got.Metadata.PipelineArn})).Tags; len(tags) != 1 || *tags[0].Value != "web" {
		t.Fatalf("tags %+v", tags)
	}

	// approved: the function runs and reports back
	id := *must(c.StartPipelineExecution(ctx, &codepipeline.StartPipelineExecutionInput{Name: aws.String("release")})).PipelineExecutionId
	must(c.PutApprovalResult(ctx, &codepipeline.PutApprovalResultInput{PipelineName: aws.String("release"), StageName: aws.String("Approve"),
		ActionName: aws.String("Gate"), Token: aws.String(token(t, c, "release")), Result: &types.ApprovalResult{Status: types.ApprovalStatusApproved, Summary: aws.String("ship it")}}))
	if x := wait(t, c, "release", id, time.Minute); x.Status != types.PipelineExecutionStatusSucceeded || len(x.ArtifactRevisions) != 1 {
		t.Fatalf("approved run: %s %v", x.Status, aws.ToString(x.StatusSummary))
	}
	acts := must(c.ListActionExecutions(ctx, &codepipeline.ListActionExecutionsInput{PipelineName: aws.String("release"),
		Filter: &types.ActionExecutionFilter{PipelineExecutionId: aws.String(id)}})).ActionExecutionDetails
	if len(acts) != 3 || *acts[0].ActionName != "Invoke" || *acts[2].ActionName != "Fetch" {
		t.Fatalf("action executions %+v", acts)
	}
	out := acts[0].Output.OutputVariables
	fetched := acts[2].Output.OutputArtifacts[0].S3location
	if out["greeting"] != "hello app.zip" || out["artifact"] != *fetched.Key || !strings.HasPrefix(*fetched.Key, "release/SourceOutp/") || *fetched.Bucket != "artifacts" ||
		acts[0].Input.ResolvedConfiguration["UserParameters"] != "hello app.zip" || *acts[1].Output.ExecutionResult.ExternalExecutionSummary != "ship it" {
		t.Fatalf("outputs %+v, artifact %s, input %+v", out, *fetched.Key, acts[0].Input.ResolvedConfiguration)
	}
	if body := must(io.ReadAll(must(s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: fetched.Bucket, Key: fetched.Key})).Body)); !bytes.Equal(body, zipOf(map[string]string{"main.txt": "v1"})) {
		t.Fatal("the source artifact is not the source object")
	}

	// rejected: the stage and the execution fail; a retry of the stage waits again
	id = *must(c.StartPipelineExecution(ctx, &codepipeline.StartPipelineExecutionInput{Name: aws.String("release")})).PipelineExecutionId
	must(c.PutApprovalResult(ctx, &codepipeline.PutApprovalResultInput{PipelineName: aws.String("release"), StageName: aws.String("Approve"),
		ActionName: aws.String("Gate"), Token: aws.String(token(t, c, "release")), Result: &types.ApprovalResult{Status: types.ApprovalStatusRejected, Summary: aws.String("not today")}}))
	if x := wait(t, c, "release", id, time.Minute); x.Status != types.PipelineExecutionStatusFailed || !strings.Contains(*x.StatusSummary, "not today") {
		t.Fatalf("rejected run: %s %v", x.Status, aws.ToString(x.StatusSummary))
	}
	st := must(c.GetPipelineState(ctx, &codepipeline.GetPipelineStateInput{Name: aws.String("release")})).StageStates
	if st[1].LatestExecution.Status != types.StageExecutionStatusFailed || st[1].ActionStates[0].LatestExecution.ErrorDetails == nil {
		t.Fatalf("state %+v", st[1].LatestExecution)
	}
	must(c.RetryStageExecution(ctx, &codepipeline.RetryStageExecutionInput{PipelineName: aws.String("release"), StageName: aws.String("Approve"),
		PipelineExecutionId: aws.String(id), RetryMode: types.StageRetryModeFailedActions}))
	must(c.PutApprovalResult(ctx, &codepipeline.PutApprovalResultInput{PipelineName: aws.String("release"), StageName: aws.String("Approve"),
		ActionName: aws.String("Gate"), Token: aws.String(token(t, c, "release")), Result: &types.ApprovalResult{Status: types.ApprovalStatusApproved, Summary: aws.String("ok now")}}))
	if x := wait(t, c, "release", id, time.Minute); x.Status != types.PipelineExecutionStatusSucceeded {
		t.Fatalf("retried run: %s %v", x.Status, aws.ToString(x.StatusSummary))
	}

	// stopped while it waits
	id = *must(c.StartPipelineExecution(ctx, &codepipeline.StartPipelineExecutionInput{Name: aws.String("release")})).PipelineExecutionId
	token(t, c, "release")
	must(c.StopPipelineExecution(ctx, &codepipeline.StopPipelineExecutionInput{PipelineName: aws.String("release"), PipelineExecutionId: aws.String(id),
		Abandon: true, Reason: aws.String("cancelled")}))
	if x := wait(t, c, "release", id, time.Minute); x.Status != types.PipelineExecutionStatusStopped {
		t.Fatalf("stopped run: %s", x.Status)
	}

	// a function that reports failure fails the run
	decl.Stages[2].Actions[0].Configuration["UserParameters"] = "fail"
	if v := *must(c.UpdatePipeline(ctx, &codepipeline.UpdatePipelineInput{Pipeline: decl})).Pipeline.Version; v != 2 {
		t.Fatalf("updated version %d", v)
	}
	id = *must(c.StartPipelineExecution(ctx, &codepipeline.StartPipelineExecutionInput{Name: aws.String("release")})).PipelineExecutionId
	must(c.PutApprovalResult(ctx, &codepipeline.PutApprovalResultInput{PipelineName: aws.String("release"), StageName: aws.String("Approve"),
		ActionName: aws.String("Gate"), Token: aws.String(token(t, c, "release")), Result: &types.ApprovalResult{Status: types.ApprovalStatusApproved, Summary: aws.String("go")}}))
	if x := wait(t, c, "release", id, time.Minute); x.Status != types.PipelineExecutionStatusFailed || !strings.Contains(*x.StatusSummary, "asked to fail") {
		t.Fatalf("failing function: %s %v", x.Status, aws.ToString(x.StatusSummary))
	}
	if list := must(c.ListPipelineExecutions(ctx, &codepipeline.ListPipelineExecutionsInput{PipelineName: aws.String("release")})).PipelineExecutionSummaries; len(list) != 4 || *list[0].PipelineExecutionId != id {
		t.Fatalf("executions %+v", list)
	}
	must(c.DeletePipeline(ctx, &codepipeline.DeletePipelineInput{Name: aws.String("release")}))
	_, err = c.GetPipeline(ctx, &codepipeline.GetPipelineInput{Name: aws.String("release")})
	if nf := (*types.PipelineNotFoundException)(nil); !errors.As(err, &nf) {
		t.Fatalf("want PipelineNotFoundException, got %v", err)
	}
}

const buildspec = `version: 0.2
phases:
  build:
    commands:
      - echo "building $CODEBUILD_BUILD_ID"
      - test -f template.yaml
artifacts:
  files:
    - template.yaml
`

const template = `AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  TopicName:
    Type: String
Resources:
  Topic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Ref TopicName
Outputs:
  TopicArn:
    Value: !Ref Topic
`

// TestBuildAndDeploy is the done-when: Source S3 → CodeBuild in Docker →
// CloudFormation creating an SNS topic.
func TestBuildAndDeploy(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return logssvc.New(h) },
		func(h *core.Hub) core.Service { return codebuildsvc.New(h) },
		func(h *core.Hub) core.Service { return cfnsvc.New(h) },
		func(h *core.Hub) core.Service { return snssvc.New(h) },
		func(h *core.Hub) core.Service { return codepipelinesvc.New(h) })
	cfg := awstest.Config(t, url)
	c, s3c := codepipeline.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	for _, b := range []string{"artifacts", "source"} {
		must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("source"), Key: aws.String("app.zip"),
		Body: bytes.NewReader(zipOf(map[string]string{"buildspec.yml": buildspec, "template.yaml": template}))}))
	must(codebuild.NewFromConfig(cfg).CreateProject(ctx, &codebuild.CreateProjectInput{Name: aws.String("build"), ServiceRole: role,
		Source: &cbtypes.ProjectSource{Type: cbtypes.SourceTypeCodepipeline}, Artifacts: &cbtypes.ProjectArtifacts{Type: cbtypes.ArtifactsTypeCodepipeline},
		Environment: &cbtypes.ProjectEnvironment{Type: cbtypes.EnvironmentTypeLinuxContainer, ComputeType: cbtypes.ComputeTypeBuildGeneral1Small,
			Image: aws.String("aws/codebuild/standard:7.0")}}))

	deploy := act("Stack", "Deploy", "CloudFormation", map[string]string{"ActionMode": "CREATE_UPDATE", "StackName": "pipeline-stack",
		"TemplatePath": "BuildOutput::template.yaml", "Capabilities": "CAPABILITY_IAM", "RoleArn": *role,
		"ParameterOverrides": `{"TopicName": "from-pipeline"}`}, "BuildOutput", "")
	deploy.Namespace = aws.String("DeployVariables")
	must(c.CreatePipeline(ctx, &codepipeline.CreatePipelineInput{Pipeline: &types.PipelineDeclaration{Name: aws.String("app"), RoleArn: role,
		ArtifactStore: &types.ArtifactStore{Type: types.ArtifactStoreTypeS3, Location: aws.String("artifacts")},
		Stages: []types.StageDeclaration{source(), stage("Build", act("Build", "Build", "CodeBuild", map[string]string{"ProjectName": "build"}, "SourceOutput", "BuildOutput")),
			stage("Deploy", deploy)}}}))
	id := *must(c.StartPipelineExecution(ctx, &codepipeline.StartPipelineExecutionInput{Name: aws.String("app")})).PipelineExecutionId
	x := wait(t, c, "app", id, 5*time.Minute)
	acts := must(c.ListActionExecutions(ctx, &codepipeline.ListActionExecutionsInput{PipelineName: aws.String("app")})).ActionExecutionDetails
	if x.Status != types.PipelineExecutionStatusSucceeded {
		for _, a := range acts {
			t.Logf("%s: %s %+v", *a.ActionName, a.Status, a.Output.ExecutionResult.ErrorDetails)
		}
		t.Fatalf("execution %s: %v", x.Status, aws.ToString(x.StatusSummary))
	}
	stack := must(cloudformation.NewFromConfig(cfg).DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String("pipeline-stack")})).Stacks[0]
	if stack.StackStatus != "CREATE_COMPLETE" {
		t.Fatalf("stack %s", stack.StackStatus)
	}
	topics := must(sns.NewFromConfig(cfg).ListTopics(ctx, &sns.ListTopicsInput{})).Topics
	if len(topics) != 1 || !strings.HasSuffix(*topics[0].TopicArn, ":from-pipeline") {
		t.Fatalf("topics %+v", topics)
	}
	if arn := acts[0].Output.OutputVariables["TopicArn"]; arn != *topics[0].TopicArn || !strings.HasPrefix(*acts[1].Output.ExecutionResult.ExternalExecutionId, "build:") {
		t.Fatalf("deploy variables %+v, build %+v", acts[0].Output.OutputVariables, acts[1].Output.ExecutionResult)
	}
}

// floci #2223: a new object in an S3 source with PollForSourceChanges "true" starts an execution.
func TestS3SourcePolling(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return codepipelinesvc.New(h) })
	cfg := awstest.Config(t, url)
	c, s3c := codepipeline.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	for _, b := range []string{"artifacts", "source"} {
		must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	put := func(body string) {
		must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("source"), Key: aws.String("app.zip"), Body: bytes.NewReader(zipOf(map[string]string{"a.txt": body}))}))
	}
	put("one")
	poll := act("Fetch", "Source", "S3", map[string]string{"S3Bucket": "source", "S3ObjectKey": "app.zip", "PollForSourceChanges": "true"}, "", "SourceOutput")
	must(c.CreatePipeline(ctx, &codepipeline.CreatePipelineInput{Pipeline: &types.PipelineDeclaration{Name: aws.String("p"), RoleArn: role,
		ArtifactStore: &types.ArtifactStore{Type: types.ArtifactStoreTypeS3, Location: aws.String("artifacts")}, Stages: []types.StageDeclaration{stage("Source", poll), stage("Approve", act("Gate", "Approval", "Manual", nil, "", ""))}}}))
	count := func() int {
		return len(must(c.ListPipelineExecutions(ctx, &codepipeline.ListPipelineExecutionsInput{PipelineName: aws.String("p")})).PipelineExecutionSummaries)
	}
	time.Sleep(2500 * time.Millisecond) // the first look records the object
	if n := count(); n != 0 {
		t.Fatalf("%d executions before any change", n)
	}
	put("two")
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if count() > 0 {
			return
		}
	}
	t.Error("no execution after the source object changed")
}
