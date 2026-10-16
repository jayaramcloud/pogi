package codedeploy_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	codedeploysvc "github.com/nrndev/pogi/internal/aws/codedeploy"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
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

func pyZip(code string) []byte {
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	w, _ := zw.Create("index.py")
	io.WriteString(w, code)
	zw.Close()
	return b.Bytes()
}

// reportFailed is a lifecycle hook that tells CodeDeploy it failed.
const reportFailed = `import json, os, urllib.request

def handler(event, context):
    body = json.dumps({"deploymentId": event["DeploymentId"], "status": "Failed",
                       "lifecycleEventHookExecutionId": event["LifecycleEventHookExecutionId"]}).encode()
    urllib.request.urlopen(urllib.request.Request(os.environ["AWS_ENDPOINT_URL"], data=body, headers={
        "X-Amz-Target": "CodeDeploy_20141006.PutLifecycleEventHookExecutionStatus",
        "Content-Type": "application/x-amz-json-1.1"})).read()
`

type env struct {
	svc *codedeploysvc.Service
	cd  *codedeploy.Client
	fn  *lambda.Client
}

// setup serves CodeDeploy and Lambda with function app at versions 1 and 2,
// its alias live on 1, and a Lambda application with deployment group group.
func setup(t *testing.T, config string) env {
	var svc *codedeploysvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { svc = codedeploysvc.New(h); return svc })
	cfg := awstest.Config(t, url)
	e := env{svc, codedeploy.NewFromConfig(cfg), lambda.NewFromConfig(cfg)}
	role := aws.String("arn:aws:iam::000000000000:role/r")
	must(e.fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("app"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("index.handler"), Role: role, Code: &lambdatypes.FunctionCode{ZipFile: pyZip("def handler(e, c):\n    return 1\n")}}))
	must(e.fn.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("app")}))
	must(e.fn.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{FunctionName: aws.String("app"), ZipFile: pyZip("def handler(e, c):\n    return 2\n")}))
	if v := must(e.fn.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("app")})); *v.Version != "2" {
		t.Fatalf("version %s", *v.Version)
	}
	must(e.fn.CreateAlias(ctx, &lambda.CreateAliasInput{FunctionName: aws.String("app"), Name: aws.String("live"), FunctionVersion: aws.String("1")}))
	must(e.cd.CreateApplication(ctx, &codedeploy.CreateApplicationInput{ApplicationName: aws.String("app"), ComputePlatform: types.ComputePlatformLambda,
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}}}))
	must(e.cd.CreateDeploymentGroup(ctx, &codedeploy.CreateDeploymentGroupInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group"),
		ServiceRoleArn: role, DeploymentConfigName: aws.String(config), DeploymentStyle: &types.DeploymentStyle{
			DeploymentType: types.DeploymentTypeBlueGreen, DeploymentOption: types.DeploymentOptionWithTrafficControl}}))
	return e
}

func deploy(t *testing.T, e env, hooks string) string {
	t.Helper()
	spec := `version: 0.0
Resources:
  - app:
      Type: AWS::Lambda::Function
      Properties:
        Name: app
        Alias: live
        CurrentVersion: 1
        TargetVersion: 2
` + hooks
	return *must(e.cd.CreateDeployment(ctx, &codedeploy.CreateDeploymentInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group"),
		Revision: &types.RevisionLocation{RevisionType: types.RevisionLocationTypeAppSpecContent, AppSpecContent: &types.AppSpecContent{Content: aws.String(spec)}}})).DeploymentId
}

// alias returns the alias' version and the weight of version 2.
func alias(e env) (string, float64) {
	a := must(e.fn.GetAlias(ctx, &lambda.GetAliasInput{FunctionName: aws.String("app"), Name: aws.String("live")}))
	if a.RoutingConfig == nil {
		return *a.FunctionVersion, 0
	}
	return *a.FunctionVersion, a.RoutingConfig.AdditionalVersionWeights["2"]
}

// waitFor polls until f holds; the service's own clock drives it.
func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); !f(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func info(e env, id string) *types.DeploymentInfo {
	return must(e.cd.GetDeployment(ctx, &codedeploy.GetDeploymentInput{DeploymentId: aws.String(id)})).DeploymentInfo
}

func TestLinearShift(t *testing.T) {
	e := setup(t, "CodeDeployDefault.LambdaLinear10PercentEvery1Minute")
	id := deploy(t, e, "")
	waitFor(t, "the first 10%", func() bool { v, w := alias(e); return v == "1" && w == 0.1 })

	// fast-forward: an hour on, one step per minute
	base := time.Now().Add(time.Hour)
	for k := 1; k <= 8; k++ {
		e.svc.Tick(base.Add(time.Duration(k) * time.Minute))
		if v, w := alias(e); v != "1" || math.Abs(w-float64(k+1)/10) > 1e-9 {
			t.Fatalf("after %d more minutes: version %s, weight %v", k, v, w)
		}
		e.svc.Tick(base.Add(time.Duration(k)*time.Minute + 30*time.Second))
		if _, w := alias(e); math.Abs(w-float64(k+1)/10) > 1e-9 {
			t.Fatalf("half a minute later the weight moved to %v", w)
		}
	}
	if d := info(e, id); d.Status != types.DeploymentStatusInProgress {
		t.Fatalf("at 90%%: %s", d.Status)
	}
	tgt := must(e.cd.GetDeploymentTarget(ctx, &codedeploy.GetDeploymentTargetInput{DeploymentId: aws.String(id), TargetId: aws.String("app:live")})).DeploymentTarget.LambdaTarget
	if tgt.Status != types.TargetStatusInProgress || math.Abs(tgt.LambdaFunctionInfo.TargetVersionWeight-0.9) > 1e-9 || *tgt.LambdaFunctionInfo.CurrentVersion != "1" {
		t.Fatalf("target %+v %+v", tgt, tgt.LambdaFunctionInfo)
	}
	e.svc.Tick(base.Add(9 * time.Minute))
	if v, w := alias(e); v != "2" || w != 0 {
		t.Fatalf("at the end the alias is on %s with weight %v", v, w)
	}
	d := info(e, id)
	if d.Status != types.DeploymentStatusSucceeded || d.CompleteTime == nil || d.DeploymentOverview.Succeeded != 1 || d.ComputePlatform != types.ComputePlatformLambda {
		t.Fatalf("deployment %+v", d)
	}
	tgt = must(e.cd.BatchGetDeploymentTargets(ctx, &codedeploy.BatchGetDeploymentTargetsInput{DeploymentId: aws.String(id),
		TargetIds: []string{"app:live"}})).DeploymentTargets[0].LambdaTarget
	var events []string
	for _, ev := range tgt.LifecycleEvents {
		events = append(events, *ev.LifecycleEventName+":"+string(ev.Status))
	}
	if got := events; len(got) != 3 || got[0] != "BeforeAllowTraffic:Skipped" || got[1] != "AllowTraffic:Succeeded" || got[2] != "AfterAllowTraffic:Skipped" {
		t.Fatalf("lifecycle events %v", got)
	}

	// StopDeployment with rollback puts the alias back
	must(e.fn.UpdateAlias(ctx, &lambda.UpdateAliasInput{FunctionName: aws.String("app"), Name: aws.String("live"), FunctionVersion: aws.String("1")}))
	id = deploy(t, e, "")
	waitFor(t, "the second deployment's 10%", func() bool { _, w := alias(e); return w == 0.1 })
	must(e.cd.StopDeployment(ctx, &codedeploy.StopDeploymentInput{DeploymentId: aws.String(id), AutoRollbackEnabled: aws.Bool(true)}))
	if v, w := alias(e); v != "1" || w != 0 {
		t.Fatalf("after the stop the alias is on %s with weight %v", v, w)
	}
	if d := info(e, id); d.Status != types.DeploymentStatusStopped || d.RollbackInfo == nil || d.ErrorInformation.Code != types.ErrorCodeManualStop {
		t.Fatalf("stopped %+v", d)
	}
	_, err := e.cd.StopDeployment(ctx, &codedeploy.StopDeploymentInput{DeploymentId: aws.String(id)})
	if done := (*types.DeploymentAlreadyCompletedException)(nil); !errors.As(err, &done) {
		t.Fatalf("want DeploymentAlreadyCompletedException, got %v", err)
	}
	if ids := must(e.cd.ListDeployments(ctx, &codedeploy.ListDeploymentsInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group")})).Deployments; len(ids) != 2 || ids[0] != id {
		t.Fatalf("deployments newest first %v", ids)
	}
}

func TestHookRollback(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	e := setup(t, "CodeDeployDefault.LambdaCanary10Percent5Minutes")
	must(e.fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("pre-traffic"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("index.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &lambdatypes.FunctionCode{ZipFile: pyZip(reportFailed)}}))
	id := deploy(t, e, "Hooks:\n  - BeforeAllowTraffic: pre-traffic\n")
	waitFor(t, "the deployment to end", func() bool {
		s := info(e, id).Status
		return s == types.DeploymentStatusFailed || s == types.DeploymentStatusSucceeded
	})
	d := info(e, id)
	if d.Status != types.DeploymentStatusFailed || d.ErrorInformation.Code != types.ErrorCodeHookExecutionFailure || d.RollbackInfo == nil {
		t.Fatalf("deployment %+v %+v", d, d.ErrorInformation)
	}
	if v, w := alias(e); v != "1" || w != 0 {
		t.Fatalf("after the rollback the alias is on %s with weight %v", v, w)
	}
	tgt := must(e.cd.GetDeploymentTarget(ctx, &codedeploy.GetDeploymentTargetInput{DeploymentId: aws.String(id), TargetId: aws.String("app:live")})).DeploymentTarget.LambdaTarget
	if tgt.Status != types.TargetStatusFailed || tgt.LifecycleEvents[0].Status != types.LifecycleEventStatusFailed ||
		tgt.LifecycleEvents[1].Status != types.LifecycleEventStatusSkipped || *tgt.LifecycleEvents[0].Diagnostics.ScriptName != "pre-traffic" {
		t.Fatalf("target %+v", tgt.LifecycleEvents)
	}
}

func TestConfigsAndGroups(t *testing.T) {
	e := setup(t, "CodeDeployDefault.LambdaAllAtOnce")
	_, err := e.cd.CreateApplication(ctx, &codedeploy.CreateApplicationInput{ApplicationName: aws.String("app")})
	if dup := (*types.ApplicationAlreadyExistsException)(nil); !errors.As(err, &dup) {
		t.Fatalf("want ApplicationAlreadyExistsException, got %v", err)
	}
	app := must(e.cd.GetApplication(ctx, &codedeploy.GetApplicationInput{ApplicationName: aws.String("app")})).Application
	if app.ComputePlatform != types.ComputePlatformLambda || *app.ApplicationId == "" {
		t.Fatalf("application %+v", app)
	}
	tags := must(e.cd.ListTagsForResource(ctx, &codedeploy.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:codedeploy:us-east-1:000000000000:application:app")})).Tags
	if len(tags) != 1 || *tags[0].Value != "web" {
		t.Fatalf("tags %+v", tags)
	}
	g := must(e.cd.GetDeploymentGroup(ctx, &codedeploy.GetDeploymentGroupInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group")})).DeploymentGroupInfo
	if g.ComputePlatform != types.ComputePlatformLambda || *g.DeploymentConfigName != "CodeDeployDefault.LambdaAllAtOnce" || g.DeploymentStyle.DeploymentType != types.DeploymentTypeBlueGreen ||
		g.OutdatedInstancesStrategy != types.OutdatedInstancesStrategyUpdate || *g.ServiceRoleArn == "" {
		t.Fatalf("group %+v", g)
	}
	_, err = e.cd.GetDeploymentGroup(ctx, &codedeploy.GetDeploymentGroupInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("nope")})
	if nf := (*types.DeploymentGroupDoesNotExistException)(nil); !errors.As(err, &nf) {
		t.Fatalf("want DeploymentGroupDoesNotExistException, got %v", err)
	}

	cfg := &types.TrafficRoutingConfig{Type: types.TrafficRoutingTypeTimeBasedCanary, TimeBasedCanary: &types.TimeBasedCanary{CanaryPercentage: 25, CanaryInterval: 2}}
	must(e.cd.CreateDeploymentConfig(ctx, &codedeploy.CreateDeploymentConfigInput{DeploymentConfigName: aws.String("quarter"), ComputePlatform: types.ComputePlatformLambda, TrafficRoutingConfig: cfg}))
	got := must(e.cd.GetDeploymentConfig(ctx, &codedeploy.GetDeploymentConfigInput{DeploymentConfigName: aws.String("quarter")})).DeploymentConfigInfo
	if got.TrafficRoutingConfig.TimeBasedCanary.CanaryPercentage != 25 || got.ComputePlatform != types.ComputePlatformLambda {
		t.Fatalf("config %+v", got)
	}
	pre := must(e.cd.GetDeploymentConfig(ctx, &codedeploy.GetDeploymentConfigInput{DeploymentConfigName: aws.String("CodeDeployDefault.LambdaLinear10PercentEvery3Minutes")})).DeploymentConfigInfo
	if pre.TrafficRoutingConfig.TimeBasedLinear.LinearInterval != 3 {
		t.Fatalf("predefined %+v", pre.TrafficRoutingConfig)
	}
	if names := must(e.cd.ListDeploymentConfigs(ctx, &codedeploy.ListDeploymentConfigsInput{})).DeploymentConfigsList; len(names) != 18 || names[len(names)-1] != "quarter" {
		t.Fatalf("configs %v", names)
	}
	_, err = e.cd.DeleteDeploymentConfig(ctx, &codedeploy.DeleteDeploymentConfigInput{DeploymentConfigName: aws.String("CodeDeployDefault.AllAtOnce")})
	if op := (*types.InvalidOperationException)(nil); !errors.As(err, &op) {
		t.Fatalf("want InvalidOperationException, got %v", err)
	}

	// an all-at-once deployment moves the alias in one tick
	id := deploy(t, e, "")
	e.svc.Tick(time.Now())
	if v, w := alias(e); v != "2" || w != 0 || info(e, id).Status != types.DeploymentStatusSucceeded {
		t.Fatalf("all at once: %s %v %s", v, w, info(e, id).Status)
	}
	must(e.cd.DeleteDeploymentGroup(ctx, &codedeploy.DeleteDeploymentGroupInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group")}))
	must(e.cd.DeleteApplication(ctx, &codedeploy.DeleteApplicationInput{ApplicationName: aws.String("app")}))
	if apps := must(e.cd.ListApplications(ctx, &codedeploy.ListApplicationsInput{})).Applications; len(apps) != 0 {
		t.Fatalf("applications %v", apps)
	}
}
