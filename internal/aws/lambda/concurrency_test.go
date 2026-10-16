package lambda_test

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

const account, region = "000000000000", "us-east-1"

// setupSvc is setup plus the hub and the Lambda service, for tests that drive its pool.
func setupSvc(t *testing.T) (*lambda.Client, *sqs.Client, *core.Hub, *lambdasvc.Service) {
	var svc *lambdasvc.Service
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { svc = lambdasvc.New(h); return svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	return lambda.NewFromConfig(cfg), sqs.NewFromConfig(cfg), h, svc
}

// sleeper sleeps for event.sleep seconds and names the environment it ran in
// (or crashes its environment on event.exit).
func sleeper(name string) *lambda.CreateFunctionInput {
	app := "import os, time\n\ndef handler(event, context):\n    if event.get('exit'):\n        os._exit(1)\n" +
		"    time.sleep(event.get('sleep', 0))\n    return {'env': os.environ['AWS_LAMBDA_LOG_STREAM_NAME']}\n"
	return &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Timeout: aws.Int32(10), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": app})}}
}

// parallel invokes n times at once.
func parallel(c *lambda.Client, in *lambda.InvokeInput, n int) ([]*lambda.InvokeOutput, []error) {
	outs, errs := make([]*lambda.InvokeOutput, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { outs[i], errs[i] = c.Invoke(ctx, in) })
	}
	wg.Wait()
	return outs, errs
}

// TestWarmPool: parallel invocations run in their own environments; the pool
// keeps lambda.warmPool of them idle and stops idle ones after 10 minutes.
func TestWarmPool(t *testing.T) {
	need(t, "python3")
	c, _, h, svc := setupSvc(t)
	h.Cfg.WarmPool = 2
	must(c.CreateFunction(ctx, sleeper("warm")))
	outs, errs := parallel(c, &lambda.InvokeInput{FunctionName: aws.String("warm"), Payload: []byte(`{"sleep":1}`)}, 3)
	envs := map[string]bool{}
	for i, out := range outs {
		if errs[i] != nil {
			t.Fatalf("invoke: %v", errs[i])
		}
		if out.FunctionError != nil {
			t.Fatalf("invoke: %s", out.Payload)
		}
		var res struct{ Env string }
		json.Unmarshal(out.Payload, &res)
		envs[res.Env] = true
	}
	if len(envs) != 3 {
		t.Fatalf("3 parallel invokes ran in %d environments", len(envs))
	}
	if live, idle := svc.Environments(account, region, "warm"); live != 2 || idle != 2 {
		t.Fatalf("after the burst %d environments (%d idle), want the warm pool's 2", live, idle)
	}
	svc.Tick(time.Now().Add(9 * time.Minute))
	if live, _ := svc.Environments(account, region, "warm"); live != 2 {
		t.Fatalf("%d environments after 9 idle minutes, want 2", live)
	}
	svc.Tick(time.Now().Add(11 * time.Minute))
	if live, _ := svc.Environments(account, region, "warm"); live != 0 {
		t.Fatalf("%d environments after 11 idle minutes, want 0", live)
	}
}

func TestConcurrencyLimits(t *testing.T) {
	need(t, "python3")
	c, q, _, _ := setupSvc(t)
	must(c.CreateFunction(ctx, sleeper("busy")))
	must(c.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{FunctionName: aws.String("busy"), ReservedConcurrentExecutions: aws.Int32(1)}))
	acct := must(c.GetAccountSettings(ctx, &lambda.GetAccountSettingsInput{})).AccountLimit
	if acct.ConcurrentExecutions != 1000 || aws.ToInt32(acct.UnreservedConcurrentExecutions) != 999 {
		t.Fatalf("account limit %d, unreserved %d", acct.ConcurrentExecutions, aws.ToInt32(acct.UnreservedConcurrentExecutions))
	}
	throttled := func(err error) bool {
		var tm *types.TooManyRequestsException
		return errors.As(err, &tm) && tm.Reason == types.ThrottleReasonReservedFunctionConcurrentInvocationLimitExceeded &&
			aws.ToString(tm.Message) == "Rate Exceeded."
	}

	// reserved concurrency 1: of two parallel invokes, one runs and one is throttled
	_, errs := parallel(c, &lambda.InvokeInput{FunctionName: aws.String("busy"), Payload: []byte(`{"sleep":1}`)}, 2)
	if (errs[0] == nil) == (errs[1] == nil) || !throttled(errors.Join(errs...)) {
		t.Fatalf("two invokes with reserved concurrency 1: %v, %v", errs[0], errs[1])
	}

	// reserved concurrency 0 throttles every invoke
	must(c.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{FunctionName: aws.String("busy"), ReservedConcurrentExecutions: aws.Int32(0)}))
	if _, err := c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("busy")}); !throttled(err) {
		t.Fatalf("invoke with reserved concurrency 0: %v", err)
	}

	// an async event waits for concurrency instead of failing
	done := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("done")}))
	must(c.PutFunctionEventInvokeConfig(ctx, &lambda.PutFunctionEventInvokeConfigInput{FunctionName: aws.String("busy"),
		DestinationConfig: &types.DestinationConfig{OnSuccess: &types.OnSuccess{Destination: aws.String("arn:aws:sqs:us-east-1:000000000000:done")}}}))
	if res := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("busy"), InvocationType: types.InvocationTypeEvent, Payload: []byte(`{}`)})); res.StatusCode != 202 {
		t.Fatalf("async status = %d", res.StatusCode)
	}
	time.Sleep(300 * time.Millisecond) // throttled at least once
	must(c.DeleteFunctionConcurrency(ctx, &lambda.DeleteFunctionConcurrencyInput{FunctionName: aws.String("busy")}))
	got := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: done.QueueUrl, WaitTimeSeconds: 10}))
	if len(got.Messages) != 1 || !strings.Contains(*got.Messages[0].Body, `"condition":"Success"`) {
		t.Fatalf("throttled async event = %+v", got.Messages)
	}

	if acct := must(c.GetAccountSettings(ctx, &lambda.GetAccountSettingsInput{})).AccountLimit; aws.ToInt32(acct.UnreservedConcurrentExecutions) != 1000 {
		t.Fatalf("unreserved after delete = %d", aws.ToInt32(acct.UnreservedConcurrentExecutions))
	}
	_, err := c.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{FunctionName: aws.String("busy"), ReservedConcurrentExecutions: aws.Int32(950)})
	var bad *types.InvalidParameterValueException
	if !errors.As(err, &bad) {
		t.Fatalf("reserving below the 100 unreserved minimum: %v", err)
	}
}

func TestProvisionedConcurrency(t *testing.T) {
	need(t, "python3")
	c, _, _, svc := setupSvc(t)
	must(c.CreateFunction(ctx, sleeper("pc")))
	v := must(c.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("pc")}))
	must(c.CreateAlias(ctx, &lambda.CreateAliasInput{FunctionName: aws.String("pc"), Name: aws.String("live"), FunctionVersion: v.Version}))
	put := func(q string) (*lambda.PutProvisionedConcurrencyConfigOutput, error) {
		return c.PutProvisionedConcurrencyConfig(ctx, &lambda.PutProvisionedConcurrencyConfigInput{FunctionName: aws.String("pc"),
			Qualifier: aws.String(q), ProvisionedConcurrentExecutions: aws.Int32(2)})
	}
	var bad *types.InvalidParameterValueException
	if _, err := put("$LATEST"); !errors.As(err, &bad) {
		t.Fatalf("provisioning $LATEST: %v", err)
	}
	if out := must(put("live")); out.Status != types.ProvisionedConcurrencyStatusEnumInProgress {
		t.Fatalf("put status = %s", out.Status)
	}
	get := &lambda.GetProvisionedConcurrencyConfigInput{FunctionName: aws.String("pc"), Qualifier: aws.String("live")}
	for deadline := time.Now().Add(10 * time.Second); must(c.GetProvisionedConcurrencyConfig(ctx, get)).Status != types.ProvisionedConcurrencyStatusEnumReady; {
		if time.Now().After(deadline) {
			t.Fatal("provisioned concurrency never became READY")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if live, idle := svc.Environments(account, region, "pc"); live != 2 || idle != 2 {
		t.Fatalf("READY with %d environments (%d idle), want 2", live, idle)
	}
	svc.Tick(time.Now().Add(time.Hour))
	if live, _ := svc.Environments(account, region, "pc"); live != 2 {
		t.Fatalf("%d provisioned environments left after an idle hour, want 2", live)
	}
	out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("pc"), Qualifier: aws.String("live")}))
	if out.FunctionError != nil || aws.ToString(out.ExecutedVersion) != "1" {
		t.Fatalf("invoke through the alias = %s %v", out.Payload, aws.ToString(out.ExecutedVersion))
	}
	if live, _ := svc.Environments(account, region, "pc"); live != 2 {
		t.Fatalf("%d environments after an invoke, want the 2 provisioned", live)
	}
	crash := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("pc"), Qualifier: aws.String("live"), Payload: []byte(`{"exit":true}`)}))
	if crash.FunctionError == nil {
		t.Fatalf("crash = %s", crash.Payload)
	}
	svc.Tick(time.Now())
	if live, idle := svc.Environments(account, region, "pc"); live != 2 || idle != 2 {
		t.Fatalf("%d environments (%d idle) after a crash and a Tick, want 2", live, idle)
	}
	list := must(c.ListProvisionedConcurrencyConfigs(ctx, &lambda.ListProvisionedConcurrencyConfigsInput{FunctionName: aws.String("pc")})).ProvisionedConcurrencyConfigs
	if len(list) != 1 || !strings.HasSuffix(aws.ToString(list[0].FunctionArn), ":pc:live") || aws.ToInt32(list[0].AllocatedProvisionedConcurrentExecutions) != 2 {
		t.Fatalf("list = %+v", list)
	}
	must(c.DeleteProvisionedConcurrencyConfig(ctx, &lambda.DeleteProvisionedConcurrencyConfigInput{FunctionName: aws.String("pc"), Qualifier: aws.String("live")}))
	var nf *types.ProvisionedConcurrencyConfigNotFoundException
	if _, err := c.GetProvisionedConcurrencyConfig(ctx, get); !errors.As(err, &nf) {
		t.Fatalf("get after delete: %v", err)
	}
	svc.Tick(time.Now().Add(time.Hour))
	if live, _ := svc.Environments(account, region, "pc"); live != 0 {
		t.Fatalf("%d environments an idle hour after delete, want 0", live)
	}
}

func TestRuntimeImages(t *testing.T) {
	for rt, want := range map[string]string{
		"python3.9": "python:3.9", "python3.10": "python:3.10", "python3.11": "python:3.11", "python3.12": "python:3.12", "python3.13": "python:3.13",
		"nodejs18.x": "nodejs:18", "nodejs20.x": "nodejs:20", "nodejs22.x": "nodejs:22",
		"java11": "java:11", "java17": "java:17", "java21": "java:21", "dotnet8": "dotnet:8", "ruby3.2": "ruby:3.2", "ruby3.3": "ruby:3.3",
		"provided": "provided:alami", "provided.al2": "provided:al2", "provided.al2023": "provided:al2023",
	} {
		if got := lambdasvc.RuntimeImage(rt); got != "public.ecr.aws/lambda/"+want {
			t.Errorf("%s runs in %s, want public.ecr.aws/lambda/%s", rt, got, want)
		}
	}
}
