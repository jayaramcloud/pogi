package stepfunctions_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/aws/stepfunctions"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

const role = "arn:aws:iam::000000000000:role/sfn"

func init() {
	stepfunctions.RetrySpeedup = 1000 // 1s backoff becomes 1ms
	stepfunctions.MaxWait = 5 * time.Second
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	sfn    *sfn.Client
	sqs    *sqs.Client
	lambda *lambda.Client
	hub    *core.Hub
	svc    *stepfunctions.Service
}

// noHostPrefix keeps StartSyncExecution and TestState on the endpoint host instead of sync-<host>.
func noHostPrefix(o *sfn.Options) {
	o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
		return s.Initialize.Add(middleware.InitializeMiddlewareFunc("noHostPrefix",
			func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				return next.HandleInitialize(smithyhttp.DisableEndpointHostPrefix(ctx, true), in)
			}), middleware.Before)
	})
}

func setup(t *testing.T) *env {
	var svc *stepfunctions.Service
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { svc = stepfunctions.New(h); return svc },
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	return &env{sfn.NewFromConfig(cfg, noHostPrefix), sqs.NewFromConfig(cfg), lambda.NewFromConfig(cfg), h, svc}
}

func (e *env) machine(t *testing.T, name, def string, typ ...types.StateMachineType) string {
	t.Helper()
	in := &sfn.CreateStateMachineInput{Name: aws.String(name), Definition: aws.String(def), RoleArn: aws.String(role)}
	if len(typ) > 0 {
		in.Type = typ[0]
	}
	out, err := e.sfn.CreateStateMachine(ctx, in)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return *out.StateMachineArn
}

func (e *env) start(t *testing.T, sm, input string) string {
	t.Helper()
	out, err := e.sfn.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: aws.String(sm), Input: aws.String(input)})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return *out.ExecutionArn
}

// wait polls DescribeExecution until the execution leaves RUNNING.
func (e *env) wait(t *testing.T, arn string) *sfn.DescribeExecutionOutput {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		d := must(e.sfn.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: aws.String(arn)}))
		if d.Status != types.ExecutionStatusRunning {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution %s still running", arn)
		}
	}
}

func (e *env) history(t *testing.T, arn string) []types.HistoryEvent {
	t.Helper()
	return must(e.sfn.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{ExecutionArn: aws.String(arn), MaxResults: 1000})).Events
}

func count(events []types.HistoryEvent, typ types.HistoryEventType) int {
	n := 0
	for _, ev := range events {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

func jsonEq(t *testing.T, got string, want any) {
	t.Helper()
	var g, w any
	json.Unmarshal([]byte(got), &g)
	b, _ := json.Marshal(want)
	json.Unmarshal(b, &w)
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("got  %s\nwant %s", got, b)
	}
}

const flow = `{
  "Comment": "paths, intrinsics, Choice and Parallel",
  "StartAt": "Prep",
  "States": {
    "Prep": {
      "Type": "Pass",
      "InputPath": "$.order",
      "Parameters": {
        "id.$": "$.id",
        "greeting.$": "States.Format('Hello {}, you owe {}', $.customer, $.total)",
        "escaped.$": "States.Format('\\{\\} and it\\'s {}', $.customer)",
        "skus.$": "$.items[*].sku",
        "first.$": "$['items'][0].sku",
        "last.$": "$.items[-1].sku",
        "count.$": "States.ArrayLength($.items)",
        "parts.$": "States.ArrayPartition(States.Array(1, 2, 3, 4, 5), 2)",
        "range.$": "States.ArrayRange(1, 9, 2)",
        "vip.$": "States.ArrayContains($.tags, 'vip')",
        "uniq.$": "States.ArrayUnique(States.Array(1, 1, 'a', 'a'))",
        "second.$": "States.ArrayGetItem($.tags, 1)",
        "b64.$": "States.Base64Encode('pogi')",
        "plain.$": "States.Base64Decode('cG9naQ==')",
        "hash.$": "States.Hash('pogi', 'SHA-256')",
        "merged.$": "States.JsonMerge($.a, $.b, false)",
        "str.$": "States.JsonToString($.a)",
        "obj.$": "States.StringToJson('\\{\"x\":1\\}')",
        "sum.$": "States.MathAdd($.total, -4)",
        "dice.$": "States.MathRandom(1, 6)",
        "words.$": "States.StringSplit('a,b;c', ',;')",
        "uuid.$": "States.UUID()",
        "exec.$": "$$.Execution.Name",
        "state.$": "$$.State.Name",
        "machine.$": "$$.StateMachine.Name",
        "nested": {"static": true, "who.$": "$.customer"}
      },
      "ResultPath": "$.prepared",
      "Next": "Route"
    },
    "Route": {
      "Type": "Choice",
      "Choices": [
        {"Variable": "$.prepared.id", "StringMatches": "zz*", "Next": "Wrong"},
        {"And": [
          {"Variable": "$.prepared.count", "NumericGreaterThanEquals": 3},
          {"Variable": "$.prepared.vip", "BooleanEquals": true},
          {"Variable": "$.order.id", "StringEqualsPath": "$.expected"},
          {"Variable": "$.order.id", "StringMatches": "o-*"},
          {"Variable": "$.missing", "IsPresent": false},
          {"Variable": "$.order.note", "IsNull": true},
          {"Variable": "$.when", "IsTimestamp": true},
          {"Variable": "$.when", "TimestampLessThan": "2030-01-01T00:00:00Z"},
          {"Or": [{"Variable": "$.order.total", "NumericEquals": 1}, {"Variable": "$.order.total", "IsNumeric": true}]},
          {"Not": {"Variable": "$.order.customer", "StringEquals": "bob"}}
        ], "Next": "Fan"}
      ],
      "Default": "Wrong"
    },
    "Fan": {
      "Type": "Parallel",
      "Parameters": {"greeting.$": "$.prepared.greeting"},
      "Branches": [
        {"StartAt": "One", "States": {"One": {"Type": "Pass", "Result": {"n": 1}, "End": true}}},
        {"StartAt": "Two", "States": {"Two": {"Type": "Pass", "OutputPath": "$.greeting", "End": true}}}
      ],
      "ResultSelector": {"n.$": "$[0].n", "greeting.$": "$[1]"},
      "ResultPath": "$.prepared.fan",
      "Next": "Done"
    },
    "Done": {"Type": "Succeed", "OutputPath": "$.prepared"},
    "Wrong": {"Type": "Fail", "Error": "Wrong", "Cause": "took the wrong branch"}
  }
}`

const flowInput = `{"order": {"id": "o-1", "customer": "ana", "total": 34, "items": [{"sku": "a"}, {"sku": "b"}, {"sku": "c"}],
  "tags": ["new", "vip"], "a": {"x": 1, "y": 2}, "b": {"y": 3}, "note": null}, "expected": "o-1", "when": "2026-01-02T03:04:05Z"}`

func TestFlowIntrinsicsAndHistory(t *testing.T) {
	e := setup(t)
	sm := e.machine(t, "flow", flow)
	arn := e.start(t, sm, flowInput)
	d := e.wait(t, arn)
	if d.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("status %s: %s %s", d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
	}
	var out map[string]any
	json.Unmarshal([]byte(*d.Output), &out)
	if dice := out["dice"].(float64); dice < 1 || dice > 6 || len(out["uuid"].(string)) != 36 || !strings.HasSuffix(arn, ":"+out["exec"].(string)) {
		t.Fatalf("dice/uuid/exec = %v %v %v", out["dice"], out["uuid"], out["exec"])
	}
	delete(out, "dice")
	delete(out, "uuid")
	delete(out, "exec")
	sum := sha256.Sum256([]byte("pogi"))
	jsonEq(t, string(must(json.Marshal(out))), map[string]any{
		"id": "o-1", "greeting": "Hello ana, you owe 34", "escaped": "{} and it's ana", "skus": []string{"a", "b", "c"},
		"first": "a", "last": "c", "count": 3, "parts": [][]int{{1, 2}, {3, 4}, {5}}, "range": []int{1, 3, 5, 7, 9},
		"vip": true, "uniq": []any{1, "a"}, "second": "vip", "b64": "cG9naQ==", "plain": "pogi", "hash": hex.EncodeToString(sum[:]),
		"merged": map[string]int{"x": 1, "y": 3}, "str": `{"x":1,"y":2}`, "obj": map[string]int{"x": 1}, "sum": 30,
		"words": []string{"a", "b", "c"}, "state": "Prep", "machine": "flow",
		"nested": map[string]any{"static": true, "who": "ana"}, "fan": map[string]any{"n": 1, "greeting": "Hello ana, you owe 34"},
	})

	events := e.history(t, arn)
	if len(events) != 16 || events[0].Type != types.HistoryEventTypeExecutionStarted || events[0].PreviousEventId != 0 ||
		aws.ToString(events[0].ExecutionStartedEventDetails.Input) != flowInput || events[15].Type != types.HistoryEventTypeExecutionSucceeded {
		t.Fatalf("history = %d events, first %s", len(events), events[0].Type)
	}
	for i, ev := range events {
		if ev.Id != int64(i+1) || ev.Timestamp == nil {
			t.Fatalf("event %d has id %d", i, ev.Id)
		}
	}
	for _, typ := range []types.HistoryEventType{"PassStateEntered", "PassStateExited", "ChoiceStateEntered", "ChoiceStateExited",
		"ParallelStateEntered", "ParallelStateStarted", "ParallelStateSucceeded", "ParallelStateExited", "SucceedStateEntered", "SucceedStateExited"} {
		if count(events, typ) == 0 {
			t.Fatalf("no %s event", typ)
		}
	}
	if ev := events[3]; ev.Type != "ChoiceStateEntered" || aws.ToString(ev.StateEnteredEventDetails.Name) != "Route" {
		t.Fatalf("event 4 = %s %+v", ev.Type, ev.StateEnteredEventDetails)
	}
	rev := must(e.sfn.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{ExecutionArn: aws.String(arn), ReverseOrder: true, MaxResults: 5}))
	if len(rev.Events) != 5 || rev.Events[0].Type != types.HistoryEventTypeExecutionSucceeded || rev.NextToken == nil ||
		!strings.Contains(aws.ToString(rev.Events[0].ExecutionSucceededEventDetails.Output), `"fan"`) {
		t.Fatalf("reverse page = %d events, next %v", len(rev.Events), rev.NextToken)
	}
	next := must(e.sfn.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{ExecutionArn: aws.String(arn), ReverseOrder: true, MaxResults: 5, NextToken: rev.NextToken}))
	if next.Events[0].Id != 11 {
		t.Fatalf("second page starts at %d", next.Events[0].Id)
	}
}

const calc = `
def handler(event, context):
    if event.get("fail"):
        raise ValueError("boom")
    if event.get("retry", 99) < 2:
        raise ValueError("flaky")
    return {"double": event.get("n", 0) * 2, "retry": event.get("retry")}
`

const lambdaFlow = `{
  "StartAt": "Flaky",
  "States": {
    "Flaky": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {"FunctionName": "calc", "Payload": {"retry.$": "$$.State.RetryCount", "n.$": "$.n"}},
      "ResultSelector": {"double.$": "$.Payload.double", "retries.$": "$.Payload.retry", "status.$": "$.StatusCode"},
      "ResultPath": "$.flaky",
      "Retry": [{"ErrorEquals": ["ValueError"], "IntervalSeconds": 1, "MaxAttempts": 3, "BackoffRate": 2}],
      "Next": "Broken"
    },
    "Broken": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:000000000000:function:calc",
      "Parameters": {"fail": true},
      "Retry": [{"ErrorEquals": ["States.Timeout"], "MaxAttempts": 5}, {"ErrorEquals": ["States.ALL"], "IntervalSeconds": 1, "MaxAttempts": 1}],
      "Catch": [{"ErrorEquals": ["ValueError"], "ResultPath": "$.error", "Next": "Items"}],
      "Next": "Items"
    },
    "Items": {
      "Type": "Map",
      "ItemsPath": "$.list",
      "MaxConcurrency": 2,
      "ItemSelector": {"n.$": "$$.Map.Item.Value", "i.$": "$$.Map.Item.Index"},
      "ItemProcessor": {
        "ProcessorConfig": {"Mode": "INLINE"},
        "StartAt": "Double",
        "States": {"Double": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:000000000000:function:calc", "OutputPath": "$.double", "End": true}}
      },
      "ResultPath": "$.doubled",
      "End": true
    }
  }
}`

func zipFiles(files map[string]string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range files {
		w, _ := zw.Create(name)
		io.WriteString(w, body)
	}
	zw.Close()
	return buf.Bytes()
}

func TestLambdaRetryCatchAndMap(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	e := setup(t)
	must(e.lambda.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("calc"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String(role), Code: &lambdatypes.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": calc})}}))
	arn := e.start(t, e.machine(t, "calc", lambdaFlow), `{"n": 21, "list": [1, 2, 3]}`)
	d := e.wait(t, arn)
	if d.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("status %s: %s %s", d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
	}
	var out map[string]any
	json.Unmarshal([]byte(*d.Output), &out)
	caught, _ := out["error"].(map[string]any)
	if caught["Error"] != "ValueError" || !strings.Contains(caught["Cause"].(string), "boom") {
		t.Fatalf("caught = %v", out["error"])
	}
	delete(out, "error")
	jsonEq(t, string(must(json.Marshal(out))), map[string]any{"n": 21, "list": []int{1, 2, 3},
		"flaky": map[string]any{"double": 42, "retries": 2, "status": 200}, "doubled": []int{2, 4, 6}})

	events := e.history(t, arn)
	for typ, want := range map[types.HistoryEventType]int{"TaskFailed": 2, "TaskSucceeded": 1, "LambdaFunctionFailed": 2,
		"LambdaFunctionSucceeded": 3, "MapStateStarted": 1, "MapIterationStarted": 3, "MapIterationSucceeded": 3, "MapStateSucceeded": 1} {
		if got := count(events, typ); got != want {
			t.Fatalf("%d %s events, want %d", got, typ, want)
		}
	}
	for _, ev := range events {
		switch ev.Type {
		case "MapStateStarted":
			if ev.MapStateStartedEventDetails.Length != 3 {
				t.Fatalf("map length = %d", ev.MapStateStartedEventDetails.Length)
			}
		case "TaskScheduled":
			if aws.ToString(ev.TaskScheduledEventDetails.ResourceType) != "lambda" || aws.ToString(ev.TaskScheduledEventDetails.Resource) != "invoke" {
				t.Fatalf("scheduled = %+v", ev.TaskScheduledEventDetails)
			}
		case "LambdaFunctionFailed":
			if aws.ToString(ev.LambdaFunctionFailedEventDetails.Error) != "ValueError" {
				t.Fatalf("lambda failure = %+v", ev.LambdaFunctionFailedEventDetails)
			}
		}
	}
}

func TestSQSIntegrationAndCallback(t *testing.T) {
	e := setup(t)
	q := *must(e.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})).QueueUrl
	sm := e.machine(t, "approve", `{
	  "StartAt": "Send",
	  "States": {
	    "Send": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
	             "Parameters": {"QueueUrl": "`+q+`", "MessageBody.$": "$"},
	             "ResultSelector": {"id.$": "$.MessageId"}, "ResultPath": "$.sent", "Next": "Lookup"},
	    "Lookup": {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:sqs:getQueueUrl", "Parameters": {"QueueName": "nope"},
	             "Catch": [{"ErrorEquals": ["Sqs.QueueDoesNotExistException"], "ResultPath": "$.lookup", "Next": "Ask"}], "Next": "Ask"},
	    "Ask": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
	            "Parameters": {"QueueUrl": "`+q+`", "MessageBody": {"token.$": "$$.Task.Token", "order.$": "$.order"}},
	            "TimeoutSeconds": 30, "ResultPath": "$.answer", "End": true}
	  }
	}`)
	recv := func() string {
		m := must(e.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(q), WaitTimeSeconds: 5}))
		if len(m.Messages) != 1 {
			t.Fatal("no message")
		}
		e.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(q), ReceiptHandle: m.Messages[0].ReceiptHandle})
		return *m.Messages[0].Body
	}
	token := func() string {
		var body struct {
			Token string
			Order int
		}
		json.Unmarshal([]byte(recv()), &body)
		if body.Token == "" || body.Order != 7 {
			t.Fatalf("callback message = %+v", body)
		}
		return body.Token
	}

	arn := e.start(t, sm, `{"order": 7}`)
	if body := recv(); body != `{"order":7}` {
		t.Fatalf("first message = %s", body)
	}
	tok := token()
	must(e.sfn.SendTaskHeartbeat(ctx, &sfn.SendTaskHeartbeatInput{TaskToken: aws.String(tok)}))
	must(e.sfn.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: aws.String(tok), Output: aws.String(`{"approved": true}`)}))
	d := e.wait(t, arn)
	var out struct {
		Sent   struct{ Id string }
		Lookup struct{ Error string }
		Answer map[string]any
	}
	json.Unmarshal([]byte(aws.ToString(d.Output)), &out)
	if d.Status != types.ExecutionStatusSucceeded || out.Sent.Id == "" || out.Lookup.Error != "Sqs.QueueDoesNotExistException" || out.Answer["approved"] != true {
		t.Fatalf("status %s output %s", d.Status, aws.ToString(d.Output))
	}
	events := e.history(t, arn)
	if count(events, "TaskSubmitted") != 1 || count(events, "TaskSucceeded") != 2 || count(events, "TaskFailed") != 1 {
		t.Fatalf("history types wrong: %d submitted", count(events, "TaskSubmitted"))
	}

	_, err := e.sfn.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: aws.String(tok), Output: aws.String(`{}`)})
	var gone *types.TaskDoesNotExist
	if !errors.As(err, &gone) {
		t.Fatalf("reused token: %v", err)
	}
	_, err = e.sfn.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: aws.String("bogus"), Output: aws.String(`{}`)})
	var invalid *types.InvalidToken
	if !errors.As(err, &invalid) {
		t.Fatalf("bogus token: %v", err)
	}

	arn = e.start(t, sm, `{"order": 7}`)
	recv()
	must(e.sfn.SendTaskFailure(ctx, &sfn.SendTaskFailureInput{TaskToken: aws.String(token()), Error: aws.String("Denied"), Cause: aws.String("no budget")}))
	if d := e.wait(t, arn); d.Status != types.ExecutionStatusFailed || aws.ToString(d.Error) != "Denied" || aws.ToString(d.Cause) != "no budget" {
		t.Fatalf("failure = %s %v %v", d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
	}
}

func TestActivityWaitAndStop(t *testing.T) {
	e := setup(t)
	act := *must(e.sfn.CreateActivity(ctx, &sfn.CreateActivityInput{Name: aws.String("review")})).ActivityArn
	if l := must(e.sfn.ListActivities(ctx, &sfn.ListActivitiesInput{})); len(l.Activities) != 1 || *l.Activities[0].Name != "review" {
		t.Fatalf("activities = %+v", l.Activities)
	}
	sm := e.machine(t, "work", `{"StartAt": "Review", "States": {"Review": {"Type": "Task", "Resource": "`+act+`", "TimeoutSeconds": 30, "End": true}}}`)
	arn := e.start(t, sm, `{"job": 1}`)
	task := must(e.sfn.GetActivityTask(ctx, &sfn.GetActivityTaskInput{ActivityArn: aws.String(act), WorkerName: aws.String("w1")}))
	if aws.ToString(task.Input) != `{"job":1}` || task.TaskToken == nil {
		t.Fatalf("activity task = %+v", task)
	}
	must(e.sfn.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: task.TaskToken, Output: aws.String(`{"done": true}`)}))
	if d := e.wait(t, arn); d.Status != types.ExecutionStatusSucceeded || aws.ToString(d.Output) != `{"done":true}` {
		t.Fatalf("activity execution = %s %s", d.Status, aws.ToString(d.Output))
	}
	events := e.history(t, arn)
	if count(events, "ActivityScheduled") != 1 || count(events, "ActivitySucceeded") != 1 || count(events, "ActivityStarted") != 1 {
		t.Fatalf("activity history wrong")
	}
	for _, ev := range events {
		if ev.Type == "ActivityStarted" && aws.ToString(ev.ActivityStartedEventDetails.WorkerName) != "w1" {
			t.Fatalf("worker = %+v", ev.ActivityStartedEventDetails)
		}
	}

	nap := e.machine(t, "nap", `{"StartAt": "Nap", "States": {"Nap": {"Type": "Wait", "SecondsPath": "$.secs", "Next": "Done"}, "Done": {"Type": "Succeed"}}}`)
	begin := time.Now()
	if d := e.wait(t, e.start(t, nap, `{"secs": 1}`)); d.Status != types.ExecutionStatusSucceeded || time.Since(begin) < 900*time.Millisecond {
		t.Fatalf("wait = %s after %v", d.Status, time.Since(begin))
	}

	long := e.start(t, nap, `{"secs": 100}`)
	time.Sleep(50 * time.Millisecond)
	must(e.sfn.StopExecution(ctx, &sfn.StopExecutionInput{ExecutionArn: aws.String(long), Error: aws.String("Halt"), Cause: aws.String("testing")}))
	d := e.wait(t, long)
	if d.Status != types.ExecutionStatusAborted || aws.ToString(d.Error) != "Halt" || aws.ToString(d.Cause) != "testing" || d.StopDate == nil {
		t.Fatalf("stopped = %s %v %v", d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
	}
	time.Sleep(50 * time.Millisecond) // the interpreter must not write after the abort
	events = e.history(t, long)
	if last := events[len(events)-1]; last.Type != types.HistoryEventTypeExecutionAborted || aws.ToString(last.ExecutionAbortedEventDetails.Error) != "Halt" {
		t.Fatalf("last event = %s", last.Type)
	}
	if l := must(e.sfn.ListExecutions(ctx, &sfn.ListExecutionsInput{StateMachineArn: aws.String(nap), StatusFilter: types.ExecutionStatusAborted})); len(l.Executions) != 1 {
		t.Fatalf("aborted executions = %d", len(l.Executions))
	}
}

func TestExpressSyncAndChildExecutions(t *testing.T) {
	e := setup(t)
	fast := e.machine(t, "fast", `{"StartAt": "Hi", "States": {"Hi": {"Type": "Pass", "Parameters": {"msg.$": "States.Format('hi {}', $.who)"}, "End": true}}}`,
		types.StateMachineTypeExpress)
	out := must(e.sfn.StartSyncExecution(ctx, &sfn.StartSyncExecutionInput{StateMachineArn: aws.String(fast), Input: aws.String(`{"who": "pogi"}`)}))
	if out.Status != types.SyncExecutionStatusSucceeded || aws.ToString(out.Output) != `{"msg":"hi pogi"}` || out.StopDate == nil || out.BillingDetails == nil {
		t.Fatalf("sync = %s %s", out.Status, aws.ToString(out.Output))
	}
	bad := e.machine(t, "bad", `{"StartAt": "No", "States": {"No": {"Type": "Fail", "ErrorPath": "$.code", "CausePath": "States.Format('bad {}', $.what)"}}}`,
		types.StateMachineTypeExpress)
	out = must(e.sfn.StartSyncExecution(ctx, &sfn.StartSyncExecutionInput{StateMachineArn: aws.String(bad), Input: aws.String(`{"code": "E42", "what": "thing"}`)}))
	if out.Status != types.SyncExecutionStatusFailed || aws.ToString(out.Error) != "E42" || aws.ToString(out.Cause) != "bad thing" {
		t.Fatalf("sync failure = %s %v %v", out.Status, aws.ToString(out.Error), aws.ToString(out.Cause))
	}
	std := e.machine(t, "std", `{"StartAt": "P", "States": {"P": {"Type": "Pass", "End": true}}}`)
	_, err := e.sfn.StartSyncExecution(ctx, &sfn.StartSyncExecutionInput{StateMachineArn: aws.String(std)})
	var unsupported *types.StateMachineTypeNotSupported
	if !errors.As(err, &unsupported) {
		t.Fatalf("sync on STANDARD: %v", err)
	}

	parent := e.machine(t, "parent", `{"StartAt": "Run", "States": {"Run": {"Type": "Task",
	  "Resource": "arn:aws:states:::states:startExecution.sync:2",
	  "Parameters": {"StateMachineArn": "`+fast+`", "Input": {"who.$": "$.name"}},
	  "ResultSelector": {"msg.$": "$.Output.msg", "status.$": "$.Status"}, "End": true}}}`)
	d := e.wait(t, e.start(t, parent, `{"name": "child"}`))
	if d.Status != types.ExecutionStatusSucceeded || aws.ToString(d.Output) != `{"msg":"hi child","status":"SUCCEEDED"}` {
		t.Fatalf("parent = %s %s %s", d.Status, aws.ToString(d.Output), aws.ToString(d.Cause))
	}
}

func TestErrors(t *testing.T) {
	e := setup(t)
	var invalid *types.InvalidDefinition
	for _, def := range []string{`{not json`, `{"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": "B"}}}`, `{"StartAt": "A", "States": {"A": {"Type": "Nope", "End": true}}}`} {
		_, err := e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("bad"), Definition: aws.String(def), RoleArn: aws.String(role)})
		if !errors.As(err, &invalid) {
			t.Fatalf("%s: want InvalidDefinition, got %v", def, err)
		}
	}
	def := `{"StartAt": "Pick", "States": {"Pick": {"Type": "Choice", "Choices": [{"Variable": "$.x", "NumericEquals": 1, "Next": "Ok"}]}, "Ok": {"Type": "Pass", "Parameters": {"y.$": "$.nope"}, "End": true}}}`
	sm := e.machine(t, "picky", def)
	if again := e.machine(t, "picky", def); again != sm {
		t.Fatalf("idempotent create returned %s", again)
	}
	_, err := e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("picky"), Definition: aws.String(flow), RoleArn: aws.String(role)})
	var exists *types.StateMachineAlreadyExists
	if !errors.As(err, &exists) {
		t.Fatalf("want StateMachineAlreadyExists, got %v", err)
	}
	if d := e.wait(t, e.start(t, sm, `{"x": 2}`)); d.Status != types.ExecutionStatusFailed || aws.ToString(d.Error) != "States.NoChoiceMatched" {
		t.Fatalf("no choice = %s %v", d.Status, aws.ToString(d.Error))
	}
	if d := e.wait(t, e.start(t, sm, `{"x": 1}`)); aws.ToString(d.Error) != "States.Runtime" || !strings.Contains(aws.ToString(d.Cause), "$.nope") {
		t.Fatalf("missing path = %v %v", aws.ToString(d.Error), aws.ToString(d.Cause))
	}

	ddb := e.machine(t, "ddb", `{"StartAt": "Put", "States": {"Put": {"Type": "Task", "Resource": "arn:aws:states:::dynamodb:putItem",
	  "Parameters": {"TableName": "t", "Item": {"pk": {"S": "1"}}}, "End": true}}}`)
	if d := e.wait(t, e.start(t, ddb, `{}`)); aws.ToString(d.Error) != "States.TaskFailed" || !strings.Contains(aws.ToString(d.Cause), "DynamoDB_20120810") {
		t.Fatalf("disabled service = %v %v", aws.ToString(d.Error), aws.ToString(d.Cause))
	}

	_, err = e.sfn.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: aws.String(sm), Input: aws.String(`{oops`)})
	var badInput *types.InvalidExecutionInput
	if !errors.As(err, &badInput) {
		t.Fatalf("want InvalidExecutionInput, got %v", err)
	}
	must(e.sfn.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: aws.String(sm), Name: aws.String("once"), Input: aws.String(`{"x": 2}`)}))
	_, err = e.sfn.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: aws.String(sm), Name: aws.String("once"), Input: aws.String(`{"x": 3}`)})
	var dup *types.ExecutionAlreadyExists
	if !errors.As(err, &dup) {
		t.Fatalf("want ExecutionAlreadyExists, got %v", err)
	}
	_, err = e.sfn.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: aws.String(sm[:strings.Index(sm, "stateMachine")] + "execution:picky:nope")})
	var noExec *types.ExecutionDoesNotExist
	if !errors.As(err, &noExec) {
		t.Fatalf("want ExecutionDoesNotExist, got %v", err)
	}
	_, err = e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String("not-an-arn")})
	var badArn *types.InvalidArn
	if !errors.As(err, &badArn) {
		t.Fatalf("want InvalidArn, got %v", err)
	}
	_, err = e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm + "x")})
	var noSM *types.StateMachineDoesNotExist
	if !errors.As(err, &noSM) {
		t.Fatalf("want StateMachineDoesNotExist, got %v", err)
	}
}

func TestValidateAndTestState(t *testing.T) {
	e := setup(t)
	ok := must(e.sfn.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: aws.String(flow)}))
	if ok.Result != types.ValidateStateMachineDefinitionResultCodeOk || len(ok.Diagnostics) != 0 {
		t.Fatalf("valid definition = %s %+v", ok.Result, ok.Diagnostics)
	}
	bad := must(e.sfn.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{
		Definition: aws.String(`{"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": "Gone"}}}`)}))
	if bad.Result != types.ValidateStateMachineDefinitionResultCodeFail || len(bad.Diagnostics) != 1 ||
		aws.ToString(bad.Diagnostics[0].Code) != "MISSING_TRANSITION_TARGET" || aws.ToString(bad.Diagnostics[0].Location) != "/States/A/Next" {
		t.Fatalf("invalid definition = %s %+v", bad.Result, bad.Diagnostics)
	}

	test := func(def, input string) *sfn.TestStateOutput {
		return must(e.sfn.TestState(ctx, &sfn.TestStateInput{Definition: aws.String(def), RoleArn: aws.String(role), Input: aws.String(input),
			InspectionLevel: types.InspectionLevelDebug}))
	}
	pass := test(`{"Type": "Pass", "Parameters": {"sum.$": "States.MathAdd($.a, $.b)"}, "ResultPath": "$.r", "Next": "After"}`, `{"a": 1, "b": 2}`)
	if pass.Status != types.TestExecutionStatusSucceeded || aws.ToString(pass.Output) != `{"a":1,"b":2,"r":{"sum":3}}` ||
		aws.ToString(pass.NextState) != "After" || aws.ToString(pass.InspectionData.AfterParameters) != `{"sum":3}` {
		t.Fatalf("pass = %s %s %s", pass.Status, aws.ToString(pass.Output), aws.ToString(pass.NextState))
	}
	choice := test(`{"Type": "Choice", "Choices": [{"Variable": "$.n", "NumericGreaterThan": 5, "Next": "Big"}], "Default": "Small"}`, `{"n": 9}`)
	if aws.ToString(choice.NextState) != "Big" {
		t.Fatalf("choice = %s", aws.ToString(choice.NextState))
	}
	fail := test(`{"Type": "Fail", "Error": "Oops", "Cause": "bad"}`, `{}`)
	if fail.Status != types.TestExecutionStatusFailed || aws.ToString(fail.Error) != "Oops" {
		t.Fatalf("fail = %s %v", fail.Status, aws.ToString(fail.Error))
	}
	task := `{"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"FunctionName": "missing"}, %s "End": true}`
	caught := test(strings.Replace(task, "%s", `"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Handle"}],`, 1), `{}`)
	if caught.Status != types.TestExecutionStatusCaughtError || aws.ToString(caught.NextState) != "Handle" || aws.ToString(caught.Error) != "Lambda.ResourceNotFoundException" {
		t.Fatalf("caught = %s %s %v", caught.Status, aws.ToString(caught.NextState), aws.ToString(caught.Error))
	}
	retry := test(strings.Replace(task, "%s", `"Retry": [{"ErrorEquals": ["Lambda.ResourceNotFoundException"]}],`, 1), `{}`)
	if retry.Status != types.TestExecutionStatusRetriable {
		t.Fatalf("retry = %s", retry.Status)
	}
}

func TestLifecycleTagsDeliveryAndPersistence(t *testing.T) {
	e := setup(t)
	v1 := `{"StartAt": "A", "States": {"A": {"Type": "Pass", "Result": 1, "End": true}}}`
	sm := *must(e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("life"), Definition: aws.String(v1), RoleArn: aws.String(role),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("core")}}})).StateMachineArn
	first := e.start(t, sm, `{}`)
	e.wait(t, first)
	v2 := `{"StartAt": "B", "States": {"B": {"Type": "Pass", "Result": 2, "End": true}}}`
	up := must(e.sfn.UpdateStateMachine(ctx, &sfn.UpdateStateMachineInput{StateMachineArn: aws.String(sm), Definition: aws.String(v2)}))
	if up.UpdateDate == nil || up.RevisionId == nil {
		t.Fatalf("update = %+v", up)
	}
	if d := must(e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm)})); *d.Definition != v2 || d.Type != types.StateMachineTypeStandard {
		t.Fatalf("describe = %s %s", *d.Definition, d.Type)
	}
	if d := must(e.sfn.DescribeStateMachineForExecution(ctx, &sfn.DescribeStateMachineForExecutionInput{ExecutionArn: aws.String(first)})); *d.Definition != v1 {
		t.Fatalf("definition for execution = %s", *d.Definition)
	}
	if d := e.wait(t, e.start(t, sm, `{}`)); aws.ToString(d.Output) != "2" {
		t.Fatalf("updated output = %s", aws.ToString(d.Output))
	}
	if l := must(e.sfn.ListStateMachines(ctx, &sfn.ListStateMachinesInput{})); len(l.StateMachines) != 1 || *l.StateMachines[0].Name != "life" {
		t.Fatalf("machines = %+v", l.StateMachines)
	}

	must(e.sfn.TagResource(ctx, &sfn.TagResourceInput{ResourceArn: aws.String(sm), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(e.sfn.UntagResource(ctx, &sfn.UntagResourceInput{ResourceArn: aws.String(sm), TagKeys: []string{"team"}}))
	if tags := must(e.sfn.ListTagsForResource(ctx, &sfn.ListTagsForResourceInput{ResourceArn: aws.String(sm)})).Tags; len(tags) != 1 || *tags[0].Key != "env" {
		t.Fatalf("tags = %+v", tags)
	}

	// EventBridge and Scheduler targets deliver to the state machine ARN.
	if err := e.hub.Deliver(sm, &core.Message{Body: `{"from": "events"}`}); err != nil {
		t.Fatal(err)
	}
	console, err := e.svc.StartConsole(core.DefaultAccount, core.DefaultRegion, "life", `{"from": "console"}`)
	if err != nil {
		t.Fatal(err)
	}
	e.wait(t, console)
	if h := e.svc.History(console); len(h) != 4 || h[0]["type"] != "ExecutionStarted" {
		t.Fatalf("console history = %v", h)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		l := must(e.sfn.ListExecutions(ctx, &sfn.ListExecutionsInput{StateMachineArn: aws.String(sm), StatusFilter: types.ExecutionStatusSucceeded}))
		if len(l.Executions) == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d succeeded executions", len(l.Executions))
		}
	}
	tables := e.svc.Resources()
	if len(tables) != 2 || len(tables[0].Rows) != 1 || tables[0].Rows[0][4] != "4" || len(tables[1].Rows) != 4 {
		t.Fatalf("resources = %+v", tables)
	}
	if e.svc.Inspect("Executions", tables[1].Rows[0][0]) == nil || e.svc.Inspect("State machines", "life") == nil {
		t.Fatal("inspect returned nothing")
	}

	// A running execution fails with pogi.Restarted after a save and load.
	act := *must(e.sfn.CreateActivity(ctx, &sfn.CreateActivityInput{Name: aws.String("never")})).ActivityArn
	stuck := e.machine(t, "stuck", `{"StartAt": "T", "States": {"T": {"Type": "Task", "Resource": "`+act+`", "End": true}}}`)
	running := e.start(t, stuck, `{}`)
	saved := must(e.svc.Save())
	if err := e.svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	if d := e.wait(t, running); d.Status != types.ExecutionStatusFailed || aws.ToString(d.Error) != "pogi.Restarted" {
		t.Fatalf("after load = %s %v", d.Status, aws.ToString(d.Error))
	}
	if d := must(e.sfn.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: aws.String(first)})); d.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("finished execution after load = %s", d.Status)
	}

	must(e.sfn.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: aws.String(sm)}))
	_, err = e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm)})
	var gone *types.StateMachineDoesNotExist
	if !errors.As(err, &gone) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestFailurePropagationAndTimeouts(t *testing.T) {
	e := setup(t)
	sm := e.machine(t, "fails", `{
	  "StartAt": "Par",
	  "States": {
	    "Par": {"Type": "Parallel", "Next": "Items",
	      "Branches": [
	        {"StartAt": "Ok", "States": {"Ok": {"Type": "Pass", "End": true}}},
	        {"StartAt": "Boom", "States": {"Boom": {"Type": "Fail", "Error": "BranchBoom", "Cause": "branch 2"}}}
	      ],
	      "Catch": [{"ErrorEquals": ["BranchBoom"], "ResultPath": "$.parErr", "Next": "Items"}]},
	    "Items": {"Type": "Map", "ItemsPath": "$.items", "Next": "Keep",
	      "Iterator": {"StartAt": "Check", "States": {
	        "Check": {"Type": "Choice", "Choices": [{"Variable": "$", "NumericEquals": 2, "Next": "Bad"}], "Default": "Good"},
	        "Bad": {"Type": "Fail", "Error": "ItemBoom"},
	        "Good": {"Type": "Pass", "End": true}}},
	      "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.mapErr", "Next": "Keep"}]},
	    "Keep": {"Type": "Pass", "Result": "dropped", "ResultPath": null, "End": true}
	  }
	}`)
	d := e.wait(t, e.start(t, sm, `{"items": [1, 2, 3]}`))
	var out struct {
		ParErr, MapErr struct{ Error, Cause string }
	}
	json.Unmarshal([]byte(aws.ToString(d.Output)), &out)
	if d.Status != types.ExecutionStatusSucceeded || out.ParErr.Error != "BranchBoom" || out.ParErr.Cause != "branch 2" || out.MapErr.Error != "ItemBoom" {
		t.Fatalf("status %s output %s", d.Status, aws.ToString(d.Output))
	}
	events := e.history(t, *d.ExecutionArn)
	if count(events, "ParallelStateFailed") != 1 || count(events, "MapIterationFailed") != 1 || count(events, "MapStateFailed") != 1 || count(events, "FailStateEntered") != 2 {
		t.Fatal("failure events missing")
	}

	act := *must(e.sfn.CreateActivity(ctx, &sfn.CreateActivityInput{Name: aws.String("slow")})).ActivityArn
	slowTask := e.start(t, e.machine(t, "slowtask", `{"StartAt": "T", "States": {"T": {"Type": "Task", "Resource": "`+act+`", "TimeoutSeconds": 1, "End": true}}}`), `{}`)
	slowRun := e.start(t, e.machine(t, "slowrun", `{"TimeoutSeconds": 1, "StartAt": "W", "States": {"W": {"Type": "Wait", "Seconds": 10, "End": true}}}`), `{}`)
	if d := e.wait(t, slowTask); d.Status != types.ExecutionStatusFailed || aws.ToString(d.Error) != "States.Timeout" || count(e.history(t, slowTask), "ActivityTimedOut") != 1 {
		t.Fatalf("task timeout = %s %v", d.Status, aws.ToString(d.Error))
	}
	if d := e.wait(t, slowRun); d.Status != types.ExecutionStatusTimedOut || count(e.history(t, slowRun), "ExecutionTimedOut") != 1 {
		t.Fatalf("execution timeout = %s", d.Status)
	}
}

// A task state's internal call carries the execution's flow: a message it sends keeps the trace
// the execution was started with.
func TestTaskCallKeepsTrace(t *testing.T) {
	e := setup(t)
	q := *must(e.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("traced")})).QueueUrl
	sm := e.machine(t, "tr", `{"StartAt": "Send", "States": {"Send": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
	  "Parameters": {"QueueUrl": "`+q+`", "MessageBody": "x"}, "End": true}}}`)
	root := "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	out := must(e.sfn.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: aws.String(sm), Input: aws.String("{}")},
		func(o *sfn.Options) {
			o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("X-Amzn-Trace-Id", "Root="+root))
		}))
	e.wait(t, *out.ExecutionArn)
	m := must(e.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(q), MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"All"}})).Messages
	if len(m) != 1 || core.TraceRoot(m[0].Attributes["AWSTraceHeader"]) != root {
		t.Fatalf("message lost the trace: %+v", m)
	}
}

func TestResultPathIndexAndStrictDefinitions(t *testing.T) {
	e := setup(t)
	// floci #3479: ResultPath may point into an existing array element.
	sm := e.machine(t, "idx", `{"StartAt":"Set","States":{"Set":{"Type":"Pass","Result":{"s":1},"ResultPath":"$[1].payload","End":true}}}`)
	d := e.wait(t, e.start(t, sm, `[{"a":1},{"id":7},4]`))
	jsonEq(t, aws.ToString(d.Output), []any{map[string]any{"a": 1.0}, map[string]any{"id": 7.0, "payload": map[string]any{"s": 1.0}}, 4.0})

	// floci #2735: no terminal state, an unreachable state, TimeoutSeconds off a Task and Catch on a Choice are refused.
	for want, def := range map[string]string{
		"MISSING_END_STATE":         `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"B"},"B":{"Type":"Pass","Next":"A"}}}`,
		"MISSING_TRANSITION_TARGET": `{"StartAt":"A","States":{"A":{"Type":"Pass","End":true},"C":{"Type":"Pass","End":true}}}`,
		"SCHEMA_VALIDATION_FAILED":  `{"StartAt":"A","States":{"A":{"Type":"Pass","TimeoutSeconds":5,"End":true}}}`,
		"Choice":                    `{"StartAt":"A","States":{"A":{"Type":"Choice","Choices":[{"Variable":"$.a","IsPresent":true,"Next":"B"}],"Catch":[{"ErrorEquals":["States.ALL"],"Next":"B"}]},"B":{"Type":"Succeed"}}}`,
	} {
		r := must(e.sfn.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: aws.String(def)}))
		if want == "Choice" {
			want = "SCHEMA_VALIDATION_FAILED"
		}
		if r.Result != types.ValidateStateMachineDefinitionResultCodeFail || aws.ToString(r.Diagnostics[0].Code) != want {
			t.Errorf("%s: validate = %s %+v", want, r.Result, r.Diagnostics)
		}
		if _, err := e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("bad"), Definition: aws.String(def), RoleArn: aws.String(role)}); err == nil {
			t.Errorf("%s: CreateStateMachine accepted it", want)
		}
	}
}

func TestLowFloci(t *testing.T) {
	e := setup(t)
	// floci #3733: Map ToleratedFailure* fields: a literal and its Path are exclusive, a count is not negative, a percentage is 0 to 100.
	mk := func(extra string) string {
		return `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items",` + extra + `"ItemProcessor":{"ProcessorConfig":{"Mode":"INLINE"},"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	}
	for name, extra := range map[string]string{
		"both":  `"ToleratedFailureCount":1,"ToleratedFailureCountPath":"$.n",`,
		"pct-1": `"ToleratedFailurePercentage":-1,`, "pct101": `"ToleratedFailurePercentage":101,`, "count-1": `"ToleratedFailureCount":-1,`,
	} {
		r := must(e.sfn.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: aws.String(mk(extra))}))
		if r.Result != types.ValidateStateMachineDefinitionResultCodeFail {
			t.Errorf("%s: validate = %s", name, r.Result)
		}
	}
	for _, ok := range []string{`"ToleratedFailurePercentage":100,`, `"ToleratedFailureCountPath":"$.n",`} {
		if r := must(e.sfn.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: aws.String(mk(ok))})); r.Result != types.ValidateStateMachineDefinitionResultCodeOk {
			t.Errorf("%s: validate = %s %+v", ok, r.Result, r.Diagnostics)
		}
	}

	// floci #2870: a path in an intrinsic argument that matches nothing fails with States.Runtime.
	d := e.wait(t, e.start(t, e.machine(t, "intr", `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"v.$":"States.Format('{}', $.nope)"},"End":true}}}`), `{"other":1}`))
	if d.Status != types.ExecutionStatusFailed || aws.ToString(d.Error) != "States.Runtime" {
		t.Errorf("intrinsic with a missing path: %s %q", d.Status, aws.ToString(d.Error))
	}

	// floci #2733: a TIMED_OUT execution has no error or cause of its own.
	d = e.wait(t, e.start(t, e.machine(t, "tmo", `{"TimeoutSeconds":1,"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":10,"End":true}}}`), `{}`))
	if d.Status != types.ExecutionStatusTimedOut || d.Error != nil || d.Cause != nil {
		t.Errorf("timed out: %s %v %v", d.Status, d.Error, d.Cause)
	}

	// floci #4722: a Task cut by a sibling iteration's failure records TaskStateAborted.
	act := *must(e.sfn.CreateActivity(ctx, &sfn.CreateActivityInput{Name: aws.String("hang")})).ActivityArn
	sm := e.machine(t, "cut", `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":0,"ItemProcessor":{"ProcessorConfig":{"Mode":"INLINE"},"StartAt":"C","States":{
	  "C":{"Type":"Choice","Choices":[{"Variable":"$.kind","StringEquals":"hang","Next":"T"}],"Default":"W"},
	  "T":{"Type":"Task","Resource":"`+act+`","End":true},
	  "W":{"Type":"Wait","Seconds":1,"Next":"F"},"F":{"Type":"Fail","Error":"Boom"}}},"End":true}}}`)
	d = e.wait(t, e.start(t, sm, `{"items":[{"kind":"hang"},{"kind":"fail"}]}`))
	ev := e.history(t, *d.ExecutionArn)
	if d.Status != types.ExecutionStatusFailed || count(ev, "TaskStateAborted") != 1 || count(ev, "MapIterationAborted") != 1 {
		t.Errorf("cut Task: %s aborted task=%d iteration=%d", d.Status, count(ev, "TaskStateAborted"), count(ev, "MapIterationAborted"))
	}
}
