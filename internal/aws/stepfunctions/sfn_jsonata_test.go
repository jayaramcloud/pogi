package stepfunctions_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var machines atomic.Int32

// run creates a state machine, runs it once and waits for the end.
func (e *env) run(t *testing.T, def, input string) *sfn.DescribeExecutionOutput {
	t.Helper()
	return e.wait(t, e.start(t, e.machine(t, "m"+strconv.Itoa(int(machines.Add(1))), def), input))
}

func (e *env) validate(def string) (string, string) {
	out := must(e.sfn.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: aws.String(def)}))
	var diags []string
	for _, d := range out.Diagnostics {
		diags = append(diags, aws.ToString(d.Code)+": "+aws.ToString(d.Message))
	}
	return string(out.Result), strings.Join(diags, "; ")
}

func mustJSON(s string) any {
	var v any
	must(0, json.Unmarshal([]byte(s), &v))
	return v
}

func TestJSONataStateMachine(t *testing.T) {
	e := setup(t)
	def := `{"QueryLanguage":"JSONata","StartAt":"Prep","States":{
	  "Prep":{"Type":"Pass","Assign":{"total":"{% $sum($states.input.items.price) %}","name":"{% $uppercase($states.input.customer) %}","offset":10},
	          "Output":{"count":"{% $count($states.input.items) %}","ids":[1,2,3]},"Next":"Route"},
	  "Route":{"Type":"Choice","Choices":[
	     {"Condition":"{% $total > 100 %}","Next":"Fan","Assign":{"tier":"gold"},"Output":"{% $merge([$states.input, {'tier': 'gold'}]) %}"}],
	     "Default":"Small"},
	  "Fan":{"Type":"Map","Items":"{% $states.input.ids %}","MaxConcurrency":"{% 2 %}",
	     "ItemSelector":{"id":"{% $states.context.Map.Item.Value %}","base":"{% $offset %}"},
	     "ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"offset":99},"Output":"{% $states.input.id + $states.input.base %}","End":true}}},
	     "Assign":{"sum":"{% $sum($states.result) %}"},"Output":"{% $states.result %}","Next":"Wait"},
	  "Wait":{"Type":"Wait","Seconds":"{% 0 %}","Next":"Both"},
	  "Both":{"Type":"Parallel","Arguments":{"n":"{% $count($states.input) %}"},
	     "Branches":[{"StartAt":"A","States":{"A":{"Type":"Pass","Output":"{% $states.input.n %}","End":true}}},
	                 {"StartAt":"B","States":{"B":{"Type":"Pass","Output":"{% $offset + 1 %}","End":true}}}],
	     "Output":{"branches":"{% $states.result %}","msg":"{% 'Hi ' & $name & ', ' & $string($total) & ' total, tier ' & $tier & ', exec ' & $states.context.Execution.Name %}",
	               "offset":"{% $offset %}","sum":"{% $sum %}"},"End":true},
	  "Small":{"Type":"Succeed"}}}`
	arn := *must(e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("jn"), RoleArn: aws.String(role), Definition: aws.String(def)})).StateMachineArn
	ex := must(e.sfn.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: aws.String(arn), Name: aws.String("run1"),
		Input: aws.String(`{"customer":"ana","items":[{"price":70.5},{"price":59.5}]}`)}))
	d := e.wait(t, *ex.ExecutionArn)
	if d.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("%s %s %s", d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
	}
	// The Map iterations assign offset = 99 inside, and that must not leak out: $offset is still 10.
	jsonEq(t, aws.ToString(d.Output), map[string]any{"branches": []any{3, 11}, "msg": "Hi ANA, 130 total, tier gold, exec run1", "offset": 10, "sum": 36})
}

func TestJSONataStateMachineVariableSum(t *testing.T) {
	e := setup(t)
	d := e.run(t, `{"QueryLanguage":"JSONata","StartAt":"A","States":{
	  "A":{"Type":"Pass","Assign":{"x":1},"Next":"B"},
	  "B":{"Type":"Pass","Assign":{"x":"{% $x + 1 %}"},"Output":"{% $x %}","Next":"C"},
	  "C":{"Type":"Pass","Output":"{% [$states.input, $x] %}","End":true}}}`, `{}`)
	jsonEq(t, aws.ToString(d.Output), []any{1, 2})
	// The StateExited event lists what the state assigned.
	var assigned map[string]string
	for _, ev := range e.history(t, *d.ExecutionArn) {
		if ev.StateExitedEventDetails == nil || *ev.StateExitedEventDetails.Name != "B" {
			continue
		}
		assigned = ev.StateExitedEventDetails.AssignedVariables
	}
	if assigned["x"] != "2" {
		t.Fatalf("assigned variables = %v", assigned)
	}
}

func TestJSONataTaskArgumentsAndOutput(t *testing.T) {
	e := setup(t)
	q := *must(e.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})).QueueUrl
	d := e.run(t, `{"QueryLanguage":"JSONata","StartAt":"Send","States":{
	  "Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage",
	    "Arguments":{"QueueUrl":"`+q+`","MessageBody":"{% $string({'order': $states.input.order, 'at': $states.context.State.Name}) %}"},
	    "Assign":{"messageId":"{% $states.result.MessageId %}"},
	    "Output":{"ok":"{% $exists($states.result.MessageId) %}","order":"{% $states.input.order %}"},"Next":"Lookup"},
	  "Lookup":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sqs:getQueueUrl","Arguments":{"QueueName":"nope"},
	    "Catch":[{"ErrorEquals":["Sqs.QueueDoesNotExistException"],"Next":"Done","Assign":{"failed":"{% $states.errorOutput.Error %}"},
	              "Output":"{% $merge([$states.input, {'caught': $states.errorOutput.Error}]) %}"}],"End":true},
	  "Done":{"Type":"Pass","Output":"{% $merge([$states.input, {'failed': $failed, 'hasId': $messageId != ''}]) %}","End":true}}}`, `{"order":7}`)
	if d.Status != types.ExecutionStatusSucceeded {
		t.Fatalf("%s %s %s", d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
	}
	jsonEq(t, aws.ToString(d.Output), map[string]any{"ok": true, "order": 7, "caught": "Sqs.QueueDoesNotExistException", "failed": "Sqs.QueueDoesNotExistException", "hasId": true})
	m := must(e.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(q), WaitTimeSeconds: 5}))
	if len(m.Messages) != 1 || *m.Messages[0].Body != `{"at":"Send","order":7}` {
		t.Fatalf("message = %+v", m.Messages)
	}
}

func TestJSONataFailures(t *testing.T) {
	e := setup(t)
	fail := func(expr, want string) {
		t.Helper()
		sm := e.machine(t, "f"+strconv.Itoa(int(machines.Add(1))), `{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Pass","Output":"{% `+strings.ReplaceAll(expr, `"`, `\"`)+` %}","End":true}}}`)
		arn := e.start(t, sm, `{}`)
		d := e.wait(t, arn)
		if d.Status != types.ExecutionStatusFailed || aws.ToString(d.Error) != "States.QueryEvaluationError" || !strings.Contains(aws.ToString(d.Cause), want) ||
			!strings.Contains(aws.ToString(d.Cause), "The JSONata expression") {
			t.Fatalf("%s: %s %s %s", expr, d.Status, aws.ToString(d.Error), aws.ToString(d.Cause))
		}
		var ev *types.EvaluationFailedEventDetails
		for _, h := range e.history(t, arn) {
			if h.Type == types.HistoryEventTypeEvaluationFailed {
				ev = h.EvaluationFailedEventDetails
			}
		}
		if ev == nil || aws.ToString(ev.Location) != "Output" || aws.ToString(ev.State) != "T" {
			t.Fatalf("%s: EvaluationFailed event = %+v", expr, ev)
		}
	}
	fail(`$abs("x")`, "T0410")
	fail(`1 + "a"`, "T2002")
	fail(`$states.input.nope`, "returned nothing (undefined)")
	fail(`$eval("1+1")`, "T1006")
	fail(`$error("boom")`, "boom")
	fail(`$string`, "no JSON form")

	// The error is catchable and retriable like any other.
	d := e.run(t, `{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Pass","Output":"{% $abs('x') %}","Next":"X"},"X":{"Type":"Succeed"}}}`, `{}`)
	if d.Status != types.ExecutionStatusFailed {
		t.Fatalf("pass with a failing expression: %s", d.Status)
	}
	d = e.run(t, `{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Parallel","Branches":[{"StartAt":"B","States":{"B":{"Type":"Pass","Output":"{% 1 / 0 %}","End":true}}}],
	  "Retry":[{"ErrorEquals":["States.QueryEvaluationError"],"IntervalSeconds":1,"MaxAttempts":2}],
	  "Catch":[{"ErrorEquals":["States.ALL"],"Next":"C","Output":"{% $states.errorOutput.Error %}"}],"End":true},"C":{"Type":"Pass","End":true}}}`, `{}`)
	if aws.ToString(d.Output) != `"States.QueryEvaluationError"` {
		t.Fatalf("caught = %s %s", d.Status, aws.ToString(d.Output))
	}
}

func TestJSONPathVariables(t *testing.T) {
	e := setup(t)
	d := e.run(t, `{"StartAt":"A","States":{
	  "A":{"Type":"Pass","Result":{"n":5,"list":[1,2]},"Assign":{"n.$":"$.n","fixed":"x","first.$":"$.list[0]","ctx.$":"$$.Execution.Name"},"ResultPath":"$.r","Next":"B"},
	  "B":{"Type":"Pass","Parameters":{"v.$":"$n","f.$":"$fixed","first.$":"$first","list.$":"States.Array($n, $first)"},"Next":"C"},
	  "C":{"Type":"Choice","Choices":[{"Variable":"$.v","NumericEqualsPath":"$n","Next":"Yes"}],"Default":"No"},
	  "Yes":{"Type":"Pass","Result":"same","End":true},"No":{"Type":"Fail"}}}`, `{}`)
	if aws.ToString(d.Output) != `"same"` {
		t.Fatalf("%s %s %s %s", d.Status, aws.ToString(d.Output), aws.ToString(d.Error), aws.ToString(d.Cause))
	}
	d = e.run(t, `{"StartAt":"A","States":{"A":{"Type":"Pass","Parameters":{"v.$":"$missing"},"End":true}}}`, `{}`)
	if d.Status != types.ExecutionStatusFailed {
		t.Fatalf("a missing variable: %s", d.Status)
	}
}

// floci #2745, #2968: expressions are parsed at create, and the two query languages keep to their own fields.
func TestJSONataDefinitionChecks(t *testing.T) {
	e := setup(t)
	_, err := e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("j"), RoleArn: aws.String(role),
		Definition: aws.String(`{"QueryLanguage":"JSONata","StartAt":"Main","States":{"Main":{"Type":"Pass","Output":"{% 1 + %}","End":true}}}`)})
	var invalid *types.InvalidDefinition
	if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "INVALID_JSONATA_EXPRESSION") {
		t.Fatalf("a malformed expression: %v", err)
	}
	pass := func(fields string) string {
		return `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass",` + fields + `"End":true}}}`
	}
	for name, def := range map[string]string{
		"JSONata-only field under JSONPath":                     `{"StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% 1 %}","End":true}}}`,
		"JSONPath field under JSONata":                          pass(`"InputPath":"$.a",`),
		"Parameters under JSONata":                              pass(`"Parameters":{"a":1},`),
		"a .$ key under JSONata":                                pass(`"Output":{"a.$":"$.b"},`),
		"state QueryLanguage JSONata but nested JSONPath field": `{"StartAt":"M","States":{"M":{"Type":"Map","QueryLanguage":"JSONata","ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% $states.input %}","End":true}}},"End":true}}}`,
		"ItemProcessor QueryLanguage":                           `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.i","ItemProcessor":{"QueryLanguage":"JSONPath","StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`,
		"Iterator QueryLanguage":                                `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.i","Iterator":{"QueryLanguage":"JSONPath","StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`,
		"JSONPath state under JSONata":                          `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","QueryLanguage":"JSONPath","End":true}}}`,
		"lowercase jsonpath":                                    `{"QueryLanguage":"jsonpath","StartAt":"P","States":{"P":{"Type":"Pass","InputPath":"$.a","End":true}}}`,
		"JSONata Choice with Variable":                          `{"QueryLanguage":"JSONata","StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.a","IsPresent":true,"Next":"C"}],"Default":"C"}}}`,
		"Catch ResultPath under JSONata":                        `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"X","States":{"X":{"Type":"Succeed"}}}],"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.e","Next":"P"}],"End":true}}}`,
	} {
		res, diag := e.validate(def)
		if res != "FAIL" {
			t.Errorf("%s: validate = %s %q", name, res, diag)
		}
	}
	for name, def := range map[string]string{
		"per-state JSONata under JSONPath": `{"StartAt":"P","States":{"P":{"Type":"Pass","QueryLanguage":"JSONata","Output":"{% $states.input %}","End":true}}}`,
		"Assign under JSONPath":            `{"StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"a.$":"$.b"},"End":true}}}`,
	} {
		if res, diag := e.validate(def); res != "OK" {
			t.Errorf("%s: validate = %s %q", name, res, diag)
		}
	}
}

func TestJSONataTestState(t *testing.T) {
	e := setup(t)
	out := must(e.sfn.TestState(ctx, &sfn.TestStateInput{
		Definition: aws.String(`{"Type":"Pass","QueryLanguage":"JSONata","Assign":{"y":"{% $x * 2 %}"},"Output":"{% $states.input.a + $x %}","Next":"After"}`),
		RoleArn:    aws.String(role), Input: aws.String(`{"a":40}`), Variables: aws.String(`{"x":2}`), InspectionLevel: types.InspectionLevelDebug}))
	if aws.ToString(out.Output) != "42" || aws.ToString(out.NextState) != "After" || !strings.Contains(aws.ToString(out.InspectionData.Variables), `"y":4`) {
		t.Fatalf("test state = %s %s %+v", aws.ToString(out.Output), aws.ToString(out.NextState), out.InspectionData)
	}
}

// floci #2737, #2736, #2738: expressions that fail on AWS fail here, with a cause that names the expression.
func TestFloci2736To2738(t *testing.T) {
	e := setup(t)
	d := e.run(t, `{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Pass","Output":"{% $join($map([1..100000], function($v){ $string($v) }), '') %}","Catch":[],"End":true}}}`, `{}`)
	if d.Status != types.ExecutionStatusSucceeded || len(aws.ToString(d.Output)) != 488895+2 {
		t.Fatalf("2738: %s %d", d.Status, len(aws.ToString(d.Output)))
	}
}

// floci #3260: JSONPath filter expressions and recursive descent.
func TestFloci3260(t *testing.T) {
	e := setup(t)
	for _, c := range []struct{ path, in, want string }{
		{`$.list[?(@.keep)]`, `{"list":[{"keep":true},{"keep":false},{"other":1}]}`, `[{"keep":true},{"keep":false}]`},
		{`$.list[?(@.keep == true)]`, `{"list":[{"keep":true},{"keep":false}]}`, `[{"keep":true}]`},
		{`$.d..a`, `{"d":{"x":{"a":[1,2]}}}`, `[[1,2]]`},
		{`$.d..a[*]`, `{"d":{"x":{"a":[1,2]}}}`, `[1,2]`},
		{`$.items[?(@.n > 1 && @.n <= 3)].id`, `{"items":[{"id":"a","n":1},{"id":"b","n":2},{"id":"c","n":3},{"id":"d","n":4}]}`, `["b","c"]`},
		{`$.items[?(@.name == 'x' || @.n != 1)].id`, `{"items":[{"id":"a","n":1,"name":"x"},{"id":"b","n":2},{"id":"c","n":1}]}`, `["a","b"]`},
		{`$.items[?(@.n == $.limit)].id`, `{"limit":2,"items":[{"id":"a","n":1},{"id":"b","n":2}]}`, `["b"]`},
		{`$.items[?(!@.session)]`, `{"items":[{"id":1,"session":null},{"id":2,"session":{"x":1}},{"id":3}]}`, `[{"id":1,"session":null},{"id":3}]`},
		{`$.items[1:3]`, `{"items":[1,2,3,4]}`, `[2,3]`},
		{`$.items[-2:]`, `{"items":[1,2,3,4]}`, `[3,4]`},
		{`$..id`, `{"a":{"id":1},"b":[{"id":2},{"c":{"id":3}}]}`, `[1,2,3]`},
	} {
		d := e.run(t, `{"StartAt":"S","States":{"S":{"Type":"Pass","Parameters":{"r.$":"`+c.path+`"},"End":true}}}`, c.in)
		if d.Status != types.ExecutionStatusSucceeded {
			t.Errorf("%s: %s %s", c.path, d.Status, aws.ToString(d.Cause))
			continue
		}
		jsonEq(t, aws.ToString(d.Output), map[string]any{"r": mustJSON(c.want)})
	}
}

// $states.context carries the task token, so a callback works in a JSONata state.
func TestJSONataCallbackToken(t *testing.T) {
	e := setup(t)
	q := *must(e.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("ask")})).QueueUrl
	sm := e.machine(t, "cb", `{"QueryLanguage":"JSONata","StartAt":"Ask","States":{"Ask":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken",
	  "Arguments":{"QueueUrl":"`+q+`","MessageBody":"{% $string({'token': $states.context.Task.Token}) %}"},"TimeoutSeconds":"{% 30 %}",
	  "Output":"{% $states.result.answer %}","End":true}}}`)
	arn := e.start(t, sm, `{}`)
	m := must(e.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(q), WaitTimeSeconds: 5}))
	if len(m.Messages) != 1 {
		t.Fatal("no message")
	}
	var body struct{ Token string }
	must(0, json.Unmarshal([]byte(*m.Messages[0].Body), &body))
	must(e.sfn.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: aws.String(body.Token), Output: aws.String(`{"answer":"yes"}`)}))
	if d := e.wait(t, arn); aws.ToString(d.Output) != `"yes"` {
		t.Fatalf("%s %s %s", d.Status, aws.ToString(d.Output), aws.ToString(d.Cause))
	}
}
