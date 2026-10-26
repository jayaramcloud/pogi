package web_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// hxPost posts a form the way htmx does from a tab or the drawer, and returns the status, headers and body.
func (c *console) hxPost(path, target string, v url.Values) (int, http.Header, string) {
	c.t.Helper()
	req, _ := http.NewRequest("POST", c.base+path, strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", target)
	res, err := c.c.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header, string(b)
}

// bulkDelete ticks the row with name on a table and deletes it, as the bulk bar does.
func (c *console) bulkDelete(svc, table, name string) string {
	c.t.Helper()
	page := c.get("/_pogi/resources/" + svc + "?tab=" + url.QueryEscape(table))
	body, _ := c.post("/_pogi/bulk/"+svc+"/"+url.PathEscape(table), url.Values{"action": {"delete"}, "row": {rowOf(c.t, page, name)}})
	return body
}

func zipOf(t *testing.T, name, src string) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	f, _ := zw.Create(name)
	f.Write([]byte(src))
	zw.Close()
	return b.Bytes()
}

func (c *console) upload(path, field string, data []byte) string {
	c.t.Helper()
	var mb bytes.Buffer
	mw := multipart.NewWriter(&mb)
	fw, _ := mw.CreateFormFile(field, "code.zip")
	fw.Write(data)
	mw.Close()
	res, err := c.c.Post(c.base+path, mw.FormDataContentType(), &mb)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return string(b)
}

func TestLambdaConsoleActions(t *testing.T) {
	c := start(t, "lambda", "sqs")
	code := zipOf(t, "app.py", "def handler(e, c):\n    return {'ok': True}\n")
	c.ok("/_pogi/new/lambda/CreateFunction", url.Values{"FunctionName": {"hello"}, "Runtime": {"python3.12"}, "Handler": {"app.handler"},
		"Role": {"arn:aws:iam::000000000000:role/lambda"}, "ZipFile": {base64.StdEncoding.EncodeToString(code)}})
	fn := "/_pogi/lambda/000000000000/us-east-1/hello"

	// the example payload is what Invoke sends
	page := c.hx(fn, "drawer")
	want(t, page, `rows="6">{"hello": "pogi"}</textarea>`)
	wantNot(t, page, "placeholder='{")

	// environment variables: saved, listed again, and a bad name is refused with what to do
	c.post(fn+"/env", url.Values{"k0": {"STAGE"}, "v0": {"dev"}, "k1": {""}, "v1": {"ignored"}})
	want(t, c.get(fn), `value="STAGE"`, `value="dev"`)
	body, _ := c.post(fn+"/env", url.Values{"k0": {"1BAD"}, "v0": {"x"}})
	want(t, body, "not a valid name")
	body, _ = c.post(fn+"/env", url.Values{"k0": {"A"}, "k1": {"A"}})
	want(t, body, "A is listed twice")
	want(t, c.get(fn), `value="STAGE"`) // a refused save changed nothing

	// update code: a zip is taken, anything else is refused
	want(t, c.upload(fn+"/code", "file", []byte("not a zip")), "Pick a .zip file")
	got := c.upload(fn+"/code", "file", zipOf(t, "app.py", "def handler(e, c):\n    return {'v': 2}\n"))
	if strings.Contains(got, "Pick a .zip") || strings.Contains(flash(got), "Error") {
		t.Fatalf("update code: %s", flash(got))
	}

	// an event source mapping from the form's choices
	call(t, c.srv, "AmazonSQS", "CreateQueue", map[string]any{"QueueName": "jobs"})
	form := c.get("/_pogi/new/lambda/CreateEventSourceMapping")
	want(t, form, "arn:aws:sqs:us-east-1:000000000000:jobs", ">hello<", `value="10"`)
	body, _ = c.post("/_pogi/new/lambda/CreateEventSourceMapping", url.Values{"FunctionName": {"hello"}, "BatchSize": {"0"},
		"EventSourceArn": {"arn:aws:sqs:us-east-1:000000000000:jobs"}})
	want(t, body, "Batch size must be 1 or more")
	c.ok("/_pogi/new/lambda/CreateEventSourceMapping", url.Values{"FunctionName": {"hello"}, "BatchSize": {"5"},
		"EventSourceArn": {"arn:aws:sqs:us-east-1:000000000000:jobs"}})
	want(t, c.get("/_pogi/resources/lambda?tab=Event+source+mappings"), "arn:aws:sqs:us-east-1:000000000000:jobs")

	// delete from the row menu and in bulk goes through the REST API
	want(t, c.get("/_pogi/resources/lambda"), `hx-post="/_pogi/bulk/lambda/Functions"`)
	c.bulkDelete("lambda", "Functions", "hello")
	wantNot(t, c.get("/_pogi/resources/lambda"), `>hello<`)
}

func TestStepFunctionsStartStop(t *testing.T) {
	c := start(t, "stepfunctions")
	call(t, c.srv, "AWSStepFunctions", "CreateStateMachine", map[string]any{"name": "slow", "roleArn": "arn:aws:iam::000000000000:role/r",
		"definition": `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":60,"End":true}}}`})
	want(t, c.hx("/_pogi/sfn?machine=slow", "xtab"), "Start execution")

	// bad input is refused before anything starts
	status, hd, _ := c.hxPost("/_pogi/sfn/start", "xtab", url.Values{"machine": {"slow"}, "input": {"{"}})
	if status != 200 || hd.Get("HX-Reswap") != "none" || !strings.Contains(hd.Get("HX-Trigger"), "not valid JSON") {
		t.Fatalf("bad input: %d %v", status, hd)
	}
	_, hd, _ = c.hxPost("/_pogi/sfn/start", "xtab", url.Values{"machine": {"nope"}})
	if !strings.Contains(hd.Get("HX-Trigger"), "Pick a state machine") {
		t.Fatalf("unknown machine: %v", hd)
	}

	// a started execution opens, running, with a Stop button
	_, hd, body := c.hxPost("/_pogi/sfn/start", "xtab", url.Values{"machine": {"slow"}, "name": {"run1"}, "input": {`{"a":1}`}})
	want(t, body, "Execution <b>run1</b>", "RUNNING", `hx-post="/_pogi/sfn/stop"`)
	if !strings.Contains(hd.Get("HX-Trigger"), "run1 started") {
		t.Fatalf("no toast: %v", hd)
	}
	// the same name twice is the API's error, shown as a toast
	_, hd, _ = c.hxPost("/_pogi/sfn/start", "xtab", url.Values{"machine": {"slow"}, "name": {"run1"}, "input": {"{}"}})
	if hd.Get("HX-Reswap") != "none" {
		t.Fatalf("duplicate name was not refused: %v", hd)
	}

	_, _, body = c.hxPost("/_pogi/sfn/stop", "xtab", url.Values{"machine": {"slow"}, "execution": {"run1"}})
	wantNot(t, body, `hx-post="/_pogi/sfn/stop"`)
	for i := 0; i < 50; i++ {
		out := call(t, c.srv, "AWSStepFunctions", "DescribeExecution", map[string]any{"executionArn": core.ARN("states", "us-east-1", "000000000000", "execution:slow:run1")})
		if out["status"] == "ABORTED" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("execution was not stopped")
}

func TestSNSSubscribeUnsubscribe(t *testing.T) {
	c := start(t, "sns", "sqs")
	c.ok("/_pogi/new/sns/CreateTopic", url.Values{"Name": {"alerts"}})
	call(t, c.srv, "AmazonSQS", "CreateQueue", map[string]any{"QueueName": "inbox"})
	topic, queue := "arn:aws:sns:us-east-1:000000000000:alerts", "arn:aws:sqs:us-east-1:000000000000:inbox"

	body, _ := c.post("/_pogi/new/sns/CreateSubscription", url.Values{"TopicArn": {topic}, "Protocol": {"sqs"}, "Endpoint": {queue}, "FilterPolicy": {"{"}})
	want(t, body, "Filter policy is not valid JSON")
	body, _ = c.post("/_pogi/new/sns/CreateSubscription", url.Values{"TopicArn": {topic}, "Protocol": {"carrier-pigeon"}, "Endpoint": {"x"}})
	want(t, body, `id="apierr"`)
	c.ok("/_pogi/new/sns/CreateSubscription", url.Values{"TopicArn": {topic}, "Protocol": {"sqs"}, "Endpoint": {queue},
		"RawMessageDelivery": {"true"}, "FilterPolicy": {`{"kind":["order"]}`}})
	subs := c.get("/_pogi/resources/sns?tab=Subscriptions")
	want(t, subs, topic+":", queue)
	out := call(t, c.srv, "AmazonSimpleNotificationService", "ListSubscriptionsByTopic", map[string]any{"TopicArn": topic})
	sub := out["Subscriptions"].([]any)[0].(map[string]any)["SubscriptionArn"].(string)
	attrs := call(t, c.srv, "AmazonSimpleNotificationService", "GetSubscriptionAttributes", map[string]any{"SubscriptionArn": sub})["Attributes"].(map[string]any)
	if attrs["RawMessageDelivery"] != "true" || attrs["FilterPolicy"] != `{"kind":["order"]}` {
		t.Fatalf("attributes: %v", attrs)
	}

	c.bulkDelete("sns", "Subscriptions", sub)
	wantNot(t, c.get("/_pogi/resources/sns?tab=Subscriptions"), queue)
}

func TestEventBridgeRuleTargets(t *testing.T) {
	c := start(t, "events", "sqs")
	call(t, c.srv, "AmazonSQS", "CreateQueue", map[string]any{"QueueName": "sink"})
	queue := "arn:aws:sqs:us-east-1:000000000000:sink"
	rule := url.Values{"Name": {"orders"}, "EventPattern": {`{"source":["shop"]}`}}

	// a target without an ARN, or with bad input, stops everything: no half-made rule
	rule.Set("Targets.0.Id", "t1")
	body, _ := c.post("/_pogi/new/events/PutRule", rule)
	want(t, body, "Target 1 needs an Id and an ARN")
	rule.Set("Targets.0.Arn", queue)
	rule.Set("Targets.0.Input", "{")
	body, _ = c.post("/_pogi/new/events/PutRule", rule)
	want(t, body, "Target 1 input is not valid JSON")
	wantNot(t, c.get("/_pogi/resources/events?tab=Rules"), ">orders<")

	rule.Set("Targets.0.Input", `{"fixed":true}`)
	c.ok("/_pogi/new/events/PutRule", rule)
	want(t, c.get("/_pogi/resources/events?tab=Rules"), ">orders<")
	out := call(t, c.srv, "AWSEvents", "ListTargetsByRule", map[string]any{"Rule": "orders"})
	ts := out["Targets"].([]any)
	if len(ts) != 1 || ts[0].(map[string]any)["Arn"] != queue || ts[0].(map[string]any)["Input"] != `{"fixed":true}` {
		t.Fatalf("targets: %v", ts)
	}

	// the rule fires into the queue
	call(t, c.srv, "AWSEvents", "PutEvents", map[string]any{"Entries": []any{map[string]any{"Source": "shop", "DetailType": "x", "Detail": "{}"}}})
	var got map[string]any
	for i := 0; i < 50 && got["Messages"] == nil; i++ {
		got = call(t, c.srv, "AmazonSQS", "ReceiveMessage", map[string]any{"QueueUrl": "http://localhost/000000000000/sink", "MaxNumberOfMessages": 1})
		time.Sleep(20 * time.Millisecond)
	}
	if got["Messages"] == nil {
		t.Fatal("the rule did not deliver to its target")
	}

	// delete removes the targets first, then the rule
	c.bulkDelete("events", "Rules", "orders")
	wantNot(t, c.get("/_pogi/resources/events?tab=Rules"), ">orders<")
}
