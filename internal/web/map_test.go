package web_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// do sends a raw request signed as service and returns the body.
func (c *console) do(method, path, service, body string) string {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.base+path, strings.NewReader(body))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+service+"/aws4_request, SignedHeaders=host, Signature=x")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		c.t.Fatalf("%s %s = %d: %s", method, path, res.StatusCode, b)
	}
	return string(b)
}

func TestMapLinksAndObservedEdges(t *testing.T) {
	c := start(t, "sns", "sqs", "s3", "lambda", "events")
	want(t, c.get("/_pogi/map"), "Nothing is wired yet", "pogi aws sns subscribe")

	topic, queue := c.wire() // SNS subscription
	dlq := "arn:aws:sqs:us-east-1:000000000000:jobs-dlq"
	c.aws("sqs", "", url.Values{"Action": {"CreateQueue"}, "QueueName": {"jobs-dlq"}})
	c.aws("sqs", "", url.Values{"Action": {"SetQueueAttributes"}, "QueueUrl": {c.base + "/000000000000/jobs"}, "Attribute.1.Name": {"RedrivePolicy"},
		"Attribute.1.Value": {`{"deadLetterTargetArn":"` + dlq + `","maxReceiveCount":2}`}}) // SQS redrive

	c.do("PUT", "/uploads", "s3", "") // S3 notification
	c.do("PUT", "/uploads?notification", "s3", `<NotificationConfiguration><QueueConfiguration><Id>n</Id><Queue>`+queue+
		`</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`)

	var zbuf bytes.Buffer // Lambda event source mapping
	zw := zip.NewWriter(&zbuf)
	f, _ := zw.Create("app.py")
	io.WriteString(f, "def handler(e, c):\n    return 1\n")
	zw.Close()
	fn := "arn:aws:lambda:us-east-1:000000000000:function:worker"
	c.do("POST", "/2015-03-31/functions", "lambda", mustJSON(map[string]any{"FunctionName": "worker", "Runtime": "python3.12", "Handler": "app.handler",
		"Role": "arn:aws:iam::000000000000:role/r", "Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString(zbuf.Bytes())}}))
	c.do("POST", "/2015-03-31/event-source-mappings", "lambda", mustJSON(map[string]any{"FunctionName": "worker", "EventSourceArn": dlq, "Enabled": false}))

	// EventBridge rule target
	if err := c.srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, "AWSEvents", "PutRule", map[string]any{"Name": "nightly", "ScheduleExpression": "rate(1 day)"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, "AWSEvents", "PutTargets", map[string]any{"Rule": "nightly", "Targets": []any{map[string]any{"Id": "1", "Arn": fn}}}, nil); err != nil {
		t.Fatal(err)
	}
	c.srv.Hub.Spans.Add(core.Span{Trace: "t1", Time: time.Now().Add(-time.Millisecond), Service: "sqs", Kind: core.SpanDeliver, From: topic, To: queue})
	c.srv.Hub.Spans.Add(core.Span{Trace: "t1", Time: time.Now(), Service: "lambda", Kind: core.SpanInvoke, Op: "Invoke worker", From: queue, To: fn + ":1", Err: "boom"})

	page := c.get("/_pogi/map")
	want(t, page, "SNS topic", "SQS queue", "S3 bucket", "EB rule", "Lambda", "subscription", "notification", "event source", "target", "redrive",
		`class="edge dlq"`, `class="edge observed"`, `class="pulse bad"`, "1 error", `data-drawer="/_pogi/map/node?id=`+url.QueryEscape(topic)+`"`, "Highlight a resource")
	wantNot(t, page, "Nothing is wired yet")

	// the node drawer: inbound, outbound, kinds
	d := c.get("/_pogi/map/node?id=" + url.QueryEscape(queue))
	want(t, d, "Receives from", "Sends to", "SNS topic", "subscription", "S3 bucket", "notification", "seen at runtime", "/_pogi/resources/sqs")
	want(t, c.drawer("/_pogi/map/node?id="+url.QueryEscape(dlq)), `<div class="eyebrow">SQS queue`, "dead-letter", "event source")
	want(t, c.get("/_pogi/map/node?id="+url.QueryEscape(fn)), "1 error", "/_pogi/flows/t1", `st bad`)

	// fragments and filters
	frag := c.get("/_pogi/map?frag=1")
	want(t, frag, "<svg")
	wantNot(t, frag, "<form")
	want(t, c.get("/_pogi/map?hl=nomatch"), `class="node dim"`)
	c.cookie("pogi-cloud", "gcp")
	wantNot(t, c.get("/_pogi/map"), "SQS queue")
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Nodes show what they hold, and the map downloads as a standalone SVG.
func TestMapCountsAndExport(t *testing.T) {
	c := start(t, "sns", "sqs")
	c.wire()
	c.aws("sqs", "", url.Values{"Action": {"SendMessage"}, "QueueUrl": {c.base + "/000000000000/jobs"}, "MessageBody": {"hi"}})
	page := c.get("/_pogi/map")
	want(t, page, `class="ct"`, "1 msg<", "1 sub<")
	res, err := http.Get(c.base + "/_pogi/map.svg")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if cd := res.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") || !strings.Contains(cd, ".svg") {
		t.Fatalf("Content-Disposition = %q", cd)
	}
	want(t, string(b), `xmlns="http://www.w3.org/2000/svg"`, "<style>", "#ff9900", "SQS queue")
	wantNot(t, string(b), "var(--")
}
