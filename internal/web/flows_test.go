package web_test

import (
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// aws sends an SNS or SQS query-protocol call like an SDK would, and returns the body.
func (c *console) aws(service string, trace string, v url.Values) string {
	c.t.Helper()
	req, _ := http.NewRequest("POST", c.base+"/", strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+service+"/aws4_request, SignedHeaders=host, Signature=x")
	if trace != "" {
		req.Header.Set("X-Amzn-Trace-Id", "Root="+trace)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		c.t.Fatalf("%s %v = %d: %s", service, v.Get("Action"), res.StatusCode, b)
	}
	return string(b)
}

// wire creates topic orders with a queue jobs subscribed to it, and returns their ARNs.
func (c *console) wire() (topic, queue string) {
	c.t.Helper()
	topic = "arn:aws:sns:us-east-1:000000000000:orders"
	queue = "arn:aws:sqs:us-east-1:000000000000:jobs"
	c.aws("sns", "", url.Values{"Action": {"CreateTopic"}, "Name": {"orders"}})
	c.aws("sqs", "", url.Values{"Action": {"CreateQueue"}, "QueueName": {"jobs"}})
	c.aws("sns", "", url.Values{"Action": {"Subscribe"}, "TopicArn": {topic}, "Protocol": {"sqs"}, "Endpoint": {queue}})
	return topic, queue
}

func TestFlowsFromSNSPublish(t *testing.T) {
	c := start(t, "sns", "sqs")
	topic, _ := c.wire()
	const trace = "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	c.aws("sns", trace, url.Values{"Action": {"Publish"}, "TopicArn": {topic}, "Message": {"hello"}})

	page := c.get("/_pogi/flows")
	want(t, page, "Flows", "Publish", "SNS", "SQS", `data-drawer="/_pogi/flows/`+trace+`"`, `hx-get="/_pogi/flows?frag=1"`)
	if m := regexp.MustCompile(`(?s)<td class="num">(\d+)</td>`).FindStringSubmatch(page); m == nil || m[1] != "2" {
		t.Fatalf("a publish reaching one queue is 2 steps: %v", m)
	}
	// a request with no second step is not a flow
	wantNot(t, page, "CreateTopic", "Subscribe")

	// the poll swaps rows only
	rows := c.get("/_pogi/flows?frag=1")
	want(t, rows, "Publish")
	wantNot(t, rows, "<html", "<form")
	wantNot(t, c.get("/_pogi/flows?f=failed"), trace)
	want(t, c.get("/_pogi/flows?q=jobs"), trace)

	// the drawer: waterfall with the request step opening in the drawer
	d := c.get("/_pogi/flows/" + trace)
	want(t, d, "Publish", "deliver → jobs", `class="wf"`, `data-drawer="/_pogi/requests/`, "/_pogi/map?trace="+trace)
	if strings.Contains(d, `class="banner warn"`) {
		t.Fatalf("an ok flow has no failure banner")
	}

	// opened from a row it is the drawer: header and body, no layout
	dr := c.drawer("/_pogi/flows/" + trace)
	want(t, dr, `<div class="eyebrow">Flow · trace `+trace, `<div class="dbody">`, `class="wf"`)
	wantNot(t, dr, "<html", `id="drawer"`)

	// the same flow when the delivery fails: a missing queue
	c.srv.Hub.Spans.Add(core.Span{Trace: trace + "-bad", Time: time.Now(), Service: "sqs", Kind: core.SpanDeliver, From: topic, To: "arn:aws:sqs:us-east-1:000000000000:gone", Err: "queue does not exist"})
	c.srv.Hub.Spans.Add(core.Span{Trace: trace + "-bad", Time: time.Now().Add(time.Millisecond), Service: "lambda", Kind: core.SpanInvoke, Op: "Invoke worker", From: topic, To: "arn:aws:lambda:us-east-1:000000000000:function:worker"})
	want(t, c.get("/_pogi/flows?f=failed"), trace+"-bad", `st bad`)
	want(t, c.get("/_pogi/flows/"+trace+"-bad"), `class="banner warn"`, "queue does not exist")
}
