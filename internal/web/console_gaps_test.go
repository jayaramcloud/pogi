package web_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

func (c *console) cookie(name, value string) {
	u, _ := url.Parse(c.base)
	c.c.Jar.SetCookies(u, []*http.Cookie{{Name: name, Value: value, Path: "/"}})
}

func TestLambdaCreateFollowsScope(t *testing.T) {
	c := start(t, "lambda")
	c.cookie("pogi-account", "111122223333")
	c.cookie("pogi-region", "eu-west-1")
	want(t, c.get("/_pogi/new/lambda/CreateFunction"), "arn:aws:iam::111122223333:role/lambda")
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, _ := zw.Create("app.py")
	f.Write([]byte("def handler(e, c): return 1"))
	zw.Close()
	c.post("/_pogi/new/lambda/CreateFunction", url.Values{"FunctionName": {"scoped"}, "Runtime": {"python3.13"}, "Handler": {"app.handler"},
		"Role": {"arn:aws:iam::111122223333:role/lambda"}, "ZipFile": {base64.StdEncoding.EncodeToString(buf.Bytes())}})
	page := c.get("/_pogi/lambda/111122223333/eu-west-1/scoped") // 404 if it was made in another account or region
	want(t, page, "eu-west-1", "111122223333")
}

func TestSQSSendDelayAttributesAndFIFO(t *testing.T) {
	c := start(t, "sqs")
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs"}})
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"jobs.fifo"}, "Attributes.0.key": {"FifoQueue"}, "Attributes.0.value": {"true"}})
	base := "/_pogi/sqs/" + core.DefaultAccount + "/" + core.DefaultRegion + "/"
	want(t, c.get(base+"jobs"), `name="delay"`, `name="ak4"`, "Same thing from the CLI", `hx-post="`+base+`jobs/cli"`)
	wantNot(t, c.get(base+"jobs"), `name="dedup"`)
	want(t, c.get(base+"jobs.fifo"), `name="group"`, `name="dedup"`)

	c.post(base+"jobs/send", url.Values{"body": {"hi"}, "delay": {"60"}, "ak0": {"color"}, "av0": {"red"}})
	page := c.get(base + "jobs")
	want(t, page, "hi", "delayed", ">1<")
	cli, _ := c.post(base+"jobs/cli", url.Values{"body": {"hi"}, "delay": {"60"}, "ak0": {"color"}, "av0": {"red"}})
	want(t, cli, "--delay-seconds 60", "--message-attributes", "color")

	c.post(base+"jobs.fifo/send", url.Values{"body": {"f1"}, "group": {"g1"}, "dedup": {"d1"}})
	want(t, c.get(base+"jobs.fifo"), "f1")
	cli, _ = c.post(base+"jobs.fifo/cli", url.Values{"body": {"f1"}, "group": {"g1"}, "dedup": {"d1"}})
	want(t, cli, "--message-group-id g1", "--message-deduplication-id d1")
	bad, _ := c.post(base+"jobs/cli", url.Values{"body": {"x"}, "delay": {"901"}})
	want(t, bad, "0 to 900")
}

func TestClearInbox(t *testing.T) {
	c := start(t, "sns")
	topic := new(struct{ TopicArn string })
	if err := c.srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, "AmazonSimpleNotificationService", "CreateTopic", map[string]any{"Name": "t"}, topic); err != nil {
		t.Fatal(err)
	}
	if err := c.srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, "AmazonSimpleNotificationService", "Publish",
		map[string]any{"PhoneNumber": "+15550100", "Message": "otp 1234"}, nil); err != nil {
		t.Fatal(err)
	}
	want(t, c.get("/_pogi/inbox"), "otp 1234", "Clear inbox")
	c.post("/_pogi/inbox/clear", nil)
	want(t, c.get("/_pogi/inbox"), "Nothing sent yet", "Clear inbox")
}

func TestResourceDrawerShowsTags(t *testing.T) {
	c := start(t, "sqs", "sns")
	if err := c.srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, "AmazonSQS", "CreateQueue",
		map[string]any{"QueueName": "tagged", "tags": map[string]string{"team": "core"}}, nil); err != nil {
		t.Fatal(err)
	}
	c.ok("/_pogi/new/sqs/CreateQueue", url.Values{"QueueName": {"plain"}})
	want(t, c.get("/_pogi/resources/sqs/Queues/tagged"), "Tags", "team", "core")
	want(t, c.get("/_pogi/resources/sqs/Queues/plain"), "No tags")
}

func TestNotificationsBaseline(t *testing.T) {
	c := start(t, "sqs")
	req, _ := http.NewRequest("POST", c.base+"/", strings.NewReader("{}")) // an error on record: no such operation
	req.Header.Set("X-Amz-Target", "AmazonSQS.Nope")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	res, err = http.Get(c.base + "/_pogi/notes/poll")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	var seen bool
	for _, ck := range res.Cookies() {
		seen = seen || ck.Name == "pogi-notes-seen"
	}
	if !seen {
		t.Fatal("the first poll does not set pogi-notes-seen")
	}
	body := c.get("/_pogi/notes/poll") // the jar now holds the cookie
	want(t, body, `"unread":0`)
}
