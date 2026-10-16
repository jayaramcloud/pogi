package sqs_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
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

func setup(t *testing.T) (*sqs.Client, string) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return sqssvc.New(h) })
	return sqs.NewFromConfig(awstest.Config(t, url)), url
}

func TestSendReceiveDelete(t *testing.T) {
	c, _ := setup(t)
	q := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs"), Attributes: map[string]string{"VisibilityTimeout": "1"}}))
	if _, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs"), Attributes: map[string]string{"VisibilityTimeout": "9"}}); err == nil {
		t.Fatal("recreating with different attributes must fail")
	}
	got := must(c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("jobs")}))
	if *got.QueueUrl != *q.QueueUrl {
		t.Fatalf("url %s != %s", *got.QueueUrl, *q.QueueUrl)
	}
	_, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("nope")})
	var dne *types.QueueDoesNotExist
	if !errors.As(err, &dne) {
		t.Fatalf("want QueueDoesNotExist, got %v", err)
	}

	// The SDK validates the body and attribute MD5s on send and receive.
	must(c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("hello"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"kind": {DataType: aws.String("String"), StringValue: aws.String("greeting")},
			"n":    {DataType: aws.String("Number"), StringValue: aws.String("42")},
			"raw":  {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2, 3}},
		}}))
	r := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}}))
	if len(r.Messages) != 1 || *r.Messages[0].Body != "hello" || *r.Messages[0].MessageAttributes["kind"].StringValue != "greeting" {
		t.Fatalf("receive = %+v", r.Messages)
	}
	if r.Messages[0].Attributes["ApproximateReceiveCount"] != "1" {
		t.Fatalf("attrs = %v", r.Messages[0].Attributes)
	}
	if again := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl})); len(again.Messages) != 0 {
		t.Fatal("message should be invisible")
	}
	time.Sleep(1100 * time.Millisecond)
	back := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl}))
	if len(back.Messages) != 1 {
		t.Fatal("message should be visible again after the visibility timeout")
	}
	must(c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: back.Messages[0].ReceiptHandle}))
	attrs := must(c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []types.QueueAttributeName{"All"}}))
	if attrs.Attributes["ApproximateNumberOfMessages"] != "0" || attrs.Attributes["QueueArn"] != "arn:aws:sqs:us-east-1:000000000000:jobs" {
		t.Fatalf("attributes = %v", attrs.Attributes)
	}
}

func TestLongPollAndBatch(t *testing.T) {
	c, _ := setup(t)
	q := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("poll")}))
	go func() {
		time.Sleep(300 * time.Millisecond)
		c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("late")})
	}()
	start := time.Now()
	r := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, WaitTimeSeconds: 5}))
	if len(r.Messages) != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("long poll got %d messages after %v", len(r.Messages), time.Since(start))
	}
	var entries []types.SendMessageBatchRequestEntry
	for _, id := range []string{"a", "b", "c"} {
		entries = append(entries, types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String("m-" + id)})
	}
	sent := must(c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: q.QueueUrl, Entries: entries}))
	if len(sent.Successful) != 3 {
		t.Fatalf("batch sent %d", len(sent.Successful))
	}
	got := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10}))
	var del []types.DeleteMessageBatchRequestEntry
	for i, m := range got.Messages {
		del = append(del, types.DeleteMessageBatchRequestEntry{Id: aws.String(string(rune('a' + i))), ReceiptHandle: m.ReceiptHandle})
	}
	if out := must(c.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: q.QueueUrl, Entries: del})); len(out.Successful) != 3 {
		t.Fatalf("batch deleted %d", len(out.Successful))
	}
}

func TestFIFOAndDeadLetters(t *testing.T) {
	c, _ := setup(t)
	f := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("orders.fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}}))
	for _, body := range []string{"1", "2", "2", "3"} { // the second "2" is deduplicated
		must(c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: f.QueueUrl, MessageBody: aws.String(body), MessageGroupId: aws.String("g")}))
	}
	r := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: f.QueueUrl, MaxNumberOfMessages: 10}))
	var bodies []string
	for _, m := range r.Messages {
		bodies = append(bodies, *m.Body)
	}
	if strings.Join(bodies, "") != "123" {
		t.Fatalf("fifo order = %v", bodies)
	}

	dlq := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("dead")}))
	src := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("work"), Attributes: map[string]string{
		"VisibilityTimeout": "0",
		"RedrivePolicy":     `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dead","maxReceiveCount":"2"}`,
	}}))
	must(c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: src.QueueUrl, MessageBody: aws.String("poison")}))
	for range 3 {
		c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: src.QueueUrl})
	}
	d := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: dlq.QueueUrl}))
	if len(d.Messages) != 1 || *d.Messages[0].Body != "poison" {
		t.Fatalf("dead letters = %+v", d.Messages)
	}
	srcs := must(c.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: dlq.QueueUrl}))
	if len(srcs.QueueUrls) != 1 {
		t.Fatalf("dlq sources = %v", srcs.QueueUrls)
	}
	must(c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: dlq.QueueUrl, ReceiptHandle: d.Messages[0].ReceiptHandle, VisibilityTimeout: 0}))
	must(c.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{SourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:dead")}))
	attrs := must(c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: src.QueueUrl, AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages"}}))
	if attrs.Attributes["ApproximateNumberOfMessages"] != "1" {
		t.Fatalf("redrive back = %v", attrs.Attributes)
	}
}

// Old SDKs and hand-written clients still speak the Query protocol.
func TestQueryProtocol(t *testing.T) {
	c, base := setup(t)
	q := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("legacy")}))
	post := func(form url.Values) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, *q.QueueUrl, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		awstest.Sign(t, req, "sqs")
		res := must(http.DefaultClient.Do(req))
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	st, body := post(url.Values{"Action": {"SendMessage"}, "MessageBody": {"via query"},
		"MessageAttribute.1.Name": {"k"}, "MessageAttribute.1.Value.DataType": {"String"}, "MessageAttribute.1.Value.StringValue": {"v"}})
	if st != 200 || !strings.Contains(body, "<SendMessageResponse") || !strings.Contains(body, "<MD5OfMessageBody>") {
		t.Fatalf("send: %d %s", st, body)
	}
	_, body = post(url.Values{"Action": {"ReceiveMessage"}, "MessageAttributeName.1": {"All"}, "AttributeName.1": {"All"}})
	var res struct {
		Messages []struct {
			Body  string `xml:"Body"`
			Attrs []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"Attribute"`
			MsgAttrs []struct {
				Name  string `xml:"Name"`
				Value struct {
					StringValue string `xml:"StringValue"`
				} `xml:"Value"`
			} `xml:"MessageAttribute"`
		} `xml:"ReceiveMessageResult>Message"`
	}
	if err := xml.Unmarshal([]byte(body), &res); err != nil || len(res.Messages) != 1 || res.Messages[0].Body != "via query" ||
		len(res.Messages[0].MsgAttrs) != 1 || res.Messages[0].MsgAttrs[0].Value.StringValue != "v" || len(res.Messages[0].Attrs) < 4 {
		t.Fatalf("receive: %v %s", err, body)
	}
	st, body = post(url.Values{"Action": {"GetQueueUrl"}, "QueueName": {"missing"}})
	if st != 400 || !strings.Contains(body, "AWS.SimpleQueueService.NonExistentQueue") {
		t.Fatalf("error: %d %s", st, body)
	}
	res2, _ := http.Get(base + "/_aws/sqs/messages?QueueUrl=" + url.QueryEscape(*q.QueueUrl))
	if b, _ := io.ReadAll(res2.Body); !strings.Contains(string(b), "via query") {
		t.Fatalf("peek: %s", b)
	}
}

// floci #1277 and #894: messages and batches may be 1 MiB; a batch over that is refused whole.
// floci #779: AddPermission stores a statement in the queue policy and refuses a repeated label.
func TestSizeLimitsAndPermissions(t *testing.T) {
	c, _ := setup(t)
	q := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("big")})).QueueUrl
	entry := func(id string, n int) types.SendMessageBatchRequestEntry {
		return types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String(strings.Repeat("x", n))}
	}
	if out, err := c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: q,
		Entries: []types.SendMessageBatchRequestEntry{entry("a", 263000), entry("b", 263000), entry("c", 263000)}}); err != nil || len(out.Failed) != 0 {
		t.Fatalf("789 KB batch: %v %+v", err, out)
	}
	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q, MessageBody: aws.String(strings.Repeat("x", 300000))}); err != nil {
		t.Fatalf("300 KB message: %v", err)
	}
	_, err := c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: q,
		Entries: []types.SendMessageBatchRequestEntry{entry("a", 262000), entry("b", 262000), entry("c", 262000), entry("d", 262000), entry("e", 262000)}})
	var tooLong *types.BatchRequestTooLong
	if !errors.As(err, &tooLong) {
		t.Fatalf("1.3 MB batch: %v", err)
	}

	add := &sqs.AddPermissionInput{QueueUrl: q, Label: aws.String("l1"), AWSAccountIds: []string{"111122223333"}, Actions: []string{"SendMessage"}}
	must(c.AddPermission(ctx, add))
	pol := must(c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q, AttributeNames: []types.QueueAttributeName{"Policy"}})).Attributes["Policy"]
	if !strings.Contains(pol, `"Sid":"l1"`) || !strings.Contains(pol, "SQS:SendMessage") || !strings.Contains(pol, "arn:aws:iam::111122223333:root") {
		t.Fatalf("policy = %s", pol)
	}
	if _, err := c.AddPermission(ctx, add); err == nil {
		t.Fatal("a repeated label was accepted")
	}
	must(c.RemovePermission(ctx, &sqs.RemovePermissionInput{QueueUrl: q, Label: aws.String("l1")}))
	if _, err := c.RemovePermission(ctx, &sqs.RemovePermissionInput{QueueUrl: q, Label: aws.String("l1")}); err == nil {
		t.Fatal("removing an unknown label succeeded")
	}
	if pol := must(c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q, AttributeNames: []types.QueueAttributeName{"Policy"}})).Attributes["Policy"]; pol != "" {
		t.Fatalf("policy after removal = %s", pol)
	}
}

// floci #4829: SenderId of an assumed-role session is <role id>:<session name>.
// floci #58: DeleteMessage with a malformed receipt handle is ReceiptHandleIsInvalid.
func TestSenderIDAndReceiptHandle(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return sqssvc.New(h) })
	h.AddIdentity("ASIATESTSESSION", core.Identity{Account: "000000000000", ARN: "arn:aws:sts::000000000000:assumed-role/r1/sess1", UserID: "AROATESTROLE:sess1", Secret: "s"})
	c := sqs.NewFromConfig(awstest.Config(t, url))
	q := must(c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sender")}))
	as := sqs.NewFromConfig(awstest.ConfigAs(t, url, "ASIATESTSESSION", "s", ""))
	must(as.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("x")}))
	r := must(c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MessageSystemAttributeNames: []types.MessageSystemAttributeName{"SenderId"}}))
	if got := r.Messages[0].Attributes["SenderId"]; got != "AROATESTROLE:sess1" {
		t.Fatalf("SenderId = %q", got)
	}
	_, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: aws.String("bogus")})
	if !strings.Contains(err.Error(), "ReceiptHandleIsInvalid") {
		t.Fatalf("bogus handle: %v", err)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: r.Messages[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: q.QueueUrl, ReceiptHandle: r.Messages[0].ReceiptHandle}); err != nil {
		t.Fatalf("stale handle must still succeed: %v", err)
	}
}
