package sns_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	snssvc "github.com/nrndev/pogi/internal/aws/sns"
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

type env struct {
	hub *core.Hub
	sns *sns.Client
	sqs *sqs.Client
	url string
}

func setup(t *testing.T) *env {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return snssvc.New(h) }, func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	return &env{h, sns.NewFromConfig(cfg), sqs.NewFromConfig(cfg), url}
}

func (e *env) svc() *snssvc.Service { return e.hub.Service("sns").(*snssvc.Service) }

func (e *env) topic(name string, attrs map[string]string) string {
	return *must(e.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(name), Attributes: attrs})).TopicArn
}

// queue creates a queue, subscribes it to topic and returns its URL.
func (e *env) queue(topic, name string, attrs map[string]string) string {
	qa := map[string]string{}
	if strings.HasSuffix(name, ".fifo") {
		qa["FifoQueue"] = "true"
	}
	q := must(e.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: qa}))
	must(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic), Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:" + name), Attributes: attrs}))
	return *q.QueueUrl
}

func (e *env) receive(queueURL string) []sqstypes.Message {
	return must(e.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10,
		MessageAttributeNames: []string{"All"}, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"All"}})).Messages
}

func (e *env) topicAttrs(arn string) map[string]string {
	return must(e.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})).Attributes
}

func bodies(msgs []sqstypes.Message) []string {
	var out []string
	for _, m := range msgs {
		out = append(out, *m.Body)
	}
	return out
}

func str(dataType, v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String(dataType), StringValue: aws.String(v)}
}

func getJSON(t *testing.T, u string, v any) {
	res := must(http.Get(u))
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func TestTopics(t *testing.T) {
	e := setup(t)
	arn := e.topic("orders", map[string]string{"DisplayName": "Orders"})
	if arn != "arn:aws:sns:us-east-1:000000000000:orders" || e.topic("orders", map[string]string{"DisplayName": "Orders"}) != arn {
		t.Fatalf("arn = %s (CreateTopic must be idempotent)", arn)
	}
	var inv *types.InvalidParameterException
	if _, err := e.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("orders"), Attributes: map[string]string{"DisplayName": "Other"}}); !errors.As(err, &inv) {
		t.Fatalf("recreating with other attributes: %v", err)
	}
	if _, err := e.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("bad.name")}); !errors.As(err, &inv) {
		t.Fatalf("bad name: %v", err)
	}

	must(e.sns.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{TopicArn: aws.String(arn), AttributeName: aws.String("DisplayName"), AttributeValue: aws.String("Shop")}))
	a := e.topicAttrs(arn)
	if a["DisplayName"] != "Shop" || a["TopicArn"] != arn || a["Owner"] != "000000000000" || a["SubscriptionsConfirmed"] != "0" ||
		!strings.Contains(a["Policy"], `"Resource":"`+arn+`"`) || !strings.Contains(a["EffectiveDeliveryPolicy"], "defaultHealthyRetryPolicy") {
		t.Fatalf("attributes = %v", a)
	}

	must(e.sns.AddPermission(ctx, &sns.AddPermissionInput{TopicArn: aws.String(arn), Label: aws.String("partner"),
		AWSAccountId: []string{"111122223333"}, ActionName: []string{"Publish"}}))
	if p := e.topicAttrs(arn)["Policy"]; !strings.Contains(p, `"Sid":"partner"`) || !strings.Contains(p, "arn:aws:iam::111122223333:root") || !strings.Contains(p, "SNS:Publish") {
		t.Fatalf("policy after AddPermission = %s", p)
	}
	must(e.sns.RemovePermission(ctx, &sns.RemovePermissionInput{TopicArn: aws.String(arn), Label: aws.String("partner")}))
	if p := e.topicAttrs(arn)["Policy"]; strings.Contains(p, "partner") || !strings.Contains(p, "__default_statement_ID") {
		t.Fatalf("policy after RemovePermission = %s", p)
	}

	must(e.sns.TagResource(ctx, &sns.TagResourceInput{ResourceArn: aws.String(arn), Tags: []types.Tag{
		{Key: aws.String("team"), Value: aws.String("core")}, {Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(e.sns.UntagResource(ctx, &sns.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}))
	if tags := must(e.sns.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(arn)})).Tags; len(tags) != 1 || *tags[0].Key != "team" || *tags[0].Value != "core" {
		t.Fatalf("tags = %+v", tags)
	}
	var rnf *types.ResourceNotFoundException
	if _, err := e.sns.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(arn + "-nope")}); !errors.As(err, &rnf) {
		t.Fatalf("tags of a missing topic: %v", err)
	}

	for i := range 101 {
		e.topic(fmt.Sprintf("t%03d", i), nil)
	}
	p1 := must(e.sns.ListTopics(ctx, &sns.ListTopicsInput{}))
	p2 := must(e.sns.ListTopics(ctx, &sns.ListTopicsInput{NextToken: p1.NextToken}))
	if len(p1.Topics) != 100 || p1.NextToken == nil || len(p2.Topics) != 2 || p2.NextToken != nil {
		t.Fatalf("pages of %d and %d", len(p1.Topics), len(p2.Topics))
	}

	must(e.sns.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)}))
	must(e.sns.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)})) // idempotent
	var nf *types.NotFoundException
	if _, err := e.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)}); !errors.As(err, &nf) {
		t.Fatalf("want NotFound, got %v", err)
	}
	if _, err := e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("x")}); !errors.As(err, &nf) {
		t.Fatalf("publish to a missing topic: %v", err)
	}
}

func TestFanoutToSQS(t *testing.T) {
	e := setup(t)
	arn := e.topic("events", nil)
	wrapped := e.queue(arn, "wrapped", nil)
	raw := e.queue(arn, "raw", map[string]string{"RawMessageDelivery": "true"})
	pub := must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("hello"), Subject: aws.String("greeting"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"kind": str("String", "order"), "n": str("Number", "42"),
			"bin": {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2}},
		}}))

	msgs := e.receive(wrapped)
	var env struct {
		Type, MessageId, TopicArn, Subject, Message, Timestamp, SignatureVersion, Signature, SigningCertURL, UnsubscribeURL string
		MessageAttributes                                                                                                   map[string]struct{ Type, Value string }
	}
	if len(msgs) != 1 || json.Unmarshal([]byte(*msgs[0].Body), &env) != nil {
		t.Fatalf("wrapped = %v", bodies(msgs))
	}
	if env.Type != "Notification" || env.MessageId != *pub.MessageId || env.TopicArn != arn || env.Subject != "greeting" || env.Message != "hello" ||
		env.SignatureVersion != "1" || env.Signature == "" || env.SigningCertURL == "" || env.MessageAttributes["n"] != (struct{ Type, Value string }{"Number", "42"}) ||
		env.MessageAttributes["bin"].Value != "AQI=" || !strings.HasPrefix(env.UnsubscribeURL, e.url+"/?Action=Unsubscribe&SubscriptionArn="+arn+":") {
		t.Fatalf("envelope = %s", *msgs[0].Body)
	}
	if _, err := time.Parse(time.RFC3339, env.Timestamp); err != nil || len(msgs[0].MessageAttributes) != 0 {
		t.Fatalf("timestamp %q, queue attributes %v", env.Timestamp, msgs[0].MessageAttributes)
	}

	// Raw delivery hands the message and its attributes to SQS as they are.
	msgs = e.receive(raw)
	if len(msgs) != 1 || *msgs[0].Body != "hello" || *msgs[0].MessageAttributes["kind"].StringValue != "order" ||
		*msgs[0].MessageAttributes["n"].DataType != "Number" || len(msgs[0].MessageAttributes["bin"].BinaryValue) != 2 {
		t.Fatalf("raw = %+v", msgs)
	}

	// Other services publish through the hub; the console has its own helper.
	if err := e.hub.Deliver(arn, &core.Message{Body: "from s3", Subject: "Amazon S3 Notification"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc().PublishConsole("000000000000", "us-east-1", arn, "", "from console"); err != nil {
		t.Fatal(err)
	}
	if got := bodies(e.receive(raw)); strings.Join(got, ",") != "from s3,from console" {
		t.Fatalf("raw = %v", got)
	}
	msgs = e.receive(wrapped)
	if len(msgs) != 2 || json.Unmarshal([]byte(*msgs[0].Body), &env) != nil || env.Subject != "Amazon S3 Notification" || !strings.HasPrefix(env.UnsubscribeURL, "http://localhost:7644/") {
		t.Fatalf("hub delivery = %v", bodies(msgs))
	}

	// State survives a snapshot round trip.
	snap := must(e.hub.Snapshot())
	e.hub.Reset()
	if n := len(must(e.sns.ListTopics(ctx, &sns.ListTopicsInput{})).Topics); n != 0 {
		t.Fatalf("%d topics after reset", n)
	}
	if err := e.hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("after restore")}))
	if got := bodies(e.receive(raw)); len(got) != 1 || got[0] != "after restore" {
		t.Fatalf("after restore = %v", got)
	}

	tables := e.svc().Resources()
	if len(tables) != 2 || len(tables[0].Rows) != 1 || tables[0].Rows[0][3] != "2" || len(tables[1].Rows) != 2 {
		t.Fatalf("resources = %+v", tables)
	}
	if v, ok := e.svc().Inspect("Topics", "events").(map[string]any); !ok || len(v["subscriptions"].([]map[string]string)) != 2 {
		t.Fatalf("inspect = %v", v)
	}

	subs := must(e.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(arn)})).Subscriptions
	all := must(e.sns.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})).Subscriptions
	if len(subs) != 2 || len(all) != 2 || *subs[0].Protocol != "sqs" || *subs[0].Owner != "000000000000" {
		t.Fatalf("subscriptions = %+v", subs)
	}
	must(e.sns.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: subs[0].SubscriptionArn}))
	if a := e.topicAttrs(arn); a["SubscriptionsConfirmed"] != "1" || a["SubscriptionsDeleted"] != "1" {
		t.Fatalf("counts = %v", a)
	}
	var nf *types.NotFoundException
	if _, err := e.sns.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: subs[0].SubscriptionArn}); !errors.As(err, &nf) {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestFilterPolicies(t *testing.T) {
	e := setup(t)
	arn := e.topic("prices", nil)
	sub := func(name, policy, scope string) string {
		attrs := map[string]string{"RawMessageDelivery": "true", "FilterPolicy": policy}
		if scope != "" {
			attrs["FilterPolicyScope"] = scope
		}
		return e.queue(arn, name, attrs)
	}
	expensive := sub("expensive", `{"price":[{"numeric":[">",100]}]}`, "")
	europe := sub("europe", `{"store":[{"prefix":"eu-"}]}`, "")
	notRed := sub("not-red", `{"color":[{"anything-but":["red"]}]}`, "")
	tagged := sub("tagged", `{"tags":["b"]}`, "")
	orders := sub("orders", `{"kind":["order"]}`, "MessageBody")

	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String(`{"kind":"order"}`), MessageAttributes: map[string]types.MessageAttributeValue{
		"price": str("Number", "150"), "store": str("String", "eu-west"), "color": str("String", "red"), "tags": str("String.Array", `["a","b"]`)}}))
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("plain"), MessageAttributes: map[string]types.MessageAttributeValue{
		"price": str("Number", "50"), "store": str("String", "us-east"), "color": str("String", "blue")}}))
	for q, want := range map[string]string{expensive: `{"kind":"order"}`, europe: `{"kind":"order"}`, notRed: "plain", tagged: `{"kind":"order"}`, orders: `{"kind":"order"}`} {
		if got := bodies(e.receive(q)); len(got) != 1 || got[0] != want {
			t.Errorf("%s got %v, want %s", q, got, want)
		}
	}

	var inv *types.InvalidParameterException
	if _, err := e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:x"), Attributes: map[string]string{"FilterPolicy": `{"price": 5}`}}); !errors.As(err, &inv) {
		t.Fatalf("invalid policy on Subscribe: %v", err)
	}
	subs := must(e.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(arn)})).Subscriptions
	if _, err := e.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: subs[0].SubscriptionArn,
		AttributeName: aws.String("FilterPolicy"), AttributeValue: aws.String("not json")}); !errors.As(err, &inv) {
		t.Fatalf("invalid policy on SetSubscriptionAttributes: %v", err)
	}
	must(e.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: subs[0].SubscriptionArn,
		AttributeName: aws.String("FilterPolicy"), AttributeValue: aws.String(`{"price":[1]}`)}))
	a := must(e.sns.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: subs[0].SubscriptionArn})).Attributes
	if a["FilterPolicy"] != `{"price":[1]}` || a["RawMessageDelivery"] != "true" || a["PendingConfirmation"] != "false" || a["FilterPolicyScope"] == "" {
		t.Fatalf("subscription attributes = %v", a)
	}
}

func TestMessageStructure(t *testing.T) {
	e := setup(t)
	arn := e.topic("multi", nil)
	q := e.queue(arn, "multi", map[string]string{"RawMessageDelivery": "true"})
	must(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("email"), Endpoint: aws.String("dev@example.com")}))
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), MessageStructure: aws.String("json"), Subject: aws.String("hi"),
		Message: aws.String(`{"default":"fallback","sqs":"for queues"}`)}))
	if got := bodies(e.receive(q)); len(got) != 1 || got[0] != "for queues" {
		t.Fatalf("sqs got %v", got)
	}
	if ob := e.svc().Outbox(); len(ob) != 1 || ob[0].Protocol != "email" || ob[0].Destination != "dev@example.com" || ob[0].Message != "fallback" || ob[0].Subject != "hi" {
		t.Fatalf("outbox = %+v", ob)
	}
	var inv *types.InvalidParameterException
	if _, err := e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), MessageStructure: aws.String("json"), Message: aws.String(`{"sqs":"x"}`)}); !errors.As(err, &inv) {
		t.Fatalf("missing default: %v", err)
	}
}

func TestFIFO(t *testing.T) {
	e := setup(t)
	arn := e.topic("orders.fifo", map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "true"})
	q := e.queue(arn, "orders.fifo", map[string]string{"RawMessageDelivery": "true"})
	var ids, seqs []string
	for _, body := range []string{"1", "2", "2", "3"} { // the second "2" is a duplicate
		out := must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String(body), MessageGroupId: aws.String("g")}))
		ids, seqs = append(ids, *out.MessageId), append(seqs, *out.SequenceNumber)
	}
	if ids[1] != ids[2] || seqs[1] != seqs[2] || !(seqs[0] < seqs[1] && seqs[2] < seqs[3]) {
		t.Fatalf("ids %v, sequence numbers %v", ids, seqs)
	}
	msgs := e.receive(q)
	if got := strings.Join(bodies(msgs), ""); got != "123" || msgs[0].Attributes["MessageGroupId"] != "g" {
		t.Fatalf("fifo order = %q, attributes %v", got, msgs[0].Attributes)
	}
	if a := e.topicAttrs(arn); a["FifoTopic"] != "true" || a["ContentBasedDeduplication"] != "true" {
		t.Fatalf("attributes = %v", a)
	}

	var inv *types.InvalidParameterException
	for name, err := range map[string]error{
		"no group id": must2(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("x")})),
		"lambda on a FIFO topic": must2(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("lambda"),
			Endpoint: aws.String("arn:aws:lambda:us-east-1:000000000000:function:f")})),
		"FIFO topic without .fifo": must2(e.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("plain"), Attributes: map[string]string{"FifoTopic": "true"}})),
		"FIFO queue on a standard topic": must2(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(e.topic("std", nil)), Protocol: aws.String("sqs"),
			Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:orders.fifo")})),
	} {
		if !errors.As(err, &inv) {
			t.Errorf("%s: want InvalidParameter, got %v", name, err)
		}
	}
}

// must2 keeps only the error of an SDK call.
func must2[T any](_ T, err error) error { return err }

func TestHTTPSubscription(t *testing.T) {
	e := setup(t)
	type hit struct {
		kind, sub string
		body      []byte
	}
	hits := make(chan hit, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		hits <- hit{r.Header.Get("x-amz-sns-message-type"), r.Header.Get("x-amz-sns-subscription-arn"), b}
	}))
	defer srv.Close()
	next := func() hit {
		select {
		case h := <-hits:
			return h
		case <-time.After(5 * time.Second):
			t.Fatal("no HTTP delivery")
			return hit{}
		}
	}

	arn := e.topic("web", nil)
	sub := must(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("http"), Endpoint: aws.String(srv.URL + "/hook")}))
	if *sub.SubscriptionArn != "pending confirmation" {
		t.Fatalf("subscribe = %s", *sub.SubscriptionArn)
	}
	h := next()
	var conf struct{ Type, Token, TopicArn, SubscribeURL string }
	if json.Unmarshal(h.body, &conf) != nil || h.kind != "SubscriptionConfirmation" || conf.Type != h.kind || conf.TopicArn != arn || conf.Token == "" {
		t.Fatalf("confirmation %s: %s", h.kind, h.body)
	}
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("too early")})) // pending: not delivered

	// A plain GET of the SubscribeURL confirms, as clicking the link does.
	res := must(http.Get(conf.SubscribeURL))
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(string(b), "<SubscriptionArn>"+arn+":") {
		t.Fatalf("confirm: %d %s", res.StatusCode, b)
	}
	if subs := must(e.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(arn)})).Subscriptions; len(subs) != 1 || *subs[0].SubscriptionArn == "PendingConfirmation" {
		t.Fatalf("subscriptions = %+v", subs)
	}

	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("hi web")}))
	h = next()
	var n struct{ Type, Message, UnsubscribeURL string }
	if json.Unmarshal(h.body, &n) != nil || h.kind != "Notification" || n.Message != "hi web" || !strings.HasPrefix(h.sub, arn+":") {
		t.Fatalf("notification %s %s: %s", h.kind, h.sub, h.body)
	}
	res = must(http.Get(n.UnsubscribeURL))
	res.Body.Close()
	if subs := must(e.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(arn)})).Subscriptions; res.StatusCode != 200 || len(subs) != 0 {
		t.Fatalf("unsubscribe link: %d, %d subscriptions left", res.StatusCode, len(subs))
	}
}

func TestSMSAndPush(t *testing.T) {
	e := setup(t)
	must(e.sns.Publish(ctx, &sns.PublishInput{PhoneNumber: aws.String("+15550001111"), Message: aws.String("code 1234")}))
	var sms struct {
		Messages map[string][]snssvc.OutboxMessage `json:"sms_messages"`
	}
	getJSON(t, e.url+"/_aws/sns/sms-messages?phoneNumber="+url.QueryEscape("+15550001111"), &sms)
	if got := sms.Messages["+15550001111"]; len(got) != 1 || got[0].Message != "code 1234" || got[0].MessageId == "" {
		t.Fatalf("sms = %+v", sms)
	}
	if out := must(e.sns.CheckIfPhoneNumberIsOptedOut(ctx, &sns.CheckIfPhoneNumberIsOptedOutInput{PhoneNumber: aws.String("+15550001111")})); out.IsOptedOut {
		t.Fatal("nobody opts out locally")
	}
	must(e.sns.SetSMSAttributes(ctx, &sns.SetSMSAttributesInput{Attributes: map[string]string{"DefaultSMSType": "Transactional"}}))
	if a := must(e.sns.GetSMSAttributes(ctx, &sns.GetSMSAttributesInput{})).Attributes; a["DefaultSMSType"] != "Transactional" {
		t.Fatalf("sms attributes = %v", a)
	}

	app := must(e.sns.CreatePlatformApplication(ctx, &sns.CreatePlatformApplicationInput{Name: aws.String("app"), Platform: aws.String("GCM"),
		Attributes: map[string]string{"PlatformCredential": "server-key"}}))
	ep := must(e.sns.CreatePlatformEndpoint(ctx, &sns.CreatePlatformEndpointInput{PlatformApplicationArn: app.PlatformApplicationArn, Token: aws.String("device-1")}))
	again := must(e.sns.CreatePlatformEndpoint(ctx, &sns.CreatePlatformEndpointInput{PlatformApplicationArn: app.PlatformApplicationArn, Token: aws.String("device-1")}))
	eps := must(e.sns.ListEndpointsByPlatformApplication(ctx, &sns.ListEndpointsByPlatformApplicationInput{PlatformApplicationArn: app.PlatformApplicationArn})).Endpoints
	if *again.EndpointArn != *ep.EndpointArn || len(eps) != 1 || eps[0].Attributes["Token"] != "device-1" || eps[0].Attributes["Enabled"] != "true" {
		t.Fatalf("endpoints = %+v", eps)
	}
	must(e.sns.Publish(ctx, &sns.PublishInput{TargetArn: ep.EndpointArn, MessageStructure: aws.String("json"),
		Message: aws.String(`{"default":"d","GCM":"{\"notification\":{\"title\":\"hi\"}}"}`)}))
	var push struct {
		Messages map[string][]snssvc.OutboxMessage `json:"platform_endpoint_messages"`
	}
	getJSON(t, e.url+"/_aws/sns/platform-endpoint-messages", &push)
	if got := push.Messages[*ep.EndpointArn]; len(got) != 1 || got[0].Message != `{"notification":{"title":"hi"}}` {
		t.Fatalf("push = %+v", push)
	}

	must(e.sns.SetEndpointAttributes(ctx, &sns.SetEndpointAttributesInput{EndpointArn: ep.EndpointArn, Attributes: map[string]string{"Enabled": "false"}}))
	var disabled *types.EndpointDisabledException
	if _, err := e.sns.Publish(ctx, &sns.PublishInput{TargetArn: ep.EndpointArn, Message: aws.String("x")}); !errors.As(err, &disabled) {
		t.Fatalf("disabled endpoint: %v", err)
	}
	must(e.sns.DeleteEndpoint(ctx, &sns.DeleteEndpointInput{EndpointArn: ep.EndpointArn}))
	var nf *types.NotFoundException
	if _, err := e.sns.GetEndpointAttributes(ctx, &sns.GetEndpointAttributesInput{EndpointArn: ep.EndpointArn}); !errors.As(err, &nf) {
		t.Fatalf("deleted endpoint: %v", err)
	}

	req, _ := http.NewRequest(http.MethodDelete, e.url+"/_aws/sns/sms-messages", nil)
	res := must(http.DefaultClient.Do(req))
	res.Body.Close()
	sms.Messages = nil
	getJSON(t, e.url+"/_aws/sns/sms-messages", &sms)
	if res.StatusCode != 204 || len(sms.Messages) != 0 || len(e.svc().Outbox()) != 1 { // the push message stays
		t.Fatalf("after DELETE: %d %+v", res.StatusCode, sms.Messages)
	}
}

func TestPublishBatch(t *testing.T) {
	e := setup(t)
	arn := e.topic("batch", nil)
	q := e.queue(arn, "batch", map[string]string{"RawMessageDelivery": "true"})
	out := must(e.sns.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(arn), PublishBatchRequestEntries: []types.PublishBatchRequestEntry{
		{Id: aws.String("a"), Message: aws.String("one")},
		{Id: aws.String("b"), Message: aws.String("two")},
		{Id: aws.String("c"), Message: aws.String("bad"), MessageAttributes: map[string]types.MessageAttributeValue{"n": str("Number", "abc")}},
	}}))
	if len(out.Successful) != 2 || *out.Successful[0].Id != "a" || *out.Successful[0].MessageId == "" ||
		len(out.Failed) != 1 || *out.Failed[0].Id != "c" || *out.Failed[0].Code != "ParameterValueInvalid" || !out.Failed[0].SenderFault {
		t.Fatalf("batch = %+v", out)
	}
	got := bodies(e.receive(q))
	slices.Sort(got)
	if strings.Join(got, ",") != "one,two" {
		t.Fatalf("delivered %v", got)
	}

	var entries []types.PublishBatchRequestEntry
	for i := range 11 {
		entries = append(entries, types.PublishBatchRequestEntry{Id: aws.String(fmt.Sprint(i)), Message: aws.String("m")})
	}
	var tooMany *types.TooManyEntriesInBatchRequestException
	if _, err := e.sns.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(arn), PublishBatchRequestEntries: entries}); !errors.As(err, &tooMany) {
		t.Fatalf("11 entries: %v", err)
	}
	var dup *types.BatchEntryIdsNotDistinctException
	if _, err := e.sns.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(arn), PublishBatchRequestEntries: []types.PublishBatchRequestEntry{entries[0], entries[0]}}); !errors.As(err, &dup) {
		t.Fatalf("duplicate ids: %v", err)
	}
}

// Lambda and Firehose are other packages; a stand-in receiver shows what they get.
func TestLambdaFirehoseEmail(t *testing.T) {
	e := setup(t)
	delivered := make(chan [2]string, 2)
	for _, svc := range []string{"lambda", "firehose"} {
		e.hub.OnDeliver(svc, func(arn string, m *core.Message) error { delivered <- [2]string{arn, m.Body}; return nil })
	}
	arn := e.topic("fan", nil)
	fn, stream := "arn:aws:lambda:us-east-1:000000000000:function:handler", "arn:aws:firehose:us-east-1:000000000000:deliverystream/archive"
	must(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("lambda"), Endpoint: aws.String(fn)}))
	must(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("firehose"), Endpoint: aws.String(stream),
		Attributes: map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns", "RawMessageDelivery": "true"}}))
	must(e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("email-json"), Endpoint: aws.String("ops@example.com")}))
	var inv *types.InvalidParameterException
	if _, err := e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("lambda"), Endpoint: aws.String(fn),
		Attributes: map[string]string{"RawMessageDelivery": "true"}}); !errors.As(err, &inv) {
		t.Fatalf("raw delivery to lambda: %v", err)
	}
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("ping")}))
	got := map[string]string{}
	for range 2 {
		select {
		case d := <-delivered:
			got[d[0]] = d[1]
		case <-time.After(5 * time.Second):
			t.Fatalf("deliveries so far: %v", got)
		}
	}

	var ev struct {
		Records []struct {
			EventSource, EventSubscriptionArn string
			Sns                               struct {
				Type, Message, TopicArn string
				Subject                 *string
				MessageAttributes       map[string]any
			}
		}
	}
	if json.Unmarshal([]byte(got[fn]), &ev) != nil || len(ev.Records) != 1 || ev.Records[0].EventSource != "aws:sns" || ev.Records[0].Sns.Type != "Notification" ||
		ev.Records[0].Sns.Message != "ping" || ev.Records[0].Sns.TopicArn != arn || ev.Records[0].Sns.Subject != nil || ev.Records[0].Sns.MessageAttributes == nil ||
		!strings.HasPrefix(ev.Records[0].EventSubscriptionArn, arn+":") {
		t.Fatalf("lambda event = %s", got[fn])
	}
	if got[stream] != "ping" {
		t.Fatalf("firehose got %q", got[stream])
	}
	if ob := e.svc().Outbox(); len(ob) != 1 || ob[0].Protocol != "email-json" || !strings.Contains(ob[0].Message, `"Message":"ping"`) {
		t.Fatalf("outbox = %+v", ob)
	}
}

// floci #4128: a firehose subscription needs SubscriptionRoleArn.
// floci #3959: Subject is printable ASCII that does not start with a space.
// floci #778: the messages of one PublishBatch together stay under 256 KiB.
func TestFirehoseRoleSubjectAndBatchSize(t *testing.T) {
	e := setup(t)
	arn := e.topic("checks", nil)
	var inv *types.InvalidParameterException
	stream := aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/s1")
	if _, err := e.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: &arn, Protocol: aws.String("firehose"), Endpoint: stream}); !errors.As(err, &inv) {
		t.Fatalf("firehose without role: %v", err)
	}
	for _, subject := range []string{" lead", "héllo", "a\tb", "a\nb", strings.Repeat("a", 100)} {
		if _, err := e.sns.Publish(ctx, &sns.PublishInput{TopicArn: &arn, Message: aws.String("m"), Subject: aws.String(subject)}); !errors.As(err, &inv) {
			t.Errorf("subject %q: %v", subject, err)
		}
	}
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: &arn, Message: aws.String("m"), Subject: aws.String("Hello, world! " + strings.Repeat("a", 85))}))
	var ents []types.PublishBatchRequestEntry
	for _, id := range []string{"1", "2", "3"} {
		ents = append(ents, types.PublishBatchRequestEntry{Id: aws.String(id), Message: aws.String(strings.Repeat("x", 100000))})
	}
	if _, err := e.sns.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: &arn, PublishBatchRequestEntries: ents}); err == nil || !strings.Contains(err.Error(), "BatchRequestTooLong") {
		t.Fatalf("300000 bytes in a batch: %v", err)
	}
}
