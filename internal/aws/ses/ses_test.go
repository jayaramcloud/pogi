package ses_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	v2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	cwsvc "github.com/nrndev/pogi/internal/aws/cloudwatch"
	eventssvc "github.com/nrndev/pogi/internal/aws/events"
	sessvc "github.com/nrndev/pogi/internal/aws/ses"
	snssvc "github.com/nrndev/pogi/internal/aws/sns"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// roundTrip snapshots, resets and restores the hub: state must survive Save and Load.
func roundTrip(t *testing.T, h *core.Hub) {
	t.Helper()
	b, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if err := h.Restore(b); err != nil {
		t.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func setup(t *testing.T) (*sessvc.Service, *core.Hub, string, aws.Config) {
	var svc *sessvc.Service
	hub, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { svc = sessvc.New(h); return svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	return svc, hub, url, awstest.Config(t, url)
}

type outbox struct {
	Messages []struct {
		Id, Source, Subject, RawData, Template string
		Destination                            struct{ ToAddresses, CcAddresses []string }
		Body                                   struct {
			TextPart string `json:"text_part"`
			HtmlPart string `json:"html_part"`
		}
	}
}

func peek(t *testing.T, url string) outbox {
	resp := must(http.Get(url))
	defer resp.Body.Close()
	var o outbox
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestSESv1(t *testing.T) {
	svc, hub, url, cfg := setup(t)
	c := ses.NewFromConfig(cfg)

	must(c.VerifyEmailIdentity(ctx, &ses.VerifyEmailIdentityInput{EmailAddress: aws.String("sender@example.com")}))
	if tok := must(c.VerifyDomainIdentity(ctx, &ses.VerifyDomainIdentityInput{Domain: aws.String("example.com")})).VerificationToken; len(*tok) != 44 {
		t.Fatalf("token = %q", *tok)
	}
	if d := must(c.VerifyDomainDkim(ctx, &ses.VerifyDomainDkimInput{Domain: aws.String("example.com")})); len(d.DkimTokens) != 3 {
		t.Fatalf("dkim = %v", d.DkimTokens)
	}
	if ids := must(c.ListIdentities(ctx, &ses.ListIdentitiesInput{IdentityType: types.IdentityTypeDomain})); len(ids.Identities) != 1 || ids.Identities[0] != "example.com" {
		t.Fatalf("domains = %v", ids.Identities)
	}
	attrs := must(c.GetIdentityVerificationAttributes(ctx, &ses.GetIdentityVerificationAttributesInput{Identities: []string{"sender@example.com", "who@else.com"}}))
	if len(attrs.VerificationAttributes) != 1 || attrs.VerificationAttributes["sender@example.com"].VerificationStatus != types.VerificationStatusSuccess {
		t.Fatalf("verification = %+v", attrs.VerificationAttributes)
	}

	// Delivery notifications go to the identity's topic (an SQS queue here, through the hub).
	q := sqs.NewFromConfig(cfg)
	qurl := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("ses-events")})).QueueUrl
	must(c.SetIdentityNotificationTopic(ctx, &ses.SetIdentityNotificationTopicInput{Identity: aws.String("sender@example.com"),
		NotificationType: types.NotificationTypeDelivery, SnsTopic: aws.String("arn:aws:sqs:us-east-1:000000000000:ses-events")}))

	id := must(c.SendEmail(ctx, &ses.SendEmailInput{Source: aws.String("sender@example.com"),
		Destination: &types.Destination{ToAddresses: []string{"to@example.com"}, CcAddresses: []string{"cc@example.com"}},
		Message: &types.Message{Subject: &types.Content{Data: aws.String("Hello")},
			Body: &types.Body{Text: &types.Content{Data: aws.String("plain")}, Html: &types.Content{Data: aws.String("<b>html</b>")}}}})).MessageId
	note := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qurl}))
	if len(note.Messages) != 1 || !strings.Contains(*note.Messages[0].Body, `"notificationType":"Delivery"`) || !strings.Contains(*note.Messages[0].Body, *id) {
		t.Fatalf("delivery notification = %+v", note.Messages)
	}

	raw := "From: Raw Sender <raw@example.com>\r\nTo: a@example.com, b@example.com\r\nSubject: =?UTF-8?Q?Caf=C3=A9?=\r\n\r\nraw body"
	must(c.SendRawEmail(ctx, &ses.SendRawEmailInput{RawMessage: &types.RawMessage{Data: []byte(raw)}}))

	must(c.CreateTemplate(ctx, &ses.CreateTemplateInput{Template: &types.Template{TemplateName: aws.String("order"),
		SubjectPart: aws.String("Hi {{name}}"), TextPart: aws.String("Order {{ order.id }} shipped"), HtmlPart: aws.String("<p>{{name}}</p>")}}))
	_, err := c.CreateTemplate(ctx, &ses.CreateTemplateInput{Template: &types.Template{TemplateName: aws.String("order")}})
	var exists *types.AlreadyExistsException
	if !errors.As(err, &exists) {
		t.Fatalf("want AlreadyExists, got %v", err)
	}
	must(c.SendTemplatedEmail(ctx, &ses.SendTemplatedEmailInput{Source: aws.String("sender@example.com"), Template: aws.String("order"),
		Destination: &types.Destination{ToAddresses: []string{"ann@example.com"}}, TemplateData: aws.String(`{"name":"Ann","order":{"id":42}}`)}))
	bulk := must(c.SendBulkTemplatedEmail(ctx, &ses.SendBulkTemplatedEmailInput{Source: aws.String("sender@example.com"), Template: aws.String("order"),
		DefaultTemplateData: aws.String(`{"name":"friend","order":{"id":1}}`), Destinations: []types.BulkEmailDestination{
			{Destination: &types.Destination{ToAddresses: []string{"x@example.com"}}},
			{Destination: &types.Destination{ToAddresses: []string{"y@example.com"}}, ReplacementTemplateData: aws.String(`{"name":"Yan"}`)},
		}}))
	if len(bulk.Status) != 2 || bulk.Status[1].Status != types.BulkEmailStatusSuccess || bulk.Status[1].MessageId == nil {
		t.Fatalf("bulk = %+v", bulk.Status)
	}
	_, err = c.SendTemplatedEmail(ctx, &ses.SendTemplatedEmailInput{Source: aws.String("a@b.c"), Template: aws.String("nope"),
		Destination: &types.Destination{ToAddresses: []string{"x@y.z"}}, TemplateData: aws.String("{}")})
	var noTpl *types.TemplateDoesNotExistException
	if !errors.As(err, &noTpl) {
		t.Fatalf("want TemplateDoesNotExist, got %v", err)
	}
	_, err = c.SendEmail(ctx, &ses.SendEmailInput{Source: aws.String("a@b.c"), Destination: &types.Destination{ToAddresses: []string{"x@y.z"}},
		Message: &types.Message{Subject: &types.Content{Data: aws.String("s")}, Body: &types.Body{}}, ConfigurationSetName: aws.String("nope")})
	var noSet *types.ConfigurationSetDoesNotExistException
	if !errors.As(err, &noSet) {
		t.Fatalf("want ConfigurationSetDoesNotExist, got %v", err)
	}

	// a line break in the subject, sender or a recipient is refused (header injection)
	for name, in := range map[string]*ses.SendEmailInput{
		"subject":   {Source: aws.String("a@b.c"), Destination: &types.Destination{ToAddresses: []string{"x@y.z"}}, Message: &types.Message{Subject: &types.Content{Data: aws.String("hi\r\nBcc: x@y.z")}, Body: &types.Body{}}},
		"source":    {Source: aws.String("a@b.c\nBcc: x@y.z"), Destination: &types.Destination{ToAddresses: []string{"x@y.z"}}, Message: &types.Message{Subject: &types.Content{Data: aws.String("s")}, Body: &types.Body{}}},
		"recipient": {Source: aws.String("a@b.c"), Destination: &types.Destination{ToAddresses: []string{"x@y.z\r\nBcc: q@y.z"}}, Message: &types.Message{Subject: &types.Content{Data: aws.String("s")}, Body: &types.Body{}}},
	} {
		_, err = c.SendEmail(ctx, in)
		var api smithy.APIError
		if !errors.As(err, &api) || api.ErrorCode() != "InvalidParameterValue" {
			t.Errorf("line break in %s: got %v", name, err)
		}
	}

	quota := must(c.GetSendQuota(ctx, &ses.GetSendQuotaInput{}))
	if quota.Max24HourSend != 200 || quota.MaxSendRate != 1 || quota.SentLast24Hours != 5 {
		t.Fatalf("quota = %+v", quota)
	}
	if stats := must(c.GetSendStatistics(ctx, &ses.GetSendStatisticsInput{})); len(stats.SendDataPoints) == 0 || stats.SendDataPoints[0].DeliveryAttempts == 0 {
		t.Fatalf("statistics = %+v", stats.SendDataPoints)
	}

	roundTrip(t, hub)
	box := peek(t, url+"/_aws/ses")
	if len(box.Messages) != 5 || len(svc.Outbox()) != 5 {
		t.Fatalf("outbox has %d messages", len(box.Messages))
	}
	first, rawMsg, tpl := box.Messages[0], box.Messages[1], box.Messages[2]
	if first.Id != *id || first.Subject != "Hello" || first.Body.TextPart != "plain" || first.Destination.CcAddresses[0] != "cc@example.com" {
		t.Fatalf("simple email = %+v", first)
	}
	if rawMsg.Subject != "Café" || rawMsg.Source != "Raw Sender <raw@example.com>" || len(rawMsg.Destination.ToAddresses) != 2 || !strings.HasSuffix(rawMsg.RawData, "raw body") {
		t.Fatalf("raw email = %+v", rawMsg)
	}
	if tpl.Subject != "Hi Ann" || tpl.Body.TextPart != "Order 42 shipped" || tpl.Template != "order" || box.Messages[4].Subject != "Hi Yan" {
		t.Fatalf("templated email = %+v / %+v", tpl, box.Messages[4])
	}
	if one := peek(t, url+"/_aws/ses?id="+*id); len(one.Messages) != 1 {
		t.Fatalf("id filter = %+v", one)
	}
	req, _ := http.NewRequest(http.MethodDelete, url+"/_aws/ses?id="+*id, nil)
	must(http.DefaultClient.Do(req))
	if left := peek(t, url+"/_aws/ses?email=sender@example.com"); len(left.Messages) != 3 {
		t.Fatalf("after delete, sender has %d messages", len(left.Messages))
	}
	req, _ = http.NewRequest(http.MethodDelete, url+"/_aws/ses", nil)
	must(http.DefaultClient.Do(req))
	if len(svc.Outbox()) != 0 {
		t.Fatal("DELETE /_aws/ses should clear the outbox")
	}

	_, err = c.SetIdentityNotificationTopic(ctx, &ses.SetIdentityNotificationTopicInput{Identity: aws.String("ghost@example.com"),
		NotificationType: types.NotificationTypeBounce, SnsTopic: aws.String("arn:aws:sns:us-east-1:000000000000:t")})
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidParameterValue" {
		t.Fatalf("want InvalidParameterValue, got %v", err)
	}
}

func TestSESv2(t *testing.T) {
	svc, _, _, cfg := setup(t)
	c := sesv2.NewFromConfig(cfg)

	created := must(c.CreateEmailIdentity(ctx, &sesv2.CreateEmailIdentityInput{EmailIdentity: aws.String("example.org"),
		Tags: []v2types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	if created.IdentityType != v2types.IdentityTypeDomain || !created.VerifiedForSendingStatus || len(created.DkimAttributes.Tokens) != 3 {
		t.Fatalf("created = %+v", created)
	}
	_, err := c.CreateEmailIdentity(ctx, &sesv2.CreateEmailIdentityInput{EmailIdentity: aws.String("example.org")})
	var exists *v2types.AlreadyExistsException
	if !errors.As(err, &exists) {
		t.Fatalf("want AlreadyExistsException, got %v", err)
	}
	must(c.CreateEmailIdentity(ctx, &sesv2.CreateEmailIdentityInput{EmailIdentity: aws.String("me@example.org")}))
	got := must(c.GetEmailIdentity(ctx, &sesv2.GetEmailIdentityInput{EmailIdentity: aws.String("me@example.org")}))
	if got.IdentityType != v2types.IdentityTypeEmailAddress || got.VerificationStatus != v2types.VerificationStatusSuccess {
		t.Fatalf("identity = %+v", got)
	}
	if l := must(c.ListEmailIdentities(ctx, &sesv2.ListEmailIdentitiesInput{})); len(l.EmailIdentities) != 2 {
		t.Fatalf("identities = %+v", l.EmailIdentities)
	}

	must(c.CreateEmailTemplate(ctx, &sesv2.CreateEmailTemplateInput{TemplateName: aws.String("welcome"),
		TemplateContent: &v2types.EmailTemplateContent{Subject: aws.String("Welcome {{name}}"), Text: aws.String("Hi {{name}}")}}))
	if tpl := must(c.GetEmailTemplate(ctx, &sesv2.GetEmailTemplateInput{TemplateName: aws.String("welcome")})); *tpl.TemplateContent.Subject != "Welcome {{name}}" {
		t.Fatalf("template = %+v", tpl.TemplateContent)
	}
	if l := must(c.ListEmailTemplates(ctx, &sesv2.ListEmailTemplatesInput{})); len(l.TemplatesMetadata) != 1 || l.TemplatesMetadata[0].CreatedTimestamp == nil {
		t.Fatalf("templates = %+v", l.TemplatesMetadata)
	}

	dest := &v2types.Destination{ToAddresses: []string{"you@example.com"}}
	simple := must(c.SendEmail(ctx, &sesv2.SendEmailInput{FromEmailAddress: aws.String("me@example.org"), Destination: dest,
		Content: &v2types.EmailContent{Simple: &v2types.Message{Subject: &v2types.Content{Data: aws.String("v2 subject")},
			Body: &v2types.Body{Html: &v2types.Content{Data: aws.String("<i>v2</i>")}}}}}))
	must(c.SendEmail(ctx, &sesv2.SendEmailInput{FromEmailAddress: aws.String("me@example.org"), Destination: dest,
		Content: &v2types.EmailContent{Template: &v2types.Template{TemplateName: aws.String("welcome"), TemplateData: aws.String(`{"name":"Bo"}`)}}}))
	_, err = c.SendEmail(ctx, &sesv2.SendEmailInput{FromEmailAddress: aws.String("me@example.org"), Destination: dest,
		Content: &v2types.EmailContent{Template: &v2types.Template{TemplateName: aws.String("missing"), TemplateData: aws.String(`{}`)}}})
	var nf *v2types.NotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("want NotFoundException, got %v", err)
	}
	box := svc.Outbox()
	if len(box) != 2 || box[0].Id != *simple.MessageId || box[0].Body.HtmlPart != "<i>v2</i>" || box[1].Subject != "Welcome Bo" || box[1].Body.TextPart != "Hi Bo" {
		t.Fatalf("outbox = %+v", box)
	}

	acct := must(c.GetAccount(ctx, &sesv2.GetAccountInput{}))
	if !acct.SendingEnabled || !acct.ProductionAccessEnabled || acct.SendQuota.SentLast24Hours != 2 || acct.SendQuota.Max24HourSend != 200 {
		t.Fatalf("account = %+v %+v", acct, acct.SendQuota)
	}
	must(c.DeleteEmailIdentity(ctx, &sesv2.DeleteEmailIdentityInput{EmailIdentity: aws.String("me@example.org")}))
	if _, err := c.GetEmailIdentity(ctx, &sesv2.GetEmailIdentityInput{EmailIdentity: aws.String("me@example.org")}); !errors.As(err, &nf) {
		t.Fatalf("want NotFoundException after delete, got %v", err)
	}
	must(c.DeleteEmailTemplate(ctx, &sesv2.DeleteEmailTemplateInput{TemplateName: aws.String("welcome")}))
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// eventSetup serves SES with the services its events go to, and returns a queue
// (its URL and ARN) that SNS topics and EventBridge rules can deliver to.
func eventSetup(t *testing.T) (*core.Hub, aws.Config, *sqs.Client, string, string) {
	hub, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return sessvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return snssvc.New(h) },
		func(h *core.Hub) core.Service { return eventssvc.New(h) },
		func(h *core.Hub) core.Service { return cwsvc.New(h) })
	cfg := awstest.Config(t, url)
	sc := sqs.NewFromConfig(cfg)
	q := must(sc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("events")}))
	arn := must(sc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"}})).Attributes["QueueArn"]
	return hub, cfg, sc, *q.QueueUrl, arn
}

// eventKinds reads the queue until it has n messages and returns the eventType and
// the detail-type or SNS envelope of each, in order.
func eventKinds(t *testing.T, sc *sqs.Client, queue string, n int) []string {
	t.Helper()
	var kinds []string
	for i := 0; i < 20 && len(kinds) < n; i++ {
		m := must(sc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queue), MaxNumberOfMessages: 10, WaitTimeSeconds: 1}))
		for _, msg := range m.Messages {
			var env struct {
				Message    string
				DetailType string `json:"detail-type"`
				Detail     struct{ EventType string }
			}
			json.Unmarshal([]byte(*msg.Body), &env)
			var ev struct {
				EventType string
				Bounce    struct{ BounceSubType string }
			}
			json.Unmarshal([]byte(env.Message), &ev)
			switch {
			case env.DetailType != "":
				kinds = append(kinds, env.DetailType)
			default:
				kinds = append(kinds, ev.EventType+" "+ev.Bounce.BounceSubType)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return kinds
}

// floci #4048, #2833: v1 event destinations publish Send and Bounce (not Reject) for the
// simulator's suppression list, and a configuration set keeps its delivery options.
func TestEventDestinationsV1(t *testing.T) {
	hub, cfg, sc, queue, qarn := eventSetup(t)
	c, sn := ses.NewFromConfig(cfg), sns.NewFromConfig(cfg)
	topic := must(sn.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("ses-events")}))
	must(sn.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(qarn)}))
	must(c.VerifyEmailIdentity(ctx, &ses.VerifyEmailIdentityInput{EmailAddress: aws.String("from@example.com")}))
	must(c.CreateConfigurationSet(ctx, &ses.CreateConfigurationSetInput{ConfigurationSet: &types.ConfigurationSet{Name: aws.String("cs")}}))
	dest := &types.EventDestination{Name: aws.String("d"), Enabled: true,
		MatchingEventTypes: []types.EventType{types.EventTypeBounce, types.EventTypeReject, types.EventTypeSend},
		SNSDestination:     &types.SNSDestination{TopicARN: topic.TopicArn}}
	must(c.CreateConfigurationSetEventDestination(ctx, &ses.CreateConfigurationSetEventDestinationInput{ConfigurationSetName: aws.String("cs"), EventDestination: dest}))
	if _, err := c.CreateConfigurationSetEventDestination(ctx, &ses.CreateConfigurationSetEventDestinationInput{ConfigurationSetName: aws.String("cs"), EventDestination: dest}); code(err) != "EventDestinationAlreadyExists" {
		t.Errorf("duplicate destination: %v", err)
	}
	if _, err := c.CreateConfigurationSetEventDestination(ctx, &ses.CreateConfigurationSetEventDestinationInput{ConfigurationSetName: aws.String("nope"), EventDestination: dest}); code(err) != "ConfigurationSetDoesNotExist" {
		t.Errorf("missing set: %v", err)
	}

	must(c.SendEmail(ctx, &ses.SendEmailInput{Source: aws.String("from@example.com"), ConfigurationSetName: aws.String("cs"),
		Destination: &types.Destination{ToAddresses: []string{"suppressionlist@simulator.amazonses.com", "success@simulator.amazonses.com"}},
		Message:     &types.Message{Subject: &types.Content{Data: aws.String("s")}, Body: &types.Body{Text: &types.Content{Data: aws.String("b")}}}}))
	if got := eventKinds(t, sc, queue, 2); strings.Join(got, ",") != "Send ,Bounce Suppressed" {
		t.Errorf("events = %q, want Send and a suppressed Bounce (Delivery is not subscribed, Reject never)", got)
	}

	// DescribeConfigurationSet shows the destination and the delivery options.
	must(c.PutConfigurationSetDeliveryOptions(ctx, &ses.PutConfigurationSetDeliveryOptionsInput{ConfigurationSetName: aws.String("cs"),
		DeliveryOptions: &types.DeliveryOptions{TlsPolicy: types.TlsPolicyRequire}}))
	roundTrip(t, hub)
	d := must(c.DescribeConfigurationSet(ctx, &ses.DescribeConfigurationSetInput{ConfigurationSetName: aws.String("cs"),
		ConfigurationSetAttributeNames: []types.ConfigurationSetAttribute{types.ConfigurationSetAttributeEventDestinations, types.ConfigurationSetAttributeDeliveryOptions}}))
	if d.DeliveryOptions == nil || d.DeliveryOptions.TlsPolicy != types.TlsPolicyRequire || len(d.EventDestinations) != 1 ||
		len(d.EventDestinations[0].MatchingEventTypes) != 3 || aws.ToString(d.EventDestinations[0].SNSDestination.TopicARN) != aws.ToString(topic.TopicArn) {
		t.Errorf("describe = %+v", d)
	}
	dest.Enabled = false
	must(c.UpdateConfigurationSetEventDestination(ctx, &ses.UpdateConfigurationSetEventDestinationInput{ConfigurationSetName: aws.String("cs"), EventDestination: dest}))
	must(c.DeleteConfigurationSetEventDestination(ctx, &ses.DeleteConfigurationSetEventDestinationInput{ConfigurationSetName: aws.String("cs"), EventDestinationName: aws.String("d")}))
	if _, err := c.DeleteConfigurationSetEventDestination(ctx, &ses.DeleteConfigurationSetEventDestinationInput{ConfigurationSetName: aws.String("cs"), EventDestinationName: aws.String("d")}); code(err) != "EventDestinationDoesNotExist" {
		t.Errorf("delete twice: %v", err)
	}
}

// floci #4630: v2 configuration sets with EventBridge (default bus only), SNS and
// CloudWatch destinations.
func TestEventDestinationsV2(t *testing.T) {
	_, cfg, sc, queue, qarn := eventSetup(t)
	c := sesv2.NewFromConfig(cfg)
	must(c.CreateEmailIdentity(ctx, &sesv2.CreateEmailIdentityInput{EmailIdentity: aws.String("from@example.com")}))
	must(c.CreateConfigurationSet(ctx, &sesv2.CreateConfigurationSetInput{ConfigurationSetName: aws.String("cs"),
		DeliveryOptions: &v2types.DeliveryOptions{TlsPolicy: v2types.TlsPolicyRequire}, Tags: []v2types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	got := must(c.GetConfigurationSet(ctx, &sesv2.GetConfigurationSetInput{ConfigurationSetName: aws.String("cs")}))
	if got.DeliveryOptions == nil || got.DeliveryOptions.TlsPolicy != v2types.TlsPolicyRequire || len(got.Tags) != 1 || !got.SendingOptions.SendingEnabled {
		t.Errorf("config set = %+v", got)
	}
	must(c.PutConfigurationSetSendingOptions(ctx, &sesv2.PutConfigurationSetSendingOptionsInput{ConfigurationSetName: aws.String("cs"), SendingEnabled: false}))
	if g := must(c.GetConfigurationSet(ctx, &sesv2.GetConfigurationSetInput{ConfigurationSetName: aws.String("cs")})); g.SendingOptions.SendingEnabled {
		t.Error("PutConfigurationSetSendingOptions was not stored")
	}

	ensure := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	create := func(name string, d *v2types.EventDestinationDefinition) error {
		_, err := c.CreateConfigurationSetEventDestination(ctx, &sesv2.CreateConfigurationSetEventDestinationInput{
			ConfigurationSetName: aws.String("cs"), EventDestinationName: aws.String(name), EventDestination: d})
		return err
	}
	types := []v2types.EventType{v2types.EventTypeSend, v2types.EventTypeDelivery}
	var bad *v2types.BadRequestException
	if err := create("custom", &v2types.EventDestinationDefinition{Enabled: true, MatchingEventTypes: types,
		EventBridgeDestination: &v2types.EventBridgeDestination{EventBusArn: aws.String("arn:aws:events:us-east-1:000000000000:event-bus/custom")}}); !errors.As(err, &bad) {
		t.Errorf("a custom event bus: want BadRequestException, got %v", err)
	}
	ensure(create("bus", &v2types.EventDestinationDefinition{Enabled: true, MatchingEventTypes: types,
		EventBridgeDestination: &v2types.EventBridgeDestination{EventBusArn: aws.String("arn:aws:events:us-east-1:000000000000:event-bus/default")}}))
	ensure(create("cw", &v2types.EventDestinationDefinition{Enabled: true, MatchingEventTypes: types,
		CloudWatchDestination: &v2types.CloudWatchDestination{DimensionConfigurations: []v2types.CloudWatchDimensionConfiguration{
			{DimensionName: aws.String("campaign"), DimensionValueSource: v2types.DimensionValueSourceMessageTag, DefaultDimensionValue: aws.String("none")}}}}))
	var exists *v2types.AlreadyExistsException
	if err := create("cw", &v2types.EventDestinationDefinition{MatchingEventTypes: types, SnsDestination: &v2types.SnsDestination{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t")}}); !errors.As(err, &exists) {
		t.Errorf("duplicate: %v", err)
	}
	if l := must(c.GetConfigurationSetEventDestinations(ctx, &sesv2.GetConfigurationSetEventDestinationsInput{ConfigurationSetName: aws.String("cs")})); len(l.EventDestinations) != 2 ||
		aws.ToString(l.EventDestinations[0].Name) != "bus" || len(l.EventDestinations[1].CloudWatchDestination.DimensionConfigurations) != 1 {
		t.Errorf("destinations = %+v", l.EventDestinations)
	}

	// A message to the simulator reaches the bus (rule to a queue) and CloudWatch.
	eb := eventbridge.NewFromConfig(cfg)
	must(eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("ses"), EventPattern: aws.String(`{"source":["aws.ses"]}`)}))
	must(eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String("ses"), Targets: []ebtypes.Target{{Id: aws.String("q"), Arn: aws.String(qarn)}}}))
	must(c.SendEmail(ctx, &sesv2.SendEmailInput{FromEmailAddress: aws.String("from@example.com"), ConfigurationSetName: aws.String("cs"),
		Destination: &v2types.Destination{ToAddresses: []string{"success@simulator.amazonses.com"}},
		Content:     &v2types.EmailContent{Simple: &v2types.Message{Subject: &v2types.Content{Data: aws.String("s")}, Body: &v2types.Body{Text: &v2types.Content{Data: aws.String("b")}}}}}))
	if got := eventKinds(t, sc, queue, 2); strings.Join(got, ",") != "Email Sent,Email Delivered" {
		t.Errorf("bus events = %q", got)
	}
	m := must(cloudwatch.NewFromConfig(cfg).ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("AWS/SES")}))
	names := map[string]bool{}
	for _, x := range m.Metrics {
		names[aws.ToString(x.MetricName)] = len(x.Dimensions) == 1 && aws.ToString(x.Dimensions[0].Value) == "none"
	}
	if !names["Send"] || !names["Delivery"] {
		t.Errorf("AWS/SES metrics = %v", names)
	}
}

// A path that is not an SES v2 call is answered in JSON, not in the XML of v1.
func TestUnknownV2PathIsJSON(t *testing.T) {
	_, _, url, _ := setup(t)
	req, _ := http.NewRequest("GET", url+"/NoSuchOperation", nil)
	awstest.Sign(t, req, "ses")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var e struct{ Message string }
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || resp.StatusCode != 404 || e.Message == "" {
		t.Fatalf("status %d, message %q, %v", resp.StatusCode, e.Message, err)
	}
}
