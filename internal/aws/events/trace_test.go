package events_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/nrndev/pogi/internal/core"
)

// An event put with a trace reaches the rule's queue under the same trace, and the span names the rule.
func TestTraceThroughRule(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("traced")
	e.rule("r", `{"source":["app"]}`, types.Target{Id: aws.String("1"), Arn: aws.String(arn)})
	root := "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	must(e.eb.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: []types.PutEventsRequestEntry{{Source: aws.String("app"), DetailType: aws.String("x"), Detail: aws.String("{}")}}},
		func(o *eventbridge.Options) {
			o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("X-Amzn-Trace-Id", "Root="+root))
		}))
	m := must(e.sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"All"}})).Messages
	if len(m) != 1 || core.TraceRoot(m[0].Attributes["AWSTraceHeader"]) != root {
		t.Fatalf("queue message lost the trace: %+v", m)
	}
	var found bool
	for _, s := range e.hub.Spans.All() {
		found = found || s.Trace == root && s.Kind == core.SpanDeliver && s.To == arn && s.From == "arn:aws:events:us-east-1:000000000000:rule/r"
	}
	if !found {
		t.Fatalf("no deliver span from the rule: %+v", e.hub.Spans.All())
	}
}
