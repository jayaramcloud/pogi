package sns_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/nrndev/pogi/internal/core"
)

// A trace on the publish reaches the queue message and shows in the span log.
func TestTracePropagates(t *testing.T) {
	e := setup(t)
	topic := e.topic("traced", nil)
	q := e.queue(topic, "traced-q", nil)
	root := "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	must(e.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(topic), Message: aws.String("hi")}, func(o *sns.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("X-Amzn-Trace-Id", "Root="+root+";Parent=53995c3f42cd8ad8;Sampled=1"))
	}))
	msgs := e.receive(q)
	if len(msgs) != 1 || core.TraceRoot(msgs[0].Attributes["AWSTraceHeader"]) != root {
		t.Fatalf("queue message lost the trace: %+v", msgs)
	}
	var span core.Span
	for _, s := range e.hub.Spans.All() {
		if s.Trace == root && s.Kind == core.SpanDeliver {
			span = s
		}
	}
	if span.From != topic || span.Service != "sqs" || !strings.Contains(span.To, "traced-q") {
		t.Fatalf("deliver span = %+v", span)
	}
	if got := e.hub.Requests.Query(0, 0, "sns", "Publish", false); len(got) != 1 || got[0].Trace != root {
		t.Fatalf("publish request not on the trace: %+v", got)
	}
}
