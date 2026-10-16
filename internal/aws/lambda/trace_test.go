package lambda_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// A queue message keeps its trace through the event source mapping: the
// invocation is a span of that trace and the function runtime gets it.
func TestESMInvokeSpan(t *testing.T) {
	need(t, "python3")
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	c, q := lambda.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	in := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("tin")}))
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("tracer"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "import os\ndef handler(e, c):\n    return os.environ['_X_AMZN_TRACE_ID']\n"})}}))
	root := "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	res := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("tracer")}, func(o *lambda.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("X-Amzn-Trace-Id", "Root="+root))
	}))
	if !strings.Contains(string(res.Payload), "Root="+root) {
		t.Fatalf("the function runtime got %s", res.Payload)
	}
	must(c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("tracer"),
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:tin"), BatchSize: aws.Int32(1)}))
	must(q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: in.QueueUrl, MessageBody: aws.String("go"),
		MessageSystemAttributes: map[string]sqstypes.MessageSystemAttributeValue{"AWSTraceHeader": {DataType: aws.String("String"), StringValue: aws.String("Root=" + root + "-esm")}}}))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range h.Spans.All() {
			if s.Kind == core.SpanInvoke && s.Trace == root+"-esm" {
				if !strings.HasSuffix(s.From, ":tin") || !strings.HasSuffix(s.To, ":function:tracer") || s.Op != "Invoke tracer" {
					t.Fatalf("span = %+v", s)
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no invoke span, have %+v", h.Spans.All())
}
