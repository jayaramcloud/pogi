package s3_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// An upload's trace follows its notification into the queue; the span names the bucket.
func TestNotificationKeepsTrace(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) }, func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	c, q := s3.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	must(c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("traced")}))
	must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("tq")}))
	arn := "arn:aws:sqs:us-east-1:000000000000:tq"
	must(c.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("traced"),
		NotificationConfiguration: &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{QueueArn: aws.String(arn), Events: []types.Event{"s3:ObjectCreated:*"}}}}}))
	root := "1-5f84c7a1-31d9a2b4c5e6f7a8b9c0d1e2"
	must(c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("traced"), Key: aws.String("a.txt"), Body: strings.NewReader("x")}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("X-Amzn-Trace-Id", "Root="+root))
	}))
	var found bool
	for _, s := range h.Spans.All() {
		found = found || s.Trace == root && s.From == "arn:aws:s3:::traced" && s.To == arn
	}
	if !found {
		t.Fatalf("no span from the bucket on %s: %+v", root, h.Spans.All())
	}
	if links := h.Service("s3").(core.Linker).Links(); len(links) != 1 || links[0].To != arn || links[0].From != "arn:aws:s3:::traced" {
		t.Fatalf("links = %+v", links)
	}
}
