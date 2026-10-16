package lambda_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	ddbsvc "github.com/nrndev/pogi/internal/aws/dynamodb"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// esmFn writes the keys of each batch to a file in $OUT, then acts on $MODE.
const esmFn = `
import json, os, time
def handler(event, context):
    out, mode = os.environ["OUT"], os.environ.get("MODE", "ok")
    recs = event["Records"]
    if "dynamodb" in recs[0]:
        pks = [r["dynamodb"]["Keys"]["pk"]["S"] for r in recs]
    else:
        pks = [r["body"] for r in recs]
    with open(os.path.join(out, "%.6f.json" % time.time()), "w") as f:
        json.dump(pks, f)
    if mode == "poison" and "poison" in pks:
        raise Exception("poison")
    if mode == "report" and "b" in pks and not os.path.exists(os.path.join(out, "reported")):
        open(os.path.join(out, "reported"), "w").close()
        return {"batchItemFailures": [{"itemIdentifier": r["dynamodb"]["SequenceNumber"]} for r in recs if r["dynamodb"]["Keys"]["pk"]["S"] == "b"]}
    if mode.startswith("{") or mode == "null":
        return json.loads(mode)
`

type esmEnv struct {
	c   *lambda.Client
	d   *dynamodb.Client
	q   *sqs.Client
	s3  *s3.Client
	out string
}

func esmSetup(t *testing.T, mode string) *esmEnv {
	need(t, "python3")
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return ddbsvc.New(h) },
		func(h *core.Hub) core.Service { return s3svc.New(h) })
	cfg := awstest.Config(t, url)
	e := &esmEnv{lambda.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), sqs.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), t.TempDir()}
	must(e.c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("fn"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Timeout: aws.Int32(10), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": esmFn})},
		Environment: &types.Environment{Variables: map[string]string{"OUT": e.out, "MODE": mode}}}))
	return e
}

// table makes a table with a stream and returns the stream ARN.
func (e *esmEnv) table() string {
	d := must(e.d.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("strm"), BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewImage}}))
	return aws.ToString(d.TableDescription.LatestStreamArn)
}

func (e *esmEnv) put(pks ...string) {
	for _, pk := range pks {
		must(e.d.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("strm"), Item: map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: pk}}}))
	}
}

// calls returns the keys of each invocation, oldest first.
func (e *esmEnv) calls() [][]string {
	files, _ := filepath.Glob(filepath.Join(e.out, "*.json"))
	sort.Strings(files)
	var out [][]string
	for _, f := range files {
		b, _ := os.ReadFile(f)
		var pks []string
		json.Unmarshal(b, &pks)
		out = append(out, pks)
	}
	return out
}

func (e *esmEnv) wait(cond func([][]string) bool) [][]string {
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if c := e.calls(); cond(c) {
			return c
		}
	}
	return e.calls()
}

// floci #4311: a LATEST stream mapping takes its position when it is created, so a record
// written right after CreateEventSourceMapping is delivered and an older one is not.
func TestStreamLatestIsTakenAtCreate(t *testing.T) {
	e := esmSetup(t, "ok")
	arn := e.table()
	e.put("old")
	must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"), EventSourceArn: aws.String(arn),
		StartingPosition: types.EventSourcePositionLatest, BatchSize: aws.Int32(10)}))
	e.put("new")
	if c := e.wait(func(c [][]string) bool { return len(c) > 0 }); len(c) != 1 || !slices.Equal(c[0], []string{"new"}) {
		t.Fatalf("calls = %v, want only [new]", c)
	}
}

// floci #3262: with ReportBatchItemFailures the retry of a stream batch starts at the failed record.
func TestStreamBatchItemFailures(t *testing.T) {
	e := esmSetup(t, "report")
	arn := e.table()
	e.put("a", "b", "c")
	must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"), EventSourceArn: aws.String(arn),
		StartingPosition: types.EventSourcePositionTrimHorizon, BatchSize: aws.Int32(10),
		FunctionResponseTypes: []types.FunctionResponseType{types.FunctionResponseTypeReportBatchItemFailures}}))
	c := e.wait(func(c [][]string) bool { return len(c) >= 2 })
	if len(c) < 2 || !slices.Equal(c[1], []string{"b", "c"}) {
		t.Fatalf("calls = %v, want the second to start at b", c)
	}
}

// floci #4312 and #1537: BisectBatchOnFunctionError is stored and returned, and a failing
// stream batch is split so the good records are still processed.
func TestStreamBisectBatch(t *testing.T) {
	e := esmSetup(t, "poison")
	arn := e.table()
	e.put("a", "b", "poison", "c")
	m := must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"), EventSourceArn: aws.String(arn),
		StartingPosition: types.EventSourcePositionTrimHorizon, BatchSize: aws.Int32(10), BisectBatchOnFunctionError: aws.Bool(true), MaximumRetryAttempts: aws.Int32(0)}))
	if !aws.ToBool(m.BisectBatchOnFunctionError) {
		t.Errorf("BisectBatchOnFunctionError = %v, want true", m.BisectBatchOnFunctionError)
	}
	g := must(e.c.GetEventSourceMapping(ctx, &lambda.GetEventSourceMappingInput{UUID: m.UUID}))
	if !aws.ToBool(g.BisectBatchOnFunctionError) {
		t.Errorf("Get: BisectBatchOnFunctionError = %v, want true", g.BisectBatchOnFunctionError)
	}
	good := map[string]bool{}
	e.wait(func(c [][]string) bool {
		for _, call := range c {
			if !slices.Contains(call, "poison") {
				for _, pk := range call {
					good[pk] = true
				}
			}
		}
		return good["a"] && good["b"] && good["c"]
	})
	if !(good["a"] && good["b"] && good["c"]) {
		t.Errorf("good records processed: %v (calls %v)", good, e.calls())
	}
}

// floci #3152: a batchItemFailures reply that is not well formed fails the whole SQS batch.
func TestSQSMalformedBatchItemFailures(t *testing.T) {
	for name, reply := range map[string]string{
		"empty id":   `{"batchItemFailures":[{"itemIdentifier":""}]}`,
		"null id":    `{"batchItemFailures":[{"itemIdentifier":null}]}`,
		"unknown id": `{"batchItemFailures":[{"itemIdentifier":"nope"}]}`,
		"wrong key":  `{"batchItemFailures":[{"itemidentifier":"x"}]}`,
		"non-array":  `{"batchItemFailures":"x"}`,
		"empty list": `{"batchItemFailures":[]}`,
		"null reply": `null`,
	} {
		t.Run(name, func(t *testing.T) {
			e := esmSetup(t, reply)
			qu := must(e.q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q"), Attributes: map[string]string{"VisibilityTimeout": "30"}})).QueueUrl
			must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"),
				EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:q"), BatchSize: aws.Int32(10),
				FunctionResponseTypes: []types.FunctionResponseType{types.FunctionResponseTypeReportBatchItemFailures}}))
			for _, b := range []string{"a", "b"} {
				must(e.q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: qu, MessageBody: aws.String(b)}))
			}
			e.wait(func(c [][]string) bool { return len(c) > 0 })
			ok := name == "empty list" || name == "null reply" // these succeed: the batch is deleted
			if !ok {
				time.Sleep(1500 * time.Millisecond) // give a wrong delete time to happen
			}
			for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
				a := must(e.q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: qu, AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessagesNotVisible"}}))
				if n := a.Attributes["ApproximateNumberOfMessagesNotVisible"]; ok && n == "0" || !ok && n == "2" {
					return
				}
			}
			t.Errorf("messages in flight are wrong for %s", name)
		})
	}
}

// floci #3155: an underfilled SQS batch waits for MaximumBatchingWindowInSeconds; a full batch does not.
func TestSQSBatchingWindow(t *testing.T) {
	e := esmSetup(t, "ok")
	qu := must(e.q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q")})).QueueUrl
	must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"),
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:q"), BatchSize: aws.Int32(2), MaximumBatchingWindowInSeconds: aws.Int32(3)}))
	start := time.Now()
	must(e.q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: qu, MessageBody: aws.String("one")}))
	time.Sleep(1500 * time.Millisecond)
	if c := e.calls(); len(c) != 0 {
		t.Fatalf("delivered %v inside the window", c)
	}
	if c := e.wait(func(c [][]string) bool { return len(c) > 0 }); len(c) != 1 || time.Since(start) < 3*time.Second {
		t.Fatalf("calls = %v after %v, want one call after the 3s window", c, time.Since(start))
	}
	must(e.q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: qu, MessageBody: aws.String("two")}))
	must(e.q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: qu, MessageBody: aws.String("three")}))
	if c := e.wait(func(c [][]string) bool { return len(c) > 1 }); len(c) != 2 || time.Since(start) > 6*time.Second {
		t.Errorf("a full batch should not wait: calls = %v after %v", c, time.Since(start))
	}
}

// floci #3620: an exhausted stream batch is written to an S3 OnFailure bucket.
func TestStreamOnFailureToS3(t *testing.T) {
	e := esmSetup(t, "poison")
	must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("dlq-bucket")}))
	arn := e.table()
	e.put("poison")
	must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"), EventSourceArn: aws.String(arn),
		StartingPosition: types.EventSourcePositionTrimHorizon, BatchSize: aws.Int32(1), MaximumRetryAttempts: aws.Int32(0),
		DestinationConfig: &types.DestinationConfig{OnFailure: &types.OnFailure{Destination: aws.String("arn:aws:s3:::dlq-bucket")}}}))
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if l := must(e.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("dlq-bucket")})); len(l.Contents) > 0 {
			if !strings.HasPrefix(aws.ToString(l.Contents[0].Key), "aws/lambda/") {
				t.Errorf("key = %s", aws.ToString(l.Contents[0].Key))
			}
			return
		}
	}
	t.Error("no object written to the S3 OnFailure bucket")
}

// floci #4313: ScalingConfig is stored, returned and can be changed (a template mapping goes through the same calls).
func TestMappingScalingConfig(t *testing.T) {
	e := esmSetup(t, "ok")
	must(e.q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("scaled")}))
	arn := "arn:aws:sqs:us-east-1:000000000000:scaled"
	m := must(e.c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fn"), EventSourceArn: aws.String(arn),
		ScalingConfig: &types.ScalingConfig{MaximumConcurrency: aws.Int32(4)}}))
	if m.ScalingConfig == nil || aws.ToInt32(m.ScalingConfig.MaximumConcurrency) != 4 {
		t.Fatalf("create: ScalingConfig = %+v", m.ScalingConfig)
	}
	u := must(e.c.UpdateEventSourceMapping(ctx, &lambda.UpdateEventSourceMappingInput{UUID: m.UUID, ScalingConfig: &types.ScalingConfig{MaximumConcurrency: aws.Int32(6)}}))
	g := must(e.c.GetEventSourceMapping(ctx, &lambda.GetEventSourceMappingInput{UUID: m.UUID}))
	if aws.ToInt32(u.ScalingConfig.MaximumConcurrency) != 6 || aws.ToInt32(g.ScalingConfig.MaximumConcurrency) != 6 {
		t.Fatalf("update: %+v / get: %+v", u.ScalingConfig, g.ScalingConfig)
	}
}
