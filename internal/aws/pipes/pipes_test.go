package pipes_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	gwtypes "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	ltypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	"github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	apigw "github.com/nrndev/pogi/internal/aws/apigateway"
	dynamodbsvc "github.com/nrndev/pogi/internal/aws/dynamodb"
	"github.com/nrndev/pogi/internal/aws/events"
	kinesissvc "github.com/nrndev/pogi/internal/aws/kinesis"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	pipessvc "github.com/nrndev/pogi/internal/aws/pipes"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/aws/stepfunctions"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

const role = "arn:aws:iam::000000000000:role/pipe"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	hub *core.Hub
	svc *pipessvc.Service
	pp  *pipes.Client
	sq  *sqs.Client
	fn  *lambda.Client
	sf  *sfn.Client
	ks  *kinesis.Client
	db  *dynamodb.Client
	eb  *eventbridge.Client
	gw  *apigatewayv2.Client
}

func setup(t *testing.T) *env {
	e := &env{}
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { e.svc = pipessvc.New(h); return e.svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return stepfunctions.New(h) },
		func(h *core.Hub) core.Service { return kinesissvc.New(h) },
		func(h *core.Hub) core.Service { return dynamodbsvc.New(h) },
		func(h *core.Hub) core.Service { return events.New(h) },
		func(h *core.Hub) core.Service { return apigw.New(h) })
	cfg := awstest.Config(t, url)
	e.hub, e.pp, e.sq, e.fn = h, pipes.NewFromConfig(cfg), sqs.NewFromConfig(cfg), lambda.NewFromConfig(cfg)
	e.sf, e.ks, e.db, e.eb = sfn.NewFromConfig(cfg), kinesis.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), eventbridge.NewFromConfig(cfg)
	e.gw = apigatewayv2.NewFromConfig(cfg)
	return e
}

func need(t *testing.T, bin string) {
	if _, err := exec.LookPath(bin); err != nil {
		t.Skipf("%s not installed", bin)
	}
}

// eventually retries f for up to 15 seconds.
func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	for end := time.Now().Add(15 * time.Second); !f(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// state drives the clock until the pipe reports want.
func (e *env) state(t *testing.T, name string, want types.PipeState) *pipes.DescribePipeOutput {
	t.Helper()
	var out *pipes.DescribePipeOutput
	eventually(t, name+" "+string(want), func() bool {
		e.svc.Tick(time.Now())
		var err error
		out, err = e.pp.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String(name)})
		return err == nil && out.CurrentState == want
	})
	return out
}

// queue creates a queue and returns its URL and ARN.
func (e *env) queue(name string, attrs ...string) (string, string) {
	in := &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: map[string]string{}}
	for i := 0; i+1 < len(attrs); i += 2 {
		in.Attributes[attrs[i]] = attrs[i+1]
	}
	q := must(e.sq.CreateQueue(ctx, in))
	return *q.QueueUrl, "arn:aws:sqs:us-east-1:000000000000:" + name
}

// receive waits for the next message of a queue and returns its body.
func (e *env) receive(t *testing.T, url string) string {
	t.Helper()
	out := must(e.sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), WaitTimeSeconds: 15}))
	if len(out.Messages) != 1 {
		t.Fatalf("%s: got %d messages", url, len(out.Messages))
	}
	must(e.sq.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: out.Messages[0].ReceiptHandle}))
	return *out.Messages[0].Body
}

// drained waits until a queue holds no message, in flight or not.
func (e *env) drained(t *testing.T, url string) {
	t.Helper()
	eventually(t, url+" to drain", func() bool {
		a := must(e.sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
			AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}}))
		return a.Attributes["ApproximateNumberOfMessages"] == "0" && a.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
	})
}

func (e *env) function(name, code string) string {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("app.py")
	w.Write([]byte(code))
	zw.Close()
	out := must(e.fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: ltypes.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String(role), Code: &ltypes.FunctionCode{ZipFile: buf.Bytes()}}))
	return *out.FunctionArn
}

func (e *env) machine(name string, typ sfntypes.StateMachineType, def string) string {
	return *must(e.sf.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String(name), RoleArn: aws.String(role),
		Type: typ, Definition: aws.String(def)})).StateMachineArn
}

func TestPipeLifecycle(t *testing.T) {
	e := setup(t)
	e.svc.Close() // the test drives the clock
	_, in := e.queue("in")
	_, out := e.queue("out")
	created := must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("p1"), RoleArn: aws.String(role), Source: aws.String(in), Target: aws.String(out),
		SourceParameters: &types.PipeSourceParameters{SqsQueueParameters: &types.PipeSourceSqsQueueParameters{BatchSize: aws.Int32(5)},
			FilterCriteria: &types.FilterCriteria{Filters: []types.Filter{{Pattern: aws.String(`{"body":{"k":["v"]}}`)}}}},
		TargetParameters: &types.PipeTargetParameters{InputTemplate: aws.String(`{"id": <$.messageId>}`)},
		Tags:             map[string]string{"team": "a"}}))
	if created.CurrentState != types.PipeStateCreating || created.DesiredState != types.RequestedPipeStateRunning ||
		*created.Arn != "arn:aws:pipes:us-east-1:000000000000:pipe/p1" || created.CreationTime == nil {
		t.Fatalf("created = %+v", created)
	}
	d := e.state(t, "p1", types.PipeStateRunning)
	if *d.SourceParameters.SqsQueueParameters.BatchSize != 5 || *d.SourceParameters.FilterCriteria.Filters[0].Pattern != `{"body":{"k":["v"]}}` ||
		*d.TargetParameters.InputTemplate != `{"id": <$.messageId>}` || d.Tags["team"] != "a" || *d.RoleArn != role {
		t.Fatalf("described = %+v", d)
	}

	// Stop passes through STOPPING; nothing else may change the pipe meanwhile.
	var conflict *types.ConflictException
	if st := must(e.pp.StopPipe(ctx, &pipes.StopPipeInput{Name: aws.String("p1")})); st.CurrentState != types.PipeStateStopping || st.DesiredState != types.RequestedPipeStateStopped {
		t.Fatalf("stop = %+v", st)
	}
	if _, err := e.pp.StartPipe(ctx, &pipes.StartPipeInput{Name: aws.String("p1")}); !errors.As(err, &conflict) {
		t.Fatalf("start while stopping: %v", err)
	}
	e.state(t, "p1", types.PipeStateStopped)
	if st := must(e.pp.StartPipe(ctx, &pipes.StartPipeInput{Name: aws.String("p1")})); st.CurrentState != types.PipeStateStarting {
		t.Fatalf("start = %+v", st)
	}
	e.state(t, "p1", types.PipeStateRunning)

	// Update changes what it sends and keeps the rest.
	upd := must(e.pp.UpdatePipe(ctx, &pipes.UpdatePipeInput{Name: aws.String("p1"), RoleArn: aws.String(role), Description: aws.String("v2"),
		TargetParameters: &types.PipeTargetParameters{InputTemplate: aws.String(`"x"`)}}))
	if upd.CurrentState != types.PipeStateUpdating {
		t.Fatalf("update = %+v", upd)
	}
	d = e.state(t, "p1", types.PipeStateRunning)
	if *d.Description != "v2" || *d.TargetParameters.InputTemplate != `"x"` || *d.SourceParameters.SqsQueueParameters.BatchSize != 5 {
		t.Fatalf("updated = %+v", d)
	}

	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("p2"), RoleArn: aws.String(role), Source: aws.String(in), Target: aws.String(out),
		DesiredState: types.RequestedPipeStateStopped}))
	e.state(t, "p2", types.PipeStateStopped)
	if l := must(e.pp.ListPipes(ctx, &pipes.ListPipesInput{NamePrefix: aws.String("p")})); len(l.Pipes) != 2 || *l.Pipes[0].Name != "p1" || *l.Pipes[0].Source != in {
		t.Fatalf("list = %+v", l.Pipes)
	}
	if l := must(e.pp.ListPipes(ctx, &pipes.ListPipesInput{CurrentState: types.PipeStateStopped})); len(l.Pipes) != 1 || *l.Pipes[0].Name != "p2" {
		t.Fatalf("stopped = %+v", l.Pipes)
	}
	first := must(e.pp.ListPipes(ctx, &pipes.ListPipesInput{Limit: aws.Int32(1)}))
	second := must(e.pp.ListPipes(ctx, &pipes.ListPipesInput{Limit: aws.Int32(1), NextToken: first.NextToken}))
	if len(first.Pipes) != 1 || first.NextToken == nil || len(second.Pipes) != 1 || *second.Pipes[0].Name != "p2" || second.NextToken != nil {
		t.Fatalf("pages = %+v %+v", first, second)
	}

	must(e.pp.TagResource(ctx, &pipes.TagResourceInput{ResourceArn: created.Arn, Tags: map[string]string{"env": "dev"}}))
	must(e.pp.UntagResource(ctx, &pipes.UntagResourceInput{ResourceArn: created.Arn, TagKeys: []string{"team"}}))
	if tags := must(e.pp.ListTagsForResource(ctx, &pipes.ListTagsForResourceInput{ResourceArn: created.Arn})).Tags; len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags = %v", tags)
	}

	// Error codes SDKs branch on.
	var nf *types.NotFoundException
	var bad *types.ValidationException
	if _, err := e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("p1"), RoleArn: aws.String(role), Source: aws.String(in), Target: aws.String(out)}); !errors.As(err, &conflict) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := e.pp.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("nope")}); !errors.As(err, &nf) {
		t.Fatalf("missing: %v", err)
	}
	for _, in := range []*pipes.CreatePipeInput{
		{Source: aws.String("arn:aws:mq:us-east-1:000000000000:broker:b:1")},
		{Source: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/s")}, // no StartingPosition
		{Source: aws.String(in), SourceParameters: &types.PipeSourceParameters{FilterCriteria: &types.FilterCriteria{Filters: []types.Filter{{Pattern: aws.String(`{"body":"v"}`)}}}}},
	} {
		in.Name, in.RoleArn, in.Target = aws.String("bad"), aws.String(role), aws.String(out)
		if _, err := e.pp.CreatePipe(ctx, in); !errors.As(err, &bad) {
			t.Fatalf("invalid %s: %v", *in.Source, err)
		}
	}

	// Snapshots round-trip the pipes.
	snap := must(e.hub.Snapshot())
	e.hub.Reset("pipes")
	if _, err := e.pp.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("p1")}); !errors.As(err, &nf) {
		t.Fatalf("after reset: %v", err)
	}
	if err := e.hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	e.state(t, "p1", types.PipeStateRunning)

	if del := must(e.pp.DeletePipe(ctx, &pipes.DeletePipeInput{Name: aws.String("p1")})); del.CurrentState != types.PipeStateDeleting {
		t.Fatalf("delete = %+v", del)
	}
	eventually(t, "p1 to go", func() bool {
		e.svc.Tick(time.Now())
		_, err := e.pp.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("p1")})
		return errors.As(err, &nf)
	})
}

// TestSQSEnrichedIntoStepFunctions is the done-when: a message goes from SQS
// through a Lambda enrichment into a state machine, and a filter drops the noise.
func TestSQSEnrichedIntoStepFunctions(t *testing.T) {
	need(t, "python3")
	e := setup(t)
	url, arn := e.queue("orders")
	enrich := e.function("enrich", "def handler(event, context):\n    return [dict(e, enriched=True) for e in event]\n")
	flow := e.machine("flow", sfntypes.StateMachineTypeStandard, `{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`)
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("orders"), RoleArn: aws.String(role), Source: aws.String(arn),
		SourceParameters: &types.PipeSourceParameters{FilterCriteria: &types.FilterCriteria{Filters: []types.Filter{{Pattern: aws.String(`{"body":{"type":["order"]}}`)}}}},
		Enrichment:       aws.String(enrich), Target: aws.String(flow),
		TargetParameters: &types.PipeTargetParameters{StepFunctionStateMachineParameters: &types.PipeTargetStateMachineParameters{
			InvocationType: types.PipeTargetInvocationTypeFireAndForget}}}))
	e.state(t, "orders", types.PipeStateRunning)
	for _, body := range []string{`{"type":"noise"}`, `{"type":"order","id":7}`} {
		must(e.sq.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String(body)}))
	}

	var input []map[string]any
	eventually(t, "an execution", func() bool {
		ex := must(e.sf.ListExecutions(ctx, &sfn.ListExecutionsInput{StateMachineArn: aws.String(flow)}))
		if len(ex.Executions) == 0 {
			return false
		}
		d := must(e.sf.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: ex.Executions[0].ExecutionArn}))
		return json.Unmarshal([]byte(*d.Input), &input) == nil
	})
	if len(input) != 1 || input[0]["enriched"] != true || input[0]["body"] != `{"type":"order","id":7}` ||
		input[0]["eventSource"] != "aws:sqs" || input[0]["eventSourceARN"] != arn {
		t.Fatalf("execution input = %v", input)
	}
	e.drained(t, url) // the order was taken, the noise filtered out
	if ex := must(e.sf.ListExecutions(ctx, &sfn.ListExecutionsInput{StateMachineArn: aws.String(flow)})); len(ex.Executions) != 1 {
		t.Fatalf("executions = %d", len(ex.Executions))
	}
}

func TestStreamSources(t *testing.T) {
	e := setup(t)

	// Kinesis: data that holds JSON is filtered and templated as an object;
	// the FIFO target's group is a JSON path into the record.
	must(e.ks.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("clicks"), ShardCount: aws.Int32(1)}))
	clicksURL, clicksOut := e.queue("clicks-out.fifo", "FifoQueue", "true")
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("clicks"), RoleArn: aws.String(role),
		Source: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/clicks"),
		SourceParameters: &types.PipeSourceParameters{
			KinesisStreamParameters: &types.PipeSourceKinesisStreamParameters{StartingPosition: types.KinesisStreamStartPositionTrimHorizon},
			FilterCriteria:          &types.FilterCriteria{Filters: []types.Filter{{Pattern: aws.String(`{"data":{"kind":["keep"]}}`)}}}},
		Target: aws.String(clicksOut),
		TargetParameters: &types.PipeTargetParameters{InputTemplate: aws.String(`{"pk": "<$.partitionKey>", "n": <$.data.n>, "pipe": "<aws.pipes.pipe-name>"}`),
			SqsQueueParameters: &types.PipeTargetSqsQueueParameters{MessageGroupId: aws.String("$.partitionKey")}}}))
	for _, rec := range []struct{ pk, data string }{{"b", `{"kind":"drop","n":2}`}, {"a", `{"kind":"keep","n":1}`}} {
		must(e.ks.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: aws.String("clicks"), PartitionKey: aws.String(rec.pk), Data: []byte(rec.data)}))
	}
	got := must(e.sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(clicksURL), WaitTimeSeconds: 15,
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"MessageGroupId"}}))
	if len(got.Messages) != 1 || *got.Messages[0].Body != `{"pk": "a", "n": 1, "pipe": "clicks"}` || got.Messages[0].Attributes["MessageGroupId"] != "a" {
		t.Fatalf("kinesis = %+v", got.Messages)
	}
	must(e.sq.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(clicksURL), ReceiptHandle: got.Messages[0].ReceiptHandle}))
	if extra := must(e.sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(clicksURL)})); len(extra.Messages) != 0 {
		t.Fatalf("filtered record delivered: %s", *extra.Messages[0].Body)
	}

	// DynamoDB Streams from LATEST, enriched by an express state machine.
	table := must(e.db.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("items"), BillingMode: dbtypes.BillingModePayPerRequest,
		KeySchema:            []dbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dbtypes.KeyTypeHash}},
		AttributeDefinitions: []dbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: dbtypes.ScalarAttributeTypeS}},
		StreamSpecification:  &dbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: dbtypes.StreamViewTypeNewImage}}))
	stream := *table.TableDescription.LatestStreamArn
	wrap := e.machine("wrap", sfntypes.StateMachineTypeExpress, `{"StartAt":"Wrap","States":{"Wrap":{"Type":"Pass","Parameters":{"records.$":"$","via":"sfn"},"End":true}}}`)
	itemsURL, itemsOut := e.queue("items-out")
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("items"), RoleArn: aws.String(role), Source: aws.String(stream),
		SourceParameters: &types.PipeSourceParameters{DynamoDBStreamParameters: &types.PipeSourceDynamoDBStreamParameters{StartingPosition: types.DynamoDBStreamStartPositionLatest}},
		Enrichment:       aws.String(wrap), Target: aws.String(itemsOut)}))
	e.state(t, "items", types.PipeStateRunning)
	must(e.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("items"), Item: map[string]dbtypes.AttributeValue{"id": &dbtypes.AttributeValueMemberS{Value: "i1"}}}))
	var wrapped struct {
		Via     string
		Records []struct {
			EventName, EventSourceARN string
			Dynamodb                  struct{ NewImage map[string]map[string]string }
		}
	}
	if err := json.Unmarshal([]byte(e.receive(t, itemsURL)), &wrapped); err != nil || wrapped.Via != "sfn" || len(wrapped.Records) != 1 ||
		wrapped.Records[0].EventName != "INSERT" || wrapped.Records[0].EventSourceARN != stream || wrapped.Records[0].Dynamodb.NewImage["id"]["S"] != "i1" {
		t.Fatalf("dynamodb = %+v (%v)", wrapped, err)
	}
}

func TestBusTargetAndRetries(t *testing.T) {
	e := setup(t)

	// An event bus target gets one event per record, from "Pipe <name>".
	busURL, busIn := e.queue("bus-in")
	seenURL, seen := e.queue("seen")
	must(e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("from-pipe"), EventPattern: aws.String(`{"source":["Pipe to-bus"],"detail-type":["Event from aws:sqs"]}`)}))
	must(e.eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String("from-pipe"), Targets: []ebtypes.Target{{Id: aws.String("q"), Arn: aws.String(seen)}}}))
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("to-bus"), RoleArn: aws.String(role), Source: aws.String(busIn),
		Target: aws.String("arn:aws:events:us-east-1:000000000000:event-bus/default")}))
	must(e.sq.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(busURL), MessageBody: aws.String("hello")}))
	var ev struct {
		Source string
		Detail struct{ Body string }
	}
	if err := json.Unmarshal([]byte(e.receive(t, seenURL)), &ev); err != nil || ev.Source != "Pipe to-bus" || ev.Detail.Body != "hello" {
		t.Fatalf("bus event = %+v (%v)", ev, err)
	}

	// A batch the target refuses stays on the source and is retried, here
	// once the pipe points at a target that works.
	inURL, in := e.queue("retry-in", "VisibilityTimeout", "1")
	outURL, out := e.queue("retry-out")
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("retry"), RoleArn: aws.String(role), Source: aws.String(in),
		Target: aws.String("arn:aws:lambda:us-east-1:000000000000:function:missing")}))
	must(e.sq.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(inURL), MessageBody: aws.String("again")}))
	eventually(t, "a failed batch", func() bool {
		d := must(e.pp.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("retry")}))
		return d.StateReason != nil && strings.Contains(*d.StateReason, "missing")
	})
	must(e.pp.UpdatePipe(ctx, &pipes.UpdatePipeInput{Name: aws.String("retry"), RoleArn: aws.String(role), Target: aws.String(out)}))
	if got := e.receive(t, outURL); !strings.Contains(got, `"body":"again"`) {
		t.Fatalf("retried = %s", got)
	}
	e.drained(t, inURL)
}

func TestHTTPEnrichments(t *testing.T) {
	e := setup(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, `{"via": %q, "got": %s}`, r.Header.Get("X-Key")+r.URL.Path, b)
	}))
	defer backend.Close()

	// An API destination gets each event, shaped by the enrichment's input template.
	conn := must(e.eb.CreateConnection(ctx, &eventbridge.CreateConnectionInput{Name: aws.String("backend"), AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
			ApiKeyName: aws.String("X-Key"), ApiKeyValue: aws.String("k1")}}}))
	dest := must(e.eb.CreateApiDestination(ctx, &eventbridge.CreateApiDestinationInput{Name: aws.String("backend"), ConnectionArn: conn.ConnectionArn,
		InvocationEndpoint: aws.String(backend.URL + "/dest"), HttpMethod: ebtypes.ApiDestinationHttpMethodPost}))
	inURL, in := e.queue("dest-in")
	outURL, out := e.queue("dest-out")
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("via-dest"), RoleArn: aws.String(role), Source: aws.String(in),
		Enrichment: dest.ApiDestinationArn, EnrichmentParameters: &types.PipeEnrichmentParameters{InputTemplate: aws.String(`{"msg": <$.body>}`)},
		Target: aws.String(out)}))
	must(e.sq.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(inURL), MessageBody: aws.String(`{"n":1}`)}))
	if got := e.receive(t, outURL); got != `{"via": "k1/dest", "got": {"msg": {"n":1}}}` {
		t.Fatalf("api destination = %s", got)
	}

	// So does an API Gateway route: an HTTP API proxying to the backend.
	api := must(e.gw.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("enrich"), ProtocolType: gwtypes.ProtocolTypeHttp}))
	integ := must(e.gw.CreateIntegration(ctx, &apigatewayv2.CreateIntegrationInput{ApiId: api.ApiId, IntegrationType: gwtypes.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String(backend.URL + "/api"), IntegrationMethod: aws.String("POST"), PayloadFormatVersion: aws.String("1.0")}))
	must(e.gw.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: api.ApiId, RouteKey: aws.String("POST /enrich"), Target: aws.String("integrations/" + *integ.IntegrationId)}))
	must(e.gw.CreateStage(ctx, &apigatewayv2.CreateStageInput{ApiId: api.ApiId, StageName: aws.String("$default"), AutoDeploy: aws.Bool(true)}))
	apiURL, apiIn := e.queue("api-in")
	must(e.pp.CreatePipe(ctx, &pipes.CreatePipeInput{Name: aws.String("via-api"), RoleArn: aws.String(role), Source: aws.String(apiIn),
		Enrichment: aws.String("arn:aws:execute-api:us-east-1:000000000000:" + *api.ApiId + "/$default/POST/enrich"), Target: aws.String(out)}))
	must(e.sq.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(apiURL), MessageBody: aws.String("two")}))
	var got struct {
		Via string
		Got struct{ Body, EventSource string }
	}
	if err := json.Unmarshal([]byte(e.receive(t, outURL)), &got); err != nil || got.Via != "/api" || got.Got.Body != "two" || got.Got.EventSource != "aws:sqs" {
		t.Fatalf("api gateway = %+v (%v)", got, err)
	}
}
