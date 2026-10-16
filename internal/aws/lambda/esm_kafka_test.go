package lambda_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	mskapi "github.com/aws/aws-sdk-go-v2/service/kafka"
	msktypes "github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nrndev/pogi/internal/aws/kafka"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/twmb/franz-go/pkg/kgo"
)

// floci #3062: a self-managed Kafka mapping is created (no EventSourceArn) and described; bad input is refused.
func TestKafkaMappingCRUD(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "def handler(e, c):\n    return e\n"})}}))
	in := &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("f"), Topics: []string{"t"},
		SelfManagedEventSource:            &types.SelfManagedEventSource{Endpoints: map[string][]string{"KAFKA_BOOTSTRAP_SERVERS": {"127.0.0.1:1"}}},
		SourceAccessConfigurations:        []types.SourceAccessConfiguration{{Type: types.SourceAccessTypeVpcSubnet, URI: aws.String("subnet-1")}},
		SelfManagedKafkaEventSourceConfig: &types.SelfManagedKafkaEventSourceConfig{ConsumerGroupId: aws.String("g1")}}
	m := must(c.CreateEventSourceMapping(ctx, in))
	if aws.ToString(m.UUID) == "" || m.EventSourceArn != nil || len(m.Topics) != 1 || m.SelfManagedEventSource == nil || aws.ToInt32(m.BatchSize) != 100 ||
		m.SelfManagedKafkaEventSourceConfig == nil || aws.ToString(m.SelfManagedKafkaEventSourceConfig.ConsumerGroupId) != "g1" || len(m.SourceAccessConfigurations) != 1 {
		t.Fatalf("created = %+v", m)
	}
	got := must(c.GetEventSourceMapping(ctx, &lambda.GetEventSourceMappingInput{UUID: m.UUID}))
	if got.SelfManagedEventSource.Endpoints["KAFKA_BOOTSTRAP_SERVERS"][0] != "127.0.0.1:1" || got.StartingPosition != types.EventSourcePositionLatest {
		t.Errorf("get = %+v", got)
	}
	if l := must(c.ListEventSourceMappings(ctx, &lambda.ListEventSourceMappingsInput{FunctionName: aws.String("f")})); len(l.EventSourceMappings) != 1 {
		t.Errorf("list = %+v", l.EventSourceMappings)
	}
	bad := *in
	bad.Topics = []string{"a", "b"}
	if _, err := c.CreateEventSourceMapping(ctx, &bad); err == nil {
		t.Error("two topics accepted")
	}
	bad = *in
	bad.SelfManagedEventSource = &types.SelfManagedEventSource{Endpoints: map[string][]string{}}
	if _, err := c.CreateEventSourceMapping(ctx, &bad); err == nil {
		t.Error("no bootstrap servers accepted")
	}
	must(c.DeleteEventSourceMapping(ctx, &lambda.DeleteEventSourceMappingInput{UUID: m.UUID}))

	// An MSK cluster ARN is a source too: the mapping is described with that ARN and the MSK config, and no brokers.
	arn := "arn:aws:kafka:us-east-1:000000000000:cluster/demo/abc"
	msk := must(c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("f"), EventSourceArn: aws.String(arn), Topics: []string{"t"},
		StartingPosition: types.EventSourcePositionTrimHorizon, AmazonManagedKafkaEventSourceConfig: &types.AmazonManagedKafkaEventSourceConfig{ConsumerGroupId: aws.String("g2")}}))
	if aws.ToString(msk.EventSourceArn) != arn || msk.SelfManagedEventSource != nil || msk.StartingPosition != types.EventSourcePositionTrimHorizon ||
		aws.ToString(msk.AmazonManagedKafkaEventSourceConfig.ConsumerGroupId) != "g2" || len(msk.Topics) != 1 {
		t.Fatalf("msk mapping = %+v", msk)
	}
	if l := must(c.ListEventSourceMappings(ctx, &lambda.ListEventSourceMappingsInput{EventSourceArn: aws.String(arn)})); len(l.EventSourceMappings) != 1 {
		t.Errorf("list by arn = %+v", l.EventSourceMappings)
	}
	if _, err := c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("f"), EventSourceArn: aws.String(arn)}); err == nil {
		t.Error("an MSK mapping without a topic was accepted")
	}
}

const kafkaFn = `
import json, os, time
def handler(event, context):
    out = os.environ["OUT"]
    with open(os.path.join(out, "%.6f.json" % time.time()), "w") as f:
        json.dump(event, f)
    flag = os.path.join(out, "failed-once")
    if not os.path.exists(flag):
        open(flag, "w").close()
        raise Exception("first try fails")
`

// A mapping reads a topic on a Redpanda broker (needs a container engine): the batch reaches the function in
// Lambda's event shape, a failed batch runs again, and committed records are not delivered twice.
func TestKafkaMappingDelivers(t *testing.T) {
	d := docker.New(docker.Options{})
	need(t, "python3")
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	rp := &kafka.Redpanda{Name: docker.Name("kafka", "lambda-test-"+core.RandHex(3)), Service: "kafka", Resource: "lambda-test"}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	t.Cleanup(func() { d.Remove(context.Background(), rp.Name) })
	broker, err := rp.Run(rctx, d)
	if err != nil {
		t.Fatal(err)
	}
	kc, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.DefaultProduceTopic("orders"), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	produce := func(values ...string) {
		t.Helper()
		var recs []*kgo.Record
		for _, v := range values {
			recs = append(recs, &kgo.Record{Key: []byte("k-" + v), Value: []byte(v), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("ab")}}})
		}
		if err := kc.ProduceSync(rctx, recs...).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	produce("one", "two", "three")

	c, _, _ := setup(t)
	out := t.TempDir()
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("k"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Environment: &types.Environment{Variables: map[string]string{"OUT": out}},
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": kafkaFn})}}))
	must(c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("k"), Topics: []string{"orders"}, StartingPosition: types.EventSourcePositionTrimHorizon,
		SelfManagedEventSource: &types.SelfManagedEventSource{Endpoints: map[string][]string{"KAFKA_BOOTSTRAP_SERVERS": {broker}}}}))

	batches := func(n int) []map[string]any {
		t.Helper()
		for end := time.Now().Add(90 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
			files, _ := filepath.Glob(filepath.Join(out, "*.json"))
			if len(files) >= n {
				sort.Strings(files)
				var all []map[string]any
				for _, f := range files {
					b, _ := os.ReadFile(f)
					var ev map[string]any
					if json.Unmarshal(b, &ev) == nil {
						all = append(all, ev)
					}
				}
				if len(all) >= n {
					return all
				}
			}
		}
		t.Fatalf("expected %d invocations", n)
		return nil
	}
	values := func(ev map[string]any, key string) []string {
		var out []string
		for _, r := range ev["records"].(map[string]any)[key].([]any) {
			v, _ := base64.StdEncoding.DecodeString(r.(map[string]any)["value"].(string))
			out = append(out, string(v))
		}
		return out
	}
	all := batches(2) // the first try fails, the same batch runs again
	for i, ev := range all[:2] {
		if ev["eventSource"] != "SelfManagedKafka" || ev["bootstrapServers"] != broker || strings.Join(values(ev, "orders-0"), ",") != "one,two,three" {
			t.Fatalf("invocation %d = %v", i, ev)
		}
	}
	r0 := all[0]["records"].(map[string]any)["orders-0"].([]any)[0].(map[string]any)
	key, _ := base64.StdEncoding.DecodeString(r0["key"].(string))
	if r0["topic"] != "orders" || r0["partition"] != float64(0) || r0["offset"] != float64(0) || string(key) != "k-one" || r0["timestampType"] != "CREATE_TIME" ||
		fmt.Sprint(r0["headers"]) != "[map[h:[97 98]]]" {
		t.Errorf("record = %v", r0)
	}
	// committed records are not delivered again; new ones are
	time.Sleep(2 * time.Second)
	produce("four")
	all = batches(3)
	if got := strings.Join(values(all[2], "orders-0"), ","); got != "four" {
		t.Fatalf("third batch = %s (all %d batches)", got, len(all))
	}
}

// An MSK mapping reads from the broker of pogi's MSK cluster (needs a container engine) and the function
// gets Lambda's MSK event shape.
func TestMSKMappingDelivers(t *testing.T) {
	d := docker.New(docker.Options{})
	need(t, "python3")
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return kafka.New(h) })
	cfg := awstest.Config(t, url)
	c, m := lambda.NewFromConfig(cfg), mskapi.NewFromConfig(cfg)
	name := "esm-" + core.RandHex(3)
	arn := must(m.CreateCluster(ctx, &mskapi.CreateClusterInput{ClusterName: aws.String(name), KafkaVersion: aws.String("3.8.x"), NumberOfBrokerNodes: aws.Int32(2),
		BrokerNodeGroupInfo: &msktypes.BrokerNodeGroupInfo{InstanceType: aws.String("kafka.t3.small"), ClientSubnets: []string{"subnet-1", "subnet-2"}}})).ClusterArn
	t.Cleanup(func() {
		m.DeleteCluster(ctx, &mskapi.DeleteClusterInput{ClusterArn: arn})
		for end := time.Now().Add(time.Minute); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
			if l, err := d.List(ctx, docker.LabelService+"=kafka", docker.LabelResource+"="+name); err == nil && len(l) == 0 {
				return
			}
		}
	})
	var brokers string
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(250 * time.Millisecond) { // the first run pulls the image
		cl := must(m.DescribeClusterV2(ctx, &mskapi.DescribeClusterV2Input{ClusterArn: arn})).ClusterInfo
		if cl.State == msktypes.ClusterStateActive {
			brokers = aws.ToString(must(m.GetBootstrapBrokers(ctx, &mskapi.GetBootstrapBrokersInput{ClusterArn: arn})).BootstrapBrokerString)
			break
		}
		if cl.State != msktypes.ClusterStateCreating || time.Now().After(end) {
			t.Fatalf("cluster is %s", cl.State)
		}
	}
	kc, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.DefaultProduceTopic("orders"), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	if err := kc.ProduceSync(ctx, &kgo.Record{Key: []byte("k"), Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	out := t.TempDir()
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("k"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Environment: &types.Environment{Variables: map[string]string{"OUT": out}},
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": kafkaFn})}}))
	must(c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("k"), EventSourceArn: arn, Topics: []string{"orders"},
		StartingPosition: types.EventSourcePositionTrimHorizon}))
	for end := time.Now().Add(90 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		files, _ := filepath.Glob(filepath.Join(out, "*.json"))
		if len(files) == 0 {
			if time.Now().After(end) {
				t.Fatal("the function was not invoked")
			}
			continue
		}
		b, _ := os.ReadFile(files[0])
		var ev struct {
			EventSource, EventSourceArn, BootstrapServers string
			Records                                       map[string][]struct{ Topic, Value string }
		}
		if err := json.Unmarshal(b, &ev); err != nil {
			continue // still being written
		}
		recs := ev.Records["orders-0"]
		if ev.EventSource != "aws:kafka" || ev.EventSourceArn != aws.ToString(arn) || ev.BootstrapServers != brokers || len(recs) != 1 || recs[0].Topic != "orders" ||
			base64.StdEncoding.EncodeToString([]byte("hello")) != recs[0].Value {
			t.Fatalf("event = %s", b)
		}
		return
	}
}
