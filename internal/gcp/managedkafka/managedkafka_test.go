package managedkafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	mk "cloud.google.com/go/managedkafka/apiv1"
	mkpb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/managedkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

const loc = "projects/p/locations/us-central1"

func start(t *testing.T, cfg pogi.Config) (*pogi.Running, *mk.Client) {
	t.Helper()
	cfg.Services = []string{"managedkafka", "operations"}
	s := pogi.Start(t, cfg)
	c := must(mk.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { c.Close() })
	return s, c
}

func newCluster(id string) *mkpb.CreateClusterRequest {
	return &mkpb.CreateClusterRequest{Parent: loc, ClusterId: id, Cluster: &mkpb.Cluster{
		Labels:         map[string]string{"env": "test"},
		CapacityConfig: &mkpb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3 << 30},
		PlatformConfig: &mkpb.Cluster_GcpConfig{GcpConfig: &mkpb.GcpConfig{AccessConfig: &mkpb.AccessConfig{
			NetworkConfigs: []*mkpb.NetworkConfig{{Subnet: "projects/p/regions/us-central1/subnetworks/default"}}}}},
	}}
}

// noEngine runs pogi as if no container engine were installed.
var noEngine = pogi.Config{DockerHost: "/nonexistent/docker.sock"}

// TestMetadata covers the API with no broker behind it: the official gRPC
// client creates and updates a cluster and manages its topics.
func TestMetadata(t *testing.T) {
	s, c := start(t, noEngine)
	svc := s.Hub.Service("managedkafka").(*managedkafka.Service)

	op := must(c.CreateCluster(ctx, newCluster("orders")))
	cl := must(op.Wait(ctx))
	name := loc + "/clusters/orders"
	if cl.Name != name || cl.State != mkpb.Cluster_ACTIVE || cl.CapacityConfig.VcpuCount != 3 ||
		cl.GetGcpConfig().AccessConfig.NetworkConfigs[0].Subnet == "" || cl.Labels["env"] != "test" || cl.CreateTime == nil {
		t.Fatalf("created cluster: %v", cl)
	}
	if _, ok := cl.Labels[managedkafka.BootstrapLabel]; ok {
		t.Fatalf("no broker, but a bootstrap label: %v", cl.Labels)
	}
	_, err := c.CreateCluster(ctx, newCluster("orders"))
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateCluster(ctx, newCluster("Bad_ID"))
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.GetCluster(ctx, &mkpb.GetClusterRequest{Name: loc + "/clusters/nope"})
	wantCode(t, err, codes.NotFound)
	must(must(c.CreateCluster(ctx, newCluster("billing"))).Wait(ctx))

	it := c.ListClusters(ctx, &mkpb.ListClustersRequest{Parent: loc, PageSize: 1})
	var names []string
	for {
		x, err := it.Next()
		if err == iterator.Done {
			break
		}
		names = append(names, must(x, err).Name)
	}
	if len(names) != 2 || names[0] != loc+"/clusters/billing" {
		t.Fatalf("list: %v", names)
	}

	// update: only what the mask names changes
	up := must(must(c.UpdateCluster(ctx, &mkpb.UpdateClusterRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"capacity_config.vcpu_count", "labels"}},
		Cluster:    &mkpb.Cluster{Name: name, Labels: map[string]string{"env": "prod"}, CapacityConfig: &mkpb.CapacityConfig{VcpuCount: 6}},
	})).Wait(ctx))
	if up.CapacityConfig.VcpuCount != 6 || up.CapacityConfig.MemoryBytes != 3<<30 || up.Labels["env"] != "prod" || up.GetGcpConfig() == nil {
		t.Fatalf("updated cluster: %v", up)
	}
	_, err = c.UpdateCluster(ctx, &mkpb.UpdateClusterRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}}, Cluster: &mkpb.Cluster{Name: name}})
	wantCode(t, err, codes.InvalidArgument)

	// topics
	topic := func(id string, parts int32, cfg map[string]string) *mkpb.CreateTopicRequest {
		return &mkpb.CreateTopicRequest{Parent: name, TopicId: id, Topic: &mkpb.Topic{PartitionCount: parts, ReplicationFactor: 3, Configs: cfg}}
	}
	tp := must(c.CreateTopic(ctx, topic("events", 3, map[string]string{"retention.ms": "60000"})))
	if tp.Name != name+"/topics/events" || tp.PartitionCount != 3 || tp.ReplicationFactor != 3 || tp.Configs["retention.ms"] != "60000" {
		t.Fatalf("topic: %v", tp)
	}
	_, err = c.CreateTopic(ctx, topic("events", 3, nil))
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateTopic(ctx, topic("zero", 0, nil))
	wantCode(t, err, codes.InvalidArgument)
	tp = must(c.UpdateTopic(ctx, &mkpb.UpdateTopicRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"partition_count"}},
		Topic: &mkpb.Topic{Name: tp.Name, PartitionCount: 6}}))
	if tp.PartitionCount != 6 || tp.Configs["retention.ms"] != "60000" {
		t.Fatalf("updated topic: %v", tp)
	}
	_, err = c.UpdateTopic(ctx, &mkpb.UpdateTopicRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"partition_count"}},
		Topic: &mkpb.Topic{Name: tp.Name, PartitionCount: 2}})
	wantCode(t, err, codes.InvalidArgument)
	must(c.CreateTopic(ctx, topic("audit", 1, nil)))
	var topics []string
	for ti := c.ListTopics(ctx, &mkpb.ListTopicsRequest{Parent: name}); ; {
		x, err := ti.Next()
		if err == iterator.Done {
			break
		}
		topics = append(topics, must(x, err).Name)
	}
	if len(topics) != 2 || topics[0] != name+"/topics/audit" {
		t.Fatalf("topics: %v", topics)
	}
	if _, err := c.ListConsumerGroups(ctx, &mkpb.ListConsumerGroupsRequest{Parent: name}).Next(); err != iterator.Done {
		t.Fatalf("groups without a broker: %v", err)
	}
	_, err = c.GetConsumerGroup(ctx, &mkpb.GetConsumerGroupRequest{Name: name + "/consumerGroups/g"})
	wantCode(t, err, codes.NotFound)

	// console and persistence
	if rows := svc.Resources(); len(rows[0].Rows) != 2 || len(rows[1].Rows) != 2 {
		t.Fatalf("resources: %v", rows)
	}
	if in, _ := json.Marshal(svc.Inspect("Clusters", name)); !strings.Contains(string(in), "bootstrap.orders.us-central1.managedkafka.p.cloud.goog:9092") {
		t.Fatalf("inspect: %s", in)
	}
	saved := must(svc.Save())
	svc.Reset()
	if len(svc.Resources()[0].Rows) != 0 {
		t.Fatal("reset left clusters")
	}
	if err := svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	tp = must(c.GetTopic(ctx, &mkpb.GetTopicRequest{Name: tp.Name}))
	if tp.PartitionCount != 6 || must(c.GetCluster(ctx, &mkpb.GetClusterRequest{Name: name})).State != mkpb.Cluster_ACTIVE {
		t.Fatalf("after load: %v", tp)
	}

	if err := c.DeleteTopic(ctx, &mkpb.DeleteTopicRequest{Name: tp.Name}); err != nil {
		t.Fatal(err)
	}
	wantCode(t, c.DeleteTopic(ctx, &mkpb.DeleteTopicRequest{Name: tp.Name}), codes.NotFound)
	if err := must(c.DeleteCluster(ctx, &mkpb.DeleteClusterRequest{Name: name})).Wait(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetCluster(ctx, &mkpb.GetClusterRequest{Name: name})
	wantCode(t, err, codes.NotFound)
}

// TestREST drives the API with plain net/http: create, poll the operation, get.
func TestREST(t *testing.T) {
	s := pogi.Start(t, pogi.Config{Services: []string{"managedkafka", "operations"}, DockerHost: "/nonexistent/docker.sock"})
	call := func(method, path, body string, want int) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(method, s.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		var m map[string]any
		json.Unmarshal(b, &m)
		if resp.StatusCode != want {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, b)
		}
		return m
	}
	op := call("POST", "/v1/"+loc+"/clusters?clusterId=rest", `{"capacityConfig":{"vcpuCount":"3","memoryBytes":"3221225472"},"labels":{"a":"b"}}`, 200)
	name := op["name"].(string)
	if !strings.HasPrefix(name, loc+"/operations/") || op["metadata"].(map[string]any)["verb"] != "create" {
		t.Fatalf("operation: %v", op)
	}
	for i := 0; op["done"] != true; i++ {
		if i > 50 {
			t.Fatalf("operation never finished: %v", op)
		}
		time.Sleep(20 * time.Millisecond)
		op = call("GET", "/v1/"+name, "", 200)
	}
	resp := op["response"].(map[string]any)
	if resp["name"] != loc+"/clusters/rest" || resp["state"] != "ACTIVE" || resp["capacityConfig"].(map[string]any)["vcpuCount"] != "3" {
		t.Fatalf("response: %v", resp)
	}
	if got := call("GET", "/v1/"+loc+"/clusters/rest", "", 200); got["state"] != "ACTIVE" || got["labels"].(map[string]any)["a"] != "b" {
		t.Fatalf("get: %v", got)
	}
	if l := call("GET", "/v1/"+loc+"/clusters", "", 200); len(l["clusters"].([]any)) != 1 {
		t.Fatalf("list: %v", l)
	}
	call("POST", "/v1/"+loc+"/clusters/rest/topics?topicId=t1", `{"partitionCount":2,"replicationFactor":3,"configs":{"cleanup.policy":"compact"}}`, 200)
	if got := call("GET", "/v1/"+loc+"/clusters/rest/topics/t1", "", 200); got["partitionCount"] != float64(2) {
		t.Fatalf("topic: %v", got)
	}
	call("PATCH", "/v1/"+loc+"/clusters/rest/topics/t1?updateMask=partitionCount", `{"partitionCount":4}`, 200)
	if l := call("GET", "/v1/"+loc+"/clusters/rest/topics", "", 200); l["topics"].([]any)[0].(map[string]any)["partitionCount"] != float64(4) {
		t.Fatalf("topics: %v", l)
	}
	if e := call("GET", "/v1/"+loc+"/clusters/nope", "", 404); e["error"].(map[string]any)["status"] != "NOT_FOUND" {
		t.Fatalf("error: %v", e)
	}
	call("DELETE", "/v1/"+loc+"/clusters/rest/topics/t1", "", 200)
	call("DELETE", "/v1/"+loc+"/clusters/rest", "", 200)
}

// TestBrokerInDocker runs a cluster on Redpanda when a container engine
// answers: the official client creates it and a topic, and franz-go produces
// to and consumes from that topic at the address the cluster reports.
func TestBrokerInDocker(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	_, c := start(t, pogi.Config{})
	id := "e2e-" + core.RandHex(3)
	name := loc + "/clusters/" + id
	cl := must(must(c.CreateCluster(ctx, newCluster(id))).Wait(ctx)) // the first run pulls the image
	if cl.State != mkpb.Cluster_ACTIVE {
		t.Fatalf("cluster: %v", cl)
	}
	t.Cleanup(func() { c.DeleteCluster(ctx, &mkpb.DeleteClusterRequest{Name: name}) })
	addr := cl.Labels[managedkafka.BootstrapLabel]
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("labels: %v", cl.Labels)
	}
	tp := must(c.CreateTopic(ctx, &mkpb.CreateTopicRequest{Parent: name, TopicId: "orders",
		Topic: &mkpb.Topic{PartitionCount: 2, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "3600000"}}}))
	if got := must(c.GetTopic(ctx, &mkpb.GetTopicRequest{Name: tp.Name})); got.PartitionCount != 2 || got.ReplicationFactor != 3 || got.Configs["retention.ms"] != "3600000" {
		t.Fatalf("topic on the broker: %v", got)
	}
	_, err := c.CreateTopic(ctx, &mkpb.CreateTopicRequest{Parent: name, TopicId: "orders", Topic: &mkpb.Topic{PartitionCount: 1, ReplicationFactor: 3}})
	wantCode(t, err, codes.AlreadyExists)

	tctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	kc, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DefaultProduceTopic("orders"), kgo.ConsumeTopics("orders"),
		kgo.ConsumerGroup("workers"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	var records []*kgo.Record
	for i := range 10 {
		records = append(records, &kgo.Record{Value: fmt.Appendf(nil, "order-%d", i)})
	}
	if err := kc.ProduceSync(tctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	got := 0
	for got < 10 {
		fs := kc.PollFetches(tctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume: %v", errs)
		}
		got += fs.NumRecords()
	}
	if err := kc.CommitUncommittedOffsets(tctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// topic update on the broker, consumer groups read from it
	up := must(c.UpdateTopic(ctx, &mkpb.UpdateTopicRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"partition_count", "configs"}},
		Topic: &mkpb.Topic{Name: tp.Name, PartitionCount: 3, Configs: map[string]string{"retention.ms": "7200000"}}}))
	if up.PartitionCount != 3 || up.Configs["retention.ms"] != "7200000" {
		t.Fatalf("updated topic: %v", up)
	}
	g := must(c.GetConsumerGroup(ctx, &mkpb.GetConsumerGroupRequest{Name: name + "/consumerGroups/workers"}))
	var total int64
	for _, p := range g.Topics["orders"].GetPartitions() {
		total += p.Offset
	}
	if total != 10 {
		t.Fatalf("group offsets: %v", g)
	}
	if x, err := c.ListConsumerGroups(ctx, &mkpb.ListConsumerGroupsRequest{Parent: name}).Next(); err != nil || x.Name != g.Name {
		t.Fatalf("group list: %v %v", x, err)
	}
	wantCode(t, c.DeleteConsumerGroup(ctx, &mkpb.DeleteConsumerGroupRequest{Name: g.Name}), codes.FailedPrecondition) // still has a member
	kc.Close()
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		err := c.DeleteConsumerGroup(ctx, &mkpb.DeleteConsumerGroupRequest{Name: g.Name})
		if err == nil {
			break
		}
		if status.Code(err) != codes.FailedPrecondition || time.Now().After(end) {
			t.Fatalf("delete group: %v", err)
		}
	}
	_, err = c.GetConsumerGroup(ctx, &mkpb.GetConsumerGroupRequest{Name: g.Name})
	wantCode(t, err, codes.NotFound)

	if err := c.DeleteTopic(ctx, &mkpb.DeleteTopicRequest{Name: tp.Name}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetTopic(ctx, &mkpb.GetTopicRequest{Name: tp.Name})
	wantCode(t, err, codes.NotFound)
	if err := must(c.DeleteCluster(ctx, &mkpb.DeleteClusterRequest{Name: name})).Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
