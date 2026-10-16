package eventhubs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/docker"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestKafkaEndpoint starts a namespace's Redpanda lazily and checks that its
// topics follow the event hubs, then produces and consumes with franz-go.
func TestKafkaEndpoint(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	h, s := start(t)
	if err := s.CreateNamespace("kafkans"); err != nil {
		t.Fatal(err)
	}
	s.CreateHub("kafkans", "before", 3, 2*time.Hour)
	s.mu.Lock()
	started := s.kafka["kafkans"]
	s.mu.Unlock()
	if started != "" {
		t.Fatal("the broker must start only when asked for")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute) // the first run pulls the image
	defer cancel()
	addr, err := s.KafkaBootstrap(ctx, "kafkans")
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	container := s.st.Namespaces["kafkans"].Container
	s.mu.Unlock()
	t.Cleanup(func() { h.Docker().Remove(context.Background(), container) })
	if again, _ := s.KafkaBootstrap(ctx, "kafkans"); again != addr {
		t.Fatalf("second call %q, first %q", again, addr)
	}
	s.CreateHub("kafkans", "after", 2, 0) // made while the broker runs

	kc, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ConsumeTopics("before"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	meta := kmsg.NewPtrMetadataRequest()
	for _, name := range []string{"before", "after"} {
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr(name)
		meta.Topics = append(meta.Topics, mt)
	}
	resp, err := meta.RequestWith(ctx, kc)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range resp.Topics {
		want := map[string]int{"before": 3, "after": 2}[*topic.Topic]
		if topic.ErrorCode != 0 || len(topic.Partitions) != want {
			t.Fatalf("topic %s: error %d, %d partitions, want %d", *topic.Topic, topic.ErrorCode, len(topic.Partitions), want)
		}
	}
	var records []*kgo.Record
	for i := range 9 {
		records = append(records, &kgo.Record{Topic: "before", Partition: int32(i % 3), Value: fmt.Appendf(nil, "event-%d", i)})
	}
	if err := kc.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	seen := map[int32]int{}
	for n := 0; n < 9; {
		fs := kc.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) { seen[r.Partition]++; n++ })
	}
	if len(seen) != 3 {
		t.Fatalf("records by partition %v", seen)
	}

	// deleting the hub deletes its topic
	if err := s.DeleteHub("kafkans", "after"); err != nil {
		t.Fatal(err)
	}
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("after")
	meta = kmsg.NewPtrMetadataRequest()
	meta.Topics = append(meta.Topics, mt)
	meta.AllowAutoTopicCreation = false
	if resp, err = meta.RequestWith(ctx, kc); err != nil || resp.Topics[0].ErrorCode == 0 {
		t.Fatalf("topic of a deleted hub: %v %+v", err, resp)
	}
}
