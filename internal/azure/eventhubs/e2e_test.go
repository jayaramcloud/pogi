package eventhubs_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/eventhubs"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/twmb/franz-go/pkg/kgo"
)

const blobKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func ptr[T any](v T) *T { return &v }

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	srv *pogi.Running
	eh  *eventhubs.Service
	cs  string
}

// startHub runs pogi with a hub of the given partitions in the localhost namespace.
func startHub(t *testing.T, hub string, partitions int) *env {
	t.Helper()
	srv := pogi.Start(t)
	eh := srv.Hub.Service("eventhubs").(*eventhubs.Service)
	if err := eh.CreateHub("localhost", hub, partitions, 0); err != nil {
		t.Fatal(err)
	}
	amqpURL := srv.Ports["amqp"]
	if amqpURL == "" {
		t.Fatal("pogi.Start opened no AMQP port")
	}
	cs := "Endpoint=sb://" + strings.TrimPrefix(amqpURL, "amqp://") + ";SharedAccessKeyName=k;SharedAccessKey=v;EntityPath=" + hub + ";UseDevelopmentEmulator=true"
	return &env{srv, eh, cs}
}

func (e *env) producer(t *testing.T) *azeventhubs.ProducerClient {
	p := must(azeventhubs.NewProducerClientFromConnectionString(e.cs, "", nil))
	t.Cleanup(func() { p.Close(context.Background()) })
	return p
}

func (e *env) consumer(t *testing.T) *azeventhubs.ConsumerClient {
	c := must(azeventhubs.NewConsumerClientFromConnectionString(e.cs, "", azeventhubs.DefaultConsumerGroup, nil))
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func timeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// sendBatch sends bodies in one batch with the given options.
func sendBatch(t *testing.T, p *azeventhubs.ProducerClient, o *azeventhubs.EventDataBatchOptions, bodies ...string) {
	t.Helper()
	ctx := timeout(t)
	b := must(p.NewEventDataBatch(ctx, o))
	for _, body := range bodies {
		if err := b.AddEventData(&azeventhubs.EventData{Body: []byte(body), Properties: map[string]any{"n": int64(len(body))}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.SendEventDataBatch(ctx, b, nil); err != nil {
		t.Fatal(err)
	}
}

func receive(t *testing.T, c *azeventhubs.ConsumerClient, partition string, pos azeventhubs.StartPosition, count int) []*azeventhubs.ReceivedEventData {
	t.Helper()
	pc := must(c.NewPartitionClient(partition, &azeventhubs.PartitionClientOptions{StartPosition: pos}))
	defer pc.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return must(pc.ReceiveEvents(ctx, count, nil))
}

func bodies(evs []*azeventhubs.ReceivedEventData) string {
	var out []string
	for _, e := range evs {
		out = append(out, string(e.Body))
	}
	return strings.Join(out, ",")
}

func TestProducerAndProperties(t *testing.T) {
	e := startHub(t, "orders", 4)
	p := e.producer(t)

	hub := must(p.GetEventHubProperties(timeout(t), nil))
	if hub.Name != "orders" || !slices.Equal(hub.PartitionIDs, []string{"0", "1", "2", "3"}) || time.Since(hub.CreatedOn) > time.Minute || hub.GeoReplicationEnabled {
		t.Fatalf("%+v", hub)
	}
	empty := must(p.GetPartitionProperties(timeout(t), "2", nil))
	if !empty.IsEmpty || empty.EventHubName != "orders" || empty.PartitionID != "2" || empty.LastEnqueuedSequenceNumber != -1 || empty.LastEnqueuedOffset != "-1" {
		t.Fatalf("%+v", empty)
	}
	if _, err := p.GetPartitionProperties(timeout(t), "9", nil); err == nil {
		t.Fatal("partition 9 does not exist")
	}

	// one key lands on one partition, in order
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionKey: ptr("customer-7")}, "a", "b", "c")
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionKey: ptr("customer-7")}, "d")
	// an explicit partition
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("3")}, "x", "y")
	var withKey string
	counts := map[string]int{}
	for _, id := range hub.PartitionIDs {
		pp := must(p.GetPartitionProperties(timeout(t), id, nil))
		counts[id] = int(pp.LastEnqueuedSequenceNumber + 1)
		if pp.LastEnqueuedSequenceNumber == 3 && id != "3" {
			withKey = id
		}
		if pp.IsEmpty != (counts[id] == 0) || (counts[id] > 0 && (pp.BeginningSequenceNumber != 0 || pp.LastEnqueuedOffset == "-1" || time.Since(pp.LastEnqueuedOn) > time.Minute)) {
			t.Fatalf("partition %s: %+v", id, pp)
		}
	}
	if withKey == "" || counts["3"] != 2 || counts[withKey]+counts["3"] != 6 {
		t.Fatalf("counts %v", counts)
	}

	c := e.consumer(t)
	evs := receive(t, c, withKey, azeventhubs.StartPosition{Earliest: ptr(true)}, 4)
	if bodies(evs) != "a,b,c,d" {
		t.Fatalf("keyed events: %s", bodies(evs))
	}
	for i, ev := range evs {
		if ev.SequenceNumber != int64(i) || ev.PartitionKey == nil || *ev.PartitionKey != "customer-7" || ev.EnqueuedTime == nil || ev.Offset == "" {
			t.Fatalf("event %d: %+v", i, ev)
		}
		if ev.Properties["n"] != int64(1) {
			t.Fatalf("application properties: %v", ev.Properties)
		}
	}
	if got := receive(t, c, "3", azeventhubs.StartPosition{Earliest: ptr(true)}, 2); bodies(got) != "x,y" || got[0].PartitionKey != nil {
		t.Fatalf("partition 3: %s", bodies(got))
	}

	// events without a key spread over the partitions
	for range 8 {
		sendBatch(t, p, nil, "rr")
	}
	spread := 0
	for _, id := range hub.PartitionIDs {
		if pp := must(p.GetPartitionProperties(timeout(t), id, nil)); pp.LastEnqueuedSequenceNumber+1 > int64(counts[id]) {
			spread++
		}
	}
	if spread != 4 {
		t.Fatalf("round-robin touched %d partitions", spread)
	}
}

func TestStartPositions(t *testing.T) {
	e := startHub(t, "pos", 1)
	p, c := e.producer(t), e.consumer(t)
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("0")}, "e0", "e1", "e2")
	time.Sleep(20 * time.Millisecond)
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("0")}, "e3", "e4")

	all := receive(t, c, "0", azeventhubs.StartPosition{Earliest: ptr(true)}, 5)
	if bodies(all) != "e0,e1,e2,e3,e4" {
		t.Fatalf("earliest: %s", bodies(all))
	}
	for name, tc := range map[string]struct {
		pos  azeventhubs.StartPosition
		want string
	}{
		"offset, exclusive":   {azeventhubs.StartPosition{Offset: ptr(all[1].Offset)}, "e2,e3,e4"},
		"offset, inclusive":   {azeventhubs.StartPosition{Offset: ptr(all[1].Offset), Inclusive: true}, "e1,e2,e3,e4"},
		"sequence, exclusive": {azeventhubs.StartPosition{SequenceNumber: ptr(int64(2))}, "e3,e4"},
		"sequence, inclusive": {azeventhubs.StartPosition{SequenceNumber: ptr(int64(2)), Inclusive: true}, "e2,e3,e4"},
		"enqueued time":       {azeventhubs.StartPosition{EnqueuedTime: all[3].EnqueuedTime, Inclusive: true}, "e3,e4"},
	} {
		n := len(strings.Split(tc.want, ","))
		if got := bodies(receive(t, c, "0", tc.pos, n)); got != tc.want {
			t.Errorf("%s: got %s, want %s", name, got, tc.want)
		}
	}

	// latest sees only what comes after the link attached
	pc := must(c.NewPartitionClient("0", &azeventhubs.PartitionClientOptions{StartPosition: azeventhubs.StartPosition{Latest: ptr(true)}}))
	defer pc.Close(context.Background())
	short, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if evs, err := pc.ReceiveEvents(short, 1, nil); len(evs) != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("latest saw old events: %v %v", bodies(evs), err)
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("0")}, "late")
	}()
	if got := bodies(must(pc.ReceiveEvents(timeout(t), 1, nil))); got != "late" {
		t.Fatalf("latest: %s", got)
	}
	// the client resumes after the last event it returned
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("0")}, "later")
	if got := bodies(must(pc.ReceiveEvents(timeout(t), 1, nil))); got != "later" {
		t.Fatalf("resume: %s", got)
	}
}

func TestEpochStealing(t *testing.T) {
	e := startHub(t, "epochs", 1)
	p := e.producer(t)
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("0")}, "one", "two", "three")

	owner := e.consumer(t)
	first := must(owner.NewPartitionClient("0", &azeventhubs.PartitionClientOptions{StartPosition: azeventhubs.StartPosition{Earliest: ptr(true)}, OwnerLevel: ptr(int64(1))}))
	defer first.Close(context.Background())
	if got := bodies(must(first.ReceiveEvents(timeout(t), 3, nil))); got != "one,two,three" {
		t.Fatal(got)
	}

	// a lower epoch cannot attach
	lower := e.consumer(t)
	low := must(lower.NewPartitionClient("0", &azeventhubs.PartitionClientOptions{StartPosition: azeventhubs.StartPosition{Earliest: ptr(true)}, OwnerLevel: ptr(int64(0))}))
	defer low.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := low.ReceiveEvents(ctx, 1, nil)
	var ehErr *azeventhubs.Error
	if !errors.As(err, &ehErr) || ehErr.Code != azeventhubs.ErrorCodeOwnershipLost {
		t.Fatalf("lower epoch: %v", err)
	}
	// so can a receiver without epoch
	plain := must(e.consumer(t).NewPartitionClient("0", &azeventhubs.PartitionClientOptions{StartPosition: azeventhubs.StartPosition{Earliest: ptr(true)}}))
	defer plain.Close(context.Background())
	if _, err := plain.ReceiveEvents(ctx, 1, nil); !errors.As(err, &ehErr) || ehErr.Code != azeventhubs.ErrorCodeOwnershipLost {
		t.Fatalf("receiver without epoch: %v", err)
	}

	// a higher epoch takes the partition; the old owner is told it lost it
	thief := must(e.consumer(t).NewPartitionClient("0", &azeventhubs.PartitionClientOptions{StartPosition: azeventhubs.StartPosition{Earliest: ptr(true)}, OwnerLevel: ptr(int64(2))}))
	defer thief.Close(context.Background())
	if got := bodies(must(thief.ReceiveEvents(timeout(t), 3, nil))); got != "one,two,three" {
		t.Fatal(got)
	}
	sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr("0")}, "four")
	_, err = first.ReceiveEvents(ctx, 1, nil)
	if !errors.As(err, &ehErr) || ehErr.Code != azeventhubs.ErrorCodeOwnershipLost || !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("the old owner: %v", err)
	}
	if got := bodies(must(thief.ReceiveEvents(timeout(t), 1, nil))); got != "four" {
		t.Fatal(got)
	}
}

// TestProcessorCheckpoints runs the Processor on pogi's own Blob emulator as
// the checkpoint store: it checkpoints, restarts and resumes after the checkpoint.
func TestProcessorCheckpoints(t *testing.T) { processorCheckpoints(t, false) }

// TestProcessorResumesAfterAClaimLostAtStop stops the first processor while one of its ownership claims is in
// flight, which made TestProcessorCheckpoints fail once under the full parallel suite (2026-10-21): the second
// processor must still get every event.
func TestProcessorResumesAfterAClaimLostAtStop(t *testing.T) { processorCheckpoints(t, true) }

// lostClaimStore is a checkpoint store whose next ownership claim, once armed, reaches Blob storage but whose answer
// is held until the processor stops: as when a processor is stopped while a claim is in flight. The ownership blobs
// get new ETags the processor never learns.
type lostClaimStore struct {
	azeventhubs.CheckpointStore
	armed atomic.Bool
	fired chan struct{}
}

func (s *lostClaimStore) ClaimOwnership(ctx context.Context, own []azeventhubs.Ownership, o *azeventhubs.ClaimOwnershipOptions) ([]azeventhubs.Ownership, error) {
	if len(own) == 0 || own[0].OwnerID == "" || !s.armed.CompareAndSwap(true, false) { // OwnerID "": the relinquish at close
		return s.CheckpointStore.ClaimOwnership(ctx, own, o)
	}
	_, err := s.CheckpointStore.ClaimOwnership(context.WithoutCancel(ctx), own, o)
	close(s.fired)
	if err != nil {
		return nil, err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func processorCheckpoints(t *testing.T, loseClaim bool) {
	e := startHub(t, "proc", 2)
	if e.srv.Ports["azblob"] == "" {
		t.Fatal("no Blob port")
	}
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + blobKey + ";BlobEndpoint=" + e.srv.Ports["azblob"] + "/devstoreaccount1;"
	blobs := must(azblob.NewClientFromConnectionString(conn, nil))
	must(blobs.CreateContainer(timeout(t), "checkpoints", nil))
	store := &lostClaimStore{CheckpointStore: must(checkpoints.NewBlobStore(blobs.ServiceClient().NewContainerClient("checkpoints"), nil)), fired: make(chan struct{})}
	p := e.producer(t)

	var mu sync.Mutex
	seen := map[string][]string{} // processor generation → bodies
	saved := map[string]int{}     // processor generation → events a stored checkpoint covers
	run := func(gen string) (stop func()) {
		consumer := must(azeventhubs.NewConsumerClientFromConnectionString(e.cs, "", azeventhubs.DefaultConsumerGroup, nil))
		proc := must(azeventhubs.NewProcessor(consumer, store, &azeventhubs.ProcessorOptions{UpdateInterval: 500 * time.Millisecond,
			// A processor stopped while an ownership claim is in flight keeps its partitions, as on Azure: Blob storage
			// applied the claim, so the relinquish at close carries the old ETag and is refused (412), and the next
			// processor gets them only when they expire. The SDK's default expiry (60 s) took all of wait's minute;
			// 10 s leaves the rest (a running processor renews its claims every UpdateInterval).
			PartitionExpirationDuration: 10 * time.Second,
			StartPositions:              azeventhubs.StartPositions{Default: azeventhubs.StartPosition{Earliest: ptr(true)}}}))
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); proc.Run(ctx) }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				pc := proc.NextPartitionClient(ctx)
				if pc == nil {
					return
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer pc.Close(context.Background())
					for ctx.Err() == nil {
						rctx, rcancel := context.WithTimeout(ctx, time.Second)
						evs, err := pc.ReceiveEvents(rctx, 10, nil)
						rcancel()
						if len(evs) > 0 {
							mu.Lock()
							for _, ev := range evs {
								seen[gen] = append(seen[gen], string(ev.Body))
							}
							mu.Unlock()
							if err := pc.UpdateCheckpoint(ctx, evs[len(evs)-1], nil); err != nil {
								if ctx.Err() == nil {
									t.Errorf("checkpoint: %v", err)
								}
							} else {
								mu.Lock()
								saved[gen] += len(evs)
								mu.Unlock()
							}
						}
						if err != nil && !errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
							t.Errorf("receive: %v", err)
							return
						}
					}
				}()
			}
		}()
		return func() { cancel(); wg.Wait(); consumer.Close(context.Background()) }
	}
	count := func(gen string) int { mu.Lock(); defer mu.Unlock(); return saved[gen] }
	// wait until n events are seen and checkpointed: stopping a processor between the two would lose the checkpoint
	wait := func(gen string, n int) {
		t.Helper()
		for end := time.Now().Add(time.Minute); count(gen) < n; time.Sleep(50 * time.Millisecond) {
			if time.Now().After(end) {
				mu.Lock()
				defer mu.Unlock()
				t.Fatalf("generation %s checkpointed %d of %d events: %v", gen, saved[gen], n, seen[gen])
			}
		}
	}

	for i := range 6 {
		sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr(fmt.Sprint(i % 2))}, fmt.Sprintf("first-%d", i))
	}
	stop := run("one")
	wait("one", 6)
	if loseClaim {
		store.armed.Store(true)
		select {
		case <-store.fired:
		case <-time.After(30 * time.Second):
			t.Fatal("the first processor made no claim to lose")
		}
	}
	stop()

	// checkpoints are in the container, one per partition
	var names []string
	owned := 0 // ownership blobs the stopped processor still holds
	pager := blobs.NewListBlobsFlatPager("checkpoints", &azblob.ListBlobsFlatOptions{Include: azblob.ListBlobsInclude{Metadata: true}})
	for pager.More() {
		page := must(pager.NextPage(timeout(t)))
		for _, b := range page.Segment.BlobItems {
			names = append(names, *b.Name)
			if o := b.Metadata["ownerid"]; strings.Contains(*b.Name, "/ownership/") && o != nil && *o != "" {
				owned++
			}
		}
	}
	if loseClaim && owned != 2 {
		t.Fatalf("the trigger did not happen: the stopped processor holds %d of 2 partitions", owned)
	}
	sort.Strings(names)
	var checkpointBlobs int
	for _, n := range names {
		if strings.Contains(n, "/checkpoint/") {
			checkpointBlobs++
		}
	}
	if checkpointBlobs != 2 {
		t.Fatalf("blobs %v", names)
	}

	// a new processor resumes after the checkpoint: it sees only what came since
	for i := range 4 {
		sendBatch(t, p, &azeventhubs.EventDataBatchOptions{PartitionID: ptr(fmt.Sprint(i % 2))}, fmt.Sprintf("second-%d", i))
	}
	stop = run("two")
	defer stop()
	wait("two", 4)
	time.Sleep(time.Second) // nothing from before the checkpoint may follow
	mu.Lock()
	defer mu.Unlock()
	got := slices.Clone(seen["two"])
	sort.Strings(got)
	if strings.Join(got, ",") != "second-0,second-1,second-2,second-3" {
		t.Fatalf("the restarted processor saw %v", got)
	}
}

// TestKafkaEndpoint produces to and consumes from the namespace's Kafka endpoint with franz-go.
func TestKafkaEndpoint(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	e := startHub(t, "kafkahub", 3)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute) // the first run pulls the image
	defer cancel()
	addr := must(e.eh.KafkaBootstrap(ctx, "localhost"))
	t.Cleanup(func() { e.eh.DeleteNamespace("localhost") })

	kc := must(kgo.NewClient(kgo.SeedBrokers(addr), kgo.DefaultProduceTopic("kafkahub"),
		kgo.ConsumeTopics("kafkahub"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())))
	defer kc.Close()
	var records []*kgo.Record
	for i := range 12 {
		records = append(records, &kgo.Record{Key: fmt.Appendf(nil, "k%d", i), Value: fmt.Appendf(nil, "event-%d", i)})
	}
	if err := kc.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	parts := map[int32]bool{}
	for n := 0; n < 12; {
		fs := kc.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatal(errs)
		}
		fs.EachRecord(func(r *kgo.Record) { parts[r.Partition] = true; n++ })
	}
	if len(parts) < 2 {
		t.Fatalf("a 3-partition topic got records on %v", parts)
	}
}
