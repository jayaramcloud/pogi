package pubsub_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ps "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	gcs "cloud.google.com/go/storage"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/pubsub"
	gcsvc "github.com/nrndev/pogi/internal/gcp/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ctx = context.Background()

const proj = "projects/proj"

func setup(t *testing.T, extra ...func(*core.Hub) core.Service) (*pubsub.Service, *ps.Client, string) {
	t.Helper()
	var svc *pubsub.Service
	_, url := core.ServeTest(t, append(extra, func(h *core.Hub) core.Service { svc = pubsub.New(h); return svc })...)
	t.Setenv("PUBSUB_EMULATOR_HOST", strings.TrimPrefix(url, "http://"))
	c, err := ps.NewClient(ctx, "proj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return svc, c, url
}

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

func topic(t *testing.T, c *ps.Client, id string) string {
	t.Helper()
	return must(c.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: proj + "/topics/" + id})).Name
}

func subscribe(t *testing.T, c *ps.Client, s *pb.Subscription) string {
	t.Helper()
	return must(c.SubscriptionAdminClient.CreateSubscription(ctx, s)).Name
}

func publish(t *testing.T, c *ps.Client, topic string, msgs ...*pb.PubsubMessage) []string {
	t.Helper()
	return must(c.TopicAdminClient.Publish(ctx, &pb.PublishRequest{Topic: topic, Messages: msgs})).MessageIds
}

func msg(data string, attrs ...string) *pb.PubsubMessage {
	m := &pb.PubsubMessage{Data: []byte(data), Attributes: map[string]string{}}
	for i := 0; i+1 < len(attrs); i += 2 {
		m.Attributes[attrs[i]] = attrs[i+1]
	}
	return m
}

func pull(t *testing.T, c *ps.Client, sub string, wait bool) []*pb.ReceivedMessage {
	t.Helper()
	return must(c.SubscriptionAdminClient.Pull(ctx, &pb.PullRequest{Subscription: sub, MaxMessages: 100, ReturnImmediately: !wait})).ReceivedMessages
}

func data(rms []*pb.ReceivedMessage) []string {
	var out []string
	for _, rm := range rms {
		out = append(out, string(rm.Message.Data))
	}
	return out
}

func ack(t *testing.T, c *ps.Client, sub string, rms ...*pb.ReceivedMessage) {
	t.Helper()
	var ids []string
	for _, rm := range rms {
		ids = append(ids, rm.AckId)
	}
	if err := c.SubscriptionAdminClient.Acknowledge(ctx, &pb.AcknowledgeRequest{Subscription: sub, AckIds: ids}); err != nil {
		t.Fatal(err)
	}
}

func nack(t *testing.T, c *ps.Client, sub string, rms ...*pb.ReceivedMessage) {
	t.Helper()
	for _, rm := range rms {
		if err := c.SubscriptionAdminClient.ModifyAckDeadline(ctx, &pb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: []string{rm.AckId}}); err != nil {
			t.Fatal(err)
		}
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func TestAdmin(t *testing.T) {
	_, c, _ := setup(t)
	tc, sc := c.TopicAdminClient, c.SubscriptionAdminClient
	orders := must(tc.CreateTopic(ctx, &pb.Topic{Name: proj + "/topics/orders", Labels: map[string]string{"env": "dev"}}))
	_, err := tc.CreateTopic(ctx, &pb.Topic{Name: orders.Name})
	wantCode(t, err, codes.AlreadyExists)
	_, err = tc.CreateTopic(ctx, &pb.Topic{Name: proj + "/topics/x"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = tc.GetTopic(ctx, &pb.GetTopicRequest{Topic: proj + "/topics/missing"})
	wantCode(t, err, codes.NotFound)

	up := must(tc.UpdateTopic(ctx, &pb.UpdateTopicRequest{Topic: &pb.Topic{Name: orders.Name, Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}}}))
	if up.Labels["env"] != "prod" || must(tc.GetTopic(ctx, &pb.GetTopicRequest{Topic: orders.Name})).Labels["env"] != "prod" {
		t.Fatalf("labels not updated: %v", up.Labels)
	}
	_, err = tc.UpdateTopic(ctx, &pb.UpdateTopicRequest{Topic: &pb.Topic{Name: orders.Name}})
	wantCode(t, err, codes.InvalidArgument)

	topic(t, c, "payments")
	topic(t, c, "refunds")
	var page []*pb.Topic
	token := must(iterator.NewPager(tc.ListTopics(ctx, &pb.ListTopicsRequest{Project: proj}), 2, "").NextPage(&page))
	if len(page) != 2 || token == "" {
		t.Fatalf("first page = %d topics, token %q", len(page), token)
	}
	var all []string
	for it := tc.ListTopics(ctx, &pb.ListTopicsRequest{Project: proj, PageSize: 2}); ; {
		tp, err := it.Next()
		if err == iterator.Done {
			break
		}
		all = append(all, must(tp, err).Name)
	}
	if strings.Join(all, ",") != proj+"/topics/orders,"+proj+"/topics/payments,"+proj+"/topics/refunds" {
		t.Fatalf("topics = %v", all)
	}

	sub := must(sc.CreateSubscription(ctx, &pb.Subscription{Name: proj + "/subscriptions/orders-sub", Topic: orders.Name}))
	if sub.AckDeadlineSeconds != 10 || sub.MessageRetentionDuration.AsDuration() != 7*24*time.Hour || sub.ExpirationPolicy.GetTtl() == nil {
		t.Fatalf("defaults = %v", sub)
	}
	_, err = sc.CreateSubscription(ctx, &pb.Subscription{Name: sub.Name, Topic: orders.Name})
	wantCode(t, err, codes.AlreadyExists)
	_, err = sc.CreateSubscription(ctx, &pb.Subscription{Name: proj + "/subscriptions/orphan", Topic: proj + "/topics/missing"})
	wantCode(t, err, codes.NotFound)
	sub = must(sc.UpdateSubscription(ctx, &pb.UpdateSubscriptionRequest{
		Subscription: &pb.Subscription{Name: sub.Name, AckDeadlineSeconds: 30, Labels: map[string]string{"team": "a"}},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"ack_deadline_seconds", "labels"}}}))
	if sub.AckDeadlineSeconds != 30 || sub.Labels["team"] != "a" {
		t.Fatalf("updated = %v", sub)
	}
	_, err = sc.UpdateSubscription(ctx, &pb.UpdateSubscriptionRequest{Subscription: &pb.Subscription{Name: sub.Name, Filter: `attributes:x`},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"filter"}}})
	wantCode(t, err, codes.InvalidArgument)
	detached := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/detached", Topic: orders.Name})
	topicSubs := func() string {
		var out []string
		for it := tc.ListTopicSubscriptions(ctx, &pb.ListTopicSubscriptionsRequest{Topic: orders.Name}); ; {
			n, err := it.Next()
			if err == iterator.Done {
				return strings.Join(out, ",")
			}
			out = append(out, must(n, err))
		}
	}
	if got := topicSubs(); got != detached+","+sub.Name {
		t.Fatalf("topic subscriptions = %s", got)
	}
	must(tc.DetachSubscription(ctx, &pb.DetachSubscriptionRequest{Subscription: detached}))
	if got := topicSubs(); got != sub.Name {
		t.Fatalf("topic subscriptions after detach = %s", got)
	}
	if !must(sc.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: detached})).Detached {
		t.Fatal("subscription not detached")
	}
	_, err = sc.Pull(ctx, &pb.PullRequest{Subscription: detached, MaxMessages: 1, ReturnImmediately: true})
	wantCode(t, err, codes.FailedPrecondition)

	if err := tc.DeleteTopic(ctx, &pb.DeleteTopicRequest{Topic: orders.Name}); err != nil {
		t.Fatal(err)
	}
	if got := must(sc.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: sub.Name})).Topic; got != "_deleted-topic_" {
		t.Fatalf("topic of orphaned subscription = %q", got)
	}
	if err := sc.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Subscription: sub.Name}); err != nil {
		t.Fatal(err)
	}
	_, err = sc.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: sub.Name})
	wantCode(t, err, codes.NotFound)
}

func TestPublishReceive(t *testing.T) {
	svc, c, _ := setup(t)
	topic(t, c, "events")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/events-sub", Topic: proj + "/topics/events"})

	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	got := map[string]string{}
	done := make(chan error)
	go func() {
		done <- c.Subscriber("events-sub").Receive(rctx, func(_ context.Context, m *ps.Message) {
			mu.Lock()
			got[string(m.Data)] = m.Attributes["n"]
			if len(got) == 5 {
				cancel()
			}
			mu.Unlock()
			m.Ack()
		})
	}()
	p := c.Publisher("events")
	defer p.Stop()
	for i := range 5 {
		must(p.Publish(ctx, &ps.Message{Data: fmt.Appendf(nil, "m%d", i), Attributes: map[string]string{"n": fmt.Sprint(i)}}).Get(ctx))
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if rctx.Err() != context.Canceled {
		t.Fatal("Receive timed out")
	}
	for i := range 5 {
		if got[fmt.Sprintf("m%d", i)] != fmt.Sprint(i) {
			t.Fatalf("received %v", got)
		}
	}
	eventually(t, "acks", func() bool { return len(svc.Peek(sub, 10)) == 0 })

	// The console helpers see what the clients see.
	id := must(svc.Publish(proj+"/topics/events", []byte("from console"), map[string]string{"k": "v"}))
	if pm := svc.Peek(sub, 10); len(pm) != 1 || pm[0].ID != id || pm[0].Data != "from console" || pm[0].Attributes["k"] != "v" {
		t.Fatalf("peek = %+v", pm)
	}
}

func TestPullNackExpiry(t *testing.T) {
	_, c, _ := setup(t)
	tp := topic(t, c, "jobs")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/jobs-sub", Topic: tp, AckDeadlineSeconds: 1})
	_, err := c.SubscriptionAdminClient.Pull(ctx, &pb.PullRequest{Subscription: sub})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.TopicAdminClient.Publish(ctx, &pb.PublishRequest{Topic: tp, Messages: []*pb.PubsubMessage{{}}})
	wantCode(t, err, codes.InvalidArgument)
	id := publish(t, c, tp, msg("job"))[0]

	first := pull(t, c, sub, false)
	if len(first) != 1 || first[0].Message.MessageId != id || first[0].Message.PublishTime == nil {
		t.Fatalf("pull = %v", first)
	}
	if len(pull(t, c, sub, false)) != 0 {
		t.Fatal("a leased message was delivered twice")
	}
	nack(t, c, sub, first...)
	second := pull(t, c, sub, false)
	if len(second) != 1 || second[0].Message.MessageId != id || second[0].AckId == first[0].AckId {
		t.Fatalf("after nack = %v", second)
	}

	// Not acked: the 1s lease expires and a waiting Pull gets it again.
	start := time.Now()
	third := pull(t, c, sub, true)
	if len(third) != 1 || third[0].Message.MessageId != id || time.Since(start) < 500*time.Millisecond {
		t.Fatalf("after expiry (%v) = %v", time.Since(start), third)
	}
	ack(t, c, sub, third...)
	if left := pull(t, c, sub, false); len(left) != 0 {
		t.Fatalf("acked message came back: %v", left)
	}
}

func TestFilter(t *testing.T) {
	_, c, _ := setup(t)
	tp := topic(t, c, "shapes")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/shapes-sub", Topic: tp,
		Filter: `(attributes.color = "red" OR hasPrefix(attributes.shape, "tri")) AND NOT attributes:skip AND attributes.size != "xl"`})
	publish(t, c, tp,
		msg("1", "color", "red"),
		msg("2", "color", "blue", "shape", "triangle"),
		msg("3", "color", "red", "skip", "yes"),
		msg("4", "color", "red", "size", "xl"),
		msg("5", "color", "blue"),
		msg("6", "color", "red", "shape", "square", "size", "s"))
	if got := data(pull(t, c, sub, false)); strings.Join(got, ",") != "1,2,6" {
		t.Fatalf("filtered = %v", got)
	}
	for _, bad := range []string{`attributes.color = red`, `attributes.color = "red" AND`, `hasPrefix(color, "x")`, `(attributes:a`} {
		_, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{Name: proj + "/subscriptions/bad", Topic: tp, Filter: bad})
		wantCode(t, err, codes.InvalidArgument)
	}
}

func TestDeadLetter(t *testing.T) {
	_, c, _ := setup(t)
	work, dead := topic(t, c, "work"), topic(t, c, "dead")
	deadSub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/dead-sub", Topic: dead})
	_, err := c.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{Name: proj + "/subscriptions/w", Topic: work,
		DeadLetterPolicy: &pb.DeadLetterPolicy{DeadLetterTopic: dead, MaxDeliveryAttempts: 4}})
	wantCode(t, err, codes.InvalidArgument)
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/work-sub", Topic: work,
		DeadLetterPolicy: &pb.DeadLetterPolicy{DeadLetterTopic: dead, MaxDeliveryAttempts: 5}})
	publish(t, c, work, msg("poison", "k", "v"))
	for attempt := int32(1); attempt <= 5; attempt++ {
		rms := pull(t, c, sub, false)
		if len(rms) != 1 || rms[0].DeliveryAttempt != attempt {
			t.Fatalf("attempt %d: %v", attempt, rms)
		}
		nack(t, c, sub, rms...)
	}
	if left := pull(t, c, sub, false); len(left) != 0 {
		t.Fatalf("message still in the source subscription: %v", left)
	}
	dl := pull(t, c, deadSub, false)
	if len(dl) != 1 || string(dl[0].Message.Data) != "poison" || dl[0].Message.Attributes["k"] != "v" ||
		dl[0].Message.Attributes["CloudPubSubDeadLetterSourceDeliveryCount"] != "5" ||
		dl[0].Message.Attributes["CloudPubSubDeadLetterSourceSubscription"] != "work-sub" {
		t.Fatalf("dead letters = %v", dl)
	}
}

func TestOrdering(t *testing.T) {
	_, c, _ := setup(t)
	tp := topic(t, c, "ordered")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/ordered-sub", Topic: tp, EnableMessageOrdering: true})
	keyed := func(d, key string) *pb.PubsubMessage { return &pb.PubsubMessage{Data: []byte(d), OrderingKey: key} }
	publish(t, c, tp, keyed("a1", "a"), keyed("b1", "b"), keyed("a2", "a"), keyed("b2", "b"), keyed("a3", "a"), msg("free"))

	// A key's messages go out as one batch, in order; the key is then held until the batch is settled.
	first := pull(t, c, sub, false)
	if got := strings.Join(data(first), ","); got != "a1,b1,a2,b2,a3,free" {
		t.Fatalf("first = %s", got)
	}
	publish(t, c, tp, keyed("a4", "a"), msg("free2"))
	if got := strings.Join(data(pull(t, c, sub, false)), ","); got != "free2" {
		t.Fatalf("keys with messages in flight must wait: %s", got)
	}
	nack(t, c, sub, first[2]) // a2
	ack(t, c, sub, first[0], first[1], first[3], first[5])
	if got := data(pull(t, c, sub, false)); len(got) != 0 {
		t.Fatalf("a3 is still in flight, key a must wait: %v", got)
	}
	ack(t, c, sub, first[4]) // a3
	if got := strings.Join(data(pull(t, c, sub, false)), ","); got != "a2,a4" {
		t.Fatalf("the nacked message must come back before the rest of its key: %s", got)
	}

	// The client's Receive delivers each key in publish order.
	topic(t, c, "ordered2")
	subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/ordered2-sub", Topic: proj + "/topics/ordered2", EnableMessageOrdering: true})
	p := c.Publisher("ordered2")
	p.EnableMessageOrdering = true
	defer p.Stop()
	var results []*ps.PublishResult
	for i := range 20 {
		results = append(results, p.Publish(ctx, &ps.Message{Data: fmt.Append(nil, i), OrderingKey: fmt.Sprint("k", i%2)}))
	}
	for _, r := range results {
		must(r.Get(ctx))
	}
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	seen := map[string][]string{}
	err := c.Subscriber("ordered2-sub").Receive(rctx, func(_ context.Context, m *ps.Message) {
		mu.Lock()
		seen[m.OrderingKey] = append(seen[m.OrderingKey], string(m.Data))
		if len(seen["k0"])+len(seen["k1"]) == 20 {
			cancel()
		}
		mu.Unlock()
		m.Ack()
	})
	if err != nil || rctx.Err() != context.Canceled {
		t.Fatalf("receive: %v %v", err, rctx.Err())
	}
	if strings.Join(seen["k0"], ",") != "0,2,4,6,8,10,12,14,16,18" || strings.Join(seen["k1"], ",") != "1,3,5,7,9,11,13,15,17,19" {
		t.Fatalf("order = %v", seen)
	}
}

func TestPush(t *testing.T) {
	svc, c, _ := setup(t)
	var mu sync.Mutex
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		defer mu.Unlock()
		bodies = append(bodies, body)
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusInternalServerError) // first attempt fails, the retry succeeds
		}
	}))
	defer srv.Close()
	tp := topic(t, c, "hooks")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/hooks-sub", Topic: tp, PushConfig: &pb.PushConfig{PushEndpoint: srv.URL + "/push"}})
	id := publish(t, c, tp, msg("hello", "k", "v"))[0]
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(bodies) }
	eventually(t, "push retry", func() bool { return count() >= 2 })
	eventually(t, "push ack", func() bool { return len(svc.Peek(sub, 10)) == 0 })
	time.Sleep(300 * time.Millisecond)
	if count() != 2 {
		t.Fatalf("pushes = %d, want 2", count())
	}
	mu.Lock()
	body := bodies[1]
	mu.Unlock()
	m := body["message"].(map[string]any)
	payload, _ := base64.StdEncoding.DecodeString(m["data"].(string))
	if string(payload) != "hello" || m["messageId"] != id || m["message_id"] != id || m["publishTime"] != m["publish_time"] ||
		m["attributes"].(map[string]any)["k"] != "v" || body["subscription"] != sub {
		t.Fatalf("push body = %v", body)
	}
	tables := svc.Resources()
	if row := tables[1].Rows[0]; row[0] != sub || row[4] != "push" {
		t.Fatalf("subscriptions table = %v", tables[1])
	}
}

func TestSeek(t *testing.T) {
	_, c, _ := setup(t)
	tp := topic(t, c, "replay")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/replay-sub", Topic: tp, RetainAckedMessages: true})
	before := time.Now()
	publish(t, c, tp, msg("m1"), msg("m2"))
	ack(t, c, sub, pull(t, c, sub, false)...)
	if len(pull(t, c, sub, false)) != 0 {
		t.Fatal("acked messages redelivered before seek")
	}
	sc := c.SubscriptionAdminClient
	must(sc.Seek(ctx, &pb.SeekRequest{Subscription: sub, Target: &pb.SeekRequest_Time{Time: timestamppb.New(before)}}))
	replayed := pull(t, c, sub, false)
	if got := strings.Join(data(replayed), ","); got != "m1,m2" {
		t.Fatalf("after seek = %s", got)
	}
	ack(t, c, sub, replayed...)

	publish(t, c, tp, msg("m3"))
	snapName := proj + "/snapshots/before-m3-ack"
	if sn := must(sc.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: snapName, Subscription: sub})); sn.Topic != tp || sn.ExpireTime == nil {
		t.Fatalf("snapshot = %v", sn)
	}
	ack(t, c, sub, pull(t, c, sub, false)...)
	must(sc.Seek(ctx, &pb.SeekRequest{Subscription: sub, Target: &pb.SeekRequest_Snapshot{Snapshot: snapName}}))
	if got := strings.Join(data(pull(t, c, sub, false)), ","); got != "m3" {
		t.Fatalf("after seek to snapshot = %s", got)
	}
	if got := must(c.TopicAdminClient.ListTopicSnapshots(ctx, &pb.ListTopicSnapshotsRequest{Topic: tp}).Next()); got != snapName {
		t.Fatalf("topic snapshots = %q", got)
	}
	if err := sc.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{Snapshot: snapName}); err != nil {
		t.Fatal(err)
	}
	_, err := sc.GetSnapshot(ctx, &pb.GetSnapshotRequest{Snapshot: snapName})
	wantCode(t, err, codes.NotFound)
}

func TestGCSNotification(t *testing.T) {
	_, c, url := setup(t, func(h *core.Hub) core.Service { return gcsvc.New(h) })
	t.Setenv("STORAGE_EMULATOR_HOST", url)
	tp := topic(t, c, "gcs-events")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/gcs-sub", Topic: tp})
	sc := must(gcs.NewClient(ctx))
	defer sc.Close()
	bkt := sc.Bucket("uploads")
	if err := bkt.Create(ctx, "proj", nil); err != nil {
		t.Fatal(err)
	}
	must(bkt.AddNotification(ctx, &gcs.Notification{TopicProjectID: "proj", TopicID: "gcs-events",
		PayloadFormat: gcs.JSONPayload, EventTypes: []string{gcs.ObjectFinalizeEvent}}))
	w := bkt.Object("photos/cat.txt").NewWriter(ctx)
	w.Write([]byte("meow"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	rms := pull(t, c, sub, true)
	if len(rms) != 1 {
		t.Fatalf("notifications = %v", rms)
	}
	m := rms[0].Message
	var obj struct{ Name, Bucket, Size string }
	json.Unmarshal(m.Data, &obj)
	if m.Attributes["eventType"] != "OBJECT_FINALIZE" || m.Attributes["objectId"] != "photos/cat.txt" || m.Attributes["bucketId"] != "uploads" ||
		obj.Name != "photos/cat.txt" || obj.Size != "4" {
		t.Fatalf("notification = %v %s", m.Attributes, m.Data)
	}
}

func TestREST(t *testing.T) {
	_, _, url := setup(t)
	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, url+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp := must(http.DefaultClient.Do(req))
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, raw)
		}
		return resp.StatusCode, out
	}
	if code, out := call("PUT", "/v1/projects/proj/topics/rest-topic", `{"labels":{"a":"b"}}`); code != 200 || out["name"] != proj+"/topics/rest-topic" {
		t.Fatalf("create topic: %d %v", code, out)
	}
	if code, out := call("PUT", "/v1/projects/proj/subscriptions/rest-sub", `{"topic":"projects/proj/topics/rest-topic","ackDeadlineSeconds":20}`); code != 200 || out["ackDeadlineSeconds"] != 20.0 {
		t.Fatalf("create subscription: %d %v", code, out)
	}
	hello := base64.StdEncoding.EncodeToString([]byte("hello"))
	_, out := call("POST", "/v1/projects/proj/topics/rest-topic:publish", `{"messages":[{"data":"`+hello+`","attributes":{"k":"v"}}]}`)
	ids, _ := out["messageIds"].([]any)
	if len(ids) != 1 {
		t.Fatalf("publish: %v", out)
	}
	_, out = call("POST", "/v1/projects/proj/subscriptions/rest-sub:pull", `{"maxMessages":10,"returnImmediately":true}`)
	rms, _ := out["receivedMessages"].([]any)
	if len(rms) != 1 {
		t.Fatalf("pull: %v", out)
	}
	rm := rms[0].(map[string]any)
	m := rm["message"].(map[string]any)
	if m["data"] != hello || m["messageId"] != ids[0] || m["attributes"].(map[string]any)["k"] != "v" {
		t.Fatalf("pulled: %v", rm)
	}
	if code, _ := call("POST", "/v1/projects/proj/subscriptions/rest-sub:acknowledge", `{"ackIds":["`+rm["ackId"].(string)+`"]}`); code != 200 {
		t.Fatalf("ack: %d", code)
	}
	if _, out = call("POST", "/v1/projects/proj/subscriptions/rest-sub:pull", `{"maxMessages":10,"returnImmediately":true}`); len(out) != 0 {
		t.Fatalf("pull after ack: %v", out)
	}
	_, out = call("PATCH", "/v1/projects/proj/subscriptions/rest-sub", `{"subscription":{"labels":{"x":"y"}},"updateMask":"labels"}`)
	if out["labels"].(map[string]any)["x"] != "y" {
		t.Fatalf("patch: %v", out)
	}
	if _, out = call("GET", "/v1/projects/proj/topics/rest-topic/subscriptions", ""); fmt.Sprint(out["subscriptions"]) != "["+proj+"/subscriptions/rest-sub]" {
		t.Fatalf("topic subscriptions: %v", out)
	}
	if _, out = call("GET", "/v1/projects/proj/topics?pageSize=1", ""); len(out["topics"].([]any)) != 1 {
		t.Fatalf("list topics: %v", out)
	}
	if _, out = call("POST", "/v1/projects/proj/schemas?schemaId=order", `{"type":"AVRO","definition":"{\"type\":\"string\"}"}`); out["name"] != proj+"/schemas/order" {
		t.Fatalf("create schema: %v", out)
	}
	code, out := call("GET", "/v1/projects/proj/topics/missing", "")
	e, _ := out["error"].(map[string]any)
	if code != 404 || e["code"] != 404.0 || e["status"] != "NOT_FOUND" || e["message"] != "Topic not found" {
		t.Fatalf("missing topic: %d %v", code, out)
	}
	if code, out = call("POST", "/v1/projects/proj/subscriptions/rest-sub:pull", `{"maxMessages":`); code != 400 {
		t.Fatalf("bad json: %d %v", code, out)
	}
}

func TestPersistence(t *testing.T) {
	svc, c, _ := setup(t)
	tp := topic(t, c, "durable")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/durable-sub", Topic: tp, Filter: `attributes:keep`})
	publish(t, c, tp, msg("one", "keep", "1"), msg("dropped"), msg("two", "keep", "2"))
	pull(t, c, sub, false)[0].GetAckId() // leases are not persisted
	b := must(svc.Save())

	restored := pubsub.New(core.NewHub(core.Config{}, nil))
	defer restored.Close()
	if err := restored.Load(b); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range restored.Peek(sub, 10) {
		got = append(got, m.Data)
	}
	if !slices.Equal(got, []string{"one", "two"}) {
		t.Fatalf("restored backlog = %v", got)
	}
	if id := must(restored.Publish(tp, []byte("x"), map[string]string{"keep": "y"})); id != "4" {
		t.Fatalf("message ids restart: %s", id)
	}
	if len(restored.Peek(sub, 10)) != 3 || restored.Inspect("Subscriptions", sub) == nil || restored.Inspect("Topics", tp) == nil {
		t.Fatal("restored subscription is not live")
	}
	restored.Reset()
	if rows := restored.Resources()[0].Rows; len(rows) != 0 {
		t.Fatalf("after reset: %v", rows)
	}
}
