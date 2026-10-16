package pubsub_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ps "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/pubsub"
	"google.golang.org/grpc/metadata"
)

// A traceparent on the publish reaches the pulled message and the push request, and the hops show in the span log.
func TestTracePropagates(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return pubsub.New(h) })
	t.Setenv("PUBSUB_EMULATOR_HOST", strings.TrimPrefix(url, "http://"))
	c, err := ps.NewClient(ctx, "proj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })

	got := make(chan string, 4)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- r.Header.Get("Traceparent") }))
	defer hook.Close()
	tp := topic(t, c, "traced")
	pullSub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/traced-pull", Topic: tp})
	subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/traced-push", Topic: tp, PushConfig: &pb.PushConfig{PushEndpoint: hook.URL}})

	const parent = "00-5f84c7a131d9a2b4c5e6f7a8b9c0d1e2-53995c3f42cd8ad8-01"
	pctx := metadata.AppendToOutgoingContext(ctx, "traceparent", parent)
	must(c.TopicAdminClient.Publish(pctx, &pb.PublishRequest{Topic: tp, Messages: []*pb.PubsubMessage{msg("hi")}}))
	root := core.TraceFromTraceparent(parent)

	if rms := pull(t, c, pullSub, true); len(rms) != 1 || len(rms[0].Message.Attributes) != 0 {
		t.Fatalf("pulled message should carry no injected attribute: %+v", rms)
	}
	eventually(t, "push", func() bool { return len(got) >= 1 })
	if core.TraceFromTraceparent(<-got) != root {
		t.Fatal("push request lost the trace")
	}
	ops := map[string]string{}
	eventually(t, "spans", func() bool {
		for _, s := range h.Spans.All() {
			if s.Trace == root {
				ops[s.Op] = s.To
			}
		}
		return ops["push"] != ""
	})
	if ops["publish"] != "//pubsub.googleapis.com/"+tp || ops["pull"] != "//pubsub.googleapis.com/"+pullSub || ops["push"] != hook.URL {
		t.Fatalf("spans = %v", ops)
	}
}

// A message published without a trace comes back with exactly its own attributes, and its hops still share one trace.
func TestTraceNotInjected(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return pubsub.New(h) })
	t.Setenv("PUBSUB_EMULATOR_HOST", strings.TrimPrefix(url, "http://"))
	c, err := ps.NewClient(ctx, "proj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	tp := topic(t, c, "plain")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/plain-pull", Topic: tp})
	m := msg("hi")
	m.Attributes = map[string]string{"k": "v"}
	must(c.TopicAdminClient.Publish(ctx, &pb.PublishRequest{Topic: tp, Messages: []*pb.PubsubMessage{m}}))
	rms := pull(t, c, sub, true)
	if len(rms) != 1 || len(rms[0].Message.Attributes) != 1 || rms[0].Message.Attributes["k"] != "v" {
		t.Fatalf("attributes = %+v", rms)
	}
	traces := map[string]string{}
	eventually(t, "spans", func() bool {
		for _, s := range h.Spans.All() {
			traces[s.Op] = s.Trace
		}
		return traces["publish"] != "" && traces["pull"] != ""
	})
	if traces["publish"] != traces["pull"] {
		t.Fatalf("publish and pull are in different traces: %v", traces)
	}
}

func TestLinks(t *testing.T) {
	svc, c, _ := setup(t)
	tp, dl := topic(t, c, "main"), topic(t, c, "dead")
	sub := subscribe(t, c, &pb.Subscription{Name: proj + "/subscriptions/main-sub", Topic: tp,
		PushConfig:       &pb.PushConfig{PushEndpoint: "http://localhost:9/hook"},
		DeadLetterPolicy: &pb.DeadLetterPolicy{DeadLetterTopic: dl, MaxDeliveryAttempts: 5}})
	const r = "//pubsub.googleapis.com/"
	want := map[core.Link]bool{
		{From: r + tp, To: r + sub, Kind: "configured", Label: "subscription"}:            true,
		{From: r + sub, To: "http://localhost:9/hook", Kind: "configured", Label: "push"}: true,
		{From: r + sub, To: r + dl, Kind: "dlq", Label: "dead letter"}:                    true,
	}
	for _, l := range svc.Links() {
		delete(want, l)
	}
	if len(want) > 0 {
		t.Fatalf("missing links: %v (got %v)", want, svc.Links())
	}
}
