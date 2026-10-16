package eventgrid_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// A traceparent on the publish reaches the webhook and shows as a span of the flow.
func TestTracePropagates(t *testing.T) {
	e := setup(t)
	e.eg.CreateTopic("t", "")
	h := webhook(t, false, ok)
	if err := e.eg.CreateSubscription("topics/t", webhookSub("s", h.URL)); err != nil {
		t.Fatal(err)
	}
	const parent = "00-5f84c7a131d9a2b4c5e6f7a8b9c0d1e2-53995c3f42cd8ad8-01"
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/_azure/eventgrid/t/api/events", strings.NewReader(
		`[{"id":"1","subject":"s","eventType":"t","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1","data":{}}]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", parent)
	must(http.DefaultClient.Do(req))
	root := core.TraceFromTraceparent(parent)
	if got := core.TraceFromTraceparent(h.next(t).header.Get("traceparent")); got != root {
		t.Fatalf("webhook trace = %q, want %q", got, root)
	}
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		for _, s := range e.srv.Hub.Spans.All() {
			if s.Trace == root && s.Kind == core.SpanNotify && s.To == h.URL {
				return
			}
		}
	}
	t.Fatal("no notify span on the trace")
}

func TestLinks(t *testing.T) {
	e := setup(t)
	e.eg.CreateTopic("t", "")
	h := webhook(t, false, ok)
	must(0, e.eg.CreateSubscription("topics/t", webhookSub("hook", h.URL)))
	must(0, e.eg.CreateSubscription("topics/t", busSub("bus", "q")))
	must(0, e.eg.CreateSystemTopic("st", account, "Microsoft.Storage.StorageAccounts"))
	must(0, e.eg.CreateSubscription("systemTopics/st", webhookSub("sys", h.URL)))
	topic := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/Microsoft.EventGrid/topics/t"
	queue := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/Microsoft.ServiceBus/namespaces/ns/queues/q"
	want := map[core.Link]bool{
		{From: topic, To: h.URL, Kind: "configured", Label: "subscription"}:   true,
		{From: topic, To: queue, Kind: "configured", Label: "subscription"}:   true,
		{From: account, To: h.URL, Kind: "configured", Label: "subscription"}: true,
	}
	for _, l := range e.eg.Links() {
		delete(want, l)
	}
	if len(want) > 0 {
		t.Fatalf("missing links: %v (got %v)", want, e.eg.Links())
	}
}
