package servicebus_test

import (
	"testing"

	"github.com/nrndev/pogi/internal/azure/servicebus"
	"github.com/nrndev/pogi/internal/core"
)

const entityPrefix = "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup +
	"/providers/Microsoft.ServiceBus/namespaces/" + ns + "/"

// A traceparent on the send follows the message through forwarding, topic fan-out and receive, each hop a span.
func TestTracePropagates(t *testing.T) {
	e := newEnv(t)
	e.queue("target", servicebus.Props{})
	e.queue("src", servicebus.Props{ForwardTo: "target"})
	if _, err := e.svc.CreateTopic(ns, "t", servicebus.Props{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.CreateSubscription(ns, "t", "s", servicebus.Props{}, nil); err != nil {
		t.Fatal(err)
	}
	const parent = "00-5f84c7a131d9a2b4c5e6f7a8b9c0d1e2-53995c3f42cd8ad8-01"
	root := core.TraceFromTraceparent(parent)
	for _, to := range []string{"src", "t"} {
		m := msg("hi", to)
		m.ApplicationProperties = map[string]any{"Diagnostic-Id": parent}
		e.send(to, m)
	}
	src, l := e.mustReceiver("target")
	if got := e.next(src, l).ApplicationProperties["Diagnostic-Id"]; got != parent {
		t.Fatalf("received Diagnostic-Id = %v", got)
	}
	seen := map[string]string{} // op -> from -> to
	for _, s := range e.h.Spans.All() {
		if s.Trace == root {
			seen[s.Op] += s.From + ">" + s.To + " "
		}
	}
	for op, want := range map[string]string{
		"forward": entityPrefix + "queues/src>" + entityPrefix + "queues/target ",
		"fan-out": entityPrefix + "topics/t>" + entityPrefix + "topics/t/subscriptions/s ",
		"receive": entityPrefix + "queues/target>" + entityPrefix + "queues/target ",
	} {
		if seen[op] != want {
			t.Errorf("%s spans = %q, want %q", op, seen[op], want)
		}
	}
	if seen["send"] == "" {
		t.Error("no send span")
	}

}

// A message sent without a trace comes back with exactly its own properties, and its hops share one trace.
func TestTraceNotInjected(t *testing.T) {
	e := newEnv(t)
	e.queue("target", servicebus.Props{})
	e.queue("src", servicebus.Props{ForwardTo: "target"})
	m := msg("fresh", "x")
	m.ApplicationProperties = map[string]any{"k": "v"}
	e.send("src", m)
	src, l := e.mustReceiver("target")
	got := e.next(src, l)
	if len(got.ApplicationProperties) != 1 || got.ApplicationProperties["k"] != "v" {
		t.Fatalf("properties = %v", got.ApplicationProperties)
	}
	traces := map[string]string{}
	for _, s := range e.h.Spans.All() {
		traces[s.Op] = s.Trace
	}
	if traces["send"] == "" || traces["forward"] != traces["send"] || traces["receive"] != traces["send"] {
		t.Fatalf("hops are not under one trace: %v", traces)
	}
}

func TestLinks(t *testing.T) {
	e := newEnv(t)
	e.queue("dead", servicebus.Props{})
	e.queue("target", servicebus.Props{})
	e.queue("src", servicebus.Props{ForwardTo: "sb://" + ns + ".servicebus.windows.net/target", ForwardDeadLetteredMessagesTo: "dead"})
	if _, err := e.svc.CreateTopic(ns, "t", servicebus.Props{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.CreateSubscription(ns, "t", "s", servicebus.Props{ForwardTo: "target"}, nil); err != nil {
		t.Fatal(err)
	}
	q, sub := entityPrefix+"queues/", entityPrefix+"topics/t/subscriptions/s"
	want := map[core.Link]bool{
		{From: q + "src", To: q + "target", Kind: "configured", Label: "forward"}:             true,
		{From: q + "src", To: q + "dead", Kind: "dlq", Label: "dead letter"}:                  true,
		{From: entityPrefix + "topics/t", To: sub, Kind: "configured", Label: "subscription"}: true,
		{From: sub, To: q + "target", Kind: "configured", Label: "forward"}:                   true,
	}
	for _, l := range e.svc.Links() {
		delete(want, l)
	}
	if len(want) > 0 {
		t.Fatalf("missing links: %v (got %v)", want, e.svc.Links())
	}
}
