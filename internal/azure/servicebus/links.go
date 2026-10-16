package servicebus

import (
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

// diagID is the application property the Azure SDKs carry a message's W3C traceparent in.
const diagID = "Diagnostic-Id"

// traceOf is the flow a client gave a message in its Diagnostic-Id property, or "".
func traceOf(m *amqp.Message) string {
	v, _ := m.ApplicationProperties[diagID].(string)
	return core.TraceFromTraceparent(v)
}

// span records one hop of a message's flow.
func (s *Service) span(kind, op, from, to, trace string) {
	if trace != "" {
		s.hub.Span(kind, "servicebus", op, from, to, trace, time.Now(), nil)
	}
}

// id is the ARM resource ID of a queue, topic, subscription or dead-letter queue.
func (n *node) id() string {
	id := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup +
		"/providers/Microsoft.ServiceBus/namespaces/" + n.nsKey
	switch n.kind {
	case "queue":
		id += "/queues/" + n.name
	case "topic":
		id += "/topics/" + n.name
	default:
		id += "/topics/" + n.t.Name + "/subscriptions/" + n.sub.Name
	}
	if n.dlq {
		id += "/$deadletterqueue"
	}
	return id
}

// Links are topic to subscription, and the forwardTo wires of queues and subscriptions
// (forwardDeadLetteredMessagesTo is the dead-letter kind).
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	forwards := func(n *node) {
		p := n.props()
		for _, f := range []struct{ to, kind, label string }{{p.ForwardTo, "configured", "forward"}, {p.ForwardDeadLetteredMessagesTo, "dlq", "dead letter"}} {
			if t := s.find(n.nsKey, lastSegments(f.to)); f.to != "" && t != nil {
				out = append(out, core.Link{From: n.id(), To: t.id(), Kind: f.kind, Label: f.label})
			}
		}
	}
	for _, ns := range core.SortedKeys(s.st.Namespaces) {
		for _, q := range core.SortedKeys(s.st.Namespaces[ns].Queues) {
			forwards(s.find(ns, q))
		}
		for _, tn := range core.SortedKeys(s.st.Namespaces[ns].Topics) {
			t := s.find(ns, tn)
			for _, sn := range core.SortedKeys(t.t.Subs) {
				sub := s.find(ns, strings.ToLower(tn)+"/Subscriptions/"+sn)
				out = append(out, core.Link{From: t.id(), To: sub.id(), Kind: "configured", Label: "subscription"})
				forwards(sub)
			}
		}
	}
	return out
}
