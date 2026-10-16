package pubsub

import "github.com/nrndev/pogi/internal/core"

const res = "//pubsub.googleapis.com/"

// Links are the wires of the subscriptions: topic to subscription, push
// subscription to its endpoint, and a subscription's dead-letter topic.
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, n := range core.SortedKeys(s.subs) {
		p := s.subs[n].p
		out = append(out, core.Link{From: res + p.Topic, To: res + p.Name, Kind: "configured", Label: "subscription"})
		if u := p.PushConfig.GetPushEndpoint(); u != "" {
			out = append(out, core.Link{From: res + p.Name, To: u, Kind: "configured", Label: "push"})
		}
		if dl := p.DeadLetterPolicy.GetDeadLetterTopic(); dl != "" {
			out = append(out, core.Link{From: res + p.Name, To: res + dl, Kind: "dlq", Label: "dead letter"})
		}
	}
	return out
}
