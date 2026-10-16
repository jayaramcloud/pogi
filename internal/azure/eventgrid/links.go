package eventgrid

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Links are the subscriptions: the topic (or, for a system topic, the Azure
// resource that publishes its events) to the endpoint or resource it delivers to.
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, k := range core.SortedKeys(s.st.Subs) {
		sub := s.st.Subs[k]
		from := resourceID(sub.Scope)
		if name, ok := strings.CutPrefix(sub.Scope, "systemTopics/"); ok && s.st.System[name] != nil {
			from = s.st.System[name].Source
		}
		to := sub.Destination.ResourceID
		if sub.Destination.Type == "WebHook" {
			to = sub.Destination.Endpoint
		}
		out = append(out, core.Link{From: from, To: to, Kind: "configured", Label: "subscription"})
	}
	return out
}
