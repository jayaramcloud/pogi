package storage

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Links are the bucket notification configs: bucket to the Pub/Sub topic.
func (s *Service) Links() []core.Link {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []core.Link
	for _, name := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[name]
		for _, id := range core.SortedKeys(b.Notifications) {
			t := strings.TrimPrefix(b.Notifications[id].Topic, "//pubsub.googleapis.com/")
			out = append(out, core.Link{From: "//storage.googleapis.com/projects/_/buckets/" + name, To: "//pubsub.googleapis.com/" + t, Kind: "configured", Label: "notification"})
		}
	}
	return out
}
