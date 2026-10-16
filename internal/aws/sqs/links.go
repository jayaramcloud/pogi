package sqs

import (
	"encoding/json"

	"github.com/nrndev/pogi/internal/core"
)

// Links are the redrive policies, from a queue to its dead-letter queue.
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, k := range core.SortedKeys(s.st.Queues) {
		q := s.st.Queues[k]
		var rp redrivePolicy
		if json.Unmarshal([]byte(q.Attrs["RedrivePolicy"]), &rp) == nil && rp.Target != "" {
			out = append(out, core.Link{From: q.arn(), To: rp.Target, Kind: "dlq", Label: "redrive"})
		}
	}
	return out
}
