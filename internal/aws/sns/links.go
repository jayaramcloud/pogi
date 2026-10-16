package sns

import "github.com/nrndev/pogi/internal/core"

// Links are the subscriptions that deliver somewhere pogi can follow.
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, arn := range core.SortedKeys(s.st.Topics) {
		t := s.st.Topics[arn]
		for _, k := range core.SortedKeys(t.Subs) {
			switch sub := t.Subs[k]; sub.Protocol {
			case "sqs", "lambda", "firehose", "http", "https":
				out = append(out, core.Link{From: t.ARN, To: sub.Endpoint, Kind: "configured", Label: "subscription"})
			}
		}
	}
	return out
}
