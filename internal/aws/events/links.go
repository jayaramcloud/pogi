package events

import "github.com/nrndev/pogi/internal/core"

// Links are the wires of EventBridge: a bus feeds its rules and archives, and a rule
// sends to its targets (another bus included).
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, k := range core.SortedKeys(s.st.Buses) {
		b := s.st.Buses[k]
		for _, name := range core.SortedKeys(b.Rules) {
			out = append(out, core.Link{From: b.arn(), To: b.ruleARN(name), Kind: "configured", Label: "rule"})
			for _, t := range b.Rules[name].Targets {
				out = append(out, core.Link{From: b.ruleARN(name), To: t.Arn, Kind: "configured", Label: "target"})
			}
		}
	}
	for _, k := range core.SortedKeys(s.st.Archives) {
		if a := s.st.Archives[k]; a.EventSourceArn != "" {
			out = append(out, core.Link{From: a.EventSourceArn, To: a.ArchiveArn, Kind: "configured", Label: "archive"})
		}
	}
	return out
}
