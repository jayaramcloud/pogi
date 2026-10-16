package lambda

import (
	"slices"

	"github.com/nrndev/pogi/internal/core"
)

// Links are the event source mappings, from the stream or queue to the function.
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, id := range core.SortedKeys(s.st.Mappings) {
		m := s.st.Mappings[id]
		out = append(out, core.Link{From: m.SourceArn, To: m.FunctionArn, Kind: "configured", Label: "event source"})
	}
	return slices.Clip(out)
}
