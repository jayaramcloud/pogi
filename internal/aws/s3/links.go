package s3

import (
	"encoding/xml"

	"github.com/nrndev/pogi/internal/core"
)

// Links are the notification configurations, from the bucket to each queue, topic or function.
func (s *Service) Links() []core.Link {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []core.Link
	for _, name := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[name]
		var n notification
		if raw := b.Config["notification"]; raw == "" || xml.Unmarshal([]byte(raw), &n) != nil {
			continue
		}
		for _, t := range n.targets() {
			out = append(out, core.Link{From: core.GlobalARN("s3", b.Region, "", b.Name), To: t.Queue + t.Topic + t.Lambda, Kind: "configured", Label: "notification"})
		}
	}
	return out
}
