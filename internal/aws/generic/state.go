package generic

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Resource is one inferred resource: what its create and update calls sent,
// merged, plus its identifiers, tags and timestamps.
type Resource struct {
	Type    string            `json:"type"`
	Key     string            `json:"key"` // the identifier values joined with "/"
	IDs     map[string]string `json:"ids,omitempty"`
	Name    string            `json:"name,omitempty"`
	ID      string            `json:"id,omitempty"`
	ARN     string            `json:"arn,omitempty"`
	Account string            `json:"account"`
	Region  string            `json:"region,omitempty"`
	Data    map[string]any    `json:"data,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Created time.Time         `json:"created"`
	Updated time.Time         `json:"updated"`
}

type state struct {
	Res map[string]*Resource `json:"res"`
}

func skey(account, region, typ, key string) string {
	return account + "|" + region + "|" + typ + "|" + key
}

func (r *Resource) skey() string { return skey(r.Account, r.Region, r.Type, r.Key) }

func (r *Resource) clone() *Resource {
	b, _ := json.Marshal(r)
	var c Resource
	json.Unmarshal(b, &c)
	return &c
}

// listLocked returns the resources of a type in creation order.
func (s *Service) listLocked(account, region, typ string) []*Resource {
	var out []*Resource
	for _, r := range s.st.Res {
		if r.Account == account && r.Region == region && r.Type == typ {
			out = append(out, r)
		}
	}
	slices.SortFunc(out, func(a, b *Resource) int {
		if c := a.Created.Compare(b.Created); c != 0 {
			return c
		}
		return strings.Compare(a.Key, b.Key)
	})
	return out
}

// byARNLocked finds the resource an ARN names. A child with no ARN of its own can carry its
// parent's (an AMP alert manager definition has its workspace's), so several may match. The one
// the ARN belongs to wins: the one whose type the ARN names ("workspace/..."), then the oldest
// (a parent exists before its children), then the lowest key. Map order used to decide, so
// tags read through the workspace's ARN sometimes came from the definition.
func (s *Service) byARNLocked(account, arn string) *Resource {
	var best *Resource
	var bestKey string
	owns := func(r *Resource) bool { return strings.Contains(arn, ":"+kebab(r.Type)+"/") }
	for k, r := range s.st.Res {
		// a client that does not know its account (Terraform with skip_requesting_account_id) leaves it out of the ARN
		if r.Account != account || r.ARN != arn && strings.Replace(r.ARN, ":"+account+":", "::", 1) != arn {
			continue
		}
		if best == nil || owns(r) && !owns(best) ||
			owns(r) == owns(best) && (r.Created.Before(best.Created) || r.Created.Equal(best.Created) && k < bestKey) {
			best, bestKey = r, k
		}
	}
	return best
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Res == nil {
		st.Res = map[string]*Resource{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	if f := resumers[s.meta.Name]; f != nil {
		f(s)
	}
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Res: map[string]*Resource{}}
	s.mu.Unlock()
}

// Resources lists the inferred resource types and their rows for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	by := map[string]*core.Table{}
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		t := by[r.Type]
		if t == nil {
			t = &core.Table{Name: r.Type, Columns: []string{"Id", "Name", "Region", "Created"}}
			by[r.Type] = t
		}
		t.Rows = append(t.Rows, []string{r.identity(), r.label(), r.Region, r.Created.Format(time.DateTime)})
	}
	var out []core.Table
	for _, n := range core.SortedKeys(by) {
		out = append(out, *by[n])
	}
	return out
}

// label is the resource's name for the console: its own, else the one it was created with (a resource whose
// identifier is an ARN or an id keeps its name only in its data, as a License Manager configuration does), else its id.
func (r *Resource) label() string {
	if r.Name != "" {
		return r.Name
	}
	for _, k := range []string{"Name", "name", r.Type + "Name", lowerFirst(r.Type) + "Name"} {
		if s, ok := r.Data[k].(string); ok && s != "" {
			return s
		}
	}
	return r.ID
}

func (r *Resource) identity() string {
	if r.ARN != "" {
		return r.ARN
	}
	return r.Account + "/" + r.Region + "/" + r.Key
}

// Inspect returns the stored resource whose first-column id is id.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.st.Res {
		if r.Type == table && r.identity() == id {
			return r.clone()
		}
	}
	return nil
}
