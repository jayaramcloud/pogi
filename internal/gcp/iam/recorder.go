package iam

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The recorder remembers which permissions every service account and user calls, on which
// resource, in every enforcement mode, so the console can turn them into role bindings. It also
// keeps what strict mode would say: in off mode the request is evaluated but never denied.

const (
	maxPrincipals = 100 // principals recorded
	maxCalls      = 500 // distinct (permission, resource) pairs per principal
)

// Principal is one service account or user and what it has called.
type Principal struct {
	Member  string // serviceAccount:<email> or user:<email>
	Since   time.Time
	Calls   int // all requests, including pairs beyond the cap
	Actions []Call
}

// Call is one distinct (permission, resource) pair.
type Call struct {
	Service  string // the pogi service
	Perm     string // pubsub.topics.publish; service/Method when the call is not modelled
	Resource string // where a binding for it belongs: projects/p or projects/p/topics/t
	Sample   string // the resource the latest call named
	Project  string
	Count    int
	Last     time.Time
	Verdict  string // allowed or denied: what strict mode says
	Reason   string
}

// Modelled is false for calls pogi allows without knowing their permission.
func (c Call) Modelled() bool { return c.Reason != "not modelled" }

type call struct {
	member, service, perm, target, sample, project, verdict, reason string
}

type recorder struct {
	mu sync.Mutex
	m  map[string]*Principal
}

func (s *Service) record(k call) {
	r := &s.rec
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*Principal{}
	}
	p := r.m[k.member]
	if p == nil {
		if len(r.m) >= maxPrincipals {
			return
		}
		p = &Principal{Member: k.member, Since: time.Now()}
		r.m[k.member] = p
	}
	p.Calls++
	i := slices.IndexFunc(p.Actions, func(c Call) bool { return c.Perm == k.perm && c.Resource == k.target })
	if i < 0 {
		if len(p.Actions) >= maxCalls {
			return // ponytail: further distinct pairs are only counted; raise maxCalls or evict if it matters
		}
		p.Actions = append(p.Actions, Call{Service: k.service, Perm: k.perm, Resource: k.target})
		i = len(p.Actions) - 1
	}
	c := &p.Actions[i]
	c.Count++
	c.Last, c.Sample, c.Project, c.Verdict, c.Reason = time.Now(), k.sample, k.project, k.verdict, k.reason
}

// Recorded is everything recorded so far, principals sorted by member and calls by count. Verdicts
// are checked again against the current policies, so a binding (or its undo) shows at once
// without new traffic.
func Recorded(hub *core.Hub) []Principal {
	if s, _ := hub.Service("gcpiam").(*Service); s != nil {
		return s.Recorded()
	}
	return nil
}

// Recorded is Recorded for this service.
func (s *Service) Recorded() []Principal {
	s.rec.mu.Lock()
	var out []Principal
	for _, p := range s.rec.m {
		c := *p
		c.Actions = slices.Clone(p.Actions)
		out = append(out, c)
	}
	s.rec.mu.Unlock()
	slices.SortFunc(out, func(a, b Principal) int { return strings.Compare(a.Member, b.Member) })
	for _, p := range out {
		for i, c := range p.Actions {
			if c.Modelled() {
				ok, why := s.decide(p.Member, c.Service, c.Perm, c.Sample, c.Project)
				p.Actions[i].Verdict, p.Actions[i].Reason = map[bool]string{true: "allowed", false: "denied"}[ok], why
			}
		}
		slices.SortStableFunc(p.Actions, func(a, b Call) int { return b.Count - a.Count })
	}
	return out
}

// Restart forgets everything recorded for member ("" for all).
func Restart(hub *core.Hub, member string) {
	s, _ := hub.Service("gcpiam").(*Service)
	if s == nil {
		return
	}
	s.rec.mu.Lock()
	defer s.rec.mu.Unlock()
	if member == "" {
		s.rec.m = nil
	} else {
		delete(s.rec.m, member)
	}
}

// WouldDeny are the recorded calls the member's bindings do not allow.
func (p Principal) WouldDeny() []Call {
	var out []Call
	for _, c := range p.Actions {
		if c.Verdict == "denied" {
			out = append(out, c)
		}
	}
	return out
}
