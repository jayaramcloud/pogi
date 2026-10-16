package iam

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The recorder remembers which (action, resource) pairs every IAM user and role
// calls, in every enforcement mode, so the console can turn them into a
// least-privilege policy. It also keeps what strict mode would say: in off mode
// the request is evaluated but never denied.

const (
	maxPrincipals = 100 // principals recorded
	maxCalls      = 500 // distinct (action, resource) pairs per principal
)

// Principal is one IAM user or role and what it has called.
type Principal struct {
	ARN     string // user ARN, or role ARN for assumed roles
	Since   time.Time
	Calls   int // all requests, including pairs beyond the cap
	Actions []Call
}

// Call is one distinct (action, resource) pair.
type Call struct {
	Action   string
	Resource string // ARN pattern, "" when pogi could not tell
	Sample   string // a real resource ARN of the latest call
	Count    int
	Last     time.Time
	Verdict  string // allowed, explicitDeny or implicitDeny: what strict mode says
	Reason   string
}

type recorder struct {
	mu sync.Mutex
	m  map[string]*Principal
}

// record notes one request. Object ARNs of a bucket collapse to bucket/*.
func (s *Service) record(principal, action, resource, verdict, reason string) {
	key := resource
	if strings.HasPrefix(key, "arn:") && strings.Contains(key, ":s3:::") {
		if i := strings.IndexByte(key, '/'); i > 0 {
			key = key[:i] + "/*"
		}
	}
	r := &s.rec
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*Principal{}
	}
	p := r.m[principal]
	if p == nil {
		if len(r.m) >= maxPrincipals {
			return
		}
		p = &Principal{ARN: principal, Since: time.Now()}
		r.m[principal] = p
	}
	p.Calls++
	i := slices.IndexFunc(p.Actions, func(c Call) bool { return c.Action == action && c.Resource == key })
	if i < 0 {
		if len(p.Actions) >= maxCalls {
			return // ponytail: further distinct pairs are only counted; raise maxCalls or evict if it matters
		}
		p.Actions = append(p.Actions, Call{Action: action, Resource: key})
		i = len(p.Actions) - 1
	}
	c := &p.Actions[i]
	c.Count++
	c.Last, c.Sample, c.Verdict, c.Reason = time.Now(), resource, verdict, reason
}

// principalOf is the ARN calls are recorded under: assumed roles count as their role.
func principalOf(c *core.Ctx) string {
	_, _, account, res, _ := core.ParseARN(c.Principal)
	if rest, ok := strings.CutPrefix(res, "assumed-role/"); ok {
		role, _, _ := strings.Cut(rest, "/")
		return "arn:" + partitionOf(c.Principal) + ":iam::" + account + ":role/" + role
	}
	return c.Principal
}

// Recorded is everything recorded so far, principals sorted by ARN and calls by
// count. Verdicts that were deny are re-checked against the principal's current
// policies, so an attached policy clears them without new traffic.
// ponytail: the re-check has no condition keys, so a conditional allow still reads as would-deny.
func Recorded(hub *core.Hub) []Principal {
	s, _ := hub.Service("iam").(*Service)
	if s == nil {
		return nil
	}
	return s.Recorded()
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
	slices.SortFunc(out, func(a, b Principal) int { return strings.Compare(a.ARN, b.ARN) })
	for _, p := range out {
		for i, c := range p.Actions {
			if c.Verdict != "allowed" {
				if d, why := s.decideFor(p.ARN, c.Action, c.Sample, map[string]string{}); d == "allowed" {
					p.Actions[i].Verdict, p.Actions[i].Reason = d, why
				}
			}
		}
		slices.SortStableFunc(p.Actions, func(a, b Call) int { return b.Count - a.Count })
	}
	return out
}

// Restart forgets everything recorded for principal ("" for all).
func Restart(hub *core.Hub, principal string) {
	s, _ := hub.Service("iam").(*Service)
	if s == nil {
		return
	}
	s.rec.mu.Lock()
	defer s.rec.mu.Unlock()
	if principal == "" {
		s.rec.m = nil
	} else {
		delete(s.rec.m, principal)
	}
}

// WouldDeny are the recorded calls the principal's policies do not allow.
func (p Principal) WouldDeny() []Call {
	var out []Call
	for _, c := range p.Actions {
		if c.Verdict != "allowed" {
			out = append(out, c)
		}
	}
	return out
}

// partitionOf is the partition of an ARN (aws, aws-cn, aws-us-gov).
func partitionOf(arn string) string {
	_, rest, _ := strings.Cut(arn, ":")
	p, _, _ := strings.Cut(rest, ":")
	return p
}
