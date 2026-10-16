package rbac

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/entra"
	"github.com/nrndev/pogi/internal/core"
)

// The recorder remembers which (action, scope) pairs every service principal and managed identity
// calls, in every enforcement mode, so the console can turn them into a least-privilege role. In
// off mode the request is evaluated but never denied.

const (
	maxPrincipals = 100 // principals recorded
	maxCalls      = 500 // distinct (action, scope) pairs per principal
)

// Principal is one service principal or managed identity and what it has called.
type Principal struct {
	OID, Name, Kind string
	Since           time.Time
	Calls           int // all requests, including pairs beyond the cap
	Actions         []Call
}

// Call is one distinct (action, scope) pair.
type Call struct {
	Action  string
	Data    bool   // a data action
	Scope   string // the resource the call was about
	Count   int
	Last    time.Time
	Verdict string // allowed or implicitDeny: what strict mode says
	Reason  string
}

func (s *Service) record(who entra.Caller, a act, verdict, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec == nil {
		s.rec = map[string]*Principal{}
	}
	p := s.rec[who.OID]
	if p == nil {
		if len(s.rec) >= maxPrincipals {
			return
		}
		p = &Principal{OID: who.OID, Name: who.Name, Kind: who.Kind, Since: time.Now()}
		s.rec[who.OID] = p
	}
	p.Calls++
	i := slices.IndexFunc(p.Actions, func(c Call) bool { return c.Action == a.Name && c.Scope == a.Scope })
	if i < 0 {
		if len(p.Actions) >= maxCalls {
			return // ponytail: further distinct pairs are only counted; raise maxCalls or evict if it matters
		}
		p.Actions = append(p.Actions, Call{Action: a.Name, Data: a.Data, Scope: a.Scope})
		i = len(p.Actions) - 1
	}
	c := &p.Actions[i]
	c.Count++
	c.Last, c.Verdict, c.Reason = time.Now(), verdict, reason
}

func service(hub *core.Hub) *Service {
	s, _ := services.Load(hub)
	r, _ := s.(*Service)
	return r
}

// Recorded is everything recorded so far, principals sorted by name and calls by count. Calls that
// were denied are checked again against the current role assignments, so an attached role clears
// them without new traffic.
func Recorded(hub *core.Hub) []Principal {
	s := service(hub)
	if s == nil {
		return nil
	}
	s.mu.Lock()
	var out []Principal
	for _, p := range s.rec {
		c := *p
		c.Actions = slices.Clone(p.Actions)
		out = append(out, c)
	}
	s.mu.Unlock()
	slices.SortFunc(out, func(a, b Principal) int { return strings.Compare(a.Name+a.OID, b.Name+b.OID) })
	for _, p := range out {
		for i, c := range p.Actions {
			if c.Verdict != "allowed" && s.arm() != nil {
				if v, why := s.decide(p.OID, act{c.Action, c.Data, c.Scope}); v == "allowed" {
					p.Actions[i].Verdict, p.Actions[i].Reason = v, why
				}
			}
		}
		slices.SortStableFunc(p.Actions, func(a, b Call) int { return b.Count - a.Count })
	}
	return out
}

// Restart forgets everything recorded for the principal ("" for all).
func Restart(hub *core.Hub, oid string) {
	s := service(hub)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if oid == "" {
		s.rec = nil
	} else {
		delete(s.rec, oid)
	}
}

// WouldDeny are the recorded calls the principal's role assignments do not allow.
func (p Principal) WouldDeny() []Call {
	var out []Call
	for _, c := range p.Actions {
		if c.Verdict != "allowed" {
			out = append(out, c)
		}
	}
	return out
}

// Short is the last two segments of a scope, for tables: ".../storageAccounts/sa".
func Short(scope string) string {
	seg := strings.Split(strings.Trim(scope, "/"), "/")
	if len(seg) <= 2 {
		return scope
	}
	return ".../" + strings.Join(seg[len(seg)-2:], "/")
}
