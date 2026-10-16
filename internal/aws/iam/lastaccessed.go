package iam

import (
	"cmp"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service last accessed reports. A report lists the services the entity's
// policies allow, and for each one when the entity last called it (from the
// recorder, so the times are for this pogi run). The job is done at once.
//
// ponytail: jobs are not saved across a restart; group and policy reports list
// services without access times; Deny statements and boundaries are not applied.

type lastJob struct {
	Created  time.Time
	Services []serviceAccess
}

// serviceAccess is one ServicesLastAccessed entry.
type serviceAccess struct {
	ServiceName, ServiceNamespace string
	LastAuthenticated             core.Time
	LastAuthenticatedEntity       string
	TotalAuthenticatedEntities    int
}

// accessDocs finds the policy documents of a user, role, group or policy ARN.
func (s *Service) accessDocs(arn string) ([]string, error) {
	_, _, account, res, ok := core.ParseARN(arn)
	kind, rest, _ := strings.Cut(res, "/")
	name := rest[strings.LastIndex(rest, "/")+1:]
	if !ok {
		return nil, core.Errorf(400, "InvalidInput", "Invalid ARN: %s", arn)
	}
	switch kind {
	case "user", "role":
		s.mu.Lock()
		var err error
		if kind == "user" {
			_, err = get(s.acct(account).Users, "user", name)
		} else {
			_, err = get(s.acct(account).Roles, "role", name)
		}
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return s.PoliciesFor(arn), nil
	case "group":
		s.mu.Lock()
		defer s.mu.Unlock()
		g, err := get(s.acct(account).Groups, "group", name)
		if err != nil {
			return nil, err
		}
		return s.docsOf(&g.perms), nil
	case "policy":
		s.mu.Lock()
		defer s.mu.Unlock()
		p, err := s.policy(arn)
		if err != nil {
			return nil, err
		}
		return []string{p.document()}, nil
	}
	return nil, core.Errorf(400, "InvalidInput", "The ARN %s is not a user, group, role or policy.", arn)
}

// docsOf lists the inline and attached policy documents of a holder; lock held.
func (s *Service) docsOf(h *perms) []string {
	var docs []string
	for _, n := range core.SortedKeys(h.Inline) {
		docs = append(docs, h.Inline[n])
	}
	for _, arn := range h.Attached {
		if p, err := s.policy(arn); err == nil {
			docs = append(docs, p.document())
		}
	}
	return docs
}

// catalog is the AWS services pogi runs: IAM namespace → display name.
func (s *Service) catalog() map[string]string {
	out := map[string]string{}
	if s.hub != nil {
		for _, svc := range s.hub.Services() {
			if i := svc.Info(); i.Cloud == core.AWS {
				out[cmp.Or(prefixes[i.Name], i.Name)] = i.Title
			}
		}
	}
	return out
}

func (s *Service) generateServiceLastAccessedDetails(c *core.Ctx, in *struct{ Arn, Granularity string }) (any, error) {
	if in.Granularity != "" && in.Granularity != "SERVICE_LEVEL" && in.Granularity != "ACTION_LEVEL" {
		return nil, core.Errorf(400, "ValidationError", "Granularity must be SERVICE_LEVEL or ACTION_LEVEL.")
	}
	docs, err := s.accessDocs(in.Arn)
	if err != nil {
		return nil, err
	}
	var patterns []string
	for _, d := range docs {
		for _, st := range statements(d) {
			if st["Effect"] != "Allow" {
				continue
			}
			if _, not := st["NotAction"]; not {
				patterns = append(patterns, "*")
			}
			for _, a := range asStrings(st["Action"]) {
				patterns = append(patterns, strings.ToLower(strings.SplitN(a, ":", 2)[0]))
			}
		}
	}
	cat := s.catalog()
	for _, p := range patterns { // a named service pogi does not run is still listed
		if _, known := cat[p]; !known && !strings.ContainsAny(p, "*?") {
			cat[p] = p
		}
	}
	used := s.lastCalls(in.Arn)
	var list []serviceAccess
	for _, ns := range core.SortedKeys(cat) {
		if !matchAny(patterns, ns, true, nil) {
			continue
		}
		sa := serviceAccess{ServiceName: cat[ns], ServiceNamespace: ns}
		if t, ok := used[ns]; ok {
			sa.LastAuthenticated, sa.LastAuthenticatedEntity, sa.TotalAuthenticatedEntities = core.Time(t.UTC().Truncate(time.Second)), in.Arn, 1
		}
		list = append(list, sa)
	}
	id := core.UUID()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs == nil {
		s.jobs = map[string]lastJob{}
	}
	s.jobs[id] = lastJob{Created: time.Now(), Services: list}
	return map[string]any{"JobId": id}, nil
}

// lastCalls is the last time the principal called each service namespace.
func (s *Service) lastCalls(arn string) map[string]time.Time {
	s.rec.mu.Lock()
	defer s.rec.mu.Unlock()
	out := map[string]time.Time{}
	if p := s.rec.m[arn]; p != nil {
		for _, c := range p.Actions {
			ns, _, _ := strings.Cut(strings.ToLower(c.Action), ":")
			if c.Last.After(out[ns]) {
				out[ns] = c.Last
			}
		}
	}
	return out
}

func (s *Service) getServiceLastAccessedDetails(c *core.Ctx, in *struct {
	JobId, Marker string
	MaxItems      int
}) (any, error) {
	s.mu.Lock()
	j, ok := s.jobs[in.JobId]
	s.mu.Unlock()
	if !ok {
		return nil, core.Errorf(404, "NoSuchEntity", "The job %s cannot be found.", in.JobId)
	}
	out := page("ServicesLastAccessed", j.Services, in.Marker, in.MaxItems)
	created := core.Time(j.Created.UTC().Truncate(time.Second))
	out["JobStatus"], out["JobType"], out["JobCreationDate"], out["JobCompletionDate"] = "COMPLETED", "SERVICE_LEVEL", created, created
	return out, nil
}
