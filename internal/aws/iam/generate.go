package iam

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Least-privilege policies from what the recorder saw, and attaching them to
// the caller as a versioned customer-managed policy.

// pairs is action → set of resources ("*" when the action has none).
type pairs map[string]map[string]bool

func (p pairs) add(action, resource string) {
	if resource == "" {
		resource = "*"
	}
	if p[action] == nil {
		p[action] = map[string]bool{}
	}
	p[action][resource] = true
}

func callPairs(calls []Call) pairs {
	p := pairs{}
	for _, c := range calls {
		p.add(c.Action, c.Resource)
	}
	return p
}

// document groups the actions of each service by identical resource sets, one
// Allow statement per group. Only actions with no known resource get "*".
func (p pairs) document() string {
	type group struct {
		svc       string
		actions   []string
		resources []string
	}
	groups := map[string]*group{}
	for _, a := range core.SortedKeys(p) {
		res := core.SortedKeys(p[a])
		svc, _, _ := strings.Cut(a, ":")
		key := svc + "|" + strings.Join(res, ",")
		if groups[key] == nil {
			groups[key] = &group{svc: svc, resources: res}
		}
		groups[key].actions = append(groups[key].actions, a)
	}
	var list []*group
	for _, k := range core.SortedKeys(groups) {
		list = append(list, groups[k])
	}
	one := func(l []string) any {
		if len(l) == 1 {
			return l[0]
		}
		return l
	}
	var stmts []any
	for _, g := range list {
		stmts = append(stmts, struct {
			Effect   string
			Action   any
			Resource any
		}{"Allow", one(g.actions), one(g.resources)})
	}
	if stmts == nil {
		stmts = []any{}
	}
	b, _ := json.MarshalIndent(struct {
		Version   string
		Statement []any
	}{"2012-10-17", stmts}, "", "  ")
	return string(b)
}

// Policy is the least-privilege IAM policy (JSON) for everything p called.
func (p Principal) Policy() string { return callPairs(p.Actions).document() }

// Terraform is Policy as an aws_iam_policy resource.
func (p Principal) Terraform() string {
	name := PolicyName(p.ARN)
	doc := strings.ReplaceAll(p.Policy(), "${", "$${") // policy variables are not Terraform interpolations
	return fmt.Sprintf("resource \"aws_iam_policy\" %q {\n  name   = %q\n  policy = jsonencode(%s)\n}\n",
		strings.ReplaceAll(name, "-", "_"), name, strings.ReplaceAll(doc, "\n", "\n  "))
}

// Short is a resource ARN without its prefix: "audit", "uploads/*", "table/orders".
func Short(resource string) string {
	if resource == "" || resource == "*" {
		return "any resource"
	}
	if r, ok := strings.CutPrefix(resource, "arn:aws:s3:::"); ok {
		return r
	}
	if f := strings.SplitN(resource, ":", 6); len(f) == 6 {
		return f[5]
	}
	return resource
}

// Diff describes what attaching would add: "sqs:SendMessage on audit".
func (p Principal) Diff() []string {
	var out []string
	for _, c := range p.WouldDeny() {
		out = append(out, c.Action+" on "+Short(c.Resource))
	}
	return out
}

// PolicyName is the managed policy pogi keeps for a principal: pogi-<name>.
func PolicyName(principal string) string {
	_, name := kindName(principal)
	return "pogi-" + name
}

// kindName splits a user or role ARN into "user"/"role" and its name.
func kindName(principal string) (kind, name string) {
	_, _, _, res, _ := core.ParseARN(principal)
	kind, rest, _ := strings.Cut(res, "/")
	return kind, rest[strings.LastIndex(rest, "/")+1:]
}

// Attachment is the result of Attach, enough to undo it.
type Attachment struct {
	Principal, Policy, Arn string
	Version                string // the version Attach made default
	Prev                   string // the default before it ("" when the policy is new)
	Linked                 bool   // Attach attached the policy to the principal
}

// Attach makes the policy pogi-<name> (customer managed) allow everything the
// principal has called, and attaches it. The first use creates it as v1; later
// uses add a default version (the oldest of 5 goes first) that keeps what the
// previous default allowed.
func Attach(hub *core.Hub, principal string) (Attachment, error) {
	s, _ := hub.Service("iam").(*Service)
	if s == nil {
		return Attachment{}, fmt.Errorf("the iam service is not enabled")
	}
	return s.Attach(principal)
}

func (s *Service) Attach(principal string) (Attachment, error) {
	_, _, account, _, ok := core.ParseARN(principal)
	kind, name := kindName(principal)
	if !ok || kind != "user" && kind != "role" {
		return Attachment{}, fmt.Errorf("%s is not an IAM user or role", principal)
	}
	var want pairs
	for _, p := range s.Recorded() {
		if p.ARN == principal {
			want = callPairs(p.Actions)
		}
	}
	if len(want) == 0 {
		return Attachment{}, fmt.Errorf("nothing recorded for %s yet", principal)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(account)
	var h *perms
	if kind == "user" {
		u, err := get(a.Users, "user", name)
		if err != nil {
			return Attachment{}, err
		}
		h = &u.perms
	} else {
		r, err := get(a.Roles, "role", name)
		if err != nil {
			return Attachment{}, err
		}
		h = &r.perms
	}
	pname := PolicyName(principal)
	att := Attachment{Principal: principal, Policy: pname, Arn: "arn:" + partitionOf(principal) + ":iam::" + account + ":policy/" + pname}
	pol := a.Policies[lower(pname)]
	if pol != nil {
		for _, st := range statements(pol.document()) { // keep what the last version allowed
			if st["Effect"] == "Allow" {
				for _, act := range asStrings(st["Action"]) {
					for _, res := range asStrings(st["Resource"]) {
						want.add(act, res)
					}
				}
			}
		}
	}
	doc, t := want.document(), now()
	if pol == nil {
		pol = &Policy{PolicyName: pname, PolicyId: newID("ANPA"), Arn: att.Arn, Path: "/", IsAttachable: true,
			Description: "Least-privilege policy generated by pogi from recorded calls", CreateDate: t, UpdateDate: t, NextVersion: 1}
		a.Policies[lower(pname)] = pol
	} else {
		att.Prev = pol.DefaultVersionId
		if len(pol.Versions) >= 5 { // AWS allows 5; drop the oldest that is not the default
			if i := slices.IndexFunc(pol.Versions, func(v *Version) bool { return !v.IsDefaultVersion }); i >= 0 {
				pol.Versions = without(pol.Versions, i)
			}
		}
	}
	att.Version = "v" + strconv.Itoa(max(pol.NextVersion, 1))
	pol.NextVersion = max(pol.NextVersion, 1) + 1
	pol.Versions = append(pol.Versions, &Version{Document: doc, VersionId: att.Version, CreateDate: t})
	pol.setDefault(att.Version)
	pol.UpdateDate = t
	if !slices.Contains(h.Attached, att.Arn) {
		h.Attached, att.Linked = append(h.Attached, att.Arn), true
	}
	return att, nil
}

// NextVersion is the version Attach would make default for principal, and
// whether the policy already exists (so a new version, not v1).
func NextVersion(hub *core.Hub, principal string) (version string, exists bool) {
	s, _ := hub.Service("iam").(*Service)
	_, _, account, _, _ := core.ParseARN(principal)
	if s == nil {
		return "v1", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.acct(account).Policies[lower(PolicyName(principal))]; p != nil {
		return "v" + strconv.Itoa(max(p.NextVersion, 1)), true
	}
	return "v1", false
}

// Undo reverses Attach: the previous version becomes the default again, or the
// policy is detached and deleted when Attach created it.
func Undo(hub *core.Hub, att Attachment) error {
	s, _ := hub.Service("iam").(*Service)
	if s == nil {
		return fmt.Errorf("the iam service is not enabled")
	}
	_, _, account, _, _ := core.ParseARN(att.Principal)
	kind, name := kindName(att.Principal)
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(account)
	pol := a.Policies[lower(att.Policy)]
	if pol == nil {
		return nil // already gone
	}
	var h *perms
	if kind == "user" {
		if u := a.Users[lower(name)]; u != nil {
			h = &u.perms
		}
	} else if r := a.Roles[lower(name)]; r != nil {
		h = &r.perms
	}
	if att.Prev != "" {
		if _, err := pol.version(att.Prev); err == nil {
			pol.setDefault(att.Prev)
		}
	}
	if h != nil && (att.Linked || att.Prev == "") {
		if i := slices.Index(h.Attached, att.Arn); i >= 0 {
			h.Attached = without(h.Attached, i)
		}
	}
	if att.Prev == "" {
		delete(a.Policies, lower(att.Policy))
	}
	return nil
}
