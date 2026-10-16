package iam

import (
	"context"
	"slices"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// IAM enforcement (pogi start --iam soft|strict): a request carrying an access token of a service
// account created in IAM, or of a user other than the default one, is checked against the IAM
// policies of the resource and of its project. Requests without such a token (none, the default
// credentials, pogi-token, an ID token) are never checked, recorded or denied.
//
// ponytail: conditions, deny policies, folder and organization policies and legacy members
// (projectOwner:) are not evaluated; a resource or project pogi cannot tell is allowed.

// defaultUser is who the OAuth endpoint's refresh tokens and authorization codes stand for.
const defaultUser = "user@example.com"

// policyService is a service that keeps IAM policies, such as Pub/Sub or Secret Manager.
type policyService interface {
	GetIamPolicy(context.Context, *iampb.GetIamPolicyRequest) (*iampb.Policy, error)
	SetIamPolicy(context.Context, *iampb.SetIamPolicyRequest) (*iampb.Policy, error)
}

func policies(hub *core.Hub, service string) policyService {
	p, _ := hub.Service(service).(policyService)
	return p
}

// member is the IAM member a bearer token stands for, "" when it is not one pogi checks.
func (s *Service) member(bearer string) string {
	t, ok := gapi.ParseAccessToken(bearer)
	switch {
	case !ok || t.Email == "" || t.Email == defaultUser:
		return ""
	case strings.HasSuffix(t.Email, ".gserviceaccount.com"):
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.st.Accounts[t.Email] == nil {
			return "" // a Google-managed or deleted account
		}
		return "serviceAccount:" + t.Email
	}
	return "user:" + t.Email
}

// authorize is the hub's Authorize hook for Google Cloud. It runs in every mode: off records the
// request and allows it.
func (s *Service) authorize(c *core.Ctx) *core.Error {
	member := s.member(c.Bearer)
	if member == "" || strings.HasSuffix(c.Op, "estIamPermissions") {
		return nil // TestIamPermissions needs no permission
	}
	c.Principal = member
	resource := resourceOf(c)
	perm, ok := Permission(c.Service, c.Op, resource)
	project := projectOf(c.Hub, c.Service, resource)
	if !ok || project == "" {
		s.record(call{member: member, service: c.Service, perm: c.Service + "/" + c.Op, verdict: "allowed", reason: "not modelled"})
		c.IAM = "allow " + c.Service + "/" + c.Op + " (not modelled)"
		return nil
	}
	resource = scoped(perm, resource)
	allowed, why := s.decide(member, c.Service, perm, resource, project)
	verdict := "allowed"
	if !allowed {
		verdict = "denied"
	}
	s.record(call{member: member, service: c.Service, perm: perm, target: targetOf(c.Service, resource, project), sample: resource, project: project, verdict: verdict, reason: why})
	if mode := c.Hub.IAMMode(); mode != "soft" && mode != "strict" {
		return nil // recorded only: the console turns it into bindings
	}
	if allowed {
		c.IAM = "allow " + perm
		return nil
	}
	c.IAM = "deny " + perm + ": " + why
	host := services[c.Service].prefix
	if host == "datastore" {
		host = "firestore"
	}
	return core.Errorf(403, "PERMISSION_DENIED", "Permission '%s' denied on resource '//%s.googleapis.com/%s' (or it may not exist).", perm, host, resource)
}

// scoped is the resource whose policy governs perm: creating X on a resource that names X is
// governed by X's parent.
func scoped(perm, resource string) string {
	p := strings.Split(perm, ".")
	if len(p) == 3 && p[2] == "create" && lastColl(resource) == p[1] && strings.Count(resource, "/") >= 3 {
		return resource[:strings.LastIndex(resource[:strings.LastIndex(resource, "/")], "/")]
	}
	return resource
}

// resourceOf is the resource name a request is about: the one gRPC found in the request message,
// else the one in the REST path (projects/p/topics/t:publish is projects/p/topics/t; a collection
// call such as projects/p/topics is about its parent, projects/p).
func resourceOf(c *core.Ctx) string {
	switch {
	case c.Resource != "":
		return c.Resource
	case c.Service == "gcs":
		if _, b, ok := strings.Cut(c.Path, "/storage/v1/b/"); ok {
			b, _, _ = strings.Cut(b, "/")
			return "projects/_/buckets/" + b
		}
		if b, _, _ := strings.Cut(strings.TrimPrefix(c.Path, "/"), "/"); b != "" && !strings.Contains(c.Path, "/storage/v1/") {
			return "projects/_/buckets/" + b // XML API, path style
		}
		return ""
	}
	_, p, ok := strings.Cut(c.Path, "/projects/")
	if !ok {
		return ""
	}
	seg := strings.Split("projects/"+p, "/")
	seg[len(seg)-1], _, _ = strings.Cut(seg[len(seg)-1], ":")
	return strings.Join(seg[:len(seg)&^1], "/")
}

// projectOf is the project a resource belongs to ("" when pogi cannot tell).
func projectOf(hub *core.Hub, service, resource string) string {
	seg := strings.Split(resource, "/")
	if len(seg) < 2 || seg[0] != "projects" {
		return ""
	}
	if seg[1] != "_" {
		return seg[1]
	}
	if b, ok := hub.Service(service).(interface{ BucketProject(string) string }); ok && len(seg) >= 4 {
		return b.BucketProject(seg[3])
	}
	return ""
}

// targetOf is where a binding that allows a call on resource belongs: the resource itself when it
// holds a policy, else the project.
func targetOf(service, resource, project string) string {
	seg := strings.Split(resource, "/")
	for n := len(seg) &^ 1; n >= 4; n -= 2 {
		if slices.Contains(policyColls(service), seg[n-2]) {
			return strings.Join(seg[:n], "/")
		}
	}
	return "projects/" + project
}

// policiesFor are the policies that apply to a resource: its own and those of the resources
// above it, then the project's.
func (s *Service) policiesFor(service, resource, project string) []*iampb.Policy {
	var out []*iampb.Policy
	get := func(svc, res string) {
		if ps := policies(s.hub, svc); ps != nil {
			if p, err := ps.GetIamPolicy(context.Background(), &iampb.GetIamPolicyRequest{Resource: res}); err == nil {
				out = append(out, p)
			}
		}
	}
	if seg := strings.Split(resource, "/"); service == "gcs" {
		get(service, resource)
	} else {
		for n := 4; n <= len(seg); n += 2 {
			get(service, strings.Join(seg[:n], "/"))
		}
	}
	get("resourcemanager", "projects/"+project)
	return out
}

// decide reports whether member holds perm on resource, and why not.
func (s *Service) decide(member, service, perm, resource, project string) (bool, string) {
	for _, p := range s.policiesFor(service, resource, project) {
		for _, b := range p.Bindings {
			if (slices.Contains(b.Members, member) || slices.Contains(b.Members, "allAuthenticatedUsers") || slices.Contains(b.Members, "allUsers")) && s.grants(b.Role, perm) {
				return true, ""
			}
		}
	}
	return false, member + " has no role that grants " + perm
}

// grants reports whether a predefined or custom role includes the permission.
func (s *Service) grants(role, perm string) bool {
	if r := predefinedByName[role]; r != nil {
		_, ok := slices.BinarySearch(r.Perms, perm)
		return ok
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.st.Roles[role]
	return r != nil && !r.Deleted && slices.Contains(r.Perms, perm)
}
