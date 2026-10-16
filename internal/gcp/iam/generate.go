package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Binding grants a role to the principal on a resource: the project, or one resource that holds a policy.
type Binding struct {
	Role     string
	Resource string // projects/p, or projects/p/topics/t
	Service  string // the pogi service that keeps the policy
	Covers   []string
}

// Plan is what the recorded calls need: the smallest set of predefined roles that covers them,
// or one custom role with exactly their permissions.
type Plan struct {
	Member    string
	Bindings  []Binding
	Perms     []string // everything recorded and modelled
	Uncovered []string // permissions no predefined role carries
	Custom    string   // the custom role's name when Bindings use one, else ""
}

func isProject(resource string) bool { return strings.Count(resource, "/") == 1 }

// Plan picks roles greedily: each round takes the role whose permissions are the most used by the
// recorded calls, so a role with few extras (publisher) goes before a big one (editor). A role used
// on one resource only is bound there, otherwise at the project. With custom, it is one role
// projects/<p>/roles/pogi_<name> with exactly the permissions.
func (p Principal) Plan(custom bool) Plan {
	pl := Plan{Member: p.Member}
	uses := map[string][]Call{} // perm → calls
	for _, c := range p.Actions {
		if c.Modelled() {
			uses[c.Perm] = append(uses[c.Perm], c)
		}
	}
	pl.Perms = sortedKeys(uses)
	if custom && len(pl.Perms) > 0 {
		project := uses[pl.Perms[0]][0].Project
		pl.Custom = "projects/" + project + "/roles/pogi_" + regexp.MustCompile(`[^A-Za-z0-9_.]`).ReplaceAllString(p.Member[strings.IndexByte(p.Member, ':')+1:strings.IndexAny(p.Member, "@")], "_")
		pl.Bindings = []Binding{{Role: pl.Custom, Resource: "projects/" + project, Service: "resourcemanager", Covers: pl.Perms}}
		return pl
	}
	left := map[string]bool{}
	for _, perm := range pl.Perms {
		left[perm] = true
	}
	for len(left) > 0 {
		var best *role
		var got []string
		for _, r := range predefined {
			if r.Name == "roles/owner" || r.Name == "roles/editor" || r.Name == "roles/viewer" {
				continue
			}
			var hit []string
			for _, perm := range r.Perms {
				if left[perm] {
					hit = append(hit, perm)
				}
			}
			// the best share of its permissions that was asked for; more covered, then name, break ties
			if len(hit) > 0 && (best == nil || len(hit)*len(best.Perms) > len(got)*len(r.Perms) ||
				len(hit)*len(best.Perms) == len(got)*len(r.Perms) && len(hit) > len(got)) {
				best, got = r, hit
			}
		}
		if best == nil {
			pl.Uncovered = sortedKeys(left)
			break
		}
		byProject := map[string][]Call{}
		for _, perm := range got {
			delete(left, perm)
			for _, c := range uses[perm] {
				byProject[c.Project] = append(byProject[c.Project], c)
			}
		}
		for _, project := range sortedKeys(byProject) {
			cs := byProject[project]
			b := Binding{Role: best.Name, Resource: "projects/" + project, Service: "resourcemanager", Covers: got}
			if first := cs[0]; !isProject(first.Resource) && !slices.ContainsFunc(cs, func(c Call) bool { return c.Resource != first.Resource }) {
				b.Resource, b.Service = first.Resource, first.Service // every use is on one resource
			}
			pl.Bindings = append(pl.Bindings, b)
		}
	}
	return pl
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Short is a resource name without its project: "topics/orders", or "project pogi".
func Short(resource string) string {
	if isProject(resource) {
		return "project " + strings.TrimPrefix(resource, "projects/")
	}
	if _, r, ok := strings.Cut(strings.TrimPrefix(resource, "projects/"), "/"); ok {
		return r
	}
	return resource
}

// Diff describes what attaching would add: "roles/pubsub.publisher on topics/orders". Bindings
// the policy already holds are left out.
func (s *Service) Diff(pl Plan) []string {
	var out []string
	if pl.Custom != "" {
		out = append(out, "custom role "+pl.Custom+": "+fmt.Sprint(len(pl.Perms))+" permissions")
	}
	for _, b := range pl.Bindings {
		if !s.has(b, pl.Member) {
			out = append(out, b.Role+" on "+Short(b.Resource))
		}
	}
	return out
}

func (s *Service) has(b Binding, member string) bool {
	ps := policies(s.hub, b.Service)
	if ps == nil {
		return false
	}
	pol, err := ps.GetIamPolicy(context.Background(), &iampb.GetIamPolicyRequest{Resource: b.Resource})
	return err == nil && slices.ContainsFunc(pol.Bindings, func(x *iampb.Binding) bool { return x.Role == b.Role && slices.Contains(x.Members, member) })
}

// The shell and Terraform to make the bindings by hand: Short, tfType and gcloud by collection.
var snippets = map[string]struct{ tf, attr, gcloud string }{
	"topics":        {"google_pubsub_topic_iam_member", "topic", "gcloud pubsub topics add-iam-policy-binding %s"},
	"subscriptions": {"google_pubsub_subscription_iam_member", "subscription", "gcloud pubsub subscriptions add-iam-policy-binding %s"},
	"buckets":       {"google_storage_bucket_iam_member", "bucket", "gcloud storage buckets add-iam-policy-binding gs://%s"},
	"secrets":       {"google_secret_manager_secret_iam_member", "secret_id", "gcloud secrets add-iam-policy-binding %s"},
	"cryptoKeys":    {"google_kms_crypto_key_iam_member", "crypto_key_id", ""},
	"keyRings":      {"google_kms_key_ring_iam_member", "key_ring_id", ""},
}

func (b Binding) project() string { return strings.Split(b.Resource, "/")[1] }

// Gcloud is the commands that make the bindings.
func (pl Plan) Gcloud() string {
	var sb strings.Builder
	if pl.Custom != "" {
		_, id, _ := strings.Cut(pl.Custom, "/roles/")
		fmt.Fprintf(&sb, "gcloud iam roles create %s --project=%s --permissions=%s\n", id, strings.Split(pl.Custom, "/")[1], strings.Join(pl.Perms, ","))
	}
	for _, b := range pl.Bindings {
		flags := fmt.Sprintf(" --member=%s --role=%s", pl.Member, b.Role)
		name := b.Resource[strings.LastIndex(b.Resource, "/")+1:]
		if sn, ok := snippets[lastColl(b.Resource)]; ok && !isProject(b.Resource) && sn.gcloud != "" {
			fmt.Fprintf(&sb, sn.gcloud+" --project=%s%s\n", name, b.project(), flags)
		} else if isProject(b.Resource) {
			fmt.Fprintf(&sb, "gcloud projects add-iam-policy-binding %s%s\n", b.project(), flags)
		} else {
			fmt.Fprintf(&sb, "# %s on %s: set it with setIamPolicy on the resource\n", b.Role, b.Resource)
		}
	}
	return sb.String()
}

// Terraform is the same as google_*_iam_member resources.
func (pl Plan) Terraform() string {
	var sb strings.Builder
	if pl.Custom != "" {
		_, id, _ := strings.Cut(pl.Custom, "/roles/")
		fmt.Fprintf(&sb, "resource \"google_project_iam_custom_role\" %q {\n  project     = %q\n  role_id     = %q\n  title       = %q\n  permissions = [%s]\n}\n\n",
			id, strings.Split(pl.Custom, "/")[1], id, id, `"`+strings.Join(pl.Perms, `", "`)+`"`)
	}
	for i, b := range pl.Bindings {
		role := fmt.Sprintf("%q", b.Role)
		if b.Role == pl.Custom {
			_, id, _ := strings.Cut(pl.Custom, "/roles/")
			role = "google_project_iam_custom_role." + id + ".name"
		}
		name := fmt.Sprintf("pogi_%d", i+1)
		switch sn, ok := snippets[lastColl(b.Resource)]; {
		case isProject(b.Resource):
			fmt.Fprintf(&sb, "resource \"google_project_iam_member\" %q {\n  project = %q\n  role    = %s\n  member  = %q\n}\n\n", name, b.project(), role, pl.Member)
		case ok:
			v := b.Resource
			if sn.attr == "bucket" {
				v = b.Resource[strings.LastIndex(b.Resource, "/")+1:]
			}
			fmt.Fprintf(&sb, "resource %q %q {\n  %s = %q\n  role = %s\n  member = %q\n}\n\n", sn.tf, name, sn.attr, v, role, pl.Member)
		default:
			fmt.Fprintf(&sb, "# %s on %s: no google_*_iam_member resource is generated for it\n\n", b.Role, b.Resource)
		}
	}
	return strings.TrimRight(sb.String(), "\n") + "\n"
}

// Change is one policy Attach replaced, kept to restore it.
type Change struct {
	Service, Resource string
	Before            json.RawMessage // the policy as it was, protojson
}

// Attachment is the result of Attach, enough to undo it.
type Attachment struct {
	Member  string
	Changes []Change
	Role    string   // the custom role Attach made or updated
	Created bool     // Attach made the role
	Prev    []string // the role's permissions before, when it existed
}

// Attach makes the plan real through the services' own SetIamPolicy: the member joins the role's
// binding on each resource. With custom, the role is created (or updated) first.
func Attach(hub *core.Hub, member string, custom bool) (Attachment, error) {
	s, _ := hub.Service("gcpiam").(*Service)
	if s == nil {
		return Attachment{}, fmt.Errorf("the gcpiam service is not enabled")
	}
	for _, p := range s.Recorded() {
		if p.Member == member {
			return s.attach(p.Plan(custom))
		}
	}
	return Attachment{}, fmt.Errorf("nothing recorded for %s yet", member)
}

func (s *Service) attach(pl Plan) (Attachment, error) {
	att := Attachment{Member: pl.Member}
	if len(pl.Bindings) == 0 {
		return att, fmt.Errorf("nothing to attach")
	}
	ctx := context.Background()
	if pl.Custom != "" {
		parent, id, _ := strings.Cut(pl.Custom, "/roles/")
		att.Role = pl.Custom
		if old, err := s.GetRole(ctx, &adminpb.GetRoleRequest{Name: pl.Custom}); err == nil {
			att.Prev = old.IncludedPermissions
			if old.Deleted {
				s.UndeleteRole(ctx, &adminpb.UndeleteRoleRequest{Name: pl.Custom})
			}
			if _, err := s.UpdateRole(ctx, &adminpb.UpdateRoleRequest{Name: pl.Custom, Role: &adminpb.Role{IncludedPermissions: pl.Perms}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"included_permissions"}}}); err != nil {
				return att, err
			}
		} else {
			att.Created = true
			if _, err := s.CreateRole(ctx, &adminpb.CreateRoleRequest{Parent: parent, RoleId: id, Role: &adminpb.Role{Title: id, Description: "Least-privilege role generated by pogi from recorded calls", IncludedPermissions: pl.Perms, Stage: adminpb.Role_GA}}); err != nil {
				return att, err
			}
		}
	}
	for _, b := range pl.Bindings {
		ps := policies(s.hub, b.Service)
		if ps == nil {
			return att, s.fail(att, fmt.Errorf("the %s service is not enabled", b.Service))
		}
		pol, err := ps.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: b.Resource})
		if err != nil {
			return att, s.fail(att, err)
		}
		before, _ := protojson.Marshal(pol)
		if i := slices.IndexFunc(pol.Bindings, func(x *iampb.Binding) bool { return x.Role == b.Role && x.Condition == nil }); i >= 0 {
			if slices.Contains(pol.Bindings[i].Members, pl.Member) {
				continue
			}
			pol.Bindings[i].Members = append(pol.Bindings[i].Members, pl.Member)
		} else {
			pol.Bindings = append(pol.Bindings, &iampb.Binding{Role: b.Role, Members: []string{pl.Member}})
		}
		if _, err := ps.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: b.Resource, Policy: pol}); err != nil {
			return att, s.fail(att, err)
		}
		att.Changes = append(att.Changes, Change{b.Service, b.Resource, before})
	}
	return att, nil
}

// fail undoes what a half-done Attach did and returns err.
func (s *Service) fail(att Attachment, err error) error {
	s.undo(att)
	return err
}

// Undo reverses Attach: each policy goes back to what it was (with the current etag, so it is not
// refused), and the custom role is deleted or gets its permissions back.
func Undo(hub *core.Hub, att Attachment) error {
	s, _ := hub.Service("gcpiam").(*Service)
	if s == nil {
		return fmt.Errorf("the gcpiam service is not enabled")
	}
	return s.undo(att)
}

func (s *Service) undo(att Attachment) error {
	ctx := context.Background()
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	for i := len(att.Changes) - 1; i >= 0; i-- {
		ch := att.Changes[i]
		ps := policies(s.hub, ch.Service)
		if ps == nil {
			continue
		}
		old := &iampb.Policy{}
		if err := protojson.Unmarshal(ch.Before, old); err != nil {
			keep(err)
			continue
		}
		cur, err := ps.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: ch.Resource})
		if err != nil {
			keep(err)
			continue
		}
		old.Etag = cur.Etag
		_, err = ps.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: ch.Resource, Policy: old})
		if err != nil {
			keep(err)
		}
	}
	switch {
	case att.Created:
		_, err := s.DeleteRole(ctx, &adminpb.DeleteRoleRequest{Name: att.Role})
		keep(err)
	case att.Role != "":
		_, err := s.UpdateRole(ctx, &adminpb.UpdateRoleRequest{Name: att.Role, Role: &adminpb.Role{IncludedPermissions: att.Prev}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"included_permissions"}}})
		keep(err)
	}
	return first
}

// Preview is the plan for a recorded principal and what attaching it would add.
func Preview(hub *core.Hub, member string, custom bool) (pl Plan, diff []string, ok bool) {
	s, _ := hub.Service("gcpiam").(*Service)
	if s == nil {
		return pl, nil, false
	}
	for _, p := range s.Recorded() {
		if p.Member == member {
			pl = p.Plan(custom)
			return pl, s.Diff(pl), true
		}
	}
	return pl, nil, false
}
