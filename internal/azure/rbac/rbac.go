// Package rbac is Azure role-based access control for pogi: with `pogi start --iam soft|strict`
// every request carrying a bearer token of a service principal or managed identity that the user
// created is checked against its role assignments (a role definition's actions and notActions for
// Resource Manager calls, dataActions and notDataActions for the data plane). pogi's default
// credentials, requests without a token, and shared-key or SAS requests are never checked.
//
// ponytail: no deny assignments, no ABAC conditions, no management groups, no groups membership,
// and a custom role with several permission blocks reads as their union.
package rbac

import (
	"cmp"
	"net/http"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/entra"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Azure authorizer.
type Service struct {
	h    *core.Hub
	mu   sync.Mutex
	rec  map[string]*Principal
	prev map[string]map[string]any // role ID -> the custom definition an attach replaced
}

// services are the authorizers by hub: rbac is no service of its own, it comes up with Entra ID.
var services sync.Map

// New creates the authorizer and makes it the hub's Azure authorizer.
func New(h *core.Hub) *Service {
	s := &Service{h: h, prev: map[string]map[string]any{}}
	h.Authorize[core.Azure] = s.authorize
	services.Store(h, s)
	return s
}

func (s *Service) arm() *arm.Service {
	a, _ := s.h.Service("arm").(*arm.Service)
	return a
}

// authorize is the hub's Authorize hook. It runs in every mode: off records the request and allows it.
func (s *Service) authorize(c *core.Ctx) *core.Error {
	who, ok := entra.CallerOf(s.h, "Bearer "+c.Bearer)
	if !ok || who.Default || s.arm() == nil {
		return nil
	}
	a, ok := s.actionOf(c)
	if !ok || (!entra.Known(s.h, who) && !s.assigned(who.OID)) {
		return nil
	}
	c.Principal = who.OID
	verdict, reason := s.decide(who.OID, a)
	s.record(who, a, verdict, reason)
	if mode := c.Hub.IAMMode(); mode != "soft" && mode != "strict" {
		return nil // recorded only: the console turns it into a role
	}
	if verdict == "allowed" {
		c.IAM = "allow " + a.Name
		return nil
	}
	c.IAM = "deny " + a.Name + ": " + reason
	code, msg := "AuthorizationFailed", "The client '"+who.OID+"' with object id '"+who.OID+"' does not have authorization to perform action '"+a.Name+
		"' over scope '"+a.Scope+"' or the scope is invalid. If access was recently granted, please refresh your credentials."
	switch c.Service {
	case "azblob", "azqueue", "aztable":
		code, msg = "AuthorizationPermissionMismatch", "This request is not authorized to perform this operation using this permission."
	case "keyvault":
		code, msg = "Forbidden", "Caller is not authorized to perform action on resource. Action: '"+a.Name+"'; Principal: "+who.OID+"; "+reason
	case "servicebus":
		msg = "Unauthorized access. '" + strings.TrimPrefix(a.Name, "Microsoft.ServiceBus/namespaces/messages/") + "' claim(s) are required to perform this operation. Resource: '" + a.Scope + "'."
	}
	return core.Errorf(http.StatusForbidden, code, "%s", msg)
}

// decide applies the principal's role assignments to one action: allowed or implicitDeny.
func (s *Service) decide(oid string, a act) (string, string) {
	for _, r := range s.rolesFor(oid, a.Scope) {
		allow, not := r.Actions, r.NotActions
		if a.Data {
			allow, not = r.Data, r.NotData
		}
		if matchAny(allow, a.Name) && !matchAny(not, a.Name) {
			return "allowed", "role " + r.Name
		}
	}
	return "implicitDeny", "no role assignment at " + a.Scope + " allows " + a.Name
}

// assignment is a role assignment: where, to whom, which role.
type assignment struct{ scope, principal, role string }

const assignMarker = "/providers/microsoft.authorization/roleassignments/"

func (s *Service) assignments() []assignment {
	var out []assignment
	for _, d := range s.arm().Generic("Microsoft.Authorization/roleAssignments") {
		id, _ := d["id"].(string)
		p, _ := d["properties"].(map[string]any)
		i := strings.Index(strings.ToLower(id), assignMarker)
		def, _ := p["roleDefinitionId"].(string)
		who, _ := p["principalId"].(string)
		if i >= 0 && who != "" {
			out = append(out, assignment{cmp.Or(id[:i], "/"), who, def[strings.LastIndexByte(def, '/')+1:]})
		}
	}
	return out
}

func (s *Service) assigned(oid string) bool {
	for _, a := range s.assignments() {
		if strings.EqualFold(a.principal, oid) {
			return true
		}
	}
	return false
}

// rolesFor are the roles the principal holds over a scope: assigned there or at an ancestor.
func (s *Service) rolesFor(oid, scope string) []arm.Role {
	var out []arm.Role
	for _, a := range s.assignments() {
		if strings.EqualFold(a.principal, oid) && covers(a.scope, scope) {
			if r, ok := s.arm().Role(a.role); ok {
				out = append(out, r)
			}
		}
	}
	return out
}

// covers reports whether an assignment at scope reaches resource.
func covers(scope, resource string) bool {
	scope, resource = strings.TrimRight(strings.ToLower(scope), "/"), strings.ToLower(resource)
	return scope == "" || resource == scope || strings.HasPrefix(resource, scope+"/")
}

func matchAny(patterns []string, action string) bool {
	for _, p := range patterns {
		if glob(p, action) {
			return true
		}
	}
	return false
}

// glob matches an action against a pattern in which * stands for any text, ignoring case.
func glob(pattern, s string) bool {
	parts := strings.Split(strings.ToLower(pattern), "*")
	s = strings.ToLower(s)
	if len(parts) == 1 {
		return parts[0] == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for _, p := range parts[1 : len(parts)-1] {
		i := strings.Index(s, p)
		if i < 0 {
			return false
		}
		s = s[i+len(p):]
	}
	return strings.HasSuffix(s, parts[len(parts)-1])
}
