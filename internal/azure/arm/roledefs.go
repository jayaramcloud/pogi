package arm

import (
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ---- role definitions ----

const (
	roleDefType   = "Microsoft.Authorization/roleDefinitions"
	roleAssignTyp = "Microsoft.Authorization/roleAssignments"
)

// roleDefinitions answers the built-in roles at any scope, and custom roles, which are kept in the
// generic store under the scope they were created at (so they persist with the rest of the state).
func (s *Service) roleDefinitions() Provider {
	scope := func(id ID) string {
		id.Namespace, id.Types, id.Names = "", nil, nil
		return id.String()
	}
	builtin := func(scope string, r Role) map[string]any {
		return roleDoc(scope+"/providers/"+roleDefType+"/"+r.GUID, r, "BuiltInRole", []string{"/"})
	}
	return Provider{Type: roleDefType,
		Get: func(c *core.Ctx, id ID) (map[string]any, error) {
			for _, r := range Roles {
				if strings.EqualFold(r.GUID, id.Name()) {
					return builtin(scope(id), r), nil
				}
			}
			for _, d := range s.Generic(roleDefType) {
				if strings.EqualFold(str(d["name"]), id.Name()) {
					return d, nil
				}
			}
			return nil, core.Errorf(http.StatusNotFound, "RoleDefinitionDoesNotExist", "The specified role definition with ID '%s' does not exist.", id.Name())
		},
		Put: func(c *core.Ctx, id ID, body map[string]any) (map[string]any, error) {
			p, _ := clone(body)["properties"].(map[string]any) // a round trip: callers inside pogi send Go slices
			if str(p["roleName"]) == "" {
				return nil, core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The role definition needs a roleName.")
			}
			for _, r := range Roles {
				if strings.EqualFold(r.GUID, id.Name()) {
					return nil, core.Errorf(http.StatusBadRequest, "RoleDefinitionCannotBeUpdated", "A built-in role definition cannot be updated.")
				}
			}
			now := time.Now().UTC().Format(time.RFC3339)
			props := map[string]any{"roleName": p["roleName"], "type": "CustomRole", "description": str(p["description"]),
				"assignableScopes": p["assignableScopes"], "permissions": p["permissions"], "createdOn": now, "updatedOn": now}
			if props["assignableScopes"] == nil {
				props["assignableScopes"] = []any{scope(id)}
			}
			if props["permissions"] == nil {
				props["permissions"] = []any{}
			}
			doc := map[string]any{"id": id.String(), "name": id.Name(), "type": roleDefType, "properties": props}
			s.mu.Lock()
			s.st.Generic[strings.ToLower(id.String())] = doc
			s.mu.Unlock()
			return doc, nil
		},
		Delete: func(c *core.Ctx, id ID) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			for k, d := range s.st.Generic {
				if strings.EqualFold(str(d["type"]), roleDefType) && strings.EqualFold(str(d["name"]), id.Name()) {
					delete(s.st.Generic, k)
				}
			}
			return nil
		},
		List: func(c *core.Ctx, parent ID) ([]map[string]any, error) {
			var out []map[string]any
			for _, r := range Roles {
				out = append(out, builtin(scope(parent), r))
			}
			return append(out, s.Generic(roleDefType)...), nil
		}}
}

func roleDoc(id string, r Role, kind string, scopes []string) map[string]any {
	l := func(a []string) []string {
		if a == nil {
			return []string{}
		}
		return a
	}
	return map[string]any{"id": id, "name": r.GUID, "type": roleDefType, "properties": map[string]any{"roleName": r.Name, "type": kind,
		"description": r.Desc, "assignableScopes": scopes, "permissions": []any{map[string]any{"actions": l(r.Actions), "notActions": l(r.NotActions),
			"dataActions": l(r.Data), "notDataActions": l(r.NotData)}}}}
}

// ---- for the authorizer and the console ----

// Generic lists the stored resources of a type that has no Provider of its own (role assignments),
// and custom role definitions.
func (s *Service) Generic(typ string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, k := range core.SortedKeys(s.st.Generic) {
		if d := s.st.Generic[k]; strings.EqualFold(str(d["type"]), typ) {
			out = append(out, d)
		}
	}
	return out
}

// Role finds a role definition by its GUID, built-in or custom. A custom role with several
// permission blocks reads as their union.
func (s *Service) Role(guid string) (Role, bool) {
	for _, r := range Roles {
		if strings.EqualFold(r.GUID, guid) {
			return r, true
		}
	}
	for _, d := range s.Generic(roleDefType) {
		if strings.EqualFold(str(d["name"]), guid) {
			return roleOf(d), true
		}
	}
	return Role{}, false
}

func roleOf(d map[string]any) Role {
	p, _ := d["properties"].(map[string]any)
	r := Role{GUID: str(d["name"]), Name: str(p["roleName"]), Desc: str(p["description"])}
	strs := func(v any) (out []string) {
		l, _ := v.([]any)
		for _, x := range l {
			out = append(out, str(x))
		}
		return out
	}
	perms, _ := p["permissions"].([]any)
	for _, x := range perms {
		m, _ := x.(map[string]any)
		r.Actions, r.NotActions = append(r.Actions, strs(m["actions"])...), append(r.NotActions, strs(m["notActions"])...)
		r.Data, r.NotData = append(r.Data, strs(m["dataActions"])...), append(r.NotData, strs(m["notDataActions"])...)
	}
	return r
}

// SubscriptionIDs lists the subscriptions.
func (s *Service) SubscriptionIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return core.SortedKeys(s.st.Subs)
}

// Put stores the resource at path as a PUT to it would: by its type's Provider, or in the generic
// store (role assignments, at any scope, live there). The console uses it to attach roles.
func (s *Service) Put(c *core.Ctx, path string, body map[string]any) (map[string]any, error) {
	g, id, p := s.locate(path)
	if p != nil {
		return p.Put(c, id, body)
	}
	doc := genericDoc(g, body)
	s.mu.Lock()
	s.st.Generic[g.key()] = doc
	s.mu.Unlock()
	return doc, nil
}

// Delete removes the resource at path, as a DELETE to it would.
func (s *Service) Delete(c *core.Ctx, path string) error {
	g, id, p := s.locate(path)
	if p != nil {
		return p.Delete(c, id)
	}
	s.deleteGeneric(g.key())
	return nil
}

// locate reads a resource path of any scope: the generic id of its innermost provider segment, and
// its Provider when the path is a plain resource ID of a type that has one.
func (s *Service) locate(path string) (gid, ID, *Provider) {
	seg := strings.Split(strings.Trim(path, "/"), "/")
	i := len(seg) - 1
	for ; i > 0 && !strings.EqualFold(seg[i], "providers"); i-- {
	}
	typ := ""
	if i >= 0 && i+2 < len(seg) {
		typ = seg[i+1]
		for j := i + 2; j < len(seg); j += 2 {
			typ += "/" + seg[j]
		}
	}
	g := gid{path: path, typ: typ, name: seg[len(seg)-1]}
	id, ok := ParseID(path)
	if !ok || strings.Count(strings.ToLower(path), "/providers/") != 1 {
		return g, id, nil
	}
	if p := Lookup(s.h, id.Type()); p != nil && p.Put != nil && p.Delete != nil {
		return g, id, p
	}
	return g, id, nil
}
