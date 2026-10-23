package arm

import (
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// gid is a resource or collection path with its type, for the generic store.
type gid struct {
	path, typ, name string
}

func (g gid) key() string { return strings.ToLower(g.path) }

// resource serves a provider path: /.../providers/<Namespace>/<type>/<name>...
// by the Provider of the type, or by the generic store when there is none.
func (s *Service) resource(k *call, path string) (int, any, error) {
	if low := strings.ToLower(path); strings.Count(low, "/providers/") > 1 || strings.HasPrefix(low, "/providers/") {
		return s.extension(k, path)
	}
	id, ok := ParseID(path)
	if !ok || id.Namespace == "" || len(id.Types) == 0 {
		return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", path)
	}
	if id.ResourceGroup != "" && !s.hasGroup(id.Subscription, id.ResourceGroup) {
		return 0, nil, groupNotFound(id.ResourceGroup)
	}
	m := k.r.Method
	if len(id.Names) == len(id.Types) {
		return s.item(k, id)
	}
	if m == http.MethodPost { // .../<name>/<action>, or Namespace-level .../<action>
		if len(id.Types) == 1 {
			return s.nsAction(k, id)
		}
		n := len(id.Types) - 1
		res := id
		res.Types = id.Types[:n]
		return s.action(k, res, id.Types[n])
	}
	if m != http.MethodGet {
		return 0, nil, methodNotAllowed(m, id.Type())
	}
	parent := id.Parent()
	if len(id.Types) > 1 && !s.exists(k.c, parent) {
		return 0, nil, NotFound(parent)
	}
	if p := Lookup(s.h, id.Type()); p != nil && p.Doc != nil {
		doc, err := p.Doc(k.c, parent, k.r.URL.Query())
		return http.StatusOK, doc, err
	}
	items, err := s.listType(k.c, id, parent)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, list(filter(items, k.r.URL.Query().Get("$filter"))...), nil
}

// parentNotFound is ARM's 404 for a PUT of a child whose parent does not exist.
func parentNotFound(parent ID) error {
	return core.Errorf(http.StatusNotFound, "ParentResourceNotFound", "Can not perform requested operation on nested resource. Parent resource '%s' not found.", parent.Name())
}

func methodNotAllowed(m, typ string) error {
	return core.Errorf(http.StatusMethodNotAllowed, "MethodNotAllowed", "The method %s is not allowed for resource type '%s'.", m, typ)
}

// item serves GET, HEAD, PUT, PATCH and DELETE of one resource.
func (s *Service) item(k *call, id ID) (int, any, error) {
	c, p := k.c, Lookup(s.h, id.Type())
	cur, getErr := s.get(c, id, p)
	switch k.r.Method {
	case http.MethodGet, http.MethodHead:
		if getErr != nil {
			return 0, nil, getErr
		}
		if k.r.Method == http.MethodHead {
			return http.StatusNoContent, nil, nil
		}
		return http.StatusOK, cur, nil
	case http.MethodPut, http.MethodPatch:
		if err := s.checkType(id.Namespace, id.Type()); err != nil {
			return 0, nil, err
		}
		in := k.payload()
		if k.r.Method == http.MethodPatch {
			if getErr != nil {
				return 0, nil, getErr
			}
			in = merge(clone(cur), in)
		} else if len(id.Types) > 1 && !s.exists(c, id.Parent()) {
			if p != nil && p.ParentNotFound {
				return 0, nil, parentNotFound(id.Parent())
			}
			return 0, nil, NotFound(id.Parent())
		}
		out, err := s.put(c, id, p, in)
		if err != nil {
			return 0, nil, err
		}
		status, created := http.StatusOK, http.StatusCreated
		if p != nil && p.Created != 0 {
			created = p.Created
		}
		if getErr != nil {
			status = created
		}
		if p != nil && (p.Async || p.AsyncPut) && k.r.Method == http.MethodPut { // a PATCH (update tags) is a plain call
			k.async(id.Subscription)
			status = created
		}
		return status, out, nil
	case http.MethodDelete:
		if getErr != nil {
			return http.StatusNoContent, nil, nil
		}
		if err := s.del(c, id, p); err != nil {
			return 0, nil, err
		}
		if p != nil && p.Deleted != 0 {
			return p.Deleted, nil, nil
		}
		if p != nil && (p.Async || p.AsyncDelete) {
			k.async(id.Subscription)
			return http.StatusAccepted, nil, nil
		}
		return http.StatusOK, nil, nil
	case http.MethodPost:
		return 0, nil, methodNotAllowed("POST", id.Type())
	}
	return 0, nil, methodNotAllowed(k.r.Method, id.Type())
}

func (s *Service) get(c *core.Ctx, id ID, p *Provider) (map[string]any, error) {
	if p != nil {
		if p.Get == nil {
			return nil, methodNotAllowed("GET", id.Type())
		}
		return p.Get(c, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if doc, ok := s.st.Generic[strings.ToLower(id.String())]; ok {
		return doc, nil
	}
	return nil, NotFound(id)
}

func (s *Service) exists(c *core.Ctx, id ID) bool {
	if id.Namespace == "" { // the resource group
		return s.hasGroup(id.Subscription, id.ResourceGroup)
	}
	_, err := s.get(c, id, Lookup(s.h, id.Type()))
	return err == nil
}

func (s *Service) put(c *core.Ctx, id ID, p *Provider, body map[string]any) (map[string]any, error) {
	if p != nil {
		if p.Put == nil {
			return nil, methodNotAllowed("PUT", id.Type())
		}
		return p.Put(c, id, body)
	}
	doc := genericDoc(gid{path: id.String(), typ: id.Type(), name: id.Name()}, body)
	if strings.EqualFold(id.Type(), "Microsoft.Resources/deployments") {
		s.checkTemplate(doc)
	}
	s.mu.Lock()
	s.st.Generic[strings.ToLower(id.String())] = doc
	s.mu.Unlock()
	return doc, nil
}

func (s *Service) del(c *core.Ctx, id ID, p *Provider) error {
	if p != nil {
		if p.Delete == nil {
			return methodNotAllowed("DELETE", id.Type())
		}
		return p.Delete(c, id)
	}
	s.deleteGeneric(strings.ToLower(id.String()))
	return nil
}

// deleteGeneric removes a generic resource and its children.
func (s *Service) deleteGeneric(key string) {
	s.mu.Lock()
	for k := range s.st.Generic {
		if k == key || strings.HasPrefix(k, key+"/") {
			delete(s.st.Generic, k)
		}
	}
	s.mu.Unlock()
}

// listType lists the resources of a collection path: a Provider's, or the generic ones.
func (s *Service) listType(c *core.Ctx, id, parent ID) ([]map[string]any, error) {
	if p := Lookup(s.h, id.Type()); p != nil {
		if p.List == nil {
			return nil, methodNotAllowed("GET", id.Type())
		}
		return p.List(c, parent)
	}
	prefix := strings.ToLower(parent.String()) + "/"
	return s.listGeneric(prefix, id.Type()), nil
}

func (s *Service) listGeneric(prefix, typ string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, key := range core.SortedKeys(s.st.Generic) {
		if doc := s.st.Generic[key]; strings.HasPrefix(key, prefix) && strings.EqualFold(str(doc["type"]), typ) {
			out = append(out, doc)
		}
	}
	return out
}

// action serves a POST on a resource.
func (s *Service) action(k *call, id ID, name string) (int, any, error) {
	p := Lookup(s.h, id.Type())
	if _, err := s.get(k.c, id, p); err != nil {
		return 0, nil, err
	}
	if p == nil {
		return http.StatusOK, genericAction(id.Type(), name), nil
	}
	for n, f := range p.Actions {
		if strings.EqualFold(n, name) {
			out, err := f(k.c, id, k.payload())
			is := func(l []string) bool {
				return slices.ContainsFunc(l, func(a string) bool { return strings.EqualFold(a, name) })
			}
			if err == nil && is(p.LongActions) {
				k.async(id.Subscription)
				return http.StatusAccepted, out, nil
			}
			if err == nil && is(p.NoContent) {
				return http.StatusNoContent, nil, nil
			}
			return http.StatusOK, out, err
		}
	}
	return 0, nil, core.Errorf(http.StatusNotFound, "InvalidActionType", "The action '%s' is not supported for resource type '%s'.", name, id.Type())
}

// nsAction serves a POST at namespace level, e.g. Microsoft.Storage/checkNameAvailability.
func (s *Service) nsAction(k *call, id ID) (int, any, error) {
	name := id.Types[0]
	if p := Lookup(s.h, id.Namespace); p != nil {
		for n, f := range p.Actions {
			if strings.EqualFold(n, name) {
				out, err := f(k.c, id, k.payload())
				return http.StatusOK, out, err
			}
		}
	}
	return http.StatusOK, genericAction(id.Namespace, name), nil
}

// genericAction answers a POST nobody provides: name checks pass, validations succeed.
// ponytail: canned answers, no template or request validation.
func genericAction(typ, name string) any {
	switch strings.ToLower(name) {
	case "checknameavailability":
		return map[string]any{"nameAvailable": true}
	case "validate":
		return map[string]any{"properties": map[string]any{"provisioningState": "Succeeded", "validatedResources": []any{}}}
	case "whatif":
		return map[string]any{"status": "Succeeded", "properties": map[string]any{"changes": []any{}}}
	case "exporttemplate":
		return map[string]any{"template": map[string]any{}}
	case "cancel":
		return nil
	}
	return map[string]any{}
}

// extension serves resources scoped by another resource, or by the tenant:
// .../providers/<A>/<x>/<y>/providers/<B>/<z>/<w>. Generic only.
// ponytail: no parent check, no Providers; role assignments and locks are all that live here.
func (s *Service) extension(k *call, path string) (int, any, error) {
	seg := strings.Split(strings.Trim(path, "/"), "/")
	i := len(seg) - 1
	for ; i >= 0 && !strings.EqualFold(seg[i], "providers"); i-- {
	}
	if i < 0 || i+2 >= len(seg) {
		return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", path)
	}
	ns, rest := seg[i+1], seg[i+2:]
	typ := ns
	for j := 0; j < len(rest); j += 2 {
		typ += "/" + rest[j]
	}
	m := k.r.Method
	if p := Lookup(s.h, typ); p != nil && p.Get != nil {
		return s.scoped(k, path, p, len(rest)%2 == 1)
	}
	if len(rest)%2 == 1 && m == http.MethodPost { // .../<name>/<action>
		return http.StatusOK, genericAction(typ, rest[len(rest)-1]), nil
	}
	if len(rest)%2 == 1 { // a collection
		if m != http.MethodGet {
			return 0, nil, methodNotAllowed(m, typ)
		}
		return http.StatusOK, list(filter(s.listGeneric(strings.ToLower(path)+"/", typ), k.r.URL.Query().Get("$filter"))...), nil
	}
	g := gid{path: path, typ: typ, name: rest[len(rest)-1]}
	s.mu.Lock()
	cur, ok := s.st.Generic[g.key()]
	s.mu.Unlock()
	switch m {
	case http.MethodGet, http.MethodHead:
		if !ok {
			return 0, nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The Resource '%s/%s' was not found.", typ, g.name)
		}
		if m == http.MethodHead {
			return http.StatusNoContent, nil, nil
		}
		return http.StatusOK, cur, nil
	case http.MethodPut, http.MethodPatch:
		in := k.payload()
		if m == http.MethodPatch {
			if !ok {
				return 0, nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The Resource '%s/%s' was not found.", typ, g.name)
			}
			in = merge(clone(cur), in)
		}
		doc := genericDoc(g, in)
		s.mu.Lock()
		s.st.Generic[g.key()] = doc
		s.mu.Unlock()
		if ok {
			return http.StatusOK, doc, nil
		}
		return http.StatusCreated, doc, nil
	case http.MethodDelete:
		if !ok {
			return http.StatusNoContent, nil, nil
		}
		s.deleteGeneric(g.key())
		return http.StatusOK, nil, nil
	}
	return 0, nil, methodNotAllowed(m, typ)
}

// scoped serves a resource of a type that has a Provider and is scoped by another resource, such as
// Microsoft.EventGrid/eventSubscriptions on a topic. ParseID reads such a path as a type chain with a
// "providers" link in it; the Provider gets that ID and works out the scope from it.
func (s *Service) scoped(k *call, path string, p *Provider, collection bool) (int, any, error) {
	id, ok := ParseID(path)
	if !ok {
		return 0, nil, core.Errorf(http.StatusNotFound, "InvalidRequestUri", "No route registered for '%s'.", path)
	}
	m := k.r.Method
	if collection {
		if m != http.MethodGet || p.List == nil {
			return 0, nil, methodNotAllowed(m, p.Type)
		}
		items, err := p.List(k.c, id)
		return http.StatusOK, list(items...), err
	}
	cur, getErr := p.Get(k.c, id)
	switch m {
	case http.MethodGet, http.MethodHead:
		if getErr != nil {
			return 0, nil, getErr
		}
		if m == http.MethodHead {
			return http.StatusNoContent, nil, nil
		}
		return http.StatusOK, cur, nil
	case http.MethodPut, http.MethodPatch:
		in := k.payload()
		if m == http.MethodPatch {
			if getErr != nil {
				return 0, nil, getErr
			}
			in = merge(clone(cur), in)
		}
		out, err := p.Put(k.c, id, in)
		if err != nil {
			return 0, nil, err
		}
		if getErr != nil {
			if p.Created != 0 { // the status the clients of this type accept
				return p.Created, out, nil
			}
			return http.StatusCreated, out, nil
		}
		return http.StatusOK, out, nil
	case http.MethodDelete:
		if getErr != nil {
			return http.StatusNoContent, nil, nil
		}
		return http.StatusOK, nil, p.Delete(k.c, id)
	}
	return 0, nil, methodNotAllowed(m, p.Type)
}

// checkType refuses a type pogi does not know: a Provider's, or one listed in builtin. A namespace
// nobody registered is NoRegisteredProviderFound; an unknown type in a known namespace is InvalidResourceType.
func (s *Service) checkType(ns, typ string) error {
	if Lookup(s.h, typ) != nil || slices.ContainsFunc(builtinTypes(ns), func(t string) bool { return strings.EqualFold(t, typ) }) {
		return nil
	}
	known := Lookup(s.h, ns) != nil || slices.ContainsFunc(Types(s.h), func(t string) bool { return strings.HasPrefix(t, strings.ToLower(ns)+"/") })
	for n := range builtin {
		known = known || strings.EqualFold(n, ns)
	}
	if !known {
		return core.Errorf(http.StatusBadRequest, "NoRegisteredProviderFound", "No registered resource provider found for namespace '%s'.", ns)
	}
	return core.Errorf(http.StatusBadRequest, "InvalidResourceType", "The resource type '%s' is not supported by pogi.", typ)
}

// checkTemplate fails a deployment whose template holds a resource type pogi does not know. Templates are
// not run, so this is the one thing that is looked at: the deployment ends Failed with the reason, not Succeeded.
func (s *Service) checkTemplate(dep map[string]any) {
	props, _ := dep["properties"].(map[string]any)
	tmpl, _ := props["template"].(map[string]any)
	var walk func(res any, parent string) error
	walk = func(res any, parent string) error {
		list, _ := res.([]any)
		for _, r := range list {
			m, _ := r.(map[string]any)
			typ := str(m["type"])
			if typ == "" || strings.Contains(typ, "[") { // an expression: unknown until the template runs
				continue
			}
			if first, _, _ := strings.Cut(typ, "/"); !strings.Contains(first, ".") && parent != "" {
				typ = parent + "/" + typ // a child resource names its type relative to its parent
			}
			ns, _, _ := strings.Cut(typ, "/")
			if !strings.Contains(typ, "/providers/") {
				if err := s.checkType(ns, typ); err != nil {
					return err
				}
			}
			if err := walk(m["resources"], typ); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(tmpl["resources"], ""); err != nil {
		e := core.AsError(err)
		props["provisioningState"] = "Failed"
		props["error"] = map[string]any{"code": e.Code, "message": e.Msg}
	}
}

// genericDoc is a stored resource: the request body as sent, with the
// envelope ARM fills in and provisioningState Succeeded.
func genericDoc(g gid, body map[string]any) map[string]any {
	doc := clone(body)
	doc["id"], doc["name"], doc["type"] = g.path, g.name, g.typ
	if doc["tags"] == nil {
		doc["tags"] = map[string]any{}
	}
	props, _ := doc["properties"].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}
	props["provisioningState"] = "Succeeded"
	if strings.EqualFold(g.typ, "Microsoft.Resources/deployments") { // metadata only: templates are not run
		if props["mode"] == nil {
			props["mode"] = "Incremental"
		}
		if props["outputs"] == nil {
			props["outputs"] = map[string]any{}
		}
		props["timestamp"], props["duration"] = time.Now().UTC().Format(time.RFC3339), "PT0.1S"
		props["correlationId"], props["outputResources"] = core.UUID(), []any{}
	}
	doc["properties"] = props
	return doc
}

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	return out
}

// merge applies a PATCH: objects merge, anything else (tags included) is replaced.
func merge(dst, src map[string]any) map[string]any {
	for k, v := range src {
		d, dok := dst[k].(map[string]any)
		sv, sok := v.(map[string]any)
		if dok && sok && k != "tags" {
			merge(d, sv)
		} else {
			dst[k] = v
		}
	}
	return dst
}

var eqFilter = regexp.MustCompile(`^\s*([\w/]+)\s+eq\s+'(.*)'\s*$`)

// filter applies a "$filter=field eq 'value'" to a listing, the one form
// azurerm uses (roleName eq 'Contributor'); other filters are ignored.
func filter(items []map[string]any, f string) []map[string]any {
	m := eqFilter.FindStringSubmatch(f)
	if m == nil {
		return items
	}
	field := m[1][strings.LastIndexByte(m[1], '/')+1:]
	if field == "resourceType" { // azurerm lists the Key Vaults of a subscription this way
		field = "type"
	}
	return slices.DeleteFunc(slices.Clone(items), func(it map[string]any) bool {
		v, ok := it[field]
		if props, _ := it["properties"].(map[string]any); !ok && props != nil {
			v = props[field]
		}
		return !strings.EqualFold(str(v), m[2])
	})
}

var _ = maps.Copy[map[string]any, map[string]any]
