package arm

import (
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/odata"
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
	const states = "/providers/microsoft.policyinsights/policystates/"
	if i := strings.Index(strings.ToLower(path), states); i >= 0 {
		return s.policyStates(k, path[:i], path[i+len(states):])
	}
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
		if isDeployment(res) && Lookup(s.h, typDeployment) == nil {
			return s.deploymentAction(k, res, id.Types[n])
		}
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
	items, err = filter(items, k.r.URL.Query().Get("$filter"))
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, list(items...), nil
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
		if p == nil && isDeployment(id) {
			return s.putDeployment(k, id, in, getErr == nil)
		}
		if err := s.checkPolicy(c, id, in); err != nil {
			return 0, nil, err
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
		if p != nil && (p.Async || p.AsyncPut) && k.r.Method == http.MethodPut {
			k.async(id.Subscription)
			status = created
		}
		if p != nil && p.AsyncPatch && k.r.Method == http.MethodPatch {
			k.async(id.Subscription)
			status = http.StatusAccepted
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
		if p != nil && (p.Async || p.AsyncDelete) || p == nil && isDeployment(id) {
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
	// Every provider has every region: <Namespace>/locations/<region> exists, so the lists and actions under it
	// (capabilities, checkNameAvailability) pass the parent check.
	if len(id.Types) == 1 && len(id.Names) == 1 && strings.EqualFold(id.Types[0], "locations") {
		return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type()}, nil
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
	items := s.listGeneric(prefix, id.Type())
	if isDeployment(id) && parent.ResourceGroup == "" { // a subscription's own deployments, not its groups'
		items = slices.DeleteFunc(items, func(d map[string]any) bool {
			did, _ := ParseID(str(d["id"]))
			return did.ResourceGroup != ""
		})
	}
	return items, nil
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
	if len(rest) == 1 && m == http.MethodPost { // a namespace's action at this scope: /providers/Microsoft.ResourceGraph/resources
		return s.nsAction(k, ID{Namespace: ns, Types: rest})
	}
	if len(rest)%2 == 1 && m == http.MethodPost { // .../<name>/<action>
		return http.StatusOK, genericAction(typ, rest[len(rest)-1]), nil
	}
	if len(rest)%2 == 1 { // a collection
		if m != http.MethodGet {
			return 0, nil, methodNotAllowed(m, typ)
		}
		items, err := filter(s.listGeneric(strings.ToLower(path)+"/", typ), k.r.URL.Query().Get("$filter"))
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, list(items...), nil
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
	if strings.EqualFold(g.typ, typDeployment) { // what a deployment has before it runs
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

var (
	tagPairRe   = regexp.MustCompile(`(?i)\btagname\s+eq\s+'((?:[^']|'')*)'\s+and\s+tagvalue\s+eq\s+'((?:[^']|'')*)'`)
	tagNameRe   = regexp.MustCompile(`(?i)\btagname\s+eq\s+('(?:[^']|'')*')`)
	tagPrefixRe = regexp.MustCompile(`(?i)\bstartswith\(\s*tagname\s*,\s*('(?:[^']|'')*')\s*\)`)
	atScopeRe   = regexp.MustCompile(`(?i)\batScope\(\s*\)`)
	assignedRe  = regexp.MustCompile(`(?i)\bassignedTo\(\s*('(?:[^']|'')*')\s*\)`)
)

// armFilter turns the tag and role-assignment forms ARM adds to OData into plain OData over the fields
// itemView adds. tagName with tagValue is one test: the tag has that value.
func armFilter(f string) string {
	f = tagPairRe.ReplaceAllString(f, "tagpairs/any(_v: _v eq '$1=$2')")
	f = tagNameRe.ReplaceAllString(f, "tagnames/any(_v: _v eq $1)")
	f = tagPrefixRe.ReplaceAllString(f, "tagnames/any(_v: startswith(_v , $1))")
	f = atScopeRe.ReplaceAllString(f, "_true eq true") // pogi lists no role assignments of a sub-scope, so atScope() keeps every one
	return assignedRe.ReplaceAllString(f, "principalId eq $1")
}

// itemView is what a filter reads: the item, its properties, the resource group from its id and its tags.
func itemView(it map[string]any) map[string]any {
	v := maps.Clone(it)
	if props, _ := it["properties"].(map[string]any); props != nil {
		for k, p := range props {
			if _, ok := v[k]; !ok {
				v[k] = p
			}
		}
	}
	if id, ok := ParseID(str(it["id"])); ok {
		v["resourceGroup"] = id.ResourceGroup
	}
	var names, pairs []any
	tags := tagMap(it["tags"])
	for _, k := range core.SortedKeys(tags) {
		names = append(names, k)
		pairs = append(pairs, k+"="+tags[k])
	}
	v["resourceType"] = it["type"] // azurerm and az list by resourceType
	v["tagnames"], v["tagpairs"], v["_true"] = names, pairs, true
	return v
}

func tagMap(v any) map[string]string {
	out := map[string]string{}
	switch t := v.(type) {
	case map[string]string:
		return t
	case map[string]any:
		for k, x := range t {
			out[k] = str(x)
		}
	}
	return out
}

// filter applies a $filter to a listing. A filter it cannot read is a 400, as in Azure.
func filter(items []map[string]any, f string) ([]map[string]any, error) {
	if strings.TrimSpace(f) == "" {
		return items, nil
	}
	match, err := odata.Parse(armFilter(f))
	if err != nil {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidFilter", "The $filter '%s' is not valid: %v.", f, err)
	}
	return slices.DeleteFunc(slices.Clone(items), func(it map[string]any) bool { return !match(itemView(it)) }), nil
}

var _ = maps.Copy[map[string]any, map[string]any]
