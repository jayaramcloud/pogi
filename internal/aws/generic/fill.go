package generic

import (
	"slices"
	"strconv"
	"strings"
)

// pagingMembers are request members that must not be echoed into the output.
var pagingMembers = []string{"nexttoken", "marker", "nextmarker", "nextpagetoken", "maxresults", "maxitems", "maxrecords", "limit", "pagesize", "token"}

type filler struct {
	c   *Call
	t   *rtype
	res *Resource
}

// fill builds an output structure from the stored resource (when there is one)
// and the request, by member name.
func (c *Call) fill(sh *shape, r *Resource) map[string]any {
	if sh == nil {
		return map[string]any{}
	}
	f := &filler{c: c, res: r, t: c.op.typ}
	if r != nil {
		f.t = c.typeOf(r)
	}
	idx := map[string]any{}
	for k, v := range c.In {
		if nk := norm(k); !slices.Contains(pagingMembers, nk) {
			idx[nk] = v
		}
	}
	if r != nil {
		for k, v := range r.Data {
			idx[norm(k)] = v
		}
		for k, v := range derived(c.Svc.meta.Name, r) {
			idx[norm(k)] = v
		}
	}
	out, _ := f.obj(sh, idx, r != nil && f.t != nil, 0)
	return out
}

// fillItem builds one entry of a list: a structure from the resource, or its name, id or ARN.
func (c *Call) fillItem(sh *shape, r *Resource, listName string) any {
	if sh.Type != "structure" {
		n := norm(listName)
		switch {
		case strings.Contains(n, "arn"):
			return r.ARN
		case strings.Contains(n, "id") && r.ID != "":
			return r.ID
		case r.Name != "":
			return r.Name
		}
		return r.Key
	}
	f := &filler{c: c, res: r, t: c.typeOf(r)}
	idx := map[string]any{}
	for k, v := range r.Data {
		idx[norm(k)] = v
	}
	for k, v := range derived(c.Svc.meta.Name, r) {
		idx[norm(k)] = v
	}
	out, _ := f.obj(sh, idx, true, 0)
	return out
}

// typeOf is the inferred type of a resource; a type an override made up (a Lightsail static IP) gets a bare one.
func (c *Call) typeOf(r *Resource) *rtype {
	if t := c.m.types[norm(r.Type)]; t != nil {
		return t
	}
	return &rtype{name: r.Type, n: norm(r.Type)}
}

func (f *filler) related(nn string) bool {
	return f.t != nil && len(nn) >= 3 && (strings.Contains(nn, f.t.n) || strings.Contains(f.t.n, nn))
}

// obj fills a structure. own says the structure describes the resource itself,
// so identifiers, ARNs, status and timestamps are made up for it; hits counts
// members found in stored data.
func (f *filler) obj(sh *shape, idx map[string]any, own bool, depth int) (map[string]any, int) {
	out := map[string]any{}
	if depth > 6 {
		return out, 0
	}
	hits := 0
	for _, n := range sh.names {
		mr := sh.Members[n]
		ms := mr.sh
		nn := norm(n)
		if v, ok := idx[nn]; ok {
			if cv, ok := coerce(ms, v); ok {
				out[n] = cv
				hits++
				continue
			}
		}
		if own {
			if v, hit, ok := f.special(n, nn, ms); ok {
				out[n] = v
				if hit {
					hits++
				}
				continue
			}
		}
		if ms.Type == "structure" {
			rel := own && (f.related(nn) || strings.HasSuffix(nn, "status") || strings.HasSuffix(nn, "state") || f.c.op.kind == kListTags) // WorkspaceStatus{statusCode}; a ListTags result nests the tags in a structure of its own (TagInfoForResource)
			sub, h := f.obj(ms, idx, rel, depth+1)
			if h > 0 || (rel && len(sub) > 0) {
				out[n] = sub
				hits += h
			}
		}
	}
	for _, n := range sh.Required { // zero values, so that clients dereferencing required members do not crash
		if _, ok := out[n]; ok || sh.Members[n] == nil { // a model can list a required member it does not define
			continue
		}
		switch ms := sh.Members[n].sh; ms.Type {
		case "integer", "long", "float", "double":
			out[n] = 0.0
		case "boolean":
			out[n] = false
		case "list":
			out[n] = []any{}
		case "map":
			out[n] = map[string]any{}
		case "structure":
			sub, _ := f.obj(ms, idx, false, depth+1)
			out[n] = sub
		}
	}
	return out, hits
}

var ready = []string{"ACTIVE", "AVAILABLE", "READY", "ENABLED", "CREATED", "COMPLETED", "SUCCEEDED", "IN_SERVICE", "OPERATIONAL", "DEPLOYED", "RUNNING", "ONLINE", "REGISTERED", "ISSUED"}

func readyValue(enum []string) string {
	for _, want := range ready {
		for _, e := range enum {
			if strings.EqualFold(e, want) {
				return e
			}
		}
	}
	return enum[0]
}

// special makes up a member of the resource itself: identifiers, tags, status and so on.
func (f *filler) special(n, nn string, ms *shape) (v any, hit, ok bool) {
	r, t := f.res, f.t
	if (ms.Type == "list" || ms.Type == "map") && isTagMember(n, ms) && !isRemoveMember(n) {
		if len(r.Tags) == 0 {
			return nil, false, false
		}
		return tagsAs(ms, r.Tags), true, true
	}
	if ms.Type == "timestamp" {
		switch {
		case strings.Contains(nn, "creat") || strings.Contains(nn, "launch") || strings.Contains(nn, "regist"):
			return r.Created.Format(timeFmt), false, true
		case strings.Contains(nn, "updat") || strings.Contains(nn, "modif"):
			return r.Updated.Format(timeFmt), false, true
		}
		return nil, false, false
	}
	if ms.Type != "string" {
		return nil, false, false
	}
	for id, val := range r.IDs {
		if norm(id) == nn {
			return val, true, true
		}
	}
	if s := stem(n); s != "" {
		for id, val := range r.IDs {
			if norm(id) == norm(s) {
				return val, true, true
			}
		}
	}
	name, id := r.Name, r.ID
	if name == "" {
		name = id
	}
	if id == "" {
		id = name
	}
	for _, ro := range []string{"name", "id", "arn"} { // Name, Id, Arn, <Type>Name…, or a tail of the type's name (PolicyId of a LifecyclePolicy)
		base, ok := strings.CutSuffix(nn, ro)
		if !ok || !(base == "" || base == "resource" || len(base) >= 3 && (strings.HasSuffix(t.n, base) || strings.HasSuffix(base, t.n))) {
			continue
		}
		switch {
		case ro == "name" && name != "":
			return name, true, true
		case ro == "id" && id != "":
			return id, true, true
		case ro == "arn" && r.ARN != "":
			return r.ARN, false, true
		}
	}
	if (nn == "region" || nn == "awsregion") && r.Region != "" {
		return r.Region, false, true
	}
	switch {
	case strings.HasSuffix(nn, "owner") || strings.HasSuffix(nn, "accountid") || nn == "ownerid":
		return r.Account, false, true
	case (strings.HasSuffix(nn, "status") || strings.HasSuffix(nn, "state") || nn == "statuscode") && nn != "relaystate": // a RelayState is a URL, not a status
		if len(ms.Enum) > 0 {
			return readyValue(ms.Enum), false, true
		}
		return "ACTIVE", false, true
	}
	return nil, false, false
}

const timeFmt = "2006-01-02T15:04:05.999999999Z07:00"

// tagsAs renders tags as a map or as a list of {Key, Value} structures, whatever the member's shape is.
func tagsAs(ms *shape, tags map[string]string) any {
	if ms.Type == "map" {
		out := map[string]any{}
		for k, v := range tags {
			out[k] = v
		}
		return out
	}
	kn, vn := "Key", "Value"
	if es := ms.Member.sh; es != nil && es.Type == "structure" {
		for _, n := range es.names {
			switch norm(n) {
			case "key", "tagkey":
				kn = n
			case "value", "tagvalue":
				vn = n
			}
		}
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{kn: k, vn: tags[k]})
	}
	return out
}

// coerce shapes a stored value to the member's shape; ok is false when it does not fit.
func coerce(sh *shape, v any) (any, bool) {
	if sh.Document {
		return v, v != nil
	}
	switch sh.Type {
	case "structure":
		mm, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		idx := normIndex(mm)
		out := map[string]any{}
		for n, mr := range sh.Members {
			if x, ok := idx[norm(n)]; ok {
				if cv, ok := coerce(mr.sh, x); ok {
					out[n] = cv
				}
			}
		}
		return out, true
	case "list":
		l, ok := v.([]any)
		if !ok {
			return nil, false
		}
		out := make([]any, 0, len(l))
		for _, x := range l {
			if cv, ok := coerce(sh.Member.sh, x); ok {
				out = append(out, cv)
			}
		}
		return out, true
	case "map":
		mm, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		out := map[string]any{}
		for k, x := range mm {
			if cv, ok := coerce(sh.Value.sh, x); ok {
				out[k] = cv
			}
		}
		return out, true
	case "timestamp":
		s, ok := normTime(v)
		return s, ok
	case "integer", "long", "float", "double":
		switch x := v.(type) {
		case float64:
			return x, true
		case string:
			f, err := strconv.ParseFloat(x, 64)
			return f, err == nil
		}
		return nil, false
	case "boolean":
		b, ok := v.(bool)
		return b, ok
	case "blob":
		s, ok := v.(string)
		return s, ok
	}
	switch v.(type) {
	case string, float64, bool:
		return str(v), true
	}
	return nil, false
}
