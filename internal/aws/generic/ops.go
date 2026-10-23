package generic

import (
	"encoding/base64"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// normIndex keys a map by normalized member name.
func normIndex(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[norm(k)] = v
	}
	return out
}

// lookupIn finds the value of identifier id of type t in a request. The type's
// own identifier is also found as Name, Id or Arn, or <Type>Name and so on.
func lookupIn(idx map[string]any, id string, t *rtype) (string, bool) {
	keys := []string{norm(id)}
	if id == t.own {
		ro := role(id)
		keys = append(keys, ro, t.n+ro, t.n+norm(id)) // a delete may call DataIntegration's Identifier DataIntegrationIdentifier
	}
	for i, k := range keys {
		s := str(idx[k])
		if s == "" {
			continue
		}
		// Name stands in for an identifier only when it is a valid one (VPC Lattice makes up rgw-... ids, and
		// a gateway's name is not one).
		if i > 0 && !fitsShape(t.idShape(id), s) {
			continue
		}
		return s, true
	}
	return "", false
}

// fitsShape says whether a string meets a string shape's length and pattern; a nil shape takes anything.
func fitsShape(sh *shape, s string) bool {
	if sh == nil {
		return true
	}
	if sh.Min != nil && float64(len(s)) < *sh.Min || sh.Max != nil && float64(len(s)) > *sh.Max {
		return false
	}
	if sh.Pattern == "" {
		return true
	}
	re, err := regexp.Compile("^(?:" + sh.Pattern + ")$")
	return err != nil || re.MatchString(s)
}

func (c *Call) key() string { return c.Svc.region(c.Region) }

func (c *Call) now() time.Time { return time.Now().UTC() }

// ids reads t's identifiers from the input; ok is false when one is missing.
func (c *Call) ids(t *rtype) (ids map[string]string, key string, ok bool) {
	idx := normIndex(c.In)
	ids, ok = map[string]string{}, true
	var vals []string
	for _, id := range t.ids {
		v, found := lookupIn(idx, id, t)
		if !found { // a create call may nest the identifiers (CreateDataCellsFilter takes them inside TableData)
			v, found = deepLookup(c.In, norm(id))
		}
		ok = ok && found
		ids[id] = v
		vals = append(vals, v)
	}
	return ids, strings.Join(vals, "/"), ok
}

// resolve finds the resource a request names: by identifiers, or by an ARN anywhere in the input.
func (c *Call) resolve(t *rtype) *Resource {
	c.Svc.mu.Lock()
	defer c.Svc.mu.Unlock()
	return c.resolveLocked(t)
}

func (c *Call) resolveLocked(t *rtype) *Resource {
	s := c.Svc
	_, key, named := c.ids(t)
	if named {
		if r := s.st.Res[skey(c.Account, c.key(), t.name, key)]; r != nil {
			return r
		}
	}
	for _, v := range c.In {
		if a, isStr := v.(string); isStr && strings.HasPrefix(a, "arn:") {
			if r := s.byARNLocked(c.Account, a); r != nil && r.Type == t.name {
				return r
			}
		}
	}
	if named { // the identifiers were all there, and name nothing
		if len(t.ids) == 1 { // unless the one identifier was given as the resource's name or ARN (Bedrock takes either)
			if v, _ := lookupIn(normIndex(c.In), t.own, t); v != "" {
				for _, r := range s.listLocked(c.Account, c.key(), t.name) {
					if r.Name == v || r.ID == v || r.ARN == v {
						return r
					}
				}
			}
		}
		return nil
	}
	// A call may name the identifier differently from the create call (DataIntegrationIdentifier, Identifier):
	// a string in the input that is a resource's name, id or ARN, or that the create call stored under the same member, finds it.
	for k, v := range c.In {
		if x, isStr := v.(string); isStr && x != "" {
			for _, r := range s.listLocked(c.Account, c.key(), t.name) {
				if x == r.Name || x == r.ID || x == r.ARN || idLike(k) && str(r.Data[k]) == x {
					return r
				}
			}
		}
	}
	return nil
}

// idLike says whether a member is named like an identifier.
func idLike(member string) bool {
	n := norm(member)
	return strings.HasSuffix(n, "arn") || strings.HasSuffix(n, "id") || strings.HasSuffix(n, "name") || strings.HasSuffix(n, "identifier")
}

// deepLookup finds the string value of a member, by normalized name, inside the structures of an input.
func deepLookup(in map[string]any, name string) (string, bool) {
	for _, k := range slices.Sorted(maps.Keys(in)) {
		if sub, ok := in[k].(map[string]any); ok {
			if v := str(normIndex(sub)[name]); v != "" {
				return v, true
			}
			if v, ok := deepLookup(sub, name); ok {
				return v, true
			}
		}
	}
	return "", false
}

func (c *Call) arn(t *rtype, ids map[string]string, tail string) string {
	if tpl, ok := arnTemplates[c.Svc.meta.Name+"/"+t.name]; ok {
		return c.arnOf(strings.ReplaceAll(tpl, "{tail}", tail), ids) // {tail} is the id made up for a resource named by its ARN
	}
	var segs []string
	for _, id := range t.ids {
		if role(id) == "arn" {
			continue
		}
		name := kebab(stem(id))
		if name == "" {
			if id == t.own {
				name = kebab(t.name)
			} else {
				name = kebab(id)
			}
		}
		segs = append(segs, name, ids[id])
	}
	if tail != "" {
		segs = append(segs, kebab(t.name), tail)
	}
	sep := "/"
	if x, ok := arnSep[c.Svc.meta.Name]; ok {
		sep = x
	}
	if len(segs) == 0 { // every identifier is another resource's ARN: this one has none
		return ""
	}
	return c.arnOf(segs[0]+sep+strings.Join(segs[1:], "/"), nil)
}

// arnOf completes the resource part of an ARN, filling {Member} placeholders from ids.
func (c *Call) arnOf(res string, ids map[string]string) string {
	for k, v := range ids {
		res = strings.ReplaceAll(res, "{"+k+"}", v)
	}
	region, svc := c.Region, c.Svc.meta.EndpointPrefix
	if a, ok := arnService[c.Svc.meta.Name]; ok {
		svc = a
	}
	if c.Svc.region(region) == "" {
		return "arn:" + core.Partition(region) + ":" + svc + "::" + c.Account + ":" + res
	}
	return core.ARN(svc, region, c.Account, res)
}

// ---- tags

func isTagMember(n string, sh *shape) bool {
	nn := norm(n)
	return (strings.HasSuffix(nn, "tags") || nn == "taglist" || nn == "tagset") && (sh.Type == "map" || sh.Type == "list")
}

func isRemoveMember(n string) bool {
	nn := norm(n)
	return strings.HasPrefix(nn, "remove") || strings.HasPrefix(nn, "untag") || strings.Contains(nn, "tagkey") || nn == "keys"
}

// tagMap reads tags from a map, a list of {Key, Value} structures or a list of keys.
func tagMap(v any) map[string]string {
	out := map[string]string{}
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			out[k] = str(e)
		}
	case []any:
		for _, e := range x {
			switch em := e.(type) {
			case string:
				out[em] = ""
			case map[string]any:
				var k, val string
				for n, ev := range em {
					switch norm(n) {
					case "key", "tagkey":
						k = str(ev)
					case "value", "tagvalue":
						val = str(ev)
					}
				}
				if k != "" {
					out[k] = val
				}
			}
		}
	}
	return out
}

// takeTags splits the tag members off an input: what is left, the tags to add and the keys to remove.
func takeTags(sh *shape, in map[string]any) (rest map[string]any, add map[string]string, remove []string) {
	rest, add = map[string]any{}, map[string]string{}
	for n, v := range in {
		mr := sh.Members[n]
		switch {
		case mr == nil || !isTagMember(n, mr.sh) && !isRemoveMember(n):
			rest[n] = v
		case isRemoveMember(n):
			remove = append(remove, slices.Collect(maps.Keys(tagMap(v)))...)
		default:
			maps.Copy(add, tagMap(v))
		}
	}
	return
}

func applyTags(r *Resource, add map[string]string, remove []string) {
	if r.Tags == nil {
		r.Tags = map[string]string{}
	}
	maps.Copy(r.Tags, add)
	for _, k := range remove {
		delete(r.Tags, k)
	}
	if len(r.Tags) == 0 {
		r.Tags = nil
	}
}

// ---- default operations

func (c *Call) create() (map[string]any, error) {
	t, s := c.op.typ, c.Svc
	ids, key, ok := c.ids(t)
	own := t.own
	var arn string
	if !ok || ids[own] == "" {
		if ids[own] == "" {
			if role(own) == "arn" {
				tail := core.UUID()
				arn = c.arn(t, ids, tail)
				ids[own] = arn
			} else {
				ids[own] = mintID(t.idShape(own), t.name)
			}
		}
		var vals []string
		for _, id := range t.ids {
			vals = append(vals, ids[id])
		}
		key = strings.Join(vals, "/")
	}
	data, add, _ := takeTags(c.op.in, c.In)
	now := c.now()
	s.mu.Lock()
	sk := skey(c.Account, c.key(), t.name, key)
	old := s.st.Res[sk]
	if old != nil && !strings.HasPrefix(c.Op, "Put") {
		s.mu.Unlock()
		return nil, c.Exists("%s %s already exists", t.name, key)
	}
	r := &Resource{Type: t.name, Key: key, IDs: ids, Account: c.Account, Region: c.key(), Data: data, Created: now, Updated: now}
	if old != nil {
		r.Created, r.Tags = old.Created, old.Tags
	}
	switch role(own) {
	case "arn":
		// A composite key made of ARNs (an inline policy of a permission set) names its parent, not itself.
		if len(t.ids) == 1 || strings.Contains(norm(own), t.n) {
			r.ARN = ids[own]
		}
	case "id":
		r.ID = ids[own]
	default:
		r.Name = ids[own]
	}
	if r.ARN == "" {
		if old != nil && old.ARN != "" {
			r.ARN = old.ARN
		} else {
			r.ARN = c.arn(t, ids, "")
		}
	}
	applyTags(r, add, nil)
	s.st.Res[sk] = r
	cp := r.clone()
	s.mu.Unlock()
	c.Res = cp
	return c.fill(c.op.out, cp), nil
}

func (c *Call) read() (map[string]any, error) {
	r := c.resolve(c.op.typ)
	if r == nil {
		return nil, c.NotFound("%s not found", c.op.typ.name)
	}
	r = r.clone()
	c.Res = r
	return c.fill(c.op.out, r), nil
}

func (c *Call) update() (map[string]any, error) {
	t, s := c.op.typ, c.Svc
	data, add, remove := takeTags(c.op.in, c.In)
	s.mu.Lock()
	r := c.resolveLocked(t)
	if r == nil {
		s.mu.Unlock()
		return nil, c.NotFound("%s not found", t.name)
	}
	if r.Data == nil {
		r.Data = map[string]any{}
	}
	for k, v := range data {
		r.Data[k] = v
	}
	applyTags(r, add, remove)
	r.Updated = c.now()
	cp := r.clone()
	s.mu.Unlock()
	c.Res = cp
	return c.fill(c.op.out, cp), nil
}

func (c *Call) del() (map[string]any, error) {
	s := c.Svc
	s.mu.Lock()
	r := c.resolveLocked(c.op.typ)
	if r == nil {
		s.mu.Unlock()
		return nil, c.NotFound("%s not found", c.op.typ.name)
	}
	delete(s.st.Res, r.skey())
	cp := r.clone()
	s.mu.Unlock()
	c.Res = cp
	return c.fill(c.op.out, cp), nil
}

// targets finds the resources a tagging call names: ARNs and identifiers in its input.
func (c *Call) targets() []*Resource {
	s := c.Svc
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Resource
	add := func(v string) {
		if r := s.byARNLocked(c.Account, v); r != nil {
			out = append(out, r)
			return
		}
		for _, r := range s.st.Res {
			if r.Account == c.Account && (r.Name == v || r.ID == v || slices.Contains(slices.Collect(maps.Values(r.IDs)), v)) {
				out = append(out, r)
			}
		}
	}
	// A member named for the resource (ResourceArn) wins over others (the InstanceArn beside it).
	named := slices.ContainsFunc(c.op.in.names, func(n string) bool { return strings.Contains(norm(n), "resource") })
	for _, n := range c.op.in.names {
		nn := norm(n)
		if named && !strings.Contains(nn, "resource") {
			continue
		}
		if !strings.Contains(nn, "arn") && !strings.Contains(nn, "resource") && !strings.HasSuffix(nn, "names") && !strings.HasSuffix(nn, "ids") {
			continue
		}
		switch x := c.In[n].(type) {
		case string:
			add(x)
		case []any:
			for _, e := range x {
				if v, ok := e.(string); ok {
					add(v)
				}
			}
		}
	}
	return out
}

func (c *Call) tag() (map[string]any, error) {
	targets := c.targets()
	if len(targets) == 0 {
		return nil, c.NotFound("resource not found")
	}
	var add map[string]string
	var remove []string
	for n, v := range c.In {
		if mr := c.op.in.Members[n]; mr != nil && (isTagMember(n, mr.sh) || isRemoveMember(n)) {
			if c.op.kind == kUntag {
				remove = append(remove, slices.Collect(maps.Keys(tagMap(v)))...)
			} else {
				add = tagMap(v)
			}
		}
	}
	c.Svc.mu.Lock()
	for _, r := range targets {
		applyTags(r, add, remove)
		r.Updated = c.now()
	}
	c.Svc.mu.Unlock()
	c.Res = targets[0].clone()
	return c.fill(c.op.out, c.Res), nil
}

func (c *Call) listTags() (map[string]any, error) {
	targets := c.targets()
	if len(targets) == 0 {
		return nil, c.NotFound("resource not found")
	}
	c.Svc.mu.Lock()
	r := targets[0].clone()
	c.Svc.mu.Unlock()
	c.Res = r
	return c.fill(c.op.out, r), nil
}

// other runs operations the engine has no model for: the output is built from
// the resource the input names, when it names one.
func (c *Call) other() (map[string]any, error) {
	var r *Resource
	for _, n := range core.SortedKeys(c.m.types) {
		t := c.m.types[n]
		if _, _, ok := c.ids(t); ok && len(t.ids) > 0 {
			if f := c.resolve(t); f != nil {
				r = f.clone()
				break
			}
		}
	}
	c.Res = r
	return c.fill(c.op.out, r), nil
}

// ---- lists

func (c *Call) list() (map[string]any, error) {
	t, op, s := c.op.typ, c.op, c.Svc
	idx := normIndex(c.In)
	s.mu.Lock()
	all := s.listLocked(c.Account, c.key(), t.name)
	items := make([]*Resource, 0, len(all))
	filtered := false
	for _, id := range t.ids {
		if _, ok := idx[norm(id)]; ok {
			filtered = true
		} else if _, ok := idx[norm(id)+"s"].([]any); ok {
			filtered = true
		}
	}
	for _, r := range all {
		ok := true
		for _, id := range t.ids {
			if v, found := lookupIn(idx, id, t); found && v != "" {
				ok = ok && r.IDs[id] == v
			} else if l, isList := idx[norm(id)+"s"].([]any); isList && len(l) > 0 {
				ok = ok && slices.ContainsFunc(l, func(e any) bool { return str(e) == r.IDs[id] })
			}
		}
		for k, v := range idx { // other scalar members of the request filter on the same member of the resource (ListAccessPoints by FileSystemId)
			if d, has := normIndex(r.Data)[k]; has && !slices.Contains(pagingMembers, k) && scalarVal(v) && scalarVal(d) {
				ok = ok && str(d) == str(v)
			}
		}
		if ok {
			items = append(items, r.clone())
		}
	}
	s.mu.Unlock()
	if filtered && len(items) == 0 && strings.HasPrefix(op.Name, "Describe") {
		return nil, c.NotFound("%s not found", t.name)
	}
	pg := c.m.pagerOf(op)
	off, limit := 0, 0
	if pg.in != "" {
		if tok := str(c.In[pg.in]); tok != "" {
			b, _ := base64.RawURLEncoding.DecodeString(tok) // the number check below rejects a bad token
			var err error
			if off, err = strconv.Atoi(string(b)); err != nil || off < 0 {
				return nil, c.Invalid("Invalid %s: %s", pg.in, tok)
			}
		}
	}
	if pg.limit != "" {
		limit, _ = strconv.Atoi(str(c.In[pg.limit]))
	}
	off = min(max(off, 0), len(items))
	page, more := items[off:], false
	if limit > 0 && off+limit < len(items) {
		page, more = items[off:off+limit], true
	}
	out := c.fill(op.out, nil)
	if out == nil {
		out = map[string]any{}
	}
	if pg.result != "" {
		rs := op.out.Members[pg.result].sh
		list := make([]any, 0, len(page))
		for _, r := range page {
			list = append(list, c.fillItem(rs.Member.sh, r, pg.result))
		}
		out[pg.result] = list
	}
	if pg.out != "" {
		delete(out, pg.out)
		if more {
			out[pg.out] = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(off + limit)))
		}
	}
	for _, n := range op.out.names {
		switch norm(n) {
		case "istruncated", "hasmore", "moreresults":
			if op.out.Members[n].sh.Type == "boolean" {
				out[n] = more
			}
		}
	}
	return out, nil
}

func scalarVal(v any) bool {
	switch v.(type) {
	case string, float64, bool:
		return true
	}
	return false
}

// pagerOf returns the paging members of a list operation, from botocore's
// paginators or, without one, from the member names.
func (m *model) pagerOf(op *operation) (pg struct{ in, out, limit, result string }) {
	if op.in == nil || op.out == nil {
		return
	}
	has := func(sh *shape, n string) string {
		if n != "" && sh.Members[n] != nil {
			return n
		}
		return ""
	}
	if p := op.pager; p != nil {
		pg.in, pg.out, pg.limit, pg.result = has(op.in, p.In), has(op.out, p.Out), has(op.in, p.Limit), has(op.out, p.Result)
	} else {
		pick := func(sh *shape, names ...string) string {
			for _, n := range sh.names {
				if slices.Contains(names, norm(n)) {
					return n
				}
			}
			return ""
		}
		pg.in = pick(op.in, "nexttoken", "marker", "nextpagetoken", "nextmarker", "token")
		pg.limit = pick(op.in, "maxresults", "maxitems", "maxrecords", "limit", "pagesize", "maxkeys", "maxentries")
		pg.out = pick(op.out, "nexttoken", "nextmarker", "nextpagetoken", "marker")
		if pg.in == "" || pg.out == "" {
			pg.in, pg.out, pg.limit = "", "", ""
		}
	}
	if pg.result == "" {
		var lists []string
		for _, n := range op.out.names {
			if sh := op.out.Members[n].sh; sh.Type == "list" {
				lists = append(lists, n)
			}
		}
		for _, n := range lists {
			if op.typ != nil && strings.Contains(norm(n), op.typ.n) {
				pg.result = n
				break
			}
		}
		if pg.result == "" && len(lists) > 0 {
			pg.result = lists[0]
		}
	}
	return
}
