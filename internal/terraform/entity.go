package terraform

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// resEnt is one resource or data block: all its instances.
type resEnt struct {
	s     *scope
	def   *resDef
	key   string
	l     *lazy
	out   []*Resource
	insts []instance
	kind  int
}

// metaAttrs are the arguments Terraform itself reads; they are not part of the resource's values.
var metaAttrs = map[string]bool{"count": true, "for_each": true, "provider": true, "depends_on": true}

// skipBlocks are nested blocks that hold no resource values.
var skipBlocks = map[string]bool{"lifecycle": true, "provisioner": true, "connection": true}

// value is the resource's value for other expressions to refer to. It adds the block to deps.
func (e *resEnt) value(deps depset) cty.Value {
	deps[e.s.block+e.key] = true
	v, _ := e.s.p.get(e.l)
	return v
}

// compute evaluates every instance of the resource.
func (e *resEnt) compute() (cty.Value, depset) {
	s, p, d := e.s, e.s.p, e.def
	deps := depset{}
	what := s.block + e.key
	var count, forEach hcl.Expression
	body, native := d.body.(*hclsyntax.Body)
	var attrs map[string]hcl.Expression
	if native {
		attrs = map[string]hcl.Expression{}
		for k, a := range body.Attributes {
			attrs[k] = a.Expr
		}
	} else {
		as, _ := d.body.JustAttributes()
		attrs = map[string]hcl.Expression{}
		for k, a := range as {
			attrs[k] = a.Expr
		}
	}
	count, forEach = attrs["count"], attrs["for_each"]
	shared := depset{}
	e.kind, e.insts = s.instances(count, forEach, what, shared)
	if de := attrs["depends_on"]; de != nil {
		items, _ := hcl.ExprList(de)
		var ts []hcl.Traversal
		for _, it := range items {
			if t, diags := hcl.AbsTraversalForExpr(it); !diags.HasErrors() {
				ts = append(ts, t)
			}
		}
		s.ctx(ts, nil, shared)
	}
	provider := cloudProvider(d.typ)
	if pe := attrs["provider"]; pe != nil {
		if k, err := provKey(pe); err == nil {
			provider = k
		}
	}
	vals := make([]cty.Value, len(e.insts))
	for i, in := range e.insts {
		if p.err != nil {
			break
		}
		if p.count++; p.count > maxInstances {
			p.fail(fmt.Errorf("more than %d resource instances: lower a count or for_each", maxInstances))
			break
		}
		idep := depset{}
		idep.add(shared)
		m := s.evalBody(d.body, in.ex, idep, what+in.suffix, true)
		obj := map[string]cty.Value{}
		for k, v := range m {
			obj[k] = v
		}
		for _, k := range []string{"id", "arn"} { // computed values other resources may refer to
			if _, ok := obj[k]; !ok {
				obj[k] = cty.UnknownVal(cty.DynamicPseudoType)
			}
		}
		vals[i] = cty.ObjectVal(obj)
		goAttrs := map[string]any{}
		for k, v := range m {
			goAttrs[k] = toGo(v)
		}
		block := s.block + e.key
		delete(idep, block)
		r := &Resource{
			Address: s.addr + e.key + in.suffix, Block: block, Module: strings.TrimSuffix(s.block, "."),
			Type: d.typ, Name: d.name, Data: d.data, Cloud: cloudOf(d.typ),
			Attrs: goAttrs, Deps: sortedKeys(idep), File: d.file, Line: d.line,
		}
		var prov map[string]any
		if c := s.provs[provider]; c != nil {
			prov = c.values()
		}
		r.Region = regionOf(r.Cloud, goAttrs, prov)
		e.out = append(e.out, r)
	}
	deps[s.block+e.key] = true
	return shape(e.kind, e.insts, vals), deps
}

// evalBody evaluates a block body into attribute values. Nested blocks become a list of objects
// under the block's name; dynamic blocks are expanded.
func (s *scope) evalBody(body hcl.Body, ex map[string]cty.Value, deps depset, what string, top bool) map[string]cty.Value {
	out := map[string]cty.Value{}
	nb, native := body.(*hclsyntax.Body)
	var attrs map[string]hcl.Expression
	if native {
		attrs = map[string]hcl.Expression{}
		for k, a := range nb.Attributes {
			attrs[k] = a.Expr
		}
	} else if as, _ := body.JustAttributes(); as != nil {
		attrs = map[string]hcl.Expression{}
		for k, a := range as {
			attrs[k] = a.Expr
		}
	}
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		if !(top && metaAttrs[k]) {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	for _, k := range names {
		out[k] = s.evalOr(attrs[k], ex, deps, what+" "+k)
	}
	if !native {
		return out
	}
	lists := map[string][]cty.Value{}
	var order []string
	add := func(name string, v cty.Value) {
		if _, ok := lists[name]; !ok {
			order = append(order, name)
		}
		lists[name] = append(lists[name], v)
	}
	for _, b := range nb.Blocks {
		switch {
		case top && skipBlocks[b.Type]:
		case b.Type == "dynamic" && len(b.Labels) == 1:
			s.dynamic(b, ex, deps, what, func(v cty.Value) { add(b.Labels[0], v) })
		default:
			add(b.Type, cty.ObjectVal(s.evalBody(b.Body, ex, deps, what+" "+b.Type, false)))
		}
	}
	for _, name := range order {
		out[name] = cty.TupleVal(lists[name])
	}
	return out
}

// dynamic expands a dynamic block: one nested block per element of its for_each.
func (s *scope) dynamic(b *hclsyntax.Block, ex map[string]cty.Value, deps depset, what string, emit func(cty.Value)) {
	var content *hclsyntax.Block
	for _, c := range b.Body.Blocks {
		if c.Type == "content" {
			content = c
		}
	}
	fe := b.Body.Attributes["for_each"]
	if content == nil || fe == nil {
		s.p.warn(fmt.Sprintf("%s: dynamic %q block needs for_each and content", what, b.Labels[0]))
		return
	}
	iter := b.Labels[0]
	if ia := b.Body.Attributes["iterator"]; ia != nil {
		if t, diags := hcl.AbsTraversalForExpr(ia.Expr); !diags.HasErrors() {
			iter = t.RootName()
		}
	}
	coll := s.evalOr(fe.Expr, ex, deps, what+" dynamic "+b.Labels[0])
	t := coll.Type()
	if !coll.IsKnown() || coll.IsNull() || !(t.IsMapType() || t.IsObjectType() || t.IsSetType() || t.IsListType() || t.IsTupleType()) {
		s.p.warn(fmt.Sprintf("%s: dynamic %q block has a for_each that is not known, so its blocks are left out", what, b.Labels[0]))
		return
	}
	for it := coll.ElementIterator(); it.Next(); {
		k, v := it.Element()
		if t.IsSetType() {
			k = v
		}
		ex2 := map[string]cty.Value{}
		for a, b := range ex {
			ex2[a] = b
		}
		ex2[iter] = cty.ObjectVal(map[string]cty.Value{"key": k, "value": v})
		emit(cty.ObjectVal(s.evalBody(content.Body, ex2, deps, what+" "+b.Labels[0], false)))
	}
}

// modEnt is one module call: all its instances.
type modEnt struct {
	s     *scope
	def   *callDef
	attrs hcl.Attributes
	done  bool
	kind  int
	insts []instance
	subs  []*scope // one per instance; nil when the module is not available
	mod   *module
}

// expand creates the module's instances (once).
func (e *modEnt) expand() {
	if e.done {
		return
	}
	e.done = true
	s, p := e.s, e.s.p
	if s.depth >= maxDepth {
		p.fail(fmt.Errorf("modules are nested more than %d levels deep (does module %q call itself?)", maxDepth, e.def.name))
		return
	}
	src := ""
	if a := e.attrs["source"]; a != nil {
		if v, diags := a.Expr.Value(nil); !diags.HasErrors() && v.Type() == cty.String && v.IsKnown() && !v.IsNull() {
			src = v.AsString()
		}
	}
	e.mod = p.moduleFor(s, e.def.name, src)
	if e.mod == nil {
		return
	}
	var count, forEach hcl.Expression
	if a := e.attrs["count"]; a != nil {
		count = a.Expr
	}
	if a := e.attrs["for_each"]; a != nil {
		forEach = a.Expr
	}
	e.kind, e.insts = s.instances(count, forEach, s.block+"module."+e.def.name, depset{})
	for _, in := range e.insts {
		if p.err != nil {
			return
		}
		call := &callInput{parent: s, def: e.def, attrs: e.attrs, ex: in.ex}
		e.subs = append(e.subs, p.newScope(s, call, e.mod, in.suffix, in.ex, s.depth+1))
	}
}

// moduleFor finds the module a call refers to: a local folder, or a downloaded one that
// modules.json maps. When there is none it warns and returns nil.
func (p *parser) moduleFor(s *scope, name, src string) *module {
	key := name
	if s.key != "" {
		key = s.key + "." + name
	}
	dir, ok := "", false
	if strings.HasPrefix(src, "./") || strings.HasPrefix(src, "../") {
		dir, ok = pathJoin(s.mod.dir, src)
		if !ok {
			p.warn(fmt.Sprintf("module %q (%s) is outside the folder", name, src))
			return nil
		}
	} else if d, found := p.modDirs[key]; found {
		dir = d
	} else {
		p.warn(fmt.Sprintf("module %q (%s) is not downloaded: run terraform init once", name, src))
		return nil
	}
	m, err := p.loadModule(dir)
	if err != nil {
		p.fail(err)
		return nil
	}
	if m == nil {
		p.warn(fmt.Sprintf("module %q (%s) has no .tf files in %s", name, src, dir))
	}
	return m
}

// value is the module's outputs as other expressions see them. outs names the outputs wanted;
// "" means all of them.
func (e *modEnt) value(outs map[string]bool, deps depset) cty.Value {
	e.expand()
	if e.subs == nil {
		return cty.UnknownVal(cty.DynamicPseudoType)
	}
	vals := make([]cty.Value, len(e.subs))
	for i, sub := range e.subs {
		m := map[string]cty.Value{}
		for name, l := range sub.outs {
			if outs[""] || outs[name] {
				v, d := sub.p.get(l)
				deps.add(d)
				m[name] = v
			}
		}
		vals[i] = cty.ObjectVal(m)
	}
	return shape(e.kind, e.insts, vals)
}

// expand makes every module instance, so that every resource can be listed.
func (s *scope) expand() {
	for _, e := range s.callOrd {
		e.expand()
		for _, sub := range e.subs {
			sub.expand()
		}
	}
	for _, e := range s.resOrd {
		s.p.get(e.l)
	}
}

// collect lists the resources of s and of its modules.
func (s *scope) collect(out *[]*Resource) {
	for _, e := range s.resOrd {
		*out = append(*out, e.out...)
	}
	for _, e := range s.callOrd {
		for _, sub := range e.subs {
			sub.collect(out)
		}
	}
}

// provCfg is a provider block; only its region and zone matter.
type provCfg struct {
	s    *scope
	body hcl.Body
	vals map[string]any
}

func (c *provCfg) values() map[string]any {
	if c.vals != nil {
		return c.vals
	}
	c.vals = map[string]any{}
	attrs, _ := c.body.JustAttributes()
	if nb, ok := c.body.(*hclsyntax.Body); ok { // a native body can hold blocks too, so read its attributes directly
		attrs = hcl.Attributes{}
		for k, a := range nb.Attributes {
			attrs[k] = a.AsHCLAttribute()
		}
	}
	for _, k := range []string{"region", "zone"} {
		if a := attrs[k]; a != nil {
			if v, diags := c.s.eval(a.Expr, nil, depset{}); !diags.HasErrors() {
				if str, ok := toGo(v).(string); ok {
					c.vals[k] = str
				}
			}
		}
	}
	return c.vals
}

// cloudProvider is the default provider name of a resource type: the part before the first "_".
func cloudProvider(typ string) string {
	p, _, _ := strings.Cut(typ, "_")
	return p
}

// cloudOf maps a resource type to "aws", "gcp" or "azure"; "" for any other provider.
func cloudOf(typ string) string {
	switch cloudProvider(typ) {
	case "aws":
		return "aws"
	case "google":
		return "gcp"
	case "azurerm":
		return "azure"
	}
	return ""
}

var (
	regionRe = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)
	zoneRe   = regexp.MustCompile(`^([a-z]+-[a-z]+[0-9]+)-[a-z]$`)
)

// regionOf works out where a resource lives from its own values, then from its provider's
// (prov may be nil). It never reads the environment.
func regionOf(cloud string, attrs, prov map[string]any) string {
	str := func(m map[string]any, k string) string {
		s, _ := m[k].(string)
		return s
	}
	switch cloud {
	case "aws":
		if r := str(attrs, "region"); r != "" {
			return r
		}
		return str(prov, "region")
	case "azure":
		return strings.ToLower(strings.ReplaceAll(str(attrs, "location"), " ", ""))
	case "gcp":
		if r := str(attrs, "region"); r != "" {
			return r
		}
		if m := zoneRe.FindStringSubmatch(str(attrs, "zone")); m != nil {
			return m[1]
		}
		if l := str(attrs, "location"); regionRe.MatchString(l) {
			return l
		} else if m := zoneRe.FindStringSubmatch(l); m != nil {
			return m[1]
		}
		if r := str(prov, "region"); r != "" {
			return r
		}
		if m := zoneRe.FindStringSubmatch(str(prov, "zone")); m != nil {
			return m[1]
		}
	}
	return ""
}

// inheritRegions gives an Azure or Google resource with no location of its own the region of a
// resource it references. A child lives where its parent does: an AKS node pool where its cluster
// is, a SQL database where its server is. It repeats so chains (database → server → group) resolve.
func inheritRegions(rs []*Resource) {
	byBlock := map[string]*Resource{}
	for _, r := range rs {
		if r.Region != "" && byBlock[r.Block] == nil {
			byBlock[r.Block] = r
		}
	}
	for changed := true; changed; {
		changed = false
		for _, r := range rs {
			if r.Region != "" || (r.Cloud != "azure" && r.Cloud != "gcp") {
				continue
			}
			for _, d := range r.Deps {
				if p := byBlock[d]; p != nil && p.Cloud == r.Cloud {
					r.Region, changed = p.Region, true
					if byBlock[r.Block] == nil {
						byBlock[r.Block] = r
					}
					break
				}
			}
		}
	}
}
