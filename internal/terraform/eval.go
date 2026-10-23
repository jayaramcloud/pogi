package terraform

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/ext/tryfunc"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

// parser holds what one Parse call shares: the files, the warnings and the first fatal error.
type parser struct {
	files   map[string][]byte
	mods    map[string]*module
	modDirs map[string]string // modules.json: module key → folder
	varVals map[string]cty.Value
	base    *hcl.EvalContext // holds the functions
	stubs   map[string]function.Function
	warns   []string
	warned  map[string]bool
	err     error
	stack   []string
	count   int // resource instances built so far
}

func newParser(files map[string][]byte) *parser {
	p := &parser{files: files, mods: map[string]*module{}, modDirs: map[string]string{}, varVals: map[string]cty.Value{},
		stubs: map[string]function.Function{}, warned: map[string]bool{}}
	p.base = &hcl.EvalContext{Functions: functions()}
	p.modulesJSON()
	return p
}

func (p *parser) warn(msg string) {
	if !p.warned[msg] {
		p.warned[msg] = true
		p.warns = append(p.warns, msg)
	}
}

func (p *parser) fail(err error) {
	if p.err == nil {
		p.err = err
	}
}

// functions is the set of Terraform functions pogi can evaluate: the go-cty standard library,
// the conversions, try/can and extraFunctions. Any other function becomes Unknown.
func functions() map[string]function.Function {
	fs := map[string]function.Function{
		"abs": stdlib.AbsoluteFunc, "ceil": stdlib.CeilFunc, "floor": stdlib.FloorFunc, "log": stdlib.LogFunc,
		"pow": stdlib.PowFunc, "signum": stdlib.SignumFunc, "parseint": stdlib.ParseIntFunc,
		"min": stdlib.MinFunc, "max": stdlib.MaxFunc,
		"chomp": stdlib.ChompFunc, "indent": stdlib.IndentFunc, "title": stdlib.TitleFunc,
		"trimspace": stdlib.TrimSpaceFunc, "trim": stdlib.TrimFunc, "trimprefix": stdlib.TrimPrefixFunc, "trimsuffix": stdlib.TrimSuffixFunc,
		"upper": stdlib.UpperFunc, "lower": stdlib.LowerFunc, "substr": stdlib.SubstrFunc, "replace": stdlib.ReplaceFunc,
		"join": stdlib.JoinFunc, "split": stdlib.SplitFunc, "format": stdlib.FormatFunc, "formatlist": stdlib.FormatListFunc,
		"regex": stdlib.RegexFunc, "regexall": stdlib.RegexAllFunc,
		"length": stdlib.LengthFunc, "element": stdlib.ElementFunc, "lookup": stdlib.LookupFunc, "merge": stdlib.MergeFunc,
		"concat": stdlib.ConcatFunc, "coalesce": stdlib.CoalesceFunc, "coalescelist": stdlib.CoalesceListFunc,
		"compact": stdlib.CompactFunc, "contains": stdlib.ContainsFunc, "distinct": stdlib.DistinctFunc,
		"flatten": stdlib.FlattenFunc, "keys": stdlib.KeysFunc, "values": stdlib.ValuesFunc, "reverse": stdlib.ReverseListFunc,
		"slice": stdlib.SliceFunc, "sort": stdlib.SortFunc, "zipmap": stdlib.ZipmapFunc, "chunklist": stdlib.ChunklistFunc,
		"range": stdlib.RangeFunc, "setunion": stdlib.SetUnionFunc, "setintersection": stdlib.SetIntersectionFunc,
		"setsubtract": stdlib.SetSubtractFunc, "setproduct": stdlib.SetProductFunc,
		"jsonencode": stdlib.JSONEncodeFunc, "jsondecode": stdlib.JSONDecodeFunc,
		"tostring": stdlib.MakeToFunc(cty.String), "tonumber": stdlib.MakeToFunc(cty.Number), "tobool": stdlib.MakeToFunc(cty.Bool),
		"tolist": stdlib.MakeToFunc(cty.List(cty.DynamicPseudoType)), "toset": stdlib.MakeToFunc(cty.Set(cty.DynamicPseudoType)),
		"tomap": stdlib.MakeToFunc(cty.Map(cty.DynamicPseudoType)),
		"try":   tryfunc.TryFunc, "can": tryfunc.CanFunc,
	}
	for k, f := range extraFunctions {
		fs[k] = f
	}
	return fs
}

// unknownFn stands in for a function pogi does not know: it accepts anything and returns Unknown.
var unknownFn = function.New(&function.Spec{
	VarParam: &function.Parameter{Name: "args", Type: cty.DynamicPseudoType, AllowUnknown: true, AllowNull: true, AllowDynamicType: true},
	Type:     function.StaticReturnType(cty.DynamicPseudoType),
	Impl: func([]cty.Value, cty.Type) (cty.Value, error) {
		return cty.UnknownVal(cty.DynamicPseudoType), nil
	},
})

// depset is a set of Blocks (addresses without instance keys).
type depset map[string]bool

func (d depset) add(o depset) {
	for k := range o {
		d[k] = true
	}
}

// lazy is a value worked out on first use. Using a lazy that is still being worked out means a
// dependency cycle.
type lazy struct {
	label string
	fn    func() (cty.Value, depset)
	state int8
	val   cty.Value
	deps  depset
}

func (p *parser) get(l *lazy) (cty.Value, depset) {
	switch l.state {
	case 2:
		return l.val, l.deps
	case 1:
		i := len(p.stack) - 1
		for i > 0 && p.stack[i] != l.label {
			i--
		}
		p.fail(fmt.Errorf("dependency cycle: %s", strings.Join(append(append([]string{}, p.stack[i:]...), l.label), " -> ")))
		return cty.UnknownVal(cty.DynamicPseudoType), depset{}
	}
	l.state = 1
	p.stack = append(p.stack, l.label)
	l.val, l.deps = l.fn()
	p.stack = p.stack[:len(p.stack)-1]
	l.state = 2
	return l.val, l.deps
}

// scope is one instance of a module (the root module is the first).
type scope struct {
	p       *parser
	mod     *module
	key     string // the call names joined by "." (the key in modules.json)
	addr    string // "module.a[\"k\"]." or ""
	block   string // "module.a." or ""
	vars    map[string]*lazy
	locals  map[string]*lazy
	outs    map[string]*lazy
	res     map[string]*resEnt // "aws_x.y" and "data.aws_x.y"
	resOrd  []*resEnt
	calls   map[string]*modEnt
	callOrd []*modEnt
	provs   map[string]*provCfg
	depth   int
}

// callInput is how a module instance gets its variables: from the call in its parent.
type callInput struct {
	parent *scope
	def    *callDef
	attrs  hcl.Attributes
	ex     map[string]cty.Value // each and count of the call
}

func (p *parser) newScope(parent *scope, call *callInput, m *module, suffix string, ex map[string]cty.Value, depth int) *scope {
	s := &scope{p: p, mod: m, vars: map[string]*lazy{}, locals: map[string]*lazy{}, outs: map[string]*lazy{},
		res: map[string]*resEnt{}, calls: map[string]*modEnt{}, provs: map[string]*provCfg{}, depth: depth}
	if parent != nil {
		name := call.def.name
		s.key = name
		if parent.key != "" {
			s.key = parent.key + "." + name
		}
		s.addr = parent.addr + "module." + name + suffix + "."
		s.block = parent.block + "module." + name + "."
		for k, c := range parent.provs { // default providers pass down on their own
			if !strings.Contains(k, ".") {
				s.provs[k] = c
			}
		}
		if a := call.attrs["providers"]; a != nil {
			pairs, _ := hcl.ExprMap(a.Expr)
			for _, kv := range pairs {
				ck, e1 := provKey(kv.Key)
				pk, e2 := provKey(kv.Value)
				if c := parent.provs[pk]; e1 == nil && e2 == nil && c != nil {
					s.provs[ck] = c
				}
			}
		}
	}
	for _, d := range m.provs {
		k := d.name
		if d.alias != "" {
			k += "." + d.alias
		}
		s.provs[k] = &provCfg{s: s, body: d.body}
	}
	for name, vd := range m.vars {
		s.vars[name] = &lazy{label: s.block + "var." + name, fn: s.varFn(name, vd, call)}
	}
	for name, e := range m.locals {
		e := e
		s.locals[name] = &lazy{label: s.block + "local." + name, fn: func() (cty.Value, depset) {
			d := depset{}
			return s.evalOr(e, nil, d, s.block+"local."), d
		}}
	}
	for name, e := range m.outputs {
		name, e := name, e
		s.outs[name] = &lazy{label: s.block + "output." + name, fn: func() (cty.Value, depset) {
			d := depset{}
			return s.evalOr(e, nil, d, s.block+"output."+name), d
		}}
	}
	for _, d := range m.resources {
		e := &resEnt{s: s, def: d}
		key := d.typ + "." + d.name
		if d.data {
			key = "data." + key
		}
		e.key = key
		e.l = &lazy{label: s.block + key, fn: e.compute}
		s.res[key] = e
		s.resOrd = append(s.resOrd, e)
	}
	for _, c := range m.calls {
		e := &modEnt{s: s, def: c}
		e.attrs, _ = c.body.JustAttributes()
		s.calls[c.name] = e
		s.callOrd = append(s.callOrd, e)
	}
	return s
}

// provKey reads a provider reference such as aws or aws.east as "aws" or "aws.east".
func provKey(e hcl.Expression) (string, error) {
	t, diags := hcl.AbsTraversalForExpr(e)
	if diags.HasErrors() {
		return "", diags
	}
	k := t.RootName()
	if len(t) > 1 {
		if a, ok := t[1].(hcl.TraverseAttr); ok {
			k += "." + a.Name
		}
	}
	return k, nil
}

// varFn works out a variable's value: the module call's input, a .tfvars value (root only), the
// default, or Unknown with a warning.
func (s *scope) varFn(name string, vd *varDef, call *callInput) func() (cty.Value, depset) {
	return func() (cty.Value, depset) {
		p := s.p
		d := depset{}
		var v cty.Value
		switch {
		case call != nil && call.attrs[name] != nil:
			v = call.parent.evalOr(call.attrs[name].Expr, call.ex, d, call.parent.block+"module."+call.def.name+" input "+name)
		case call == nil && hasVal(p.varVals, name):
			v = p.varVals[name]
		case vd.def != nil:
			v = s.evalOr(vd.def, nil, d, s.block+"var."+name)
		default:
			if call == nil {
				p.warn(fmt.Sprintf("variable %q has no value: set it in a .tfvars file", name))
			} else {
				p.warn(fmt.Sprintf("variable %q of %s has no value: set it in the module call", name, strings.TrimSuffix(s.block, ".")))
			}
			return cty.UnknownVal(cty.DynamicPseudoType), d
		}
		return p.convertVar(name, vd, v), d
	}
}

// eval evaluates expr in s. ex holds extra names (each, count, a dynamic block's iterator).
// Every Block the expression refers to is added to deps.
func (s *scope) eval(expr hcl.Expression, ex map[string]cty.Value, deps depset) (cty.Value, hcl.Diagnostics) {
	ctx := s.ctx(expr.Variables(), ex, deps)
	if n, ok := expr.(hclsyntax.Node); ok {
		hclsyntax.VisitAll(n, func(n hclsyntax.Node) hcl.Diagnostics {
			if f, ok := n.(*hclsyntax.FunctionCallExpr); ok {
				if _, known := s.p.base.Functions[f.Name]; !known {
					if ctx.Functions == nil {
						ctx.Functions = map[string]function.Function{}
					}
					ctx.Functions[f.Name] = unknownFn
					if !quietFunctions[f.Name] {
						s.p.warn(fmt.Sprintf("pogi doesn't run the function %s(), so values made with it are unknown", f.Name))
					}
				}
			}
			return nil
		})
	}
	return expr.Value(ctx)
}

// evalOr is eval for places where a failure is not fatal: the value becomes Unknown, with a warning.
func (s *scope) evalOr(expr hcl.Expression, ex map[string]cty.Value, deps depset, what string) cty.Value {
	v, diags := s.eval(expr, ex, deps)
	if diags.HasErrors() {
		s.p.warn(fmt.Sprintf("%s: could not work out the value: %s", strings.TrimSuffix(what, "."), diags[0].Summary))
		return cty.UnknownVal(cty.DynamicPseudoType)
	}
	return v
}

var emptyPath = cty.ObjectVal(map[string]cty.Value{"module": cty.StringVal("."), "root": cty.StringVal("."), "cwd": cty.StringVal(".")})

// ctx builds an evaluation context with the values the traversals refer to. It works out each
// referenced variable, local, resource and module output first, which orders the evaluation by
// dependency and finds cycles.
func (s *scope) ctx(travs []hcl.Traversal, ex map[string]cty.Value, deps depset) *hcl.EvalContext {
	p := s.p
	vars := map[string]cty.Value{"path": emptyPath, "terraform": cty.ObjectVal(map[string]cty.Value{"workspace": cty.StringVal("default")})}
	groups := map[string]map[string]cty.Value{} // root → name → value
	data := map[string]map[string]cty.Value{}
	outNames := map[string]map[string]bool{} // module name → output names asked for
	for k, v := range ex {
		vars[k] = v
	}
	put := func(g map[string]map[string]cty.Value, root, name string, v cty.Value) {
		if g[root] == nil {
			g[root] = map[string]cty.Value{}
		}
		g[root][name] = v
	}
	attr := func(t hcl.Traversal, i int) (string, bool) {
		if i < len(t) {
			if a, ok := t[i].(hcl.TraverseAttr); ok {
				return a.Name, true
			}
		}
		return "", false
	}
	for _, t := range travs {
		root := t.RootName()
		if _, ok := ex[root]; ok {
			continue
		}
		name, ok := attr(t, 1)
		if !ok {
			continue
		}
		switch root {
		case "var", "local":
			l := s.vars[name]
			if root == "local" {
				l = s.locals[name]
			}
			if l == nil {
				put(groups, root, name, cty.UnknownVal(cty.DynamicPseudoType))
				p.warn(fmt.Sprintf("%s.%s is not declared", root, name))
				continue
			}
			v, d := p.get(l)
			deps.add(d)
			put(groups, root, name, v)
		case "module":
			if outNames[name] == nil {
				outNames[name] = map[string]bool{}
			}
			if o, ok := attr(t, 2); ok && outNames[name] != nil {
				outNames[name][o] = true
			} else {
				outNames[name][""] = true // the whole module
			}
		case "data":
			typ, ok1 := attr(t, 2)
			e := s.res["data."+name+"."+typ]
			if !ok1 || e == nil {
				continue
			}
			v := e.value(deps)
			if cur, ok := data[name][typ]; ok {
				v = cur
			}
			put(data, name, typ, patch(v, t[3:]))
		default:
			e := s.res[root+"."+name]
			if e == nil {
				continue
			}
			v := e.value(deps)
			if cur, ok := groups[root][name]; ok {
				v = cur
			}
			put(groups, root, name, patch(v, t[2:]))
		}
	}
	for name, outs := range outNames {
		if e := s.calls[name]; e != nil {
			put(groups, "module", name, e.value(outs, deps))
		}
	}
	for root, g := range groups {
		vars[root] = cty.ObjectVal(g)
	}
	if len(data) > 0 {
		d := map[string]cty.Value{}
		for typ, g := range data {
			d[typ] = cty.ObjectVal(g)
		}
		vars["data"] = cty.ObjectVal(d)
	}
	child := p.base.NewChild()
	child.Variables = vars
	return child
}

// patch adds Unknown attributes along a traversal that a resource's configuration does not have
// (a computed id, an arn), so that referring to them gives Unknown instead of an error.
func patch(v cty.Value, t hcl.Traversal) cty.Value {
	if len(t) == 0 || !v.IsKnown() || v.IsNull() {
		return v
	}
	ty := v.Type()
	switch s := t[0].(type) {
	case hcl.TraverseAttr:
		if ty.IsObjectType() {
			m := v.AsValueMap()
			if m == nil {
				m = map[string]cty.Value{}
			}
			if cur, ok := m[s.Name]; ok {
				m[s.Name] = patch(cur, t[1:])
			} else {
				m[s.Name] = cty.UnknownVal(cty.DynamicPseudoType)
			}
			return cty.ObjectVal(m)
		}
	case hcl.TraverseIndex:
		k := s.Key
		switch {
		case ty.IsTupleType() && k.Type() == cty.Number && k.IsKnown():
			i, _ := k.AsBigFloat().Int64()
			els := v.AsValueSlice()
			if i >= 0 && int(i) < len(els) {
				els[i] = patch(els[i], t[1:])
				return cty.TupleVal(els)
			}
		case ty.IsObjectType() && k.Type() == cty.String && k.IsKnown():
			return patch(v, append(hcl.Traversal{hcl.TraverseAttr{Name: k.AsString()}}, t[1:]...))
		}
	}
	return v
}

// instance is one element of a count or for_each.
type instance struct {
	name   string // the for_each key; "" otherwise
	suffix string // [0] or ["k"]; "" for a single instance
	ex     map[string]cty.Value
}

const (
	kSingle = iota
	kCount
	kEach
)

// instances expands a count or for_each expression (either may be nil).
func (s *scope) instances(count, forEach hcl.Expression, what string, deps depset) (int, []instance) {
	p := s.p
	one := []instance{{}}
	switch {
	case count != nil:
		v := s.evalOr(count, nil, deps, what+" count")
		var n int
		if f, ok := numberOf(v); ok && f >= 0 {
			n = int(f)
		} else {
			p.warn(fmt.Sprintf("%s: count is not known, so 1 instance is assumed", what))
			return kSingle, one
		}
		if n > maxInstances {
			p.fail(fmt.Errorf("%s: count %d is over the limit of %d instances", what, n, maxInstances))
			return kSingle, nil
		}
		out := make([]instance, n)
		for i := range out {
			out[i] = instance{suffix: "[" + strconv.Itoa(i) + "]", ex: map[string]cty.Value{"count": cty.ObjectVal(map[string]cty.Value{"index": cty.NumberIntVal(int64(i))})}}
		}
		return kCount, out
	case forEach != nil:
		v := s.evalOr(forEach, nil, deps, what+" for_each")
		unknown := cty.UnknownVal(cty.DynamicPseudoType)
		fallback := []instance{{ex: map[string]cty.Value{"each": cty.ObjectVal(map[string]cty.Value{"key": unknown, "value": unknown})}}}
		t := v.Type()
		if !v.IsKnown() || v.IsNull() || !(t.IsMapType() || t.IsObjectType() || t.IsSetType() || t.IsListType() || t.IsTupleType()) {
			p.warn(fmt.Sprintf("%s: for_each is not known, so 1 instance is assumed", what))
			return kSingle, fallback
		}
		if v.LengthInt() > maxInstances {
			p.fail(fmt.Errorf("%s: for_each has %d elements, over the limit of %d instances", what, v.LengthInt(), maxInstances))
			return kSingle, nil
		}
		var out []instance
		for it := v.ElementIterator(); it.Next(); {
			k, e := it.Element()
			if t.IsSetType() || t.IsListType() || t.IsTupleType() {
				if e.Type() != cty.String || !e.IsKnown() || e.IsNull() {
					p.warn(fmt.Sprintf("%s: for_each has an element that is not a known string, so 1 instance is assumed", what))
					return kSingle, fallback
				}
				k = e
			}
			if !k.IsKnown() {
				p.warn(fmt.Sprintf("%s: for_each has a key that is not known, so 1 instance is assumed", what))
				return kSingle, fallback
			}
			out = append(out, instance{name: k.AsString(), suffix: "[" + strconv.Quote(k.AsString()) + "]",
				ex: map[string]cty.Value{"each": cty.ObjectVal(map[string]cty.Value{"key": k, "value": e})}})
		}
		return kEach, out
	}
	return kSingle, one
}

func numberOf(v cty.Value) (float64, bool) {
	if v.Type() != cty.Number || !v.IsKnown() || v.IsNull() {
		return 0, false
	}
	f, _ := new(big.Float).Set(v.AsBigFloat()).Float64()
	return f, true
}

// shape combines the values of a block's instances the way Terraform does: one value, a tuple
// (count) or an object keyed by the for_each key.
func shape(kind int, insts []instance, vals []cty.Value) cty.Value {
	switch kind {
	case kCount:
		if len(vals) == 0 {
			return cty.EmptyTupleVal
		}
		return cty.TupleVal(vals)
	case kEach:
		m := map[string]cty.Value{}
		for i, in := range insts {
			m[in.name] = vals[i]
		}
		if len(m) == 0 {
			return cty.EmptyObjectVal
		}
		return cty.ObjectVal(m)
	}
	if len(vals) == 0 {
		return cty.UnknownVal(cty.DynamicPseudoType)
	}
	return vals[0]
}

// toGo converts a value to the shape of Terraform's JSON; anything not known becomes Unknown{}.
func toGo(v cty.Value) any {
	if !v.IsKnown() {
		return Unknown{}
	}
	if v.IsNull() {
		return nil
	}
	t := v.Type()
	switch {
	case t == cty.String:
		return v.AsString()
	case t == cty.Number:
		f, _ := numberOf(v)
		return f
	case t == cty.Bool:
		return v.True()
	case t.IsListType() || t.IsSetType() || t.IsTupleType():
		out := []any{}
		for it := v.ElementIterator(); it.Next(); {
			_, e := it.Element()
			out = append(out, toGo(e))
		}
		return out
	case t.IsMapType() || t.IsObjectType():
		out := map[string]any{}
		for it := v.ElementIterator(); it.Next(); {
			k, e := it.Element()
			out[k.AsString()] = toGo(e)
		}
		return out
	}
	return Unknown{}
}

func sortedKeys(d depset) []string {
	out := make([]string, 0, len(d))
	for k := range d {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func hasVal(m map[string]cty.Value, k string) bool { _, ok := m[k]; return ok }
