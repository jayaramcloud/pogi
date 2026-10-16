package appsync

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// The subset of the Velocity Template Language that AppSync mapping templates
// use: references with properties, method calls and indexes; #set, #if,
// #elseif, #else, #foreach, #break and #return; comments; map, list and
// range literals; and $util. Values are nil, bool, int64, float64, string,
// map[string]any and *[]any (so methods can grow lists in place).
// ponytail: no #macro, #define, #parse or #stop.

type vnode any

type (
	vtext string
	vout  struct { // a reference in text; null prints the reference itself unless quiet ($!x)
		ref   *vref
		quiet bool
		raw   string
	}
	vset struct {
		target *vref
		value  vexpr
	}
	vif struct {
		conds  []vexpr
		blocks [][]vnode
		els    []vnode
	}
	vfor struct {
		name string
		list vexpr
		body []vnode
	}
	vret   struct{ value vexpr } // value may be nil
	vbreak struct{}
)

type vexpr any

type (
	vlit struct{ v any }
	vstr []vnode // a "string" with references in it
	vref struct {
		root  string
		steps []vstep
	}
	vstep struct {
		name  string
		call  bool
		args  []vexpr
		index vexpr
	}
	vlist  []vexpr
	vrange struct{ from, to vexpr }
	vmap   struct{ keys, vals []vexpr }
	vop    struct {
		op   string
		l, r vexpr // l is nil for unary operators
	}
)

type vtlSyntax string

func (e vtlSyntax) Error() string { return string(e) }

type vparser struct {
	s string
	i int
}

func parseVTL(s string) (nodes []vnode, err error) {
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(vtlSyntax)
			if !ok {
				panic(r)
			}
			err = e
		}
	}()
	p := &vparser{s: s}
	nodes, end := p.block()
	if end != "" {
		p.fail("unexpected #" + end)
	}
	return nodes, nil
}

func (p *vparser) fail(msg string) {
	line := strings.Count(p.s[:min(p.i, len(p.s))], "\n") + 1
	col := p.i - strings.LastIndexByte(p.s[:min(p.i, len(p.s))], '\n')
	panic(vtlSyntax(fmt.Sprintf("Template syntax error at line %d, column %d: %s", line, col, msg)))
}

func (p *vparser) peek(s string) bool { return strings.HasPrefix(p.s[p.i:], s) }

func (p *vparser) space() {
	for p.i < len(p.s) && strings.IndexByte(" \t\r\n", p.s[p.i]) >= 0 {
		p.i++
	}
}

func (p *vparser) expect(s string) {
	p.space()
	if !p.peek(s) {
		p.fail("expected " + strconv.Quote(s))
	}
	p.i += len(s)
}

func identStart(c byte) bool { return c == '_' || 'a' <= c|0x20 && c|0x20 <= 'z' }
func ident(c byte) bool      { return identStart(c) || '0' <= c && c <= '9' }

func (p *vparser) ident() string {
	j := p.i
	for j < len(p.s) && ident(p.s[j]) {
		j++
	}
	if j == p.i {
		p.fail("expected a name")
	}
	name := p.s[p.i:j]
	p.i = j
	return name
}

// word consumes a keyword (and, in, true...) that is not part of a longer name.
func (p *vparser) word(w string) bool {
	p.space()
	if p.peek(w) && (p.i+len(w) == len(p.s) || !ident(p.s[p.i+len(w)])) {
		p.i += len(w)
		return true
	}
	return false
}

// block parses text and directives up to #end, #else or #elseif (returned) or the end.
func (p *vparser) block() ([]vnode, string) {
	var nodes []vnode
	var text strings.Builder
	flush := func() {
		if text.Len() > 0 {
			nodes = append(nodes, vtext(text.String()))
			text.Reset()
		}
	}
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == '\\' && p.i+1 < len(p.s) && (p.s[p.i+1] == '$' || p.s[p.i+1] == '#'):
			text.WriteByte(p.s[p.i+1])
			p.i += 2
		case p.peek("##"):
			if j := strings.IndexByte(p.s[p.i:], '\n'); j >= 0 {
				p.i += j
			} else {
				p.i = len(p.s)
			}
		case p.peek("#*"):
			j := strings.Index(p.s[p.i+2:], "*#")
			if j < 0 {
				p.fail("unclosed comment")
			}
			p.i += j + 4
		case c == '#':
			name := p.directive()
			switch name {
			case "":
				text.WriteByte('#')
				p.i++
			case "end", "else", "elseif":
				flush()
				return nodes, name
			default:
				flush()
				nodes = append(nodes, p.parseDirective(name))
			}
		case c == '$':
			start := p.i
			if r, quiet := p.textRef(); r != nil {
				flush()
				nodes = append(nodes, &vout{ref: r, quiet: quiet, raw: p.s[start:p.i]})
			} else {
				text.WriteByte('$')
				p.i = start + 1
			}
		default:
			text.WriteByte(c)
			p.i++
		}
	}
	flush()
	return nodes, ""
}

var directives = []string{"elseif", "else", "end", "set", "if", "foreach", "break", "return"}

// directive reads #name or #{name} at a '#'; "" when no directive starts there.
func (p *vparser) directive() string {
	rest := p.s[p.i+1:]
	braced := strings.HasPrefix(rest, "{")
	if braced {
		rest = rest[1:]
	}
	for _, d := range directives {
		after, ok := strings.CutPrefix(rest, d)
		switch {
		case !ok:
		case braced && strings.HasPrefix(after, "}"):
			p.i += len(d) + 3
			return d
		case !braced && (after == "" || !ident(after[0])):
			p.i += len(d) + 1
			return d
		}
	}
	return ""
}

func (p *vparser) parens() vexpr {
	p.expect("(")
	e := p.expr()
	p.expect(")")
	return e
}

func (p *vparser) parseDirective(name string) vnode {
	switch name {
	case "set":
		p.expect("(")
		p.space()
		target, ok := p.primary().(*vref)
		if !ok {
			p.fail("#set needs a reference")
		}
		p.expect("=")
		v := p.expr()
		p.expect(")")
		return &vset{target, v}
	case "if":
		n := &vif{}
		cond := p.parens()
		for {
			body, end := p.block()
			n.conds, n.blocks = append(n.conds, cond), append(n.blocks, body)
			switch end {
			case "elseif":
				cond = p.parens()
				continue
			case "else":
				if n.els, end = p.block(); end != "end" {
					p.fail("#else without #end")
				}
			case "":
				p.fail("#if without #end")
			}
			return n
		}
	case "foreach":
		p.expect("(")
		p.expect("$")
		name := p.ident()
		if !p.word("in") {
			p.fail("expected in")
		}
		list := p.expr()
		p.expect(")")
		body, end := p.block()
		if end != "end" {
			p.fail("#foreach without #end")
		}
		return &vfor{name, list, body}
	case "return":
		if p.peek("(") {
			return &vret{p.parens()}
		}
		return &vret{}
	}
	return &vbreak{}
}

// textRef reads $x.y, $!x, ${x} or $x.m(args)[i] in text; nil when there is no reference.
func (p *vparser) textRef() (*vref, bool) {
	i := p.i + 1
	quiet := i < len(p.s) && p.s[i] == '!'
	if quiet {
		i++
	}
	braced := i < len(p.s) && p.s[i] == '{'
	if braced {
		i++
	}
	if i >= len(p.s) || !identStart(p.s[i]) {
		return nil, false
	}
	p.i = i
	r := p.refBody()
	if braced {
		if !p.peek("}") {
			return nil, false
		}
		p.i++
	}
	return r, quiet
}

func (p *vparser) refBody() *vref {
	r := &vref{root: p.ident()}
	for p.i < len(p.s) {
		switch {
		case p.s[p.i] == '.' && p.i+1 < len(p.s) && identStart(p.s[p.i+1]):
			p.i++
			st := vstep{name: p.ident()}
			if p.peek("(") {
				p.i++
				st.call, st.args = true, p.args()
			}
			r.steps = append(r.steps, st)
		case p.s[p.i] == '[':
			p.i++
			r.steps = append(r.steps, vstep{index: p.expr()})
			p.expect("]")
		default:
			return r
		}
	}
	return r
}

func (p *vparser) args() []vexpr {
	var a []vexpr
	if p.space(); p.peek(")") {
		p.i++
		return a
	}
	for {
		a = append(a, p.expr())
		if p.space(); p.peek(",") {
			p.i++
			continue
		}
		p.expect(")")
		return a
	}
}

// op consumes an operator, given as a symbol and a word ("&&" and "and").
func (p *vparser) op(sym, word string) bool {
	p.space()
	if p.peek(sym) {
		p.i += len(sym)
		return true
	}
	return word != "" && p.word(word)
}

func (p *vparser) expr() vexpr {
	l := p.and()
	for p.op("||", "or") {
		l = &vop{"||", l, p.and()}
	}
	return l
}

func (p *vparser) and() vexpr {
	l := p.not()
	for p.op("&&", "and") {
		l = &vop{"&&", l, p.not()}
	}
	return l
}

func (p *vparser) not() vexpr {
	if p.op("!", "not") {
		return &vop{op: "!", r: p.not()}
	}
	l := p.sum()
	for _, o := range [][2]string{{"==", "eq"}, {"!=", "ne"}, {"<=", "le"}, {">=", "ge"}, {"<", "lt"}, {">", "gt"}} {
		if p.op(o[0], o[1]) {
			return &vop{o[0], l, p.sum()}
		}
	}
	return l
}

func (p *vparser) sum() vexpr {
	l := p.product()
	for {
		switch {
		case p.op("+", ""):
			l = &vop{"+", l, p.product()}
		case p.op("-", ""):
			l = &vop{"-", l, p.product()}
		default:
			return l
		}
	}
}

func (p *vparser) product() vexpr {
	l := p.unary()
	for {
		switch {
		case p.op("*", ""):
			l = &vop{"*", l, p.unary()}
		case p.op("/", ""):
			l = &vop{"/", l, p.unary()}
		case p.op("%", ""):
			l = &vop{"%", l, p.unary()}
		default:
			return l
		}
	}
}

func (p *vparser) unary() vexpr {
	if p.op("-", "") {
		return &vop{op: "neg", r: p.unary()}
	}
	return p.primary()
}

func (p *vparser) primary() vexpr {
	p.space()
	if p.i >= len(p.s) {
		p.fail("unexpected end of template")
	}
	switch c := p.s[p.i]; {
	case c == '$':
		p.i++
		if p.peek("!") {
			p.i++
		}
		if !p.peek("{") {
			return p.refBody()
		}
		p.i++
		r := p.refBody()
		p.expect("}")
		return r
	case c == '"' || c == '\'':
		return p.str(c)
	case c == '[':
		p.i++
		if p.space(); p.peek("]") {
			p.i++
			return vlist{}
		}
		l := vlist{p.expr()}
		if p.space(); p.peek("..") {
			p.i += 2
			to := p.expr()
			p.expect("]")
			return &vrange{l[0], to}
		}
		for p.space(); p.peek(","); p.space() {
			p.i++
			l = append(l, p.expr())
		}
		p.expect("]")
		return l
	case c == '{':
		p.i++
		m := &vmap{}
		if p.space(); p.peek("}") {
			p.i++
			return m
		}
		for {
			k := p.expr()
			p.expect(":")
			m.keys, m.vals = append(m.keys, k), append(m.vals, p.expr())
			if p.space(); p.peek(",") {
				p.i++
				continue
			}
			p.expect("}")
			return m
		}
	case c == '(':
		return p.parens()
	case '0' <= c && c <= '9':
		j := p.i
		for j < len(p.s) && '0' <= p.s[j] && p.s[j] <= '9' {
			j++
		}
		if j+1 < len(p.s) && p.s[j] == '.' && '0' <= p.s[j+1] && p.s[j+1] <= '9' {
			for j++; j < len(p.s) && '0' <= p.s[j] && p.s[j] <= '9'; j++ {
			}
			f, _ := strconv.ParseFloat(p.s[p.i:j], 64)
			p.i = j
			return &vlit{f}
		}
		n, _ := strconv.ParseInt(p.s[p.i:j], 10, 64)
		p.i = j
		return &vlit{n}
	}
	for _, w := range []struct {
		s string
		v any
	}{{"true", true}, {"false", false}, {"null", nil}} {
		if p.word(w.s) {
			return &vlit{w.v}
		}
	}
	p.fail("unexpected " + strconv.Quote(p.s[p.i:p.i+1]))
	return nil
}

// str reads a string literal; double-quoted ones interpolate references.
func (p *vparser) str(q byte) vexpr {
	var b strings.Builder
	for p.i++; p.i < len(p.s) && p.s[p.i] != q; p.i++ {
		if p.s[p.i] == '\\' && p.i+1 < len(p.s) && p.s[p.i+1] == q {
			p.i++
		}
		b.WriteByte(p.s[p.i])
	}
	if p.i >= len(p.s) {
		p.fail("unterminated string")
	}
	p.i++
	s := b.String()
	if q == '\'' || !strings.ContainsAny(s, "$#") {
		return &vlit{s}
	}
	nodes, err := parseVTL(s)
	if err != nil {
		p.fail(err.Error())
	}
	return vstr(nodes)
}

// ---- evaluation ----

// utilNS is $util or one of its namespaces ($util.dynamodb).
type utilNS string

type venv struct {
	rc       *rctx
	vars     map[string]any
	ret      any
	returned bool
}

// Ways out of a block.
const (
	vnext = iota
	vbrk
	vreturn
)

// renderVTL evaluates a template against rc; it returns the text and, if the
// template used #return, its value. Stash changes and appended errors land in rc.
func renderVTL(src string, rc *rctx) (out string, ret any, returned bool, err error) {
	nodes, err := parseVTL(src)
	if err != nil {
		return "", nil, false, &resolveErr{Message: err.Error(), Type: "MappingTemplate"}
	}
	ctx := vv(rc.ctx()).(map[string]any)
	ctx["args"] = ctx["arguments"]
	e := &venv{rc: rc, vars: map[string]any{"ctx": ctx, "context": ctx, "util": utilNS(""), "utils": utilNS("")}}
	defer func() {
		if r := recover(); r != nil {
			re, ok := r.(*resolveErr)
			if !ok {
				panic(r)
			}
			err = re
		}
		if rc.stash = asMap(ctx["stash"]); rc.stash == nil {
			rc.stash = map[string]any{}
		}
	}()
	var b strings.Builder
	e.exec(nodes, &b)
	return b.String(), e.ret, e.returned, nil
}

// runVTL renders a mapping template into the value its JSON output (or #return) gives.
func runVTL(tmpl string, rc *rctx) (any, bool, error) {
	out, ret, returned, err := renderVTL(tmpl, rc)
	switch {
	case err != nil:
		return nil, false, err
	case returned:
		var v any
		json.Unmarshal([]byte(toJSON(ret)), &v)
		return v, true, nil
	case strings.TrimSpace(out) == "":
		return nil, false, nil
	}
	var v any
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		return nil, false, &resolveErr{Message: "Unable to parse the JSON document: " + err.Error(), Type: "MappingTemplate"}
	}
	return v, false, nil
}

// vv converts a Go value into a VTL value: lists become *[]any, whole floats
// int64, and nil maps nil.
func vv(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if t == nil {
			return nil
		}
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = vv(e)
		}
		return m
	case header:
		m := map[string]any{}
		for k, e := range t {
			m[k] = e
		}
		return m
	case []any:
		l := make([]any, len(t))
		for i, e := range t {
			l[i] = vv(e)
		}
		return &l
	case []string:
		l := make([]any, len(t))
		for i, e := range t {
			l[i] = e
		}
		return &l
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1e15 {
			return int64(t)
		}
	case int:
		return int64(t)
	}
	return v
}

func (e *venv) exec(nodes []vnode, out *strings.Builder) int {
	for _, n := range nodes {
		switch n := n.(type) {
		case vtext:
			out.WriteString(string(n))
		case *vout:
			if v := e.ref(n.ref); v != nil {
				out.WriteString(render(v))
			} else if !n.quiet {
				out.WriteString(n.raw)
			}
		case *vset:
			e.assign(n.target, e.eval(n.value))
		case *vif:
			body := n.els
			for i, c := range n.conds {
				if truthy(e.eval(c)) {
					body = n.blocks[i]
					break
				}
			}
			if sig := e.exec(body, out); sig != vnext {
				return sig
			}
		case *vfor:
			items := iterate(e.eval(n.list))
			for i, it := range items {
				e.vars[n.name], e.vars["velocityCount"] = it, int64(i+1)
				e.vars["foreach"] = map[string]any{"index": int64(i), "count": int64(i + 1), "hasNext": i < len(items)-1,
					"first": i == 0, "last": i == len(items)-1}
				sig := e.exec(n.body, out)
				if sig == vbrk {
					break
				}
				if sig == vreturn {
					return sig
				}
			}
		case *vret:
			if n.value != nil {
				e.ret = e.eval(n.value)
			}
			e.returned = true
			return vreturn
		case *vbreak:
			return vbrk
		}
	}
	return vnext
}

func (e *venv) eval(x vexpr) any {
	switch x := x.(type) {
	case *vlit:
		return x.v
	case vstr:
		var b strings.Builder
		e.exec([]vnode(x), &b)
		return b.String()
	case *vref:
		return e.ref(x)
	case vlist:
		l := make([]any, len(x))
		for i, el := range x {
			l[i] = e.eval(el)
		}
		return &l
	case *vrange:
		from, to := toInt(e.eval(x.from)), toInt(e.eval(x.to))
		l, step := []any{}, int64(1)
		if to < from {
			step = -1
		}
		for i := from; ; i += step {
			l = append(l, i)
			if i == to {
				break
			}
		}
		return &l
	case *vmap:
		m := map[string]any{}
		for i := range x.keys {
			m[render(e.eval(x.keys[i]))] = e.eval(x.vals[i])
		}
		return m
	case *vop:
		return e.op(x)
	}
	return nil
}

func (e *venv) ref(r *vref) any {
	v := e.vars[r.root]
	for _, st := range r.steps {
		switch {
		case st.index != nil:
			v = index(v, e.eval(st.index))
		case st.call:
			args := make([]any, len(st.args))
			for i, a := range st.args {
				args[i] = e.eval(a)
			}
			v = e.call(v, st.name, args)
		default:
			switch t := v.(type) {
			case map[string]any:
				v = t[st.name]
			case utilNS:
				v = utilNS(strings.TrimPrefix(string(t)+"."+st.name, "."))
			default:
				v = nil
			}
		}
	}
	return v
}

// assign is #set: a variable, or a key of a map or an index of a list.
func (e *venv) assign(r *vref, v any) {
	if len(r.steps) == 0 {
		e.vars[r.root] = v
		return
	}
	last := r.steps[len(r.steps)-1]
	switch parent := e.ref(&vref{r.root, r.steps[:len(r.steps)-1]}).(type) {
	case map[string]any:
		if last.index != nil {
			parent[render(e.eval(last.index))] = v
		} else if !last.call {
			parent[last.name] = v
		}
	case *[]any:
		if last.index == nil {
			return
		}
		if i := toInt(e.eval(last.index)); 0 <= i && int(i) < len(*parent) {
			(*parent)[i] = v
		}
	}
}

func index(v, i any) any {
	switch t := v.(type) {
	case map[string]any:
		return t[render(i)]
	case *[]any:
		if n := toInt(i); 0 <= n && int(n) < len(*t) {
			return (*t)[n]
		}
	}
	return nil
}

func iterate(v any) []any {
	switch t := v.(type) {
	case *[]any:
		return slices.Clone(*t)
	case map[string]any:
		var out []any
		for _, k := range core.SortedKeys(t) {
			out = append(out, t[k])
		}
		return out
	}
	return nil
}

func truthy(v any) bool { return v != nil && v != false }

func numeric(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

func toInt(v any) int64 {
	f, _ := numeric(v)
	return int64(f)
}

// equal compares numbers by value and anything else by its rendering, as Velocity does.
func equal(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	x, ok1 := numeric(a)
	y, ok2 := numeric(b)
	if ok1 && ok2 {
		return x == y
	}
	return render(a) == render(b)
}

func (e *venv) op(x *vop) any {
	switch x.op {
	case "!":
		return !truthy(e.eval(x.r))
	case "neg":
		switch n := e.eval(x.r).(type) {
		case int64:
			return -n
		case float64:
			return -n
		}
		return nil
	case "&&":
		return truthy(e.eval(x.l)) && truthy(e.eval(x.r))
	case "||":
		return truthy(e.eval(x.l)) || truthy(e.eval(x.r))
	}
	l, r := e.eval(x.l), e.eval(x.r)
	switch x.op {
	case "==":
		return equal(l, r)
	case "!=":
		return !equal(l, r)
	case "<", "<=", ">", ">=":
		var c int
		a, ok1 := numeric(l)
		b, ok2 := numeric(r)
		ls, ok3 := l.(string)
		rs, ok4 := r.(string)
		switch {
		case ok1 && ok2:
			c = cmp.Compare(a, b)
		case ok3 && ok4:
			c = strings.Compare(ls, rs)
		default:
			return false
		}
		switch x.op {
		case "<":
			return c < 0
		case "<=":
			return c <= 0
		case ">":
			return c > 0
		}
		return c >= 0
	}
	_, ls := l.(string)
	_, rs := r.(string)
	if x.op == "+" && (ls || rs) {
		return render(l) + render(r)
	}
	a, ok1 := l.(int64)
	b, ok2 := r.(int64)
	if ok1 && ok2 {
		switch x.op {
		case "+":
			return a + b
		case "-":
			return a - b
		case "*":
			return a * b
		case "/", "%":
			if b == 0 {
				return nil
			}
			if x.op == "/" {
				return a / b
			}
			return a % b
		}
	}
	f, ok1 := numeric(l)
	g, ok2 := numeric(r)
	if !ok1 || !ok2 {
		return nil
	}
	switch x.op {
	case "+":
		return f + g
	case "-":
		return f - g
	case "*":
		return f * g
	case "/":
		return f / g
	}
	return math.Mod(f, g)
}

// render is a value as template output.
func render(v any) string {
	switch t := v.(type) {
	case nil, utilNS:
		return ""
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	}
	return toJSON(v)
}

// call runs a method of a value, or a $util function.
func (e *venv) call(v any, name string, a []any) any {
	arg := func(i int) any {
		if i < len(a) {
			return a[i]
		}
		return nil
	}
	s := func(i int) string { return render(arg(i)) }
	switch t := v.(type) {
	case utilNS:
		if f := vtlUtil[strings.TrimPrefix(string(t)+"."+name, ".")]; f != nil {
			return f(e, a)
		}
		return nil
	case map[string]any:
		switch name {
		case "put":
			old := t[s(0)]
			t[s(0)] = arg(1)
			return old
		case "putAll":
			for k, x := range asMap(arg(0)) {
				t[k] = x
			}
			return ""
		case "get":
			return t[s(0)]
		case "getOrDefault":
			if x, ok := t[s(0)]; ok {
				return x
			}
			return arg(1)
		case "containsKey":
			_, ok := t[s(0)]
			return ok
		case "remove":
			old := t[s(0)]
			delete(t, s(0))
			return old
		case "keySet", "values", "entrySet":
			l := []any{}
			for _, k := range core.SortedKeys(t) {
				switch name {
				case "keySet":
					l = append(l, k)
				case "values":
					l = append(l, t[k])
				default:
					l = append(l, map[string]any{"key": k, "value": t[k]})
				}
			}
			return &l
		case "size":
			return int64(len(t))
		case "isEmpty":
			return len(t) == 0
		case "clear":
			clear(t)
			return ""
		case "getKey", "getValue":
			return t[strings.ToLower(name[3:])]
		}
	case *[]any:
		switch name {
		case "add":
			if len(a) == 2 {
				i := min(max(int(toInt(a[0])), 0), len(*t))
				*t = slices.Insert(*t, i, a[1])
			} else {
				*t = append(*t, arg(0))
			}
			return true
		case "addAll":
			*t = append(*t, asList(arg(0))...)
			return true
		case "get":
			return index(t, arg(0))
		case "set":
			old := index(t, arg(0))
			if i := toInt(arg(0)); 0 <= i && int(i) < len(*t) {
				(*t)[i] = arg(1)
			}
			return old
		case "size":
			return int64(len(*t))
		case "isEmpty":
			return len(*t) == 0
		case "contains":
			return slices.ContainsFunc(*t, func(x any) bool { return equal(x, arg(0)) })
		case "indexOf":
			return int64(slices.IndexFunc(*t, func(x any) bool { return equal(x, arg(0)) }))
		case "remove":
			if i, ok := arg(0).(int64); ok {
				old := index(t, i)
				if 0 <= i && int(i) < len(*t) {
					*t = slices.Delete(*t, int(i), int(i)+1)
				}
				return old
			}
			i := slices.IndexFunc(*t, func(x any) bool { return equal(x, arg(0)) })
			if i >= 0 {
				*t = slices.Delete(*t, i, i+1)
			}
			return i >= 0
		case "clear":
			*t = (*t)[:0]
			return ""
		}
	case string:
		switch name {
		case "length", "size":
			return int64(utf8.RuneCountInString(t))
		case "isEmpty":
			return t == ""
		case "contains":
			return strings.Contains(t, s(0))
		case "startsWith":
			return strings.HasPrefix(t, s(0))
		case "endsWith":
			return strings.HasSuffix(t, s(0))
		case "equals":
			return t == s(0)
		case "equalsIgnoreCase":
			return strings.EqualFold(t, s(0))
		case "indexOf":
			return int64(strings.Index(t, s(0)))
		case "lastIndexOf":
			return int64(strings.LastIndex(t, s(0)))
		case "toLowerCase":
			return strings.ToLower(t)
		case "toUpperCase":
			return strings.ToUpper(t)
		case "trim":
			return strings.TrimSpace(t)
		case "concat":
			return t + s(0)
		case "replace":
			return strings.ReplaceAll(t, s(0), s(1))
		case "matches":
			re, err := regexp.Compile("^(?:" + s(0) + ")$")
			return err == nil && re.MatchString(t)
		case "replaceAll", "split":
			re, err := regexp.Compile(s(0))
			if err != nil {
				return nil
			}
			if name == "replaceAll" {
				return re.ReplaceAllString(t, s(1))
			}
			l := []any{}
			for _, part := range re.Split(t, -1) {
				l = append(l, part)
			}
			for len(l) > 0 && l[len(l)-1] == "" { // Java drops trailing empty strings
				l = l[:len(l)-1]
			}
			return &l
		case "substring":
			from, to := min(max(int(toInt(arg(0))), 0), len(t)), len(t)
			if len(a) > 1 {
				to = min(max(int(toInt(arg(1))), from), len(t))
			}
			return t[from:to]
		case "charAt":
			if i := int(toInt(arg(0))); 0 <= i && i < len(t) {
				return t[i : i+1]
			}
		}
	case int64, float64:
		switch name {
		case "intValue", "longValue":
			return toInt(t)
		case "doubleValue", "floatValue":
			f, _ := numeric(t)
			return f
		}
	}
	if name == "toString" {
		return render(v)
	}
	return nil
}

// vtlUtil is $util, keyed by the function's path below it.
var vtlUtil = map[string]func(e *venv, a []any) any{
	"qr":        func(*venv, []any) any { return "" },
	"quiet":     func(*venv, []any) any { return "" },
	"toJson":    func(_ *venv, a []any) any { return toJSON(at(a, 0)) },
	"parseJson": func(_ *venv, a []any) any { return vv(parseJSON(render(at(a, 0)))) },
	"autoId":    func(*venv, []any) any { return core.UUID() },
	"isNull":    func(_ *venv, a []any) any { return at(a, 0) == nil },
	"isNullOrEmpty": func(_ *venv, a []any) any {
		return at(a, 0) == nil || render(at(a, 0)) == "" || isEmpty(at(a, 0))
	},
	"isNullOrBlank":        func(_ *venv, a []any) any { return strings.TrimSpace(render(at(a, 0))) == "" },
	"defaultIfNull":        func(_ *venv, a []any) any { return or(at(a, 0) == nil, a) },
	"defaultIfNullOrEmpty": func(_ *venv, a []any) any { return or(at(a, 0) == nil || render(at(a, 0)) == "", a) },
	"defaultIfNullOrBlank": func(_ *venv, a []any) any { return or(strings.TrimSpace(render(at(a, 0))) == "", a) },
	"isString":             func(_ *venv, a []any) any { _, ok := at(a, 0).(string); return ok },
	"isNumber":             func(_ *venv, a []any) any { _, ok := numeric(at(a, 0)); return ok },
	"isBoolean":            func(_ *venv, a []any) any { _, ok := at(a, 0).(bool); return ok },
	"isList":               func(_ *venv, a []any) any { _, ok := at(a, 0).(*[]any); return ok },
	"isMap":                func(_ *venv, a []any) any { _, ok := at(a, 0).(map[string]any); return ok },
	"matches": func(_ *venv, a []any) any {
		re, err := regexp.Compile("^(?:" + render(at(a, 0)) + ")$")
		return err == nil && re.MatchString(render(at(a, 1)))
	},
	"error": func(_ *venv, a []any) any {
		panic(&resolveErr{Message: render(at(a, 0)), Type: render(at(a, 1)), Data: plainValue(at(a, 2)), Info: plainValue(at(a, 3))})
	},
	"appendError": func(e *venv, a []any) any {
		e.rc.errors = append(e.rc.errors, &resolveErr{Message: render(at(a, 0)), Type: render(at(a, 1)), Data: plainValue(at(a, 2)), Info: plainValue(at(a, 3))})
		return ""
	},
	"unauthorized": func(e *venv, _ []any) any {
		panic(&resolveErr{Message: fmt.Sprintf("Not Authorized to access %v on type %v", e.rc.info["fieldName"], e.rc.info["parentTypeName"]), Type: "Unauthorized"})
	},
	"log.info":                                func(e *venv, a []any) any { e.rc.logs = append(e.rc.logs, logLine(a)); return "" },
	"log.error":                               func(e *venv, a []any) any { e.rc.logs = append(e.rc.logs, logLine(a)); return "" },
	"time.nowISO8601":                         func(*venv, []any) any { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") },
	"time.nowEpochSeconds":                    func(*venv, []any) any { return time.Now().Unix() },
	"time.nowEpochMilliSeconds":               func(*venv, []any) any { return time.Now().UnixMilli() },
	"dynamodb.toDynamoDB":                     func(_ *venv, a []any) any { return vv(toDDB(at(a, 0))) },
	"dynamodb.toDynamoDBJson":                 func(_ *venv, a []any) any { return toJSON(toDDB(at(a, 0))) },
	"dynamodb.toMapValues":                    func(_ *venv, a []any) any { return vv(toDDBMap(asMap(at(a, 0)))) },
	"dynamodb.toMapValuesJson":                func(_ *venv, a []any) any { return toJSON(toDDBMap(asMap(at(a, 0)))) },
	"dynamodb.toString":                       func(_ *venv, a []any) any { return vv(toDDB(render(at(a, 0)))) },
	"dynamodb.toStringJson":                   func(_ *venv, a []any) any { return toJSON(toDDB(render(at(a, 0)))) },
	"dynamodb.toNumber":                       func(_ *venv, a []any) any { return vv(toDDB(at(a, 0))) },
	"dynamodb.toNumberJson":                   func(_ *venv, a []any) any { return toJSON(toDDB(at(a, 0))) },
	"transform.toDynamoDBFilterExpression":    func(_ *venv, a []any) any { return toJSON(condition(asMap(at(a, 0)))) },
	"transform.toDynamoDBConditionExpression": func(_ *venv, a []any) any { return toJSON(condition(asMap(at(a, 0)))) },
	"rds.toJsonObject":                        func(_ *venv, a []any) any { return vv(rdsObjects(render(at(a, 0)))) },
	"rds.toJsonString":                        func(_ *venv, a []any) any { return toJSON(rdsObjects(render(at(a, 0)))) },
}

func at(a []any, i int) any {
	if i < len(a) {
		return a[i]
	}
	return nil
}

// or is defaultIfNull and friends: the default when empty, else the value.
func or(empty bool, a []any) any {
	if empty {
		return at(a, 1)
	}
	return at(a, 0)
}

func isEmpty(v any) bool {
	switch t := v.(type) {
	case *[]any:
		return len(*t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

func parseJSON(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		panic(&resolveErr{Message: "Unable to parse the JSON document: " + err.Error(), Type: "MappingTemplate"})
	}
	return v
}

// plainValue turns a VTL value back into plain JSON values.
func plainValue(v any) any {
	if v == nil {
		return nil
	}
	var out any
	json.Unmarshal([]byte(toJSON(v)), &out)
	return out
}

func logLine(a []any) string {
	var parts []string
	for _, v := range a {
		parts = append(parts, render(v))
	}
	return strings.Join(parts, " ")
}
