package stepfunctions

// A JSONata evaluator for Step Functions: the parser and the evaluator.
// The built-in functions are in jsonata_funcs.go.
//
// It follows jsonata.js: names are paths, a path result is a "sequence" (a
// single item collapses to the item itself, an empty one is undefined), and
// undefined is not the same as null.
//
// Not supported, on purpose: the parent operator (%), the focus and index
// bindings (@ and #), $eval, and the picture-string functions $formatInteger
// and $parseInteger. A definition that uses %, @ or # is rejected at create.

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"
)

// ---- values ----
//
// JSON values are nil, bool, float64, string, []any and map[string]any.
// The rest only live inside the evaluator.

type undefined struct{}

// undef is "no value": a missing field, an empty path, $exists(x) false.
var undef any = undefined{}

func isUndef(v any) bool { _, ok := v.(undefined); return ok }

// seq is a path result that may still collapse to one value.
type seq []any

type lambda struct {
	params []string
	body   *node
	env    *env
	input  any
}

type builtin struct {
	name     string
	min, max int  // argument counts; min counts the context argument, max < 0 means any
	ctx      byte // 0: no context argument; 'x' any kind; 's', 'n', 'a', 'o': the kind that is not replaced by the context
	fn       func(x *jx, a []any) (any, error)
}

// partial is a function with some arguments fixed; hole marks the open ones.
type partial struct {
	fn   any
	args []any
}

type hole struct{}

type jregex struct{ re *regexp.Regexp }

// jerr is a JSONata error with its documented code (S… syntax, T… type, D… data).
type jerr struct{ code, msg string }

func (e *jerr) Error() string { return e.code + ": " + e.msg }

func jfail(code, format string, a ...any) error { return &jerr{code, fmt.Sprintf(format, a...)} }

// ---- syntax tree ----

type node struct {
	typ    string // path name string number value variable wildcard descendant regex binary neg array object cond block bind call lambda partial apply transform sort
	val    any    // literal, name or operator
	lhs    *node  // call: the function; apply and bind: the left side; cond: the test
	rhs    *node
	els    *node
	args   []*node    // call and partial arguments, block and array items
	pairs  [][2]*node // object constructor
	steps  []*node    // path
	stages []*node    // filters on a path step
	preds  []*node    // filters on any other node
	group  *node      // an object constructor grouping this node's result: path{key: value}
	keep   bool       // a [] after the node: keep a single item as an array
	cons   bool       // an array constructor at the start or end of a path: do not flatten it
	terms  []sortTerm
	params []string
	body   *node
	upd    *node // transform
	del    *node
}

type sortTerm struct {
	expr *node
	desc bool
}

// ---- lexer ----

type token struct {
	typ string // end name variable string number value op regex function
	val any
	pos int
}

type parser struct {
	src string
	pos int
	tok token
}

const delims = " \t\n\r[]{}(),@#;:?+-*/%|=<>^&!~."

var ops2 = []string{"..", ":=", "!=", ">=", "<=", "~>", "?:", "??", "**"}

func (p *parser) fail(code, format string, a ...any) {
	panic(&jerr{code, fmt.Sprintf(format, a...) + fmt.Sprintf(" (position %d)", p.tok.pos)})
}

func (p *parser) lex(infix bool) token {
	for p.pos < len(p.src) {
		switch c := p.src[p.pos]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			p.pos++
			continue
		case strings.HasPrefix(p.src[p.pos:], "/*"):
			end := strings.Index(p.src[p.pos+2:], "*/")
			if end < 0 {
				p.fail("S0106", "Comment has no closing tag")
			}
			p.pos += end + 4
			continue
		}
		break
	}
	start := p.pos
	if p.pos >= len(p.src) {
		return token{"end", nil, start}
	}
	c := p.src[p.pos]
	if !infix && c == '/' {
		return p.regex()
	}
	for _, op := range ops2 {
		if strings.HasPrefix(p.src[p.pos:], op) {
			p.pos += len(op)
			return token{"op", op, start}
		}
	}
	switch {
	case strings.IndexByte("[]{}(),@#;:?+-*/%|=<>^&!~.", c) >= 0:
		p.pos++
		return token{"op", string(c), start}
	case c == '"' || c == '\'':
		return p.str(c)
	case c >= '0' && c <= '9':
		return p.number()
	case c == '`':
		end := strings.IndexByte(p.src[p.pos+1:], '`')
		if end < 0 {
			p.fail("S0105", "Quoted property name must be terminated with a backquote (`)")
		}
		p.pos += end + 2
		return token{"name", p.src[start+1 : start+1+end], start}
	case c == '$':
		p.pos++
		if p.pos < len(p.src) && p.src[p.pos] == '$' {
			p.pos++
			return token{"variable", "$", start}
		}
		return token{"variable", p.word(), start}
	}
	w := p.word()
	switch w {
	case "and", "or", "in":
		return token{"op", w, start}
	case "true", "false":
		return token{"value", w == "true", start}
	case "null":
		return token{"value", nil, start}
	case "function", "λ":
		return token{"function", w, start}
	}
	return token{"name", w, start}
}

// word reads characters up to the next delimiter.
func (p *parser) word() string {
	s := p.pos
	for p.pos < len(p.src) && strings.IndexByte(delims, p.src[p.pos]) < 0 {
		p.pos++
	}
	return p.src[s:p.pos]
}

func (p *parser) number() token {
	s := p.pos
	digits := func() {
		for p.pos < len(p.src) && p.src[p.pos] >= '0' && p.src[p.pos] <= '9' {
			p.pos++
		}
	}
	digits()
	if p.pos+1 < len(p.src) && p.src[p.pos] == '.' && p.src[p.pos+1] >= '0' && p.src[p.pos+1] <= '9' {
		p.pos++
		digits()
	}
	if p.pos < len(p.src) && (p.src[p.pos] == 'e' || p.src[p.pos] == 'E') {
		q := p.pos + 1
		if q < len(p.src) && (p.src[q] == '+' || p.src[q] == '-') {
			q++
		}
		if q < len(p.src) && p.src[q] >= '0' && p.src[q] <= '9' {
			p.pos = q
			digits()
		}
	}
	f, err := strconv.ParseFloat(p.src[s:p.pos], 64)
	if err != nil || math.IsInf(f, 0) {
		p.tok.pos = s
		p.fail("S0102", "Number out of range: %s", p.src[s:p.pos])
	}
	return token{"number", f, s}
}

func (p *parser) str(q byte) token {
	start := p.pos
	p.pos++
	var b strings.Builder
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		switch {
		case c == q:
			p.pos++
			return token{"string", b.String(), start}
		case c != '\\':
			b.WriteByte(c)
			p.pos++
			continue
		}
		p.pos++
		if p.pos >= len(p.src) {
			break
		}
		e := p.src[p.pos]
		p.pos++
		if esc := strings.IndexByte(`"\/bfnrt`, e); esc >= 0 {
			b.WriteByte("\"\\/\b\f\n\r\t"[esc])
			continue
		}
		if e == q {
			b.WriteByte(e)
			continue
		}
		if e != 'u' || p.pos+4 > len(p.src) {
			p.tok.pos = p.pos - 1
			p.fail("S0301", "Unsupported escape sequence: \\%c", e)
		}
		r, err := strconv.ParseUint(p.src[p.pos:p.pos+4], 16, 32)
		if err != nil {
			p.fail("S0104", "The escape sequence \\u must be followed by 4 hex digits")
		}
		p.pos += 4
		if utf16.IsSurrogate(rune(r)) && strings.HasPrefix(p.src[p.pos:], "\\u") && p.pos+6 <= len(p.src) {
			if r2, err := strconv.ParseUint(p.src[p.pos+2:p.pos+6], 16, 32); err == nil {
				if d := utf16.DecodeRune(rune(r), rune(r2)); d != utf8.RuneError {
					p.pos += 6
					b.WriteRune(d)
					continue
				}
			}
		}
		b.WriteRune(rune(r))
	}
	p.tok.pos = start
	p.fail("S0101", "String literal must be terminated by a matching quote")
	return token{}
}

func (p *parser) regex() token {
	start := p.pos
	p.pos++
	depth := 0
	for ; p.pos < len(p.src); p.pos++ {
		switch p.src[p.pos] {
		case '\\':
			p.pos++
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '/':
			if depth > 0 {
				continue
			}
			pat := p.src[start+1 : p.pos]
			p.pos++
			fs := p.pos
			for p.pos < len(p.src) && strings.IndexByte("ims", p.src[p.pos]) >= 0 {
				p.pos++
			}
			if pat == "" {
				p.tok.pos = start
				p.fail("S0301", "Empty regular expressions are not allowed")
			}
			flags := p.src[fs:p.pos]
			if flags != "" {
				pat = "(?" + flags + ")" + pat
			}
			re, err := regexp.Compile(pat)
			if err != nil {
				p.tok.pos = start
				p.fail("S0303", "Invalid regular expression: %v", err)
			}
			return token{"regex", &jregex{re}, start}
		}
	}
	p.tok.pos = start
	p.fail("S0302", "No terminating / in regular expression")
	return token{}
}

// ---- parser ----

var infixBP = map[string]int{".": 75, "[": 80, "(": 80, "{": 70, "^": 40, "?": 20, "+": 50, "-": 50, "*": 60, "/": 60, "%": 60,
	"=": 40, "!=": 40, "<": 40, ">": 40, "<=": 40, ">=": 40, "&": 50, "and": 30, "or": 25, "in": 40, "..": 20, ":=": 10,
	"~>": 40, "?:": 40, "??": 40, "@": 80, "#": 80}

func (p *parser) advance(infix bool) { p.tok = p.lex(infix) }

func (p *parser) isOp(op string) bool { return p.tok.typ == "op" && p.tok.val == op }

// expect consumes op. After a closing bracket the next token is an infix operator, so a / is a division.
func (p *parser) expect(op string) {
	if !p.isOp(op) {
		got := "end of the expression"
		if p.tok.typ != "end" {
			got = fmt.Sprint(p.tok.val)
		}
		p.fail("S0202", "Syntax error: expected '%s' but got '%s'", op, got)
	}
	p.advance(op == ")" || op == "]" || op == "}")
}

// jsonataCache holds parsed expressions; the same few are evaluated again and again.
var jsonataCache sync.Map // string → *node

// jsonataParse parses a JSONata expression.
func jsonataParse(src string) (n *node, err error) {
	if c, ok := jsonataCache.Load(src); ok {
		return c.(*node), nil
	}
	defer func() {
		if r := recover(); r != nil {
			je, ok := r.(*jerr)
			if !ok {
				panic(r)
			}
			n, err = nil, je
		}
	}()
	p := &parser{src: src}
	p.advance(false)
	n = p.expr(0)
	if p.tok.typ != "end" {
		p.fail("S0201", "Syntax error: %q", fmt.Sprint(p.tok.val))
	}
	jsonataCache.Store(src, n)
	return n, nil
}

func (p *parser) expr(rbp int) *node {
	t := p.tok
	p.advance(true)
	left := p.nud(t)
	for p.tok.typ == "op" && rbp < infixBP[p.tok.val.(string)] {
		t = p.tok
		p.advance(false)
		left = p.led(t, left)
	}
	return left
}

func (p *parser) nud(t token) *node {
	switch t.typ {
	case "string":
		return &node{typ: "string", val: t.val}
	case "number":
		return &node{typ: "number", val: t.val}
	case "value":
		return &node{typ: "value", val: t.val}
	case "regex":
		return &node{typ: "regex", val: t.val}
	case "variable":
		return &node{typ: "variable", val: t.val}
	case "name":
		return &node{typ: "path", steps: []*node{{typ: "name", val: t.val}}}
	case "function":
		return p.lambda()
	case "op":
		switch t.val {
		case "*":
			return &node{typ: "wildcard"}
		case "**":
			return &node{typ: "descendant"}
		case "-":
			e := p.expr(70)
			if e.typ == "number" {
				return &node{typ: "number", val: -e.val.(float64)}
			}
			return &node{typ: "neg", lhs: e}
		case "(":
			b := &node{typ: "block"}
			for !p.isOp(")") {
				b.args = append(b.args, p.expr(0))
				if !p.isOp(";") {
					break
				}
				p.advance(false)
				if p.isOp(")") {
					break
				}
			}
			p.expect(")")
			return b
		case "[":
			a := &node{typ: "array"}
			for !p.isOp("]") {
				a.args = append(a.args, p.expr(0))
				if !p.isOp(",") {
					break
				}
				p.advance(false)
			}
			p.expect("]")
			return a
		case "{":
			return p.object(&node{typ: "object"})
		case "|":
			n := &node{typ: "transform", lhs: p.expr(0)}
			p.expect("|")
			n.upd = p.expr(0)
			if p.isOp(",") {
				p.advance(false)
				n.del = p.expr(0)
			}
			if !p.isOp("|") {
				p.fail("S0202", "Syntax error: expected '|' to end the transform")
			}
			p.advance(true)
			return n
		case "%", "@", "#":
			p.tok = t
			p.fail("S0201", "The operator '%s' is not supported by pogi", t.val)
		}
	}
	p.tok = t
	if t.typ == "end" {
		p.fail("S0207", "Syntax error: unexpected end of the expression")
	}
	p.fail("S0211", "The symbol '%v' cannot be used as a unary operator", t.val)
	return nil
}

// object parses the pairs of an object constructor after its {.
func (p *parser) object(n *node) *node {
	for !p.isOp("}") {
		k := p.expr(0)
		p.expect(":")
		n.pairs = append(n.pairs, [2]*node{k, p.expr(0)})
		if !p.isOp(",") {
			break
		}
		p.advance(false)
	}
	p.expect("}")
	return n
}

func (p *parser) lambda() *node {
	p.expect("(")
	n := &node{typ: "lambda"}
	for !p.isOp(")") {
		if p.tok.typ != "variable" {
			p.fail("S0208", "Parameters of a function definition must be variables")
		}
		n.params = append(n.params, p.tok.val.(string))
		p.advance(false)
		if !p.isOp(",") {
			break
		}
		p.advance(false)
	}
	p.expect(")")
	if p.isOp("<") { // a signature: not checked
		depth := 0
		for ; p.tok.typ != "end"; p.advance(false) {
			if p.isOp("<") {
				depth++
			} else if p.isOp(">") {
				if depth--; depth == 0 {
					break
				}
			}
		}
		p.advance(false)
	}
	p.expect("{")
	n.body = p.expr(0)
	p.expect("}")
	return n
}

func (p *parser) led(t token, left *node) *node {
	op := t.val.(string)
	switch op {
	case ".":
		return p.dot(left, p.expr(75))
	case "[":
		if p.isOp("]") { // a[]: keep a single item as an array
			p.advance(true)
			if left.typ == "path" {
				left.steps[len(left.steps)-1].keep = true
				left.keep = true
			} else {
				left.keep = true
			}
			return left
		}
		f := p.expr(0)
		p.expect("]")
		if left.typ == "path" {
			last := left.steps[len(left.steps)-1]
			last.stages = append(last.stages, f)
		} else {
			left.preds = append(left.preds, f)
		}
		return left
	case "{":
		g := p.object(&node{typ: "object"})
		left.group = g
		return left
	case "(":
		call := &node{typ: "call", lhs: left}
		partial := false
		for !p.isOp(")") {
			if p.isOp("?") {
				partial = true
				call.args = append(call.args, &node{typ: "hole"})
				p.advance(false)
			} else {
				call.args = append(call.args, p.expr(0))
			}
			if !p.isOp(",") {
				break
			}
			p.advance(false)
		}
		p.expect(")")
		if partial {
			call.typ = "partial"
		}
		return call
	case "^":
		p.expect("(")
		n := &node{typ: "sort"}
		for {
			term := sortTerm{}
			if p.isOp("<") {
				p.advance(false)
			} else if p.isOp(">") {
				term.desc = true
				p.advance(false)
			}
			term.expr = p.expr(0)
			n.terms = append(n.terms, term)
			if !p.isOp(",") {
				break
			}
			p.advance(false)
		}
		p.expect(")")
		if left.typ != "path" {
			left = &node{typ: "path", steps: []*node{left}}
		}
		left.steps = append(left.steps, n)
		return left
	case "?":
		n := &node{typ: "cond", lhs: left, rhs: p.expr(0)}
		if p.isOp(":") {
			p.advance(false)
			n.els = p.expr(0)
		}
		return n
	case ":=":
		if left.typ != "variable" {
			p.fail("S0212", "The left side of := must be a variable name")
		}
		return &node{typ: "bind", val: left.val, rhs: p.expr(infixBP[op] - 1)}
	case "~>":
		return &node{typ: "apply", lhs: left, rhs: p.expr(infixBP[op])}
	case "@", "#":
		p.fail("S0201", "The operator '%s' is not supported by pogi", op)
	}
	return &node{typ: "binary", val: op, lhs: left, rhs: p.expr(infixBP[op])}
}

// dot joins two nodes into one path.
func (p *parser) dot(l, r *node) *node {
	if l.typ != "path" {
		l = &node{typ: "path", steps: []*node{l}}
	}
	if r.typ == "path" {
		l.steps = append(l.steps, r.steps...)
		l.keep = l.keep || r.keep
		if r.group != nil {
			l.group = r.group
		}
	} else {
		if r.typ == "number" || r.typ == "value" {
			p.fail("S0213", "The literal value %v cannot be used as a step within a path expression", r.val)
		}
		if r.typ == "string" {
			r = &node{typ: "name", val: r.val}
		}
		r.stages, r.preds = r.preds, nil
		l.steps = append(l.steps, r)
		l.keep = l.keep || r.keep
	}
	for i, st := range l.steps { // an array constructor at either end is not flattened
		st.cons = st.typ == "array" && (i == 0 || i == len(l.steps)-1)
	}
	return l
}

// ---- evaluator ----

// env is a frame of variables; variables in an inner frame hide outer ones.
type env struct {
	vars   map[string]any
	parent *env
}

func (e *env) lookup(name string) (any, bool) {
	for ; e != nil; e = e.parent {
		if v, ok := e.vars[name]; ok {
			return v, true
		}
	}
	return nil, false
}

// jx is the state of one evaluation.
type jx struct {
	root  any // $$
	now   int64
	depth int
	steps int
}

const (
	maxDepth = 10000    // nested function calls
	maxRange = 1000000  // items the .. operator may create (jsonata.js allows 1e7)
	maxSteps = 20000000 // evaluated nodes: a path over huge lists must end
)

// jsonataEval evaluates a parsed expression with the context value input and the
// variables vars. The result is a plain JSON value; ok is false when it is undefined.
func jsonataEval(n *node, input any, vars map[string]any) (out any, ok bool, err error) {
	x := &jx{root: input, now: nowMillis()}
	v, err := x.eval(n, input, &env{vars: map[string]any{}, parent: &env{vars: vars}})
	if err != nil {
		return nil, false, err
	}
	if isUndef(v) {
		return nil, false, nil
	}
	if out, err = toJSON(v); err != nil {
		return nil, false, err
	}
	return out, true, nil
}

// jsonataRun parses and evaluates src.
func jsonataRun(src string, input any, vars map[string]any) (any, bool, error) {
	n, err := jsonataParse(src)
	if err != nil {
		return nil, false, err
	}
	return jsonataEval(n, input, vars)
}

// toJSON checks that a result holds only JSON values.
func toJSON(v any) (any, error) {
	switch t := v.(type) {
	case []any:
		for i, e := range t {
			r, err := toJSON(e)
			if err != nil {
				return nil, err
			}
			t[i] = r
		}
		return t, nil
	case seq:
		return toJSON([]any(t))
	case map[string]any:
		for k, e := range t {
			r, err := toJSON(e)
			if err != nil {
				return nil, err
			}
			t[k] = r
		}
		return t, nil
	case *lambda, *builtin, *partial, *jregex:
		return nil, jfail("D3070", "The result is a function or a regular expression, which has no JSON form")
	case undefined:
		return nil, jfail("D3071", "The result holds an undefined value")
	}
	return v, nil
}

func (x *jx) eval(n *node, in any, e *env) (any, error) {
	if x.steps++; x.steps > maxSteps {
		return nil, jfail("U1002", "The expression needs more than %d steps; it is too expensive to evaluate", maxSteps)
	}
	v, err := x.eval1(n, in, e)
	if err != nil {
		return nil, err
	}
	for _, f := range n.preds {
		if v, err = x.filter(f, v, e); err != nil {
			return nil, err
		}
	}
	if n.group != nil && n.typ != "path" {
		if v, err = x.groupBy(n.group, v, e); err != nil {
			return nil, err
		}
	}
	if s, ok := v.(seq); ok {
		switch {
		case len(s) == 0:
			v = undef
		case len(s) == 1 && !n.keep:
			v = s[0]
		default:
			v = []any(s)
		}
	}
	return v, nil
}

func (x *jx) eval1(n *node, in any, e *env) (any, error) {
	switch n.typ {
	case "path":
		return x.path(n, in, e)
	case "name":
		return lookupName(in, n.val.(string)), nil
	case "string", "number", "value":
		return n.val, nil
	case "regex":
		return n.val, nil
	case "variable":
		return x.variable(n.val.(string), in, e), nil
	case "wildcard":
		return wildcard(in), nil
	case "descendant":
		return descendants(in), nil
	case "neg":
		v, err := x.eval(n.lhs, in, e)
		if err != nil || isUndef(v) {
			return v, err
		}
		f, ok := v.(float64)
		if !ok {
			return nil, jfail("T1002", "The operand of the unary - operator must evaluate to a number")
		}
		return -f, nil
	case "array":
		out := []any{}
		for _, a := range n.args {
			v, err := x.eval(a, in, e)
			if err != nil {
				return nil, err
			}
			switch {
			case isUndef(v):
			case a.typ == "array":
				out = append(out, v)
			default:
				out = appendValue(out, v)
			}
		}
		return out, nil
	case "object":
		return x.groupBy(n, in, e)
	case "cond":
		c, err := x.eval(n.lhs, in, e)
		if err != nil {
			return nil, err
		}
		switch {
		case truthy(c):
			return x.eval(n.rhs, in, e)
		case n.els != nil:
			return x.eval(n.els, in, e)
		}
		return undef, nil
	case "block":
		inner := &env{vars: map[string]any{}, parent: e}
		v := undef
		for _, a := range n.args {
			var err error
			if v, err = x.eval(a, in, inner); err != nil {
				return nil, err
			}
		}
		return v, nil
	case "bind":
		v, err := x.eval(n.rhs, in, e)
		if err != nil {
			return nil, err
		}
		e.vars[n.val.(string)] = v
		return v, nil
	case "call", "partial":
		return x.call(n, in, e, nil)
	case "lambda":
		return &lambda{n.params, n.body, e, in}, nil
	case "apply":
		return x.applyOp(n, in, e)
	case "transform":
		return x.transform(n, e), nil
	case "binary":
		return x.binary(n, in, e)
	}
	return nil, jfail("S0201", "Unknown expression %s", n.typ)
}

func (x *jx) variable(name string, in any, e *env) any {
	switch name {
	case "":
		return in
	case "$":
		return x.root
	}
	if v, ok := e.lookup(name); ok {
		return v
	}
	if b := builtins[name]; b != nil {
		return b
	}
	return undef
}

// lookupName reads a field; on an array it reads the field of every member.
func lookupName(in any, key string) any {
	switch t := in.(type) {
	case []any:
		out := seq{}
		for _, m := range t {
			if r := lookupName(m, key); !isUndef(r) {
				out = appendFlat(out, r)
			}
		}
		return out
	case map[string]any:
		if v, ok := t[key]; ok {
			return v
		}
	}
	return undef
}

func isArr(v any) bool {
	switch v.(type) {
	case []any, seq:
		return true
	}
	return false
}

func items(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case seq:
		return t
	}
	return []any{v}
}

// appendFlat adds v, or the members of v when it is an array.
func appendFlat(out seq, v any) seq {
	if isArr(v) {
		return append(out, items(v)...)
	}
	return append(out, v)
}

func appendValue(out []any, v any) []any {
	if isArr(v) {
		return append(out, items(v)...)
	}
	return append(out, v)
}

func wildcard(in any) any {
	switch t := in.(type) {
	case map[string]any:
		out := seq{}
		for _, k := range sortedKeys(t) {
			out = appendFlat(out, t[k])
		}
		return out
	case []any:
		out := seq{}
		for _, m := range t {
			if r := wildcard(m); !isUndef(r) {
				out = append(out, items(r)...)
			}
		}
		return out
	}
	return seq{}
}

func descendants(in any) any {
	out := seq{}
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case []any:
			for _, m := range t {
				walk(m)
			}
		case map[string]any:
			out = append(out, t)
			for _, k := range sortedKeys(t) {
				walk(t[k])
			}
		default:
			out = append(out, v)
		}
	}
	if !isUndef(in) {
		walk(in)
	}
	return out
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// path evaluates the steps of a path one after the other.
func (x *jx) path(n *node, in any, e *env) (any, error) {
	var cur seq
	if a, ok := in.([]any); ok && n.steps[0].typ != "variable" {
		cur = seq(a)
	} else {
		cur = seq{in}
	}
	var res any = cur
	for i, st := range n.steps {
		r, whole, err := x.step(st, cur, e, i == len(n.steps)-1)
		if err != nil {
			return nil, err
		}
		if whole != nil {
			res = whole
			break
		}
		res, cur = r, r
		if len(r) == 0 {
			break
		}
	}
	if n.keep {
		if s, ok := res.(seq); ok {
			res = []any(s)
			if len(s) == 0 {
				res = undef
			}
		} else if !isArr(res) && !isUndef(res) {
			res = []any{res}
		}
	}
	if n.group != nil {
		if isUndef(res) {
			res = seq{}
		}
		return x.groupBy(n.group, res, e)
	}
	return res, nil
}

// step runs one step for every item. whole is set when the last step produced a single array, which is kept as it is.
func (x *jx) step(st *node, in seq, e *env, last bool) (seq, []any, error) {
	if st.typ == "sort" {
		r, err := x.sortStep(st, in, e)
		return r, nil, err
	}
	var out seq
	for _, item := range in {
		v, err := x.eval(st, item, e)
		if err != nil {
			return nil, nil, err
		}
		for _, f := range st.stages {
			if v, err = x.filter(f, v, e); err != nil {
				return nil, nil, err
			}
		}
		if !isUndef(v) {
			out = append(out, v)
		}
	}
	if last && len(out) == 1 && !st.cons {
		if a, ok := out[0].([]any); ok {
			return nil, a, nil
		}
	}
	var flat seq
	for _, v := range out {
		if isArr(v) && !st.cons {
			flat = append(flat, items(v)...)
		} else {
			flat = append(flat, v)
		}
	}
	return flat, nil, nil
}

// filter keeps the members of v that a predicate selects: by index, or by truth.
func (x *jx) filter(p *node, v any, e *env) (any, error) {
	out := seq{}
	if isUndef(v) {
		return out, nil
	}
	list := items(v)
	if p.typ == "number" {
		i := int(math.Floor(p.val.(float64)))
		if i < 0 {
			i += len(list)
		}
		if i >= 0 && i < len(list) {
			out = appendFlat(out, list[i])
		}
		return out, nil
	}
	for i, item := range list {
		r, err := x.eval(p, item, e)
		if err != nil {
			return nil, err
		}
		if f, ok := r.(float64); ok {
			r = []any{f}
		}
		if nums, ok := r.([]any); ok && len(nums) > 0 && allNumbers(nums) {
			for _, f := range nums {
				j := int(math.Floor(f.(float64)))
				if j < 0 {
					j += len(list)
				}
				if j == i {
					out = append(out, item)
				}
			}
		} else if truthy(r) {
			out = append(out, item)
		}
	}
	return out, nil
}

func allNumbers(a []any) bool {
	for _, v := range a {
		if _, ok := v.(float64); !ok {
			return false
		}
	}
	return true
}

// truthy is the $boolean rule.
func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case []any:
		return slices.ContainsFunc(t, truthy)
	case seq:
		return slices.ContainsFunc(t, truthy)
	case map[string]any:
		return len(t) > 0
	}
	return false // null, undefined, functions
}

func (x *jx) sortStep(st *node, in seq, e *env) (seq, error) {
	type row struct {
		item any
		keys []any
	}
	rows := make([]row, len(in))
	for i, item := range in {
		rows[i] = row{item: item}
		for _, t := range st.terms {
			k, err := x.eval(t.expr, item, e)
			if err != nil {
				return nil, err
			}
			rows[i].keys = append(rows[i].keys, k)
		}
	}
	var sortErr error
	slices.SortStableFunc(rows, func(a, b row) int {
		for i, t := range st.terms {
			c, err := compareKeys(a.keys[i], b.keys[i])
			if err != nil {
				sortErr = err
			}
			if t.desc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return 0
	})
	if sortErr != nil {
		return nil, sortErr
	}
	out := make(seq, len(rows))
	for i, r := range rows {
		out[i] = r.item
	}
	return out, nil
}

// compareKeys orders numbers or strings; undefined goes last.
func compareKeys(a, b any) (int, error) {
	switch {
	case isUndef(a) && isUndef(b):
		return 0, nil
	case isUndef(a):
		return 1, nil
	case isUndef(b):
		return -1, nil
	}
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			return cmpFloat(av, bv), nil
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv), nil
		}
	}
	return 0, jfail("T2007", "Type mismatch when comparing values %s and %s in order-by clause", jsonString(a, false), jsonString(b, false))
}

// groupBy builds an object: each key is evaluated for every item of the input, the
// items with the same key are grouped, and the value is evaluated once per group.
func (x *jx) groupBy(n *node, in any, e *env) (any, error) {
	list := items(in)
	if isUndef(in) || len(list) == 0 {
		list = []any{undef}
	}
	type group struct {
		data []any
		idx  int
	}
	groups, order := map[string]*group{}, []string{}
	for _, item := range list {
		for i, pr := range n.pairs {
			k, err := x.eval(pr[0], item, e)
			if err != nil {
				return nil, err
			}
			if isUndef(k) {
				continue
			}
			key, ok := k.(string)
			if !ok {
				return nil, jfail("T1003", "Key in object structure must evaluate to a string; got: %s", jsonString(k, false))
			}
			g := groups[key]
			if g == nil {
				groups[key] = &group{[]any{item}, i}
				order = append(order, key)
			} else if g.idx != i {
				return nil, jfail("D1009", "Multiple key definitions evaluate to same key: %s", key)
			} else {
				g.data = append(g.data, item)
			}
		}
	}
	out := map[string]any{}
	for _, key := range order {
		g := groups[key]
		var ctx any = g.data
		if len(g.data) == 1 {
			ctx = g.data[0]
		}
		v, err := x.eval(n.pairs[g.idx][1], ctx, e)
		if err != nil {
			return nil, err
		}
		if !isUndef(v) {
			out[key] = v
		}
	}
	return out, nil
}

func (x *jx) binary(n *node, in any, e *env) (any, error) {
	op := n.val.(string)
	l, err := x.eval(n.lhs, in, e)
	if err != nil {
		return nil, err
	}
	switch op { // these may not need the right side
	case "and":
		if !truthy(l) {
			return false, nil
		}
		r, err := x.eval(n.rhs, in, e)
		return truthy(r), err
	case "or":
		if truthy(l) {
			return true, nil
		}
		r, err := x.eval(n.rhs, in, e)
		return truthy(r), err
	case "?:":
		if truthy(l) {
			return l, nil
		}
		return x.eval(n.rhs, in, e)
	case "??":
		if !isUndef(l) {
			return l, nil
		}
		return x.eval(n.rhs, in, e)
	}
	r, err := x.eval(n.rhs, in, e)
	if err != nil {
		return nil, err
	}
	switch op {
	case "+", "-", "*", "/", "%":
		return arith(op, l, r)
	case "=":
		return !isUndef(l) && !isUndef(r) && deepEqual(l, r), nil
	case "!=":
		return !isUndef(l) && !isUndef(r) && !deepEqual(l, r), nil
	case "<", "<=", ">", ">=":
		return order(op, l, r)
	case "&":
		a, err1 := toString(l)
		b, err2 := toString(r)
		if err1 != nil || err2 != nil {
			return nil, cmpErr(err1, err2)
		}
		return a + b, nil
	case "in":
		if isUndef(l) || isUndef(r) {
			return false, nil
		}
		for _, v := range items(r) {
			if deepEqual(l, v) {
				return true, nil
			}
		}
		return false, nil
	case "..":
		if isUndef(l) || isUndef(r) {
			return undef, nil
		}
		a, ok1 := l.(float64)
		b, ok2 := r.(float64)
		switch {
		case !ok1 || a != math.Trunc(a):
			return nil, jfail("T2003", "The left side of the range operator (..) must evaluate to an integer")
		case !ok2 || b != math.Trunc(b):
			return nil, jfail("T2004", "The right side of the range operator (..) must evaluate to an integer")
		case a > b:
			return undef, nil
		case b-a+1 > maxRange:
			return nil, jfail("D2014", "The size of the sequence allocated by the range operator (..) must not exceed %d", maxRange)
		}
		out := make([]any, 0, int(b-a)+1)
		for i := a; i <= b; i++ {
			out = append(out, i)
		}
		return out, nil
	}
	return nil, jfail("S0201", "Unknown operator %s", op)
}

func cmpErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

func arith(op string, l, r any) (any, error) {
	if isUndef(l) || isUndef(r) {
		return undef, nil
	}
	a, ok1 := l.(float64)
	b, ok2 := r.(float64)
	if !ok1 {
		return nil, jfail("T2001", "The left side of the %s operator must evaluate to a number", op)
	}
	if !ok2 {
		return nil, jfail("T2002", "The right side of the %s operator must evaluate to a number", op)
	}
	var f float64
	switch op {
	case "+":
		f = a + b
	case "-":
		f = a - b
	case "*":
		f = a * b
	case "/":
		f = a / b
	case "%":
		f = math.Mod(a, b)
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return nil, jfail("D1001", "Number out of range: %s %s %s", numStr(a), op, numStr(b))
	}
	return f, nil
}

func order(op string, l, r any) (any, error) {
	if isUndef(l) || isUndef(r) {
		return undef, nil
	}
	var c int
	switch a := l.(type) {
	case float64:
		b, ok := r.(float64)
		if !ok {
			return nil, jfail("T2009", "The values either side of the operator %s must be of the same data type", op)
		}
		c = cmpFloat(a, b)
	case string:
		b, ok := r.(string)
		if !ok {
			return nil, jfail("T2009", "The values either side of the operator %s must be of the same data type", op)
		}
		c = strings.Compare(a, b)
	default:
		return nil, jfail("T2010", "The expressions either side of operator %s must evaluate to numeric or string values", op)
	}
	switch op {
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	}
	return c >= 0, nil
}

func deepEqual(a, b any) bool {
	switch x := a.(type) {
	case []any, seq:
		y, ok := b.([]any)
		if s, isSeq := b.(seq); isSeq {
			y, ok = s, true
		}
		l := items(x)
		if !ok || len(l) != len(y) {
			return false
		}
		for i := range l {
			if !deepEqual(l[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, found := y[k]
			if !found || !deepEqual(v, w) {
				return false
			}
		}
		return true
	case float64, string, bool, nil:
		return a == b
	}
	return false // functions are never equal
}

// ---- functions ----

// call evaluates a function call or a partial application. first, when set, is
// the value on the left of ~>, passed as the first argument.
func (x *jx) call(n *node, in any, e *env, first []any) (any, error) {
	fn, err := x.eval(n.lhs, in, e)
	if err != nil {
		return nil, err
	}
	args := slices.Clone(first)
	for _, a := range n.args {
		if a.typ == "hole" {
			args = append(args, hole{})
			continue
		}
		v, err := x.eval(a, in, e)
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	if n.typ == "partial" {
		if !isFn(fn) {
			return nil, jfail("T1008", "Attempted to partially apply a non-function")
		}
		return &partial{fn, args}, nil
	}
	if !isFn(fn) {
		if isUndef(fn) && n.lhs.typ == "path" && len(n.lhs.steps) == 1 && n.lhs.steps[0].typ == "name" {
			return nil, jfail("T1005", "Attempted to invoke a non-function. Did you mean $%v?", n.lhs.steps[0].val)
		}
		return nil, jfail("T1006", "Attempted to invoke a non-function")
	}
	return x.apply(fn, args, in)
}

func isFn(v any) bool {
	switch v.(type) {
	case *lambda, *builtin, *partial:
		return true
	}
	return false
}

// apply calls a function value; ctx is the context value for built-ins that default to it.
func (x *jx) apply(fn any, args []any, ctx any) (any, error) {
	switch f := fn.(type) {
	case *lambda:
		if x.depth++; x.depth > maxDepth {
			return nil, jfail("U1001", "Stack overflow error: Check for non-terminating recursive function. Consider rewriting as tail-recursive.")
		}
		defer func() { x.depth-- }()
		frame := &env{vars: make(map[string]any, len(f.params)), parent: f.env}
		for i, name := range f.params {
			if i < len(args) {
				frame.vars[name] = args[i]
			}
		}
		return x.eval(f.body, f.input, frame)
	case *partial:
		merged, rest := make([]any, 0, len(f.args)+len(args)), args
		for _, a := range f.args {
			if _, isHole := a.(hole); isHole && len(rest) > 0 {
				a, rest = rest[0], rest[1:]
			} else if isHole {
				a = undef
			}
			merged = append(merged, a)
		}
		return x.apply(f.fn, append(merged, rest...), ctx)
	case *builtin:
		if f.ctx != 0 && (len(args) < f.min || len(args) > 0 && f.ctx != 'x' && !isUndef(args[0]) && !kindIs(f.ctx, args[0])) {
			args = append([]any{ctx}, args...)
		}
		if len(args) < f.min || f.max >= 0 && len(args) > f.max {
			return nil, jfail("T0410", "Argument %d of function %s does not match function signature", min(len(args), f.min)+1, f.name)
		}
		return f.fn(x, args)
	}
	return nil, jfail("T1006", "Attempted to invoke a non-function")
}

func kindIs(k byte, v any) bool {
	switch k {
	case 's':
		_, ok := v.(string)
		return ok
	case 'n':
		_, ok := v.(float64)
		return ok
	case 'a':
		return isArr(v)
	case 'o':
		_, ok := v.(map[string]any)
		return ok
	}
	return true
}

// arity is how many arguments a callback takes.
func arity(fn any) int {
	switch f := fn.(type) {
	case *lambda:
		return len(f.params)
	case *builtin:
		if f.max < 0 {
			return f.min
		}
		return f.max
	case *partial:
		n := 0
		for _, a := range f.args {
			if _, ok := a.(hole); ok {
				n++
			}
		}
		return n
	}
	return 0
}

// callback calls fn with as many of args as it takes.
func (x *jx) callback(fn any, args ...any) (any, error) {
	if n := arity(fn); n < len(args) {
		args = args[:max(n, 1)]
	}
	return x.apply(fn, args, undef)
}

// applyOp is lhs ~> rhs: call rhs with lhs, or join two functions.
func (x *jx) applyOp(n *node, in any, e *env) (any, error) {
	l, err := x.eval(n.lhs, in, e)
	if err != nil {
		return nil, err
	}
	if n.rhs.typ == "call" {
		return x.call(n.rhs, in, e, []any{l})
	}
	f, err := x.eval(n.rhs, in, e)
	if err != nil {
		return nil, err
	}
	if !isFn(f) {
		return nil, jfail("T2006", "The right side of the function application operator ~> must be a function")
	}
	if isFn(l) {
		return &builtin{name: "chain", min: 1, max: 1, fn: func(x *jx, a []any) (any, error) {
			v, err := x.apply(l, a, undef)
			if err != nil {
				return nil, err
			}
			return x.apply(f, []any{v}, undef)
		}}, nil
	}
	return x.apply(f, []any{l}, in)
}

// transform is |pattern|update, delete|: a function that returns a changed copy of its argument.
func (x *jx) transform(n *node, e *env) any {
	return &builtin{name: "transform", min: 1, max: 1, fn: func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		cp := deepCopy(a[0])
		m, err := x.eval(n.lhs, cp, e)
		if err != nil || isUndef(m) {
			return cp, err
		}
		for _, t := range items(m) {
			obj, ok := t.(map[string]any)
			if !ok {
				continue
			}
			u, err := x.eval(n.upd, obj, e)
			if err != nil {
				return nil, err
			}
			if !isUndef(u) {
				um, ok := u.(map[string]any)
				if !ok {
					return nil, jfail("T2011", "The insert/update clause of the transform expression must evaluate to an object")
				}
				for k, v := range um {
					obj[k] = v
				}
			}
			if n.del == nil {
				continue
			}
			d, err := x.eval(n.del, obj, e)
			if err != nil {
				return nil, err
			}
			if isUndef(d) {
				continue
			}
			for _, k := range items(d) {
				ks, ok := k.(string)
				if !ok {
					return nil, jfail("T2012", "The delete clause of the transform expression must evaluate to a string or an array of strings")
				}
				delete(obj, ks)
			}
		}
		return cp, nil
	}}
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = deepCopy(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = deepCopy(e)
		}
		return out
	}
	return v
}
