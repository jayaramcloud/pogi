package cosmos

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// The Cosmos DB SQL subset: a tokenizer, a recursive-descent parser and an
// evaluator over decoded JSON (nil, bool, float64, string, []any,
// map[string]any). undef is SQL undefined (a missing value): operators that
// meet it or mismatched types yield undefined, WHERE keeps only rows that are
// exactly true, and projections drop undefined values.

type undefined struct{}

var undef any = undefined{}

type sqlError string

func (e sqlError) Error() string { return string(e) }

func fail(format string, a ...any) { panic(sqlError(fmt.Sprintf(format, a...))) }

// catch turns a parse or evaluation failure into a returned error.
func catch(err *error) {
	if r := recover(); r != nil {
		e, ok := r.(sqlError)
		if !ok {
			panic(r)
		}
		*err = e
	}
}

// execute runs sql with @parameters over docs (in order) and returns every result.
func execute(sql string, params map[string]any, docs []any) (out []any, err error) {
	defer catch(&err)
	if docs == nil {
		docs = []any{}
	}
	return (&evaluator{params: params}).run(parse(sql), nil, docs), nil
}

// parseSQL checks a query without running it.
func parseSQL(sql string) (q *query, err error) {
	defer catch(&err)
	return parse(sql), nil
}

// ---- tokenizer ----

type token struct {
	kind byte // 'w' word, 'n' number, 's' string, '@' parameter, 'o' operator, 0 end
	text string
	num  float64
}

var operators = []string{"<=", ">=", "!=", "<>", "||", "??", "(", ")", "[", "]", "{", "}", ",", ".", ":", "?", "*", "/", "%", "+", "-", "=", "<", ">"}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' }
func isDigit(c byte) bool  { return c >= '0' && c <= '9' }

func lex(src string) []token {
	var out []token
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			i++
		case strings.HasPrefix(src[i:], "--"):
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '@' || isLetter(c):
			j := i + 1
			for j < len(src) && (isLetter(src[j]) || isDigit(src[j])) {
				j++
			}
			kind := byte('w')
			if c == '@' {
				kind = '@'
			}
			out = append(out, token{kind: kind, text: src[i:j]})
			i = j
		case isDigit(c) || c == '.' && i+1 < len(src) && isDigit(src[i+1]):
			j := i + 1
			for j < len(src) && (isDigit(src[j]) || src[j] == '.' || src[j] == 'e' || src[j] == 'E' ||
				(src[j] == '+' || src[j] == '-') && (src[j-1] == 'e' || src[j-1] == 'E')) {
				j++
			}
			f, err := strconv.ParseFloat(src[i:j], 64)
			if err != nil {
				fail("Syntax error, invalid number '%s'.", src[i:j])
			}
			out = append(out, token{kind: 'n', text: src[i:j], num: f})
			i = j
		case c == '\'' || c == '"':
			var s string
			s, i = unquote(src, i)
			out = append(out, token{kind: 's', text: s})
		default:
			k := slices.IndexFunc(operators, func(op string) bool { return strings.HasPrefix(src[i:], op) })
			if k < 0 {
				fail("Syntax error, unexpected character '%c'.", c)
			}
			out = append(out, token{kind: 'o', text: operators[k]})
			i += len(operators[k])
		}
	}
	return out
}

// unquote reads the string literal at src[i] and returns it and the index after it.
func unquote(src string, i int) (string, int) {
	var b strings.Builder
	for j := i + 1; j < len(src); j++ {
		c := src[j]
		switch {
		case c == src[i]:
			return b.String(), j + 1
		case c != '\\' || j+1 == len(src):
			b.WriteByte(c)
			continue
		}
		j++
		switch e := src[j]; {
		case strings.IndexByte("bfnrt", e) >= 0:
			b.WriteByte("\b\f\n\r\t"[strings.IndexByte("bfnrt", e)])
		case e == 'u':
			if j+5 > len(src) {
				fail("Syntax error, invalid escape sequence.")
			}
			n, err := strconv.ParseUint(src[j+1:j+5], 16, 32)
			if err != nil {
				fail("Syntax error, invalid escape sequence.")
			}
			b.WriteRune(rune(n))
			j += 4
		default:
			b.WriteByte(e)
		}
	}
	fail("Syntax error, unterminated string literal.")
	return "", 0
}

// ---- parser ----

type node struct {
	op   string // "lit", "@", "id", ".", "[]", "{}", "call", "EXISTS", "ARRAY", "subquery" or an operator
	val  any    // literal, name or function name
	args []*node
	keys []string // object literal keys
	q    *query
}

type query struct {
	distinct, star, value, agg bool
	top, offset, limit         *node
	sel                        []column
	from                       *source
	joins                      []source
	where                      *node
	groupBy                    []*node
	orderBy                    []sortKey
	root                       string // what a top-level FROM binds each document to
}

type column struct {
	e    *node
	name string
}

// source is "alias IN e" (each element of the array e) or "e [AS] alias".
type source struct {
	alias string
	e     *node
	in    bool
}

type sortKey struct {
	e    *node
	desc bool
}

const keywords = " SELECT DISTINCT TOP VALUE AS FROM JOIN IN WHERE AND OR NOT BETWEEN LIKE ESCAPE GROUP ORDER BY ASC DESC OFFSET LIMIT TRUE FALSE NULL UNDEFINED EXISTS ARRAY UDF "

func keyword(s string) bool { return strings.Contains(keywords, " "+strings.ToUpper(s)+" ") }

func lit(v any) *node { return &node{op: "lit", val: v} }

type parser struct {
	toks  []token
	pos   int
	depth int // how deep the parser is nested right now
}

// maxDepth stops a query like 1,000,000 nested brackets from overflowing the Go stack, which would kill
// the whole process. Real queries nest a few levels.
const maxDepth = 1000

// enter counts one more nesting level and fails when there are too many. Use it as `defer p.enter()()`.
func (p *parser) enter() func() {
	if p.depth++; p.depth > maxDepth {
		fail("The query is nested more than %d levels deep.", maxDepth)
	}
	return func() { p.depth-- }
}

// parse parses one query; it panics with a sqlError on bad input.
func parse(src string) *query {
	p := &parser{toks: lex(src)}
	q := p.query()
	if p.pos < len(p.toks) {
		p.syntax()
	}
	return q
}

func (p *parser) peek(k int) token {
	if p.pos+k < len(p.toks) {
		return p.toks[p.pos+k]
	}
	return token{}
}

// is reports whether the token k ahead is the keyword or operator s.
func (p *parser) is(k int, s string) bool {
	t := p.peek(k)
	return t.kind == 'o' && t.text == s || t.kind == 'w' && strings.EqualFold(t.text, s)
}

func (p *parser) accept(s string) bool {
	if p.is(0, s) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expect(s string) {
	if !p.accept(s) {
		p.syntax()
	}
}

func (p *parser) syntax() {
	if t := p.peek(0); t.kind != 0 {
		fail("Syntax error, incorrect syntax near '%s'.", t.text)
	}
	fail("Syntax error, unexpected end of query.")
}

func (p *parser) isName() bool { t := p.peek(0); return t.kind == 'w' && !keyword(t.text) }

func (p *parser) name() string {
	if !p.isName() {
		p.syntax()
	}
	p.pos++
	return p.toks[p.pos-1].text
}

func (p *parser) list() []*node {
	l := []*node{p.expr()}
	for p.accept(",") {
		l = append(l, p.expr())
	}
	return l
}

func (p *parser) query() *query {
	q := &query{}
	p.expect("SELECT")
	q.distinct = p.accept("DISTINCT")
	if p.accept("TOP") {
		q.top = p.primary()
	}
	switch {
	case p.accept("*"):
		q.star = true
	case p.accept("VALUE"):
		q.value, q.sel = true, []column{{e: p.expr()}}
	default:
		for unnamed := 0; ; {
			c := column{e: p.expr()}
			if p.accept("AS") || p.isName() {
				c.name = p.name()
			} else if c.name = implicitName(c.e); c.name == "" {
				unnamed++
				c.name = "$" + strconv.Itoa(unnamed)
			}
			q.sel = append(q.sel, c)
			if !p.accept(",") {
				break
			}
		}
	}
	if p.accept("FROM") {
		s := p.source()
		q.from, q.root = &s, rootName(s.e)
		for p.accept("JOIN") {
			q.joins = append(q.joins, p.source())
		}
	}
	if q.star && (q.from == nil || len(q.joins) > 0) {
		fail("'SELECT *' is only valid with a single input set.")
	}
	if p.accept("WHERE") {
		q.where = p.expr()
	}
	if p.accept("GROUP") {
		p.expect("BY")
		q.groupBy = p.list()
	}
	if p.accept("ORDER") {
		p.expect("BY")
		for {
			k := sortKey{e: p.expr(), desc: p.accept("DESC")}
			if !k.desc {
				p.accept("ASC")
			}
			q.orderBy = append(q.orderBy, k)
			if !p.accept(",") {
				break
			}
		}
	}
	if p.accept("OFFSET") {
		q.offset = p.primary()
		p.expect("LIMIT")
		q.limit = p.primary()
	}
	for _, c := range q.sel {
		q.agg = q.agg || hasAgg(c.e)
	}
	if len(q.orderBy) > 0 && (q.agg || len(q.groupBy) > 0) {
		fail("ORDER BY is not supported with GROUP BY or aggregates.")
	}
	return q
}

func (p *parser) source() source {
	if p.isName() && p.is(1, "IN") {
		alias := p.name()
		p.pos++
		return source{alias: alias, e: p.postfix(), in: true}
	}
	s := source{e: p.postfix()}
	if s.e.op == "subquery" { // JOIN (SELECT …) iterates the results
		s.e.op, s.in = "ARRAY", true
	}
	if p.accept("AS") || p.isName() {
		s.alias = p.name()
	} else {
		s.alias = implicitName(s.e)
	}
	return s
}

// implicitName is the property name a projection gets without an alias.
func implicitName(e *node) string {
	switch {
	case e.op == "id":
		return e.val.(string)
	case e.op == "." && e.args[1].op == "lit":
		s, _ := e.args[1].val.(string)
		return s
	}
	return ""
}

func rootName(e *node) string {
	for e.op == "." {
		e = e.args[0]
	}
	if e.op == "id" {
		return e.val.(string)
	}
	return ""
}

func (p *parser) expr() *node {
	defer p.enter()()
	c := p.coalesce()
	if !p.accept("?") {
		return c
	}
	a := p.expr()
	p.expect(":")
	return &node{op: "?:", args: []*node{c, a, p.expr()}}
}

func (p *parser) coalesce() *node {
	defer p.enter()()
	l := p.or()
	if p.accept("??") {
		return &node{op: "??", args: []*node{l, p.coalesce()}}
	}
	return l
}

func (p *parser) or() *node {
	l := p.and()
	for p.accept("OR") {
		l = &node{op: "OR", args: []*node{l, p.and()}}
	}
	return l
}

func (p *parser) and() *node {
	l := p.not()
	for p.accept("AND") {
		l = &node{op: "AND", args: []*node{l, p.not()}}
	}
	return l
}

func (p *parser) not() *node {
	defer p.enter()()
	if p.accept("NOT") {
		return &node{op: "NOT", args: []*node{p.not()}}
	}
	return p.compare()
}

func (p *parser) compare() *node {
	l := p.arith(0)
	for {
		neg := p.is(0, "NOT") && (p.is(1, "IN") || p.is(1, "BETWEEN") || p.is(1, "LIKE"))
		if neg {
			p.pos++
		}
		n := &node{args: []*node{l}}
		switch t := p.peek(0); {
		case p.accept("IN"):
			p.expect("(")
			n.op, n.args = "IN", append(n.args, p.list()...)
			p.expect(")")
		case p.accept("BETWEEN"):
			n.op, n.args = "BETWEEN", append(n.args, p.arith(0))
			p.expect("AND")
			n.args = append(n.args, p.arith(0))
		case p.accept("LIKE"):
			n.op, n.args = "LIKE", append(n.args, p.arith(0))
			if p.accept("ESCAPE") {
				n.args = append(n.args, p.arith(0))
			}
		case t.kind == 'o' && strings.Contains(" = != <> < <= > >= ", " "+t.text+" "):
			p.pos++
			n.op, n.args = t.text, append(n.args, p.arith(0))
		default:
			return l
		}
		if l = n; neg {
			l = &node{op: "NOT", args: []*node{n}}
		}
	}
}

var arithOps = [][]string{{"||"}, {"+", "-"}, {"*", "/", "%"}}

func (p *parser) arith(level int) *node {
	if level == len(arithOps) {
		return p.unary()
	}
	l := p.arith(level + 1)
	for t := p.peek(0); t.kind == 'o' && slices.Contains(arithOps[level], t.text); t = p.peek(0) {
		p.pos++
		l = &node{op: t.text, args: []*node{l, p.arith(level + 1)}}
	}
	return l
}

func (p *parser) unary() *node {
	defer p.enter()()
	if p.is(0, "-") || p.is(0, "+") {
		op := "u" + p.toks[p.pos].text
		p.pos++
		return &node{op: op, args: []*node{p.unary()}}
	}
	return p.postfix()
}

func (p *parser) postfix() *node {
	x := p.primary()
	for {
		switch {
		case p.accept("."):
			if p.peek(0).kind != 'w' {
				p.syntax()
			}
			p.pos++
			x = &node{op: ".", args: []*node{x, lit(p.toks[p.pos-1].text)}}
		case p.accept("["):
			x = &node{op: ".", args: []*node{x, p.expr()}}
			p.expect("]")
		default:
			return x
		}
	}
}

func (p *parser) primary() *node {
	t := p.peek(0)
	p.pos++
	switch up := strings.ToUpper(t.text); {
	case t.kind == 'n':
		return lit(t.num)
	case t.kind == 's':
		return lit(t.text)
	case t.kind == '@':
		return &node{op: "@", val: t.text}
	case t.kind == 'o' && t.text == "(":
		var n *node
		if p.is(0, "SELECT") {
			n = &node{op: "subquery", q: p.query()}
		} else {
			n = p.expr()
		}
		p.expect(")")
		return n
	case t.kind == 'o' && t.text == "[":
		n := &node{op: "[]"}
		if !p.accept("]") {
			n.args = p.list()
			p.expect("]")
		}
		return n
	case t.kind == 'o' && t.text == "{":
		n := &node{op: "{}"}
		for !p.accept("}") {
			if len(n.keys) > 0 {
				p.expect(",")
			}
			k := p.peek(0)
			if k.kind != 'w' && k.kind != 's' {
				p.syntax()
			}
			p.pos++
			p.expect(":")
			n.keys, n.args = append(n.keys, k.text), append(n.args, p.expr())
		}
		return n
	case t.kind != 'w':
	case up == "TRUE" || up == "FALSE":
		return lit(up == "TRUE")
	case up == "NULL":
		return lit(nil)
	case up == "UNDEFINED":
		return lit(undef)
	case up == "UDF":
		fail("pogi does not run user-defined functions.")
	case (up == "EXISTS" || up == "ARRAY") && p.is(0, "("):
		p.pos++
		n := &node{op: up, q: p.query()}
		p.expect(")")
		return n
	case p.accept("("):
		n := &node{op: "call", val: up}
		if p.accept("*") { // COUNT(*)
			n.args = []*node{lit(1.0)}
			p.expect(")")
		} else if !p.accept(")") {
			n.args = p.list()
			p.expect(")")
		}
		checkCall(n)
		return n
	case !keyword(t.text):
		return &node{op: "id", val: t.text}
	}
	p.pos--
	p.syntax()
	return nil
}

func checkCall(n *node) {
	name := n.val.(string)
	lo, hi := 1, 1
	if f, ok := funcs[name]; ok {
		lo, hi = f.min, f.max
	} else if !aggregates[name] {
		fail("'%s' is not a recognized built-in function name.", name)
	}
	if len(n.args) < lo || hi >= 0 && len(n.args) > hi {
		fail("The %s function requires %d argument(s).", name, lo)
	}
}

func hasAgg(n *node) bool {
	return n.op == "call" && aggregates[n.val.(string)] || slices.ContainsFunc(n.args, hasAgg)
}

// ---- evaluator ----

// scope is a chain of FROM and JOIN bindings.
type scope struct {
	name string
	val  any
	up   *scope
}

func (s *scope) lookup(name string) (any, bool) {
	for ; s != nil; s = s.up {
		if s.name == name {
			return s.val, true
		}
	}
	return nil, false
}

type evaluator struct {
	params map[string]any
	group  []*scope // the rows an aggregate runs over
	agg    bool     // aggregates are allowed (projecting a group)
	likes  map[string]*regexp.Regexp
}

// run evaluates q. docs are the documents a top-level query reads; nil runs a
// subquery whose FROM is an expression over the enclosing scope.
func (ev *evaluator) run(q *query, outer *scope, docs []any) []any {
	group, agg := ev.group, ev.agg
	defer func() { ev.group, ev.agg = group, agg }()
	ev.group, ev.agg = nil, false
	var srcs []source
	if q.from != nil {
		srcs = append([]source{*q.from}, q.joins...)
	}
	var rows []*scope
	var expand func(sc *scope, i int)
	expand = func(sc *scope, i int) {
		if i == len(srcs) {
			if q.where == nil || ev.eval(q.where, sc) == true {
				rows = append(rows, sc)
			}
			return
		}
		s := srcs[i]
		v := ev.eval(s.e, sc)
		if !s.in {
			if v != undef {
				expand(&scope{s.alias, v, sc}, i+1)
			}
			return
		}
		arr, _ := v.([]any)
		for _, el := range arr {
			expand(&scope{s.alias, el, sc}, i+1)
		}
	}
	if q.from == nil || docs == nil {
		expand(outer, 0)
	} else {
		for _, d := range docs {
			expand(&scope{q.root, d, outer}, 0)
		}
	}

	var out []any
	if q.agg || len(q.groupBy) > 0 {
		index := map[string]int{}
		var groups [][]*scope
		for _, r := range rows {
			var k strings.Builder
			for _, g := range q.groupBy {
				k.WriteString(key(ev.eval(g, r)) + "\x00")
			}
			i, ok := index[k.String()]
			if !ok {
				i = len(groups)
				index[k.String()] = i
				groups = append(groups, nil)
			}
			groups[i] = append(groups[i], r)
		}
		if len(groups) == 0 && len(q.groupBy) == 0 {
			groups = [][]*scope{nil} // aggregates over no rows still answer: COUNT is 0
		}
		for _, g := range groups {
			first := outer
			if len(g) > 0 {
				first = g[0]
			}
			ev.group, ev.agg = g, true
			out = ev.project(q, first, out)
		}
		ev.group, ev.agg = nil, false
	} else {
		ev.sort(q.orderBy, rows)
		for _, r := range rows {
			out = ev.project(q, r, out)
		}
	}
	if q.distinct {
		seen := map[string]bool{}
		out = slices.DeleteFunc(out, func(v any) bool {
			k := key(v)
			dup := seen[k]
			seen[k] = true
			return dup
		})
	}
	if q.offset != nil {
		out = clip(out, ev.count(q.offset), ev.count(q.limit))
	}
	if q.top != nil {
		out = clip(out, 0, ev.count(q.top))
	}
	return out
}

func (ev *evaluator) sort(keys []sortKey, rows []*scope) {
	if len(keys) == 0 {
		return
	}
	vals := make(map[*scope][]any, len(rows))
	for _, r := range rows {
		for _, k := range keys {
			vals[r] = append(vals[r], ev.eval(k.e, r))
		}
	}
	slices.SortStableFunc(rows, func(a, b *scope) int {
		for i, k := range keys {
			if c := order(vals[a][i], vals[b][i]); c != 0 {
				if k.desc {
					return -c
				}
				return c
			}
		}
		return 0
	})
}

func (ev *evaluator) project(q *query, sc *scope, out []any) []any {
	var v any
	switch {
	case q.star:
		v, _ = sc.lookup(q.from.alias)
	case q.value:
		v = ev.eval(q.sel[0].e, sc)
	default:
		m := map[string]any{}
		for _, c := range q.sel {
			if x := ev.eval(c.e, sc); x != undef {
				m[c.name] = x
			}
		}
		v = m
	}
	if v == undef {
		return out
	}
	return append(out, v)
}

// count evaluates a TOP, OFFSET or LIMIT argument.
func (ev *evaluator) count(n *node) int {
	f, ok := ev.eval(n, nil).(float64)
	if !ok || f < 0 || f != math.Trunc(f) {
		fail("TOP, OFFSET and LIMIT take non-negative integers.")
	}
	return int(min(f, math.MaxInt32))
}

func (ev *evaluator) eval(n *node, sc *scope) any {
	switch n.op {
	case "lit":
		return n.val
	case "@":
		v, ok := ev.params[n.val.(string)]
		if !ok {
			fail("Parameter '%s' is not defined.", n.val)
		}
		return v
	case "id":
		v, ok := sc.lookup(n.val.(string))
		if !ok {
			fail("Identifier '%s' could not be resolved.", n.val)
		}
		return v
	case "[]":
		out := []any{}
		for _, a := range n.args {
			if v := ev.eval(a, sc); v != undef {
				out = append(out, v)
			}
		}
		return out
	case "{}":
		out := map[string]any{}
		for i, a := range n.args {
			if v := ev.eval(a, sc); v != undef {
				out[n.keys[i]] = v
			}
		}
		return out
	case "u-", "u+":
		f, ok := ev.eval(n.args[0], sc).(float64)
		if !ok {
			return undef
		}
		if n.op == "u-" {
			return -f
		}
		return f
	case "NOT":
		if b, ok := ev.eval(n.args[0], sc).(bool); ok {
			return !b
		}
		return undef
	case "AND":
		if l := ev.eval(n.args[0], sc); l != false {
			return and3(l, ev.eval(n.args[1], sc))
		}
		return false
	case "OR":
		if l := ev.eval(n.args[0], sc); l != true {
			return or3(l, ev.eval(n.args[1], sc))
		}
		return true
	case "??":
		if v := ev.eval(n.args[0], sc); v != undef {
			return v
		}
		return ev.eval(n.args[1], sc)
	case "?:":
		if ev.eval(n.args[0], sc) == true {
			return ev.eval(n.args[1], sc)
		}
		return ev.eval(n.args[2], sc)
	case "IN":
		v := ev.eval(n.args[0], sc)
		if v == undef {
			return undef
		}
		for _, a := range n.args[1:] {
			if compare("=", v, ev.eval(a, sc)) == true {
				return true
			}
		}
		return false
	case "BETWEEN":
		v := ev.eval(n.args[0], sc)
		return and3(compare(">=", v, ev.eval(n.args[1], sc)), compare("<=", v, ev.eval(n.args[2], sc)))
	case "LIKE":
		return ev.like(n, sc)
	case "call":
		return ev.call(n, sc)
	case "EXISTS":
		return len(ev.run(n.q, sc, nil)) > 0
	case "ARRAY":
		return append([]any{}, ev.run(n.q, sc, nil)...)
	case "subquery":
		if r := ev.run(n.q, sc, nil); len(r) > 0 {
			return r[0]
		}
		return undef
	}
	l, r := ev.eval(n.args[0], sc), ev.eval(n.args[1], sc)
	switch n.op {
	case ".":
		return member(l, r)
	case "||":
		a, ok1 := l.(string)
		b, ok2 := r.(string)
		if ok1 && ok2 {
			return a + b
		}
		return undef
	case "+", "-", "*", "/", "%":
		x, ok1 := l.(float64)
		y, ok2 := r.(float64)
		if !ok1 || !ok2 {
			return undef
		}
		switch n.op {
		case "+":
			return number(x + y)
		case "-":
			return number(x - y)
		case "*":
			return number(x * y)
		case "/":
			return number(x / y)
		}
		return number(math.Mod(x, y))
	}
	return compare(n.op, l, r)
}

// and3 and or3 are three-valued logic over true, false and undefined.
func and3(l, r any) any {
	if l == false || r == false {
		return false
	}
	if l == true && r == true {
		return true
	}
	return undef
}

func or3(l, r any) any {
	if l == true || r == true {
		return true
	}
	if l == false && r == false {
		return false
	}
	return undef
}

// member is v.k or v[k].
func member(v, k any) any {
	switch x := v.(type) {
	case map[string]any:
		if s, ok := k.(string); ok {
			if r, ok := x[s]; ok {
				return r
			}
		}
	case []any:
		if f, ok := k.(float64); ok && f >= 0 && f == math.Trunc(f) && f < float64(len(x)) {
			return x[int(f)]
		}
	}
	return undef
}

// rank is the Cosmos DB type order: undefined < null < bool < number < string < array < object.
func rank(v any) int {
	switch v.(type) {
	case undefined:
		return 0
	case nil:
		return 1
	case bool:
		return 2
	case float64:
		return 3
	case string:
		return 4
	case []any:
		return 5
	}
	return 6
}

// order is the ORDER BY comparison: by type, then by value.
func order(a, b any) int {
	if c := cmp.Compare(rank(a), rank(b)); c != 0 {
		return c
	}
	switch x := a.(type) {
	case bool:
		y := b.(bool)
		if x == y {
			return 0
		}
		if y {
			return -1
		}
		return 1
	case float64:
		return cmp.Compare(x, b.(float64))
	case string:
		return strings.Compare(x, b.(string))
	case []any, map[string]any:
		return strings.Compare(key(a), key(b))
	}
	return 0
}

// compare is a comparison operator: undefined across types, for undefined,
// and (except =) for arrays and objects.
func compare(op string, a, b any) any {
	ra := rank(a)
	if ra == 0 || ra != rank(b) {
		return undef
	}
	if ra >= 5 {
		if op == "=" {
			return reflect.DeepEqual(a, b)
		}
		return undef
	}
	c := order(a, b)
	switch op {
	case "=":
		return c == 0
	case "!=", "<>":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}
	return c >= 0
}

// key is a canonical encoding for grouping, DISTINCT and sorting.
func key(v any) string {
	if v == undef {
		return "~"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// number keeps results JSON-encodable: NaN and infinities are undefined.
func number(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return undef
	}
	return f
}

// clip returns up to n elements of s starting at i.
func clip[T any](s []T, i, n int) []T {
	i = max(0, min(i, len(s)))
	return s[i:max(i, min(i+n, len(s)))]
}

func (ev *evaluator) like(n *node, sc *scope) any {
	s, ok1 := ev.eval(n.args[0], sc).(string)
	pat, ok2 := ev.eval(n.args[1], sc).(string)
	esc := ""
	if len(n.args) > 2 {
		esc, _ = ev.eval(n.args[2], sc).(string)
	}
	if !ok1 || !ok2 {
		return undef
	}
	k := pat + "\x00" + esc
	re := ev.likes[k]
	if re == nil {
		re = likeRegexp(pat, esc)
		if ev.likes == nil {
			ev.likes = map[string]*regexp.Regexp{}
		}
		ev.likes[k] = re
	}
	return re.MatchString(s)
}

// likeRegexp translates a LIKE pattern (% _ [set] [^set], optional escape character).
func likeRegexp(pat, esc string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString(`(?s)\A`)
	r := []rune(pat)
	for i := 0; i < len(r); i++ {
		switch c := r[i]; {
		case esc != "" && string(c) == esc && i+1 < len(r):
			i++
			b.WriteString(regexp.QuoteMeta(string(r[i])))
		case c == '%':
			b.WriteString(".*")
		case c == '_':
			b.WriteString(".")
		case c == '[' && slices.Contains(r[i+1:], ']'):
			j := i + 1 + slices.Index(r[i+1:], ']')
			set, neg := string(r[i+1:j]), ""
			if strings.HasPrefix(set, "^") {
				set, neg = set[1:], "^"
			}
			b.WriteString("[" + neg + strings.NewReplacer(`\`, `\\`, `[`, `\[`).Replace(set) + "]")
			i = j
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	re, err := regexp.Compile(b.String() + `\z`)
	if err != nil {
		fail("Invalid LIKE pattern '%s'.", pat)
	}
	return re
}

func (ev *evaluator) call(n *node, sc *scope) any {
	name := n.val.(string)
	if !aggregates[name] {
		args := make([]any, len(n.args))
		for i, a := range n.args {
			args[i] = ev.eval(a, sc)
		}
		return funcs[name].f(args)
	}
	if !ev.agg {
		fail("Aggregate function %s cannot be used here.", name)
	}
	ev.agg = false // arguments are per row: no nested aggregates
	defer func() { ev.agg = true }()
	var count, sum float64
	best, numbers := undef, true
	for _, r := range ev.group {
		v := ev.eval(n.args[0], r)
		if v == undef {
			continue
		}
		count++
		f, ok := v.(float64)
		sum, numbers = sum+f, numbers && ok
		if c := order(v, best); best == undef || name == "MIN" && c < 0 || name == "MAX" && c > 0 {
			best = v
		}
	}
	switch name {
	case "COUNT":
		return count
	case "SUM":
		if numbers {
			return sum
		}
		return undef
	case "AVG":
		if numbers && count > 0 {
			return sum / count
		}
		return undef
	}
	return best
}

// ---- functions ----

type function struct {
	min, max int // arity; max < 0 is variadic
	f        func(a []any) any
}

var aggregates = map[string]bool{"COUNT": true, "SUM": true, "MIN": true, "MAX": true, "AVG": true}

var funcs = map[string]function{
	"IS_DEFINED":   {1, 1, func(a []any) any { return a[0] != undef }},
	"IS_NULL":      {1, 1, func(a []any) any { return a[0] == nil }},
	"IS_BOOL":      is[bool](),
	"IS_NUMBER":    is[float64](),
	"IS_STRING":    is[string](),
	"IS_ARRAY":     is[[]any](),
	"IS_OBJECT":    is[map[string]any](),
	"IS_PRIMITIVE": {1, 1, func(a []any) any { r := rank(a[0]); return r > 0 && r < 5 }},

	"CONTAINS":     strTest(strings.Contains),
	"STARTSWITH":   strTest(strings.HasPrefix),
	"ENDSWITH":     strTest(strings.HasSuffix),
	"STRINGEQUALS": strTest(func(s, t string) bool { return s == t }),
	"LOWER":        str1(strings.ToLower),
	"UPPER":        str1(strings.ToUpper),
	"TRIM":         str1(strings.TrimSpace),
	"LTRIM":        str1(func(s string) string { return strings.TrimLeftFunc(s, unicode.IsSpace) }),
	"RTRIM":        str1(func(s string) string { return strings.TrimRightFunc(s, unicode.IsSpace) }),
	"REVERSE":      str1(func(s string) string { r := []rune(s); slices.Reverse(r); return string(r) }),
	"LENGTH": {1, 1, func(a []any) any {
		if s, ok := a[0].(string); ok {
			return float64(utf8.RuneCountInString(s))
		}
		return undef
	}},
	"CONCAT": {2, -1, func(a []any) any {
		var b strings.Builder
		for _, v := range a {
			s, ok := v.(string)
			if !ok {
				return undef
			}
			b.WriteString(s)
		}
		return b.String()
	}},
	"SUBSTRING": {3, 3, func(a []any) any {
		if r, n, ok := strInts(a); ok {
			return string(clip(r, n[0], n[1]))
		}
		return undef
	}},
	"LEFT": {2, 2, func(a []any) any {
		if r, n, ok := strInts(a); ok {
			return string(clip(r, 0, n[0]))
		}
		return undef
	}},
	"RIGHT": {2, 2, func(a []any) any {
		if r, n, ok := strInts(a); ok {
			return string(clip(r, len(r)-n[0], n[0]))
		}
		return undef
	}},
	"REPLICATE": {2, 2, func(a []any) any {
		if r, n, ok := strInts(a); ok && n[0] >= 0 && len(r)*n[0] <= 10000 {
			return strings.Repeat(string(r), n[0])
		}
		return undef
	}},
	"INDEX_OF": {2, 3, func(a []any) any {
		r, start, ok := strInts(append([]any{a[0]}, a[2:]...))
		sub, ok2 := a[1].(string)
		if !ok || !ok2 {
			return undef
		}
		t, i := []rune(sub), 0
		if len(start) > 0 {
			i = max(start[0], 0)
		}
		for ; i+len(t) <= len(r); i++ {
			if slices.Equal(r[i:i+len(t)], t) {
				return float64(i)
			}
		}
		return -1.0
	}},
	"REPLACE": {3, 3, func(a []any) any {
		s, ok1 := a[0].(string)
		old, ok2 := a[1].(string)
		with, ok3 := a[2].(string)
		if ok1 && ok2 && ok3 {
			return strings.ReplaceAll(s, old, with)
		}
		return undef
	}},
	"TOSTRING": {1, 1, func(a []any) any {
		switch v := a[0].(type) {
		case undefined:
			return undef
		case string:
			return v
		}
		return key(a[0])
	}},
	"STRINGTOARRAY":   parseAs[[]any](),
	"STRINGTOOBJECT":  parseAs[map[string]any](),
	"STRINGTONUMBER":  parseAs[float64](),
	"STRINGTOBOOLEAN": parseAs[bool](),
	"STRINGTONULL": {1, 1, func(a []any) any {
		if s, ok := a[0].(string); ok && strings.TrimSpace(s) == "null" {
			return nil
		}
		return undef
	}},
	"REGEXMATCH": {2, 3, func(a []any) any {
		s, ok1 := a[0].(string)
		pat, ok2 := a[1].(string)
		if len(a) > 2 {
			if flags, _ := a[2].(string); flags != "" {
				pat = "(?" + flags + ")" + pat
			}
		}
		re, err := regexp.Compile(pat)
		if !ok1 || !ok2 || err != nil {
			return undef
		}
		return re.MatchString(s)
	}},

	"ARRAY_CONTAINS": {2, 3, func(a []any) any {
		x, ok := a[0].([]any)
		if !ok {
			return undef
		}
		partial := len(a) > 2 && a[2] == true
		return slices.ContainsFunc(x, func(e any) bool { return contains(e, a[1], partial) })
	}},
	"ARRAY_LENGTH": {1, 1, func(a []any) any {
		if x, ok := a[0].([]any); ok {
			return float64(len(x))
		}
		return undef
	}},
	"ARRAY_SLICE": {2, 3, func(a []any) any {
		x, ok := a[0].([]any)
		n, ok2 := ints(a[1:]...)
		if !ok || !ok2 {
			return undef
		}
		if n[0] < 0 {
			n[0] += len(x)
		}
		if len(n) == 1 {
			n = append(n, len(x))
		}
		return clip(x, n[0], n[1])
	}},
	"ARRAY_CONCAT": {2, -1, func(a []any) any {
		out := []any{}
		for _, v := range a {
			x, ok := v.([]any)
			if !ok {
				return undef
			}
			out = append(out, x...)
		}
		return out
	}},

	"ABS":     math1(math.Abs),
	"CEILING": math1(math.Ceil),
	"FLOOR":   math1(math.Floor),
	"ROUND":   math1(math.Round),
	"TRUNC":   math1(math.Trunc),
	"SQRT":    math1(math.Sqrt),
	"SQUARE":  math1(func(x float64) float64 { return x * x }),
	"EXP":     math1(math.Exp),
	"LOG":     math1(math.Log),
	"LOG10":   math1(math.Log10),
	"SIGN":    math1(func(x float64) float64 { return float64(cmp.Compare(x, 0)) }),
	"POWER": {2, 2, func(a []any) any {
		x, ok1 := a[0].(float64)
		y, ok2 := a[1].(float64)
		if ok1 && ok2 {
			return number(math.Pow(x, y))
		}
		return undef
	}},

	"GETCURRENTDATETIME":  {0, 0, func([]any) any { return time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z") }},
	"GETCURRENTTIMESTAMP": {0, 0, func([]any) any { return float64(time.Now().UnixMilli()) }},
	"IIF": {3, 3, func(a []any) any {
		if a[0] == true {
			return a[1]
		}
		return a[2]
	}},
}

func is[T any]() function {
	return function{1, 1, func(a []any) any { _, ok := a[0].(T); return ok }}
}

func parseAs[T any]() function {
	return function{1, 1, func(a []any) any {
		var v T
		if s, ok := a[0].(string); ok && strings.TrimSpace(s) != "null" && json.Unmarshal([]byte(s), &v) == nil {
			return v
		}
		return undef
	}}
}

func str1(f func(string) string) function {
	return function{1, 1, func(a []any) any {
		if s, ok := a[0].(string); ok {
			return f(s)
		}
		return undef
	}}
}

// strTest is a string predicate with an optional ignore-case flag.
func strTest(f func(s, t string) bool) function {
	return function{2, 3, func(a []any) any {
		s, ok1 := a[0].(string)
		t, ok2 := a[1].(string)
		if !ok1 || !ok2 {
			return undef
		}
		if len(a) > 2 && a[2] == true {
			s, t = strings.ToLower(s), strings.ToLower(t)
		}
		return f(s, t)
	}}
}

func math1(f func(float64) float64) function {
	return function{1, 1, func(a []any) any {
		if x, ok := a[0].(float64); ok {
			return number(f(x))
		}
		return undef
	}}
}

// ints reads integer arguments.
func ints(a ...any) ([]int, bool) {
	out := make([]int, len(a))
	for i, v := range a {
		f, ok := v.(float64)
		if !ok {
			return nil, false
		}
		out[i] = int(f)
	}
	return out, true
}

// strInts reads a string argument followed by integer arguments.
func strInts(a []any) ([]rune, []int, bool) {
	s, ok := a[0].(string)
	n, ok2 := ints(a[1:]...)
	return []rune(s), n, ok && ok2
}

// contains reports whether e equals v or, when partial, e is an object holding all of v's fields.
func contains(e, v any, partial bool) bool {
	if reflect.DeepEqual(e, v) {
		return true
	}
	em, ok1 := e.(map[string]any)
	vm, ok2 := v.(map[string]any)
	if !partial || !ok1 || !ok2 {
		return false
	}
	for k, x := range vm {
		if y, ok := em[k]; !ok || !reflect.DeepEqual(x, y) {
			return false
		}
	}
	return true
}
