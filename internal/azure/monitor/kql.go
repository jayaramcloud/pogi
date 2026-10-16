package monitor

// A subset of the Kusto query language: a lexer, a parser and a typed
// evaluator over in-memory tables.
//
//	let x = ...;  Table | where | project | project-away | project-rename | project-keep
//	| extend | summarize agg() by key | sort by / order by | take / limit | top N by
//	| distinct | count | render (ignored)
//
// Aggregates: count, countif, sum, sumif, avg, min, max, dcount, make_list,
// make_set. Scalars: comparisons, and/or/not, ==, =~, contains, startswith,
// endswith, has, in, between, matches regex, arithmetic, ago, now, datetime,
// the to* conversions, strcat, iff, case, bin and friends (see funcs).
// Column types: string, int, long, real, bool, datetime, timespan, guid, dynamic.
//
// ponytail: no join, union, parse, mv-expand, datatable, print or
// user-defined functions; table, column and function names are matched as
// written, except that table names ignore case; dcount is exact, not HLL.

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type col struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// rel is a table: typed columns and rows of typed Go values (nil, string,
// int64, float64, bool, time.Time, time.Duration, or dynamic: []any, map[string]any, ...).
type rel struct {
	cols []col
	rows [][]any
}

func (r *rel) schema() map[string]string {
	m := make(map[string]string, len(r.cols))
	for _, c := range r.cols {
		m[c.Name] = c.Type
	}
	return m
}

type kqlError struct{ kind, msg string }

func (e *kqlError) Error() string { return e.msg }

func syntaxf(f string, a ...any) error   { return &kqlError{"SyntaxError", fmt.Sprintf(f, a...)} }
func semanticf(f string, a ...any) error { return &kqlError{"SemanticError", fmt.Sprintf(f, a...)} }

const timeFmt = "2006-01-02T15:04:05.0000000Z"

// ---- lexer ----

const (
	tEOF = iota
	tIdent
	tInt
	tReal
	tStr
	tSpan
	tTime
	tOp
)

type token struct {
	k   int
	s   string
	v   any
	pos int
}

var unitDur = map[string]time.Duration{"d": 24 * time.Hour, "h": time.Hour, "m": time.Minute, "s": time.Second,
	"ms": time.Millisecond, "microsecond": time.Microsecond, "tick": 100 * time.Nanosecond}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isLetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
}

func lex(src string) ([]token, error) {
	var out []token
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '/' && i+1 < len(src) && src[i+1] == '/':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case isLetter(c):
			j := i
			for j < len(src) && (isLetter(src[j]) || isDigit(src[j])) {
				j++
			}
			w := src[i:j]
			if w == "project" {
				for _, suf := range []string{"-away", "-rename", "-keep"} {
					if strings.HasPrefix(src[j:], suf) {
						w, j = w+suf, j+len(suf)
						break
					}
				}
			}
			if w == "datetime" {
				k := j
				for k < len(src) && src[k] == ' ' {
					k++
				}
				if k < len(src) && src[k] == '(' {
					end := strings.IndexByte(src[k:], ')')
					if end < 0 {
						return nil, syntaxf("unterminated datetime literal")
					}
					t, err := parseDatetime(strings.TrimSpace(src[k+1 : k+end]))
					if err != nil {
						return nil, err
					}
					out = append(out, token{k: tTime, v: t, pos: i})
					i = k + end + 1
					continue
				}
			}
			out = append(out, token{k: tIdent, s: w, pos: i})
			i = j
		case isDigit(c):
			j, real := i, false
			for j < len(src) && isDigit(src[j]) {
				j++
			}
			if j+1 < len(src) && src[j] == '.' && isDigit(src[j+1]) {
				real = true
				for j++; j < len(src) && isDigit(src[j]); j++ {
				}
			}
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
				k := j + 1
				if k < len(src) && (src[k] == '+' || src[k] == '-') {
					k++
				}
				if k < len(src) && isDigit(src[k]) {
					real = true
					for j = k; j < len(src) && isDigit(src[j]); j++ {
					}
				}
			}
			f, _ := strconv.ParseFloat(src[i:j], 64)
			k := j
			for k < len(src) && isLetter(src[k]) {
				k++
			}
			switch {
			case k > j:
				d, ok := unitDur[src[j:k]]
				if !ok {
					return nil, syntaxf("unknown timespan unit %q", src[j:k])
				}
				out = append(out, token{k: tSpan, v: time.Duration(f * float64(d)), pos: i})
			case real:
				out = append(out, token{k: tReal, v: f, pos: i})
			default:
				n, err := strconv.ParseInt(src[i:j], 10, 64)
				if err != nil {
					return nil, syntaxf("bad number %q", src[i:j])
				}
				out = append(out, token{k: tInt, v: n, pos: i})
			}
			i = k
		case c == '"' || c == '\'' || c == '@' && i+1 < len(src) && (src[i+1] == '"' || src[i+1] == '\''):
			verbatim := c == '@'
			if verbatim {
				i++
			}
			q := src[i]
			var b strings.Builder
			j := i + 1
			for ; ; j++ {
				if j >= len(src) {
					return nil, syntaxf("unterminated string literal")
				}
				ch := src[j]
				if verbatim && ch == q && j+1 < len(src) && src[j+1] == q {
					b.WriteByte(q)
					j++
					continue
				}
				if ch == q {
					break
				}
				if ch == '\\' && !verbatim && j+1 < len(src) {
					j++
					switch src[j] {
					case 'n':
						b.WriteByte('\n')
					case 't':
						b.WriteByte('\t')
					case 'r':
						b.WriteByte('\r')
					case 'u':
						if j+4 < len(src) {
							if n, err := strconv.ParseUint(src[j+1:j+5], 16, 32); err == nil {
								b.WriteRune(rune(n))
								j += 4
								continue
							}
						}
						b.WriteByte('u')
					default:
						b.WriteByte(src[j])
					}
					continue
				}
				b.WriteByte(ch)
			}
			out = append(out, token{k: tStr, s: b.String(), pos: i})
			i = j + 1
		case c == '!' && i+1 < len(src) && isLetter(src[i+1]):
			j := i + 1
			for j < len(src) && isLetter(src[j]) {
				j++
			}
			out = append(out, token{k: tIdent, s: src[i:j], pos: i})
			i = j
		default:
			if i+1 < len(src) {
				switch two := src[i : i+2]; two {
				case "==", "!=", "<=", ">=", "=~", "!~", "..", "<>":
					out = append(out, token{k: tOp, s: two, pos: i})
					i += 2
					continue
				}
			}
			if strings.IndexByte("|()[],.=<>+-*/%;:{}", c) < 0 {
				r, _ := utf8.DecodeRuneInString(src[i:])
				return nil, syntaxf("unexpected character %q", r)
			}
			out = append(out, token{k: tOp, s: string(c), pos: i})
			i++
		}
	}
	return append(out, token{k: tEOF, pos: len(src)}), nil
}

func parseDatetime(s string) (any, error) {
	if s == "null" {
		return nil, nil
	}
	for _, l := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04", "2006-01-02"} {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), nil
		}
	}
	return nil, syntaxf("cannot parse datetime %q", s)
}

// ---- AST ----

type expr any

type (
	lit    struct{ v any }
	ref    struct{ name string }
	binary struct {
		op   string
		l, r expr
	}
	unary struct {
		op string
		x  expr
	}
	call struct {
		name string
		args []expr
	}
	index  struct{ x, key expr }
	inList struct {
		x    expr
		list []expr
		neg  bool
	}
	between struct {
		x, lo, hi expr
		neg       bool
	}
	array struct{ items []expr }
)

type item struct {
	name string // "" when unnamed
	e    expr
}

type sortKey struct {
	e    expr
	desc bool
}

type op any

type (
	opWhere   struct{ cond expr }
	opProject struct{ items []item }
	opExtend  struct{ items []item }
	opAway    struct{ names []string }
	opKeep    struct{ names []string }
	opRename  struct{ items []item }
	opSummary struct{ aggs, by []item }
	opSort    struct{ keys []sortKey }
	opTake    struct{ n expr }
	opTop     struct {
		n expr
		k sortKey
	}
	opDistinct struct{ items []item } // none: distinct *
	opCount    struct{}
)

type pipe struct {
	table string // a table or tabular let
	sub   *pipe  // ( pipe )
	ops   []op
}

type letDef struct {
	scalar expr
	tab    *pipe
}

type query struct {
	lets map[string]*letDef
	main *pipe
}

// ---- parser ----

type parser struct {
	toks  []token
	p     int
	depth int // how deep the parser is nested right now
}

// maxDepth stops a query like 1,000,000 nested brackets from overflowing the Go stack, which would kill
// the whole process. Real queries nest a few levels.
const maxDepth = 1000

// enter counts one more nesting level. Callers must `defer ps.leave()` when it returns nil.
func (ps *parser) enter() error {
	if ps.depth++; ps.depth > maxDepth {
		ps.depth--
		return syntaxf("The query is nested more than %d levels deep", maxDepth)
	}
	return nil
}
func (ps *parser) leave() { ps.depth-- }

func (ps *parser) peek() token { return ps.toks[ps.p] }
func (ps *parser) next() token {
	t := ps.toks[ps.p]
	if t.k != tEOF {
		ps.p++
	}
	return t
}
func (ps *parser) isOp(s string) bool   { t := ps.peek(); return t.k == tOp && t.s == s }
func (ps *parser) isWord(s string) bool { t := ps.peek(); return t.k == tIdent && t.s == s }
func (ps *parser) accept(s string) bool {
	if ps.isOp(s) {
		ps.p++
		return true
	}
	return false
}
func (ps *parser) expect(s string) error {
	if ps.accept(s) {
		return nil
	}
	return ps.fail("expected '" + s + "'")
}
func (ps *parser) fail(msg string) error {
	t := ps.peek()
	got := "end of query"
	if t.k != tEOF {
		got = "'" + cmpOr(t.s, fmt.Sprint(t.v)) + "'"
	}
	return syntaxf("A recognition error occurred in the query: %s at offset %d, found %s", msg, t.pos, got)
}
func (ps *parser) ident() (string, error) {
	t := ps.peek()
	if t.k != tIdent {
		return "", ps.fail("expected a name")
	}
	ps.p++
	return t.s, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func parseKQL(src string) (*query, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	ps := &parser{toks: toks}
	q := &query{lets: map[string]*letDef{}}
	for ps.isWord("let") {
		ps.next()
		name, err := ps.ident()
		if err != nil {
			return nil, err
		}
		if err := ps.expect("="); err != nil {
			return nil, err
		}
		l := &letDef{}
		e, err := ps.expr()
		if err != nil {
			return nil, err
		}
		if r, isRef := e.(*ref); isRef && (ps.isOp("|") || ps.isOp(";")) {
			if l.tab, err = ps.pipeFrom(&pipe{table: r.name}); err != nil {
				return nil, err
			}
		} else {
			l.scalar = e
		}
		q.lets[name] = l
		if err := ps.expect(";"); err != nil {
			return nil, err
		}
	}
	if q.main, err = ps.pipe(); err != nil {
		return nil, err
	}
	ps.accept(";")
	if ps.peek().k != tEOF {
		return nil, ps.fail("unexpected input")
	}
	return q, nil
}

func (ps *parser) pipe() (*pipe, error) {
	if err := ps.enter(); err != nil {
		return nil, err
	}
	defer ps.leave()
	p := &pipe{}
	switch {
	case ps.isOp("("):
		ps.next()
		sub, err := ps.pipe()
		if err != nil {
			return nil, err
		}
		if err := ps.expect(")"); err != nil {
			return nil, err
		}
		p.sub = sub
	default:
		name, err := ps.ident()
		if err != nil {
			return nil, err
		}
		p.table = name
	}
	return ps.pipeFrom(p)
}

func (ps *parser) pipeFrom(p *pipe) (*pipe, error) {
	for ps.accept("|") {
		o, err := ps.operator()
		if err != nil {
			return nil, err
		}
		if o != nil {
			p.ops = append(p.ops, o)
		}
	}
	return p, nil
}

func (ps *parser) items(named bool) ([]item, error) {
	var out []item
	for {
		var it item
		if t := ps.peek(); named && t.k == tIdent && ps.toks[ps.p+1].k == tOp && ps.toks[ps.p+1].s == "=" {
			it.name = t.s
			ps.p += 2
		}
		e, err := ps.expr()
		if err != nil {
			return nil, err
		}
		it.e = e
		out = append(out, it)
		if !ps.accept(",") {
			return out, nil
		}
	}
}

func (ps *parser) names() ([]string, error) {
	var out []string
	for {
		n, err := ps.ident()
		if err != nil {
			return nil, err
		}
		out = append(out, n)
		if !ps.accept(",") {
			return out, nil
		}
	}
}

func (ps *parser) sortKey() (sortKey, error) {
	e, err := ps.expr()
	if err != nil {
		return sortKey{}, err
	}
	k := sortKey{e: e, desc: true}
	if ps.isWord("asc") {
		k.desc = false
		ps.next()
	} else if ps.isWord("desc") {
		ps.next()
	}
	if ps.isWord("nulls") { // ponytail: nulls sort first ascending and last descending, whatever is asked
		ps.next()
		ps.next()
	}
	return k, nil
}

func (ps *parser) operator() (op, error) {
	w, err := ps.ident()
	if err != nil {
		return nil, err
	}
	switch w {
	case "where", "filter":
		e, err := ps.expr()
		return &opWhere{e}, err
	case "project":
		it, err := ps.items(true)
		return &opProject{it}, err
	case "extend":
		it, err := ps.items(true)
		return &opExtend{it}, err
	case "project-away":
		n, err := ps.names()
		return &opAway{n}, err
	case "project-keep":
		n, err := ps.names()
		return &opKeep{n}, err
	case "project-rename":
		it, err := ps.items(true)
		return &opRename{it}, err
	case "summarize":
		s := &opSummary{}
		if !ps.isWord("by") {
			if s.aggs, err = ps.items(true); err != nil {
				return nil, err
			}
		}
		if ps.isWord("by") {
			ps.next()
			s.by, err = ps.items(true)
		}
		return s, err
	case "sort", "order":
		if !ps.isWord("by") {
			return nil, ps.fail("expected 'by'")
		}
		ps.next()
		s := &opSort{}
		for {
			k, err := ps.sortKey()
			if err != nil {
				return nil, err
			}
			s.keys = append(s.keys, k)
			if !ps.accept(",") {
				return s, nil
			}
		}
	case "take", "limit":
		n, err := ps.expr()
		return &opTake{n}, err
	case "top":
		n, err := ps.add()
		if err != nil {
			return nil, err
		}
		if !ps.isWord("by") {
			return nil, ps.fail("expected 'by'")
		}
		ps.next()
		k, err := ps.sortKey()
		return &opTop{n, k}, err
	case "distinct":
		if ps.accept("*") {
			return &opDistinct{}, nil
		}
		it, err := ps.items(false)
		return &opDistinct{it}, err
	case "count":
		return &opCount{}, nil
	case "render":
		for ps.peek().k != tEOF && !ps.isOp("|") {
			ps.next()
		}
		return nil, nil
	}
	return nil, syntaxf("The operator '%s' is not supported", w)
}

func (ps *parser) expr() (expr, error) {
	if err := ps.enter(); err != nil {
		return nil, err
	}
	defer ps.leave()
	return ps.or()
}

func (ps *parser) or() (expr, error) {
	l, err := ps.and()
	for err == nil && ps.isWord("or") {
		ps.next()
		var r expr
		if r, err = ps.and(); err == nil {
			l = &binary{"or", l, r}
		}
	}
	return l, err
}

func (ps *parser) and() (expr, error) {
	l, err := ps.cmp()
	for err == nil && ps.isWord("and") {
		ps.next()
		var r expr
		if r, err = ps.cmp(); err == nil {
			l = &binary{"and", l, r}
		}
	}
	return l, err
}

func (ps *parser) cmp() (expr, error) {
	l, err := ps.add()
	if err != nil {
		return nil, err
	}
	t := ps.peek()
	switch {
	case t.k == tOp && (t.s == "==" || t.s == "!=" || t.s == "<" || t.s == "<=" || t.s == ">" || t.s == ">=" || t.s == "=~" || t.s == "!~" || t.s == "<>"),
		t.k == tIdent && textOps[t.s]:
		ps.next()
		r, err := ps.add()
		if t.s == "<>" {
			t.s = "!="
		}
		return &binary{t.s, l, r}, err
	case t.k == tIdent && t.s == "matches":
		ps.next()
		if !ps.isWord("regex") {
			return nil, ps.fail("expected 'regex'")
		}
		ps.next()
		r, err := ps.add()
		return &binary{"matches regex", l, r}, err
	case t.k == tIdent && (t.s == "in" || t.s == "!in"):
		ps.next()
		if err := ps.expect("("); err != nil {
			return nil, err
		}
		n := &inList{x: l, neg: t.s == "!in"}
		for !ps.isOp(")") {
			e, err := ps.expr()
			if err != nil {
				return nil, err
			}
			n.list = append(n.list, e)
			if !ps.accept(",") {
				break
			}
		}
		return n, ps.expect(")")
	case t.k == tIdent && (t.s == "between" || t.s == "!between"):
		ps.next()
		n := &between{x: l, neg: t.s == "!between"}
		if err := ps.expect("("); err != nil {
			return nil, err
		}
		if n.lo, err = ps.add(); err != nil {
			return nil, err
		}
		if err := ps.expect(".."); err != nil {
			return nil, err
		}
		if n.hi, err = ps.add(); err != nil {
			return nil, err
		}
		return n, ps.expect(")")
	}
	return l, nil
}

var textOps = map[string]bool{"contains": true, "!contains": true, "startswith": true, "!startswith": true,
	"endswith": true, "!endswith": true, "has": true, "!has": true}

func (ps *parser) add() (expr, error) {
	l, err := ps.mul()
	for err == nil && (ps.isOp("+") || ps.isOp("-")) {
		o := ps.next().s
		var r expr
		if r, err = ps.mul(); err == nil {
			l = &binary{o, l, r}
		}
	}
	return l, err
}

func (ps *parser) mul() (expr, error) {
	l, err := ps.unary()
	for err == nil && (ps.isOp("*") || ps.isOp("/") || ps.isOp("%")) {
		o := ps.next().s
		var r expr
		if r, err = ps.unary(); err == nil {
			l = &binary{o, l, r}
		}
	}
	return l, err
}

func (ps *parser) unary() (expr, error) {
	if err := ps.enter(); err != nil {
		return nil, err
	}
	defer ps.leave()
	if ps.isOp("-") || ps.isOp("+") {
		o := ps.next().s
		x, err := ps.unary()
		if o == "+" {
			return x, err
		}
		return &unary{"-", x}, err
	}
	x, err := ps.primary()
	for err == nil {
		switch {
		case ps.isOp("."):
			ps.next()
			var n string
			if n, err = ps.ident(); err == nil {
				x = &index{x, &lit{n}}
			}
		case ps.isOp("["):
			ps.next()
			var k expr
			if k, err = ps.expr(); err == nil {
				err = ps.expect("]")
				x = &index{x, k}
			}
		default:
			return x, nil
		}
	}
	return x, err
}

func (ps *parser) primary() (expr, error) {
	t := ps.next()
	switch t.k {
	case tInt, tReal, tSpan, tTime:
		return &lit{t.v}, nil
	case tStr:
		return &lit{t.s}, nil
	case tIdent:
		if ps.isOp("(") {
			ps.next()
			c := &call{name: t.s}
			for !ps.isOp(")") {
				a, err := ps.expr()
				if err != nil {
					return nil, err
				}
				c.args = append(c.args, a)
				if !ps.accept(",") {
					break
				}
			}
			return c, ps.expect(")")
		}
		switch t.s {
		case "true":
			return &lit{true}, nil
		case "false":
			return &lit{false}, nil
		case "null":
			return &lit{nil}, nil
		}
		return &ref{t.s}, nil
	case tOp:
		switch t.s {
		case "(":
			e, err := ps.expr()
			if err != nil {
				return nil, err
			}
			return e, ps.expect(")")
		case "[":
			if n := ps.peek(); n.k == tStr && ps.toks[ps.p+1].k == tOp && ps.toks[ps.p+1].s == "]" { // ['column name']
				ps.p += 2
				return &ref{n.s}, nil
			}
			a := &array{}
			for !ps.isOp("]") {
				e, err := ps.expr()
				if err != nil {
					return nil, err
				}
				a.items = append(a.items, e)
				if !ps.accept(",") {
					break
				}
			}
			return a, ps.expect("]")
		}
	}
	ps.p--
	return nil, ps.fail("expected an expression")
}

// ---- values ----

func toF(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func toI(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		return int64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(strings.TrimSpace(x), 64)
			return int64(f), ferr == nil
		}
		return n, true
	}
	return 0, false
}

func fmtDur(d time.Duration) string {
	neg := d < 0
	if neg {
		d = -d
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	s := fmt.Sprintf("%02d:%02d:%02d", d/time.Hour, d%time.Hour/time.Minute, d%time.Minute/time.Second)
	if ns := d % time.Second; ns != 0 {
		s += fmt.Sprintf(".%07d", ns/100)
	}
	if days != 0 {
		s = fmt.Sprintf("%d.%s", days, s)
	}
	if neg {
		s = "-" + s
	}
	return s
}

// tostr is Kusto's tostring().
func tostr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case time.Time:
		return x.UTC().Format(timeFmt)
	case time.Duration:
		return fmtDur(x)
	}
	b, _ := json.Marshal(jsonVal(v))
	return string(b)
}

// jsonVal makes a value JSON-encodable: times and timespans become strings.
func jsonVal(v any) any {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format(timeFmt)
	case time.Duration:
		return fmtDur(x)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return strconv.FormatFloat(x, 'g', -1, 64)
		}
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonVal(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = jsonVal(e)
		}
		return out
	}
	return v
}

// parseTime reads the datetime formats clients send.
func parseTime(s string) (time.Time, bool) {
	for _, l := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(l, strings.TrimSpace(s)); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// cmpVals orders two values; ok is false when they are not comparable.
func cmpVals(a, b any) (int, bool) {
	if a == nil || b == nil {
		return 0, false
	}
	if x, ok := a.(int64); ok {
		if y, ok := b.(int64); ok {
			return cmpOrdered(x, y), true
		}
	}
	if x, ok := toF(a); ok {
		if y, ok := toF(b); ok {
			return cmpOrdered(x, y), true
		}
		return 0, false
	}
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), true
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y), true
		}
	case time.Duration:
		if y, ok := b.(time.Duration); ok {
			return cmpOrdered(x, y), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			return cmpOrdered(b2i(x), b2i(y)), true
		}
	}
	return 0, false
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func cmpOrdered[T int | int64 | float64 | time.Duration](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func keyOf(v any) string {
	if t, ok := v.(time.Time); ok {
		return "t" + strconv.FormatInt(t.UnixNano(), 10)
	}
	if f, ok := v.(float64); ok && f == math.Trunc(f) && math.Abs(f) < 1e18 {
		v = int64(f)
	}
	return fmt.Sprintf("%T:%v", v, v)
}

// ---- static types ----

type env struct {
	now    time.Time
	tables func(name string) (*rel, bool)
	lets   map[string]*letDef
	busy   map[*letDef]bool // lets being resolved now; a second visit means a cycle
}

// useLet marks a let as being resolved and returns the call that clears it. A let that refers to itself
// (directly or through others) gets Kusto's "failed to resolve" error instead of looping forever.
func (e *env) useLet(l *letDef, name string) (func(), error) {
	if e.busy[l] {
		return nil, semanticf("Failed to resolve scalar expression named '%s'", name)
	}
	if e.busy == nil {
		e.busy = map[*letDef]bool{}
	}
	e.busy[l] = true
	return func() { delete(e.busy, l) }, nil
}

func isNum(t string) bool { return t == "long" || t == "int" || t == "real" }

func (e *env) ty(x expr, sch map[string]string) (string, error) {
	switch x := x.(type) {
	case *lit:
		switch x.v.(type) {
		case string:
			return "string", nil
		case int64:
			return "long", nil
		case float64:
			return "real", nil
		case bool:
			return "bool", nil
		case time.Time:
			return "datetime", nil
		case time.Duration:
			return "timespan", nil
		}
		return "dynamic", nil
	case *ref:
		if t, ok := sch[x.name]; ok {
			return t, nil
		}
		if l := e.lets[x.name]; l != nil && l.scalar != nil {
			done, err := e.useLet(l, x.name)
			if err != nil {
				return "", err
			}
			defer done()
			return e.ty(l.scalar, nil)
		}
		return "", semanticf("Failed to resolve scalar expression named '%s'", x.name)
	case *binary:
		l, err := e.ty(x.l, sch)
		if err != nil {
			return "", err
		}
		r, err := e.ty(x.r, sch)
		if err != nil {
			return "", err
		}
		switch x.op {
		case "+", "-", "*", "/", "%":
			switch {
			case l == "datetime" && r == "datetime":
				return "timespan", nil
			case l == "datetime":
				return "datetime", nil
			case r == "datetime" && x.op == "+":
				return "datetime", nil
			case l == "timespan" && r == "timespan" && x.op == "/":
				return "real", nil
			case l == "timespan" || r == "timespan":
				return "timespan", nil
			case l == "real" || r == "real":
				return "real", nil
			case isNum(l) && isNum(r):
				return "long", nil
			}
			return "dynamic", nil
		}
		return "bool", nil
	case *unary:
		return e.ty(x.x, sch)
	case *inList:
		for _, a := range append([]expr{x.x}, x.list...) {
			if _, err := e.ty(a, sch); err != nil {
				return "", err
			}
		}
		return "bool", nil
	case *between:
		for _, a := range []expr{x.x, x.lo, x.hi} {
			if _, err := e.ty(a, sch); err != nil {
				return "", err
			}
		}
		return "bool", nil
	case *array:
		for _, a := range x.items {
			if _, err := e.ty(a, sch); err != nil {
				return "", err
			}
		}
		return "dynamic", nil
	case *index:
		if _, err := e.ty(x.x, sch); err != nil {
			return "", err
		}
		if _, err := e.ty(x.key, sch); err != nil {
			return "", err
		}
		return "dynamic", nil
	case *call:
		f, ok := funcs[x.name]
		if !ok {
			if aggs[x.name] {
				return "", semanticf("The aggregate function '%s' can only be used in summarize", x.name)
			}
			return "", semanticf("The function '%s' is not supported", x.name)
		}
		if len(x.args) < f.min || f.max >= 0 && len(x.args) > f.max {
			return "", semanticf("%s(): wrong number of arguments", x.name)
		}
		ts := make([]string, len(x.args))
		for i, a := range x.args {
			t, err := e.ty(a, sch)
			if err != nil {
				return "", err
			}
			ts[i] = t
		}
		return f.ret(ts), nil
	}
	return "", syntaxf("unsupported expression")
}

// ---- evaluation ----

type scope struct {
	idx map[string]int
	row []any
	e   *env
}

func (s *scope) eval(x expr) any {
	switch x := x.(type) {
	case *lit:
		return x.v
	case *ref:
		if i, ok := s.idx[x.name]; ok && i < len(s.row) {
			return s.row[i]
		}
		if l := s.e.lets[x.name]; l != nil && l.scalar != nil {
			return (&scope{e: s.e}).eval(l.scalar)
		}
		return nil
	case *unary:
		switch v := s.eval(x.x).(type) {
		case int64:
			return -v
		case float64:
			return -v
		case time.Duration:
			return -v
		}
		return nil
	case *binary:
		return s.binary(x)
	case *inList:
		v := s.eval(x.x)
		var set []any
		for _, a := range x.list {
			if arr, ok := s.eval(a).([]any); ok && len(x.list) == 1 {
				set = arr
			} else {
				set = append(set, s.eval(a))
			}
		}
		found := false
		for _, m := range set {
			if looseEq(v, m) {
				found = true
				break
			}
		}
		return found != x.neg
	case *between:
		v, lo, hi := s.eval(x.x), s.eval(x.lo), s.eval(x.hi)
		c1, ok1 := cmpVals(v, lo)
		c2, ok2 := cmpVals(v, hi)
		if !ok1 || !ok2 {
			return nil
		}
		return (c1 >= 0 && c2 <= 0) != x.neg
	case *array:
		out := make([]any, len(x.items))
		for i, a := range x.items {
			out[i] = s.eval(a)
		}
		return out
	case *index:
		k := s.eval(x.key)
		switch c := s.eval(x.x).(type) {
		case map[string]any:
			if ks, ok := k.(string); ok {
				return c[ks]
			}
		case []any:
			if n, ok := toI(k); ok {
				if n < 0 {
					n += int64(len(c))
				}
				if n >= 0 && n < int64(len(c)) {
					return c[n]
				}
			}
		case string: // a dynamic that holds JSON text
			var m any
			if json.Unmarshal([]byte(c), &m) == nil {
				return (&scope{e: s.e}).eval(&index{&lit{m}, &lit{k}})
			}
		}
		return nil
	case *call:
		args := make([]any, len(x.args))
		for i, a := range x.args {
			args[i] = s.eval(a)
		}
		return funcs[x.name].call(s.e, args)
	}
	return nil
}

func tri(v any) (b, known bool) {
	b, known = v.(bool)
	return
}

func looseEq(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if sa, ok := a.(string); ok {
		sb, ok := b.(string)
		return ok && sa == sb
	}
	c, ok := cmpVals(a, b)
	return ok && c == 0
}

func (s *scope) binary(x *binary) any {
	l := s.eval(x.l)
	switch x.op {
	case "and":
		lb, lk := tri(l)
		if lk && !lb {
			return false
		}
		rb, rk := tri(s.eval(x.r))
		switch {
		case rk && !rb:
			return false
		case lk && rk:
			return true
		}
		return nil
	case "or":
		lb, lk := tri(l)
		if lk && lb {
			return true
		}
		rb, rk := tri(s.eval(x.r))
		switch {
		case rk && rb:
			return true
		case lk && rk:
			return false
		}
		return nil
	}
	r := s.eval(x.r)
	switch x.op {
	case "==", "!=":
		// Kusto strings are never null: a missing string is "".
		if _, ok := l.(string); ok && r == nil {
			r = ""
		} else if _, ok := r.(string); ok && l == nil {
			l = ""
		}
		if l == nil || r == nil {
			return nil
		}
		return looseEq(l, r) == (x.op == "==")
	case "<", "<=", ">", ">=":
		c, ok := cmpVals(l, r)
		if !ok {
			return nil
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
	case "=~", "!~":
		return strings.EqualFold(tostr(l), tostr(r)) == (x.op == "=~")
	case "+", "-", "*", "/", "%":
		return arith(x.op, l, r)
	case "matches regex":
		re, err := regexp.Compile(tostr(r))
		if err != nil {
			return nil
		}
		return re.MatchString(tostr(l))
	}
	neg := strings.HasPrefix(x.op, "!")
	ls, rs := strings.ToLower(tostr(l)), strings.ToLower(tostr(r))
	var m bool
	switch strings.TrimPrefix(x.op, "!") {
	case "contains":
		m = strings.Contains(ls, rs)
	case "startswith":
		m = strings.HasPrefix(ls, rs)
	case "endswith":
		m = strings.HasSuffix(ls, rs)
	case "has":
		m = hasTerm(ls, rs)
	}
	return m != neg
}

func isTermChar(c byte) bool { return isLetter(c) && c != '_' || isDigit(c) || c >= 0x80 }

// hasTerm is Kusto's has: s contains t as a whole term.
func hasTerm(s, t string) bool {
	if t == "" {
		return true
	}
	for from := 0; ; {
		i := strings.Index(s[from:], t)
		if i < 0 {
			return false
		}
		i += from
		end := i + len(t)
		if (i == 0 || !isTermChar(s[i-1]) || !isTermChar(t[0])) && (end == len(s) || !isTermChar(s[end]) || !isTermChar(t[len(t)-1])) {
			return true
		}
		from = i + 1
	}
}

func arith(op string, a, b any) any {
	switch x := a.(type) {
	case time.Time:
		switch y := b.(type) {
		case time.Time:
			if op == "-" {
				return x.Sub(y)
			}
		case time.Duration:
			switch op {
			case "+":
				return x.Add(y)
			case "-":
				return x.Add(-y)
			}
		}
		return nil
	case time.Duration:
		switch y := b.(type) {
		case time.Duration:
			switch op {
			case "+":
				return x + y
			case "-":
				return x - y
			case "/":
				return float64(x) / float64(y)
			}
		case time.Time:
			if op == "+" {
				return y.Add(x)
			}
		default:
			if f, ok := toF(b); ok {
				switch op {
				case "*":
					return time.Duration(float64(x) * f)
				case "/":
					return time.Duration(float64(x) / f)
				}
			}
		}
		return nil
	}
	if d, ok := b.(time.Duration); ok && op == "*" {
		if f, ok := toF(a); ok {
			return time.Duration(f * float64(d))
		}
	}
	if x, ok := a.(int64); ok {
		if y, ok := b.(int64); ok {
			switch op {
			case "+":
				return x + y
			case "-":
				return x - y
			case "*":
				return x * y
			case "/":
				if y == 0 {
					return nil
				}
				return x / y
			case "%":
				if y == 0 {
					return nil
				}
				return x % y
			}
		}
	}
	x, ok1 := toF(a)
	y, ok2 := toF(b)
	if !ok1 || !ok2 {
		return nil
	}
	switch op {
	case "+":
		return x + y
	case "-":
		return x - y
	case "*":
		return x * y
	case "/":
		return x / y
	}
	return math.Mod(x, y)
}

// ---- scalar functions ----

type fn struct {
	ret      func(args []string) string
	call     func(e *env, a []any) any
	min, max int // arity; max -1: any
}

var funcs map[string]fn

func fixed(t string) func([]string) string { return func([]string) string { return t } }
func argT(i int) func([]string) string {
	return func(a []string) string {
		if i < len(a) {
			return a[i]
		}
		return "dynamic"
	}
}

// binSize rounds down to a multiple of size.
func binVal(v, size any) any {
	switch x := v.(type) {
	case time.Time:
		d, ok := size.(time.Duration)
		if !ok || d <= 0 {
			return nil
		}
		n := x.UnixNano()
		r := n % int64(d)
		if r < 0 {
			r += int64(d)
		}
		return time.Unix(0, n-r).UTC()
	case time.Duration:
		d, ok := size.(time.Duration)
		if !ok || d <= 0 {
			return nil
		}
		return x - ((x%d)+d)%d
	case int64:
		if s, ok := size.(int64); ok && s > 0 {
			return x - ((x%s)+s)%s
		}
	}
	if f, ok := toF(v); ok {
		if s, ok := toF(size); ok && s > 0 {
			return math.Floor(f/s) * s
		}
	}
	return nil
}

func isNullish(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && s == ""
}

func init() {
	conv := func(t string, f func(any) any) fn {
		return fn{fixed(t), func(_ *env, a []any) any {
			if a[0] == nil {
				return nil
			}
			return f(a[0])
		}, 1, 1}
	}
	same := func(f func(*env, []any) any, min, max int) fn { return fn{argT(0), f, min, max} }
	funcs = map[string]fn{
		"now": {fixed("datetime"), func(e *env, a []any) any { return e.now }, 0, 0},
		"ago": {fixed("datetime"), func(e *env, a []any) any {
			if d, ok := a[0].(time.Duration); ok {
				return e.now.Add(-d)
			}
			return nil
		}, 1, 1},
		"todatetime": conv("datetime", toTime),
		"tolong":     conv("long", toLong), "long": conv("long", toLong),
		"toint": conv("int", toLong), "int": conv("int", toLong),
		"todouble": conv("real", toReal), "toreal": conv("real", toReal), "real": conv("real", toReal),
		"tobool": conv("bool", toBool), "toboolean": conv("bool", toBool), "bool": conv("bool", toBool),
		"totimespan": conv("timespan", toSpan), "timespan": conv("timespan", toSpan), "time": conv("timespan", toSpan),
		"tostring": {fixed("string"), func(_ *env, a []any) any { return tostr(a[0]) }, 1, 1},
		"string":   {fixed("string"), func(_ *env, a []any) any { return tostr(a[0]) }, 1, 1},
		"strcat": {fixed("string"), func(_ *env, a []any) any {
			var b strings.Builder
			for _, v := range a {
				b.WriteString(tostr(v))
			}
			return b.String()
		}, 1, -1},
		"strlen":  {fixed("long"), func(_ *env, a []any) any { return int64(utf8.RuneCountInString(tostr(a[0]))) }, 1, 1},
		"tolower": {fixed("string"), func(_ *env, a []any) any { return strings.ToLower(tostr(a[0])) }, 1, 1},
		"toupper": {fixed("string"), func(_ *env, a []any) any { return strings.ToUpper(tostr(a[0])) }, 1, 1},
		"substring": {fixed("string"), func(_ *env, a []any) any {
			r := []rune(tostr(a[0]))
			start, _ := toI(a[1])
			if start < 0 {
				start = max(0, int64(len(r))+start)
			}
			if start > int64(len(r)) {
				return ""
			}
			end := int64(len(r))
			if len(a) > 2 {
				if n, ok := toI(a[2]); ok {
					end = min(end, start+n)
				}
			}
			return string(r[start:max(start, end)])
		}, 2, 3},
		"replace_string": {fixed("string"), func(_ *env, a []any) any {
			return strings.ReplaceAll(tostr(a[0]), tostr(a[1]), tostr(a[2]))
		}, 3, 3},
		"extract": {fixed("string"), func(_ *env, a []any) any {
			re, err := regexp.Compile(tostr(a[0]))
			g, _ := toI(a[1])
			if err != nil || g < 0 {
				return nil
			}
			if m := re.FindStringSubmatch(tostr(a[2])); int(g) < len(m) {
				return m[g]
			}
			return nil
		}, 3, 3},
		"split": {fixed("dynamic"), func(_ *env, a []any) any {
			var out []any
			for _, p := range strings.Split(tostr(a[0]), tostr(a[1])) {
				out = append(out, p)
			}
			return out
		}, 2, 2},
		"parse_json": {fixed("dynamic"), parseJSON, 1, 1},
		"todynamic":  {fixed("dynamic"), parseJSON, 1, 1},
		"dynamic":    {fixed("dynamic"), func(_ *env, a []any) any { return a[0] }, 1, 1},
		"array_length": {fixed("long"), func(_ *env, a []any) any {
			if x, ok := a[0].([]any); ok {
				return int64(len(x))
			}
			return nil
		}, 1, 1},
		"iff": {func(a []string) string {
			if a[1] == "dynamic" {
				return a[2]
			}
			return a[1]
		}, func(_ *env, a []any) any {
			if b, _ := a[0].(bool); b {
				return a[1]
			}
			return a[2]
		}, 3, 3},
		"case": {func(a []string) string { return a[1] }, func(_ *env, a []any) any {
			for i := 0; i+1 < len(a); i += 2 {
				if b, _ := a[i].(bool); b {
					return a[i+1]
				}
			}
			if len(a)%2 == 1 {
				return a[len(a)-1]
			}
			return nil
		}, 3, -1},
		"coalesce": same(func(_ *env, a []any) any {
			for _, v := range a {
				if !isNullish(v) {
					return v
				}
			}
			return nil
		}, 1, -1),
		"isnull":     {fixed("bool"), func(_ *env, a []any) any { return a[0] == nil }, 1, 1},
		"isnotnull":  {fixed("bool"), func(_ *env, a []any) any { return a[0] != nil }, 1, 1},
		"isempty":    {fixed("bool"), func(_ *env, a []any) any { return isNullish(a[0]) }, 1, 1},
		"isnotempty": {fixed("bool"), func(_ *env, a []any) any { return !isNullish(a[0]) }, 1, 1},
		"not": {fixed("bool"), func(_ *env, a []any) any {
			if b, ok := a[0].(bool); ok {
				return !b
			}
			return nil
		}, 1, 1},
		"bin":   same(func(_ *env, a []any) any { return binVal(a[0], a[1]) }, 2, 2),
		"floor": same(func(_ *env, a []any) any { return binVal(a[0], a[1]) }, 2, 2),
		"round": same(func(_ *env, a []any) any {
			f, ok := toF(a[0])
			if !ok {
				return nil
			}
			p := 0.0
			if len(a) > 1 {
				p, _ = toF(a[1])
			}
			m := math.Pow(10, p)
			if _, isInt := a[0].(int64); isInt {
				return a[0]
			}
			return math.Round(f*m) / m
		}, 1, 2),
		"abs": same(func(_ *env, a []any) any {
			switch x := a[0].(type) {
			case int64:
				if x < 0 {
					return -x
				}
				return x
			case float64:
				return math.Abs(x)
			}
			return nil
		}, 1, 1),
		"startofday": {fixed("datetime"), func(_ *env, a []any) any {
			if t, ok := a[0].(time.Time); ok {
				return t.UTC().Truncate(24 * time.Hour)
			}
			return nil
		}, 1, 1},
	}
}

func toLong(v any) any {
	if n, ok := toI(v); ok {
		return n
	}
	return nil
}
func toReal(v any) any {
	switch x := v.(type) {
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return f
		}
		return nil
	case bool:
		return float64(b2i(x))
	}
	if f, ok := toF(v); ok {
		return f
	}
	return nil
}
func toTime(v any) any {
	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		if t, ok := parseTime(x); ok {
			return t
		}
	case int64, float64: // epoch seconds is not Kusto, but ingestion clients send it
		f, _ := toF(x)
		return time.Unix(int64(f), 0).UTC()
	}
	return nil
}
func toBool(v any) any {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(x) {
		case "true":
			return true
		case "false":
			return false
		}
	}
	if n, ok := toI(v); ok {
		return n != 0
	}
	return nil
}
func toSpan(v any) any {
	switch x := v.(type) {
	case time.Duration:
		return x
	case string:
		if d, err := time.ParseDuration(x); err == nil {
			return d
		}
	case int64:
		return time.Duration(x) // stored timespans are nanoseconds
	case float64:
		return time.Duration(x)
	}
	return nil
}

func parseJSON(_ *env, a []any) any {
	s, ok := a[0].(string)
	if !ok {
		return a[0]
	}
	var v any
	if json.Unmarshal([]byte(s), &v) != nil {
		return nil
	}
	return v
}

// ---- operators ----

var aggs = map[string]bool{"count": true, "countif": true, "sum": true, "sumif": true, "avg": true, "min": true,
	"max": true, "dcount": true, "make_list": true, "make_set": true}

func (r *rel) scope(e *env) (*scope, map[string]string) {
	idx := make(map[string]int, len(r.cols))
	for i, c := range r.cols {
		idx[c.Name] = i
	}
	return &scope{idx: idx, e: e}, r.schema()
}

// name is the output column name of an unnamed expression.
func exprName(x expr, n *int) string {
	switch x := x.(type) {
	case *ref:
		return x.name
	case *call:
		if len(x.args) > 0 {
			if r, ok := x.args[0].(*ref); ok {
				if x.name == "bin" || x.name == "floor" {
					return r.name
				}
				return x.name + "_" + r.name
			}
		}
	}
	*n++
	return fmt.Sprintf("Column%d", *n)
}

func (p *pipe) run(e *env) (*rel, error) {
	var r *rel
	switch {
	case p.sub != nil:
		var err error
		if r, err = p.sub.run(e); err != nil {
			return nil, err
		}
	default:
		if l := e.lets[p.table]; l != nil && l.tab != nil {
			done, err := e.useLet(l, p.table)
			if err != nil {
				return nil, err
			}
			r, err = l.tab.run(e)
			done()
			if err != nil {
				return nil, err
			}
		} else if t, ok := e.tables(p.table); ok {
			r = t
		} else {
			return nil, semanticf("Failed to resolve table or column expression named '%s'", p.table)
		}
	}
	for _, o := range p.ops {
		var err error
		if r, err = apply(e, o, r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (q *query) run(e *env) (*rel, error) {
	e.lets = q.lets
	return q.main.run(e)
}

func apply(e *env, o op, r *rel) (*rel, error) {
	sc, sch := r.scope(e)
	switch o := o.(type) {
	case *opWhere:
		if _, err := e.ty(o.cond, sch); err != nil {
			return nil, err
		}
		out := &rel{cols: r.cols}
		for _, row := range r.rows {
			sc.row = row
			if b, _ := sc.eval(o.cond).(bool); b {
				out.rows = append(out.rows, row)
			}
		}
		return out, nil
	case *opProject:
		out := &rel{}
		var exprs []expr
		n := 0
		for _, it := range o.items {
			t, err := e.ty(it.e, sch)
			if err != nil {
				return nil, err
			}
			out.cols = append(out.cols, col{cmpOr(it.name, exprName(it.e, &n)), t})
			exprs = append(exprs, it.e)
		}
		for _, row := range r.rows {
			sc.row = row
			nr := make([]any, len(exprs))
			for i, x := range exprs {
				nr[i] = sc.eval(x)
			}
			out.rows = append(out.rows, nr)
		}
		return out, nil
	case *opExtend:
		cur := r
		n := 0
		for _, it := range o.items {
			sc, sch := cur.scope(e)
			t, err := e.ty(it.e, sch)
			if err != nil {
				return nil, err
			}
			name := cmpOr(it.name, exprName(it.e, &n))
			pos, replace := sc.idx[name]
			next := &rel{cols: append([]col(nil), cur.cols...)}
			if replace {
				next.cols[pos].Type = t
			} else {
				next.cols = append(next.cols, col{name, t})
			}
			for _, row := range cur.rows {
				sc.row = row
				v := sc.eval(it.e)
				nr := append([]any(nil), row...)
				if replace {
					nr[pos] = v
				} else {
					nr = append(nr, v)
				}
				next.rows = append(next.rows, nr)
			}
			cur = next
		}
		return cur, nil
	case *opAway, *opKeep:
		var names []string
		_, away := o.(*opAway)
		if away {
			names = o.(*opAway).names
		} else {
			names = o.(*opKeep).names
		}
		for _, n := range names {
			if _, ok := sch[n]; !ok {
				return nil, semanticf("'project-%s' operator: Failed to resolve scalar expression named '%s'", map[bool]string{true: "away", false: "keep"}[away], n)
			}
		}
		var keep []int
		for i, c := range r.cols {
			if listed := containsStr(names, c.Name); listed != away {
				keep = append(keep, i)
			}
		}
		return pick(r, keep), nil
	case *opRename:
		out := &rel{cols: append([]col(nil), r.cols...), rows: r.rows}
		for _, it := range o.items {
			old, ok := it.e.(*ref)
			if !ok {
				return nil, syntaxf("project-rename expects name = column")
			}
			i, found := sc.idx[old.name]
			if !found {
				return nil, semanticf("'project-rename' operator: Failed to resolve scalar expression named '%s'", old.name)
			}
			out.cols[i].Name = it.name
		}
		return out, nil
	case *opSummary:
		return summarize(e, o, r)
	case *opSort:
		return sorted(e, r, o.keys)
	case *opTake:
		n, ok := toI(sc.eval(o.n))
		if !ok {
			return nil, semanticf("take expects an integer")
		}
		return &rel{cols: r.cols, rows: r.rows[:min(max(int(n), 0), len(r.rows))]}, nil
	case *opTop:
		s, err := sorted(e, r, []sortKey{o.k})
		if err != nil {
			return nil, err
		}
		n, ok := toI(sc.eval(o.n))
		if !ok {
			return nil, semanticf("top expects an integer")
		}
		return &rel{cols: s.cols, rows: s.rows[:min(max(int(n), 0), len(s.rows))]}, nil
	case *opDistinct:
		src := r
		if len(o.items) > 0 {
			var err error
			if src, err = apply(e, &opProject{o.items}, r); err != nil {
				return nil, err
			}
		}
		out := &rel{cols: src.cols}
		seen := map[string]bool{}
		for _, row := range src.rows {
			var k strings.Builder
			for _, v := range row {
				k.WriteString(keyOf(v) + "\x00")
			}
			if !seen[k.String()] {
				seen[k.String()] = true
				out.rows = append(out.rows, row)
			}
		}
		return out, nil
	case *opCount:
		return &rel{cols: []col{{"Count", "long"}}, rows: [][]any{{int64(len(r.rows))}}}, nil
	}
	return nil, syntaxf("unsupported operator")
}

func containsStr(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func pick(r *rel, keep []int) *rel {
	out := &rel{}
	for _, i := range keep {
		out.cols = append(out.cols, r.cols[i])
	}
	for _, row := range r.rows {
		nr := make([]any, len(keep))
		for j, i := range keep {
			nr[j] = row[i]
		}
		out.rows = append(out.rows, nr)
	}
	return out
}

func sorted(e *env, r *rel, keys []sortKey) (*rel, error) {
	sc, sch := r.scope(e)
	for _, k := range keys {
		if _, err := e.ty(k.e, sch); err != nil {
			return nil, err
		}
	}
	type srow struct {
		row []any
		k   []any
	}
	rows := make([]srow, len(r.rows))
	for i, row := range r.rows {
		sc.row = row
		rows[i].row = row
		for _, k := range keys {
			rows[i].k = append(rows[i].k, sc.eval(k.e))
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		for i, k := range keys {
			x, y := rows[a].k[i], rows[b].k[i]
			var c int
			switch {
			case x == nil && y == nil:
			case x == nil:
				c = -1
			case y == nil:
				c = 1
			default:
				c, _ = cmpVals(x, y)
			}
			if c != 0 {
				return (c < 0) != k.desc
			}
		}
		return false
	})
	out := &rel{cols: r.cols, rows: make([][]any, len(rows))}
	for i, x := range rows {
		out.rows[i] = x.row
	}
	return out, nil
}

// ---- summarize ----

type accum struct {
	n      int64
	isum   int64
	fsum   float64
	allInt bool
	best   any
	seen   map[string]bool
	list   []any
}

func summarize(e *env, o *opSummary, r *rel) (*rel, error) {
	sc, sch := r.scope(e)
	type agg struct {
		kind    string
		arg, cn expr // argument and condition
		typ     string
	}
	var aggl []agg
	out := &rel{}
	n := 0
	for _, it := range o.by {
		t, err := e.ty(it.e, sch)
		if err != nil {
			return nil, err
		}
		out.cols = append(out.cols, col{cmpOr(it.name, exprName(it.e, &n)), t})
	}
	for _, it := range o.aggs {
		c, ok := it.e.(*call)
		if !ok || !aggs[c.name] {
			return nil, semanticf("summarize expects aggregate functions such as count() or sum(x)")
		}
		a := agg{kind: c.name}
		want := map[string]int{"count": 0, "countif": 1, "sum": 1, "sumif": 2, "avg": 1, "min": 1, "max": 1, "dcount": 1, "make_list": 1, "make_set": 1}[c.name]
		if len(c.args) < want || len(c.args) > want+1 || len(c.args) == want+1 && !strings.HasPrefix(c.name, "make_") {
			return nil, semanticf("%s(): wrong number of arguments", c.name)
		}
		argT := "long"
		switch c.name {
		case "countif":
			a.cn = c.args[0]
		case "sumif":
			a.arg, a.cn = c.args[0], c.args[1]
		case "count":
		default:
			a.arg = c.args[0]
		}
		for _, x := range []expr{a.arg, a.cn} {
			if x != nil {
				t, err := e.ty(x, sch)
				if err != nil {
					return nil, err
				}
				if x == a.arg {
					argT = t
				}
			}
		}
		switch c.name {
		case "count", "countif", "dcount":
			a.typ = "long"
		case "sum", "sumif":
			a.typ = map[bool]string{true: "long", false: "real"}[argT == "long" || argT == "int" || argT == "bool"]
		case "avg":
			a.typ = "real"
		case "min", "max":
			a.typ = argT
		default:
			a.typ = "dynamic"
		}
		name := it.name
		if name == "" {
			name = c.name + "_"
			if ref, ok := a.arg.(*ref); ok {
				name += ref.name
			}
		}
		out.cols = append(out.cols, col{name, a.typ})
		aggl = append(aggl, a)
	}
	type group struct {
		key  []any
		accs []*accum
	}
	groups := map[string]*group{}
	var order []*group
	newGroup := func(key []any) *group {
		g := &group{key: key}
		for range aggl {
			g.accs = append(g.accs, &accum{allInt: true, seen: map[string]bool{}})
		}
		order = append(order, g)
		return g
	}
	for _, row := range r.rows {
		sc.row = row
		key := make([]any, len(o.by))
		var ks strings.Builder
		for i, it := range o.by {
			key[i] = sc.eval(it.e)
			ks.WriteString(keyOf(key[i]) + "\x00")
		}
		g := groups[ks.String()]
		if g == nil {
			g = newGroup(key)
			groups[ks.String()] = g
		}
		for i, a := range aggl {
			ac := g.accs[i]
			if a.cn != nil {
				if b, _ := sc.eval(a.cn).(bool); !b {
					continue
				}
			}
			if a.arg == nil {
				ac.n++
				continue
			}
			v := sc.eval(a.arg)
			if v == nil {
				continue
			}
			ac.n++
			switch a.kind {
			case "sum", "sumif", "avg":
				if iv, ok := v.(int64); ok {
					ac.isum += iv
					ac.fsum += float64(iv)
				} else if b, ok := v.(bool); ok {
					ac.isum += int64(b2i(b))
					ac.fsum += float64(b2i(b))
				} else if f, ok := toF(v); ok {
					ac.allInt = false
					ac.fsum += f
				}
			case "min", "max":
				if ac.best == nil {
					ac.best = v
				} else if c, ok := cmpVals(v, ac.best); ok && (a.kind == "min" && c < 0 || a.kind == "max" && c > 0) {
					ac.best = v
				}
			case "dcount":
				ac.seen[keyOf(v)] = true
			case "make_list":
				ac.list = append(ac.list, v)
			case "make_set":
				if k := keyOf(v); !ac.seen[k] {
					ac.seen[k] = true
					ac.list = append(ac.list, v)
				}
			}
		}
	}
	if len(o.by) == 0 && len(order) == 0 {
		newGroup(nil)
	}
	for _, g := range order {
		row := append([]any(nil), g.key...)
		for i, a := range aggl {
			ac := g.accs[i]
			var v any
			switch a.kind {
			case "count", "countif":
				v = ac.n
			case "sum", "sumif":
				if a.typ == "long" {
					v = ac.isum
				} else {
					v = ac.fsum
				}
			case "avg":
				if ac.n > 0 {
					v = ac.fsum / float64(ac.n)
				}
			case "min", "max":
				v = ac.best
			case "dcount":
				v = int64(len(ac.seen))
			default:
				if ac.list == nil {
					ac.list = []any{}
				}
				v = ac.list
			}
			row = append(row, v)
		}
		out.rows = append(out.rows, row)
	}
	return out, nil
}
