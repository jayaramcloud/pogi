package logs

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// matcher reports whether a log message matches a compiled filter pattern.
type matcher func(msg string) bool

// compile parses the CloudWatch filter pattern subset pogi supports: terms
// that must all appear, "quoted phrases", -exclusions, ?alternatives,
// %regex%, and JSON patterns such as { $.level = "ERROR" && $.ms > 100 }.
func compile(pattern string) (matcher, error) {
	bad := func(err error) error {
		return core.Errorf(400, "InvalidParameterException", "Invalid filter pattern %q: %v", pattern, err)
	}
	p := strings.TrimSpace(pattern)
	switch {
	case p == "" || p == `""`:
		return func(string) bool { return true }, nil
	case strings.HasPrefix(p, "{"):
		if !strings.HasSuffix(p, "}") {
			return nil, bad(errors.New("missing }"))
		}
		n, err := parseExpr(p[1:len(p)-1], true)
		if err != nil {
			return nil, bad(err)
		}
		return func(msg string) bool {
			var doc any
			return json.Unmarshal([]byte(msg), &doc) == nil && n.eval(func(sel string) (any, bool) { return lookup(doc, sel) })
		}, nil
	case strings.HasPrefix(p, "["):
		// ponytail: space-delimited [ip, user, status=404] patterns are accepted but match every event; parse the columns if anyone filters on them.
		return func(string) bool { return true }, nil
	}
	var all, none, some []func(string) bool
	for _, t := range splitTerms(p) {
		list := &all
		switch t[0] {
		case '-':
			list, t = &none, t[1:]
		case '?':
			list, t = &some, t[1:]
		}
		if len(t) >= 2 && t[0] == '"' && t[len(t)-1] == '"' {
			t = t[1 : len(t)-1]
		}
		f := func(m string) bool { return strings.Contains(m, t) }
		if len(t) >= 2 && t[0] == '%' && t[len(t)-1] == '%' {
			re, err := regexp.Compile(t[1 : len(t)-1])
			if err != nil {
				return nil, bad(err)
			}
			f = re.MatchString
		}
		*list = append(*list, f)
	}
	return func(m string) bool {
		for _, f := range all {
			if !f(m) {
				return false
			}
		}
		for _, f := range none {
			if f(m) {
				return false
			}
		}
		return len(some) == 0 || slices.ContainsFunc(some, func(f func(string) bool) bool { return f(m) })
	}, nil
}

// splitTerms splits a text pattern on blanks, keeping "quoted phrases" whole.
func splitTerms(p string) []string {
	var out []string
	var b strings.Builder
	quoted := false
	for i := 0; i < len(p); i++ {
		switch c := p[i]; {
		case c == '"':
			quoted = !quoted
			b.WriteByte(c)
		case c == '\\' && quoted && i+1 < len(p):
			i++
			b.WriteByte(p[i])
		case (c == ' ' || c == '\t') && !quoted:
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
		default:
			b.WriteByte(c)
		}
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

// lookup follows a selector such as $.a.b[0].c (a.b in Insights) into decoded JSON.
func lookup(doc any, sel string) (any, bool) {
	cur := doc
	for _, part := range strings.FieldsFunc(strings.TrimPrefix(sel, "$"), func(r rune) bool { return r == '.' || r == '[' }) {
		if idx, ok := strings.CutSuffix(part, "]"); ok {
			arr, _ := cur.([]any)
			i, err := strconv.Atoi(idx)
			if err != nil || i < 0 || i >= len(arr) {
				return nil, false
			}
			cur = arr[i]
			continue
		}
		obj, _ := cur.(map[string]any)
		v, ok := obj[part]
		if !ok {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func num(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}

// ---- boolean expressions: JSON filter patterns and Insights filters ----

type token struct {
	kind byte // 'w' word, 's' quoted string, 'r' /regex/, 'o' operator or parenthesis
	text string
}

func lex(src string) ([]token, error) {
	const ops = "=!<>&|~"
	var out []token
	for i := 0; i < len(src); {
		switch c := src[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(' || c == ')':
			out = append(out, token{'o', string(c)})
			i++
		case c == '"' || c == '\'' || c == '/':
			var b strings.Builder
			j := i + 1
			for ; j < len(src) && src[j] != c; j++ {
				if src[j] == '\\' && j+1 < len(src) {
					if c == '/' && src[j+1] != '/' {
						b.WriteByte('\\') // keep regex escapes such as \d
					}
					j++
				}
				b.WriteByte(src[j])
			}
			if j == len(src) {
				return nil, fmt.Errorf("unterminated %c", c)
			}
			kind := byte('s')
			if c == '/' {
				kind = 'r'
			}
			out = append(out, token{kind, b.String()})
			i = j + 1
		case strings.IndexByte(ops, c) >= 0:
			j := i + 1
			for j < len(src) && strings.IndexByte(ops, src[j]) >= 0 {
				j++
			}
			out = append(out, token{'o', src[i:j]})
			i = j
		default:
			j := i
			for j < len(src) && strings.IndexByte(" \t\r\n()\"'"+ops, src[j]) < 0 {
				j++
			}
			out = append(out, token{'w', src[i:j]})
			i = j
		}
	}
	return out, nil
}

// node is a parsed expression: and/or/not over comparisons of one field.
type node struct {
	op    string // and, or, not, exists, null, true, false, like, =, !=, <, <=, >, >=
	kids  []*node
	field string
	val   string
	num   float64
	isNum bool
	re    *regexp.Regexp // like, and wildcard = / != in filter patterns
}

type parser struct {
	toks []token
	i    int
	glob bool // filter patterns: '*' in = and != values is a wildcard
}

func parseExpr(src string, glob bool) (*node, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, glob: glob}
	n, err := p.or()
	if err == nil && p.i < len(p.toks) {
		err = fmt.Errorf("unexpected %q", p.toks[p.i].text)
	}
	return n, err
}

// peek reports whether the next token is one of words (case-insensitive).
func (p *parser) peek(words ...string) bool {
	if p.i >= len(p.toks) || p.toks[p.i].kind == 's' || p.toks[p.i].kind == 'r' {
		return false
	}
	return slices.ContainsFunc(words, func(w string) bool { return strings.EqualFold(p.toks[p.i].text, w) })
}

func (p *parser) next() token {
	if p.i >= len(p.toks) {
		return token{}
	}
	p.i++
	return p.toks[p.i-1]
}

func (p *parser) or() (*node, error) {
	l, err := p.and()
	for err == nil && p.peek("||", "or") {
		p.next()
		var r *node
		r, err = p.and()
		l = &node{op: "or", kids: []*node{l, r}}
	}
	return l, err
}

func (p *parser) and() (*node, error) {
	l, err := p.unary()
	for err == nil && p.peek("&&", "and") {
		p.next()
		var r *node
		r, err = p.unary()
		l = &node{op: "and", kids: []*node{l, r}}
	}
	return l, err
}

func (p *parser) unary() (*node, error) {
	switch {
	case p.peek("not", "!"):
		p.next()
		n, err := p.unary()
		return &node{op: "not", kids: []*node{n}}, err
	case p.peek("("):
		p.next()
		n, err := p.or()
		if err == nil && !p.peek(")") {
			err = errors.New("missing )")
		}
		p.next()
		return n, err
	}
	return p.compare()
}

func (p *parser) compare() (*node, error) {
	f := p.next()
	if f.kind != 'w' {
		return nil, fmt.Errorf("expected a field, got %q", f.text)
	}
	n := &node{field: f.text}
	neg := p.peek("not")
	if neg {
		p.next()
	}
	switch {
	case p.peek("exists"):
		p.next()
		n.op = "exists"
	case p.peek("is"):
		p.next()
		if p.peek("not") {
			p.next()
			neg = !neg
		}
		if n.op = strings.ToLower(p.next().text); n.op != "null" && n.op != "true" && n.op != "false" {
			return nil, errors.New("IS must be followed by NULL, TRUE or FALSE")
		}
	case p.peek("like", "=~"):
		p.next()
		t := p.next()
		n.op = "like"
		switch t.kind {
		case 'r':
			re, err := regexp.Compile(t.text)
			if err != nil {
				return nil, err
			}
			n.re = re
		case 's':
			n.re = regexp.MustCompile(regexp.QuoteMeta(t.text))
		default:
			return nil, errors.New(`like needs a /regex/ or a "string"`)
		}
	case neg:
		return nil, errors.New("NOT must be followed by EXISTS or LIKE")
	default:
		op, v := p.next(), p.next()
		if op.kind != 'o' || !slices.Contains([]string{"=", "==", "!=", "<", "<=", ">", ">="}, op.text) {
			return nil, fmt.Errorf("expected a comparison after %s", f.text)
		}
		if v.kind != 'w' && v.kind != 's' {
			return nil, fmt.Errorf("expected a value after %s %s", f.text, op.text)
		}
		n.op, n.val = op.text, v.text
		if n.op == "==" {
			n.op = "="
		}
		if v.kind == 'w' {
			var err error
			n.num, err = strconv.ParseFloat(v.text, 64)
			n.isNum = err == nil
		}
		if p.glob && (n.op == "=" || n.op == "!=") && strings.Contains(v.text, "*") {
			n.re = regexp.MustCompile("^" + strings.ReplaceAll(regexp.QuoteMeta(v.text), `\*`, ".*") + "$")
		}
	}
	if neg {
		return &node{op: "not", kids: []*node{n}}, nil
	}
	return n, nil
}

func (n *node) eval(get func(string) (any, bool)) bool {
	switch n.op {
	case "and":
		return n.kids[0].eval(get) && n.kids[1].eval(get)
	case "or":
		return n.kids[0].eval(get) || n.kids[1].eval(get)
	case "not":
		return !n.kids[0].eval(get)
	}
	v, ok := get(n.field)
	switch {
	case n.op == "exists":
		return ok
	case n.op == "null":
		return ok && v == nil
	case n.op == "true" || n.op == "false":
		return v == (n.op == "true")
	case !ok || v == nil:
		return false
	case n.re != nil:
		return n.re.MatchString(str(v)) != (n.op == "!=")
	}
	c := strings.Compare(str(v), n.val)
	if n.isNum {
		f, isNum := num(v)
		if !isNum {
			return n.op == "!="
		}
		c = cmp.Compare(f, n.num)
	}
	switch n.op {
	case "=":
		return c == 0
	case "!=":
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

// ---- Logs Insights ----

// insights is a parsed Logs Insights query: fields, filter, sort and limit.
type insights struct {
	fields  []string
	filters []*node
	sortBy  string
	desc    bool
	limit   int
}

// splitPipes splits a query into commands, ignoring | inside quotes and /regexes/.
func splitPipes(q string) []string {
	var parts []string
	var quote byte
	start := 0
	for i := 0; i < len(q); i++ {
		switch c := q[i]; {
		case quote != 0 && c == '\\':
			i++
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'' || c == '/':
			quote = c
		case c == '|':
			parts = append(parts, q[start:i])
			start = i + 1
		}
	}
	return append(parts, q[start:])
}

func parseQuery(q string) (*insights, error) {
	out := &insights{sortBy: "@timestamp", desc: true}
	for _, cmd := range splitPipes(q) {
		cmd = strings.TrimSpace(cmd)
		i := strings.IndexAny(cmd, " \t\r\n")
		if i < 0 {
			i = len(cmd)
		}
		verb, rest := strings.ToLower(cmd[:i]), strings.TrimSpace(cmd[i:])
		switch verb {
		case "":
		case "fields", "display":
			for _, f := range strings.Split(rest, ",") {
				if f = strings.TrimSpace(f); f != "" {
					out.fields = append(out.fields, f)
				}
			}
		case "filter":
			n, err := parseExpr(rest, false)
			if err != nil {
				return nil, err
			}
			out.filters = append(out.filters, n)
		case "sort":
			f := strings.Fields(strings.ReplaceAll(rest, ",", " "))
			if len(f) == 0 {
				return nil, errors.New("sort needs a field")
			}
			out.sortBy, out.desc = f[0], len(f) > 1 && strings.EqualFold(f[1], "desc")
		case "limit":
			n, err := strconv.Atoi(rest)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("invalid limit %q", rest)
			}
			out.limit = n
		default:
			// ponytail: no stats/parse/dedup; add them when a test needs aggregation.
			return nil, fmt.Errorf("pogi runs the fields, filter, sort and limit commands, not %q", verb)
		}
	}
	if len(out.fields) == 0 {
		out.fields = []string{"@timestamp", "@message", "@logStream", "@log"}
	}
	return out, nil
}

type resultField struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

type queryStats struct {
	RecordsMatched float64 `json:"recordsMatched"`
	RecordsScanned float64 `json:"recordsScanned"`
	BytesScanned   float64 `json:"bytesScanned"`
}

// fields resolves Insights fields of one event: @-fields, then JSON keys of the message.
func fields(g *Group, stream string, e event) func(string) (any, bool) {
	var doc any
	parsed := false
	return func(name string) (any, bool) {
		switch name {
		case "@timestamp":
			return float64(e.T), true
		case "@ingestionTime":
			return float64(e.In), true
		case "@message":
			return e.M, true
		case "@logStream":
			return stream, true
		case "@log":
			return g.Account + ":" + g.Name, true
		}
		if !parsed {
			parsed = true
			json.Unmarshal([]byte(e.M), &doc)
		}
		return lookup(doc, name)
	}
}

// run executes the query over events in [start, end] (epoch millis). Lock held.
func (q *insights) run(groups []*Group, start, end int64, limit int) ([][]resultField, queryStats) {
	type row struct {
		get func(string) (any, bool)
		id  int64
	}
	var rows []row
	var st queryStats
	for _, g := range groups {
		for _, name := range core.SortedKeys(g.Streams) {
			for _, e := range g.Streams[name].Events {
				if e.T < start || e.T > end {
					continue
				}
				st.RecordsScanned++
				st.BytesScanned += float64(len(e.M))
				get := fields(g, name, e)
				if !slices.ContainsFunc(q.filters, func(n *node) bool { return !n.eval(get) }) {
					rows = append(rows, row{get, e.ID})
				}
			}
		}
	}
	st.RecordsMatched = float64(len(rows))
	slices.SortStableFunc(rows, func(a, b row) int {
		x, _ := a.get(q.sortBy)
		y, _ := b.get(q.sortBy)
		c := strings.Compare(str(x), str(y))
		if fx, ok := num(x); ok {
			if fy, ok := num(y); ok {
				c = cmp.Compare(fx, fy)
			}
		}
		if q.desc {
			return -c
		}
		return c
	})
	if q.limit > 0 {
		limit = min(limit, q.limit)
	}
	rows = rows[:min(len(rows), limit)]
	out := make([][]resultField, len(rows))
	for i, r := range rows {
		for _, f := range q.fields {
			v, ok := r.get(f)
			if !ok {
				continue
			}
			s := str(v)
			if f == "@timestamp" || f == "@ingestionTime" {
				s = time.UnixMilli(int64(v.(float64))).UTC().Format("2006-01-02 15:04:05.000")
			}
			out[i] = append(out[i], resultField{f, s})
		}
		out[i] = append(out[i], resultField{"@ptr", strconv.FormatInt(r.id, 10)})
	}
	return out, st
}
