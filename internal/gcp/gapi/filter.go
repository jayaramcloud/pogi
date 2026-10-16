package gapi

import (
	"fmt"
	"strings"
)

// Filter is a parsed Google filter expression, in the syntax Cloud Logging
// and Cloud Monitoring share: terms joined by AND (or just a space) and OR,
// NOT or -, and parentheses. OR binds tighter than AND, as in Logging.
//
//	field OP value      OP is one of = != > >= < <= : =~ !~
//	field = fn(a, b)    a value function (Monitoring: starts_with, one_of, ...)
//	fn(a, b)            a function alone (Logging: log_id("x"))
//	word, "a phrase"    a global text search
//
// The service gives the terms their meaning: see Match.
type Filter struct{ root *node }

type node struct {
	kind    string // and, or, not, term, text
	kids    []*node
	field   string
	op, val string
}

// ParseFilter parses a filter; the empty filter matches everything.
func ParseFilter(s string) (*Filter, error) {
	p := &fparser{s: s}
	p.skip()
	if p.i >= len(s) {
		return &Filter{}, nil
	}
	n, err := p.and()
	if p.skip(); err == nil && p.i < len(s) {
		err = fmt.Errorf("unexpected %q", s[p.i:])
	}
	if err != nil {
		return nil, fmt.Errorf("invalid filter %q: %w", s, err)
	}
	return &Filter{n}, nil
}

// Match evaluates the filter. term decides "field op value" (a value function
// gives op "fn", or "!fn" for !=, and its arguments joined by NUL as the
// value; a function alone gives field "fn" and op "()"), and text decides a
// global search.
func (f *Filter) Match(term func(field, op, value string) bool, text func(string) bool) bool {
	return f.root == nil || f.root.eval(term, text)
}

func (n *node) eval(term func(field, op, value string) bool, text func(string) bool) bool {
	switch n.kind {
	case "and":
		for _, k := range n.kids {
			if !k.eval(term, text) {
				return false
			}
		}
		return true
	case "or":
		for _, k := range n.kids {
			if k.eval(term, text) {
				return true
			}
		}
		return false
	case "not":
		return !n.kids[0].eval(term, text)
	case "text":
		return text(n.val)
	}
	return term(n.field, n.op, n.val)
}

type fparser struct {
	s string
	i int
}

func (p *fparser) skip() {
	for p.i < len(p.s) && strings.IndexByte(" \t\r\n", p.s[p.i]) >= 0 {
		p.i++
	}
}

// keyword consumes AND, OR or NOT when it stands alone.
func (p *fparser) keyword(k string) bool {
	p.skip()
	e := p.i + len(k)
	if strings.HasPrefix(p.s[p.i:], k) && (e == len(p.s) || strings.IndexByte(" \t\r\n(", p.s[e]) >= 0) {
		p.i = e
		return true
	}
	return false
}

func (p *fparser) and() (*node, error) {
	n := &node{kind: "and"}
	for {
		k, err := p.or()
		if err != nil {
			return nil, err
		}
		n.kids = append(n.kids, k)
		p.keyword("AND")
		if p.skip(); p.i >= len(p.s) || p.s[p.i] == ')' {
			break
		}
	}
	return n, nil
}

func (p *fparser) or() (*node, error) {
	n := &node{kind: "or"}
	for {
		k, err := p.unary()
		if err != nil {
			return nil, err
		}
		n.kids = append(n.kids, k)
		if !p.keyword("OR") {
			break
		}
	}
	return n, nil
}

func (p *fparser) unary() (*node, error) {
	p.skip()
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("unexpected end")
	}
	minus := p.s[p.i] == '-' && p.i+1 < len(p.s) && p.s[p.i+1] != ' '
	if minus {
		p.i++
	}
	if minus || p.keyword("NOT") {
		k, err := p.unary()
		return &node{kind: "not", kids: []*node{k}}, err
	}
	if p.s[p.i] == '(' {
		p.i++
		n, err := p.and()
		if err != nil {
			return nil, err
		}
		if p.skip(); p.i >= len(p.s) || p.s[p.i] != ')' {
			return nil, fmt.Errorf("missing )")
		}
		p.i++
		return n, nil
	}
	return p.term()
}

const (
	fieldStop = " \t\r\n()=!<>:,~"
	valueStop = " \t\r\n(),"
)

// word reads a bare token, up to a byte of stop, or a quoted string
// (unescaped); quotes inside a bare token (labels."a.b") stay part of it.
func (p *fparser) word(stop string) (s string, quoted bool, err error) {
	p.skip()
	if p.i < len(p.s) && p.s[p.i] == '"' {
		s, err = p.quote()
		return s, true, err
	}
	start := p.i
	for p.i < len(p.s) && strings.IndexByte(stop, p.s[p.i]) < 0 {
		if p.s[p.i] == '"' {
			if _, err = p.quote(); err != nil {
				return "", false, err
			}
			continue
		}
		p.i++
	}
	if p.i == start {
		return "", false, fmt.Errorf("unexpected %q", p.s[p.i:])
	}
	return p.s[start:p.i], false, nil
}

func (p *fparser) quote() (string, error) {
	var b strings.Builder
	for p.i++; p.i < len(p.s); p.i++ {
		switch c := p.s[p.i]; {
		case c == '\\' && p.i+1 < len(p.s):
			p.i++
			b.WriteByte(p.s[p.i])
		case c == '"':
			p.i++
			return b.String(), nil
		default:
			b.WriteByte(c)
		}
	}
	return "", fmt.Errorf("unterminated string")
}

var filterOps = []string{">=", "<=", "=~", "!~", "!=", "=", ">", "<", ":"}

func (p *fparser) op() string {
	p.skip()
	for _, o := range filterOps {
		if strings.HasPrefix(p.s[p.i:], o) {
			p.i += len(o)
			return o
		}
	}
	return ""
}

// args reads "(a, b, ...)" with p at the "(".
func (p *fparser) args() ([]string, error) {
	var out []string
	p.i++
	for {
		p.skip()
		if p.i < len(p.s) && p.s[p.i] == ')' {
			p.i++
			return out, nil
		}
		w, _, err := p.word(valueStop)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
		if p.skip(); p.i < len(p.s) && p.s[p.i] == ',' {
			p.i++
		}
	}
}

func (p *fparser) term() (*node, error) {
	w, quoted, err := p.word(fieldStop)
	if err != nil {
		return nil, err
	}
	if p.i < len(p.s) && p.s[p.i] == '(' && !quoted { // fn(args)
		a, err := p.args()
		return &node{kind: "term", field: w, op: "()", val: strings.Join(a, "\x00")}, err
	}
	save := p.i
	op := p.op()
	if !quoted {
		w = strings.ReplaceAll(w, `"`, "") // labels."a.b" is the field labels.a.b
	}
	if op == "" || quoted {
		p.i = save
		return &node{kind: "text", val: w}, nil
	}
	p.skip()
	v, vq, err := p.word(valueStop)
	if err != nil {
		return nil, fmt.Errorf("%s %s needs a value", w, op)
	}
	if p.i < len(p.s) && p.s[p.i] == '(' && !vq { // field = fn(args)
		a, err := p.args()
		if op == "!=" {
			v = "!" + v
		} else if op != "=" {
			return nil, fmt.Errorf("%s %s %s(...) is not supported", w, op, v)
		}
		return &node{kind: "term", field: w, op: v, val: strings.Join(a, "\x00")}, err
	}
	return &node{kind: "term", field: w, op: op, val: v}, nil
}
