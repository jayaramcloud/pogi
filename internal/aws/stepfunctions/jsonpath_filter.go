package stepfunctions

// JSONPath filter expressions: [?(@.price > 10 && @.name == 'x')].
//
//	or    := and ('||' and)*
//	and   := not ('&&' not)*
//	not   := '!' not | cmp
//	cmp   := atom (('==' | '!=' | '<' | '<=' | '>' | '>=') atom)?
//	atom  := '(' or ')' | number | 'string' | true | false | null | @path | $path
//
// A path on its own tests that the value exists and is not null. A comparison
// with a missing value is false, except != which is true.

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type fparser struct {
	s string
	i int
}

// parseFilter compiles the text between [?( and )].
func parseFilter(src string) (func(item, root any) bool, error) {
	p := &fparser{s: src}
	f, err := p.or()
	if err == nil && p.ws() < len(p.s) {
		err = fmt.Errorf("unexpected %q in the filter", p.s[p.i:])
	}
	return f, err
}

func (p *fparser) ws() int {
	for p.i < len(p.s) && p.s[p.i] == ' ' {
		p.i++
	}
	return p.i
}

func (p *fparser) eat(tok string) bool {
	if p.ws(); strings.HasPrefix(p.s[p.i:], tok) {
		p.i += len(tok)
		return true
	}
	return false
}

type test = func(item, root any) bool

func (p *fparser) or() (test, error) {
	l, err := p.and()
	for err == nil && p.eat("||") {
		var r test
		if r, err = p.and(); err == nil {
			l = func(a, b test) test { return func(i, r any) bool { return a(i, r) || b(i, r) } }(l, r)
		}
	}
	return l, err
}

func (p *fparser) and() (test, error) {
	l, err := p.not()
	for err == nil && p.eat("&&") {
		var r test
		if r, err = p.not(); err == nil {
			l = func(a, b test) test { return func(i, r any) bool { return a(i, r) && b(i, r) } }(l, r)
		}
	}
	return l, err
}

func (p *fparser) not() (test, error) {
	if p.ws(); p.i < len(p.s) && p.s[p.i] == '!' && !strings.HasPrefix(p.s[p.i:], "!=") {
		p.i++
		f, err := p.not()
		return func(i, r any) bool { return !f(i, r) }, err
	}
	return p.cmp()
}

// value finds what an atom stands for: a literal or a path. ok is false when a path finds nothing.
type value = func(item, root any) (v any, ok bool)

func (p *fparser) cmp() (test, error) {
	if p.ws(); p.i < len(p.s) && p.s[p.i] == '(' {
		p.i++
		f, err := p.or()
		if err == nil && !p.eat(")") {
			err = fmt.Errorf("missing ) in the filter")
		}
		return f, err
	}
	l, isPath, err := p.atom()
	if err != nil {
		return nil, err
	}
	op := ""
	for _, o := range []string{"==", "!=", "<=", ">=", "<", ">"} {
		if p.eat(o) {
			op = o
			break
		}
	}
	if op == "" {
		return func(i, r any) bool {
			v, ok := l(i, r)
			if b, isBool := v.(bool); ok && !isPath && isBool {
				return b
			}
			return ok && v != nil
		}, nil
	}
	right, _, err := p.atom()
	if err != nil {
		return nil, err
	}
	return func(i, r any) bool {
		a, ok1 := l(i, r)
		b, ok2 := right(i, r)
		if !ok1 || !ok2 {
			return op == "!="
		}
		switch op {
		case "==":
			return reflect.DeepEqual(a, b)
		case "!=":
			return !reflect.DeepEqual(a, b)
		}
		switch x := a.(type) {
		case float64:
			if y, ok := b.(float64); ok {
				return holds(op, cmpFloat(x, y))
			}
		case string:
			if y, ok := b.(string); ok {
				return holds(op, strings.Compare(x, y))
			}
		}
		return false
	}, nil
}

func holds(op string, c int) bool {
	switch op {
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}
	return c >= 0
}

func (p *fparser) atom() (v value, isPath bool, err error) {
	p.ws()
	if p.i >= len(p.s) {
		return nil, false, fmt.Errorf("the filter ends too early")
	}
	lit := func(x any) value { return func(_, _ any) (any, bool) { return x, true } }
	rest := p.s[p.i:]
	switch c := rest[0]; {
	case c == '\'' || c == '"':
		var b strings.Builder
		for j := 1; j < len(rest); j++ {
			switch {
			case rest[j] == '\\' && j+1 < len(rest):
				j++
				b.WriteByte(rest[j])
			case rest[j] == c:
				p.i += j + 1
				return lit(b.String()), false, nil
			default:
				b.WriteByte(rest[j])
			}
		}
		return nil, false, fmt.Errorf("unterminated string in the filter")
	case c == '@' || c == '$':
		depth, quote, j := 0, byte(0), 1
	scan:
		for ; j < len(rest); j++ {
			switch d := rest[j]; {
			case quote != 0:
				if d == quote {
					quote = 0
				}
			case d == '\'' || d == '"':
				quote = d
			case d == '[':
				depth++
			case d == ']':
				depth--
			case depth == 0 && strings.IndexByte(" =!<>&|)", d) >= 0:
				break scan
			}
		}
		text := "$" + rest[1:j]
		segs, err := parsePath(text)
		if err != nil {
			return nil, false, err
		}
		p.i += j
		if c == '@' {
			return func(item, root any) (any, bool) { return get(item, segs, root) }, true, nil
		}
		return func(_, root any) (any, bool) { return get(root, segs, root) }, true, nil
	}
	j := strings.IndexAny(rest, " =!<>&|)")
	if j < 0 {
		j = len(rest)
	}
	tok := rest[:j]
	p.i += j
	switch tok {
	case "true", "false":
		return lit(tok == "true"), false, nil
	case "null":
		return lit(nil), false, nil
	}
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		return nil, false, fmt.Errorf("unexpected %q in the filter", tok)
	}
	return lit(f), false, nil
}
