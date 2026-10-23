package workflows

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Values are what encoding/json gives (nil, bool, string, []any, map[string]any), except that whole
// numbers are int64 and the others float64, as in the Workflows language.

// wfErr is an error a workflow can raise, catch and retry. val is what an `except` block sees
// (a map with message and tags, or whatever `raise` was given).
type wfErr struct {
	val   any
	fatal bool // cannot be caught: the workflow uses something pogi does not support
	stack []frame
}

type frame struct{ step, routine string }

func (e *wfErr) Error() string {
	if m, ok := e.val.(map[string]any); ok {
		if s, ok := m["message"].(string); ok {
			return s
		}
	}
	b, _ := json.Marshal(e.val)
	return string(b)
}

func newErr(msg string, tags ...string) *wfErr {
	t := make([]any, len(tags))
	for i, s := range tags {
		t[i] = s
	}
	return &wfErr{val: map[string]any{"message": msg, "tags": t}}
}

func unsupported(format string, a ...any) *wfErr {
	e := newErr("pogi does not support "+fmt.Sprintf(format, a...), "UnsupportedError")
	e.fatal = true
	return e
}

// ---- tokens ----

type token struct {
	kind byte // n number, s string, i identifier, o operator, 0 end
	text string
	val  any
}

func lex(src string) ([]token, error) {
	var out []token
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c >= '0' && c <= '9':
			j := i
			for j < len(src) && (src[j] >= '0' && src[j] <= '9' || src[j] == '.' || src[j] == 'e' || src[j] == 'E' ||
				(src[j] == '-' || src[j] == '+') && (src[j-1] == 'e' || src[j-1] == 'E')) {
				j++
			}
			var v any
			if n, err := strconv.ParseInt(src[i:j], 10, 64); err == nil {
				v = n
			} else if f, err := strconv.ParseFloat(src[i:j], 64); err == nil {
				v = f
			} else {
				return nil, fmt.Errorf("bad number %q", src[i:j])
			}
			out = append(out, token{kind: 'n', val: v})
			i = j
		case c == '"' || c == '\'':
			var sb strings.Builder
			j := i + 1
			for ; j < len(src) && src[j] != c; j++ {
				if src[j] == '\\' && j+1 < len(src) {
					j++
					switch src[j] {
					case 'n':
						sb.WriteByte('\n')
					case 't':
						sb.WriteByte('\t')
					case 'r':
						sb.WriteByte('\r')
					default:
						sb.WriteByte(src[j])
					}
					continue
				}
				sb.WriteByte(src[j])
			}
			if j >= len(src) {
				return nil, fmt.Errorf("unterminated string")
			}
			out = append(out, token{kind: 's', val: sb.String()})
			i = j + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			j := i
			for j < len(src) && (src[j] == '_' || src[j] >= 'a' && src[j] <= 'z' || src[j] >= 'A' && src[j] <= 'Z' || src[j] >= '0' && src[j] <= '9') {
				j++
			}
			out = append(out, token{kind: 'i', text: src[i:j]})
			i = j
		default:
			op := string(c)
			if i+1 < len(src) {
				if two := src[i : i+2]; two == "==" || two == "!=" || two == "<=" || two == ">=" || two == "//" {
					op = two
				}
			}
			if !strings.Contains("+-*/%<>()[].,{}:", op[:1]) && op != "==" && op != "!=" {
				return nil, fmt.Errorf("unexpected character %q", op)
			}
			out = append(out, token{kind: 'o', text: op})
			i += len(op)
		}
	}
	return append(out, token{kind: 0}), nil
}

// ---- syntax tree ----

type node interface{}

type (
	lit    struct{ v any }
	list   struct{ elems []node }
	object struct{ keys, vals []node }
	ref    struct {
		name string
		sels []node
	} // a variable with .key and [index] selectors; a selector is a node
	call struct {
		fn   string
		args []node
	}
	unary struct {
		op string
		x  node
	}
	binary struct {
		op   string
		l, r node
	}
)

type parser struct {
	t []token
	i int
}

func parseExpr(src string) (node, error) {
	t, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{t: t}
	n, err := p.or()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != 0 {
		return nil, fmt.Errorf("unexpected %q", p.peek().text)
	}
	return n, nil
}

func (p *parser) peek() token { return p.t[p.i] }
func (p *parser) next() token { t := p.t[p.i]; p.i++; return t }
func (p *parser) isOp(s string) bool {
	t := p.peek()
	return t.kind == 'o' && t.text == s
}
func (p *parser) isKw(s string) bool {
	t := p.peek()
	return t.kind == 'i' && t.text == s
}

func (p *parser) or() (node, error) {
	l, err := p.and()
	for err == nil && p.isKw("or") {
		p.next()
		var r node
		if r, err = p.and(); err == nil {
			l = &binary{"or", l, r}
		}
	}
	return l, err
}

func (p *parser) and() (node, error) {
	l, err := p.not()
	for err == nil && p.isKw("and") {
		p.next()
		var r node
		if r, err = p.not(); err == nil {
			l = &binary{"and", l, r}
		}
	}
	return l, err
}

func (p *parser) not() (node, error) {
	if p.isKw("not") {
		p.next()
		x, err := p.not()
		return &unary{"not", x}, err
	}
	return p.cmp()
}

func (p *parser) cmp() (node, error) {
	l, err := p.add()
	for err == nil {
		t := p.peek()
		op := t.text
		switch {
		case t.kind == 'o' && (op == "==" || op == "!=" || op == "<" || op == ">" || op == "<=" || op == ">="):
		case t.kind == 'i' && op == "in":
		case t.kind == 'i' && op == "not" && p.t[p.i+1].text == "in":
			p.next()
			op = "not in"
		default:
			return l, nil
		}
		p.next()
		var r node
		if r, err = p.add(); err == nil {
			l = &binary{op, l, r}
		}
	}
	return l, err
}

func (p *parser) add() (node, error) {
	l, err := p.mul()
	for err == nil && (p.isOp("+") || p.isOp("-")) {
		op := p.next().text
		var r node
		if r, err = p.mul(); err == nil {
			l = &binary{op, l, r}
		}
	}
	return l, err
}

func (p *parser) mul() (node, error) {
	l, err := p.unary()
	for err == nil && (p.isOp("*") || p.isOp("/") || p.isOp("//") || p.isOp("%")) {
		op := p.next().text
		var r node
		if r, err = p.unary(); err == nil {
			l = &binary{op, l, r}
		}
	}
	return l, err
}

func (p *parser) unary() (node, error) {
	if p.isOp("-") {
		p.next()
		x, err := p.unary()
		return &unary{"-", x}, err
	}
	return p.primary()
}

func (p *parser) primary() (node, error) {
	t := p.next()
	switch {
	case t.kind == 'n' || t.kind == 's':
		return &lit{t.val}, nil
	case t.kind == 'o' && t.text == "[":
		l := &list{}
		for !p.isOp("]") {
			e, err := p.or()
			if err != nil {
				return nil, err
			}
			l.elems = append(l.elems, e)
			if p.isOp(",") {
				p.next()
			} else if !p.isOp("]") {
				return nil, fmt.Errorf("missing ] in a list")
			}
		}
		p.next()
		return l, nil
	case t.kind == 'o' && t.text == "{":
		o := &object{}
		for !p.isOp("}") {
			k, err := p.or()
			if err != nil {
				return nil, err
			}
			if !p.isOp(":") {
				return nil, fmt.Errorf("missing : after a map key")
			}
			p.next()
			v, err := p.or()
			if err != nil {
				return nil, err
			}
			o.keys, o.vals = append(o.keys, k), append(o.vals, v)
			if p.isOp(",") {
				p.next()
			} else if !p.isOp("}") {
				return nil, fmt.Errorf("missing } in a map")
			}
		}
		p.next()
		return o, nil
	case t.kind == 'o' && t.text == "(":
		n, err := p.or()
		if err == nil && !p.isOp(")") {
			err = fmt.Errorf("missing )")
		} else if err == nil {
			p.next()
		}
		return n, err
	case t.kind == 'i':
		switch t.text {
		case "true":
			return &lit{true}, nil
		case "false":
			return &lit{false}, nil
		case "null":
			return &lit{nil}, nil
		case "and", "or", "not", "in":
			return nil, fmt.Errorf("unexpected %q", t.text)
		}
		return p.selectors(t.text)
	}
	return nil, fmt.Errorf("unexpected %q", t.text)
}

// selectors parses what follows a name: a call (for a dotted name like map.get), .key and [index].
func (p *parser) selectors(name string) (node, error) {
	r := &ref{name: name}
	dotted := name
	for {
		switch {
		case p.isOp("(") && dotted != "":
			p.next()
			var args []node
			for !p.isOp(")") {
				a, err := p.or()
				if err != nil {
					return nil, err
				}
				args = append(args, a)
				if p.isOp(",") {
					p.next()
				} else if !p.isOp(")") {
					return nil, fmt.Errorf("missing ) after arguments of %s", dotted)
				}
			}
			p.next()
			return &call{dotted, args}, nil
		case p.isOp(".") && p.t[p.i+1].kind == 'i':
			p.next()
			k := p.next().text
			r.sels = append(r.sels, &lit{k})
			if dotted != "" {
				dotted += "." + k
			}
		case p.isOp("["):
			p.next()
			k, err := p.or()
			if err != nil {
				return nil, err
			}
			if !p.isOp("]") {
				return nil, fmt.Errorf("missing ]")
			}
			p.next()
			r.sels = append(r.sels, k)
			dotted = ""
		default:
			return r, nil
		}
	}
}

// ---- evaluation ----

// scope holds a routine's variables.
type scope struct {
	vars    map[string]any
	env     *runEnv
	routine string
	depth   int
}

func (s *scope) eval(n node) (any, error) {
	switch n := n.(type) {
	case *lit:
		return n.v, nil
	case *ref:
		v, ok := s.vars[n.name]
		if !ok {
			return nil, newErr(fmt.Sprintf("variable not defined: %s", n.name), "KeyError")
		}
		return s.walk(v, n.sels)
	case *list:
		out := make([]any, len(n.elems))
		for i, e := range n.elems {
			v, err := s.eval(e)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	case *object:
		out := make(map[string]any, len(n.keys))
		for i, kn := range n.keys {
			k, err := s.eval(kn)
			if err != nil {
				return nil, err
			}
			ks, ok := k.(string)
			if !ok {
				return nil, newErr(fmt.Sprintf("map keys must be strings, got %s", typeName(k)), "TypeError")
			}
			if out[ks], err = s.eval(n.vals[i]); err != nil {
				return nil, err
			}
		}
		return out, nil
	case *unary:
		x, err := s.eval(n.x)
		if err != nil {
			return nil, err
		}
		if n.op == "not" {
			b, ok := x.(bool)
			if !ok {
				return nil, typeErr("not", x)
			}
			return !b, nil
		}
		switch v := x.(type) {
		case int64:
			return -v, nil
		case float64:
			return -v, nil
		}
		return nil, typeErr("-", x)
	case *binary:
		return s.binary(n)
	case *call:
		return s.call(n)
	}
	return nil, newErr("bad expression")
}

func (s *scope) walk(v any, sels []node) (any, error) {
	for _, sn := range sels {
		k, err := s.eval(sn)
		if err != nil {
			return nil, err
		}
		if v, err = index(v, k); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func index(v, k any) (any, error) {
	switch c := v.(type) {
	case map[string]any:
		ks, ok := k.(string)
		if !ok {
			return nil, newErr(fmt.Sprintf("map keys must be strings, got %s", typeName(k)), "TypeError")
		}
		x, found := c[ks]
		if !found {
			return nil, newErr(fmt.Sprintf("key not found: %s", ks), "KeyError")
		}
		return x, nil
	case []any:
		i, ok := k.(int64)
		if !ok {
			return nil, newErr(fmt.Sprintf("list index must be an integer, got %s", typeName(k)), "TypeError")
		}
		if i < 0 || i >= int64(len(c)) {
			return nil, newErr(fmt.Sprintf("list index out of range: %d", i), "IndexError")
		}
		return c[i], nil
	}
	return nil, newErr(fmt.Sprintf("cannot index into %s", typeName(v)), "TypeError")
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int64:
		return "int"
	case float64:
		return "double"
	case string:
		return "string"
	case []any:
		return "list"
	}
	return "map"
}

func typeErr(op string, vs ...any) *wfErr {
	names := make([]string, len(vs))
	for i, v := range vs {
		names[i] = typeName(v)
	}
	return newErr(fmt.Sprintf("operator %s does not work on %s", op, strings.Join(names, " and ")), "TypeError")
}

func (s *scope) binary(n *binary) (any, error) {
	l, err := s.eval(n.l)
	if err != nil {
		return nil, err
	}
	if n.op == "and" || n.op == "or" { // short circuit
		lb, ok := l.(bool)
		if !ok {
			return nil, typeErr(n.op, l)
		}
		if lb == (n.op == "or") {
			return lb, nil
		}
		r, err := s.eval(n.r)
		if err != nil {
			return nil, err
		}
		if _, ok := r.(bool); !ok {
			return nil, typeErr(n.op, l, r)
		}
		return r, nil
	}
	r, err := s.eval(n.r)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case "==":
		return equal(l, r), nil
	case "!=":
		return !equal(l, r), nil
	case "in", "not in":
		var found bool
		switch c := r.(type) {
		case map[string]any:
			k, ok := l.(string)
			if !ok {
				return nil, typeErr("in", l, r)
			}
			_, found = c[k]
		case []any:
			for _, e := range c {
				found = found || equal(e, l)
			}
		default:
			return nil, typeErr("in", l, r)
		}
		return found == (n.op == "in"), nil
	}
	if ls, ok := l.(string); ok {
		rs, ok := r.(string)
		if !ok {
			return nil, typeErr(n.op, l, r)
		}
		switch n.op {
		case "+":
			return ls + rs, nil
		case "<":
			return ls < rs, nil
		case ">":
			return ls > rs, nil
		case "<=":
			return ls <= rs, nil
		case ">=":
			return ls >= rs, nil
		}
		return nil, typeErr(n.op, l, r)
	}
	li, lInt := l.(int64)
	ri, rInt := r.(int64)
	if lInt && rInt {
		switch n.op {
		case "+":
			return li + ri, nil
		case "-":
			return li - ri, nil
		case "*":
			return li * ri, nil
		case "%", "//":
			if ri == 0 {
				return nil, newErr("division by zero", "ZeroDivisionError")
			}
			if n.op == "%" {
				return li % ri, nil
			}
			return int64(math.Floor(float64(li) / float64(ri))), nil
		}
	}
	lf, lok := num(l)
	rf, rok := num(r)
	if !lok || !rok {
		return nil, typeErr(n.op, l, r)
	}
	switch n.op {
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "/", "//", "%":
		if rf == 0 {
			return nil, newErr("division by zero", "ZeroDivisionError")
		}
		switch n.op {
		case "/":
			return lf / rf, nil
		case "//":
			return math.Floor(lf / rf), nil
		}
		return math.Mod(lf, rf), nil
	case "<":
		return lf < rf, nil
	case ">":
		return lf > rf, nil
	case "<=":
		return lf <= rf, nil
	case ">=":
		return lf >= rf, nil
	}
	return nil, typeErr(n.op, l, r)
}

func num(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func equal(a, b any) bool {
	if af, ok := num(a); ok {
		bf, ok := num(b)
		return ok && af == bf
	}
	return reflect.DeepEqual(a, b)
}

// call runs a standard library function. default() is lazy: a missing key or variable in its
// first argument gives the default.
func (s *scope) call(n *call) (any, error) {
	if n.fn == "default" && len(n.args) == 2 {
		v, err := s.eval(n.args[0])
		if e, ok := err.(*wfErr); ok && !e.fatal {
			v, err = nil, nil
		}
		if err != nil || v != nil {
			return v, err
		}
		return s.eval(n.args[1])
	}
	f, ok := stdlib[n.fn]
	if !ok {
		return nil, unsupported("the function %s", n.fn)
	}
	args := make([]any, len(n.args))
	for i, a := range n.args {
		v, err := s.eval(a)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	return f(s.env, args)
}

// evalValue evaluates every ${...} string inside a YAML value.
func (s *scope) evalValue(v any) (any, error) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "${") && strings.HasSuffix(x, "}") {
			n, err := parseExpr(x[2 : len(x)-1])
			if err != nil {
				return nil, newErr(fmt.Sprintf("bad expression %s: %v", x, err), "SyntaxError")
			}
			return s.eval(n)
		}
		return x, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			r, err := s.evalValue(e)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			r, err := s.evalValue(e)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return v, nil
}

// ---- standard library ----

type fn func(e *runEnv, a []any) (any, error)

func argErr(name string, want int) *wfErr {
	return newErr(fmt.Sprintf("%s takes %d argument(s)", name, want), "TypeError")
}

// fns wraps a function of strings.
func strFn(name string, n int, f func(a []string) (any, error)) fn {
	return func(_ *runEnv, a []any) (any, error) {
		if len(a) != n {
			return nil, argErr(name, n)
		}
		ss := make([]string, n)
		for i, v := range a {
			s, ok := v.(string)
			if !ok {
				return nil, newErr(fmt.Sprintf("%s wants strings, got %s", name, typeName(v)), "TypeError")
			}
			ss[i] = s
		}
		return f(ss)
	}
}

var stdlib map[string]fn

func init() {
	stdlib = map[string]fn{
		"len": func(_ *runEnv, a []any) (any, error) {
			if len(a) == 1 {
				switch x := a[0].(type) {
				case string:
					return int64(utf8.RuneCountInString(x)), nil
				case []any:
					return int64(len(x)), nil
				case map[string]any:
					return int64(len(x)), nil
				}
			}
			return nil, newErr("len wants one string, list or map", "TypeError")
		},
		"keys": func(_ *runEnv, a []any) (any, error) {
			m, ok := first(a).(map[string]any)
			if !ok || len(a) != 1 {
				return nil, newErr("keys wants one map", "TypeError")
			}
			ks := make([]string, 0, len(m))
			for k := range m {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			out := make([]any, len(ks))
			for i, k := range ks {
				out[i] = k
			}
			return out, nil
		},
		"type":   func(_ *runEnv, a []any) (any, error) { return typeName(first(a)), nil },
		"int":    toInt,
		"double": toDouble,
		"string": func(_ *runEnv, a []any) (any, error) {
			switch x := first(a).(type) {
			case string:
				return x, nil
			case nil, bool, int64, float64:
				return fmtValue(x), nil
			}
			return nil, newErr("string wants a string, number, bool or null", "TypeError")
		},
		"map.get": func(_ *runEnv, a []any) (any, error) {
			if len(a) != 2 {
				return nil, argErr("map.get", 2)
			}
			keys, ok := a[1].([]any)
			if !ok {
				keys = []any{a[1]}
			}
			v := a[0]
			for _, k := range keys {
				m, ok := v.(map[string]any)
				ks, isStr := k.(string)
				if !ok || !isStr {
					return nil, nil
				}
				if v, ok = m[ks]; !ok {
					return nil, nil
				}
			}
			return v, nil
		},
		"map.merge": func(_ *runEnv, a []any) (any, error) {
			if len(a) != 2 {
				return nil, argErr("map.merge", 2)
			}
			x, ok1 := a[0].(map[string]any)
			y, ok2 := a[1].(map[string]any)
			if !ok1 || !ok2 {
				return nil, newErr("map.merge wants two maps", "TypeError")
			}
			out := copyOf(x).(map[string]any)
			for k, v := range y {
				out[k] = copyOf(v)
			}
			return out, nil
		},
		"list.concat": func(_ *runEnv, a []any) (any, error) {
			if len(a) != 2 {
				return nil, argErr("list.concat", 2)
			}
			x, ok1 := a[0].([]any)
			y, ok2 := a[1].([]any)
			if !ok1 || !ok2 {
				return nil, newErr("list.concat wants two lists", "TypeError")
			}
			return copyOf(append(append([]any{}, x...), y...)), nil
		},
		"list.prepend": func(_ *runEnv, a []any) (any, error) {
			x, ok := first(a).([]any)
			if !ok || len(a) != 2 {
				return nil, newErr("list.prepend wants a list and a value", "TypeError")
			}
			return copyOf(append([]any{a[1]}, x...)), nil
		},
		"json.encode":           jsonEncode,
		"json.encode_to_string": jsonEncode,
		"json.decode": func(_ *runEnv, a []any) (any, error) {
			s, ok := first(a).(string)
			if !ok || len(a) != 1 {
				return nil, newErr("json.decode wants one string", "TypeError")
			}
			v, err := decodeJSON([]byte(s))
			if err != nil {
				return nil, newErr("json.decode: "+err.Error(), "ValueError")
			}
			return v, nil
		},
		"text.to_lower":   strFn("text.to_lower", 1, func(a []string) (any, error) { return strings.ToLower(a[0]), nil }),
		"text.to_upper":   strFn("text.to_upper", 1, func(a []string) (any, error) { return strings.ToUpper(a[0]), nil }),
		"text.url_encode": strFn("text.url_encode", 1, func(a []string) (any, error) { return url.QueryEscape(a[0]), nil }),
		"text.split": strFn("text.split", 2, func(a []string) (any, error) {
			var out []any
			for _, p := range strings.Split(a[0], a[1]) {
				out = append(out, p)
			}
			return out, nil
		}),
		"text.replace_all": strFn("text.replace_all", 3, func(a []string) (any, error) {
			return strings.ReplaceAll(a[0], a[1], a[2]), nil
		}),
		"text.match_regex": strFn("text.match_regex", 2, func(a []string) (any, error) {
			re, err := regexp.Compile(a[1])
			if err != nil {
				return nil, newErr("text.match_regex: "+err.Error(), "ValueError")
			}
			return re.MatchString(a[0]), nil
		}),
		"text.substring": func(_ *runEnv, a []any) (any, error) {
			s, ok := first(a).(string)
			if !ok || len(a) != 3 {
				return nil, newErr("text.substring wants a string, a start and an end", "TypeError")
			}
			from, ok1 := a[1].(int64)
			to, ok2 := a[2].(int64)
			r := []rune(s)
			if !ok1 || !ok2 || from < 0 || to < from || to > int64(len(r)) {
				return nil, newErr("text.substring: bad range", "ValueError")
			}
			return string(r[from:to]), nil
		},
		"time.format": func(_ *runEnv, a []any) (any, error) {
			f, ok := num(first(a))
			if !ok || len(a) != 1 {
				return nil, newErr("time.format wants seconds since the epoch", "TypeError")
			}
			sec, frac := math.Modf(f)
			return time.Unix(int64(sec), int64(frac*1e9)).UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), nil
		},
		"sys.now": func(_ *runEnv, a []any) (any, error) { return float64(time.Now().UnixNano()) / 1e9, nil },
		"sys.get_env": func(e *runEnv, a []any) (any, error) {
			n, ok := first(a).(string)
			if !ok || len(a) != 1 {
				return nil, newErr("sys.get_env wants a name", "TypeError")
			}
			return e.getEnv(n)
		},
		"math.abs": func(_ *runEnv, a []any) (any, error) {
			switch x := first(a).(type) {
			case int64:
				if x < 0 {
					x = -x
				}
				return x, nil
			case float64:
				return math.Abs(x), nil
			}
			return nil, newErr("math.abs wants a number", "TypeError")
		},
		"math.floor": func(_ *runEnv, a []any) (any, error) {
			f, ok := num(first(a))
			if !ok {
				return nil, newErr("math.floor wants a number", "TypeError")
			}
			return int64(math.Floor(f)), nil
		},
		"math.max": minmax(true),
		"math.min": minmax(false),
	}
}

func first(a []any) any {
	if len(a) == 0 {
		return nil
	}
	return a[0]
}

func minmax(max bool) fn {
	return func(_ *runEnv, a []any) (any, error) {
		if len(a) != 2 {
			return nil, argErr("math.max/min", 2)
		}
		x, ok1 := num(a[0])
		y, ok2 := num(a[1])
		if !ok1 || !ok2 {
			return nil, newErr("math.max and math.min want numbers", "TypeError")
		}
		if (x >= y) == max {
			return a[0], nil
		}
		return a[1], nil
	}
}

func toInt(_ *runEnv, a []any) (any, error) {
	switch x := first(a).(type) {
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	case string:
		if n, err := strconv.ParseInt(x, 10, 64); err == nil {
			return n, nil
		}
	}
	return nil, newErr("int wants a number or a numeric string", "ValueError")
}

func toDouble(_ *runEnv, a []any) (any, error) {
	switch x := first(a).(type) {
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return f, nil
		}
	}
	return nil, newErr("double wants a number or a numeric string", "ValueError")
}

func jsonEncode(_ *runEnv, a []any) (any, error) {
	if len(a) < 1 {
		return nil, argErr("json.encode", 1)
	}
	b, err := json.Marshal(a[0])
	if err != nil {
		return nil, newErr("json.encode: "+err.Error(), "ValueError")
	}
	return string(b), nil
}

func fmtValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	}
	return fmt.Sprint(v)
}

// decodeJSON parses JSON into workflow values (whole numbers become int64).
func decodeJSON(b []byte) (any, error) {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	if d.More() {
		return nil, fmt.Errorf("extra data after the JSON value")
	}
	return fixNumbers(v), nil
}

func fixNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		f, _ := x.Float64()
		return f
	case map[string]any:
		for k, e := range x {
			x[k] = fixNumbers(e)
		}
	case []any:
		for i, e := range x {
			x[i] = fixNumbers(e)
		}
	}
	return v
}

// copyOf deep-copies a value: workflow variables have value semantics.
func copyOf(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = copyOf(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = copyOf(e)
		}
		return out
	}
	return v
}
