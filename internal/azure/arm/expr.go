package arm

// The template language that ARM templates and policy rules share: a string "[func(args).prop[index]]" is an
// expression; "[[" at the start escapes a literal "[". Function names are matched case-insensitively.
//
// ponytail: uniqueString and guid are deterministic but are not the values Azure computes; no lambda functions
// (filter, map, reduce, sort, toObject), no dateTimeAdd and no uri functions.

import (
	"cmp"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

type (
	lit   struct{ v any }
	fcall struct {
		name string // lower-cased
		args []any
	}
	member struct{ x, key any } // x.key or x[key]
)

// isExpr reports whether a template string is an expression.
func isExpr(s string) bool {
	return len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']' && !strings.HasPrefix(s, "[[")
}

// maxExpr bounds an expression's length, and with it the parser's depth.
const maxExpr = 64 << 10

func parseExpr(src string) (any, error) {
	if len(src) > maxExpr {
		return nil, fmt.Errorf("the expression is longer than %d characters", maxExpr)
	}
	p := &exprParser{s: src[1 : len(src)-1]}
	n, err := p.expr()
	if err != nil {
		return nil, fmt.Errorf("the expression '%s' is not valid: %v", src, err)
	}
	if p.skip(); p.i < len(p.s) {
		return nil, fmt.Errorf("the expression '%s' is not valid: unexpected '%s' at position %d", src, p.s[p.i:], p.i+1)
	}
	return n, nil
}

type exprParser struct {
	s string
	i int
}

func (p *exprParser) skip() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n' || p.s[p.i] == '\r') {
		p.i++
	}
}

func (p *exprParser) eat(c byte) bool {
	if p.skip(); p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

func identChar(c byte, first bool) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || !first && c >= '0' && c <= '9'
}

func (p *exprParser) ident() string {
	p.skip()
	start := p.i
	for p.i < len(p.s) && identChar(p.s[p.i], p.i == start) {
		p.i++
	}
	return p.s[start:p.i]
}

func (p *exprParser) expr() (any, error) {
	x, err := p.primary()
	for err == nil {
		switch {
		case p.eat('.'):
			name := p.ident()
			if name == "" {
				return nil, fmt.Errorf("a property name must follow '.' at position %d", p.i+1)
			}
			x = &member{x, &lit{name}}
		case p.eat('['):
			var k any
			if k, err = p.expr(); err == nil && !p.eat(']') {
				err = fmt.Errorf("']' expected at position %d", p.i+1)
			}
			x = &member{x, k}
		default:
			return x, nil
		}
	}
	return nil, err
}

func (p *exprParser) primary() (any, error) {
	p.skip()
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("the expression ends too early")
	}
	switch c := p.s[p.i]; {
	case c == '\'':
		var b strings.Builder
		for p.i++; p.i < len(p.s); p.i++ {
			if p.s[p.i] == '\'' {
				if p.i+1 < len(p.s) && p.s[p.i+1] == '\'' {
					b.WriteByte('\'')
					p.i++
					continue
				}
				p.i++
				return &lit{b.String()}, nil
			}
			b.WriteByte(p.s[p.i])
		}
		return nil, fmt.Errorf("a string is not closed")
	case c == '-' || c >= '0' && c <= '9':
		start := p.i
		for p.i++; p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9'; p.i++ {
		}
		n, err := strconv.ParseInt(p.s[start:p.i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("'%s' is not a whole number", p.s[start:p.i])
		}
		return &lit{n}, nil
	case identChar(c, true):
		name := p.ident()
		if !p.eat('(') {
			return nil, fmt.Errorf("'%s' must be a function call, as in %s()", name, name)
		}
		f := &fcall{name: strings.ToLower(name)}
		if p.eat(')') {
			return f, nil
		}
		for {
			a, err := p.expr()
			if err != nil {
				return nil, err
			}
			f.args = append(f.args, a)
			if p.eat(')') {
				return f, nil
			}
			if !p.eat(',') {
				return nil, fmt.Errorf("',' or ')' expected at position %d", p.i+1)
			}
		}
	}
	return nil, fmt.Errorf("unexpected '%c' at position %d", p.s[p.i], p.i+1)
}

// exprEnv evaluates expressions. fn answers the functions of the context (parameters, field, resourceId, ...)
// with ok false for a name it does not have; the functions of exprFuncs answer the rest.
type exprEnv struct {
	fn func(name string, args []any) (v any, ok bool, err error)
}

// value evaluates a template value: expression strings, in arrays and objects too. "[[" strings lose a "[".
func (e *exprEnv) value(v any) (any, error) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "[[") {
			return x[1:], nil
		}
		if !isExpr(x) {
			return x, nil
		}
		n, err := parseExpr(x)
		if err != nil {
			return nil, err
		}
		return e.eval(n)
	case []any:
		out := make([]any, len(x))
		for i, it := range x {
			var err error
			if out[i], err = e.value(it); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, it := range x {
			var err error
			if out[k], err = e.value(it); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return v, nil
}

func (e *exprEnv) eval(n any) (any, error) {
	switch n := n.(type) {
	case *lit:
		return n.v, nil
	case *member:
		x, err := e.eval(n.x)
		if err != nil {
			return nil, err
		}
		k, err := e.eval(n.key)
		if err != nil {
			return nil, err
		}
		return index(x, k)
	case *fcall:
		if n.name == "if" { // only the branch the condition picks is evaluated
			if len(n.args) != 3 {
				return nil, fmt.Errorf("if() takes 3 arguments")
			}
			c, err := e.eval(n.args[0])
			if err != nil {
				return nil, err
			}
			b, ok := c.(bool)
			if !ok {
				return nil, fmt.Errorf("the condition of if() must be a boolean, not %s", kind(c))
			}
			return e.eval(n.args[map[bool]int{true: 1, false: 2}[b]])
		}
		args := make([]any, len(n.args))
		for i, a := range n.args {
			var err error
			if args[i], err = e.eval(a); err != nil {
				return nil, err
			}
		}
		if e.fn != nil {
			if v, ok, err := e.fn(n.name, args); ok || err != nil {
				return v, err
			}
		}
		f := exprFuncs[n.name]
		if f == nil {
			return nil, fmt.Errorf("the template function '%s' is not valid here, or pogi does not support it", n.name)
		}
		v, err := f(args)
		if err != nil {
			return nil, fmt.Errorf("%s(): %v", n.name, err)
		}
		return v, nil
	}
	return nil, fmt.Errorf("not an expression")
}

// index is x.k or x[k]: object members are matched case-insensitively.
func index(x, k any) (any, error) {
	switch x := x.(type) {
	case map[string]any:
		key, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("an object member name must be a string, not %s", kind(k))
		}
		if v, ok := x[key]; ok {
			return v, nil
		}
		for name, v := range x {
			if strings.EqualFold(name, key) {
				return v, nil
			}
		}
		return nil, fmt.Errorf("the language expression property '%s' doesn't exist, available properties are '%s'", key, strings.Join(core.SortedKeys(x), ", "))
	case []any:
		i, ok := toInt(k)
		if !ok || i < 0 || i >= int64(len(x)) {
			return nil, fmt.Errorf("the index %v is out of range: the array has %d items", k, len(x))
		}
		return x[i], nil
	}
	return nil, fmt.Errorf("the language expression property '%v' can't be read from %s", k, kind(x))
}

func kind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case int64, float64, json.Number:
		return "an integer"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	}
	return fmt.Sprintf("%T", v)
}

func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		return int64(x), x == math.Trunc(x) && math.Abs(x) < 1<<62
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	}
	return 0, false
}

// jsonEqual compares two template values: numbers by value, the rest as JSON.
func jsonEqual(a, b any) bool {
	if x, ok := toInt(a); ok {
		y, ok := toInt(b)
		return ok && x == y
	}
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// normalize turns numbers into int64 inside arrays and objects, so that equal values compare equal.
func normalize(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalize(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalize(e)
		}
		return out
	}
	if n, ok := toInt(v); ok {
		return n
	}
	return v
}

// exprFuncs are the functions that need nothing but their arguments.
var exprFuncs = map[string]func(a []any) (any, error){}

func init() {
	str := func(a []any, i int) (string, error) {
		if i >= len(a) {
			return "", fmt.Errorf("argument %d is missing", i+1)
		}
		s, ok := a[i].(string)
		if !ok {
			return "", fmt.Errorf("argument %d must be a string, not %s", i+1, kind(a[i]))
		}
		return s, nil
	}
	num := func(a []any, i int) (int64, error) {
		if i >= len(a) {
			return 0, fmt.Errorf("argument %d is missing", i+1)
		}
		n, ok := toInt(a[i])
		if !ok {
			return 0, fmt.Errorf("argument %d must be an integer, not %s", i+1, kind(a[i]))
		}
		return n, nil
	}
	argc := func(a []any, lo, hi int) error {
		if len(a) < lo || len(a) > hi {
			if lo == hi {
				return fmt.Errorf("takes %d arguments, got %d", lo, len(a))
			}
			return fmt.Errorf("takes %d to %d arguments, got %d", lo, hi, len(a))
		}
		return nil
	}
	str1 := func(f func(string) any) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if err := argc(a, 1, 1); err != nil {
				return nil, err
			}
			s, err := str(a, 0)
			if err != nil {
				return nil, err
			}
			return f(s), nil
		}
	}
	arith := func(f func(x, y int64) (int64, error)) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if err := argc(a, 2, 2); err != nil {
				return nil, err
			}
			x, err := num(a, 0)
			if err != nil {
				return nil, err
			}
			y, err := num(a, 1)
			if err != nil {
				return nil, err
			}
			return f(x, y)
		}
	}
	compare := func(f func(c int) bool) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if err := argc(a, 2, 2); err != nil {
				return nil, err
			}
			if x, ok := toInt(a[0]); ok {
				y, err := num(a, 1)
				if err != nil {
					return nil, err
				}
				return f(cmp.Compare(x, y)), nil
			}
			x, err := str(a, 0)
			if err != nil {
				return nil, err
			}
			y, err := str(a, 1)
			if err != nil {
				return nil, err
			}
			return f(strings.Compare(x, y)), nil
		}
	}
	// seq reads an array or a string argument as a list, for the functions that take either.
	type seq struct {
		arr   []any
		s     string
		isStr bool
	}
	seqOf := func(v any) (seq, error) {
		switch x := v.(type) {
		case []any:
			return seq{arr: x}, nil
		case string:
			return seq{s: x, isStr: true}, nil
		}
		return seq{}, fmt.Errorf("an array or a string is needed, not %s", kind(v))
	}
	f := exprFuncs
	f["concat"] = func(a []any) (any, error) {
		if len(a) > 0 {
			if _, ok := a[0].([]any); ok {
				var out []any
				for i, v := range a {
					l, ok := v.([]any)
					if !ok {
						return nil, fmt.Errorf("argument %d must be an array like the first, not %s", i+1, kind(v))
					}
					out = append(out, l...)
				}
				return append([]any{}, out...), nil
			}
		}
		var b strings.Builder
		for _, v := range a {
			s, err := toString(v)
			if err != nil {
				return nil, err
			}
			b.WriteString(s)
		}
		return b.String(), nil
	}
	f["format"] = func(a []any) (any, error) {
		tmpl, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		var b strings.Builder
		for i := 0; i < len(tmpl); i++ {
			switch c := tmpl[i]; {
			case c == '{' && i+1 < len(tmpl) && tmpl[i+1] == '{', c == '}' && i+1 < len(tmpl) && tmpl[i+1] == '}':
				b.WriteByte(c)
				i++
			case c == '{':
				end := strings.IndexByte(tmpl[i:], '}')
				if end < 0 {
					return nil, fmt.Errorf("the format '%s' has an unclosed '{'", tmpl)
				}
				spec, _, _ := strings.Cut(tmpl[i+1:i+end], ":") // ponytail: format specifiers ({0:N2}) are ignored
				n, err := strconv.Atoi(strings.TrimSpace(spec))
				if err != nil || n < 0 || n+1 >= len(a) {
					return nil, fmt.Errorf("the format '%s' names argument {%s}, which is not given", tmpl, spec)
				}
				s, err := toString(a[n+1])
				if err != nil {
					return nil, err
				}
				b.WriteString(s)
				i += end
			default:
				b.WriteByte(c)
			}
		}
		return b.String(), nil
	}
	f["tolower"] = str1(func(s string) any { return strings.ToLower(s) })
	f["toupper"] = str1(func(s string) any { return strings.ToUpper(s) })
	f["trim"] = str1(func(s string) any { return strings.TrimSpace(s) })
	f["base64"] = str1(func(s string) any { return base64.StdEncoding.EncodeToString([]byte(s)) })
	f["base64tostring"] = func(a []any) (any, error) {
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("'%s' is not base64", s)
		}
		return string(b), nil
	}
	f["uniquestring"] = func(a []any) (any, error) {
		if len(a) == 0 {
			return nil, fmt.Errorf("takes at least 1 argument")
		}
		parts := make([]string, len(a))
		for i := range a {
			var err error
			if parts[i], err = str(a, i); err != nil {
				return nil, err
			}
		}
		sum := sha256.Sum256([]byte(strings.Join(parts, "-")))
		return strings.ToLower(base32.StdEncoding.EncodeToString(sum[:8]))[:13], nil
	}
	f["guid"] = func(a []any) (any, error) {
		if len(a) == 0 {
			return nil, fmt.Errorf("takes at least 1 argument")
		}
		parts := make([]string, len(a))
		for i := range a {
			var err error
			if parts[i], err = str(a, i); err != nil {
				return nil, err
			}
		}
		h := sha1.Sum([]byte(strings.Join(parts, "-")))
		h[6], h[8] = h[6]&0x0f|0x50, h[8]&0x3f|0x80
		return fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16]), nil
	}
	f["newguid"] = func(a []any) (any, error) { return core.UUID(), argc(a, 0, 0) }
	f["utcnow"] = func(a []any) (any, error) {
		if err := argc(a, 0, 1); err != nil {
			return nil, err
		}
		layout := "20060102T150405Z"
		if len(a) == 1 {
			s, err := str(a, 0)
			if err != nil {
				return nil, err
			}
			layout = dotnetTime(s)
		}
		return time.Now().UTC().Format(layout), nil
	}
	f["string"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		return toString(a[0])
	}
	f["int"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		if n, ok := toInt(a[0]); ok {
			return n, nil
		}
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("'%s' is not a whole number", s)
		}
		return n, nil
	}
	f["bool"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		switch x := a[0].(type) {
		case bool:
			return x, nil
		case string:
			if b, err := strconv.ParseBool(strings.ToLower(x)); err == nil {
				return b, nil
			}
		}
		if n, ok := toInt(a[0]); ok {
			return n != 0, nil
		}
		return nil, fmt.Errorf("%v can't be read as a boolean", a[0])
	}
	f["json"] = func(a []any) (any, error) {
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		var v any
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			return nil, fmt.Errorf("'%s' is not JSON", s)
		}
		return v, nil
	}
	f["true"] = func(a []any) (any, error) { return true, argc(a, 0, 0) }
	f["false"] = func(a []any) (any, error) { return false, argc(a, 0, 0) }
	f["null"] = func(a []any) (any, error) { return nil, argc(a, 0, 0) }
	f["not"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		b, ok := a[0].(bool)
		if !ok {
			return nil, fmt.Errorf("needs a boolean, not %s", kind(a[0]))
		}
		return !b, nil
	}
	logic := func(and bool) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if len(a) < 2 {
				return nil, fmt.Errorf("takes at least 2 arguments")
			}
			for i, v := range a {
				b, ok := v.(bool)
				if !ok {
					return nil, fmt.Errorf("argument %d must be a boolean, not %s", i+1, kind(v))
				}
				if b != and {
					return !and, nil
				}
			}
			return and, nil
		}
	}
	f["and"], f["or"] = logic(true), logic(false)
	f["equals"] = func(a []any) (any, error) {
		if err := argc(a, 2, 2); err != nil {
			return nil, err
		}
		return jsonEqual(a[0], a[1]), nil
	}
	f["greater"] = compare(func(c int) bool { return c > 0 })
	f["greaterorequals"] = compare(func(c int) bool { return c >= 0 })
	f["less"] = compare(func(c int) bool { return c < 0 })
	f["lessorequals"] = compare(func(c int) bool { return c <= 0 })
	f["add"] = arith(func(x, y int64) (int64, error) { return x + y, nil })
	f["sub"] = arith(func(x, y int64) (int64, error) { return x - y, nil })
	f["mul"] = arith(func(x, y int64) (int64, error) { return x * y, nil })
	f["div"] = arith(func(x, y int64) (int64, error) {
		if y == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		return x / y, nil
	})
	f["mod"] = arith(func(x, y int64) (int64, error) {
		if y == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		return x % y, nil
	})
	minmax := func(less bool) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if len(a) == 1 {
				if l, ok := a[0].([]any); ok {
					a = l
				}
			}
			if len(a) == 0 {
				return nil, fmt.Errorf("needs at least one integer")
			}
			best, err := num(a, 0)
			if err != nil {
				return nil, err
			}
			for i := 1; i < len(a); i++ {
				n, err := num(a, i)
				if err != nil {
					return nil, err
				}
				if n < best == less {
					best = n
				}
			}
			return best, nil
		}
	}
	f["min"], f["max"] = minmax(true), minmax(false)
	f["range"] = func(a []any) (any, error) {
		if err := argc(a, 2, 2); err != nil {
			return nil, err
		}
		start, err := num(a, 0)
		if err != nil {
			return nil, err
		}
		n, err := num(a, 1)
		if err != nil {
			return nil, err
		}
		if n < 0 || n > 10000 {
			return nil, fmt.Errorf("the count must be from 0 to 10000, not %d", n)
		}
		out := make([]any, n)
		for i := range out {
			out[i] = start + int64(i)
		}
		return out, nil
	}
	f["coalesce"] = func(a []any) (any, error) {
		for _, v := range a {
			if v != nil {
				return v, nil
			}
		}
		return nil, nil
	}
	f["empty"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		switch x := a[0].(type) {
		case nil:
			return true, nil
		case string:
			return x == "", nil
		case []any:
			return len(x) == 0, nil
		case map[string]any:
			return len(x) == 0, nil
		}
		return false, nil
	}
	f["length"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		switch x := a[0].(type) {
		case string:
			return int64(utf8.RuneCountInString(x)), nil
		case []any:
			return int64(len(x)), nil
		case map[string]any:
			return int64(len(x)), nil
		}
		return nil, fmt.Errorf("needs a string, an array or an object, not %s", kind(a[0]))
	}
	f["contains"] = func(a []any) (any, error) {
		if err := argc(a, 2, 2); err != nil {
			return nil, err
		}
		switch x := a[0].(type) {
		case string:
			s, err := toString(a[1])
			return strings.Contains(x, s), err
		case []any:
			return slices.ContainsFunc(x, func(e any) bool { return jsonEqual(e, a[1]) }), nil
		case map[string]any:
			k, err := str(a, 1)
			_, ierr := index(x, k)
			return ierr == nil, err
		}
		return nil, fmt.Errorf("needs a string, an array or an object, not %s", kind(a[0]))
	}
	f["startswith"] = func(a []any) (any, error) {
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		p, err := str(a, 1)
		return strings.HasPrefix(strings.ToLower(s), strings.ToLower(p)), err
	}
	f["endswith"] = func(a []any) (any, error) {
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		p, err := str(a, 1)
		return strings.HasSuffix(strings.ToLower(s), strings.ToLower(p)), err
	}
	indexOf := func(last bool) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if err := argc(a, 2, 2); err != nil {
				return nil, err
			}
			if l, ok := a[0].([]any); ok {
				for i := range l {
					j := i
					if last {
						j = len(l) - 1 - i
					}
					if jsonEqual(l[j], a[1]) {
						return int64(j), nil
					}
				}
				return int64(-1), nil
			}
			s, err := str(a, 0)
			if err != nil {
				return nil, err
			}
			t, err := str(a, 1)
			if err != nil {
				return nil, err
			}
			s, t = strings.ToLower(s), strings.ToLower(t)
			i := strings.Index(s, t)
			if last {
				i = strings.LastIndex(s, t)
			}
			if i > 0 {
				i = utf8.RuneCountInString(s[:i])
			}
			return int64(i), nil
		}
	}
	f["indexof"], f["lastindexof"] = indexOf(false), indexOf(true)
	f["substring"] = func(a []any) (any, error) {
		if err := argc(a, 2, 3); err != nil {
			return nil, err
		}
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		r := []rune(s)
		start, err := num(a, 1)
		if err != nil {
			return nil, err
		}
		n := int64(len(r)) - start
		if len(a) == 3 {
			if n, err = num(a, 2); err != nil {
				return nil, err
			}
		}
		if start < 0 || n < 0 || start+n > int64(len(r)) {
			return nil, fmt.Errorf("start %d and length %d are outside the string of %d characters", start, n, len(r))
		}
		return string(r[start : start+n]), nil
	}
	f["replace"] = func(a []any) (any, error) {
		if err := argc(a, 3, 3); err != nil {
			return nil, err
		}
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		o, err := str(a, 1)
		if err != nil {
			return nil, err
		}
		n, err := str(a, 2)
		return strings.ReplaceAll(s, o, n), err
	}
	f["split"] = func(a []any) (any, error) {
		if err := argc(a, 2, 2); err != nil {
			return nil, err
		}
		s, err := str(a, 0)
		if err != nil {
			return nil, err
		}
		var seps []string
		if l, ok := a[1].([]any); ok {
			for i := range l {
				sep, err := str(l, i)
				if err != nil {
					return nil, err
				}
				seps = append(seps, sep)
			}
		} else if sep, err := str(a, 1); err == nil {
			seps = []string{sep}
		} else {
			return nil, err
		}
		parts := []string{s}
		for _, sep := range seps {
			var next []string
			for _, p := range parts {
				next = append(next, strings.Split(p, sep)...)
			}
			parts = next
		}
		out := make([]any, len(parts))
		for i, p := range parts {
			out[i] = p
		}
		return out, nil
	}
	f["join"] = func(a []any) (any, error) {
		if err := argc(a, 2, 2); err != nil {
			return nil, err
		}
		l, ok := a[0].([]any)
		if !ok {
			return nil, fmt.Errorf("argument 1 must be an array, not %s", kind(a[0]))
		}
		sep, err := str(a, 1)
		if err != nil {
			return nil, err
		}
		parts := make([]string, len(l))
		for i, v := range l {
			if parts[i], err = toString(v); err != nil {
				return nil, err
			}
		}
		return strings.Join(parts, sep), nil
	}
	f["padleft"] = func(a []any) (any, error) {
		if err := argc(a, 2, 3); err != nil {
			return nil, err
		}
		s, err := toString(a[0])
		if err != nil {
			return nil, err
		}
		n, err := num(a, 1)
		if err != nil {
			return nil, err
		}
		if n > 16<<10 {
			return nil, fmt.Errorf("the length %d is too long", n)
		}
		pad := " "
		if len(a) == 3 {
			if pad, err = str(a, 2); err != nil || utf8.RuneCountInString(pad) != 1 {
				return nil, fmt.Errorf("the padding must be one character")
			}
		}
		if c := int(n) - utf8.RuneCountInString(s); c > 0 {
			s = strings.Repeat(pad, c) + s
		}
		return s, nil
	}
	ends := func(first bool) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if err := argc(a, 1, 1); err != nil {
				return nil, err
			}
			q, err := seqOf(a[0])
			if err != nil {
				return nil, err
			}
			if q.isStr {
				r := []rune(q.s)
				if len(r) == 0 {
					return "", nil
				}
				if first {
					return string(r[0]), nil
				}
				return string(r[len(r)-1]), nil
			}
			if len(q.arr) == 0 {
				return nil, nil
			}
			if first {
				return q.arr[0], nil
			}
			return q.arr[len(q.arr)-1], nil
		}
	}
	f["first"], f["last"] = ends(true), ends(false)
	cut := func(take bool) func([]any) (any, error) {
		return func(a []any) (any, error) {
			if err := argc(a, 2, 2); err != nil {
				return nil, err
			}
			q, err := seqOf(a[0])
			if err != nil {
				return nil, err
			}
			n, err := num(a, 1)
			if err != nil {
				return nil, err
			}
			r := []rune(q.s)
			size := int64(len(q.arr))
			if q.isStr {
				size = int64(len(r))
			}
			n = max(0, min(n, size))
			lo, hi := n, size
			if take {
				lo, hi = 0, n
			}
			if q.isStr {
				return string(r[lo:hi]), nil
			}
			return append([]any{}, q.arr[lo:hi]...), nil
		}
	}
	f["take"], f["skip"] = cut(true), cut(false)
	f["array"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		if l, ok := a[0].([]any); ok {
			return l, nil
		}
		return []any{a[0]}, nil
	}
	f["createarray"] = func(a []any) (any, error) { return append([]any{}, a...), nil }
	f["createobject"] = func(a []any) (any, error) {
		if len(a)%2 != 0 {
			return nil, fmt.Errorf("takes pairs of a name and a value")
		}
		out := map[string]any{}
		for i := 0; i < len(a); i += 2 {
			k, err := str(a, i)
			if err != nil {
				return nil, err
			}
			out[k] = a[i+1]
		}
		return out, nil
	}
	f["union"] = func(a []any) (any, error) {
		if len(a) == 0 {
			return nil, fmt.Errorf("takes at least 1 argument")
		}
		if _, ok := a[0].(map[string]any); ok {
			out := map[string]any{}
			for i, v := range a {
				m, ok := v.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("argument %d must be an object like the first, not %s", i+1, kind(v))
				}
				for k, e := range m {
					out[k] = e
				}
			}
			return out, nil
		}
		var out []any
		for i, v := range a {
			l, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("argument %d must be an array like the first, not %s", i+1, kind(v))
			}
			for _, e := range l {
				if !slices.ContainsFunc(out, func(o any) bool { return jsonEqual(o, e) }) {
					out = append(out, e)
				}
			}
		}
		return append([]any{}, out...), nil
	}
	f["intersection"] = func(a []any) (any, error) {
		if len(a) < 2 {
			return nil, fmt.Errorf("takes at least 2 arguments")
		}
		if first, ok := a[0].(map[string]any); ok {
			out := map[string]any{}
			for k, v := range first {
				in := true
				for _, o := range a[1:] {
					m, _ := o.(map[string]any)
					in = in && m != nil && jsonEqual(m[k], v)
				}
				if in {
					out[k] = v
				}
			}
			return out, nil
		}
		first, ok := a[0].([]any)
		if !ok {
			return nil, fmt.Errorf("needs arrays or objects, not %s", kind(a[0]))
		}
		out := []any{}
		for _, e := range first {
			in := true
			for _, o := range a[1:] {
				l, _ := o.([]any)
				in = in && slices.ContainsFunc(l, func(x any) bool { return jsonEqual(x, e) })
			}
			if in && !slices.ContainsFunc(out, func(x any) bool { return jsonEqual(x, e) }) {
				out = append(out, e)
			}
		}
		return out, nil
	}
	f["objectkeys"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		m, ok := a[0].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("needs an object, not %s", kind(a[0]))
		}
		out := []any{}
		for _, k := range core.SortedKeys(m) {
			out = append(out, k)
		}
		return out, nil
	}
	f["items"] = func(a []any) (any, error) {
		if err := argc(a, 1, 1); err != nil {
			return nil, err
		}
		m, ok := a[0].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("needs an object, not %s", kind(a[0]))
		}
		out := []any{}
		for _, k := range core.SortedKeys(m) {
			out = append(out, map[string]any{"key": k, "value": m[k]})
		}
		return out, nil
	}
}

// toString is string(): strings as they are, other values as compact JSON.
func toString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case nil:
		return "", nil
	case bool:
		return strconv.FormatBool(x), nil
	}
	if n, ok := toInt(v); ok {
		return strconv.FormatInt(n, 10), nil
	}
	b, err := json.Marshal(v)
	return string(b), err
}

// dotnetTime turns the .NET date format letters utcNow takes (yyyy MM dd HH mm ss, or "u", "s", "o") into Go's.
func dotnetTime(f string) string {
	switch f {
	case "u":
		return "2006-01-02 15:04:05Z"
	case "s":
		return "2006-01-02T15:04:05"
	case "o", "O":
		return "2006-01-02T15:04:05.0000000Z"
	}
	return strings.NewReplacer("yyyy", "2006", "yy", "06", "MM", "01", "dd", "02", "HH", "15", "mm", "04", "ss", "05").Replace(f)
}
