package stepfunctions

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// seg is one step of a JSONPath: a field, an array index, a wildcard, a slice,
// a filter or a recursive descent.
type seg struct {
	key    string
	idx    int
	isIdx  bool
	all    bool
	desc   bool                      // ..  every descendant, then the rest of the path
	slice  *[2]*int                  // [from:to]
	filter func(item, root any) bool // [?(...)]
}

// multi is true for the steps that select several values.
func (s seg) multi() bool { return s.all || s.desc || s.slice != nil || s.filter != nil }

// parsePath parses the JSONPath subset ASL uses: $, $.a.b, $['a'], $.a[0], $.a[-1],
// $.a[*], $.a.*, $.a[1:3], $..a and filters such as $.a[?(@.n > 1)].
func parsePath(p string) ([]seg, error) {
	if !strings.HasPrefix(p, "$") {
		return nil, fmt.Errorf("invalid JSONPath %q", p)
	}
	var out []seg
	for s := p[1:]; s != ""; {
		switch s[0] {
		case '.':
			if strings.HasPrefix(s, "..") {
				out = append(out, seg{desc: true})
				if s = s[2:]; s == "" {
					return nil, fmt.Errorf("invalid JSONPath %q", p)
				}
				if s[0] == '[' {
					continue
				}
			} else {
				s = s[1:]
			}
			n := strings.IndexAny(s, ".[")
			if n < 0 {
				n = len(s)
			}
			if n == 0 {
				return nil, fmt.Errorf("invalid JSONPath %q", p)
			}
			out = append(out, seg{key: s[:n], all: s[:n] == "*"})
			s = s[n:]
		case '[':
			if len(s) > 1 && (s[1] == '\'' || s[1] == '"') {
				end := strings.Index(s[2:], string(s[1])+"]")
				if end < 0 {
					return nil, fmt.Errorf("invalid JSONPath %q", p)
				}
				out = append(out, seg{key: s[2 : 2+end]})
				s = s[end+4:]
				continue
			}
			if strings.HasPrefix(s, "[?(") {
				end := filterEnd(s)
				if end < 0 {
					return nil, fmt.Errorf("invalid JSONPath %q", p)
				}
				f, err := parseFilter(s[3:end])
				if err != nil {
					return nil, fmt.Errorf("invalid JSONPath %q: %v", p, err)
				}
				out = append(out, seg{filter: f})
				s = s[end+2:]
				continue
			}
			end := strings.IndexByte(s, ']')
			if end < 0 {
				return nil, fmt.Errorf("invalid JSONPath %q", p)
			}
			in := strings.TrimSpace(s[1:end])
			if from, to, ok := strings.Cut(in, ":"); ok {
				sl := &[2]*int{}
				for i, v := range []string{from, to} {
					if v = strings.TrimSpace(v); v != "" {
						n, err := strconv.Atoi(v)
						if err != nil {
							return nil, fmt.Errorf("unsupported JSONPath %q", p)
						}
						sl[i] = &n
					}
				}
				out = append(out, seg{slice: sl})
			} else if in == "*" {
				out = append(out, seg{all: true})
			} else if i, err := strconv.Atoi(in); err == nil {
				out = append(out, seg{idx: i, isIdx: true})
			} else {
				return nil, fmt.Errorf("unsupported JSONPath %q", p)
			}
			s = s[end+1:]
		default:
			return nil, fmt.Errorf("invalid JSONPath %q", p)
		}
	}
	return out, nil
}

// filterEnd finds the ) that closes the [?( at the start of s; the ] follows it. -1 when there is none.
func filterEnd(s string) int {
	depth, quote := 0, byte(0)
	for i := 2; i < len(s); i++ {
		switch c := s[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			} else if c == '\\' {
				i++
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			if depth--; depth == 0 {
				if i+1 < len(s) && s[i+1] == ']' {
					return i
				}
				return -1
			}
		}
	}
	return -1
}

// candidates are the values a multi-valued step selects from v.
func (s seg) candidates(v, root any) []any {
	var vals []any
	switch c := v.(type) {
	case []any:
		vals = c
	case map[string]any:
		if s.desc || s.all || s.filter != nil {
			for _, k := range core.SortedKeys(c) {
				vals = append(vals, c[k])
			}
		}
	}
	switch {
	case s.desc:
		out := []any{}
		var walk func(x any)
		walk = func(x any) {
			out = append(out, x)
			switch c := x.(type) {
			case []any:
				for _, e := range c {
					walk(e)
				}
			case map[string]any:
				for _, k := range core.SortedKeys(c) {
					walk(c[k])
				}
			}
		}
		walk(v)
		return out
	case s.filter != nil:
		var out []any
		for _, x := range vals {
			if s.filter(x, root) {
				out = append(out, x)
			}
		}
		return out
	case s.slice != nil:
		n := len(vals)
		from, to := 0, n
		if f := s.slice[0]; f != nil {
			from = *f
			if from < 0 {
				from += n
			}
		}
		if t := s.slice[1]; t != nil {
			to = *t
			if to < 0 {
				to += n
			}
		}
		from, to = min(max(from, 0), n), min(max(to, 0), n)
		if from >= to {
			return nil
		}
		return vals[from:to]
	}
	return vals
}

// get evaluates a parsed path; a step that selects several values collects its matches into a list.
// root is what $ means inside a filter.
func get(v any, path []seg, root any) (any, bool) {
	for i, s := range path {
		switch {
		case s.multi():
			switch v.(type) {
			case []any, map[string]any:
			default:
				if !s.desc {
					return nil, false
				}
			}
			out, deeper := []any{}, wild(path[i+1:])
			for _, x := range s.candidates(v, root) {
				if r, ok := get(x, path[i+1:], root); ok {
					if l, isList := r.([]any); isList && deeper {
						out = append(out, l...)
					} else {
						out = append(out, r)
					}
				}
			}
			return out, true
		case s.isIdx:
			a, ok := v.([]any)
			i := s.idx
			if i < 0 {
				i += len(a)
			}
			if !ok || i < 0 || i >= len(a) {
				return nil, false
			}
			v = a[i]
		default:
			m, ok := v.(map[string]any)
			if !ok {
				return nil, false
			}
			if v, ok = m[s.key]; !ok {
				return nil, false
			}
		}
	}
	return v, true
}

func wild(path []seg) bool {
	for _, s := range path {
		if s.multi() {
			return true
		}
	}
	return false
}

// set returns a copy of root with val placed at path, as ResultPath does:
// fields are created, but an array index must already exist.
func set(root any, path []seg, val any) (any, error) {
	if len(path) == 0 {
		return val, nil
	}
	if a, ok := root.([]any); ok && path[0].isIdx {
		i := path[0].idx
		if i < 0 {
			i += len(a)
		}
		if i < 0 || i >= len(a) {
			return nil, fmt.Errorf("no index %d in %s", path[0].idx, js(root))
		}
		v, err := set(a[i], path[1:], val)
		if err != nil {
			return nil, err
		}
		cp := slices.Clone(a)
		cp[i] = v
		return cp, nil
	}
	m, ok := root.(map[string]any)
	if !ok || path[0].multi() || path[0].isIdx {
		return nil, fmt.Errorf("cannot set a field on %s", js(root))
	}
	cp := make(map[string]any, len(m)+1)
	for k, v := range m {
		cp[k] = v
	}
	child, ok := m[path[0].key]
	if !ok {
		child = map[string]any{}
	}
	v, err := set(child, path[1:], val)
	if err != nil {
		return nil, err
	}
	cp[path[0].key] = v
	return cp, nil
}

// lookup evaluates a reference path ($… on input, $$… on the context object).
func (t *thread) lookup(p string, input any) (any, bool, error) {
	if isVariable(p) {
		return t.variable(p)
	}
	if strings.HasPrefix(p, "$$") {
		p, input = p[1:], t.ctxObj()
	}
	segs, err := parsePath(p)
	if err != nil {
		return nil, false, runtimeErr("%v", err)
	}
	v, ok := get(input, segs, input)
	return v, ok, nil
}

// path evaluates a reference path that must exist.
func (t *thread) path(p string, input any, field string) (any, error) {
	v, ok, err := t.lookup(p, input)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, runtimeErr("Invalid path '%s' for the field '%s': no results in the input '%s'", p, field, js(input))
	}
	return v, nil
}

// resolve evaluates the value of a "key.$" field: a path or an intrinsic function.
func (t *thread) resolve(expr string, input any, field string) (any, error) {
	if strings.HasPrefix(expr, "States.") {
		p := &intrinsicParser{s: expr, t: t, input: input}
		v, err := p.expr()
		if err == nil && p.skipWS() < len(p.s) {
			err = fmt.Errorf("unexpected %q", p.s[p.i:])
		}
		if errors.As(err, new(*failure)) { // a missing path is States.Runtime, as for a plain path
			return nil, err
		}
		if err != nil {
			return nil, &failure{"States.IntrinsicFailure", fmt.Sprintf("Invalid intrinsic function in the field '%s': %s: %v", field, expr, err)}
		}
		return unraw(v), nil
	}
	v, ok, err := t.lookup(expr, input)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, runtimeErr("The JSONPath '%s' specified for the field '%s' could not be found in the input '%s'", expr, field, js(input))
	}
	return v, nil
}

// template applies a payload template (Parameters, ResultSelector, ItemSelector).
func (t *thread) template(tpl, input any) (any, error) {
	switch v := tpl.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, x := range v {
			if name, ok := strings.CutSuffix(k, ".$"); ok {
				expr, isStr := x.(string)
				if !isStr {
					return nil, runtimeErr("The value for the field '%s' must be a STRING that contains a JSONPath or an intrinsic function", k)
				}
				r, err := t.resolve(expr, input, k)
				if err != nil {
					return nil, err
				}
				out[name] = r
				continue
			}
			r, err := t.template(x, input)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			r, err := t.template(x, input)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return tpl, nil
}

// ---- intrinsic functions ----

// rawStr is a string literal still holding its escapes (States.Format needs to see \{ and \}).
type rawStr string

func unraw(v any) any {
	r, ok := v.(rawStr)
	if !ok {
		return v
	}
	var b strings.Builder
	for i := 0; i < len(r); i++ {
		if r[i] == '\\' && i+1 < len(r) {
			i++
		}
		b.WriteByte(r[i])
	}
	return b.String()
}

type intrinsicParser struct {
	s     string
	i     int
	t     *thread
	input any
}

func (p *intrinsicParser) skipWS() int {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n') {
		p.i++
	}
	return p.i
}

// expr parses and evaluates one argument: a literal, a path or a nested call.
func (p *intrinsicParser) expr() (any, error) {
	p.skipWS()
	rest := p.s[p.i:]
	switch {
	case rest == "":
		return nil, fmt.Errorf("unexpected end")
	case rest[0] == '\'':
		var b strings.Builder
		for j := 1; j < len(rest); j++ {
			switch rest[j] {
			case '\\':
				if j+1 < len(rest) {
					b.WriteByte('\\')
					b.WriteByte(rest[j+1])
					j++
				}
			case '\'':
				p.i += j + 1
				return rawStr(b.String()), nil
			default:
				b.WriteByte(rest[j])
			}
		}
		return nil, fmt.Errorf("unterminated string")
	case strings.HasPrefix(rest, "States."):
		open := strings.IndexByte(rest, '(')
		if open < 0 {
			return nil, fmt.Errorf("missing '(' after %s", rest)
		}
		name := strings.TrimSpace(rest[:open])
		p.i += open + 1
		var args []any
		if p.skipWS(); p.i < len(p.s) && p.s[p.i] == ')' {
			p.i++
			return intrinsic(name, args)
		}
		for {
			a, err := p.expr()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if p.skipWS(); p.i >= len(p.s) {
				return nil, fmt.Errorf("missing ')'")
			}
			c := p.s[p.i]
			p.i++
			if c == ')' {
				return intrinsic(name, args)
			}
			if c != ',' {
				return nil, fmt.Errorf("unexpected %q", c)
			}
		}
	case rest[0] == '$':
		depth, quote, j := 0, byte(0), 0
	scan:
		for ; j < len(rest); j++ {
			switch c := rest[j]; {
			case quote != 0:
				if c == quote {
					quote = 0
				}
			case c == '\'' || c == '"':
				quote = c
			case c == '[':
				depth++
			case c == ']':
				depth--
			case depth == 0 && (c == ',' || c == ')'):
				break scan
			}
		}
		p.i += j
		path := strings.TrimSpace(rest[:j])
		v, ok, err := p.t.lookup(path, p.input)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, runtimeErr("The JSONPath '%s' in the intrinsic function could not be found in the input '%s'", path, js(p.input))
		}
		return v, nil
	}
	j := strings.IndexAny(rest, ",)")
	if j < 0 {
		j = len(rest)
	}
	tok := strings.TrimSpace(rest[:j])
	p.i += j
	switch tok {
	case "true", "false":
		return tok == "true", nil
	case "null":
		return nil, nil
	}
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid argument %q", tok)
	}
	return f, nil
}

func intrinsic(name string, a []any) (any, error) {
	for i := range a {
		if name != "States.Format" || i > 0 {
			a[i] = unraw(a[i])
		}
	}
	want := func(n int) error {
		if len(a) != n {
			return fmt.Errorf("%s takes %d arguments, got %d", name, n, len(a))
		}
		return nil
	}
	switch name {
	case "States.Format":
		if len(a) == 0 {
			return nil, fmt.Errorf("States.Format needs a template")
		}
		tpl, raw := a[0].(rawStr)
		if !raw {
			s, ok := a[0].(string)
			if !ok {
				return nil, fmt.Errorf("States.Format template must be a string")
			}
			tpl = rawStr(strings.NewReplacer(`\`, `\\`).Replace(s))
		}
		var b strings.Builder
		n := 1
		for i := 0; i < len(tpl); i++ {
			switch {
			case tpl[i] == '\\' && i+1 < len(tpl):
				i++
				b.WriteByte(tpl[i])
			case tpl[i] == '{' && i+1 < len(tpl) && tpl[i+1] == '}':
				if n >= len(a) {
					return nil, fmt.Errorf("States.Format has more placeholders than arguments")
				}
				if s, ok := a[n].(string); ok {
					b.WriteString(s)
				} else {
					b.WriteString(js(a[n]))
				}
				n++
				i++
			default:
				b.WriteByte(tpl[i])
			}
		}
		if n != len(a) {
			return nil, fmt.Errorf("States.Format has more arguments than placeholders")
		}
		return b.String(), nil
	case "States.StringToJson":
		s, err := str(a, 0, want(1))
		if err != nil {
			return nil, err
		}
		var v any
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			return nil, err
		}
		return v, nil
	case "States.JsonToString":
		if err := want(1); err != nil {
			return nil, err
		}
		return js(a[0]), nil
	case "States.Array":
		return append([]any{}, a...), nil
	case "States.ArrayPartition":
		arr, err := list(a, 0, want(2))
		if err != nil {
			return nil, err
		}
		size, err := integer(a[1])
		if err != nil || size <= 0 {
			return nil, fmt.Errorf("chunk size must be a positive integer")
		}
		out := []any{}
		for i := 0; i < len(arr); i += size {
			out = append(out, append([]any{}, arr[i:min(i+size, len(arr))]...))
		}
		return out, nil
	case "States.ArrayContains":
		arr, err := list(a, 0, want(2))
		if err != nil {
			return nil, err
		}
		for _, x := range arr {
			if reflect.DeepEqual(x, a[1]) {
				return true, nil
			}
		}
		return false, nil
	case "States.ArrayRange":
		if err := want(3); err != nil {
			return nil, err
		}
		start, e1 := integer(a[0])
		end, e2 := integer(a[1])
		step, e3 := integer(a[2])
		if e1 != nil || e2 != nil || e3 != nil || step == 0 {
			return nil, fmt.Errorf("States.ArrayRange takes integers and a non-zero step")
		}
		out := []any{}
		for i := start; (step > 0 && i <= end) || (step < 0 && i >= end); i += step {
			if len(out) == 1000 {
				return nil, fmt.Errorf("States.ArrayRange is limited to 1000 items")
			}
			out = append(out, float64(i))
		}
		return out, nil
	case "States.ArrayGetItem":
		arr, err := list(a, 0, want(2))
		if err != nil {
			return nil, err
		}
		i, err := integer(a[1])
		if err != nil || i < 0 || i >= len(arr) {
			return nil, fmt.Errorf("index %v is out of bounds", a[1])
		}
		return arr[i], nil
	case "States.ArrayLength":
		arr, err := list(a, 0, want(1))
		return float64(len(arr)), err
	case "States.ArrayUnique":
		arr, err := list(a, 0, want(1))
		if err != nil {
			return nil, err
		}
		seen, out := map[string]bool{}, []any{}
		for _, x := range arr {
			if k := js(x); !seen[k] {
				seen[k] = true
				out = append(out, x)
			}
		}
		return out, nil
	case "States.Base64Encode":
		s, err := str(a, 0, want(1))
		return base64.StdEncoding.EncodeToString([]byte(s)), err
	case "States.Base64Decode":
		s, err := str(a, 0, want(1))
		if err != nil {
			return nil, err
		}
		b, err := base64.StdEncoding.DecodeString(s)
		return string(b), err
	case "States.Hash":
		if err := want(2); err != nil {
			return nil, err
		}
		data, ok := a[0].(string)
		if !ok {
			data = js(a[0])
		}
		algo, _ := a[1].(string)
		h, ok := hashHex(data, algo)
		if !ok {
			return nil, fmt.Errorf("unsupported hash algorithm %q", algo)
		}
		return h, nil
	case "States.JsonMerge":
		if err := want(3); err != nil {
			return nil, err
		}
		x, ok1 := a[0].(map[string]any)
		y, ok2 := a[1].(map[string]any)
		deep, _ := a[2].(bool)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("States.JsonMerge merges two objects")
		}
		return merge(x, y, deep), nil
	case "States.MathRandom":
		if len(a) != 2 && len(a) != 3 {
			return nil, fmt.Errorf("States.MathRandom takes a start, an end and an optional seed")
		}
		lo, e1 := integer(a[0])
		hi, e2 := integer(a[1])
		if e1 != nil || e2 != nil || hi < lo {
			return nil, fmt.Errorf("States.MathRandom takes integers with start <= end")
		}
		r := rand.IntN
		if len(a) == 3 {
			seed, err := integer(a[2])
			if err != nil {
				return nil, err
			}
			r = rand.New(rand.NewPCG(uint64(seed), 0)).IntN
		}
		return float64(lo + r(hi-lo+1)), nil
	case "States.MathAdd":
		if err := want(2); err != nil {
			return nil, err
		}
		x, e1 := integer(a[0])
		y, e2 := integer(a[1])
		if e1 != nil || e2 != nil {
			return nil, fmt.Errorf("States.MathAdd adds two integers")
		}
		return float64(x + y), nil
	case "States.StringSplit":
		s, err := str(a, 0, want(2))
		if err != nil {
			return nil, err
		}
		seps, _ := a[1].(string)
		out := []any{}
		for _, part := range strings.FieldsFunc(s, func(r rune) bool { return strings.ContainsRune(seps, r) }) {
			out = append(out, part)
		}
		return out, nil
	case "States.UUID":
		return core.UUID(), want(0)
	}
	return nil, fmt.Errorf("unknown intrinsic function %s", name)
}

func merge(x, y map[string]any, deep bool) map[string]any {
	out := make(map[string]any, len(x)+len(y))
	for k, v := range x {
		out[k] = v
	}
	for k, v := range y {
		xm, ok1 := out[k].(map[string]any)
		ym, ok2 := v.(map[string]any)
		if deep && ok1 && ok2 {
			v = merge(xm, ym, true)
		}
		out[k] = v
	}
	return out
}

func str(a []any, i int, err error) (string, error) {
	if err != nil {
		return "", err
	}
	s, ok := a[i].(string)
	if !ok {
		return "", fmt.Errorf("argument %d must be a string", i+1)
	}
	return s, nil
}

func list(a []any, i int, err error) ([]any, error) {
	if err != nil {
		return nil, err
	}
	l, ok := a[i].([]any)
	if !ok {
		return nil, fmt.Errorf("argument %d must be an array", i+1)
	}
	return l, nil
}

func integer(v any) (int, error) {
	f, ok := v.(float64)
	if !ok || f != math.Trunc(f) {
		return 0, fmt.Errorf("%s is not an integer", js(v))
	}
	return int(f), nil
}

// ---- Choice rules ----

// rule evaluates one Choice rule (And/Or/Not or a comparison) against input.
func (t *thread) rule(r map[string]any, input any) (bool, error) {
	if l, ok := r["And"].([]any); ok {
		for _, x := range l {
			m, _ := x.(map[string]any)
			if ok, err := t.rule(m, input); !ok || err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if l, ok := r["Or"].([]any); ok {
		for _, x := range l {
			m, _ := x.(map[string]any)
			if ok, err := t.rule(m, input); ok || err != nil {
				return ok, err
			}
		}
		return false, nil
	}
	if m, ok := r["Not"].(map[string]any); ok {
		ok, err := t.rule(m, input)
		return !ok, err
	}
	variable, _ := r["Variable"].(string)
	val, found, err := t.lookup(variable, input)
	if err != nil {
		return false, err
	}
	for op, want := range r {
		if op == "Variable" || op == "Next" || op == "Comment" {
			continue
		}
		if op == "IsPresent" {
			return found == want, nil
		}
		if !found {
			return false, runtimeErr("Invalid path '%s': The choice state's condition path references an invalid value.", variable)
		}
		if base, ok := strings.CutSuffix(op, "Path"); ok {
			p, _ := want.(string)
			if want, err = t.path(p, input, op); err != nil {
				return false, err
			}
			op = base
		}
		return compare(op, val, want), nil
	}
	return false, runtimeErr("Choice rule without a comparison: %s", js(r))
}

func compare(op string, v, want any) bool {
	switch op {
	case "IsNull":
		return (v == nil) == want
	case "IsString":
		_, ok := v.(string)
		return ok == want
	case "IsNumeric":
		_, ok := v.(float64)
		return ok == want
	case "IsBoolean":
		_, ok := v.(bool)
		return ok == want
	case "IsTimestamp":
		_, ok := timestamp(v)
		return ok == want
	case "BooleanEquals":
		b, ok := v.(bool)
		return ok && b == want
	case "StringMatches":
		s, ok1 := v.(string)
		p, ok2 := want.(string)
		return ok1 && ok2 && glob(p).MatchString(s)
	}
	var c int
	switch {
	case strings.HasPrefix(op, "String"):
		a, ok1 := v.(string)
		b, ok2 := want.(string)
		if !ok1 || !ok2 {
			return false
		}
		c = strings.Compare(a, b)
	case strings.HasPrefix(op, "Numeric"):
		a, ok1 := v.(float64)
		b, ok2 := want.(float64)
		if !ok1 || !ok2 {
			return false
		}
		c = cmpFloat(a, b)
	case strings.HasPrefix(op, "Timestamp"):
		a, ok1 := timestamp(v)
		b, ok2 := timestamp(want)
		if !ok1 || !ok2 {
			return false
		}
		c = a.Compare(b)
	default:
		return false
	}
	switch {
	case strings.HasSuffix(op, "GreaterThanEquals"):
		return c >= 0
	case strings.HasSuffix(op, "LessThanEquals"):
		return c <= 0
	case strings.HasSuffix(op, "GreaterThan"):
		return c > 0
	case strings.HasSuffix(op, "LessThan"):
		return c < 0
	case strings.HasSuffix(op, "Equals"):
		return c == 0
	}
	return false
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func timestamp(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	return t, err == nil
}

// glob turns a StringMatches pattern (* wildcards, \* and \\ escapes) into a regexp.
func glob(p string) *regexp.Regexp {
	var b strings.Builder
	b.WriteString(`(?s)^`)
	rs := []rune(p)
	for i := 0; i < len(rs); i++ {
		switch {
		case rs[i] == '\\' && i+1 < len(rs):
			i++
			b.WriteString(regexp.QuoteMeta(string(rs[i])))
		case rs[i] == '*':
			b.WriteString(`.*`)
		default:
			b.WriteString(regexp.QuoteMeta(string(rs[i])))
		}
	}
	b.WriteString(`$`)
	return regexp.MustCompile(b.String())
}
