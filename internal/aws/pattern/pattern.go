// Package pattern implements EventBridge event patterns, which SNS filter
// policies (and Lambda event filters) share: literals, prefix, suffix,
// anything-but, numeric, exists, equals-ignore-case, wildcard, cidr and $or.
package pattern

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

// Pattern is a compiled pattern.
type Pattern struct {
	fields map[string]field
	ors    [][]*Pattern // every group must have one matching alternative
}

type field struct {
	sub      *Pattern // nested object pattern
	matchers []matcher
}

// matcher tests one value; present is false when the field is absent.
// exists matchers test the field itself, never the items of an array.
type matcher struct {
	test   func(v any, present bool) bool
	exists bool
}

func m(test func(v any, present bool) bool) matcher { return matcher{test: test} }

// Compile parses a JSON pattern.
func Compile(src string) (*Pattern, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(src), &raw); err != nil {
		return nil, fmt.Errorf("pattern is not a JSON object: %w", err)
	}
	return compile(raw)
}

func compile(raw map[string]any) (*Pattern, error) {
	p := &Pattern{fields: map[string]field{}}
	for k, v := range raw {
		if k == "$or" {
			alts, ok := v.([]any)
			if !ok || len(alts) < 2 {
				return nil, fmt.Errorf("$or needs an array of at least two patterns")
			}
			var group []*Pattern
			for _, a := range alts {
				m, ok := a.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("$or alternatives must be objects")
				}
				sp, err := compile(m)
				if err != nil {
					return nil, err
				}
				group = append(group, sp)
			}
			p.ors = append(p.ors, group)
			continue
		}
		switch x := v.(type) {
		case map[string]any:
			sp, err := compile(x)
			if err != nil {
				return nil, err
			}
			p.fields[k] = field{sub: sp}
		case []any:
			var ms []matcher
			for _, item := range x {
				m, err := leaf(item)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", k, err)
				}
				ms = append(ms, m)
			}
			p.fields[k] = field{matchers: ms}
		default:
			return nil, fmt.Errorf("%s: match values must be inside an array", k)
		}
	}
	return p, nil
}

func leaf(item any) (matcher, error) {
	op, ok := item.(map[string]any)
	if !ok {
		return m(literal(item)), nil
	}
	if len(op) != 1 {
		return matcher{}, fmt.Errorf("a matcher object must have exactly one key")
	}
	for name, arg := range op {
		switch name {
		case "prefix", "suffix":
			test, err := stringTest(name, arg)
			if err != nil {
				return matcher{}, err
			}
			return m(onString(test)), nil
		case "equals-ignore-case":
			s, ok := arg.(string)
			if !ok {
				return matcher{}, fmt.Errorf("equals-ignore-case needs a string")
			}
			return m(onString(func(v string) bool { return strings.EqualFold(v, s) })), nil
		case "wildcard":
			s, ok := arg.(string)
			if !ok {
				return matcher{}, fmt.Errorf("wildcard needs a string")
			}
			return m(onString(func(v string) bool { return wildcard(s, v) })), nil
		case "exists":
			want, ok := arg.(bool)
			if !ok {
				return matcher{}, fmt.Errorf("exists needs true or false")
			}
			return matcher{test: func(v any, present bool) bool { return present == want }, exists: true}, nil
		case "numeric":
			return numeric(arg)
		case "cidr":
			s, _ := arg.(string)
			_, n, err := net.ParseCIDR(s)
			if err != nil {
				return matcher{}, fmt.Errorf("invalid cidr %q", s)
			}
			return m(onString(func(v string) bool { ip := net.ParseIP(v); return ip != nil && n.Contains(ip) })), nil
		case "anything-but":
			return anythingBut(arg)
		}
		return matcher{}, fmt.Errorf("unsupported matcher %q", name)
	}
	return matcher{}, nil
}

func literal(want any) func(any, bool) bool {
	return func(v any, present bool) bool {
		if !present {
			return false
		}
		switch w := want.(type) {
		case float64:
			n, ok := toNumber(v)
			return ok && n == w
		case nil:
			return v == nil
		default:
			return v == want
		}
	}
}

func onString(test func(string) bool) func(any, bool) bool {
	return func(v any, present bool) bool {
		s, ok := v.(string)
		return present && ok && test(s)
	}
}

// stringTest builds prefix/suffix tests, including the equals-ignore-case form.
func stringTest(kind string, arg any) (func(string) bool, error) {
	ignoreCase := false
	s, ok := arg.(string)
	if m, isMap := arg.(map[string]any); isMap {
		s, ok = m["equals-ignore-case"].(string)
		ignoreCase = true
	}
	if !ok {
		return nil, fmt.Errorf("%s needs a string", kind)
	}
	return func(v string) bool {
		if ignoreCase {
			v, s = strings.ToLower(v), strings.ToLower(s)
		}
		if kind == "prefix" {
			return strings.HasPrefix(v, s)
		}
		return strings.HasSuffix(v, s)
	}, nil
}

func numeric(arg any) (matcher, error) {
	parts, ok := arg.([]any)
	if !ok || len(parts) == 0 || len(parts)%2 != 0 {
		return matcher{}, fmt.Errorf("numeric needs operator/value pairs")
	}
	type cond struct {
		op  string
		val float64
	}
	var conds []cond
	for i := 0; i < len(parts); i += 2 {
		op, ok1 := parts[i].(string)
		val, ok2 := parts[i+1].(float64)
		if !ok1 || !ok2 {
			return matcher{}, fmt.Errorf("numeric needs operator/value pairs")
		}
		switch op {
		case "=", "<", "<=", ">", ">=":
		default:
			return matcher{}, fmt.Errorf("unknown numeric operator %q", op)
		}
		conds = append(conds, cond{op, val})
	}
	return m(func(v any, present bool) bool {
		n, ok := toNumber(v)
		if !present || !ok {
			return false
		}
		for _, c := range conds {
			ok := map[string]bool{"=": n == c.val, "<": n < c.val, "<=": n <= c.val, ">": n > c.val, ">=": n >= c.val}[c.op]
			if !ok {
				return false
			}
		}
		return true
	}), nil
}

func anythingBut(arg any) (matcher, error) {
	var inner []matcher
	switch x := arg.(type) {
	case []any:
		for _, item := range x {
			inner = append(inner, m(literal(item)))
		}
	case map[string]any:
		lm, err := leaf(x)
		if err != nil {
			return matcher{}, err
		}
		inner = []matcher{lm}
	default:
		inner = []matcher{m(literal(x))}
	}
	return m(func(v any, present bool) bool {
		if !present {
			return false
		}
		for _, im := range inner {
			if im.test(v, true) {
				return false
			}
		}
		return true
	}), nil
}

func toNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case int:
		return float64(n), true
	}
	return 0, false
}

// wildcard matches s against a pattern where * matches any run of characters.
func wildcard(pat, s string) bool {
	star, mark := -1, 0
	i, j := 0, 0
	for j < len(s) {
		switch {
		case i < len(pat) && pat[i] == '*':
			star, mark = i, j
			i++
		case i < len(pat) && pat[i] == s[j]:
			i++
			j++
		case star >= 0:
			i, mark = star+1, mark+1
			j = mark
		default:
			return false
		}
	}
	for i < len(pat) && pat[i] == '*' {
		i++
	}
	return i == len(pat)
}

// Match reports whether a decoded JSON event matches.
func (p *Pattern) Match(event map[string]any) bool {
	for k, f := range p.fields {
		v, present := event[k]
		if f.sub != nil {
			if !matchNested(f.sub, v) {
				return false
			}
			continue
		}
		if !matchLeaf(f.matchers, v, present) {
			return false
		}
	}
	for _, group := range p.ors {
		ok := false
		for _, alt := range group {
			if alt.Match(event) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func matchNested(sub *Pattern, v any) bool {
	switch x := v.(type) {
	case map[string]any:
		return sub.Match(x)
	case []any:
		for _, item := range x {
			if m, ok := item.(map[string]any); ok && sub.Match(m) {
				return true
			}
		}
		return false
	}
	return sub.Match(map[string]any{})
}

func matchLeaf(ms []matcher, v any, present bool) bool {
	arr, isArr := v.([]any)
	for _, mt := range ms {
		if !isArr || !present || mt.exists {
			if mt.test(v, present) {
				return true
			}
			continue
		}
		for _, item := range arr {
			if mt.test(item, true) {
				return true
			}
		}
	}
	return false
}

// MatchJSON matches a raw JSON event; invalid JSON never matches.
func (p *Pattern) MatchJSON(b []byte) bool {
	var ev map[string]any
	if json.Unmarshal(b, &ev) != nil {
		return false
	}
	return p.Match(ev)
}
