package eventgrid

import (
	"encoding/json"
	"slices"
	"strings"
)

// match reports whether e passes the filter.
func (f *Filter) match(e Event) bool {
	if len(f.IncludedEventTypes) > 0 && !slices.ContainsFunc(f.IncludedEventTypes, func(t string) bool { return strings.EqualFold(t, e.EventType) }) {
		return false
	}
	subj, begins, ends := e.Subject, f.SubjectBeginsWith, f.SubjectEndsWith
	if !f.IsSubjectCaseSensitive {
		subj, begins, ends = strings.ToLower(subj), strings.ToLower(begins), strings.ToLower(ends)
	}
	if !strings.HasPrefix(subj, begins) || !strings.HasSuffix(subj, ends) {
		return false
	}
	var data any // decoded once, for the data.* keys
	var decoded bool
	for _, a := range f.Advanced {
		if !decoded && len(e.Data) > 0 && strings.HasPrefix(strings.ToLower(a.Key), "data") {
			json.Unmarshal(e.Data, &data)
			decoded = true
		}
		if !a.match(field(e, a.Key, data)) {
			return false
		}
	}
	return true
}

// field reads an event field by filter key; ok is false when it does not exist.
func field(e Event, key string, data any) (v any, ok bool) {
	switch strings.ToLower(key) {
	case "id":
		return e.ID, true
	case "topic", "source":
		return e.Topic, true
	case "subject":
		return e.Subject, true
	case "eventtype", "type":
		return e.EventType, true
	case "eventtime", "time":
		return e.EventTime, true
	case "dataversion":
		return e.DataVersion, true
	case "metadataversion":
		return "1", true
	}
	if len(key) < 4 || !strings.EqualFold(key[:4], "data") || data == nil && len(key) > 4 {
		return nil, false
	}
	v = data
	for _, k := range strings.Split(strings.TrimPrefix(key[4:], "."), ".") {
		if k == "" {
			continue
		}
		m, isMap := v.(map[string]any)
		if !isMap {
			return nil, false
		}
		if v, ok = m[k]; !ok {
			return nil, false
		}
	}
	return v, true
}

// match applies one advanced filter. A key the event lacks matches nothing but IsNullOrUndefined.
func (a *AdvancedFilter) match(v any, ok bool) bool {
	op := strings.ToLower(a.Operator)
	switch op {
	case "isnullorundefined":
		return !ok || v == nil
	case "isnotnull":
		return ok && v != nil
	}
	if !ok {
		return false
	}
	vals := a.Values
	if a.Value != nil {
		vals = append([]any{a.Value}, vals...)
	}
	switch op {
	case "boolequals":
		b, isBool := v.(bool)
		return isBool && len(vals) > 0 && vals[0] == b
	case "numberin", "numbernotin", "numberlessthan", "numberlessthanorequals", "numbergreaterthan", "numbergreaterthanorequals":
		n, isNum := v.(float64)
		if !isNum {
			return false
		}
		return number(op, n, vals)
	case "numberinrange", "numbernotinrange":
		n, isNum := v.(float64)
		if !isNum {
			return false
		}
		in := false
		for _, r := range vals {
			if p, isPair := r.([]any); isPair && len(p) == 2 {
				lo, _ := p[0].(float64)
				hi, _ := p[1].(float64)
				in = in || n >= lo && n <= hi
			}
		}
		return in == (op == "numberinrange")
	}
	str, isStr := v.(string)
	if !isStr {
		return false
	}
	str = strings.ToLower(str)
	some := func(f func(string, string) bool) bool { // the filter is true if any value passes f
		return slices.ContainsFunc(vals, func(x any) bool { s, _ := x.(string); return f(str, strings.ToLower(s)) })
	}
	eq := func(a, b string) bool { return a == b }
	switch op {
	case "stringin":
		return some(eq)
	case "stringnotin":
		return !some(eq)
	case "stringbeginswith":
		return some(strings.HasPrefix)
	case "stringnotbeginswith":
		return !some(strings.HasPrefix)
	case "stringendswith":
		return some(strings.HasSuffix)
	case "stringnotendswith":
		return !some(strings.HasSuffix)
	case "stringcontains":
		return some(strings.Contains)
	case "stringnotcontains":
		return !some(strings.Contains)
	}
	return false // an unknown operator matches nothing
}

func number(op string, n float64, vals []any) bool {
	cmp := func(f func(x float64) bool) bool {
		return slices.ContainsFunc(vals, func(v any) bool { x, isNum := v.(float64); return isNum && f(x) })
	}
	switch op {
	case "numberin":
		return cmp(func(x float64) bool { return n == x })
	case "numbernotin":
		return !cmp(func(x float64) bool { return n == x })
	case "numberlessthan":
		return cmp(func(x float64) bool { return n < x })
	case "numberlessthanorequals":
		return cmp(func(x float64) bool { return n <= x })
	case "numbergreaterthan":
		return cmp(func(x float64) bool { return n > x })
	}
	return cmp(func(x float64) bool { return n >= x }) // numbergreaterthanorequals
}
