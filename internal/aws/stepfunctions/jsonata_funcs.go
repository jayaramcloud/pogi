package stepfunctions

// The JSONata built-in functions: the ones jsonata.org documents, minus $eval,
// $formatInteger and $parseInteger, plus the ones Step Functions adds ($partition,
// $range, $hash, $random with a seed, $uuid, $parse).

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

var builtins = map[string]*builtin{}

func reg(name string, min, max int, ctx byte, fn func(x *jx, a []any) (any, error)) {
	builtins[name] = &builtin{name, min, max, ctx, fn}
}

func nowMillis() int64 { return time.Now().UnixMilli() }

// ---- conversions ----

// numStr renders a number as JavaScript does after rounding to 15 digits, so 0.1+0.2 is "0.3".
func numStr(f float64) string {
	if f == 0 {
		return "0"
	}
	p, _ := strconv.ParseFloat(strconv.FormatFloat(f, 'g', 15, 64), 64)
	if a := math.Abs(p); a >= 1e21 || a < 1e-6 {
		s := strconv.FormatFloat(p, 'e', -1, 64) // 1.5e-07 becomes 1.5e-7
		i := strings.IndexByte(s, 'e')
		exp, _ := strconv.Atoi(s[i+1:])
		sign := "+"
		if exp < 0 {
			sign, exp = "-", -exp
		}
		return s[:i] + "e" + sign + strconv.Itoa(exp)
	}
	return strconv.FormatFloat(p, 'f', -1, 64)
}

func writeJSON(b *strings.Builder, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool, string:
		b.WriteString(js(t))
	case float64:
		b.WriteString(numStr(t))
	case []any, seq:
		b.WriteByte('[')
		for i, e := range items(t) {
			if i > 0 {
				b.WriteByte(',')
			}
			writeJSON(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		b.WriteByte('{')
		for i, k := range sortedKeys(t) {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(js(k) + ":")
			writeJSON(b, t[k])
		}
		b.WriteByte('}')
	default: // functions have no JSON form: $string gives ""
		b.WriteString(`""`)
	}
}

func jsonString(v any, pretty bool) string {
	var b strings.Builder
	writeJSON(&b, v)
	if !pretty {
		return b.String()
	}
	var out bytes.Buffer
	if json.Indent(&out, []byte(b.String()), "", "  ") != nil {
		return b.String()
	}
	return out.String()
}

// jstring is $string for a defined value.
func jstring(v any, pretty bool) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return numStr(t)
	case *lambda, *builtin, *partial:
		return ""
	}
	return jsonString(v, pretty)
}

// toString is the conversion the & operator uses: undefined is "".
func toString(v any) (string, error) {
	if isUndef(v) {
		return "", nil
	}
	return jstring(v, false), nil
}

// ---- argument helpers ----

func badArg(name string, i int) error {
	return jfail("T0410", "Argument %d of function %s does not match function signature", i+1, name)
}

func argAt(a []any, i int) any {
	if i < len(a) {
		return a[i]
	}
	return undef
}

func strArg(name string, a []any, i int) (string, bool, error) {
	switch s := argAt(a, i).(type) {
	case string:
		return s, true, nil
	case undefined:
		return "", false, nil
	}
	return "", false, badArg(name, i)
}

func numArg(name string, a []any, i int) (float64, bool, error) {
	switch n := argAt(a, i).(type) {
	case float64:
		return n, true, nil
	case undefined:
		return 0, false, nil
	}
	return 0, false, badArg(name, i)
}

// intArg is an optional integer argument.
func intArg(name string, a []any, i int) (int, bool, error) {
	f, ok, err := numArg(name, a, i)
	return int(f), ok, err
}

func fnArg(name string, a []any, i int) (any, error) {
	if !isFn(argAt(a, i)) {
		return nil, badArg(name, i)
	}
	return a[i], nil
}

// numList reads an array of numbers (or one number).
func numList(name string, a []any) ([]float64, bool, error) {
	if isUndef(a[0]) {
		return nil, false, nil
	}
	var out []float64
	for _, v := range items(a[0]) {
		f, ok := v.(float64)
		if !ok {
			return nil, false, jfail("T0412", "Argument 1 of function %s must be an array of numbers", name)
		}
		out = append(out, f)
	}
	return out, true, nil
}

func hashHex(data, algo string) (string, bool) {
	h := map[string]func() hash.Hash{"MD5": md5.New, "SHA-1": sha1.New, "SHA-256": sha256.New, "SHA-384": sha512.New384, "SHA-512": sha512.New}[algo]
	if h == nil {
		return "", false
	}
	sum := h()
	sum.Write([]byte(data))
	return hex.EncodeToString(sum.Sum(nil)), true
}

func init() {
	// ---- string ----
	reg("string", 1, 2, 'x', func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		return jstring(a[0], len(a) > 1 && truthy(a[1])), nil
	})
	reg("length", 1, 1, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("length", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		return float64(utf8.RuneCountInString(s)), nil
	})
	reg("substring", 2, 3, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("substring", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		start, _, err := intArg("substring", a, 1)
		if err != nil {
			return nil, err
		}
		rs := []rune(s)
		if start < 0 {
			start = max(len(rs)+start, 0)
		}
		start = min(start, len(rs))
		if n, ok, err := intArg("substring", a, 2); err != nil {
			return nil, err
		} else if ok {
			if n <= 0 {
				return "", nil
			}
			return string(rs[start:min(start+n, len(rs))]), nil
		}
		return string(rs[start:]), nil
	})
	cut := func(name string, after bool) {
		reg(name, 2, 2, 's', func(x *jx, a []any) (any, error) {
			s, ok, err := strArg(name, a, 0)
			if !ok || err != nil {
				return undef, err
			}
			sep, _, err := strArg(name, a, 1)
			if err != nil {
				return nil, err
			}
			i := strings.Index(s, sep)
			switch {
			case i < 0:
				return s, nil
			case after:
				return s[i+len(sep):], nil
			}
			return s[:i], nil
		})
	}
	cut("substringBefore", false)
	cut("substringAfter", true)
	strFn := func(name string, f func(string) string) {
		reg(name, 1, 1, 's', func(x *jx, a []any) (any, error) {
			s, ok, err := strArg(name, a, 0)
			if !ok || err != nil {
				return undef, err
			}
			return f(s), nil
		})
	}
	strFn("uppercase", strings.ToUpper)
	strFn("lowercase", strings.ToLower)
	strFn("trim", func(s string) string { return strings.TrimSpace(spaces.ReplaceAllString(s, " ")) })
	strFn("base64encode", func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) })
	strFn("encodeUrlComponent", func(s string) string { return escapeURL(s, "") })
	strFn("encodeUrl", func(s string) string { return escapeURL(s, ";,/?:@&=+$#") })
	strFn("decodeUrlComponent", func(s string) string { return unescapeURL(s, "") })
	strFn("decodeUrl", func(s string) string { return unescapeURL(s, ";,/?:@&=+$#") })
	reg("base64decode", 1, 1, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("base64decode", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			if b, err = base64.RawStdEncoding.DecodeString(strings.TrimRight(s, "=")); err != nil {
				return nil, jfail("D3137", "The string is not valid base64: %v", err)
			}
		}
		return string(b), nil
	})
	reg("pad", 2, 3, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("pad", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		width, _, err := intArg("pad", a, 1)
		if err != nil {
			return nil, err
		}
		pad, ok, err := strArg("pad", a, 2)
		if err != nil {
			return nil, err
		}
		if !ok || pad == "" {
			pad = " "
		}
		n := max(width, -width) - utf8.RuneCountInString(s)
		if n <= 0 {
			return s, nil
		}
		if n > maxRange {
			return nil, jfail("D3011", "The pad width %d is too large", width)
		}
		fill, pr := make([]rune, 0, n), []rune(pad)
		for i := 0; i < n; i++ {
			fill = append(fill, pr[i%len(pr)])
		}
		if width < 0 {
			return string(fill) + s, nil
		}
		return s + string(fill), nil
	})
	reg("contains", 2, 2, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("contains", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		switch p := a[1].(type) {
		case string:
			return strings.Contains(s, p), nil
		case *jregex:
			return p.re.MatchString(s), nil
		}
		return nil, badArg("contains", 1)
	})
	reg("split", 2, 3, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("split", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		limit, hasLimit, err := intArg("split", a, 2)
		if err != nil {
			return nil, err
		}
		if hasLimit && limit < 0 {
			return nil, jfail("D3020", "Third argument of split function must evaluate to a positive number")
		}
		var parts []string
		switch p := a[1].(type) {
		case string:
			if p == "" {
				for _, r := range s {
					parts = append(parts, string(r))
				}
			} else {
				parts = strings.Split(s, p)
			}
		case *jregex:
			parts = p.re.Split(s, -1)
		default:
			return nil, badArg("split", 1)
		}
		if hasLimit && limit < len(parts) {
			parts = parts[:limit]
		}
		out := make([]any, len(parts))
		for i, p := range parts {
			out[i] = p
		}
		return out, nil
	})
	reg("join", 1, 2, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		sep, _, err := strArg("join", a, 1)
		if err != nil {
			return nil, err
		}
		parts := []string{}
		for _, v := range items(a[0]) {
			s, ok := v.(string)
			if !ok {
				return nil, jfail("T0412", "Argument 1 of function join must be an array of strings")
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, sep), nil
	})
	reg("match", 2, 3, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("match", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		re, ok := a[1].(*jregex)
		if !ok {
			return nil, badArg("match", 1)
		}
		limit, hasLimit, err := intArg("match", a, 2)
		if err != nil {
			return nil, err
		}
		out := seq{}
		for _, m := range re.re.FindAllStringSubmatchIndex(s, -1) {
			if hasLimit && len(out) >= limit {
				break
			}
			groups := []any{}
			for g := 2; g < len(m); g += 2 {
				if m[g] < 0 {
					groups = append(groups, "")
				} else {
					groups = append(groups, s[m[g]:m[g+1]])
				}
			}
			out = append(out, map[string]any{"match": s[m[0]:m[1]], "index": float64(utf8.RuneCountInString(s[:m[0]])), "groups": groups})
		}
		return out, nil
	})
	reg("replace", 3, 4, 's', func(x *jx, a []any) (any, error) {
		return replace(x, a)
	})

	// ---- numbers ----
	num := func(name string, f func(float64) (float64, error)) {
		reg(name, 1, 1, 'n', func(x *jx, a []any) (any, error) {
			n, ok, err := numArg(name, a, 0)
			if !ok || err != nil {
				return undef, err
			}
			return f(n)
		})
	}
	num("abs", func(n float64) (float64, error) { return math.Abs(n), nil })
	num("floor", func(n float64) (float64, error) { return math.Floor(n), nil })
	num("ceil", func(n float64) (float64, error) { return math.Ceil(n), nil })
	num("sqrt", func(n float64) (float64, error) {
		if n < 0 {
			return 0, jfail("D3060", "The sqrt function cannot be applied to a negative number: %s", numStr(n))
		}
		return math.Sqrt(n), nil
	})
	reg("round", 1, 2, 'n', func(x *jx, a []any) (any, error) {
		n, ok, err := numArg("round", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		p, _, err := intArg("round", a, 1)
		if err != nil {
			return nil, err
		}
		return shift10(math.RoundToEven(shift10(n, p)), -p), nil
	})
	reg("power", 2, 2, 0, func(x *jx, a []any) (any, error) {
		b, ok, err := numArg("power", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		e, _, err := numArg("power", a, 1)
		if err != nil {
			return nil, err
		}
		r := math.Pow(b, e)
		if math.IsNaN(r) || math.IsInf(r, 0) {
			return nil, jfail("D3061", "The power function has resulted in a value that cannot be represented as a JSON number: base=%s, exponent=%s", numStr(b), numStr(e))
		}
		return r, nil
	})
	reg("random", 0, 1, 0, func(x *jx, a []any) (any, error) { // the argument is a seed (Step Functions)
		if seed, ok, err := numArg("random", a, 0); err != nil {
			return nil, err
		} else if ok {
			return rand.New(rand.NewPCG(uint64(int64(seed)), 0)).Float64(), nil
		}
		return rand.Float64(), nil
	})
	reg("number", 1, 1, 'x', func(x *jx, a []any) (any, error) {
		switch v := a[0].(type) {
		case undefined:
			return undef, nil
		case float64:
			return v, nil
		case string:
			s := strings.TrimSpace(v)
			if f, ok := parseNumber(s); ok {
				return f, nil
			}
			return nil, jfail("D3030", "Unable to cast value to a number: %s", v)
		}
		return nil, jfail("T0410", "Argument 1 of function number does not match function signature")
	})
	reg("formatNumber", 2, 3, 0, func(x *jx, a []any) (any, error) {
		n, ok, err := numArg("formatNumber", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		pic, _, err := strArg("formatNumber", a, 1)
		if err != nil {
			return nil, err
		}
		opts, _ := argAt(a, 2).(map[string]any)
		return formatNumber(n, pic, opts)
	})
	reg("formatBase", 1, 2, 0, func(x *jx, a []any) (any, error) {
		n, ok, err := numArg("formatBase", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		radix := 10
		if r, ok, err := intArg("formatBase", a, 1); err != nil {
			return nil, err
		} else if ok {
			radix = r
		}
		if radix < 2 || radix > 36 {
			return nil, jfail("D3100", "The radix of the formatBase function must be between 2 and 36. It was given %d", radix)
		}
		return strconv.FormatInt(int64(math.RoundToEven(n)), radix), nil
	})

	// ---- aggregates ----
	reg("sum", 1, 1, 0, func(x *jx, a []any) (any, error) {
		l, ok, err := numList("sum", a)
		if !ok || err != nil {
			return undef, err
		}
		s := 0.0
		for _, f := range l {
			s += f
		}
		return s, nil
	})
	reg("average", 1, 1, 0, func(x *jx, a []any) (any, error) {
		l, ok, err := numList("average", a)
		if !ok || err != nil || len(l) == 0 {
			return undef, err
		}
		s := 0.0
		for _, f := range l {
			s += f
		}
		return s / float64(len(l)), nil
	})
	reg("max", 1, 1, 0, func(x *jx, a []any) (any, error) {
		l, ok, err := numList("max", a)
		if !ok || err != nil || len(l) == 0 {
			return undef, err
		}
		return slices.Max(l), nil
	})
	reg("min", 1, 1, 0, func(x *jx, a []any) (any, error) {
		l, ok, err := numList("min", a)
		if !ok || err != nil || len(l) == 0 {
			return undef, err
		}
		return slices.Min(l), nil
	})

	// ---- booleans ----
	reg("boolean", 1, 1, 'x', func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		return truthy(a[0]), nil
	})
	reg("not", 1, 1, 'x', func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		return !truthy(a[0]), nil
	})
	reg("exists", 1, 1, 0, func(x *jx, a []any) (any, error) { return !isUndef(a[0]), nil })

	// ---- arrays ----
	reg("count", 1, 1, 0, func(x *jx, a []any) (any, error) {
		switch {
		case isUndef(a[0]):
			return 0.0, nil
		case isArr(a[0]):
			return float64(len(items(a[0]))), nil
		}
		return 1.0, nil
	})
	reg("append", 2, 2, 0, func(x *jx, a []any) (any, error) {
		switch {
		case isUndef(a[0]):
			return a[1], nil
		case isUndef(a[1]):
			return a[0], nil
		}
		return append(slices.Clone(items(a[0])), items(a[1])...), nil
	})
	reg("reverse", 1, 1, 0, func(x *jx, a []any) (any, error) {
		if !isArr(a[0]) {
			return a[0], nil
		}
		r := slices.Clone(items(a[0]))
		slices.Reverse(r)
		return r, nil
	})
	reg("shuffle", 1, 1, 0, func(x *jx, a []any) (any, error) {
		if !isArr(a[0]) {
			return a[0], nil
		}
		r := slices.Clone(items(a[0]))
		rand.Shuffle(len(r), func(i, j int) { r[i], r[j] = r[j], r[i] })
		return r, nil
	})
	reg("distinct", 1, 1, 0, func(x *jx, a []any) (any, error) {
		if !isArr(a[0]) || len(items(a[0])) <= 1 {
			return a[0], nil
		}
		seen, out := map[string]bool{}, []any{}
		for _, v := range items(a[0]) {
			if k := jsonString(v, false); !seen[k] {
				seen[k] = true
				out = append(out, v)
			}
		}
		return out, nil
	})
	reg("zip", 1, -1, 0, func(x *jx, a []any) (any, error) {
		n := -1
		for _, v := range a {
			if n < 0 || len(items(v)) < n {
				n = len(items(v))
			}
			if isUndef(v) {
				n = 0
			}
		}
		out := []any{}
		for i := 0; i < n; i++ {
			row := make([]any, len(a))
			for j, v := range a {
				row[j] = items(v)[i]
			}
			out = append(out, row)
		}
		return out, nil
	})
	reg("sort", 1, 2, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		if !isArr(a[0]) || len(items(a[0])) <= 1 {
			return a[0], nil
		}
		list := slices.Clone(items(a[0]))
		if len(a) == 1 {
			if !allOf(list, func(v any) bool { _, ok := v.(float64); return ok }) && !allOf(list, func(v any) bool { _, ok := v.(string); return ok }) {
				return nil, jfail("D3070", "The single argument form of the sort function can only be applied to an array of strings or an array of numbers. Use the second argument to specify a comparison function")
			}
			slices.SortStableFunc(list, func(p, q any) int { c, _ := compareKeys(p, q); return c })
			return list, nil
		}
		fn, err := fnArg("sort", a, 1)
		if err != nil {
			return nil, err
		}
		return mergeSort(list, func(l, r any) (bool, error) {
			v, err := x.apply(fn, []any{l, r}, undef)
			return truthy(v), err
		})
	})

	// ---- objects ----
	reg("keys", 1, 1, 'x', func(x *jx, a []any) (any, error) {
		seen, out := map[string]bool{}, seq{}
		for _, v := range items(a[0]) {
			if m, ok := v.(map[string]any); ok {
				for _, k := range sortedKeys(m) {
					if !seen[k] {
						seen[k] = true
						out = append(out, k)
					}
				}
			}
		}
		return out, nil
	})
	reg("lookup", 2, 2, 'x', func(x *jx, a []any) (any, error) {
		k, ok, err := strArg("lookup", a, 1)
		if !ok || err != nil {
			return undef, err
		}
		return lookupName(a[0], k), nil
	})
	reg("spread", 1, 1, 'x', func(x *jx, a []any) (any, error) {
		out := seq{}
		for _, v := range items(a[0]) {
			if m, ok := v.(map[string]any); ok {
				for _, k := range sortedKeys(m) {
					out = append(out, map[string]any{k: m[k]})
				}
			} else if !isUndef(v) {
				out = append(out, v)
			}
		}
		return out, nil
	})
	reg("merge", 1, 1, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		out := map[string]any{}
		for _, v := range items(a[0]) {
			m, ok := v.(map[string]any)
			if !ok {
				return nil, jfail("T0412", "Argument 1 of function merge must be an array of objects")
			}
			for k, e := range m {
				out[k] = e
			}
		}
		return out, nil
	})
	reg("each", 2, 2, 'o', func(x *jx, a []any) (any, error) {
		m, ok := a[0].(map[string]any)
		if !ok {
			return undef, nil
		}
		fn, err := fnArg("each", a, 1)
		if err != nil {
			return nil, err
		}
		out := seq{}
		for _, k := range sortedKeys(m) {
			v, err := x.callback(fn, m[k], k, m)
			if err != nil {
				return nil, err
			}
			if !isUndef(v) {
				out = append(out, v)
			}
		}
		return out, nil
	})
	reg("sift", 1, 2, 'o', func(x *jx, a []any) (any, error) {
		m, ok := a[0].(map[string]any)
		if !ok {
			return undef, nil
		}
		fn, err := fnArg("sift", a, 1)
		if err != nil {
			return nil, err
		}
		out := map[string]any{}
		for _, k := range sortedKeys(m) {
			v, err := x.callback(fn, m[k], k, m)
			if err != nil {
				return nil, err
			}
			if truthy(v) {
				out[k] = m[k]
			}
		}
		if len(out) == 0 {
			return undef, nil
		}
		return out, nil
	})
	reg("type", 1, 1, 0, func(x *jx, a []any) (any, error) {
		switch a[0].(type) {
		case undefined:
			return undef, nil
		case nil:
			return "null", nil
		case float64:
			return "number", nil
		case string:
			return "string", nil
		case bool:
			return "boolean", nil
		case []any, seq:
			return "array", nil
		case map[string]any:
			return "object", nil
		}
		return "function", nil
	})
	reg("error", 0, 1, 0, func(x *jx, a []any) (any, error) {
		msg, ok, err := strArg("error", a, 0)
		if err != nil {
			return nil, err
		}
		if !ok {
			msg = "$error() function evaluated"
		}
		return nil, jfail("D3137", "%s", msg)
	})
	reg("assert", 1, 2, 0, func(x *jx, a []any) (any, error) {
		ok, isBool := a[0].(bool)
		if !isBool {
			return nil, badArg("assert", 0)
		}
		if ok {
			return undef, nil
		}
		msg, has, err := strArg("assert", a, 1)
		if err != nil {
			return nil, err
		}
		if !has {
			msg = "$assert() statement failed"
		}
		return nil, jfail("D3141", "%s", msg)
	})

	// ---- higher-order ----
	reg("map", 2, 2, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		fn, err := fnArg("map", a, 1)
		if err != nil {
			return nil, err
		}
		out := seq{}
		for i, v := range items(a[0]) {
			r, err := x.callback(fn, v, float64(i), a[0])
			if err != nil {
				return nil, err
			}
			if !isUndef(r) {
				out = append(out, r)
			}
		}
		return out, nil
	})
	reg("filter", 2, 2, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		fn, err := fnArg("filter", a, 1)
		if err != nil {
			return nil, err
		}
		out := seq{}
		for i, v := range items(a[0]) {
			r, err := x.callback(fn, v, float64(i), a[0])
			if err != nil {
				return nil, err
			}
			if truthy(r) {
				out = append(out, v)
			}
		}
		return out, nil
	})
	reg("reduce", 2, 3, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		fn, err := fnArg("reduce", a, 1)
		if err != nil {
			return nil, err
		}
		if arity(fn) < 2 {
			return nil, jfail("D3050", "The second argument of reduce function must be a function with at least two arguments")
		}
		list, start := items(a[0]), 0
		var acc any
		switch {
		case len(a) > 2:
			acc = a[2]
		case len(list) == 0:
			return undef, nil
		default:
			acc, start = list[0], 1
		}
		for i := start; i < len(list); i++ {
			if acc, err = x.callback(fn, acc, list[i], float64(i), a[0]); err != nil {
				return nil, err
			}
		}
		return acc, nil
	})
	reg("single", 1, 2, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		var fn any
		if len(a) > 1 {
			var err error
			if fn, err = fnArg("single", a, 1); err != nil {
				return nil, err
			}
		}
		found := undef
		n := 0
		for i, v := range items(a[0]) {
			keep := true
			if fn != nil {
				r, err := x.callback(fn, v, float64(i), a[0])
				if err != nil {
					return nil, err
				}
				keep = truthy(r)
			}
			if keep {
				if n++; n == 1 {
					found = v
				}
			}
		}
		switch n {
		case 0:
			return nil, jfail("D3139", "The $single() function expected exactly 1 matching result. Instead it matched 0.")
		case 1:
			return found, nil
		}
		return nil, jfail("D3138", "The $single() function expected exactly 1 matching result. Instead it matched %d.", n)
	})

	// ---- time ----
	reg("now", 0, 2, 0, func(x *jx, a []any) (any, error) {
		pic, tz, err := picArgs("now", a, 0)
		if err != nil {
			return nil, err
		}
		return formatTime(x.now, pic, tz)
	})
	reg("millis", 0, 0, 0, func(x *jx, a []any) (any, error) { return float64(x.now), nil })
	reg("fromMillis", 1, 3, 0, func(x *jx, a []any) (any, error) {
		n, ok, err := numArg("fromMillis", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		pic, tz, err := picArgs("fromMillis", a, 1)
		if err != nil {
			return nil, err
		}
		return formatTime(int64(n), pic, tz)
	})
	reg("toMillis", 1, 2, 's', func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("toMillis", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		if len(a) > 1 {
			return nil, jfail("D3136", "pogi does not support a picture string in $toMillis")
		}
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04Z07:00", "2006-01-02T15:04", "2006-01-02", "2006-01", "2006", "20060102T150405Z0700", "2006-01-02T15:04:05.999999999Z0700"} {
			if t, err := time.Parse(layout, s); err == nil {
				return float64(t.UnixMilli()), nil
			}
		}
		return nil, jfail("D3110", "The timestamp %q cannot be parsed", s)
	})

	// ---- Step Functions ----
	reg("partition", 2, 2, 0, func(x *jx, a []any) (any, error) {
		if isUndef(a[0]) {
			return undef, nil
		}
		size, _, err := intArg("partition", a, 1)
		if err != nil {
			return nil, err
		}
		if size <= 0 {
			return nil, jfail("D3050", "The chunk size of $partition must be a positive integer")
		}
		list, out := items(a[0]), []any{}
		for i := 0; i < len(list); i += size {
			out = append(out, slices.Clone(list[i:min(i+size, len(list))]))
		}
		return out, nil
	})
	reg("range", 2, 3, 0, func(x *jx, a []any) (any, error) {
		lo, ok1, e1 := numArg("range", a, 0)
		hi, ok2, e2 := numArg("range", a, 1)
		step, ok3, e3 := numArg("range", a, 2)
		if err := cmpErr(e1, cmpErr(e2, e3)); err != nil {
			return nil, err
		}
		if !ok3 {
			step = 1
		}
		if !ok1 || !ok2 || lo != math.Trunc(lo) || hi != math.Trunc(hi) || step != math.Trunc(step) || step == 0 {
			return nil, jfail("D3050", "$range takes integers and a step that is not 0")
		}
		out := []any{}
		for v := lo; step > 0 && v <= hi || step < 0 && v >= hi; v += step {
			if len(out) >= maxRange {
				return nil, jfail("D2014", "The size of the sequence allocated by $range must not exceed %d", maxRange)
			}
			out = append(out, v)
		}
		return out, nil
	})
	reg("hash", 2, 2, 0, func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("hash", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		algo, _, err := strArg("hash", a, 1)
		if err != nil {
			return nil, err
		}
		h, ok := hashHex(s, algo)
		if !ok {
			return nil, jfail("D3050", "Unsupported hash algorithm %q", algo)
		}
		return h, nil
	})
	reg("uuid", 0, 0, 0, func(x *jx, a []any) (any, error) { return core.UUID(), nil })
	reg("parse", 1, 1, 0, func(x *jx, a []any) (any, error) {
		s, ok, err := strArg("parse", a, 0)
		if !ok || err != nil {
			return undef, err
		}
		var v any
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			return nil, jfail("D3140", "$parse could not read the string as JSON: %v", err)
		}
		return v, nil
	})
}

var spaces = regexp.MustCompile(`[ \t\n\r]+`)

func allOf(l []any, ok func(any) bool) bool {
	for _, v := range l {
		if !ok(v) {
			return false
		}
	}
	return true
}

// mergeSort is jsonata's stable sort: swap(l, r) says l must come after r.
func mergeSort(l []any, swap func(l, r any) (bool, error)) ([]any, error) {
	if len(l) <= 1 {
		return l, nil
	}
	left, err := mergeSort(slices.Clone(l[:len(l)/2]), swap)
	if err != nil {
		return nil, err
	}
	right, err := mergeSort(slices.Clone(l[len(l)/2:]), swap)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(l))
	for len(left) > 0 && len(right) > 0 {
		s, err := swap(left[0], right[0])
		if err != nil {
			return nil, err
		}
		if s {
			out, right = append(out, right[0]), right[1:]
		} else {
			out, left = append(out, left[0]), left[1:]
		}
	}
	return append(append(out, left...), right...), nil
}

var numberText = regexp.MustCompile(`^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$`)

func parseNumber(s string) (float64, bool) {
	if len(s) > 2 && s[0] == '0' && strings.ContainsRune("xXoObB", rune(s[1])) {
		n, err := strconv.ParseInt(s, 0, 64)
		return float64(n), err == nil
	}
	if !numberText.MatchString(s) {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil && !math.IsInf(f, 0)
}

// shift10 multiplies by 10^n through the decimal text, so 1.005 stays 1.005.
func shift10(f float64, n int) float64 {
	if n == 0 || f == 0 {
		return f
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	i := strings.IndexByte(s, 'e')
	exp, _ := strconv.Atoi(s[i+1:])
	r, _ := strconv.ParseFloat(s[:i]+"e"+strconv.Itoa(exp+n), 64)
	return r
}

// escapeURL percent-encodes s, keeping letters, digits, -_.!~*'() and the characters in keep.
func escapeURL(s, keep string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("-_.!~*'()", c) >= 0 || strings.IndexByte(keep, c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// unescapeURL decodes percent escapes, except those that stand for a character in keep.
func unescapeURL(s, keep string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+3 <= len(s) {
			if raw, err := hex.DecodeString(s[i+1 : i+3]); err == nil {
				if strings.IndexByte(keep, raw[0]) >= 0 {
					b.WriteString(s[i : i+3])
				} else {
					b.WriteByte(raw[0])
				}
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// replace is $replace(str, pattern, replacement[, limit]).
func replace(x *jx, a []any) (any, error) {
	s, ok, err := strArg("replace", a, 0)
	if !ok || err != nil {
		return undef, err
	}
	limit, hasLimit, err := intArg("replace", a, 3)
	if err != nil {
		return nil, err
	}
	if hasLimit && limit < 0 {
		return nil, jfail("D3011", "Fourth argument of replace function must evaluate to a positive number")
	}
	var matches [][]int
	switch p := a[1].(type) {
	case string:
		if p == "" {
			return nil, jfail("D3010", "Second argument of replace function cannot be an empty string")
		}
		for i := 0; ; {
			j := strings.Index(s[i:], p)
			if j < 0 {
				break
			}
			matches = append(matches, []int{i + j, i + j + len(p)})
			i += j + len(p)
		}
	case *jregex:
		matches = p.re.FindAllStringSubmatchIndex(s, -1)
	default:
		return nil, badArg("replace", 1)
	}
	if hasLimit && limit < len(matches) {
		matches = matches[:limit]
	}
	var b strings.Builder
	last := 0
	for _, m := range matches {
		b.WriteString(s[last:m[0]])
		last = m[1]
		groups := []any{}
		for g := 2; g < len(m); g += 2 {
			if m[g] < 0 {
				groups = append(groups, "")
			} else {
				groups = append(groups, s[m[g]:m[g+1]])
			}
		}
		switch r := a[2].(type) {
		case string:
			if _, isRe := a[1].(*jregex); isRe {
				b.WriteString(expandGroups(r, s[m[0]:m[1]], groups))
			} else {
				b.WriteString(r)
			}
		default:
			if !isFn(r) {
				return nil, badArg("replace", 2)
			}
			v, err := x.apply(r, []any{map[string]any{"match": s[m[0]:m[1]], "start": float64(m[0]), "end": float64(m[1]), "groups": groups}}, undef)
			if err != nil {
				return nil, err
			}
			rs, ok := v.(string)
			if !ok {
				return nil, jfail("D3012", "Attempted to replace a matched string with a non-string value")
			}
			b.WriteString(rs)
		}
	}
	b.WriteString(s[last:])
	return b.String(), nil
}

// expandGroups fills $0, $1, ... and $$ in a replacement string.
func expandGroups(r, match string, groups []any) string {
	var b strings.Builder
	for i := 0; i < len(r); i++ {
		if r[i] != '$' || i+1 >= len(r) {
			b.WriteByte(r[i])
			continue
		}
		if r[i+1] == '$' {
			b.WriteByte('$')
			i++
			continue
		}
		j := i + 1
		for j < len(r) && r[j] >= '0' && r[j] <= '9' && (j == i+1 || len(groups) >= 10) {
			j++
		}
		if j == i+1 {
			b.WriteByte('$')
			continue
		}
		n, _ := strconv.Atoi(r[i+1 : j])
		switch {
		case n == 0:
			b.WriteString(match)
		case n <= len(groups):
			b.WriteString(groups[n-1].(string))
		}
		i = j - 1
	}
	return b.String()
}

// formatNumber supports the picture characters 0 # , . % and literal text, a negative sub-picture after ;,
// and the options decimal-separator and grouping-separator.
func formatNumber(n float64, pic string, opts map[string]any) (any, error) {
	dec, grp := ".", ","
	if s, ok := opts["decimal-separator"].(string); ok {
		dec = s
	}
	if s, ok := opts["grouping-separator"].(string); ok {
		grp = s
	}
	pos, neg, hasNeg := strings.Cut(pic, ";")
	sub := pos
	if n < 0 && hasNeg {
		sub = neg
	}
	active := func(r rune) bool { return strings.ContainsRune("0#,.", r) }
	rs := []rune(sub)
	first, last := slices.IndexFunc(rs, active), -1
	for i, r := range rs {
		if active(r) {
			last = i
		}
	}
	if first < 0 {
		return nil, jfail("D3090", "The picture string must contain at least one of the digit characters # or 0: %s", pic)
	}
	prefix, body, suffix := string(rs[:first]), string(rs[first:last+1]), string(rs[last+1:])
	if strings.Count(body, ".") > 1 {
		return nil, jfail("D3081", "The sub-picture must not contain more than one decimal separator character: %s", pic)
	}
	if strings.Contains(prefix+suffix, "%") {
		n *= 100
	}
	intPart, fracPart, _ := strings.Cut(body, ".")
	minInt, minFrac := strings.Count(intPart, "0"), strings.Count(fracPart, "0")
	maxFrac := minFrac + strings.Count(fracPart, "#")
	groupSize := 0
	if i := strings.LastIndex(intPart, ","); i >= 0 {
		groupSize = len(intPart) - i - 1
	}
	abs := shift10(math.RoundToEven(shift10(math.Abs(n), maxFrac)), -maxFrac)
	text := strconv.FormatFloat(abs, 'f', maxFrac, 64)
	ip, fp, _ := strings.Cut(text, ".")
	fp = strings.TrimRight(fp, "0")
	for len(fp) < minFrac {
		fp += "0"
	}
	ip = strings.TrimLeft(ip, "0")
	for len(ip) < minInt {
		ip = "0" + ip
	}
	if groupSize > 0 {
		var g []string
		for len(ip) > groupSize {
			g = append([]string{ip[len(ip)-groupSize:]}, g...)
			ip = ip[:len(ip)-groupSize]
		}
		ip = strings.Join(append([]string{ip}, g...), grp)
	}
	out := ip
	if fp != "" {
		out += dec + fp
	}
	if n < 0 && !hasNeg {
		prefix = "-" + prefix
	}
	return prefix + out + suffix, nil
}

// ---- time pictures ----

func picArgs(name string, a []any, i int) (pic, tz string, err error) {
	if pic, _, err = strArg(name, a, i); err != nil {
		return
	}
	tz, _, err = strArg(name, a, i+1)
	return
}

var (
	monthNames = []string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}
	dayNames   = []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
)

// formatTime renders epoch milliseconds: ISO 8601 by default, or with an XPath style picture
// such as [Y0001]-[M01]-[D01] (components Y M D d F H h P m s f Z).
func formatTime(ms int64, pic, tz string) (any, error) {
	t := time.UnixMilli(ms).UTC()
	if tz != "" {
		if len(tz) != 5 || (tz[0] != '+' && tz[0] != '-') {
			return nil, jfail("D3134", "The timezone %q must look like +hhmm", tz)
		}
		h, e1 := strconv.Atoi(tz[1:3])
		m, e2 := strconv.Atoi(tz[3:5])
		if e1 != nil || e2 != nil {
			return nil, jfail("D3134", "The timezone %q must look like +hhmm", tz)
		}
		off := (h*60 + m) * 60
		if tz[0] == '-' {
			off = -off
		}
		t = t.In(time.FixedZone(tz, off))
	}
	if pic == "" {
		return t.UTC().Format("2006-01-02T15:04:05.000Z"), nil
	}
	var b strings.Builder
	for i := 0; i < len(pic); i++ {
		switch {
		case strings.HasPrefix(pic[i:], "[["), strings.HasPrefix(pic[i:], "]]"):
			b.WriteByte(pic[i])
			i++
		case pic[i] == '[':
			end := strings.IndexByte(pic[i:], ']')
			if end < 0 {
				return nil, jfail("D3135", "The picture string has a [ without a closing ]")
			}
			s, err := component(t, strings.ReplaceAll(pic[i+1:i+end], " ", ""))
			if err != nil {
				return nil, err
			}
			b.WriteString(s)
			i += end
		default:
			b.WriteByte(pic[i])
		}
	}
	return b.String(), nil
}

func component(t time.Time, m string) (string, error) {
	if m == "" {
		return "", jfail("D3132", "Unknown component specifier in the picture string")
	}
	spec, width, _ := strings.Cut(m[1:], ",")
	var val int
	switch m[0] {
	case 'Y':
		val = t.Year()
	case 'M':
		val = int(t.Month())
	case 'D':
		val = t.Day()
	case 'd':
		val = t.YearDay()
	case 'H':
		val = t.Hour()
	case 'h':
		val = t.Hour()%12 + 12*btoi(t.Hour()%12 == 0)
	case 'm':
		val = t.Minute()
	case 's':
		val = t.Second()
	case 'f':
		digits := len(spec)
		if digits == 0 {
			digits = 3
		}
		return fmt.Sprintf("%09d", t.Nanosecond())[:min(digits, 9)], nil
	case 'P':
		ap := "am"
		if t.Hour() >= 12 {
			ap = "pm"
		}
		if spec == "N" {
			ap = strings.ToUpper(ap)
		}
		return ap, nil
	case 'Z':
		_, off := t.Zone()
		sign := "+"
		if off < 0 {
			sign, off = "-", -off
		}
		if strings.Contains(spec, ":") {
			return fmt.Sprintf("%s%02d:%02d", sign, off/3600, off%3600/60), nil
		}
		return fmt.Sprintf("%s%02d%02d", sign, off/3600, off%3600/60), nil
	case 'F':
		return named(dayNames[(int(t.Weekday())+6)%7], spec, width), nil
	default:
		return "", jfail("D3132", "Unknown component specifier %q in the picture string", m[:1])
	}
	if m[0] == 'M' && (spec == "N" || spec == "n" || spec == "Nn") {
		return named(monthNames[val-1], spec, width), nil
	}
	pad := 1
	if strings.Trim(spec, "0123456789") == "" && len(spec) > 0 && strings.HasPrefix(spec, "0") {
		pad = len(spec)
	} else if (m[0] == 'm' || m[0] == 's') && spec == "" {
		pad = 2
	}
	s := fmt.Sprintf("%0*d", pad, val)
	if width != "" { // min or min-max digits; a maximum cuts a year to its last digits
		lo, hi, _ := strings.Cut(width, "-")
		if mn, err := strconv.Atoi(lo); err == nil && len(s) < mn {
			s = strings.Repeat("0", mn-len(s)) + s
		}
		if mx, err := strconv.Atoi(hi); err == nil && m[0] == 'Y' && len(s) > mx {
			s = s[len(s)-mx:]
		}
	}
	return s, nil
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// named writes a month or day name: N is upper case, n lower case, Nn as is; a width such as 3-3 cuts it.
func named(name, spec, width string) string {
	if _, hi, ok := strings.Cut(width, "-"); ok {
		if mx, err := strconv.Atoi(hi); err == nil && mx < len(name) {
			name = name[:mx]
		}
	}
	switch spec {
	case "N":
		return strings.ToUpper(name)
	case "n":
		return strings.ToLower(name)
	}
	return name
}
