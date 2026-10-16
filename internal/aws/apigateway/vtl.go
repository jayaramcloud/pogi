package apigateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

// A Velocity Template Language (VTL) evaluator for API Gateway mapping
// templates. It reads what the AWS docs describe: references with properties,
// methods and indexes ($a.b, $a.get('k'), $a[0], ${a}, $!a), the directives
// #set, #if/#elseif/#else, #foreach, #break and #stop, comments, expressions
// (+ - * / %, comparisons, && || !, lists, ranges and maps), and the objects
// $input, $util, $context and $stageVariables.
// A reference to nothing prints as an empty string (AWS does too, where plain
// Velocity prints the reference text).
// ponytail: no #macro, #include, #parse or #evaluate (AWS does not offer the
// first three either), and no setting of a property ($a.b = 1); add on demand.

// ---- values ----
// Values are nil, bool, int64, float64, string, *vmap, *vlist and vobjects.

// vobject is a Go object that templates can read properties of and call methods on.
type vobject interface {
	vprop(name string) (any, bool)
	vcall(name string, args []any) (any, bool)
}

// vmap is a Java Map: insertion ordered, like the LinkedHashMap that AWS
// builds from JSON. fold makes get ignore the case of keys (headers).
type vmap struct {
	keys []string
	m    map[string]any
	fold bool
}

func newVmap() *vmap { return &vmap{m: map[string]any{}} }

func (m *vmap) put(k string, v any) {
	if _, ok := m.m[k]; !ok {
		m.keys = append(m.keys, k)
	}
	m.m[k] = v
}

func (m *vmap) get(k string) (any, bool) {
	if v, ok := m.m[k]; ok || !m.fold {
		return v, ok
	}
	for _, key := range m.keys {
		if strings.EqualFold(key, k) {
			return m.m[key], true
		}
	}
	return nil, false
}

func (m *vmap) vprop(name string) (any, bool) { return m.get(name) }

func (m *vmap) vcall(name string, a []any) (any, bool) {
	arg := func(i int) any {
		if i < len(a) {
			return a[i]
		}
		return nil
	}
	switch name {
	case "get":
		v, _ := m.get(vstr(arg(0)))
		return v, true
	case "put":
		old, _ := m.get(vstr(arg(0)))
		m.put(vstr(arg(0)), arg(1))
		return old, true
	case "remove":
		k := vstr(arg(0))
		old := m.m[k]
		delete(m.m, k)
		m.keys = slices.DeleteFunc(m.keys, func(s string) bool { return s == k })
		return old, true
	case "keySet":
		l := &vlist{}
		for _, k := range m.keys {
			l.items = append(l.items, k)
		}
		return l, true
	case "values":
		l := &vlist{}
		for _, k := range m.keys {
			l.items = append(l.items, m.m[k])
		}
		return l, true
	case "entrySet":
		l := &vlist{}
		for _, k := range m.keys {
			e := newVmap()
			e.put("key", k)
			e.put("value", m.m[k])
			l.items = append(l.items, e)
		}
		return l, true
	case "getKey", "getValue": // Map.Entry
		v, _ := m.get(strings.ToLower(strings.TrimPrefix(name, "get")))
		return v, true
	case "size":
		return int64(len(m.keys)), true
	case "isEmpty":
		return len(m.keys) == 0, true
	case "containsKey":
		_, ok := m.get(vstr(arg(0)))
		return ok, true
	case "containsValue":
		for _, v := range m.m {
			if equal(v, arg(0)) {
				return true, true
			}
		}
		return false, true
	case "toString":
		return vstr(m), true
	}
	return nil, false
}

// vlist is a Java List.
type vlist struct{ items []any }

func (l *vlist) vprop(string) (any, bool) { return nil, false }

func (l *vlist) vcall(name string, a []any) (any, bool) {
	arg := func(i int) any {
		if i < len(a) {
			return a[i]
		}
		return nil
	}
	switch name {
	case "get":
		i, _ := arg(0).(int64)
		if i < 0 || int(i) >= len(l.items) {
			return nil, true
		}
		return l.items[i], true
	case "size":
		return int64(len(l.items)), true
	case "isEmpty":
		return len(l.items) == 0, true
	case "contains":
		return slices.ContainsFunc(l.items, func(v any) bool { return equal(v, arg(0)) }), true
	case "indexOf":
		return int64(slices.IndexFunc(l.items, func(v any) bool { return equal(v, arg(0)) })), true
	case "add":
		l.items = append(l.items, arg(0))
		return true, true
	case "toString":
		return vstr(l), true
	}
	return nil, false
}

// vstr is a value as the template prints it: like Java's toString, with nil
// as an empty string.
func vstr(v any) string {
	if v == nil {
		return ""
	}
	return vstrIn(v)
}

// vstrIn prints v inside a collection, where nil is "null".
func vstrIn(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		s := strconv.FormatFloat(v, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	case *vmap:
		parts := make([]string, len(v.keys))
		for i, k := range v.keys {
			parts[i] = k + "=" + vstrIn(v.m[k])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case *vlist:
		parts := make([]string, len(v.items))
		for i, e := range v.items {
			parts[i] = vstrIn(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(v)
}

// truthy is Velocity's test of #if: anything but null and false.
func truthy(v any) bool {
	b, isBool := v.(bool)
	return v != nil && (!isBool || b)
}

func toFloat(v any) (float64, bool) {
	switch v := v.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}

// equal is Velocity's ==: numbers by value, else by their text.
func equal(a, b any) bool {
	if af, ok := toFloat(a); ok {
		if bf, ok := toFloat(b); ok {
			return af == bf
		}
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return vstrIn(a) == vstrIn(b)
}

// compare orders two numbers, or two strings.
func compare(a, b any) (int, bool) {
	if af, ok := toFloat(a); ok {
		if bf, ok := toFloat(b); ok {
			switch {
			case af < bf:
				return -1, true
			case af > bf:
				return 1, true
			}
			return 0, true
		}
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return strings.Compare(as, bs), true
	}
	return 0, false
}

func arith(op string, a, b any) any {
	if op == "+" {
		_, as := a.(string)
		_, bs := b.(string)
		if as || bs {
			return vstr(a) + vstr(b)
		}
	}
	if ai, ok := a.(int64); ok {
		if bi, ok := b.(int64); ok {
			switch op {
			case "+":
				return ai + bi
			case "-":
				return ai - bi
			case "*":
				return ai * bi
			case "/", "%":
				if bi == 0 {
					return nil
				}
				if op == "/" {
					return ai / bi
				}
				return ai % bi
			}
		}
	}
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if !aok || !bok {
		return nil
	}
	switch op {
	case "+":
		return af + bf
	case "-":
		return af - bf
	case "*":
		return af * bf
	case "/":
		return af / bf
	}
	return math.Mod(af, bf)
}

// ---- JSON ----

// decodeJSON reads JSON into values, keeping the order of object keys.
func decodeJSON(b []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	v, err := readJSON(d)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("unexpected data after the JSON value")
	}
	return v, nil
}

func readJSON(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t := t.(type) {
	case json.Delim:
		if t == '{' {
			m := newVmap()
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				v, err := readJSON(d)
				if err != nil {
					return nil, err
				}
				m.put(k.(string), v)
			}
			_, err := d.Token()
			return m, err
		}
		l := &vlist{items: []any{}}
		for d.More() {
			v, err := readJSON(d)
			if err != nil {
				return nil, err
			}
			l.items = append(l.items, v)
		}
		_, err := d.Token()
		return l, err
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, nil
		}
		f, _ := t.Float64()
		return f, nil
	}
	return t, nil // string, bool, nil
}

// toJSON writes values as JSON, in the order they have.
func toJSON(v any) string {
	var b strings.Builder
	writeJSON(&b, v)
	return b.String()
}

func writeJSON(b *strings.Builder, v any) {
	switch v := v.(type) {
	case nil:
		b.WriteString("null")
	case string:
		var buf bytes.Buffer
		e := json.NewEncoder(&buf)
		e.SetEscapeHTML(false)
		e.Encode(v)
		b.Write(bytes.TrimRight(buf.Bytes(), "\n"))
	case *vmap:
		b.WriteByte('{')
		for i, k := range v.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeJSON(b, k)
			b.WriteByte(':')
			writeJSON(b, v.m[k])
		}
		b.WriteByte('}')
	case *vlist:
		b.WriteByte('[')
		for i, e := range v.items {
			if i > 0 {
				b.WriteByte(',')
			}
			writeJSON(b, e)
		}
		b.WriteByte(']')
	default:
		b.WriteString(vstrIn(v))
	}
}

// fromGo converts what encoding/json made (maps, slices, float64) into values.
func fromGo(v any) any {
	switch v := v.(type) {
	case map[string]any:
		m := newVmap()
		for _, k := range sortedKeys(v) {
			m.put(k, fromGo(v[k]))
		}
		return m
	case map[string]string:
		m := newVmap()
		for _, k := range sortedKeys(v) {
			m.put(k, v[k])
		}
		return m
	case []any:
		l := &vlist{items: []any{}}
		for _, e := range v {
			l.items = append(l.items, fromGo(e))
		}
		return l
	case float64:
		if v == math.Trunc(v) && math.Abs(v) < 1e15 {
			return int64(v)
		}
	}
	return v
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---- JSONPath ----

// jp reads a JSONPath from doc: $, .name, ['name'], [0], [-1], [*], .*,
// ..name and the filter [?(@.a == 1)] (== != < > <= >=, or @.a alone for
// "has a"). A path that matches nothing gives nil; ok is false only for a path
// this reader cannot parse. A path with a wildcard, .. or a filter gives a list.
func jp(doc any, path string) (v any, ok bool) {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "$") && !strings.HasPrefix(path, "@") {
		return nil, false
	}
	cur, multi := []any{doc}, false
	for i := 1; i < len(path); {
		var next []any
		switch {
		case strings.HasPrefix(path[i:], ".."):
			name, n := pathName(path[i+2:])
			if n == 0 {
				return nil, false
			}
			i += 2 + n
			multi = true
			for _, c := range cur {
				walk(c, func(n any) {
					if m, isMap := n.(*vmap); isMap {
						if v, has := m.get(name); has && name != "*" {
							next = append(next, v)
						}
					}
				})
			}
		case path[i] == '.':
			name, n := pathName(path[i+1:])
			if n == 0 {
				return nil, false
			}
			i += 1 + n
			if name == "*" {
				multi = true
			}
			next = child(cur, name, name == "*")
		case path[i] == '[':
			end := closing(path, i)
			if end < 0 {
				return nil, false
			}
			inner := strings.TrimSpace(path[i+1 : end])
			i = end + 1
			switch {
			case inner == "*":
				multi = true
				next = child(cur, "*", true)
			case len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[len(inner)-1] == inner[0]:
				next = child(cur, inner[1:len(inner)-1], false)
			case strings.HasPrefix(inner, "?("):
				multi = true
				for _, c := range cur {
					if l, isList := c.(*vlist); isList {
						for _, e := range l.items {
							if filter(e, strings.TrimSuffix(inner[2:], ")")) {
								next = append(next, e)
							}
						}
					}
				}
			default:
				n, err := strconv.Atoi(inner)
				if err != nil {
					return nil, false
				}
				for _, c := range cur {
					if l, isList := c.(*vlist); isList {
						if n < 0 {
							n += len(l.items)
						}
						if n >= 0 && n < len(l.items) {
							next = append(next, l.items[n])
						}
					}
				}
			}
		default:
			return nil, false
		}
		cur = next
	}
	if multi {
		return &vlist{items: append([]any{}, cur...)}, true
	}
	if len(cur) == 0 {
		return nil, true
	}
	return cur[0], true
}

// pathName reads a property name after a dot, and how many bytes it took.
func pathName(s string) (string, int) {
	if strings.HasPrefix(s, "*") {
		return "*", 1
	}
	n := strings.IndexAny(s, ".[")
	if n < 0 {
		n = len(s)
	}
	return s[:n], n
}

// closing finds the ] that matches the [ at s[open], skipping quotes and brackets inside.
func closing(s string, open int) int {
	depth, quote := 0, byte(0)
	for i := open; i < len(s); i++ {
		switch c := s[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '[':
			depth++
		case c == ']':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

func child(cur []any, name string, all bool) (out []any) {
	for _, c := range cur {
		switch c := c.(type) {
		case *vmap:
			if all {
				for _, k := range c.keys {
					out = append(out, c.m[k])
				}
			} else if v, ok := c.get(name); ok {
				out = append(out, v)
			}
		case *vlist:
			if all {
				out = append(out, c.items...)
			}
		}
	}
	return out
}

// walk calls f on n and everything below it.
func walk(n any, f func(any)) {
	f(n)
	switch n := n.(type) {
	case *vmap:
		for _, k := range n.keys {
			walk(n.m[k], f)
		}
	case *vlist:
		for _, e := range n.items {
			walk(e, f)
		}
	}
}

var filterOps = []string{"==", "!=", "<=", ">=", "<", ">"}

// filter tests item against "@.a.b op literal" or "@.a.b".
func filter(item any, cond string) bool {
	cond = strings.TrimSpace(cond)
	for _, op := range filterOps {
		if lhs, rhs, ok := strings.Cut(cond, op); ok {
			v, _ := jp(item, strings.TrimSpace(lhs))
			lit := literal(strings.TrimSpace(rhs))
			if op == "==" || op == "!=" {
				return equal(v, lit) == (op == "==")
			}
			c, ok := compare(v, lit)
			return ok && (op == "<" && c < 0 || op == ">" && c > 0 || op == "<=" && c <= 0 || op == ">=" && c >= 0)
		}
	}
	v, _ := jp(item, cond)
	return v != nil
}

func literal(s string) any {
	switch {
	case s == "true", s == "false":
		return s == "true"
	case s == "null":
		return nil
	case len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[len(s)-1] == s[0]:
		return s[1 : len(s)-1]
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// ---- $util, $input, $context ----

type vutil struct{}

func (vutil) vprop(string) (any, bool) { return nil, false }

func (vutil) vcall(name string, a []any) (any, bool) {
	s := ""
	if len(a) > 0 {
		s = vstr(a[0])
	}
	switch name {
	case "escapeJavaScript":
		return escapeJS(s), true
	case "urlEncode": // like Java's URLEncoder: ~ is escaped and * is not
		return strings.NewReplacer("%2A", "*", "~", "%7E").Replace(url.QueryEscape(s)), true
	case "urlDecode":
		d, err := url.QueryUnescape(s)
		if err != nil {
			return s, true
		}
		return d, true
	case "base64Encode":
		return base64.StdEncoding.EncodeToString([]byte(s)), true
	case "base64Decode":
		d, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			d, err = base64.RawStdEncoding.DecodeString(s)
		}
		if err != nil {
			return "", true
		}
		return string(d), true
	case "parseJson":
		v, err := decodeJSON([]byte(s))
		if err != nil {
			return nil, true
		}
		return v, true
	case "quiet":
		return nil, true
	}
	return nil, false
}

// escapeJS is Apache Commons escapeJavaScript, which AWS uses: it escapes
// ' " \ and /, control characters and everything above ASCII. A \' is not
// valid JSON, and AWS has the same quirk.
func escapeJS(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\f':
			b.WriteString(`\f`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < 32 || r > 0x7f:
			for _, u := range utf16.Encode([]rune{r}) {
				fmt.Fprintf(&b, `\u%04X`, u)
			}
		case strings.ContainsRune(`'"\/`, r):
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// vinput is $input.
type vinput struct {
	x    *call
	body []byte
	once bool
	doc  any
	ok   bool
}

func (in *vinput) path(p string) (any, bool) {
	if !in.once {
		in.once = true
		d, err := decodeJSON(in.body)
		in.doc, in.ok = d, err == nil
	}
	if !in.ok {
		return nil, false
	}
	return jp(in.doc, p)
}

func (in *vinput) vprop(name string) (any, bool) {
	if name == "body" {
		return string(in.body), true
	}
	return nil, false
}

func (in *vinput) vcall(name string, a []any) (any, bool) {
	p := "$"
	if len(a) > 0 {
		p = vstr(a[0])
	}
	switch name {
	case "body":
		return string(in.body), true
	case "json": // the JSON text; an empty body is {}
		v, ok := in.path(p)
		if !ok && len(bytes.TrimSpace(in.body)) == 0 {
			return "{}", true
		}
		return toJSON(v), true
	case "path": // the value itself, to read with . [] and methods
		v, ok := in.path(p)
		if !ok && len(bytes.TrimSpace(in.body)) == 0 {
			return newVmap(), true
		}
		return v, true
	case "params":
		if len(a) > 0 {
			return in.x.param(p), true
		}
		path, query, header := newVmap(), newVmap(), newVmap()
		header.fold = true
		for _, k := range sortedKeys(in.x.params) {
			path.put(k, in.x.params[k])
		}
		q := in.x.r.URL.Query()
		for _, k := range sortedKeys(q) {
			query.put(k, q[k][0])
		}
		h := in.x.r.Header
		for _, k := range sortedKeys(h) {
			header.put(k, h[k][len(h[k])-1])
		}
		m := newVmap()
		m.put("path", path)
		m.put("querystring", query)
		m.put("header", header)
		return m, true
	}
	return nil, false
}

// vcontext builds $context.
func (x *call) vcontext() *vmap {
	m := newVmap()
	for _, k := range []string{"accountId", "apiId", "domainName", "extendedRequestId", "httpMethod", "path", "protocol",
		"requestId", "resourceId", "resourcePath", "stage", "connectionId", "routeKey"} {
		if v := x.context(k); v != "" {
			m.put(k, v)
		}
	}
	m.put("domainPrefix", strings.SplitN(x.r.Host, ".", 2)[0])
	id := newVmap()
	id.put("sourceIp", x.context("identity.sourceIp"))
	id.put("userAgent", x.context("identity.userAgent"))
	m.put("identity", id)
	m.put("authorizer", fromGo(x.auth))
	now := time.Now().UTC()
	m.put("requestTimeEpoch", now.UnixMilli())
	m.put("requestTime", now.Format("02/Jan/2006:15:04:05 -0700"))
	return m
}

// ---- strings and numbers ----

func strMethod(s, name string, a []any) any {
	arg := func(i int) string {
		if i < len(a) {
			return vstr(a[i])
		}
		return ""
	}
	num := func(i int) int {
		if i < len(a) {
			if n, ok := a[i].(int64); ok {
				return int(n)
			}
		}
		return 0
	}
	switch name {
	case "length":
		return int64(len([]rune(s)))
	case "isEmpty":
		return s == ""
	case "substring":
		r := []rune(s)
		from, to := num(0), len(r)
		if len(a) > 1 {
			to = num(1)
		}
		if from < 0 || to > len(r) || from > to {
			return nil
		}
		return string(r[from:to])
	case "indexOf":
		return int64(strings.Index(s, arg(0)))
	case "lastIndexOf":
		return int64(strings.LastIndex(s, arg(0)))
	case "contains":
		return strings.Contains(s, arg(0))
	case "startsWith":
		return strings.HasPrefix(s, arg(0))
	case "endsWith":
		return strings.HasSuffix(s, arg(0))
	case "equals":
		return s == arg(0)
	case "equalsIgnoreCase":
		return strings.EqualFold(s, arg(0))
	case "toUpperCase":
		return strings.ToUpper(s)
	case "toLowerCase":
		return strings.ToLower(s)
	case "trim":
		return strings.TrimSpace(s)
	case "concat":
		return s + arg(0)
	case "replace":
		return strings.ReplaceAll(s, arg(0), arg(1))
	case "replaceAll":
		re, err := regexp.Compile(arg(0))
		if err != nil {
			return nil
		}
		return re.ReplaceAllString(s, arg(1))
	case "matches":
		ok, _ := regexp.MatchString("^(?:"+arg(0)+")$", s)
		return ok
	case "charAt":
		r := []rune(s)
		if i := num(0); i >= 0 && i < len(r) {
			return string(r[i])
		}
		return nil
	case "split": // like Java: trailing empty strings are dropped
		re, err := regexp.Compile(arg(0))
		if err != nil {
			return nil
		}
		parts := re.Split(s, -1)
		for len(parts) > 1 && parts[len(parts)-1] == "" {
			parts = parts[:len(parts)-1]
		}
		l := &vlist{items: []any{}}
		for _, p := range parts {
			l.items = append(l.items, p)
		}
		return l
	case "toString":
		return s
	}
	return nil
}

func numMethod(v any, name string) any {
	f, _ := toFloat(v)
	switch name {
	case "intValue", "longValue":
		return int64(f)
	case "doubleValue", "floatValue":
		return f
	case "toString":
		return vstr(v)
	}
	return nil
}

func callMethod(obj any, name string, args []any) any {
	switch o := obj.(type) {
	case vobject:
		v, _ := o.vcall(name, args)
		return v
	case string:
		return strMethod(o, name, args)
	case int64, float64:
		return numMethod(o, name)
	}
	return nil
}

func getProp(obj any, name string) any {
	if o, ok := obj.(vobject); ok {
		v, _ := o.vprop(name)
		return v
	}
	return nil
}

func index(obj, i any) any {
	switch o := obj.(type) {
	case *vlist:
		n, _ := i.(int64)
		if n >= 0 && int(n) < len(o.items) {
			return o.items[n]
		}
	case *vmap:
		v, _ := o.get(vstr(i))
		return v
	}
	return nil
}

// vforeach is $foreach inside #foreach.
type vforeach struct {
	index, count   int64
	hasNext, first bool
	last           bool
}

func (f *vforeach) vcall(string, []any) (any, bool) { return nil, false }

func (f *vforeach) vprop(name string) (any, bool) {
	switch name {
	case "index":
		return f.index, true
	case "count":
		return f.count, true
	case "hasNext":
		return f.hasNext, true
	case "first":
		return f.first, true
	case "last":
		return f.last, true
	}
	return nil, false
}

// ---- parser ----

// vctx is one run of a template.
type vctx struct {
	vars       map[string]any
	out        *strings.Builder
	brk, stop  bool
	iterations int
}

type (
	vnode func(*vctx)
	vexpr func(*vctx) any
)

// maxIterations stops runaway loops.
const maxIterations = 1_000_000

type vparser struct {
	s string
	i int
}

type vsyntax string

func (p *vparser) fail(format string, a ...any) {
	panic(vsyntax(fmt.Sprintf(format, a...) + " at offset " + strconv.Itoa(p.i)))
}

// vtlRender runs the template with the given variables and returns the text.
func vtlRender(tmpl string, vars map[string]any) (out string, err error) {
	if !strings.ContainsAny(tmpl, "$#") {
		return tmpl, nil
	}
	defer func() {
		if r := recover(); r != nil {
			s, ok := r.(vsyntax)
			if !ok {
				panic(r)
			}
			err = errors.New(string(s))
		}
	}()
	p := &vparser{s: tmpl}
	nodes, _ := p.nodes()
	c := &vctx{vars: vars, out: &strings.Builder{}}
	runNodes(c, nodes)
	return c.out.String(), nil
}

func runNodes(c *vctx, nodes []vnode) {
	for _, n := range nodes {
		if c.brk || c.stop {
			return
		}
		n(c)
	}
}

func isIdentStart(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isIdent(c byte) bool      { return isIdentStart(c) || c >= '0' && c <= '9' || c == '_' || c == '-' }

func (p *vparser) peek() byte {
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *vparser) ws() {
	for p.i < len(p.s) && strings.IndexByte(" \t\r\n", p.s[p.i]) >= 0 {
		p.i++
	}
}

func (p *vparser) expect(c byte) {
	p.ws()
	if p.peek() != c {
		p.fail("expected %q", c)
	}
	p.i++
}

func (p *vparser) ident() string {
	start := p.i
	for p.i < len(p.s) && isIdent(p.s[p.i]) {
		p.i++
	}
	return p.s[start:p.i]
}

// eol skips the line break after a directive, as Velocity does.
func (p *vparser) eol() {
	if rest := p.s[p.i:]; strings.HasPrefix(rest, "\r\n") {
		p.i += 2
	} else if strings.HasPrefix(rest, "\n") {
		p.i++
	}
}

// directive reads #name or #{name} at p.i and returns the name and the offset after it.
func (p *vparser) directive() (name string, after int) {
	i := p.i + 1
	if i < len(p.s) && p.s[i] == '{' {
		if end := strings.IndexByte(p.s[i:], '}'); end > 0 {
			return p.s[i+1 : i+end], i + end + 1
		}
		return "", 0
	}
	j := i
	for j < len(p.s) && isIdentStart(p.s[j]) {
		j++
	}
	return p.s[i:j], j
}

// nodes parses text, references and directives up to one of terms (#end,
// #else, #elseif) and returns which one it met; with no terms, to the end.
func (p *vparser) nodes(terms ...string) (out []vnode, term string) {
	var text strings.Builder
	flush := func() {
		if text.Len() > 0 {
			s := text.String()
			out = append(out, func(c *vctx) { c.out.WriteString(s) })
			text.Reset()
		}
	}
	for p.i < len(p.s) {
		switch ch := p.s[p.i]; {
		case ch == '\\' && p.i+2 < len(p.s) && (p.s[p.i+1] == '$' || p.s[p.i+1] == '#') && isIdentStart(p.s[p.i+2]):
			text.WriteByte(p.s[p.i+1]) // an escaped reference or directive is printed as text
			p.i += 2
		case ch == '#':
			rest := p.s[p.i:]
			switch {
			case strings.HasPrefix(rest, "##"):
				end := strings.IndexByte(rest, '\n')
				if end < 0 {
					end = len(rest) - 1
				}
				p.i += end + 1
				continue
			case strings.HasPrefix(rest, "#*"):
				end := strings.Index(rest, "*#")
				if end < 0 {
					p.fail("comment is not closed")
				}
				p.i += end + 2
				continue
			case strings.HasPrefix(rest, "#[["):
				end := strings.Index(rest, "]]#")
				if end < 0 {
					p.fail("#[[ is not closed")
				}
				text.WriteString(rest[3:end])
				p.i += end + 3
				continue
			}
			name, after := p.directive()
			if slices.Contains(terms, name) {
				flush()
				p.i = after
				return out, name
			}
			switch name {
			case "set", "if", "foreach", "break", "stop":
				flush()
				p.i = after
				out = append(out, p.block(name))
			case "end", "else", "elseif":
				p.fail("#%s without #if or #foreach", name)
			default:
				text.WriteByte('#')
				p.i++
			}
		case ch == '$':
			if e, ok := p.ref(); ok {
				flush()
				out = append(out, func(c *vctx) { c.out.WriteString(vstr(e(c))) })
			} else {
				text.WriteByte('$')
				p.i++
			}
		default:
			text.WriteByte(ch)
			p.i++
		}
	}
	if len(terms) > 0 {
		p.fail("missing #end")
	}
	flush()
	return out, ""
}

// block parses the rest of a directive whose name was just read.
func (p *vparser) block(name string) vnode {
	switch name {
	case "break":
		p.eol()
		return func(c *vctx) { c.brk = true }
	case "stop":
		p.eol()
		return func(c *vctx) { c.stop = true }
	case "set":
		p.ws()
		p.expect('(')
		p.ws()
		if p.peek() != '$' {
			p.fail("#set needs a $variable")
		}
		p.i++
		p.ws()
		if p.peek() == '{' {
			p.i++
		}
		target := p.ident()
		if p.peek() == '}' {
			p.i++
		}
		p.expect('=')
		e := p.expr()
		p.expect(')')
		p.eol()
		return func(c *vctx) {
			if v := e(c); v != nil { // Velocity leaves the variable alone when the value is null
				c.vars[target] = v
			}
		}
	case "foreach":
		p.ws()
		p.expect('(')
		p.ws()
		if p.peek() != '$' {
			p.fail("#foreach needs a $variable")
		}
		p.i++
		item := p.ident()
		p.ws()
		if !strings.HasPrefix(p.s[p.i:], "in") {
			p.fail("#foreach needs \"in\"")
		}
		p.i += 2
		coll := p.expr()
		p.expect(')')
		p.eol()
		body, _ := p.nodes("end")
		p.eol()
		return func(c *vctx) {
			var items []any
			switch v := coll(c).(type) {
			case *vlist:
				items = v.items
			case *vmap:
				for _, k := range v.keys {
					items = append(items, v.m[k])
				}
			}
			old, hadOld := c.vars[item]
			oldFE, hadFE := c.vars["foreach"]
			for i, it := range items {
				if c.iterations++; c.iterations > maxIterations {
					c.stop = true
				}
				if c.stop {
					break
				}
				c.vars[item] = it
				c.vars["foreach"] = &vforeach{index: int64(i), count: int64(i + 1), hasNext: i < len(items)-1, first: i == 0, last: i == len(items)-1}
				c.vars["velocityCount"] = int64(i + 1)
				runNodes(c, body)
				if c.brk {
					c.brk = false
					break
				}
			}
			delete(c.vars, "velocityCount")
			restore(c.vars, item, old, hadOld)
			restore(c.vars, "foreach", oldFE, hadFE)
		}
	}
	// #if
	var conds []vexpr
	var bodies [][]vnode
	var other []vnode
	for {
		p.ws()
		p.expect('(')
		cond := p.expr()
		p.expect(')')
		p.eol()
		body, term := p.nodes("elseif", "else", "end")
		conds, bodies = append(conds, cond), append(bodies, body)
		if term == "elseif" {
			continue
		}
		if term == "else" {
			p.eol()
			other, _ = p.nodes("end")
		}
		p.eol()
		break
	}
	return func(c *vctx) {
		for i, cond := range conds {
			if truthy(cond(c)) {
				runNodes(c, bodies[i])
				return
			}
		}
		runNodes(c, other)
	}
}

func restore(vars map[string]any, k string, v any, had bool) {
	if had {
		vars[k] = v
	} else {
		delete(vars, k)
	}
}

// ref parses a reference at p.i ($a, $!a, ${a}) and its .property, .method(args)
// and [index] tail. It returns false, without moving, when p.i is a lone $.
func (p *vparser) ref() (vexpr, bool) {
	i := p.i + 1
	if i < len(p.s) && p.s[i] == '!' {
		i++
	}
	formal := i < len(p.s) && p.s[i] == '{'
	if formal {
		i++
	}
	if i >= len(p.s) || !isIdentStart(p.s[i]) {
		return nil, false
	}
	p.i = i
	name := p.ident()
	e := vexpr(func(c *vctx) any { return c.vars[name] })
	for {
		switch {
		case p.peek() == '.' && p.i+1 < len(p.s) && isIdentStart(p.s[p.i+1]):
			p.i++
			prop := p.ident()
			base := e
			if p.peek() == '(' {
				args := p.args()
				e = func(c *vctx) any {
					obj := base(c)
					vals := make([]any, len(args))
					for i, a := range args {
						vals[i] = a(c)
					}
					return callMethod(obj, prop, vals)
				}
			} else {
				e = func(c *vctx) any { return getProp(base(c), prop) }
			}
			continue
		case p.peek() == '[':
			p.i++
			idx := p.expr()
			p.expect(']')
			base := e
			e = func(c *vctx) any { return index(base(c), idx(c)) }
			continue
		}
		break
	}
	if formal {
		if p.peek() != '}' {
			p.fail("missing } in reference")
		}
		p.i++
	}
	return e, true
}

func (p *vparser) args() (out []vexpr) {
	p.expect('(')
	p.ws()
	if p.peek() == ')' {
		p.i++
		return nil
	}
	for {
		out = append(out, p.expr())
		p.ws()
		switch p.peek() {
		case ',':
			p.i++
		case ')':
			p.i++
			return out
		default:
			p.fail("expected , or )")
		}
	}
}

func (p *vparser) expr() vexpr { return p.binary(0) }

// levels are the binary operators from the loosest to the tightest.
var levels = [][]string{{"||"}, {"&&"}, {"==", "!="}, {"<=", ">=", "<", ">"}, {"+", "-"}, {"*", "/", "%"}}

func (p *vparser) binary(level int) vexpr {
	if level == len(levels) {
		return p.unary()
	}
	l := p.binary(level + 1)
	for {
		p.ws()
		op := ""
		for _, o := range levels[level] { // longer operators come first in a level
			if strings.HasPrefix(p.s[p.i:], o) {
				op = o
				break
			}
		}
		if op == "" {
			return l
		}
		p.i += len(op)
		r, left := p.binary(level+1), l
		switch op {
		case "||":
			l = func(c *vctx) any { return truthy(left(c)) || truthy(r(c)) }
		case "&&":
			l = func(c *vctx) any { return truthy(left(c)) && truthy(r(c)) }
		case "==":
			l = func(c *vctx) any { return equal(left(c), r(c)) }
		case "!=":
			l = func(c *vctx) any { return !equal(left(c), r(c)) }
		case "<", ">", "<=", ">=":
			l = func(c *vctx) any {
				n, ok := compare(left(c), r(c))
				return ok && (op == "<" && n < 0 || op == ">" && n > 0 || op == "<=" && n <= 0 || op == ">=" && n >= 0)
			}
		default:
			l = func(c *vctx) any { return arith(op, left(c), r(c)) }
		}
	}
}

func (p *vparser) unary() vexpr {
	p.ws()
	switch p.peek() {
	case '!':
		p.i++
		e := p.unary()
		return func(c *vctx) any { return !truthy(e(c)) }
	case '-':
		p.i++
		e := p.unary()
		return func(c *vctx) any { return arith("-", int64(0), e(c)) }
	}
	return p.primary()
}

func (p *vparser) primary() vexpr {
	p.ws()
	switch ch := p.peek(); {
	case ch == '$':
		e, ok := p.ref()
		if !ok {
			p.fail("bad reference")
		}
		return e
	case ch == '(':
		p.i++
		e := p.expr()
		p.expect(')')
		return e
	case ch == '\'':
		var b strings.Builder
		for p.i++; ; p.i++ {
			if p.i >= len(p.s) {
				p.fail("string is not closed")
			}
			if p.s[p.i] == '\'' {
				if p.i+1 < len(p.s) && p.s[p.i+1] == '\'' {
					p.i++
				} else {
					break
				}
			}
			b.WriteByte(p.s[p.i])
		}
		p.i++
		s := b.String()
		return func(*vctx) any { return s }
	case ch == '"':
		var b strings.Builder
		for p.i++; ; p.i++ {
			if p.i >= len(p.s) {
				p.fail("string is not closed")
			}
			if p.s[p.i] == '"' {
				if p.i+1 < len(p.s) && p.s[p.i+1] == '"' {
					p.i++
				} else {
					break
				}
			}
			b.WriteByte(p.s[p.i])
		}
		p.i++
		sub := &vparser{s: b.String()}
		nodes, _ := sub.nodes() // a double-quoted string is a template
		return func(c *vctx) any {
			inner := &vctx{vars: c.vars, out: &strings.Builder{}}
			runNodes(inner, nodes)
			return inner.out.String()
		}
	case ch >= '0' && ch <= '9':
		start := p.i
		for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
			p.i++
		}
		if p.i+1 < len(p.s) && p.s[p.i] == '.' && p.s[p.i+1] >= '0' && p.s[p.i+1] <= '9' {
			for p.i++; p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9'; p.i++ {
			}
			f, _ := strconv.ParseFloat(p.s[start:p.i], 64)
			return func(*vctx) any { return f }
		}
		n, err := strconv.ParseInt(p.s[start:p.i], 10, 64)
		if err != nil {
			p.fail("number is too big")
		}
		return func(*vctx) any { return n }
	case ch == '[':
		p.i++
		p.ws()
		var items []vexpr
		if p.peek() != ']' {
			first := p.expr()
			p.ws()
			if strings.HasPrefix(p.s[p.i:], "..") { // a range
				p.i += 2
				hi := p.expr()
				p.expect(']')
				return func(c *vctx) any {
					lo, _ := first(c).(int64)
					top, _ := hi(c).(int64)
					l := &vlist{items: []any{}}
					step := int64(1)
					if top < lo {
						step = -1
					}
					for n := lo; len(l.items) < maxIterations; n += step {
						l.items = append(l.items, n)
						if n == top {
							break
						}
					}
					return l
				}
			}
			items = append(items, first)
			for p.ws(); p.peek() == ','; p.ws() {
				p.i++
				items = append(items, p.expr())
			}
		}
		p.expect(']')
		return func(c *vctx) any {
			l := &vlist{items: []any{}}
			for _, e := range items {
				l.items = append(l.items, e(c))
			}
			return l
		}
	case ch == '{':
		p.i++
		var keys, vals []vexpr
		for p.ws(); p.peek() != '}'; p.ws() {
			keys = append(keys, p.expr())
			p.expect(':')
			vals = append(vals, p.expr())
			if p.ws(); p.peek() == ',' {
				p.i++
			}
		}
		p.i++
		return func(c *vctx) any {
			m := newVmap()
			for i := range keys {
				m.put(vstr(keys[i](c)), vals[i](c))
			}
			return m
		}
	case isIdentStart(ch):
		switch word := p.ident(); word {
		case "true", "false":
			return func(*vctx) any { return word == "true" }
		case "null":
			return func(*vctx) any { return nil }
		}
	}
	p.fail("unexpected character")
	return nil
}
