package gapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Rule is one REST call, written as in a google.api.http annotation:
//
//	"POST /v1/{name=projects/*/serviceAccounts/*}:disable"
//
// "*" is one path segment and "**" the rest of the path. A {variable} fills
// the request field of that (dotted, JSON) name with the segments it covers.
// It is the Mux's answer to REST paths that REST's projects/<p>/<collection>
// shape can't express: organizations, top-level collections, ids without a
// "projects/" prefix.
type Rule struct {
	HTTP   string
	Method string // gRPC method name; with Fn, the operation name shown in the request log
	Body   string // request field the JSON body fills; "" means the whole body is the request
	// Fn handles the call instead of a gRPC method: r has the body in
	// core.Body(r) and vars holds the path variables. A proto.Message is
	// answered as protojson, anything else as plain JSON.
	Fn func(r *http.Request, vars map[string]string) (any, error)
}

// Mux serves Rules by calling a service's gRPC handlers, the way REST does.
type Mux struct {
	impl  any
	rules []*rule
}

type rule struct {
	Rule
	verb, method string
	parts        []part
	vars         []group
}

// part matches one request segment: a literal, "*" (lit == "") or "**" (rest).
type part struct {
	lit  string
	rest bool
}

// group is a {variable} covering n parts from index from.
type group struct {
	name    string
	from, n int
}

// NewMux compiles the rules.
func NewMux(impl any, rules ...Rule) *Mux {
	m := &Mux{impl: impl}
	for _, r := range rules {
		method, path, _ := strings.Cut(r.HTTP, " ")
		rl := &rule{Rule: r, method: method}
		if i := strings.LastIndex(path, ":"); i > strings.LastIndex(path, "/") && i > strings.LastIndex(path, "}") {
			path, rl.verb = path[:i], path[i+1:]
		}
		for _, seg := range splitTemplate(path) {
			if !strings.HasPrefix(seg, "{") {
				rl.parts = append(rl.parts, mkPart(seg))
				continue
			}
			name, tmpl, ok := strings.Cut(strings.Trim(seg, "{}"), "=")
			if !ok {
				tmpl = "*"
			}
			segs := strings.Split(tmpl, "/")
			rl.vars = append(rl.vars, group{name, len(rl.parts), len(segs)})
			for _, s := range segs {
				rl.parts = append(rl.parts, mkPart(s))
			}
		}
		m.rules = append(m.rules, rl)
	}
	return m
}

func mkPart(s string) part {
	switch s {
	case "*":
		return part{}
	case "**":
		return part{rest: true}
	}
	return part{lit: s}
}

// splitTemplate cuts a path on "/" outside {braces}.
func splitTemplate(path string) (out []string) {
	depth, start := 0, 1
	for i := 1; i < len(path); i++ {
		switch path[i] {
		case '{':
			depth++
		case '}':
			depth--
		case '/':
			if depth == 0 {
				out, start = append(out, path[start:i]), i+1
			}
		}
	}
	return append(out, path[start:])
}

// find returns the first rule that fits the request, with its variables.
func (m *Mux) find(r *http.Request) (*rule, map[string]string) {
	seg := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	last := seg[len(seg)-1]
	verb := ""
	if i := strings.LastIndex(last, ":"); i >= 0 {
		seg[len(seg)-1], verb = last[:i], last[i+1:]
	}
	for _, rl := range m.rules {
		if rl.method != r.Method || rl.verb != verb {
			continue
		}
		if vars, ok := rl.match(seg); ok {
			return rl, vars
		}
	}
	return nil, nil
}

func (rl *rule) match(seg []string) (map[string]string, bool) {
	n := len(rl.parts)
	open := rl.parts[n-1].rest
	if len(seg) != n && !(open && len(seg) > n) {
		return nil, false
	}
	for i, p := range rl.parts {
		if seg[i] == "" || p.lit != "" && p.lit != seg[i] {
			return nil, false
		}
	}
	vars := map[string]string{}
	for _, g := range rl.vars {
		end := g.from + g.n
		if open && end == n {
			end = len(seg)
		}
		vars[g.name] = strings.Join(seg[g.from:end], "/")
	}
	return vars, true
}

// Match is for core.Info.Match.
func (m *Mux) Match(r *http.Request) bool { rl, _ := m.find(r); return rl != nil }

func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	rl, vars := m.find(r)
	if rl == nil {
		Err(w, c, status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	c.Op = rl.Method
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	var out any
	var err error
	if rl.Fn != nil {
		out, err = rl.Fn(r, vars)
	} else {
		out, err = invoke(r, m.impl, rl.Method, rl.Body, vars)
	}
	if err != nil {
		Err(w, c, err)
		return
	}
	JSON(w, out)
}

// JSON writes a 200 response: protojson for a proto message, encoding/json for the rest.
func JSON(w http.ResponseWriter, out any) {
	b, err := json.Marshal(out)
	if pm, ok := out.(proto.Message); ok {
		b, err = protojson.Marshal(pm)
	}
	if err != nil {
		b = []byte(`{}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// invoke builds a gRPC request from the JSON body, the query parameters and
// the path variables (field name -> value), and calls the method of the
// gRPC server impl with that name.
func invoke(r *http.Request, impl any, method, body string, vars map[string]string) (proto.Message, error) {
	in, err := ParseBody(core.Body(r), body)
	if err != nil {
		return nil, err
	}
	for k, v := range r.URL.Query() {
		if k == "alt" || k == "$alt" || k == "prettyPrint" || k == "key" || k == "fields" {
			continue
		}
		if len(v) == 1 {
			setPath(in, k, v[0])
		} else {
			setPath(in, k, v)
		}
	}
	for k, v := range vars {
		if k != "" {
			setPath(in, k, v)
		}
	}
	fn := reflect.ValueOf(impl).MethodByName(method)
	if !fn.IsValid() || fn.Type().NumIn() != 2 || fn.Type().NumOut() != 2 {
		return nil, status.Errorf(codes.Unimplemented, "%s is not implemented", method)
	}
	req := reflect.New(fn.Type().In(1).Elem())
	if err := Decode(req.Interface().(proto.Message), in); err != nil {
		return nil, err
	}
	out := fn.Call([]reflect.Value{reflect.ValueOf(r.Context()), req})
	if err, _ := out[1].Interface().(error); err != nil {
		return nil, err
	}
	return out[0].Interface().(proto.Message), nil
}

// ParseBody reads a JSON request body into the map that Decode turns into a
// request. field is the request member the body fills ("" means the body is
// the request). A body of null is an empty message.
func ParseBody(b []byte, field string) (map[string]any, error) {
	in := map[string]any{}
	if b = bytes.TrimSpace(b); len(b) == 0 {
		return in, nil
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return nil, Invalid("Invalid JSON payload received. %v", err)
	}
	obj, isObj := v.(map[string]any)
	switch {
	case v == nil:
		if field != "" {
			in[field] = map[string]any{}
		}
	case field != "":
		in[field] = v
	case isObj:
		in = obj
	default:
		return nil, Invalid("Invalid JSON payload received: want an object")
	}
	return in, nil
}

// Decode fills a request message from the JSON members in. It refuses an
// unknown enum name and a negative page_size, as Google does. Unknown fields
// are dropped.
func Decode(msg proto.Message, in map[string]any) error {
	md := msg.ProtoReflect().Descriptor()
	coerce(md, in)
	if err := checkEnums(md, in, ""); err != nil {
		return err
	}
	raw, _ := json.Marshal(in)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, msg); err != nil {
		return Invalid("Invalid JSON payload received. %v", err)
	}
	if fd := md.Fields().ByName("page_size"); fd != nil && fd.Kind() == protoreflect.Int32Kind {
		if n := msg.ProtoReflect().Get(fd).Int(); n < 0 { // Page cannot refuse: it has no error to return
			return Invalid("Invalid value at 'page_size' (TYPE_INT32), %d", n)
		}
	}
	return nil
}

// checkEnums refuses an enum name the field does not have. protojson with
// DiscardUnknown would drop it without a word; Google answers 400.
func checkEnums(md protoreflect.MessageDescriptor, in map[string]any, path string) error {
	for k, v := range in {
		fd := md.Fields().ByJSONName(k)
		if fd == nil {
			fd = md.Fields().ByName(protoreflect.Name(k))
		}
		if fd == nil || fd.IsMap() {
			continue
		}
		at := path + string(fd.Name())
		elems := []any{v}
		if list, ok := v.([]any); ok && fd.IsList() {
			elems = list
		}
		for i, e := range elems {
			if fd.IsList() {
				at = path + string(fd.Name()) + "[" + strconv.Itoa(i) + "]"
			}
			switch x := e.(type) {
			case string:
				if fd.Kind() == protoreflect.EnumKind && x != "" && fd.Enum().Values().ByName(protoreflect.Name(x)) == nil &&
					fd.Enum().FullName() != "google.protobuf.NullValue" {
					return Invalid("Invalid value at '%s' (type.googleapis.com/%s), %q", at, fd.Enum().FullName(), x)
				}
			case map[string]any:
				if fd.Message() != nil && !strings.HasPrefix(string(fd.Message().FullName()), "google.protobuf.") {
					if err := checkEnums(fd.Message(), x, at+"."); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// coerce makes query-string values fit their fields: a lone value for a
// repeated field becomes a list, and "true" a bool.
func coerce(md protoreflect.MessageDescriptor, in map[string]any) {
	for k, v := range in {
		fd := md.Fields().ByJSONName(k)
		if fd == nil {
			fd = md.Fields().ByName(protoreflect.Name(k))
		}
		if fd == nil || fd.IsMap() {
			continue
		}
		if fd.IsList() {
			list, ok := v.([]any)
			if !ok {
				if ss, isStrs := v.([]string); isStrs {
					for _, s := range ss {
						list = append(list, s)
					}
				} else {
					list = []any{v}
				}
				in[k] = list
			}
			for _, e := range list {
				if em, ok := e.(map[string]any); ok && fd.Message() != nil {
					coerce(fd.Message(), em)
				}
			}
			continue
		}
		switch x := v.(type) {
		case string:
			if b, err := strconv.ParseBool(x); err == nil && fd.Kind() == protoreflect.BoolKind {
				in[k] = b
			}
		case map[string]any:
			if fd.Message() != nil {
				coerce(fd.Message(), x)
			}
		}
	}
}
