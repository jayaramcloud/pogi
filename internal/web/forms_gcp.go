package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Google Cloud forms come from the protobuf descriptors of the service's gRPC API: every Create RPC
// that has a REST binding (google.api.http) is a form, its fields are the request message's, and a
// submit sends the REST call the binding describes to the service in-process.

// gcpMethod is a Create RPC and its REST binding.
type gcpMethod struct {
	Name             string // "Publisher.CreateTopic"
	md               protoreflect.MethodDescriptor
	verb, tmpl, body string
}

func (ui *UI) gcpMethods(svc, prefix string) []gcpMethod {
	s := ui.hub.Service(svc)
	if s == nil || s.Info().Cloud != core.GCP {
		return nil
	}
	var out []gcpMethod
	for _, t := range s.Info().Targets {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(t))
		sd, ok := d.(protoreflect.ServiceDescriptor)
		if err != nil || !ok {
			continue
		}
		for i := 0; i < sd.Methods().Len(); i++ {
			md := sd.Methods().Get(i)
			if !strings.HasPrefix(string(md.Name()), prefix) || md.IsStreamingClient() || md.IsStreamingServer() {
				continue
			}
			rule, _ := proto.GetExtension(md.Options(), annotations.E_Http).(*annotations.HttpRule)
			m := gcpMethod{Name: string(sd.Name()) + "." + string(md.Name()), md: md, body: rule.GetBody()}
			switch p := rule.GetPattern().(type) {
			case *annotations.HttpRule_Post:
				m.verb, m.tmpl = http.MethodPost, p.Post
			case *annotations.HttpRule_Put:
				m.verb, m.tmpl = http.MethodPut, p.Put
			case *annotations.HttpRule_Delete:
				m.verb, m.tmpl = http.MethodDelete, p.Delete
			default:
				continue
			}
			out = append(out, m)
		}
	}
	return out
}

func (ui *UI) gcpOps(svc string) []formOp {
	var out []formOp
	for _, m := range ui.gcpMethods(svc, "Create") {
		out = append(out, formOp{m.Name, core.Label(string(m.md.Name()))})
	}
	return out
}

func (ui *UI) gcpMethod(svc, op string) *gcpMethod {
	for _, m := range ui.gcpMethods(svc, "Create") {
		if m.Name == op {
			return &m
		}
	}
	return nil
}

var pathVar = regexp.MustCompile(`\{([^=}]+)(?:=([^}]*))?\}`)

func (ui *UI) gcpForm(svc, op string, sc scope) *core.Form {
	m := ui.gcpMethod(svc, op)
	if m == nil {
		return nil
	}
	f := &core.Form{Op: m.Name, Title: core.Label(string(m.md.Name()))}
	f.Fields = gcpFields(m.md.Input(), 3)
	for _, v := range pathVar.FindAllStringSubmatch(m.tmpl, -1) {
		mark(f.Fields, strings.Split(v[1], "."), prefill(v[2], sc.project()))
	}
	return f
}

// mark makes the field at a dotted name required, with a default.
func mark(fields []core.Field, name []string, def string) {
	for i := range fields {
		if fields[i].Name != name[0] {
			continue
		}
		if len(name) > 1 {
			fields[i].Required = true
			mark(fields[i].Fields, name[1:], def)
			return
		}
		fields[i].Required, fields[i].Default = true, def
	}
}

// prefill is a resource name pattern up to where the user must say something:
// projects/*/topics/* is projects/<project>/topics/.
func prefill(pattern, project string) string {
	var out []string
	for _, seg := range strings.Split(pattern, "/") {
		switch {
		case seg == "*" && len(out) > 0 && out[len(out)-1] == "projects":
			seg = project
		case seg == "*" || seg == "**":
			return strings.Join(out, "/") + "/"
		}
		out = append(out, seg)
	}
	return strings.Join(out, "/")
}

func gcpFields(md protoreflect.MessageDescriptor, depth int) []core.Field {
	var req, opt []core.Field
	for i := 0; i < md.Fields().Len(); i++ {
		fd := md.Fields().Get(i)
		var beh []annotations.FieldBehavior
		beh, _ = proto.GetExtension(fd.Options(), annotations.E_FieldBehavior).([]annotations.FieldBehavior)
		if has(beh, annotations.FieldBehavior_OUTPUT_ONLY) {
			continue
		}
		f := gcpField(fd, depth)
		f.Required = has(beh, annotations.FieldBehavior_REQUIRED)
		if f.Required {
			req = append(req, f)
		} else {
			opt = append(opt, f)
		}
	}
	return append(req, opt...)
}

func has(l []annotations.FieldBehavior, b annotations.FieldBehavior) bool {
	for _, x := range l {
		if x == b {
			return true
		}
	}
	return false
}

func gcpField(fd protoreflect.FieldDescriptor, depth int) core.Field {
	name := fd.JSONName()
	f := core.Field{Name: name, Label: core.Label(name)}
	switch {
	case fd.IsMap():
		e := gcpValue(fd.MapValue(), depth-1)
		f.Kind, f.Elem = "map", &e
		if depth == 0 && e.Kind != "string" {
			f.Kind, f.Elem = "json", nil
		}
	case fd.IsList():
		e := gcpValue(fd, depth-1)
		f.Kind, f.Elem = "list", &e
		if depth == 0 && e.Kind != "string" {
			f.Kind, f.Elem = "json", nil
		}
	default:
		v := gcpValue(fd, depth)
		v.Name, v.Label = f.Name, f.Label
		return v
	}
	return f
}

// gcpValue is the field's value, a single element when it is repeated.
func gcpValue(fd protoreflect.FieldDescriptor, depth int) core.Field {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return core.Field{Kind: "bool"}
	case protoreflect.EnumKind:
		var vals []string
		for i := 0; i < fd.Enum().Values().Len(); i++ {
			vals = append(vals, string(fd.Enum().Values().Get(i).Name()))
		}
		return core.Field{Kind: "enum", Enum: vals}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return core.Field{Kind: "number"}
	case protoreflect.StringKind, protoreflect.BytesKind: // bytes are base64 in JSON
		return core.Field{Kind: "string"}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		md := fd.Message()
		switch md.FullName() {
		case "google.protobuf.Timestamp", "google.protobuf.Duration", "google.protobuf.FieldMask", "google.protobuf.StringValue", "google.protobuf.BytesValue":
			return core.Field{Kind: "string"}
		case "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue", "google.protobuf.Any":
			return core.Field{Kind: "json"}
		case "google.protobuf.BoolValue":
			return core.Field{Kind: "bool"}
		case "google.protobuf.DoubleValue", "google.protobuf.FloatValue":
			return core.Field{Kind: "number"}
		case "google.protobuf.Int32Value", "google.protobuf.Int64Value", "google.protobuf.UInt32Value", "google.protobuf.UInt64Value":
			return core.Field{Kind: "integer"}
		}
		if depth <= 0 {
			return core.Field{Kind: "json"} // ponytail: deeper messages are a JSON box
		}
		return core.Field{Kind: "struct", Fields: gcpFields(md, depth-1)}
	}
	return core.Field{Kind: "integer"} // the 32 and 64 bit integers
}

// gcpSubmit sends the call the RPC's REST binding describes: the path variables are taken out of the
// form, the rest goes in the body (or the query string, for fields the binding leaves out of it).
func (ui *UI) gcpSubmit(svc, op string, in map[string]any) error {
	verb, uri, body, err := ui.gcpCall(svc, op, in)
	if err != nil {
		return err
	}
	req, _ := http.NewRequest(verb, "http://localhost"+uri, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	status, resp := ui.hub.DoService(svc, req)
	if status >= 300 {
		return gcpError(status, resp)
	}
	return nil
}

// gcpCall is the REST call of a form: the verb, the path with its query string, and the JSON body (none for "").
func (ui *UI) gcpCall(svc, op string, in map[string]any) (verb, uri string, body []byte, err error) {
	m := ui.gcpMethod(svc, op)
	if m == nil {
		return "", "", nil, fmt.Errorf("%s has no form %s", svc, op)
	}
	var missing string
	path := pathVar.ReplaceAllStringFunc(m.tmpl, func(v string) string {
		name := pathVar.FindStringSubmatch(v)[1]
		val, ok := take(in, strings.Split(name, "."))
		if !ok {
			missing = name
		}
		return val
	})
	if missing != "" {
		return "", "", nil, fmt.Errorf("%s is required", missing)
	}
	var payload any = in
	q := url.Values{}
	if m.body != "*" {
		payload = in[m.body]
		for k, v := range in {
			if k != m.body {
				q.Set(k, fmt.Sprint(v))
			}
		}
	}
	if payload != nil {
		body, _ = json.Marshal(payload)
	}
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	return m.verb, path, body, nil
}

// gcpDelete is the Delete RPC of the resource a table lists: "Topics" are deleted by DeleteTopic.
func (ui *UI) gcpDelete(svc, table string) *gcpMethod {
	one := strings.ToLower(strings.ReplaceAll(table, " ", ""))
	if strings.HasSuffix(one, "ies") {
		one = one[:len(one)-3] + "y"
	} else {
		one = strings.TrimSuffix(one, "s")
	}
	for _, m := range ui.gcpMethods(svc, "Delete") {
		if m.verb == http.MethodDelete && strings.EqualFold(string(m.md.Name()), "Delete"+one) {
			return &m
		}
	}
	return nil
}

// gcpBulkDelete deletes the resource whose full name is name (the first cell of its row).
func (ui *UI) gcpBulkDelete(svc, table, name string) error {
	m := ui.gcpDelete(svc, table)
	if m == nil {
		return fmt.Errorf("%s %s cannot be deleted in bulk", svc, table)
	}
	req, _ := http.NewRequest(http.MethodDelete, "http://localhost"+pathVar.ReplaceAllLiteralString(m.tmpl, name), nil)
	if status, resp := ui.hub.DoService(svc, req); status >= 300 {
		return gcpError(status, resp)
	}
	return nil
}

// take removes the string at a dotted name of a JSON object.
func take(in map[string]any, name []string) (string, bool) {
	v, ok := in[name[0]]
	if len(name) > 1 {
		child, _ := v.(map[string]any)
		if child == nil {
			return "", false
		}
		return take(child, name[1:])
	}
	delete(in, name[0])
	s, _ := v.(string)
	return s, ok && s != ""
}

// gcpError is Google's {"error": {"status", "message"}} as a console error.
func gcpError(status int, body []byte) error {
	var e struct {
		Error struct{ Status, Message string }
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return &core.Error{Status: status, Code: e.Error.Status, Msg: e.Error.Message}
	}
	return fmt.Errorf("%d %s", status, bytes.TrimSpace(body))
}
