package generic

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Wire formats.
const (
	wJSON = iota
	wQuery
	wEC2
	wRestJSON
	wRestXML
	wCBOR // Smithy RPCv2 CBOR, which the Go SDK speaks to the services that moved to it
)

func (m *model) wireOf(r *http.Request) int {
	if strings.HasPrefix(r.Header.Get("Smithy-Protocol"), "rpc-v2-cbor") {
		return wCBOR
	}
	if r.Header.Get("X-Amz-Target") != "" {
		return wJSON // JSON also reaches Query services in-process (Hub.Call)
	}
	switch m.meta.Protocol {
	case "query":
		return wQuery
	case "ec2":
		return wEC2
	case "rest-json":
		return wRestJSON
	case "rest-xml":
		return wRestXML
	}
	return wJSON
}

func badRequest(code, format string, a ...any) *core.Error {
	return core.Errorf(http.StatusBadRequest, code, format, a...)
}

// ---- time and scalar helpers

func normTime(v any) (string, bool) {
	var t time.Time
	switch x := v.(type) {
	case float64:
		t = time.UnixMilli(int64(x * 1000))
	case string:
		var err error
		if t, err = time.Parse(time.RFC3339Nano, x); err != nil {
			if t, err = time.Parse(http.TimeFormat, x); err != nil {
				f, err := strconv.ParseFloat(x, 64)
				if err != nil {
					return "", false
				}
				t = time.UnixMilli(int64(f * 1000))
			}
		}
	default:
		return "", false
	}
	return t.UTC().Format(time.RFC3339Nano), true
}

// scalar converts a string from a URI, query string or header to the value of shape sh.
func scalar(sh *shape, s string) any {
	switch sh.Type {
	case "integer", "long", "float", "double":
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	case "boolean":
		return s == "true"
	case "timestamp":
		if t, ok := normTime(s); ok {
			return t
		}
	}
	return s
}

func tsFormat(r *ref, sh *shape, def string) string {
	if r != nil && r.TimestampFormat != "" {
		return r.TimestampFormat
	}
	if sh != nil && sh.TimestampFormat != "" {
		return sh.TimestampFormat
	}
	return def
}

// fmtTime renders a stored timestamp (RFC 3339) in a wire format.
func fmtTime(v any, format string) any {
	s, _ := v.(string)
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return v
	}
	switch format {
	case "unixTimestamp":
		return core.Unix(t)
	case "rfc822":
		return t.UTC().Format(http.TimeFormat)
	}
	return t.UTC().Format(core.ISO)
}

func str(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return fmt.Sprint(v)
}

func locName(name string, r *ref) string {
	if r != nil && r.LocationName != "" {
		return r.LocationName
	}
	return name
}

// ---- JSON

// fromJSON converts decoded JSON into member-named values.
func fromJSON(sh *shape, v any) any {
	if sh.Document {
		return v
	}
	switch sh.Type {
	case "structure":
		mm, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		out := map[string]any{}
		for n, mr := range sh.Members {
			x, ok := mm[locName(n, mr)]
			if !ok {
				if x, ok = mm[n]; !ok {
					continue
				}
			}
			if x == nil {
				continue
			}
			if c := fromJSON(mr.sh, x); c != nil {
				out[n] = c
			}
		}
		return out
	case "list":
		l, ok := v.([]any)
		if !ok {
			return nil
		}
		out := make([]any, 0, len(l))
		for _, x := range l {
			if c := fromJSON(sh.Member.sh, x); c != nil {
				out = append(out, c)
			}
		}
		return out
	case "map":
		mm, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		out := map[string]any{}
		for k, x := range mm {
			if c := fromJSON(sh.Value.sh, x); c != nil {
				out[k] = c
			}
		}
		return out
	case "timestamp":
		if t, ok := normTime(v); ok {
			return t
		}
		return nil
	case "integer", "long", "float", "double":
		if s, ok := v.(string); ok {
			return scalar(sh, s)
		}
	}
	return v
}

// toJSON renders member-named values for a JSON body; loc says whether
// locationName renames members (it does for the json protocols).
func toJSON(r *ref, sh *shape, v any, loc, rest bool) any {
	if sh == nil || sh.Document {
		return v
	}
	switch sh.Type {
	case "structure":
		mm, _ := v.(map[string]any)
		out := map[string]any{}
		for n, x := range mm {
			mr := sh.Members[n]
			if mr == nil || x == nil || (rest && mr.Location != "") {
				continue
			}
			key := n
			if loc {
				key = locName(n, mr)
			}
			out[key] = toJSON(mr, mr.sh, x, loc, rest)
		}
		return out
	case "list":
		l, _ := v.([]any)
		out := make([]any, 0, len(l))
		for _, x := range l {
			out = append(out, toJSON(sh.Member, sh.Member.sh, x, loc, rest))
		}
		return out
	case "map":
		mm, _ := v.(map[string]any)
		out := map[string]any{}
		for k, x := range mm {
			out[k] = toJSON(sh.Value, sh.Value.sh, x, loc, rest)
		}
		return out
	case "timestamp":
		return fmtTime(v, tsFormat(r, sh, "unixTimestamp"))
	case "integer", "long":
		if f, ok := v.(float64); ok {
			return int64(f)
		}
	}
	return v
}

// ---- query

type qn struct {
	val  string
	kids map[string]*qn
}

func buildQ(form url.Values) *qn {
	root := &qn{}
	for k, vs := range form {
		if k == "Action" || k == "Version" || len(vs) == 0 {
			continue
		}
		n := root
		for _, p := range strings.Split(k, ".") {
			if n.kids == nil {
				n.kids = map[string]*qn{}
			}
			c := n.kids[p]
			if c == nil {
				c = &qn{}
				n.kids[p] = c
			}
			n = c
		}
		n.val = vs[0]
	}
	return root
}

// indexed returns the children of n named 1, 2, 3… in order.
func (n *qn) indexed() []*qn {
	var idx []int
	for k := range n.kids {
		if i, err := strconv.Atoi(k); err == nil {
			idx = append(idx, i)
		}
	}
	sort.Ints(idx)
	out := make([]*qn, len(idx))
	for i, x := range idx {
		out[i] = n.kids[strconv.Itoa(x)]
	}
	return out
}

func queryKey(name string, mr *ref, ec2 bool) string {
	if ec2 {
		k := mr.QueryName
		if k == "" {
			k = locName(name, mr)
		}
		return strings.ToUpper(k[:1]) + k[1:]
	}
	return locName(name, mr)
}

func (n *qn) decode(sh *shape, r *ref, ec2 bool) any {
	switch sh.Type {
	case "structure":
		out := map[string]any{}
		for name, mr := range sh.Members {
			k := n.kids[queryKey(name, mr, ec2)]
			if k == nil {
				continue
			}
			if v := k.decode(mr.sh, mr, ec2); v != nil {
				out[name] = v
			}
		}
		if len(out) == 0 && n.val == "" && len(n.kids) == 0 {
			return nil
		}
		return out
	case "list":
		src := n
		flat := ec2 || (r != nil && r.Flattened) || sh.Flattened
		if !flat {
			if w := n.kids[locName("member", sh.Member)]; w != nil {
				src = w
			} else if w := n.kids["member"]; w != nil {
				src = w
			}
		}
		items := src.indexed()
		if len(items) == 0 {
			return nil
		}
		out := make([]any, 0, len(items))
		for _, it := range items {
			if v := it.decode(sh.Member.sh, sh.Member, ec2); v != nil {
				out = append(out, v)
			}
		}
		return out
	case "map":
		src := n
		if !((r != nil && r.Flattened) || sh.Flattened) {
			if w := n.kids["entry"]; w != nil {
				src = w
			}
		}
		out := map[string]any{}
		for _, e := range src.indexed() {
			k := e.kids[locName("key", sh.Key)]
			if k == nil {
				continue
			}
			var v any = ""
			if ve := e.kids[locName("value", sh.Value)]; ve != nil {
				v = ve.decode(sh.Value.sh, sh.Value, ec2)
			}
			out[k.val] = v
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return scalar(sh, n.val)
}

// ---- XML

type xn struct {
	name  string
	text  string
	attrs map[string]string
	kids  []*xn
}

func parseXML(b []byte) (*xn, error) {
	d := xml.NewDecoder(bytes.NewReader(b))
	var stack []*xn
	var root *xn
	for {
		t, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := t.(type) {
		case xml.StartElement:
			n := &xn{name: t.Name.Local, attrs: map[string]string{}}
			for _, a := range t.Attr {
				n.attrs[a.Name.Local] = a.Value
			}
			if len(stack) > 0 {
				p := stack[len(stack)-1]
				p.kids = append(p.kids, n)
			} else {
				root = n
			}
			stack = append(stack, n)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				stack[len(stack)-1].text += string(t)
			}
		}
	}
	if root == nil {
		return nil, fmt.Errorf("empty XML document")
	}
	return root, nil
}

func (n *xn) child(name string) *xn {
	for _, k := range n.kids {
		if k.name == name {
			return k
		}
	}
	return nil
}

func (n *xn) children(name string) []*xn {
	var out []*xn
	for _, k := range n.kids {
		if k.name == name {
			out = append(out, k)
		}
	}
	return out
}

// fromXML converts the element of one value; skipLocated leaves out members bound to headers, labels and so on.
func fromXML(sh *shape, n *xn, skipLocated bool) any {
	switch sh.Type {
	case "structure":
		out := map[string]any{}
		for name, mr := range sh.Members {
			if skipLocated && mr.Location != "" {
				continue
			}
			ln := locName(name, mr)
			if mr.XMLAttribute {
				if a, ok := n.attrs[ln]; ok {
					out[name] = scalar(mr.sh, a)
				}
				continue
			}
			ms := mr.sh
			flat := mr.Flattened || ms.Flattened
			switch ms.Type {
			case "list":
				var els []*xn
				if flat {
					els = n.children(ln)
				} else if w := n.child(ln); w != nil {
					els = w.kids
				}
				if len(els) > 0 {
					l := make([]any, 0, len(els))
					for _, e := range els {
						l = append(l, fromXML(ms.Member.sh, e, false))
					}
					out[name] = l
				}
			case "map":
				var els []*xn
				if flat {
					els = n.children(ln)
				} else if w := n.child(ln); w != nil {
					els = w.kids
				}
				if len(els) > 0 {
					mp := map[string]any{}
					for _, e := range els {
						if k := e.child(locName("key", ms.Key)); k != nil {
							var v any = ""
							if ve := e.child(locName("value", ms.Value)); ve != nil {
								v = fromXML(ms.Value.sh, ve, false)
							}
							mp[k.text] = v
						}
					}
					out[name] = mp
				}
			default:
				if c := n.child(ln); c != nil {
					out[name] = fromXML(ms, c, false)
				}
			}
		}
		return out
	}
	return scalar(sh, n.text)
}

var xmlEsc = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")

// writeXML renders one value as element name. Lists inside Query and EC2
// responses are wrapped unless flattened; EC2 members are <item>.
func writeXML(b *strings.Builder, name string, r *ref, sh *shape, v any, ec2 bool) {
	if v == nil || sh == nil {
		return
	}
	switch sh.Type {
	case "structure":
		mm, _ := v.(map[string]any)
		b.WriteString("<" + name)
		if ns := nsOf(r, sh); ns != "" {
			b.WriteString(` xmlns="` + ns + `"`)
		}
		for _, n := range sh.names {
			if mr := sh.Members[n]; mr.XMLAttribute && mm[n] != nil {
				b.WriteString(" " + locName(n, mr) + `="` + xmlEsc.Replace(str(mm[n])) + `"`)
			}
		}
		b.WriteString(">")
		for _, n := range sh.names {
			mr := sh.Members[n]
			if mr.XMLAttribute || mr.Location != "" && mr.Location != "body" {
				continue
			}
			writeXML(b, locName(n, mr), mr, mr.sh, mm[n], ec2)
		}
		b.WriteString("</" + name + ">")
	case "list":
		l, _ := v.([]any)
		flat := (r != nil && r.Flattened) || sh.Flattened
		item := locName("member", sh.Member)
		if ec2 && sh.Member.LocationName == "" {
			item = "item"
		}
		if flat {
			for _, x := range l {
				writeXML(b, name, sh.Member, sh.Member.sh, x, ec2)
			}
			return
		}
		b.WriteString("<" + name + ">")
		for _, x := range l {
			writeXML(b, item, sh.Member, sh.Member.sh, x, ec2)
		}
		b.WriteString("</" + name + ">")
	case "map":
		mm, _ := v.(map[string]any)
		flat := (r != nil && r.Flattened) || sh.Flattened
		if !flat {
			b.WriteString("<" + name + ">")
		}
		for _, k := range core.SortedKeys(mm) {
			if flat {
				b.WriteString("<" + name + ">")
			} else {
				b.WriteString("<entry>")
			}
			b.WriteString("<" + locName("key", sh.Key) + ">" + xmlEsc.Replace(k) + "</" + locName("key", sh.Key) + ">")
			writeXML(b, locName("value", sh.Value), sh.Value, sh.Value.sh, mm[k], ec2)
			if flat {
				b.WriteString("</" + name + ">")
			} else {
				b.WriteString("</entry>")
			}
		}
		if !flat {
			b.WriteString("</" + name + ">")
		}
	default:
		var s string
		switch sh.Type {
		case "timestamp":
			s = str(fmtTime(v, tsFormat(r, sh, "iso8601")))
		case "integer", "long":
			if f, ok := v.(float64); ok {
				s = strconv.FormatInt(int64(f), 10)
			} else {
				s = str(v)
			}
		default:
			s = str(v)
		}
		b.WriteString("<" + name + ">" + xmlEsc.Replace(s) + "</" + name + ">")
	}
}

func nsOf(r *ref, sh *shape) string {
	if r != nil && r.XMLNamespace != nil {
		return r.XMLNamespace.URI
	}
	if sh != nil && sh.XMLNamespace != nil {
		return sh.XMLNamespace.URI
	}
	return ""
}

// ---- requests

// parse finds the operation of a request and decodes its input into member-named values.
func (m *model) parse(r *http.Request, body []byte) (*operation, map[string]any, int, error) {
	w := m.wireOf(r)
	switch w {
	case wJSON:
		name := core.TargetOp(r.Header.Get("X-Amz-Target"))
		op := m.ops[name]
		if op == nil {
			return nil, nil, w, badRequest("UnknownOperationException", "pogi does not implement %s.%s", m.meta.Name, name)
		}
		in := map[string]any{}
		if len(bytes.TrimSpace(body)) > 0 && op.in != nil {
			var raw any
			if err := json.Unmarshal(body, &raw); err != nil {
				return op, nil, w, badRequest("SerializationException", "invalid JSON: %v", err)
			}
			if v, ok := fromJSON(op.in, raw).(map[string]any); ok {
				in = v
			}
		}
		return op, in, w, nil
	case wCBOR: // POST /service/{Service}/operation/{Op}
		op := m.ops[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]
		if op == nil {
			return nil, nil, w, badRequest("UnknownOperationException", "pogi does not implement %s", r.URL.Path)
		}
		in := map[string]any{}
		if len(body) > 0 && op.in != nil {
			var raw any
			if err := core.DecodeCBOR(body, &raw); err != nil {
				return op, nil, w, badRequest("SerializationException", "invalid CBOR: %v", err)
			}
			if v, ok := fromJSON(op.in, raw).(map[string]any); ok {
				in = v
			}
		}
		return op, in, w, nil
	case wQuery, wEC2:
		form, _ := url.ParseQuery(string(body))
		for k, v := range r.URL.Query() {
			if form.Get(k) == "" {
				form[k] = v
			}
		}
		op := m.ops[form.Get("Action")]
		if op == nil {
			return nil, nil, w, badRequest("InvalidAction", "pogi does not implement %s.%s", m.meta.Name, form.Get("Action"))
		}
		in := map[string]any{}
		if op.in != nil {
			if v, ok := buildQ(form).decode(op.in, nil, w == wEC2).(map[string]any); ok {
				in = v
			}
		}
		return op, in, w, nil
	}
	op, labels := m.match(r)
	if op == nil {
		return nil, nil, w, core.Errorf(http.StatusNotFound, "UnknownOperationException", "pogi does not implement %s %s", r.Method, r.URL.Path)
	}
	in, err := m.restInput(op, labels, r, body, w == wRestXML)
	return op, in, w, err
}

func (m *model) restInput(op *operation, labels map[string]string, r *http.Request, body []byte, isXML bool) (map[string]any, error) {
	in := map[string]any{}
	sh := op.in
	if sh == nil {
		return in, nil
	}
	q := r.URL.Query()
	named := map[string]bool{}
	for _, n := range sh.names {
		if mr := sh.Members[n]; mr.Location == "querystring" && mr.sh.Type != "map" {
			named[locName(n, mr)] = true
		}
	}
	for _, n := range sh.names {
		mr := sh.Members[n]
		ln := locName(n, mr)
		switch mr.Location {
		case "uri":
			if v, ok := labels[ln]; ok {
				in[n] = scalar(mr.sh, v)
			}
		case "querystring":
			switch {
			case mr.sh.Type == "map":
				mp := map[string]any{}
				for k, vs := range q {
					if !named[k] && len(vs) > 0 {
						if mr.sh.Value.sh.Type == "list" {
							l := make([]any, len(vs))
							for i, v := range vs {
								l[i] = v
							}
							mp[k] = l
						} else {
							mp[k] = vs[0]
						}
					}
				}
				if len(mp) > 0 {
					in[n] = mp
				}
			case mr.sh.Type == "list":
				if vs := q[ln]; len(vs) > 0 {
					l := make([]any, len(vs))
					for i, v := range vs {
						l[i] = scalar(mr.sh.Member.sh, v)
					}
					in[n] = l
				}
			case q.Has(ln):
				in[n] = scalar(mr.sh, q.Get(ln))
			}
		case "header":
			if vs := r.Header.Values(ln); len(vs) > 0 { // an empty header is a value, so the limits check sees it
				v := vs[0]
				switch {
				case mr.sh.Type != "list":
					in[n] = scalar(mr.sh, v)
				case v != "":
					var l []any
					for p := range strings.SplitSeq(v, ",") {
						l = append(l, scalar(mr.sh.Member.sh, strings.TrimSpace(p)))
					}
					in[n] = l
				}
			}
		case "headers":
			mp := map[string]any{}
			for k, vs := range r.Header {
				if len(k) >= len(ln) && strings.EqualFold(k[:len(ln)], ln) && len(vs) > 0 {
					mp[k[len(ln):]] = vs[0]
				}
			}
			if len(mp) > 0 {
				in[n] = mp
			}
		}
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return in, nil
	}
	if sh.Payload != "" {
		pm := sh.Members[sh.Payload]
		if pm == nil {
			return in, nil
		}
		switch pm.sh.Type {
		case "blob":
			in[sh.Payload] = base64.StdEncoding.EncodeToString(body)
		case "string":
			in[sh.Payload] = string(body)
		default:
			if isXML {
				root, err := parseXML(body)
				if err != nil {
					return nil, badRequest("MalformedXML", "%v", err)
				}
				in[sh.Payload] = fromXML(pm.sh, root, false)
			} else {
				var raw any
				if err := json.Unmarshal(body, &raw); err != nil {
					return nil, badRequest("SerializationException", "invalid JSON: %v", err)
				}
				if v := fromJSON(pm.sh, raw); v != nil {
					in[sh.Payload] = v
				}
			}
		}
		return in, nil
	}
	if isXML {
		root, err := parseXML(body)
		if err != nil {
			return nil, badRequest("MalformedXML", "%v", err)
		}
		if v, ok := fromXML(sh, root, true).(map[string]any); ok {
			for k, x := range v {
				in[k] = x
			}
		}
		return in, nil
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, badRequest("SerializationException", "invalid JSON: %v", err)
	}
	for _, n := range sh.names {
		mr := sh.Members[n]
		if mr.Location != "" {
			continue
		}
		x, ok := raw[locName(n, mr)]
		if !ok || x == nil {
			continue
		}
		if v := fromJSON(mr.sh, x); v != nil {
			in[n] = v
		}
	}
	return in, nil
}

// ---- responses

func (s *Service) respond(w http.ResponseWriter, c *core.Ctx, m *model, wire int, op *operation, out map[string]any) {
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	switch wire {
	case wJSON:
		var body any = map[string]any{}
		if op.out != nil {
			body = toJSON(op.Output, op.out, out, m.meta.Protocol == "json" || m.meta.Protocol == "rest-json", false)
		}
		core.WriteJSON(w, c, m.jsonVersion(), 200, body)
	case wCBOR:
		var body any = map[string]any{}
		if op.out != nil {
			body = toCBOR(op.out, out)
		}
		cborReply(w, 200, body)
	case wQuery, wEC2:
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
		ec2 := wire == wEC2
		if ec2 {
			b.WriteString("<" + op.Name + `Response xmlns="` + m.xmlns + `"><requestId>` + c.RequestID + "</requestId>")
			if op.out != nil {
				for _, n := range op.out.names {
					mr := op.out.Members[n]
					writeXML(&b, locName(n, mr), mr, mr.sh, out[n], true)
				}
			}
			b.WriteString("</" + op.Name + "Response>")
		} else {
			wrap := op.Name + "Result"
			if op.Output != nil && op.Output.ResultWrapper != "" {
				wrap = op.Output.ResultWrapper
			}
			b.WriteString("<" + op.Name + `Response xmlns="` + m.xmlns + `"><` + wrap + ">")
			if op.out != nil {
				for _, n := range op.out.names {
					mr := op.out.Members[n]
					writeXML(&b, locName(n, mr), mr, mr.sh, out[n], false)
				}
			}
			b.WriteString("</" + wrap + "><ResponseMetadata><RequestId>" + c.RequestID + "</RequestId></ResponseMetadata></" + op.Name + "Response>")
		}
		w.Header().Set("Content-Type", "text/xml")
		w.Write([]byte(b.String()))
	default:
		s.respondREST(w, m, wire, op, out)
	}
}

func cborReply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	w.Header().Set("Content-Type", "application/cbor")
	w.WriteHeader(status)
	w.Write(core.EncodeCBOR(body))
}

// toCBOR renders member-named values for a CBOR body: timestamps are tag-1
// epoch times, blobs byte strings.
func toCBOR(sh *shape, v any) any {
	if sh == nil || sh.Document {
		return v
	}
	switch sh.Type {
	case "structure":
		mm, _ := v.(map[string]any)
		out := map[string]any{}
		for n, x := range mm {
			if mr := sh.Members[n]; mr != nil && x != nil {
				out[n] = toCBOR(mr.sh, x)
			}
		}
		return out
	case "list":
		l, _ := v.([]any)
		out := make([]any, 0, len(l))
		for _, x := range l {
			out = append(out, toCBOR(sh.Member.sh, x))
		}
		return out
	case "map":
		mm, _ := v.(map[string]any)
		out := map[string]any{}
		for k, x := range mm {
			out[k] = toCBOR(sh.Value.sh, x)
		}
		return out
	case "timestamp":
		if s, _ := v.(string); s != "" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return t
			}
		}
	case "blob":
		if b, err := base64.StdEncoding.DecodeString(str(v)); err == nil {
			return b
		}
	case "integer", "long":
		if f, ok := v.(float64); ok {
			return int64(f)
		}
	}
	return v
}

func (m *model) jsonVersion() string {
	if m.meta.JSONVersion != "" && m.meta.Protocol == "json" {
		return m.meta.JSONVersion
	}
	return "1.0"
}

func (s *Service) respondREST(w http.ResponseWriter, m *model, wire int, op *operation, out map[string]any) {
	status := op.HTTP.ResponseCode
	if status == 0 {
		status = 200
	}
	sh := op.out
	var body []byte
	ct := "application/json"
	if wire == wRestXML {
		ct = "application/xml"
	}
	if sh != nil {
		for _, n := range sh.names {
			mr := sh.Members[n]
			v := out[n]
			if v == nil {
				continue
			}
			ln := locName(n, mr)
			switch mr.Location {
			case "header":
				if mr.sh.Type == "timestamp" {
					v = fmtTime(v, tsFormat(mr, mr.sh, "rfc822"))
				}
				w.Header().Set(ln, str(v))
			case "headers":
				for k, x := range v.(map[string]any) {
					w.Header().Set(ln+k, str(x))
				}
			case "statusCode":
				if f, ok := v.(float64); ok && f > 0 {
					status = int(f)
				}
			}
		}
		if sh.Payload != "" {
			pm := sh.Members[sh.Payload]
			if v := out[sh.Payload]; v != nil && pm != nil {
				switch pm.sh.Type {
				case "blob":
					body, _ = base64.StdEncoding.DecodeString(str(v))
					ct = "application/octet-stream"
				case "string":
					body = []byte(str(v))
					ct = "text/plain"
				default:
					if wire == wRestXML {
						var b strings.Builder
						writeXML(&b, locName(sh.Payload, pm), pm, pm.sh, v, false)
						body = []byte(b.String())
					} else {
						body, _ = json.Marshal(toJSON(pm, pm.sh, v, true, true))
					}
				}
			}
		} else if wire == wRestXML {
			var b strings.Builder
			root := sh.LocationName
			if root == "" {
				root = sh.Name
			}
			writeXML(&b, root, op.Output, sh, out, false)
			body = []byte(b.String())
		} else {
			mm := toJSON(op.Output, sh, out, true, true).(map[string]any)
			if len(mm) > 0 || status != 204 {
				body, _ = json.Marshal(mm)
			}
		}
	}
	if status == 204 {
		body = nil
	}
	if w.Header().Get("Content-Type") == "" { // a Content-Type member (InvokeModel) names it
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(status)
	w.Write(body)
}

// writeErr renders an error in the wire protocol.
func (m *model) writeErr(w http.ResponseWriter, c *core.Ctx, wire int, err error) {
	e := core.AsError(err)
	switch wire {
	case wJSON:
		core.WriteJSONError(w, c, m.jsonVersion(), e)
	case wCBOR:
		c.ErrCode = e.Code
		cborReply(w, e.Status, map[string]string{"__type": e.Code, "message": e.Msg})
	case wQuery:
		core.WriteQueryError(w, c, m.xmlns, e.Status, e.Code, e.Msg)
	case wEC2:
		c.ErrCode = e.Code
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(e.Status)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<Response><Errors><Error><Code>%s</Code><Message>%s</Message></Error></Errors><RequestID>%s</RequestID></Response>`,
			xmlEsc.Replace(e.Code), xmlEsc.Replace(e.Msg), c.RequestID)
	case wRestJSON:
		core.RESTJSONError(w, c, e)
	default:
		c.ErrCode = e.Code
		kind := "Sender"
		if e.Status >= 500 {
			kind = "Receiver"
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		w.Header().Set("X-Amzn-ErrorType", e.Code)
		w.WriteHeader(e.Status)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<ErrorResponse xmlns="%s"><Error><Type>%s</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>`,
			m.xmlns, kind, xmlEsc.Replace(e.Code), xmlEsc.Replace(e.Msg), c.RequestID)
	}
}
