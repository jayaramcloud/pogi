package web

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Generated forms. A form is a tree of core.Fields (from a botocore model, a protobuf message, or
// written by hand). Inputs are named by their path, "Tags.0.Key", so a posted form is the tree again:
// lists and maps are repeated fieldsets, each added with one htmx request, and collect reads the values
// back into the JSON the API takes.

// node is a field at a place in the form, what the "field" template renders.
type node struct {
	Base string // the form's URL, for its add buttons
	V    url.Values
	F    core.Field
	Path string
	Req  bool // the browser may insist on it: it and everything around it is required
	N    int  // the index the next item of a list gets
}

func (n *node) Name() string { return n.Path }
func (n *node) Val() string  { return n.V.Get(n.Path) }
func (n *node) Marker() bool { return n.F.Required }
func (n *node) Open() bool   { return n.has(n.Path) }
func (n *node) has(path string) bool {
	for k := range n.V {
		if (k == path || strings.HasPrefix(k, path+".")) && n.V.Get(k) != "" {
			return true
		}
	}
	return false
}
func (n *node) Items() []int { return indices(n.V, n.Path) }

// Sub is a member of a struct.
func (n *node) Sub(f core.Field) *node {
	p := f.Name
	if n.Path != "" {
		p = n.Path + "." + f.Name
	}
	return &node{n.Base, n.V, f, p, n.Req && f.Required, 0}
}

// Item is the i-th element of a list or entry of a map. A map entry is a struct with a key and a value.
func (n *node) Item(i int) *node {
	e := *n.F.Elem
	if n.F.Kind == "map" {
		e = core.Field{Kind: "struct", Fields: []core.Field{{Name: "k", Label: "Key", Kind: "string", Required: true}, mapValue(e)}}
	}
	e.Required = true
	return &node{n.Base, n.V, e, n.Path + "." + strconv.Itoa(i), true, 0}
}

func mapValue(e core.Field) core.Field {
	e.Name, e.Label, e.Required = "v", "Value", false
	return e
}

// Add is the button that appends an item numbered next, and keeps a button for the one after.
func (n *node) Add(next int) *node { m := *n; m.N = next; return &m }

func (n *node) AddURL() string {
	return n.Base + "/item?path=" + url.QueryEscape(n.Path) + "&n=" + strconv.Itoa(n.N)
}

func (n *node) Number() bool { return n.F.Kind == "integer" || n.F.Kind == "number" }
func (n *node) Step() string {
	if n.F.Kind == "integer" {
		return "1"
	}
	return "any"
}

// next is the index after the last of a list's items in a posted form.
func (n *node) Next() int {
	is := n.Items()
	if len(is) == 0 {
		return 0
	}
	return is[len(is)-1] + 1
}

// indices are the item numbers a posted form has under path, in order.
func indices(v url.Values, path string) []int {
	seen := map[int]bool{}
	for k := range v {
		if rest, ok := strings.CutPrefix(k, path+"."); ok {
			seg, _, _ := strings.Cut(rest, ".")
			if i, err := strconv.Atoi(seg); err == nil {
				seen[i] = true
			}
		}
	}
	out := make([]int, 0, len(seen))
	for i := range seen {
		out = append(out, i)
	}
	slices.Sort(out)
	return out
}

// seed prefills the defaults of a fresh form.
func seed(fields []core.Field, path string, v url.Values) {
	for _, f := range fields {
		p := path + f.Name
		if f.Default != "" {
			v.Set(p, f.Default)
		}
		seed(f.Fields, p+".", v)
	}
}

// collect reads a field's value from a posted form. ok is false when nothing was entered.
func collect(f core.Field, path string, v url.Values) (val any, ok bool, err error) {
	switch f.Kind {
	case "struct":
		m := map[string]any{}
		for _, c := range f.Fields {
			cv, cok, cerr := collect(c, path+"."+c.Name, v)
			if cerr != nil {
				return nil, false, cerr
			}
			if cok {
				m[c.Name] = cv
			}
		}
		return m, len(m) > 0 || f.Required, nil
	case "list":
		var l []any
		for _, i := range indices(v, path) {
			ev, eok, eerr := collect(*f.Elem, path+"."+strconv.Itoa(i), v)
			if eerr != nil {
				return nil, false, eerr
			}
			if eok {
				l = append(l, ev)
			}
		}
		return l, len(l) > 0, nil
	case "map":
		m := map[string]any{}
		for _, i := range indices(v, path) {
			p := path + "." + strconv.Itoa(i)
			k := strings.TrimSpace(v.Get(p + ".k"))
			if k == "" {
				continue
			}
			ev, eok, eerr := collect(mapValue(*f.Elem), p+".v", v)
			if eerr != nil {
				return nil, false, eerr
			}
			if !eok && f.Elem.Kind == "string" {
				ev, eok = "", true
			}
			if eok {
				m[k] = ev
			}
		}
		return m, len(m) > 0, nil
	}
	raw := strings.TrimSpace(v.Get(path))
	if raw == "" {
		return nil, false, nil
	}
	label := f.Label
	if label == "" {
		label = path
	}
	switch f.Kind {
	case "integer":
		n, e := strconv.ParseInt(raw, 10, 64)
		if e != nil {
			return nil, false, fmt.Errorf("%s: %q is not a whole number", label, raw)
		}
		return n, true, nil
	case "number":
		n, e := strconv.ParseFloat(raw, 64)
		if e != nil {
			return nil, false, fmt.Errorf("%s: %q is not a number", label, raw)
		}
		return n, true, nil
	case "bool":
		b, e := strconv.ParseBool(raw)
		if e != nil {
			return nil, false, fmt.Errorf("%s: %q is not true or false", label, raw)
		}
		return b, true, nil
	case "json":
		var j any
		if e := json.Unmarshal([]byte(raw), &j); e != nil {
			return nil, false, fmt.Errorf("%s is not valid JSON: %v", label, e)
		}
		return j, true, nil
	}
	return v.Get(path), true, nil
}

// input reads a whole form.
func input(fields []core.Field, v url.Values) (map[string]any, error) {
	in := map[string]any{}
	for _, f := range fields {
		val, ok, err := collect(f, f.Name, v)
		if err != nil {
			return nil, err
		}
		if ok {
			in[f.Name] = val
		}
	}
	return in, nil
}

// lookup finds the field at a form path such as "Rules.0.Conditions".
func lookup(fields []core.Field, path string) (core.Field, bool) {
	cur := core.Field{Kind: "struct", Fields: fields}
	for _, seg := range strings.Split(path, ".") {
		switch cur.Kind {
		case "struct":
			i := slices.IndexFunc(cur.Fields, func(f core.Field) bool { return f.Name == seg })
			if i < 0 {
				return cur, false
			}
			cur = cur.Fields[i]
		case "list", "map":
			if _, err := strconv.Atoi(seg); err != nil {
				return cur, false
			}
			node := (&node{F: cur}).Item(0)
			cur = node.F
		default:
			return cur, false
		}
	}
	return cur, true
}

// ---- pages ----

// formData is what the form page shows.
type formData struct {
	Service string
	Form    *core.Form
	Root    *node
	Err     string
	Scope   scope
	Back    string // where a link back to the service goes
	Eyebrow string
}

// Required are the form's required top-level fields: the drawer shows them first.
func (d formData) Required() (out []*node) {
	for _, f := range d.Form.Fields {
		if f.Required {
			out = append(out, d.Root.Sub(f))
		}
	}
	return
}

// Optional are the others, behind "Optional (n)".
func (d formData) Optional() (out []*node) {
	for _, f := range d.Form.Fields {
		if !f.Required {
			out = append(out, d.Root.Sub(f))
		}
	}
	return
}

// OptionalOpen is true when an optional field has a value and the form is shown again after an error.
func (d formData) OptionalOpen() bool {
	for _, n := range d.Optional() {
		if d.Err == "" {
			break
		}
		if n.has(n.Path) {
			return true
		}
	}
	return false
}

func (ui *UI) newForm(w http.ResponseWriter, r *http.Request) {
	svc, op := r.PathValue("service"), r.PathValue("op")
	f := ui.form(svc, op, ui.scope(r))
	if f == nil {
		http.NotFound(w, r)
		return
	}
	v := url.Values{}
	seed(f.Fields, "", v)
	ui.renderForm(w, r, svc, f, v, "")
}

func (ui *UI) renderForm(w http.ResponseWriter, r *http.Request, svc string, f *core.Form, v url.Values, errMsg string) {
	base := "/_pogi/new/" + svc + "/" + f.Op
	root := &node{Base: base, V: v, F: core.Field{Kind: "struct", Fields: f.Fields}, Req: true}
	sc, title := ui.scope(r), svc
	if s := ui.hub.Service(svc); s != nil {
		title = s.Info().Title
	}
	ui.render(w, r, "form", f.Title, svc, formData{svc, f, root, errMsg, sc, base, title + " · " + sc.target(ui.formCloud(svc), ui.hub.Cfg.Region)})
}

func (ui *UI) newSubmit(w http.ResponseWriter, r *http.Request) {
	svc, op := r.PathValue("service"), r.PathValue("op")
	f := ui.form(svc, op, ui.scope(r))
	if f == nil {
		http.NotFound(w, r)
		return
	}
	code := svc == "lambda" // its only file field is the function's zip
	limit := ui.hub.MaxUpload()
	if code {
		limit = ui.hub.CodeLimit(false)
	}
	if err := ui.parseUpload(w, r, code); err != nil {
		ui.renderForm(w, r, svc, f, url.Values{}, err.Error())
		return
	}
	for _, fld := range f.Fields {
		if fld.Kind != "file" || r.MultipartForm == nil || len(r.MultipartForm.File[fld.Name]) == 0 {
			continue
		}
		if size := r.MultipartForm.File[fld.Name][0].Size; size > limit {
			ui.renderForm(w, r, svc, f, url.Values{}, ui.tooBig(r.URL.Path, size, limit, code))
			return
		}
		if fh, err := r.MultipartForm.File[fld.Name][0].Open(); err == nil { // a file's value is its bytes in base64
			data, _ := io.ReadAll(fh)
			fh.Close()
			r.PostForm.Set(fld.Name, base64.StdEncoding.EncodeToString(data))
		}
	}
	in, err := input(f.Fields, r.PostForm)
	if err == nil {
		err = ui.submit(svc, op, ui.scope(r), in)
	}
	if err != nil {
		ui.renderForm(w, r, svc, f, r.PostForm, errText(err)) // 200, not 4xx: htmx does not swap error statuses
		return
	}
	done(w, r, "/_pogi/resources/"+svc, doneText(f, in), nil)
}

// errText is an API error as the console shows it.
func errText(err error) string {
	if e, ok := err.(*core.Error); ok {
		return e.Code + ": " + e.Msg
	}
	return err.Error()
}

// formItem answers an add button: one more item of a list or map, and the button for the next.
func (ui *UI) formItem(w http.ResponseWriter, r *http.Request) {
	f := ui.form(r.PathValue("service"), r.PathValue("op"), ui.scope(r))
	if f == nil {
		http.NotFound(w, r)
		return
	}
	path := r.URL.Query().Get("path")
	fld, ok := lookup(f.Fields, path)
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	if !ok || fld.Elem == nil {
		http.NotFound(w, r)
		return
	}
	list := &node{Base: "/_pogi/new/" + r.PathValue("service") + "/" + f.Op, V: url.Values{}, F: fld, Path: path, N: n}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	ui.pages["form"].ExecuteTemplate(w, "added", list)
}

// blank answers the remove buttons: htmx deletes the item whatever comes back.
func blank(w http.ResponseWriter, r *http.Request) {}

// doneText says what a finished form did to which resource: "Bucket photos created", "Rule saved".
// A form whose verb or name it cannot tell gets "<title>: done".
func doneText(f *core.Form, in map[string]any) string {
	if m, _ := in["_msg"].(string); m != "" { // a special form that says it itself
		return m
	}
	verb := ""
	for prefix, v := range map[string]string{"Create": "created", "Delete": "deleted", "Remove": "removed", "Update": "updated",
		"Put": "saved", "Set": "saved", "Modify": "updated", "Add": "added"} {
		if strings.HasPrefix(f.Op, prefix) {
			verb = v
		}
	}
	if verb == "" {
		return f.Title + ": done"
	}
	_, noun, _ := strings.Cut(f.Title, " ") // "Create bucket" -> "bucket"
	name := ""
	for _, fld := range f.Fields { // the first field that names the thing: Name, QueueName, Bucket...
		if s, ok := in[fld.Name].(string); ok && s != "" && len(s) <= 80 && !strings.ContainsAny(s, "\n{[") &&
			(strings.HasSuffix(strings.ToLower(fld.Name), "name") || strings.EqualFold(fld.Name, noun)) { // "Bucket" names a bucket
			name = s
			break
		}
	}
	out := strings.TrimSpace(noun + " " + name)
	if out == "" {
		return f.Title + ": done"
	}
	return strings.ToUpper(out[:1]) + out[1:] + " " + verb
}

// formCloud is the cloud a service's form belongs to; its drawer says that cloud's scope.
func (ui *UI) formCloud(svc string) string {
	if s := ui.hub.Service(svc); s != nil {
		return s.Info().Cloud
	}
	return core.AWS
}
