package web

// The Terraform page (/_pogi/terraform) and the Cost page's Terraform tab: a browser or `pogi tf
// diagram` posts a Terraform folder (or a plan.json), and gets back an HTML fragment. pogi holds the
// last stack that loaded without error, in memory only, so both pages and a reload show the same
// stack, and a change (var file, size pick, grouping) posts only the view state.
// Nothing is written to disk or logged.

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/cost"
	"github.com/nrndev/pogi/internal/terraform"
)

//go:embed tfsample
var tfSampleFS embed.FS

const (
	tfFormMax  = 1 << 20  // room in an upload for the form fields and the multipart framing
	tfValueMax = 64 << 10 // the largest form field
	tfListMax  = 1000     // the most var files, modules or what-if sizes in one post
)

var errNotPlan = errors.New("That file is not a Terraform plan. Make one with: terraform plan -out plan.out && terraform show -json plan.out > plan.json")

// tfHeld is the stack the console holds: the last one that loaded without error. A stack is
// never edited: a change replaces the whole value.
type tfHeld struct {
	mu  sync.Mutex
	in  *tfIn
	gen int // counts the stacks loaded, so a slow view change can't put an older stack back
}

func (h *tfHeld) get() *tfIn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.in
}

// set holds in. fresh says it is a new upload; otherwise it is a view change of the stack it was
// read from, and it is dropped when another stack was loaded meanwhile.
func (h *tfHeld) set(in *tfIn, fresh bool) {
	c := *in
	c.Format, c.spans = "", nil
	h.mu.Lock()
	defer h.mu.Unlock()
	if fresh {
		h.gen++
		c.gen = h.gen
	} else if h.in == nil || h.in.gen != c.gen {
		return
	}
	h.in = &c
}

const tfNoStack = "No Terraform loaded yet. Drop a folder or a plan.json on the Terraform page."

func (ui *UI) tfRoutes(m *http.ServeMux) {
	// the shared pieces are in tfparts.html; each page file adds its own head and content
	for p, f := range map[string][]string{"tf": {"tf"}, "terraform": {"terraform"}, "tfsizes": {"tfsizes"}} {
		fs := []string{"templates/" + f[0] + ".html"}
		if p != "tfsizes" {
			fs = append(fs, "templates/tfparts.html")
		}
		ui.pages[p] = template.Must(template.Must(shell().Clone()).Funcs(tfFuncs).ParseFS(files, fs...))
	}
	m.HandleFunc("GET /_pogi/terraform", ui.tfDiagramPage)
	m.HandleFunc("POST /_pogi/terraform", ui.tfPost)
	m.HandleFunc("GET /_pogi/terraform/sample", tfSample)
	m.HandleFunc("GET /_pogi/terraform/live", ui.tfLive)
	m.HandleFunc("GET /_pogi/cost/terraform", ui.tfPage)
	m.HandleFunc("POST /_pogi/cost/terraform", ui.tfPost)
	m.HandleFunc("GET /_pogi/cost/terraform/sizes", ui.tfSizes)
}

// tfPageData is what the two Terraform pages show: the held stack's fragment, or an error.
type tfPageData struct {
	MaxFiles, MaxBytes int
	Loaded             template.HTML // the fragment of the held stack; "" when none is held
	Err                string
}

// tfShow renders the held stack for a page, as the fragment for view ("table" or "diagram").
func (ui *UI) tfShow(view string) tfPageData {
	d := tfPageData{MaxFiles: terraform.MaxFiles, MaxBytes: terraform.MaxBytes}
	h := ui.tf.get()
	if h == nil {
		return d
	}
	in := *h
	in.View = view
	var buf bytes.Buffer
	if _, _, err := ui.tfRender(&buf, &in); err != nil {
		d.Err = err.Error()
	}
	d.Loaded = template.HTML(buf.String())
	return d
}

func (ui *UI) tfPage(w http.ResponseWriter, r *http.Request) {
	ui.render(w, r, "tf", "Cost estimate", "cost", ui.tfShow("table"))
}

func (ui *UI) tfDiagramPage(w http.ResponseWriter, r *http.Request) {
	ui.render(w, r, "terraform", "Terraform", "terraform", ui.tfShow("diagram"))
}

// tfLive says which data-flow lines of the held stack have live traffic: {"<line>": "live" or "bad"}.
// It reads the parsed stack kept with the held one, so it neither reads nor prices the files. The
// page script calls it every few seconds, and a quiet or empty answer is {}.
func (ui *UI) tfLive(w http.ResponseWriter, r *http.Request) {
	out := map[string]string{}
	if in := ui.tf.get(); in != nil && in.cfg != nil && len(in.flows) > 0 && ui.hub != nil {
		lines := slices.Clone(in.flows) // markLive writes to its lines; the held ones stay as they are
		markLive(lines, in.cfg, ui.hub.Spans.All(), time.Now())
		for i, l := range lines {
			if l.Live {
				out[strconv.Itoa(i)] = map[bool]string{true: "bad", false: "live"}[l.Bad]
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(out)
}

// tfSample sends the embedded sample stack as {path: content}; the page treats it like a picked folder.
func tfSample(w http.ResponseWriter, r *http.Request) {
	out := map[string]string{}
	err := fs.WalkDir(tfSampleFS, "tfsample", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := tfSampleFS.ReadFile(p)
		out[strings.TrimPrefix(p, "tfsample/")] = string(b)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// tfIn is one post: the files, and the view state the page sends with them.
type tfIn struct {
	files    map[string][]byte
	Vars     []string          // variable files to read, in order
	Sizes    map[string]string // what-if: resource address → size
	Expanded []string          // modules to draw in full
	Group    string            // the table's grouping: module, service or cloud
	Layout   string            // the diagram's grouping: module or network
	View     string            // table or diagram: which fragment to answer with
	Format   string            // "" for the page fragment, or json, markdown or svg for a download
	state    bool              // the post carries the whole view state; without it, only files, view and format count
	gen      int               // which upload this stack came from (see tfHeld)

	// What the last render found, kept with the held stack so the live refresh reads it instead of
	// reading and pricing the files again. Like the rest of a held stack, never edited after it is held.
	cfg   *terraform.Config
	flows []flowLine
	spans []core.Span // the span log at render time; used while drawing, never held
}

var tfPathBad = regexp.MustCompile(`[\\\x00-\x1f]`)

// tfPath checks a file's path from the browser: relative, clean, no "..".
func tfPath(p string) error {
	switch {
	case p == "" || len(p) > 1024:
		return errors.New("a file has an empty or very long name")
	case tfPathBad.MatchString(p):
		return fmt.Errorf("the file name %q has a backslash or a control character", p)
	case strings.HasPrefix(p, "/") || path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../"):
		return fmt.Errorf("the file path %q must be relative and must not use .. or //", p)
	}
	return nil
}

// tfReadUpload reads the multipart body in memory, checking every limit as it goes. Files are
// parts with a file name; the other parts are the view state. The int is the HTTP status for an error.
func tfReadUpload(w http.ResponseWriter, r *http.Request) (*tfIn, int, error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, http.StatusBadRequest, errors.New("The page sends the files as multipart/form-data.")
	}
	in := &tfIn{files: map[string][]byte{}, Sizes: map[string]string{}, Group: "module", Layout: "module", View: "table"}
	var total int
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return tfBodyErr(err)
		}
		name := p.FormName()
		if p.FileName() == "" {
			b, err := io.ReadAll(io.LimitReader(p, tfValueMax+1))
			if err != nil {
				return tfBodyErr(err)
			}
			if len(b) > tfValueMax {
				return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("the form field %q is too long", name)
			}
			if err := in.field(name, string(b)); err != nil {
				return nil, http.StatusBadRequest, err
			}
			continue
		}
		if err := tfPath(name); err != nil {
			return nil, http.StatusBadRequest, err
		}
		if len(in.files) >= terraform.MaxFiles {
			return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("That is more than %d files. Pick the folder with the Terraform files, not a parent folder.", terraform.MaxFiles)
		}
		if _, dup := in.files[name]; dup {
			return nil, http.StatusBadRequest, fmt.Errorf("the file %q was sent twice", name)
		}
		b, err := io.ReadAll(io.LimitReader(p, int64(terraform.MaxBytes-total)+1))
		if err != nil {
			return tfBodyErr(err)
		}
		if total += len(b); total > terraform.MaxBytes {
			return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("The files are bigger than %d MB in total.", terraform.MaxBytes>>20)
		}
		in.files[name] = b
	}
	return in, 0, nil
}

// tfCheckVars fails when a var file is not among the files.
func tfCheckVars(in *tfIn) error {
	for _, v := range in.Vars {
		if _, ok := in.files[v]; !ok {
			return fmt.Errorf("the variable file %q was not sent", v)
		}
	}
	return nil
}

func tfBodyErr(err error) (*tfIn, int, error) {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("The upload is bigger than %d MB.", terraform.MaxBytes>>20)
	}
	return nil, http.StatusBadRequest, errors.New("The upload could not be read.")
}

// field stores one view-state field and rejects values it does not know how to use.
func (in *tfIn) field(name, v string) error {
	list := func(dst *[]string) error {
		if len(*dst) >= tfListMax {
			return fmt.Errorf("too many %q values", name)
		}
		*dst = append(*dst, v)
		return nil
	}
	switch name {
	case "vars":
		if err := tfPath(v); err != nil {
			return err
		}
		return list(&in.Vars)
	case "expanded":
		return list(&in.Expanded)
	case "sizes":
		if err := json.Unmarshal([]byte(v), &in.Sizes); err != nil || len(in.Sizes) > tfListMax {
			return errors.New("the what-if sizes are not a JSON object of address → size")
		}
	case "group":
		if !slices.Contains([]string{"module", "service", "cloud"}, v) {
			return fmt.Errorf("unknown grouping %q", v)
		}
		in.Group = v
	case "layout":
		if !slices.Contains([]string{"module", "network"}, v) {
			return fmt.Errorf("unknown grouping %q", v)
		}
		in.Layout = v
	case "state":
		in.state = true
	case "view":
		if !slices.Contains([]string{"table", "diagram"}, v) {
			return fmt.Errorf("unknown view %q", v)
		}
		in.View = v
	case "format":
		if !slices.Contains([]string{"", "json", "markdown", "svg"}, v) {
			return fmt.Errorf("unknown export format %q", v)
		}
		in.Format = v
	}
	return nil
}

// tfRead turns the files into a Config: a lone plan JSON file is a plan, anything else is a folder.
func tfRead(in *tfIn) (*terraform.Config, error) {
	if len(in.files) == 1 {
		for name, b := range in.files {
			if l := strings.ToLower(name); strings.HasSuffix(l, ".json") && !strings.HasSuffix(l, ".tf.json") && !strings.HasSuffix(l, ".tfvars.json") {
				if !terraform.IsPlan(b) {
					return nil, errNotPlan
				}
				cfg, err := terraform.ReadPlan(b)
				if err != nil {
					return nil, fmt.Errorf("Could not read the plan: %w", err)
				}
				return cfg, nil
			}
		}
	}
	if !slices.ContainsFunc(slices.Collect(maps.Keys(in.files)), func(n string) bool {
		return strings.HasSuffix(n, ".tf") || strings.HasSuffix(n, ".tf.json")
	}) {
		return nil, errors.New("No Terraform files (*.tf) were found. Pick the folder that holds them, or a plan.json.")
	}
	return terraform.Parse(in.files, in.Vars)
}

// tfPost is the one handler for both pages. With files, they become the new stack once they load.
// Without, it answers from the held stack with the posted view state, or fails when none is held.
func (ui *UI) tfPost(w http.ResponseWriter, r *http.Request) {
	// Nothing in reading a Terraform file may take pogi down: a panic becomes a 400, and the stack is logged once.
	defer func() {
		if p := recover(); p != nil {
			ui.hub.Log.Error("reading Terraform files crashed", "panic", p, "stack", string(debug.Stack()))
			ui.tfFail(w, http.StatusBadRequest, errors.New("pogi could not read these Terraform files. Check them for a syntax problem and try again."))
		}
	}()
	in, code, err := tfReadUpload(w, r)
	if err != nil {
		ui.tfFail(w, code, err)
		return
	}
	fresh := len(in.files) > 0
	if !fresh {
		h := ui.tf.get()
		if h == nil {
			ui.tfFail(w, http.StatusBadRequest, errors.New(tfNoStack))
			return
		}
		next := *h
		next.Format, next.View = in.Format, in.View
		if in.state {
			next.Vars, next.Sizes, next.Expanded, next.Group, next.Layout = in.Vars, in.Sizes, in.Expanded, in.Group, in.Layout
		}
		in = &next
	}
	if err := tfCheckVars(in); err != nil {
		ui.tfFail(w, http.StatusBadRequest, err)
		return
	}
	var buf bytes.Buffer
	ctype, file, err := ui.tfRender(&buf, in)
	if err != nil {
		code := http.StatusInternalServerError
		if bad, ok := err.(tfInputErr); ok {
			code, err = http.StatusUnprocessableEntity, bad.error
		}
		ui.tfFail(w, code, err)
		return
	}
	ui.tf.set(in, fresh)
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Cache-Control", "no-store")
	if file != "" {
		w.Header().Set("Content-Disposition", `attachment; filename="`+file+`"`)
	}
	buf.WriteTo(w)
}

// tfInputErr marks an error that is about the files, not about pogi.
type tfInputErr struct{ error }

// tfRender reads and prices the stack, and writes the page fragment (or a download, for the export
// formats) to buf. It returns the content type and, for a download, the file name.
func (ui *UI) tfRender(buf *bytes.Buffer, in *tfIn) (ctype, file string, err error) {
	cfg, err := tfRead(in)
	if err != nil {
		return "", "", tfInputErr{err}
	}
	db, err := cost.Default()
	var est *cost.Estimate
	if err == nil {
		est, err = cost.Price(cfg, cost.Options{DB: db, Sizes: in.Sizes})
	}
	if err != nil {
		return "", "", fmt.Errorf("Could not price the files: %w", err)
	}
	in.cfg, in.spans = cfg, nil
	if ui.hub != nil {
		in.spans = ui.hub.Spans.All()
	}
	data := tfBuild(cfg, est, in, db.Source)
	in.flows, in.spans = data.graph.flows, nil
	switch in.Format {
	case "json":
		b, err := json.MarshalIndent(est, "", "  ")
		if err != nil {
			return "", "", err
		}
		buf.Write(append(b, '\n'))
		return "application/json", "pogi-terraform-cost.json", nil
	case "markdown":
		buf.WriteString(tfMarkdown(est, db.Source))
		return "text/markdown; charset=utf-8", "pogi-terraform-cost.md", nil
	case "svg":
		buf.WriteString(data.graph.export())
		return "image/svg+xml", "pogi-terraform-diagram.svg", nil
	}
	name := map[string]string{"diagram": "tfdiagram"}[in.View]
	if name == "" {
		name = "result"
	}
	if err := ui.pages["tf"].ExecuteTemplate(buf, name, data); err != nil {
		return "", "", err
	}
	return "text/html; charset=utf-8", "", nil
}

// tfFail answers with an inline error the page shows above the drop zone.
func (ui *UI) tfFail(w http.ResponseWriter, code int, err error) {
	var buf bytes.Buffer
	if e := ui.pages["tf"].ExecuteTemplate(&buf, "error", err.Error()); e != nil {
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	buf.WriteTo(w)
}

// tfSizesData is the size picker's drawer.
type tfSizesData struct {
	Address, Attr, Region, Current string
	Rows                           []tfSizeRow
	HasGPU                         bool
}

type tfSizeRow struct {
	cost.SizePrice
	Monthly, Diff float64
	Current       bool
	Copy          string // the attribute line to paste into the .tf file
}

var tfAttr = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// tfSizes lists every size of one price column in one region. It is stateless: the page asks with
// the column, region and current size of the row.
func (ui *UI) tfSizes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db, err := cost.Default()
	if err == nil {
		var d *tfSizesData
		if d, err = tfSizeList(db, q.Get("column"), q.Get("region"), q.Get("current"), q.Get("address"), q.Get("attr")); err == nil {
			ui.renderX(w, r, "tfsizes", "Try another size", "cost", "What if · "+d.Address, d)
			return
		}
	}
	w.Header().Set("HX-Reswap", "none")
	trigger(w, "pogi-toast", map[string]string{"msg": err.Error(), "kind": "bad"})
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func tfSizeList(db *cost.DB, column, region, current, address, attr string) (*tfSizesData, error) {
	c := db.Columns[column]
	switch {
	case c == nil:
		return nil, fmt.Errorf("There is no size list called %q.", column)
	case len(address) > 512 || len(current) > 100:
		return nil, errors.New("The address or the size is too long.")
	case attr != "" && !tfAttr.MatchString(attr):
		return nil, fmt.Errorf("%q is not an attribute name.", attr)
	}
	sizes := db.Sizes(column, region)
	if len(sizes) == 0 {
		return nil, fmt.Errorf("There are no sizes for %q in the region %q.", column, region)
	}
	per := 1.0 // a month's price from the column's unit
	if c.Unit == "hour" {
		per = cost.HoursPerMonth
	}
	d := &tfSizesData{Address: address, Attr: attr, Region: region, Current: current}
	var base float64
	for _, s := range sizes {
		if s.Name == current {
			base = s.Price * per
		}
	}
	for _, s := range sizes {
		row := tfSizeRow{SizePrice: s, Monthly: s.Price * per, Current: s.Name == current, Copy: s.Name}
		if base > 0 {
			row.Diff = row.Monthly - base
		}
		if attr != "" {
			row.Copy = attr + ` = "` + s.Name + `"`
		}
		d.HasGPU = d.HasGPU || s.GPU > 0
		d.Rows = append(d.Rows, row)
	}
	return d, nil
}

// tfSizesURL is the address of the size picker for a row.
func tfSizesURL(r cost.Row) string {
	s := r.Sizes
	v := url.Values{"column": {s.Column}, "region": {r.Region}, "current": {s.Current}, "address": {r.Address}, "attr": {s.Attr}}
	return "/_pogi/cost/terraform/sizes?" + v.Encode()
}

// TerraformSVG draws the diagram as a standalone SVG file, for `pogi tf diagram -o`. It is the
// same drawing the Terraform page exports. network groups by network instead of by module.
func TerraformSVG(cfg *terraform.Config, est *cost.Estimate, network bool) string {
	return buildTFGraphBy(cfg, est, nil, network).export()
}
