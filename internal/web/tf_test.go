package web

import (
	"bytes"
	"fmt"
	"html/template"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/cost"
	"github.com/nrndev/pogi/internal/terraform"
)

func tfTestUI(t *testing.T) *UI {
	t.Helper()
	ui := &UI{pages: map[string]*template.Template{}}
	ui.tfRoutes(http.NewServeMux())
	return ui
}

// upload posts files (path → content) and form fields the way the page does.
func tfUpload(t *testing.T, ui *UI, files map[string]string, fields map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for p, c := range files {
		fw, err := mw.CreateFormFile(p, p)
		if err != nil {
			t.Fatal(err)
		}
		fw.Write([]byte(c))
	}
	for k, v := range fields {
		mw.WriteField(k, v)
	}
	mw.Close()
	r := httptest.NewRequest("POST", "/_pogi/cost/terraform", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()
	ui.tfPost(w, r)
	return w
}

func TestTFUploadFailures(t *testing.T) {
	ui := tfTestUI(t)
	many := map[string]string{}
	for i := 0; i <= terraform.MaxFiles; i++ {
		many[fmt.Sprintf("f%d.tf", i)] = ""
	}
	big := map[string]string{"main.tf": strings.Repeat("a", terraform.MaxBytes+1)}
	tests := []struct {
		name   string
		files  map[string]string
		fields map[string]string
		code   int
		says   string
	}{
		{"empty upload", nil, nil, 400, "No Terraform loaded yet"},
		{"too many files", many, nil, 413, "more than 2000 files"},
		{"too big", big, nil, 413, "bigger than"},
		{"dot dot path", map[string]string{"../x.tf": ""}, nil, 400, "must be relative"},
		{"nested dot dot", map[string]string{"a/../../x.tf": ""}, nil, 400, "must be relative"},
		{"absolute path", map[string]string{"/etc/x.tf": ""}, nil, 400, "must be relative"},
		{"backslash", map[string]string{`a\x.tf`: ""}, nil, 400, "backslash"},
		{"not a plan", map[string]string{"x.json": `{"hello":1}`}, nil, 422, "not a Terraform plan"},
		{"no terraform files", map[string]string{"readme.txt": "hi"}, nil, 422, "No Terraform files"},
		{"var file not sent", map[string]string{"main.tf": ""}, map[string]string{"vars": "prod.tfvars"}, 400, "was not sent"},
		{"unknown grouping", map[string]string{"main.tf": ""}, map[string]string{"group": "colour"}, 400, "unknown grouping"},
		{"unknown layout", map[string]string{"main.tf": ""}, map[string]string{"layout": "colour"}, 400, "unknown grouping"},
		{"unknown format", map[string]string{"main.tf": ""}, map[string]string{"format": "xml"}, 400, "unknown export format"},
		{"bad sizes", map[string]string{"main.tf": ""}, map[string]string{"sizes": "[1]"}, 400, "what-if sizes"},
		{"bad HCL", map[string]string{"main.tf": `resource "aws_vpc" "a" {`}, nil, 422, `class="flash"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := tfUpload(t, ui, tc.files, tc.fields)
			if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.says) {
				t.Fatalf("got %d %q, want %d with %q", w.Code, w.Body.String(), tc.code, tc.says)
			}
		})
	}
	t.Run("not multipart", func(t *testing.T) {
		w := httptest.NewRecorder()
		ui.tfPost(w, httptest.NewRequest("POST", "/_pogi/cost/terraform", strings.NewReader("x=1")))
		if w.Code != 400 {
			t.Fatalf("got %d", w.Code)
		}
	})
}

func tfTestDB(t *testing.T) *cost.DB {
	t.Helper()
	db := &cost.DB{
		Tables:  map[string][]cost.Size{"aws/ec2": {{Name: "m7g.large", Family: "m7g", VCPU: 2, MemGB: 8}, {Name: "m7g.xlarge", Family: "m7g", VCPU: 4, MemGB: 16}, {Name: "m4.large", Family: "m4", VCPU: 2, MemGB: 8, Old: true}}},
		Columns: map[string]*cost.Column{"aws/ec2/linux": {Table: "aws/ec2", Unit: "hour", Prices: map[string][]float64{"us-east-1": {0.1, 0.2, 0.15}, "eu-west-1": {0.11, 0, 0}}}},
	}
	b, err := cost.Encode(db)
	if err != nil {
		t.Fatal(err)
	}
	if db, err = cost.Decode(b); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestTFSizeList(t *testing.T) {
	db := tfTestDB(t)
	d, err := tfSizeList(db, "aws/ec2/linux", "us-east-1", "m7g.large", "aws_instance.a", "instance_type")
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Rows) != 3 || d.Rows[0].Name != "m7g.large" || !d.Rows[0].Current || d.Rows[0].Monthly != 0.1*730 {
		t.Fatalf("rows: %+v", d.Rows)
	}
	if got := d.Rows[1].Diff; got < 0.05*730-1e-6 || got > 0.05*730+1e-6 || d.Rows[1].Name != "m4.large" {
		t.Fatalf("the diff of %s is %v", d.Rows[1].Name, got)
	}
	if d.Rows[2].Copy != `instance_type = "m7g.xlarge"` {
		t.Fatalf("copy line %q", d.Rows[2].Copy)
	}
	for name, args := range map[string][5]string{
		"unknown column": {"aws/nope", "us-east-1", "", "a", ""},
		"unknown region": {"aws/ec2/linux", "mars-1", "", "a", ""},
		"bad attribute":  {"aws/ec2/linux", "us-east-1", "", "a", `x" y`},
		"long address":   {"aws/ec2/linux", "us-east-1", "", strings.Repeat("a", 600), ""},
	} {
		if _, err := tfSizeList(db, args[0], args[1], args[2], args[3], args[4]); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

// a hand-built plan: a vpc and two subnets exist; the plan replaces the vpc's gateway, creates an
// instance in a subnet, and leaves a database that uses the subnets alone.
func tfPlan() (*terraform.Config, *cost.Estimate) {
	res := func(addr, block, mod, typ, name, action string, deps ...string) *terraform.Resource {
		return &terraform.Resource{Address: addr, Block: block, Module: mod, Type: typ, Name: name, Cloud: "aws", Action: action, Deps: deps}
	}
	cfg := &terraform.Config{Source: "plan", Resources: []*terraform.Resource{
		res("module.net.aws_vpc.main", "module.net.aws_vpc.main", "module.net", "aws_vpc", "main", "replace"),
		res("module.net.aws_subnet.a[0]", "module.net.aws_subnet.a", "module.net", "aws_subnet", "a", "no-op", "module.net.aws_vpc.main"),
		res("module.net.aws_subnet.a[1]", "module.net.aws_subnet.a", "module.net", "aws_subnet", "a", "no-op", "module.net.aws_vpc.main"),
		res("aws_instance.app", "aws_instance.app", "", "aws_instance", "app", "create", "module.net.aws_subnet.a", "data.aws_ami.x"),
		res("aws_db_instance.db", "aws_db_instance.db", "", "aws_db_instance", "db", "no-op", "module.net.aws_subnet.a"),
		res("aws_s3_bucket.logs", "aws_s3_bucket.logs", "", "aws_s3_bucket", "logs", "no-op"),
		{Address: "data.aws_ami.x", Block: "data.aws_ami.x", Type: "aws_ami", Name: "x", Data: true, Cloud: "aws"},
	}}
	est := &cost.Estimate{Source: "plan", Total: 300, Before: 250, Clouds: map[string]float64{"aws": 300}}
	for _, r := range cfg.Resources {
		if r.Data {
			continue
		}
		row := cost.Row{Address: r.Address, Block: r.Block, Module: r.Module, Type: r.Type, Name: r.Name, Cloud: "aws", Service: "x", Action: r.Action, Status: cost.Known, Monthly: 50, Before: 50}
		switch r.Type {
		case "aws_vpc", "aws_subnet":
			row.Status, row.Monthly, row.Before = cost.Free, 0, 0
		case "aws_instance":
			row.Monthly, row.Before = 100, 0
		}
		est.Rows = append(est.Rows, row)
	}
	return cfg, est
}

func TestTFDiagramPlan(t *testing.T) {
	cfg, est := tfPlan()
	g := buildTFGraph(cfg, est, nil)
	if len(g.Nodes) != 5 { // one box per block; the data source is left out
		t.Fatalf("%d boxes", len(g.Nodes))
	}
	sub := g.byID["module.net.aws_subnet.a"]
	if len(sub.Rows) != 2 || sub.Action != "" || !sub.Affected {
		t.Fatalf("the subnet box: %+v", sub)
	}
	if got := g.byID["aws_instance.app"].Deps; len(got) != 1 || got[0] != "module.net.aws_subnet.a" {
		t.Fatalf("the instance depends on %v (the data source must be left out)", got)
	}
	if !g.byID["aws_db_instance.db"].Affected || g.byID["aws_s3_bucket.logs"].Affected || g.byID["aws_instance.app"].Affected {
		t.Fatal("affected set is wrong")
	}
	if got := g.summary(); got != "1 to create, 1 to replace, 2 affected" {
		t.Fatalf("summary %q", got)
	}
	if g.byID["module.net.aws_vpc.main"].col != 0 || sub.col != 1 || g.byID["aws_instance.app"].col != 2 {
		t.Fatalf("columns %d %d %d: foundations belong on the left", g.byID["module.net.aws_vpc.main"].col, sub.col, g.byID["aws_instance.app"].col)
	}
	s := g.svg(false)
	for _, w := range []string{"a-create", "a-replace", "affected", `×2`, `class="band"`, "module.net", `data-deps="module.net.aws_subnet.a"`, "$100.00/mo"} {
		if !strings.Contains(s, w) {
			t.Errorf("svg lacks %q", w)
		}
	}
	if strings.Contains(s, "$0.00") || strings.Contains(g.svg(false), "NaN") {
		t.Error("a free box shows a cost, or a number is NaN")
	}
	if buildTFGraph(cfg, est, nil).svg(false) != s {
		t.Fatal("the same input drew a different diagram")
	}
	if e := g.export(); !strings.Contains(e, `xmlns="http://www.w3.org/2000/svg"`) || strings.Contains(e, "var(--") || strings.Contains(e, "data-id") {
		t.Fatal("the export is not a standalone SVG")
	}
}

func TestTFDiagramCollapse(t *testing.T) {
	cfg := &terraform.Config{}
	est := &cost.Estimate{Source: "folder", Total: 10, Clouds: map[string]float64{}}
	for i := range tfCollapseAt + 1 {
		a := fmt.Sprintf("module.big.aws_sqs_queue.q%d", i)
		cfg.Resources = append(cfg.Resources, &terraform.Resource{Address: a, Block: a, Module: "module.big", Type: "aws_sqs_queue", Cloud: "aws", Name: "q"})
		est.Rows = append(est.Rows, cost.Row{Address: a, Block: a, Module: "module.big", Cloud: "aws", Status: cost.Known, Monthly: 1})
	}
	cfg.Resources = append(cfg.Resources, &terraform.Resource{Address: "aws_lb.x", Block: "aws_lb.x", Type: "aws_lb", Cloud: "aws", Name: "x", Deps: []string{"module.big.aws_sqs_queue.q3"}})
	est.Rows = append(est.Rows, cost.Row{Address: "aws_lb.x", Block: "aws_lb.x", Cloud: "aws", Status: cost.Known, Monthly: 5})

	g := buildTFGraph(cfg, est, nil)
	if len(g.Nodes) != 2 || !g.byID["module.big"].Collapsed || g.byID["module.big"].Blocks != tfCollapseAt+1 || g.byID["module.big"].Monthly != float64(tfCollapseAt+1) {
		t.Fatalf("a big module should be one box: %d boxes", len(g.Nodes))
	}
	if d := g.byID["aws_lb.x"].Deps; len(d) != 1 || d[0] != "module.big" {
		t.Fatalf("an arrow into a collapsed module goes to its box: %v", d)
	}
	if !strings.Contains(g.svg(false), `data-expand="module.big"`) {
		t.Fatal("a collapsed box can be expanded")
	}
	if g = buildTFGraph(cfg, est, []string{"module.big"}); len(g.Nodes) != tfCollapseAt+2 {
		t.Fatalf("expanded: %d boxes", len(g.Nodes))
	}
	if buildTFGraph(&terraform.Config{}, &cost.Estimate{}, nil).svg(false) == "" {
		t.Fatal("an empty config still draws")
	}
}

func TestTFWhatIf(t *testing.T) {
	cfg, est := tfPlan()
	est.Rows[3].Sizes = &cost.SizeRef{Column: "aws/ec2/linux", Attr: "instance_type", Current: "m7g.xlarge", File: "m7g.large"}
	est.Rows[3].Region = "us-east-1"
	d := tfBuild(cfg, est, &tfIn{Group: "module", Sizes: map[string]string{"aws_instance.app": "m7g.xlarge"}}, nil)
	if d.WhatIfs != 1 || !d.Rows[3].WhatIf || !strings.Contains(d.Rows[3].SizesURL, "column=aws%2Fec2%2Flinux") {
		t.Fatalf("what-if: %+v", d.Rows[3])
	}
}

// tfNet makes a config of one cloud: nets (type → names), then boxes with their dependencies.
func tfNet(cloud string, types [3]string, specs ...[3]string) (*terraform.Config, *cost.Estimate) {
	cfg := &terraform.Config{Source: "folder"}
	est := &cost.Estimate{Source: "folder", Total: 10, Clouds: map[string]float64{}}
	for _, sp := range specs { // type, name, deps (space separated)
		addr := sp[0] + "." + sp[1]
		r := &terraform.Resource{Address: addr, Block: addr, Type: sp[0], Name: sp[1], Cloud: cloud}
		if sp[2] != "" {
			r.Deps = strings.Fields(sp[2])
		}
		cfg.Resources = append(cfg.Resources, r)
		est.Rows = append(est.Rows, cost.Row{Address: addr, Block: addr, Type: sp[0], Name: sp[1], Cloud: cloud, Status: cost.Known, Monthly: 1})
	}
	return cfg, est
}

func tfNetAWS() (*terraform.Config, *cost.Estimate) {
	return tfNet("aws", [3]string{}, [3]string{"aws_vpc", "v", ""}, [3]string{"aws_subnet", "a", "aws_vpc.v"}, [3]string{"aws_subnet", "b", "aws_vpc.v"},
		[3]string{"aws_vpc", "w", ""}, [3]string{"aws_subnet", "c", "aws_vpc.w"}, [3]string{"aws_subnet", "empty", "aws_vpc.w"},
		[3]string{"aws_instance", "i1", "aws_subnet.a aws_vpc.v"},    // innermost: the subnet
		[3]string{"aws_instance", "i2", "aws_subnet.a aws_subnet.b"}, // two subnets of one network: the network
		[3]string{"aws_instance", "i3", "aws_vpc.v"},
		[3]string{"aws_instance", "i4", "aws_subnet.a aws_subnet.c"}, // two networks: no common frame, so the lane
		[3]string{"aws_instance", "i5", ""},                          // no network: the lane
		[3]string{"aws_lb", "lb", "aws_instance.i1"})
}

func TestTFNetworkFrames(t *testing.T) {
	cfg, est := tfNetAWS()
	g := buildTFGraphBy(cfg, est, nil, true)
	for id, want := range map[string]string{"aws_instance.i1": "aws_subnet.a", "aws_instance.i2": "aws_vpc.v", "aws_instance.i3": "aws_vpc.v",
		"aws_instance.i4": "", "aws_instance.i5": "", "aws_lb.lb": "", "aws_subnet.a": "aws_vpc.v"} {
		if got := g.byID[id].in; got != want {
			t.Errorf("%s sits in %q, want %q", id, got, want)
		}
	}
	if g.byID["aws_vpc.v"].Frame != "net" || g.byID["aws_subnet.a"].Frame != "sub" || g.boxes() != 6 {
		t.Fatalf("frames: %d boxes", g.boxes())
	}
	s := g.svg(false)
	if n := strings.Count(s, `class="band frame net"`); n != 2 {
		t.Errorf("%d network frames, want 2", n)
	}
	if n := strings.Count(s, `class="band frame sub"`); n != 4 { // a, b, c and the empty one
		t.Errorf("%d subnet frames, want 4", n)
	}
	// a frame's label is focusable and selects its resource
	if !strings.Contains(s, `tabindex="0" role="button" aria-label="subnet a"`) || !strings.Contains(s, `data-id="aws_subnet.a"`) {
		t.Error("a frame's label is not a selectable control")
	}
	if strings.Contains(s, `data-f="aws_subnet.a" data-t="aws_instance.i1"`) || strings.Contains(s, `data-f="aws_vpc.v" data-t="aws_subnet.a"`) {
		t.Error("an arrow from a frame to what it contains")
	}
	if !strings.Contains(s, `data-f="aws_subnet.a" data-t="aws_instance.i4"`) || !strings.Contains(s, `data-f="aws_instance.i1" data-t="aws_lb.lb"`) {
		t.Error("an arrow that crosses a frame is gone")
	}
	// a box is inside its frame's rectangle, and the box with no network is outside every frame
	inside := func(n *tfNode, bd tfBand) bool {
		return n.x >= bd.x && n.x+tfW <= bd.x+bd.w && n.y >= bd.y && n.y+tfH <= bd.y+bd.h
	}
	var in1, in5 bool
	for _, bd := range g.bands {
		in1 = in1 || bd.ID == "aws_subnet.a" && inside(g.byID["aws_instance.i1"], bd)
		for _, id := range []string{"aws_instance.i4", "aws_instance.i5"} {
			if bd.Kind != "" && inside(g.byID[id], bd) {
				t.Errorf("%s drew inside the frame %s", id, bd.ID)
				in5 = true
			}
		}
	}
	if !in1 || in5 {
		t.Error("boxes are not where their frames say")
	}
	if len(g.lanes) != 1 {
		t.Errorf("%d lanes", len(g.lanes))
	}
	if buildTFGraphBy(cfg, est, nil, true).svg(false) != s {
		t.Fatal("the same input drew a different diagram")
	}
	// module mode draws no frames, and every resource is a box
	m := buildTFGraphBy(cfg, est, nil, false)
	if strings.Contains(m.svg(false), "band frame") || m.boxes() != len(m.Nodes) || m.byID["aws_vpc.v"].Frame != "" {
		t.Error("module mode drew frames")
	}
}

func TestTFNetworkClouds(t *testing.T) {
	for _, c := range []struct{ cloud, net, sub, box string }{
		{"azure", "azurerm_virtual_network", "azurerm_subnet", "azurerm_linux_virtual_machine"},
		{"gcp", "google_compute_network", "google_compute_subnetwork", "google_compute_instance"},
	} {
		cfg, est := tfNet(c.cloud, [3]string{}, [3]string{c.net, "n", ""}, [3]string{c.sub, "s", c.net + ".n"}, [3]string{c.box, "vm", c.sub + ".s"}, [3]string{c.box, "free", ""})
		g := buildTFGraphBy(cfg, est, nil, true)
		if g.byID[c.sub+".s"].parent != c.net+".n" || g.byID[c.box+".vm"].in != c.sub+".s" || g.byID[c.box+".free"].in != "" || g.boxes() != 2 {
			t.Errorf("%s: frames are wrong", c.cloud)
		}
	}
	// a box whose network is inside a collapsed module is not put in a frame
	cfg, est := tfNet("aws", [3]string{}, [3]string{"aws_instance", "x", ""})
	if g := buildTFGraphBy(cfg, est, nil, true); g.byID["aws_instance.x"].in != "" {
		t.Error("a box with no network sits in a frame")
	}
	if g := buildTFGraphBy(&terraform.Config{}, &cost.Estimate{}, nil, true); g.svg(false) == "" {
		t.Error("an empty config in network mode does not draw")
	}
}

// The console holds the last stack that loaded: a bad or too-large upload keeps it, a good one replaces it.
func TestTFStack(t *testing.T) {
	ui := tfTestUI(t)
	one := map[string]string{"main.tf": `resource "aws_s3_bucket" "one" {}`}
	two := map[string]string{"main.tf": `resource "aws_s3_bucket" "two" {}`, "prod.tfvars": ""}
	view := map[string]string{"view": "diagram"}

	if w := tfUpload(t, ui, nil, view); w.Code != 400 || !strings.Contains(w.Body.String(), tfNoStack) {
		t.Fatalf("a view-only post with no stack: %d %s", w.Code, w.Body.String())
	}
	if ui.tf.get() != nil {
		t.Fatal("a failed post made a stack")
	}
	if w := tfUpload(t, ui, one, view); w.Code != 200 || !strings.Contains(w.Body.String(), "aws_s3_bucket") || !strings.Contains(w.Body.String(), `&#34;vars&#34;:[]`) || !strings.Contains(w.Body.String(), `&#34;expanded&#34;:[]`) {
		t.Fatalf("good upload: %d %s", w.Code, w.Body.String())
	}
	first := ui.tf.get()
	if first == nil || string(first.files["main.tf"]) != one["main.tf"] {
		t.Fatal("the good upload was not held")
	}
	// bad uploads keep the old stack
	big := map[string]string{"main.tf": strings.Repeat("a", terraform.MaxBytes+1)}
	for name, files := range map[string]map[string]string{"not a plan": {"x.json": "{}"}, "too big": big, "bad HCL": {"main.tf": `resource "aws_vpc" "a" {`}} {
		if w := tfUpload(t, ui, files, nil); w.Code < 400 || ui.tf.get() != first {
			t.Errorf("%s: %d, and the stack changed", name, w.Code)
		}
	}
	// a post with the view state and no files re-renders the held files with the new state
	if w := tfUpload(t, ui, nil, map[string]string{"state": "1", "layout": "network", "view": "diagram"}); w.Code != 200 || !strings.Contains(w.Body.String(), `value="network" selected`) {
		t.Fatalf("view-only post: %d %s", w.Code, w.Body.String())
	}
	if h := ui.tf.get(); h == first || h.Layout != "network" || string(h.files["main.tf"]) != one["main.tf"] {
		t.Fatalf("held after a view change: %+v", h)
	}
	if w := tfUpload(t, ui, nil, map[string]string{"state": "1", "vars": "prod.tfvars"}); w.Code != 400 || !strings.Contains(w.Body.String(), "was not sent") {
		t.Fatalf("a var file that is not in the stack: %d", w.Code)
	}
	// without state, a post only picks the fragment or the export
	if w := tfUpload(t, ui, nil, map[string]string{"format": "svg"}); w.Code != 200 || !strings.HasPrefix(w.Body.String(), "<svg xmlns") || !strings.Contains(w.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("svg export of the held stack: %d", w.Code)
	}
	if ui.tf.get().Layout != "network" || ui.tf.get().Format != "" {
		t.Fatal("an export changed the view state, or was held as the format")
	}
	for format, say := range map[string]string{"json": `"monthly"`, "markdown": "## Terraform cost estimate"} {
		if w := tfUpload(t, ui, nil, map[string]string{"format": format}); w.Code != 200 || !strings.Contains(w.Body.String(), say) {
			t.Errorf("%s export: %d %s", format, w.Code, w.Body.String())
		}
	}
	// a good upload replaces the stack, and its view state starts empty
	if w := tfUpload(t, ui, two, map[string]string{"layout": "network", "view": "table"}); w.Code != 200 || !strings.Contains(w.Body.String(), `data-tf-var="prod.tfvars"`) || !strings.Contains(w.Body.String(), "Open diagram") {
		t.Fatalf("second upload: %d %s", w.Code, w.Body.String())
	}
	if h := ui.tf.get(); h == first || len(h.Vars) != 0 || string(h.files["main.tf"]) != two["main.tf"] {
		t.Fatalf("the second upload did not replace the stack: %+v", h)
	}
}

// A view change that finishes after a new upload must not put the older stack back.
func TestTFHeldKeepsNewerStack(t *testing.T) {
	var h tfHeld
	h.set(&tfIn{Layout: "module"}, true)
	old := *h.get() // a view change starts from this stack
	h.set(&tfIn{Layout: "network"}, true)
	old.Group = "service"
	h.set(&old, false)
	if got := h.get(); got.Layout != "network" || got.Group == "service" {
		t.Fatalf("a late view change put the older stack back: %+v", got)
	}
	cur := *h.get()
	cur.Group = "cloud"
	if h.set(&cur, false); h.get().Group != "cloud" {
		t.Fatal("a view change of the current stack was dropped")
	}
}
