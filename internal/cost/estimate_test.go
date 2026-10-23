package cost

import (
	"testing"

	"github.com/nrndev/pogi/internal/terraform"
)

// testDB is a tiny price list for the pricer's own tests.
func testDB(t *testing.T) *DB {
	t.Helper()
	db := &DB{
		Published: map[string]string{"aws": "2026-10-20"},
		Tables:    map[string][]Size{"test/vm": {{Name: "small", VCPU: 1, MemGB: 2}, {Name: "big", VCPU: 4, MemGB: 16}}},
		Columns:   map[string]*Column{"test/vm/linux": {Table: "test/vm", Unit: "hour", Prices: map[string][]float64{"us-east-1": {0.01, 0.04}, "eu-west-1": {0.012, 0}}}},
		Rates:     map[string]*Rate{"test/disk": {Unit: "GB-month", Prices: map[string]float64{"us-east-1": 0.1}}, "test/req": {Unit: "1M requests", Prices: map[string]float64{"us-east-1": 0.2}}},
	}
	b, err := Encode(db)
	if err != nil {
		t.Fatal(err)
	}
	if db, err = Decode(b); err != nil {
		t.Fatal(err)
	}
	return db
}

func init() {
	mappings["aws_test_vm"] = mapping{service: "Test", price: func(r *res) {
		size := r.size("test/vm/linux", "size", "")
		r.hourly("test/vm/linux", size, HoursPerMonth, "Instance hours ("+size+")")
		r.rate("test/disk", r.num("disk_gb", 8), "Disk")
	}}
	mappings["aws_test_fn"] = mapping{service: "Test", price: func(r *res) {
		r.usage("monthly_requests", "test/req", 1e-6, "Requests")
	}}
	free["aws_test_net"] = true
}

func TestPrice(t *testing.T) {
	db := testDB(t)
	cfg := &terraform.Config{Source: "plan", Resources: []*terraform.Resource{
		{Address: "aws_test_vm.a", Type: "aws_test_vm", Cloud: "aws", Region: "us-east-1", Attrs: map[string]any{"size": "small", "disk_gb": 20.0}},
		{Address: "aws_test_vm.b", Type: "aws_test_vm", Cloud: "aws", Attrs: map[string]any{"size": terraform.Unknown{}}},
		{Address: "aws_test_vm.c", Type: "aws_test_vm", Cloud: "aws", Region: "eu-west-1", Attrs: map[string]any{"size": "big"}},
		{Address: "aws_test_vm.d", Type: "aws_test_vm", Cloud: "aws", Region: "us-east-1", Action: "update",
			Attrs: map[string]any{"size": "big", "disk_gb": "10"}, Before: map[string]any{"size": "small", "disk_gb": 10.0}},
		{Address: "aws_test_fn.f", Type: "aws_test_fn", Cloud: "aws", Region: "us-east-1"},
		{Address: "aws_test_net.n", Type: "aws_test_net", Cloud: "aws"},
		{Address: "aws_glue_job.j", Type: "aws_glue_job", Cloud: "aws"},
		{Address: "random_id.r", Type: "random_id"},
		{Address: "aws_test_vm.nodata", Type: "aws_test_vm", Cloud: "gcp", Region: "us-central1", Attrs: map[string]any{"size": "small"}},
		{Address: "data.aws_ami.x", Type: "aws_ami", Cloud: "aws", Data: true},
	}}
	e, err := Price(cfg, Options{DB: db, Usage: map[string]map[string]any{"aws_test_fn.f": {"monthly_requests": 3e6}}, Sizes: map[string]string{"aws_test_vm.a": "big"}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Row{}
	for _, r := range e.Rows {
		got[r.Address] = r
	}
	if len(e.Rows) != 9 {
		t.Fatalf("%d rows, want 9 (data sources are left out)", len(e.Rows))
	}
	// A cloud the price list has no date for has no prices at all: its rows say so, they don't "need" a value.
	if r := got["aws_test_vm.nodata"]; r.Status != Unpriced || len(r.Notes) != 1 || r.Notes[0] != "pogi has no Google Cloud prices yet" {
		t.Errorf("a cloud with no prices: %+v", r)
	}
	near := func(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }
	// a: the what-if picks "big" over the file's "small": 730 × 0.04 + 20 × 0.1.
	if r := got["aws_test_vm.a"]; !near(r.Monthly, 31.2) || r.Status != Known || r.Sizes.Current != "big" || r.Sizes.File != "small" {
		t.Errorf("a: %+v", r)
	}
	// b: the size is unknown and has no default, so the row needs it; the region is assumed.
	if r := got["aws_test_vm.b"]; r.Status != Needs || r.Region != "us-east-1" {
		t.Errorf("b: %+v", r)
	}
	// c: big isn't sold in eu-west-1.
	if r := got["aws_test_vm.c"]; r.Status != Needs {
		t.Errorf("c: %+v", r)
	}
	// d: priced before and after; "10" counts as 10.
	if r := got["aws_test_vm.d"]; !near(r.Monthly, 730*0.04+1) || !near(r.Before, 730*0.01+1) {
		t.Errorf("d: %+v", r)
	}
	if r := got["aws_test_fn.f"]; !near(r.Monthly, 0.6) || r.Status != Known || r.UsageKeys[0] != "monthly_requests" {
		t.Errorf("f: %+v", r)
	}
	for addr, want := range map[string]string{"aws_test_net.n": Free, "aws_glue_job.j": Unpriced, "random_id.r": Free} {
		if got[addr].Status != want {
			t.Errorf("%s: status %q, want %q", addr, got[addr].Status, want)
		}
	}
	if got["aws_glue_job.j"].Service != "glue" {
		t.Errorf("service of an unmapped type: %q", got["aws_glue_job.j"].Service)
	}

	// Without usage, a usage-only row says so and adds nothing.
	e, _ = Price(&terraform.Config{Resources: cfg.Resources[4:5]}, Options{DB: db})
	if r := e.Rows[0]; r.Status != Usage || r.Monthly != 0 {
		t.Errorf("no usage: %+v", r)
	}
}

func TestSizesCheapestFirst(t *testing.T) {
	db := testDB(t)
	s := db.Sizes("test/vm/linux", "us-east-1")
	if len(s) != 2 || s[0].Name != "small" || s[1].Price != 0.04 {
		t.Errorf("us-east-1: %+v", s)
	}
	if s := db.Sizes("test/vm/linux", "eu-west-1"); len(s) != 1 {
		t.Errorf("eu-west-1 sells only small: %+v", s)
	}
	if got := db.Regions("test"); len(got) != 2 {
		t.Errorf("regions: %v", got)
	}
}

func TestDecodeRejectsMisalignedColumn(t *testing.T) {
	db := &DB{Tables: map[string][]Size{"t": {{Name: "a"}}}, Columns: map[string]*Column{"t/x": {Table: "t", Prices: map[string][]float64{"r": {1, 2}}}}}
	b, _ := Encode(db)
	if _, err := Decode(b); err == nil {
		t.Error("a column with more prices than sizes was accepted")
	}
	if _, err := Decode([]byte("not gzip")); err == nil {
		t.Error("garbage was accepted")
	}
}

func TestDefaultLoads(t *testing.T) {
	if _, err := Default(); err != nil {
		t.Fatal(err)
	}
}

// A plan that deletes a resource saves its cost; the before side prices a group with its
// template's old size; a changed row pogi can't price is listed as unsure.
func TestPricePlanBeforeSide(t *testing.T) {
	db := testDB(t)
	mappings["aws_test_group"] = mapping{service: "Test", price: func(r *res) {
		if lt := r.dep("aws_test_template"); lt != nil {
			size, _ := lt.Attrs["size"].(string)
			r.hourly("test/vm/linux", size, HoursPerMonth, "Instance hours")
		}
	}}
	defer delete(mappings, "aws_test_group")
	cfg := &terraform.Config{Source: "plan", Resources: []*terraform.Resource{
		{Address: "aws_test_vm.gone", Block: "aws_test_vm.gone", Type: "aws_test_vm", Cloud: "aws", Region: "us-east-1", Action: "delete", Attrs: map[string]any{"size": "small", "disk_gb": 0.0}},
		{Address: "aws_test_template.t", Block: "aws_test_template.t", Type: "aws_test_net", Cloud: "aws", Action: "update", Attrs: map[string]any{"size": "big"}, Before: map[string]any{"size": "small"}},
		{Address: "aws_test_group.g", Block: "aws_test_group.g", Type: "aws_test_group", Cloud: "aws", Region: "us-east-1", Action: "no-op", Deps: []string{"aws_test_template.t"}},
		{Address: "aws_glue_job.new", Block: "aws_glue_job.new", Type: "aws_glue_job", Cloud: "aws", Action: "create"},
	}}
	// The template is free (aws_test_net) but its type stands in for a launch template here.
	cfg.Resources[1].Type = "aws_test_template"
	free["aws_test_template"] = true
	defer delete(free, "aws_test_template")
	e, err := Price(cfg, Options{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	gone, group := e.Rows[0], e.Rows[2]
	if gone.Monthly != 0 || gone.Before != 730*0.01 {
		t.Errorf("deleted: monthly %v before %v, want 0 and %v", gone.Monthly, gone.Before, 730*0.01)
	}
	if group.Monthly != 730*0.04 || group.Before != 730*0.01 {
		t.Errorf("group: monthly %v before %v; the before side should use the template's old size", group.Monthly, group.Before)
	}
	if u := e.Unsure(); len(u) != 1 || u[0] != "aws_glue_job.new" {
		t.Errorf("unsure: %v", u)
	}
}
