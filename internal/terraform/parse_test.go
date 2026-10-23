package terraform

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// load reads a testdata folder and returns its resources by address.
func load(t *testing.T, dir string, varFiles ...string) (map[string]*Resource, *Config) {
	t.Helper()
	cfg, err := Load(filepath.Join("testdata", dir), varFiles)
	if err != nil {
		t.Fatalf("%s: %v", dir, err)
	}
	m := map[string]*Resource{}
	for _, r := range cfg.Resources {
		m[r.Address] = r
	}
	return m, cfg
}

func must(t *testing.T, m map[string]*Resource, addr string) *Resource {
	t.Helper()
	r := m[addr]
	if r == nil {
		keys := []string{}
		for k := range m {
			keys = append(keys, k)
		}
		t.Fatalf("no resource %s in %v", addr, keys)
	}
	return r
}

func hasWarning(cfg *Config, sub string) bool {
	for _, w := range cfg.Warnings {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}

func TestVariablesOrder(t *testing.T) {
	m, cfg := load(t, "vars", "prod.tfvars")
	r := must(t, m, "aws_instance.a[1]") // count_n = "2" is converted to a number
	if r.Attrs["instance_type"] != "medium" {
		t.Errorf("tfvars should beat the default: %v", r.Attrs["instance_type"])
	}
	if !reflect.DeepEqual(r.Attrs["tags"], map[string]any{"a": "1"}) {
		t.Errorf("default map: %v", r.Attrs["tags"])
	}
	// default < terraform.tfvars < *.auto.tfvars (a, then b) < --var-file
	if got := r.Attrs["user_data"]; got != "explicit-auto-b" {
		t.Errorf("order of variable files: %v", got)
	}
	if _, ok := r.Attrs["ami"].(Unknown); !ok {
		t.Errorf("a variable with no value should be Unknown: %v", r.Attrs["ami"])
	}
	if !hasWarning(cfg, `variable "nodefault" has no value: set it in a .tfvars file`) {
		t.Errorf("warnings: %v", cfg.Warnings)
	}
}

func TestMissingVarFile(t *testing.T) {
	_, err := Load("testdata/vars", []string{"nope.tfvars"})
	if err == nil || !strings.Contains(err.Error(), "nope.tfvars") {
		t.Errorf("want an error naming the file, got %v", err)
	}
}

func TestCountAndForEach(t *testing.T) {
	m, _ := load(t, "count")
	if v := must(t, m, `aws_ebs_volume.d["data"]`); v.Attrs["size"] != 100.0 || v.Block != "aws_ebs_volume.d" {
		t.Errorf("for_each over a map: %+v", v)
	}
	must(t, m, `aws_ebs_volume.d["logs"]`)
	must(t, m, `aws_sqs_queue.q["y"]`)
	if r := must(t, m, "aws_instance.web[0]"); r.Attrs["instance_type"] != "m5.large" {
		t.Errorf("count.index: %v", r.Attrs)
	}
	if r := must(t, m, "aws_instance.web[2]"); r.Attrs["instance_type"] != "t3.micro" {
		t.Errorf("count.index: %v", r.Attrs)
	}
	for a := range m {
		if strings.HasPrefix(a, "aws_instance.none") {
			t.Errorf("count = 0 should give no instances: %s", a)
		}
	}
	e := must(t, m, "aws_eip.e")
	if _, ok := e.Attrs["instance"].(Unknown); !ok {
		t.Errorf("a computed id is Unknown: %v", e.Attrs["instance"])
	}
	if !reflect.DeepEqual(e.Deps, []string{"aws_instance.web"}) {
		t.Errorf("deps: %v", e.Deps)
	}
}

func TestNestedAndDynamicBlocks(t *testing.T) {
	m, _ := load(t, "dynamic")
	a := must(t, m, "aws_instance.a")
	want := []any{map[string]any{"volume_size": 50.0}}
	if !reflect.DeepEqual(a.Attrs["root_block_device"], want) {
		t.Errorf("root_block_device: %#v", a.Attrs["root_block_device"])
	}
	sg := must(t, m, "aws_security_group.sg")
	in, _ := sg.Attrs["ingress"].([]any)
	if len(in) != 2 || in[1].(map[string]any)["from_port"] != 443.0 {
		t.Errorf("dynamic ingress: %#v", sg.Attrs["ingress"])
	}
	eg, _ := sg.Attrs["egress"].([]any)
	if len(eg) != 1 || eg[0].(map[string]any)["description"] != "a" {
		t.Errorf("dynamic egress with an iterator: %#v", sg.Attrs["egress"])
	}
}

func TestReferencesAndDeps(t *testing.T) {
	m, _ := load(t, "refs")
	sa := must(t, m, "azurerm_storage_account.sa")
	if sa.Attrs["location"] != "East US" || sa.Region != "eastus" || sa.Cloud != "azure" {
		t.Errorf("location through a reference: %v region %q", sa.Attrs["location"], sa.Region)
	}
	if sa.Attrs["resource_group_name"] != "rg1" {
		t.Errorf("through a local: %v", sa.Attrs["resource_group_name"])
	}
	if sa.Deps[0] != "azurerm_resource_group.rg" || len(sa.Deps) != 1 {
		t.Errorf("deps through a local: %v", sa.Deps)
	}
	i := must(t, m, "aws_instance.i")
	if !reflect.DeepEqual(i.Deps, []string{"aws_subnet.s", "azurerm_storage_account.sa"}) {
		t.Errorf("deps with depends_on: %v", i.Deps)
	}
	s := must(t, m, "aws_subnet.s")
	if !reflect.DeepEqual(s.Deps, []string{"data.aws_vpc.default"}) {
		t.Errorf("data deps: %v", s.Deps)
	}
	d := must(t, m, "data.aws_vpc.default")
	if !d.Data || d.Address != "data.aws_vpc.default" || d.Block != "data.aws_vpc.default" {
		t.Errorf("data source: %+v", d)
	}
	if sa.File != "main.tf" || sa.Line != 8 {
		t.Errorf("file and line: %s:%d", sa.File, sa.Line)
	}
}

func TestLocalModules(t *testing.T) {
	m, _ := load(t, "modules")
	v := must(t, m, "module.net.aws_ebs_volume.v")
	if v.Attrs["type"] != "gp3" || v.Module != "module.net" || v.Block != "module.net.aws_ebs_volume.v" {
		t.Errorf("module input: %+v", v)
	}
	b := must(t, m, `module.multi["b"].aws_ebs_volume.v`)
	if b.Attrs["type"] != "io2" || b.Block != "module.multi.aws_ebs_volume.v" || b.Module != "module.multi" {
		t.Errorf("module for_each: %+v", b)
	}
	if s := must(t, m, `module.multi["a"].aws_subnet.s`); s.Attrs["vpc_id"] != "v-a" {
		t.Errorf("each.key in a module input: %v", s.Attrs["vpc_id"])
	}
	// the input is a computed id, so Unknown; the dependency is followed through var and output.
	if s := must(t, m, "module.net.aws_subnet.s"); !reflect.DeepEqual(s.Deps, []string{"aws_vpc.main"}) {
		t.Errorf("deps through a module input: %v", s.Deps)
	}
	if app := must(t, m, "aws_instance.app"); !reflect.DeepEqual(app.Deps, []string{"module.net.aws_subnet.s"}) {
		t.Errorf("deps through a module output: %v", app.Deps)
	}
}

func TestDownloadedAndMissingModules(t *testing.T) {
	m, cfg := load(t, "downloaded")
	must(t, m, "module.vpc.aws_vpc.this")
	if !hasWarning(cfg, `module "missing" (git::https://example.com/x.git) is not downloaded: run terraform init once`) {
		t.Errorf("warnings: %v", cfg.Warnings)
	}
	if hasWarning(cfg, `module "vpc"`) || len(m) != 1 {
		t.Errorf("the downloaded module should parse: %v %v", cfg.Warnings, len(m))
	}
}

func TestRegion(t *testing.T) {
	m, _ := load(t, "region")
	for addr, want := range map[string]string{
		"aws_instance.default":                              "us-east-1",
		"aws_instance.west":                                 "us-west-2",
		"aws_instance.own":                                  "eu-central-1",
		"module.m.aws_instance.inside":                      "us-west-2", // providers = { aws = aws.west }
		"module.inherit.aws_instance.inside":                "us-east-1", // inherits the default provider
		"google_compute_instance.z":                         "us-central1",
		"google_compute_instance.p":                         "europe-west1",
		"google_container_cluster.r":                        "asia-east1",
		"google_container_cluster.zonal":                    "asia-east1",
		"google_storage_bucket.multi":                       "europe-west1", // "US" is not a region: the provider's
		"azurerm_resource_group.rg":                         "westeurope",
		"azurerm_mssql_database.db":                         "westeurope", // no location: its server's
		"azurerm_mssql_database_extended_auditing_policy.a": "westeurope", // through the database
	} {
		if got := must(t, m, addr).Region; got != want {
			t.Errorf("%s: region %q, want %q", addr, got, want)
		}
	}
}

func TestJSONSyntax(t *testing.T) {
	m, _ := load(t, "json")
	w := must(t, m, "aws_instance.web[1]")
	if w.Attrs["instance_type"] != "t3.micro" || !reflect.DeepEqual(w.Attrs["root_block_device"], []any{map[string]any{"volume_size": 20.0}}) {
		t.Errorf("json resource: %#v", w.Attrs)
	}
	if !reflect.DeepEqual(w.Deps, []string{"aws_vpc.main"}) {
		t.Errorf("json depends_on: %v", w.Deps)
	}
}

func TestFunctions(t *testing.T) {
	m, cfg := load(t, "functions")
	r := must(t, m, "aws_instance.f")
	for k, want := range map[string]any{
		"instance_type": "m5.large", "ami": "fallback", "user_data": "n-3", "key_name": "a,b,c",
		"subnet_id": "s2", "ok": true, "n": 3.0,
		"tags": map[string]any{"a": "1", "b": "X"},
	} {
		if !reflect.DeepEqual(r.Attrs[k], want) {
			t.Errorf("%s = %#v, want %#v", k, r.Attrs[k], want)
		}
	}
	for k, want := range map[string]string{"cidr": "10.0.1.0/24", "host": "10.0.1.254", "mask": "255.240.0.0", "v6": "fd00:0:0:3::/64", "b64": "hi"} {
		if r.Attrs[k] != want {
			t.Errorf("%s = %#v, want %q", k, r.Attrs[k], want)
		}
	}
	for _, k := range []string{"quiet", "odd"} {
		if _, ok := r.Attrs[k].(Unknown); !ok {
			t.Errorf("a function pogi doesn't run gives Unknown: %s = %#v", k, r.Attrs[k])
		}
	}
	if !hasWarning(cfg, "yamlencode()") || hasWarning(cfg, "file()") {
		t.Errorf("warnings: only yamlencode() should warn, got %v", cfg.Warnings)
	}
}

func TestErrors(t *testing.T) {
	for dir, want := range map[string]string{
		"err_syntax":     "main.tf:2:",
		"err_cycle":      "dependency cycle: ",
		"err_empty":      "no .tf files found",
		"err_selfmodule": "nested more than",
	} {
		_, err := Load(filepath.Join("testdata", dir), nil)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want an error with %q, got %v", dir, want, err)
		}
	}
	_, err := Load("testdata/err_cycle", nil)
	if err == nil || !strings.Contains(err.Error(), "aws_instance.a") || !strings.Contains(err.Error(), "aws_subnet.b") {
		t.Errorf("a cycle should name the resources: %v", err)
	}
}

func TestLimits(t *testing.T) {
	files := map[string][]byte{}
	for i := 0; i <= MaxFiles; i++ {
		files[fmt.Sprintf("f%d.tf", i)] = nil
	}
	if _, err := Parse(files, nil); err == nil || !strings.Contains(err.Error(), "too many files") {
		t.Errorf("file limit: %v", err)
	}
	if _, err := Parse(map[string][]byte{"main.tf": make([]byte, MaxBytes+1)}, nil); err == nil || !strings.Contains(err.Error(), "too big") {
		t.Errorf("size limit: %v", err)
	}
	huge := map[string][]byte{"main.tf": []byte(`resource "aws_instance" "a" { count = 1000000000 }`)}
	if _, err := Parse(huge, nil); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Errorf("count limit: %v", err)
	}
}

// Every example folder must parse; warnings are fine.
func TestExamplesParse(t *testing.T) {
	dirs, _ := filepath.Glob("../../examples/terraform*")
	if len(dirs) == 0 {
		t.Fatal("no examples found")
	}
	for _, d := range dirs {
		cfg, err := Load(d, nil)
		if err != nil || len(cfg.Resources) == 0 {
			t.Errorf("%s: %d resources, error %v", d, len(cfg.Resources), err)
		}
	}
}

func TestLoadFile(t *testing.T) {
	if _, err := Load("testdata/vars/main.tf", nil); err == nil || !strings.Contains(err.Error(), "plan JSON") {
		t.Errorf("a .tf file should be refused with a hint: %v", err)
	}
}

func TestLoadSkipsSymlinksAndHiddenFolders(t *testing.T) {
	outside, dir := t.TempDir(), t.TempDir()
	write := func(p, s string) {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(dir, "main.tf"), `resource "aws_sqs_queue" "q" {}`)
	write(filepath.Join(outside, "secret.tf"), `resource "aws_sqs_queue" "leak" {}`)
	write(filepath.Join(dir, ".git", "x.tf"), `resource "aws_sqs_queue" "git" {}`)
	write(filepath.Join(dir, "node_modules", "x.tf"), `resource "aws_sqs_queue" "node" {}`)
	write(filepath.Join(dir, ".terraform", "providers", "x.tf"), `resource "aws_sqs_queue" "prov" {}`)
	write(filepath.Join(dir, "vars", "abs.tfvars"), `x = 1`)
	if err := os.Symlink(filepath.Join(outside, "secret.tf"), filepath.Join(dir, "link.tf")); err != nil {
		t.Skip("no symlinks here")
	}
	if err := os.Symlink(outside, filepath.Join(dir, "linkdir")); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(dir, []string{filepath.Join(dir, "vars", "abs.tfvars")}) // an absolute var file works
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Resources) != 1 || cfg.Resources[0].Name != "q" {
		t.Errorf("only main.tf should be read: %+v", cfg.Resources)
	}
}

func TestNestingTooDeepIsRefused(t *testing.T) {
	deep := strings.Repeat("(", MaxDepth+1) + "1" + strings.Repeat(")", MaxDepth+1)
	_, err := Parse(map[string][]byte{"main.tf": []byte("locals {\n a = " + deep + "\n}\n")}, nil)
	if err == nil || !strings.Contains(err.Error(), "main.tf:2: nested too deep (over 200 levels)") {
		t.Errorf("deep file: %v", err)
	}
	ok := "locals {\n a = " + strings.Repeat("(", 50) + "1" + strings.Repeat(")", 50) + " # ((((((\n b = \"((((\"\n}\n" + strings.Repeat("// (((((\n", 300)
	if _, err := Parse(map[string][]byte{"main.tf": []byte(ok)}, nil); err != nil {
		t.Errorf("ordinary file refused: %v", err)
	}
}
