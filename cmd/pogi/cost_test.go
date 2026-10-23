package main

import (
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/cost"
)

func handBuilt(source string) *cost.Estimate {
	e := &cost.Estimate{
		Source: source, Version: "1.9.5", Published: map[string]string{"aws": "2026-10-20"},
		Total: 1123.45, Before: 1000, Clouds: map[string]float64{"aws": 1123.45},
		Warnings: []string{"module ./net was not downloaded"},
		Rows: []cost.Row{
			{Address: "aws_instance.web", Service: "EC2", Action: "create", Monthly: 60.74, Status: cost.Known,
				Lines: []cost.Line{{What: "m7g.large", Qty: 730, Unit: "hour", UnitPrice: 0.0832, Monthly: 60.74}}},
			{Address: "aws_db_instance.db", Service: "RDS", Action: "update", Before: 1000, Monthly: 1062.71, Status: cost.Assumed, Notes: []string{"assumed 8 GB root disk"}},
			{Address: "aws_lambda_function.f", Service: "Lambda", Action: "replace", Status: cost.Usage, Notes: []string{"no usage given"},
				Lines: []cost.Line{{What: "Requests", Unit: "1M requests", UnitPrice: 0.2, Usage: true}}},
			{Address: "aws_s3_bucket.old", Service: "S3", Action: "delete", Before: 5, Status: cost.Needs, Notes: []string{"needs a value for storage_gb"}},
			{Address: "aws_vpc.main", Status: cost.Free},
			{Address: "aws_glue_job.j", Status: cost.Unpriced},
		},
	}
	return e
}

func TestCostTable(t *testing.T) {
	var b bytes.Buffer
	writeTable(&b, handBuilt("plan"), "./plan.json", false)
	got := b.String()
	for _, want := range []string{"Source: plan ./plan.json (Terraform 1.9.5)", "AWS prices of 2026-10-20", "730 h × $0.0832 m7g.large",
		"+ create", "~ update", "-/+ replace", "− delete", "assumed", "needs", "aws_db_instance.db: assumed 8 GB root disk",
		"$1,062.71", "This plan adds $123.45 a month (from $1,000.00 to $1,123.45)", "Free: aws_vpc.main",
		"Not priced: aws_glue_job.j", "real bill could be higher", "warning: module ./net was not downloaded",
		"$0.2 per 1M requests"} {
		if !strings.Contains(got, want) {
			t.Errorf("table lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "\x1b[") {
		t.Error("escape codes written when not a terminal")
	}
	b.Reset()
	writeTable(&b, handBuilt("folder"), "./infra", true)
	if !strings.Contains(b.String(), "\x1b[1m") || strings.Contains(b.String(), "This plan") {
		t.Errorf("folder table on a terminal:\n%s", b.String())
	}
}

func TestCostMarkdownAndJSON(t *testing.T) {
	var b bytes.Buffer
	writeMarkdown(&b, handBuilt("plan"), "./plan.json")
	for _, want := range []string{"| Action | Resource |", "| + create | `aws_instance.web` |", "**Total: $1,123.45 a month**", "This plan adds $123.45", "Not priced"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("markdown lacks %q:\n%s", want, b.String())
		}
	}
	// the plan sentence for a drop and for no change
	e := handBuilt("plan")
	e.Total = 900
	if got := planLine(e); !strings.Contains(got, "saves $100.00") {
		t.Error(got)
	}
	e.Total = 1000
	if got := planLine(e); !strings.Contains(got, "does not change") {
		t.Error(got)
	}
	// A changed row pogi can't price is named, so the sentence never claims more than was priced.
	e.Rows = append(e.Rows, cost.Row{Address: "aws_glue_job.new", Action: "create", Status: cost.Unpriced})
	if got := planLine(e); !strings.Contains(got, "not counting 2 changed resources pogi can't price: aws_s3_bucket.old, aws_glue_job.new") {
		t.Error(got)
	}
}

func TestCostFlagErrors(t *testing.T) {
	for args, want := range map[string]string{
		"./infra --format xml":            `unknown --format "xml"`,
		"./infra --size aws_instance.web": "address=size",
		"./infra --size =m7g.large":       "address=size",
		"./infra --var-file":              "needs an argument",
		"./infra --usage /srv/none.yaml":  "usage file",
		"/srv/nowhere":                    "/srv/nowhere was not found",
		"a b":                             "one path at a time",
		"--usage u.yaml":                  "need a path",
		"./infra --bogus":                 "not defined",
		"sizes":                           "usage: pogi cost sizes",
		"sizes aws_nothing":               "no size list",
		"sizes aws_instance --os beos":    "use linux or windows",
	} {
		a := strings.Fields(args)
		var err error
		if a[0] == "sizes" {
			err = costSizes(&bytes.Buffer{}, a[1:])
		} else {
			err = costRun(&bytes.Buffer{}, false, a)
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: want error with %q, got %v", args, want, err)
		}
	}
	if err := costSizes(&bytes.Buffer{}, []string{"nope"}); err == nil || !strings.Contains(err.Error(), "Types with one") {
		t.Errorf("unknown type error: %v", err)
	}
}

// needsPrices skips a test until the clouds' price mappings are merged.
func needsPrices(t *testing.T) {
	t.Helper()
	if !slices.Contains(cost.SizeTypes(), "aws_instance") {
		t.Skip("the AWS price mappings are not merged yet")
	}
}

func TestCostSizeErrors(t *testing.T) {
	needsPrices(t)
	if err := costSizes(&bytes.Buffer{}, []string{"aws_instance", "--region", "mars"}); err == nil || !strings.Contains(err.Error(), "unknown aws region") {
		t.Errorf("unknown region: %v", err)
	}
	if err := costSizes(&bytes.Buffer{}, []string{"nope"}); err == nil || !strings.Contains(err.Error(), "aws_instance") {
		t.Errorf("unknown type should list the types: %v", err)
	}
}

func TestCostSizes(t *testing.T) {
	needsPrices(t)
	var b bytes.Buffer
	if err := costSizes(&b, []string{"aws_instance", "--region", "eu-west-1", "--vcpu", "4", "--memory", "16"}); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if !strings.Contains(out, "SIZE") || !strings.Contains(out, "$/MONTH") || strings.Contains(out, " 2  ") && !strings.Contains(out, "VCPU") {
		t.Errorf("sizes output:\n%s", out)
	}
	if err := costSizes(&bytes.Buffer{}, []string{"aws_instance", "--vcpu", "100000"}); err == nil || !strings.Contains(err.Error(), "no aws_instance sizes match") {
		t.Errorf("nothing matches: %v", err)
	}
}

func TestCostNoPath(t *testing.T) {
	err := costRun(&bytes.Buffer{}, false, []string{"--endpoint", "http://127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "cannot reach pogi") || !strings.Contains(err.Error(), "pogi cost ./infra") {
		t.Fatalf("pogi not running: %v", err)
	}
	srv := pogi.Start(t)
	var b bytes.Buffer
	if err := costRun(&b, false, []string{"--endpoint", srv.URL}); err != nil || !strings.Contains(b.String(), "Total  $") {
		t.Fatalf("table: %v\n%s", err, b.String())
	}
	b.Reset()
	if err := costRun(&b, false, []string{"--endpoint", srv.URL, "--format", "json"}); err != nil || !json.Valid(b.Bytes()) {
		t.Fatalf("json: %v\n%s", err, b.String())
	}
	if err := costRun(&b, false, []string{"--endpoint", srv.URL, "--region", "mars-1"}); err == nil || !strings.Contains(err.Error(), "unknown region") {
		t.Fatalf("bad region: %v", err)
	}
	if err := costRun(&b, false, []string{"--endpoint", srv.URL, "--format", "markdown"}); err == nil {
		t.Fatal("markdown with no path should fail")
	}
}

// A what-if or usage entry for an address that isn't in the folder must not pass silently.
func TestCostUnknownAddresses(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/main.tf", []byte("resource \"aws_instance\" \"web\" {\n  instance_type = \"t3.micro\"\n  ami = \"ami-1\"\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/u.yaml", []byte("aws_instance.ghost: {os: windows}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	err := costRun(&b, false, []string{dir, "--size", "aws_instance.ghost=m7g.large"})
	if err == nil || !strings.Contains(err.Error(), "aws_instance.ghost") {
		t.Fatalf("unknown --size address: %v", err)
	}
	b.Reset()
	if err := costRun(&b, false, []string{dir, "--usage", dir + "/u.yaml"}); err != nil || !strings.Contains(b.String(), "warning: usage file: aws_instance.ghost matches no resource") {
		t.Fatalf("unknown usage address: %v\n%s", err, b.String())
	}
	b.Reset()
	if err := costRun(&b, false, []string{dir, "--size", "aws_instance.web=zz.huge"}); err != nil || !strings.Contains(b.String(), "no price for zz.huge") {
		t.Fatalf("unknown size value: %v\n%s", err, b.String())
	}
	// the same for a folder with a tfvars file
	os.WriteFile(dir+"/prod.tfvars", []byte("x = 1\n"), 0o644)
	if err := costRun(&b, false, []string{dir, "--var-file", "prod.tfvars"}); err != nil {
		t.Fatalf("--var-file: %v", err)
	}
	if err := costRun(&b, false, []string{dir, "--var-file", "missing.tfvars"}); err == nil {
		t.Fatal("a missing --var-file should fail")
	}
}
