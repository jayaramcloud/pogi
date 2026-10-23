package cost

import (
	"strings"
	"testing"
)

func TestReadUsage(t *testing.T) {
	u, err := ReadUsage([]byte("aws_lambda_function.api: {monthly_requests: 2000000, avg_duration_ms: 12.5}\naws_instance.web: {os: windows}\nmodule.m.aws_s3_bucket.b: {storage_gb: 0}\n"))
	if err != nil || u["aws_instance.web"]["os"] != "windows" || u["aws_lambda_function.api"]["monthly_requests"] != 2000000 {
		t.Fatalf("got %v, %v", u, err)
	}
	for in, want := range map[string]string{
		"nodots: {a_b: 1}":         `"nodots" is not a resource address`,
		"aws_x.y: {MonthlyReq: 1}": `aws_x.y: key "MonthlyReq"`,
		"aws_x.y: {a_b: -1}":       "aws_x.y: a_b must not be negative",
		"aws_x.y: {a_b: lots}":     "aws_x.y: a_b must be a number",
		"aws_x.y: {os: beos}":      "aws_x.y: os must be linux or windows",
		"aws_x.y: [1, 2]":          "usage file:",
		"aws_x.y: {a_b: 1":         "usage file:",
		strings.Repeat("#", 2<<20): "larger than 1 MB",
	} {
		if _, err := ReadUsage([]byte(in)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%.30q: want error with %q, got %v", in, want, err)
		}
	}
}

func TestUsageTemplate(t *testing.T) {
	e := &Estimate{Rows: []Row{
		{Address: "b.x", UsageKeys: []string{"zeta", "alpha"}},
		{Address: "a.x", UsageKeys: []string{"os", "gb"}},
		{Address: "c.x"},
	}}
	got := string(UsageTemplate(e))
	want := "a.x:\n  gb: 0\n  os: linux\nb.x:\n  alpha: 0\n  zeta: 0\n"
	if !strings.HasPrefix(got, "#") || !strings.HasSuffix(got, want) || strings.Contains(got, "c.x") {
		t.Fatalf("template:\n%s", got)
	}
	if _, err := ReadUsage([]byte(got)); err != nil {
		t.Fatalf("the template doesn't read back: %v", err)
	}
}
