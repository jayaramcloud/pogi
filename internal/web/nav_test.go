package web

import (
	"os"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

func TestDisambiguate(t *testing.T) {
	inUse := []navSvc{
		{Name: "iam", Title: "IAM", Cloud: "aws", Full: "IAM · AWS"},
		{Name: "gcpiam", Title: "IAM", Cloud: "gcp", Full: "IAM · GCP"},
		{Name: "secretsmanager", Title: "Secrets Manager", Cloud: "aws", Full: "Secrets Manager · AWS"},
		{Name: "secretmanager", Title: "Secret Manager", Cloud: "gcp", Full: "Secret Manager · GCP"},
		{Name: "s3", Title: "S3", Cloud: "aws", Full: "S3 · AWS"},
	}
	pinned := []navSvc{inUse[1]} // a pinned service already in use is one service, not a clash
	disambiguate(inUse, pinned)
	got := map[string]string{}
	for _, s := range inUse {
		got[s.Name] = s.Title
	}
	for name, w := range map[string]string{
		"iam": "IAM · AWS", "gcpiam": "IAM · GCP", // same title: the label says the cloud
		"secretsmanager": "Secrets Manager", "secretmanager": "Secret Manager", "s3": "S3", // no clash: untouched
	} {
		if got[name] != w {
			t.Errorf("%s = %v, want %v", name, got[name], w)
		}
	}
	if pinned[0].Title != "IAM · GCP" {
		t.Errorf("the pinned copy = %q", pinned[0].Title)
	}
}

func TestPerMonth(t *testing.T) {
	for v, w := range map[float64]string{0.004: "<$1", 0.99: "<$1", 1: "$1.0", 4.25: "$4.2", 29.4: "$29", 1234: "$1234"} {
		if got := perMonth(v); got != w {
			t.Errorf("perMonth(%v) = %q, want %q", v, got, w)
		}
	}
}

// The count is the README's: products from coverage.json, with the depth split, never a hard-coded number.
func TestServiceCount(t *testing.T) {
	var all []core.ServiceStatus
	for id, c := range coverage().Services {
		all = append(all, core.ServiceStatus{Name: id, Cloud: c.Cloud})
	}
	n, full := serviceCount("", all)
	if readme, err := os.ReadFile("../../README.md"); err == nil && !strings.Contains(string(readme), full) {
		t.Errorf("README does not say %q", full)
	}
	if n < 300 || !strings.Contains(full, "Thin") {
		t.Errorf("all clouds: %d %q", n, full)
	}
	if n, full := serviceCount("aws", all); n != coverage().Counts["aws"].Total || !strings.Contains(full, " AWS services (") {
		t.Errorf("aws: %d %q", n, full)
	}
	// pogi started with some services only: the products can't be counted, the running services are
	if n, full := serviceCount("", all[:3]); n != 3 || full != "3 services running" {
		t.Errorf("subset: %d %q", n, full)
	}
	// The start banner shows the same count as the console.
	if got := ServiceCount(all); got != n {
		t.Errorf("ServiceCount(all) = %d, want %d", got, n)
	}
	if got := ServiceCount(all[:3]); got != 3 {
		t.Errorf("ServiceCount(subset) = %d, want 3", got)
	}
}

func TestDepthTag(t *testing.T) {
	for _, d := range []string{"Deep", "core", "Thin"} {
		got := string(depthTag(d, false))
		if !strings.Contains(got, `title="`) || !strings.Contains(got, `class="sr"`) || strings.Contains(got, "<a ") {
			t.Errorf("%s: %s", d, got)
		}
	}
	if got := string(depthTag("Deep", true)); !strings.Contains(got, `href="`+coverageURL+`"`) || !strings.Contains(got, `rel="noopener"`) {
		t.Errorf("link: %s", got)
	}
	if depthTag("", true) != "" {
		t.Error("no depth, no badge")
	}
}
