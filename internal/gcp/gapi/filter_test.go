package gapi_test

import (
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

func TestFilter(t *testing.T) {
	fields := map[string]string{"severity": "ERROR", "resource.type": "gce", "labels.a.b": "x y", "ts": "2026-01-02T03:04:05Z"}
	run := func(f string) bool {
		t.Helper()
		p, err := gapi.ParseFilter(f)
		if err != nil {
			t.Fatalf("%q: %v", f, err)
		}
		return p.Match(func(field, op, v string) bool {
			got := fields[field]
			switch op {
			case "=":
				return got == v
			case "!=":
				return got != v
			case ">=":
				return got >= v
			case ":":
				return strings.Contains(got, v)
			case "starts_with":
				return strings.HasPrefix(got, v)
			case "!starts_with":
				return !strings.HasPrefix(got, v)
			case "one_of":
				return strings.Contains(v, got)
			case "()":
				return field == "log_id" && v == "app"
			}
			return false
		}, func(s string) bool { return s == "boom" })
	}
	for f, want := range map[string]bool{
		``: true, `severity=ERROR`: true, `severity = "ERROR" AND resource.type=gce`: true, `severity=ERROR resource.type=gke`: false,
		`severity=WARN OR resource.type=gce`: true, `NOT severity=ERROR`: false, `-severity=WARN`: true,
		`(severity=WARN OR severity=ERROR) AND boom`: true, `boom AND "nope"`: false, `labels."a.b":"x "`: true,
		`resource.type = starts_with("gc")`: true, `resource.type != starts_with("gc")`: false,
		`severity = one_of("ERROR","WARN")`: true, `log_id("app")`: true, `ts>=2026-01-02T00:00:00Z`: true,
		`severity=WARN AND boom OR resource.type=gce`: false, `severity>=A`: true,
	} {
		if got := run(f); got != want {
			t.Errorf("%q = %v, want %v", f, got, want)
		}
	}
	for _, bad := range []string{`severity=`, `(severity=ERROR`, `a "b`, `x = fn(1) )`} {
		if _, err := gapi.ParseFilter(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}
