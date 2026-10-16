package core

import (
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/docker"
)

func TestFormatShutdown(t *testing.T) {
	items := []closed{
		{"rds", "container", "stopped", "users-db", 0},
		{"lambda", "process", "stopped", "resize", 0},
		{"rds", "container", "stopped", "orders-db", 0},
		{"ses", "port", "closed", "SMTP", 2525},
		{"ecr", "container", "removed", "4510", 0},
	}
	for i := range 7 { // a long list is cut
		items = append(items, closed{"sqs", "process", "stopped", string(rune('a' + i)), 0})
	}
	got := formatShutdown(items, "/srv/pogi", 3100*time.Millisecond)
	got = squash(got)
	for _, want := range []string{
		"pogi stopped:\n",
		"ecr 1 container removed 4510\n",
		"lambda 1 process stopped resize\n",
		"rds 2 containers stopped orders-db, users-db\n",
		"ses port 2525 closed SMTP\n",
		"sqs 7 processes stopped a, b, c, d, e, +2 more\n",
		"state saved to /srv/pogi\n",
		"done in 3.1s\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if none := squash(formatShutdown(nil, "", time.Second)); none != "pogi stopped:\ndone in 1.0s\n" {
		t.Errorf("nothing to report: %q", none)
	}
}

// Containers that ran before shutdown are reported as stopped when they are
// still there (saved state) and as removed when they are gone; the record
// stays empty until shutdown begins.
func TestRecordContainers(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	run := func(id, res string) docker.Summary {
		return docker.Summary{ID: id, State: "running", Labels: map[string]string{docker.LabelService: "rds", docker.LabelResource: res}}
	}
	before := []docker.Summary{run("1", "kept"), run("2", "gone"), run("3", "was-stopped")}
	before[2].State = "exited"
	after := []docker.Summary{{ID: "1", State: "exited"}, {ID: "3", State: "exited"}}

	h.recordContainers(before, after)
	if len(h.shut.items) != 0 {
		t.Fatal("recorded before shutdown began")
	}
	h.BeginShutdown()
	h.recordContainers(before, after)
	got := squash(h.ShutdownList())
	if !strings.Contains(got, "rds 1 container stopped kept\n") || !strings.Contains(got, "rds 1 container removed gone\n") || strings.Contains(got, "was-stopped") {
		t.Fatalf("list:\n%s", got)
	}
}

// squash makes every line single-spaced and unindented, so a test does not depend on column widths.
func squash(s string) string {
	var out strings.Builder
	for _, l := range strings.Split(s, "\n") {
		if l != "" {
			out.WriteString(strings.Join(strings.Fields(l), " ") + "\n")
		}
	}
	return out.String()
}
