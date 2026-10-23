package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/docker"
)

// pogi clean picks what a gone pogi left, never what a live pogi owns or what pogi did not make.
func TestCleanSelection(t *testing.T) {
	host, _ := os.Hostname()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	dead := host + "/" + strconv.Itoa(cmd.Process.Pid) + "/x"
	live := host + "/" + strconv.Itoa(os.Getpid()) + "/x"
	inUse := t.TempDir()
	label := func(owner, data string) map[string]string {
		l := map[string]string{docker.LabelManaged: "true", docker.LabelOwner: owner, docker.LabelService: "rds"}
		if data != "" {
			l[docker.LabelData] = data
		}
		return l
	}
	list := []docker.Summary{
		{ID: "dead", Names: []string{"/pogi-rds-a"}, Labels: label(dead, "")},
		{ID: "live", Names: []string{"/pogi-rds-b"}, Labels: label(live, "")},
		{ID: "other", Names: []string{"/redis"}, Labels: map[string]string{"app": "x"}},
		{ID: "unlabelled-owner", Names: []string{"/nope"}, Labels: map[string]string{docker.LabelOwner: dead}},
		{ID: "taken-over", Names: []string{"/pogi-rds-c"}, Labels: label(dead, inUse)},
	}
	labels := func(c docker.Summary) map[string]string { return c.Labels }
	os.WriteFile(filepath.Join(inUse, ".pogi-owner"), []byte(live), 0o600) // a pogi runs on inUse
	got := orphans(list, labels)
	if len(got) != 1 || got[0].ID != "dead" {
		t.Fatalf("picked %v, want only the dead owner's", got)
	}
	os.WriteFile(filepath.Join(inUse, ".pogi-owner"), []byte(dead), 0o600) // that pogi stopped
	if got := orphans(list, labels); len(got) != 2 || got[1].ID != "taken-over" {
		t.Fatalf("with no pogi running: %v", got)
	}
	vols := orphans([]docker.Volume{{Name: "v1", Labels: label(dead, "")}, {Name: "v2", Labels: label(live, "")}, {Name: "v3"}}, func(v docker.Volume) map[string]string { return v.Labels })
	if len(vols) != 1 || vols[0].Name != "v1" {
		t.Fatalf("volumes: %v", vols)
	}
	out := describeLeft(got, vols)
	if !strings.Contains(strings.Join(strings.Fields(out), " "), "rds 1 container pogi-rds-a") || !strings.Contains(out, "1 volume") {
		t.Fatalf("description:\n%s", out)
	}
}

// Containers and volumes kept for another data dir's saved state are listed by data dir, with the command that
// deletes them, and are not part of what this run deletes.
func TestCleanListsKeptForSavedState(t *testing.T) {
	l := func(data string) map[string]string {
		m := map[string]string{docker.LabelManaged: "true", docker.LabelService: "rds"}
		if data != "" {
			m[docker.LabelData] = data
		}
		return m
	}
	labels := func(c docker.Summary) map[string]string { return c.Labels }
	cs := []docker.Summary{{ID: "1", Names: []string{"/pogi-rds-a"}, Labels: l("")}, {ID: "2", Names: []string{"/pogi-rds-b"}, Labels: l("/d/one")},
		{ID: "3", Names: []string{"/pogi-rds-c"}, Labels: l("/d/mine")}}
	mine, kept := splitKept(cs, "/d/mine", labels)
	if len(mine) != 2 || len(kept) != 1 || len(kept["/d/one"]) != 1 {
		t.Fatalf("mine %v kept %v", mine, kept)
	}
	vols, keptV := splitKept([]docker.Volume{{Name: "pogi-rds-b-data", Labels: l("/d/one")}}, "/d/mine", func(v docker.Volume) map[string]string { return v.Labels })
	if len(vols) != 0 {
		t.Fatalf("volumes kept for /d/one were picked: %v", vols)
	}
	out := describeKept(kept, keptV)
	for _, want := range []string{"/d/one", "pogi-rds-b", "pogi-rds-b-data", "pogi clean --data-dir /d/one"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// Without a terminal pogi clean cannot ask, so it needs --yes or --dry-run, and says so. The check
// comes before Docker or any file is touched.
func TestCleanWithoutTerminalNeedsAFlag(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	old := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = old }()
	err = clean([]string{"--data-dir", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "--yes") || !strings.Contains(err.Error(), "--dry-run") || !errors.As(err, new(usageError)) {
		t.Fatalf("got %v, want a usage error naming --yes and --dry-run", err)
	}
}

// Only the deletes that worked are counted, and each failure says why.
func TestDeleteEachCountsFailures(t *testing.T) {
	var w strings.Builder
	ok, failed := deleteEach(&w, []string{"a", "b", "c"}, func(s string) string { return "container " + s },
		func(s string) error {
			if s == "b" {
				return errors.New("409 Conflict: in use")
			}
			return nil
		})
	if ok != 2 || failed != 1 || w.String() != "could not delete container b: 409 Conflict: in use\n" {
		t.Fatalf("got %d deleted, %d failed, %q", ok, failed, w.String())
	}
}
