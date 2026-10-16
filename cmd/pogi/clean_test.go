package main

import (
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
