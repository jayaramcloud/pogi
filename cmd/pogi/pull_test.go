package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func stack(t *testing.T, tf string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(tf), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPullBadPathAndUsage(t *testing.T) {
	var out bytes.Buffer
	if err := pull([]string{filepath.Join(t.TempDir(), "nope")}, &out); err == nil {
		t.Error("a missing path pulled without error")
	}
	for _, args := range [][]string{nil, {"a", "b"}, {"--all", "x"}} {
		if err := pull(args, &out); err == nil || !strings.Contains(err.Error(), "usage") {
			t.Errorf("pull %v: err = %v, want a usage error", args, err)
		}
	}
}

func TestPullEmptyStack(t *testing.T) {
	var out bytes.Buffer
	dir := stack(t, `resource "aws_s3_bucket" "b" { bucket = "x" }`)
	if err := pull([]string{dir}, &out); err != nil || !strings.Contains(out.String(), "Nothing to pull") {
		t.Fatalf("err = %v, out = %q", err, out.String())
	}
}

func TestPullStackImages(t *testing.T) {
	dir := stack(t, `
resource "aws_db_instance" "db" {
  engine         = "mariadb"
  engine_version = "11.4"
}
resource "aws_db_instance" "pg" {
  engine         = "postgres"
  engine_version = "16.8"
}
resource "aws_db_instance" "again" {
  engine         = "mariadb"
  engine_version = "11.4"
}
resource "aws_elasticache_cluster" "c" { engine = "valkey" }
resource "google_sql_database_instance" "s" { database_version = "POSTGRES_16" }
resource "aws_s3_bucket" "b" { bucket = "x" }
`)
	ws, err := stackImages(dir)
	if err != nil {
		t.Fatal(err)
	}
	d, err := pullClient()
	if err != nil {
		t.Fatal(err)
	}
	got := resolve(d, ws)
	want := []string{"mariadb:11.4", "postgres:16-alpine", "valkey/valkey:8-alpine"}
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("images %v lack %s", got, w)
		}
	}
	if n := strings.Count(strings.Join(got, " "), "mariadb:11.4"); n != 1 {
		t.Errorf("mariadb:11.4 appears %d times, want once", n)
	}
	if len(got) != 3 { // Cloud SQL's POSTGRES_16 and RDS's 16.8 run the same image
		t.Errorf("images = %v", got)
	}
}

func TestPullNoDocker(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:1") // nothing listens
	var out bytes.Buffer
	dir := stack(t, `resource "aws_db_instance" "db" { engine = "mariadb" }`)
	err := pull([]string{dir}, &out)
	if err == nil || !strings.Contains(err.Error(), "Docker isn't running") {
		t.Fatalf("err = %v", err)
	}
	if err := pull([]string{"--all"}, &out); err == nil || !strings.Contains(err.Error(), "Docker isn't running") {
		t.Fatalf("--all: err = %v", err)
	}
}

// docker.images in pogi.yaml changes what pull downloads, as it changes what the services run.
func TestPullHonorsImageOverrides(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("POGI_CONFIG", "")
	if err := os.WriteFile("pogi.yaml", []byte("docker:\n  images:\n    rds.mariadb: registry.example/mariadb:11.4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ws, err := stackImages(stack(t, `resource "aws_db_instance" "db" {
  engine         = "mariadb"
  engine_version = "11.4"
}
resource "aws_db_instance" "pg" { engine = "postgres" }`))
	if err != nil {
		t.Fatal(err)
	}
	d, err := pullClient()
	if err != nil {
		t.Fatal(err)
	}
	got := resolve(d, ws)
	if !slices.Contains(got, "registry.example/mariadb:11.4") || slices.Contains(got, "mariadb:11.4") || len(got) != 2 {
		t.Errorf("images = %v", got)
	}
}
