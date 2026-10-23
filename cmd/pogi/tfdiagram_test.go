package main

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	tfmodel "github.com/nrndev/pogi/internal/terraform"
)

const tfdMain = `
resource "aws_vpc" "main" { cidr_block = "10.0.0.0/16" }
resource "aws_subnet" "a" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.1.0/24"
}
resource "aws_instance" "web" {
  ami           = "ami-1"
  instance_type = "t3.micro"
  subnet_id     = aws_subnet.a.id
}
`

func tfdFolder(t *testing.T, main string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestTFDiagramFile(t *testing.T) {
	dir := tfdFolder(t, tfdMain)
	svg := filepath.Join(t.TempDir(), "d.svg")
	var out bytes.Buffer
	// no pogi is running, and -o needs none
	if err := tfDiagramCmd(&out, true, []string{dir, "-o", svg, "--group", "network", "--endpoint", "http://127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(svg)
	for _, want := range []string{`<svg xmlns="http://www.w3.org/2000/svg"`, `band frame net`, `band frame sub`, "instance"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the SVG lacks %q", want)
		}
	}
	if strings.Contains(string(b), "var(--") || !strings.Contains(out.String(), svg) {
		t.Errorf("not a standalone file, or nothing said: %q", out.String())
	}
	// a var file may be anywhere with -o
	vf := filepath.Join(t.TempDir(), "x.tfvars")
	os.WriteFile(vf, nil, 0o644)
	if err := tfDiagramCmd(&out, true, []string{dir, "-o", svg, "--var-file", vf}); err != nil {
		t.Errorf("var file outside the folder with -o: %v", err)
	}
}

func TestTFDiagramErrors(t *testing.T) {
	dir := tfdFolder(t, tfdMain)
	big := t.TempDir()
	f, _ := os.Create(filepath.Join(big, "main.tf"))
	f.Truncate(tfmodel.MaxBytes + 1) // a sparse file: the reader checks the size before it reads
	f.Close()
	outside := filepath.Join(t.TempDir(), "x.tfvars")
	os.WriteFile(outside, nil, 0o644)
	srv := pogi.Start(t)
	tests := []struct {
		name string
		args []string
		says string
	}{
		{"pogi not running", []string{dir, "--endpoint", "http://127.0.0.1:1"}, "pogi isn't running at http://127.0.0.1:1; start it with `pogi start`, or write a file with -o"},
		{"bad path", []string{"/srv/nowhere", "--endpoint", srv.URL}, "/srv/nowhere was not found"},
		{"bad path with -o", []string{"/srv/nowhere", "-o", "x.svg"}, "/srv/nowhere was not found"},
		{"too big", []string{big, "--endpoint", srv.URL}, "too big"},
		{"-o without .svg", []string{dir, "-o", "x.png"}, "-o must name a .svg file"},
		{"var file outside", []string{dir, "--var-file", outside, "--endpoint", srv.URL}, "must be inside the folder"},
		{"var file up", []string{dir, "--var-file", "../x.tfvars", "--endpoint", srv.URL}, "must be inside the folder"},
		{"bad group", []string{dir, "--group", "colour"}, "unknown --group"},
		{"two paths", []string{dir, dir}, "one path at a time"},
		{"pogi says no", []string{tfdFolder(t, `resource "aws_vpc" "a" {`), "--endpoint", srv.URL}, "422"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tfDiagramCmd(io.Discard, true, tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("got %v, want %q", err, tc.says)
			}
			if strings.Contains(err.Error(), "<div") {
				t.Fatalf("the error shows HTML: %v", err)
			}
		})
	}
}

func TestTFDiagramSend(t *testing.T) {
	srv := pogi.Start(t)
	dir := tfdFolder(t, tfdMain)
	var out bytes.Buffer
	if err := tfDiagramCmd(&out, true, []string{dir, "--no-open", "--group", "network", "--endpoint", srv.URL + "/"}); err != nil {
		t.Fatal(err)
	}
	if out.String() != srv.URL+"/_pogi/terraform\n" {
		t.Fatalf("--no-open printed %q", out.String())
	}
	res, err := http.Get(srv.URL + "/_pogi/terraform") // a reload shows the stack with no new post
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, want := range []string{"aws_instance", "band frame sub", `value="network" selected`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the page lacks %q", want)
		}
	}
	// without --no-open the browser is opened on that address, and a failure still prints it
	var opened string
	defer func(old func(string) error) { openURL = old }(openURL)
	openURL = func(u string) error { opened = u; return nil }
	out.Reset()
	if err := tfDiagramCmd(&out, true, []string{dir, "--endpoint", srv.URL}); err != nil || opened != srv.URL+"/_pogi/terraform" {
		t.Fatalf("open: %v %q", err, opened)
	}
	openURL = func(string) error { return io.ErrClosedPipe }
	out.Reset()
	if err := tfDiagramCmd(&out, true, []string{dir, "--endpoint", srv.URL}); err != nil || !strings.Contains(out.String(), srv.URL+"/_pogi/terraform") {
		t.Fatalf("browser failed: %v %q", err, out.String())
	}
	// without a terminal, as in CI, no browser is opened: the address is printed, as with --no-open
	opened = ""
	openURL = func(u string) error { opened = u; return nil }
	out.Reset()
	if err := tfDiagramCmd(&out, false, []string{dir, "--endpoint", srv.URL}); err != nil || opened != "" || out.String() != srv.URL+"/_pogi/terraform\n" {
		t.Fatalf("no terminal: %v, opened %q, printed %q", err, opened, out.String())
	}
}
