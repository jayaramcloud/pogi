package main

import (
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Asking a command for help prints its usage and returns flag.ErrHelp, which
// main turns into exit code 0. None of these may reach a server or a file.
func TestHelpIsNotAnError(t *testing.T) {
	for _, tc := range []struct {
		run  func([]string) error
		args []string
	}{
		{costCmd, []string{"-h"}},
		{costCmd, []string{"--help"}},
		{costCmd, []string{"./infra", "-h"}},
		{costCmd, []string{"sizes", "-h"}},
		{snapshot, []string{"-h"}},
		{chaos, []string{"--help"}},
		{configCmd, []string{"-h"}},
		{agentsCmd, []string{"help"}},
		// -h at the end of anything, as clig.dev asks
		{start, []string{"--headless", "-h"}},
		{list, []string{"s3", "-h"}},
		{reset, []string{"s3", "--help"}},
		{snapshot, []string{"export", "x", "-h"}},
		{chaos, []string{"rm", "id", "-h"}},
		{kubeconfig, []string{"gke", "c1", "-h"}},
		{configCmd, []string{"validate", "-h"}},
		{agentsCmd, []string{"init", "-h"}},
	} {
		if err := tc.run(slices.Clone(tc.args)); !errors.Is(err, flag.ErrHelp) {
			t.Errorf("%v: got %v, want flag.ErrHelp", tc.args, err)
		}
	}
}

// A first word that is a flag belongs to start, except --version and -v, which print the version.
func TestCommand(t *testing.T) {
	for _, tc := range []struct {
		args []string
		cmd  string
		rest []string
	}{
		{nil, "start", nil},
		{[]string{"--headless"}, "start", []string{"--headless"}},
		{[]string{"ls", "s3"}, "ls", []string{"s3"}},
		{[]string{"--version"}, "--version", []string{}},
		{[]string{"-v"}, "-v", []string{}},
		{[]string{"help", "status"}, "status", []string{"-h"}},
		{[]string{"help"}, "help", []string{}},
	} {
		cmd, rest := command(tc.args)
		if cmd != tc.cmd || !slices.Equal(rest, tc.rest) {
			t.Errorf("%v: got %q %v, want %q %v", tc.args, cmd, rest, tc.cmd, tc.rest)
		}
	}
}

// A word a command does not take, or a flag it does not have, is an error, not silently ignored.
// Each call fails before it can reach a server or a file.
func TestExtraArgsFail(t *testing.T) {
	for _, tc := range []struct {
		run  func([]string) error
		args []string
		want string
	}{
		{start, []string{"version"}, `unexpected argument "version"`},
		{status, []string{"now"}, `unexpected argument "now"`},
		{list, []string{"s3", "sqs"}, `unexpected argument "sqs"`},
		{reset, []string{"s3", "-e", "http://x"}, "flag provided but not defined: -e"},
		{snapshot, []string{"rm", "a", "b"}, `unexpected argument "b"`},
		{chaos, []string{"clear", "all"}, `unexpected argument "all"`},
		{chaos, []string{"ls", "--status", "500"}, "--status only work with chaos add"},
		{kubeconfig, []string{"gke", "c1", "extra"}, `unexpected argument "extra"`},
		{configCmd, []string{"validate", "a", "b"}, `unexpected argument "b"`},
		{env, []string{"--shell", "nushell"}, `--shell "nushell" is not one of`},
		{logs, []string{"-n", "0"}, "-n 0 is not a positive number"},
		{snapshot, []string{"rm", "x", "--json"}, "--json only works with snapshot ls"},
		{snapshot, []string{"import", "-"}, "stdin has no file name"},
		{chaos, []string{"clear", "--json"}, "--json only works with chaos ls"},
	} {
		err := tc.run(slices.Clone(tc.args))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: got %v, want %q", tc.args, err, tc.want)
		}
		if !errors.As(err, new(usageError)) {
			t.Errorf("%v: %v is not a usage error, so pogi would exit 1, not 2", tc.args, err)
		}
	}
}

// Help shows what the command does, an example and the flags as the docs spell them: --name, and
// -n for a one-letter flag.
func TestHelpText(t *testing.T) {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	setUsage(fs, "Show recent API calls.", "pogi logs [flags]", "pogi logs -f")
	fs.String("endpoint", "http://localhost:7644", "pogi endpoint")
	fs.Int("n", 50, "how many")
	fs.Bool("json", false, "print JSON")
	var b strings.Builder
	fs.SetOutput(&b)
	fs.Usage()
	for _, want := range []string{"Show recent API calls.", "Usage:\n  pogi logs [flags]", "Example:\n  pogi logs -f",
		`--endpoint string  pogi endpoint (default "http://localhost:7644")`, "-n int", "(default 50)", "--json"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("help lacks %q:\n%s", want, b.String())
		}
	}
	if strings.Contains(b.String(), " -endpoint") || strings.Contains(b.String(), "(default false)") {
		t.Errorf("help has a one-dash long flag or a false default:\n%s", b.String())
	}
}

// Flags work before or after the arguments, and before the subcommand of snapshot, chaos and config:
// each call reaches the --endpoint it was given, where nothing listens.
func TestFlagsAnywhere(t *testing.T) {
	const ep = "http://127.0.0.1:1"
	for _, tc := range []struct {
		run  func([]string) error
		args []string
	}{
		{list, []string{"s3", "--endpoint", ep}},
		{reset, []string{"s3", "sqs", "--endpoint", ep}},
		{snapshot, []string{"--endpoint", ep, "ls"}},
		{snapshot, []string{"rm", "x", "--endpoint", ep}},
		{chaos, []string{"--endpoint", ep, "clear"}},
		{chaos, []string{"add", "--status", "500", "--endpoint", ep}},
		{kubeconfig, []string{"gke", "c1", "--endpoint", ep}},
	} {
		err := tc.run(slices.Clone(tc.args))
		if err == nil || !strings.Contains(err.Error(), "cannot reach pogi at "+ep) {
			t.Errorf("%v: got %v, want the call to reach %s", tc.args, err, ep)
		}
		if errors.As(err, new(usageError)) {
			t.Errorf("%v: an unreachable pogi is not a usage error", tc.args)
		}
	}
}

// pogi status lists each problem on its own line, in red only on a terminal that allows colour.
func TestWriteProblems(t *testing.T) {
	ps := []core.Problem{{Kind: "port", Text: "smtp: port 2525 is in use"}, {Kind: "state", Text: "can't save s3"}}
	var plain, red strings.Builder
	writeProblems(&plain, ps, false)
	writeProblems(&red, ps[:1], true)
	if plain.String() != "! smtp: port 2525 is in use\n! can't save s3\n" || red.String() != "\x1b[31m! smtp: port 2525 is in use\x1b[0m\n" {
		t.Fatalf("plain %q, red %q", plain.String(), red.String())
	}
}

// The start banner's env hint adds --endpoint only when plain `pogi env` would reach another pogi.
func TestEnvHint(t *testing.T) {
	t.Setenv("POGI_ENDPOINT", "")
	for u, want := range map[string]string{
		"http://127.0.0.1:7644":  `eval "$(pogi env)"`,
		"http://localhost:7644":  `eval "$(pogi env)"`,
		"http://127.0.0.1:27644": `eval "$(pogi env --endpoint http://127.0.0.1:27644)"`,
		"http://10.0.0.5:7644":   `eval "$(pogi env --endpoint http://10.0.0.5:7644)"`,
	} {
		if got := envHint(u); got != want {
			t.Errorf("%s: got %s, want %s", u, got, want)
		}
	}
	t.Setenv("POGI_ENDPOINT", "http://localhost:9000")
	if got := envHint("http://127.0.0.1:9000"); got != `eval "$(pogi env)"` {
		t.Errorf("POGI_ENDPOINT on the same port: got %s", got)
	}
}

// A pogi that accepts the call and never answers is given up on, with a message that says so.
func TestAPITimeout(t *testing.T) {
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-stop }))
	defer srv.Close()
	defer close(stop) // runs before srv.Close, which waits for the handler
	old := apiClient.Timeout
	apiClient.Timeout = 50 * time.Millisecond
	defer func() { apiClient.Timeout = old }()
	if err := api(srv.URL, "GET", "/_pogi/api/info", nil, nil); err == nil || !strings.Contains(err.Error(), "did not answer within 50ms") {
		t.Fatalf("got %v", err)
	}
}

// A snapshot export writes the file only when the whole snapshot arrived.
func TestExportSnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/nope") {
			http.Error(w, `{"error":"no snapshot named nope"}`, 404)
			return
		}
		w.Write([]byte(`{"s3":{}}`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	if err := exportSnapshot(srv.URL, "/_pogi/api/snapshots/nope", missing); err == nil {
		t.Fatal("exporting an unknown snapshot succeeded")
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Fatalf("a failed export left %v behind", left)
	}
	good := filepath.Join(dir, "s1.json")
	if err := exportSnapshot(srv.URL, "/_pogi/api/snapshots/s1", good); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(good); string(b) != `{"s3":{}}` {
		t.Fatalf("exported %q", b)
	}
}
