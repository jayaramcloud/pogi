package docker

// A dead, cut or silent engine gives plain errors and never hangs a call. The engines here are fakes;
// no test touches the real one.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// failEngine serves a fake engine: /_ping and /version answer, everything else goes to h.
func failEngine(t *testing.T, h http.HandlerFunc) (*httptest.Server, *Client) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_ping", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "OK") })
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"Version":"29.0.0","ApiVersion":"1.54","Arch":"amd64"}`)
	})
	mux.HandleFunc("/", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, New(Options{Host: "tcp://" + srv.Listener.Addr().String()})
}

// D1: Docker is not running when pogi starts. Create gives the plain "no engine" error, and an
// engine that starts later is found within two seconds.
func TestNoEngineAtStartIsPlainAndFoundLater(t *testing.T) {
	dir, err := os.MkdirTemp("", "pa") // short: unix socket paths are limited to ~104 bytes
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "e.sock")
	c := New(Options{Host: "unix://" + sock})
	_, err = c.Create(context.Background(), &Container{Name: "x", Image: "alpine", Service: "t"})
	if !errors.Is(err, errNoEngine) || Plain(err) != "Docker isn't running" {
		t.Fatalf("Create with no engine: %v (plain %q)", err, Plain(err))
	}
	if c.Available() {
		t.Fatal("Available with no engine")
	}
	ln, err := net.Listen("unix", sock) // the engine starts now
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/version") {
			io.WriteString(w, `{"Version":"29.0.0","ApiVersion":"1.54"}`)
			return
		}
		io.WriteString(w, "OK")
	}))
	srv.Listener = ln
	srv.Start()
	defer srv.Close()
	time.Sleep(2100 * time.Millisecond)
	if !c.Available() {
		t.Fatal("an engine that starts after pogi is not found")
	}
}

// D1b: the engine was found, then dies (Docker Desktop quit). Calls fail with a raw Go network
// error, not a plain sentence; when it comes back at the same address they work again.
func TestEngineDyingGivesPlainError(t *testing.T) {
	srv, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "[]") })
	if !c.Available() {
		t.Fatal("fake engine not found")
	}
	addr := srv.Listener.Addr().String()
	srv.Close()
	_, err := c.List(context.Background(), LabelManaged+"=true")
	if err == nil {
		t.Fatal("List on a dead engine succeeded")
	}
	t.Logf("raw error after the engine died: %v | Plain: %q", err, Plain(err))
	if p := Plain(err); strings.Contains(p, "dial tcp") || strings.Contains(p, "127.0.0.1") {
		t.Errorf("a dead engine shows a raw network error: %q (want the plain 'Docker isn't running')", p)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skip("port taken by someone else: " + err.Error())
	}
	back := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "[]") }))
	back.Listener = ln
	back.Start()
	defer back.Close()
	if _, err := c.List(context.Background(), LabelManaged+"=true"); err != nil {
		t.Errorf("no recovery when the engine returns: %v", err)
	}
}

// D2a: the engine dies in the middle of a pull (some progress, then the connection drops).
// pogi must say the engine went away, not that "the registry can't be reached".
func TestEngineDyingMidPullIsNotTheRegistry(t *testing.T) {
	_, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"id":"a","status":"Downloading","progressDetail":{"current":5,"total":100}}`+"\n")
		w.(http.Flusher).Flush()
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close() // the engine is gone
	})
	err := c.Pull(context.Background(), "mariadb:11.4")
	if err == nil {
		t.Fatal("pull of a dying engine succeeded")
	}
	p := Plain(err)
	t.Logf("pull when the engine died: %q", p)
	if strings.Contains(p, "registry") {
		t.Errorf("blames the registry although the engine died: %q", p)
	}
}

// D2b: the engine dies in the middle of a create (no answer at all, connection cut).
func TestEngineDyingMidCreateIsPlain(t *testing.T) {
	_, c2 := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/images/") && strings.HasSuffix(r.URL.Path, "/json"):
			io.WriteString(w, "{}")
		case strings.Contains(r.URL.Path, "/networks/"):
			io.WriteString(w, "{}")
		case strings.Contains(r.URL.Path, "/containers/create"):
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
		}
	})
	_, err := c2.Create(context.Background(), &Container{Name: "x", Image: "mariadb:11.4", Service: "rds"})
	if err == nil {
		t.Fatal("create on a dying engine succeeded")
	}
	t.Logf("create when the engine died: %q | Plain %q", err, Plain(err))
	if p := Plain(err); strings.Contains(p, "EOF") || strings.Contains(p, "http://docker") {
		t.Errorf("a raw Go error reaches the user: %q", p)
	}
}

// D2c: the engine accepts a pull and then sends nothing. The stall limit ends it (handled; the
// real limit is two minutes, shortened here).
func TestPullStallLimit(t *testing.T) {
	stallLimit = 300 * time.Millisecond
	t.Cleanup(func() { stallLimit = 2 * time.Minute })
	_, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	start := time.Now()
	err := c.Pull(context.Background(), "mariadb:11.4")
	if !errors.Is(err, errStalled) || time.Since(start) > 3*time.Second {
		t.Fatalf("hung pull: %v after %v", err, time.Since(start))
	}
}

// D2d: the engine accepts a create and then never answers. Nothing but the caller's context
// ends it: no timeout in the docker client. RDS passes a context that lives as long as pogi.
func TestCreateOnSilentEngineTimesOut(t *testing.T) {
	CallLimit = 500 * time.Millisecond
	t.Cleanup(func() { CallLimit = 2 * time.Minute })
	release := make(chan struct{})
	_, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/images/") && strings.HasSuffix(r.URL.Path, "/json"), strings.Contains(r.URL.Path, "/networks/"):
			io.WriteString(w, "{}")
		default:
			io.Copy(io.Discard, r.Body) // reading the body is how the server notices the client left
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := c.Create(context.Background(), &Container{Name: "x", Image: "mariadb:11.4", Service: "rds"})
		done <- err
	}()
	select {
	case err := <-done:
		t.Logf("create ended by itself: %v", err)
		if Plain(err) != "Docker stopped answering" {
			t.Errorf("a create that timed out says %q", Plain(err))
		}
	case <-time.After(5 * time.Second):
		close(release)
		<-done
		t.Error("Create still blocked after 5s on an engine that never answers: no timeout")
	}
}

// Watch says why a container stopped: out of memory, or its exit code and last log line.
func TestWatchSaysWhyContainerStopped(t *testing.T) {
	for _, tc := range []struct{ oom, want string }{
		{"true", "killed: out of memory (raise Docker's memory limit)"},
		{"false", "stopped: exit 3 — boom"},
	} {
		_, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/wait"):
				io.WriteString(w, `{"StatusCode":3}`)
			case strings.HasSuffix(r.URL.Path, "/logs"):
				w.Write(append([]byte{1, 0, 0, 0, 0, 0, 0, 5}, "boom\n"...))
			default:
				io.WriteString(w, `{"State":{"Status":"exited","OOMKilled":`+tc.oom+`,"ExitCode":3}}`)
			}
		})
		got := make(chan string, 1)
		c.Watch(context.Background(), "x", func(n string) { got <- n })
		select {
		case n := <-got:
			if n != tc.want {
				t.Errorf("oom=%s: note %q, want %q", tc.oom, n, tc.want)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("oom=%s: no note", tc.oom)
		}
	}
}

// Watch stays quiet when its context ends or the engine can't be asked.
func TestWatchIsQuietWhenEngineGoes(t *testing.T) {
	srv, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	})
	got := make(chan string, 1)
	c.Watch(context.Background(), "x", func(n string) { got <- n })
	select {
	case n := <-got:
		t.Errorf("a dead engine gave the note %q", n)
	case <-time.After(300 * time.Millisecond):
	}
	srv.Close()
}
