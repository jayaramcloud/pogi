package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeEngine answers the calls of a pull; create handles POST /images/create.
func fakeEngine(t *testing.T, create http.HandlerFunc) *Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_ping", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "OK") })
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"Version":"29.0.0","ApiVersion":"1.54","Arch":"amd64"}`)
	})
	mux.HandleFunc("/{v}/images/create", create)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return New(Options{Host: "tcp://" + srv.Listener.Addr().String()})
}

func only(t *testing.T, c *Client) Task {
	t.Helper()
	l := c.Tasks().List()
	if len(l) != 1 {
		t.Fatalf("tasks = %+v, want one", l)
	}
	return l[0]
}

func TestPullStalls(t *testing.T) {
	stallLimit = 200 * time.Millisecond
	t.Cleanup(func() { stallLimit = 2 * time.Minute })
	c := fakeEngine(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"id":"a","status":"Downloading","progressDetail":{"current":5,"total":100}}`+"\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done() // no more bytes
	})
	err := c.pullFor(context.Background(), "mariadb:11.4", "rds", "uxdemo")
	if !errors.Is(err, errStalled) {
		t.Fatalf("err = %v, want the stall", err)
	}
	if tk := only(t, c); tk.State != "failed" || !strings.HasPrefix(tk.Error, "the download stalled") || tk.Service != "rds" || tk.Resource != "uxdemo" || tk.Done != 5 {
		t.Fatalf("task = %+v", tk)
	}
}

func TestPullTooLong(t *testing.T) {
	pullLimit = 200 * time.Millisecond
	t.Cleanup(func() { pullLimit = 30 * time.Minute })
	c := fakeEngine(t, func(w http.ResponseWriter, r *http.Request) {
		for r.Context().Err() == nil { // bytes keep coming, so it never stalls
			io.WriteString(w, `{"id":"a","status":"Waiting"}`+"\n")
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	})
	if err := c.Pull(context.Background(), "x:1"); !errors.Is(err, errTooLong) {
		t.Fatalf("err = %v", err)
	}
}

func TestPullFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create http.HandlerFunc
		want   string
	}{
		{"not found", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			io.WriteString(w, `{"message":"manifest for mariadb:99 not found: manifest unknown"}`)
		}, "image not found: mariadb:99 — "},
		{"not found in the stream", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"error":"pull access denied for mariadb, repository does not exist"}`+"\n")
		}, "image not found: mariadb:99 — "},
		{"disk full", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"error":"write /var/lib/docker/tmp: no space left on device"}`+"\n")
		}, "The disk is full — "},
		{"registry down", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			io.WriteString(w, `{"message":"Get \"https://registry-1.docker.io/v2/\": dial tcp: lookup registry-1.docker.io: no such host"}`)
		}, "The registry can't be reached — "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fakeEngine(t, tc.create)
			if err := c.pullFor(context.Background(), "mariadb:99", "rds", "uxdemo"); err == nil {
				t.Fatal("no error")
			}
			if tk := only(t, c); tk.State != "failed" || !strings.HasPrefix(tk.Error, tc.want) {
				t.Fatalf("error = %q, want %q first", tk.Error, tc.want)
			}
		})
	}
}

func TestPullNoEngine(t *testing.T) {
	c := New(Options{Host: "tcp://127.0.0.1:1"})
	err := c.Pull(context.Background(), "alpine")
	if !errors.Is(err, errNoEngine) || Plain(err) != "Docker isn't running" {
		t.Fatalf("err = %v, plain %q", err, Plain(err))
	}
	if tk := only(t, c); tk.State != "failed" || tk.Error != "Docker isn't running" {
		t.Fatalf("task = %+v", tk)
	}
}

func TestPullProgress(t *testing.T) {
	// No sizes at all: the task finishes with Total 0 (not known).
	c := fakeEngine(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":"Pulling from library/alpine","id":"3"}`+"\n"+`{"id":"a","status":"Downloading","progress":"[=>]"}`+"\n")
	})
	if err := c.Pull(context.Background(), "alpine:3"); err != nil {
		t.Fatal(err)
	}
	if tk := only(t, c); tk.State != "done" || tk.Done != 0 || tk.Total != 0 || tk.Error != "" || tk.What != "alpine:3" || tk.Kind != "download" {
		t.Fatalf("task = %+v", tk)
	}
	// Two layers add up; Extracting does not count; a finished download is whole.
	c = fakeEngine(t, func(w http.ResponseWriter, r *http.Request) {
		for _, l := range []string{
			`{"id":"a","status":"Downloading","progressDetail":{"current":10,"total":100}}`,
			`{"id":"b","status":"Downloading","progressDetail":{"current":20,"total":300}}`,
			`{"id":"a","status":"Download complete"}`,
			`{"id":"a","status":"Extracting","progressDetail":{"current":1,"total":100}}`,
		} {
			io.WriteString(w, l+"\n")
		}
	})
	var mid Task
	tr := c.Track("", "", "download", "x")
	tr.Progress(layers{}.add("a", 10, 100))
	mid = c.Tasks().List()[0]
	if mid.Done != 10 || mid.Total != 100 {
		t.Fatalf("mid = %+v", mid)
	}
	c.Pull(context.Background(), "x:1")
	if tk := c.Tasks().List()[1]; tk.Done != 400 || tk.Total != 400 { // a is whole (100), b is 20 of 300... then End makes done = total
		t.Fatalf("task = %+v", tk)
	}
}

func TestTasksRetention(t *testing.T) {
	now := time.Now()
	ts := &Tasks{now: func() time.Time { return now }}
	a, b := ts.begin("rds", "a", "start", "x"), ts.begin("rds", "b", "start", "x")
	a.End(nil)
	b.End(errors.New("docker: boom"))
	b.End(nil) // the first end counts
	if l := ts.List(); len(l) != 2 || l[0].State != "done" || l[1].State != "failed" || l[1].Error != "boom" {
		t.Fatalf("list = %+v", l)
	}
	now = now.Add(61 * time.Second)
	run := ts.begin("rds", "c", "download", "x")
	if l := ts.List(); len(l) != 1 || l[0].Resource != "c" {
		t.Fatalf("after 61s: %+v", l)
	}
	if l := ts.Recent(time.Hour); len(l) != 3 { // the bell looks further back
		t.Fatalf("recent = %d", len(l))
	}
	run.End(nil)
}

func TestTasksLimit(t *testing.T) {
	ts := &Tasks{}
	first := ts.begin("s", "keep", "start", "x") // running: stays while finished ones go
	for i := 0; i < maxTasks+50; i++ {
		ts.begin("s", fmt.Sprint(i), "start", "x").End(nil)
	}
	l := ts.Recent(time.Hour)
	if len(l) != maxTasks || l[0].Resource != "keep" || l[len(l)-1].Resource != fmt.Sprint(maxTasks+49) {
		t.Fatalf("len %d first %q last %q", len(l), l[0].Resource, l[len(l)-1].Resource)
	}
	first.End(nil)
}
