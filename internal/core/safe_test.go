package core

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestGoRecoversPanic(t *testing.T) {
	var buf syncBuf
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)
	Go("test-worker", func() { panic("boom") })
	for i := 0; i < 200 && !strings.Contains(buf.String(), "boom"); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	out := buf.String()
	if !strings.Contains(out, "test-worker") || !strings.Contains(out, "boom") || !strings.Contains(out, "stack") {
		t.Fatalf("panic not logged: %q", out)
	}
}

// panicSvc is a service whose handler panics (after writing "partial", if set).
type panicSvc struct {
	name, cloud, protocol string
	partial               bool
	abort                 bool
}

func (s panicSvc) Info() Info {
	return Info{Name: s.name, Cloud: s.cloud, Protocol: s.protocol,
		Match: func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/"+s.name) }}
}

func (s panicSvc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.partial {
		io.WriteString(w, "partial")
	}
	if s.abort {
		panic(http.ErrAbortHandler)
	}
	panic("boom")
}

func TestRequestPanic(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	for _, s := range []panicSvc{
		{name: "awsjson", cloud: AWS, protocol: "json"},
		{name: "awsquery", cloud: AWS, protocol: "query"},
		{name: "awsrestjson", cloud: AWS, protocol: "rest-json"},
		{name: "awsrestxml", cloud: AWS, protocol: "rest-xml"},
		{name: "gcp", cloud: GCP},
		{name: "azure", cloud: Azure, protocol: "azure-json"},
		{name: "started", cloud: AWS, protocol: "json", partial: true},
		{name: "abort", cloud: AWS, protocol: "json", abort: true},
	} {
		h.Register(s)
	}
	base := Listen(t, h)
	for _, tc := range []struct {
		path   string
		status int
		want   []string // text the body must contain
	}{
		{"/awsjson", 500, []string{`"__type":"InternalFailure"`}},
		{"/awsquery", 500, []string{"<ErrorResponse", "<Code>InternalFailure</Code>"}},
		{"/awsrestjson", 500, []string{`"__type":"InternalFailure"`}},
		{"/awsrestxml", 500, []string{"<Error><Code>InternalFailure</Code>"}},
		{"/gcp", 500, []string{`"code":500`, `"status":"INTERNAL"`}},
		{"/azure", 500, []string{`"code":"InternalServerError"`}},
		{"/started", 200, []string{"partial"}}, // already started: no second header, no error body
	} {
		res, err := http.Get(base + tc.path)
		if err != nil {
			t.Fatalf("%s: %v", tc.path, err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != tc.status {
			t.Errorf("%s: status %d, want %d", tc.path, res.StatusCode, tc.status)
		}
		for _, w := range tc.want {
			if !strings.Contains(string(b), w) {
				t.Errorf("%s: body %q lacks %q", tc.path, b, w)
			}
		}
		if tc.path == "/started" && strings.Contains(string(b), "InternalFailure") {
			t.Errorf("%s: error written after the response started: %q", tc.path, b)
		}
	}
	// http.ErrAbortHandler is not swallowed: the server drops the connection.
	if res, err := http.Get(base + "/abort"); err == nil {
		res.Body.Close()
		t.Errorf("/abort: got status %d, want a dropped connection", res.StatusCode)
	}
}
