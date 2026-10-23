package lambda

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// floci #1417: the Runtime API answers a response, error or init error post with 202 and {"status":"OK"}.
func TestRuntimeAPIAnswersStatusOK(t *testing.T) {
	in := &instance{current: &invocation{id: "r1", done: make(chan outcome, 2)}}
	for _, path := range []string{"/2018-06-01/runtime/invocation/r1/response", "/2018-06-01/runtime/invocation/r1/error", "/2018-06-01/runtime/init/error"} {
		w := httptest.NewRecorder()
		in.api().ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader("x")))
		if w.Code != 202 || w.Body.String() != `{"status":"OK"}` {
			t.Errorf("%s: %d %q", path, w.Code, w.Body.String())
		}
	}
}

func testInstance() *instance {
	return &instance{pool: &pool{svc: &Service{logs: map[string][]string{}}}, spec: &spec{}, dead: make(chan struct{})}
}

// closeOnWrite closes itself on the first write.
type closeOnWrite chan struct{}

func (c closeOnWrite) Write(p []byte) (int, error) { close(c); return len(p), nil }

// The full gate once lost a function's print line from its log: the response came in before the
// output reader had read the line, and END followed at once. flush must wait for the reader.
func TestFlushWaitsForTheOutputReader(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	in := testInstance()
	gate := make(chan struct{})
	in.out = io.MultiWriter(w, closeOnWrite(gate)) // the reader may start only once flush has written its mark
	go func() { <-gate; in.readOutput(r) }()
	w.WriteString("hello from python\n") // the function prints, then answers
	in.flush()
	in.mu.Lock()
	defer in.mu.Unlock()
	if !strings.Contains(in.logs.String(), "hello from python") || in.seen != 1 {
		t.Fatalf("after flush: seen %d marks, logs %q", in.seen, in.logs.String())
	}
}

// A line too long for the buffer must not stop the reader, or flush's mark would never be read; a line
// the runtime has not finished is still logged.
func TestOutputReaderGetsPastLongLines(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	in := testInstance()
	in.out = w
	go in.readOutput(r)
	w.WriteString(strings.Repeat("x", 2<<20) + "unfinished")
	in.flush()
	in.mu.Lock()
	defer in.mu.Unlock()
	lines := in.pool.svc.logs[key("", "", "")]
	if in.seen != 1 || len(lines) != 3 || lines[2] != "unfinished" {
		t.Fatalf("seen %d marks, %d lines, last %.20q", in.seen, len(lines), lines[len(lines)-1])
	}
}

// When the runtime has exited, flush must not wait for a reader that has finished.
func TestFlushAfterTheRuntimeExited(t *testing.T) {
	_, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	in := testInstance()
	in.out = w
	close(in.dead)
	in.flush()
}
