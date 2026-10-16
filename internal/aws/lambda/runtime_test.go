package lambda

import (
	"net/http"
	"net/http/httptest"
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
