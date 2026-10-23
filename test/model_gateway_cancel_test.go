package test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// The local model endpoint reads the request and never answers. The Bedrock call must end when the caller
// gives up, and the call to the model must be cancelled with it.
func TestModelCallEndsWhenClientLeaves(t *testing.T) {
	cancelled := make(chan time.Duration, 1)
	got := make(chan struct{}, 4)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		got <- struct{}{}
		t0 := time.Now()
		select {
		case <-r.Context().Done():
			cancelled <- time.Since(t0)
		case <-time.After(25 * time.Second):
			cancelled <- -1
		}
	}))
	defer up.Close()
	srv := pogi.Start(t, pogi.Config{AIEndpoint: up.URL + "/v1"})
	body := []byte(`{"messages":[{"role":"user","content":[{"text":"hi"}]}]}`)
	req, _ := http.NewRequest("POST", srv.URL+"/model/anthropic.claude-3-haiku-20240307-v1:0/converse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	awstest.Sign(t, req, "bedrock")
	_, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	select {
	case <-got:
	default:
		t.Fatalf("the Converse call never reached the model endpoint (err %v)", err)
	}
	select {
	case d := <-cancelled:
		if d < 0 || d > 5*time.Second {
			t.Errorf("the model call kept running after the client left")
		}
	case <-time.After(15 * time.Second):
		t.Errorf("the model call is still open 15s after the client left")
	}
}
