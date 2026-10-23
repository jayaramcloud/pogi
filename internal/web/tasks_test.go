package web

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

// GET /_pogi/tasks has the contract's shape, and the bell tells when a resource is ready or failed.
func TestTasksJSONAndNotes(t *testing.T) {
	hub := core.NewHub(core.Config{}, nil)
	h := New(hub)
	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	var empty struct{ Tasks []any }
	if w := get("/_pogi/tasks"); w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &empty) != nil || empty.Tasks == nil || len(empty.Tasks) != 0 {
		t.Fatalf("empty: %d %s", w.Code, w.Body)
	}
	d := hub.Docker()
	dl := d.Track("rds", "uxdemo", "download", "mariadb:11.4")
	dl.Progress(140, 410)
	d.Track("rds", "ready1", "start", "mariadb:11.4").End(nil)
	d.Track("rds", "bad1", "start", "mariadb:11.4").End(errors.New("nothing answered"))
	d.Track("rds", "bad2", "download", "mariadb:99").End(errors.New("docker: manifest unknown"))
	d.Track("rds", "bad2", "start", "mariadb:99").End(errors.New("docker: manifest unknown")) // the start that cannot follow
	d.Track("", "", "download", "pogi/none:1").End(errors.New("docker: boom"))                // a download for no resource

	var got struct {
		Tasks []map[string]any
	}
	w := get("/_pogi/tasks")
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got.Tasks) != 6 {
		t.Fatalf("tasks: %v %s", err, w.Body)
	}
	first := got.Tasks[0]
	for k, want := range map[string]any{"id": "t-1", "service": "rds", "resource": "uxdemo", "kind": "download", "what": "mariadb:11.4",
		"done": 140.0, "total": 410.0, "state": "running", "error": ""} {
		if first[k] != want {
			t.Errorf("%s = %v, want %v", k, first[k], want)
		}
	}
	if s, _ := first["since"].(string); !strings.HasSuffix(s, "Z") {
		t.Errorf("since = %v, want an RFC 3339 UTC time", first["since"])
	}
	if got.Tasks[3]["state"] != "failed" || got.Tasks[3]["error"] != "image not found — manifest unknown" {
		t.Errorf("failed task = %v", got.Tasks[3])
	}

	var titles []string
	for _, n := range taskNotes(&UI{hub: hub}) {
		titles = append(titles, n.Title)
		if noteGroup(n) != "mine" {
			t.Errorf("%q is in group %q", n.Title, noteGroup(n))
		}
	}
	for _, want := range []string{"ready1 is ready", "bad1 failed: nothing answered", "bad2 failed: couldn't download mariadb:99: image not found — manifest unknown", "Download of pogi/none:1 failed: boom"} {
		found := false
		for _, g := range titles {
			found = found || g == want
		}
		if !found {
			t.Errorf("no notice %q in %q", want, titles)
		}
	}
	if len(titles) != 4 { // the running download is no notice
		t.Errorf("notices = %q", titles)
	}
}
