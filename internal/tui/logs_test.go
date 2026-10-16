package tui

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// load runs the model's fetch against a live pogi and feeds the result back.
func load(t *testing.T, m *model) {
	t.Helper()
	msg := m.fetch()()
	d, ok := msg.(dataMsg)
	if !ok || d.err != nil {
		t.Fatalf("fetch = %#v", msg)
	}
	m.Update(d)
}

func TestLogsTab(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"logs", "logging"}})
	call := func(op string, in any) {
		t.Helper()
		if err := srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, "Logs_20140328", op, in, nil); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
	}
	call("CreateLogGroup", map[string]any{"logGroupName": "/app/api"})
	call("CreateLogStream", map[string]any{"logGroupName": "/app/api", "logStreamName": "s"})
	now := time.Now().UnixMilli()
	call("PutLogEvents", map[string]any{"logGroupName": "/app/api", "logStreamName": "s", "logEvents": []any{
		map[string]any{"timestamp": now - 5000, "message": "booting api"},
		map[string]any{"timestamp": now - 1000, "message": "ERROR upstream timed out"},
	}})
	entries := `{"entries":[{"logName":"projects/` + core.DefaultProject + `/logs/worker","resource":{"type":"global"},"severity":"WARNING","textPayload":"queue is deep"}]}`
	if res, err := http.Post(srv.URL+"/v2/entries:write", "application/json", strings.NewReader(entries)); err != nil || res.StatusCode != 200 {
		t.Fatalf("write entries: %v %v", res, err)
	}

	m := &model{endpoint: srv.URL, tab: 5}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	load(t, m)
	out := m.render()
	for _, s := range []string{"Logs", "aws /app/api", "booting api", "ERROR upstream timed out", "gcp worker", "queue is deep", "p pause"} {
		if !strings.Contains(out, s) {
			t.Errorf("logs tab lacks %q:\n%s", s, out)
		}
	}
	if strings.Index(out, "booting api") > strings.Index(out, "ERROR upstream") {
		t.Errorf("events are not oldest first:\n%s", out)
	}

	// paused: new events do not move the view
	m.key(tea.KeyPressMsg{Code: 'p', Text: "p"})
	call("PutLogEvents", map[string]any{"logGroupName": "/app/api", "logStreamName": "s", "logEvents": []any{map[string]any{"timestamp": now, "message": "late event"}}})
	load(t, m)
	if out := m.render(); strings.Contains(out, "late event") || !strings.Contains(out, "[paused]") {
		t.Errorf("paused tail moved:\n%s", out)
	}
	m.key(tea.KeyPressMsg{Code: 'p', Text: "p"})
	load(t, m)
	if out := m.render(); !strings.Contains(out, "late event") {
		t.Errorf("resumed tail did not catch up:\n%s", out)
	}

	// the key switches to the tab from anywhere
	m.tab = 0
	m.key(tea.KeyPressMsg{Code: '6', Text: "6"})
	if m.tab != 5 {
		t.Errorf("key 6 went to tab %d", m.tab)
	}
}

func TestManyServicesAcrossClouds(t *testing.T) {
	m := &model{endpoint: "http://localhost:7644"}
	m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	st := core.Status{Version: "test"}
	res := map[string][]core.Table{}
	for i := 0; i < 40; i++ {
		cloud := []string{core.AWS, core.GCP, core.Azure}[i%3]
		name := fmt.Sprintf("svc%02d", i)
		st.Services = append(st.Services, core.ServiceStatus{Name: name, Cloud: cloud, Title: "Service " + name, Resources: map[bool]int{true: 2}[i == 1 || i == 2]})
		res[name] = []core.Table{{Name: "Things", Columns: []string{"Name"}, Rows: [][]string{{"thing-of-" + name}}}}
	}
	m.Update(dataMsg{status: st, res: res})

	// the dashboard lists services in use and counts the idle ones, per cloud
	out := m.render()
	for _, s := range []string{"GCP", "AZURE", "Service svc01", "Service svc02", "+ 14 services with nothing", "+ 12 services with nothing"} {
		if !strings.Contains(out, s) {
			t.Errorf("dashboard lacks %q:\n%s", s, out)
		}
	}
	if strings.Contains(out, "Service svc03") {
		t.Errorf("dashboard lists an idle service:\n%s", out)
	}

	// the resource tab can reach a service far down the list and shows it selected
	m.tab, m.svc = 2, 37
	out = m.render()
	if !strings.Contains(out, "svc37") || !strings.Contains(out, "thing-of-svc37") || !strings.Contains(out, "‹") {
		t.Errorf("resources tab does not show the selected service:\n%s", out)
	}
	m.key(tea.KeyPressMsg{Code: tea.KeyLeft})
	if m.svc != 36 {
		t.Errorf("left moved to service %d", m.svc)
	}
}
