package tui

import (
	"net/http"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// load runs the model's fetch against a live pogi and feeds the result back.
func load(t *testing.T, m *model) {
	t.Helper()
	msg := m.fetch(false)()
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

	m := &model{endpoint: srv.URL, tab: tabLogs}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	load(t, m)
	out := ansi.Strip(m.render())
	for _, s := range []string{"3 lines · 2 sources", "TIME", "LEVEL", "/app/api", "booting api", "ERROR upstream timed out", "worker", "WARNING", "queue is deep", "p pause"} {
		if !strings.Contains(out, s) {
			t.Errorf("logs tab lacks %q:\n%s", s, out)
		}
	}
	if strings.Index(out, "booting api") > strings.Index(out, "ERROR upstream") {
		t.Errorf("events are not oldest first:\n%s", out)
	}

	// w keeps warnings and errors; / filters by text
	m.key(tea.KeyPressMsg{Code: 'w', Text: "w"})
	if out := ansi.Strip(m.render()); strings.Contains(out, "booting api") || !strings.Contains(out, "queue is deep") || !strings.Contains(out, "2 lines") {
		t.Errorf("warnings and errors only:\n%s", out)
	}
	m.key(tea.KeyPressMsg{Code: 'w', Text: "w"})
	m.key(tea.KeyPressMsg{Code: '/', Text: "/"})
	for _, r := range "deep" {
		m.key(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	if out := ansi.Strip(m.render()); strings.Contains(out, "booting api") || !strings.Contains(out, "1 line ") {
		t.Errorf("text filter:\n%s", out)
	}
	m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
	m.logFilter = ""

	// paused: new events do not move the view
	m.key(tea.KeyPressMsg{Code: 'p', Text: "p"})
	call("PutLogEvents", map[string]any{"logGroupName": "/app/api", "logStreamName": "s", "logEvents": []any{map[string]any{"timestamp": now, "message": "late event"}}})
	load(t, m)
	if out := ansi.Strip(m.render()); strings.Contains(out, "late event") || !strings.Contains(out, "paused") {
		t.Errorf("paused tail moved:\n%s", out)
	}
	m.key(tea.KeyPressMsg{Code: 'p', Text: "p"})
	load(t, m)
	if out := ansi.Strip(m.render()); !strings.Contains(out, "late event") {
		t.Errorf("resumed tail did not catch up:\n%s", out)
	}

	// the key switches to the tab from anywhere
	m.tab = 0
	m.key(tea.KeyPressMsg{Code: '6', Text: "6"})
	if m.tab != tabLogs {
		t.Errorf("key 6 went to tab %d", m.tab)
	}
}

// The Flows tab reads the spans a live pogi recorded.
func TestFlowsTabLoadsSpans(t *testing.T) {
	srv := pogi.Start(t)
	srv.Hub.OnDeliver("sqs", func(string, *core.Message) error { return nil })
	srv.Hub.Deliver("arn:aws:sqs:us-east-1:000000000000:orders", &core.Message{Body: "x", Trace: "1-a-b", From: "arn:aws:sns:us-east-1:000000000000:alerts"})
	m := &model{endpoint: srv.URL, tab: tabFlows}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	load(t, m)
	if out := ansi.Strip(m.render()); !strings.Contains(out, "alerts") || !strings.Contains(out, "orders") || !strings.Contains(out, "1-a-b") {
		t.Errorf("flows tab:\n%s", out)
	}
}

// With CloudWatch Logs off the tab still shows Google Cloud's logs, not an error: it does not ask
// for a service that is off, which the API answers with 404.
func TestLogsTabWithoutCloudWatchLogs(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"logging"}})
	entries := `{"entries":[{"logName":"projects/` + core.DefaultProject + `/logs/worker","resource":{"type":"global"},"severity":"WARNING","textPayload":"queue is deep"}]}`
	if res, err := http.Post(srv.URL+"/v2/entries:write", "application/json", strings.NewReader(entries)); err != nil || res.StatusCode != 200 {
		t.Fatalf("write entries: %v %v", res, err)
	}
	m := &model{endpoint: srv.URL, tab: tabLogs}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	load(t, m)
	if out := ansi.Strip(m.render()); !strings.Contains(out, "queue is deep") {
		t.Errorf("logs tab lacks the Google Cloud entry:\n%s", out)
	}
}
