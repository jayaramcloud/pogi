package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/nrndev/pogi/internal/core"
)

func press(m *model, key string) tea.Cmd {
	k := tea.KeyPressMsg{Code: rune(key[0]), Text: key}
	switch key {
	case "up":
		k = tea.KeyPressMsg{Code: tea.KeyUp}
	case "down":
		k = tea.KeyPressMsg{Code: tea.KeyDown}
	case "enter":
		k = tea.KeyPressMsg{Code: tea.KeyEnter}
	}
	_, cmd := m.key(k)
	return cmd
}

func calls(from, to int, now time.Time) []core.Entry {
	var out []core.Entry
	for i := from; i <= to; i++ {
		e := core.Entry{ID: int64(i), Time: now.Add(-time.Duration(30-i) * time.Second), Cloud: core.AWS, Service: "sqs", Op: "SendMessage", Method: "POST", Path: "/",
			Status: 200, Ms: 800.5, Region: "us-east-1", Account: "000000000000", RequestID: "4f1c09d2"}
		if i%4 == 0 {
			e.Status, e.Error, e.Chaos = 400, "ThrottlingException", true
		}
		out = append(out, e)
	}
	return out
}

func fixture(embedded bool) *model {
	m := &model{endpoint: "http://localhost:7644", embedded: embedded}
	now := time.Now()
	m.now = now
	m.Update(dataMsg{
		status: core.Status{Version: "0.1.0", Uptime: 32, Ready: true, Region: "us-east-1", IAM: "off", DataDir: "/srv/pogi", Services: []core.ServiceStatus{
			{Name: "accessanalyzer", Cloud: core.AWS, Title: "Access Analyzer"},
			{Name: "sqs", Cloud: core.AWS, Title: "SQS", Resources: 2, Requests: 27},
			{Name: "s3", Cloud: core.AWS, Title: "S3", Resources: 2, Requests: 5},
			{Name: "rm", Cloud: core.GCP, Title: "Resource Manager", Resources: 1},
		}},
		reqs:  calls(1, 27, now),
		rules: []core.Rule{{ID: "cf34f91d", Probability: .2, Status: 400, Code: "ThrottlingException", Hits: 4}},
	})
	// what the other tabs fetched
	m.res = map[string][]core.Table{
		"accessanalyzer": {{Name: "Analyzers", Columns: []string{"Name"}}},
		"s3":             {{Name: "Buckets", Columns: []string{"Name", "Objects", "Size", "Created"}, Rows: [][]string{{"photos", "2", "6 B", "2026-10-22 19:59:12"}}}},
		"sqs":            {{Name: "Queues", Columns: []string{"Name"}, Rows: [][]string{{"orders"}}}},
	}
	m.snaps = []core.SnapshotInfo{{Name: "before-demo", Size: 22733, Saved: now.Add(-4 * time.Minute)}}
	m.logs = []logLine{{T: now, Cloud: core.AWS, Source: "/app/api", Level: "ERROR", Text: "ERROR db timeout"}}
	m.spans = []core.Span{{Trace: "1-6719a0", Time: now, Ms: 0.3, Service: "sqs", Op: "Deliver", Kind: "notify", From: "arn:aws:sns:us-east-1:0:alerts", To: "arn:aws:sqs:us-east-1:0:orders"}}
	return m
}

func TestRenderEveryTabFitsEverySize(t *testing.T) {
	keys := map[int]string{tabOverview: "enter open in Resources", tabRequests: "enter full request", tabResources: "enter inspect", tabChaos: "d remove",
		tabSnapshots: "enter load", tabLogs: "w warnings+errors", tabFlows: "enter see its requests"}
	for _, sz := range [][2]int{{120, 30}, {80, 24}, {60, 12}} {
		for _, embedded := range []bool{true, false} {
			m := fixture(embedded)
			m.Update(tea.WindowSizeMsg{Width: sz[0], Height: sz[1]})
			for tab := range tabs {
				m.tab = tab
				out := ansi.Strip(m.render())
				lines := strings.Split(out, "\n")
				if len(lines) > sz[1] {
					t.Errorf("%s %dx%d: %d lines", tabs[tab], sz[0], sz[1], len(lines))
				}
				for _, l := range lines {
					if ansi.StringWidth(l) > sz[0] {
						t.Errorf("%s %dx%d: line is %d wide: %q", tabs[tab], sz[0], sz[1], ansi.StringWidth(l), l)
					}
				}
				quit, other := "q stop pogi", "q quit (pogi keeps running)"
				if !embedded {
					quit, other = other, quit
				}
				if !strings.Contains(lines[len(lines)-1], quit) || strings.Contains(out, other) {
					t.Errorf("%s %dx%d embedded=%v: footer should say %q:\n%s", tabs[tab], sz[0], sz[1], embedded, quit, lines[len(lines)-1])
				}
				if sz[0] == 120 && !strings.Contains(lines[len(lines)-1], keys[tab]) {
					t.Errorf("%s: footer lacks %q: %s", tabs[tab], keys[tab], lines[len(lines)-1])
				}
				if !strings.Contains(lines[0], "● ready") || !strings.Contains(lines[0], "chaos 1") {
					t.Errorf("%s %dx%d: status line = %q", tabs[tab], sz[0], sz[1], lines[0])
				}
			}
		}
	}
}

func TestRenderContent(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	want := map[int][]string{
		tabOverview:  {"SERVICES IN USE  3", "ACTIVITY  last 27 calls", "6 of 27 (22%)", "RECENT CALLS", "TOP ERRORS", "ThrottlingException"},
		tabRequests:  {"ID  TIME", "● live", "27 of 27 shown", "── #27 sqs SendMessage"},
		tabResources: {"2 hold resources", "S3 · AWS · 2 resources", "Buckets (1)", "photos"},
		tabChaos:     {"1 rule · 4 calls changed so far", "20%", "400 ThrottlingException"},
		tabSnapshots: {"saved in /srv/pogi/snapshots", "state in memory", "22.2 KB", "4m ago"},
		tabLogs:      {"1 line · 1 source", "/app/api", "ERROR"},
		tabFlows:     {"alerts", "orders", "1-6719a0", "notify"},
	}
	for tab, strs := range want {
		m.tab = tab
		out := ansi.Strip(m.render())
		for _, s := range strs {
			if !strings.Contains(out, s) {
				t.Errorf("%s lacks %q:\n%s", tabs[tab], s, out)
			}
		}
	}
	m.tab = tabOverview
	if out := ansi.Strip(m.render()); strings.Contains(out, " of 4") || strings.Contains(out, "Access Analyzer") {
		t.Errorf("overview shows a service total or an idle service:\n%s", out)
	}
	m.tab, m.err = tabFlows, nil
	m.spans = nil
	if out := m.render(); !strings.Contains(out, "no flows yet") {
		t.Errorf("empty flows:\n%s", out)
	}
	m.w, m.h = 50, 8
	if out := m.render(); !strings.Contains(out, "at least 60×10") || strings.Count(out, "\n") > 0 {
		t.Errorf("too small: %q", out)
	}
}

func TestResourcesStartOnAServiceThatHoldsResources(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.tab = tabResources
	out := ansi.Strip(m.render())
	if !strings.Contains(out, "S3 · AWS") || strings.Contains(out, "Access Analyzer") {
		t.Errorf("should start on S3 and hide empty services:\n%s", out)
	}
	press(m, "a")
	if out := ansi.Strip(m.render()); !strings.Contains(out, "Access Analyzer") {
		t.Errorf("a should show every browsable service:\n%s", out)
	}
}

func TestDestructiveKeysAskFirst(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	cases := []struct {
		tab  int
		key  string
		asks string
	}{{tabOverview, "R", "Reset ALL"}, {tabResources, "x", "Reset every resource in S3"}, {tabSnapshots, "enter", "Load snapshot before-demo"}, {tabSnapshots, "d", "Delete snapshot before-demo"}}
	for _, c := range cases {
		m.tab, m.confirm = c.tab, nil
		if cmd := press(m, c.key); cmd != nil || m.confirm == nil {
			t.Fatalf("%s on %s sent a command without asking", c.key, tabs[c.tab])
		}
		if out := ansi.Strip(m.render()); !strings.Contains(out, c.asks) || !strings.Contains(out, "y/N") {
			t.Errorf("%s on %s: footer does not ask %q:\n%s", c.key, tabs[c.tab], c.asks, out)
		}
		if cmd := press(m, "n"); cmd != nil || m.confirm != nil || m.flash != "cancelled" {
			t.Errorf("%s: n should cancel", c.key)
		}
		press(m, c.key)
		if cmd := press(m, "y"); cmd == nil || m.confirm != nil {
			t.Errorf("%s: y should run the command", c.key)
		}
	}
}

func TestRequestsSelectionFollowsTheRequest(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.tab = tabRequests
	selected := func() (int, int64) {
		vis := m.visibleReqs()
		i := m.reqIndex(vis)
		return i, vis[i].ID
	}
	more := func(from, to int) { m.Update(dataMsg{status: m.status, rules: m.rules, reqs: calls(from, to, m.now)}) }
	if i, id := selected(); i != 0 || id != 27 {
		t.Fatalf("starts at %d #%d", i, id)
	}
	more(28, 28) // at the top: follows the newest call
	if i, id := selected(); i != 0 || id != 28 {
		t.Errorf("at the top it should follow the newest, got row %d #%d", i, id)
	}
	press(m, "down")
	press(m, "down")
	if i, id := selected(); i != 2 || id != 26 {
		t.Fatalf("after two downs: row %d #%d", i, id)
	}
	more(29, 31)
	if i, id := selected(); i != 5 || id != 26 {
		t.Errorf("new calls moved the selection: row %d #%d", i, id)
	}
	// a filter that hides the request puts the selection back on top
	press(m, "e")
	if i, _ := selected(); i != 0 {
		t.Errorf("filter hid the request but the row is %d", i)
	}
}

func TestRequestsFilters(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.tab = tabRequests
	press(m, "e")
	if n := len(m.visibleReqs()); n != 6 {
		t.Errorf("errors only shows %d, want 6", n)
	}
	press(m, "f") // AWS: all of them are AWS
	press(m, "f") // Google Cloud: none
	if out := ansi.Strip(m.render()); !strings.Contains(out, "cloud: Google Cloud") || !strings.Contains(out, "no requests match") {
		t.Errorf("cloud filter:\n%s", out)
	}
	press(m, "c")
	press(m, "/")
	for _, r := range "trace-x" {
		m.key(tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	if len(m.visibleReqs()) != 0 {
		t.Error("text filter should match nothing")
	}
	m.reqs[3].Trace = "trace-x"
	if n := len(m.visibleReqs()); n != 1 {
		t.Errorf("filter matches the trace, got %d", n)
	}
	press(m, "enter") // stop typing
	press(m, "s")
	press(m, "s")
	if m.reqSort != 2 {
		t.Errorf("sort = %d", m.reqSort)
	}
}

func TestFlowsEnterOpensRequestsByTrace(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.tab = tabFlows
	if cmd := press(m, "enter"); cmd == nil || m.tab != tabRequests || m.filter != "1-6719a0" {
		t.Errorf("tab %d filter %q", m.tab, m.filter)
	}
}

func TestOverviewSortAndOpen(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	list := m.overviewServices(summarize(m.reqs, m.clock()))
	if len(list) != 3 || list[0].Name != "sqs" || list[2].Name != "rm" { // called services first, then the rest by resources
		t.Errorf("last active order: %+v", list)
	}
	press(m, "s")
	press(m, "s")
	press(m, "s") // name
	if list := m.overviewServices(summarize(m.reqs, m.clock())); list[0].Name != "rm" {
		t.Errorf("name order starts with %s", list[0].Name)
	}
	press(m, "down")
	press(m, "enter")
	if m.tab != tabResources || m.resName != "s3" {
		t.Errorf("enter opened tab %d service %q", m.tab, m.resName)
	}
}

func TestStatusLineNarrowsAndShowsProblems(t *testing.T) {
	m := fixture(true)
	m.status.Problems = []core.Problem{{Text: "could not read s3.json"}, {Text: "b"}, {Text: "c"}, {Text: "d"}, {Text: "e"}}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	out := ansi.Strip(m.render())
	for _, s := range []string{"5 problems", "state in memory", "IAM off", "us-east-1", "! could not read s3.json", "+ 2 more"} {
		if !strings.Contains(out, s) {
			t.Errorf("120 columns lacks %q:\n%s", s, out)
		}
	}
	m.Update(tea.WindowSizeMsg{Width: 70, Height: 30})
	first := strings.Split(ansi.Strip(m.render()), "\n")[0]
	if strings.Contains(first, "us-east-1") || !strings.Contains(first, "5 problems") || !strings.Contains(first, ":7644") {
		t.Errorf("narrow status line = %q", first)
	}
}

func TestNoColourStillShowsSelection(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.tab = tabRequests
	if out := ansi.Strip(m.render()); !strings.Contains(out, "▸ 27") {
		t.Errorf("no ▸ gutter on the selected row:\n%s", out)
	}
}

func TestEntryLinesStartWithTheSamePairs(t *testing.T) {
	e := calls(4, 4, time.Now())[0]
	got := strings.Join(entryLines(e), "\n")
	for _, p := range pairs(e) {
		if !strings.Contains(got, p) {
			t.Errorf("full view lacks %q", p)
		}
	}
	if !strings.Contains(fmt.Sprint(pairs(e)), "changed by a chaos rule") {
		t.Error("chaos note missing")
	}
}

// Text from the server never breaks a line or reaches the terminal as an escape code.
func TestServerTextIsCleaned(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.tab = tabLogs
	m.logs = []logLine{{T: time.Now(), Cloud: core.AWS, Source: "/app", Level: "ERROR", Text: "panic: boom\n\tat main.go:1\x1b]0;owned\x07"}}
	out := m.render()
	if strings.Contains(out, "\x1b]") || strings.Contains(out, "\x07") || len(strings.Split(out, "\n")) != 30 {
		t.Errorf("a log line with a line break and an escape code was drawn as is:\n%q", out)
	}
	if !strings.Contains(ansi.Strip(out), "panic: boom  at main.go:1") {
		t.Errorf("the message is missing:\n%s", ansi.Strip(out))
	}
	if !numeric([][]string{{" "}, {"6 B"}}, 0) {
		t.Error("a blank cell must not stop a column of numbers being one")
	}
}

func TestStatusLineWithoutPort(t *testing.T) {
	m := fixture(true)
	m.endpoint = "http://pogi.test"
	m.Update(tea.WindowSizeMsg{Width: 60, Height: 20})
	if first := strings.Split(ansi.Strip(m.render()), "\n")[0]; !strings.Contains(first, "pogi") {
		t.Errorf("status line = %q", first)
	}
}

func TestFlowsSelectionStaysOnItsStep(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	now := time.Now()
	m.spans = []core.Span{{Time: now, Op: "a", Trace: "t-a"}, {Time: now.Add(time.Millisecond), Op: "b", Trace: "t-b"}}
	m.tab = tabFlows
	m.key(tea.KeyPressMsg{Code: tea.KeyDown}) // from the newest (b) to a
	m.spans = append(m.spans, core.Span{Time: now.Add(2 * time.Millisecond), Op: "c", Trace: "t-c"})
	m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.tab != tabRequests || m.filter != "t-a" {
		t.Errorf("after a new step arrived, enter opened %q on tab %d; want t-a on Requests", m.filter, m.tab)
	}
}

// Two fetches in flight bring the same calls: they are kept once. Only the poll schedules the next fetch,
// so switching tabs does not start a second poll.
func TestFetchesNeitherDuplicateNorMultiply(t *testing.T) {
	m := fixture(true)
	n := len(m.reqs)
	more := calls(28, 29, m.now)
	m.Update(dataMsg{status: m.status, reqs: more})
	if _, cmd := m.Update(dataMsg{status: m.status, reqs: more}); cmd != nil {
		t.Error("a fetch made for a key press scheduled another poll")
	}
	if len(m.reqs) != n+2 {
		t.Errorf("have %d calls, want %d: the same call was added twice", len(m.reqs), n+2)
	}
	if _, cmd := m.Update(dataMsg{status: m.status, loop: true}); cmd == nil {
		t.Error("the poll did not schedule the next fetch")
	}
}

// Until the user moves it, the Overview selection stays on the top row, so a busy service never
// scrolls out of view because the selection is stuck on one that was first a moment ago.
func TestOverviewSelectionStaysOnTop(t *testing.T) {
	m := fixture(true)
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.render()
	if m.ovSvc != "" {
		t.Fatalf("rendering pinned the selection to %q", m.ovSvc)
	}
	m.key(tea.KeyPressMsg{Code: tea.KeyDown})
	m.key(tea.KeyPressMsg{Code: tea.KeyUp})
	if m.ovSvc != "" {
		t.Errorf("back at the top, the selection is still pinned to %q", m.ovSvc)
	}
}
