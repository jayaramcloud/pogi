package tui

// The Logs tab: a tail of what the log services hold, CloudWatch Logs groups
// and Cloud Logging entries merged by time, newest at the bottom.
// ponytail: the last 100 events of each of the first 10 log groups and the last
// 200 Cloud Logging entries, polled every second; Azure Monitor tables are not
// shown. The upgrade is a streaming admin endpoint.

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/nrndev/pogi/internal/core"
)

type logLine struct {
	T                          time.Time
	Cloud, Source, Level, Text string
}

var levelWords = []string{"DEBUG", "INFO", "NOTICE", "WARN", "WARNING", "ERROR", "CRITICAL", "FATAL"}

// levelOf reads a CloudWatch message's level: its first word, if that is a known level.
func levelOf(msg string) string {
	w := strings.ToUpper(strings.TrimRight(strings.TrimLeft(strings.TrimSpace(msg), "["), ":"))
	if i := strings.IndexAny(w, " \t"); i >= 0 {
		w = w[:i]
	}
	w = strings.TrimRight(w, "]:")
	if slices.Contains(levelWords, w) {
		return w
	}
	return "–"
}

// fetchLogs gathers the tail from every log service that is running. One that is off is not asked:
// the API answers 404 for it.
func (m *model) fetchLogs(running []core.ServiceStatus) ([]logLine, error) {
	on := func(name string) bool {
		return slices.ContainsFunc(running, func(s core.ServiceStatus) bool { return s.Name == name })
	}
	out := []logLine{}
	var groups map[string][]core.Table
	if on("logs") {
		if err := m.get("/_pogi/api/resources?service=logs", &groups); err != nil {
			return nil, err
		}
	}
	for _, t := range groups["logs"] {
		for i, r := range t.Rows {
			if i == 10 || len(r) == 0 {
				break
			}
			var v struct {
				Events []struct {
					Timestamp       int64
					Stream, Message string
				}
			}
			if m.get("/_pogi/api/resources/logs/"+url.PathEscape(t.Name)+"/"+url.PathEscape(r[0]), &v) != nil {
				continue
			}
			for _, e := range v.Events {
				out = append(out, logLine{time.UnixMilli(e.Timestamp), core.AWS, r[0], levelOf(e.Message), strings.TrimRight(e.Message, "\n")})
			}
		}
	}
	var gcp map[string][]core.Table
	if on("logging") && m.get("/_pogi/api/resources?service=logging", &gcp) == nil {
		for _, t := range gcp["logging"] {
			if t.Name != "Entries" {
				continue
			}
			for _, r := range t.Rows { // time, severity, log, resource, message
				if len(r) < 5 {
					continue
				}
				ts, _ := time.ParseInLocation(time.DateTime, r[0], time.UTC)
				out = append(out, logLine{ts, core.GCP, r[2][strings.LastIndexByte(r[2], '/')+1:], or(strings.ToUpper(r[1]), "–"), r[4]})
			}
		}
	}
	slices.SortStableFunc(out, func(a, b logLine) int { return a.T.Compare(b.T) })
	return out[max(0, len(out)-1000):], nil
}

// visibleLogs applies the filter text and the warnings-and-errors switch.
func (m *model) visibleLogs() []logLine {
	if m.logFilter == "" && !m.logWarn {
		return m.logs
	}
	f := strings.ToLower(m.logFilter)
	var out []logLine
	for _, l := range m.logs {
		if m.logWarn && !slices.Contains([]string{"WARN", "WARNING", "ERROR", "CRITICAL", "FATAL", "ALERT", "EMERGENCY"}, l.Level) {
			continue
		}
		if f != "" && !strings.Contains(strings.ToLower(l.Source+" "+l.Level+" "+l.Text), f) {
			continue
		}
		out = append(out, l)
	}
	return out
}

func (m *model) logsKey(key string) (tea.Model, tea.Cmd) {
	n := len(m.visibleLogs())
	switch key {
	case "p", " ":
		m.logHold = !m.logHold
		m.logAbove = 0
	case "up", "k":
		m.logHold = true
		m.logAbove = min(m.logAbove+1, max(0, n-1))
	case "down", "j":
		m.logAbove = max(0, m.logAbove-1)
	case "pgup":
		m.logHold = true
		m.logAbove = min(m.logAbove+m.h-6, max(0, n-1))
	case "pgdown":
		m.logAbove = max(0, m.logAbove-(m.h-6))
	case "G", "end":
		m.logHold, m.logAbove = false, 0
	case "/":
		m.filtering = true
	case "w":
		m.logWarn, m.logAbove = !m.logWarn, 0
	}
	return m, nil
}

func levelStyle(l string) lipgloss.Style {
	switch l {
	case "ERROR", "CRITICAL", "FATAL", "ALERT", "EMERGENCY":
		return bad
	case "WARN", "WARNING":
		return warn
	case "DEBUG":
		return dim
	}
	return lipgloss.NewStyle()
}

func (m *model) logsView(room int) []string {
	vis := m.visibleLogs()
	sources := map[string]bool{}
	for _, l := range m.logs {
		sources[l.Cloud+l.Source] = true
	}
	live := ok.Render("● live")
	if m.logHold {
		live = warn.Render("paused")
	}
	head := fmt.Sprintf("%d %s · %d %s · ", len(vis), plural(len(vis), "line", "lines"), len(sources), plural(len(sources), "source", "sources")) + live
	if m.logFilter != "" || m.filtering {
		head = m.acc("/ "+m.logFilter+map[bool]string{true: "▏"}[m.filtering]) + " · " + head
	}
	if m.logWarn {
		head += " · warnings and errors only"
	}
	out := []string{head}
	if len(m.logs) == 0 {
		return append(out, dim.Render("no log events yet. CloudWatch Logs groups and Cloud Logging entries show up here as they arrive."))
	}
	rows := max(1, room-2)
	end := max(0, len(vis)-m.logAbove)
	from := max(0, end-rows)
	t := table{cols: []col{{"TIME", false, 0, 5}, {"CLOUD", false, 0, 2}, {"SOURCE", false, 28, 3}, {"LEVEL", false, 0, 4}, {"MESSAGE", false, 0, 6}}, sel: -1}
	for _, l := range vis[from:end] {
		t.rows = append(t.rows, []string{l.T.Local().Format("15:04:05.000"), cloudShort[l.Cloud], l.Source, l.Level, strings.ReplaceAll(l.Text, "\t", " ")})
	}
	t.cell = func(r, c int, s string) string {
		l := vis[from+r]
		switch c {
		case 0:
			return dim.Render(s)
		case 1:
			return clouds[l.Cloud].Render(s)
		case 3:
			return levelStyle(l.Level).Render(s)
		}
		return s
	}
	return append(out, t.render(m.w)...)
}
