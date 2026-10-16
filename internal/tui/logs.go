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
	"github.com/nrndev/pogi/internal/core"
)

type logLine struct {
	T            time.Time
	Source, Text string
}

// fetchLogs gathers the tail from every log service that is running.
func (m *model) fetchLogs() ([]logLine, error) {
	out := []logLine{}
	var groups map[string][]core.Table
	if err := m.get("/_pogi/api/resources?service=logs", &groups); err != nil {
		return nil, err
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
				out = append(out, logLine{time.UnixMilli(e.Timestamp), "aws " + r[0], strings.TrimRight(e.Message, "\n")})
			}
		}
	}
	var gcp map[string][]core.Table
	if m.get("/_pogi/api/resources?service=logging", &gcp) == nil {
		for _, t := range gcp["logging"] {
			if t.Name != "Entries" {
				continue
			}
			for _, r := range t.Rows { // time, severity, log, resource, message
				if len(r) < 5 {
					continue
				}
				ts, _ := time.ParseInLocation(time.DateTime, r[0], time.UTC)
				out = append(out, logLine{ts, "gcp " + r[2][strings.LastIndexByte(r[2], '/')+1:], r[1] + " " + r[4]})
			}
		}
	}
	slices.SortStableFunc(out, func(a, b logLine) int { return a.T.Compare(b.T) })
	return out[max(0, len(out)-1000):], nil
}

func (m *model) logsKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "p", " ":
		m.logHold = !m.logHold
		m.logAbove = 0
	case "up", "k":
		m.logHold = true
		m.logAbove = min(m.logAbove+1, max(0, len(m.logs)-1))
	case "down", "j":
		m.logAbove = max(0, m.logAbove-1)
	case "pgup":
		m.logHold = true
		m.logAbove = min(m.logAbove+m.h-6, max(0, len(m.logs)-1))
	case "pgdown":
		m.logAbove = max(0, m.logAbove-(m.h-6))
	case "G", "end":
		m.logHold, m.logAbove = false, 0
	}
	return m, nil
}

func (m *model) logsView() string {
	if len(m.logs) == 0 {
		return dim.Render("no log events yet — CloudWatch Logs groups and Cloud Logging entries show up here as they arrive")
	}
	room := max(1, m.h-6)
	end := len(m.logs) - m.logAbove
	var b strings.Builder
	for _, l := range m.logs[max(0, end-room):end] {
		text := l.Text
		switch {
		case strings.Contains(text, "ERROR") || strings.Contains(text, "CRITICAL"):
			text = bad.Render(text)
		case strings.Contains(text, "WARN"):
			text = warn.Render(text)
		}
		fmt.Fprintf(&b, "%s %s %s\n", dim.Render(l.T.Local().Format("15:04:05.000")), accent.Render(l.Source), text)
	}
	return b.String()
}
