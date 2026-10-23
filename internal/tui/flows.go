package tui

// The Flows tab: steps where one service hands work to another, from /_pogi/api/spans.

import (
	"fmt"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/nrndev/pogi/internal/core"
)

// spanID tells steps apart, so the selection stays on its step when new ones arrive.
func spanID(s core.Span) string {
	return s.Time.Format(time.RFC3339Nano) + " " + s.Kind + " " + s.Op + " " + s.To
}

// flowIndex is the selected row, counted from the newest step.
func (m *model) flowIndex() int {
	for i := range m.spans {
		if m.flowPin != "" && spanID(m.spans[len(m.spans)-1-i]) == m.flowPin {
			return i
		}
	}
	return 0
}

func (m *model) flowsKey(key string) (tea.Model, tea.Cmd) {
	cur := m.flowIndex()
	if c, ok := m.nav(key, cur, len(m.spans)); ok {
		m.flowPin = ""
		if c > 0 {
			m.flowPin = spanID(m.spans[len(m.spans)-1-c])
		}
		return m, nil
	}
	if key == "enter" && cur < len(m.spans) {
		tr := m.spans[len(m.spans)-1-cur].Trace
		if tr == "" {
			m.flash = "this step has no trace"
			return m, nil
		}
		m.filter, m.errors, m.cloud, m.reqPin = tr, false, 0, false
		return m.goTab(tabRequests)
	}
	return m, nil
}

func (m *model) flowsView(room int) []string {
	if len(m.spans) == 0 {
		return []string{dim.Render("no flows yet. They show when one service hands work to another,"),
			dim.Render("such as an SNS message delivered to SQS, or an SQS message that runs a Lambda function.")}
	}
	n, cur := len(m.spans), m.flowIndex()
	t := table{cols: []col{{"TIME", false, 0, 8}, {"KIND", false, 0, 5}, {"SERVICE", false, 14, 9}, {"OPERATION", false, 28, 7}, {"FROM", false, 30, 6},
		{"TO", false, 30, 6}, {"MS", true, 0, 4}, {"ERROR", false, 30, 3}, {"TRACE", false, 24, 1}}, sel: cur}
	for i := n - 1; i >= 0; i-- { // newest at the top
		s := m.spans[i]
		t.rows = append(t.rows, []string{s.Time.Local().Format("15:04:05.000"), s.Kind, s.Service, s.Op, shortName(s.From), shortName(s.To),
			fmt.Sprintf("%.1f", s.Ms), s.Err, orDash(s.Trace)})
	}
	t.from, t.to = window(n, cur, room-2)
	t.cell = func(r, c int, s string) string {
		if c == 7 {
			return bad.Render(s)
		}
		return s
	}
	return append([]string{m.acc(fmt.Sprintf("%d %s, newest first", n, plural(n, "step", "steps")))}, t.render(m.w)...)
}
