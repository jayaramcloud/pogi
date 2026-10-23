package tui

// The Overview tab: services in use, what is happening, and the latest calls.

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/nrndev/pogi/internal/core"
)

var sortNames = []string{"last active", "calls", "resources", "name"}

// overviewServices lists services with resources or calls, in the chosen order.
func (m *model) overviewServices(st stats) []core.ServiceStatus {
	var out []core.ServiceStatus
	for _, s := range m.status.Services {
		if s.Resources > 0 || s.Requests > 0 {
			out = append(out, s)
		}
	}
	byName := func(a, b core.ServiceStatus) int {
		return cmp.Or(cmp.Compare(a.Title, b.Title), cmp.Compare(a.Name, b.Name))
	}
	slices.SortStableFunc(out, func(a, b core.ServiceStatus) int {
		switch m.ovSort {
		case 1:
			return cmp.Or(cmp.Compare(b.Requests, a.Requests), cmp.Compare(b.Resources, a.Resources), byName(a, b))
		case 2:
			return cmp.Or(cmp.Compare(b.Resources, a.Resources), cmp.Compare(b.Requests, a.Requests), byName(a, b))
		case 3:
			return byName(a, b)
		}
		la, lb := st.Svc[a.Name].Last, st.Svc[b.Name].Last
		if la.IsZero() != lb.IsZero() { // services with no calls come last
			return cmp.Compare(btoi(la.IsZero()), btoi(lb.IsZero()))
		}
		return cmp.Or(lb.Compare(la), cmp.Compare(b.Resources, a.Resources), byName(a, b))
	})
	return out
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (m *model) overviewKey(key string) (tea.Model, tea.Cmd) {
	list := m.overviewServices(summarize(m.reqs, m.clock()))
	cur := 0
	for i, s := range list {
		if s.Name == m.ovSvc {
			cur = i
		}
	}
	if c, ok := m.nav(key, cur, len(list)); ok {
		m.ovSvc = "" // at the top, the selection follows the busiest row as the order changes
		if c > 0 && c < len(list) {
			m.ovSvc = list[c].Name
		}
		return m, nil
	}
	switch key {
	case "s":
		m.ovSort = (m.ovSort + 1) % len(sortNames)
	case "enter":
		if cur < len(list) {
			m.resName, m.pane, m.row, m.find = list[cur].Name, 1, 0, ""
			if list[cur].Resources == 0 { // it holds nothing, so show every browsable service
				m.resAll = true
			}
			return m.goTab(tabResources)
		}
	}
	return m, nil
}

func (m *model) overviewView(room int) []string {
	now := m.clock()
	st := summarize(m.reqs, now)
	wide, showAct := m.w >= 100, m.h >= 20
	var out []string
	for i, p := range m.status.Problems {
		if i == 3 {
			out = append(out, bad.Render(fmt.Sprintf("+ %d more", len(m.status.Problems)-3)))
			break
		}
		out = append(out, bad.Render(truncate("! "+p.Text, m.w)))
	}
	avail := room - len(out)
	if showAct {
		avail-- // the console hint, with a blank line above it
	}
	var act []string
	if showAct && !wide {
		act = append(m.activityShort(st), "")
		avail -= len(act)
	}
	recentMin := 0
	switch {
	case avail >= 14:
		recentMin = 7
	case avail >= 10:
		recentMin = 5
	}
	limit := max(4, avail-recentMin)

	list := m.overviewServices(st)
	lw := m.w
	var panel []string
	if showAct && wide {
		lw = max(56, min(66, m.w-50))
		panel = m.activityPanel(st, m.w-lw-3)
	}
	svc := m.servicesBlock(list, st, lw, limit)
	block := svc
	if panel != nil {
		block = joinCols(svc, panel, lw)
		block = block[:min(len(block), limit)]
	}
	out = append(append(out, act...), block...)

	if left := room - len(out) - btoi(showAct); left >= 4 {
		n := min(left-3, len(m.reqs))
		rows := make([]core.Entry, n)
		for i := range rows {
			rows[i] = m.reqs[len(m.reqs)-1-i]
		}
		out = append(out, "", m.acc("RECENT CALLS"))
		out = append(out, m.callsTable(rows, m.w, -1, 0, len(rows), []string{"TIME", "CLOUD", "SERVICE", "OPERATION", "STATUS", "MS", "ERROR"})...)
	}
	if showAct {
		for len(out) < room-1 {
			out = append(out, "")
		}
		out = append(out, dim.Render("console "+m.endpoint+"/_pogi/ · env eval \"$(pogi env)\""))
	}
	return out
}

// servicesBlock is the "services in use" table, at most limit lines.
func (m *model) servicesBlock(list []core.ServiceStatus, st stats, w, limit int) []string {
	now := m.clock()
	out := []string{m.acc("SERVICES IN USE") + "  " + fmt.Sprint(len(list))}
	if len(list) == 0 {
		return append(out, dim.Render("nothing in use yet"))
	}
	t := table{cols: []col{{"CLOUD", false, 0, 1}, {"SERVICE", false, 24, 10}, {"RESOURCES", true, 0, 6},
		{"CALLS", true, 0, 7}, {"ERRORS", true, 0, 5}, {"AVG MS", true, 0, 2}, {"LAST", true, 0, 3}}, sel: -1}
	var notes []string
	var total int
	for _, s := range m.status.Services {
		total += s.Requests
	}
	if st.Total < total {
		notes = append(notes, fmt.Sprintf("errors, avg ms and last cover the last %d calls", st.Total))
	}
	if len(list) < len(m.status.Services) {
		notes = append(notes, "the other services have nothing in them yet")
	}
	for i, s := range list {
		v, seen := st.Svc[s.Name]
		errs, avg, last := "–", "–", "–"
		if seen {
			errs, avg, last = fmt.Sprint(v.Errors), fmt.Sprintf("%.1f", v.AvgMs), ago(now.Sub(v.Last))
		}
		t.rows = append(t.rows, []string{cloudShort[s.Cloud], s.Title, fmt.Sprint(s.Resources), fmt.Sprint(s.Requests), errs, avg, last})
		if s.Name == m.ovSvc {
			t.sel = i
		}
	}
	if t.sel < 0 {
		t.sel = 0
	}
	room := limit - 2 // the title and the header
	if room-len(notes) >= 1 {
		room -= len(notes)
	} else {
		notes = nil
	}
	t.from, t.to = window(len(list), t.sel, max(1, room))
	t.cell = func(r, c int, s string) string {
		switch c {
		case 0, 1:
			return clouds[list[r].Cloud].Render(s)
		case 4:
			if v := st.Svc[list[r].Name]; v.Errors > 0 {
				return bad.Render(s)
			}
		}
		return s
	}
	out = append(out, t.render(w)...)
	for _, n := range notes {
		out = append(out, dim.Render(n))
	}
	return out[:min(len(out), limit)]
}

// activityShort is the narrow-screen summary: two lines.
func (m *model) activityShort(st stats) []string {
	if st.Total == 0 {
		return []string{m.acc("ACTIVITY") + "  " + dim.Render("no calls yet. Try: pogi aws s3 mb s3://demo")}
	}
	head := m.acc("ACTIVITY") + fmt.Sprintf("  last %d calls · %d errors (%d%%)", st.Total, st.Errors, pct(st.Errors, st.Total))
	if st.ChaosErrors > 0 {
		head += fmt.Sprintf(", %d by chaos", st.ChaosErrors)
	}
	head += fmt.Sprintf(" · slowest %.1f ms %s", st.Slowest.Ms, st.Slowest.Service)
	return []string{truncate(head, m.w), m.callsPerSec(st, m.w)}
}

func pct(n, total int) int { return int(float64(n)*100/float64(max(1, total)) + 0.5) }

// callsPerSec is the "calls/s" line: a one-row sparkline, one bucket per second.
func (m *model) callsPerSec(st stats, w int) string {
	n := max(10, min(60, w-9-24))
	v := st.PerSec[60-n:]
	peak := slices.Max(v)
	runes := []rune("▂▃▄▅▆▇█")
	var b strings.Builder
	for _, x := range v {
		if x == 0 {
			b.WriteString(dim.Render("▁"))
			continue
		}
		b.WriteRune(runes[(x*len(runes)+peak-1)/peak-1])
	}
	return fmt.Sprintf("calls/s  %s  %s", b.String(), dim.Render(fmt.Sprintf("last %d s · peak %d/s", n, peak)))
}

// activityPanel is the right-hand panel of the wide Overview.
func (m *model) activityPanel(st stats, w int) []string {
	if st.Total == 0 {
		return []string{m.acc("ACTIVITY"), dim.Render("no calls yet. Try: pogi aws s3 mb s3://demo")}
	}
	out := []string{m.acc("ACTIVITY") + fmt.Sprintf("  last %d calls", st.Total), m.callsPerSec(st, w)}
	e := fmt.Sprintf("errors   %d of %d (%d%%)", st.Errors, st.Total, pct(st.Errors, st.Total))
	if st.ChaosErrors > 0 {
		e += fmt.Sprintf(" · %d caused by chaos", st.ChaosErrors)
	}
	out = append(out, e, fmt.Sprintf("slowest  %.1f ms · %s %s", st.Slowest.Ms, st.Slowest.Service, st.Slowest.Op))
	if len(st.Top) > 0 {
		out = append(out, "", m.acc("TOP ERRORS"))
		for _, t := range st.Top {
			out = append(out, fmt.Sprintf("%4d  %-24s %s", t.N, ansi.Truncate(t.Code, 24, "…"), dim.Render(t.Service)))
		}
	}
	return out
}

// joinCols puts two blocks side by side with a dim │ between them.
func joinCols(left, right []string, lw int) []string {
	var out []string
	for i := 0; i < max(len(left), len(right)); i++ {
		var l, r string
		if i < len(left) {
			l = left[i]
		}
		if i < len(right) {
			r = right[i]
		}
		out = append(out, pad(ansi.Truncate(l, lw, "…"), lw, false)+dim.Render(" │ ")+r)
	}
	return out
}

// callsTable draws request rows with the named columns (TIME, CLOUD, ...).
// sel is the selected row, or -1.
func (m *model) callsTable(rows []core.Entry, w, sel, from, to int, names []string) []string {
	all := map[string]col{
		"ID": {"ID", true, 0, 4}, "TIME": {"TIME", false, 0, 6}, "CLOUD": {"CLOUD", false, 0, 1},
		"SERVICE": {"SERVICE", false, 14, 10}, "OPERATION": {"OPERATION", false, 28, 9}, "METHOD": {"METHOD", false, 0, 3},
		"STATUS": {"STATUS", true, 0, 8}, "MS": {"MS", true, 0, 5}, "REGION": {"REGION", false, 14, 2}, "ERROR": {"ERROR", false, 40, 7},
	}
	t := table{sel: sel, from: from, to: to}
	for _, n := range names {
		t.cols = append(t.cols, all[n])
	}
	long := w >= 100
	for _, e := range rows {
		var r []string
		for _, n := range names {
			r = append(r, callCell(e, n, long))
		}
		t.rows = append(t.rows, r)
	}
	t.cell = func(r, c int, s string) string {
		e := rows[r]
		switch names[c] {
		case "TIME":
			return dim.Render(s)
		case "CLOUD", "SERVICE":
			return clouds[e.Cloud].Render(s)
		case "STATUS", "ERROR":
			return statusStyle(e.Status).Render(s)
		}
		return s
	}
	return t.render(w)
}

func statusStyle(code int) lipgloss.Style {
	switch {
	case code >= 500:
		return bad
	case code >= 400:
		return warn
	}
	return ok
}

func callCell(e core.Entry, name string, long bool) string {
	switch name {
	case "ID":
		return fmt.Sprint(e.ID)
	case "TIME":
		if long {
			return e.Time.Local().Format("15:04:05.000")
		}
		return e.Time.Local().Format("15:04:05")
	case "CLOUD":
		return cloudShort[e.Cloud]
	case "SERVICE":
		return e.Service
	case "OPERATION":
		return e.Op
	case "METHOD":
		return e.Method
	case "STATUS":
		return fmt.Sprint(e.Status)
	case "MS":
		return fmt.Sprintf("%.1f", e.Ms)
	case "REGION":
		return e.Region
	}
	s := e.Error
	if e.Chaos {
		s += " · chaos"
	}
	if e.Console {
		s += " · console"
	}
	return strings.TrimPrefix(s, " · ")
}
