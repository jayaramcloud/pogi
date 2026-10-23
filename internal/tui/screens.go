package tui

// The Requests, Resources, Chaos and Snapshots tabs.

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/nrndev/pogi/internal/core"
)

var (
	cloudFilter = []string{"", core.AWS, core.GCP, core.Azure}
	reqSorts    = []string{"newest", "slowest", "errors first"}
)

// ---- Requests ----

func (m *model) visibleReqs() []core.Entry {
	var out []core.Entry
	f := strings.ToLower(m.filter)
	for i := len(m.reqs) - 1; i >= 0; i-- {
		e := m.reqs[i]
		if m.errors && e.Status < 400 {
			continue
		}
		if c := cloudFilter[m.cloud]; c != "" && e.Cloud != c {
			continue
		}
		if f != "" && !strings.Contains(strings.ToLower(strings.Join([]string{e.Service, e.Op, e.Path, e.Error, e.Region, e.Principal, e.Trace, e.RequestID}, " ")), f) {
			continue
		}
		out = append(out, e)
	}
	switch m.reqSort {
	case 1:
		slices.SortStableFunc(out, func(a, b core.Entry) int { return cmp.Compare(b.Ms, a.Ms) })
	case 2:
		slices.SortStableFunc(out, func(a, b core.Entry) int { return cmp.Compare(btoi(b.Status >= 400), btoi(a.Status >= 400)) })
	}
	return out
}

// reqIndex finds the selected row: the selection follows the request, not the row number.
func (m *model) reqIndex(vis []core.Entry) int {
	if m.reqPin {
		if i := slices.IndexFunc(vis, func(e core.Entry) bool { return e.ID == m.reqSel }); i >= 0 {
			return i
		}
	}
	return 0
}

func (m *model) pinReq(vis []core.Entry, i int) {
	if i <= 0 || i >= len(vis) {
		m.reqPin = false
		return
	}
	m.reqPin, m.reqSel = true, vis[i].ID
}

func (m *model) requestsKey(key string) (tea.Model, tea.Cmd) {
	vis := m.visibleReqs()
	cur := m.reqIndex(vis)
	if c, ok := m.nav(key, cur, len(vis)); ok {
		m.pinReq(vis, c)
		return m, nil
	}
	switch key {
	case "p", " ":
		m.paused = !m.paused
	case "/":
		m.filtering = true
	case "e":
		m.errors, m.reqPin = !m.errors, false
	case "f":
		m.cloud, m.reqPin = (m.cloud+1)%len(cloudFilter), false
	case "s":
		m.reqSort, m.reqPin = (m.reqSort+1)%len(reqSorts), false
	case "c":
		m.filter, m.errors, m.cloud, m.reqSort, m.reqPin = "", false, 0, 0, false
	case "enter":
		if cur < len(vis) {
			id := vis[cur].ID
			return m, func() tea.Msg {
				var e core.Entry
				if err := m.get("/_pogi/api/requests/"+strconv.FormatInt(id, 10), &e); err != nil {
					return flashMsg(err.Error())
				}
				return linesMsg{lines: entryLines(e)}
			}
		}
	}
	return m, nil
}

func (m *model) requestsView(room int) []string {
	vis := m.visibleReqs()
	cur := m.reqIndex(vis)
	f := "/ " + or(m.filter, "any text")
	if m.filtering {
		f += "▏"
	}
	f += " · " + map[bool]string{true: "errors only", false: "any status"}[m.errors]
	f += " · cloud: " + or(cloudNames[cloudFilter[m.cloud]], "all") + " · sort: " + reqSorts[m.reqSort]
	live := ok.Render("● live")
	if m.paused {
		live = warn.Render("paused")
	}
	right := fmt.Sprintf("%d of %d shown · ", len(vis), len(m.reqs)) + live
	gap := max(1, m.w-ansi.StringWidth(f)-ansi.StringWidth(right))
	out := []string{truncate(m.acc(f)+strings.Repeat(" ", gap)+right, m.w)}
	if len(vis) == 0 {
		return append(out, dim.Render("no requests match"))
	}
	preview := room >= 12
	rows := room - 2 // the filter line and the header
	if preview {
		rows -= 4
	}
	from, to := window(len(vis), cur, rows)
	names := []string{"ID", "TIME", "CLOUD", "SERVICE", "OPERATION", "METHOD", "STATUS", "MS", "REGION", "ERROR"}
	out = append(out, m.callsTable(vis, m.w, cur, from, to, names)...)
	if preview {
		e := vis[cur]
		title := strings.TrimSpace(fmt.Sprintf("── #%d %s %s · %d %s", e.ID, e.Service, e.Op, e.Status, e.Error)) + " "
		out = append(out, "", dim.Render(truncate(title+strings.Repeat("─", max(0, m.w-ansi.StringWidth(title))), m.w)))
		out = append(out, wrapPairs(pairs(e), m.w-2, 2)...)
	}
	return out
}

// pairs are the label/value facts shown under the list and at the top of the full view.
func pairs(e core.Entry) []string {
	p := []string{e.Method + " " + e.Path, "account " + orDash(e.Account), "region " + orDash(e.Region), "key " + orDash(e.AccessKey),
		"principal " + orDash(e.Principal), "request id " + orDash(e.RequestID), "trace " + orDash(e.Trace), "IAM " + orDash(e.IAM)}
	if e.Chaos {
		p = append(p, "changed by a chaos rule")
	}
	if e.Console {
		p = append(p, "made from the console")
	}
	return p
}

// wrapPairs fills at most n lines of width w; what does not fit is left out.
func wrapPairs(items []string, w, n int) []string {
	var out []string
	line := ""
	for _, it := range items {
		it = clean(it)
		switch {
		case line == "":
			line = it
		case ansi.StringWidth(line)+3+ansi.StringWidth(it) <= w:
			line += "   " + it
		default:
			out = append(out, " "+truncate(line, w))
			if len(out) == n {
				return out
			}
			line = it
		}
	}
	if line != "" {
		out = append(out, " "+truncate(line, w))
	}
	return out
}

func entryLines(e core.Entry) []string {
	out := []string{
		bold.Render(e.Service+" "+e.Op) + " " + strconv.Itoa(e.Status) + " " + e.Error,
		dim.Render(fmt.Sprintf("#%d · %s · %.2f ms", e.ID, e.Time.Local().Format(time.DateTime), e.Ms)),
	}
	for _, p := range pairs(e) {
		out = append(out, "  "+p)
	}
	out = append(out, "", bold.Render("Request headers"))
	for _, k := range core.SortedKeys(e.ReqHeaders) {
		out = append(out, "  "+dim.Render(k+":")+" "+clean(e.ReqHeaders[k]))
	}
	out = append(out, "", bold.Render("Request body"))
	out = append(out, strings.Split(prettyBody(e.ReqBody), "\n")...)
	out = append(out, "", bold.Render("Response body"))
	return append(out, strings.Split(prettyBody(e.RespBody), "\n")...)
}

func prettyBody(s string) string {
	if s == "" {
		return dim.Render("  (empty)")
	}
	var buf bytes.Buffer
	if json.Indent(&buf, []byte(s), "  ", "  ") == nil {
		s = buf.String()
	} else {
		s = strings.ReplaceAll(s, "><", ">\n<")
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = "  " + clean(l)
	}
	return strings.Join(lines, "\n")
}

// ---- Resources ----

type svcRow struct {
	name, cloud, title string
	n                  int
}

// resServices lists the services the left pane shows: those holding resources, or all browsable ones.
func (m *model) resServices() []svcRow {
	var out []svcRow
	f := strings.ToLower(m.find)
	for _, s := range m.status.Services {
		if _, ok := m.res[s.Name]; !ok || (!m.resAll && s.Resources == 0) {
			continue
		}
		if f != "" && !strings.Contains(strings.ToLower(s.Name+" "+s.Title), f) {
			continue
		}
		out = append(out, svcRow{s.Name, s.Cloud, s.Title, s.Resources})
	}
	rank := map[string]int{core.AWS: 0, core.GCP: 1, core.Azure: 2}
	slices.SortFunc(out, func(a, b svcRow) int {
		return cmp.Or(cmp.Compare(rank[a.cloud], rank[b.cloud]), cmp.Compare(a.title, b.title), cmp.Compare(a.name, b.name))
	})
	return out
}

func (m *model) resIndex(list []svcRow) int {
	return max(0, slices.IndexFunc(list, func(r svcRow) bool { return r.name == m.resName }))
}

// resRows flattens the selected service's tables, so one row number reaches every row.
func (m *model) resRows(name string) (rows [][]string, tableOf []string) {
	for _, t := range m.res[name] {
		for _, r := range t.Rows {
			rows, tableOf = append(rows, r), append(tableOf, t.Name)
		}
	}
	return
}

func (m *model) resourcesKey(key string) (tea.Model, tea.Cmd) {
	list := m.resServices()
	if len(list) == 0 && key != "a" && key != "/" {
		return m, nil
	}
	wide := m.w >= 100
	if !wide {
		m.pane = 1
	}
	cur := m.resIndex(list)
	pick := func(i int) {
		if i >= 0 && i < len(list) && list[i].name != m.resName {
			m.resName, m.row = list[i].name, 0
		}
	}
	switch key {
	case "right", "l":
		if wide {
			m.pane = 1
		} else {
			pick(min(cur+1, len(list)-1))
		}
		return m, nil
	case "left", "h":
		if wide {
			m.pane = 0
		} else {
			pick(max(0, cur-1))
		}
		return m, nil
	case "a":
		m.resAll = !m.resAll
		return m, nil
	case "/":
		m.filtering = true
		return m, nil
	case "x":
		if cur < len(list) {
			s := list[cur]
			return m.ask("Reset every resource in "+s.title+"?", func() tea.Cmd {
				return m.send("POST", "/_pogi/api/reset?service="+url.QueryEscape(s.name), nil)
			})
		}
		return m, nil
	}
	if len(list) == 0 {
		return m, nil
	}
	name := list[cur].name
	rows, tableOf := m.resRows(name)
	if m.pane == 0 {
		if c, ok := m.nav(key, cur, len(list)); ok {
			pick(c)
		} else if key == "enter" {
			m.pane = 1
		}
		return m, nil
	}
	if r, ok := m.nav(key, m.row, len(rows)); ok {
		m.row = r
		return m, nil
	}
	if key == "enter" && m.row < len(rows) {
		path := "/_pogi/api/resources/" + name + "/" + url.PathEscape(tableOf[m.row]) + "/" + url.PathEscape(rows[m.row][0])
		return m, func() tea.Msg {
			var v any
			if err := m.get(path, &v); err != nil {
				return flashMsg(err.Error())
			}
			b, _ := json.MarshalIndent(v, "", "  ")
			return linesMsg{lines: strings.Split(string(b), "\n"), inspect: true}
		}
	}
	return m, nil
}

func (m *model) resourcesView(room int) []string {
	list := m.resServices()
	finding := ""
	if m.filtering || m.find != "" {
		finding = "find: " + m.find
		if m.filtering {
			finding += "▏"
		}
	}
	if len(list) == 0 {
		msg := "no service holds resources yet. Press a to show all browsable services."
		if m.resAll || m.find != "" {
			msg = "no browsable service matches"
		}
		return []string{m.acc(finding), dim.Render(msg)}
	}
	cur := m.resIndex(list)
	sel := list[cur]
	wide := m.w >= 100
	if !wide {
		m.pane = 1
		strip := m.serviceStrip(list, cur)
		out := []string{strip}
		if finding != "" {
			out = append([]string{m.acc(finding)}, out...)
		}
		return append(out, m.resTables(sel, m.w, room-len(out), true)...)
	}
	lw := min(46, m.w*2/5)
	rw := m.w - lw - 3
	head := fmt.Sprintf("SERVICES  %d hold resources · a show %s", m.holding(), map[bool]string{true: "only those", false: "all"}[m.resAll])
	if finding != "" {
		head = finding
	}
	left := []string{m.acc(truncate(head, lw))}
	t := table{cols: []col{{"CLOUD", false, 0, 1}, {"SERVICE", false, 22, 3}, {"RESOURCES", true, 0, 2}}, sel: cur, blur: m.pane != 0}
	for _, s := range list {
		t.rows = append(t.rows, []string{cloudShort[s.cloud], s.title, fmt.Sprint(s.n)})
	}
	t.from, t.to = window(len(list), cur, room-2)
	t.cell = func(r, c int, s string) string { return clouds[list[r].cloud].Render(s) }
	left = append(left, t.render(lw)...)
	return joinCols(left, m.resTables(sel, rw, room, false), lw)
}

func (m *model) holding() int {
	n := 0
	for _, s := range m.status.Services {
		if _, ok := m.res[s.Name]; ok && s.Resources > 0 {
			n++
		}
	}
	return n
}

// resTables draws the selected service's tables and scrolls to keep the selected row in view.
func (m *model) resTables(s svcRow, w, room int, narrow bool) []string {
	var out []string
	if !narrow {
		out = append(out, m.acc(fmt.Sprintf("%s · %s · %d %s", s.title, cloudShort[s.cloud], s.n, plural(s.n, "resource", "resources"))))
	}
	selLine, idx := 0, 0
	for _, t := range m.res[s.name] {
		out = append(out, bold.Render(fmt.Sprintf("%s (%d)", t.Name, len(t.Rows))))
		if len(t.Rows) == 0 {
			out = append(out, dim.Render("  (none)"), "")
			continue
		}
		tb := table{sel: -1, blur: m.pane != 1}
		if m.row >= idx && m.row < idx+len(t.Rows) {
			tb.sel = m.row - idx
		}
		var keepCols []int
		for i, c := range t.Columns {
			if slices.ContainsFunc(t.Rows, func(r []string) bool { return i < len(r) && r[i] != "" }) {
				keepCols = append(keepCols, i)
				tb.cols = append(tb.cols, col{strings.ToUpper(c), numeric(t.Rows, i), 40, len(t.Columns) - i})
			}
		}
		for _, r := range t.Rows {
			var row []string
			for _, i := range keepCols {
				row = append(row, r[i])
			}
			tb.rows = append(tb.rows, row)
		}
		if len(tb.cols) == 0 {
			tb.cols = []col{{"", false, 0, 1}}
		}
		lines := tb.render(w)
		if tb.sel >= 0 {
			selLine = len(out) + 1 + tb.sel
		}
		out = append(append(out, lines...), "")
		idx += len(t.Rows)
	}
	if len(out) <= room {
		return out
	}
	// keep the title lines when the top is in view, otherwise follow the selection
	from, to := window(len(out), selLine, room)
	return out[from:to]
}

// numeric: every non-empty cell starts with a number ("6 B" yes, "2026-10-22 19:59:12" no).
func numeric(rows [][]string, c int) bool {
	seen := false
	for _, r := range rows {
		if c >= len(r) || r[c] == "" {
			continue
		}
		f := strings.Fields(r[c])
		if len(f) == 0 {
			continue
		}
		if _, err := strconv.ParseFloat(f[0], 64); err != nil {
			return false
		}
		seen = true
	}
	return seen
}

// serviceStrip shows the listed services as one line: the selected one with as many neighbours as fit.
func (m *model) serviceStrip(list []svcRow, cur int) string {
	lo, hi, width, room := cur, cur, ansi.StringWidth(list[cur].title)+3, max(20, m.w-8)
	for grew := true; grew; {
		grew = false
		if lo > 0 && width+ansi.StringWidth(list[lo-1].title)+3 <= room {
			lo--
			width += ansi.StringWidth(list[lo].title) + 3
			grew = true
		}
		if hi < len(list)-1 && width+ansi.StringWidth(list[hi+1].title)+3 <= room {
			hi++
			width += ansi.StringWidth(list[hi].title) + 3
			grew = true
		}
	}
	var parts []string
	if lo > 0 {
		parts = append(parts, dim.Render(fmt.Sprintf("‹%d", lo)))
	}
	for i := lo; i <= hi; i++ {
		s := list[i]
		if i == cur {
			parts = append(parts, selStyle.Render(" "+s.title+" · "+cloudShort[s.cloud]+" "))
		} else {
			parts = append(parts, clouds[s.cloud].Render(s.title))
		}
	}
	if hi < len(list)-1 {
		parts = append(parts, dim.Render(fmt.Sprintf("%d›", len(list)-1-hi)))
	}
	return strings.Join(parts, "  ")
}

// ---- Chaos ----

func (m *model) chaosView(room int) []string {
	hits := 0
	for _, r := range m.rules {
		hits += r.Hits
	}
	out := []string{m.acc(fmt.Sprintf("%d %s · %d %s changed so far", len(m.rules), plural(len(m.rules), "rule", "rules"), hits, plural(hits, "call", "calls")))}
	if len(m.rules) == 0 {
		return append(out, dim.Render("no chaos rules, so everything behaves. t: throttle 20% · s: add 800 ms · o: outage"))
	}
	t := table{cols: []col{{"ID", false, 0, 7}, {"CLOUD", false, 0, 2}, {"SERVICE", false, 0, 10}, {"OPERATION", false, 28, 9}, {"REGION", false, 0, 3},
		{"WHO", false, 30, 4}, {"HOW OFTEN", true, 0, 6}, {"ADDS", true, 0, 5}, {"ERROR", false, 40, 8}, {"HITS", true, 0, 7}}}
	for _, r := range m.rules {
		often := "always"
		if r.Probability > 0 && r.Probability < 1 {
			often = fmt.Sprintf("%.0f%%", r.Probability*100)
		}
		adds, errs := "–", "–"
		if r.LatencyMs > 0 {
			adds = fmt.Sprintf("+%d ms", r.LatencyMs)
		}
		if r.Status > 0 {
			errs = strings.TrimSpace(fmt.Sprintf("%d %s", r.Status, r.Code))
		}
		t.rows = append(t.rows, []string{r.ID, or(r.Cloud, "any"), or(r.Service, "any"), or(r.Op, "any"), or(r.Region, "any"),
			or(or(r.Principal, r.Path), "any"), often, adds, errs, fmt.Sprint(r.Hits)})
	}
	m.pick = min(m.pick, len(m.rules)-1)
	t.sel = m.pick
	t.from, t.to = window(len(m.rules), m.pick, room-2)
	return append(out, t.render(m.w)...)
}

// ---- Snapshots ----

func (m *model) snapshotsView(room int) []string {
	where := "saved snapshots"
	if m.status.DataDir != "" {
		where = "saved in " + tilde(strings.TrimRight(m.status.DataDir, "/")+"/snapshots")
	}
	state := "state in memory (lost when pogi stops)"
	if m.status.Persist {
		state = "state saved"
	}
	out := []string{m.acc(state + " · " + where)}
	if len(m.snaps) == 0 {
		return append(out, dim.Render("no snapshots yet. Press s to save the current state."))
	}
	t := table{cols: []col{{"NAME", false, 40, 4}, {"SIZE", true, 0, 3}, {"SAVED", false, 0, 2}, {"AGE", true, 0, 1}}}
	now := m.clock()
	for _, s := range m.snaps {
		t.rows = append(t.rows, []string{s.Name, humanSize(s.Size), s.Saved.Local().Format(time.DateTime), ago(now.Sub(s.Saved)) + " ago"})
	}
	m.pick = min(m.pick, len(m.snaps)-1)
	t.sel = m.pick
	t.from, t.to = window(len(m.snaps), m.pick, room-2)
	return append(out, t.render(m.w)...)
}
