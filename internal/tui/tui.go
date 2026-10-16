// Package tui is pogi's terminal UI. It talks to a running pogi only through
// the admin API, so it works embedded (`pogi`) or attached (`pogi tui`).
package tui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/nrndev/pogi/internal/core"
)

// Run shows the TUI until the user quits.
func Run(endpoint string, embedded bool) error {
	_, err := tea.NewProgram(&model{endpoint: strings.TrimRight(endpoint, "/"), embedded: embedded}).Run()
	return err
}

var tabs = []string{"Dashboard", "Requests", "Resources", "Chaos", "Snapshots", "Logs"}

var (
	accent = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true) // cotton candy pink
	dim    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	bold   = lipgloss.NewStyle().Bold(true)
	ok     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	warn   = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	bad    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	sel    = lipgloss.NewStyle().Reverse(true)
	clouds = map[string]lipgloss.Style{
		core.AWS:   lipgloss.NewStyle().Foreground(lipgloss.Color("214")),
		core.GCP:   lipgloss.NewStyle().Foreground(lipgloss.Color("33")),
		core.Azure: lipgloss.NewStyle().Foreground(lipgloss.Color("39")),
	}
)

type model struct {
	endpoint string
	embedded bool
	w, h     int
	tab      int
	err      error
	flash    string
	confirm  string // pending confirmation: "reset"

	status core.Status
	reqs   []core.Entry
	after  int64
	paused bool
	cursor int // index from the newest request
	detail []string
	scroll int

	filter    string
	filtering bool
	errors    bool

	res     map[string][]core.Table
	svc     int
	row     int
	inspect []string

	rules []core.Rule
	snaps []core.SnapshotInfo
	pick  int

	logs     []logLine
	logHold  bool // paused: the tail stops following
	logAbove int  // lines scrolled up from the newest while paused
}

type tickMsg struct{}
type dataMsg struct {
	status core.Status
	reqs   []core.Entry
	res    map[string][]core.Table
	rules  []core.Rule
	snaps  []core.SnapshotInfo
	logs   []logLine
	err    error
}
type linesMsg struct {
	lines   []string
	inspect bool
}
type flashMsg string

func (m *model) Init() tea.Cmd { return m.fetch() }

func tick() tea.Cmd { return tea.Tick(time.Second, func(time.Time) tea.Msg { return tickMsg{} }) }

var client = &http.Client{Timeout: 5 * time.Second}

func (m *model) get(path string, out any) error {
	res, err := client.Get(m.endpoint + path)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("%s: %s", res.Status, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (m *model) send(method, path string, body any) tea.Cmd {
	return func() tea.Msg {
		var r io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			r = bytes.NewReader(b)
		}
		req, _ := http.NewRequest(method, m.endpoint+path, r)
		res, err := client.Do(req)
		if err != nil {
			return flashMsg("error: " + err.Error())
		}
		res.Body.Close()
		if res.StatusCode >= 400 {
			return flashMsg("error: " + res.Status)
		}
		return flashMsg(method + " " + path + " ✓")
	}
}

// fetch loads everything the visible tab needs in one command.
func (m *model) fetch() tea.Cmd {
	after, tab := m.after, m.tab
	return func() tea.Msg {
		var d dataMsg
		if d.err = m.get("/_pogi/api/info", &d.status); d.err != nil {
			return d
		}
		d.err = m.get("/_pogi/api/requests?limit=500&after="+strconv.FormatInt(after, 10), &d.reqs)
		switch tab {
		case 2:
			d.err = m.get("/_pogi/api/resources", &d.res)
		case 3:
			d.err = m.get("/_pogi/api/chaos", &d.rules)
		case 4:
			d.err = m.get("/_pogi/api/snapshots", &d.snaps)
		case 5:
			d.logs, d.err = m.fetchLogs()
		}
		return d
	}
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tickMsg:
		return m, m.fetch()
	case dataMsg:
		m.err = msg.err
		if msg.err == nil {
			m.status = msg.status
			if !m.paused && len(msg.reqs) > 0 {
				m.reqs = append(m.reqs, msg.reqs...)
				m.after = msg.reqs[len(msg.reqs)-1].ID
				if len(m.reqs) > 2000 {
					m.reqs = m.reqs[len(m.reqs)-2000:]
				}
			}
			if msg.res != nil {
				m.res = msg.res
			}
			if msg.rules != nil || m.tab == 3 {
				m.rules = msg.rules
			}
			if msg.snaps != nil || m.tab == 4 {
				m.snaps = msg.snaps
			}
			if msg.logs != nil && !m.logHold {
				m.logs = msg.logs
			}
		}
		return m, tick()
	case linesMsg:
		m.scroll = 0
		if msg.inspect {
			m.inspect = msg.lines
		} else {
			m.detail = msg.lines
		}
	case flashMsg:
		m.flash = string(msg)
		return m, m.fetch()
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m *model) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	key := k.String()
	if m.filtering {
		switch key {
		case "enter", "esc":
			m.filtering = false
		case "backspace":
			if m.filter != "" {
				m.filter = m.filter[:len(m.filter)-1]
			}
		default:
			m.filter += k.Text
		}
		m.cursor = 0
		return m, nil
	}
	if m.confirm != "" {
		c := m.confirm
		m.confirm = ""
		if key == "y" && c == "reset" {
			return m, m.send("POST", "/_pogi/api/reset", nil)
		}
		m.flash = "cancelled"
		return m, nil
	}
	if m.detail != nil || m.inspect != nil {
		switch key {
		case "esc", "q", "backspace", "left", "h":
			m.detail, m.inspect = nil, nil
		case "down", "j":
			m.scroll++
		case "up", "k":
			m.scroll = max(0, m.scroll-1)
		case "pgdown", "space":
			m.scroll += m.h - 6
		case "pgup":
			m.scroll = max(0, m.scroll-(m.h-6))
		case "ctrl+c":
			return m, tea.Quit
		}
		return m, nil
	}
	switch key {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "tab", "right", "l":
		if m.tab == 2 && (key == "right" || key == "l") {
			m.svc, m.row = m.svc+1, 0
			return m, nil
		}
		m.tab = (m.tab + 1) % len(tabs)
		m.pick, m.flash = 0, ""
		return m, m.fetch()
	case "shift+tab", "left", "h":
		if m.tab == 2 && (key == "left" || key == "h") {
			m.svc, m.row = max(0, m.svc-1), 0
			return m, nil
		}
		m.tab = (m.tab + len(tabs) - 1) % len(tabs)
		m.pick, m.flash = 0, ""
		return m, m.fetch()
	case "1", "2", "3", "4", "5", "6":
		m.tab = int(key[0] - '1')
		m.pick, m.flash = 0, ""
		return m, m.fetch()
	case "R":
		m.confirm = "reset"
		return m, nil
	}
	switch m.tab {
	case 1:
		return m.requestsKey(key)
	case 2:
		return m.resourcesKey(key)
	case 3:
		return m.chaosKey(key)
	case 4:
		return m.snapshotsKey(key)
	case 5:
		return m.logsKey(key)
	}
	return m, nil
}

func (m *model) visibleReqs() []core.Entry {
	var out []core.Entry
	f := strings.ToLower(m.filter)
	for i := len(m.reqs) - 1; i >= 0; i-- {
		e := m.reqs[i]
		if m.errors && e.Status < 400 {
			continue
		}
		if f != "" && !strings.Contains(strings.ToLower(e.Service+" "+e.Op+" "+e.Path+" "+e.Error), f) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (m *model) requestsKey(key string) (tea.Model, tea.Cmd) {
	vis := m.visibleReqs()
	switch key {
	case "down", "j":
		m.cursor = min(m.cursor+1, max(0, len(vis)-1))
	case "up", "k":
		m.cursor = max(0, m.cursor-1)
	case "p", " ":
		m.paused = !m.paused
	case "/":
		m.filtering = true
	case "e":
		m.errors, m.cursor = !m.errors, 0
	case "c":
		m.filter, m.errors, m.cursor = "", false, 0
	case "enter":
		if m.cursor < len(vis) {
			id := vis[m.cursor].ID
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

func (m *model) tables() (string, []core.Table) {
	names := core.SortedKeys(m.res)
	if len(names) == 0 {
		return "", nil
	}
	m.svc = min(m.svc, len(names)-1)
	return names[m.svc], m.res[names[m.svc]]
}

func (m *model) resourcesKey(key string) (tea.Model, tea.Cmd) {
	svc, tables := m.tables()
	var rows [][]string
	var tableOf []string
	for _, t := range tables {
		for _, r := range t.Rows {
			rows, tableOf = append(rows, r), append(tableOf, t.Name)
		}
	}
	switch key {
	case "down", "j":
		m.row = min(m.row+1, max(0, len(rows)-1))
	case "up", "k":
		m.row = max(0, m.row-1)
	case "enter":
		if m.row < len(rows) {
			path := "/_pogi/api/resources/" + svc + "/" + url.PathEscape(tableOf[m.row]) + "/" + url.PathEscape(rows[m.row][0])
			return m, func() tea.Msg {
				var v any
				if err := m.get(path, &v); err != nil {
					return flashMsg(err.Error())
				}
				b, _ := json.MarshalIndent(v, "", "  ")
				return linesMsg{lines: strings.Split(string(b), "\n"), inspect: true}
			}
		}
	case "x":
		return m, m.send("POST", "/_pogi/api/reset?service="+svc, nil)
	}
	return m, nil
}

func (m *model) chaosKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "down", "j":
		m.pick = min(m.pick+1, max(0, len(m.rules)-1))
	case "up", "k":
		m.pick = max(0, m.pick-1)
	case "t":
		return m, m.send("POST", "/_pogi/api/chaos", core.Rule{Probability: 0.2, Status: 400, Code: "ThrottlingException", Message: "Rate exceeded (pogi chaos)"})
	case "s":
		return m, m.send("POST", "/_pogi/api/chaos", core.Rule{LatencyMs: 800})
	case "o":
		return m, m.send("POST", "/_pogi/api/chaos", core.Rule{Status: 503, Code: "ServiceUnavailable", Message: "Service unavailable (pogi chaos)"})
	case "d", "x", "backspace":
		if m.pick < len(m.rules) {
			return m, m.send("DELETE", "/_pogi/api/chaos/"+m.rules[m.pick].ID, nil)
		}
	case "C":
		return m, m.send("DELETE", "/_pogi/api/chaos", nil)
	}
	return m, nil
}

func (m *model) snapshotsKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "down", "j":
		m.pick = min(m.pick+1, max(0, len(m.snaps)-1))
	case "up", "k":
		m.pick = max(0, m.pick-1)
	case "s":
		return m, m.send("POST", "/_pogi/api/snapshots/"+time.Now().Format("2006-01-02_15-04-05"), nil)
	case "enter":
		if m.pick < len(m.snaps) {
			return m, m.send("POST", "/_pogi/api/snapshots/"+url.PathEscape(m.snaps[m.pick].Name)+"/load", nil)
		}
	case "d", "x":
		if m.pick < len(m.snaps) {
			return m, m.send("DELETE", "/_pogi/api/snapshots/"+url.PathEscape(m.snaps[m.pick].Name), nil)
		}
	}
	return m, nil
}

// ---- view ----

func (m *model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	v.WindowTitle = "pogi"
	return v
}

func (m *model) render() string {
	if m.w == 0 {
		return "starting…"
	}
	var b strings.Builder
	total := 0
	for _, s := range m.status.Services {
		total += s.Requests
	}
	head := accent.Render("pogi") + dim.Render(fmt.Sprintf(" %s · %s · up %s · %d services · %d requests",
		m.status.Version, m.endpoint, time.Duration(m.status.Uptime)*time.Second, len(m.status.Services), total))
	b.WriteString(head + "\n")
	var t []string
	for i, name := range tabs {
		label := fmt.Sprintf(" %d %s ", i+1, name)
		if i == m.tab {
			t = append(t, sel.Render(label))
		} else {
			t = append(t, dim.Render(label))
		}
	}
	b.WriteString(strings.Join(t, " ") + "\n\n")
	body := m.body()
	if m.err != nil {
		body = bad.Render("cannot reach pogi: "+m.err.Error()) + "\n" + body
	}
	lines := strings.Split(body, "\n")
	room := max(1, m.h-5)
	if len(lines) > room {
		lines = lines[:room]
	}
	for _, l := range lines {
		b.WriteString(truncate(l, m.w) + "\n")
	}
	for i := len(lines); i < room; i++ {
		b.WriteString("\n")
	}
	foot := m.help()
	if m.confirm == "reset" {
		foot = warn.Render("Reset ALL state in every service? y/N")
	} else if m.flash != "" {
		foot = m.flash + dim.Render("  ·  ") + foot
	}
	b.WriteString(dim.Render(truncate(foot, m.w)))
	return b.String()
}

func (m *model) help() string {
	common := "tab/1-6 switch · R reset all · q quit"
	if m.detail != nil || m.inspect != nil {
		return "↑↓ scroll · esc back"
	}
	switch m.tab {
	case 1:
		state := map[bool]string{true: "paused", false: "live"}[m.paused]
		if m.filtering {
			return "type to filter · enter done"
		}
		return fmt.Sprintf("[%s] ↑↓ move · enter details · / filter · e errors · p pause · c clear · %s", state, common)
	case 2:
		return "←→ service · ↑↓ row · enter inspect · x reset service · " + common
	case 3:
		return "t throttle 20% · s +800ms · o outage · ↑↓ d remove · C clear · " + common
	case 4:
		return "s save · ↑↓ enter load · d delete · " + common
	case 5:
		return map[bool]string{true: "[paused]", false: "[live]"}[m.logHold] + " p pause · ↑↓ scroll · " + common
	}
	return common
}

func (m *model) body() string {
	if m.detail != nil {
		return scrolled(m.detail, m.scroll)
	}
	if m.inspect != nil {
		return scrolled(m.inspect, m.scroll)
	}
	switch m.tab {
	case 1:
		return m.requestsView()
	case 2:
		return m.resourcesView()
	case 3:
		return m.chaosView()
	case 4:
		return m.snapshotsView()
	case 5:
		return m.logsView()
	}
	return m.dashboard()
}

func scrolled(lines []string, off int) string {
	off = min(off, max(0, len(lines)-1))
	return strings.Join(lines[off:], "\n")
}

func (m *model) dashboard() string {
	var b strings.Builder
	for _, c := range []string{core.AWS, core.GCP, core.Azure} {
		first, idle := true, 0
		for _, s := range m.status.Services {
			if s.Cloud != c {
				continue
			}
			if first {
				b.WriteString(clouds[c].Bold(true).Render(strings.ToUpper(c)) + "\n")
				first = false
			}
			if s.Resources == 0 && s.Requests == 0 { // hundreds of services: list the ones in use
				idle++
				continue
			}
			fmt.Fprintf(&b, "  %-16s %s %s\n", truncate(s.Title, 16), bold.Render(fmt.Sprintf("%5d", s.Resources))+dim.Render(" resources"),
				bold.Render(fmt.Sprintf("%6d", s.Requests))+dim.Render(" requests"))
		}
		if idle > 0 {
			b.WriteString(dim.Render(fmt.Sprintf("  + %d services with nothing in them yet", idle)) + "\n")
		}
		if !first {
			b.WriteString("\n")
		}
	}
	b.WriteString(bold.Render("Recent") + "\n")
	n := 0
	for i := len(m.reqs) - 1; i >= 0 && n < 10; i, n = i-1, n+1 {
		b.WriteString("  " + entryLine(m.reqs[i]) + "\n")
	}
	if n == 0 {
		b.WriteString(dim.Render("  no requests yet — try: pogi aws s3 mb s3://demo") + "\n")
	}
	b.WriteString("\n" + dim.Render("console: "+m.endpoint+"/_pogi/   ·   env: eval \"$(pogi env)\""))
	return b.String()
}

func entryLine(e core.Entry) string {
	st := ok
	switch {
	case e.Status >= 500:
		st = bad
	case e.Status >= 400:
		st = warn
	}
	s := fmt.Sprintf("%s %s %-28s %s %s", dim.Render(e.Time.Local().Format("15:04:05.000")),
		clouds[e.Cloud].Render(fmt.Sprintf("%-10s", e.Service)), e.Op, st.Render(strconv.Itoa(e.Status)), dim.Render(fmt.Sprintf("%.1fms", e.Ms)))
	if e.Error != "" {
		s += " " + st.Render(e.Error)
	}
	if e.Chaos {
		s += bad.Render(" chaos")
	}
	return s
}

func (m *model) requestsView() string {
	vis := m.visibleReqs()
	var b strings.Builder
	if m.filter != "" || m.filtering || m.errors {
		f := "filter: " + m.filter
		if m.filtering {
			f += "▏"
		}
		if m.errors {
			f += "  [errors only]"
		}
		b.WriteString(accent.Render(f) + "\n")
	}
	if len(vis) == 0 {
		b.WriteString(dim.Render("no requests match") + "\n")
	}
	m.cursor = min(m.cursor, max(0, len(vis)-1))
	start := max(0, m.cursor-(m.h-10))
	for i := start; i < len(vis) && i < start+m.h; i++ {
		line := fmt.Sprintf("%5d %s", vis[i].ID, entryLine(vis[i]))
		if i == m.cursor {
			line = sel.Render(fmt.Sprintf("%5d", vis[i].ID)) + line[5:]
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

func entryLines(e core.Entry) []string {
	out := []string{
		bold.Render(e.Service+" "+e.Op) + " " + strconv.Itoa(e.Status) + " " + e.Error,
		dim.Render(e.Method + " " + e.Path),
		dim.Render(fmt.Sprintf("%s · %.2fms · account %s · %s", e.Time.Local().Format(time.DateTime), e.Ms, e.Account, e.Region)),
		"", bold.Render("Request headers"),
	}
	for _, k := range core.SortedKeys(e.ReqHeaders) {
		out = append(out, "  "+dim.Render(k+":")+" "+e.ReqHeaders[k])
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
		return "  " + buf.String()
	}
	return "  " + strings.ReplaceAll(s, "><", ">\n  <")
}

func (m *model) resourcesView() string {
	names := core.SortedKeys(m.res)
	if len(names) == 0 {
		return dim.Render("no browsable services")
	}
	svc, tables := m.tables()
	var b strings.Builder
	b.WriteString(m.serviceStrip(names, svc) + "\n\n")
	idx := 0
	for _, t := range tables {
		b.WriteString(bold.Render(fmt.Sprintf("%s (%d)", t.Name, len(t.Rows))) + "\n")
		widths := make([]int, len(t.Columns))
		for i, c := range t.Columns {
			widths[i] = len(c)
		}
		for _, r := range t.Rows {
			for i, c := range r {
				if i < len(widths) {
					widths[i] = min(max(widths[i], len(c)), 40)
				}
			}
		}
		b.WriteString(dim.Render("  "+row(t.Columns, widths)) + "\n")
		for _, r := range t.Rows {
			line := "  " + row(r, widths)
			if idx == m.row {
				line = sel.Render(line)
			}
			b.WriteString(line + "\n")
			idx++
		}
		if len(t.Rows) == 0 {
			b.WriteString(dim.Render("  (none)") + "\n")
		}
		b.WriteString("\n")
	}
	return b.String()
}

// serviceStrip shows the services of all three clouds as one line: the selected
// one with as many neighbours as fit, and a count where the line is cut.
func (m *model) serviceStrip(names []string, selected string) string {
	cur := max(0, slices.Index(names, selected))
	lo, hi, width, room := cur, cur, len(selected)+3, max(20, m.w-8)
	for grew := true; grew; {
		grew = false
		if lo > 0 && width+len(names[lo-1])+3 <= room {
			lo--
			width += len(names[lo]) + 3
			grew = true
		}
		if hi < len(names)-1 && width+len(names[hi+1])+3 <= room {
			hi++
			width += len(names[hi]) + 3
			grew = true
		}
	}
	var parts []string
	if lo > 0 {
		parts = append(parts, dim.Render(fmt.Sprintf("‹%d", lo)))
	}
	for _, n := range names[lo : hi+1] {
		style := dim
		for _, s := range m.status.Services {
			if s.Name == n {
				style = clouds[s.Cloud]
			}
		}
		if n == selected {
			style = sel
		}
		parts = append(parts, style.Render(" "+n+" "))
	}
	if hi < len(names)-1 {
		parts = append(parts, dim.Render(fmt.Sprintf("%d›", len(names)-1-hi)))
	}
	return strings.Join(parts, " ")
}

func row(cells []string, widths []int) string {
	var parts []string
	for i, c := range cells {
		if i < len(widths) {
			parts = append(parts, fmt.Sprintf("%-*s", widths[i], truncate(c, widths[i])))
		}
	}
	return strings.Join(parts, "  ")
}

func (m *model) chaosView() string {
	if len(m.rules) == 0 {
		return dim.Render("no chaos rules — everything behaves. t: throttle 20% · s: +800ms · o: outage")
	}
	var b strings.Builder
	b.WriteString(dim.Render(fmt.Sprintf("  %-10s %-10s %-20s %6s %8s %-28s %5s", "ID", "SERVICE", "OP", "RATE", "LATENCY", "ERROR", "HITS")) + "\n")
	for i, r := range m.rules {
		p := r.Probability
		if p == 0 {
			p = 1
		}
		line := fmt.Sprintf("  %-10s %-10s %-20s %5.0f%% %6dms %-28s %5d", r.ID, or(r.Service, "*"), or(r.Op, "*"), p*100, r.LatencyMs,
			strings.TrimSpace(fmt.Sprintf("%s %s", map[bool]string{true: strconv.Itoa(r.Status), false: ""}[r.Status > 0], r.Code)), r.Hits)
		if i == m.pick {
			line = sel.Render(line)
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

func (m *model) snapshotsView() string {
	if len(m.snaps) == 0 {
		return dim.Render("no snapshots yet — press s to save the current state")
	}
	var b strings.Builder
	for i, s := range m.snaps {
		line := fmt.Sprintf("  %-40s %10d bytes  %s", s.Name, s.Size, s.Saved.Local().Format(time.DateTime))
		if i == m.pick {
			line = sel.Render(line)
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// truncate cuts a styled line to the terminal width.
func truncate(s string, w int) string {
	if w <= 0 {
		return s
	}
	return ansi.Truncate(s, w, "…")
}
