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

var tabs = []string{"Overview", "Requests", "Resources", "Chaos", "Snapshots", "Logs", "Flows"}

const (
	tabOverview = iota
	tabRequests
	tabResources
	tabChaos
	tabSnapshots
	tabLogs
	tabFlows
)

var (
	// selStyle marks the selection and the active tab. Bold keeps it visible when colour is off.
	selStyle = lipgloss.NewStyle().Background(lipgloss.Color("#ff8cc6")).Foreground(lipgloss.Color("#3d0a26")).Bold(true)
	dim      = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	bold     = lipgloss.NewStyle().Bold(true)
	ok       = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	warn     = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	bad      = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	clouds   = map[string]lipgloss.Style{
		core.AWS:   lipgloss.NewStyle().Foreground(lipgloss.Color("214")),
		core.GCP:   lipgloss.NewStyle().Foreground(lipgloss.Color("33")),
		core.Azure: lipgloss.NewStyle().Foreground(lipgloss.Color("39")),
	}
	cloudNames = map[string]string{core.AWS: "AWS", core.GCP: "Google Cloud", core.Azure: "Azure"}
	cloudShort = map[string]string{core.AWS: "AWS", core.GCP: "GCP", core.Azure: "AZURE"}
)

// pending is a question the user must answer with y before the command runs.
type pending struct {
	prompt string
	run    func() tea.Cmd
}

type model struct {
	endpoint string
	embedded bool
	w, h     int
	tab      int
	err      error
	flash    string
	light    bool // the terminal has a light background
	now      time.Time
	confirm  *pending

	status core.Status
	rules  []core.Rule
	reqs   []core.Entry
	after  int64
	paused bool

	filter    string // the text typed after / on the current tab
	filtering bool
	ovSvc     string // Overview: the selected service; "" is the top row
	ovSort    int

	reqPin    bool  // the Requests selection stays on reqSel; false: it follows the newest call
	reqSel    int64 // ID of the selected request
	errors    bool
	cloud     int // 0 all, then aws, gcp, azure
	reqSort   int // 0 newest, 1 slowest, 2 errors first
	detail    []string
	inspect   []string
	scroll    int
	find      string // Resources: the text typed after / (kept on the tab)
	logFilter string
	logWarn   bool

	res     map[string][]core.Table
	resName string
	resAll  bool
	pane    int // 0 services, 1 tables
	row     int

	snaps []core.SnapshotInfo
	pick  int

	logs     []logLine
	logHold  bool // paused: the tail stops following
	logAbove int  // lines scrolled up from the newest while paused

	spans   []core.Span
	flowPin string // spanID of the selected Flows step; "" follows the newest
}

type tickMsg struct{}
type dataMsg struct {
	tab    int  // the tab the fetch was for
	loop   bool // the fetch came from the once-a-second poll, which schedules the next one
	status core.Status
	rules  []core.Rule
	reqs   []core.Entry
	res    map[string][]core.Table
	snaps  []core.SnapshotInfo
	logs   []logLine
	spans  []core.Span
	err    error
}
type linesMsg struct {
	lines   []string
	inspect bool
}
type flashMsg string

func (m *model) Init() tea.Cmd { return tea.Batch(m.fetch(true), tea.RequestBackgroundColor) }

func tick() tea.Cmd { return tea.Tick(time.Second, func(time.Time) tea.Msg { return tickMsg{} }) }

var client = &http.Client{Timeout: 5 * time.Second}

func (m *model) clock() time.Time {
	if m.now.IsZero() {
		return time.Now()
	}
	return m.now
}

// acc draws accent text: pink on dark terminals, a deeper pink on light ones.
func (m *model) acc(s string) string {
	c := "#ff8cc6"
	if m.light {
		c = "#c4176f"
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(c)).Bold(true).Render(s)
}

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

// ask shows a question in the footer; the command runs only if the user answers y.
func (m *model) ask(prompt string, run func() tea.Cmd) (tea.Model, tea.Cmd) {
	m.confirm = &pending{prompt, run}
	return m, nil
}

// fetch loads everything the visible tab needs in one command. Only the poll (loop) schedules the next
// fetch: a fetch made for a key press must not start a second poll, or each tab switch adds one more.
func (m *model) fetch(loop bool) tea.Cmd {
	after, tab := m.after, m.tab
	return func() tea.Msg {
		d := dataMsg{tab: tab, loop: loop}
		if d.err = m.get("/_pogi/api/info", &d.status); d.err != nil {
			return d
		}
		if d.err = m.get("/_pogi/api/chaos", &d.rules); d.err != nil {
			return d
		}
		if d.err = m.get("/_pogi/api/requests?limit=500&after="+strconv.FormatInt(after, 10), &d.reqs); d.err != nil {
			return d
		}
		switch tab {
		case tabResources:
			d.err = m.get("/_pogi/api/resources", &d.res)
		case tabSnapshots:
			d.err = m.get("/_pogi/api/snapshots", &d.snaps)
		case tabLogs:
			d.logs, d.err = m.fetchLogs(d.status.Services)
		case tabFlows:
			d.err = m.get("/_pogi/api/spans?limit=500", &d.spans)
		}
		return d
	}
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.BackgroundColorMsg:
		m.light = !msg.IsDark()
	case tickMsg:
		return m, m.fetch(true)
	case dataMsg:
		m.err = msg.err
		if msg.err == nil {
			m.status, m.rules = msg.status, msg.rules
			if !m.paused {
				for _, e := range msg.reqs { // two fetches in flight can bring the same calls
					if e.ID > m.after {
						m.reqs, m.after = append(m.reqs, e), e.ID
					}
				}
				if len(m.reqs) > 2000 {
					m.reqs = m.reqs[len(m.reqs)-2000:]
				}
			}
			switch msg.tab {
			case tabResources:
				m.res = msg.res
			case tabSnapshots:
				m.snaps = msg.snaps
			case tabLogs:
				if !m.logHold {
					m.logs = msg.logs
				}
			case tabFlows:
				m.spans = msg.spans
			}
		}
		if msg.loop {
			return m, tick()
		}
		return m, nil
	case linesMsg:
		m.scroll = 0
		if msg.inspect {
			m.inspect = msg.lines
		} else {
			m.detail = msg.lines
		}
	case flashMsg:
		m.flash = string(msg)
		return m, m.fetch(false)
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

// goTab switches tabs and loads what the new tab shows.
func (m *model) goTab(t int) (tea.Model, tea.Cmd) {
	m.tab, m.pick, m.flash = t, 0, ""
	m.detail, m.inspect = nil, nil
	return m, m.fetch(false)
}

func (m *model) filterText() *string {
	switch m.tab {
	case tabResources:
		return &m.find
	case tabLogs:
		return &m.logFilter
	}
	return &m.filter
}

func (m *model) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	key := k.String()
	if m.filtering {
		f := m.filterText()
		switch key {
		case "enter", "esc":
			m.filtering = false
		case "backspace":
			if *f != "" {
				_, n := lastRune(*f)
				*f = (*f)[:len(*f)-n]
			}
		default:
			*f += k.Text
		}
		m.reqPin, m.flowPin, m.row = false, "", 0
		return m, nil
	}
	if m.confirm != nil {
		c := m.confirm
		m.confirm = nil
		if key == "y" {
			return m, c.run()
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
		if m.tab == tabResources && key != "tab" {
			return m.resourcesKey(key)
		}
		return m.goTab((m.tab + 1) % len(tabs))
	case "shift+tab", "left", "h":
		if m.tab == tabResources && key != "shift+tab" {
			return m.resourcesKey(key)
		}
		return m.goTab((m.tab + len(tabs) - 1) % len(tabs))
	case "1", "2", "3", "4", "5", "6", "7":
		return m.goTab(int(key[0] - '1'))
	case "R":
		return m.ask("Reset ALL state in every service?", func() tea.Cmd { return m.send("POST", "/_pogi/api/reset", nil) })
	}
	switch m.tab {
	case tabOverview:
		return m.overviewKey(key)
	case tabRequests:
		return m.requestsKey(key)
	case tabResources:
		return m.resourcesKey(key)
	case tabChaos:
		return m.chaosKey(key)
	case tabSnapshots:
		return m.snapshotsKey(key)
	case tabLogs:
		return m.logsKey(key)
	case tabFlows:
		return m.flowsKey(key)
	}
	return m, nil
}

func lastRune(s string) (rune, int) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i]&0xC0 != 0x80 {
			return rune(s[i]), len(s) - i
		}
	}
	return 0, 0
}

// nav moves a list selection for the keys every list shares.
func (m *model) nav(key string, cur, n int) (int, bool) {
	page := max(1, m.h-8)
	switch key {
	case "down", "j":
		cur++
	case "up", "k":
		cur--
	case "pgdown":
		cur += page
	case "pgup":
		cur -= page
	case "home":
		cur = 0
	case "end":
		cur = n - 1
	default:
		return cur, false
	}
	return min(max(0, cur), max(0, n-1)), true
}

func (m *model) chaosKey(key string) (tea.Model, tea.Cmd) {
	if p, ok := m.nav(key, m.pick, len(m.rules)); ok {
		m.pick = p
		return m, nil
	}
	switch key {
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
	if p, ok := m.nav(key, m.pick, len(m.snaps)); ok {
		m.pick = p
		return m, nil
	}
	switch key {
	case "s":
		return m, m.send("POST", "/_pogi/api/snapshots/"+time.Now().Format("2006-01-02_15-04-05"), nil)
	case "enter":
		if m.pick < len(m.snaps) {
			name := m.snaps[m.pick].Name
			return m.ask("Load snapshot "+name+"? It replaces the current state.", func() tea.Cmd {
				return m.send("POST", "/_pogi/api/snapshots/"+url.PathEscape(name)+"/load", nil)
			})
		}
	case "d", "x":
		if m.pick < len(m.snaps) {
			name := m.snaps[m.pick].Name
			return m.ask("Delete snapshot "+name+"?", func() tea.Cmd {
				return m.send("DELETE", "/_pogi/api/snapshots/"+url.PathEscape(name), nil)
			})
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
	if m.w < 60 || m.h < 10 {
		return truncate("make the window bigger (at least 60×10)", m.w)
	}
	lines := []string{m.statusLine(), m.tabLine(), ""}
	room := m.h - 4
	body := m.body(room)
	if m.err != nil {
		body = append([]string{bad.Render("cannot reach pogi: " + m.err.Error())}, body...)
	}
	for i := 0; i < room; i++ {
		l := ""
		if i < len(body) {
			l = body[i]
		}
		lines = append(lines, truncate(l, m.w))
	}
	return strings.Join(append(lines, m.footer()), "\n")
}

// statusLine: what is running, on the left; chaos and problems on the right.
func (m *model) statusLine() string {
	st := m.status
	host := strings.TrimPrefix(strings.TrimPrefix(m.endpoint, "http://"), "https://")
	ready := ok.Render("● ready")
	if !st.Ready {
		ready = warn.Render("○ starting")
	}
	state := "state in memory"
	if st.Persist {
		state = "state saved"
	}
	chaos, probs := dim.Render("chaos off"), dim.Render("0 problems")
	if n := len(m.rules); n > 0 {
		chaos = warn.Render(fmt.Sprintf("chaos %d %s", n, plural(n, "rule", "rules")))
	}
	if n := len(st.Problems); n > 0 {
		probs = bad.Render(fmt.Sprintf("%d %s", n, plural(n, "problem", "problems")))
	}
	// Too wide: drop the region, then IAM, then shorten the host to its port, the state to one word and the
	// chaos count, then drop the state and the uptime.
	for drop := 0; ; drop++ {
		h, s, c := host, state, chaos
		if i := strings.LastIndexByte(host, ':'); drop >= 2 && i >= 0 {
			h = host[i:]
		}
		if drop >= 3 {
			s = map[bool]string{true: "saved", false: "memory"}[st.Persist]
		}
		if drop >= 4 && len(m.rules) > 0 {
			c = warn.Render(fmt.Sprintf("chaos %d", len(m.rules)))
		}
		parts := []string{or(st.Version, "dev"), h}
		if drop < 6 {
			parts = append(parts, "up "+uptime(st.Uptime))
		}
		parts = append(parts, ready)
		if drop < 5 {
			parts = append(parts, dim.Render(s))
		}
		if drop < 1 {
			parts = append(parts, dim.Render("IAM "+or(st.IAM, "off")))
		}
		if drop < 1 && st.Region != "" {
			parts = append(parts, dim.Render(st.Region))
		}
		left := selStyle.Render("pogi") + " " + strings.Join(parts, dim.Render(" · "))
		right := c + dim.Render(" · ") + probs
		gap := m.w - ansi.StringWidth(left) - ansi.StringWidth(right)
		if gap >= 2 || drop == 6 {
			return truncate(left+strings.Repeat(" ", max(1, gap))+right, m.w)
		}
	}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func (m *model) tabLine() string {
	build := func(short bool) string {
		var t []string
		for i, name := range tabs {
			label := fmt.Sprintf(" %d %s ", i+1, name)
			switch {
			case i == m.tab:
				t = append(t, selStyle.Render(label))
			case short:
				t = append(t, dim.Render(fmt.Sprintf(" %d ", i+1)))
			default:
				t = append(t, dim.Render(label))
			}
		}
		return strings.Join(t, "")
	}
	if s := build(false); ansi.StringWidth(s) <= m.w {
		return s
	}
	return build(true)
}

func (m *model) footer() string {
	if m.confirm != nil {
		return truncate(warn.Render(m.confirm.prompt+" y/N"), m.w)
	}
	quit := "q quit (pogi keeps running)"
	if m.embedded {
		quit = "q stop pogi"
	}
	var items []string
	switch {
	case m.detail != nil || m.inspect != nil:
		items = []string{"↑↓ scroll", "esc back"}
	case m.filtering:
		items = []string{"type to filter", "enter done", "esc done"}
	default:
		switch m.tab {
		case tabOverview:
			items = []string{"↑↓ service", "enter open in Resources", "s sort", "1-7 tabs", "R reset all"}
		case tabRequests:
			items = []string{"↑↓ move", "enter full request", "/ filter", "e errors", "f cloud", "s sort", "p pause", "c clear", "1-7 tabs", "R reset all"}
		case tabResources:
			pane := "←→ pane"
			if m.w < 100 {
				pane = "←→ service"
			}
			items = []string{pane, "↑↓ move", "enter inspect", "/ find service", "a all services", "x reset service", "R reset all"}
		case tabChaos:
			items = []string{"t throttle 20%", "s add 800 ms", "o outage", "↑↓ pick", "d remove", "C clear all", "R reset all"}
		case tabSnapshots:
			items = []string{"s save", "↑↓ pick", "enter load", "d delete", "R reset all"}
		case tabLogs:
			items = []string{"p pause", "↑↓ scroll", "G latest", "/ filter", "w warnings+errors", "R reset all"}
		case tabFlows:
			items = []string{"↑↓ move", "enter see its requests", "R reset all"}
		}
	}
	// keep the tab's own keys first; the quit hint always stays
	var out []string
	for _, it := range items {
		if ansi.StringWidth(strings.Join(append(out, it, quit), " · ")) > m.w {
			break
		}
		out = append(out, it)
	}
	foot := strings.Join(append(out, quit), " · ")
	if m.flash != "" {
		foot = m.flash + " · " + foot
	}
	return truncate(dim.Render(foot), m.w)
}

func (m *model) body(room int) []string {
	switch {
	case m.detail != nil:
		return scrolled(m.detail, m.scroll)
	case m.inspect != nil:
		return scrolled(m.inspect, m.scroll)
	}
	switch m.tab {
	case tabRequests:
		return m.requestsView(room)
	case tabResources:
		return m.resourcesView(room)
	case tabChaos:
		return m.chaosView(room)
	case tabSnapshots:
		return m.snapshotsView(room)
	case tabLogs:
		return m.logsView(room)
	case tabFlows:
		return m.flowsView(room)
	}
	return m.overviewView(room)
}

func scrolled(lines []string, off int) []string {
	off = min(off, max(0, len(lines)-1))
	return lines[off:]
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
