package web

// The shell around every page: the sidebar, the ⌘K search, the drawer and toast protocol,
// and the service catalog data generated from docs/reference/coverage.yaml.

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nrndev/pogi/internal/core"
)

//go:embed coverage.json
var coverageJSON []byte

// covService is one service of coverage.json: its catalog category and what it supports.
type covService struct {
	Title, Cloud, Category, Depth string
	Docker                        bool
	Covered                       string
	NotYet                        string `json:"not_yet"`
}

// coverage is coverage.json, decoded once.
var coverage = sync.OnceValue(func() (c struct {
	Categories []string
	Services   map[string]covService
}) {
	if err := json.Unmarshal(coverageJSON, &c); err != nil {
		panic(err)
	}
	return c
})

// nav is the sidebar: services in use, pinned services, and badges by key (a service name or a
// tool: "requests", "chaos", "iam", "cost", "inbox").
type nav struct {
	InUse, Pinned []navSvc
	Badges        map[string]badge
	Total         int
}

// navSvc is one sidebar service. Title is its label, Abbr its rail label; when two services share
// either, both carry the cloud. Full is the label with the cloud always, for the rail's title and aria-label.
type navSvc struct {
	Name, Title, Cloud, Abbr, Full string
	Resources                      int
}

// badge is a sidebar count; Kind "bad" or "warn" also puts a dot on the collapsed rail.
type badge struct{ Text, Kind string }

// Hooks that features fill in from init functions, so one page can show another's numbers
// without the two depending on each other.
var (
	// badgeFuncs add sidebar badges (keys as in nav.Badges).
	badgeFuncs []func(ui *UI, r *http.Request, b map[string]badge)
	// costTotal is the Cost page's monthly estimate, for the Home card; ok is false when nothing is priced.
	costTotal func(ui *UI, r *http.Request) (perMonth float64, ok bool)
	// noteFuncs add notifications to the bell beyond failed requests (failed flows, IAM denials).
	noteFuncs []func(ui *UI) []note
)

// note is one notification. Kind is "bad" or "warn"; Href opens in the drawer when Drawer is set.
type note struct {
	Time                          time.Time
	Kind, Icon, Title, Text, Href string
	Drawer                        bool
}

func (ui *UI) nav(r *http.Request, st core.Status) nav {
	n := nav{Badges: map[string]badge{}, Total: len(st.Services)}
	pins := map[string]bool{}
	if c, err := r.Cookie("pogi-pins"); err == nil {
		v, _ := url.QueryUnescape(c.Value)
		for _, p := range strings.Split(v, ",") {
			pins[p] = true
		}
	}
	cloud := ui.cloud(r)
	for _, s := range st.Services {
		if cloud != "" && s.Cloud != cloud {
			continue
		}
		ns := navSvc{Name: s.Name, Title: s.Title, Cloud: s.Cloud, Abbr: abbr(s.Title), Resources: s.Resources}
		ns.Full = ns.Title + " · " + cloudShort[s.Cloud]
		// ponytail: "in use" needs traffic too, so built-in defaults (the default resource group,
		// project and users) stay out; state loaded from disk shows once it is called again.
		if s.Resources > 0 && s.Requests > 0 {
			n.InUse = append(n.InUse, ns)
		}
		if pins[s.Name] {
			n.Pinned = append(n.Pinned, ns)
		}
	}
	disambiguate(n.InUse, n.Pinned)
	slices.SortFunc(n.InUse, func(a, b navSvc) int { return strings.Compare(a.Title, b.Title) })
	if e := len(ui.hub.Requests.Query(0, 500, "", "", true)); e > 0 {
		n.Badges["requests"] = badge{plural(e, "error"), "bad"}
	}
	if c := len(ui.hub.Chaos.List()); c > 0 {
		n.Badges["chaos"] = badge{plural(c, "rule") + " on", "warn"}
	}
	for _, f := range badgeFuncs {
		f(ui, r, n.Badges)
	}
	return n
}

// cloudShort names a cloud in a label ("IAM · GCP"); cloudLetter ends its rail abbreviation ("IAMg").
var (
	cloudShort  = map[string]string{core.AWS: "AWS", core.GCP: "GCP", core.Azure: "Azure"}
	cloudLetter = map[string]string{core.AWS: "a", core.GCP: "g", core.Azure: "z"} // z: "a" is taken by AWS
)

// disambiguate gives the services in the lists that share a title or an abbreviation (IAM on AWS
// and on Google Cloud) the cloud in both: "IAM · AWS" with the abbreviation "IAMa".
func disambiguate(lists ...[]navSvc) {
	titles, abbrs := map[string]map[string]bool{}, map[string]map[string]bool{}
	seen := func(m map[string]map[string]bool, k, name string) {
		if m[k] == nil {
			m[k] = map[string]bool{}
		}
		m[k][name] = true
	}
	for _, l := range lists {
		for _, s := range l {
			seen(titles, s.Title, s.Name)
			seen(abbrs, s.Abbr, s.Name)
		}
	}
	for _, l := range lists {
		for i, s := range l {
			dupTitle := len(titles[s.Title]) > 1
			if dupTitle || len(abbrs[s.Abbr]) > 1 {
				l[i].Abbr += cloudLetter[s.Cloud]
			}
			if dupTitle {
				l[i].Title, l[i].Full = s.Full, s.Full
			}
		}
	}
}

// plural is "1 rule", "2 rules".
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// abbr is a service's label on the collapsed rail: its capitals and digits (DDB for DynamoDB),
// else its first two letters.
func abbr(title string) string {
	var b []rune
	for _, r := range title {
		if unicode.IsUpper(r) || unicode.IsDigit(r) {
			b = append(b, r)
		}
	}
	if len(b) < 2 {
		b = []rune(title)
		if len(b) > 2 {
			b = b[:2]
		}
	}
	return string(b[:min(3, len(b))])
}

type navLink struct {
	Href, Icon, Label string
	On                bool
	Badge             *badge
}

// navlink is the data of one sidebar link: active when the page's Active is key.
func navlink(href, icon, label, _ /* reserved */, active, key string, badges map[string]badge) navLink {
	l := navLink{Href: href, Icon: icon, Label: label, On: active == key}
	if b, ok := badges[key]; ok {
		l.Badge = &b
	}
	return l
}

// foot is the sidebar's last line.
func foot(st core.Status) string {
	where := "in memory"
	if st.Persist {
		where = "persisted"
	}
	return fmt.Sprintf("Up %s · %s", (time.Duration(st.Uptime) * time.Second).String(), where)
}

// cookie is a cookie's value when it is one of allowed, else "".
func cookie(r *http.Request, name string, allowed ...string) string {
	if c, err := r.Cookie(name); err == nil && slices.Contains(allowed, c.Value) {
		return c.Value
	}
	return ""
}

func inDrawer(r *http.Request) bool { return r.Header.Get("HX-Target") == "drawer" }

// trigger sends an htmx event with a detail object to the browser (HX-Trigger), ASCII-only.
func trigger(w http.ResponseWriter, event string, detail any) {
	b, _ := json.Marshal(map[string]any{event: detail})
	var sb strings.Builder
	for _, r := range string(b) {
		if r > 126 {
			fmt.Fprintf(&sb, `\u%04x`, r)
		} else {
			sb.WriteRune(r)
		}
	}
	w.Header().Set("HX-Trigger", sb.String())
}

// ---- ⌘K search ----

// hit is one search result. Drawer opens Href in the drawer, JS runs a client action
// (theme, side, keys, copy) and Post posts to Href.
type hit struct {
	Group, Label, Hint, Href, Icon, Cloud, JS string
	Drawer, Post                              bool
}

// pages are the console's pages and tools, always searchable.
var pages = []hit{
	{Label: "Home", Href: "/_pogi/", Icon: "home"}, {Label: "Activity", Href: "/_pogi/requests", Icon: "pulse"},
	{Label: "Flows", Href: "/_pogi/flows", Icon: "map"}, {Label: "Services", Href: "/_pogi/services", Icon: "grid"},
	{Label: "Map", Href: "/_pogi/map", Icon: "map"}, {Label: "IAM policy", Href: "/_pogi/iam", Icon: "lock"},
	{Label: "Cost estimate", Href: "/_pogi/cost", Icon: "dollar"}, {Label: "SQL editor", Href: "/_pogi/sql", Icon: "db"},
	{Label: "Logs", Href: "/_pogi/logs", Icon: "logs"}, {Label: "Secrets", Href: "/_pogi/secrets", Icon: "key"},
	{Label: "Inbox", Href: "/_pogi/inbox", Icon: "mail"}, {Label: "Chaos", Href: "/_pogi/chaos", Icon: "bolt"},
	{Label: "Snapshots", Href: "/_pogi/snapshots", Icon: "camera"}, {Label: "Connect", Href: "/_pogi/connect", Icon: "plug"},
}

// actions run from the palette.
var actions = []hit{
	{Label: "Save snapshot", Href: "/_pogi/snapshots", Post: true, Icon: "camera"},
	{Label: "Add chaos rule", Href: "/_pogi/chaos/new", Drawer: true, Icon: "bolt"},
	{Label: "Clear inbox", Href: "/_pogi/inbox/clear", Post: true, Icon: "trash"},
	{Label: "Show errors", Href: "/_pogi/requests?errors=1", Icon: "pulse"},
	{Label: "Copy endpoint", JS: "copy-endpoint", Icon: "copy"},
	{Label: "Switch theme", JS: "theme", Icon: "moon"},
	{Label: "Collapse sidebar", JS: "side", Icon: "panel"},
	{Label: "Keyboard shortcuts", JS: "keys", Icon: "help"},
}

var cloudNames = map[string]string{core.AWS: "AWS", core.GCP: "Google Cloud", core.Azure: "Azure"}

// searchAliases are the plain words people type for a service ("queue" finds SQS), by service name.
var searchAliases = map[string]string{
	"sqs": "queue", "sns": "topic notification", "s3": "bucket", "dynamodb": "table", "pubsub": "topic queue", "lambda": "function",
}

func (ui *UI) search(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	match := func(s ...string) bool { return q == "" || strings.Contains(strings.ToLower(strings.Join(s, " ")), q) }
	var out []hit
	add := func(group string, max int, hs []hit) {
		for i, h := range hs {
			if i == max {
				break
			}
			h.Group = group
			out = append(out, h)
		}
	}
	cloud := ui.cloud(r)
	var svcs, res, acts []hit
	var svcsUse, actsUse []hit // the same for services in use, which rank first
	for _, s := range ui.hub.Status().Services {
		if cloud != "" && s.Cloud != cloud {
			continue
		}
		inUse := s.Resources > 0 && s.Requests > 0
		if q != "" {
			for _, f := range ui.allFormOps()[s.Name] {
				if match(f.Title, s.Title) {
					h := hit{Label: f.Title, Hint: s.Title + " · " + cloudShort[s.Cloud], Href: "/_pogi/new/" + s.Name + "/" + f.Op, Drawer: true, Icon: "plus"}
					if inUse {
						actsUse = append(actsUse, h)
					} else {
						acts = append(acts, h)
					}
				}
			}
		}
		if q == "" && !inUse || !match(s.Title, s.Name, cloudNames[s.Cloud], searchAliases[s.Name]) {
			continue
		}
		h := hit{Label: s.Title, Hint: cloudNames[s.Cloud], Href: "/_pogi/resources/" + s.Name, Cloud: s.Cloud}
		if inUse {
			svcsUse = append(svcsUse, h)
		} else {
			svcs = append(svcs, h)
		}
	}
	svcs, acts = append(svcsUse, svcs...), append(actsUse, acts...)
	// "create queue" ranks create actions whose title starts with the query first
	slices.SortStableFunc(acts, func(a, b hit) int {
		return cmpBool(strings.HasPrefix(strings.ToLower(b.Label), q), strings.HasPrefix(strings.ToLower(a.Label), q))
	})
	if q != "" {
		for _, s := range ui.hub.Services() {
			b, ok := s.(core.Browsable)
			if !ok || len(res) >= 8 {
				continue
			}
			info := s.Info()
			if cloud != "" && info.Cloud != cloud {
				continue
			}
			for _, t := range b.Resources() {
				for _, row := range t.Rows {
					if len(row) > 0 && strings.Contains(strings.ToLower(row[0]), q) && len(res) < 8 {
						res = append(res, hit{Label: row[0], Hint: info.Title + " · " + strings.TrimSuffix(t.Name, "s"), Href: link(info.Name, t.Name, row), Drawer: true, Icon: "box"})
					}
				}
			}
		}
	}
	for _, a := range actions {
		if q != "" && match(a.Label) {
			acts = append(acts, a)
		}
	}
	var pg []hit
	for _, p := range pages {
		if q != "" && match(p.Label) {
			pg = append(pg, p)
		}
	}
	add("Pages", 6, pg)
	add("Services", 8, svcs)
	add("Resources", 8, res)
	if q == "" {
		acts = actions[:4]
	}
	add("Actions", 8, acts)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	searchTmpl.Execute(w, out)
}

// allFormOps are every service's form ops, which do not change while pogi runs.
func (ui *UI) allFormOps() map[string][]formOp {
	ui.formOpsOnce.Do(func() {
		ui.formOpsAll = map[string][]formOp{}
		for _, s := range ui.hub.Services() {
			ui.formOpsAll[s.Info().Name] = ui.formOps(s.Info().Name)
		}
	})
	return ui.formOpsAll
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	}
	return -1
}

var searchTmpl = template.Must(template.New("search").Funcs(funcs).Parse(`{{$g := ""}}{{range .}}{{if ne .Group $g}}{{$g = .Group}}<div class="grp">{{.Group}}</div>{{end}}<a class="it" {{with .Href}}href="{{.}}"{{end}} {{if .Drawer}}data-drawer{{end}} {{if .Post}}data-post{{end}} {{with .JS}}data-js="{{.}}"{{end}}><span class="ico">{{if .Cloud}}<span class="dot {{.Cloud}}"></span>{{else}}{{icon .Icon}}{{end}}</span>{{.Label}}<small>{{.Hint}}</small></a>
{{else}}<div class="empty" style="padding:24px">No matches</div>{{end}}`))
