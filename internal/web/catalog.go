package web

// The services catalog (/_pogi/services) and the service page (/_pogi/resources/{service}):
// resource tables as tabs, explorers as tabs, activity and about.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// ---- catalog ----

type tile struct {
	Name, Title, Cloud, Depth, Q string
	Res                          int
	Used, Pinned                 bool
}

type catGroup struct {
	Name  string
	Tiles []tile
}

type catData struct {
	Groups  []catGroup
	Cloud   string
	UseOnly bool
	Count   string // the services count with its depth split
}

// Href is the catalog's URL with the in-use filter changed.
func (c catData) Href(use bool) string {
	if use {
		return "/_pogi/services?use=1"
	}
	return "/_pogi/services"
}

func (ui *UI) catalog(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	d := catData{Cloud: ui.cloud(r), UseOnly: q.Get("use") != ""}
	_, d.Count = serviceCount(d.Cloud, ui.hub.Status().Services)
	cov, pins := coverage(), pinned(r)
	by := map[string][]tile{}
	for _, s := range ui.hub.Status().Services {
		if d.Cloud != "" && s.Cloud != d.Cloud {
			continue
		}
		used := ui.inUse(s)
		if d.UseOnly && !used {
			continue
		}
		c := cov.Services[s.Name]
		cat := cmp.Or(c.Category, "Other")
		by[cat] = append(by[cat], tile{Name: s.Name, Title: s.Title, Cloud: s.Cloud, Depth: c.Depth, Used: used, Pinned: pins[s.Name],
			Res: s.Resources, Q: strings.ToLower(s.Title + " " + s.Name)})
	}
	order := slices.Clone(cov.Categories)
	for c := range by {
		if !slices.Contains(order, c) {
			order = append(order, c)
		}
	}
	for _, c := range order {
		if t := by[c]; len(t) > 0 {
			slices.SortFunc(t, func(a, b tile) int { return strings.Compare(strings.ToLower(a.Title), strings.ToLower(b.Title)) })
			d.Groups = append(d.Groups, catGroup{c, t})
		}
	}
	ui.render(w, r, "catalog", "Services", "services", d)
}

// ---- service page ----

type svcTab struct {
	Key, Label, Href string
	Count            int
	Show             bool // show the count
}

type menuItem struct {
	Title, Href string
	Sep         bool
}

// createBtn is the page's primary action: one form opens straight away, several make a menu.
type createBtn struct {
	Label, Href string
	Menu        []menuItem
}

type scopeBar struct {
	Text, AllLabel, Vals string // Vals is the JSON body of "show all", empty when everything is shown
}

type svcPage struct {
	Name, Title, Cloud, CloudName, Depth, Sub, Tab, Back string
	Rev                                                  string // see svcRev
	Pinned                                               bool
	Tabs                                                 []svcTab
	Table                                                *tbl
	Num                                                  []bool
	Scope                                                scopeBar
	Create                                               *createBtn
	Activity                                             []core.Entry
	Frag                                                 string // an explorer tab loads this
	Covered, NotYet                                      string
	CLI, Endpoint                                        string
}

func (t tile) DC() string     { return strings.ToLower(t.Depth) }
func (p *svcPage) DC() string { return strings.ToLower(p.Depth) }

// IsNum says whether column i holds numbers (right-aligned).
func (p *svcPage) IsNum(i int) bool { return i < len(p.Num) && p.Num[i] }

// Noun is what the current table lists, in lower case.
func (p *svcPage) Noun() string { return strings.ToLower(p.Table.Name) }

// ResetVals is the body of the Reset post.
func (p *svcPage) ResetVals() string {
	b, _ := json.Marshal(map[string]string{"service": p.Name, "back": "/_pogi/resources/" + p.Name})
	return string(b)
}

// RowVals is the body of the post that deletes one row.
func (p *svcPage) RowVals(enc string) string {
	b, _ := json.Marshal(map[string]string{"action": "delete", "row": enc, "back": p.Back})
	return string(b)
}

// JSONURL is the generic JSON view of a row.
func (p *svcPage) JSONURL(row []string) string {
	return "/_pogi/resources/" + url.PathEscape(p.Name) + "/" + url.PathEscape(p.Table.Name) + "/" + url.PathEscape(row[0])
}

// explorerTabs are the explorers that live on a service's page.
var explorerTabs = map[string]struct{ Key, Label, URL string }{
	"dynamodb":      {"query", "Query", "/_pogi/query/dynamodb"},
	"cosmos":        {"query", "Query", "/_pogi/query/cosmos"},
	"firestore":     {"query", "Query", "/_pogi/query/firestore"},
	"kms":           {"crypto", "Encrypt & decrypt", "/_pogi/crypto"},
	"cloudkms":      {"crypto", "Encrypt & decrypt", "/_pogi/crypto"},
	"keyvault":      {"crypto", "Encrypt & decrypt", "/_pogi/crypto"},
	"ec2":           {"network", "Network", "/_pogi/network"},
	"stepfunctions": {"exec", "Executions", "/_pogi/sfn"},
}

// upFirst capitalises the first letter and ends the text with a full stop.
func upFirst(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToUpper(s[:1]) + s[1:]
	if !strings.HasSuffix(s, ".") {
		s += "."
	}
	return s
}

// numCols marks the columns whose cells are all whole numbers.
func numCols(t core.Table, rows [][]string) []bool {
	out := make([]bool, len(t.Columns))
	for i := range out {
		n := 0
		out[i] = len(rows) > 0
		for _, r := range rows {
			if i >= len(r) || r[i] == "" {
				continue
			}
			if _, err := strconv.Atoi(r[i]); err != nil || len(r[i]) > 9 || len(r[i]) > 1 && r[i][0] == '0' { // not an account ID
				out[i] = false
				break
			}
			n++
		}
		out[i] = out[i] && n > 0 && i > 0
	}
	return out
}

// createFor is the Create button of a service: its Create* forms, else all its forms.
func (ui *UI) createFor(name string) *createBtn {
	if name == "arm" {
		return &createBtn{Label: "Create resource", Href: "/_pogi/arm/new"}
	}
	var items, rest []menuItem
	for _, f := range ui.formOps(name) {
		m := menuItem{Title: f.Title, Href: "/_pogi/new/" + name + "/" + f.Op}
		op := f.Op[strings.LastIndex(f.Op, ".")+1:]
		if strings.HasPrefix(op, "Create") {
			items = append(items, m)
		} else {
			rest = append(rest, m)
		}
	}
	if len(items) == 0 {
		items, rest = rest, nil
	}
	switch {
	case len(items) == 0:
		return nil
	case len(items) == 1 && len(rest) == 0:
		return &createBtn{Label: items[0].Title, Href: items[0].Href}
	}
	if len(rest) > 0 {
		items = append(items, menuItem{Sep: true})
		items = append(items, rest...)
	}
	return &createBtn{Label: "Create…", Menu: items}
}

// scopeFor says what the table is filtered by, for the scope bar.
func scopeFor(cloud string, sc scope, back string) scopeBar {
	val, key, all1, plural := sc.Account+" / "+sc.Region, "region", sc.Region, "regions"
	switch cloud {
	case core.GCP:
		val, key, all1, plural = sc.Project, "project", sc.Project, "projects"
	case core.Azure:
		val, key, all1, plural = sc.Subscription, "subscription", sc.Subscription, "subscriptions"
	}
	b := scopeBar{Text: strings.ReplaceAll(val, all, "all "+plural)}
	if all1 != all {
		v, _ := json.Marshal(map[string]string{key: all, "back": back})
		b.AllLabel, b.Vals = "Show all "+plural, string(v)
	}
	return b
}

func (ui *UI) servicePage(w http.ResponseWriter, r *http.Request, name string) {
	svc := ui.hub.Service(name)
	if svc == nil {
		http.Redirect(w, r, "/_pogi/services", http.StatusSeeOther)
		return
	}
	info, sc, cov := svc.Info(), ui.scope(r), coverage().Services[name]
	p := &svcPage{Name: name, Title: info.Title, Cloud: info.Cloud, CloudName: cloudNames[info.Cloud], Depth: cov.Depth, Back: r.URL.RequestURI(),
		Pinned: pinned(r)[name], Covered: upFirst(cov.Covered), NotYet: upFirst(cov.NotYet), Create: ui.createFor(name), Endpoint: "http://" + r.Host}
	if info.Cloud == core.AWS {
		p.CLI = "pogi aws " + name
	}
	rowScope := sc.forCloud(info.Cloud)
	var tables []tbl
	if b, ok := svc.(core.Browsable); ok {
		for _, t := range b.Resources() {
			x := tbl{Table: core.Table{Name: t.Name, Columns: t.Columns}}
			for _, row := range t.Rows {
				if rowScope.allows(t.Columns, row) {
					x.Rows, x.Enc = append(x.Rows, row), append(x.Enc, encRow(row))
				}
			}
			x.Del, x.Tag = ui.bulkable(name, t.Name)
			tables = append(tables, x)
		}
	}

	var res, reqs int
	for _, s := range ui.hub.Status().Services {
		if s.Name == name {
			res, reqs = s.Resources, s.Requests
		}
	}
	p.Rev = rev(res, reqs)
	if res == 0 && reqs == 0 {
		p.Sub = "Nothing here yet"
	} else {
		p.Sub = plural(res, "resource") + " · " + plural(reqs, "request")
		if last := ui.hub.Requests.Query(0, 1, name, "", false); len(last) > 0 {
			p.Sub += " · last call " + ago(last[0].Time)
		}
	}

	href := func(key string) string {
		return "/_pogi/resources/" + url.PathEscape(name) + "?tab=" + url.QueryEscape(key)
	}
	for _, t := range tables {
		p.Tabs = append(p.Tabs, svcTab{Key: t.Name, Label: t.Name, Href: href(t.Name), Count: len(t.Rows), Show: true})
	}
	if x, ok := explorerTabs[name]; ok {
		p.Tabs = append(p.Tabs, svcTab{Key: x.Key, Label: x.Label, Href: href(x.Key)})
	}
	p.Tabs = append(p.Tabs, svcTab{Key: "activity", Label: "Activity", Href: href("activity")}, svcTab{Key: "about", Label: "About", Href: href("about")})
	p.Tab = r.URL.Query().Get("tab")
	if !slices.ContainsFunc(p.Tabs, func(t svcTab) bool { return t.Key == p.Tab }) {
		p.Tab = p.Tabs[0].Key
		if len(tables) == 0 && explorerTabs[name].Key == "" {
			p.Tab = "activity"
		}
	}
	switch p.Tab {
	case "activity":
		p.Activity = ui.hub.Requests.Query(0, 200, name, "", false)
		slices.Reverse(p.Activity)
	case "about":
	default:
		if i := slices.IndexFunc(tables, func(t tbl) bool { return t.Name == p.Tab }); i >= 0 {
			p.Table, p.Num = &tables[i], numCols(tables[i].Table, tables[i].Rows)
			p.Scope = scopeFor(info.Cloud, sc, p.Back)
		} else {
			p.Frag = explorerTabs[name].URL
		}
	}
	ui.render(w, r, "resources", info.Title, name, p)
}

// mentions are the requests to a service that name the resource, newest first: by id in the path,
// or in the body for the JSON-protocol services (their paths are all "/").
func (ui *UI) mentions(svc, id string) []core.Entry {
	short := id[strings.LastIndexAny(id, "/:")+1:]
	if len(short) < 3 {
		short = id
	}
	var out []core.Entry
	for _, e := range ui.hub.Requests.Query(0, 300, svc, "", false) {
		if full, ok := ui.hub.Requests.Get(e.ID); ok && (strings.Contains(full.Path, short) || strings.Contains(full.ReqBody, short)) {
			out = append(out, e)
		}
	}
	slices.Reverse(out)
	return out
}

// ---- bulk tag drawer ----

// bulkTag answers the bulk bar's Tag button: a drawer that asks for the tags of the ticked rows.
func (ui *UI) bulkTag(w http.ResponseWriter, r *http.Request) {
	svc, table := r.PathValue("service"), r.PathValue("table")
	rows := r.URL.Query()["row"]
	title := "Add tags"
	if len(rows) == 0 {
		back(w, r, "/_pogi/resources/"+url.PathEscape(svc), fmt.Errorf("tick the rows to tag first"))
		return
	}
	ui.renderX(w, r, "tag", title, svc, plural(len(rows), "row")+" selected", map[string]any{
		"Service": svc, "Table": table, "Rows": rows, "Back": r.URL.Query().Get("back")})
}

// rev changes whenever a service gains or loses resources or answers a call. A service page polls it
// (GET /_pogi/rev?svc=) so resources made elsewhere, by the CLI, an SDK or Terraform, show up without a Refresh.
func rev(resources, requests int) string {
	return strconv.Itoa(resources) + "-" + strconv.Itoa(requests)
}

// svcRev is GET /_pogi/rev?svc=name: {"rev": "..."}, or "" for a service pogi does not run.
func (ui *UI) svcRev(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("svc")
	if name == "" {
		http.Error(w, "svc is required", http.StatusBadRequest)
		return
	}
	out := ""
	for _, s := range ui.hub.Status().Services {
		if s.Name == name {
			out = rev(s.Resources, s.Requests)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]string{"rev": out})
}
