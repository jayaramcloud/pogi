// Package web is pogi's console: server-rendered HTML with htmx, mounted at /_pogi/.
package web

import (
	"bytes"
	"cmp"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/terraform"
)

//go:embed templates/*.html static/*
var files embed.FS

// UI serves the console.
type UI struct {
	hub   *core.Hub
	pages map[string]*template.Template
	tf    tfHeld // the Terraform stack the console holds

	base        map[string]int // resources each service held at start, by service name
	formOpsOnce sync.Once
	formOpsAll  map[string][]formOp // allFormOps
}

var funcs = template.FuncMap{
	"cloudName": func(c string) string { return cloudNames[c] },
	"ago":       ago,
	"size": func(n any) string {
		switch v := n.(type) {
		case int:
			return s3.HumanSize(int64(v))
		case int64:
			return s3.HumanSize(v)
		}
		return fmt.Sprint(n)
	},
	"mul100":    func(f float64) float64 { return f * 100 },
	"link":      link,
	"status":    statusClass,
	"pretty":    pretty,
	"hasPrefix": strings.HasPrefix,
	"hostport":  core.Hostport,
	"json": func(v any) string {
		b, _ := json.MarshalIndent(v, "", "  ")
		return string(b)
	},
	"time": func(t time.Time) string { return t.Local().Format("15:04:05.000") },
	"date": func(t time.Time) string { return t.Local().Format(time.DateTime) },
	"q":    func(s string) string { return s }, // html/template already escapes query values in URLs; escaping here too doubled them
	"path": url.PathEscape,
	"crumbs": func(prefix string) [][2]string {
		var out [][2]string
		acc := ""
		for _, p := range strings.Split(strings.TrimSuffix(prefix, "/"), "/") {
			if p == "" {
				continue
			}
			acc += p + "/"
			out = append(out, [2]string{p, acc})
		}
		return out
	},
	"base": func(key, prefix string) string { return strings.TrimPrefix(key, prefix) },
	"add":  func(a, b int) int { return a + b },
	"ms": func(ms float64) string { // latency: one decimal under 10 ms
		if ms < 10 {
			return strconv.FormatFloat(ms, 'f', 1, 64) + "ms"
		}
		return strconv.FormatFloat(ms, 'f', 0, 64) + "ms"
	},
	"title": func(svc string) string { // a service's display title
		if t, ok := titles.Load(svc); ok {
			return t.(string)
		}
		return svc
	},
	"icon": func(name string) template.HTML {
		return template.HTML(`<svg class="i"><use href="#i-` + template.HTMLEscapeString(name) + `"/></svg>`)
	},
	"navlink":    navlink,
	"svcIcon":    svcIconHTML,
	"depthTag":   depthTag,
	"trimScheme": func(u string) string { _, rest, _ := strings.Cut(u, "://"); return rest },
}

// titles are the registry titles by service name, for the "title" template func.
var titles sync.Map

// shell parses the layout every page is cloned from.
func shell() *template.Template {
	return template.Must(template.New("layout.html").Funcs(funcs).ParseFS(files, "templates/layout.html", "templates/icons.html"))
}

// New builds the console handler.
func New(hub *core.Hub) http.Handler {
	ui := &UI{hub: hub, pages: map[string]*template.Template{}, base: map[string]int{}}
	for _, s := range hub.Status().Services {
		ui.base[s.Name] = s.Resources // what a service holds from the start
	}
	for _, s := range hub.Services() {
		titles.Store(s.Info().Name, s.Info().Title)
	}
	layout := shell()
	for _, p := range []string{"overview", "requests", "request", "resources", "inspect", "objects", "sqs", "lambda", "inbox", "chaos", "chaos-new", "snapshots", "connect", "form", "arm", "armgroup", "flows", "map"} {
		ui.pages[p] = template.Must(template.Must(layout.Clone()).ParseFS(files, "templates/"+p+".html"))
	}
	m := http.NewServeMux()
	m.Handle("GET /_pogi/static/", http.StripPrefix("/_pogi/", http.FileServerFS(files)))
	m.Handle("GET /_pogi/icons/", iconHandler())
	m.HandleFunc("GET /_pogi/{$}", ui.overview)
	m.HandleFunc("GET /_pogi/search", ui.search)
	m.HandleFunc("GET /_pogi/requests", ui.requests)
	m.HandleFunc("GET /_pogi/requests/{id}", ui.request)
	m.HandleFunc("GET /_pogi/flows", ui.flows)
	m.HandleFunc("GET /_pogi/flows/{trace}", ui.flow)
	m.HandleFunc("GET /_pogi/map", ui.mapPage)
	m.HandleFunc("GET /_pogi/map.svg", ui.mapSVG)
	m.HandleFunc("GET /_pogi/map/node", ui.mapNode)
	m.HandleFunc("GET /_pogi/tasks", ui.tasks)
	m.HandleFunc("GET /_pogi/rev", ui.svcRev)
	m.HandleFunc("GET /_pogi/resources", ui.resources)
	m.HandleFunc("GET /_pogi/resources/{service}", ui.resources)
	m.HandleFunc("GET /_pogi/resources/{service}/{table}/{id...}", ui.inspect)
	m.HandleFunc("GET /_pogi/kubeconfig/{service}/{id}", ui.kubeconfigFile)
	ui.serviceRoutes(m)
	ui.exploreRoutes(m)
	ui.noteRoutes(m)
	ui.iamRoutes(m)
	ui.costRoutes(m)
	m.HandleFunc("GET /_pogi/new/{service}/{op}", ui.newForm)
	m.HandleFunc("POST /_pogi/new/{service}/{op}", ui.newSubmit)
	m.HandleFunc("GET /_pogi/new/{service}/{op}/item", ui.formItem)
	m.HandleFunc("GET /_pogi/blank", blank)
	m.HandleFunc("GET /_pogi/arm/new", ui.armPage)
	m.HandleFunc("POST /_pogi/arm/new", ui.armSubmit)
	m.HandleFunc("GET /_pogi/arm/group", ui.groupPage)
	m.HandleFunc("POST /_pogi/arm/group", ui.groupSubmit)
	m.HandleFunc("POST /_pogi/bulk/{service}/{table}", ui.bulk)
	m.HandleFunc("POST /_pogi/scope", ui.scopeSet)
	m.HandleFunc("GET /_pogi/scope/values", ui.scopeValues)
	m.HandleFunc("POST /_pogi/cloud", ui.cloudSet)
	m.HandleFunc("GET /_pogi/chaos", ui.chaos)
	m.HandleFunc("POST /_pogi/chaos", ui.chaosAdd)
	m.HandleFunc("POST /_pogi/chaos/{id}/delete", ui.chaosDelete)
	m.HandleFunc("GET /_pogi/chaos/new", ui.chaosNew)
	m.HandleFunc("GET /_pogi/snapshots", ui.snapshots)
	m.HandleFunc("POST /_pogi/snapshots", ui.snapshotSave)
	m.HandleFunc("POST /_pogi/snapshots/{name}/load", ui.snapshotLoad)
	m.HandleFunc("POST /_pogi/snapshots/{name}/delete", ui.snapshotDelete)
	m.HandleFunc("POST /_pogi/reset", ui.reset)
	m.HandleFunc("GET /_pogi/connect", ui.connect)
	safe := http.NewCrossOriginProtection().Handler(m)
	// Console headers: no sniffing, no framing or opening by other sites, no referrer to other sites, and a
	// CSP that allows scripts only from pogi's own files (the templates have no inline script or handler).
	// style-src keeps 'unsafe-inline': the templates and the map and graph SVGs use style="" attributes.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hd := w.Header()
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("Content-Security-Policy", consoleCSP)
		hd.Set("X-Frame-Options", "SAMEORIGIN")
		hd.Set("Referrer-Policy", "same-origin")
		hd.Set("Cross-Origin-Opener-Policy", "same-origin")
		hd.Set("Cross-Origin-Resource-Policy", "same-origin")
		hd.Set("Cross-Origin-Embedder-Policy", "require-corp")
		hd.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), bluetooth=(), midi=(), display-capture=(), accelerometer=(), gyroscope=(), magnetometer=()")
		if r.Body != nil && r.Body != http.NoBody {
			raw := r.Body
			r = r.WithContext(context.WithValue(r.Context(), rawBodyKey{}, raw))
			r.Body = http.MaxBytesReader(w, raw, ui.bodyLimit(r))
		}
		safe.ServeHTTP(w, r)
	})
}

// rawBodyKey holds the request body before the limit was put on it: a refused upload is drained from it,
// so the message can show the real size.
type rawBodyKey struct{}

// bodyLimit is the most any console request may send: the upload limit, or the code limit for a new Lambda
// function (its zip), or the Terraform limit, each plus room for the form's own framing. It is set once, here,
// so no console form can fill the disk or memory.
func (ui *UI) bodyLimit(r *http.Request) int64 {
	switch {
	case strings.HasPrefix(r.URL.Path, "/_pogi/new/lambda/"):
		return ui.hub.CodeLimit(false) + 1<<20
	case r.URL.Path == "/_pogi/cost/terraform":
		return terraform.MaxBytes + tfFormMax
	}
	return ui.hub.MaxUpload() + 1<<20
}

const consoleCSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; " +
	"object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'self'"

// Favicon answers browsers' automatic /favicon.ico so it never reaches S3.
var Favicon = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "max-age=86400")
	io.WriteString(w, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="8" fill="#ff8cc6"/><text x="16" y="23" font-size="20" text-anchor="middle" fill="#3d0a26" font-family="sans-serif" font-weight="700">p</text></svg>`)
})

// page is what every template receives.
type page struct {
	Title, Active, Error, Flash, Version, Endpoint string
	Eyebrow                                        string // the small line above a drawer's title
	Scope                                          scope
	Pill                                           string // the header pill's text
	Back                                           string // where the switcher returns to
	Cloud, Foreign                                 string // the cloud selector ("" is all), and the cloud of a service page that is not it
	Note                                           bool   // show the "showing one cloud only" notice
	Data                                           any
	Nav                                            nav
	Problems                                       []core.Problem // pogi-wide: saved state that can't be read or written, ports that could not be opened
	Theme, Side, Foot                              string         // the theme and sidebar cookies, the sidebar footer
	UploadMB, CodeMB                               int64          // the upload limit and the function code limit, for the upload forms
}

// render writes a page. A request htmx sends into the drawer (hx-target="#drawer") gets the page's
// "drawer" template when it defines one, else its content in the standard drawer shell; any other
// request gets the full page.
func (ui *UI) render(w http.ResponseWriter, r *http.Request, name, title, active string, data any) {
	ui.renderX(w, r, name, title, active, "", data)
}

// renderX is render with a drawer eyebrow.
func (ui *UI) renderX(w http.ResponseWriter, r *http.Request, name, title, active, eyebrow string, data any) {
	st := ui.hub.Status()
	p := page{Title: title, Active: active, Eyebrow: eyebrow, Error: r.URL.Query().Get("err"), Flash: r.URL.Query().Get("ok"), Version: st.Version, Endpoint: "http://" + r.Host,
		Scope: ui.scope(r), Back: r.URL.RequestURI(), Data: data, Cloud: ui.cloud(r), UploadMB: ui.hub.MaxUpload() >> 20, CodeMB: ui.hub.CodeLimit(false) >> 20}
	p.Pill = ui.pill(p.Scope, p.Cloud)
	switch name {
	case "overview", "requests", "flows", "catalog", "map", "cost":
		p.Note = p.Cloud != ""
	case "resources":
		if svc := ui.hub.Service(active); svc != nil && p.Cloud != "" && svc.Info().Cloud != p.Cloud {
			p.Note, p.Foreign = true, cloudNames[svc.Info().Cloud]
		}
	}
	t, root := ui.pages[name], "layout.html"
	if inDrawer(r) {
		if root = "drawer-shell"; t.Lookup("drawer") != nil {
			root = "drawer"
		}
	} else {
		p.Nav, p.Foot, p.Problems = ui.nav(r, st), foot(st), st.Problems
		p.Theme, p.Side = cookie(r, "pogi-theme", "light", "dark"), cookie(r, "pogi-side", "mini", "wide")
	}
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, root, p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf.WriteTo(w)
}

// back redirects after a form post, carrying an error if any.
func back(w http.ResponseWriter, r *http.Request, to string, err error) { done(w, r, to, "", err) }

// done ends a form post. A post from the drawer or another htmx element (not a boosted form)
// gets a toast and a swap of the page at to, or an error toast and no swap; others a redirect.
func done(w http.ResponseWriter, r *http.Request, to, msg string, err error) {
	if r.Header.Get("HX-Request") != "" && r.Header.Get("HX-Boosted") == "" {
		if err != nil {
			w.Header().Set("HX-Reswap", "none")
			trigger(w, "pogi-toast", map[string]string{"msg": err.Error(), "kind": "bad"})
			return
		}
		trigger(w, "pogi-done", map[string]string{"msg": cmp.Or(msg, "Done")})
		loc, _ := json.Marshal(map[string]string{"path": to, "target": "body"})
		w.Header().Set("HX-Location", string(loc))
		return
	}
	if err != nil {
		to = withNote(to, "err", err.Error())
	} else if msg != "" {
		to = withNote(to, "ok", msg) // a redirect has no toast, so the next page shows a success flash
	}
	http.Redirect(w, r, to, http.StatusSeeOther)
}

// withNote adds ?err= or ?ok= to a path, replacing a note an earlier post left there.
func withNote(to, key, msg string) string {
	u, err := url.Parse(to)
	if err != nil {
		return to
	}
	q := u.Query()
	q.Del("err")
	q.Del("ok")
	q.Set(key, msg)
	u.RawQuery = q.Encode()
	return u.String()
}

// resources is a service's page; without a service it is the catalog's old address.
func (ui *UI) resources(w http.ResponseWriter, r *http.Request) {
	if svc := r.PathValue("service"); svc != "" {
		ui.servicePage(w, r, svc)
		return
	}
	http.Redirect(w, r, "/_pogi/services", http.StatusSeeOther)
}

// inspect is one resource in the drawer: its JSON and the requests that mention it.
func (ui *UI) inspect(w http.ResponseWriter, r *http.Request) {
	svc, table, id := r.PathValue("service"), r.PathValue("table"), r.PathValue("id")
	var v any
	if s, ok := ui.hub.Service(svc).(core.Inspectable); ok {
		v = s.Inspect(table, id)
	}
	if v == nil {
		back(w, r, "/_pogi/resources/"+svc, fmt.Errorf("%s %q not found", table, id))
		return
	}
	self := "/_pogi/resources/" + url.PathEscape(svc) + "/" + url.PathEscape(table) + "/" + url.PathEscape(id)
	d := map[string]any{"Service": svc, "Table": table, "ID": id, "Value": v, "Self": self, "Tab": "json",
		"Back": "/_pogi/resources/" + url.PathEscape(svc) + "?tab=" + url.QueryEscape(table)}
	if r.URL.Query().Get("tab") == "activity" {
		d["Tab"], d["Activity"] = "activity", ui.mentions(svc, id)
	} else if tags, ok := ui.resourceTags(svc, table, id); ok {
		d["Tags"], d["HasTags"] = tags, true
	}
	if d["Tab"] == "json" {
		if c := ui.kubeConnectFor(r, svc, table, id); c != nil {
			d["Connect"] = c
		}
	}
	if del, _ := ui.bulkable(svc, table); del {
		if b, ok := ui.hub.Service(svc).(core.Browsable); ok {
			for _, t := range b.Resources() {
				for _, row := range t.Rows {
					if t.Name == table && len(row) > 0 && row[0] == id {
						d["Enc"] = encRow(row)
						vals, _ := json.Marshal(map[string]string{"action": "delete", "row": encRow(row), "back": d["Back"].(string)})
						d["DelVals"] = string(vals)
					}
				}
			}
		}
	}
	if svc == "arm" && table == "Resource groups" && d["Tab"] == "json" {
		d["Members"] = ui.groupMembers(ui.scope(r).sub(), id)
	}
	title := svc
	if s := ui.hub.Service(svc); s != nil {
		title = s.Info().Title
	}
	ui.renderX(w, r, "inspect", id, svc, title+" · "+strings.TrimSuffix(table, "s"), d)
}

// ---- chaos, snapshots, reset ----

func (ui *UI) chaos(w http.ResponseWriter, r *http.Request) {
	ui.render(w, r, "chaos", "Chaos", "chaos", map[string]any{"Rules": ui.hub.Chaos.List()})
}

// chaosForm is the add-rule drawer's starting values.
type chaosForm struct {
	Service, Op, Status, Code string
	Pct, Latency              int
}

// chaosNew is the add-rule drawer. ?preset= fills it in; the CLI band shows the same rule as a command.
func (ui *UI) chaosNew(w http.ResponseWriter, r *http.Request) {
	preset := r.URL.Query().Get("preset")
	f := chaosForm{Status: "400", Code: "ThrottlingException", Pct: 20}
	switch preset {
	case "outage":
		f = chaosForm{Status: "503", Code: "ServiceUnavailable", Pct: 100}
	case "slow":
		f = chaosForm{Pct: 100, Latency: 800}
	case "custom":
		f = chaosForm{Pct: 100}
	default:
		preset = "throttle"
	}
	var names []string
	for _, s := range ui.hub.Services() {
		names = append(names, s.Info().Name)
	}
	cmd := "pogi chaos add"
	if f.Service != "" {
		cmd += " --service " + f.Service
	}
	cmd += fmt.Sprintf(" --rate %.2f", float64(f.Pct)/100)
	if f.Status != "" {
		cmd += " --status " + f.Status
	}
	if f.Code != "" {
		cmd += " --code " + f.Code
	}
	if f.Latency > 0 {
		cmd += fmt.Sprintf(" --latency %dms", f.Latency)
	}
	ui.renderX(w, r, "chaos-new", "Add rule", "chaos", "Chaos", map[string]any{"F": f, "Preset": preset, "Services": names, "Cmd": cmd})
}

func (ui *UI) chaosAdd(w http.ResponseWriter, r *http.Request) {
	rule := core.Rule{Service: r.FormValue("service"), Op: r.FormValue("op"), Code: r.FormValue("code"), Message: r.FormValue("message")}
	rule.Probability, _ = strconv.ParseFloat(r.FormValue("probability"), 64)
	if rule.Probability > 1 {
		rule.Probability /= 100 // accept percentages
	}
	if pct, err := strconv.ParseFloat(r.FormValue("pct"), 64); err == nil { // the drawer's slider
		rule.Probability = pct / 100
	}
	rule.LatencyMs, _ = strconv.Atoi(r.FormValue("latency"))
	rule.Status, _ = strconv.Atoi(r.FormValue("status"))
	switch r.FormValue("preset") {
	case "throttle":
		rule = core.Rule{Probability: 0.2, Status: 400, Code: "ThrottlingException", Message: "Rate exceeded (pogi chaos)"}
	case "outage":
		rule = core.Rule{Service: r.FormValue("service"), Status: 503, Code: "ServiceUnavailable", Message: "Service unavailable (pogi chaos)"}
	case "slow":
		rule = core.Rule{LatencyMs: 800}
	}
	if err := rule.Check(); err != nil {
		done(w, r, "/_pogi/chaos", "", err)
		return
	}
	ui.hub.Chaos.Add(rule)
	done(w, r, "/_pogi/chaos", "Chaos rule added", nil)
}

func (ui *UI) chaosDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "all" {
		id = ""
	}
	ui.hub.Chaos.Remove(id)
	back(w, r, "/_pogi/chaos", nil)
}

func (ui *UI) snapshots(w http.ResponseWriter, r *http.Request) {
	ui.render(w, r, "snapshots", "Snapshots", "snapshots", map[string]any{"Snapshots": ui.hub.Snapshots(), "DataDir": ui.hub.Cfg.DataDir, "Persist": ui.hub.Status().Persist})
}

func (ui *UI) snapshotSave(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		name = time.Now().Format("2006-01-02_15-04-05")
	}
	p, err := ui.hub.SnapshotPath(name)
	if err == nil {
		if f, fh, ferr := r.FormFile("file"); ferr == nil { // import
			defer f.Close()
			if name == "" || r.FormValue("name") == "" {
				name = strings.TrimSuffix(fh.Filename, ".json")
				p, err = ui.hub.SnapshotPath(name)
			}
			if err == nil {
				err = ui.hub.ImportSnapshot(p, f)
			}
		} else {
			err = ui.hub.SaveFile(p)
		}
	}
	back(w, r, "/_pogi/snapshots", err)
}

func (ui *UI) snapshotLoad(w http.ResponseWriter, r *http.Request) {
	p, err := ui.hub.SnapshotPath(r.PathValue("name"))
	if err == nil {
		err = ui.hub.LoadFile(p)
	}
	back(w, r, "/_pogi/snapshots", err)
}

func (ui *UI) snapshotDelete(w http.ResponseWriter, r *http.Request) {
	p, err := ui.hub.SnapshotPath(r.PathValue("name"))
	if err == nil {
		err = ui.hub.DeleteSnapshot(p)
	}
	back(w, r, "/_pogi/snapshots", err)
}

func (ui *UI) reset(w http.ResponseWriter, r *http.Request) {
	var names []string
	if s := r.FormValue("service"); s != "" {
		names = []string{s}
	}
	ui.hub.Reset(names...)
	to := r.FormValue("back")
	if !strings.HasPrefix(to, "/_pogi/") {
		to = "/_pogi/"
	}
	back(w, r, to, nil)
}

// connectTabs are the snippet tabs of the Connect page.
var connectTabs = [][2]string{{"shell", "Shell"}, {"cli", "AWS CLI"}, {"tf", "Terraform"}, {"sdk", "SDKs"}, {"gcp", "Google Cloud"}, {"azure", "Azure"}}

// dockerEngine is probed on the Connect page, like pogi doctor does.
var dockerEngine = docker.New(docker.Options{})

type healthRow struct{ Label, State, Kind, Note string }

// health is what pogi depends on, from the same checks as pogi doctor.
func (ui *UI) health() []healthRow {
	var out []healthRow
	if e, ok := dockerEngine.Engine(); ok {
		out = append(out, healthRow{"Docker", "running", "ok", e.Name + " " + e.Version + ": ECS, EKS, RDS, Cloud Run and VMs work"})
	} else {
		out = append(out, healthRow{"Docker", "not found", "warn", "RDS, ElastiCache, Cloud Run, VMs and --lambda docker need Docker, OrbStack, Colima or Podman"})
	}
	if _, err := os.Stat(filepath.Join(ui.hub.Cfg.DataDir, "certs", "ca.pem")); err == nil {
		out = append(out, healthRow{"CA certificate", "created", "ok", "run pogi cert once to trust it for Cosmos DB and Key Vault"})
	} else {
		out = append(out, healthRow{"CA certificate", "missing", "warn", "run pogi cert (it is created on first start)"})
	}
	return append(out, healthRow{"Lambda runtimes", cmp.Or(ui.hub.Cfg.Lambda, "native"), "ok", "python3 and nodejs run natively when installed; pogi doctor checks them"})
}

func (ui *UI) connect(w http.ResponseWriter, r *http.Request) {
	endpoint := "http://" + r.Host
	tab := r.URL.Query().Get("tab")
	if !slices.ContainsFunc(connectTabs, func(t [2]string) bool { return t[0] == tab }) {
		tab = "shell"
	}
	ui.render(w, r, "connect", "Connect", "connect", map[string]any{
		"Endpoint": endpoint, "Region": ui.hub.Cfg.Region, "Env": core.Env(endpoint, ui.hub.Cfg.Region), "Tab": tab, "Tabs": connectTabs, "Health": ui.health(),
	})
}

// ---- helpers ----

// link is where a resource row's first cell points: a rich page when the
// service has one, the generic JSON view otherwise.
func link(svc, table string, row []string) string {
	switch {
	case svc == "s3", svc == "gcs":
		return "/_pogi/objects/" + svc + "/" + url.PathEscape(row[0])
	case svc == "azblob" && len(row) > 1:
		return "/_pogi/objects/azblob/" + url.PathEscape(row[1]) + "/" + url.PathEscape(row[0])
	case svc == "sqs" && len(row) > 2:
		return "/_pogi/sqs/" + row[2] + "/" + row[1] + "/" + url.PathEscape(row[0])
	case svc == "lambda" && table == "Functions" && len(row) > 2:
		return "/_pogi/lambda/" + row[2] + "/" + row[1] + "/" + url.PathEscape(row[0])
	}
	return "/_pogi/resources/" + svc + "/" + url.PathEscape(table) + "/" + url.PathEscape(row[0])
}

func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return t.Local().Format(time.DateOnly)
}

func statusClass(code int) string {
	switch {
	case code >= 500:
		return "s5"
	case code >= 400:
		return "s4"
	case code >= 300:
		return "s3"
	}
	return "s2"
}

// pretty indents JSON bodies and leaves everything else alone.
func pretty(s string) string {
	t := strings.TrimSpace(s)
	if strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[") {
		var buf bytes.Buffer
		if json.Indent(&buf, []byte(t), "", "  ") == nil {
			return buf.String()
		}
	}
	if strings.HasPrefix(t, "<?xml") || strings.HasPrefix(t, "<") {
		return strings.ReplaceAll(t, "><", ">\n<")
	}
	return s
}
