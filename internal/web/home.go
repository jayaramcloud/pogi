package web

// Home, Activity and the request drawer: everything computed from the request log.

import (
	"cmp"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

func init() {
	funcs["lat"] = func(ms float64) int { return min(40, int(ms*3)) } // width in px of the latency bar
	funcs["stc"] = func(code int) string {                            // .st colour of a status code
		switch {
		case code >= 500:
			return "bad"
		case code >= 400:
			return "warn"
		}
		return "ok"
	}
}

type homeSvc struct {
	Name, Title, Cloud          string
	Resources, Requests, Errors int
	Last                        time.Time
}

type quickAct struct{ Icon, Title, Sub, Href string }

type homeTile struct {
	Name, Title, Cloud, Depth string
	Pinned                    bool
}

type homeData struct {
	FirstRun                     bool
	Endpoint, Region             string
	Env                          []string
	InUse, Resources             int
	Count                        int    // services, counted as products
	CountFull                    string // the count with its depth split
	Total, Errors, PerMin, Chaos int
	ErrPct                       string
	ReqSpark, ErrSpark           template.HTML
	Cost                         string // "$29" or "<$1" when the Cost page has an estimate
	OtherErrors                  int    // errors in the clouds a picked cloud hides, for the empty "Recent errors"
	Scope                        scope
	ScopeLine                    string // the scope of each cloud shown (or in use)
	Cloud                        string // the selected cloud, "" for all
	Rows                         []homeSvc
	Quick                        []quickAct
	Tiles                        []homeTile
	Feed, Recent                 []core.Entry // newest first
}

// spark draws per-minute counts as a line.
func spark(pts []int, color string) template.HTML {
	const w, h = 90.0, 26.0
	mx := max(1, slices.Max(pts))
	var d strings.Builder
	for i, p := range pts {
		c := "L"
		if i == 0 {
			c = "M"
		}
		fmt.Fprintf(&d, "%s%.1f,%.1f", c, float64(i)/float64(len(pts)-1)*w, h-2-float64(p)/float64(mx)*(h-4))
	}
	return template.HTML(fmt.Sprintf(`<svg class="spark" viewBox="0 0 90 26"><path d="%s" fill="none" stroke="%s" stroke-width="1.6"/></svg>`, d.String(), color))
}

func (ui *UI) overview(w http.ResponseWriter, r *http.Request) {
	st := ui.hub.Status()
	d := homeData{Chaos: len(ui.hub.Chaos.List()), Scope: ui.scope(r), Cloud: ui.cloud(r)}
	cloud, traffic := ui.cloud(r), 0
	for _, s := range st.Services {
		traffic += s.Requests
		if cloud != "" && s.Cloud != cloud {
			continue
		}
		d.Total += s.Requests
		if ui.inUse(s) { // built-in defaults (the default resource group, project, users) don't count
			d.Resources += s.Resources
		}
	}
	var used []string
	for _, c := range []string{core.AWS, core.GCP, core.Azure} {
		if cloud != "" && cloud != c {
			continue
		}
		in := cloud == c
		for _, s := range st.Services {
			in = in || (s.Cloud == c && ui.inUse(s))
		}
		if in {
			used = append(used, cloudNames[c]+" "+d.Scope.text(c))
		}
	}
	if len(used) == 0 {
		used = []string{"AWS " + d.Scope.text(core.AWS)}
	}
	d.ScopeLine = strings.Join(used, "; ")
	d.Count, d.CountFull = serviceCount(cloud, st.Services)
	d.FirstRun = traffic == 0
	if r.URL.Query().Get("wait") != "" { // the first-run page polls: reload once traffic shows up
		if d.FirstRun {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.Header().Set("HX-Refresh", "true")
		}
		return
	}
	d.Quick = ui.quickActs(d.Cloud)
	if d.FirstRun {
		d.Endpoint, d.Region = "http://"+r.Host, ui.hub.Cfg.Region
		d.Env = core.Env(d.Endpoint, d.Region)
		pins := pinned(r)
		for _, n := range []string{"s3", "sqs", "dynamodb", "lambda", "pubsub", "gcs", "run", "servicebus", "cosmos", "keyvault"} {
			if s := ui.hub.Service(n); s != nil {
				i := s.Info()
				d.Tiles = append(d.Tiles, homeTile{n, i.Title, i.Cloud, strings.ToLower(coverage().Services[n].Depth), pins[n]})
			}
		}
		ui.render(w, r, "overview", "Welcome to pogi", "overview", d)
		return
	}

	log := ui.hub.Requests.Query(0, 0, "", "", false) // oldest first
	log = slices.DeleteFunc(log, func(e core.Entry) bool {
		if cloud == "" || e.Cloud == cloud {
			return false
		}
		if e.Alarming() {
			d.OtherErrors++
		}
		return true
	})
	now := time.Now()
	var reqs, errs [12]int
	bySvc := map[string]*homeSvc{}
	for _, s := range st.Services {
		if ui.inUse(s) && (cloud == "" || s.Cloud == cloud) {
			bySvc[s.Name] = &homeSvc{Name: s.Name, Title: s.Title, Cloud: s.Cloud, Resources: s.Resources, Requests: s.Requests}
		}
	}
	for _, e := range log {
		bad := e.Alarming()
		if m := int(now.Sub(e.Time) / time.Minute); m >= 0 && m < 12 {
			reqs[11-m]++
			if bad {
				errs[11-m]++
			}
		}
		if bad {
			d.Errors++
		}
		if s := bySvc[e.Service]; s != nil {
			if bad {
				s.Errors++
			}
			if e.Time.After(s.Last) {
				s.Last = e.Time
			}
		}
	}
	d.PerMin, d.InUse = reqs[11], len(bySvc)
	d.ReqSpark, d.ErrSpark = spark(reqs[:], "var(--accent)"), spark(errs[:], "var(--bad)")
	if len(log) > 0 {
		d.ErrPct = strconv.FormatFloat(float64(d.Errors)*100/float64(len(log)), 'f', 1, 64)
	}
	if costTotal != nil {
		if v, ok := costTotal(ui, r); ok {
			d.Cost = perMonth(v)
		}
	}
	for _, s := range bySvc {
		d.Rows = append(d.Rows, *s)
	}
	sort.Slice(d.Rows, func(i, j int) bool {
		return cmp.Or(d.Rows[j].Requests-d.Rows[i].Requests, strings.Compare(d.Rows[i].Name, d.Rows[j].Name)) < 0
	})
	slices.Reverse(log)
	d.Feed = log[:min(9, len(log))]
	for _, e := range log {
		if e.Status >= 400 && len(d.Recent) < 7 {
			d.Recent = append(d.Recent, e)
		}
	}
	ui.render(w, r, "overview", "Home", "overview", d)
}

// quickStarts are the Home shortcuts per cloud; All shows AWS's.
var quickStarts = map[string][][4]string{
	core.AWS: {
		{"s3", "CreateBucket", "box", "Create an S3 bucket"},
		{"sqs", "CreateQueue", "send", "Create an SQS queue"},
		{"lambda", "CreateFunction", "code", "Create a Lambda function"},
	},
	core.GCP: {
		{"gcs", "CreateBucket", "box", "Create a Cloud Storage bucket"},
		{"pubsub", "Publisher.CreateTopic", "send", "Create a Pub/Sub topic"},
		{"cloudfunctions", "FunctionService.CreateFunction", "code", "Create a Cloud Function"},
	},
	core.Azure: {
		{"azblob", "CreateContainer", "box", "Create a blob container"},
	},
}

// quickActs are the selected cloud's Home shortcuts whose create form exists.
func (ui *UI) quickActs(cloud string) []quickAct {
	var out []quickAct
	for _, q := range quickStarts[cmp.Or(cloud, core.AWS)] {
		if s := ui.hub.Service(q[0]); s != nil && slices.ContainsFunc(ui.formOps(q[0]), func(f formOp) bool { return f.Op == q[1] }) {
			i := s.Info()
			out = append(out, quickAct{q[2], q[3], i.Title + " · " + cloudNames[i.Cloud], "/_pogi/new/" + q[0] + "/" + q[1]})
		}
	}
	return append(out, quickAct{"camera", "Load a snapshot", "Restore saved state", ""})
}

// pinned is the sidebar pins cookie as a set.
func pinned(r *http.Request) map[string]bool {
	pins := map[string]bool{}
	if c, err := r.Cookie("pogi-pins"); err == nil {
		v, _ := url.QueryUnescape(c.Value)
		for _, p := range strings.Split(v, ",") {
			pins[p] = true
		}
	}
	return pins
}

// ---- Activity ----

// reqQuery is the Activity filter: q and service as before, plus cloud and one of errors|chaos|iam.
type reqQuery struct{ Q, Service, Cloud, Show string }

func (r reqQuery) url() string {
	v := url.Values{"show": {r.Show}, "service": {r.Service}}
	for k, s := range v {
		if s[0] == "" {
			delete(v, k)
		}
	}
	return "/_pogi/requests?" + v.Encode()
}

func (ui *UI) requests(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	q := reqQuery{v.Get("q"), v.Get("service"), ui.cloud(r), v.Get("show")}
	if v.Get("errors") != "" {
		q.Show = "errors"
	}
	entries := ui.hub.Requests.Query(0, 0, q.Service, q.Q, q.Show == "errors")
	entries = slices.DeleteFunc(entries, func(e core.Entry) bool {
		return q.Cloud != "" && e.Cloud != q.Cloud || q.Show == "chaos" && !e.Chaos || q.Show == "iam" && !strings.HasPrefix(e.IAM, "deny")
	})
	slices.Reverse(entries)
	total := 0
	for _, n := range ui.hub.Requests.Totals() {
		total += n
	}
	data := map[string]any{"Entries": entries[:min(300, len(entries))], "Q": q, "Poll": q.url(), "Total": total}
	if r.Header.Get("HX-Request") != "" && r.Header.Get("HX-Target") == "reqs" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		ui.pages["requests"].ExecuteTemplate(w, "rows", data)
		return
	}
	ui.render(w, r, "requests", "Activity", "requests", data)
}

// ---- request drawer ----

type reqView struct {
	core.Entry
	Curl string
}

func (ui *UI) request(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	e, ok := ui.hub.Requests.Get(id)
	if !ok {
		back(w, r, "/_pogi/requests", fmt.Errorf("request %d is no longer in the log", id))
		return
	}
	ui.render(w, r, "request", fmt.Sprintf("%s %s", e.Service, e.Op), "requests", reqView{e, curl(e, "http://"+r.Host)})
}

// curl rebuilds a logged request as a one-line curl command.
func curl(e core.Entry, base string) string {
	q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
	b := []string{"curl", "-X", e.Method, q(base + e.Path)}
	keys := make([]string, 0, len(e.ReqHeaders))
	for k := range e.ReqHeaders {
		if !slices.Contains([]string{"Host", "Content-Length", "Accept-Encoding", "Connection"}, k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		b = append(b, "-H", q(k+": "+e.ReqHeaders[k]))
	}
	if e.ReqBody != "" {
		b = append(b, "--data-raw", q(e.ReqBody))
	}
	return strings.Join(b, " ")
}
