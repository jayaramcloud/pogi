package web

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// A flow is one trace of two or more steps: the requests that carry its id
// and the spans (deliveries, invocations, redrives) the hub logged for it.

func init() { noteFuncs = append(noteFuncs, flowNotes) }

type flowStep struct {
	Time          time.Time
	Ms            float64
	Cloud         string
	Service       string // title
	Op            string
	Indent        string
	ReqID         int64 // > 0 for a request: opens in the drawer
	Err           string
	Left, Width   string // percent of the flow's span of time
	Dur, Hint     string
	svc, from, to string // registry names and resource ids, for depth and the map
	kind          string // "" for a request, else a core.Span kind
	depth         int
}

type flowChip struct {
	Cloud, Name string
	Err         bool
}

type flow struct {
	Trace    string
	Start    time.Time
	Cloud    string
	Trigger  string
	Path     []flowChip
	Steps    []flowStep
	Ms       float64
	Duration string
	Axis     []string
	Failed   bool
	Why      string
}

// flowsPage is the data of the list, and of one flow when One is set.
type flowsPage struct {
	One      *flow
	Flows    []*flow
	Q, F     string // filter text and chip: "", "failed" or "slow"
	Live     bool
	NotShown int
}

// svcMeta is the cloud and title of the service a request or span names. Spans carry the
// name Hub.Deliver routes by, which for Azure is the lower-cased resource provider.
func (ui *UI) svcMeta(name, resource string) (cloud, title string) {
	svc := ui.hub.Service(name)
	if svc == nil {
		svc = ui.hub.Service(strings.TrimPrefix(name, "microsoft."))
	}
	if svc != nil {
		i := svc.Info()
		return i.Cloud, i.Title
	}
	switch {
	case strings.HasPrefix(resource, "arn:"):
		cloud = core.AWS
	case strings.HasPrefix(resource, "//"):
		cloud = core.GCP
	case strings.HasPrefix(resource, "/subscriptions/"):
		cloud = core.Azure
	}
	return cloud, name
}

// shortName is the last part of a resource id: a queue's name, a topic's name.
func shortName(id string) string {
	id = strings.TrimSuffix(id, "/")
	if strings.HasPrefix(id, "arn:") {
		parts := strings.SplitN(id, ":", 6)
		if len(parts) == 6 {
			id = parts[5]
			if svc := parts[2]; svc == "lambda" || svc == "logs" || svc == "states" { // function:name[:alias]
				if _, rest, ok := strings.Cut(id, ":"); ok {
					id = rest
				}
			}
		}
	}
	if i := strings.LastIndexAny(id, "/:"); i >= 0 && i < len(id)-1 {
		id = id[i+1:]
	}
	return id
}

func fmtMs(ms float64) string {
	switch {
	case ms >= 1000:
		return fmt.Sprintf("%.1f s", ms/1000)
	case ms >= 10:
		return fmt.Sprintf("%.0f ms", ms)
	}
	return fmt.Sprintf("%.1f ms", ms)
}

// buildFlows groups the request log and the span log by trace, newest flow first.
func (ui *UI) buildFlows() []*flow {
	by := map[string][]flowStep{}
	for _, e := range ui.hub.Requests.Query(0, 0, "", "", false) {
		if e.Trace == "" {
			continue
		}
		cloud, title := ui.svcMeta(e.Service, "")
		st := flowStep{Time: e.Time, Ms: e.Ms, Cloud: cloud, Service: title, Op: e.Op, ReqID: e.ID, svc: e.Service}
		if cloud == "" {
			st.Cloud = e.Cloud
		}
		if st.Op == "" {
			st.Op = e.Method + " " + strings.SplitN(e.Path, "?", 2)[0]
		} else if p := strings.SplitN(e.Path, "?", 2)[0]; p != "/" && len(p) <= 48 {
			st.Hint = p
		}
		if e.Status >= 400 || e.Error != "" {
			st.Err = fmt.Sprintf("HTTP %d %s", e.Status, e.Error)
		}
		by[e.Trace] = append(by[e.Trace], st)
	}
	for _, s := range ui.hub.Spans.All() {
		if s.Trace == "" {
			continue
		}
		cloud, title := ui.svcMeta(s.Service, s.To)
		st := flowStep{Time: s.Time, Ms: s.Ms, Cloud: cloud, Service: title, Op: s.Op, Err: s.Err, svc: s.Service, from: s.From, to: s.To, kind: s.Kind}
		switch s.Kind {
		case core.SpanDeliver:
			st.Op = "deliver → " + shortName(s.To)
		case core.SpanRedrive:
			st.Service, st.Op = title+" DLQ", "redrive → "+shortName(s.To)
		case core.SpanNotify:
			st.Op = "notify → " + s.To
		}
		by[s.Trace] = append(by[s.Trace], st)
	}
	var out []*flow
	for trace, steps := range by {
		if len(steps) >= 2 {
			out = append(out, ui.makeFlow(trace, steps))
		}
	}
	slices.SortFunc(out, func(a, b *flow) int { return b.Start.Compare(a.Start) })
	return out
}

func (ui *UI) makeFlow(trace string, steps []flowStep) *flow {
	slices.SortStableFunc(steps, func(a, b flowStep) int { return a.Time.Compare(b.Time) })
	// depth follows cause: a delivery or invocation hangs under the latest earlier step that
	// ended at its sender, a request under the latest earlier invocation (the code that made it).
	for i := range steps {
		s := &steps[i]
		for j := i - 1; j >= 0; j-- {
			p := steps[j]
			from, _ := core.DeliverTarget(s.from)
			if s.kind != "" && from != "" && p.svc == from || s.kind == "" && p.kind == core.SpanInvoke {
				s.depth = min(p.depth+1, 8)
				break
			}
		}
		s.Indent = strings.Repeat("· ", s.depth)
	}
	f := &flow{Trace: trace, Start: steps[0].Time, Steps: steps, Cloud: steps[0].Cloud}
	end := steps[0].Time
	for _, s := range steps {
		end = later(end, s.Time.Add(time.Duration(s.Ms*float64(time.Millisecond))))
	}
	f.Ms = float64(end.Sub(f.Start).Microseconds()) / 1000
	tot := max(f.Ms, 1)
	for i := range f.Steps {
		s := &f.Steps[i]
		at := float64(s.Time.Sub(f.Start).Microseconds()) / 1000
		s.Left, s.Width = fmt.Sprintf("%.2f", at/tot*82), fmt.Sprintf("%.2f", max(.4, s.Ms/tot*82))
		s.Dur = fmtMs(s.Ms)
		if s.Err != "" && !f.Failed {
			f.Failed, f.Why = true, fmt.Sprintf("%s %s failed: %s", s.Service, s.Op, strings.TrimSpace(s.Err))
		}
		if n := len(f.Path); n == 0 || f.Path[n-1].Name != s.Service {
			f.Path = append(f.Path, flowChip{Cloud: s.Cloud, Name: s.Service})
		}
		if s.Err != "" {
			f.Path[len(f.Path)-1].Err = true
		}
	}
	f.Trigger = strings.TrimSpace(steps[0].Op + " " + steps[0].Hint)
	f.Duration = fmtMs(f.Ms)
	for _, q := range []float64{0, .25, .5, .75, 1} {
		f.Axis = append(f.Axis, fmtMs(f.Ms*q))
	}
	return f
}

func (f *flow) matches(q string) bool {
	if q == "" {
		return true
	}
	q = strings.ReplaceAll(strings.ToLower(q), "-", "") // a W3C id has no dashes
	hay := strings.ToLower(f.Trace+" "+f.Trigger) + " " + strings.ReplaceAll(f.Trace, "-", "")
	for _, s := range f.Steps {
		hay += " " + strings.ToLower(s.Service+" "+s.Op+" "+s.from+" "+s.to)
	}
	return strings.Contains(strings.ReplaceAll(hay, "-", ""), q)
}

func (ui *UI) flows(w http.ResponseWriter, r *http.Request) {
	q, chip := strings.TrimSpace(r.URL.Query().Get("q")), r.URL.Query().Get("f")
	all := ui.buildFlows()
	pg := flowsPage{Q: q, F: chip, Live: true}
	cloud := ui.cloud(r)
	for _, f := range all {
		if (cloud == "" || slices.ContainsFunc(f.Steps, func(s flowStep) bool { return s.Cloud == cloud })) && f.matches(q) && (chip == "" || chip == "failed" && f.Failed || chip == "slow" && f.Ms > 1000) {
			pg.Flows = append(pg.Flows, f)
		}
	}
	pg.NotShown = len(all) - len(pg.Flows)
	if r.URL.Query().Get("frag") != "" { // the 3 s poll swaps only the rows
		ui.fragment(w, "flows", "rows", pg)
		return
	}
	ui.render(w, r, "flows", "Flows", "flows", pg)
}

func (ui *UI) flow(w http.ResponseWriter, r *http.Request) {
	trace := r.PathValue("trace")
	for _, f := range ui.buildFlows() {
		if f.Trace == trace {
			ui.renderX(w, r, "flows", f.Trigger, "flows", "Flow · trace "+f.Trace+" · "+f.Start.Local().Format("15:04:05"), flowsPage{One: f})
			return
		}
	}
	back(w, r, "/_pogi/flows", fmt.Errorf("flow %s is no longer in the log", trace))
}

// fragment writes one named template of a page, for htmx polls.
func (ui *UI) fragment(w http.ResponseWriter, page, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := ui.pages[page].ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// flowNotes tells the bell about flows that failed in the last half hour.
func flowNotes(ui *UI) []note {
	var out []note
	for _, f := range ui.buildFlows() {
		if len(out) == 5 || time.Since(f.Start) > 30*time.Minute {
			break
		}
		if f.Failed {
			out = append(out, note{Time: f.Start, Kind: "bad", Icon: "pulse", Title: "Flow failed: " + f.Trigger, Text: f.Why,
				Href: "/_pogi/flows/" + url.PathEscape(f.Trace), Drawer: true})
		}
	}
	return out
}
