package web

// The IAM policy page: what each user and role called, what strict mode would say about it, and
// the least-privilege policy that follows, which can be attached as a versioned managed policy.

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/core"
)

func init() {
	badgeFuncs = append(badgeFuncs, func(ui *UI, r *http.Request, b map[string]badge) {
		mode := ui.iamMode()
		if mode == "off" {
			return
		}
		n := 0
		for _, p := range iam.Recorded(ui.hub) {
			n += len(p.WouldDeny())
		}
		if n > 0 {
			b["iam"] = badge{fmt.Sprintf("%d %s", n, map[string]string{"soft": "would deny", "strict": "denied"}[mode]), "warn"}
		}
	})
	noteFuncs = append(noteFuncs, func(ui *UI) []note {
		if ui.iamMode() != "soft" {
			return nil
		}
		var out []note
		for _, p := range iam.Recorded(ui.hub) {
			for _, c := range p.WouldDeny() {
				out = append(out, note{Time: c.Last, Kind: "warn", Icon: "lock", Title: "IAM would deny " + c.Action,
					Text: iamLabel(p.ARN) + " → " + iam.Short(c.Resource) + " · soft mode", Href: "/_pogi/iam?principal=" + url.QueryEscape(p.ARN)})
			}
		}
		slices.SortFunc(out, func(a, b note) int { return b.Time.Compare(a.Time) })
		return out[:min(len(out), 10)]
	})
}

func (ui *UI) iamRoutes(m *http.ServeMux) {
	layout := shell()
	for _, p := range []string{"iam", "iam-attach"} {
		ui.pages[p] = template.Must(template.Must(layout.Clone()).ParseFS(files, "templates/"+p+".html", "templates/iam-common.html"))
	}
	m.HandleFunc("GET /_pogi/iam", ui.iamPage)
	m.HandleFunc("GET /_pogi/iam/policy", ui.iamDownload)
	m.HandleFunc("GET /_pogi/iam/attach", ui.iamAttachPage)
	m.HandleFunc("POST /_pogi/iam/attach", ui.iamAttach)
	m.HandleFunc("POST /_pogi/iam/undo", ui.iamUndo)
	m.HandleFunc("POST /_pogi/iam/mode", ui.iamModeSet)
	m.HandleFunc("POST /_pogi/iam/restart", ui.iamRestart)
	for _, f := range iamRouteFuncs {
		f(ui, m)
	}
}

// iamMode is the enforcement mode, never "".
func (ui *UI) iamMode() string {
	if m := ui.hub.IAMMode(); m == "soft" || m == "strict" {
		return m
	}
	return "off"
}

// iamLabel is "role/orders-worker" for an ARN.
func iamLabel(arn string) string {
	_, _, _, res, _ := core.ParseARN(arn)
	return res
}

type iamRow struct {
	Action, Op, Resource, Full, Reason, Verdict string
	Count                                       int
}

// iamHead is the part of every cloud's IAM view the shared "iam-head" template draws: the
// enforcement switch (one mode for all clouds) and, when the cloud picker is on All, the cloud tabs.
type iamHead struct {
	Mode, Selected, Sub string
	View                string // the cloud this view shows
	Tabs                bool
}

// iamViews are the IAM page's Google Cloud and Azure views, keyed by cloud; each registers its
// own routes (under /_pogi/iam/<cloud>/) through iamRouteFuncs. AWS is the built-in view below.
var (
	iamViews      = map[string]func(ui *UI, w http.ResponseWriter, r *http.Request, h iamHead){}
	iamRouteFuncs []func(ui *UI, m *http.ServeMux)
)

// iamHeadFor is the head of the IAM view this request shows: the picked cloud, or with All the
// ?c= tab (AWS by default).
func (ui *UI) iamHeadFor(r *http.Request) iamHead {
	h := iamHead{Mode: ui.iamMode(), View: ui.cloud(r)}
	if h.View == "" {
		h.Tabs, h.View = true, core.AWS
		if c := r.URL.Query().Get("c"); c == core.GCP || c == core.Azure {
			h.View = c
		}
	}
	return h
}

type iamData struct {
	iamHead
	Banner, Format, Doc, Label string
	Principals                 []iam.Principal
	Labels                     map[string]string
	Rows                       []iamRow
	Calls                      int
	Since                      string
	Missing                    int
	Policy                     string // pogi-<name>
	Attached                   *iam.Attachment
}

var iamBanners = map[string]string{
	"off":    "Off: every call is allowed and nothing is checked. Recording still runs, so you can generate a policy.",
	"soft":   "Soft: every call is allowed; calls a strict check would deny are flagged below and in Activity.",
	"strict": "Strict: calls without an allowing policy fail with AccessDenied, as on real AWS.",
}

// principals are the recorded principals in the console's account scope.
func (ui *UI) iamPrincipals(r *http.Request) []iam.Principal {
	sc := ui.scope(r)
	var out []iam.Principal
	for _, p := range iam.Recorded(ui.hub) {
		if _, _, acct, _, _ := core.ParseARN(p.ARN); sc.Account == all || acct == sc.Account {
			out = append(out, p)
		}
	}
	return out
}

// iamPick is the recorded principal the request asks for. It never falls back to another one.
func iamPick(ps []iam.Principal, want string) (iam.Principal, bool) {
	if i := slices.IndexFunc(ps, func(p iam.Principal) bool { return p.ARN == want }); i >= 0 {
		return ps[i], true
	}
	return iam.Principal{}, false
}

// iamDefault is what the console page selects when the asked-for principal has nothing recorded:
// the first principal with calls strict would deny, else the first.
func iamDefault(ps []iam.Principal) (iam.Principal, bool) {
	if i := slices.IndexFunc(ps, func(p iam.Principal) bool { return len(p.WouldDeny()) > 0 }); i >= 0 {
		return ps[i], true
	}
	if len(ps) > 0 {
		return ps[0], true
	}
	return iam.Principal{}, false
}

func (ui *UI) iamPage(w http.ResponseWriter, r *http.Request) {
	q, head := r.URL.Query(), ui.iamHeadFor(r)
	if head.View != core.AWS {
		if f := iamViews[head.View]; f != nil {
			f(ui, w, r, head)
			return
		}
	}
	head.Sub = "pogi records which actions each principal calls, and turns that into a least-privilege policy."
	ps := ui.iamPrincipals(r)
	d := iamData{iamHead: head, Format: "json", Principals: ps}
	d.Banner, d.Labels = iamBanners[d.Mode], map[string]string{}
	for _, p := range ps {
		d.Labels[p.ARN] = iamLabel(p.ARN)
	}
	if q.Get("fmt") == "tf" {
		d.Format = "tf"
	}
	p, ok := iamPick(ps, q.Get("principal"))
	if !ok {
		p, ok = iamDefault(ps)
	}
	if ok {
		d.Selected, d.Label, d.Calls, d.Since = p.ARN, iamLabel(p.ARN), p.Calls, p.Since.Format("15:04")
		d.Doc, d.Missing, d.Policy = p.Policy(), len(p.WouldDeny()), iam.PolicyName(p.ARN)
		if d.Format == "tf" {
			d.Doc = p.Terraform()
		}
		for _, c := range p.Actions {
			_, op, _ := strings.Cut(c.Action, ":")
			d.Rows = append(d.Rows, iamRow{Action: c.Action, Op: op, Resource: iam.Short(c.Resource), Full: c.Resource, Count: c.Count, Verdict: c.Verdict, Reason: c.Reason})
		}
	}
	if v := q.Get("attached"); v != "" {
		d.Attached = &iam.Attachment{Principal: q.Get("principal"), Policy: q.Get("policy"), Arn: q.Get("arn"), Version: v, Prev: q.Get("prev"), Linked: q.Get("linked") == "1"}
	}
	ui.render(w, r, "iam", "IAM policy", "iam", d)
}

func (ui *UI) iamDownload(w http.ResponseWriter, r *http.Request) {
	want := r.URL.Query().Get("principal")
	if want == "" {
		http.Error(w, "missing principal", http.StatusNotFound)
		return
	}
	p, ok := iamPick(ui.iamPrincipals(r), want)
	if !ok {
		http.Error(w, "nothing recorded for "+want, http.StatusNotFound)
		return
	}
	doc, ext := p.Policy(), ".json"
	if r.URL.Query().Get("fmt") == "tf" {
		doc, ext = p.Terraform(), ".tf"
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+iam.PolicyName(p.ARN)+ext+`"`)
	fmt.Fprint(w, doc)
}

type iamAttachData struct {
	Principal, Label, Policy, Version, Doc string
	Diff                                   []string
	Exists                                 bool
}

func (ui *UI) iamAttachPage(w http.ResponseWriter, r *http.Request) {
	want := r.URL.Query().Get("principal")
	p, ok := iamPick(ui.iamPrincipals(r), want)
	if !ok {
		back(w, r, "/_pogi/iam", fmt.Errorf("%s has no recorded calls", want))
		return
	}
	v, exists := iam.NextVersion(ui.hub, p.ARN)
	ui.renderX(w, r, "iam-attach", "Attach to "+iamLabel(p.ARN), "iam", "IAM policy", iamAttachData{p.ARN, iamLabel(p.ARN), iam.PolicyName(p.ARN), v, p.Policy(), p.Diff(), exists})
}

func (ui *UI) iamAttach(w http.ResponseWriter, r *http.Request) {
	principal := r.FormValue("principal")
	to := "/_pogi/iam?principal=" + url.QueryEscape(principal)
	a, err := iam.Attach(ui.hub, principal)
	if err != nil {
		done(w, r, to, "", err)
		return
	}
	linked := "0"
	if a.Linked {
		linked = "1"
	}
	to += "&attached=" + a.Version + "&policy=" + a.Policy + "&arn=" + url.QueryEscape(a.Arn) + "&prev=" + a.Prev + "&linked=" + linked
	done(w, r, to, fmt.Sprintf("Attached %s of %s", a.Version, a.Policy), nil)
}

func (ui *UI) iamUndo(w http.ResponseWriter, r *http.Request) {
	a := iam.Attachment{Principal: r.FormValue("principal"), Policy: r.FormValue("policy"), Arn: r.FormValue("arn"),
		Version: r.FormValue("attached"), Prev: r.FormValue("prev"), Linked: r.FormValue("linked") == "1"}
	err := iam.Undo(ui.hub, a)
	done(w, r, "/_pogi/iam?principal="+url.QueryEscape(a.Principal), "Undid "+a.Version+" of "+a.Policy, err)
}

func (ui *UI) iamModeSet(w http.ResponseWriter, r *http.Request) {
	mode, to := r.FormValue("mode"), r.FormValue("to")
	if mode != "off" && mode != "soft" && mode != "strict" {
		back(w, r, "/_pogi/iam", fmt.Errorf("unknown mode %q", mode))
		return
	}
	if !strings.HasPrefix(to, "/_pogi/") {
		to = "/_pogi/iam?principal=" + url.QueryEscape(r.FormValue("principal"))
		if c := r.FormValue("c"); c == core.GCP || c == core.Azure {
			to += "&c=" + c
		}
	}
	ui.hub.SetIAMMode(mode)
	done(w, r, to, "Enforcement: "+mode, nil)
}

func (ui *UI) iamRestart(w http.ResponseWriter, r *http.Request) {
	iam.Restart(ui.hub, r.FormValue("principal"))
	done(w, r, "/_pogi/iam", "Recording cleared", nil)
}
