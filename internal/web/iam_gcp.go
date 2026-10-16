package web

// The Google Cloud view of the IAM page: what each service account and user called, what strict
// mode would say, and the role bindings that follow, which can be attached through the services'
// own setIamPolicy and undone. The AWS view and the shared parts are in iam.go.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	giam "github.com/nrndev/pogi/internal/gcp/iam"
)

func init() {
	iamViews[core.GCP] = func(ui *UI, w http.ResponseWriter, r *http.Request, h iamHead) { ui.iamGCPPage(w, r, h) }
	iamRouteFuncs = append(iamRouteFuncs, func(ui *UI, m *http.ServeMux) {
		for _, p := range []string{"iam-gcp", "iam-gcp-attach"} {
			ui.pages[p] = template.Must(template.Must(shell().Clone()).ParseFS(files, "templates/"+p+".html", "templates/iam-common.html"))
		}
		m.HandleFunc("GET /_pogi/iam/gcp/attach", ui.iamGCPAttachPage)
		m.HandleFunc("POST /_pogi/iam/gcp/attach", ui.iamGCPAttach)
		m.HandleFunc("POST /_pogi/iam/gcp/undo", ui.iamGCPUndo)
		m.HandleFunc("POST /_pogi/iam/gcp/restart", ui.iamGCPRestart)
		m.HandleFunc("GET /_pogi/iam/gcp/snippet", ui.iamGCPDownload)
	})
	badgeFuncs = append(badgeFuncs, func(ui *UI, r *http.Request, b map[string]badge) {
		mode := ui.iamMode()
		if mode == "off" {
			return
		}
		n := 0
		for _, p := range giam.Recorded(ui.hub) {
			n += len(p.WouldDeny())
		}
		if prev, ok := b["iam"]; ok { // the AWS count is already there
			var m int
			fmt.Sscanf(prev.Text, "%d", &m)
			n += m
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
		for _, p := range giam.Recorded(ui.hub) {
			for _, c := range p.WouldDeny() {
				out = append(out, note{Time: c.Last, Kind: "warn", Icon: "lock", Title: "IAM would deny " + c.Perm,
					Text: p.Member + " → " + giam.Short(c.Resource) + " · soft mode", Href: "/_pogi/iam?c=gcp&principal=" + url.QueryEscape(p.Member)})
			}
		}
		slices.SortFunc(out, func(a, b note) int { return b.Time.Compare(a.Time) })
		return out[:min(len(out), 10)]
	})
}

type iamGCPRow struct {
	Perm, Resource, Full, Reason, Verdict string
	Count                                 int
	Modelled                              bool
}

type iamGCPData struct {
	iamHead
	Banner, Format, Role, Doc string
	Principals                []giam.Principal
	Rows                      []iamGCPRow
	Bindings                  []giam.Binding
	Uncovered                 []string
	Calls, Missing            int
	Since                     string
	Attached                  string // the undo token of an Attach that just happened
	AttachedWhat              string
}

// iamGCPPrincipals are the recorded principals, with the calls in the console's project.
func (ui *UI) iamGCPPrincipals(r *http.Request) []giam.Principal {
	sc := ui.scope(r)
	var out []giam.Principal
	for _, p := range giam.Recorded(ui.hub) {
		p.Actions = slices.DeleteFunc(p.Actions, func(c giam.Call) bool { return sc.Project != all && c.Project != "" && c.Project != sc.Project })
		if len(p.Actions) > 0 {
			out = append(out, p)
		}
	}
	return out
}

func iamGCPPick(ps []giam.Principal, want string) (giam.Principal, bool) {
	if i := slices.IndexFunc(ps, func(p giam.Principal) bool { return p.Member == want }); i >= 0 {
		return ps[i], true
	}
	if i := slices.IndexFunc(ps, func(p giam.Principal) bool { return len(p.WouldDeny()) > 0 }); i >= 0 {
		return ps[i], true
	}
	if len(ps) > 0 {
		return ps[0], true
	}
	return giam.Principal{}, false
}

func (ui *UI) iamGCPPage(w http.ResponseWriter, r *http.Request, head iamHead) {
	q := r.URL.Query()
	head.Sub = "pogi records which permissions each service account and user uses, and turns that into role bindings."
	ps := ui.iamGCPPrincipals(r)
	d := iamGCPData{iamHead: head, Format: "gcloud", Role: "predefined", Principals: ps, Banner: strings.NewReplacer("AWS", "Google Cloud", "AccessDenied", "PERMISSION_DENIED").Replace(iamBanners[head.Mode])}
	if f := q.Get("fmt"); f == "tf" {
		d.Format = f
	}
	if q.Get("role") == "custom" {
		d.Role = "custom"
	}
	if p, ok := iamGCPPick(ps, q.Get("principal")); ok {
		d.Selected, d.Calls, d.Since, d.Missing = p.Member, p.Calls, p.Since.Format("15:04"), len(p.WouldDeny())
		for _, c := range p.Actions {
			d.Rows = append(d.Rows, iamGCPRow{Perm: c.Perm, Resource: giam.Short(c.Resource), Full: c.Resource, Count: c.Count, Verdict: c.Verdict, Reason: c.Reason, Modelled: c.Modelled()})
		}
		pl := p.Plan(d.Role == "custom")
		d.Doc, d.Bindings, d.Uncovered = iamGCPDoc(pl, d.Format), pl.Bindings, pl.Uncovered
	}
	if u := q.Get("attached"); u != "" {
		d.Attached = u
		if a, err := decodeAttachment(u); err == nil {
			d.AttachedWhat = fmt.Sprintf("%d policy %s", len(a.Changes), map[bool]string{true: "change", false: "changes"}[len(a.Changes) == 1])
			if a.Role != "" {
				d.AttachedWhat += " and the role " + a.Role[strings.LastIndex(a.Role, "/")+1:]
			}
		}
	}
	ui.render(w, r, "iam-gcp", "IAM policy", "iam", d)
}

func iamGCPDoc(pl giam.Plan, format string) string {
	if format == "tf" {
		return pl.Terraform()
	}
	return pl.Gcloud()
}

func encodeAttachment(a giam.Attachment) string {
	b, _ := json.Marshal(a)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeAttachment(s string) (a giam.Attachment, err error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err == nil {
		err = json.Unmarshal(b, &a)
	}
	return a, err
}

func (ui *UI) iamGCPDownload(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pl, _, ok := giam.Preview(ui.hub, q.Get("principal"), q.Get("role") == "custom")
	if !ok {
		http.Error(w, "nothing recorded", http.StatusNotFound)
		return
	}
	doc, ext := pl.Gcloud(), ".sh"
	if q.Get("fmt") == "tf" {
		doc, ext = pl.Terraform(), ".tf"
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="pogi-iam`+ext+`"`)
	fmt.Fprint(w, doc)
}

type iamGCPAttachData struct {
	Principal, Role, Doc string
	Diff                 []string
}

func (ui *UI) iamGCPAttachPage(w http.ResponseWriter, r *http.Request) {
	want, role := r.URL.Query().Get("principal"), r.URL.Query().Get("role")
	pl, diff, ok := giam.Preview(ui.hub, want, role == "custom")
	if !ok {
		back(w, r, "/_pogi/iam?c=gcp", fmt.Errorf("%s has no recorded calls", want))
		return
	}
	ui.renderX(w, r, "iam-gcp-attach", "Attach to "+want, "iam", "IAM policy", iamGCPAttachData{want, role, pl.Gcloud(), diff})
}

func (ui *UI) iamGCPAttach(w http.ResponseWriter, r *http.Request) {
	principal := r.FormValue("principal")
	to := "/_pogi/iam?c=gcp&principal=" + url.QueryEscape(principal)
	if r.FormValue("role") == "custom" {
		to += "&role=custom"
	}
	a, err := giam.Attach(ui.hub, principal, r.FormValue("role") == "custom")
	if err != nil {
		done(w, r, to, "", err)
		return
	}
	done(w, r, to+"&attached="+encodeAttachment(a), fmt.Sprintf("Attached %d %s to %s", len(a.Changes), map[bool]string{true: "binding", false: "bindings"}[len(a.Changes) == 1], principal), nil)
}

func (ui *UI) iamGCPUndo(w http.ResponseWriter, r *http.Request) {
	a, err := decodeAttachment(r.FormValue("u"))
	if err == nil {
		err = giam.Undo(ui.hub, a)
	}
	done(w, r, "/_pogi/iam?c=gcp&principal="+url.QueryEscape(r.FormValue("principal")), "Undid the bindings", err)
}

func (ui *UI) iamGCPRestart(w http.ResponseWriter, r *http.Request) {
	giam.Restart(ui.hub, r.FormValue("principal"))
	done(w, r, "/_pogi/iam?c=gcp", "Recording cleared", nil)
}
