package web

// The Azure view of the IAM page: what each service principal and managed identity called, what
// strict mode would say, and the least-privilege custom role that follows, which can be created and
// assigned in one step (and undone).

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/azure/rbac"
	"github.com/nrndev/pogi/internal/core"
)

func init() {
	iamViews[core.Azure] = (*UI).iamAzure
	iamRouteFuncs = append(iamRouteFuncs, func(ui *UI, m *http.ServeMux) {
		ui.pages["iam-azure"] = template.Must(template.Must(shell().Clone()).ParseFS(files, "templates/iam-azure.html", "templates/iam-common.html"))
		m.HandleFunc("GET /_pogi/iam/azure/policy", ui.iamAzureDownload)
		m.HandleFunc("GET /_pogi/iam/azure/attach", ui.iamAzureAttachPage)
		m.HandleFunc("POST /_pogi/iam/azure/attach", ui.iamAzureAttach)
		m.HandleFunc("POST /_pogi/iam/azure/undo", ui.iamAzureUndo)
		m.HandleFunc("POST /_pogi/iam/azure/restart", ui.iamAzureRestart)
	})
	badgeFuncs = append(badgeFuncs, func(ui *UI, r *http.Request, b map[string]badge) {
		mode := ui.iamMode()
		n := 0
		for _, p := range rbac.Recorded(ui.hub) {
			n += len(p.WouldDeny())
		}
		if mode == "off" || n == 0 {
			return
		}
		var prev int
		fmt.Sscanf(b["iam"].Text, "%d", &prev) // the AWS badge is "N would deny": add to it
		b["iam"] = badge{fmt.Sprintf("%d %s", prev+n, map[string]string{"soft": "would deny", "strict": "denied"}[mode]), "warn"}
	})
	noteFuncs = append(noteFuncs, func(ui *UI) []note {
		if ui.iamMode() != "soft" {
			return nil
		}
		var out []note
		for _, p := range rbac.Recorded(ui.hub) {
			for _, c := range p.WouldDeny() {
				out = append(out, note{Time: c.Last, Kind: "warn", Icon: "lock", Title: "RBAC would deny " + c.Action,
					Text: p.Name + " → " + rbac.Short(c.Scope) + " · soft mode", Href: "/_pogi/iam?c=azure&principal=" + url.QueryEscape(p.OID)})
			}
		}
		slices.SortFunc(out, func(a, b note) int { return b.Time.Compare(a.Time) })
		return out[:min(len(out), 10)]
	})
}

var iamAzureBanners = map[string]string{
	"off":    "Off: every call is allowed and nothing is checked. Recording still runs, so you can generate a role.",
	"soft":   "Soft: every call is allowed; calls a strict check would deny are flagged below and in Activity.",
	"strict": "Strict: calls without an allowing role assignment fail with AuthorizationFailed, as on real Azure.",
}

type iamAzureRow struct {
	Action, Tail, Scope, Full, Reason, Verdict string
	Data                                       bool
	Count                                      int
}

type iamAzureData struct {
	iamHead
	Banner, Format, Doc, Label, Hint, Role string
	Principals                             []rbac.Principal
	Labels                                 map[string]string
	Rows                                   []iamAzureRow
	Calls                                  int
	Since                                  string
	Missing                                int
	Attached                               *rbac.Attachment
	Attach                                 *iamAzureAttach // set on the attach page and drawer
}

type iamAzureAttach struct {
	Principal, Label, Role, Scope, Assignable, Doc string
	Add                                            []string
	Exists, Assigned                               bool
}

func iamAzureLabel(p rbac.Principal) string { return p.Name + " · " + p.Kind }

func iamAzurePick(ps []rbac.Principal, want string) (rbac.Principal, bool) {
	if i := slices.IndexFunc(ps, func(p rbac.Principal) bool { return p.OID == want }); i >= 0 {
		return ps[i], true
	}
	if i := slices.IndexFunc(ps, func(p rbac.Principal) bool { return len(p.WouldDeny()) > 0 }); i >= 0 {
		return ps[i], true
	}
	if len(ps) > 0 {
		return ps[0], true
	}
	return rbac.Principal{}, false
}

func (ui *UI) iamAzure(w http.ResponseWriter, r *http.Request, head iamHead) {
	q := r.URL.Query()
	head.Sub = "pogi records which actions each service principal and managed identity calls, and turns that into a least-privilege custom role."
	ps := rbac.Recorded(ui.hub)
	d := iamAzureData{iamHead: head, Format: "json", Principals: ps, Banner: iamAzureBanners[head.Mode], Labels: map[string]string{}}
	for _, p := range ps {
		d.Labels[p.OID] = iamAzureLabel(p)
	}
	if q.Get("fmt") == "cli" {
		d.Format = "cli"
	}
	if p, ok := iamAzurePick(ps, q.Get("principal")); ok {
		def := p.Definition()
		d.Selected, d.Label, d.Calls, d.Since, d.Missing, d.Role = p.OID, iamAzureLabel(p), p.Calls, p.Since.Format("15:04"), len(p.WouldDeny()), def.Name
		d.Doc = def.JSON()
		if d.Format == "cli" {
			d.Doc = def.CLI()
		}
		if name, n := p.Closest(); n > 0 {
			d.Hint = fmt.Sprintf("Closest built-in role: %s, which covers %d of %d recorded calls.", name, n, len(p.Actions))
		}
		for _, c := range p.Actions {
			d.Rows = append(d.Rows, iamAzureRow{Action: c.Action, Tail: iamAzureTail(c.Action), Scope: rbac.Short(c.Scope), Full: c.Scope, Data: c.Data, Count: c.Count, Verdict: c.Verdict, Reason: c.Reason})
		}
	}
	if v := q.Get("attached"); v != "" {
		d.Attached = &rbac.Attachment{Principal: q.Get("principal"), Role: q.Get("role"), RoleID: q.Get("roleid"), Assignment: q.Get("assignment"), Scope: q.Get("scope"), Existed: q.Get("existed") == "1"}
	}
	ui.render(w, r, "iam-azure", "IAM policy", "iam", d)
}

func (ui *UI) iamAzureDownload(w http.ResponseWriter, r *http.Request) {
	p, ok := iamAzurePick(rbac.Recorded(ui.hub), r.URL.Query().Get("principal"))
	if !ok {
		http.Error(w, "nothing recorded", http.StatusNotFound)
		return
	}
	doc, ext := p.Definition().JSON(), ".json"
	if r.URL.Query().Get("fmt") == "cli" {
		doc, ext = p.Definition().CLI(), ".sh"
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+p.Definition().Name+ext+`"`)
	fmt.Fprint(w, doc)
}

func (ui *UI) iamAzureAttachPage(w http.ResponseWriter, r *http.Request) {
	oid := r.URL.Query().Get("principal")
	pl, err := rbac.Preview(ui.hub, oid)
	if err != nil {
		back(w, r, "/_pogi/iam?c=azure", err)
		return
	}
	label := pl.Def.Name
	if p, ok := iamAzurePick(rbac.Recorded(ui.hub), oid); ok {
		label = p.Name
	}
	d := iamAzureData{iamHead: ui.iamHeadFor(r), Attach: &iamAzureAttach{Principal: oid, Label: label, Role: pl.Def.Name, Scope: pl.Def.Scope, Assignable: pl.Def.Assignable,
		Doc: pl.Doc, Add: pl.Add, Exists: pl.Existing, Assigned: pl.AssignmentExists}}
	ui.renderX(w, r, "iam-azure", "Attach to "+label, "iam", "IAM policy", d)
}

func (ui *UI) iamAzureAttach(w http.ResponseWriter, r *http.Request) {
	oid := r.FormValue("principal")
	if oid == "" {
		oid = r.URL.Query().Get("principal")
	}
	to := "/_pogi/iam?c=azure&principal=" + url.QueryEscape(oid)
	a, err := rbac.Attach(ui.hub, oid)
	if err != nil {
		done(w, r, to, "", err)
		return
	}
	existed := ""
	if a.Existed {
		existed = "1"
	}
	to += "&attached=1&role=" + url.QueryEscape(a.Role) + "&roleid=" + a.RoleID + "&assignment=" + url.QueryEscape(a.Assignment) + "&scope=" + url.QueryEscape(a.Scope) + "&existed=" + existed
	done(w, r, to, "Assigned "+a.Role, nil)
}

func (ui *UI) iamAzureUndo(w http.ResponseWriter, r *http.Request) {
	a := rbac.Attachment{Principal: r.FormValue("principal"), Role: r.FormValue("role"), RoleID: r.FormValue("roleid"), Assignment: r.FormValue("assignment"),
		Scope: r.FormValue("scope"), Existed: r.FormValue("existed") == "1"}
	err := rbac.Undo(ui.hub, a)
	done(w, r, "/_pogi/iam?c=azure&principal="+url.QueryEscape(a.Principal), "Undid "+a.Role, err)
}

func (ui *UI) iamAzureRestart(w http.ResponseWriter, r *http.Request) {
	rbac.Restart(ui.hub, strings.TrimSpace(r.FormValue("principal")))
	done(w, r, "/_pogi/iam?c=azure", "Recording cleared", nil)
}

// iamAzureTail shortens an action to its last three segments: the end says what it does.
func iamAzureTail(action string) string {
	seg := strings.Split(action, "/")
	if len(seg) <= 4 {
		return action
	}
	return ".../" + strings.Join(seg[len(seg)-3:], "/")
}
