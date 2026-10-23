package web

// The bell: failed requests, IAM denials and chaos faults from the request log, plus whatever
// other features add through noteFuncs. Read state and muted kinds live in cookies.

import (
	"encoding/json"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// noteKinds are the bell's categories, in the order of its footer chips.
var noteKinds = [][2]string{{"sdk", "SDK errors"}, {"flows", "Failed flows"}, {"iam", "IAM denials"}, {"chaos", "Chaos faults"}, {"mine", "Your resources"}}

func (ui *UI) noteRoutes(m *http.ServeMux) {
	m.HandleFunc("GET /_pogi/notes", ui.notes)
	m.HandleFunc("POST /_pogi/notes", ui.notes)
	m.HandleFunc("GET /_pogi/notes/poll", ui.notesPoll)
}

type noteRow struct {
	note
	Group  string
	Unread bool
	Ago    string
}

func cookieInt(r *http.Request, name string) int64 {
	c, err := r.Cookie(name)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(c.Value, 10, 64)
	return n
}

// notesSeen is when the user last marked the bell read. On the first visit there is no cookie: it is
// set to now, so earlier errors are listed but not counted as unread.
func notesSeen(w http.ResponseWriter, r *http.Request) int64 {
	if _, err := r.Cookie("pogi-notes-seen"); err == nil {
		return cookieInt(r, "pogi-notes-seen")
	}
	now := time.Now().UnixMilli()
	setNoteCookie(w, "pogi-notes-seen", strconv.FormatInt(now, 10))
	return now
}

func offKinds(r *http.Request) []string {
	if c, err := r.Cookie("pogi-notes-off"); err == nil {
		v, _ := url.QueryUnescape(c.Value)
		return strings.Split(v, ",")
	}
	return nil
}

func setNoteCookie(w http.ResponseWriter, name, v string) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: url.QueryEscape(v), Path: "/", MaxAge: 365 * 24 * 3600, SameSite: http.SameSiteLaxMode})
}

// noteGroup files a note under a footer chip by its icon (what other features' notes carry).
func noteGroup(n note) string {
	switch n.Icon {
	case "lock":
		return "iam"
	case "pulse":
		return "flows"
	case "bolt":
		return "chaos"
	case "box":
		return "mine"
	}
	return "sdk"
}

// noteRows are all notifications, newest first, minus muted kinds. seen is the unix ms the user last marked read.
func (ui *UI) noteRows(off []string, seen int64) []noteRow {
	var all []note
	for _, e := range ui.hub.Requests.Query(0, 0, "", "", false) {
		if strings.HasPrefix(e.Path, "/_pogi/") {
			continue
		}
		n := note{Time: e.Time, Href: "/_pogi/requests/" + strconv.FormatInt(e.ID, 10), Drawer: true}
		switch {
		case e.Chaos:
			n.Kind, n.Icon, n.Title, n.Text = "warn", "bolt", "Chaos fault injected", strings.TrimSpace(strconv.Itoa(e.Status)+" "+e.Error)+" on "+e.Service+" "+e.Op
		case strings.HasPrefix(e.IAM, "deny"):
			n.Kind, n.Icon, n.Title, n.Text = "warn", "lock", "IAM would deny "+e.Service+" "+e.Op, strings.TrimPrefix(strings.TrimPrefix(e.IAM, "deny"), ": ")
		case e.Status >= 400:
			n.Kind, n.Icon, n.Title, n.Text, n.Quiet = "bad", "x", e.Service+" "+e.Op+" failed", strings.TrimSpace(strconv.Itoa(e.Status)+" "+e.Error), e.Console
		default:
			continue
		}
		all = append(all, n)
	}
	for _, f := range noteFuncs {
		all = append(all, f(ui)...)
	}
	slices.SortStableFunc(all, func(a, b note) int { return b.Time.Compare(a.Time) })
	var out []noteRow
	for _, n := range all {
		if g := noteGroup(n); !slices.Contains(off, g) {
			out = append(out, noteRow{n, g, n.Time.UnixMilli() > seen, strings.TrimSuffix(ago(n.Time), " ago")})
		}
	}
	return out
}

var bellTmpl = template.Must(template.New("bell").Funcs(funcs).Parse(`<header>Notifications<a hx-post="/_pogi/notes" hx-vals='{"read":"1"}' hx-target="#bell" hx-swap="innerHTML">Mark all read</a></header>
<div style="max-height:360px;overflow-y:auto">{{range .Items}}<a class="note-it {{if .Unread}}unread{{end}}" href="{{.Href}}" {{if .Drawer}}data-drawer{{end}}><span class="ico {{.Kind}}">{{icon .Icon}}</span><span><b>{{.Title}}</b><small>{{.Text}}</small></span><span class="ago">{{.Ago}}</span></a>
{{else}}<div class="empty" style="padding:30px">Failed calls, failed flows, IAM denials, chaos faults and your resources being ready or failing show up here.</div>{{end}}</div>
<footer>Notify me about{{range .Kinds}}<span class="chip {{if .On}}on{{end}}" hx-post="/_pogi/notes" hx-vals='{"toggle":"{{.Key}}"}' hx-target="#bell" hx-swap="innerHTML">{{.Label}}</span>{{end}}</footer>
{{if .OOB}}<span class="badge-dot" id="bellN" hx-swap-oob="true">{{if .Unread}}{{.Unread}}{{end}}</span>{{end}}`))

// notes is the bell popover. A post marks everything read ("read") or mutes and unmutes a kind ("toggle").
func (ui *UI) notes(w http.ResponseWriter, r *http.Request) {
	off, seen, post := offKinds(r), notesSeen(w, r), r.Method == http.MethodPost
	if post {
		if r.FormValue("read") != "" {
			seen = time.Now().UnixMilli()
			setNoteCookie(w, "pogi-notes-seen", strconv.FormatInt(seen, 10))
		}
		if k := r.FormValue("toggle"); slices.ContainsFunc(noteKinds, func(n [2]string) bool { return n[0] == k }) {
			if i := slices.Index(off, k); i >= 0 {
				off = slices.Delete(off, i, i+1)
			} else {
				off = append(off, k)
			}
			setNoteCookie(w, "pogi-notes-off", strings.Join(off, ","))
		}
	}
	rows := ui.noteRows(off, seen)
	unread := 0
	for _, n := range rows {
		if n.Unread {
			unread++
		}
	}
	type kind struct {
		Key, Label string
		On         bool
	}
	var kinds []kind
	for _, k := range noteKinds {
		kinds = append(kinds, kind{k[0], k[1], !slices.Contains(off, k[0])})
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	bellTmpl.Execute(w, map[string]any{"Items": rows[:min(8, len(rows))], "Kinds": kinds, "OOB": post, "Unread": unread})
}

type newestMail struct {
	ID      string `json:"id"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	At      int64  `json:"at"` // unix ms it was captured
}

// notesPoll feeds the bell badge and the toasts: the unread count, and what is newer than since (unix ms).
func (ui *UI) notesPoll(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	out := struct {
		Unread int                 `json:"unread"`
		Now    int64               `json:"now"`
		New    []map[string]string `json:"new"`
		Mail   *newestMail         `json:"mail"` // the newest captured message, for the live inbox
	}{Now: time.Now().UnixMilli(), New: []map[string]string{}}
	for _, n := range ui.noteRows(offKinds(r), notesSeen(w, r)) {
		if n.Unread {
			out.Unread++
		}
		if n.Time.UnixMilli() > since && !n.Quiet {
			out.New = append(out.New, map[string]string{"msg": n.Title + " · " + n.Text, "kind": n.Kind, "href": n.Href})
		}
	}
	if l := ui.mailbox(); len(l) > 0 {
		out.Mail = &newestMail{ID: strconv.FormatInt(l[0].Time.UnixNano(), 10) + "/" + strconv.Itoa(len(l)), To: l[0].To, Subject: l[0].Subject, At: l[0].Time.UnixMilli()}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
