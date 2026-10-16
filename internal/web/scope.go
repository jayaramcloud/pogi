package web

import (
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The header's switcher: which AWS account and region, Google Cloud project and Azure subscription
// the console looks at. It scopes the resource tables and is the identity forms submit as. It is kept in
// cookies and defaults to pogi's own defaults; "*" shows everything.

type scope struct{ Account, Region, Project, Subscription string }

const all = "*"

var scopeRE = regexp.MustCompile(`^(\*|[A-Za-z0-9_.-]{1,64})$`)

// awsRegions are the choices of the region selector (the cookie's own value is always one more).
var awsRegions = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1",
	"ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "ca-central-1", "sa-east-1"}

func (ui *UI) scope(r *http.Request) scope {
	get := func(name, def string) string {
		if c, err := r.Cookie("pogi-" + name); err == nil && scopeRE.MatchString(c.Value) {
			return c.Value
		}
		return def
	}
	return scope{get("account", core.DefaultAccount), get("region", ui.hub.Cfg.Region), get("project", core.DefaultProject), get("subscription", core.AzureSubscription)}
}

// acct, region, project and sub are what a submit runs as: "all" falls back to the default.
func (s scope) acct() string {
	if s.Account == all {
		return core.DefaultAccount
	}
	return s.Account
}

func (s scope) region(def string) string {
	if s.Region == all {
		return def
	}
	return s.Region
}

func (s scope) project() string {
	if s.Project == all {
		return core.DefaultProject
	}
	return s.Project
}

func (s scope) sub() string {
	if s.Subscription == all {
		return core.AzureSubscription
	}
	return s.Subscription
}

// Regions are the selector's options.
func (s scope) Regions() []string {
	out := awsRegions
	if s.Region != all && !contains(out, s.Region) {
		out = append([]string{s.Region}, out...)
	}
	return append([]string{all}, out...)
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// scopeSet saves the switcher and goes back to the page it was used on.
func (ui *UI) scopeSet(w http.ResponseWriter, r *http.Request) {
	cur := ui.scope(r)
	for name, def := range map[string]string{"account": cur.Account, "region": cur.Region, "project": cur.Project, "subscription": cur.Subscription} {
		v := strings.TrimSpace(r.FormValue(name))
		if !scopeRE.MatchString(v) {
			v = def
		}
		http.SetCookie(w, &http.Cookie{Name: "pogi-" + name, Value: v, Path: "/_pogi/", SameSite: http.SameSiteLaxMode, Expires: time.Now().AddDate(1, 0, 0)})
	}
	redirectBack(w, r)
}

// cloud is the header's cloud selector: aws, gcp or azure, or "" for all clouds.
func (ui *UI) cloud(r *http.Request) string {
	return cookie(r, "pogi-cloud", core.AWS, core.GCP, core.Azure)
}

// cloudSet saves the selector and goes back to the page it was used on.
func (ui *UI) cloudSet(w http.ResponseWriter, r *http.Request) {
	c := &http.Cookie{Name: "pogi-cloud", Path: "/_pogi/", SameSite: http.SameSiteLaxMode, Expires: time.Now().AddDate(1, 0, 0)}
	if v := r.FormValue("cloud"); slices.Contains([]string{core.AWS, core.GCP, core.Azure}, v) {
		c.Value = v
	} else {
		c.MaxAge = -1
	}
	http.SetCookie(w, c)
	redirectBack(w, r)
}

// redirectBack goes to the form's back, which must be a console URL.
func redirectBack(w http.ResponseWriter, r *http.Request) {
	to := r.FormValue("back")
	if !strings.HasPrefix(to, "/_pogi/") {
		to = "/_pogi/"
	}
	if r.Header.Get("HX-Request") != "" {
		w.Header().Set("HX-Redirect", to)
		return
	}
	http.Redirect(w, r, to, http.StatusSeeOther)
}

// allows reports whether a resource row belongs to the scope. A table says where a row lives in its
// Region, Account, Project or Subscription column, else its first cell does (an ARN, a Google resource
// name, an Azure resource ID, the generic engine's account/region/key); a row that says nothing stays.
// forCloud is the scope for one cloud's rows: the switcher's account and region are AWS ones, and
// a Google or Azure service's Account or Region column (a storage account, us-central1) is not.
func (s scope) forCloud(cloud string) scope {
	if cloud != core.AWS {
		s.Account, s.Region = all, all
	}
	return s
}

func (s scope) allows(cols, row []string) bool {
	var account, region, project, sub string
	for i, c := range cols {
		if i >= len(row) {
			break
		}
		switch strings.ToLower(c) {
		case "account":
			account = row[i]
		case "region":
			region = row[i]
		case "project":
			project = row[i]
		case "subscription":
			sub = row[i]
		}
	}
	if len(row) > 0 {
		first := row[0]
		switch {
		case strings.HasPrefix(first, "arn:"):
			_, r, a, _, _ := core.ParseARN(first)
			region, account = orElse(region, r), orElse(account, a)
		case strings.HasPrefix(first, "projects/"):
			project = orElse(project, strings.Split(first, "/")[1])
		case strings.HasPrefix(first, "/subscriptions/"):
			sub = orElse(sub, strings.Split(first, "/")[2])
		case account == "" && len(first) > 13 && first[12] == '/' && strings.Trim(first[:12], "0123456789") == "":
			p := strings.SplitN(first, "/", 3) // the generic engine's account/region/key
			account, region = p[0], orElse(region, p[1])
		}
	}
	return match(s.Account, account) && match(s.Region, region) && match(s.Project, project) && match(s.Subscription, sub)
}

func match(want, have string) bool { return want == all || have == "" || want == have }

func orElse(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
