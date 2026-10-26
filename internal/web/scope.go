package web

import (
	"encoding/json"
	"fmt"
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

type scope struct{ Account, Region, Project, Subscription, Location, Group, AzLocation string }

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
	return scope{get("account", core.DefaultAccount), get("region", ui.hub.Cfg.Region), get("project", core.DefaultProject), get("subscription", core.AzureSubscription),
		get("location", all), get("group", all), get("azlocation", all)}
}

// What a create uses when the scope says "*" (all). The Google and Azure filters start at "*", as their
// own consoles list every location and resource group.
const (
	defaultLocation   = "us-central1"
	defaultGroup      = "pogi"
	defaultAzLocation = "eastus"
)

// defaults is the scope with nothing changed.
func (ui *UI) defaults() scope {
	return scope{core.DefaultAccount, ui.hub.Cfg.Region, core.DefaultProject, core.AzureSubscription, all, all, all}
}

// filters counts the values that are not the default, for the pill of "All clouds".
func (ui *UI) filters(s scope) (n int) {
	d := ui.defaults()
	for _, p := range [][2]string{{s.Account, d.Account}, {s.Region, d.Region}, {s.Project, d.Project}, {s.Subscription, d.Subscription},
		{s.Location, d.Location}, {s.Group, d.Group}, {s.AzLocation, d.AzLocation}} {
		if p[0] != p[1] {
			n++
		}
	}
	return n
}

// scopeNames are the cookie names (after "pogi-") and form fields of the scope, in the scope struct's order.
var scopeNames = []string{"account", "region", "project", "subscription", "location", "group", "azlocation"}

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

// location is where a Google create goes, group and azLocation are where an Azure one goes.
func (s scope) location() string   { return orAll(s.Location, defaultLocation) }
func (s scope) group() string      { return orAll(s.Group, defaultGroup) }
func (s scope) azLocation() string { return orAll(s.AzLocation, defaultAzLocation) }

func orAll(v, def string) string {
	if v == all || v == "" {
		return def
	}
	return v
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
	curs := []string{cur.Account, cur.Region, cur.Project, cur.Subscription, cur.Location, cur.Group, cur.AzLocation}
	reset := r.FormValue("reset") != ""
	for i, name := range scopeNames {
		v := strings.TrimSpace(r.FormValue(name))
		c := &http.Cookie{Name: "pogi-" + name, Value: v, Path: "/_pogi/", SameSite: http.SameSiteLaxMode, Expires: time.Now().AddDate(1, 0, 0)}
		switch {
		case reset:
			c.Value, c.MaxAge, c.Expires = "", -1, time.Time{}
		case !scopeRE.MatchString(v):
			c.Value = curs[i] // missing or bad: keep what it was
		}
		http.SetCookie(w, c)
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
// Location becomes the cloud's own location (Google's or Azure's), and the other clouds' fields match everything.
func (s scope) forCloud(cloud string) scope {
	switch cloud {
	case core.AWS:
		s.Location, s.Group = all, all
	case core.GCP:
		s.Account, s.Region, s.Subscription, s.Group = all, all, all, all
	default:
		s.Account, s.Region, s.Project, s.Location = all, all, all, s.AzLocation
		s.AzLocation = all
	}
	return s
}

// keeps is allows for a row of table t: a global table ignores the region.
func (s scope) keeps(t core.Table, row []string) bool {
	if t.Global {
		s.Region = all
	}
	return s.allows(t.Columns, row)
}

func (s scope) allows(cols, row []string) bool {
	d, loc := dimsOf(cols, row), strings.ToLower(s.Location)
	return match(s.Account, d.account) && match(s.Region, d.region) && match(s.Project, d.project) && match(s.Subscription, d.sub) &&
		match(s.Group, d.group) && (d.loc == "global" || match(loc, d.loc) || strings.HasPrefix(d.loc, loc+"-"))
}

// dims are the places a row says it lives in; "" is not said.
type dims struct{ account, region, project, sub, group, loc string }

func dimsOf(cols, row []string) (d dims) {
	for i, c := range cols {
		if i >= len(row) {
			break
		}
		switch strings.ToLower(c) {
		case "account":
			d.account = row[i]
		case "region":
			d.region = row[i]
		case "project":
			d.project = row[i]
		case "subscription":
			d.sub = row[i]
		case "resource group":
			d.group = row[i]
		case "location":
			d.loc = row[i]
		}
	}
	if len(row) > 0 {
		first := row[0]
		p := strings.Split(first, "/")
		switch {
		case strings.HasPrefix(first, "arn:"):
			_, r, a, _, _ := core.ParseARN(first)
			d.region, d.account = orElse(d.region, r), orElse(d.account, a)
		case strings.HasPrefix(first, "projects/"):
			d.project = orElse(d.project, p[1])
			if len(p) > 3 && p[2] == "locations" {
				d.loc = orElse(d.loc, p[3])
			}
		case strings.HasPrefix(first, "/subscriptions/"):
			d.sub = orElse(d.sub, p[2])
			if len(p) > 4 && strings.EqualFold(p[3], "resourceGroups") {
				d.group = orElse(d.group, p[4])
			}
		case d.account == "" && len(first) > 13 && first[12] == '/' && strings.Trim(first[:12], "0123456789") == "":
			q := strings.SplitN(first, "/", 3) // the generic engine's account/region/key
			d.account, d.region = q[0], orElse(d.region, q[1])
		}
	}
	d.loc = strings.ToLower(d.loc) // Cloud Storage says US-CENTRAL1 where other services say us-central1
	return d
}

func match(want, have string) bool { return want == all || have == "" || want == have }

// A zone (us-central1-a) is in its region: the location test above also keeps a row whose location starts with the picked one and "-".

func orElse(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func show(v, what string) string {
	if v == all {
		return "all " + what
	}
	return v
}

// subName is the name of an Azure subscription, or its ID when it has none.
func (ui *UI) subName(id string) string {
	if id == all {
		return "all subscriptions"
	}
	if b, ok := ui.hub.Service("arm").(core.Browsable); ok {
		for _, t := range b.Resources() {
			if t.Name == "Subscriptions" {
				for _, row := range t.Rows {
					if len(row) > 1 && row[0] == id && row[1] != "" {
						return row[1]
					}
				}
			}
		}
	}
	return id
}

// text is the scope of one cloud, as the form drawer and the scope bar say it.
func (s scope) text(cloud string) string {
	switch cloud {
	case core.GCP:
		return show(s.Project, "projects") + " · " + show(s.Location, "locations")
	case core.Azure:
		return show(s.Group, "groups") + " · " + show(s.AzLocation, "locations")
	}
	return show(s.Account, "accounts") + " / " + show(s.Region, "regions")
}

// target is where a create of one cloud goes, as a form drawer says it: "*" falls back to the default.
func (s scope) target(cloud, region string) string {
	switch cloud {
	case core.GCP:
		return s.project() + " · " + s.location()
	case core.Azure:
		return s.group() + " · " + s.azLocation()
	}
	return s.acct() + " / " + s.region(region)
}

// pill is the header pill: the full scope of the picked cloud, or "All clouds" and how many values are not the default.
func (ui *UI) pill(s scope, cloud string) string {
	switch cloud {
	case core.AWS:
		return "AWS · " + show(s.Account, "accounts") + " · " + show(s.Region, "regions")
	case core.GCP:
		return "Google Cloud · " + s.text(core.GCP)
	case core.Azure:
		return "Azure · " + show(ui.subName(s.Subscription), "subscriptions") + " · " + s.text(core.Azure)
	}
	if n := ui.filters(s); n > 0 {
		return fmt.Sprintf("All clouds · %d %s", n, map[bool]string{true: "filter", false: "filters"}[n == 1])
	}
	return "All clouds"
}

type scopeOpt struct{ Value, Label string }

// scopeValues is every value pogi holds for the account center's lists, each with how many resources use it.
// The default and the current value are always offered, and "*" for all.
func (ui *UI) scopeValues(w http.ResponseWriter, r *http.Request) {
	cur, def := ui.scope(r), ui.defaults()
	n := map[string]map[string]int{}
	add := func(k, v string) {
		if v == "" {
			return
		}
		if n[k] == nil {
			n[k] = map[string]int{}
		}
		n[k][v]++
	}
	for _, svc := range ui.hub.Services() {
		b, ok := svc.(core.Browsable)
		if !ok {
			continue
		}
		cloud := svc.Info().Cloud
		for _, t := range b.Resources() {
			if t.Log {
				continue
			}
			for _, row := range t.Rows {
				d := dimsOf(t.Columns, row)
				switch cloud {
				case core.AWS:
					add("account", d.account)
				case core.GCP:
					add("project", d.project)
					add("location", d.loc)
				default:
					add("subscription", d.sub)
					add("azlocation", d.loc)
					if t.Name == "Resource groups" && len(row) > 0 {
						add("group", row[0])
					} else {
						add("group", d.group)
					}
				}
			}
		}
	}
	out := map[string][]scopeOpt{}
	for k, want := range map[string][2]string{"account": {cur.Account, def.Account}, "project": {cur.Project, def.Project}, "subscription": {cur.Subscription, def.Subscription},
		"location": {cur.Location, defaultLocation}, "group": {cur.Group, defaultGroup}, "azlocation": {cur.AzLocation, defaultAzLocation}} {
		if n[k] == nil {
			n[k] = map[string]int{}
		}
		for _, v := range want {
			if v != all {
				n[k][v] += 0 // offered even when nothing uses it
			}
		}
		opts := []scopeOpt{{all, "* (all)"}}
		for _, v := range core.SortedKeys(n[k]) {
			if !scopeRE.MatchString(v) {
				continue
			}
			l := plural(n[k][v], "resource")
			if k == "subscription" {
				l = ui.subName(v) + " · " + l
			}
			opts = append(opts, scopeOpt{v, l})
		}
		out[k] = opts
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
