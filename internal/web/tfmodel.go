package web

import (
	"cmp"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/cost"
	"github.com/nrndev/pogi/internal/terraform"
)

// tfData is what the "result" fragment shows.
type tfData struct {
	Plan                 bool
	Total, Before, Delta float64
	Clouds               []tfCloud
	Prices               []string // where each cloud's prices come from
	Warnings             []string
	Groups               []tfGroup
	Free, Unpriced       []tfRow
	Rows                 []tfRow // every row, for the drawers
	Vars                 []tfVar
	Group, View, Layout  string
	State                string // the view state as JSON: the page sends it back with a change
	WhatIfs              int
	Empty                bool
	Summary              string   // the change view's one-line summary
	Unsure               []string // changed resources pogi can't price: the plan's change leaves them out
	Nodes, Edges, Flows  int
	graph                *tfGraph
	SVG                  template.HTML
}

type tfCloud struct {
	ID, Name string
	Monthly  float64
}

type tfGroup struct {
	Name            string
	Monthly, Before float64
	Rows            []tfRow
}

type tfRow struct {
	cost.Row
	N        int // the row's place in the estimate: its drawer is "tfr-N"
	Drives   string
	WhatIf   bool
	SizesURL string
	Delta    float64 // plan: after minus before
}

type tfVar struct {
	Path string
	On   bool
}

var tfFuncs = template.FuncMap{
	"usd": tfUSD,
	"sgn": func(v float64) string {
		if v < 0 {
			return "−" + tfUSD(-v)
		}
		return "+" + tfUSD(v)
	},
	"plural": plural,
	"fmtn":   func(v float64) string { return tfNum(v) },
}

// tfUSD is a dollar amount with cents and thousands: $1,234.56.
func tfUSD(v float64) string {
	ip, fp, _ := strings.Cut(strconv.FormatFloat(math.Abs(v), 'f', 2, 64), ".")
	for i := len(ip) - 3; i > 0; i -= 3 {
		ip = ip[:i] + "," + ip[i:]
	}
	return map[bool]string{true: "-", false: ""}[v < 0 && v != 0] + "$" + ip + "." + fp
}

// tfNum trims a quantity or a unit price to what is worth reading: 730, 0.0832.
func tfNum(v float64) string {
	return strconv.FormatFloat(math.Round(v*1e6)/1e6, 'f', -1, 64)
}

// tfDrives is the main charge of a row, as a short line that leads with what it is, since a narrow
// column cuts the end: "Instance hours (m7g.large, Linux) · 730 h × $0.0832".
func tfDrives(r cost.Row) string {
	var best *cost.Line
	for i := range r.Lines {
		if best == nil || r.Lines[i].Monthly > best.Monthly {
			best = &r.Lines[i]
		}
	}
	switch {
	case best == nil:
		return cmp.Or(strings.Join(r.Notes, "; "), map[string]string{cost.Free: "free", cost.Unpriced: "pogi has no price for this type"}[r.Status])
	case best.Usage && best.Qty == 0:
		return best.What + ": usage not known"
	}
	unit := cmp.Or(map[string]string{"hour": "h"}[best.Unit], best.Unit)
	return fmt.Sprintf("%s · %s %s × $%s", best.What, tfNum(best.Qty), unit, tfNum(best.UnitPrice))
}

var tfCloudNames = []tfCloud{{"aws", "AWS", 0}, {"gcp", "Google Cloud", 0}, {"azure", "Azure", 0}}

// tfBuild turns a priced Config into what the page shows. src says where each cloud's prices come from.
func tfBuild(cfg *terraform.Config, est *cost.Estimate, in *tfIn, src map[string]string) *tfData {
	d := &tfData{Plan: est.Source == "plan", Total: est.Total, Before: est.Before, Delta: est.Total - est.Before, Warnings: est.Warnings,
		Group: in.Group, View: in.View, Layout: in.Layout, Empty: len(est.Rows) == 0}
	st, _ := json.Marshal(map[string]any{"vars": append([]string{}, in.Vars...), "sizes": in.Sizes, "expanded": append([]string{}, in.Expanded...), "group": in.Group, "layout": in.Layout})
	d.State = string(st)
	groups := map[string]*tfGroup{}
	for i, r := range est.Rows {
		row := tfRow{Row: r, N: i, Drives: tfDrives(r), Delta: r.Monthly - r.Before}
		if r.Sizes != nil {
			row.SizesURL = tfSizesURL(r)
			row.WhatIf = in.Sizes[r.Address] != "" && r.Sizes.Current != r.Sizes.File
			if row.WhatIf {
				d.WhatIfs++
			}
		}
		d.Rows = append(d.Rows, row)
		switch r.Status {
		case cost.Free:
			d.Free = append(d.Free, row)
			continue
		case cost.Unpriced:
			d.Unpriced = append(d.Unpriced, row)
			continue
		}
		key := ""
		switch in.Group {
		case "module":
			key = cmp.Or(r.Module, "root module")
		case "service":
			key = cmp.Or(r.Service, "other")
		default:
			key = cmp.Or(cloudNames[r.Cloud], "Other")
		}
		g := groups[key]
		if g == nil {
			g = &tfGroup{Name: key}
			groups[key] = g
		}
		g.Rows = append(g.Rows, row)
		g.Monthly += r.Monthly
		g.Before += r.Before
	}
	for _, g := range groups {
		slices.SortStableFunc(g.Rows, func(a, b tfRow) int {
			return cmp.Or(cmp.Compare(b.Monthly, a.Monthly), cmp.Compare(a.Address, b.Address))
		})
		d.Groups = append(d.Groups, *g)
	}
	slices.SortFunc(d.Groups, func(a, b tfGroup) int { return cmp.Or(cmp.Compare(b.Monthly, a.Monthly), cmp.Compare(a.Name, b.Name)) })
	for _, c := range tfCloudNames {
		if _, ok := est.Clouds[c.ID]; ok {
			c.Monthly = est.Clouds[c.ID]
			d.Clouds = append(d.Clouds, c)
			if est.Published[c.ID] != "" {
				d.Prices = append(d.Prices, fmt.Sprintf("%s: %s, fetched %s", c.Name, cmp.Or(src[c.ID], "the provider's price list"), est.Published[c.ID]))
			}
		}
	}
	for name := range in.files {
		l := strings.ToLower(name)
		base := l[strings.LastIndex(l, "/")+1:]
		if (strings.HasSuffix(l, ".tfvars") || strings.HasSuffix(l, ".tfvars.json")) && !strings.HasPrefix(base, "terraform.tfvars") && !strings.Contains(base, ".auto.tfvars") {
			d.Vars = append(d.Vars, tfVar{Path: name, On: slices.Contains(in.Vars, name)})
		}
	}
	slices.SortFunc(d.Vars, func(a, b tfVar) int { return strings.Compare(a.Path, b.Path) })
	d.Unsure = est.Unsure()
	d.graph = buildTFGraphBy(cfg, est, in.Expanded, in.Layout == "network")
	d.graph.markLive(cfg, in.spans, time.Now())
	d.Flows = len(d.graph.Lines)
	d.SVG, d.Nodes, d.Edges, d.Summary = template.HTML(d.graph.svg(false)), d.graph.boxes(), d.graph.edges(), d.graph.summary()
	return d
}

// tfMarkdown is the estimate as a table with totals, for a pull request.
func tfMarkdown(est *cost.Estimate, src map[string]string) string {
	cell := func(s string) string { return strings.NewReplacer("|", `\|`, "\n", " ").Replace(s) }
	code := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "'") + "`" }
	var b strings.Builder
	plan := est.Source == "plan"
	b.WriteString("## Terraform cost estimate\n\n")
	fmt.Fprintf(&b, "**%s a month**", tfUSD(est.Total))
	if plan {
		fmt.Fprintf(&b, " after the plan (%s before, a change of %s)", tfUSD(est.Before), map[bool]string{true: "+", false: ""}[est.Total >= est.Before]+tfUSD(est.Total-est.Before))
	}
	if u := est.Unsure(); plan && len(u) > 0 {
		fmt.Fprintf(&b, "\n\nNot counting %s pogi can't price: %s.", plural(len(u), "changed resource"), strings.Join(u, ", "))
	}
	b.WriteString("\n\n")
	head, sep := "| Resource | Service | Region | Monthly | What drives it |", "|---|---|---|---:|---|"
	if plan {
		head, sep = "| Resource | Action | Service | Region | Before | After | What drives it |", "|---|---|---|---|---:|---:|---|"
	}
	b.WriteString(head + "\n" + sep + "\n")
	rows := slices.Clone(est.Rows)
	slices.SortStableFunc(rows, func(a, b cost.Row) int {
		return cmp.Or(cmp.Compare(b.Monthly, a.Monthly), cmp.Compare(a.Address, b.Address))
	})
	var free, unpriced []string
	for _, r := range rows {
		switch r.Status {
		case cost.Free:
			free = append(free, code(r.Address))
			continue
		case cost.Unpriced:
			unpriced = append(unpriced, code(r.Address))
			continue
		}
		note := tfDrives(r)
		if r.Status != cost.Known {
			note += " (" + r.Status + ")"
		}
		if plan {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", code(r.Address), cell(r.Action), cell(r.Service), cell(r.Region), tfUSD(r.Before), tfUSD(r.Monthly), cell(note))
		} else {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", code(r.Address), cell(r.Service), cell(r.Region), tfUSD(r.Monthly), cell(note))
		}
	}
	if len(free) > 0 {
		fmt.Fprintf(&b, "\nFree: %s.\n", strings.Join(free, ", "))
	}
	if len(unpriced) > 0 {
		fmt.Fprintf(&b, "\nNot priced (the real bill could be higher): %s.\n", strings.Join(unpriced, ", "))
	}
	for _, w := range est.Warnings {
		fmt.Fprintf(&b, "\n> %s\n", cell(w))
	}
	b.WriteString("\nOn-demand list prices, 730 hours a month, USD; no free tier or discounts.")
	for _, c := range tfCloudNames {
		if est.Published[c.ID] != "" {
			fmt.Fprintf(&b, " %s prices: %s, fetched %s.", c.Name, cmp.Or(src[c.ID], "the provider's price list"), est.Published[c.ID])
		}
	}
	b.WriteString("\n")
	return b.String()
}
