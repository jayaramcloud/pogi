package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/cost"
	tfmodel "github.com/nrndev/pogi/internal/terraform"
)

// strList is a repeatable string flag.
type strList []string

func (l *strList) String() string     { return strings.Join(*l, ",") }
func (l *strList) Set(v string) error { *l = append(*l, v); return nil }

// costCmd is `pogi cost`: price a Terraform folder or plan, list sizes, or show what is running.
func costCmd(args []string) error {
	if len(args) > 0 && args[0] == "sizes" {
		return costSizes(os.Stdout, args[1:])
	}
	return costRun(os.Stdout, styled(os.Stdout), args)
}

// parseArgs lets flags come before or after the positional arguments.
// Parse errors come back once, as a usageError, so the flag package stays quiet;
// -h prints the usage to stdout and returns flag.ErrHelp, which main treats as success.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	fs.SetOutput(io.Discard)
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				fs.SetOutput(os.Stdout)
				fs.Usage()
				return nil, err
			}
			return nil, usagef("%s: %v (pogi %s -h lists the flags)", fs.Name(), err, fs.Name())
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos, args = append(pos, fs.Arg(0)), fs.Args()[1:]
	}
}

func costRun(w io.Writer, term bool, args []string) error {
	fs := flag.NewFlagSet("cost", flag.ContinueOnError)
	setUsage(fs, "Estimate what a Terraform folder or plan would cost a month; with no path, what runs in pogi now.",
		"pogi cost [folder | plan.json] [flags]\npogi cost [flags]                  (needs a running pogi)\npogi cost sizes <resource type>    (sizes and their prices)",
		"pogi cost ./infra --size aws_instance.web=m7g.large")
	var vars, sizes strList
	fs.Var(&vars, "var-file", "a .tfvars file (repeatable)")
	fs.Var(&sizes, "size", "what-if: address=size (repeatable)")
	usageFile := fs.String("usage", "", "usage file")
	template := fs.Bool("usage-template", false, "print a usage file with every key set to 0")
	format := fs.String("format", "table", "table, json or markdown")
	asJSON := fs.Bool("json", false, "same as --format json")
	region := fs.String("region", "", "region, when no path is given")
	ep := endpointFlag(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if *asJSON {
		*format = "json"
	}
	if !slices.Contains([]string{"table", "json", "markdown"}, *format) {
		return usagef("unknown --format %q (use table, json or markdown)", *format)
	}
	if len(pos) > 1 {
		return usagef("one path at a time, got %d", len(pos))
	}
	if len(pos) == 0 {
		if len(vars)+len(sizes) > 0 || *usageFile != "" || *template {
			return usagef("--var-file, --size, --usage and --usage-template need a path: pogi cost ./infra")
		}
		return costRunning(w, *ep, *region, *format)
	}
	opt := cost.Options{Sizes: map[string]string{}}
	for _, s := range sizes {
		addr, size, ok := strings.Cut(s, "=")
		if !ok || addr == "" || size == "" {
			return usagef("--size %q: write it as address=size, for example aws_instance.web=m7g.large", s)
		}
		opt.Sizes[addr] = size
	}
	if *usageFile != "" {
		f, err := os.Open(*usageFile)
		if err != nil {
			return fmt.Errorf("usage file: %w", err)
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, cost.MaxUsageFile+1))
		if err != nil {
			return fmt.Errorf("usage file: %w", err)
		}
		if opt.Usage, err = cost.ReadUsage(data); err != nil {
			return err
		}
	}
	cfg, err := tfmodel.Load(pos[0], vars)
	if err != nil {
		return err
	}
	e, err := cost.Price(cfg, opt)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, r := range e.Rows {
		known[r.Address] = true
	}
	for _, addr := range slices.Sorted(maps.Keys(opt.Sizes)) {
		if !known[addr] {
			return fmt.Errorf("--size %s: no resource has that address in %s", addr, pos[0])
		}
	}
	for _, addr := range slices.Sorted(maps.Keys(opt.Usage)) {
		if !known[addr] {
			e.Warnings = append(e.Warnings, fmt.Sprintf("usage file: %s matches no resource, so it was ignored", addr))
		}
	}
	if *template {
		_, err := w.Write(cost.UsageTemplate(e))
		return err
	}
	switch *format {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(e)
	case "markdown":
		writeMarkdown(w, e, pos[0])
	default:
		writeTable(w, e, pos[0], term)
	}
	return nil
}

// ---- formatting ----

// money formats dollars with thousands separators: 1234.5 → "$1,234.50".
func money(v float64) string {
	s := strconv.FormatFloat(v, 'f', 2, 64)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, _ := strings.Cut(s, ".")
	for i := len(whole) - 3; i > 0; i -= 3 {
		whole = whole[:i] + "," + whole[i:]
	}
	return map[bool]string{true: "-$", false: "$"}[neg] + whole + "." + frac
}

// price shows a unit price with enough digits for small rates: 0.0832 and 0.2.
func price(v float64) string { return "$" + strconv.FormatFloat(v, 'f', -1, 64) }

// drives says what a row's cost comes from, in one line: "730 h × $0.0832 m7g.large".
func drives(r cost.Row) string {
	if r.Status == cost.Needs { // say what is missing, not the first thing that was assumed
		for _, n := range r.Notes {
			if !strings.HasPrefix(n, "assumed") {
				return n
			}
		}
	}
	if len(r.Lines) == 0 {
		if len(r.Notes) > 0 {
			return r.Notes[0]
		}
		return ""
	}
	l := r.Lines[0]
	unit := map[string]string{"hour": "h"}[l.Unit]
	if unit == "" {
		unit = l.Unit
	}
	if l.Usage && l.Qty == 0 {
		return fmt.Sprintf("%s per %s: %s (no usage given)", price(l.UnitPrice), l.Unit, l.What)
	}
	return fmt.Sprintf("%s %s × %s %s", strconv.FormatFloat(l.Qty, 'f', -1, 64), unit, price(l.UnitPrice), l.What)
}

var actions = map[string]string{"create": "+ create", "update": "~ update", "replace": "-/+ replace", "delete": "− delete"}

var marks = map[string]string{cost.Assumed: "assumed", cost.Usage: "usage", cost.Needs: "needs"}

// split sorts a row into the main table, the free list or the unpriced list.
func split(e *cost.Estimate) (priced []cost.Row, free, unpriced []string) {
	for _, r := range e.Rows {
		switch r.Status {
		case cost.Free:
			free = append(free, r.Address)
		case cost.Unpriced:
			unpriced = append(unpriced, r.Address)
		default:
			priced = append(priced, r)
		}
	}
	return
}

// planLine is the sentence under a plan's table.
func planLine(e *cost.Estimate) string {
	d := e.Total - e.Before
	var line string
	switch {
	case d > 0.005:
		line = fmt.Sprintf("This plan adds %s a month (from %s to %s)", money(d), money(e.Before), money(e.Total))
	case d < -0.005:
		line = fmt.Sprintf("This plan saves %s a month (from %s to %s)", money(-d), money(e.Before), money(e.Total))
	default:
		line = fmt.Sprintf("This plan does not change the monthly cost (%s)", money(e.Total))
	}
	if u := e.Unsure(); len(u) > 0 { // never claim more than pogi priced
		line += fmt.Sprintf(", not counting %d changed %s pogi can't price: %s", len(u), plural("resource", len(u)), strings.Join(u, ", "))
	}
	return line
}

func sourceLine(e *cost.Estimate, path string) string {
	s := e.Source + " " + path
	if e.Version != "" {
		s += " (Terraform " + e.Version + ")"
	}
	return s
}

func priceLines(e *cost.Estimate) []string {
	db, _ := cost.Default()
	var out []string
	for _, c := range slices.Sorted(maps.Keys(e.Published)) {
		if _, used := e.Clouds[c]; !used && len(e.Clouds) > 0 {
			continue // only the clouds this estimate uses
		}
		s := cmp.Or(cost.CloudName[c], c) + " prices of " + e.Published[c]
		if db != nil && db.Source[c] != "" {
			s += ", from " + db.Source[c]
		}
		out = append(out, s)
	}
	return out
}

const basis = "On-demand list prices, 730 h/month, USD; no free tier or discounts."

// grid prints rows with each column padded; right-aligned columns are listed in right.
func grid(w io.Writer, rows [][]string, right ...int) {
	width := make([]int, len(rows[0]))
	for _, r := range rows {
		for i, c := range r {
			width[i] = max(width[i], len([]rune(c)))
		}
	}
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			pad := strings.Repeat(" ", width[i]-len([]rune(c)))
			switch {
			case slices.Contains(right, i):
				b.WriteString(pad + c)
			case i == len(r)-1:
				b.WriteString(c)
			default:
				b.WriteString(c + pad)
			}
			if i < len(r)-1 {
				b.WriteString("  ")
			}
		}
		fmt.Fprintln(w, strings.TrimRight(b.String(), " "))
	}
}

func writeTable(w io.Writer, e *cost.Estimate, path string, term bool) {
	bold := func(s string) string {
		if term {
			return "\x1b[1m" + s + "\x1b[0m"
		}
		return s
	}
	plan := e.Source == "plan"
	fmt.Fprintln(w, bold("Source: "+sourceLine(e, path)))
	for _, l := range priceLines(e) {
		fmt.Fprintln(w, l)
	}
	fmt.Fprintln(w, basis)
	fmt.Fprintln(w)
	priced, free, unpriced := split(e)
	head := []string{"RESOURCE", "SERVICE", "MONTHLY", "WHAT DRIVES IT"}
	right := []int{2}
	if plan {
		head = []string{"ACTION", "RESOURCE", "SERVICE", "BEFORE", "AFTER", "WHAT DRIVES IT"}
		right = []int{3, 4}
	}
	rows := [][]string{head}
	for _, r := range priced {
		note := drives(r)
		if m := marks[r.Status]; m != "" {
			note = strings.TrimSpace(m + "  " + note)
		}
		if plan {
			rows = append(rows, []string{actions[r.Action], r.Address, r.Service, money(r.Before), money(r.Monthly), note})
		} else {
			rows = append(rows, []string{r.Address, r.Service, money(r.Monthly), note})
		}
	}
	if len(priced) > 0 {
		grid(w, rows, right...)
		fmt.Fprintln(w)
	}
	for _, r := range priced {
		for _, n := range r.Notes {
			fmt.Fprintf(w, "  %s: %s\n", r.Address, n)
		}
	}
	if slices.ContainsFunc(priced, func(r cost.Row) bool { return len(r.Notes) > 0 }) {
		fmt.Fprintln(w)
	}
	for _, c := range slices.Sorted(maps.Keys(e.Clouds)) {
		fmt.Fprintf(w, "%-12s %s a month\n", cmp.Or(cost.CloudName[c], c), money(e.Clouds[c]))
	}
	fmt.Fprintln(w, bold(fmt.Sprintf("%-12s %s a month", "Total", money(e.Total))))
	if plan {
		fmt.Fprintln(w, bold(planLine(e)))
	}
	if len(free) > 0 {
		fmt.Fprintf(w, "\nFree: %s\n", strings.Join(free, ", "))
	}
	if len(unpriced) > 0 {
		fmt.Fprintf(w, "\nNot priced: %s\nThese have no price in pogi yet, so the real bill could be higher.\n", strings.Join(unpriced, ", "))
	}
	for _, s := range e.Warnings {
		fmt.Fprintln(w, "warning:", s)
	}
}

// cell makes text safe inside a markdown table cell.
func cell(s string) string { return strings.NewReplacer("|", "\\|", "\n", " ").Replace(s) }

func writeMarkdown(w io.Writer, e *cost.Estimate, path string) {
	plan := e.Source == "plan"
	fmt.Fprintf(w, "### Cost estimate: %s\n\n", cell(sourceLine(e, path)))
	priced, free, unpriced := split(e)
	if plan {
		fmt.Fprintln(w, "| Action | Resource | Service | Before | After | What drives it |\n|---|---|---|--:|--:|---|")
	} else {
		fmt.Fprintln(w, "| Resource | Service | Monthly | What drives it |\n|---|---|--:|---|")
	}
	for _, r := range priced {
		note := drives(r)
		if m := marks[r.Status]; m != "" {
			note = strings.TrimSpace(m + ": " + note)
		}
		if plan {
			fmt.Fprintf(w, "| %s | `%s` | %s | %s | %s | %s |\n", actions[r.Action], cell(r.Address), cell(r.Service), money(r.Before), money(r.Monthly), cell(note))
		} else {
			fmt.Fprintf(w, "| `%s` | %s | %s | %s |\n", cell(r.Address), cell(r.Service), money(r.Monthly), cell(note))
		}
	}
	fmt.Fprintf(w, "\n**Total: %s a month**", money(e.Total))
	for _, c := range slices.Sorted(maps.Keys(e.Clouds)) {
		fmt.Fprintf(w, " · %s %s", cmp.Or(cost.CloudName[c], c), money(e.Clouds[c]))
	}
	fmt.Fprintln(w)
	if plan {
		fmt.Fprintf(w, "\n**%s.**\n", planLine(e))
	}
	for _, r := range priced {
		for _, n := range r.Notes {
			fmt.Fprintf(w, "\n- `%s`: %s", cell(r.Address), cell(n))
		}
	}
	fmt.Fprintln(w)
	if len(free) > 0 {
		fmt.Fprintf(w, "\nFree: %s\n", strings.Join(free, ", "))
	}
	if len(unpriced) > 0 {
		fmt.Fprintf(w, "\nNot priced (the real bill could be higher): %s\n", strings.Join(unpriced, ", "))
	}
	for _, s := range e.Warnings {
		fmt.Fprintf(w, "\n> warning: %s\n", cell(s))
	}
	fmt.Fprintf(w, "\n_%s %s_\n", basis, strings.Join(priceLines(e), "; "))
}

// ---- pogi cost with no path ----

type runningCost struct {
	Region  string  `json:"region"`
	Monthly float64 `json:"monthly"`
	Basis   string  `json:"basis"`
	Rows    []struct {
		Cloud, Service, Name, Detail string
		Monthly                      float64
		Priced                       bool
	} `json:"rows"`
}

func costRunning(w io.Writer, ep, region, format string) error {
	if format == "markdown" {
		return errors.New("--format markdown is for a Terraform folder or plan: pogi cost ./infra --format markdown")
	}
	path := "/_pogi/cost.json"
	if region != "" {
		path += "?region=" + region
	}
	var c runningCost
	if err := api(ep, "GET", path, nil, &c); err != nil {
		return fmt.Errorf("%w\nTo price Terraform files instead, run: pogi cost ./infra", err)
	}
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(c)
	}
	fmt.Fprintf(w, "What is running in pogi, region %s (%s)\n\n", c.Region, c.Basis)
	rows := [][]string{{"CLOUD", "SERVICE", "RESOURCE", "MONTHLY", "WHAT DRIVES IT"}}
	var unpriced []string
	for _, r := range c.Rows {
		if !r.Priced {
			unpriced = append(unpriced, r.Name)
			continue
		}
		rows = append(rows, []string{r.Cloud, r.Service, r.Name, money(r.Monthly), r.Detail})
	}
	if len(rows) > 1 {
		grid(w, rows, 3)
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "Total  %s a month\n", money(c.Monthly))
	if len(unpriced) > 0 {
		fmt.Fprintf(w, "\nNot priced: %s\nThe real bill could be higher.\n", strings.Join(unpriced, ", "))
	}
	return nil
}

// ---- pogi cost sizes ----

func costSizes(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("cost sizes", flag.ContinueOnError)
	setUsage(fs, "List the sizes of a resource type and their prices, cheapest first.\nTypes: "+strings.Join(cost.SizeTypes(), ", "),
		"pogi cost sizes <resource type> [flags]", "pogi cost sizes aws_instance --vcpu 4 --memory 16")
	region := fs.String("region", "", "region (default: the cloud's usual one)")
	osName := fs.String("os", "linux", "linux or windows")
	vcpu := fs.Float64("vcpu", 0, "at least this many vCPUs")
	mem := fs.Float64("memory", 0, "at least this much memory, in GB")
	old := fs.Bool("old", false, "include older generations")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usagef("usage: pogi cost sizes <resource type> [--region r] [--vcpu n] [--memory gb] [--os windows] [--old]\nTypes: %s", strings.Join(cost.SizeTypes(), ", "))
	}
	db, err := cost.Default()
	if err != nil {
		return err
	}
	found, reg, err := cost.ListSizes(db, cost.SizeQuery{Type: pos[0], OS: *osName, Region: *region, VCPU: *vcpu, MemGB: *mem, Old: *old})
	if err != nil {
		return err
	}
	rows := [][]string{{"SIZE", "FAMILY", "VCPU", "MEMORY GB", "GPU", "$/HOUR", "$/MONTH"}}
	for _, s := range found {
		gpu := ""
		if s.GPU > 0 {
			gpu = strings.TrimSpace(fmt.Sprintf("%d %s", s.GPU, s.GPUModel))
		}
		rows = append(rows, []string{s.Name, s.Family, strconv.FormatFloat(s.VCPU, 'f', -1, 64), strconv.FormatFloat(s.MemGB, 'f', -1, 64),
			gpu, price(s.Price), money(s.Price * cost.HoursPerMonth)})
	}
	fmt.Fprintf(w, "%s sizes in %s, cheapest first (on-demand, 730 h/month, USD)\n\n", pos[0], reg)
	grid(w, rows, 2, 3, 5, 6)
	return nil
}
