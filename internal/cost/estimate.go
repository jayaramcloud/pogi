package cost

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/terraform"
)

// HoursPerMonth is the month every price uses: 730 hours, as the clouds' calculators do.
const HoursPerMonth = 730

// CloudName is how a cloud is written in notes and errors.
var CloudName = map[string]string{"aws": "AWS", "gcp": "Google Cloud", "azure": "Azure"}

// DefaultRegion is the region assumed when the files don't say one.
var DefaultRegion = map[string]string{"aws": "us-east-1", "gcp": "us-central1", "azure": "eastus"}

// Line is one charge of a resource for a month.
type Line struct {
	What      string  `json:"what"` // "Instance hours (m7g.large, Linux)"
	Qty       float64 `json:"qty"`
	Unit      string  `json:"unit"` // the price's unit: "hour", "GB-month", "1M requests"
	UnitPrice float64 `json:"unitPrice"`
	Monthly   float64 `json:"monthly"`
	Usage     bool    `json:"usage,omitempty"` // priced by use; Qty is 0 when no usage is known
}

// Row statuses, from the most to the least certain.
const (
	Known    = "known"    // every input came from the files
	Assumed  = "assumed"  // a default was used; Notes say which
	Usage    = "usage"    // only usage charges, and no usage is known
	Needs    = "needs"    // a value pogi needs is unknown; Notes name it
	Free     = "free"     // the resource costs nothing
	Unpriced = "unpriced" // pogi has no price mapping for this type
)

// Row is the estimate for one resource.
type Row struct {
	Address string `json:"address"`
	Block   string `json:"block"`
	Module  string `json:"module,omitempty"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Cloud   string `json:"cloud,omitempty"`
	Region  string `json:"region,omitempty"`
	Service string `json:"service"`
	File    string `json:"file,omitempty"`
	Line    int    `json:"line,omitempty"`

	Action  string  `json:"action,omitempty"` // from a plan: create, update, replace, delete, no-op
	Monthly float64 `json:"monthly"`
	Before  float64 `json:"before"` // from a plan: the monthly cost before the change

	Lines     []Line   `json:"lines,omitempty"`
	Status    string   `json:"status"`
	Notes     []string `json:"notes,omitempty"`
	UsageKeys []string `json:"usageKeys,omitempty"` // the usage-file keys this row reads
	Sizes     *SizeRef `json:"sizes,omitempty"`     // set when another size can be picked
}

// SizeRef says which size list a row's size comes from.
type SizeRef struct {
	Column  string `json:"column"`  // "aws/ec2/linux"
	Attr    string `json:"attr"`    // the attribute that sets it: "instance_type"
	Current string `json:"current"` // the size priced (the what-if pick, if any)
	File    string `json:"file"`    // the size the files say
}

// Estimate is the priced Config.
type Estimate struct {
	Source    string             `json:"source"` // "folder" or "plan"
	Version   string             `json:"terraformVersion,omitempty"`
	Published map[string]string  `json:"pricesPublished"`
	Rows      []Row              `json:"rows"`
	Total     float64            `json:"monthly"`
	Before    float64            `json:"before"` // from a plan only
	Clouds    map[string]float64 `json:"clouds"`
	Warnings  []string           `json:"warnings,omitempty"`
}

// Options change how Price works.
type Options struct {
	DB    *DB                       // nil means Default()
	Usage map[string]map[string]any // the usage file: resource address → usage keys
	Sizes map[string]string         // a what-if: resource address → size to price instead
}

// mapping prices one resource type.
type mapping struct {
	service string // shown in the Service column: "EC2", "Cloud SQL", "Virtual Machines"
	price   func(r *res)
	sizes   string // the size list's column, with %s for the OS ("aws/ec2/%s"); "" when there is no size picker
}

// SizeColumn returns the column that lists the sizes of a resource type, for an OS ("linux" or
// "windows"; types with one price list ignore it).
func SizeColumn(typ, os string) (string, bool) {
	m := mappings[typ]
	if m.sizes == "" {
		return "", false
	}
	if strings.Contains(m.sizes, "%s") {
		return fmt.Sprintf(m.sizes, cmp.Or(os, "linux")), true
	}
	return m.sizes, true
}

// SizeTypes lists the resource types that have a size list, sorted.
func SizeTypes() []string {
	var out []string
	for t, m := range mappings {
		if m.sizes != "" {
			out = append(out, t)
		}
	}
	slices.Sort(out)
	return out
}

// mappings holds the priced types; each cloud's file adds its own from init.
var mappings = map[string]mapping{}

// free lists types that cost nothing on their own (VPCs, subnets, IAM, security groups).
var free = map[string]bool{}

// Price works out the monthly cost of each resource in cfg.
func Price(cfg *terraform.Config, opt Options) (*Estimate, error) {
	db := opt.DB
	if db == nil {
		var err error
		if db, err = Default(); err != nil {
			return nil, err
		}
	}
	// byBlock is every resource as the plan leaves it; before is every resource as it was, for
	// pricing the "before" side (a group then reads its launch template's old instance type).
	byBlock := make(map[string]*terraform.Resource, len(cfg.Resources))
	before := make(map[string]*terraform.Resource, len(cfg.Resources))
	for _, tr := range cfg.Resources {
		if byBlock[tr.Block] == nil && tr.Action != "delete" {
			byBlock[tr.Block] = tr
		}
		if before[tr.Block] != nil || tr.Action == "create" {
			continue
		}
		if tr.Before != nil {
			old := *tr
			old.Attrs = tr.Before
			before[tr.Block] = &old
		} else {
			before[tr.Block] = tr
		}
	}
	e := &Estimate{Source: cfg.Source, Version: cfg.Version, Published: db.Published, Clouds: map[string]float64{}, Warnings: cfg.Warnings}
	for _, tr := range cfg.Resources {
		if tr.Data || tr.Action == "read" {
			continue
		}
		var row Row
		switch {
		case tr.Action == "delete": // its Attrs are what it was; it costs nothing after
			row = priceOne(db, tr, tr.Attrs, opt.Usage[tr.Address], "", before)
			row.Before, row.Monthly = row.Monthly, 0
		case tr.Before != nil:
			row = priceOne(db, tr, tr.Attrs, opt.Usage[tr.Address], opt.Sizes[tr.Address], byBlock)
			row.Before = priceOne(db, tr, tr.Before, opt.Usage[tr.Address], "", before).Monthly
		default:
			row = priceOne(db, tr, tr.Attrs, opt.Usage[tr.Address], opt.Sizes[tr.Address], byBlock)
			if tr.Action == "no-op" { // unchanged itself, but what it reads may change: a group's template
				row.Before = priceOne(db, tr, tr.Attrs, opt.Usage[tr.Address], "", before).Monthly
			}
		}
		e.Total += row.Monthly
		e.Before += row.Before
		if row.Cloud != "" {
			e.Clouds[row.Cloud] += row.Monthly
		}
		e.Rows = append(e.Rows, row)
	}
	return e, nil
}

// Unsure lists the resources a plan changes (creates, updates, replaces or deletes) whose cost
// pogi can't fully work out. The plan's change in cost leaves them out, so it may be off by them.
func (e *Estimate) Unsure() []string {
	var out []string
	for _, r := range e.Rows {
		changed := r.Action != "" && r.Action != "no-op"
		if changed && (r.Status == Needs || r.Status == Unpriced) {
			out = append(out, r.Address)
		}
	}
	return out
}

func priceOne(db *DB, tr *terraform.Resource, attrs map[string]any, usage map[string]any, pick string, byBlock map[string]*terraform.Resource) Row {
	row := Row{Address: tr.Address, Block: tr.Block, Module: tr.Module, Type: tr.Type, Name: tr.Name, Cloud: tr.Cloud,
		Region: tr.Region, File: tr.File, Line: tr.Line, Action: tr.Action}
	m, ok := mappings[tr.Type]
	row.Service = cmp.Or(m.service, serviceOf(tr.Type))
	switch {
	case tr.Cloud == "":
		row.Status, row.Notes = Free, []string{"not a cloud resource"}
		return row
	case free[tr.Type]:
		row.Status = Free
		return row
	case !ok:
		row.Status = Unpriced
		return row
	case db.Published[tr.Cloud] == "": // this build carries no prices for the cloud at all
		row.Status, row.Notes = Unpriced, []string{"pogi has no " + CloudName[tr.Cloud] + " prices yet"}
		return row
	}
	r := &res{Resource: tr, attrs: attrs, db: db, usageIn: usage, pick: pick, region: tr.Region, byBlock: byBlock}
	if r.region == "" {
		r.region = DefaultRegion[tr.Cloud]
		r.assume("assumed region " + r.region + " (the files don't say one)")
	}
	m.price(r)
	row.Region = r.region // a mapping may price elsewhere: a multi-region bucket under "us"
	row.Lines, row.Notes, row.UsageKeys, row.Sizes = r.lines, r.notes, r.usageKeys, r.sizes
	usageOnly := len(r.lines) > 0
	for _, l := range r.lines {
		row.Monthly += l.Monthly
		usageOnly = usageOnly && l.Usage && l.Qty == 0
	}
	switch {
	case r.needs:
		row.Status = Needs
	case usageOnly:
		row.Status = Usage
	case r.assumed:
		row.Status = Assumed
	default:
		row.Status = Known
	}
	return row
}

// serviceOf names a type's service when it has no mapping: aws_glue_job → glue.
func serviceOf(typ string) string {
	_, rest, _ := strings.Cut(typ, "_")
	svc, _, _ := strings.Cut(rest, "_")
	return svc
}

// res is the resource being priced. Mappings read attributes and add lines through its methods.
type res struct {
	*terraform.Resource
	attrs   map[string]any
	db      *DB
	region  string
	usageIn map[string]any
	pick    string                         // a what-if size
	byBlock map[string]*terraform.Resource // every resource by Block, for dep

	lines     []Line
	notes     []string
	usageKeys []string
	sizes     *SizeRef
	assumed   bool
	needs     bool
}

// dep returns the first resource of one of the given types that this resource references (its
// Deps), or nil. It lets an autoscaling group read its launch template, or a service its task
// definition.
func (r *res) dep(types ...string) *terraform.Resource {
	for _, d := range r.Deps {
		if t := r.byBlock[d]; t != nil && slices.Contains(types, t.Type) {
			return t
		}
	}
	return nil
}

// get returns the value at a dotted path: "root_block_device.volume_size" or "node_pool.1.count".
// A name segment applied to a list takes its first element, since most nested blocks hold one.
func (r *res) get(path string) (any, bool) {
	var v any = r.attrs
	for _, seg := range strings.Split(path, ".") {
		if l, ok := v.([]any); ok {
			i, err := strconv.Atoi(seg)
			if err != nil {
				if len(l) == 0 {
					return nil, false
				}
				v = l[0]
			} else {
				if i < 0 || i >= len(l) {
					return nil, false
				}
				v = l[i]
				continue
			}
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if v, ok = m[seg]; !ok {
			return nil, false
		}
	}
	return v, v != nil
}

// value returns the attribute at path. When it isn't set, it returns def: the provider's
// documented default, which is not an assumption. When it is set but unknown, it returns def and
// notes the assumption; with no def, the row needs that value.
func (r *res) value(path string, def any) (any, bool) {
	v, ok := r.get(path)
	if !ok {
		return def, def != nil
	}
	if _, unknown := v.(terraform.Unknown); unknown {
		if def == nil {
			r.need(path)
			return nil, false
		}
		r.assume(fmt.Sprintf("assumed %s = %v (its value isn't known before apply)", path, def))
		return def, true
	}
	return v, true
}

// str returns a string attribute, or def (see value). A non-string def of "" means none.
func (r *res) str(path, def string) string {
	var d any
	if def != "" {
		d = def
	}
	v, ok := r.value(path, d)
	if !ok {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return def
}

// num returns a number attribute, or def (see value). Terraform turns "20" into 20, so does num.
func (r *res) num(path string, def float64) float64 {
	v, ok := r.value(path, def)
	if !ok {
		return def
	}
	switch v := v.(type) {
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	case bool:
		if v {
			return 1
		}
	}
	return def
}

// flag returns a bool attribute, or def.
func (r *res) flag(path string, def bool) bool {
	v, ok := r.value(path, def)
	if !ok {
		return def
	}
	switch v := v.(type) {
	case bool:
		return v
	case string:
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

// assume marks the row as resting on a default and says which.
func (r *res) assume(note string) {
	r.assumed = true
	if !slices.Contains(r.notes, note) {
		r.notes = append(r.notes, note)
	}
}

// need marks the row as missing a value it can't be priced without.
func (r *res) need(path string) {
	r.needs = true
	r.notes = append(r.notes, "needs a value for "+path)
}

// miss marks the row as unpriceable for a reason other than a missing attribute.
func (r *res) miss(why string) {
	r.needs = true
	r.notes = append(r.notes, why)
}

// size returns the size to price for a resource whose size comes from attr, and offers the size
// picker for column. A what-if pick wins over the files.
func (r *res) size(column, attr, def string) string {
	file := r.str(attr, def)
	cur := cmp.Or(r.pick, file)
	if cur == "" {
		if !r.needs {
			r.need(attr)
		}
		return ""
	}
	r.sizes = &SizeRef{Column: column, Attr: attr, Current: cur, File: file}
	return cur
}

// hourly adds a line for qty units (usually hours) of a size from a column.
func (r *res) hourly(column, size string, qty float64, what string) {
	if size == "" {
		return
	}
	p, ok := r.db.Hourly(column, r.region, size)
	if !ok {
		r.miss(fmt.Sprintf("no price for %s in %s", size, r.region))
		return
	}
	r.lines = append(r.lines, Line{What: what, Qty: qty, Unit: r.db.Columns[column].Unit, UnitPrice: p, Monthly: qty * p})
}

// rate adds a line for qty units of a rate.
func (r *res) rate(key string, qty float64, what string) {
	p, unit, ok := r.db.Rate(key, r.region)
	if !ok {
		r.miss(fmt.Sprintf("no price for %s in %s", what, r.region))
		return
	}
	r.lines = append(r.lines, Line{What: what, Qty: qty, Unit: unit, UnitPrice: p, Monthly: qty * p})
}

// usage adds a line priced by use. The quantity is the usage file's value for key times scale
// (to turn, say, a request count into the rate's "1M requests"), or 0 when the file doesn't
// give one.
func (r *res) usage(key, rateKey string, scale float64, what string) {
	if !slices.Contains(r.usageKeys, key) {
		r.usageKeys = append(r.usageKeys, key)
	}
	p, unit, ok := r.db.Rate(rateKey, r.region)
	if !ok {
		r.miss(fmt.Sprintf("no price for %s in %s", what, r.region))
		return
	}
	q := toNum(r.usageIn[key]) * scale
	r.lines = append(r.lines, Line{What: what, Qty: q, Unit: unit, UnitPrice: p, Monthly: q * p, Usage: true})
}

// usageStr returns a string from the usage file, such as os: windows.
func (r *res) usageStr(key, def string) string {
	if !slices.Contains(r.usageKeys, key) {
		r.usageKeys = append(r.usageKeys, key)
	}
	if s, ok := r.usageIn[key].(string); ok && s != "" {
		return strings.ToLower(s)
	}
	return def
}

func toNum(v any) float64 {
	switch v := v.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	}
	return 0
}
