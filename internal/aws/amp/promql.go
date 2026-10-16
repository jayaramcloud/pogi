package amp

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// lookback is how far back an instant selector looks for a sample (Prometheus's default staleness).
const lookback = 5 * time.Minute

type matcher struct {
	name, op, value string
	re              *regexp.Regexp
}

func (m matcher) match(labels map[string]string) bool {
	v := labels[m.name]
	switch m.op {
	case "=":
		return v == m.value
	case "!=":
		return v != m.value
	case "=~":
		return m.re.MatchString(v)
	}
	return !m.re.MatchString(v) // !~
}

type selector struct {
	matchers []matcher
	rng      time.Duration // 0: an instant vector
}

type call struct { // rate, irate, increase
	fn  string
	sel *selector
}

type agg struct { // sum, avg, min, max, count
	op      string
	by      []string
	without bool
	arg     any
}

var aggOps = map[string]bool{"sum": true, "avg": true, "min": true, "max": true, "count": true}
var rangeFns = map[string]bool{"rate": true, "irate": true, "increase": true}

type parser struct {
	s string
	i int
}

func (p *parser) ws() {
	for p.i < len(p.s) && strings.ContainsRune(" \t\n\r", rune(p.s[p.i])) {
		p.i++
	}
}

func (p *parser) peek() byte {
	p.ws()
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *parser) expect(c byte) error {
	if p.peek() != c {
		return fmt.Errorf("expected %q at position %d", c, p.i)
	}
	p.i++
	return nil
}

func isIdent(c byte, first bool) bool {
	return c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || !first && c >= '0' && c <= '9'
}

func (p *parser) ident() string {
	p.ws()
	start := p.i
	for p.i < len(p.s) && isIdent(p.s[p.i], p.i == start) {
		p.i++
	}
	return p.s[start:p.i]
}

// parse reads one expression of the supported subset.
func parse(q string) (any, error) {
	p := &parser{s: q}
	e, err := p.expr()
	if err != nil {
		return nil, err
	}
	if p.peek() != 0 {
		return nil, fmt.Errorf("unsupported expression: %q is not part of the supported PromQL subset", p.s[p.i:])
	}
	return e, nil
}

func (p *parser) expr() (any, error) {
	save := p.i
	id := p.ident()
	switch {
	case aggOps[id] && (p.peek() == '(' || p.isGrouping()):
		a := &agg{op: id}
		var err error
		if p.isGrouping() {
			if a.by, a.without, err = p.grouping(); err != nil {
				return nil, err
			}
		}
		if err := p.expect('('); err != nil {
			return nil, err
		}
		if a.arg, err = p.expr(); err != nil {
			return nil, err
		}
		if err := p.expect(')'); err != nil {
			return nil, err
		}
		if p.isGrouping() {
			if a.by, a.without, err = p.grouping(); err != nil {
				return nil, err
			}
		}
		return a, nil
	case rangeFns[id] && p.peek() == '(':
		p.i++
		sel, err := p.selector(p.ident())
		if err != nil {
			return nil, err
		}
		if sel.rng == 0 {
			return nil, fmt.Errorf("%s needs a range vector such as metric[5m]", id)
		}
		if err := p.expect(')'); err != nil {
			return nil, err
		}
		return &call{fn: id, sel: sel}, nil
	}
	p.i = save
	return p.selector(p.ident())
}

func (p *parser) isGrouping() bool {
	save := p.i
	id := p.ident()
	p.i = save
	return id == "by" || id == "without"
}

func (p *parser) grouping() (labels []string, without bool, err error) {
	without = p.ident() == "without"
	if err = p.expect('('); err != nil {
		return
	}
	for p.peek() != ')' {
		l := p.ident()
		if l == "" {
			return nil, false, fmt.Errorf("expected a label name at position %d", p.i)
		}
		labels = append(labels, l)
		if p.peek() == ',' {
			p.i++
		}
	}
	p.i++
	return
}

func (p *parser) selector(name string) (*selector, error) {
	sel := &selector{}
	if name != "" {
		sel.matchers = append(sel.matchers, matcher{name: "__name__", op: "=", value: name})
	}
	if p.peek() == '{' {
		p.i++
		for p.peek() != '}' {
			l := p.ident()
			p.ws()
			op := ""
			for _, o := range []string{"=~", "!~", "!=", "="} {
				if strings.HasPrefix(p.s[p.i:], o) {
					op = o
					break
				}
			}
			if l == "" || op == "" {
				return nil, fmt.Errorf("bad label matcher at position %d", p.i)
			}
			p.i += len(op)
			v, err := p.str()
			if err != nil {
				return nil, err
			}
			m := matcher{name: l, op: op, value: v}
			if op == "=~" || op == "!~" {
				if m.re, err = regexp.Compile("^(?:" + v + ")$"); err != nil {
					return nil, err
				}
			}
			sel.matchers = append(sel.matchers, m)
			if p.peek() == ',' {
				p.i++
			}
		}
		p.i++
	}
	if len(sel.matchers) == 0 {
		return nil, fmt.Errorf("a selector needs a metric name or a label matcher")
	}
	if p.peek() == '[' {
		p.i++
		end := strings.IndexByte(p.s[p.i:], ']')
		if end < 0 {
			return nil, fmt.Errorf("unterminated range")
		}
		d, err := parseDuration(strings.TrimSpace(p.s[p.i : p.i+end]))
		if err != nil {
			return nil, err
		}
		sel.rng, p.i = d, p.i+end+1
	}
	return sel, nil
}

func (p *parser) str() (string, error) {
	p.ws()
	if p.i >= len(p.s) || !strings.ContainsRune("\"'`", rune(p.s[p.i])) {
		return "", fmt.Errorf("expected a string at position %d", p.i)
	}
	q := p.s[p.i]
	j := p.i + 1
	for j < len(p.s) && p.s[j] != q {
		if p.s[j] == '\\' && q != '`' {
			j++
		}
		j++
	}
	if j >= len(p.s) {
		return "", fmt.Errorf("unterminated string")
	}
	lit := p.s[p.i : j+1]
	p.i = j + 1
	if q == '\'' {
		lit = `"` + strings.ReplaceAll(lit[1:len(lit)-1], `"`, `\"`) + `"`
	}
	return strconv.Unquote(lit)
}

var durRe = regexp.MustCompile(`^(\d+)(ms|s|m|h|d|w|y)`)

func parseDuration(s string) (time.Duration, error) {
	var total time.Duration
	for rest := s; rest != ""; {
		m := durRe.FindStringSubmatch(rest)
		if m == nil {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		n, _ := strconv.Atoi(m[1])
		unit := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour,
			"d": 24 * time.Hour, "w": 7 * 24 * time.Hour, "y": 365 * 24 * time.Hour}[m[2]]
		total += time.Duration(n) * unit
		rest = rest[len(m[0]):]
	}
	if total == 0 {
		return 0, fmt.Errorf("bad duration %q", s)
	}
	return total, nil
}

// Point is a value at a time in milliseconds.
type Point struct {
	T int64
	V float64
}

// Result is one series of an answer: a single point for a vector, many for a matrix.
type Result struct {
	Metric map[string]string
	Points []Point
}

func (sel *selector) series(all []*Series) []*Series {
	var out []*Series
next:
	for _, s := range all {
		for _, m := range sel.matchers {
			if !m.match(s.Labels) {
				continue next
			}
		}
		out = append(out, s)
	}
	return out
}

// eval answers an expression at one time (ms). A range selector alone answers with all its points.
func eval(e any, all []*Series, t int64) ([]Result, error) {
	switch x := e.(type) {
	case *selector:
		var out []Result
		for _, s := range x.series(all) {
			if x.rng > 0 {
				if pts := window(s, t, x.rng); len(pts) > 0 {
					out = append(out, Result{s.Labels, pts})
				}
			} else if pts := window(s, t, lookback); len(pts) > 0 {
				out = append(out, Result{s.Labels, pts[len(pts)-1:]})
			}
		}
		return sortResults(out), nil
	case *call:
		var out []Result
		for _, s := range x.sel.series(all) {
			pts := window(s, t, x.sel.rng)
			if len(pts) < 2 {
				continue
			}
			lbl := map[string]string{}
			for k, v := range s.Labels {
				if k != "__name__" {
					lbl[k] = v
				}
			}
			if x.fn == "irate" {
				pts = pts[len(pts)-2:]
			}
			first, last := pts[0], pts[len(pts)-1]
			var inc float64
			prev := first.V
			for _, p := range pts[1:] {
				if p.V < prev { // the counter was reset
					inc += p.V
				} else {
					inc += p.V - prev
				}
				prev = p.V
			}
			v := inc
			if x.fn != "increase" {
				v = inc / (float64(last.T-first.T) / 1000)
			}
			out = append(out, Result{lbl, []Point{{t, v}}})
		}
		return sortResults(out), nil
	case *agg:
		in, err := eval(x.arg, all, t)
		if err != nil {
			return nil, err
		}
		type group struct {
			labels map[string]string
			vals   []float64
		}
		groups := map[string]*group{}
		for _, r := range in {
			if len(r.Points) != 1 {
				return nil, fmt.Errorf("%s needs an instant vector", x.op)
			}
			lbl := map[string]string{}
			for k, v := range r.Metric {
				if x.without && k != "__name__" && !contains(x.by, k) {
					lbl[k] = v
				}
			}
			for _, k := range x.by {
				if v, ok := r.Metric[k]; ok && !x.without {
					lbl[k] = v
				}
			}
			key := labelsKey(lbl)
			g := groups[key]
			if g == nil {
				g = &group{labels: lbl}
				groups[key] = g
			}
			g.vals = append(g.vals, r.Points[0].V)
		}
		var out []Result
		for _, g := range groups {
			out = append(out, Result{g.labels, []Point{{t, reduce(x.op, g.vals)}}})
		}
		return sortResults(out), nil
	}
	return nil, fmt.Errorf("unsupported expression")
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func reduce(op string, v []float64) float64 {
	r := v[0]
	switch op {
	case "count":
		return float64(len(v))
	case "sum", "avg":
		r = 0
		for _, x := range v {
			r += x
		}
		if op == "avg" {
			r /= float64(len(v))
		}
	case "min":
		for _, x := range v {
			r = math.Min(r, x)
		}
	case "max":
		for _, x := range v {
			r = math.Max(r, x)
		}
	}
	return r
}

// window returns the samples in (t-d, t].
func window(s *Series, t int64, d time.Duration) []Point {
	lo := t - d.Milliseconds()
	var out []Point
	for _, p := range s.Samples {
		if p.T > lo && p.T <= t {
			out = append(out, Point(p))
		}
	}
	return out
}

func sortResults(r []Result) []Result {
	sort.Slice(r, func(i, j int) bool { return labelsKey(r[i].Metric) < labelsKey(r[j].Metric) })
	return r
}

// Query answers an instant query at time t (ms); the result is a vector, or a matrix for a bare range selector.
func (s *Store) Query(ws, q string, t int64) (res []Result, matrix bool, err error) {
	e, err := parse(q)
	if err != nil {
		return nil, false, err
	}
	sel, isSel := e.(*selector)
	res, err = eval(e, s.snapshot(ws), t)
	return res, isSel && sel.rng > 0, err
}

// QueryRange answers a query at every step from start to end (ms).
func (s *Store) QueryRange(ws, q string, start, end, step int64) ([]Result, error) {
	e, err := parse(q)
	if err != nil {
		return nil, err
	}
	if sel, ok := e.(*selector); ok && sel.rng > 0 {
		return nil, fmt.Errorf("invalid expression type \"range vector\" for range query, must be Scalar or instant Vector")
	}
	if step <= 0 || end < start || (end-start)/step > 11000 {
		return nil, fmt.Errorf("bad range: step %d from %d to %d", step, start, end)
	}
	all := s.snapshot(ws)
	byKey := map[string]*Result{}
	var order []string
	for t := start; t <= end; t += step {
		rs, err := eval(e, all, t)
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			k := labelsKey(r.Metric)
			if byKey[k] == nil {
				byKey[k] = &Result{Metric: r.Metric}
				order = append(order, k)
			}
			byKey[k].Points = append(byKey[k].Points, Point{t, r.Points[0].V})
		}
	}
	sort.Strings(order)
	var out []Result
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out, nil
}

// Series returns the label sets of the series that match a selector (the series API), or all of them.
func (s *Store) SeriesOf(ws, match string) ([]map[string]string, error) {
	var sel *selector
	if match != "" {
		e, err := parse(match)
		if sel, _ = e.(*selector); err != nil || sel == nil {
			return nil, fmt.Errorf("match[] must be a selector")
		}
	}
	var out []map[string]string
	for _, sr := range s.snapshot(ws) {
		if sel == nil || len(sel.series([]*Series{sr})) == 1 {
			out = append(out, sr.Labels)
		}
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i]) < labelsKey(out[j]) })
	return out, nil
}
