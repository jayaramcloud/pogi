package web

import (
	"cmp"
	"fmt"
	"html"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/cost"
	"github.com/nrndev/pogi/internal/terraform"
)

// The Terraform diagram: one box per resource block (a count or for_each is one box, "×N"), an
// arrow from each block to the blocks that use it, so foundations sit on the left. Boxes sit in a
// lane per cloud, and a module is a dashed band inside its lane. It is drawn on the server, in
// the same style as the Map, and it is the same for the same input.
//
// Data sources are left out: they cost nothing and their arrows only add noise.
// ponytail: a dependency that passes through a data source is lost; add it back if people miss it.

const (
	tfW, tfH     = 210, 58
	tfColStep    = tfW + 70
	tfRowStep    = tfH + 18
	tfCollapseAt = 40 // a module with more blocks than this starts as one box
)

type tfNode struct {
	ID, Name, Type, Cloud, Module string
	Service                       string // the estimate row's service, which picks the icon
	Action                        string // plan: create, update, replace or delete; "" when unchanged
	Rows                          []int  // the estimate rows (resource instances) in this box
	Deps                          []string
	Monthly, Before               float64
	Affected, Collapsed, Unsure   bool // unsure: a price is missing, so the cost is a lower bound
	Blocks                        int  // collapsed: how many blocks are inside
	col, row, x, y                int
	Frame                         string // network mode: "net" or "sub" for a network or subnet, drawn as a frame; "" for a box
	parent, in                    string // a subnet's network; the frame this box or frame sits in ("" for the lane)
	w, h                          int    // a frame's label tab; 0 for a box
	acts                          map[string]bool
	free                          int
}

type tfGraph struct {
	Nodes    []*tfNode
	byID     map[string]*tfNode
	Plan     bool
	Total    float64
	Counts   map[string]int // plan: boxes per action
	Affected int
	Network  bool // frames for networks and subnets in place of module bands
	Lines    []*tfLine
	flows    []flowLine // every data-flow line the stack has, drawn or not; a tfLine's ID indexes it
	lanes    []tfLane
	bands    []tfBand
	w, h     int
}

// tfLine is a data-flow line drawn between two boxes: a wiring resource that is not a box.
type tfLine struct {
	ID       int    // the place in tfGraph.flows
	From, To string // the boxes (or collapsed module boxes) at its ends
	Label    string
	Name     string // the wiring resource, for its label
	Type     string
	N        int
	Rows     []int // the estimate rows of the wiring resource, for the side panel
	Action   string
	Live     bool
	Bad      bool
	acts     map[string]bool
}

type tfLane struct {
	Cloud string
	y, h  int
}

// tfBand is a dashed module band, or in network mode a frame: Kind "net" or "sub", ID the resource it is.
type tfBand struct {
	Module     string
	ID, Kind   string
	x, y, w, h int
}

// buildTFGraph groups the resources into boxes, finds their dependencies and, for a plan, the
// boxes the change reaches, then lays them out. expanded names the big modules to draw in full.
func buildTFGraph(cfg *terraform.Config, est *cost.Estimate, expanded []string) *tfGraph {
	return buildTFGraphBy(cfg, est, expanded, false)
}

// buildTFGraphBy is buildTFGraph that can group by network instead of by module.
func buildTFGraphBy(cfg *terraform.Config, est *cost.Estimate, expanded []string, network bool) *tfGraph {
	g := &tfGraph{Network: network, byID: map[string]*tfNode{}, Plan: est.Source == "plan", Total: est.Total, Counts: map[string]int{}}
	rowOf := map[string]int{}
	for i, r := range est.Rows {
		rowOf[r.Address] = i
	}
	blocks := map[string]*tfNode{}
	perModule := map[string]int{}
	var all []*tfNode
	g.flows = tfFlows(cfg)
	wiring := map[string]*tfLine{} // the wiring blocks that draw as a line, not a box
	for _, f := range g.flows {
		if !f.Box {
			wiring[f.Wiring] = &tfLine{acts: map[string]bool{}}
		}
	}
	for _, tr := range cfg.Resources {
		i, ok := rowOf[tr.Address]
		if !ok { // data sources and reads have no row
			continue
		}
		if w := wiring[tr.Block]; w != nil {
			w.Rows = append(w.Rows, i)
			w.Name, w.Type = tr.Name, tr.Type
			w.acts[est.Rows[i].Action] = true
			continue
		}
		n := blocks[tr.Block]
		if n == nil {
			n = &tfNode{ID: tr.Block, Name: tr.Name, Type: tr.Type, Cloud: tr.Cloud, Module: tr.Module, acts: map[string]bool{}}
			blocks[tr.Block] = n
			all = append(all, n)
			perModule[tr.Module]++
		}
		n.add(i, est.Rows[i])
	}
	slices.SortFunc(all, func(a, b *tfNode) int { return strings.Compare(a.ID, b.ID) })

	// a big module that is not expanded becomes one box
	nodeOf := map[string]string{}
	mods := map[string]*tfNode{}
	for _, n := range all {
		if m := n.Module; m != "" && perModule[m] > tfCollapseAt && !slices.Contains(expanded, m) {
			c := mods[m]
			if c == nil {
				c = &tfNode{ID: m, Name: m, Type: "module", Cloud: n.Cloud, Module: m, Collapsed: true, acts: map[string]bool{}}
				mods[m] = c
				g.Nodes = append(g.Nodes, c)
			}
			c.Blocks++
			c.Rows = append(c.Rows, n.Rows...)
			c.Monthly += n.Monthly
			c.Before += n.Before
			c.Unsure = c.Unsure || n.Unsure
			c.free += n.free
			for a := range n.acts {
				c.acts[a] = true
			}
			nodeOf[n.ID] = m
			continue
		}
		g.Nodes = append(g.Nodes, n)
		nodeOf[n.ID] = n.ID
	}
	for _, n := range g.Nodes {
		g.byID[n.ID] = n
	}
	g.addLines(cfg, wiring, nodeOf)
	for _, tr := range cfg.Resources {
		from := g.byID[nodeOf[tr.Block]]
		if from == nil {
			continue
		}
		for _, dep := range tr.Deps {
			if to := nodeOf[dep]; to != "" && to != from.ID && !slices.Contains(from.Deps, to) {
				from.Deps = append(from.Deps, to)
			}
		}
	}
	for _, n := range g.Nodes {
		slices.Sort(n.Deps)
		slices.Sort(n.Rows)
		n.Rows = slices.Compact(n.Rows)
		n.Action = tfAction(n.acts)
	}
	if network {
		g.assignFrames()
	}
	if g.Plan {
		g.markPlan()
	}
	g.layout()
	return g
}

// addLines draws each resolved flow between the boxes that hold its ends: a box, or the collapsed
// module box when the end sits in one. A line with both ends in one box is not drawn.
func (g *tfGraph) addLines(cfg *terraform.Config, wiring map[string]*tfLine, nodeOf map[string]string) {
	rows := map[string]*tfLine{}
	for _, tr := range cfg.Resources {
		rows[tr.Block] = &tfLine{Name: tr.Name, Type: tr.Type}
	}
	boxRows := map[string][]int{} // a wiring block that stays a box: its own rows
	for _, n := range g.Nodes {
		if !n.Collapsed {
			boxRows[n.ID] = n.Rows
		}
	}
	drawn := map[string]bool{}
	for i, f := range g.flows {
		from, to := g.byID[nodeOf[f.From]], g.byID[nodeOf[f.To]]
		if from == nil || to == nil || from == to {
			continue
		}
		l := &tfLine{ID: i, From: from.ID, To: to.ID, Label: f.Label, N: f.N, Name: rows[f.Wiring].Name, Type: rows[f.Wiring].Type, Live: f.Live, Bad: f.Bad}
		if w := wiring[f.Wiring]; w != nil {
			l.Rows, l.Action = w.Rows, tfAction(w.acts)
		} else {
			l.Rows = boxRows[f.Wiring]
			if n := g.byID[f.Wiring]; n != nil {
				l.Action = n.Action
			}
		}
		g.Lines = append(g.Lines, l)
		drawn[f.Wiring] = true
	}
	for _, f := range g.flows { // both ends in one collapsed module: the wiring resource is counted in it
		if w := wiring[f.Wiring]; w != nil && !drawn[f.Wiring] {
			if n := g.byID[nodeOf[f.From]]; n != nil && n.Collapsed {
				n.Rows = append(n.Rows, w.Rows...)
			}
			delete(wiring, f.Wiring)
		}
	}
}

// markLive lights the lines pogi saw traffic on in the last liveWindow.
func (g *tfGraph) markLive(cfg *terraform.Config, spans []core.Span, now time.Time) {
	markLive(g.flows, cfg, spans, now)
	for _, l := range g.Lines {
		l.Live, l.Bad = g.flows[l.ID].Live, g.flows[l.ID].Bad
	}
}

// The network and subnet types that draw as frames in network mode.
var tfFrameTypes = map[string]string{
	"aws_vpc": "net", "aws_subnet": "sub",
	"azurerm_virtual_network": "net", "azurerm_subnet": "sub",
	"google_compute_network": "net", "google_compute_subnetwork": "sub",
}

// assignFrames turns the networks and subnets into frames and finds the frame each box sits in.
func (g *tfGraph) assignFrames() {
	for _, n := range g.Nodes {
		if !n.Collapsed {
			n.Frame = tfFrameTypes[n.Type]
		}
	}
	for _, n := range g.Nodes { // a subnet sits in the first network it depends on
		if n.Frame != "sub" {
			continue
		}
		for _, d := range n.Deps {
			if f := g.byID[d]; f.Frame == "net" && f.Cloud == n.Cloud {
				n.parent, n.in = f.ID, f.ID
				break
			}
		}
	}
	for _, n := range g.Nodes {
		if n.Frame == "" {
			n.in = g.homeFrame(n)
		}
	}
}

// homeFrame is the frame a box sits in, from the frames among its direct dependencies: its subnet;
// the network when it uses two subnets of one network, or a network alone; else "" (the lane),
// as when it reaches into two networks.
func (g *tfGraph) homeFrame(n *tfNode) string {
	var subs, tops []string // the subnets, and the outermost frame of each frame it uses
	add := func(l []string, id string) []string {
		if slices.Contains(l, id) {
			return l
		}
		return append(l, id)
	}
	for _, d := range n.Deps {
		f := g.byID[d]
		if f.Frame == "" || f.Cloud != n.Cloud {
			continue
		}
		top := f.ID
		if f.Frame == "sub" {
			subs = add(subs, f.ID)
			top = cmp.Or(f.parent, f.ID)
		}
		tops = add(tops, top)
	}
	switch {
	case len(tops) != 1:
		return ""
	case len(subs) == 1:
		return subs[0]
	}
	return tops[0]
}

// holds reports whether frame f is the frame n sits in, or one around it.
func (g *tfGraph) holds(f, n *tfNode) bool {
	for id := n.in; id != ""; id = g.byID[id].in {
		if id == f.ID {
			return true
		}
	}
	return false
}

// boxes counts the boxes, not the frames.
func (g *tfGraph) boxes() (c int) {
	for _, n := range g.Nodes {
		if n.Frame == "" {
			c++
		}
	}
	return c
}

// add counts one resource instance in the box.
func (n *tfNode) add(i int, r cost.Row) {
	n.Rows = append(n.Rows, i)
	n.Service = r.Service
	n.Monthly += r.Monthly
	n.Before += r.Before
	n.acts[r.Action] = true
	switch r.Status {
	case cost.Free:
		n.free++
	case cost.Needs, cost.Unpriced, cost.Usage:
		n.Unsure = true
	}
}

// tfAction rolls the actions of a box's instances into one: the single change they share, or
// "update" when they differ. Unchanged and read-only instances do not count.
func tfAction(acts map[string]bool) string {
	var changes []string
	for a := range acts {
		if a != "" && a != "no-op" && a != "read" {
			changes = append(changes, a)
		}
	}
	switch len(changes) {
	case 0:
		return ""
	case 1:
		return changes[0]
	}
	return "update"
}

// markPlan finds the boxes that depend, directly or not, on a changed box and have no change of their own.
func (g *tfGraph) markPlan() {
	users := map[string][]string{}
	var queue []string
	for _, n := range g.Nodes {
		for _, d := range n.Deps {
			users[d] = append(users[d], n.ID)
		}
		if n.Action != "" {
			g.Counts[n.Action]++
			queue = append(queue, n.ID)
		}
	}
	seen := map[string]bool{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, u := range users[id] {
			if !seen[u] {
				seen[u] = true
				queue = append(queue, u)
				if n := g.byID[u]; n.Action == "" {
					n.Affected = true
					g.Affected++
				}
			}
		}
	}
}

// summary is the change view's line: "3 to create, 1 to replace, 4 affected".
func (g *tfGraph) summary() string {
	if !g.Plan {
		return ""
	}
	var p []string
	for _, a := range []string{"create", "update", "replace", "delete"} {
		if c := g.Counts[a]; c > 0 {
			p = append(p, fmt.Sprintf("%d to %s", c, a))
		}
	}
	if g.Affected > 0 {
		p = append(p, fmt.Sprintf("%d affected", g.Affected))
	}
	return cmp.Or(strings.Join(p, ", "), "The plan changes nothing")
}

func (g *tfGraph) edges() (n int) {
	for _, x := range g.Nodes {
		n += len(x.Deps)
	}
	return n
}

var tfLanes = []string{"aws", "gcp", "azure", ""}

// layout puts each box in a column by its longest path from the foundations, then in a lane per
// cloud, a band per module, and rows in order.
func (g *tfGraph) layout() {
	flows := g.orderedFlows()
	for round := 0; round < len(g.Nodes); round++ {
		moved := false
		for _, n := range g.Nodes {
			if n.Frame != "" { // frames are not in a column
				continue
			}
			for _, d := range n.Deps {
				if dn := g.byID[d]; dn.Frame == "" && dn.col >= n.col && dn.col < len(g.Nodes)-1 { // the cap ends a cycle
					n.col, moved = dn.col+1, true
				}
			}
		}
		for _, l := range flows { // data flows left to right, like a dependency
			if s, t := g.byID[l.From], g.byID[l.To]; s.Frame == "" && t.Frame == "" && s.col >= t.col && s.col < len(g.Nodes)-1 {
				t.col, moved = s.col+1, true
			}
		}
		if !moved {
			break
		}
	}
	g.w, g.h = 640, 0
	for _, cloud := range tfLanes {
		var ns []*tfNode
		for _, n := range g.Nodes {
			if n.Cloud == cloud {
				ns = append(ns, n)
			}
		}
		if len(ns) == 0 {
			continue
		}
		top, cur := g.h, g.h+30
		// The bands of the lane. A key is (network frame, band): in module mode the band is a module
		// ("" holds the root module and the collapsed modules); in network mode it is a subnet frame,
		// or "" for the boxes that sit in the network itself or outside every frame.
		type key struct{ net, band string }
		var keys []key
		addKey := func(k key) {
			if !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
		for _, n := range ns {
			if n.Frame == "" {
				addKey(g.bandKey(n))
			} else if n.Frame == "sub" {
				addKey(key{n.parent, n.ID})
			}
		}
		for _, n := range ns {
			if n.Frame == "net" && !slices.ContainsFunc(keys, func(k key) bool { return k.net == n.ID }) {
				addKey(key{n.ID, ""}) // an empty network still draws
			}
		}
		slices.SortFunc(keys, func(a, b key) int { return cmp.Or(strings.Compare(a.net, b.net), strings.Compare(a.band, b.band)) })
		var netTop, netL, netR, bandsAt int
		for ki, k := range keys {
			if k.net != "" && (ki == 0 || keys[ki-1].net != k.net) { // a network frame opens
				netTop, netL, netR, bandsAt = cur, 1<<30, 0, len(g.bands)
				cur += 30
			}
			rows, most := map[int]int{}, 0
			minX, maxX := 1<<30, 0
			label := 0
			if k.band != "" {
				label = 22
			}
			var band []*tfNode
			for _, n := range ns {
				if n.Frame == "" && g.bandKey(n) == (struct{ net, band string }(k)) {
					band = append(band, n)
				}
			}
			g.orderRows(band)
			for _, n := range band {
				rows[n.col] = max(rows[n.col], n.row+1)
				most = max(most, rows[n.col])
				n.x, n.y = 30+n.col*tfColStep, cur+label+n.row*tfRowStep
				minX, maxX = min(minX, n.x), max(maxX, n.x+tfW)
				g.w = max(g.w, n.x+tfW+30)
			}
			if len(band) == 0 { // an empty subnet or network: a frame with no boxes in it
				minX, maxX, most = 30, 30+tfW, 1
			}
			if k.band != "" {
				bd := tfBand{Module: k.band, x: minX - 12, y: cur, w: maxX - minX + 24, h: label + most*tfRowStep - 10}
				if g.Network {
					f := g.byID[k.band]
					bd.Module, bd.ID, bd.Kind = f.Name, f.ID, "sub"
					f.setLabel(bd.x+8, bd.y+4, bd.w-16)
				}
				g.bands = append(g.bands, bd)
			}
			netL, netR = min(netL, minX-12), max(netR, maxX+12)
			cur += label + most*tfRowStep + 8
			if k.net != "" && (ki == len(keys)-1 || keys[ki+1].net != k.net) { // the network frame closes
				f := g.byID[k.net]
				bd := tfBand{Module: f.Name, ID: f.ID, Kind: "net", x: netL - 10, y: netTop, w: netR - netL + 20, h: cur - 6 - netTop}
				f.setLabel(bd.x+10, bd.y+5, bd.w-20)
				g.bands = slices.Insert(g.bands, bandsAt, bd) // drawn before its subnets
				cur += 8
			}
		}
		g.lanes = append(g.lanes, tfLane{cloud, top, cur - top})
		g.h = cur + 12
	}
	g.h = max(g.h, 80)
}

// bandKey is the band a box is laid out in: its module, or in network mode its frame.
func (g *tfGraph) bandKey(n *tfNode) (k struct{ net, band string }) {
	if !g.Network {
		k.band = tfBandOf(n)
		return k
	}
	if n.Collapsed || n.in == "" {
		return k
	}
	if f := g.byID[n.in]; f.Frame == "sub" {
		return struct{ net, band string }{f.parent, f.ID}
	}
	k.net = n.in
	return k
}

// setLabel places a frame's label tab: its name, clipped to fit the frame.
func (n *tfNode) setLabel(x, y, room int) {
	n.x, n.y, n.h = x, y, 18
	n.w = max(60, min(room, 16+7*len([]rune(mapClip(n.shortType()+" "+n.Name, 40)))))
}

// orderedFlows is the data-flow lines that also set the column order. A line that would close a loop
// with the dependencies or the lines before it is left out: a subscription box depends on the queue it
// forwards to, so pushing the queue right of it too would push both right until the cap.
func (g *tfGraph) orderedFlows() []*tfLine {
	before := map[string][]string{} // box → the boxes that sit left of it
	for _, n := range g.Nodes {
		before[n.ID] = slices.Clone(n.Deps)
	}
	var out []*tfLine
	for _, l := range g.Lines {
		if !tfReaches(before, l.From, l.To) {
			before[l.To] = append(before[l.To], l.From)
			out = append(out, l)
		}
	}
	return out
}

// tfReaches reports whether box a sits right of box b through before, directly or not.
func tfReaches(before map[string][]string, a, b string) bool {
	seen := map[string]bool{}
	stack := []string{a}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, d := range before[id] {
			if d == b {
				return true
			}
			if !seen[d] {
				seen[d] = true
				stack = append(stack, d)
			}
		}
	}
	return false
}

// orderRows gives each box of a band its row. Each box goes as near as it can to the average
// row of the boxes it connects to (the barycenter heuristic), so chains such as VPC → subnet →
// instance run straight and few edges cross other boxes. Sweeps go left to right (by what a box
// depends on), right to left (by what depends on it), then left to right again. It's
// deterministic: ties keep the id order of g.Nodes.
func (g *tfGraph) orderRows(band []*tfNode) {
	in := map[*tfNode]bool{}
	cols := map[int][]*tfNode{}
	maxCol := 0
	for _, n := range band {
		in[n] = true
		cols[n.col] = append(cols[n.col], n)
		maxCol = max(maxCol, n.col)
	}
	users := map[*tfNode][]*tfNode{} // what depends on each box
	for _, n := range band {
		for _, d := range n.Deps {
			if dn := g.byID[d]; dn != nil && in[dn] {
				users[dn] = append(users[dn], n)
			}
		}
	}
	place := func(col int, near func(n *tfNode) []*tfNode) {
		ns := cols[col]
		want := map[*tfNode]float64{}
		for i, n := range ns {
			want[n] = float64(i) // no neighbours: keep the order it has
			if nb := near(n); len(nb) > 0 {
				sum := 0.0
				for _, o := range nb {
					sum += float64(o.row)
				}
				want[n] = sum / float64(len(nb))
			}
		}
		slices.SortStableFunc(ns, func(a, b *tfNode) int { return cmp.Compare(want[a], want[b]) })
		next := 0
		for _, n := range ns { // as near the wanted row as the boxes above allow
			n.row = max(next, int(math.Round(want[n])))
			next = n.row + 1
		}
	}
	deps := func(n *tfNode) (out []*tfNode) {
		for _, d := range n.Deps {
			if dn := g.byID[d]; dn != nil && in[dn] && dn.col < n.col {
				out = append(out, dn)
			}
		}
		return out
	}
	dependents := func(n *tfNode) (out []*tfNode) {
		for _, u := range users[n] {
			if u.col > n.col {
				out = append(out, u)
			}
		}
		return out
	}
	for i := range cols[0] { // start in id order
		cols[0][i].row = i
	}
	for c := 1; c <= maxCol; c++ {
		place(c, deps)
	}
	for c := maxCol - 1; c >= 0; c-- {
		place(c, dependents)
	}
	for c := 1; c <= maxCol; c++ {
		place(c, deps)
	}
	used := map[int]bool{} // close up rows no box ended in
	for _, n := range band {
		used[n.row] = true
	}
	rows := slices.Sorted(maps.Keys(used))
	for _, n := range band {
		n.row, _ = slices.BinarySearch(rows, n.row)
	}
}

func tfBandOf(n *tfNode) string {
	if n.Collapsed {
		return ""
	}
	return n.Module
}

// tfLaneName is the label of a lane.
func tfLaneName(c string) string {
	return cmp.Or(map[string]string{"aws": "AWS", "gcp": "GOOGLE CLOUD", "azure": "AZURE"}[c], "OTHER")
}

// tfBadge is the cost on a box: "$123.45/mo", with a plus when a price is missing, "?" when none is known.
func (n *tfNode) badge(plan bool) string {
	switch {
	case n.Monthly == 0 && n.Unsure:
		return "?"
	case n.Monthly == 0 && plan && n.Action == "delete" && n.Before > 0:
		return "−" + tfUSD(n.Before) + "/mo"
	case n.Monthly == 0 || n.free == len(n.Rows) && n.free > 0:
		return ""
	}
	return tfUSD(n.Monthly) + map[bool]string{true: "+", false: ""}[n.Unsure] + "/mo"
}

func (n *tfNode) shortType() string {
	if n.Collapsed {
		return "module"
	}
	for _, p := range []string{"aws_", "google_", "azurerm_"} {
		if s, ok := strings.CutPrefix(n.Type, p); ok {
			return strings.ReplaceAll(s, "_", " ")
		}
	}
	return strings.ReplaceAll(n.Type, "_", " ")
}

// svg draws the graph. With export set, it is a standalone file in the light theme.
func (g *tfGraph) svg(export bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="map tfmap" viewBox="0 0 %[1]d %[2]d" width="%[1]d" height="%[2]d" style="width:%[1]dpx;height:%[2]dpx" role="group" aria-label="Resource diagram">`, g.w, g.h)
	b.WriteString(`<defs>` + g.iconDefs(export) + `<marker id="tfarrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--faint)"/></marker></defs>`)
	for _, l := range g.lanes {
		cl := cmp.Or(l.Cloud, "faint")
		fmt.Fprintf(&b, `<rect class="lane" x="0" y="%d" width="%d" height="%d" opacity=".55"/><text class="lanel" x="14" y="%d"><tspan fill="var(--%s)">●</tspan> %s</text>`, l.y, g.w, l.h, l.y+20, cl, tfLaneName(l.Cloud))
	}
	for _, bd := range g.bands {
		cls := "band"
		if bd.Kind != "" { // a frame's label is its resource, drawn with the boxes
			cls += " frame " + bd.Kind
		}
		fmt.Fprintf(&b, `<rect class="%s" x="%d" y="%d" width="%d" height="%d" rx="10"/>`, cls, bd.x, bd.y, bd.w, bd.h)
		if bd.Kind == "" {
			fmt.Fprintf(&b, `<text class="bandl" x="%d" y="%d">%s</text>`, bd.x+10, bd.y+15, html.EscapeString(bd.Module))
		}
	}
	for _, n := range g.Nodes {
		for _, d := range n.Deps {
			s := g.byID[d]
			if s.Frame != "" && g.holds(s, n) { // a frame already shows what is inside it
				continue
			}
			p := tfCurve(s, n)
			cls := "edge"
			if g.Plan && s.Action == "" && !s.Affected && n.Action == "" && !n.Affected {
				cls += " dim"
			}
			fmt.Fprintf(&b, `<path class="%s" data-f="%s" data-t="%s" d="%s" marker-end="url(#tfarrow)"/>`, cls, html.EscapeString(s.ID), html.EscapeString(n.ID), p)
		}
	}
	for _, l := range g.Lines {
		g.line(&b, l, export)
	}
	for _, n := range g.Nodes {
		g.node(&b, n, export)
	}
	b.WriteString("</svg>")
	return b.String()
}

// tfCurve is the path of a line from box s to box n.
func tfCurve(s, n *tfNode) string {
	sw, sh := s.size()
	_, nh := n.size()
	x1, y1, x2, y2 := s.x+sw, s.y+sh/2, n.x, n.y+nh/2
	mx := (x1 + x2) / 2
	if x2 <= x1 { // against the columns (a cycle): leave from the bottom
		return fmt.Sprintf("M%d,%d C%d,%d %d,%d %d,%d", s.x+sw/2, s.y+sh, s.x+sw/2, y2, x2-40, y2, x2, y2)
	}
	return fmt.Sprintf("M%d,%d C%d,%d %d,%d %d,%d", x1, y1, mx, y1, mx, y2, x2, y2)
}

// size is the width and height of what is drawn for the node: a box, or a frame's label tab.
func (n *tfNode) size() (int, int) {
	if n.Frame != "" {
		return n.w, n.h
	}
	return tfW, tfH
}

// line draws a data flow: a dotted line with an arrow, from the source to the target. The page's copy
// is a button (a wide, clear path is its target) and has pulses when traffic is live; an export is
// a still picture.
func (g *tfGraph) line(b *strings.Builder, l *tfLine, export bool) {
	s, t := g.byID[l.From], g.byID[l.To]
	d := tfCurve(s, t)
	sw, sh := s.size()
	_, th := t.size()
	mx, my := (s.x+sw+t.x)/2, (s.y+sh/2+t.y+th/2)/2-6
	typ := (&tfNode{Type: l.Type}).shortType()
	label := fmt.Sprintf("%s %s, %s → %s, data flow", typ, l.Name, s.Name, t.Name)
	if l.N > 1 {
		label += fmt.Sprintf(", ×%d", l.N)
	}
	if l.Action != "" {
		label += ", " + l.Action
	}
	if l.Live {
		label += map[bool]string{true: ", live traffic with errors", false: ", live traffic"}[l.Bad]
	}
	cls := "flowg"
	switch {
	case l.Action != "":
		cls += " a-" + l.Action
	case g.Plan && s.Action == "" && !s.Affected && t.Action == "" && !t.Affected:
		cls += " dim"
	}
	attrs := ""
	if !export {
		rows := make([]string, len(l.Rows))
		for i, r := range l.Rows {
			rows[i] = strconv.Itoa(r)
		}
		attrs = fmt.Sprintf(` tabindex="0" role="button" aria-label="%s" data-id="flow:%d" data-name="%s" data-f="%s" data-t="%s" data-rows="%s" data-n="%d"`,
			html.EscapeString(label), l.ID, html.EscapeString(l.Name), html.EscapeString(s.ID), html.EscapeString(t.ID), strings.Join(rows, " "), len(l.Rows))
	}
	fmt.Fprintf(b, `<g class="%s tff"%s><title>%s</title>`, cls, attrs, html.EscapeString(label))
	fmt.Fprintf(b, `<path class="flow" data-line="%d" d="%s" marker-end="url(#tfarrow)"/>`, l.ID, d)
	if !export {
		fmt.Fprintf(b, `<path class="flowhit" d="%s"/>`, d)
	}
	text := l.Label
	if l.N > 1 {
		text += fmt.Sprintf(" ×%d", l.N)
	}
	fmt.Fprintf(b, `<text class="elabel" x="%d" y="%d" text-anchor="middle">%s</text>`, mx, my, html.EscapeString(text))
	if l.Live && !export {
		b.WriteString(pulse(l.ID, l.Bad, d))
	}
	b.WriteString("</g>")
}

func (g *tfGraph) node(b *strings.Builder, n *tfNode, export bool) {
	cls := "node tfn"
	switch {
	case n.Action != "":
		cls += " a-" + n.Action
	case n.Affected:
		cls += " affected"
	case g.Plan:
		cls += " dim"
	}
	if n.Collapsed {
		cls += " collapsed"
	}
	if n.Frame != "" {
		cls += " frame"
	}
	cloud := cmp.Or(n.Cloud, "faint")
	badge := n.badge(g.Plan)
	if n.Frame != "" { // a frame shows no cost
		badge = ""
	}
	label := n.shortType() + " " + n.Name
	if len(n.Rows) > 1 && !n.Collapsed {
		label += fmt.Sprintf(" ×%d", len(n.Rows))
	}
	if badge != "" {
		label += ", " + badge
	}
	if n.Action != "" {
		label += ", " + n.Action
	} else if n.Affected {
		label += ", affected by the plan"
	}
	attrs := ""
	if !export {
		rows := make([]string, len(n.Rows))
		for i, r := range n.Rows {
			rows[i] = strconv.Itoa(r)
		}
		attrs = fmt.Sprintf(` tabindex="0" role="button" aria-label="%s" data-id="%s" data-name="%s" data-deps="%s" data-rows="%s" data-n="%d"%s`,
			html.EscapeString(label), html.EscapeString(n.ID), html.EscapeString(n.Name), html.EscapeString(strings.Join(n.Deps, " ")), strings.Join(rows, " "), len(n.Rows),
			map[bool]string{true: ` data-expand="` + html.EscapeString(n.ID) + `"`, false: ""}[n.Collapsed])
	}
	fmt.Fprintf(b, `<g class="%s"%s><title>%s</title>`, cls, attrs, html.EscapeString(label))
	if n.Frame != "" { // network mode: a network or subnet is a frame, and this is its label tab
		fmt.Fprintf(b, `<rect x="%d" y="%d" width="%d" height="%d" rx="5"/><circle cx="%d" cy="%d" r="3" fill="var(--%s)"/><text class="ty" x="%d" y="%d">%s</text></g>`,
			n.x, n.y, n.w, n.h, n.x+10, n.y+9, cloud, n.x+19, n.y+13, html.EscapeString(mapClip(n.shortType()+" "+n.Name, 40)))
		return
	}
	if n.Collapsed { // a stack, so it reads as many boxes
		fmt.Fprintf(b, `<rect class="stack" x="%d" y="%d" width="%d" height="%d" rx="8"/>`, n.x+5, n.y+5, tfW, tfH)
	}
	fmt.Fprintf(b, `<rect x="%d" y="%d" width="%d" height="%d" rx="8"/>%s<text class="ty" x="%d" y="%d">%s</text>`,
		n.x, n.y, tfW, tfH, n.use(n.x+8, n.y+(tfH-28)/2), n.x+44, n.y+19, html.EscapeString(mapClip(n.shortType(), tfTypeClip(badge))))
	if badge != "" {
		share := ""
		if g.Total > 0 && n.Monthly/g.Total >= .2 {
			share = " c1"
		} else if g.Total > 0 && n.Monthly/g.Total >= .05 {
			share = " c2"
		}
		fmt.Fprintf(b, `<text class="ct%s" x="%d" y="%d" text-anchor="end">%s</text>`, share, n.x+tfW-10, n.y+19, html.EscapeString(badge))
	}
	clip := 24
	if len(n.Rows) > 1 && !n.Collapsed {
		clip = 18
		fmt.Fprintf(b, `<text class="cnt" x="%d" y="%d" text-anchor="end">×%d</text>`, n.x+tfW-10, n.y+39, len(n.Rows))
	}
	fmt.Fprintf(b, `<text class="nm" x="%d" y="%d">%s</text>`, n.x+44, n.y+39, html.EscapeString(mapClip(n.Name, clip)))
	switch {
	case n.Collapsed:
		fmt.Fprintf(b, `<text class="tag" x="%d" y="%d">%s</text>`, n.x+44, n.y+53, html.EscapeString(plural(n.Blocks, "resource block")+" · click to open"))
	case n.Action != "":
		fmt.Fprintf(b, `<text class="tag" x="%d" y="%d">%s</text>`, n.x+44, n.y+53, n.Action)
	case n.Affected:
		fmt.Fprintf(b, `<text class="tag" x="%d" y="%d">affected</text>`, n.x+44, n.y+53)
	}
	b.WriteString("</g>")
}

// export is the diagram as a standalone SVG file in the light theme.
func (g *tfGraph) export() string {
	s := strings.Replace(g.svg(true), `<svg class="map tfmap" `, `<svg xmlns="http://www.w3.org/2000/svg" class="map tfmap" `, 1)
	i := strings.Index(s, ">") + 1
	return exportColors.Replace(s[:i] + exportStyle + tfExportStyle + s[i:])
}

const tfExportStyle = `<style>.band{fill:none;stroke:#a1a1aa;stroke-width:1.2;stroke-dasharray:5 4}.bandl{fill:#6b6b74;font:600 11px ui-monospace,Menlo,monospace}` +
	`.band.frame{stroke-dasharray:none;stroke:#a1a1aa}.band.net{fill:#f4f4f5;fill-opacity:.6}.node.frame rect{fill:#fff;stroke:#a1a1aa}` +
	`.stack{fill:#f4f4f5;stroke:#d4d4d8;stroke-width:1.2}.cnt{fill:#52525b;font:600 11px ui-monospace,Menlo,monospace}.tag{fill:#6b6b74;font:10px system-ui}` +
	`.ct.c1{fill:#18181b;font-weight:700}.ct.c2{fill:#18181b}` +
	`.a-create rect{fill:#dcfce7;stroke:#15803d}.a-update rect{fill:#fef3c7;stroke:#b45309}.a-replace rect,.a-delete rect{fill:#fee2e2;stroke:#b91c1c}` +
	`.a-replace rect{stroke-dasharray:5 3}.affected rect{stroke:#b45309;stroke-dasharray:2 3}.node.dim{opacity:.4}` +
	`.flow{fill:none;stroke:#c4176f;stroke-width:1.8;stroke-dasharray:2 5;stroke-linecap:round}.flowg.dim{opacity:.4}</style>`

// tfTypeClip is how many characters of the type fit before the cost badge.
func tfTypeClip(badge string) int {
	if badge == "" {
		return 24
	}
	return max(8, 22-len([]rune(badge)))
}
