package web

import (
	"cmp"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The map draws how resources are wired: the links services report (core.Linker) and
// the edges the span log saw at runtime, which also covers GCP and Azure.

const (
	mapW, mapH     = 168, 52
	mapColStep     = mapW + 80
	mapRowStep     = 84
	observedWindow = 15 * time.Minute
	liveWindow     = 10 * time.Second
)

type mapNode struct {
	ID, Name, Type, Cloud, Svc, Ct string
	Count                          string // "12 msgs": the items the resource holds, when its service lists them
	Err                            bool
	col, row, x, y                 int
}

// mapEdge kind: "c" configured, "o" seen at runtime, "d" dead-letter.
type mapEdge struct {
	From, To, Label, Kind string
	Live, Bad             bool
}

type mapModel struct {
	nodes []*mapNode
	edges []mapEdge
	byID  map[string]*mapNode
}

type mapNodeLink struct {
	Node        *mapNode
	Label, Kind string
}

// mapPage is the data of the map, or of one node's drawer when Node is set.
type mapPage struct {
	Cloud, Hl, Trace string
	SVG              template.HTML
	Hidden           int
	Empty            bool
	Node             *mapNode
	In, Out          []mapNodeLink
	Flows            []*flow
	Open             string // the service page of the node
}

var cloudLanes = []string{core.AWS, core.GCP, core.Azure}

// normID makes the id of a resource the same whoever names it: a function with or
// without its alias or version is one node.
func normID(id string) string {
	if p := strings.Split(id, ":"); len(p) == 8 && p[0] == "arn" && p[2] == "lambda" && p[5] == "function" {
		return strings.Join(p[:7], ":")
	}
	return strings.TrimSuffix(id, "/")
}

// classify says what a resource id is: its cloud, the registry name of its service, a
// type label (SQS queue) and the short name.
func (ui *UI) classify(id string) (cloud, svc, typ, name string) {
	name = shortName(id)
	var res string
	switch {
	case strings.HasPrefix(id, "http://") || strings.HasPrefix(id, "https://"):
		_, rest, _ := strings.Cut(id, "://")
		return "", "", "HTTP endpoint", rest
	case strings.HasPrefix(id, "arn:"):
		svc, _, _, res, _ = core.ParseARN(id)
		cloud = core.AWS
	case strings.HasPrefix(id, "//"):
		host, rest, _ := strings.Cut(strings.TrimPrefix(id, "//"), "/")
		svc, res, cloud = strings.TrimSuffix(host, ".googleapis.com"), rest, core.GCP
		if svc == "storage" {
			svc = "gcs" // the host says storage, the registry says gcs
		}
	case strings.HasPrefix(strings.ToLower(id), "/subscriptions/"):
		_, rest, _ := strings.Cut(strings.ToLower(id), "/providers/")
		svc, res, cloud = strings.TrimPrefix(strings.SplitN(rest, "/", 2)[0], "microsoft."), rest, core.Azure
	}
	c, title := ui.svcMeta(svc, id)
	if c != "" {
		cloud = c
	}
	typ = title
	kind := func(prefix, label string) bool {
		if strings.HasPrefix(res, prefix) {
			typ = label
			return true
		}
		return false
	}
	switch svc {
	case "sqs":
		typ = "SQS queue"
	case "sns":
		typ = "SNS topic"
	case "s3":
		typ = "S3 bucket"
	case "lambda":
		typ = "Lambda"
	case "events":
		_ = kind("rule/", "EB rule") || kind("event-bus/", "EB bus") || kind("archive/", "EB archive")
	case "dynamodb":
		_ = kind("table/", "DynamoDB table")
		if strings.Contains(res, "/stream/") {
			typ = "DynamoDB stream"
		}
	case "kinesis":
		typ = "Kinesis stream"
	case "firehose":
		typ = "Firehose stream"
	case "pubsub":
		_ = kind("topics/", "Pub/Sub topic") || kind("subscriptions/", "Pub/Sub subscription")
	case "gcs":
		typ = "GCS bucket"
	case "eventarc":
		typ = "Eventarc trigger"
	case "servicebus":
		switch {
		case strings.Contains(res, "/subscriptions/"):
			typ = "Service Bus subscription"
		case strings.Contains(res, "/topics/"):
			typ = "Service Bus topic"
		case strings.Contains(res, "/queues/"):
			typ = "Service Bus queue"
		}
	case "eventgrid":
		typ = "Event Grid topic"
		if strings.Contains(res, "systemtopics/") {
			typ = "Event Grid system topic"
		}
	}
	return cloud, svc, typ, name
}

// buildMapModel gathers links and runtime edges into nodes and edges.
func (ui *UI) buildMapModel() *mapModel {
	m := &mapModel{byID: map[string]*mapNode{}}
	type pair struct{ f, t string }
	idx := map[pair]int{}
	node := func(id string) {
		if m.byID[id] == nil {
			cloud, svc, typ, name := ui.classify(id)
			n := &mapNode{ID: id, Name: name, Type: typ, Cloud: cloud, Svc: svc}
			m.byID[id] = n
			m.nodes = append(m.nodes, n)
		}
	}
	add := func(from, to, label, kind string) int {
		node(from)
		node(to)
		m.edges = append(m.edges, mapEdge{From: from, To: to, Label: label, Kind: kind})
		idx[pair{from, to}] = len(m.edges) - 1
		return len(m.edges) - 1
	}
	for _, svc := range ui.hub.Services() {
		if l, ok := svc.(core.Linker); ok {
			for _, k := range l.Links() {
				if k.From != "" && k.To != "" {
					add(normID(k.From), normID(k.To), k.Label, map[bool]string{true: "d", false: "c"}[k.Kind == "dlq"])
				}
			}
		}
	}
	now, fails := time.Now(), map[string]int{}
	for _, sp := range ui.hub.Spans.All() {
		if sp.From == "" || sp.To == "" || now.Sub(sp.Time) > observedWindow {
			continue
		}
		f, t := normID(sp.From), normID(sp.To)
		i, ok := idx[pair{f, t}]
		if !ok {
			i = add(f, t, sp.Kind, map[bool]string{true: "d", false: "o"}[sp.Kind == core.SpanRedrive])
		}
		if now.Sub(sp.Time) <= liveWindow {
			m.edges[i].Live = true
			m.edges[i].Bad = m.edges[i].Bad || sp.Err != ""
		}
		if sp.Err != "" {
			fails[t]++
		}
	}
	for id, n := range fails {
		m.byID[id].Err, m.byID[id].Ct = true, plural(n, "error")
	}
	ui.mapCounts(m)
	return m
}

// mapItems says which column of a service's table counts the items a node holds.
var mapItems = map[string]struct{ table, col, noun, typ string }{
	"sqs": {"Queues", "Visible", "msg", "SQS queue"}, "sns": {"Topics", "Subscriptions", "sub", "SNS topic"},
	"s3": {"Buckets", "Objects", "object", "S3 bucket"}, "dynamodb": {"Tables", "Items", "item", "DynamoDB table"},
}

// mapCounts reads each node's count from the rows its service already lists.
// ponytail: rows are matched by name alone, so two regions' same-named queues share a count.
func (ui *UI) mapCounts(m *mapModel) {
	for svc, it := range mapItems {
		var nodes []*mapNode
		for _, n := range m.nodes {
			if n.Svc == svc && n.Type == it.typ {
				nodes = append(nodes, n)
			}
		}
		b, ok := ui.hub.Service(svc).(core.Browsable)
		if !ok || len(nodes) == 0 {
			continue
		}
		for _, t := range b.Resources() {
			col := slices.Index(t.Columns, it.col)
			if t.Name != it.table || col < 0 {
				continue
			}
			for _, row := range t.Rows {
				c, err := strconv.Atoi(row[col])
				for _, n := range nodes {
					if err == nil && n.Name == row[0] && n.Count == "" {
						n.Count = plural(c, it.noun)
					}
				}
			}
		}
	}
}

// layout puts the nodes of each visible cloud in a lane: a column by longest path from the
// sources, rows in order. It returns the lanes' tops and heights and the drawing's size.
func (m *mapModel) layout(cloud string) (lanes []mapLane, w, h int) {
	h, w = 0, 1000
	for _, c := range cloudLanes {
		if cloud != "" && cloud != c {
			continue
		}
		var ns []*mapNode
		for _, n := range m.nodes {
			if n.Cloud == c || c == core.AWS && n.Cloud == "" { // endpoints with no cloud sit with AWS
				ns = append(ns, n)
			}
		}
		if len(ns) == 0 {
			continue
		}
		in := map[*mapNode]bool{}
		for _, n := range ns {
			in[n] = true
		}
		for round := 0; round < len(ns); round++ { // longest path; the cap ends cycles
			moved := false
			for _, e := range m.edges {
				f, t := m.byID[e.From], m.byID[e.To]
				if in[f] && in[t] && t.col <= f.col && f.col < len(ns)-1 {
					t.col, moved = f.col+1, true
				}
			}
			if !moved {
				break
			}
		}
		slices.SortStableFunc(ns, func(a, b *mapNode) int { return a.col - b.col })
		rows := map[int]int{}
		maxRows := 0
		for _, n := range ns {
			n.row = rows[n.col]
			rows[n.col]++
			maxRows = max(maxRows, rows[n.col])
			w = max(w, 30+(n.col+1)*mapColStep)
		}
		lh := 34 + (maxRows-1)*mapRowStep + mapH + 18
		for _, n := range ns {
			n.x, n.y = 30+n.col*mapColStep, h+34+n.row*mapRowStep
		}
		lanes = append(lanes, mapLane{c, h, lh})
		h += lh + 12
	}
	return lanes, w, h
}

type mapLane struct {
	cloud string
	y, h  int
}

// svg draws the model. Nodes outside the highlight are dimmed; edges of a flow are kept.
func (m *mapModel) svg(cloud, hl string, flowNodes map[string]bool, hot map[string]bool) template.HTML {
	lanes, w, h := m.layout(cloud)
	var b strings.Builder
	fmt.Fprintf(&b, `<svg class="map" viewBox="0 0 %d %d"><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--faint)"/></marker>`+
		`<marker id="arrowd" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--bad)"/></marker></defs>`, w, h)
	names := map[string]string{core.AWS: "AWS", core.GCP: "GOOGLE CLOUD", core.Azure: "AZURE"}
	for _, l := range lanes {
		fmt.Fprintf(&b, `<rect class="lane" x="0" y="%d" width="%d" height="%d" opacity=".55"/><text class="lanel" x="14" y="%d"><tspan fill="var(--%s)">●</tspan> %s</text>`, l.y, w, l.h, l.y+20, l.cloud, names[l.cloud])
	}
	hl = strings.ToLower(hl)
	active := hl != "" || flowNodes != nil
	lit := func(n *mapNode) bool {
		if flowNodes != nil {
			return flowNodes[n.ID]
		}
		return hl == "" || strings.Contains(strings.ToLower(n.Name+" "+n.Type+" "+n.ID), hl)
	}
	visible := func(n *mapNode) bool {
		return cloud == "" || n.Cloud == cloud || n.Cloud == "" && cloud == core.AWS
	}
	for i, e := range m.edges {
		s, t := m.byID[e.From], m.byID[e.To]
		if !visible(s) || !visible(t) {
			continue
		}
		x1, y1, x2, y2 := s.x+mapW, s.y+mapH/2, t.x, t.y+mapH/2
		mx := (x1 + x2) / 2
		d := fmt.Sprintf("M%d,%d C%d,%d %d,%d %d,%d", x1, y1, mx, y1, mx, y2, x2, y2)
		if x2 <= x1 { // against the flow of columns: leave from the bottom
			d = fmt.Sprintf("M%d,%d C%d,%d %d,%d %d,%d", s.x+mapW/2, s.y+mapH, s.x+mapW/2, y2, x2-40, y2, x2, y2)
		}
		cls := map[string]string{"c": "", "o": " observed", "d": " dlq"}[e.Kind]
		if active && !(lit(s) && lit(t)) || hot != nil && !hot[e.From+"|"+e.To] {
			cls += " dim"
		}
		marker := map[bool]string{true: "arrowd", false: "arrow"}[e.Kind == "d"]
		fmt.Fprintf(&b, `<path class="edge%s" d="%s" marker-end="url(#%s)"/><text class="elabel%s" x="%d" y="%d" text-anchor="middle">%s</text>`,
			cls, d, marker, map[bool]string{true: " dim", false: ""}[strings.HasSuffix(cls, " dim")], mx, (y1+y2)/2-6, html.EscapeString(e.Label))
		if e.Live {
			dur := 1.6 + float64(i%4)*.35
			if e.Bad {
				dur = 3.4
			}
			fmt.Fprintf(&b, `<circle class="pulse%s" r="3.5"><animateMotion dur="%.2fs" repeatCount="indefinite" path="%s"/></circle>`, map[bool]string{true: " bad", false: ""}[e.Bad], dur, d)
		}
	}
	for _, n := range m.nodes {
		if !visible(n) {
			continue
		}
		cls := "node"
		if n.Err {
			cls += " err"
		}
		if active && !lit(n) {
			cls += " dim"
		}
		cloud := n.Cloud
		if cloud == "" {
			cloud = core.AWS
		}
		fmt.Fprintf(&b, `<a class="%s" href="/_pogi/map/node?id=%[2]s" data-drawer="/_pogi/map/node?id=%[2]s"><title>%s</title><rect x="%d" y="%d" width="%d" height="%d" rx="8"/>`+
			`<circle cx="%d" cy="%d" r="3.5" fill="var(--%s)"/><text class="ty" x="%d" y="%.1f">%s</text><text class="nm" x="%d" y="%d">%s</text>`,
			cls, url.QueryEscape(n.ID), html.EscapeString(n.Type+" "+n.ID), n.x, n.y, mapW, mapH, n.x+14, n.y+16, cloud, n.x+24, float64(n.y)+19.5, html.EscapeString(n.Type), n.x+12, n.y+38, html.EscapeString(mapClip(n.Name, 21)))
		if ct := cmp.Or(n.Ct, n.Count); ct != "" { // errors first, else what the resource holds
			fmt.Fprintf(&b, `<text class="ct" x="%d" y="%.1f" text-anchor="end"%s>%s</text>`, n.x+mapW-10, float64(n.y)+19.5, map[bool]string{true: ` fill="var(--bad)"`, false: ""}[n.Err], html.EscapeString(ct))
		}
		b.WriteString("</a>")
	}
	b.WriteString("</svg>")
	return template.HTML(b.String())
}

func mapClip(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// traceNodes are the resources and edges a flow's spans pass through.
func (ui *UI) traceNodes(trace string) (nodes, edges map[string]bool) {
	nodes, edges = map[string]bool{}, map[string]bool{}
	for _, sp := range ui.hub.Spans.All() {
		if sp.Trace == trace && sp.From != "" && sp.To != "" {
			f, t := normID(sp.From), normID(sp.To)
			nodes[f], nodes[t], edges[f+"|"+t] = true, true, true
		}
	}
	return nodes, edges
}

// unconnected counts the queues, topics, buckets, functions and rules that have no link.
func (ui *UI) unconnected(m *mapModel) int {
	total := 0
	for _, svc := range ui.hub.Services() {
		if _, ok := svc.(core.Linker); !ok {
			continue
		}
		b, ok := svc.(core.Browsable)
		if !ok {
			continue
		}
		for _, t := range b.Resources() {
			switch t.Name {
			case "Queues", "Topics", "Buckets", "Functions", "Rules":
				total += len(t.Rows)
			}
		}
	}
	linked := 0
	for _, n := range m.nodes {
		switch n.Type {
		case "SQS queue", "SNS topic", "S3 bucket", "Lambda", "EB rule":
			linked++
		}
	}
	return max(0, total-linked)
}

// mapDraw builds the map for the query's cloud, highlight and flow.
func (ui *UI) mapDraw(r *http.Request) (pg mapPage, m *mapModel) {
	q := r.URL.Query()
	pg = mapPage{Cloud: ui.cloud(r), Hl: strings.TrimSpace(q.Get("hl")), Trace: q.Get("trace")}
	m = ui.buildMapModel()
	pg.Empty = len(m.edges) == 0
	var fn, fe map[string]bool
	if pg.Trace != "" {
		fn, fe = ui.traceNodes(pg.Trace)
	}
	pg.SVG = m.svg(pg.Cloud, pg.Hl, fn, fe)
	return pg, m
}

func (ui *UI) mapPage(w http.ResponseWriter, r *http.Request) {
	pg, m := ui.mapDraw(r)
	if r.URL.Query().Get("frag") != "" { // the 3 s poll swaps only the drawing
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, pg.SVG)
		return
	}
	pg.Hidden = ui.unconnected(m)
	ui.render(w, r, "map", "Map", "map", pg)
}

// exportColors are the light theme's colors, for an SVG that has no stylesheet to ask.
var exportColors = strings.NewReplacer("var(--panel)", "#ffffff", "var(--sunken)", "#f4f4f5", "var(--line2)", "#d4d4d8", "var(--text)", "#18181b",
	"var(--dim)", "#52525b", "var(--faint)", "#6b6b74", "var(--accent)", "#c4176f", "var(--bad)", "#b91c1c", "var(--aws)", "#ff9900",
	"var(--gcp)", "#4285f4", "var(--azure)", "#00a2ed")

const exportStyle = `<style>.lane{fill:#f4f4f5}.lanel{fill:#a1a1aa;font:600 10.5px system-ui;letter-spacing:.08em}.node rect{fill:#ffffff;stroke:#d4d4d8;stroke-width:1.2}` +
	`.node.err rect{stroke:#dc2626}.node.dim,.edge.dim,.elabel.dim{opacity:.2}.ty{fill:#a1a1aa;font:10.5px system-ui}.nm{fill:#18181b;font:600 12px ui-monospace,Menlo,monospace}` +
	`.ct{fill:#52525b;font:10.5px ui-monospace,Menlo,monospace}.edge{fill:none;stroke:#a1a1aa;stroke-width:1.5;opacity:.7}.edge.observed{stroke-dasharray:2 4}` +
	`.edge.dlq{stroke:#dc2626;stroke-dasharray:6 4}.elabel{fill:#a1a1aa;font:10px system-ui;paint-order:stroke;stroke:#ffffff;stroke-width:4px}` +
	`.pulse{fill:#c4176f}.pulse.bad{fill:#b91c1c}</style>`

// mapSVG downloads the current map as a standalone SVG: the stylesheet is inlined with the light theme's colors.
func (ui *UI) mapSVG(w http.ResponseWriter, r *http.Request) {
	pg, _ := ui.mapDraw(r)
	svg := strings.Replace(string(pg.SVG), `<svg class="map" `, `<svg xmlns="http://www.w3.org/2000/svg" class="map" `, 1)
	i := strings.Index(svg, ">") + 1
	svg = exportColors.Replace(svg[:i] + exportStyle + svg[i:])
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Content-Disposition", `attachment; filename="pogi-map`+map[bool]string{true: "", false: "-" + pg.Cloud}[pg.Cloud == ""]+`.svg"`)
	fmt.Fprint(w, svg)
}

func (ui *UI) mapNode(w http.ResponseWriter, r *http.Request) {
	id := normID(r.URL.Query().Get("id"))
	m := ui.buildMapModel()
	n := m.byID[id]
	if n == nil {
		back(w, r, "/_pogi/map", fmt.Errorf("%s is not on the map", id))
		return
	}
	pg := mapPage{Node: n}
	for _, e := range m.edges {
		switch {
		case e.To == id:
			pg.In = append(pg.In, mapNodeLink{m.byID[e.From], e.Label, e.Kind})
		case e.From == id:
			pg.Out = append(pg.Out, mapNodeLink{m.byID[e.To], e.Label, e.Kind})
		}
	}
	for _, f := range ui.buildFlows() {
		if len(pg.Flows) < 5 && slices.ContainsFunc(f.Steps, func(s flowStep) bool { return s.kind != "" && (normID(s.from) == id || normID(s.to) == id) }) {
			pg.Flows = append(pg.Flows, f)
		}
	}
	if ui.hub.Service(n.Svc) != nil {
		pg.Open = "/_pogi/resources/" + n.Svc
	}
	ui.renderX(w, r, "map", n.Name, "map", n.Type+" · "+cloudNames[n.Cloud], pg)
}
