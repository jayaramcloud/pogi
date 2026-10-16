package web

// Step Functions execution graphs: an SVG drawn from the state machine's
// definition, each state coloured by what the execution's history says.
// ponytail: top-level states only (Parallel branches and Map iterators are one
// node each), layers by distance from StartAt; the upgrade is nested layouts.

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

const (
	nodeW, nodeH = 170, 40
	gapX, gapY   = 30, 56
)

type gnode struct {
	Name, Type, Status string
	X, Y               int
}

type gedge struct {
	D, Class string
}

type graph struct {
	Nodes         []gnode
	Edges         []gedge
	Width, Height int
}

type sfnData struct {
	Machines   []choice
	Machine    string
	Executions []any
	Execution  string
	Status     string
	Graph      *graph
	Err        string
}

func (ui *UI) sfnPage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	d := &sfnData{Machine: q.Get("machine"), Execution: q.Get("execution")}
	for _, m := range ui.rows(r, "stepfunctions", "State machines") {
		d.Machines = append(d.Machines, choice{m[0], m[0] + " (" + m[3] + ")"})
		if d.Machine == "" {
			d.Machine = m[0]
		}
	}
	if m := ui.inspectMap("stepfunctions", "State machines", d.Machine); m != nil {
		d.Executions, _ = m["executions"].([]any)
		def, _ := m["definition"].(map[string]any)
		var statuses map[string]string
		if d.Execution != "" {
			if e := ui.inspectMap("stepfunctions", "Executions", d.Execution); e != nil {
				d.Status = dig(e, "status")
				events, _ := e["history"].([]any)
				statuses = stateStatus(def, events, d.Status)
			} else {
				d.Err = "execution not found"
			}
		}
		if def == nil {
			d.Err = "the state machine definition is not readable"
		} else {
			d.Graph = layout(def, statuses)
		}
	}
	ui.xrender(w, r, "x-sfn", "Step Functions", "stepfunctions", d)
}

// stateStatus walks an execution's history and says, per state name,
// "running", "succeeded" or "failed".
func stateStatus(def map[string]any, events []any, execStatus string) map[string]string {
	st := map[string]string{}
	cur := ""
	for _, e := range events {
		ev, _ := e.(map[string]any)
		typ, _ := ev["type"].(string)
		name := dig(ev, "stateEnteredEventDetails", "name") + dig(ev, "stateExitedEventDetails", "name")
		switch {
		case strings.HasSuffix(typ, "StateEntered"):
			cur = name
			st[name] = "running"
			if typ == "FailStateEntered" {
				st[name] = "failed"
			}
		case strings.HasSuffix(typ, "StateExited"):
			if st[name] != "failed" {
				st[name] = "succeeded"
			}
		case strings.HasPrefix(typ, "Execution"):
		case strings.HasSuffix(typ, "Failed") || strings.HasSuffix(typ, "TimedOut") || strings.HasSuffix(typ, "Aborted"):
			if cur != "" {
				st[cur] = "failed"
			}
		}
	}
	states, _ := def["States"].(map[string]any)
	for n, s := range st {
		switch {
		case s != "running" || execStatus == "RUNNING":
		case dig(states[n], "Type") == "Succeed" || execStatus == "SUCCEEDED": // an ended execution leaves nothing running
			st[n] = "succeeded"
		default:
			st[n] = "failed"
		}
	}
	return st
}

// successors lists the states a state can go to: its transitions, and its Catch targets.
func successors(s any) (next, catch []string) {
	m, _ := s.(map[string]any)
	for _, k := range []string{"Next", "Default"} {
		if n := dig(m, k); n != "" {
			next = append(next, n)
		}
	}
	for _, key := range []string{"Choices", "Catch"} {
		list, _ := m[key].([]any)
		for _, c := range list {
			if n := dig(c, "Next"); n != "" {
				if key == "Choices" {
					next = append(next, n)
				} else {
					catch = append(catch, n)
				}
			}
		}
	}
	return
}

// layout places the states in layers (breadth first from StartAt) and draws an
// edge for every transition.
func layout(def map[string]any, status map[string]string) *graph {
	states, _ := def["States"].(map[string]any)
	start := dig(def, "StartAt")
	level := map[string]int{}
	var order []string
	if states[start] != nil {
		level[start] = 0
		order = []string{start}
	}
	for i := 0; i < len(order); i++ {
		n := order[i]
		nx, ca := successors(states[n])
		for _, t := range append(nx, ca...) {
			if _, seen := level[t]; !seen && states[t] != nil {
				level[t] = level[n] + 1
				order = append(order, t)
			}
		}
	}
	for _, n := range core.SortedKeys(states) { // unreachable states go in a layer of their own
		if _, ok := level[n]; !ok {
			level[n] = len(order)
			order = append(order, n)
		}
	}
	g := &graph{}
	perLayer, pos := map[int]int{}, map[string]*gnode{}
	maxLayer, maxCol := 0, 0
	for _, n := range order {
		l := level[n]
		col := perLayer[l]
		perLayer[l]++
		g.Nodes = append(g.Nodes, gnode{Name: n, Type: dig(states[n], "Type"), Status: status[n], X: 20 + col*(nodeW+gapX), Y: 20 + l*(nodeH+gapY)})
		maxLayer, maxCol = max(maxLayer, l), max(maxCol, col)
	}
	for i := range g.Nodes {
		pos[g.Nodes[i].Name] = &g.Nodes[i]
	}
	for _, n := range order {
		nx, ca := successors(states[n])
		for _, t := range nx {
			if pos[t] != nil {
				g.Edges = append(g.Edges, gedge{edgePath(pos[n], pos[t]), ""})
			}
		}
		for _, t := range ca {
			if pos[t] != nil {
				g.Edges = append(g.Edges, gedge{edgePath(pos[n], pos[t]), "catch"})
			}
		}
	}
	g.Width, g.Height = 40+(maxCol+1)*(nodeW+gapX)+60, 40+(maxLayer+1)*(nodeH+gapY)
	return g
}

// edgePath is a curve from the bottom of a to the top of b, or a loop round
// the right side when b is not below a.
func edgePath(a, b *gnode) string {
	if b.Y > a.Y {
		x1, y1, x2, y2 := a.X+nodeW/2, a.Y+nodeH, b.X+nodeW/2, b.Y
		return fmt.Sprintf("M%d,%d C%d,%d %d,%d %d,%d", x1, y1, x1, y1+28, x2, y2-28, x2, y2)
	}
	x1, y1, x2, y2 := a.X+nodeW, a.Y+nodeH/2, b.X+nodeW, b.Y+nodeH/2
	out := max(x1, x2) + 50
	return fmt.Sprintf("M%d,%d C%d,%d %d,%d %d,%d", x1, y1, out, y1, out, y2, x2, y2)
}
