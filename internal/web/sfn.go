package web

// Step Functions execution graphs: an SVG drawn from the state machine's
// definition, each state coloured by what the execution's history says.
// ponytail: top-level states only (Parallel branches and Map iterators are one
// node each), layers by distance from StartAt; the upgrade is nested layouts.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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

func (ui *UI) sfnPage(w http.ResponseWriter, r *http.Request) { ui.sfnRender(w, r, "") }

func (ui *UI) sfnRender(w http.ResponseWriter, r *http.Request, errMsg string) {
	q := r.URL.Query()
	d := &sfnData{Machine: q.Get("machine"), Execution: q.Get("execution"), Err: errMsg}
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

// machineRef finds a state machine by name among the rows: its account and region make its ARN.
func (ui *UI) machineRef(r *http.Request, name string) (arn, account, region string, ok bool) {
	for _, m := range ui.rows(r, "stepfunctions", "State machines") {
		if m[0] == name {
			return core.ARN("states", m[1], m[2], "stateMachine:"+name), m[2], m[1], true
		}
	}
	return "", "", "", false
}

// sfnFail answers a failed post from the Executions tab: an error toast, and the tab stays as it is.
func sfnFail(w http.ResponseWriter, err error) {
	w.Header().Set("HX-Reswap", "none")
	trigger(w, "pogi-toast", map[string]string{"msg": errText(err), "kind": "bad"})
}

// sfnStart starts an execution and shows it, coloured, in the graph.
func (ui *UI) sfnStart(w http.ResponseWriter, r *http.Request) {
	machine, name, input := r.FormValue("machine"), strings.TrimSpace(r.FormValue("name")), strings.TrimSpace(r.FormValue("input"))
	arn, acct, region, ok := ui.machineRef(r, machine)
	switch {
	case !ok:
		sfnFail(w, fmt.Errorf("Pick a state machine"))
		return
	case len(name) > 80:
		sfnFail(w, fmt.Errorf("The execution name is too long: use 80 characters or fewer"))
		return
	}
	if input == "" {
		input = "{}"
	}
	if !json.Valid([]byte(input)) {
		sfnFail(w, fmt.Errorf("The input is not valid JSON"))
		return
	}
	call := map[string]any{"stateMachineArn": arn, "input": input}
	if name != "" {
		call["name"] = name
	}
	var out struct{ ExecutionArn string }
	if err := ui.hub.ConsoleCall(acct, region, "AWSStepFunctions", "StartExecution", call, &out); err != nil {
		sfnFail(w, err)
		return
	}
	started := out.ExecutionArn[strings.LastIndex(out.ExecutionArn, ":")+1:]
	r.URL.RawQuery = "machine=" + url.QueryEscape(machine) + "&execution=" + url.QueryEscape(started)
	trigger(w, "pogi-toast", map[string]string{"msg": "Execution " + started + " started on " + machine})
	ui.sfnRender(w, r, "")
}

// sfnStop stops a running execution.
func (ui *UI) sfnStop(w http.ResponseWriter, r *http.Request) {
	machine, name := r.FormValue("machine"), r.FormValue("execution")
	_, acct, region, ok := ui.machineRef(r, machine)
	if !ok || name == "" {
		sfnFail(w, fmt.Errorf("Pick an execution of a state machine"))
		return
	}
	arn := core.ARN("states", region, acct, "execution:"+machine+":"+name)
	if err := ui.hub.ConsoleCall(acct, region, "AWSStepFunctions", "StopExecution", map[string]any{"executionArn": arn, "cause": "Stopped from the console"}, nil); err != nil {
		sfnFail(w, err)
		return
	}
	r.URL.RawQuery = "machine=" + url.QueryEscape(machine) + "&execution=" + url.QueryEscape(name)
	trigger(w, "pogi-toast", map[string]string{"msg": "Execution " + name + " stopped"})
	ui.sfnRender(w, r, "")
}
