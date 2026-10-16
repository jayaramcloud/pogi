package stepfunctions

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// MaxWait caps every real sleep (Wait states and Retry backoff) so local runs never stall.
// This is on purpose, a known difference from AWS (a Wait of 3600 seconds ends
// after 60), listed in docs/reference/coverage.yaml. Do not remove it without
// the owner's say-so.
var MaxWait = 60 * time.Second

// RetrySpeedup divides Retry intervals: 10 makes retries ten times faster than on AWS.
var RetrySpeedup = 1.0

// ---- definitions ----

// Def is a state machine, a Parallel branch or a Map item processor.
type Def struct {
	StartAt        string
	States         map[string]*State
	TimeoutSeconds int
	QueryLanguage  string
	lang           string // the top-level language, for every nested list too (see setLang)
}

// setLang gives every state its query language: its own, or the top-level one.
// Branches and item processors always take the top-level language.
func (d *Def) setLang(lang string) {
	d.lang = lang
	for _, st := range d.States {
		if st == nil {
			continue
		}
		st.lang = cmp.Or(st.QueryLanguage, lang)
		for _, b := range st.Branches {
			if b != nil {
				b.setLang(lang)
			}
		}
		for _, p := range []*Def{st.Iterator, st.ItemProcessor} {
			if p != nil {
				p.setLang(lang)
			}
		}
	}
}

// State is one ASL state; the fields its type does not use stay zero.
type State struct {
	Type, Next, Default, Resource                string
	End                                          bool
	QueryLanguage                                string
	lang                                         string // QueryLanguage or the top-level one (see setLang)
	InputPath, OutputPath, ResultPath, ItemsPath jpath
	Parameters, ResultSelector, ItemSelector     any
	Arguments, Output, Items                     any // JSONata states
	Assign, ItemReader                           any
	Result                                       json.RawMessage
	TimeoutSeconds, HeartbeatSeconds, Seconds    num
	TimeoutSecondsPath                           string
	Retry                                        []Retrier
	Catch                                        []Catcher
	Choices                                      []map[string]any
	SecondsPath, Timestamp, TimestampPath        string
	Error, Cause, ErrorPath, CausePath           string
	Branches                                     []*Def
	Iterator, ItemProcessor                      *Def
	MaxConcurrency                               num
	MaxConcurrencyPath                           string
	ToleratedFailureCount                        num // Map: checked, not applied (a failed item still fails the Map)
	ToleratedFailurePercentage                   num
	ToleratedFailureCountPath                    string
	ToleratedFailurePercentagePath               string
}

// num is a number field. A JSONata state may give an expression string instead.
type num struct {
	f    float64
	set  bool
	expr string
}

func (n *num) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	n.set = true
	if b[0] == '"' {
		return json.Unmarshal(b, &n.expr)
	}
	return json.Unmarshal(b, &n.f)
}

// jpath is a reference path that tells "absent" (means $) from JSON null (means {} or "discard").
type jpath struct {
	p    string
	null bool
}

func (j *jpath) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		j.null = true
		return nil
	}
	return json.Unmarshal(b, &j.p)
}

func (j jpath) path() string { return cmp.Or(j.p, "$") }

func (j jpath) isSet() bool { return j.p != "" || j.null }

// Retrier is one Retry entry.
type Retrier struct {
	ErrorEquals     []string
	IntervalSeconds *float64
	MaxAttempts     *int
	BackoffRate     *float64
	MaxDelaySeconds float64
}

func (r Retrier) attempts() int {
	if r.MaxAttempts == nil {
		return 3
	}
	return *r.MaxAttempts
}

// delay is the sleep before retry n (0-based), sped up by RetrySpeedup.
func (r Retrier) delay(n int) time.Duration {
	interval, rate := 1.0, 2.0
	if r.IntervalSeconds != nil {
		interval = *r.IntervalSeconds
	}
	if r.BackoffRate != nil {
		rate = *r.BackoffRate
	}
	d := interval * math.Pow(rate, float64(n))
	if r.MaxDelaySeconds > 0 {
		d = min(d, r.MaxDelaySeconds)
	}
	return seconds(d / RetrySpeedup)
}

// Catcher is one Catch entry.
type Catcher struct {
	ErrorEquals    []string
	ResultPath     jpath
	Next           string
	Output, Assign any // Output is JSONata only
}

// diag is a ValidateStateMachineDefinition diagnostic.
type diag struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
}

// parseDef parses and validates a definition.
func parseDef(src string) (*Def, []diag) {
	if !json.Valid([]byte(src)) {
		return nil, []diag{{"ERROR", "INVALID_JSON_DESCRIPTION", "The definition is not valid JSON", ""}}
	}
	var d Def
	if err := json.Unmarshal([]byte(src), &d); err != nil {
		return nil, []diag{{"ERROR", "SCHEMA_VALIDATION_FAILED", err.Error(), ""}}
	}
	d.setLang(cmp.Or(d.QueryLanguage, "JSONPath"))
	return &d, d.validate("")
}

var stateTypes = map[string]bool{"Pass": true, "Task": true, "Choice": true, "Wait": true, "Succeed": true, "Fail": true, "Parallel": true, "Map": true}

func (d *Def) validate(at string) (out []diag) {
	bad := func(code, loc, format string, a ...any) {
		out = append(out, diag{"ERROR", code, fmt.Sprintf(format, a...), loc})
	}
	target := func(loc, next string) {
		if d.States[next] == nil {
			bad("MISSING_TRANSITION_TARGET", loc, "Missing 'Next' target: %s", next)
		}
	}
	if d.QueryLanguage != "" && at != "" {
		bad("SCHEMA_VALIDATION_FAILED", at+"/QueryLanguage", "The field 'QueryLanguage' is only allowed at the top level of the definition")
	} else if d.QueryLanguage != "" && d.QueryLanguage != "JSONPath" && d.QueryLanguage != "JSONata" {
		bad("SCHEMA_VALIDATION_FAILED", at+"/QueryLanguage", "The field 'QueryLanguage' should be JSONPath or JSONata")
	}
	if len(d.States) == 0 {
		bad("SCHEMA_VALIDATION_FAILED", at+"/States", "The field 'States' should be non-empty")
	}
	if d.StartAt == "" {
		bad("SCHEMA_VALIDATION_FAILED", at+"/StartAt", "The field 'StartAt' should be non-null")
	} else {
		target(at+"/StartAt", d.StartAt)
	}
	for _, name := range core.SortedKeys(d.States) {
		st, loc := d.States[name], at+"/States/"+name
		switch {
		case st == nil || !stateTypes[st.Type]:
			bad("SCHEMA_VALIDATION_FAILED", loc+"/Type", "The field 'Type' should be one of Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map")
			continue
		}
		out = append(out, st.validateLang(loc, d.lang)...)
		switch st.Type {
		case "Succeed", "Fail":
		case "Choice":
			if len(st.Choices) == 0 {
				bad("SCHEMA_VALIDATION_FAILED", loc+"/Choices", "The field 'Choices' should be non-empty")
			}
			for i, c := range st.Choices {
				next, _ := c["Next"].(string)
				target(fmt.Sprintf("%s/Choices/%d/Next", loc, i), next)
			}
			if st.Default != "" {
				target(loc+"/Default", st.Default)
			}
		default:
			if st.End == (st.Next != "") {
				bad("SCHEMA_VALIDATION_FAILED", loc, "Exactly one of 'Next' or 'End' is required")
			} else if !st.End {
				target(loc+"/Next", st.Next)
			}
		}
		switch st.Type {
		case "Task":
			if _, _, _, ok := parseResource(st.Resource); !ok {
				bad("SCHEMA_VALIDATION_FAILED", loc+"/Resource", "Value is not a valid resource ARN: %q", st.Resource)
			}
		case "Wait":
			n := 0
			for _, set := range []bool{st.Seconds.set, st.SecondsPath != "", st.Timestamp != "", st.TimestampPath != ""} {
				if set {
					n++
				}
			}
			if n != 1 {
				bad("SCHEMA_VALIDATION_FAILED", loc, "A Wait state needs exactly one of Seconds, SecondsPath, Timestamp or TimestampPath")
			}
		case "Parallel":
			if len(st.Branches) == 0 {
				bad("SCHEMA_VALIDATION_FAILED", loc+"/Branches", "The field 'Branches' should be non-empty")
			}
			for i, b := range st.Branches {
				if b == nil {
					b = &Def{}
				}
				out = append(out, b.validate(fmt.Sprintf("%s/Branches/%d", loc, i))...)
			}
		case "Map":
			if p := cmp.Or(st.ItemProcessor, st.Iterator); p == nil {
				bad("SCHEMA_VALIDATION_FAILED", loc, "The field 'ItemProcessor' should be non-null")
			} else {
				out = append(out, p.validate(loc+"/ItemProcessor")...)
			}
			for _, f := range []struct {
				name string
				lit  num
				path string
				max  float64
			}{{"ToleratedFailureCount", st.ToleratedFailureCount, st.ToleratedFailureCountPath, math.MaxFloat64},
				{"ToleratedFailurePercentage", st.ToleratedFailurePercentage, st.ToleratedFailurePercentagePath, 100}} {
				switch {
				case f.lit.set && f.path != "":
					bad("SCHEMA_VALIDATION_FAILED", loc, "Only one of '%s' and '%s' is allowed", f.name, f.name+"Path")
				case f.lit.set && f.lit.expr == "" && (f.lit.f < 0 || f.lit.f > f.max):
					bad("SCHEMA_VALIDATION_FAILED", loc+"/"+f.name, "The field '%s' is out of range", f.name)
				}
			}
		}
		for i, c := range st.Catch {
			target(fmt.Sprintf("%s/Catch/%d/Next", loc, i), c.Next)
		}
		// AWS accepts TimeoutSeconds on Task only, and Retry and Catch on Task, Parallel and Map only.
		if st.TimeoutSeconds.set && st.Type != "Task" {
			bad("SCHEMA_VALIDATION_FAILED", loc, "Field 'TimeoutSeconds' is not supported")
		}
		if len(st.Retry)+len(st.Catch) > 0 && st.Type != "Task" && st.Type != "Parallel" && st.Type != "Map" {
			bad("SCHEMA_VALIDATION_FAILED", loc, "Field 'Retry' and 'Catch' are not supported")
		}
	}
	if len(out) == 0 { // like AWS, report the flow problems only once the fields are fine
		out = d.checkFlow(at)
	}
	return out
}

// checkFlow finds a workflow with no terminal state, and states that nothing
// leads to (the checks AWS makes after the field checks).
func (d *Def) checkFlow(at string) (out []diag) {
	terminal, seen := false, map[string]bool{}
	var visit func(name string)
	visit = func(name string) {
		st := d.States[name]
		if seen[name] || st == nil {
			return
		}
		seen[name] = true
		next := []string{st.Next, st.Default}
		for _, c := range st.Choices {
			n, _ := c["Next"].(string)
			next = append(next, n)
		}
		for _, c := range st.Catch {
			next = append(next, c.Next)
		}
		for _, n := range next {
			if n != "" {
				visit(n)
			}
		}
	}
	for _, st := range d.States {
		terminal = terminal || st != nil && (st.End || st.Type == "Succeed" || st.Type == "Fail")
	}
	if !terminal && len(d.States) > 0 {
		out = append(out, diag{"ERROR", "MISSING_END_STATE", "Workflow has no terminal state", at})
	}
	visit(d.StartAt)
	for _, name := range core.SortedKeys(d.States) {
		if !seen[name] && d.States[name] != nil && d.StartAt != "" && d.States[d.StartAt] != nil {
			out = append(out, diag{"ERROR", "MISSING_TRANSITION_TARGET", fmt.Sprintf("State %q is not reachable.", name), at + "/States/" + name})
		}
	}
	return out
}

func defError(diags []diag) error {
	var parts []string
	for _, d := range diags {
		parts = append(parts, fmt.Sprintf("%s: %s at %s", d.Code, d.Message, cmp.Or(d.Location, "/")))
	}
	return core.Errorf(400, "InvalidDefinition", "Invalid State Machine Definition: '%s'", strings.Join(parts, ", "))
}

// parseResource splits a Task resource: ("function", …) for a Lambda ARN,
// ("activity", …), or an integration such as ("sqs", "sendMessage", "waitForTaskToken")
// or ("aws-sdk:dynamodb", "putItem", "").
func parseResource(arn string) (svc, action, mode string, ok bool) {
	service, _, _, res, ok := core.ParseARN(arn)
	switch {
	case !ok:
		return "", "", "", false
	case service == "lambda":
		return "function", "", "", strings.HasPrefix(res, "function:")
	case service != "states":
		return "", "", "", false
	case strings.HasPrefix(res, "activity:"):
		return "activity", "", "", true
	}
	for _, m := range []string{".waitForTaskToken", ".sync:2", ".sync"} {
		if r, found := strings.CutSuffix(res, m); found {
			res, mode = r, m[1:]
			break
		}
	}
	i := strings.LastIndexByte(res, ':')
	if i <= 0 || i == len(res)-1 {
		return "", "", "", false
	}
	return res[:i], res[i+1:], mode, true
}

// ---- errors ----

// failure is an ASL error: it can be retried and caught, or it fails the execution.
type failure struct{ name, cause string }

func (f *failure) Error() string { return f.name + ": " + f.cause }

func runtimeErr(format string, a ...any) error {
	return &failure{"States.Runtime", fmt.Sprintf(format, a...)}
}

var (
	errAborted  = errors.New("execution stopped") // a thread saw its context end
	errStopped  = errors.New("execution aborted") // context cause: StopExecution, Reset, Close
	errTimedOut = errors.New("execution timed out")
)

// matches applies ErrorEquals; States.Runtime is terminal and never matches.
func matches(names []string, err string) bool {
	if err == "States.Runtime" || err == "States.DataLimitExceeded" {
		return false
	}
	for _, n := range names {
		if n == err || n == "States.ALL" || n == "States.TaskFailed" && !strings.HasSuffix(err, "Timeout") {
			return true
		}
	}
	return false
}

// ---- interpreter ----

// shared is what every thread of one execution reads.
type shared struct {
	s     *Service
	ex    *Execution
	input any  // parsed execution input, $$.Execution.Input
	test  bool // TestState: no sleeping, no retries
}

// thread walks one list of states: the execution, a Parallel branch or a Map iteration.
type thread struct {
	*shared
	ctx      context.Context
	prev     int // id of this thread's last event
	state    string
	entered  time.Time
	enterID  int // id of the StateEntered event
	retries  int
	vars     map[string]any // variables set by Assign; a branch or an iteration gets a copy
	assigned map[string]any // what the current state assigned, for its StateExited event
	token    string
	item     map[string]any // $$.Map.Item while evaluating an ItemSelector
	inspect  map[string]any // TestState inspection data
	status   string         // TestState: RETRIABLE
	caught   *failure       // TestState: the error a Catch handled
}

type result struct {
	v   any
	err error
}

var (
	notTruncated = map[string]any{"truncated": false}
	included     = map[string]any{"included": true}
)

// event appends to the execution history unless the execution already ended.
func (t *thread) event(typ string, details map[string]any) {
	t.s.mu.Lock()
	if t.ex.Status == "RUNNING" {
		t.prev = addEvent(t.ex, t.prev, typ, details)
	}
	t.s.mu.Unlock()
}

// addEvent appends a history event and returns its id; lock held.
func addEvent(ex *Execution, prev int, typ string, details map[string]any) int {
	id := len(ex.Events) + 1
	e := map[string]any{"timestamp": core.Time(time.Now()), "type": typ, "id": id, "previousEventId": prev}
	if details != nil {
		key := strings.ToLower(typ[:1]) + typ[1:] + "EventDetails"
		if strings.HasSuffix(typ, "StateEntered") {
			key = "stateEnteredEventDetails"
		} else if strings.HasSuffix(typ, "StateExited") {
			key = "stateExitedEventDetails"
		}
		e[key] = details
	}
	ex.Events = append(ex.Events, e)
	return id
}

func (t *thread) note(k string, v any) {
	if t.inspect != nil {
		t.inspect[k] = js(v)
	}
}

// ctxObj is the context object ($$) for the current state.
func (t *thread) ctxObj() map[string]any {
	c := map[string]any{
		"Execution": map[string]any{"Id": t.ex.ARN, "Input": t.input, "Name": t.ex.Name, "RoleArn": t.ex.RoleArn,
			"StartTime": iso(t.ex.Start), "RedriveCount": 0.0},
		"State":        map[string]any{"Name": t.state, "EnteredTime": iso(t.entered), "RetryCount": float64(t.retries)},
		"StateMachine": map[string]any{"Id": t.ex.MachineARN, "Name": nameOf(t.ex.MachineARN)},
	}
	if t.token != "" {
		c["Task"] = map[string]any{"Token": t.token}
	}
	if t.item != nil {
		c["Map"] = map[string]any{"Item": t.item}
	}
	return c
}

// maxEvents is the AWS history limit; an execution that reaches it fails.
const maxEvents = 25000

// exec runs states from StartAt until a terminal state and returns its output.
func (t *thread) exec(d *Def, input any) (any, error) {
	if d == nil {
		return nil, runtimeErr("missing state list")
	}
	for name := d.StartAt; ; {
		if t.ctx.Err() != nil {
			return nil, errAborted
		}
		if t.prev >= maxEvents { // a state loop that never blocks would grow forever
			return nil, runtimeErr("The execution reached the maximum number of history events (%d)", maxEvents)
		}
		st := d.States[name]
		if st == nil {
			return nil, runtimeErr("State '%s' does not exist", name)
		}
		out, next, err := t.step(name, st, input)
		if err != nil || next == "" {
			return out, err
		}
		name, input = next, out
	}
}

// step runs one state and returns its output and the next state ("" when it ends).
func (t *thread) step(name string, st *State, raw any) (out any, next string, err error) {
	t.state, t.entered, t.retries, t.token, t.assigned = name, time.Now().UTC(), 0, "", nil
	t.event(st.Type+"StateEntered", map[string]any{"name": name, "input": js(raw), "inputDetails": notTruncated})
	t.enterID = t.prev
	next = st.Next
	switch st.Type {
	case "Pass":
		out, err = t.pass(st, raw)
	case "Wait":
		out, err = t.waitState(st, raw)
	case "Choice":
		out, next, err = t.choice(st, raw)
	case "Succeed":
		if st.lang == "JSONata" {
			out, err = t.jfinish(st.Assign, st.Output, bind{input: raw}, raw)
		} else if out, err = t.inputPath(st, raw); err == nil {
			out, err = t.outputPath(st, out)
		}
		next = ""
	case "Fail":
		return nil, "", t.fail(st, raw)
	case "Task", "Parallel", "Map":
		out, next, err = t.retry(st, raw)
	default:
		return nil, "", runtimeErr("Unknown state type '%s'", st.Type)
	}
	if err != nil {
		if st.Type == "Task" && errors.Is(err, errAborted) { // cut by a sibling's failure or a stop
			t.event("TaskStateAborted", nil)
		}
		return nil, "", err
	}
	exited := map[string]any{"name": name, "output": js(out), "outputDetails": notTruncated}
	if t.assigned != nil {
		vars := make(map[string]string, len(t.assigned))
		for k, v := range t.assigned {
			vars[k] = js(v)
		}
		exited["assignedVariables"], exited["assignedVariablesDetails"] = vars, notTruncated
	}
	t.event(st.Type+"StateExited", exited)
	return out, next, nil
}

func (t *thread) inputPath(st *State, raw any) (any, error) {
	if st.InputPath.null {
		return map[string]any{}, nil
	}
	v, err := t.path(st.InputPath.path(), raw, "InputPath")
	if err == nil {
		t.note("afterInputPath", v)
	}
	return v, err
}

func (t *thread) params(st *State, eff any) (any, error) {
	if st.lang == "JSONata" {
		if st.Arguments == nil {
			return eff, nil
		}
		v, err := t.jtemplate(st.Arguments, "Arguments", bind{input: eff})
		if err == nil {
			t.note("afterArguments", v)
		}
		return v, err
	}
	if st.Parameters == nil {
		return eff, nil
	}
	v, err := t.template(st.Parameters, eff)
	if err == nil {
		t.note("afterParameters", v)
	}
	return v, err
}

func (t *thread) resultPath(rp jpath, raw, res any) (any, error) {
	if rp.null {
		return raw, nil
	}
	segs, err := parsePath(rp.path())
	if err != nil {
		return nil, runtimeErr("Invalid ResultPath: %v", err)
	}
	out, err := set(raw, segs, res)
	if err != nil {
		return nil, &failure{"States.ResultPathMatchFailure", fmt.Sprintf("Unable to apply ResultPath '%s' to input '%s'", rp.path(), js(raw))}
	}
	t.note("afterResultPath", out)
	return out, nil
}

func (t *thread) outputPath(st *State, v any) (any, error) {
	if st.OutputPath.null {
		return map[string]any{}, nil
	}
	return t.path(st.OutputPath.path(), v, "OutputPath")
}

// resultOut applies ResultPath and OutputPath.
func (t *thread) resultOut(st *State, raw, res any) (any, error) {
	out, err := t.resultPath(st.ResultPath, raw, res)
	if err != nil {
		return nil, err
	}
	return t.outputPath(st, out)
}

func (t *thread) pass(st *State, raw any) (any, error) {
	if st.lang == "JSONata" {
		return t.jfinish(st.Assign, st.Output, bind{input: raw}, raw)
	}
	eff, err := t.inputPath(st, raw)
	if err != nil {
		return nil, err
	}
	var res any
	if st.Result != nil {
		if err := json.Unmarshal(st.Result, &res); err != nil {
			return nil, runtimeErr("Invalid Result: %v", err)
		}
	} else if res, err = t.params(st, eff); err != nil {
		return nil, err
	}
	if err := t.assignPath(st.Assign, res); err != nil {
		return nil, err
	}
	return t.resultOut(st, raw, res)
}

func (t *thread) choice(st *State, raw any) (any, string, error) {
	if st.lang == "JSONata" {
		return t.jchoice(st, raw)
	}
	eff, err := t.inputPath(st, raw)
	if err != nil {
		return nil, "", err
	}
	next := st.Default
	for _, r := range st.Choices {
		ok, err := t.rule(r, eff)
		if err != nil {
			return nil, "", err
		}
		if ok {
			next, _ = r["Next"].(string)
			if err := t.assignPath(r["Assign"], eff); err != nil {
				return nil, "", err
			}
			break
		}
	}
	if next == "" {
		return nil, "", &failure{"States.NoChoiceMatched", "No Choice Rules matched and no Default was specified"}
	}
	out, err := t.outputPath(st, eff)
	return out, next, err
}

func (t *thread) waitState(st *State, raw any) (any, error) {
	eff, err := t.inputPath(st, raw)
	if err != nil {
		return nil, err
	}
	var d time.Duration
	switch {
	case st.Seconds.set:
		f, err := t.numField(st, st.Seconds, "Seconds", raw)
		if err != nil {
			return nil, err
		}
		if f < 0 {
			return nil, runtimeErr("The Seconds field must not be negative: %s", numStr(f))
		}
		d = seconds(f)
	case st.SecondsPath != "":
		v, err := t.path(st.SecondsPath, eff, "SecondsPath")
		if err != nil {
			return nil, err
		}
		f, ok := v.(float64)
		if !ok || f < 0 {
			return nil, runtimeErr("The SecondsPath parameter does not reference a valid number of seconds: %s", js(v))
		}
		d = seconds(f)
	default:
		var v any = st.Timestamp
		if src, ok := jsonataExpr(st.Timestamp); ok && st.lang == "JSONata" {
			if v, err = t.jeval(src, "Timestamp", bind{input: raw}); err != nil {
				return nil, err
			}
		} else if st.TimestampPath != "" {
			if v, err = t.path(st.TimestampPath, eff, "TimestampPath"); err != nil {
				return nil, err
			}
		}
		at, ok := timestamp(v)
		if !ok {
			return nil, runtimeErr("The Timestamp %s is not a valid ISO-8601 timestamp", js(v))
		}
		d = time.Until(at)
	}
	if err := t.sleep(d); err != nil {
		return nil, err
	}
	if st.lang == "JSONata" {
		return t.jfinish(st.Assign, st.Output, bind{input: raw}, raw)
	}
	if err := t.assignPath(st.Assign, eff); err != nil {
		return nil, err
	}
	return t.outputPath(st, eff)
}

func (t *thread) fail(st *State, raw any) error {
	f := &failure{st.Error, st.Cause}
	if st.lang == "JSONata" {
		for _, p := range []struct {
			set   *string
			field string
		}{{&f.name, "Error"}, {&f.cause, "Cause"}} {
			if src, ok := jsonataExpr(*p.set); ok {
				v, err := t.jeval(src, p.field, bind{input: raw})
				if err != nil {
					return err
				}
				*p.set = text(v)
			}
		}
		return f
	}
	if st.ErrorPath != "" {
		v, err := t.resolve(st.ErrorPath, raw, "ErrorPath")
		if err != nil {
			return err
		}
		f.name = text(v)
	}
	if st.CausePath != "" {
		v, err := t.resolve(st.CausePath, raw, "CausePath")
		if err != nil {
			return err
		}
		f.cause = text(v)
	}
	return f
}

// sleep waits (capped at MaxWait) unless the execution stops first.
func (t *thread) sleep(d time.Duration) error {
	if t.test || d <= 0 {
		return nil
	}
	tm := time.NewTimer(min(d, MaxWait))
	defer tm.Stop()
	select {
	case <-tm.C:
		return nil
	case <-t.ctx.Done():
		return errAborted
	}
}

// retry runs a Task, Parallel or Map state with its Retry and Catch rules.
func (t *thread) retry(st *State, raw any) (any, string, error) {
	tries := make([]int, len(st.Retry))
	for {
		out, err := t.attempt(st, raw)
		var f *failure
		if !errors.As(err, &f) {
			return out, st.Next, err
		}
		i := slices.IndexFunc(st.Retry, func(r Retrier) bool { return matches(r.ErrorEquals, f.name) })
		if i >= 0 && tries[i] < st.Retry[i].attempts() {
			if t.test {
				t.status = "RETRIABLE"
				return nil, "", f
			}
			d := st.Retry[i].delay(tries[i])
			tries[i]++
			t.retries++
			if err := t.sleep(d); err != nil {
				return nil, "", err
			}
			continue
		}
		for _, c := range st.Catch {
			if matches(c.ErrorEquals, f.name) {
				t.caught = f
				errOut := map[string]any{"Error": f.name, "Cause": f.cause}
				if st.lang == "JSONata" {
					out, err := t.jfinish(c.Assign, c.Output, bind{input: raw, errOut: errOut, hasErr: true}, errOut)
					return out, c.Next, err
				}
				if err := t.assignPath(c.Assign, errOut); err != nil {
					return nil, "", err
				}
				out, err := t.resultPath(c.ResultPath, raw, errOut)
				return out, c.Next, err
			}
		}
		return nil, "", f
	}
}

// attempt is one try of a Task, Parallel or Map state, with input and output processing.
func (t *thread) attempt(st *State, raw any) (any, error) {
	eff, err := t.inputPath(st, raw)
	if err != nil {
		return nil, err
	}
	var res any
	switch st.Type {
	case "Task":
		res, err = t.task(st, raw, eff)
	case "Parallel":
		if res, err = t.params(st, eff); err == nil {
			res, err = t.parallel(st, res)
		}
	case "Map":
		res, err = t.mapState(st, eff)
	}
	if err != nil {
		return nil, err
	}
	t.note("result", res)
	if st.lang == "JSONata" {
		return t.jfinish(st.Assign, st.Output, bind{input: raw, result: res, hasResult: true}, res)
	}
	if st.ResultSelector != nil {
		if res, err = t.template(st.ResultSelector, res); err != nil {
			return nil, err
		}
		t.note("afterResultSelector", res)
	}
	if err := t.assignPath(st.Assign, res); err != nil {
		return nil, err
	}
	return t.resultOut(st, raw, res)
}

// ---- Task ----

func (t *thread) task(st *State, raw, eff any) (any, error) {
	svc, action, mode, _ := parseResource(st.Resource)
	t.token = ""
	var w *waiter
	if mode == "waitForTaskToken" || svc == "activity" {
		t.token = newToken()
		w = t.s.expect(t.token)
		defer t.s.forget(t.token)
	}
	params, err := t.params(st, eff)
	if err != nil {
		return nil, err
	}
	secs, err := t.numField(st, st.TimeoutSeconds, "TimeoutSeconds", raw)
	if err != nil {
		return nil, err
	}
	timeout := seconds(secs)
	if st.TimeoutSecondsPath != "" {
		v, err := t.path(st.TimeoutSecondsPath, raw, "TimeoutSecondsPath")
		if err != nil {
			return nil, err
		}
		n, err := integer(v)
		if err != nil || n <= 0 {
			return nil, runtimeErr("TimeoutSecondsPath must select a positive integer, got %s", js(v))
		}
		timeout = time.Duration(n) * time.Second
	}
	var deadline <-chan time.Time
	limits := map[string]any{}
	if timeout > 0 {
		tm := time.NewTimer(timeout)
		defer tm.Stop()
		deadline = tm.C
		limits["timeoutInSeconds"] = int(timeout / time.Second)
	}
	if hb, err := t.numField(st, st.HeartbeatSeconds, "HeartbeatSeconds", raw); err != nil {
		return nil, err
	} else if hb > 0 {
		limits["heartbeatInSeconds"] = int(hb)
	}
	switch svc {
	case "function":
		t.event("LambdaFunctionScheduled", with(limits, "resource", st.Resource, "input", js(params), "inputDetails", notTruncated))
		t.event("LambdaFunctionStarted", nil)
		out, err := t.await(deadline, func() (any, error) { return t.invoke(st.Resource, params) })
		return t.settle("LambdaFunction", nil, out, err)
	case "activity":
		t.event("ActivityScheduled", with(limits, "resource", st.Resource, "input", js(params), "inputDetails", notTruncated))
		t.s.enqueue(st.Resource, t.token, js(params))
		out, err := t.recv(w, deadline)
		return t.settle("Activity", nil, out, err)
	}
	base := map[string]any{"resourceType": svc, "resource": action}
	if mode != "" {
		base["resource"] = action + "." + mode
	}
	t.event("TaskScheduled", with(merge(base, limits, false), "region", t.ex.Region, "parameters", js(params)))
	t.event("TaskStarted", base)
	out, err := t.await(deadline, func() (any, error) { return t.integrate(svc, action, params) })
	if err == nil && (mode == "waitForTaskToken" || svc == "states" && mode != "") {
		t.event("TaskSubmitted", with(base, "output", js(out), "outputDetails", notTruncated))
		if mode == "waitForTaskToken" {
			out, err = t.recv(w, deadline)
		} else {
			out, err = t.child(out, mode, deadline)
		}
	}
	return t.settle("Task", base, out, err)
}

// settle records how a task ended.
func (t *thread) settle(prefix string, base map[string]any, out any, err error) (any, error) {
	var f *failure
	switch {
	case err == nil:
		t.event(prefix+"Succeeded", with(base, "output", js(out), "outputDetails", notTruncated))
	case errors.As(err, &f) && f.name == "States.Timeout":
		t.event(prefix+"TimedOut", merge(base, errDetails(f.name, f.cause), false))
	case errors.As(err, &f):
		t.event(prefix+"Failed", merge(base, errDetails(f.name, f.cause), false))
	}
	return out, err
}

// await runs f until it returns, the task times out or the execution stops.
func (t *thread) await(deadline <-chan time.Time, f func() (any, error)) (any, error) {
	ch := make(chan result, 1)
	core.Go("stepfunctions.await", func() {
		var v any
		err := errors.New("internal error") // sent if f panics
		defer func() { ch <- result{v, err} }()
		v, err = f()
	})
	select {
	case r := <-ch:
		return r.v, r.err
	case <-deadline:
		return nil, &failure{"States.Timeout", ""}
	case <-t.ctx.Done():
		return nil, errAborted
	}
}

// recv waits for SendTaskSuccess/Failure, recording when an activity worker picks the task up.
func (t *thread) recv(w *waiter, deadline <-chan time.Time) (any, error) {
	started := func(name string) { t.event("ActivityStarted", map[string]any{"workerName": name}) }
	for {
		select {
		case name := <-w.started:
			started(name)
		case r := <-w.done:
			select {
			case name := <-w.started:
				started(name)
			default:
			}
			return r.v, r.err
		case <-deadline:
			return nil, &failure{"States.Timeout", ""}
		case <-t.ctx.Done():
			return nil, errAborted
		}
	}
}

// child waits for an execution started by states:startExecution.sync.
func (t *thread) child(started any, mode string, deadline <-chan time.Time) (any, error) {
	arn, _ := started.(map[string]any)["ExecutionArn"].(string)
	t.s.mu.Lock()
	ex := t.s.st.Executions[arn]
	t.s.mu.Unlock()
	if ex == nil {
		return nil, &failure{"States.TaskFailed", "execution " + arn + " does not exist"}
	}
	select {
	case <-ex.finished():
	case <-deadline:
		t.s.stop(arn, "States.Timeout", "The parent task timed out")
		return nil, &failure{"States.Timeout", ""}
	case <-t.ctx.Done():
		t.s.stop(arn, "", "The parent execution stopped")
		return nil, errAborted
	}
	t.s.mu.Lock()
	out := map[string]any{"ExecutionArn": ex.ARN, "StateMachineArn": ex.MachineARN, "Name": ex.Name, "Status": ex.Status,
		"StartDate": ex.Start.UnixMilli(), "StopDate": ex.Stop.UnixMilli(), "Input": ex.Input, "InputDetails": map[string]any{"Included": true}}
	if ex.Status == "SUCCEEDED" {
		out["Output"], out["OutputDetails"] = ex.Output, map[string]any{"Included": true}
	}
	if ex.Error != "" {
		out["Error"] = ex.Error
	}
	if ex.Cause != "" {
		out["Cause"] = ex.Cause
	}
	t.s.mu.Unlock()
	if mode == "sync:2" {
		out["Input"], out["Output"] = parseJSON(ex.Input), parseJSON(text(out["Output"]))
	}
	if out["Status"] != "SUCCEEDED" {
		return nil, &failure{"States.TaskFailed", js(out)}
	}
	return out, nil
}

// invoke calls a Lambda function; a function error becomes its errorType.
func (t *thread) invoke(arn string, payload any) (any, error) {
	b, err := t.s.hub.Invoke(arn, []byte(js(payload)))
	var fe *core.FunctionError
	switch {
	case errors.As(err, &fe):
		var e struct {
			ErrorType string `json:"errorType"`
		}
		json.Unmarshal(fe.Payload, &e)
		return nil, &failure{cmp.Or(e.ErrorType, "Lambda.Unknown"), string(fe.Payload)}
	case err != nil:
		return nil, sdkFailure("Lambda", err)
	}
	var v any
	if json.Unmarshal(b, &v) != nil {
		v = string(b)
	}
	return v, nil
}

// sdkTargets maps aws-sdk integration service names to JSON-protocol targets.
var sdkTargets = map[string]string{
	"dynamodb": "DynamoDB_20120810", "sqs": "AmazonSQS", "sfn": "AWSStepFunctions", "eventbridge": "AWSEvents",
	"secretsmanager": "secretsmanager", "ssm": "AmazonSSM", "kms": "TrentService", "kinesis": "Kinesis_20131202",
	"firehose": "Firehose_20150804", "cloudwatchlogs": "Logs_20140328",
}

// sdkNames prefixes aws-sdk integration errors, e.g. DynamoDb.ConditionalCheckFailedException.
var sdkNames = map[string]string{
	"dynamodb": "DynamoDb", "sqs": "Sqs", "sfn": "Sfn", "eventbridge": "EventBridge", "secretsmanager": "SecretsManager",
	"ssm": "Ssm", "kms": "Kms", "kinesis": "Kinesis", "firehose": "Firehose", "cloudwatchlogs": "CloudWatchLogs",
}

// integrate performs one service integration call.
func (t *thread) integrate(svc, action string, params any) (any, error) {
	p, _ := params.(map[string]any)
	switch {
	case svc == "lambda" && action == "invoke":
		fn, _ := p["FunctionName"].(string)
		if !strings.HasPrefix(fn, "arn:") {
			fn = core.ARN("lambda", t.ex.Region, t.ex.Account, "function:"+fn)
		}
		if q, _ := p["Qualifier"].(string); q != "" {
			fn += ":" + q
		}
		payload, ok := p["Payload"]
		if !ok {
			payload = map[string]any{}
		}
		out, err := t.invoke(fn, payload)
		if err != nil {
			return nil, err
		}
		return map[string]any{"ExecutedVersion": "$LATEST", "Payload": out, "StatusCode": 200}, nil
	case svc == "sqs":
		return t.call("AmazonSQS", action, stringify(p, "MessageBody"), "SQS")
	case svc == "sns" && action == "publish":
		var in struct {
			TopicArn, Subject, MessageGroupId, MessageDeduplicationId string
			Message                                                   any
			MessageAttributes                                         map[string]core.MessageAttribute
		}
		json.Unmarshal([]byte(js(p)), &in)
		m := &core.Message{Body: text(in.Message), Subject: in.Subject, GroupID: in.MessageGroupId, DedupID: in.MessageDeduplicationId, Attributes: in.MessageAttributes}
		if err := t.s.hub.Deliver(in.TopicArn, m); err != nil {
			return nil, sdkFailure("SNS", err)
		}
		return map[string]any{"MessageId": core.UUID()}, nil
	case svc == "dynamodb":
		return t.call("DynamoDB_20120810", action, p, "DynamoDB")
	case svc == "events":
		if entries, ok := p["Entries"].([]any); ok {
			list := make([]any, len(entries))
			for i, e := range entries {
				m, _ := e.(map[string]any)
				list[i] = stringify(m, "Detail")
			}
			p = with(p, "Entries", list)
		}
		return t.call("AWSEvents", action, p, "EventBridge")
	case svc == "states" && action == "startExecution":
		arn, _ := p["StateMachineArn"].(string)
		name, _ := p["Name"].(string)
		input := "{}"
		if v, ok := p["Input"]; ok {
			input = text(v)
		}
		ex, err := t.s.start(t.ex.trace, arn, name, input)
		if err != nil {
			return nil, sdkFailure("StepFunctions", err)
		}
		return map[string]any{"ExecutionArn": ex.ARN, "StartDate": ex.Start.UnixMilli()}, nil
	}
	if sdk, ok := strings.CutPrefix(svc, "aws-sdk:"); ok && sdkTargets[sdk] != "" {
		out, err := t.call(sdkTargets[sdk], action, p, sdkNames[sdk])
		if sdk == "sfn" || sdk == "cloudwatchlogs" { // camelCase wire members; SFN returns PascalCase
			out = pascalKeys(out)
		}
		return out, err
	}
	return nil, &failure{"States.TaskFailed", fmt.Sprintf("pogi does not support the %s:%s integration yet", svc, action)}
}

// call invokes a JSON-protocol service in-process as the execution's account.
func (t *thread) call(target, action string, in any, prefix string) (any, error) {
	var out any
	err := t.s.hub.CallTrace(t.ex.trace, t.ex.Account, t.ex.Region, target, strings.ToUpper(action[:1])+action[1:], in, &out)
	if e, ok := err.(*core.Error); ok && (e.Code == "" || e.Code == "ServiceNotEnabled") { // pogi's problem, not an AWS error
		return nil, &failure{"States.TaskFailed", cmp.Or(e.Msg, fmt.Sprintf("%s.%s failed with HTTP %d", target, action, e.Status))}
	}
	if err != nil {
		return nil, sdkFailure(prefix, err)
	}
	return out, nil
}

// sdkFailure names a service error the way Step Functions does: Prefix.CodeException.
func sdkFailure(prefix string, err error) error {
	var e *core.Error
	if !errors.As(err, &e) || e.Code == "" {
		return &failure{"States.TaskFailed", err.Error()}
	}
	code := e.Code
	if !strings.HasSuffix(code, "Exception") {
		code += "Exception"
	}
	return &failure{prefix + "." + code, e.Msg}
}

// ---- Parallel and Map ----

func (t *thread) parallel(st *State, input any) (any, error) {
	t.event("ParallelStateStarted", nil)
	out := make([]any, len(st.Branches))
	err := t.fan(len(st.Branches), 0, func(i int, c *thread) (err error) {
		out[i], err = c.exec(st.Branches[i], input)
		return err
	})
	if err != nil {
		t.event("ParallelStateFailed", nil)
		return nil, err
	}
	t.event("ParallelStateSucceeded", nil)
	return out, nil
}

func (t *thread) mapState(st *State, eff any) (any, error) {
	if st.ItemReader != nil {
		return nil, runtimeErr("pogi does not support ItemReader yet")
	}
	v := eff
	var err error
	if st.lang != "JSONata" {
		v, err = t.path(st.ItemsPath.path(), eff, "ItemsPath")
	} else if st.Items != nil {
		v, err = t.jtemplate(st.Items, "Items", bind{input: eff})
	}
	if err != nil {
		return nil, err
	}
	items, ok := v.([]any)
	if !ok {
		return nil, runtimeErr("The Items of a Map state must be an array, got %s", js(v))
	}
	t.note("afterItemsPath", items)
	n, err := t.numField(st, st.MaxConcurrency, "MaxConcurrency", eff)
	if err != nil {
		return nil, err
	}
	limit := int(n)
	if st.MaxConcurrencyPath != "" {
		v, err := t.path(st.MaxConcurrencyPath, eff, "MaxConcurrencyPath")
		if err != nil {
			return nil, err
		}
		if limit, err = integer(v); err != nil {
			return nil, runtimeErr("MaxConcurrencyPath must select an integer, got %s", js(v))
		}
	}
	if limit <= 0 {
		limit = 40 // the AWS ceiling for inline Map iterations
	}
	proc, sel := cmp.Or(st.ItemProcessor, st.Iterator), st.ItemSelector
	if sel == nil {
		sel = st.Parameters
	}
	t.event("MapStateStarted", map[string]any{"length": len(items)})
	out := make([]any, len(items))
	err = t.fan(len(items), limit, func(i int, c *thread) error {
		c.state, c.entered = t.state, t.entered
		in := items[i]
		if sel != nil {
			c.item = map[string]any{"Index": float64(i), "Value": items[i]}
			var err error
			if st.lang == "JSONata" {
				in, err = c.jtemplate(sel, "ItemSelector", bind{input: eff})
			} else {
				in, err = c.template(sel, eff)
			}
			if err != nil {
				return err
			}
			c.item = nil
		}
		ev := map[string]any{"name": t.state, "index": i}
		c.event("MapIterationStarted", ev)
		v, err := c.exec(proc, in)
		switch {
		case err == nil:
			c.event("MapIterationSucceeded", ev)
		case errors.Is(err, errAborted):
			c.event("MapIterationAborted", ev)
		default:
			c.event("MapIterationFailed", ev)
		}
		out[i] = v
		return err
	})
	if err != nil {
		t.event("MapStateFailed", nil)
		return nil, err
	}
	t.event("MapStateSucceeded", nil)
	return out, nil
}

// fan runs n child threads, at most limit at once (0 = all), and stops the rest when one fails.
func (t *thread) fan(n, limit int, f func(i int, c *thread) error) error {
	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	if limit <= 0 || limit > n {
		limit = max(n, 1)
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for i := range n {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		c := &thread{shared: t.shared, ctx: ctx, prev: t.prev, vars: maps.Clone(t.vars)}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := f(i, c); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
				cancel()
			}
		})
	}
	wg.Wait()
	t.s.mu.Lock()
	t.prev = len(t.ex.Events)
	t.s.mu.Unlock()
	if t.ctx.Err() != nil {
		return errAborted
	}
	return first
}

// ---- helpers ----

// js renders v as compact JSON without HTML escaping, as Step Functions does.
func js(v any) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
	return strings.TrimSuffix(b.String(), "\n")
}

// text passes strings through and renders anything else as JSON.
func text(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return js(v)
}

func parseJSON(s string) any {
	var v any
	json.Unmarshal([]byte(s), &v)
	return v
}

// with returns a copy of m plus the given key/value pairs.
func with(m map[string]any, kv ...any) map[string]any {
	out := make(map[string]any, len(m)+len(kv)/2)
	for k, v := range m {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

// stringify JSON-encodes field k of m when it is not a string, as integrations accept objects there.
func stringify(m map[string]any, k string) map[string]any {
	v, ok := m[k]
	if _, isStr := v.(string); !ok || isStr {
		return m
	}
	return with(m, k, js(v))
}

// pascalKeys renames object keys to PascalCase, the way aws-sdk integrations return them.
func pascalKeys(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if k != "" {
				k = strings.ToUpper(k[:1]) + k[1:]
			}
			out[k] = pascalKeys(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = pascalKeys(e)
		}
		return out
	}
	return v
}

func errDetails(name, cause string) map[string]any {
	d := map[string]any{}
	if name != "" {
		d["error"] = name
	}
	if cause != "" {
		d["cause"] = cause
	}
	return d
}

func seconds(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }

func iso(t time.Time) string { return t.UTC().Format(core.ISO) }

func nameOf(arn string) string { return arn[strings.LastIndexByte(arn, ':')+1:] }

func newToken() string { return "AQ" + core.RandString(126, core.AlphaNum) }
