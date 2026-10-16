// Package stepfunctions emulates AWS Step Functions over JSON 1.0: state
// machines, activities and an ASL interpreter that runs each execution in the
// background (Lambda and service integrations, callbacks, Retry/Catch,
// Parallel, Map, intrinsics) and records its full history.
package stepfunctions

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Step Functions emulator.
type Service struct {
	hub    *core.Hub
	rpc    *core.RPC
	mu     sync.Mutex
	st     state
	tokens map[string]*waiter  // open callback and activity tasks by token
	queued map[string][]string // activity ARN → tokens no worker has taken yet
	wake   chan struct{}       // closed when an activity task is queued
}

type state struct {
	Machines   map[string]*Machine   `json:"machines"` // by ARN
	Executions map[string]*Execution `json:"executions"`
	Activities map[string]*Activity  `json:"activities"`
	Versions   map[string]*Version   `json:"versions,omitempty"` // by version ARN
	Aliases    map[string]*Alias     `json:"aliases,omitempty"`  // by alias ARN
}

// Machine is a state machine.
type Machine struct {
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	Account     string            `json:"account"`
	Region      string            `json:"region"`
	Type        string            `json:"type"`
	Definition  string            `json:"definition"`
	RoleArn     string            `json:"roleArn"`
	Logging     json.RawMessage   `json:"logging,omitempty"`
	Tracing     json.RawMessage   `json:"tracing,omitempty"`
	Revision    string            `json:"revision"`
	LastVersion int               `json:"lastVersion,omitempty"` // the newest version number; numbers are not reused
	Created     time.Time         `json:"created"`
	Updated     time.Time         `json:"updated"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// Execution is one run of a state machine with its history.
type Execution struct {
	ARN        string           `json:"arn"`
	Name       string           `json:"name"`
	MachineARN string           `json:"machineArn"`
	Account    string           `json:"account"`
	Region     string           `json:"region"`
	Definition string           `json:"definition"` // as of the start
	RoleArn    string           `json:"roleArn"`
	Updated    time.Time        `json:"updated"`              // machine update date as of the start
	VersionARN string           `json:"versionArn,omitempty"` // when started through a version or an alias
	AliasARN   string           `json:"aliasArn,omitempty"`
	Status     string           `json:"status"`
	Input      string           `json:"input"`
	Output     string           `json:"output,omitempty"`
	Error      string           `json:"error,omitempty"`
	Cause      string           `json:"cause,omitempty"`
	Start      time.Time        `json:"start"`
	Stop       time.Time        `json:"stop,omitzero"`
	Events     []map[string]any `json:"events"`
	trace      string
	cancel     context.CancelCauseFunc
	done       chan struct{}
}

// Activity is a task queue served by external workers.
type Activity struct {
	ARN     string            `json:"arn"`
	Name    string            `json:"name"`
	Account string            `json:"account"`
	Region  string            `json:"region"`
	Created time.Time         `json:"created"`
	Tags    map[string]string `json:"tags,omitempty"`
}

// waiter is a task waiting for SendTaskSuccess/SendTaskFailure.
type waiter struct {
	done    chan result
	started chan string // activity worker name
	input   string      // activity input
}

var closed = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

// finished is closed once the execution's goroutine is done.
func (e *Execution) finished() <-chan struct{} {
	if e.done == nil {
		return closed
	}
	return e.done
}

// New creates the Step Functions service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.0", Ops: map[string]core.Handler{
		"CreateStateMachine":               core.H(s.createStateMachine),
		"DescribeStateMachine":             core.H(s.describeStateMachine),
		"UpdateStateMachine":               core.H(s.updateStateMachine),
		"DeleteStateMachine":               core.H(s.deleteStateMachine),
		"ListStateMachines":                core.H(s.listStateMachines),
		"PublishStateMachineVersion":       core.H(s.publishStateMachineVersion),
		"ListStateMachineVersions":         core.H(s.listStateMachineVersions),
		"DeleteStateMachineVersion":        core.H(s.deleteStateMachineVersion),
		"CreateStateMachineAlias":          core.H(s.createStateMachineAlias),
		"DescribeStateMachineAlias":        core.H(s.describeStateMachineAlias),
		"UpdateStateMachineAlias":          core.H(s.updateStateMachineAlias),
		"DeleteStateMachineAlias":          core.H(s.deleteStateMachineAlias),
		"ListStateMachineAliases":          core.H(s.listStateMachineAliases),
		"StartExecution":                   core.H(s.startExecution),
		"StartSyncExecution":               core.H(s.startSyncExecution),
		"DescribeExecution":                core.H(s.describeExecution),
		"ListExecutions":                   core.H(s.listExecutions),
		"StopExecution":                    core.H(s.stopExecution),
		"GetExecutionHistory":              core.H(s.getExecutionHistory),
		"DescribeStateMachineForExecution": core.H(s.describeStateMachineForExecution),
		"SendTaskSuccess":                  core.H(s.sendTaskSuccess),
		"SendTaskFailure":                  core.H(s.sendTaskFailure),
		"SendTaskHeartbeat":                core.H(s.sendTaskHeartbeat),
		"CreateActivity":                   core.H(s.createActivity),
		"DeleteActivity":                   core.H(s.deleteActivity),
		"DescribeActivity":                 core.H(s.describeActivity),
		"ListActivities":                   core.H(s.listActivities),
		"GetActivityTask":                  core.H(s.getActivityTask),
		"ValidateStateMachineDefinition":   core.H(s.validateDefinition),
		"TestState":                        core.H(s.testState),
		"TagResource":                      core.H(s.tagResource),
		"UntagResource":                    core.H(s.untagResource),
		"ListTagsForResource":              core.H(s.listTags),
	}}
	if hub != nil {
		hub.OnDeliver("states", s.deliver)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "stepfunctions", Cloud: core.AWS, Title: "Step Functions", Targets: []string{"AWSStepFunctions"}, Signing: []string{"states"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopAll()
	s.st = state{Machines: map[string]*Machine{}, Executions: map[string]*Execution{}, Activities: map[string]*Activity{},
		Versions: map[string]*Version{}, Aliases: map[string]*Alias{}}
	s.tokens, s.queued = map[string]*waiter{}, map[string][]string{}
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state; executions that were running fail with pogi.Restarted.
func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Machines == nil {
		st.Machines = map[string]*Machine{}
	}
	if st.Executions == nil {
		st.Executions = map[string]*Execution{}
	}
	if st.Activities == nil {
		st.Activities = map[string]*Activity{}
	}
	if st.Versions == nil {
		st.Versions = map[string]*Version{}
	}
	if st.Aliases == nil {
		st.Aliases = map[string]*Alias{}
	}
	for _, e := range st.Executions {
		if e.Status == "RUNNING" {
			e.Status, e.Error, e.Cause, e.Stop = "FAILED", "pogi.Restarted", "pogi restarted while the execution was running", time.Now().UTC()
			addEvent(e, len(e.Events), "ExecutionFailed", errDetails(e.Error, e.Cause))
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopAll()
	s.st = st
	s.tokens, s.queued = map[string]*waiter{}, map[string][]string{}
	return nil
}

// Close stops running executions.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopAll()
	return nil
}

// stopAll cancels every running execution; lock held.
func (s *Service) stopAll() {
	for _, e := range s.st.Executions {
		if e.cancel != nil {
			e.cancel(errStopped)
		}
	}
}

// ---- lookups and errors ----

func checkARN(arn, kind string) error {
	svc, _, _, res, ok := core.ParseARN(arn)
	if !ok || svc != "states" || !strings.HasPrefix(res, kind+":") {
		return core.Errorf(400, "InvalidArn", "Invalid Arn: 'Resource type not valid in this context: %s'", arn)
	}
	return nil
}

func validName(n string) bool {
	return n != "" && utf8.RuneCountInString(n) <= 80 && !strings.ContainsFunc(n, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r) || strings.ContainsRune("<>{}[]?*\"#%\\^|~`$&,;:/", r)
	})
}

func errName(n string) error { return core.Errorf(400, "InvalidName", "Invalid Name: '%s'", n) }

// execution finds an execution by ARN; lock held.
func (s *Service) execution(arn string) (*Execution, error) {
	if err := checkARN(arn, "execution"); err != nil {
		return nil, err
	}
	if e := s.st.Executions[arn]; e != nil {
		return e, nil
	}
	return nil, core.Errorf(400, "ExecutionDoesNotExist", "Execution Does Not Exist: '%s'", arn)
}

// executions returns the executions keep selects, newest first; lock held.
func (s *Service) executions(keep func(*Execution) bool) []*Execution {
	var out []*Execution
	for _, e := range s.st.Executions {
		if keep(e) {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b *Execution) int { return cmp.Or(b.Start.Compare(a.Start), strings.Compare(a.ARN, b.ARN)) })
	return out
}

// paginate pages a list with an offset token.
func paginate[T any](items []T, limit int, token string) ([]T, string) {
	if limit <= 0 {
		limit = 100
	}
	start, _ := strconv.Atoi(token)
	start = min(max(start, 0), len(items))
	end := min(start+limit, len(items))
	if end < len(items) {
		return items[start:end], strconv.Itoa(end)
	}
	return items[start:end], ""
}

func page[T any](key string, items []T, maxResults int, token string) map[string]any {
	p, next := paginate(items, maxResults, token)
	out := map[string]any{key: p}
	if next != "" {
		out["nextToken"] = next
	}
	return out
}

// ---- state machines ----

type tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func addTags(m map[string]string, tags []tag) map[string]string {
	if m == nil {
		m = map[string]string{}
	}
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

func rawOr(r json.RawMessage, def string) json.RawMessage {
	if len(r) == 0 {
		return json.RawMessage(def)
	}
	return r
}

func (m *Machine) describe() map[string]any {
	return map[string]any{"stateMachineArn": m.ARN, "name": m.Name, "status": "ACTIVE", "definition": m.Definition,
		"roleArn": m.RoleArn, "type": m.Type, "creationDate": core.Time(m.Created), "revisionId": m.Revision,
		"loggingConfiguration": rawOr(m.Logging, `{"level":"OFF","includeExecutionData":false}`),
		"tracingConfiguration": rawOr(m.Tracing, `{"enabled":false}`)}
}

func (s *Service) createStateMachine(c *core.Ctx, in *struct {
	Name, Definition, RoleArn, Type, VersionDescription string
	Publish                                             bool
	LoggingConfiguration, TracingConfiguration          json.RawMessage
	Tags                                                []tag
}) (any, error) {
	if !validName(in.Name) {
		return nil, errName(in.Name)
	}
	typ := cmp.Or(in.Type, "STANDARD")
	if typ != "STANDARD" && typ != "EXPRESS" {
		return nil, core.Errorf(400, "ValidationException", "Value '%s' at 'type' failed to satisfy constraint: Member must satisfy enum value set: [STANDARD, EXPRESS]", typ)
	}
	if _, diags := parseDef(in.Definition); len(diags) > 0 {
		return nil, defError(diags)
	}
	arn := c.ARN("states", "stateMachine:"+in.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.st.Machines[arn]; m != nil {
		if m.Definition != in.Definition || m.Type != typ { // AWS ignores roleArn and tag differences
			return nil, core.Errorf(400, "StateMachineAlreadyExists", "State Machine Already Exists: '%s'", arn)
		}
		return map[string]any{"stateMachineArn": arn, "creationDate": core.Time(m.Created)}, nil
	}
	now := time.Now().UTC()
	s.st.Machines[arn] = &Machine{ARN: arn, Name: in.Name, Account: c.Account, Region: c.Region, Type: typ, Definition: in.Definition,
		RoleArn: in.RoleArn, Logging: in.LoggingConfiguration, Tracing: in.TracingConfiguration, Revision: core.UUID(),
		Created: now, Updated: now, Tags: addTags(nil, in.Tags)}
	out := map[string]any{"stateMachineArn": arn, "creationDate": core.Time(now)}
	if in.Publish {
		v, err := s.publish(s.st.Machines[arn], in.VersionDescription)
		if err != nil {
			delete(s.st.Machines, arn)
			return nil, err
		}
		out["stateMachineVersionArn"] = v.ARN
	}
	return out, nil
}

type arnInput struct {
	StateMachineArn string
}

func (s *Service) describeStateMachine(c *core.Ctx, in *arnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.resolve(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	switch {
	case t.a != nil: // an alias describes the version it points at (the first one when it splits traffic)
		return s.st.Versions[t.a.Routes[0].Version].describe(t.m.Name), nil
	case t.v != nil:
		return t.v.describe(t.m.Name), nil
	}
	return t.m.describe(), nil
}

func (s *Service) updateStateMachine(c *core.Ctx, in *struct {
	StateMachineArn, Definition, RoleArn, VersionDescription string
	Publish                                                  bool
	LoggingConfiguration, TracingConfiguration               json.RawMessage
}) (any, error) {
	if in.Definition == "" && in.RoleArn == "" && in.LoggingConfiguration == nil && in.TracingConfiguration == nil {
		return nil, core.Errorf(400, "MissingRequiredParameter", "Either the definition, the roleArn, the loggingConfiguration or the tracingConfiguration must be specified")
	}
	if in.Definition != "" {
		if _, diags := parseDef(in.Definition); len(diags) > 0 {
			return nil, defError(diags)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.machineFor(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	m.Definition, m.RoleArn = cmp.Or(in.Definition, m.Definition), cmp.Or(in.RoleArn, m.RoleArn)
	if in.LoggingConfiguration != nil {
		m.Logging = in.LoggingConfiguration
	}
	if in.TracingConfiguration != nil {
		m.Tracing = in.TracingConfiguration
	}
	m.Updated, m.Revision = time.Now().UTC(), core.UUID()
	out := map[string]any{"updateDate": core.Time(m.Updated), "revisionId": m.Revision}
	if in.Publish {
		v, err := s.publish(m, in.VersionDescription)
		if err != nil {
			return nil, err
		}
		out["stateMachineVersionArn"] = v.ARN
	}
	return out, nil
}

// deleteStateMachine removes the machine; its executions keep running and stay listed.
func (s *Service) deleteStateMachine(c *core.Ctx, in *arnInput) (any, error) {
	if err := checkARN(in.StateMachineArn, "stateMachine"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if m := s.st.Machines[in.StateMachineArn]; m != nil {
		s.dropVersions(m)
		delete(s.st.Machines, in.StateMachineArn)
	}
	s.mu.Unlock()
	return nil, nil
}

type listInput struct {
	MaxResults int
	NextToken  string
}

func (s *Service) listStateMachines(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []map[string]any{}
	for _, k := range core.SortedKeys(s.st.Machines) {
		if m := s.st.Machines[k]; m.Account == c.Account && m.Region == c.Region {
			items = append(items, map[string]any{"stateMachineArn": m.ARN, "name": m.Name, "type": m.Type, "creationDate": core.Time(m.Created)})
		}
	}
	return page("stateMachines", items, in.MaxResults, in.NextToken), nil
}

// ---- executions ----

// maxExecutions caps stored executions; the oldest finished ones go first.
// ponytail: a fixed cap, make it a Config knob if anyone needs more history.
const maxExecutions = 1000

// start creates an execution of a state machine (or one of its versions) and runs it in the background.
// start begins an execution in the flow of trace (a new one when empty); its task calls carry it.
func (s *Service) start(trace, smArn, name, input string) (*Execution, error) {
	input = cmp.Or(input, "{}")
	var in any
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		return nil, core.Errorf(400, "InvalidExecutionInput", "Invalid State Machine Execution Input: '%v'", err)
	}
	if name == "" {
		name = core.UUID()
	} else if !validName(name) {
		return nil, errName(name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.resolve(smArn)
	if err != nil {
		return nil, err
	}
	// A version or an alias runs the definition it was published with.
	m, defn, role, updated, versionARN, aliasARN := t.m, t.m.Definition, t.m.RoleArn, t.m.Updated, "", ""
	if t.a != nil {
		aliasARN = t.a.ARN
		t.v = s.st.Versions[t.a.pick()]
	}
	if t.v != nil {
		defn, role, updated, versionARN = t.v.Definition, t.v.RoleArn, t.v.Created, t.v.ARN
	}
	arn := core.ARN("states", m.Region, m.Account, "execution:"+m.Name+":"+name)
	if old := s.st.Executions[arn]; old != nil {
		if old.Status == "RUNNING" && old.Input == input {
			return old, nil
		}
		return nil, core.Errorf(400, "ExecutionAlreadyExists", "Execution Already Exists: '%s'", arn)
	}
	def, diags := parseDef(defn)
	if len(diags) > 0 {
		return nil, defError(diags)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	ex := &Execution{ARN: arn, Name: name, MachineARN: m.ARN, Account: m.Account, Region: m.Region, VersionARN: versionARN, AliasARN: aliasARN,
		Definition: defn, RoleArn: role, Updated: updated, Status: "RUNNING", Input: input,
		Start: time.Now().UTC(), cancel: cancel, done: make(chan struct{}), trace: cmp.Or(trace, core.NewTrace())}
	addEvent(ex, 0, "ExecutionStarted", map[string]any{"input": input, "inputDetails": notTruncated, "roleArn": role})
	s.st.Executions[arn] = ex
	if len(s.st.Executions) > maxExecutions {
		old := s.executions(func(e *Execution) bool { return e.Status != "RUNNING" })
		for _, e := range old[max(0, len(old)-(len(s.st.Executions)-maxExecutions)):] {
			delete(s.st.Executions, e.ARN)
		}
	}
	core.Go("stepfunctions.run", func() {
		s.run(&thread{shared: &shared{s: s, ex: ex, input: in}, ctx: ctx, prev: 1, vars: map[string]any{}}, def)
	})
	return ex, nil
}

func (s *Service) run(t *thread, def *Def) {
	if def.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		t.ctx, cancel = context.WithTimeoutCause(t.ctx, time.Duration(def.TimeoutSeconds)*time.Second, errTimedOut)
		defer cancel()
	}
	out, err := t.exec(def, t.input)
	s.finish(t, out, err)
}

// finish records the outcome unless StopExecution already did.
func (s *Service) finish(t *thread, out any, err error) {
	ex := t.ex
	defer close(ex.done)
	s.mu.Lock()
	defer s.mu.Unlock()
	if ex.Status != "RUNNING" {
		return
	}
	ex.Stop = time.Now().UTC()
	var f *failure
	switch {
	case err == nil:
		ex.Status, ex.Output = "SUCCEEDED", js(out)
		addEvent(ex, t.prev, "ExecutionSucceeded", map[string]any{"output": ex.Output, "outputDetails": notTruncated})
		return
	case errors.As(err, &f):
		ex.Status, ex.Error, ex.Cause = "FAILED", f.name, f.cause
		addEvent(ex, t.prev, "ExecutionFailed", errDetails(f.name, f.cause))
	case context.Cause(t.ctx) == errTimedOut:
		// AWS leaves error and cause off the execution itself; only the history event carries them.
		ex.Status = "TIMED_OUT"
		addEvent(ex, t.prev, "ExecutionTimedOut", errDetails("States.Timeout", "The execution exceeded its TimeoutSeconds"))
	default:
		ex.Status = "ABORTED"
		addEvent(ex, t.prev, "ExecutionAborted", errDetails("", "pogi stopped the execution"))
	}
}

// abort stops a running execution; lock held.
func (s *Service) abort(ex *Execution, name, cause string) {
	if ex.Status != "RUNNING" {
		return
	}
	ex.Status, ex.Stop, ex.Error, ex.Cause = "ABORTED", time.Now().UTC(), name, cause
	addEvent(ex, len(ex.Events), "ExecutionAborted", errDetails(name, cause))
	if ex.cancel != nil {
		ex.cancel(errStopped)
	}
}

func (s *Service) stop(arn, name, cause string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.st.Executions[arn]; e != nil {
		s.abort(e, name, cause)
	}
}

func (e *Execution) describe() map[string]any {
	out := map[string]any{"executionArn": e.ARN, "stateMachineArn": e.MachineARN, "name": e.Name, "status": e.Status,
		"startDate": core.Time(e.Start), "input": e.Input, "inputDetails": included}
	if !e.Stop.IsZero() {
		out["stopDate"] = core.Time(e.Stop)
	}
	if e.VersionARN != "" {
		out["stateMachineVersionArn"] = e.VersionARN
	}
	if e.AliasARN != "" {
		out["stateMachineAliasArn"] = e.AliasARN
	}
	if e.Status == "SUCCEEDED" {
		out["output"], out["outputDetails"] = e.Output, included
	}
	if e.Error != "" {
		out["error"] = e.Error
	}
	if e.Cause != "" {
		out["cause"] = e.Cause
	}
	return out
}

type startInput struct {
	StateMachineArn, Name, Input string
}

func (s *Service) startExecution(c *core.Ctx, in *startInput) (any, error) {
	ex, err := s.start(c.Trace, in.StateMachineArn, in.Name, in.Input)
	if err != nil {
		return nil, err
	}
	return map[string]any{"executionArn": ex.ARN, "startDate": core.Time(ex.Start)}, nil
}

// startSyncExecution runs an EXPRESS execution to completion.
func (s *Service) startSyncExecution(c *core.Ctx, in *startInput) (any, error) {
	s.mu.Lock()
	t, err := s.resolve(in.StateMachineArn)
	express := err == nil && t.m.Type == "EXPRESS"
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !express {
		return nil, core.Errorf(400, "StateMachineTypeNotSupported", "This operation is not supported by this type of state machine")
	}
	ex, err := s.start(c.Trace, in.StateMachineArn, in.Name, in.Input)
	if err != nil {
		return nil, err
	}
	<-ex.finished()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := ex.describe()
	out["billingDetails"] = map[string]any{"billedMemoryUsedInMB": 64, "billedDurationInMilliseconds": max(100, ex.Stop.Sub(ex.Start).Milliseconds())}
	return out, nil
}

type execInput struct {
	ExecutionArn string
}

func (s *Service) describeExecution(c *core.Ctx, in *execInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ex, err := s.execution(in.ExecutionArn)
	if err != nil {
		return nil, err
	}
	return ex.describe(), nil
}

func (s *Service) listExecutions(c *core.Ctx, in *struct {
	StateMachineArn, StatusFilter, NextToken string
	MaxResults                               int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.resolve(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	items := []map[string]any{}
	for _, e := range s.executions(func(e *Execution) bool {
		return e.MachineARN == t.m.ARN && (t.v == nil || e.VersionARN == t.v.ARN) && (t.a == nil || e.AliasARN == t.a.ARN) &&
			(in.StatusFilter == "" || e.Status == in.StatusFilter)
	}) {
		item := map[string]any{"executionArn": e.ARN, "stateMachineArn": e.MachineARN, "name": e.Name, "status": e.Status, "startDate": core.Time(e.Start)}
		if e.VersionARN != "" {
			item["stateMachineVersionArn"] = e.VersionARN
		}
		if e.AliasARN != "" {
			item["stateMachineAliasArn"] = e.AliasARN
		}
		if !e.Stop.IsZero() {
			item["stopDate"] = core.Time(e.Stop)
		}
		items = append(items, item)
	}
	return page("executions", items, in.MaxResults, in.NextToken), nil
}

func (s *Service) stopExecution(c *core.Ctx, in *struct{ ExecutionArn, Error, Cause string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ex, err := s.execution(in.ExecutionArn)
	if err != nil {
		return nil, err
	}
	s.abort(ex, in.Error, in.Cause)
	return map[string]any{"stopDate": core.Time(ex.Stop)}, nil
}

func (s *Service) getExecutionHistory(c *core.Ctx, in *struct {
	ExecutionArn, NextToken string
	MaxResults              int
	ReverseOrder            bool
}) (any, error) {
	s.mu.Lock()
	ex, err := s.execution(in.ExecutionArn)
	var events []map[string]any
	if err == nil {
		events = slices.Clone(ex.Events)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if in.ReverseOrder {
		slices.Reverse(events)
	}
	return page("events", events, in.MaxResults, in.NextToken), nil
}

func (s *Service) describeStateMachineForExecution(c *core.Ctx, in *execInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ex, err := s.execution(in.ExecutionArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"stateMachineArn": ex.MachineARN, "name": nameOf(ex.MachineARN), "definition": ex.Definition,
		"roleArn": ex.RoleArn, "updateDate": core.Time(ex.Updated)}, nil
}

// ---- callbacks and activities ----

// expect registers an open task token.
func (s *Service) expect(token string) *waiter {
	w := &waiter{done: make(chan result, 1), started: make(chan string, 1)}
	s.mu.Lock()
	s.tokens[token] = w
	s.mu.Unlock()
	return w
}

func (s *Service) forget(token string) {
	s.mu.Lock()
	delete(s.tokens, token)
	s.mu.Unlock()
}

// enqueue offers an activity task to workers.
func (s *Service) enqueue(activity, token, input string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w := s.tokens[token]; w != nil {
		w.input = input
	}
	s.queued[activity] = append(s.queued[activity], token)
	if s.wake != nil {
		close(s.wake)
		s.wake = nil
	}
}

func noTask(token string) error {
	if len(token) == 128 && strings.HasPrefix(token, "AQ") {
		return core.Errorf(400, "TaskDoesNotExist", "Task does not exist anymore")
	}
	return core.Errorf(400, "InvalidToken", "Invalid Token: '%s'", token)
}

// complete hands a task result to the waiting execution.
func (s *Service) complete(token string, r result) error {
	s.mu.Lock()
	w := s.tokens[token]
	delete(s.tokens, token)
	s.mu.Unlock()
	if w == nil {
		return noTask(token)
	}
	w.done <- r
	return nil
}

func (s *Service) sendTaskSuccess(c *core.Ctx, in *struct{ TaskToken, Output string }) (any, error) {
	var v any
	if err := json.Unmarshal([]byte(in.Output), &v); err != nil {
		return nil, core.Errorf(400, "InvalidOutput", "Invalid Output: '%v'", err)
	}
	return nil, s.complete(in.TaskToken, result{v: v})
}

func (s *Service) sendTaskFailure(c *core.Ctx, in *struct{ TaskToken, Error, Cause string }) (any, error) {
	return nil, s.complete(in.TaskToken, result{err: &failure{in.Error, in.Cause}})
}

func (s *Service) sendTaskHeartbeat(c *core.Ctx, in *struct{ TaskToken string }) (any, error) {
	s.mu.Lock()
	w := s.tokens[in.TaskToken]
	s.mu.Unlock()
	if w == nil {
		return nil, noTask(in.TaskToken)
	}
	return nil, nil
}

type activityInput struct {
	ActivityArn string
}

func (s *Service) createActivity(c *core.Ctx, in *struct {
	Name string
	Tags []tag
}) (any, error) {
	if !validName(in.Name) {
		return nil, errName(in.Name)
	}
	arn := c.ARN("states", "activity:"+in.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Activities[arn]
	if a == nil {
		a = &Activity{ARN: arn, Name: in.Name, Account: c.Account, Region: c.Region, Created: time.Now().UTC(), Tags: addTags(nil, in.Tags)}
		s.st.Activities[arn] = a
	}
	return map[string]any{"activityArn": arn, "creationDate": core.Time(a.Created)}, nil
}

func (s *Service) deleteActivity(c *core.Ctx, in *activityInput) (any, error) {
	if err := checkARN(in.ActivityArn, "activity"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	delete(s.st.Activities, in.ActivityArn)
	s.mu.Unlock()
	return nil, nil
}

// activity finds an activity; lock held.
func (s *Service) activity(arn string) (*Activity, error) {
	if err := checkARN(arn, "activity"); err != nil {
		return nil, err
	}
	if a := s.st.Activities[arn]; a != nil {
		return a, nil
	}
	return nil, core.Errorf(400, "ActivityDoesNotExist", "Activity Does Not Exist: '%s'", arn)
}

func (s *Service) describeActivity(c *core.Ctx, in *activityInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.activity(in.ActivityArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"activityArn": a.ARN, "name": a.Name, "creationDate": core.Time(a.Created)}, nil
}

func (s *Service) listActivities(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []map[string]any{}
	for _, k := range core.SortedKeys(s.st.Activities) {
		if a := s.st.Activities[k]; a.Account == c.Account && a.Region == c.Region {
			items = append(items, map[string]any{"activityArn": a.ARN, "name": a.Name, "creationDate": core.Time(a.Created)})
		}
	}
	return page("activities", items, in.MaxResults, in.NextToken), nil
}

// activityPoll is how long GetActivityTask waits for a task before returning none.
const activityPoll = 5 * time.Second

func (s *Service) getActivityTask(c *core.Ctx, in *struct{ ActivityArn, WorkerName string }) (any, error) {
	deadline := time.Now().Add(activityPoll)
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if _, err := s.activity(in.ActivityArn); err != nil {
			return nil, err
		}
		for q := s.queued[in.ActivityArn]; len(q) > 0; q = s.queued[in.ActivityArn] {
			tok := q[0]
			s.queued[in.ActivityArn] = q[1:]
			if w := s.tokens[tok]; w != nil {
				select {
				case w.started <- in.WorkerName:
				default:
				}
				return map[string]any{"taskToken": tok, "input": w.input}, nil
			}
		}
		left := time.Until(deadline)
		if left <= 0 {
			return nil, nil
		}
		if s.wake == nil {
			s.wake = make(chan struct{})
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-wake:
		case <-time.After(left):
		}
		s.mu.Lock()
	}
}

// ---- validation and TestState ----

func (s *Service) validateDefinition(c *core.Ctx, in *struct {
	Definition string
	MaxResults int
}) (any, error) {
	_, diags := parseDef(in.Definition)
	out := map[string]any{"result": "OK", "diagnostics": []diag{}, "truncated": false}
	if len(diags) > 0 {
		if in.MaxResults > 0 && len(diags) > in.MaxResults {
			diags, out["truncated"] = diags[:in.MaxResults], true
		}
		out["result"], out["diagnostics"] = "FAIL", diags
	}
	return out, nil
}

// testState runs one state once, without retries or waits.
func (s *Service) testState(c *core.Ctx, in *struct {
	Definition, RoleArn, Input, InspectionLevel, StateName, Variables string
}) (any, error) {
	var d Def
	var st *State
	if json.Unmarshal([]byte(in.Definition), &d) == nil && d.States != nil {
		d.setLang(cmp.Or(d.QueryLanguage, "JSONPath"))
		st = d.States[cmp.Or(in.StateName, d.StartAt)]
	} else if json.Unmarshal([]byte(in.Definition), &st) != nil {
		st = nil
	} else if st != nil {
		st.lang = cmp.Or(st.QueryLanguage, "JSONPath")
	}
	vars := map[string]any{}
	if in.Variables != "" {
		if json.Unmarshal([]byte(in.Variables), &vars) != nil || vars == nil {
			return nil, core.Errorf(400, "ValidationException", "The variables must be a JSON object")
		}
	}
	if st == nil || !stateTypes[st.Type] {
		return nil, core.Errorf(400, "InvalidDefinition", "Invalid State Machine Definition: 'the definition must be one state with a valid Type'")
	}
	if svc, _, mode, _ := parseResource(st.Resource); st.Type == "Task" && (svc == "activity" || mode != "") {
		return nil, core.Errorf(400, "ValidationException", "TestState does not support activities, .sync or .waitForTaskToken tasks")
	}
	input := cmp.Or(in.Input, "{}")
	var v any
	if err := json.Unmarshal([]byte(input), &v); err != nil {
		return nil, core.Errorf(400, "InvalidExecutionInput", "Invalid input: '%v'", err)
	}
	ex := &Execution{ARN: c.ARN("states", "execution:TestState:"+core.UUID()), Name: "TestState", MachineARN: c.ARN("states", "stateMachine:TestState"),
		Account: c.Account, Region: c.Region, RoleArn: in.RoleArn, Status: "RUNNING", Input: input, Start: time.Now().UTC()}
	t := &thread{shared: &shared{s: s, ex: ex, input: v, test: true}, ctx: context.Background(), inspect: map[string]any{"input": js(v)}, vars: vars}
	out, next, err := t.step(cmp.Or(in.StateName, "TestState"), st, v)
	res := map[string]any{"status": "SUCCEEDED"}
	var f *failure
	switch {
	case errors.As(err, &f):
		res["status"], res["error"], res["cause"] = cmp.Or(t.status, "FAILED"), f.name, f.cause
	case err != nil:
		return nil, err
	default:
		res["output"] = js(out)
		if next != "" {
			res["nextState"] = next
		}
		if t.caught != nil {
			res["status"], res["error"], res["cause"] = "CAUGHT_ERROR", t.caught.name, t.caught.cause
		}
	}
	if in.InspectionLevel == "DEBUG" || in.InspectionLevel == "TRACE" {
		if len(t.vars) > 0 {
			t.inspect["variables"] = js(t.vars)
		}
		res["inspectionData"] = t.inspect
	}
	return res, nil
}

// ---- tags ----

// tags returns the tag map of a state machine or activity; lock held.
func (s *Service) tags(arn string) (*map[string]string, error) {
	if m := s.st.Machines[arn]; m != nil {
		return &m.Tags, nil
	}
	if a := s.st.Activities[arn]; a != nil {
		return &a.Tags, nil
	}
	return nil, core.Errorf(400, "ResourceNotFound", "Resource not found: '%s'", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        []tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	*t = addTags(*t, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(*t, k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	out := []tag{}
	for _, k := range core.SortedKeys(*t) {
		out = append(out, tag{k, (*t)[k]})
	}
	return map[string]any{"tags": out}, nil
}

// ---- hub, console ----

// deliver starts an execution for EventBridge rules and schedules; the body is the input.
func (s *Service) deliver(arn string, m *core.Message) error {
	in := m.Body
	if !json.Valid([]byte(in)) {
		in = js(in)
	}
	_, err := s.start(m.Trace, arn, "", in)
	return err
}

// StartConsole starts an execution from the console; smArn may also be a bare name.
func (s *Service) StartConsole(account, region, smArn, input string) (string, error) {
	if !strings.HasPrefix(smArn, "arn:") {
		smArn = core.ARN("states", region, account, "stateMachine:"+smArn)
	}
	ex, err := s.start("", smArn, "", input)
	if err != nil {
		return "", err
	}
	return ex.ARN, nil
}

// History returns an execution's events (nil when unknown).
func (s *Service) History(executionArn string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.st.Executions[executionArn]; e != nil {
		return slices.Clone(e.Events)
	}
	return nil
}

// Resources lists state machines and the latest executions.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	exs := s.executions(func(*Execution) bool { return true })
	count := map[string]int{}
	for _, e := range exs {
		count[e.MachineARN]++
	}
	sm := core.Table{Name: "State machines", Columns: []string{"Name", "Region", "Account", "Type", "Executions"}}
	for _, k := range core.SortedKeys(s.st.Machines) {
		m := s.st.Machines[k]
		sm.Rows = append(sm.Rows, []string{m.Name, m.Region, m.Account, m.Type, strconv.Itoa(count[m.ARN])})
	}
	et := core.Table{Name: "Executions", Columns: []string{"Name", "State machine", "Status", "Started"}}
	for _, e := range exs[:min(len(exs), 200)] {
		et.Rows = append(et.Rows, []string{e.Name, nameOf(e.MachineARN), e.Status, e.Start.Format(time.RFC3339)})
	}
	return []core.Table{sm, et}
}

// Inspect shows a state machine with its recent executions, or an execution with its history.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "State machines":
		for _, m := range s.st.Machines {
			if m.Name != id && m.ARN != id {
				continue
			}
			recent := []map[string]any{}
			for _, e := range s.executions(func(e *Execution) bool { return e.MachineARN == m.ARN }) {
				if len(recent) == 20 {
					break
				}
				recent = append(recent, map[string]any{"name": e.Name, "executionArn": e.ARN, "status": e.Status, "startDate": e.Start})
			}
			d := m.describe()
			d["definition"], d["tags"], d["executions"] = parseJSON(m.Definition), m.Tags, recent
			return d
		}
	case "Executions":
		for _, e := range s.st.Executions {
			if e.Name == id || e.ARN == id {
				d := e.describe()
				d["input"], d["output"], d["history"] = parseJSON(e.Input), parseJSON(e.Output), slices.Clone(e.Events)
				d["startDate"] = e.Start
				if !e.Stop.IsZero() {
					d["stopDate"] = e.Stop
				}
				return d
			}
		}
	}
	return nil
}
