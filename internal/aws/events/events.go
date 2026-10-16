// Package events emulates Amazon EventBridge over JSON 1.1: event buses,
// rules with event patterns and schedules, targets with input
// transformation, archives and replays, connections and API destinations.
// Other services publish through the hub: S3 sends "Object Created" events
// to the default bus, Scheduler sends to buses it targets.
package events

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the EventBridge emulator.
type Service struct {
	stop func()
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex
	st   state
}

type state struct { // every map is keyed account/region/name
	Buses        map[string]*Bus            `json:"buses"`
	Archives     map[string]*Archive        `json:"archives"`
	Replays      map[string]*Replay         `json:"replays"`
	Connections  map[string]*Connection     `json:"connections"`
	Destinations map[string]*APIDestination `json:"destinations"`
}

// Bus is an event bus and its rules.
type Bus struct {
	Name        string            `json:"name"`
	Account     string            `json:"account"`
	Region      string            `json:"region"`
	Description string            `json:"description,omitempty"`
	Policy      string            `json:"policy,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Created     time.Time         `json:"created"`
	Modified    time.Time         `json:"modified"`
	Rules       map[string]*Rule  `json:"rules"`
}

// Rule sends matching or scheduled events to its targets.
type Rule struct {
	Name               string            `json:"name"`
	Description        string            `json:"description,omitempty"`
	EventPattern       string            `json:"eventPattern,omitempty"`
	ScheduleExpression string            `json:"scheduleExpression,omitempty"`
	State              string            `json:"state"`
	RoleArn            string            `json:"roleArn,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	Targets            []*Target         `json:"targets"`
	Next               time.Time         `json:"next,omitzero"` // next scheduled run
	pat                *pattern.Pattern
}

// Target is stored as sent; the parameters pogi does not act on are kept
// verbatim so that ListTargetsByRule round-trips them.
type Target struct {
	Id                string
	Arn               string
	RoleArn           string                             `json:",omitempty"`
	Input             string                             `json:",omitempty"`
	InputPath         string                             `json:",omitempty"`
	InputTransformer  *InputTransformer                  `json:",omitempty"`
	SqsParameters     *struct{ MessageGroupId string }   `json:",omitempty"`
	KinesisParameters *struct{ PartitionKeyPath string } `json:",omitempty"`
	DeadLetterConfig  *struct{ Arn string }              `json:",omitempty"`

	RetryPolicy, HttpParameters, EcsParameters, BatchParameters, RedshiftDataParameters,
	SageMakerPipelineParameters, RunCommandParameters, AppSyncParameters json.RawMessage `json:",omitempty"`
}

// InputTransformer rewrites an event with JSONPath variables and a template.
type InputTransformer struct {
	InputPathsMap map[string]string `json:",omitempty"`
	InputTemplate string
}

// New creates the EventBridge service and starts its schedule clock.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateEventBus":         core.H(s.createEventBus),
		"DeleteEventBus":         core.H(s.deleteEventBus),
		"DescribeEventBus":       core.H(s.describeEventBus),
		"ListEventBuses":         core.H(s.listEventBuses),
		"PutRule":                core.H(s.putRule),
		"DescribeRule":           core.H(s.describeRule),
		"ListRules":              core.H(s.listRules),
		"DeleteRule":             core.H(s.deleteRule),
		"EnableRule":             core.H(s.setState("ENABLED")),
		"DisableRule":            core.H(s.setState("DISABLED")),
		"PutTargets":             core.H(s.putTargets),
		"RemoveTargets":          core.H(s.removeTargets),
		"ListTargetsByRule":      core.H(s.listTargetsByRule),
		"ListRuleNamesByTarget":  core.H(s.listRuleNamesByTarget),
		"PutEvents":              core.H(s.putEvents),
		"TestEventPattern":       core.H(s.testEventPattern),
		"TagResource":            core.H(s.tagResource),
		"UntagResource":          core.H(s.untagResource),
		"ListTagsForResource":    core.H(s.listTagsForResource),
		"PutPermission":          core.H(s.putPermission),
		"RemovePermission":       core.H(s.removePermission),
		"CreateArchive":          core.H(s.createArchive),
		"DescribeArchive":        core.H(s.describeArchive),
		"ListArchives":           core.H(s.listArchives),
		"UpdateArchive":          core.H(s.updateArchive),
		"DeleteArchive":          core.H(s.deleteArchive),
		"StartReplay":            core.H(s.startReplay),
		"DescribeReplay":         core.H(s.describeReplay),
		"ListReplays":            core.H(s.listReplays),
		"CancelReplay":           core.H(s.cancelReplay),
		"CreateConnection":       core.H(s.createConnection),
		"DescribeConnection":     core.H(s.describeConnection),
		"ListConnections":        core.H(s.listConnections),
		"DeleteConnection":       core.H(s.deleteConnection),
		"CreateApiDestination":   core.H(s.createAPIDestination),
		"DescribeApiDestination": core.H(s.describeAPIDestination),
		"ListApiDestinations":    core.H(s.listAPIDestinations),
		"DeleteApiDestination":   core.H(s.deleteAPIDestination),
	}}
	hub.OnDeliver("events", s.deliver)
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

// Close stops the schedule clock.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Info() core.Info {
	return core.Info{Name: "events", Cloud: core.AWS, Title: "EventBridge", Targets: []string{"AWSEvents"}, Signing: []string{"events"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{}
	s.st.fill()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	st.fill()
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (st *state) fill() {
	if st.Buses == nil {
		st.Buses = map[string]*Bus{}
	}
	if st.Archives == nil {
		st.Archives = map[string]*Archive{}
	}
	if st.Replays == nil {
		st.Replays = map[string]*Replay{}
	}
	if st.Connections == nil {
		st.Connections = map[string]*Connection{}
	}
	if st.Destinations == nil {
		st.Destinations = map[string]*APIDestination{}
	}
	for _, b := range st.Buses {
		if b.Rules == nil {
			b.Rules = map[string]*Rule{}
		}
	}
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func notFound(format string, a ...any) error {
	return core.Errorf(400, "ResourceNotFoundException", format, a...)
}

func exists(format string, a ...any) error {
	return core.Errorf(400, "ResourceAlreadyExistsException", format, a...)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func compilePattern(src string) (*pattern.Pattern, error) {
	p, err := pattern.Compile(src)
	if err != nil {
		return nil, core.Errorf(400, "InvalidEventPatternException", "Event pattern is not valid. Reason: %v", err)
	}
	return p, nil
}

var (
	nameRe = regexp.MustCompile(`^[\.\-_A-Za-z0-9]{1,64}$`)
	busRe  = regexp.MustCompile(`^([\.\-_A-Za-z0-9]{1,256}|aws\.partner(/[\.\-_A-Za-z0-9]+)+)$`)
)

// ---- buses ----

func (b *Bus) arn() string { return core.ARN("events", b.Region, b.Account, "event-bus/"+b.Name) }

// ruleARN leaves the bus out for rules on the default bus, as AWS does.
func (b *Bus) ruleARN(rule string) string {
	if b.Name != "default" {
		rule = b.Name + "/" + rule
	}
	return core.ARN("events", b.Region, b.Account, "rule/"+rule)
}

type busOut struct {
	Name             string
	Arn              string
	Description      string `json:",omitempty"`
	Policy           string `json:",omitempty"`
	CreationTime     float64
	LastModifiedTime float64
}

func (b *Bus) out() busOut {
	return busOut{b.Name, b.arn(), b.Description, b.Policy, core.Unix(b.Created), core.Unix(b.Modified)}
}

// busRef splits a bus name or ARN into account, region and name.
func busRef(account, region, nameOrARN string) (string, string, string) {
	if _, r, a, res, ok := core.ParseARN(nameOrARN); ok {
		return a, r, strings.TrimPrefix(res, "event-bus/")
	}
	return account, region, cmp.Or(nameOrARN, "default")
}

// bus finds a bus; every account and region has a default one. Lock held.
func (s *Service) bus(account, region, name string) *Bus {
	k := key(account, region, name)
	b := s.st.Buses[k]
	if b == nil && name == "default" {
		now := time.Now().UTC()
		b = &Bus{Name: name, Account: account, Region: region, Created: now, Modified: now, Rules: map[string]*Rule{}}
		s.st.Buses[k] = b
	}
	return b
}

// findBus resolves a request's EventBusName (name or ARN); lock held.
func (s *Service) findBus(c *core.Ctx, nameOrARN string) (*Bus, error) {
	account, region, name := busRef(c.Account, c.Region, nameOrARN)
	if b := s.bus(account, region, name); b != nil {
		return b, nil
	}
	return nil, notFound("Event bus %s does not exist.", name)
}

func (s *Service) findRule(c *core.Ctx, bus, name string) (*Bus, *Rule, error) {
	b, err := s.findBus(c, bus)
	if err != nil {
		return nil, nil, err
	}
	r := b.Rules[name]
	if r == nil {
		return nil, nil, notFound("Rule %s does not exist on EventBus %s.", name, b.Name)
	}
	return b, r, nil
}

type tag struct{ Key, Value string }

func (s *Service) createEventBus(c *core.Ctx, in *struct {
	Name, Description string
	Tags              []tag
}) (any, error) {
	if !busRe.MatchString(in.Name) {
		return nil, invalid("Event bus name %s is not valid.", in.Name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bus(c.Account, c.Region, in.Name) != nil {
		return nil, exists("Event bus %s already exists.", in.Name)
	}
	now := time.Now().UTC()
	b := &Bus{Name: in.Name, Account: c.Account, Region: c.Region, Description: in.Description,
		Tags: addTags(nil, in.Tags), Created: now, Modified: now, Rules: map[string]*Rule{}}
	s.st.Buses[key(c.Account, c.Region, in.Name)] = b
	return &struct {
		EventBusArn string
		Description string `json:",omitempty"`
	}{b.arn(), in.Description}, nil
}

func (s *Service) deleteEventBus(c *core.Ctx, in *struct{ Name string }) (any, error) {
	if in.Name == "default" {
		return nil, invalid("Cannot delete event bus default.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Buses, key(c.Account, c.Region, in.Name))
	return nil, nil
}

func (s *Service) describeEventBus(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.Name)
	if err != nil {
		return nil, err
	}
	return b.out(), nil
}

type listInput struct {
	NamePrefix, NextToken string
	Limit                 int
}

func (s *Service) listEventBuses(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bus(c.Account, c.Region, "default")
	var out []busOut
	for _, b := range s.st.Buses {
		if b.Account == c.Account && b.Region == c.Region && strings.HasPrefix(b.Name, in.NamePrefix) {
			out = append(out, b.out())
		}
	}
	items, next := page(out, func(b busOut) string { return b.Name }, in.NextToken, in.Limit)
	return list("EventBuses", items, next), nil
}

// ---- rules ----

type ruleOut struct {
	Name, Arn, EventBusName, State, CreatedBy              string
	EventPattern, ScheduleExpression, Description, RoleArn string `json:",omitempty"`
}

func (b *Bus) ruleOut(r *Rule) ruleOut {
	return ruleOut{Name: r.Name, Arn: b.ruleARN(r.Name), EventBusName: b.Name, State: r.State, CreatedBy: b.Account,
		EventPattern: r.EventPattern, ScheduleExpression: r.ScheduleExpression, Description: r.Description, RoleArn: r.RoleArn}
}

func (s *Service) putRule(c *core.Ctx, in *struct {
	Name, Description, EventBusName, EventPattern, RoleArn, ScheduleExpression, State string
	Tags                                                                              []tag
}) (any, error) {
	state := cmp.Or(in.State, "ENABLED")
	switch {
	case !nameRe.MatchString(in.Name):
		return nil, invalid("Rule name %s is not valid.", in.Name)
	case in.EventPattern == "" && in.ScheduleExpression == "":
		return nil, invalid("Parameter(s) EventPattern or ScheduleExpression must be specified.")
	case state != "ENABLED" && state != "DISABLED" && state != "ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS":
		return nil, invalid("State %s is not valid.", state)
	}
	if in.EventPattern != "" {
		if _, err := compilePattern(in.EventPattern); err != nil {
			return nil, err
		}
	}
	if in.ScheduleExpression != "" {
		if _, err := ParseSchedule(in.ScheduleExpression); err != nil || strings.HasPrefix(in.ScheduleExpression, "at(") {
			return nil, invalid("Parameter ScheduleExpression is not valid.")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.EventBusName)
	if err != nil {
		return nil, err
	}
	if in.ScheduleExpression != "" && b.Name != "default" {
		return nil, invalid("ScheduleExpression is supported only on the default event bus.")
	}
	r := b.Rules[in.Name]
	if r == nil {
		r = &Rule{Name: in.Name}
		b.Rules[in.Name] = r
	}
	// Updating replaces the rule's settings, as in AWS; targets and tags stay.
	r.Description, r.EventPattern, r.ScheduleExpression, r.State, r.RoleArn = in.Description, in.EventPattern, in.ScheduleExpression, state, in.RoleArn
	r.Next, r.pat, r.Tags = nextRun(in.ScheduleExpression, time.Now()), nil, addTags(r.Tags, in.Tags)
	return map[string]string{"RuleArn": b.ruleARN(in.Name)}, nil
}

type ruleRef struct{ Name, EventBusName string }

func (s *Service) describeRule(c *core.Ctx, in *ruleRef) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, r, err := s.findRule(c, in.EventBusName, in.Name)
	if err != nil {
		return nil, err
	}
	return b.ruleOut(r), nil
}

func (s *Service) listRules(c *core.Ctx, in *struct {
	listInput
	EventBusName string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.EventBusName)
	if err != nil {
		return nil, err
	}
	var out []ruleOut
	for _, r := range b.Rules {
		if strings.HasPrefix(r.Name, in.NamePrefix) {
			out = append(out, b.ruleOut(r))
		}
	}
	items, next := page(out, func(r ruleOut) string { return r.Name }, in.NextToken, in.Limit)
	return list("Rules", items, next), nil
}

// deleteRule succeeds for rules that do not exist, as AWS does.
func (s *Service) deleteRule(c *core.Ctx, in *ruleRef) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.EventBusName)
	if err != nil {
		return nil, err
	}
	if r := b.Rules[in.Name]; r != nil && len(r.Targets) > 0 {
		return nil, invalid("Rule can't be deleted since it has targets.")
	}
	delete(b.Rules, in.Name)
	return nil, nil
}

func (s *Service) setState(state string) func(*core.Ctx, *ruleRef) (any, error) {
	return func(c *core.Ctx, in *ruleRef) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, r, err := s.findRule(c, in.EventBusName, in.Name)
		if err != nil {
			return nil, err
		}
		if r.State != state { // a re-enabled schedule restarts from now
			r.Next = nextRun(r.ScheduleExpression, time.Now())
		}
		r.State = state
		return nil, nil
	}
}

// ---- targets ----

func (t *Target) check() error {
	inputs, paths := 0, []string{}
	if t.Input != "" {
		inputs++
	}
	if t.InputPath != "" {
		inputs++
		paths = append(paths, t.InputPath)
	}
	if t.InputTransformer != nil {
		inputs++
		for _, p := range t.InputTransformer.InputPathsMap {
			paths = append(paths, p)
		}
	}
	switch {
	case !nameRe.MatchString(t.Id):
		return invalid("Target id %s is not valid.", t.Id)
	case t.Arn == "":
		return invalid("Target %s needs an Arn.", t.Id)
	case inputs > 1:
		return invalid("Only one of Input, InputPath, or InputTransformer must be provided for target %s.", t.Id)
	}
	for _, p := range paths {
		if !strings.HasPrefix(p, "$") {
			return invalid("JSONPath %s of target %s is not valid.", p, t.Id)
		}
	}
	return nil
}

var noFailures = map[string]any{"FailedEntryCount": 0, "FailedEntries": []any{}}

func (s *Service) putTargets(c *core.Ctx, in *struct {
	Rule, EventBusName string
	Targets            []*Target
}) (any, error) {
	if len(in.Targets) == 0 || len(in.Targets) > 10 {
		return nil, invalid("Targets must contain between 1 and 10 items.")
	}
	for _, t := range in.Targets {
		if err := t.check(); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, r, err := s.findRule(c, in.EventBusName, in.Rule)
	if err != nil {
		return nil, err
	}
	for _, t := range in.Targets {
		if i := slices.IndexFunc(r.Targets, func(x *Target) bool { return x.Id == t.Id }); i >= 0 {
			r.Targets[i] = t
		} else {
			r.Targets = append(r.Targets, t)
		}
	}
	return noFailures, nil
}

func (s *Service) removeTargets(c *core.Ctx, in *struct {
	Rule, EventBusName string
	Ids                []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, r, err := s.findRule(c, in.EventBusName, in.Rule)
	if err != nil {
		return nil, err
	}
	r.Targets = slices.DeleteFunc(r.Targets, func(t *Target) bool { return slices.Contains(in.Ids, t.Id) })
	return noFailures, nil
}

func (s *Service) listTargetsByRule(c *core.Ctx, in *struct {
	listInput
	Rule, EventBusName string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, r, err := s.findRule(c, in.EventBusName, in.Rule)
	if err != nil {
		return nil, err
	}
	items, next := page(slices.Clone(r.Targets), func(t *Target) string { return t.Id }, in.NextToken, in.Limit)
	return list("Targets", items, next), nil
}

func (s *Service) listRuleNamesByTarget(c *core.Ctx, in *struct {
	listInput
	TargetArn, EventBusName string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.EventBusName)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, r := range b.Rules {
		if slices.ContainsFunc(r.Targets, func(t *Target) bool { return t.Arn == in.TargetArn }) {
			names = append(names, r.Name)
		}
	}
	items, next := page(names, func(n string) string { return n }, in.NextToken, in.Limit)
	return list("RuleNames", items, next), nil
}

// ---- events ----

const timeFormat = "2006-01-02T15:04:05Z"

// event is the EventBridge envelope, in AWS field order.
type event struct {
	Version    string          `json:"version"`
	ID         string          `json:"id"`
	DetailType string          `json:"detail-type"`
	Source     string          `json:"source"`
	Account    string          `json:"account"`
	Time       string          `json:"time"`
	Region     string          `json:"region"`
	Resources  []string        `json:"resources"`
	Detail     json.RawMessage `json:"detail,omitempty"`
	ReplayName string          `json:"replay-name,omitempty"`
	trace      string          // the flow that caused it
}

type entry struct {
	Source, DetailType, Detail, EventBusName string
	Resources                                []string
	Time                                     *float64
}

// newEvent validates a PutEvents entry and builds its envelope.
func newEvent(account, region string, e *entry) (*event, error) {
	missing := ""
	switch {
	case e.Source == "":
		missing = "Source"
	case e.DetailType == "":
		missing = "DetailType"
	case e.Detail == "":
		missing = "Detail"
	}
	if missing != "" {
		return nil, core.Errorf(400, "InvalidArgument", "Parameter %[1]s is not valid. Reason: %[1]s is a required argument.", missing)
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(e.Detail), &obj) != nil || obj == nil {
		return nil, core.Errorf(400, "MalformedDetail", "Detail is malformed.")
	}
	t := time.Now()
	if e.Time != nil {
		t = fromUnix(*e.Time)
	}
	return &event{Version: "0", ID: core.UUID(), DetailType: e.DetailType, Source: e.Source, Account: account,
		Time: t.UTC().Format(timeFormat), Region: region, Resources: append([]string{}, e.Resources...), Detail: json.RawMessage(e.Detail)}, nil
}

func (s *Service) putEvents(c *core.Ctx, in *struct{ Entries []entry }) (any, error) {
	if len(in.Entries) == 0 || len(in.Entries) > 10 {
		return nil, invalid("Entries must contain between 1 and 10 items.")
	}
	type result struct {
		EventId, ErrorCode, ErrorMessage string `json:",omitempty"`
	}
	var out struct {
		Entries          []result
		FailedEntryCount int
	}
	for i := range in.Entries {
		account, region, bus := busRef(c.Account, c.Region, in.Entries[i].EventBusName)
		ev, err := newEvent(account, region, &in.Entries[i])
		if err != nil {
			e := core.AsError(err)
			out.Entries = append(out.Entries, result{ErrorCode: e.Code, ErrorMessage: e.Msg})
			out.FailedEntryCount++
			continue
		}
		ev.trace = c.Trace
		// AWS accepts events for buses that do not exist and drops them.
		if err := s.publish(account, region, bus, ev, nil, 0); err != nil {
			s.hub.Log.Debug("events: event dropped", "err", err)
		}
		out.Entries = append(out.Entries, result{EventId: ev.ID})
	}
	return &out, nil
}

// decodeEvent decodes an event for pattern matching, keeping numbers exact.
func decodeEvent(raw string) (map[string]any, error) {
	var m map[string]any
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&m); err != nil || m == nil {
		return nil, errors.New("not a JSON object")
	}
	return m, nil
}

func (s *Service) testEventPattern(c *core.Ctx, in *struct{ Event, EventPattern string }) (any, error) {
	p, err := compilePattern(in.EventPattern)
	if err != nil {
		return nil, err
	}
	ev, err := decodeEvent(in.Event)
	if err != nil {
		return nil, invalid("Parameter Event is not valid.")
	}
	return map[string]bool{"Result": p.Match(ev)}, nil
}

// ---- routing ----

// publish puts an event on a bus and delivers it to the targets of the
// matching rules (only the rule ARNs in only, when set).
func (s *Service) publish(account, region, bus string, ev *event, only []string, depth int) error {
	s.mu.Lock()
	b := s.bus(account, region, bus)
	var ds []delivery
	if b != nil {
		ds = s.route(b, ev, only)
	}
	s.mu.Unlock()
	if b == nil {
		return fmt.Errorf("event bus %s does not exist", bus)
	}
	s.send(ds, depth)
	return nil
}

// route archives an event and renders the deliveries of the enabled rules
// that match it; lock held. Replayed events are not archived again.
func (s *Service) route(b *Bus, ev *event, only []string) []delivery {
	raw := marshal(ev)
	m, _ := decodeEvent(raw)
	if ev.ReplayName == "" {
		for _, a := range s.st.Archives {
			if a.EventSourceArn == b.arn() && match(&a.pat, a.EventPattern, m) {
				a.Events = append(a.Events, archived{time.Now().UTC(), raw})
			}
		}
	}
	var ds []delivery
	for _, name := range core.SortedKeys(b.Rules) {
		r, arn := b.Rules[name], b.ruleARN(name)
		if r.State != "DISABLED" && r.EventPattern != "" && (len(only) == 0 || slices.Contains(only, arn)) && match(&r.pat, r.EventPattern, m) {
			ds = append(ds, r.deliveries(arn, ev, raw)...)
		}
	}
	return ds
}

// match tests an event against a pattern compiled on first use, so it
// survives Load; an empty pattern matches everything.
func match(cache **pattern.Pattern, src string, ev map[string]any) bool {
	if src == "" {
		return true
	}
	if *cache == nil {
		p, err := pattern.Compile(src)
		if err != nil {
			return false
		}
		*cache = p
	}
	return (*cache).Match(ev)
}

// delivery is one message for a target, sent once the lock is released.
type delivery struct {
	rule, arn, body, group, dlq, event string
	trace                              string
}

func (r *Rule) deliveries(ruleARN string, ev *event, raw string) []delivery {
	var ds []delivery
	for _, t := range r.Targets {
		d := delivery{rule: ruleARN, arn: t.Arn, body: t.input(ev, raw, ruleARN, r.Name), event: raw, trace: ev.trace}
		switch {
		case t.SqsParameters != nil:
			d.group = t.SqsParameters.MessageGroupId
		case t.KinesisParameters != nil: // core.Message has no partition key; GroupID carries it
			v, _ := JSONPath(raw, t.KinesisParameters.PartitionKeyPath)
			d.group = Text(v)
		case strings.Contains(t.Arn, ":kinesis:"):
			d.group = ev.ID // AWS partitions by event id by default
		}
		if t.DeadLetterConfig != nil {
			d.dlq = t.DeadLetterConfig.Arn
		}
		ds = append(ds, d)
	}
	return ds
}

// send makes deliveries outside the lock; a failed one goes to the target's
// dead-letter queue when it has one.
func (s *Service) send(ds []delivery, depth int) {
	for _, d := range ds {
		err := s.sendOne(d, depth)
		if err != nil && d.dlq != "" {
			err = s.hub.Deliver(d.dlq, &core.Message{Body: d.event, Trace: d.trace, From: d.rule, Attributes: map[string]core.MessageAttribute{
				"RULE_ARN":      {DataType: "String", StringValue: d.rule},
				"TARGET_ARN":    {DataType: "String", StringValue: d.arn},
				"ERROR_MESSAGE": {DataType: "String", StringValue: err.Error()},
			}})
		}
		if err != nil {
			s.hub.Log.Warn("events: delivery failed", "rule", d.rule, "target", d.arn, "err", err)
		}
	}
}

// sendOne goes through the hub, except for event bus targets: those are
// routed here with a hop count so buses forwarding to each other cannot
// recurse forever.
func (s *Service) sendOne(d delivery, depth int) error {
	if svc, _, _, res, _ := core.ParseARN(d.arn); svc == "events" && strings.HasPrefix(res, "event-bus/") {
		return s.forward(d.arn, d.body, depth+1, d.trace)
	}
	return s.hub.Deliver(d.arn, &core.Message{Body: d.body, GroupID: d.group, Trace: d.trace, From: d.rule})
}

// forward routes an event envelope sent on by another bus.
func (s *Service) forward(arn, body string, depth int, trace string) error {
	if depth > 10 {
		return errors.New("event forwarded between buses more than 10 times")
	}
	var ev event
	if err := json.Unmarshal([]byte(body), &ev); err != nil || ev.Source == "" || ev.DetailType == "" {
		return fmt.Errorf("%s only accepts EventBridge events", arn)
	}
	ev.trace = trace
	_, region, account, res, _ := core.ParseARN(arn)
	return s.publish(account, region, strings.TrimPrefix(res, "event-bus/"), &ev, nil, depth)
}

// deliver receives hub messages for EventBridge ARNs: new events from other
// services (Source set, as S3 and Scheduler send them), envelopes forwarded
// by another bus, and API destination calls.
func (s *Service) deliver(arn string, m *core.Message) error {
	_, region, account, res, _ := core.ParseARN(arn)
	bus, isBus := strings.CutPrefix(res, "event-bus/")
	switch {
	case strings.HasPrefix(res, "api-destination/"):
		_, err := s.Invoke(arn, m.Body)
		return err
	case !isBus:
		return fmt.Errorf("events cannot deliver to %s", arn)
	case m.Source == "":
		return s.forward(arn, m.Body, 0, m.Trace)
	}
	ev, err := newEvent(account, region, &entry{Source: m.Source, DetailType: m.DetailType, Detail: m.Body, Resources: m.Resources})
	if err != nil {
		return err
	}
	ev.trace = m.Trace
	return s.publish(account, region, bus, ev, nil, 0)
}

// ---- input transformation ----

// input is what a target receives: its constant Input, the InputPath
// selection, the InputTransformer output, or else the whole event.
func (t *Target) input(ev *event, raw, ruleARN, ruleName string) string {
	switch {
	case t.Input != "":
		return t.Input
	case t.InputPath != "":
		if v, ok := JSONPath(raw, t.InputPath); ok {
			return string(v)
		}
		return "null"
	case t.InputTransformer != nil:
		return t.InputTransformer.render(ev, raw, ruleARN, ruleName)
	}
	return raw
}

// render fills the template's <placeholders>. Inside a JSON string a value
// goes in as text; anywhere else as its JSON value.
func (it *InputTransformer) render(ev *event, raw, ruleARN, ruleName string) string {
	vals := map[string]json.RawMessage{
		"aws.events.rule-arn":             json.RawMessage(marshal(ruleARN)),
		"aws.events.rule-name":            json.RawMessage(marshal(ruleName)),
		"aws.events.event.ingestion-time": json.RawMessage(marshal(time.Now().UTC().Format(timeFormat))),
		"aws.events.event.json":           json.RawMessage(raw),
	}
	if strings.Contains(it.InputTemplate, "<aws.events.event>") { // the event without its detail
		e := *ev
		e.Detail = nil
		vals["aws.events.event"] = json.RawMessage(marshal(&e))
	}
	for name, path := range it.InputPathsMap {
		vals[name], _ = JSONPath(raw, path) // nil when absent
	}
	return Render(it.InputTemplate, func(name string) (json.RawMessage, bool) { v, ok := vals[name]; return v, ok })
}

// Render fills a template's <placeholders> with the values lookup knows (nil
// for an absent one); EventBridge Pipes input templates share it. Inside a
// JSON string a value goes in as text; anywhere else as its JSON value.
func Render(tpl string, lookup func(name string) (json.RawMessage, bool)) string {
	inString := false
	var b strings.Builder
	for i := 0; i < len(tpl); i++ {
		ch := tpl[i]
		switch {
		case ch == '\\' && inString && i+1 < len(tpl):
			b.WriteByte(ch)
			i++
			ch = tpl[i]
		case ch == '"':
			inString = !inString
		case ch == '<':
			if end := strings.IndexByte(tpl[i:], '>'); end > 0 {
				if v, ok := lookup(tpl[i+1 : i+end]); ok {
					b.WriteString(fill(v, inString))
					i += end
					continue
				}
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func fill(v json.RawMessage, inString bool) string {
	switch {
	case inString && len(v) > 1 && v[0] == '"':
		return string(v[1 : len(v)-1])
	case v == nil && inString:
		return ""
	case v == nil:
		return "null"
	}
	return string(v)
}

// JSONPath returns the raw JSON at path, in the subset AWS users write:
// $, $.a.b, $.a[0] and $['a'].
func JSONPath(doc, path string) (json.RawMessage, bool) {
	if !strings.HasPrefix(path, "$") {
		return nil, false
	}
	cur := json.RawMessage(doc)
	for p := path[1:]; p != ""; {
		name, idx := "", -1
		switch p[0] {
		case '.':
			n := strings.IndexAny(p[1:], ".[") + 1
			if n == 0 {
				n = len(p)
			}
			name, p = p[1:n], p[n:]
		case '[':
			n := strings.IndexByte(p, ']')
			if n < 0 {
				return nil, false
			}
			sel := p[1:n]
			p = p[n+1:]
			if i, err := strconv.Atoi(sel); err == nil {
				idx = i
			} else {
				name = strings.Trim(sel, `'"`)
			}
		default:
			return nil, false
		}
		if idx >= 0 {
			var arr []json.RawMessage
			if json.Unmarshal(cur, &arr) != nil || idx >= len(arr) {
				return nil, false
			}
			cur = arr[idx]
			continue
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(cur, &obj) != nil || obj[name] == nil {
			return nil, false
		}
		cur = obj[name]
	}
	return cur, true
}

// Text is a JSON string's value, or the JSON text of anything else.
func Text(v json.RawMessage) string {
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s
	}
	return string(v)
}

// ---- schedules ----

// Tick fires the scheduled rules due at now. A goroutine calls it every
// second; tests call it with a later time instead of sleeping.
func (s *Service) Tick(now time.Time) {
	var ds []delivery
	s.mu.Lock()
	for _, b := range s.st.Buses {
		for _, r := range b.Rules {
			if r.Next.IsZero() || r.Next.After(now) || r.State == "DISABLED" {
				continue
			}
			arn := b.ruleARN(r.Name)
			ev := &event{Version: "0", ID: core.UUID(), DetailType: "Scheduled Event", Source: "aws.events", Account: b.Account,
				Time: r.Next.UTC().Format(timeFormat), Region: b.Region, Resources: []string{arn}, Detail: json.RawMessage("{}"), trace: core.NewTrace()}
			ds = append(ds, r.deliveries(arn, ev, marshal(ev))...)
			r.Next = advance(r.ScheduleExpression, r.Next, now)
		}
	}
	s.mu.Unlock()
	s.send(ds, 0)
}

// nextRun is the first run of a schedule expression after t, in UTC, or zero.
func nextRun(expr string, t time.Time) time.Time {
	sch, err := ParseSchedule(expr)
	if err != nil {
		return time.Time{}
	}
	return sch.Next(t.UTC())
}

// advance moves a schedule past now, skipping runs missed while pogi was busy or stopped.
func advance(expr string, prev, now time.Time) time.Time {
	if next := nextRun(expr, prev); next.IsZero() || next.After(now) {
		return next
	}
	return nextRun(expr, now)
}

// ---- tags and permissions ----

// tags finds the tag map of a bus or rule ARN; lock held.
func (s *Service) tags(arn string) (*map[string]string, error) {
	_, region, account, res, _ := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, "/")
	switch kind {
	case "event-bus":
		if b := s.bus(account, region, name); b != nil {
			return &b.Tags, nil
		}
	case "rule":
		bus, rule := "default", name
		if i := strings.LastIndexByte(name, '/'); i >= 0 {
			bus, rule = name[:i], name[i+1:]
		}
		if b := s.bus(account, region, bus); b != nil && b.Rules[rule] != nil {
			return &b.Rules[rule].Tags, nil
		}
	}
	return nil, notFound("Resource %s does not exist.", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceARN string
	Tags        []tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.tags(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	*m = addTags(*m, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceARN string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.tags(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(*m, k)
	}
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *struct{ ResourceARN string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.tags(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	out := []tag{}
	for _, k := range core.SortedKeys(*m) {
		out = append(out, tag{k, (*m)[k]})
	}
	return map[string]any{"Tags": out}, nil
}

func addTags(m map[string]string, tags []tag) map[string]string {
	if m == nil && len(tags) > 0 {
		m = map[string]string{}
	}
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

type policy struct {
	Version   string
	Statement []statement
}

type statement struct {
	Sid       string
	Effect    string
	Principal any
	Action    any
	Resource  any
	Condition any `json:",omitempty"`
}

func (b *Bus) policy() policy {
	p := policy{Version: "2012-10-17"}
	json.Unmarshal([]byte(b.Policy), &p)
	return p
}

// putPermission sets the bus policy, or adds a statement letting an account
// (or everyone, "*") call PutEvents on the bus.
func (s *Service) putPermission(c *core.Ctx, in *struct {
	EventBusName, Action, Principal, StatementId, Policy string
	Condition                                            *struct{ Type, Key, Value string }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.EventBusName)
	if err != nil {
		return nil, err
	}
	switch {
	case in.Policy != "" && !json.Valid([]byte(in.Policy)):
		return nil, invalid("Policy is not valid JSON.")
	case in.Policy != "":
		b.Policy, b.Modified = in.Policy, time.Now().UTC()
		return nil, nil
	case in.Action != "events:PutEvents":
		return nil, invalid("Provided value in parameter 'action' is not supported.")
	case in.StatementId == "" || in.Principal == "":
		return nil, invalid("StatementId and Principal are required.")
	}
	st := statement{Sid: in.StatementId, Effect: "Allow", Principal: "*", Action: in.Action, Resource: b.arn()}
	if in.Principal != "*" {
		st.Principal = map[string]string{"AWS": core.GlobalARN("iam", c.Region, in.Principal, "root")}
	}
	if in.Condition != nil {
		st.Condition = map[string]map[string]string{in.Condition.Type: {in.Condition.Key: in.Condition.Value}}
	}
	p := b.policy()
	p.Statement = append(slices.DeleteFunc(p.Statement, func(x statement) bool { return x.Sid == in.StatementId }), st)
	b.Policy, b.Modified = marshal(p), time.Now().UTC()
	return nil, nil
}

func (s *Service) removePermission(c *core.Ctx, in *struct {
	EventBusName, StatementId string
	RemoveAllPermissions      bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.findBus(c, in.EventBusName)
	if err != nil {
		return nil, err
	}
	if in.RemoveAllPermissions {
		b.Policy = ""
		return nil, nil
	}
	if b.Policy == "" {
		return nil, notFound("EventBus does not have a policy.")
	}
	p := b.policy()
	n := len(p.Statement)
	p.Statement = slices.DeleteFunc(p.Statement, func(x statement) bool { return x.Sid == in.StatementId })
	if len(p.Statement) == n {
		return nil, notFound("Statement with the provided id does not exist.")
	}
	b.Policy = ""
	if len(p.Statement) > 0 {
		b.Policy = marshal(p)
	}
	return nil, nil
}

// ---- console ----

// Resources lists buses and rules for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	buses := core.Table{Name: "Buses", Columns: []string{"Name", "Region", "Account", "Rules"}}
	rules := core.Table{Name: "Rules", Columns: []string{"Name", "Bus", "State", "Pattern/Schedule", "Targets"}}
	for _, k := range core.SortedKeys(s.st.Buses) {
		b := s.st.Buses[k]
		buses.Rows = append(buses.Rows, []string{b.Name, b.Region, b.Account, strconv.Itoa(len(b.Rules))})
		for _, n := range core.SortedKeys(b.Rules) {
			r := b.Rules[n]
			rules.Rows = append(rules.Rows, []string{r.Name, b.Name, r.State, cmp.Or(r.EventPattern, r.ScheduleExpression), strconv.Itoa(len(r.Targets))})
		}
	}
	return []core.Table{buses, rules}
}

// Inspect returns a bus (with its rules) or a rule (with its targets).
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Buses) {
		b := s.st.Buses[k]
		if table == "Buses" && b.Name == id {
			return json.RawMessage(marshal(map[string]any{"arn": b.arn(), "bus": b}))
		}
		if r := b.Rules[id]; table == "Rules" && r != nil {
			return json.RawMessage(marshal(map[string]any{"arn": b.ruleARN(id), "bus": b.Name, "rule": r}))
		}
	}
	return nil
}

// PutEvent publishes an event from the console and returns its id.
func (s *Service) PutEvent(account, region, bus, source, detailType, detail string) (string, error) {
	account, region, bus = busRef(account, region, bus)
	ev, err := newEvent(account, region, &entry{Source: source, DetailType: detailType, Detail: detail})
	if err != nil {
		return "", err
	}
	ev.trace = core.NewTrace()
	return ev.ID, s.publish(account, region, bus, ev, nil, 0)
}

// ---- helpers ----

// page sorts items by key and returns those after token, at most limit (default 100).
func page[T any](items []T, key func(T) string, token string, limit int) ([]T, string) {
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
	items = items[sort.Search(len(items), func(i int) bool { return key(items[i]) > token }):]
	if limit <= 0 {
		limit = 100
	}
	if len(items) > limit {
		return items[:limit], key(items[limit-1])
	}
	if items == nil {
		items = []T{}
	}
	return items, ""
}

func list(name string, items any, next string) map[string]any {
	out := map[string]any{name: items}
	if next != "" {
		out["NextToken"] = next
	}
	return out
}

// marshal encodes JSON without Go's HTML escaping, as AWS does.
func marshal(v any) string {
	var b strings.Builder
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	e.Encode(v)
	return strings.TrimSuffix(b.String(), "\n")
}

func fromUnix(sec float64) time.Time { return time.UnixMilli(int64(math.Round(sec * 1000))).UTC() }

// TaggedResources lists buses and the rules on every bus (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, b := range s.st.Buses {
		if b.Account != account || b.Region != region {
			continue
		}
		out[b.arn()] = maps.Clone(b.Tags)
		for name, r := range b.Rules {
			out[b.ruleARN(name)] = maps.Clone(r.Tags)
		}
	}
	return out
}
