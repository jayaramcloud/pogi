// Package fis emulates AWS Fault Injection Service (REST-JSON): experiment
// templates, and experiments whose actions drive pogi's chaos rules (API
// errors for the calls of target IAM roles, Lambda invocation errors and
// delays) or stop EC2 instances and ECS tasks, halted by CloudWatch alarm
// stop conditions.
package fis

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Fault Injection Service emulator.
type Service struct {
	hub  *core.Hub
	mux  *http.ServeMux
	stop func()
	mu   sync.Mutex
	st   state
}

type state struct {
	Templates   map[string]*Template   `json:"templates"`   // account/region/id
	Experiments map[string]*Experiment `json:"experiments"` // account/region/id
}

func newState() state {
	return state{Templates: map[string]*Template{}, Experiments: map[string]*Experiment{}}
}

// Template is an experiment template in the API's shape.
type Template struct {
	ID                            string             `json:"id"`
	Arn                           string             `json:"arn"`
	Description                   string             `json:"description"`
	Targets                       map[string]*Target `json:"targets"`
	Actions                       map[string]*Action `json:"actions"`
	StopConditions                []StopCondition    `json:"stopConditions"`
	CreationTime                  core.Time          `json:"creationTime"`
	LastUpdateTime                core.Time          `json:"lastUpdateTime"`
	RoleArn                       string             `json:"roleArn"`
	Tags                          map[string]string  `json:"tags,omitempty"`
	LogConfiguration              json.RawMessage    `json:"logConfiguration,omitempty"`
	ExperimentOptions             json.RawMessage    `json:"experimentOptions,omitempty"`
	ExperimentReportConfiguration json.RawMessage    `json:"experimentReportConfiguration,omitempty"`
}

// Target selects the resources actions act on.
type Target struct {
	ResourceType  string            `json:"resourceType"`
	ResourceArns  []string          `json:"resourceArns,omitempty"`
	ResourceTags  map[string]string `json:"resourceTags,omitempty"`
	Filters       json.RawMessage   `json:"filters,omitempty"` // ponytail: kept, not applied
	SelectionMode string            `json:"selectionMode"`
	Parameters    map[string]string `json:"parameters,omitempty"`
}

// Action is an action of a template, and with a state and times, of an experiment.
type Action struct {
	ActionID    string            `json:"actionId"`
	Description string            `json:"description,omitempty"`
	Parameters  map[string]string `json:"parameters,omitempty"`
	Targets     map[string]string `json:"targets,omitempty"`
	StartAfter  []string          `json:"startAfter,omitempty"`
	State       *State            `json:"state,omitempty"`
	StartTime   *core.Time        `json:"startTime,omitempty"`
	EndTime     *core.Time        `json:"endTime,omitempty"`
	until       time.Time         // when a running action with a duration ends
	rules       []string          // the chaos rules it added
}

// StopCondition stops an experiment when its alarm goes into ALARM.
type StopCondition struct {
	Source string `json:"source"`
	Value  string `json:"value,omitempty"`
}

// State is the status of an experiment or of one of its actions.
type State struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Experiment is a run of a template.
type Experiment struct {
	ID                   string              `json:"id"`
	Arn                  string              `json:"arn"`
	ExperimentTemplateID string              `json:"experimentTemplateId"`
	RoleArn              string              `json:"roleArn"`
	State                State               `json:"state"`
	Targets              map[string]*Target  `json:"targets"`
	Actions              map[string]*Action  `json:"actions"`
	StopConditions       []StopCondition     `json:"stopConditions"`
	CreationTime         core.Time           `json:"creationTime"`
	StartTime            *core.Time          `json:"startTime,omitempty"`
	EndTime              *core.Time          `json:"endTime,omitempty"`
	Tags                 map[string]string   `json:"tags,omitempty"`
	LogConfiguration     json.RawMessage     `json:"logConfiguration,omitempty"`
	ExperimentOptions    json.RawMessage     `json:"experimentOptions,omitempty"`
	resolved             map[string][]string // target name → resource ARNs
}

// actionType is one action of the catalog: its target and parameters.
type actionType struct {
	description, target, key string          // target resource type and its key in an action's targets
	params                   map[string]bool // name → required
}

var apiParams = map[string]bool{"service": true, "operations": true, "percentage": true, "duration": true}

// catalog is the actions pogi runs.
var catalog = map[string]actionType{
	"aws:fis:inject-api-internal-error":    {"Injects internal errors into the requests made by the target IAM roles.", "aws:iam:role", "Roles", apiParams},
	"aws:fis:inject-api-throttle-error":    {"Injects throttling errors into the requests made by the target IAM roles.", "aws:iam:role", "Roles", apiParams},
	"aws:fis:inject-api-unavailable-error": {"Injects service unavailable errors into the requests made by the target IAM roles.", "aws:iam:role", "Roles", apiParams},
	"aws:fis:wait":                         {"Waits for the specified duration.", "", "", map[string]bool{"duration": true}},
	"aws:ec2:stop-instances": {"Stops the target EC2 instances, and starts them again after startInstancesAfterDuration.", "aws:ec2:instance", "Instances",
		map[string]bool{"startInstancesAfterDuration": false, "completeIfInstancesTerminated": false}},
	"aws:ec2:terminate-instances": {"Terminates the target EC2 instances.", "aws:ec2:instance", "Instances", map[string]bool{}},
	"aws:ecs:stop-task":           {"Stops the target ECS tasks.", "aws:ecs:task", "Tasks", map[string]bool{}},
	"aws:lambda:invocation-error": {"Fails invocations of the target Lambda functions.", "aws:lambda:function", "Functions",
		map[string]bool{"duration": true, "invocationPercentage": false, "preventExecution": false}},
	"aws:lambda:invocation-add-delay": {"Delays the start of invocations of the target Lambda functions.", "aws:lambda:function", "Functions",
		map[string]bool{"duration": true, "invocationPercentage": false, "startupDelayMilliseconds": false}},
	"aws:dynamodb:global-table-pause-replication": {"Pauses replication of the target global tables; pogi does not replicate, so it only waits.",
		"aws:dynamodb:global-table", "Tables", map[string]bool{"duration": true}},
}

// resourceTypes are the target types, with the ARN service and resource prefix of their resources.
var resourceTypes = map[string][3]string{
	"aws:iam:role":              {"Identity and Access Management roles.", "iam", "role/"},
	"aws:ec2:instance":          {"Amazon EC2 instances.", "ec2", "instance/"},
	"aws:ecs:task":              {"Amazon ECS tasks.", "ecs", "task/"},
	"aws:lambda:function":       {"AWS Lambda functions.", "lambda", "function:"},
	"aws:dynamodb:global-table": {"Amazon DynamoDB global tables.", "dynamodb", "table/"},
}

// faults are the chaos errors of the API injection actions.
var faults = map[string]struct {
	status int
	code   string
}{
	"aws:fis:inject-api-throttle-error":    {400, "ThrottlingException"},
	"aws:fis:inject-api-internal-error":    {500, "InternalError"},
	"aws:fis:inject-api-unavailable-error": {503, "ServiceUnavailable"},
}

// New creates the FIS service and starts its clock.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: newState()}
	for pattern, op := range map[string]struct {
		name string
		fn   func(*core.Ctx, *http.Request) (any, error)
	}{
		"POST /experimentTemplates":             {"CreateExperimentTemplate", s.createTemplate},
		"GET /experimentTemplates":              {"ListExperimentTemplates", s.listTemplates},
		"GET /experimentTemplates/{id}":         {"GetExperimentTemplate", s.getTemplate},
		"PATCH /experimentTemplates/{id}":       {"UpdateExperimentTemplate", s.updateTemplate},
		"DELETE /experimentTemplates/{id}":      {"DeleteExperimentTemplate", s.deleteTemplate},
		"POST /experiments":                     {"StartExperiment", s.startExperiment},
		"GET /experiments":                      {"ListExperiments", s.listExperiments},
		"GET /experiments/{id}":                 {"GetExperiment", s.getExperiment},
		"DELETE /experiments/{id}":              {"StopExperiment", s.stopExperiment},
		"GET /actions":                          {"ListActions", s.listActions},
		"GET /actions/{id}":                     {"GetAction", s.getAction},
		"GET /targetResourceTypes":              {"ListTargetResourceTypes", s.listResourceTypes},
		"GET /targetResourceTypes/{type}":       {"GetTargetResourceType", s.getResourceType},
		"POST /tags/{arn...}":                   {"TagResource", s.tagResource},
		"GET /tags/{arn...}":                    {"ListTagsForResource", s.listTags},
		"DELETE /tags/{arn...}":                 {"UntagResource", s.untagResource},
		"GET /experiments/{id}/resolvedTargets": {"ListExperimentResolvedTargets", s.resolvedTargets},
		"GET /experimentTemplates/{id}/targetAccountConfigurations": {"ListTargetAccountConfigurations", func(*core.Ctx, *http.Request) (any, error) {
			return map[string]any{"targetAccountConfigurations": []any{}}, nil
		}},
		"/": {"Unknown", func(c *core.Ctx, r *http.Request) (any, error) {
			return nil, core.Errorf(501, "NotImplemented", "pogi does not implement FIS %s %s", r.Method, r.URL.Path)
		}},
	} {
		s.mux.HandleFunc(pattern, s.handle(op.name, op.fn))
	}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "fis", Cloud: core.AWS, Title: "Fault Injection Service", Signing: []string{"fis"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Close stops the clock.
func (s *Service) Close() error { s.stop(); return nil }

// Reset ends running experiments, removing their chaos rules, and clears all.
func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.st.Experiments {
		for _, a := range e.Actions {
			s.unrule(a)
		}
	}
	s.st = newState()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores templates and experiments; experiments that were running
// fail, as their chaos rules did not survive.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	now := time.Now()
	for _, e := range st.Experiments {
		if e.live() {
			end(e, now, "failed", "pogi restarted while the experiment ran")
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// handle runs one operation and writes its REST-JSON result or error.
func (s *Service) handle(op string, fn func(*core.Ctx, *http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		var out any
		err := c.Intercept()
		if err == nil {
			out, err = fn(c, r)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if out == nil {
			out = struct{}{}
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	}
}

func validation(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "ResourceNotFoundException", format, a...)
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 {
		if err := json.Unmarshal(b, v); err != nil {
			return validation("Invalid request body: %v", err)
		}
	}
	return nil
}

func key(c *core.Ctx, id string) string { return c.Account + "/" + c.Region + "/" + id }

func ptr[T any](v T) *T { return &v }

// snap renders stored state under the lock, so the clock cannot change it
// while the response is written.
func snap(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

var isoDuration = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

// duration parses an ISO 8601 duration such as PT5M.
func duration(s string) (time.Duration, error) {
	m := isoDuration.FindStringSubmatch(s)
	if m == nil || strings.HasSuffix(s, "P") || strings.HasSuffix(s, "T") {
		return 0, validation("Invalid duration %q: use ISO 8601, such as PT5M", s)
	}
	var d time.Duration
	for i, unit := range []time.Duration{24 * time.Hour, time.Hour, time.Minute} {
		n, _ := strconv.Atoi(m[i+1])
		d += time.Duration(n) * unit
	}
	sec, _ := strconv.ParseFloat(cmp.Or(m[4], "0"), 64)
	return d + time.Duration(sec*float64(time.Second)), nil
}

var selection = regexp.MustCompile(`^(ALL|COUNT\((\d+)\)|PERCENT\((\d+)\))$`)

// ---- templates ----

type templateInput struct {
	Description                   *string
	StopConditions                []StopCondition
	Targets                       map[string]*Target
	Actions                       map[string]*Action
	RoleArn                       *string
	Tags                          map[string]string
	LogConfiguration              json.RawMessage
	ExperimentOptions             json.RawMessage
	ExperimentReportConfiguration json.RawMessage
}

// apply sets what the input gives; a target without a resource type or an
// action without an action ID removes it.
func (t *Template) apply(in *templateInput) {
	if in.Description != nil {
		t.Description = *in.Description
	}
	if in.RoleArn != nil {
		t.RoleArn = *in.RoleArn
	}
	if in.StopConditions != nil {
		t.StopConditions = in.StopConditions
	}
	for name, tg := range in.Targets {
		delete(t.Targets, name)
		if tg != nil && tg.ResourceType != "" {
			t.Targets[name] = tg
		}
	}
	for name, a := range in.Actions {
		delete(t.Actions, name)
		if a != nil && a.ActionID != "" {
			a.State, a.StartTime, a.EndTime = nil, nil, nil
			t.Actions[name] = a
		}
	}
	for dst, v := range map[*json.RawMessage]json.RawMessage{&t.LogConfiguration: in.LogConfiguration, &t.ExperimentOptions: in.ExperimentOptions,
		&t.ExperimentReportConfiguration: in.ExperimentReportConfiguration} {
		if len(v) > 0 && string(v) != "null" {
			*dst = v
		}
	}
}

// validate checks a template the way FIS does before storing it.
func (t *Template) validate() error {
	if t.RoleArn == "" {
		return validation("roleArn is required")
	}
	if len(t.Actions) == 0 {
		return validation("An experiment template must have at least one action")
	}
	for name, tg := range t.Targets {
		if _, ok := resourceTypes[tg.ResourceType]; !ok {
			return validation("Target %s: resource type %s is not supported", name, tg.ResourceType)
		}
		if !selection.MatchString(tg.SelectionMode) {
			return validation("Target %s: selection mode %q must be ALL, COUNT(n) or PERCENT(n)", name, tg.SelectionMode)
		}
		if len(tg.ResourceArns) == 0 && len(tg.ResourceTags) == 0 {
			return validation("Target %s must have resourceArns or resourceTags", name)
		}
	}
	for name, a := range t.Actions {
		at, ok := catalog[a.ActionID]
		if !ok {
			return validation("Action %s: action type %s is not supported", name, a.ActionID)
		}
		for p, required := range at.params {
			if required && a.Parameters[p] == "" {
				return validation("Action %s: parameter %s is required", name, p)
			}
		}
		for _, p := range []string{"duration", "startInstancesAfterDuration"} {
			if v := a.Parameters[p]; v != "" {
				if _, err := duration(v); err != nil {
					return err
				}
			}
		}
		if p, ok := a.Parameters["percentage"]; ok {
			if n, err := strconv.ParseFloat(p, 64); err != nil || n <= 0 || n > 100 {
				return validation("Action %s: percentage must be between 1 and 100", name)
			}
		}
		for k, target := range a.Targets {
			if k != at.key || t.Targets[target] == nil || t.Targets[target].ResourceType != at.target {
				return validation("Action %s: target %s=%s is not a %s target of the template", name, k, target, at.target)
			}
		}
		for _, after := range a.StartAfter {
			if t.Actions[after] == nil || after == name {
				return validation("Action %s: startAfter names unknown action %s", name, after)
			}
		}
	}
	for _, sc := range t.StopConditions {
		if sc.Source != "none" && (sc.Source != "aws:cloudwatch:alarm" || sc.Value == "") {
			return validation("Stop condition source must be none or aws:cloudwatch:alarm with an alarm ARN")
		}
	}
	return nil
}

func (s *Service) createTemplate(c *core.Ctx, r *http.Request) (any, error) {
	var in templateInput
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	now := core.Time(time.Now())
	id := "EXT" + core.RandString(14, core.AlphaNum)
	t := &Template{ID: id, Arn: c.ARN("fis", "experiment-template/"+id), Targets: map[string]*Target{}, Actions: map[string]*Action{},
		StopConditions: []StopCondition{}, CreationTime: now, LastUpdateTime: now, Tags: in.Tags,
		ExperimentOptions: json.RawMessage(`{"accountTargeting":"single-account","emptyTargetResolutionMode":"fail"}`)}
	t.apply(&in)
	if err := t.validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Templates[key(c, id)] = t
	return map[string]any{"experimentTemplate": snap(t)}, nil
}

// template finds a template; lock held.
func (s *Service) template(c *core.Ctx, id string) (*Template, error) {
	if t := s.st.Templates[key(c, id)]; t != nil {
		return t, nil
	}
	return nil, notFound("Experiment template %s not found.", id)
}

func (s *Service) getTemplate(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.template(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"experimentTemplate": snap(t)}, nil
}

func (s *Service) updateTemplate(c *core.Ctx, r *http.Request) (any, error) {
	var in templateInput
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.template(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	var next Template
	copyJSON(&next, t)
	next.apply(&in)
	if err := next.validate(); err != nil {
		return nil, err
	}
	next.LastUpdateTime = core.Time(time.Now())
	*t = next
	return map[string]any{"experimentTemplate": snap(t)}, nil
}

func (s *Service) deleteTemplate(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.template(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Templates, key(c, t.ID))
	return map[string]any{"experimentTemplate": snap(t)}, nil
}

func (s *Service) listTemplates(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []*Template{}
	for _, k := range core.SortedKeys(s.st.Templates) {
		if strings.HasPrefix(k, key(c, "")) {
			list = append(list, s.st.Templates[k])
		}
	}
	return map[string]any{"experimentTemplates": snap(list)}, nil
}

// copyJSON deep-copies src into dst.
func copyJSON(dst, src any) {
	b, _ := json.Marshal(src)
	json.Unmarshal(b, dst)
}

// ---- experiments ----

func (s *Service) startExperiment(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ExperimentTemplateID string
		ExperimentOptions    json.RawMessage
		Tags                 map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.template(c, in.ExperimentTemplateID)
	if err != nil {
		return nil, err
	}
	id := "EXP" + core.RandString(14, core.AlphaNum)
	e := &Experiment{ID: id, Arn: c.ARN("fis", "experiment/"+id), ExperimentTemplateID: t.ID, RoleArn: t.RoleArn,
		State: State{"pending", "Experiment is pending."}, CreationTime: core.Time(time.Now()), Tags: in.Tags,
		StopConditions: t.StopConditions, LogConfiguration: t.LogConfiguration, ExperimentOptions: t.ExperimentOptions}
	copyJSON(&e.Targets, t.Targets)
	copyJSON(&e.Actions, t.Actions)
	for _, a := range e.Actions {
		a.State = &State{"pending", "Initial state"}
	}
	s.st.Experiments[key(c, id)] = e
	return map[string]any{"experiment": snap(e)}, nil
}

// experiment finds an experiment; lock held.
func (s *Service) experiment(c *core.Ctx, id string) (*Experiment, error) {
	if e := s.st.Experiments[key(c, id)]; e != nil {
		return e, nil
	}
	return nil, notFound("Experiment %s not found.", id)
}

func (s *Service) getExperiment(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.experiment(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"experiment": snap(e)}, nil
}

func (s *Service) stopExperiment(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.experiment(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	if e.live() {
		s.end(e, time.Now(), "stopped", "Experiment stopped by user.")
	}
	return map[string]any{"experiment": snap(e)}, nil
}

func (s *Service) listExperiments(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []*Experiment{}
	for k, e := range s.st.Experiments {
		if t := r.URL.Query().Get("experimentTemplateId"); strings.HasPrefix(k, key(c, "")) && (t == "" || e.ExperimentTemplateID == t) {
			list = append(list, e)
		}
	}
	slices.SortFunc(list, func(a, b *Experiment) int { return time.Time(b.CreationTime).Compare(time.Time(a.CreationTime)) })
	return map[string]any{"experiments": snap(list)}, nil
}

func (s *Service) resolvedTargets(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.experiment(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, name := range core.SortedKeys(e.resolved) {
		for _, arn := range e.resolved[name] {
			list = append(list, map[string]any{"resourceType": e.Targets[name].ResourceType, "targetName": name,
				"targetInformation": map[string]string{"resourceArn": arn}})
		}
	}
	return map[string]any{"resolvedTargets": list}, nil
}

// ---- catalog ----

func actionSummary(region, id string) map[string]any {
	at := catalog[id]
	out := map[string]any{"id": id, "arn": core.ARN("fis", region, "", "action/"+id), "description": at.description, "targets": map[string]any{}}
	if at.target != "" {
		out["targets"] = map[string]any{at.key: map[string]string{"resourceType": at.target}}
	}
	return out
}

func (s *Service) listActions(c *core.Ctx, r *http.Request) (any, error) {
	list := []map[string]any{}
	for _, id := range core.SortedKeys(catalog) {
		list = append(list, actionSummary(c.Region, id))
	}
	return map[string]any{"actions": list}, nil
}

func (s *Service) getAction(c *core.Ctx, r *http.Request) (any, error) {
	at, ok := catalog[r.PathValue("id")]
	if !ok {
		return nil, notFound("Action %s not found.", r.PathValue("id"))
	}
	out := actionSummary(c.Region, r.PathValue("id"))
	params := map[string]any{}
	for p, required := range at.params {
		params[p] = map[string]any{"description": p, "required": required}
	}
	out["parameters"] = params
	return map[string]any{"action": out}, nil
}

func (s *Service) listResourceTypes(c *core.Ctx, r *http.Request) (any, error) {
	list := []map[string]string{}
	for _, t := range core.SortedKeys(resourceTypes) {
		list = append(list, map[string]string{"resourceType": t, "description": resourceTypes[t][0]})
	}
	return map[string]any{"targetResourceTypes": list}, nil
}

func (s *Service) getResourceType(c *core.Ctx, r *http.Request) (any, error) {
	t, ok := resourceTypes[r.PathValue("type")]
	if !ok {
		return nil, notFound("Target resource type %s not found.", r.PathValue("type"))
	}
	return map[string]any{"targetResourceType": map[string]any{"resourceType": r.PathValue("type"), "description": t[0], "parameters": map[string]any{}}}, nil
}

// ---- tags ----

// tags returns the tags of a template or experiment, found by the ID its
// ARN ends in, as Terraform leaves the account out. Lock held.
func (s *Service) tags(c *core.Ctx, arn string) (*map[string]string, error) {
	_, _, _, res, _ := core.ParseARN(arn)
	if t := s.st.Templates[key(c, path.Base(res))]; t != nil && strings.HasPrefix(res, "experiment-template/") {
		return &t.Tags, nil
	}
	if e := s.st.Experiments[key(c, path.Base(res))]; e != nil && strings.HasPrefix(res, "experiment/") {
		return &e.Tags, nil
	}
	return nil, notFound("Resource %s not found.", arn)
}

func (s *Service) tagResource(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Tags map[string]string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tags(c, r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	for k, v := range in.Tags {
		(*tags)[k] = v
	}
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tags(c, r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(*tags, k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tags(c, r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for k, v := range *tags {
		out[k] = v
	}
	return map[string]any{"tags": out}, nil
}

// ---- running experiments ----

func (e *Experiment) live() bool {
	return e.State.Status == "pending" || e.State.Status == "initiating" || e.State.Status == "running"
}

// Tick moves running experiments along: it resolves targets, checks stop
// conditions, starts actions whose startAfter actions completed, and ends
// actions whose duration passed.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Experiments) {
		if e := s.st.Experiments[k]; e.live() {
			s.step(e, now)
		}
	}
}

// step advances one experiment; lock held. The services it calls never
// call back into FIS.
func (s *Service) step(e *Experiment, now time.Time) {
	_, region, account, _, _ := core.ParseARN(e.Arn)
	if e.State.Status == "pending" {
		e.State = State{"initiating", "Experiment is initiating."}
		if err := s.resolve(e, account, region); err != nil {
			end(e, now, "failed", err.Error())
			return
		}
		e.State, e.StartTime = State{"running", "Experiment is running."}, ptr(core.Time(now))
	}
	if alarm := s.alarm(e, account, region); alarm != "" {
		s.end(e, now, "stopped", "Experiment halted by stop condition "+alarm+".")
		return
	}
	for changed := true; changed; {
		changed = false
		for _, name := range core.SortedKeys(e.Actions) {
			a := e.Actions[name]
			switch {
			case a.State.Status == "pending" && e.ready(a):
				if err := s.start(e, a, account, region, now); err != nil {
					s.unrule(a)
					a.State, a.EndTime = &State{"failed", err.Error()}, ptr(core.Time(now))
					s.end(e, now, "failed", "Action "+name+" failed: "+err.Error())
					return
				}
				changed = true
			case a.State.Status == "running" && !now.Before(a.until):
				s.finish(e, a, account, region, now)
				changed = true
			}
		}
	}
	for _, a := range e.Actions {
		if a.State.Status == "pending" || a.State.Status == "running" {
			return
		}
	}
	e.State, e.EndTime = State{"completed", "Experiment completed."}, ptr(core.Time(now))
}

// ready reports whether every action an action starts after has completed.
func (e *Experiment) ready(a *Action) bool {
	return !slices.ContainsFunc(a.StartAfter, func(n string) bool { return e.Actions[n] == nil || e.Actions[n].State.Status != "completed" })
}

// end stops an experiment's actions and gives it a final state; lock held.
func (s *Service) end(e *Experiment, now time.Time, status, reason string) {
	for _, a := range e.Actions {
		s.unrule(a)
	}
	end(e, now, status, reason)
}

func end(e *Experiment, now time.Time, status, reason string) {
	for _, a := range e.Actions {
		switch a.State.Status {
		case "running":
			a.State, a.EndTime = &State{"stopped", "Experiment ended."}, ptr(core.Time(now))
		case "pending":
			a.State = &State{"cancelled", "Experiment ended."}
		}
	}
	e.State, e.EndTime = State{status, reason}, ptr(core.Time(now))
}

func (s *Service) unrule(a *Action) {
	for _, id := range a.rules {
		s.hub.Chaos.Remove(id)
	}
	a.rules = nil
}

// resolve finds the resources of each target: its ARNs and the resources
// with its tags, then picks per selection mode.
// ponytail: resources by tag come from services that list their tags (EC2
// instances); target filters are not applied.
func (s *Service) resolve(e *Experiment, account, region string) error {
	e.resolved = map[string][]string{}
	for _, name := range core.SortedKeys(e.Targets) {
		t := e.Targets[name]
		arns := slices.Clone(t.ResourceArns)
		if len(t.ResourceTags) > 0 {
			shape := resourceTypes[t.ResourceType]
			for _, svc := range s.hub.Services() {
				tagged, ok := svc.(interface {
					TaggedResources(account, region string) map[string]map[string]string
				})
				if !ok {
					continue
				}
				for arn, tags := range tagged.TaggedResources(account, region) {
					sv, _, _, res, _ := core.ParseARN(arn)
					if sv == shape[1] && strings.HasPrefix(res, shape[2]) && !slices.Contains(arns, arn) &&
						!slices.ContainsFunc(core.SortedKeys(t.ResourceTags), func(k string) bool { return tags[k] != t.ResourceTags[k] }) {
						arns = append(arns, arn)
					}
				}
			}
		}
		slices.Sort(arns)
		rand.Shuffle(len(arns), func(i, j int) { arns[i], arns[j] = arns[j], arns[i] })
		m := selection.FindStringSubmatch(t.SelectionMode)
		if n, err := strconv.Atoi(m[2]); err == nil {
			arns = arns[:min(n, len(arns))]
		} else if p, err := strconv.Atoi(m[3]); err == nil {
			arns = arns[:int(math.Ceil(float64(len(arns))*float64(p)/100))]
		}
		if len(arns) == 0 {
			return fmt.Errorf("Target resolution for %s found no resources", name)
		}
		slices.Sort(arns)
		e.resolved[name] = arns
	}
	return nil
}

// alarm returns the name of a stop condition alarm in ALARM, if any.
func (s *Service) alarm(e *Experiment, account, region string) string {
	var names []string
	for _, sc := range e.StopConditions {
		if _, _, _, res, ok := core.ParseARN(sc.Value); ok && sc.Source == "aws:cloudwatch:alarm" {
			names = append(names, strings.TrimPrefix(res, "alarm:"))
		}
	}
	if len(names) == 0 {
		return ""
	}
	var out struct{ MetricAlarms, CompositeAlarms []struct{ AlarmName string } }
	if err := s.hub.Call(account, region, "GraniteServiceVersion20100801", "DescribeAlarms",
		map[string]any{"AlarmNames": names, "StateValue": "ALARM", "AlarmTypes": []string{"MetricAlarm", "CompositeAlarm"}}, &out); err != nil {
		return ""
	}
	if all := append(out.MetricAlarms, out.CompositeAlarms...); len(all) > 0 {
		return all[0].AlarmName
	}
	return ""
}

// start runs an action's effect: chaos rules for its duration, or a call
// to the target service.
func (s *Service) start(e *Experiment, a *Action, account, region string, now time.Time) error {
	a.State, a.StartTime = &State{"running", "Action is running."}, ptr(core.Time(now))
	d, _ := duration(a.Parameters["duration"])
	a.until = now.Add(d)
	at := catalog[a.ActionID]
	targets := e.resolved[a.Targets[at.key]]
	rule := core.Rule{Cloud: core.AWS, Region: region, Probability: 1, Message: "Injected by AWS FIS experiment " + e.ID}
	switch a.ActionID {
	case "aws:fis:inject-api-throttle-error", "aws:fis:inject-api-internal-error", "aws:fis:inject-api-unavailable-error":
		pct, _ := strconv.ParseFloat(a.Parameters["percentage"], 64)
		rule.Service, rule.Probability, rule.Status, rule.Code = a.Parameters["service"], pct/100, faults[a.ActionID].status, faults[a.ActionID].code
		principals := []string{""} // without targets, the calls of every caller
		if len(targets) > 0 {
			principals = nil // the sessions of the target roles
			for _, role := range targets {
				_, _, acct, res, _ := core.ParseARN(role)
				principals = append(principals, "arn:*:sts::"+acct+":assumed-role/"+path.Base(res)+"/*")
			}
		}
		for _, op := range strings.Split(a.Parameters["operations"], ",") {
			for _, p := range principals {
				rule.Op, rule.Principal = strings.TrimSpace(op), p
				s.rule(a, rule)
			}
		}
	case "aws:lambda:invocation-error", "aws:lambda:invocation-add-delay":
		pct, err := strconv.ParseFloat(cmp.Or(a.Parameters["invocationPercentage"], "100"), 64)
		if err != nil {
			return err
		}
		rule.Service, rule.Op, rule.Probability = "lambda", "Invoke", pct/100
		if a.ActionID == "aws:lambda:invocation-error" {
			// ponytail: the Invoke call fails, where FIS makes the function return an error
			rule.Status, rule.Code = 500, "ServiceException"
		} else if rule.LatencyMs, err = strconv.Atoi(cmp.Or(a.Parameters["startupDelayMilliseconds"], "1000")); err != nil {
			return err
		}
		for _, fn := range targets {
			_, fnRegion, _, res, _ := core.ParseARN(fn)
			name := strings.Split(res+":", ":")[1]
			rule.Region = fnRegion
			for _, p := range []string{"/2015-03-31/functions/" + name + "/invocations", "/2015-03-31/functions/*:function:" + name + "/invocations"} {
				rule.Path = p
				s.rule(a, rule)
			}
		}
	case "aws:ec2:stop-instances", "aws:ec2:terminate-instances":
		op := map[bool]string{true: "StopInstances", false: "TerminateInstances"}[a.ActionID == "aws:ec2:stop-instances"]
		if err := s.hub.Call(account, region, "AmazonEC2", op, map[string]any{"InstanceIds": ids(targets)}, nil); err != nil {
			return err
		}
		a.until = now
		if after := a.Parameters["startInstancesAfterDuration"]; after != "" && op == "StopInstances" {
			d, _ := duration(after)
			a.until = now.Add(d)
		}
	case "aws:ecs:stop-task":
		for _, task := range targets {
			_, _, _, res, _ := core.ParseARN(task)
			in := map[string]string{"cluster": strings.Split(res+"/", "/")[1], "task": task, "reason": "Stopped by AWS FIS experiment " + e.ID}
			if err := s.hub.Call(account, region, "AmazonEC2ContainerServiceV20141113", "StopTask", in, nil); err != nil {
				return err
			}
		}
		a.until = now
	case "aws:dynamodb:global-table-pause-replication":
		a.State.Reason = "pogi does not replicate global tables, so there is nothing to pause."
	}
	return nil
}

func (s *Service) rule(a *Action, r core.Rule) {
	r.ID = "fis-" + core.RandHex(6)
	a.rules = append(a.rules, s.hub.Chaos.Add(r).ID)
}

func ids(arns []string) []string {
	var out []string
	for _, arn := range arns {
		out = append(out, path.Base(arn))
	}
	return out
}

// finish ends an action whose duration passed: its chaos rules go, and
// stopped instances start again.
func (s *Service) finish(e *Experiment, a *Action, account, region string, now time.Time) {
	s.unrule(a)
	a.State, a.EndTime = &State{"completed", "Action completed."}, ptr(core.Time(now))
	if a.ActionID == "aws:ec2:stop-instances" && a.Parameters["startInstancesAfterDuration"] != "" {
		in := map[string]any{"InstanceIds": ids(e.resolved[a.Targets["Instances"]])}
		if err := s.hub.Call(account, region, "AmazonEC2", "StartInstances", in, nil); err != nil {
			a.State = &State{"failed", err.Error()}
		}
	}
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	templates := core.Table{Name: "Experiment templates", Columns: []string{"ID", "Description", "Actions", "Region"}}
	for _, k := range core.SortedKeys(s.st.Templates) {
		t := s.st.Templates[k]
		_, region, _, _, _ := core.ParseARN(t.Arn)
		templates.Rows = append(templates.Rows, []string{t.ID, t.Description, strings.Join(core.SortedKeys(t.Actions), ", "), region})
	}
	experiments := core.Table{Name: "Experiments", Columns: []string{"ID", "Template", "Status", "Created"}}
	for _, k := range core.SortedKeys(s.st.Experiments) {
		e := s.st.Experiments[k]
		experiments.Rows = append(experiments.Rows, []string{e.ID, e.ExperimentTemplateID, e.State.Status, time.Time(e.CreationTime).Format(time.RFC3339)})
	}
	return []core.Table{templates, experiments}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.st.Templates {
		if t.ID == id {
			return snap(t)
		}
	}
	for _, e := range s.st.Experiments {
		if e.ID == id {
			return snap(e)
		}
	}
	return nil
}
