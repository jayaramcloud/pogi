// Package scheduler emulates Amazon EventBridge Scheduler (REST-JSON):
// schedule groups and one-time, rate and cron schedules whose targets are
// reached through the hub.
package scheduler

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the EventBridge Scheduler emulator.
type Service struct {
	stop func()
	hub  *core.Hub
	mux  *http.ServeMux
	mu   sync.Mutex
	st   state
}

type state struct {
	Groups    map[string]*Group            `json:"groups"`    // account/region/name
	Schedules map[string]*entry            `json:"schedules"` // account/region/group/name
	Tags      map[string]map[string]string `json:"tags"`      // group ARN → tags
}

// Group is a schedule group; "default" always exists.
type Group struct {
	Name, Arn, State                   string
	CreationDate, LastModificationDate float64
}

// Schedule is a schedule in the GetSchedule shape.
type Schedule struct {
	Arn                        string
	Name                       string
	GroupName                  string
	ScheduleExpression         string
	ScheduleExpressionTimezone string   `json:",omitempty"`
	StartDate                  *float64 `json:",omitempty"`
	EndDate                    *float64 `json:",omitempty"`
	State                      string
	Description                string `json:",omitempty"`
	KmsKeyArn                  string `json:",omitempty"`
	ActionAfterCompletion      string
	FlexibleTimeWindow         *struct {
		Mode                   string
		MaximumWindowInMinutes int `json:",omitempty"`
	}
	Target                             *Target
	CreationDate, LastModificationDate float64
}

// Target is where a schedule delivers; parameters pogi does not act on are
// stored verbatim.
type Target struct {
	Arn                   string
	RoleArn               string
	Input                 string                               `json:",omitempty"`
	SqsParameters         *struct{ MessageGroupId string }     `json:",omitempty"`
	KinesisParameters     *struct{ PartitionKey string }       `json:",omitempty"`
	EventBridgeParameters *struct{ DetailType, Source string } `json:",omitempty"`
	DeadLetterConfig      *struct{ Arn string }                `json:",omitempty"`

	RetryPolicy, EcsParameters, SageMakerPipelineParameters json.RawMessage `json:",omitempty"`
}

// entry is a stored schedule and its next run.
type entry struct {
	Schedule
	Next time.Time `json:"next,omitzero"`
}

// New creates the Scheduler service and starts its clock.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	for pattern, op := range map[string]struct {
		name string
		fn   func(*core.Ctx, *http.Request) (any, error)
	}{
		"POST /schedules/{Name}":         {"CreateSchedule", s.createSchedule},
		"GET /schedules/{Name}":          {"GetSchedule", s.getSchedule},
		"PUT /schedules/{Name}":          {"UpdateSchedule", s.updateSchedule},
		"DELETE /schedules/{Name}":       {"DeleteSchedule", s.deleteSchedule},
		"GET /schedules":                 {"ListSchedules", s.listSchedules},
		"POST /schedule-groups/{Name}":   {"CreateScheduleGroup", s.createGroup},
		"GET /schedule-groups/{Name}":    {"GetScheduleGroup", s.getGroup},
		"DELETE /schedule-groups/{Name}": {"DeleteScheduleGroup", s.deleteGroup},
		"GET /schedule-groups":           {"ListScheduleGroups", s.listGroups},
		"POST /tags/{ResourceArn}":       {"TagResource", s.tagResource},
		"GET /tags/{ResourceArn}":        {"ListTagsForResource", s.listTags},
		"DELETE /tags/{ResourceArn}":     {"UntagResource", s.untagResource},
		"/":                              {"Unknown", s.unknown},
	} {
		s.mux.HandleFunc(pattern, s.handle(op.name, op.fn))
	}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

// Close stops the schedule clock.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Info() core.Info {
	return core.Info{Name: "scheduler", Cloud: core.AWS, Title: "EventBridge Scheduler", Signing: []string{"scheduler"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

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

func (s *Service) unknown(c *core.Ctx, r *http.Request) (any, error) {
	return nil, core.Errorf(404, "UnknownOperationException", "pogi does not implement this Scheduler request (%s %s)", r.Method, r.URL.Path)
}

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
	if st.Groups == nil {
		st.Groups = map[string]*Group{}
	}
	if st.Schedules == nil {
		st.Schedules = map[string]*entry{}
	}
	if st.Tags == nil {
		st.Tags = map[string]map[string]string{}
	}
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func notFound(format string, a ...any) error {
	return core.Errorf(404, "ResourceNotFoundException", format, a...)
}

func conflict(format string, a ...any) error {
	return core.Errorf(409, "ConflictException", format, a...)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, v) != nil {
		return invalid("The request body is not valid JSON.")
	}
	return nil
}

var nameRe = regexp.MustCompile(`^[0-9a-zA-Z\-_.]{1,64}$`)

// roleRe is the shape of Target.RoleArn: an IAM role ARN.
var roleRe = regexp.MustCompile(`^arn:aws(-[a-z]+)?:iam::\d{12}:role/[\w+=,.@/-]+$`)

// templated says whether a target ARN is one of the templated targets (Lambda,
// Step Functions, EventBridge), whose Input must be well-formed JSON.
func templated(arn string) bool {
	svc, _, _, _, _ := core.ParseARN(arn)
	return svc == "lambda" || svc == "states" || svc == "events"
}

// ---- schedules ----

// input decodes and validates a CreateSchedule or UpdateSchedule request.
func input(r *http.Request) (*Schedule, error) {
	in := &Schedule{}
	if err := decode(r, in); err != nil {
		return nil, err
	}
	in.Name = r.PathValue("Name")
	in.GroupName = cmp.Or(in.GroupName, "default")
	in.State = cmp.Or(in.State, "ENABLED")
	in.ActionAfterCompletion = cmp.Or(in.ActionAfterCompletion, "NONE")
	_, exprErr := events.ParseSchedule(in.ScheduleExpression)
	_, tzErr := time.LoadLocation(in.ScheduleExpressionTimezone)
	switch {
	case !nameRe.MatchString(in.Name):
		return nil, invalid("Schedule name %s is not valid.", in.Name)
	case exprErr != nil:
		return nil, invalid("Invalid Schedule Expression %s.", in.ScheduleExpression)
	case tzErr != nil:
		return nil, invalid("Invalid ScheduleExpressionTimezone %s.", in.ScheduleExpressionTimezone)
	case in.State != "ENABLED" && in.State != "DISABLED":
		return nil, invalid("State %s is not valid.", in.State)
	case in.ActionAfterCompletion != "NONE" && in.ActionAfterCompletion != "DELETE":
		return nil, invalid("ActionAfterCompletion %s is not valid.", in.ActionAfterCompletion)
	case in.FlexibleTimeWindow == nil || in.FlexibleTimeWindow.Mode != "OFF" && in.FlexibleTimeWindow.Mode != "FLEXIBLE":
		return nil, invalid("FlexibleTimeWindow with Mode OFF or FLEXIBLE is required.")
	case in.Target == nil || in.Target.Arn == "" || in.Target.RoleArn == "":
		return nil, invalid("Target with Arn and RoleArn is required.")
	case !roleRe.MatchString(in.Target.RoleArn):
		return nil, invalid("Target RoleArn %s is not a valid IAM role ARN.", in.Target.RoleArn)
	case in.Target.Input != "" && templated(in.Target.Arn) && !json.Valid([]byte(substitute(in.Target.Input, "arn", time.Time{}))):
		return nil, invalid("The Input of a templated target must be well-formed JSON.")
	case in.StartDate != nil && in.EndDate != nil && *in.EndDate <= *in.StartDate:
		return nil, invalid("The EndDate must be after the StartDate.")
	}
	return in, nil
}

func (s *Service) schedule(c *core.Ctx, group, name string) (*entry, error) {
	if e := s.st.Schedules[key(c.Account, c.Region, cmp.Or(group, "default")+"/"+name)]; e != nil {
		return e, nil
	}
	return nil, notFound("Schedule %s does not exist.", name)
}

func (s *Service) createSchedule(c *core.Ctx, r *http.Request) (any, error) {
	in, err := input(r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.findGroup(c.Account, c.Region, in.GroupName); err != nil {
		return nil, err
	}
	k := key(c.Account, c.Region, in.GroupName+"/"+in.Name)
	if s.st.Schedules[k] != nil {
		return nil, conflict("Schedule %s already exists.", in.Name)
	}
	now := time.Now()
	in.Arn = c.ARN("scheduler", "schedule/"+in.GroupName+"/"+in.Name)
	in.CreationDate, in.LastModificationDate = core.Unix(now), core.Unix(now)
	s.st.Schedules[k] = &entry{*in, next(in, now)}
	return map[string]string{"ScheduleArn": in.Arn}, nil
}

// updateSchedule replaces every setting, as UpdateSchedule does in AWS.
func (s *Service) updateSchedule(c *core.Ctx, r *http.Request) (any, error) {
	in, err := input(r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.schedule(c, in.GroupName, in.Name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	in.Arn, in.CreationDate, in.LastModificationDate = e.Arn, e.CreationDate, core.Unix(now)
	e.Schedule, e.Next = *in, next(in, now)
	return map[string]string{"ScheduleArn": in.Arn}, nil
}

func (s *Service) getSchedule(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.schedule(c, r.URL.Query().Get("groupName"), r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	return e.Schedule, nil
}

func (s *Service) deleteSchedule(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.schedule(c, r.URL.Query().Get("groupName"), r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Schedules, key(c.Account, c.Region, e.GroupName+"/"+e.Name))
	return nil, nil
}

func (s *Service) listSchedules(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	group, prefix, state := cmp.Or(q.Get("ScheduleGroup"), q.Get("GroupName")), q.Get("NamePrefix"), q.Get("State")
	type summary struct {
		Arn, Name, GroupName, State        string
		CreationDate, LastModificationDate float64
		Target                             struct{ Arn string }
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []summary
	for k, e := range s.st.Schedules {
		if strings.HasPrefix(k, key(c.Account, c.Region, "")) && (group == "" || e.GroupName == group) &&
			strings.HasPrefix(e.Name, prefix) && (state == "" || e.State == state) {
			sum := summary{Arn: e.Arn, Name: e.Name, GroupName: e.GroupName, State: e.State, CreationDate: e.CreationDate, LastModificationDate: e.LastModificationDate}
			sum.Target.Arn = e.Target.Arn
			out = append(out, sum)
		}
	}
	limit, _ := strconv.Atoi(q.Get("MaxResults"))
	items, next := page(out, func(x summary) string { return x.GroupName + "/" + x.Name }, q.Get("NextToken"), limit)
	return list("Schedules", items, next), nil
}

// ---- groups ----

// group finds a group, creating the default one on first use; lock held.
func (s *Service) group(account, region, name string) *Group {
	k := key(account, region, name)
	g := s.st.Groups[k]
	if g == nil && name == "default" {
		g = newGroup(account, region, name)
		s.st.Groups[k] = g
	}
	return g
}

func newGroup(account, region, name string) *Group {
	now := core.Unix(time.Now())
	return &Group{Name: name, Arn: core.ARN("scheduler", region, account, "schedule-group/"+name), State: "ACTIVE", CreationDate: now, LastModificationDate: now}
}

func (s *Service) findGroup(account, region, name string) (*Group, error) {
	if g := s.group(account, region, name); g != nil {
		return g, nil
	}
	return nil, notFound("Schedule group %s does not exist.", name)
}

type tag struct{ Key, Value string }

func (s *Service) createGroup(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Tags []tag }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	name := r.PathValue("Name")
	if !nameRe.MatchString(name) {
		return nil, invalid("Schedule group name %s is not valid.", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.group(c.Account, c.Region, name) != nil {
		return nil, conflict("Schedule group %s already exists.", name)
	}
	g := newGroup(c.Account, c.Region, name)
	s.st.Groups[key(c.Account, c.Region, name)] = g
	s.addTags(g.Arn, in.Tags)
	return map[string]string{"ScheduleGroupArn": g.Arn}, nil
}

func (s *Service) getGroup(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.findGroup(c.Account, c.Region, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	return *g, nil
}

// deleteGroup deletes a group and its schedules.
func (s *Service) deleteGroup(c *core.Ctx, r *http.Request) (any, error) {
	name := r.PathValue("Name")
	if name == "default" {
		return nil, invalid("The default schedule group cannot be deleted.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.findGroup(c.Account, c.Region, name)
	if err != nil {
		return nil, err
	}
	for k := range s.st.Schedules {
		if strings.HasPrefix(k, key(c.Account, c.Region, name+"/")) {
			delete(s.st.Schedules, k)
		}
	}
	delete(s.st.Groups, key(c.Account, c.Region, name))
	delete(s.st.Tags, g.Arn)
	return nil, nil
}

func (s *Service) listGroups(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.group(c.Account, c.Region, "default")
	var out []Group
	for k, g := range s.st.Groups {
		if strings.HasPrefix(k, key(c.Account, c.Region, q.Get("NamePrefix"))) {
			out = append(out, *g)
		}
	}
	limit, _ := strconv.Atoi(q.Get("MaxResults"))
	items, next := page(out, func(g Group) string { return g.Name }, q.Get("NextToken"), limit)
	return list("ScheduleGroups", items, next), nil
}

// ---- tags (schedule groups carry them) ----

func (s *Service) tagged(arn string) (*Group, error) {
	_, region, account, res, _ := core.ParseARN(arn)
	if name, ok := strings.CutPrefix(res, "schedule-group/"); ok {
		if g := s.group(account, region, name); g != nil {
			return g, nil
		}
	}
	return nil, notFound("Resource %s does not exist.", arn)
}

func (s *Service) addTags(arn string, tags []tag) {
	if len(tags) > 0 && s.st.Tags[arn] == nil {
		s.st.Tags[arn] = map[string]string{}
	}
	for _, t := range tags {
		s.st.Tags[arn][t.Key] = t.Value
	}
}

func (s *Service) tagResource(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Tags []tag }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.tagged(r.PathValue("ResourceArn"))
	if err != nil {
		return nil, err
	}
	s.addTags(g.Arn, in.Tags)
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.tagged(r.PathValue("ResourceArn"))
	if err != nil {
		return nil, err
	}
	out, tags := []tag{}, s.st.Tags[g.Arn]
	for _, k := range core.SortedKeys(tags) {
		out = append(out, tag{k, tags[k]})
	}
	return map[string]any{"Tags": out}, nil
}

func (s *Service) untagResource(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.tagged(r.PathValue("ResourceArn"))
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["TagKeys"] {
		delete(s.st.Tags[g.Arn], k)
	}
	return nil, nil
}

// ---- firing ----

// Tick runs the schedules due at now. A goroutine calls it every second;
// tests call it with a later time instead of sleeping.
func (s *Service) Tick(now time.Time) {
	type run struct {
		arn    string
		target Target
		at     time.Time
	}
	var runs []run
	s.mu.Lock()
	for k, e := range s.st.Schedules {
		if e.State != "ENABLED" || e.Next.IsZero() || e.Next.After(now) {
			continue
		}
		runs = append(runs, run{e.Arn, *e.Target, e.Next})
		if e.Next = next(&e.Schedule, e.Next); !e.Next.IsZero() && !e.Next.After(now) {
			e.Next = next(&e.Schedule, now) // skip runs missed while pogi was busy or stopped
		}
		if e.Next.IsZero() && e.ActionAfterCompletion == "DELETE" {
			delete(s.st.Schedules, k)
		}
	}
	s.mu.Unlock()
	for _, r := range runs {
		s.fire(r.arn, r.target, r.at)
	}
}

// next is the first run after t in the schedule's time zone and between its
// start and end dates, or zero when there is none.
func next(sc *Schedule, t time.Time) time.Time {
	expr, err := events.ParseSchedule(sc.ScheduleExpression)
	loc, tzErr := time.LoadLocation(sc.ScheduleExpressionTimezone)
	if err != nil || tzErr != nil {
		return time.Time{}
	}
	if sc.StartDate != nil && t.Before(unix(*sc.StartDate)) {
		t = unix(*sc.StartDate).Add(-time.Nanosecond)
	}
	n := expr.Next(t.In(loc))
	if sc.EndDate != nil && n.After(unix(*sc.EndDate)) {
		return time.Time{}
	}
	return n
}

// substitute fills in the context attributes AWS replaces in a target's input.
func substitute(input, arn string, at time.Time) string {
	return strings.NewReplacer(
		"<aws.scheduler.schedule-arn>", arn,
		"<aws.scheduler.scheduled-time>", at.UTC().Format(time.RFC3339),
		"<aws.scheduler.execution-id>", core.UUID(),
		"<aws.scheduler.attempt-number>", "1",
	).Replace(input)
}

// fire delivers one run; a failed delivery goes to the dead-letter queue when there is one.
func (s *Service) fire(arn string, t Target, at time.Time) {
	body := substitute(cmp.Or(t.Input, "{}"), arn, at)
	m := &core.Message{Body: body}
	switch {
	case t.SqsParameters != nil:
		m.GroupID = t.SqsParameters.MessageGroupId
	case t.KinesisParameters != nil: // core.Message has no partition key; GroupID carries it
		m.GroupID = t.KinesisParameters.PartitionKey
	case t.EventBridgeParameters != nil:
		m.Source, m.DetailType = t.EventBridgeParameters.Source, t.EventBridgeParameters.DetailType
	}
	var err error
	if _, sdk, ok := strings.Cut(t.Arn, ":scheduler:::aws-sdk:"); ok { // any partition
		err = s.callSDK(arn, sdk, body)
	} else {
		err = s.hub.Deliver(t.Arn, m)
	}
	if err != nil && t.DeadLetterConfig != nil {
		err = s.hub.Deliver(t.DeadLetterConfig.Arn, &core.Message{Body: body, Attributes: map[string]core.MessageAttribute{
			"ERROR_MESSAGE": {DataType: "String", StringValue: err.Error()},
			"TARGET_ARN":    {DataType: "String", StringValue: t.Arn},
		}})
	}
	if err != nil {
		s.hub.Log.Warn("scheduler: delivery failed", "schedule", arn, "target", t.Arn, "err", err)
	}
}

// sdkServices maps the names universal targets use to pogi's service names.
var sdkServices = map[string]string{"eventbridge": "events", "sfn": "stepfunctions"}

// callSDK runs a universal target, "<service>:<action>" such as "sns:publish". The input is the JSON
// request of that API call. It works for every enabled service that accepts X-Amz-Target requests.
// The schedule's own account and region are used.
func (s *Service) callSDK(scheduleArn, target, input string) error {
	svcName, action, _ := strings.Cut(target, ":")
	svcName = cmp.Or(sdkServices[svcName], svcName)
	svc := s.hub.Service(svcName)
	if svc == nil || len(svc.Info().Targets) == 0 || action == "" {
		return fmt.Errorf("universal target aws-sdk:%s is not supported", target)
	}
	var in map[string]any
	if err := json.Unmarshal([]byte(input), &in); err != nil {
		return fmt.Errorf("the input of a universal target must be a JSON object: %w", err)
	}
	_, region, account, _, _ := core.ParseARN(scheduleArn)
	return s.hub.Call(account, region, svc.Info().Targets[0], strings.ToUpper(action[:1])+action[1:], in, nil)
}

// ---- console ----

// Resources lists schedules for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Schedules", Columns: []string{"Name", "Group", "Expression", "State", "Target", "Next run"}}
	for _, k := range core.SortedKeys(s.st.Schedules) {
		e, run := s.st.Schedules[k], ""
		if !e.Next.IsZero() {
			run = e.Next.UTC().Format(time.RFC3339)
		}
		t.Rows = append(t.Rows, []string{e.Name, e.GroupName, e.ScheduleExpression, e.State, e.Target.Arn, run})
	}
	return []core.Table{t}
}

// Inspect returns a schedule and its next run.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Schedules) {
		if e := s.st.Schedules[k]; table == "Schedules" && e.Name == id {
			b, _ := json.Marshal(e)
			return json.RawMessage(b)
		}
	}
	return nil
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

func unix(sec float64) time.Time { return time.UnixMilli(int64(math.Round(sec * 1000))) }
