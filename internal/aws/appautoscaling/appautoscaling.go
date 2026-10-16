// Package appautoscaling emulates Application Auto Scaling over JSON 1.1:
// scalable targets, step and target tracking policies (CloudWatch alarms run
// them, through Auto Scaling, which owns the policy ARN namespace), scheduled
// actions, activities and tags. Scaling sets the desired count of ECS
// services, the capacity units of DynamoDB tables and Lambda provisioned
// concurrency through those services; other targets keep pogi's own count.
package appautoscaling

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/autoscaling"
	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/core"
)

const ecs = "AmazonEC2ContainerServiceV20141113" // ECS's in-process target

// Service is the Application Auto Scaling emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex
	st   state
	stop func()
}

type state struct {
	Targets map[string]*target `json:"targets"` // account/region/namespace/resource/dimension
}

// target is a scalable target with its policies and schedule.
type target struct {
	ScalableTarget
	Account    string             `json:"account"`
	Region     string             `json:"region"`
	Capacity   int                `json:"capacity"` // what pogi set last: the capacity of targets it cannot read
	Policies   map[string]*Policy `json:"policies"`
	Actions    map[string]*action `json:"actions"`
	Activities []Activity         `json:"activities,omitempty"`
	Tags       map[string]string  `json:"tags,omitempty"`
}

// ScalableTarget is a scalable target in its wire shape.
type ScalableTarget struct {
	ServiceNamespace, ResourceId, ScalableDimension string
	MinCapacity, MaxCapacity                        int
	RoleARN                                         string
	CreationTime                                    core.Time
	SuspendedState                                  *Suspended `json:",omitempty"`
	ScalableTargetARN                               string
}

// Suspended says which scaling a target suspends.
type Suspended struct{ DynamicScalingInSuspended, DynamicScalingOutSuspended, ScheduledScalingSuspended bool }

// Policy is a scaling policy in its wire shape.
type Policy struct {
	PolicyARN, PolicyName, ServiceNamespace, ResourceId, ScalableDimension, PolicyType string
	StepScalingPolicyConfiguration                                                     *StepConfig           `json:",omitempty"`
	TargetTrackingScalingPolicyConfiguration                                           *autoscaling.Tracking `json:",omitempty"`
	Alarms                                                                             []autoscaling.Alarm   `json:",omitempty"`
	CreationTime                                                                       core.Time
}

// StepConfig is a step scaling policy's configuration.
type StepConfig struct {
	AdjustmentType         string
	StepAdjustments        []autoscaling.StepAdjustment
	MinAdjustmentMagnitude *int `json:",omitempty"`
	Cooldown               *int `json:",omitempty"`
	MetricAggregationType  string
}

// Action is a scheduled action in its wire shape.
type Action struct {
	ScheduledActionName, ScheduledActionARN, ServiceNamespace, Schedule string
	Timezone                                                            string `json:",omitempty"`
	ResourceId, ScalableDimension                                       string
	StartTime                                                           *core.Time `json:",omitempty"`
	EndTime                                                             *core.Time `json:",omitempty"`
	ScalableTargetAction                                                *struct {
		MinCapacity *int `json:",omitempty"`
		MaxCapacity *int `json:",omitempty"`
	} `json:",omitempty"`
	CreationTime core.Time
}

type action struct {
	Action
	Next time.Time `json:"next"` // zero: never again
}

// Activity is a scaling activity.
type Activity struct {
	ActivityId, ServiceNamespace, ResourceId, ScalableDimension, Description, Cause string
	StartTime, EndTime                                                              core.Time
	StatusCode                                                                      string
	StatusMessage                                                                   string `json:",omitempty"`
}

// New creates the Application Auto Scaling service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"RegisterScalableTarget":    core.H(s.register),
		"DescribeScalableTargets":   core.H(s.describeTargets),
		"DeregisterScalableTarget":  core.H(s.deregister),
		"PutScalingPolicy":          core.H(s.putPolicy),
		"DescribeScalingPolicies":   core.H(s.describePolicies),
		"DeleteScalingPolicy":       core.H(s.deletePolicy),
		"PutScheduledAction":        core.H(s.putAction),
		"DescribeScheduledActions":  core.H(s.describeActions),
		"DeleteScheduledAction":     core.H(s.deleteAction),
		"DescribeScalingActivities": core.H(s.describeActivities),
		"TagResource":               core.H(s.tagResource),
		"UntagResource":             core.H(s.untagResource),
		"ListTagsForResource":       core.H(s.listTags),
	}}
	hub.OnDeliver("application-autoscaling", s.deliver)
	s.stop = core.Every(10*time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "application-autoscaling", Cloud: core.AWS, Title: "Application Auto Scaling", Targets: []string{"AnyScaleFrontendService"},
		Signing: []string{"application-autoscaling"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// Close stops the clock.
func (s *Service) Close() error {
	s.stop()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Targets: map[string]*target{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Targets: map[string]*target{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	for _, t := range st.Targets {
		if t.Policies == nil {
			t.Policies, t.Actions = map[string]*Policy{}, map[string]*action{}
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func validation(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func key(account, region, ns, id, dim string) string {
	return account + "/" + region + "/" + ns + "/" + id + "/" + dim
}

// target finds a scalable target; lock held.
func (s *Service) target(c *core.Ctx, ns, id, dim string) (*target, error) {
	if t := s.st.Targets[key(c.Account, c.Region, ns, id, dim)]; t != nil {
		return t, nil
	}
	return nil, core.Errorf(400, "ObjectNotFoundException",
		"No scalable target registered for service namespace: %s, resource ID: %s, scalable dimension: %s", ns, id, dim)
}

// selects reports whether a describe call's filters keep t.
func (t *target) selects(c *core.Ctx, ns string, ids []string, dim string) bool {
	return t.Account == c.Account && t.Region == c.Region && t.ServiceNamespace == ns && (len(ids) == 0 || slices.Contains(ids, t.ResourceId)) &&
		(dim == "" || dim == t.ScalableDimension)
}

// ref is what scaling a target needs outside the lock.
type ref struct {
	account, region, ns, id, dim string
	min, max, own                int
}

func (t *target) ref() ref {
	return ref{t.Account, t.Region, t.ServiceNamespace, t.ResourceId, t.ScalableDimension, t.MinCapacity, t.MaxCapacity, t.Capacity}
}

// ---- targets ----

func (s *Service) register(c *core.Ctx, in *struct {
	ServiceNamespace, ResourceId, ScalableDimension, RoleARN string
	MinCapacity, MaxCapacity                                 *int
	SuspendedState                                           *Suspended
	Tags                                                     map[string]string
}) (any, error) {
	if in.ResourceId == "" || !strings.HasPrefix(in.ScalableDimension, in.ServiceNamespace+":") {
		return nil, validation("Unsupported scalable dimension %s for service namespace %s", in.ScalableDimension, in.ServiceNamespace)
	}
	s.mu.Lock()
	k := key(c.Account, c.Region, in.ServiceNamespace, in.ResourceId, in.ScalableDimension)
	t := s.st.Targets[k]
	if t == nil && (in.MinCapacity == nil || in.MaxCapacity == nil) {
		s.mu.Unlock()
		return nil, validation("MinCapacity and MaxCapacity are required for registering a new scalable target")
	}
	if t == nil {
		t = &target{ScalableTarget: ScalableTarget{ServiceNamespace: in.ServiceNamespace, ResourceId: in.ResourceId, ScalableDimension: in.ScalableDimension,
			CreationTime: core.Time(time.Now().UTC()), ScalableTargetARN: c.ARN("application-autoscaling", "scalable-target/"+core.RandHex(17)),
			RoleARN: c.GlobalARN("iam", "role/aws-service-role/"+in.ServiceNamespace+".application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling")},
			Account: c.Account, Region: c.Region, Capacity: *in.MinCapacity, Policies: map[string]*Policy{}, Actions: map[string]*action{}, Tags: in.Tags}
	}
	lo, hi := t.MinCapacity, t.MaxCapacity
	if in.MinCapacity != nil {
		lo = *in.MinCapacity
	}
	if in.MaxCapacity != nil {
		hi = *in.MaxCapacity
	}
	if lo > hi {
		s.mu.Unlock()
		return nil, validation("Minimum capacity cannot be greater than maximum capacity")
	}
	t.MinCapacity, t.MaxCapacity, t.RoleARN = lo, hi, cmp.Or(in.RoleARN, t.RoleARN)
	if in.SuspendedState != nil {
		t.SuspendedState = in.SuspendedState
	}
	s.st.Targets[k] = t
	r, arn := t.ref(), t.ScalableTargetARN
	s.mu.Unlock()
	s.clamp(r, "a user request registered the scalable target")
	return map[string]string{"ScalableTargetARN": arn}, nil
}

// clamp moves a target's capacity into its bounds.
func (s *Service) clamp(r ref, cause string) {
	if cur := s.capacity(r); cur < r.min || cur > r.max {
		s.scale(r, min(max(cur, r.min), r.max), cause)
	}
}

func (s *Service) describeTargets(c *core.Ctx, in *struct {
	ServiceNamespace, ScalableDimension string
	ResourceIds                         []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []ScalableTarget{}
	for _, k := range core.SortedKeys(s.st.Targets) {
		if t := s.st.Targets[k]; t.selects(c, in.ServiceNamespace, in.ResourceIds, in.ScalableDimension) {
			out = append(out, t.ScalableTarget)
		}
	}
	return map[string]any{"ScalableTargets": out}, nil
}

func (s *Service) deregister(c *core.Ctx, in *struct{ ServiceNamespace, ResourceId, ScalableDimension string }) (any, error) {
	s.mu.Lock()
	t, err := s.target(c, in.ServiceNamespace, in.ResourceId, in.ScalableDimension)
	var alarms []autoscaling.Alarm
	if err == nil {
		delete(s.st.Targets, key(c.Account, c.Region, in.ServiceNamespace, in.ResourceId, in.ScalableDimension))
		for _, p := range t.Policies {
			alarms = append(alarms, p.Alarms...)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	autoscaling.DeleteAlarms(s.hub, c.Account, c.Region, alarms)
	return nil, nil
}

// ---- capacity of the target's service ----

func part(s, sep string, i int) string {
	p := strings.Split(s, sep)
	if i < len(p) {
		return p[i]
	}
	return ""
}

// capacity reads a target's capacity from its service: ECS services and
// DynamoDB tables; other targets report what pogi set last.
func (s *Service) capacity(r ref) int {
	switch {
	case r.dim == "ecs:service:DesiredCount":
		var out struct{ Services []struct{ DesiredCount int } }
		in := map[string]any{"cluster": part(r.id, "/", 1), "services": []string{part(r.id, "/", 2)}}
		if s.hub.Call(r.account, r.region, ecs, "DescribeServices", in, &out) == nil && len(out.Services) > 0 {
			return out.Services[0].DesiredCount
		}
	case strings.HasPrefix(r.dim, "dynamodb:table:"):
		if pt, err := s.throughput(r); err == nil {
			return pt[strings.TrimPrefix(r.dim, "dynamodb:table:")]
		}
	}
	return r.own
}

func (s *Service) throughput(r ref) (map[string]int, error) {
	var out struct {
		Table struct {
			ProvisionedThroughput struct{ ReadCapacityUnits, WriteCapacityUnits int }
		}
	}
	err := s.hub.Call(r.account, r.region, "DynamoDB_20120810", "DescribeTable", map[string]string{"TableName": part(r.id, "/", 1)}, &out)
	pt := out.Table.ProvisionedThroughput
	return map[string]int{"ReadCapacityUnits": pt.ReadCapacityUnits, "WriteCapacityUnits": pt.WriteCapacityUnits}, err
}

// apply sets a target's capacity in its service.
// ponytail: DynamoDB index dimensions and the other namespaces keep pogi's count only.
func (s *Service) apply(r ref, n int) error {
	switch {
	case r.dim == "ecs:service:DesiredCount":
		return s.hub.Call(r.account, r.region, ecs, "UpdateService",
			map[string]any{"cluster": part(r.id, "/", 1), "service": part(r.id, "/", 2), "desiredCount": n}, nil)
	case strings.HasPrefix(r.dim, "dynamodb:table:"):
		pt, err := s.throughput(r)
		if err != nil {
			return err
		}
		pt[strings.TrimPrefix(r.dim, "dynamodb:table:")] = n
		return s.hub.Call(r.account, r.region, "DynamoDB_20120810", "UpdateTable", map[string]any{"TableName": part(r.id, "/", 1), "ProvisionedThroughput": pt}, nil)
	case r.dim == "lambda:function:ProvisionedConcurrency": // function:<name>:<qualifier>
		body, _ := json.Marshal(map[string]int{"ProvisionedConcurrentExecutions": n})
		path := "/2019-09-30/functions/" + url.PathEscape(part(r.id, ":", 1)) + "/provisioned-concurrency?Qualifier=" + url.QueryEscape(part(r.id, ":", 2))
		if st, _, b := s.hub.Do(r.account, r.region, "lambda", http.MethodPut, path, http.Header{"Content-Type": {"application/json"}}, body); st >= 300 {
			s.hub.Log.Info("application-autoscaling: Lambda took no provisioned concurrency, pogi keeps its own count", "function", r.id, "status", st,
				"body", string(b))
		}
	}
	return nil
}

// scale sets a target's capacity and records the activity.
func (s *Service) scale(r ref, n int, cause string) {
	err := s.apply(r, n)
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.st.Targets[key(r.account, r.region, r.ns, r.id, r.dim)]
	if t == nil {
		return
	}
	now := core.Time(time.Now().UTC())
	a := Activity{ActivityId: core.UUID(), ServiceNamespace: r.ns, ResourceId: r.id, ScalableDimension: r.dim,
		Description: fmt.Sprintf("Setting %s to %d.", r.dim, n), Cause: cause, StartTime: now, EndTime: now, StatusCode: "Successful"}
	if err != nil {
		a.StatusCode, a.StatusMessage = "Failed", core.AsError(err).Msg
	} else {
		t.Capacity = n
	}
	t.Activities = append([]Activity{a}, t.Activities[:min(len(t.Activities), 99)]...)
}

// ---- policies ----

func (s *Service) putPolicy(c *core.Ctx, in *Policy) (any, error) {
	p := *in
	p.PolicyType, p.Alarms = cmp.Or(p.PolicyType, "StepScaling"), nil
	switch {
	case p.PolicyType == "StepScaling" && p.StepScalingPolicyConfiguration == nil:
		return nil, validation("StepScalingPolicyConfiguration is required for StepScaling policies")
	case p.PolicyType == "TargetTrackingScaling" && p.TargetTrackingScalingPolicyConfiguration == nil:
		return nil, validation("TargetTrackingScalingPolicyConfiguration is required for TargetTrackingScaling policies")
	case !slices.Contains([]string{"StepScaling", "TargetTrackingScaling", "PredictiveScaling"}, p.PolicyType):
		return nil, validation("Unsupported policy type %s", p.PolicyType)
	}
	if sc := p.StepScalingPolicyConfiguration; sc != nil {
		cp := *sc
		cp.AdjustmentType, cp.MetricAggregationType = cmp.Or(cp.AdjustmentType, "ChangeInCapacity"), cmp.Or(cp.MetricAggregationType, "Average")
		p.StepScalingPolicyConfiguration = &cp
	}
	s.mu.Lock()
	t, err := s.target(c, p.ServiceNamespace, p.ResourceId, p.ScalableDimension)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var stale []autoscaling.Alarm
	if old := t.Policies[p.PolicyName]; old != nil {
		p.PolicyARN, p.CreationTime, stale = old.PolicyARN, old.CreationTime, old.Alarms
	} else {
		p.PolicyARN = c.ARN("autoscaling", "scalingPolicy:"+core.UUID()+":resource/"+p.ServiceNamespace+"/"+p.ResourceId+":policyName/"+p.PolicyName)
		p.CreationTime = core.Time(time.Now().UTC())
	}
	t.Policies[p.PolicyName] = &p
	s.mu.Unlock()
	autoscaling.DeleteAlarms(s.hub, c.Account, c.Region, stale)
	if tr := p.TargetTrackingScalingPolicyConfiguration; p.PolicyType == "TargetTrackingScaling" {
		alarms := autoscaling.TrackingAlarms(s.hub, c.Account, c.Region, p.ResourceId, p.PolicyARN, metric(tr, p.ServiceNamespace, p.ResourceId),
			tr.TargetValue, !tr.DisableScaleIn)
		s.mu.Lock()
		if cur := t.Policies[p.PolicyName]; cur != nil && cur.PolicyARN == p.PolicyARN {
			cur.Alarms = alarms
		}
		s.mu.Unlock()
		return map[string]any{"PolicyARN": p.PolicyARN, "Alarms": alarms}, nil
	}
	return map[string]any{"PolicyARN": p.PolicyARN}, nil
}

// metric is the CloudWatch metric a target tracking configuration follows.
func metric(t *autoscaling.Tracking, ns, id string) autoscaling.Metric {
	if c := t.CustomizedMetricSpecification; c != nil {
		return autoscaling.Metric{Namespace: c.Namespace, MetricName: c.MetricName, Statistic: cmp.Or(c.Statistic, "Average"), Dimensions: c.Dimensions}
	}
	kind := ""
	if p := t.PredefinedMetricSpecification; p != nil {
		kind = p.PredefinedMetricType
		if kind == "ALBRequestCountPerTarget" {
			return autoscaling.RequestsPerTarget(p.ResourceLabel)
		}
	}
	type dim = autoscaling.Dimension
	switch kind {
	case "ECSServiceAverageCPUUtilization", "ECSServiceAverageMemoryUtilization":
		name := strings.TrimSuffix(strings.TrimPrefix(kind, "ECSServiceAverage"), "Utilization") + "Utilization"
		return autoscaling.Metric{Namespace: "AWS/ECS", MetricName: name, Statistic: "Average",
			Dimensions: []dim{{Name: "ClusterName", Value: part(id, "/", 1)}, {Name: "ServiceName", Value: part(id, "/", 2)}}}
	case "DynamoDBReadCapacityUtilization", "DynamoDBWriteCapacityUtilization":
		name := map[bool]string{true: "ConsumedReadCapacityUnits", false: "ConsumedWriteCapacityUnits"}[strings.Contains(kind, "Read")]
		return autoscaling.Metric{Namespace: "AWS/DynamoDB", MetricName: name, Statistic: "Sum", Dimensions: []dim{{Name: "TableName", Value: part(id, "/", 1)}}}
	case "LambdaProvisionedConcurrencyUtilization":
		fn := part(id, ":", 1)
		return autoscaling.Metric{Namespace: "AWS/Lambda", MetricName: "ProvisionedConcurrencyUtilization", Statistic: "Maximum",
			Dimensions: []dim{{Name: "FunctionName", Value: fn}, {Name: "Resource", Value: fn + ":" + part(id, ":", 2)}}}
	}
	return autoscaling.Metric{Namespace: "AWS/" + ns, MetricName: kind, Statistic: "Average"}
}

func (s *Service) describePolicies(c *core.Ctx, in *struct {
	ServiceNamespace, ResourceId, ScalableDimension string
	PolicyNames                                     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Policy{}
	var ids []string
	if in.ResourceId != "" {
		ids = []string{in.ResourceId}
	}
	for _, k := range core.SortedKeys(s.st.Targets) {
		if t := s.st.Targets[k]; t.selects(c, in.ServiceNamespace, ids, in.ScalableDimension) {
			for _, n := range core.SortedKeys(t.Policies) {
				if len(in.PolicyNames) == 0 || slices.Contains(in.PolicyNames, n) {
					p := *t.Policies[n]
					p.Alarms = slices.Clone(p.Alarms)
					out = append(out, p)
				}
			}
		}
	}
	return map[string]any{"ScalingPolicies": out}, nil
}

func (s *Service) deletePolicy(c *core.Ctx, in *struct{ PolicyName, ServiceNamespace, ResourceId, ScalableDimension string }) (any, error) {
	s.mu.Lock()
	t, err := s.target(c, in.ServiceNamespace, in.ResourceId, in.ScalableDimension)
	var p *Policy
	if err == nil {
		if p = t.Policies[in.PolicyName]; p == nil {
			err = core.Errorf(400, "ObjectNotFoundException", "No scaling policy found for service namespace: %s, resource ID: %s, scalable dimension: %s, policy name: %s",
				in.ServiceNamespace, in.ResourceId, in.ScalableDimension, in.PolicyName)
		} else {
			delete(t.Policies, in.PolicyName)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	autoscaling.DeleteAlarms(s.hub, c.Account, c.Region, p.Alarms)
	return nil, nil
}

// policy finds a policy by ARN; lock held.
func (s *Service) policy(arn string) (*target, *Policy) {
	for _, t := range s.st.Targets {
		for _, p := range t.Policies {
			if p.PolicyARN == arn {
				return t, p
			}
		}
	}
	return nil, nil
}

// deliver runs the policy of an alarm action Auto Scaling handed on: its
// ARN names application-autoscaling where the policy's says autoscaling.
func (s *Service) deliver(arn string, m *core.Message) error {
	arn = strings.Replace(arn, ":application-autoscaling:", ":autoscaling:", 1)
	_, region, account, _, _ := core.ParseARN(arn)
	s.mu.Lock()
	t, p := s.policy(arn)
	var pol Policy
	var r ref
	var sus Suspended
	if p != nil {
		pol, r = *p, t.ref()
		if t.SuspendedState != nil {
			sus = *t.SuspendedState
		}
	}
	s.mu.Unlock()
	if p == nil {
		return fmt.Errorf("no scaling policy %s", arn)
	}
	a := autoscaling.ReadSignal(s.hub, account, region, m)
	if a.High && sus.DynamicScalingOutSuspended || !a.High && sus.DynamicScalingInSuspended {
		return nil
	}
	cur := s.capacity(r)
	want := cur
	switch sc, tr := pol.StepScalingPolicyConfiguration, pol.TargetTrackingScalingPolicyConfiguration; {
	case sc != nil:
		if adj, ok := autoscaling.Step(sc.StepAdjustments, a.Breach()); ok {
			want = autoscaling.Adjust(cur, sc.AdjustmentType, adj, deref(sc.MinAdjustmentMagnitude))
		}
	case tr != nil:
		want = autoscaling.Track(cur, a.Ratio(tr.TargetValue), a.High, tr.DisableScaleIn)
	}
	if want = min(max(want, r.min), r.max); want != cur {
		s.scale(r, want, "monitor alarm "+a.AlarmName+" in state "+a.NewStateValue+" triggered policy "+pol.PolicyName)
	}
	return nil
}

func deref(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// ---- scheduled actions ----

func (s *Service) putAction(c *core.Ctx, in *Action) (any, error) {
	a := &action{Action: *in}
	if _, err := events.ParseSchedule(a.Schedule); err != nil {
		return nil, validation("Invalid schedule expression %s: %v", a.Schedule, err)
	}
	a.Next = a.next(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.target(c, a.ServiceNamespace, a.ResourceId, a.ScalableDimension)
	if err != nil {
		return nil, err
	}
	if old := t.Actions[a.ScheduledActionName]; old != nil {
		a.ScheduledActionARN, a.CreationTime = old.ScheduledActionARN, old.CreationTime
	} else {
		a.ScheduledActionARN = c.ARN("autoscaling", "scheduledAction:"+core.UUID()+":resource/"+a.ServiceNamespace+"/"+a.ResourceId+
			":scheduledActionName/"+a.ScheduledActionName)
		a.CreationTime = core.Time(time.Now().UTC())
	}
	t.Actions[a.ScheduledActionName] = a
	return nil, nil
}

// next is when a scheduled action runs after t, within its start and end.
func (a *action) next(t time.Time) time.Time {
	sched, err := events.ParseSchedule(a.Schedule)
	loc, lerr := time.LoadLocation(cmp.Or(a.Timezone, "UTC"))
	if err != nil || lerr != nil {
		return time.Time{}
	}
	if a.StartTime != nil && time.Time(*a.StartTime).After(t) {
		t = time.Time(*a.StartTime).Add(-time.Second)
	}
	n := sched.Next(t.In(loc))
	if a.EndTime != nil && n.After(time.Time(*a.EndTime)) {
		return time.Time{}
	}
	return n
}

// Tick runs due scheduled actions; it runs every 10 seconds.
func (s *Service) Tick(now time.Time) {
	var refs []ref
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.Targets) {
		t := s.st.Targets[k]
		if t.SuspendedState != nil && t.SuspendedState.ScheduledScalingSuspended {
			continue
		}
		for _, n := range core.SortedKeys(t.Actions) {
			a := t.Actions[n]
			if a.Next.IsZero() || now.Before(a.Next) {
				continue
			}
			if ta := a.ScalableTargetAction; ta != nil && ta.MinCapacity != nil {
				t.MinCapacity = *ta.MinCapacity
			}
			if ta := a.ScalableTargetAction; ta != nil && ta.MaxCapacity != nil {
				t.MaxCapacity = *ta.MaxCapacity
			}
			a.Next = a.next(now)
			refs = append(refs, t.ref())
		}
	}
	s.mu.Unlock()
	for _, r := range refs {
		s.clamp(r, "scheduled action ran")
	}
}

func (s *Service) describeActions(c *core.Ctx, in *struct {
	ServiceNamespace, ResourceId, ScalableDimension string
	ScheduledActionNames                            []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Action{}
	var ids []string
	if in.ResourceId != "" {
		ids = []string{in.ResourceId}
	}
	for _, k := range core.SortedKeys(s.st.Targets) {
		if t := s.st.Targets[k]; t.selects(c, in.ServiceNamespace, ids, in.ScalableDimension) {
			for _, n := range core.SortedKeys(t.Actions) {
				if len(in.ScheduledActionNames) == 0 || slices.Contains(in.ScheduledActionNames, n) {
					out = append(out, t.Actions[n].Action)
				}
			}
		}
	}
	return map[string]any{"ScheduledActions": out}, nil
}

func (s *Service) deleteAction(c *core.Ctx, in *struct{ ServiceNamespace, ScheduledActionName, ResourceId, ScalableDimension string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.target(c, in.ServiceNamespace, in.ResourceId, in.ScalableDimension)
	if err == nil && t.Actions[in.ScheduledActionName] == nil {
		err = core.Errorf(400, "ObjectNotFoundException", "No scheduled action found for service namespace: %s, resource ID: %s, scalable dimension: %s, scheduled action name: %s",
			in.ServiceNamespace, in.ResourceId, in.ScalableDimension, in.ScheduledActionName)
	}
	if err != nil {
		return nil, err
	}
	delete(t.Actions, in.ScheduledActionName)
	return nil, nil
}

func (s *Service) describeActivities(c *core.Ctx, in *struct {
	ServiceNamespace, ResourceId, ScalableDimension string
	MaxResults                                      int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Activity{}
	var ids []string
	if in.ResourceId != "" {
		ids = []string{in.ResourceId}
	}
	for _, k := range core.SortedKeys(s.st.Targets) {
		if t := s.st.Targets[k]; t.selects(c, in.ServiceNamespace, ids, in.ScalableDimension) {
			out = append(out, t.Activities...)
		}
	}
	return map[string]any{"ScalingActivities": out[:min(len(out), cmp.Or(in.MaxResults, 50))]}, nil
}

// ---- tags ----

// byARN finds a target by its ARN; lock held.
func (s *Service) byARN(arn string) (*target, error) {
	for _, t := range s.st.Targets {
		if t.ScalableTargetARN == arn {
			return t, nil
		}
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "No scalable target found for ARN %s", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceARN string
	Tags        map[string]string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.byARN(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	if t.Tags == nil {
		t.Tags = map[string]string{}
	}
	for k, v := range in.Tags {
		t.Tags[k] = v
	}
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceARN string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.byARN(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(t.Tags, k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceARN string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.byARN(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	tags := map[string]string{}
	for k, v := range t.Tags {
		tags[k] = v
	}
	return map[string]any{"Tags": tags}, nil
}

// ---- console ----

// Resources lists scalable targets.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	tb := core.Table{Name: "Scalable targets", Columns: []string{"Resource", "Dimension", "Min", "Max", "Policies", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Targets) {
		t := s.st.Targets[k]
		tb.Rows = append(tb.Rows, []string{t.ResourceId, t.ScalableDimension, strconv.Itoa(t.MinCapacity), strconv.Itoa(t.MaxCapacity),
			strconv.Itoa(len(t.Policies)), t.Region, t.Account})
	}
	return []core.Table{tb}
}

// Inspect returns a target with its policies and actions.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.st.Targets {
		if t.ResourceId == id {
			b, _ := json.Marshal(t)
			var v any
			json.Unmarshal(b, &v)
			return v
		}
	}
	return nil
}
