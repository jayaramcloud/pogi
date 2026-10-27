package ecs

import (
	"cmp"
	"encoding/json"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// maxTaskSets is how many task sets a service may hold, as in AWS.
const maxTaskSets = 5

// TaskSet is a set of tasks of one task definition inside a service. pogi
// has them for services that use the CODE_DEPLOY deployment controller: the
// service starts with a PRIMARY one, and CodeDeploy adds a replacement set
// and moves the load balancer's traffic to it.
type TaskSet struct {
	Id                       string          `json:"id"`
	TaskSetArn               string          `json:"taskSetArn"`
	ServiceArn               string          `json:"serviceArn"`
	ClusterArn               string          `json:"clusterArn"`
	ExternalId               string          `json:"externalId,omitempty"`
	Status                   string          `json:"status"` // PRIMARY | ACTIVE | DRAINING
	TaskDefinition           string          `json:"taskDefinition"`
	ComputedDesiredCount     int             `json:"computedDesiredCount"`
	PendingCount             int             `json:"pendingCount"`
	RunningCount             int             `json:"runningCount"`
	CreatedAt                core.Time       `json:"createdAt"`
	UpdatedAt                core.Time       `json:"updatedAt"`
	LaunchType               string          `json:"launchType,omitempty"`
	CapacityProviderStrategy []Strategy      `json:"capacityProviderStrategy,omitempty"`
	PlatformVersion          string          `json:"platformVersion,omitempty"`
	PlatformFamily           string          `json:"platformFamily,omitempty"`
	NetworkConfiguration     *NetConf        `json:"networkConfiguration,omitempty"`
	LoadBalancers            []LB            `json:"loadBalancers"`
	ServiceRegistries        json.RawMessage `json:"serviceRegistries,omitempty"`
	Scale                    Scale           `json:"scale"`
	StabilityStatus          string          `json:"stabilityStatus"` // STEADY_STATE | STABILIZING
	StabilityStatusAt        core.Time       `json:"stabilityStatusAt"`
	Tags                     []Tag           `json:"tags,omitempty"`

	failed   int       // tasks that failed to start, for backing off
	failedAt time.Time // the last one
}

// Scale is the share of the service's desired count a task set runs.
type Scale struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

// codeDeploy says whether the service is deployed by CodeDeploy.
func codeDeploy(sv *Svc) bool {
	var dc struct{ Type string }
	json.Unmarshal(sv.DeploymentController, &dc)
	return dc.Type == "CODE_DEPLOY"
}

func (sv *Svc) taskSet(id string) *TaskSet {
	for _, ts := range sv.TaskSets {
		if ts.Id == id {
			return ts
		}
	}
	return nil
}

// addSet adds ts to the service as ACTIVE (PRIMARY when it is the first); lock held.
func addSet(sv *Svc, ts *TaskSet, now time.Time) {
	account, region := where(sv.ServiceArn)
	ts.Id = "ecs-svc/" + core.RandString(19, "0123456789")
	ts.TaskSetArn = core.ARN("ecs", region, account, "task-set/"+last(sv.ClusterArn)+"/"+sv.ServiceName+"/"+ts.Id)
	ts.ServiceArn, ts.ClusterArn = sv.ServiceArn, sv.ClusterArn
	ts.CreatedAt, ts.UpdatedAt, ts.StabilityStatus, ts.StabilityStatusAt = core.Time(now), core.Time(now), "STABILIZING", core.Time(now)
	ts.Status = cmp.Or(ts.Status, "ACTIVE")
	ts.LoadBalancers = orEmpty(ts.LoadBalancers)
	sv.TaskSets = append(sv.TaskSets, ts)
}

// setOf finds a task set of the service by ID or ARN; lock held.
func (s *Service) setOf(c *core.Ctx, cluster, svc, ref string) (*Svc, *TaskSet, error) {
	if svc == "" {
		return nil, nil, invalid("Service cannot be empty.")
	}
	sv, err := s.service(c, cluster, svc)
	if err != nil {
		return nil, nil, err
	}
	if !codeDeploy(sv) {
		return nil, nil, invalid("pogi supports task sets only for services with the CODE_DEPLOY deployment controller.")
	}
	for _, ts := range sv.TaskSets {
		if ts.Id == ref || ts.TaskSetArn == ref {
			return sv, ts, nil
		}
	}
	return sv, nil, core.Errorf(400, "TaskSetNotFoundException", "The specified task set was not found. You can view your available task sets with DescribeTaskSets.")
}

func checkScale(sc *Scale) error {
	if sc.Unit = cmp.Or(sc.Unit, "PERCENT"); sc.Unit != "PERCENT" || sc.Value < 0 || sc.Value > 100 {
		return invalid("scale must be a PERCENT value from 0 to 100.")
	}
	return nil
}

func (s *Service) createTaskSet(c *core.Ctx, in *struct {
	Cluster, Service, TaskDefinition, ExternalId, LaunchType, PlatformVersion string
	NetworkConfiguration                                                      *NetConf
	LoadBalancers                                                             []LB
	ServiceRegistries                                                         json.RawMessage
	CapacityProviderStrategy                                                  []Strategy
	Scale                                                                     *Scale
	Tags                                                                      []Tag
}) (any, error) {
	if in.TaskDefinition == "" {
		return nil, invalid("taskDefinition cannot be empty.")
	}
	sc := Scale{Value: 100}
	if in.Scale != nil {
		sc = *in.Scale
	}
	if err := checkScale(&sc); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, _, err := s.setOf(c, in.Cluster, in.Service, "")
	if err != nil && sv == nil {
		return nil, err
	}
	if sv.Status != "ACTIVE" {
		return nil, errActive
	}
	if len(sv.TaskSets) >= maxTaskSets {
		return nil, core.Errorf(400, "LimitExceededException", "A service can have at most %d task sets.", maxTaskSets)
	}
	d := s.taskDef(c, in.TaskDefinition)
	launch, strat := in.LaunchType, in.CapacityProviderStrategy
	if launch == "" && len(strat) == 0 { // as the service does
		launch, strat = sv.LaunchType, sv.CapacityProviderStrategy
	}
	net := network(cmp.Or(in.NetworkConfiguration, sv.NetworkConfiguration))
	if err := runnable(d, launch, net, invalid); err != nil {
		return nil, err
	}
	if err := checkLBs(d, in.LoadBalancers); err != nil {
		return nil, err
	}
	ts := &TaskSet{TaskDefinition: d.Arn, ExternalId: in.ExternalId, LaunchType: launch, CapacityProviderStrategy: strat, NetworkConfiguration: net,
		PlatformVersion: cmp.Or(in.PlatformVersion, sv.PlatformVersion), LoadBalancers: in.LoadBalancers, ServiceRegistries: in.ServiceRegistries, Scale: sc, Tags: in.Tags}
	if launch == "FARGATE" {
		ts.PlatformVersion, ts.PlatformFamily = cmp.Or(ts.PlatformVersion, "LATEST"), "Linux"
	}
	now := time.Now()
	addSet(sv, ts, now)
	s.reconcile(sv, now)
	return snap(map[string]any{"taskSet": ts}), nil
}

func (s *Service) describeTaskSets(c *core.Ctx, in *struct {
	Cluster, Service string
	TaskSets         []string
	Include          []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, _, err := s.setOf(c, in.Cluster, in.Service, "")
	if err != nil && sv == nil {
		return nil, err
	}
	out := struct {
		TaskSets []TaskSet `json:"taskSets"`
		Failures []failure `json:"failures"`
	}{[]TaskSet{}, []failure{}}
	add := func(ts *TaskSet) {
		v := *ts
		if !slices.Contains(in.Include, "TAGS") {
			v.Tags = nil
		}
		out.TaskSets = append(out.TaskSets, v)
	}
	if len(in.TaskSets) == 0 {
		for _, ts := range sv.TaskSets {
			add(ts)
		}
	}
	for _, ref := range in.TaskSets {
		if _, ts, err := s.setOf(c, in.Cluster, in.Service, ref); err == nil {
			add(ts)
		} else {
			out.Failures = append(out.Failures, failure{Arn: ref, Reason: "MISSING"})
		}
	}
	return snap(out), nil
}

func (s *Service) updateTaskSet(c *core.Ctx, in *struct {
	Cluster, Service, TaskSet string
	Scale                     *Scale
}) (any, error) {
	if in.Scale == nil {
		return nil, invalid("scale cannot be empty.")
	}
	if err := checkScale(in.Scale); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, ts, err := s.setOf(c, in.Cluster, in.Service, in.TaskSet)
	if err != nil {
		return nil, err
	}
	if ts.Status == "DRAINING" {
		return nil, invalid("The task set is draining.")
	}
	now := time.Now()
	ts.Scale, ts.UpdatedAt = *in.Scale, core.Time(now)
	s.reconcile(sv, now)
	return snap(map[string]any{"taskSet": ts}), nil
}

func (s *Service) updateServicePrimaryTaskSet(c *core.Ctx, in *struct{ Cluster, Service, PrimaryTaskSet string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, ts, err := s.setOf(c, in.Cluster, in.Service, in.PrimaryTaskSet)
	if err != nil {
		return nil, err
	}
	if ts.Status == "DRAINING" {
		return nil, invalid("A draining task set cannot be the primary task set.")
	}
	now := time.Now()
	for _, o := range sv.TaskSets {
		if o.Status == "PRIMARY" {
			o.Status, o.UpdatedAt = "ACTIVE", core.Time(now)
		}
	}
	ts.Status, ts.UpdatedAt = "PRIMARY", core.Time(now)
	sv.TaskDefinition, sv.LoadBalancers = ts.TaskDefinition, ts.LoadBalancers
	s.reconcile(sv, now)
	return snap(map[string]any{"taskSet": ts}), nil
}

func (s *Service) deleteTaskSet(c *core.Ctx, in *struct {
	Cluster, Service, TaskSet string
	Force                     bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, ts, err := s.setOf(c, in.Cluster, in.Service, in.TaskSet)
	if err != nil {
		return nil, err
	}
	if ts.Status != "DRAINING" && ts.Scale.Value > 0 && !in.Force {
		return nil, invalid("The task set is not scaled down to 0: scale it down or use force.")
	}
	now := time.Now()
	ts.Status, ts.ComputedDesiredCount, ts.UpdatedAt = "DRAINING", 0, core.Time(now)
	out := snap(map[string]any{"taskSet": ts}) // DRAINING, even when no task is left to stop
	s.reconcile(sv, now)
	return out, nil
}

// reconcileSets keeps the tasks of each task set at its computed count and
// drops drained sets; lock held. Like reconcile, tasks that keep failing to
// start are retried with a growing delay.
func (s *Service) reconcileSets(sv *Svc, now time.Time) {
	account, region := where(sv.ServiceArn)
	cl := s.st.Clusters[key(account, region, last(sv.ClusterArn))]
	mine := s.tasksOf(sv)
	var started, stopped []string
	stop := func(t *Task, set string) {
		if t.DesiredStatus != "STOPPED" {
			s.stopTask(t, "ServiceSchedulerInitiated", "Scaling activity initiated by (task-set "+set+")")
			stopped = append(stopped, "(task "+last(t.TaskArn)+")")
		}
	}
	for _, ts := range sv.TaskSets {
		var live []*Task
		for _, t := range mine {
			if t.StartedBy == ts.Id && t.DesiredStatus == "RUNNING" {
				live = append(live, t)
			}
		}
		want := 0
		if sv.Status == "ACTIVE" && ts.Status != "DRAINING" {
			// a stored count may be bad; it must never panic the loop
			want = max(int(math.Ceil(float64(max(sv.DesiredCount, 0))*ts.Scale.Value/100)), 0)
			ts.ComputedDesiredCount = want
		}
		for len(live) > want { // scale in, newest first
			stop(live[len(live)-1], ts.Id)
			live = live[:len(live)-1]
		}
		backoff := time.Duration(min(1<<min(ts.failed, 6), 60)) * time.Second
		if n := want - len(live); n > 0 && !(ts.failed > 0 && now.Sub(ts.failedAt) < backoff) {
			if d := s.st.Defs[key(account, region, last(ts.TaskDefinition))]; d != nil && cl != nil {
				o := &taskOpts{launchType: ts.LaunchType, platformVersion: ts.PlatformVersion, startedBy: ts.Id, group: "service:" + sv.ServiceName,
					strategy: ts.CapacityProviderStrategy, net: ts.NetworkConfiguration, tags: taskTags(sv, d, cl), exec: sv.EnableExecuteCommand, lbs: ts.LoadBalancers}
				for range n {
					started = append(started, "(task "+last(s.newTask(account, region, cl, d, o).TaskArn)+")")
				}
			}
		}
	}
	mine = s.tasksOf(sv)
	for _, ts := range sv.TaskSets {
		ts.RunningCount, ts.PendingCount = 0, 0
		for _, t := range mine {
			switch {
			case t.StartedBy != ts.Id:
			case t.LastStatus == "RUNNING":
				ts.RunningCount++
			default:
				ts.PendingCount++
			}
		}
		st := "STABILIZING"
		if ts.Status != "DRAINING" && ts.RunningCount == ts.ComputedDesiredCount && ts.PendingCount == 0 {
			st = "STEADY_STATE"
		}
		if st != ts.StabilityStatus {
			ts.StabilityStatus, ts.StabilityStatusAt, ts.UpdatedAt = st, core.Time(now), core.Time(now)
		}
	}
	sv.TaskSets = slices.DeleteFunc(sv.TaskSets, func(ts *TaskSet) bool {
		return ts.Status == "DRAINING" && ts.RunningCount+ts.PendingCount == 0
	})
	sv.RunningCount, sv.PendingCount = 0, 0
	for _, t := range mine {
		if t.LastStatus == "RUNNING" {
			sv.RunningCount++
		} else {
			sv.PendingCount++
		}
	}
	if sv.Status == "DRAINING" && len(mine) == 0 {
		sv.Status, sv.TaskSets = "INACTIVE", nil
	}
	if len(stopped) > 0 {
		sv.event(now, "has stopped %d running tasks: %s.", len(stopped), strings.Join(stopped, " "))
	}
	if len(started) > 0 {
		sv.event(now, "has started %d tasks: %s.", len(started), strings.Join(started, " "))
	}
}
