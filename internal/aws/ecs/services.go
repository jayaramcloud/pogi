package ecs

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Svc is an ECS service.
type Svc struct {
	ServiceArn                    string          `json:"serviceArn"`
	ServiceName                   string          `json:"serviceName"`
	ClusterArn                    string          `json:"clusterArn"`
	Status                        string          `json:"status"`
	CurrentServiceDeployment      string          `json:"currentServiceDeployment,omitempty"`
	TaskDefinition                string          `json:"taskDefinition"`
	DesiredCount                  int             `json:"desiredCount"`
	RunningCount                  int             `json:"runningCount"`
	PendingCount                  int             `json:"pendingCount"`
	LaunchType                    string          `json:"launchType,omitempty"`
	CapacityProviderStrategy      []Strategy      `json:"capacityProviderStrategy,omitempty"`
	PlatformVersion               string          `json:"platformVersion,omitempty"`
	PlatformFamily                string          `json:"platformFamily,omitempty"`
	LoadBalancers                 []LB            `json:"loadBalancers"`
	ServiceRegistries             json.RawMessage `json:"serviceRegistries,omitempty"`
	NetworkConfiguration          *NetConf        `json:"networkConfiguration,omitempty"`
	DeploymentConfiguration       map[string]any  `json:"deploymentConfiguration"`
	DeploymentController          json.RawMessage `json:"deploymentController,omitempty"`
	Deployments                   []*Deployment   `json:"deployments"`
	TaskSets                      []*TaskSet      `json:"taskSets,omitempty"` // CODE_DEPLOY services
	Events                        []Event         `json:"events"`
	PlacementConstraints          json.RawMessage `json:"placementConstraints,omitempty"`
	PlacementStrategy             json.RawMessage `json:"placementStrategy,omitempty"`
	HealthCheckGracePeriodSeconds *int            `json:"healthCheckGracePeriodSeconds,omitempty"`
	SchedulingStrategy            string          `json:"schedulingStrategy"`
	RoleArn                       string          `json:"roleArn,omitempty"`
	PropagateTags                 string          `json:"propagateTags,omitempty"`
	EnableECSManagedTags          bool            `json:"enableECSManagedTags"`
	EnableExecuteCommand          bool            `json:"enableExecuteCommand"`
	AvailabilityZoneRebalancing   string          `json:"availabilityZoneRebalancing,omitempty"`
	Tags                          []Tag           `json:"tags,omitempty"`
	CreatedAt                     core.Time       `json:"createdAt"`
	CreatedBy                     string          `json:"createdBy,omitempty"`
}

// LB is a load balancer (target group) of a service.
type LB struct {
	TargetGroupArn   string `json:"targetGroupArn,omitempty"`
	LoadBalancerName string `json:"loadBalancerName,omitempty"`
	ContainerName    string `json:"containerName,omitempty"`
	ContainerPort    int    `json:"containerPort,omitempty"`
}

// Deployment is a revision of a service: PRIMARY, and ACTIVE while a newer
// one replaces its tasks.
type Deployment struct {
	Id                          string          `json:"id"`
	Status                      string          `json:"status"`
	TaskDefinition              string          `json:"taskDefinition"`
	DesiredCount                int             `json:"desiredCount"`
	PendingCount                int             `json:"pendingCount"`
	RunningCount                int             `json:"runningCount"`
	FailedTasks                 int             `json:"failedTasks"`
	CreatedAt                   core.Time       `json:"createdAt"`
	UpdatedAt                   core.Time       `json:"updatedAt"`
	LaunchType                  string          `json:"launchType,omitempty"`
	CapacityProviderStrategy    []Strategy      `json:"capacityProviderStrategy,omitempty"`
	PlatformVersion             string          `json:"platformVersion,omitempty"`
	PlatformFamily              string          `json:"platformFamily,omitempty"`
	NetworkConfiguration        *NetConf        `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration json.RawMessage `json:"serviceConnectConfiguration,omitempty"`
	RolloutState                string          `json:"rolloutState"`
	RolloutStateReason          string          `json:"rolloutStateReason"`

	failedAt time.Time // last failed task, for backing off
}

// Event is a service event.
type Event struct {
	Id        string    `json:"id"`
	CreatedAt core.Time `json:"createdAt"`
	Message   string    `json:"message"`
}

type serviceInput struct {
	Cluster, ServiceName, Service, TaskDefinition, LaunchType, PlatformVersion, Role string
	SchedulingStrategy, PropagateTags, AvailabilityZoneRebalancing                   string
	DesiredCount                                                                     *int
	CapacityProviderStrategy                                                         []Strategy
	LoadBalancers                                                                    []LB
	NetworkConfiguration                                                             *NetConf
	DeploymentConfiguration                                                          map[string]any
	ServiceRegistries, PlacementConstraints, PlacementStrategy, DeploymentController json.RawMessage
	ServiceConnectConfiguration                                                      json.RawMessage
	HealthCheckGracePeriodSeconds                                                    *int
	EnableECSManagedTags, EnableExecuteCommand                                       *bool
	ForceNewDeployment, Force                                                        bool
	Tags                                                                             []Tag
}

func deref[T any](p *T) (v T) {
	if p != nil {
		v = *p
	}
	return v
}

// checkLBs checks that each load balancer names a container and port of d.
func checkLBs(d *TaskDef, lbs []LB) error {
	if d == nil { // a deleted revision: nothing to check against
		return nil
	}
	var sp defSpec
	remarshal(d.Def, &sp)
	for _, lb := range lbs {
		i := slices.IndexFunc(sp.ContainerDefinitions, func(cd containerDef) bool { return cd.Name == lb.ContainerName })
		if i < 0 {
			return invalid("The container %s does not exist in the task definition.", lb.ContainerName)
		}
		if !slices.ContainsFunc(sp.ContainerDefinitions[i].PortMappings, func(pm portMapping) bool { return pm.ContainerPort == lb.ContainerPort }) {
			return invalid("The container %s did not have a container port %d defined.", lb.ContainerName, lb.ContainerPort)
		}
	}
	return nil
}

// network defaults assignPublicIp the way AWS shows it.
func network(n *NetConf) *NetConf {
	if n != nil && n.AwsvpcConfiguration.AssignPublicIp == "" {
		n.AwsvpcConfiguration.AssignPublicIp = "DISABLED"
	}
	return n
}

func (s *Service) createService(c *core.Ctx, in *serviceInput) (any, error) {
	if !nameRe.MatchString(in.ServiceName) {
		return nil, invalid("Service name must be 1 to 255 letters, numbers, hyphens and underscores.")
	}
	if in.DesiredCount != nil && *in.DesiredCount < 0 {
		return nil, invalid("desiredCount must be 0 or more.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	d := s.taskDef(c, in.TaskDefinition)
	if err := runnable(d, in.LaunchType, in.NetworkConfiguration, invalid); err != nil {
		return nil, err
	}
	if err := checkLBs(d, in.LoadBalancers); err != nil {
		return nil, err
	}
	k := key(c.Account, c.Region, cl.ClusterName, in.ServiceName)
	switch old := s.st.Services[k]; {
	case old == nil || old.Status == "INACTIVE":
	case old.Status == "DRAINING":
		return nil, invalid("Unable to Start a service that is still Draining.")
	default:
		return nil, invalid("Creation of service was not idempotent.")
	}
	sched := cmp.Or(in.SchedulingStrategy, "REPLICA")
	dc := in.DeploymentConfiguration
	if dc == nil {
		dc = map[string]any{}
	}
	maxPct, minPct := 200.0, 100.0
	if sched == "DAEMON" {
		maxPct, minPct = 100, 0
	}
	if _, ok := dc["maximumPercent"]; !ok {
		dc["maximumPercent"] = maxPct
	}
	if _, ok := dc["minimumHealthyPercent"]; !ok {
		dc["minimumHealthyPercent"] = minPct
	}
	breakerSwitches(dc)
	launch := in.LaunchType
	if launch == "" && len(in.CapacityProviderStrategy) == 0 && len(cl.DefaultCapacityProviderStrategy) == 0 {
		launch = "EC2"
	}
	now := time.Now()
	sv := &Svc{ServiceArn: c.ARN("ecs", "service/"+cl.ClusterName+"/"+in.ServiceName), ServiceName: in.ServiceName, ClusterArn: cl.ClusterArn,
		Status: "ACTIVE", TaskDefinition: d.Arn, DesiredCount: deref(in.DesiredCount), LaunchType: launch, CapacityProviderStrategy: in.CapacityProviderStrategy,
		PlatformVersion: in.PlatformVersion, LoadBalancers: orEmpty(in.LoadBalancers), ServiceRegistries: in.ServiceRegistries,
		NetworkConfiguration: network(in.NetworkConfiguration), DeploymentConfiguration: dc, DeploymentController: in.DeploymentController,
		PlacementConstraints: in.PlacementConstraints, PlacementStrategy: in.PlacementStrategy, HealthCheckGracePeriodSeconds: in.HealthCheckGracePeriodSeconds,
		SchedulingStrategy: sched, RoleArn: in.Role, PropagateTags: cmp.Or(in.PropagateTags, "NONE"), EnableECSManagedTags: deref(in.EnableECSManagedTags),
		EnableExecuteCommand: deref(in.EnableExecuteCommand), AvailabilityZoneRebalancing: cmp.Or(in.AvailabilityZoneRebalancing, rebalanceDefault(sched)), Tags: in.Tags,
		CreatedAt: core.Time(now), CreatedBy: c.Principal, Events: []Event{}}
	if sv.DeploymentController == nil {
		sv.DeploymentController = json.RawMessage(`{"type":"ECS"}`)
	}
	if launch == "FARGATE" {
		sv.PlatformVersion, sv.PlatformFamily = cmp.Or(sv.PlatformVersion, "LATEST"), "Linux"
	}
	if codeDeploy(sv) { // CodeDeploy runs the deployments: the service starts with one PRIMARY task set
		addSet(sv, &TaskSet{Status: "PRIMARY", TaskDefinition: d.Arn, LaunchType: launch, CapacityProviderStrategy: sv.CapacityProviderStrategy,
			PlatformVersion: sv.PlatformVersion, PlatformFamily: sv.PlatformFamily, NetworkConfiguration: sv.NetworkConfiguration,
			LoadBalancers: sv.LoadBalancers, ServiceRegistries: sv.ServiceRegistries, Scale: Scale{100, "PERCENT"}}, now)
	} else {
		sv.deploy(now, in.ServiceConnectConfiguration)
		s.emitDeploying(sv, now)
	}
	s.st.Services[k] = sv
	s.reconcile(sv, now)
	return snap(map[string]any{"service": sv}), nil
}

// service finds a service of a cluster by name or ARN; lock held.
func (s *Service) service(c *core.Ctx, cluster, ref string) (*Svc, error) {
	cl, err := s.cluster(c, cmp.Or(cluster, clusterOf(ref)))
	if err != nil {
		return nil, err
	}
	if sv := s.st.Services[key(c.Account, c.Region, cl.ClusterName, last(ref))]; sv != nil {
		return sv, nil
	}
	return nil, errService
}

func (s *Service) updateService(c *core.Ctx, in *serviceInput) (any, error) {
	if in.DesiredCount != nil && *in.DesiredCount < 0 {
		return nil, invalid("desiredCount must be 0 or more.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.Cluster, in.Service)
	if err != nil {
		return nil, err
	}
	if sv.Status != "ACTIVE" {
		return nil, errActive
	}
	if codeDeploy(sv) {
		for what, set := range map[string]bool{"task definition": in.TaskDefinition != "", "load balancers": in.LoadBalancers != nil,
			"network parameters": in.NetworkConfiguration != nil, "platform version": in.PlatformVersion != "",
			"capacity providers": in.CapacityProviderStrategy != nil, "deployments": in.ForceNewDeployment} {
			if set {
				return nil, invalid("Unable to update %s on services with a CODE_DEPLOY deployment controller. Use AWS CodeDeploy to trigger a new deployment.", what)
			}
		}
	}
	changed := func(a, b any) bool { return string(snap(a)) != string(snap(b)) }
	deploy := in.ForceNewDeployment
	if in.TaskDefinition != "" {
		d := s.taskDef(c, in.TaskDefinition)
		if err := runnable(d, sv.LaunchType, cmp.Or(in.NetworkConfiguration, sv.NetworkConfiguration), invalid); err != nil {
			return nil, err
		}
		deploy = deploy || d.Arn != sv.TaskDefinition
		sv.TaskDefinition = d.Arn
	}
	if in.LoadBalancers != nil {
		if err := checkLBs(s.taskDef(c, sv.TaskDefinition), in.LoadBalancers); err != nil {
			return nil, err
		}
		deploy = deploy || changed(in.LoadBalancers, sv.LoadBalancers)
		sv.LoadBalancers = in.LoadBalancers
	}
	if in.NetworkConfiguration != nil {
		deploy = deploy || changed(network(in.NetworkConfiguration), sv.NetworkConfiguration)
		sv.NetworkConfiguration = in.NetworkConfiguration
	}
	if in.CapacityProviderStrategy != nil {
		deploy = deploy || changed(in.CapacityProviderStrategy, sv.CapacityProviderStrategy)
		sv.CapacityProviderStrategy = in.CapacityProviderStrategy
	}
	if in.ServiceConnectConfiguration != nil && len(sv.Deployments) > 0 {
		deploy = deploy || changed(in.ServiceConnectConfiguration, sv.Deployments[0].ServiceConnectConfiguration)
	}
	if in.DesiredCount != nil {
		sv.DesiredCount = *in.DesiredCount
	}
	breakerSwitches(in.DeploymentConfiguration)
	for k, v := range in.DeploymentConfiguration {
		sv.DeploymentConfiguration[k] = v
	}
	sv.PlatformVersion = cmp.Or(in.PlatformVersion, sv.PlatformVersion)
	sv.PropagateTags = cmp.Or(in.PropagateTags, sv.PropagateTags)
	sv.AvailabilityZoneRebalancing = cmp.Or(in.AvailabilityZoneRebalancing, sv.AvailabilityZoneRebalancing)
	sv.HealthCheckGracePeriodSeconds = cmp.Or(in.HealthCheckGracePeriodSeconds, sv.HealthCheckGracePeriodSeconds)
	if in.EnableExecuteCommand != nil {
		sv.EnableExecuteCommand = *in.EnableExecuteCommand
	}
	if in.EnableECSManagedTags != nil {
		sv.EnableECSManagedTags = *in.EnableECSManagedTags
	}
	for _, raw := range []struct{ in, to *json.RawMessage }{{&in.ServiceRegistries, &sv.ServiceRegistries},
		{&in.PlacementConstraints, &sv.PlacementConstraints}, {&in.PlacementStrategy, &sv.PlacementStrategy}, {&in.DeploymentController, &sv.DeploymentController}} {
		if *raw.in != nil {
			*raw.to = *raw.in
		}
	}
	now := time.Now()
	if deploy {
		sv.deploy(now, in.ServiceConnectConfiguration)
		s.emitDeploying(sv, now)
	}
	s.reconcile(sv, now)
	return snap(map[string]any{"service": sv}), nil
}

func (s *Service) deleteService(c *core.Ctx, in *serviceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.Cluster, in.Service)
	if err != nil {
		return nil, err
	}
	switch sv.Status {
	case "INACTIVE":
		return nil, errActive
	case "ACTIVE":
		if sv.DesiredCount > 0 && !in.Force && sv.SchedulingStrategy == "REPLICA" {
			return nil, invalid("The service cannot be stopped while it is scaled above 0.")
		}
		sv.Status, sv.DesiredCount = "DRAINING", 0
	}
	out := snap(map[string]any{"service": sv}) // DRAINING, even when no task is left to stop
	s.reconcile(sv, time.Now())
	return out, nil
}

func (s *Service) describeServices(c *core.Ctx, in *struct {
	Cluster  string
	Services []string
	Include  []string
}) (any, error) {
	if len(in.Services) == 0 {
		return nil, invalid("Services cannot be empty.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, cmp.Or(in.Cluster, clusterOf(in.Services[0])))
	if err != nil {
		return nil, err
	}
	out := struct {
		Services []Svc     `json:"services"`
		Failures []failure `json:"failures"`
	}{[]Svc{}, []failure{}}
	for _, ref := range in.Services {
		sv := s.st.Services[key(c.Account, c.Region, cl.ClusterName, last(ref))]
		if sv == nil {
			out.Failures = append(out.Failures, failure{Arn: c.ARN("ecs", "service/"+cl.ClusterName+"/"+last(ref)), Reason: "MISSING"})
			continue
		}
		v := *sv
		if !slices.Contains(in.Include, "TAGS") {
			v.Tags = nil
		}
		out.Services = append(out.Services, v)
	}
	return snap(out), nil
}

func (s *Service) listServices(c *core.Ctx, in *struct {
	Cluster, LaunchType, SchedulingStrategy, NextToken string
	MaxResults                                         int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	var arns []string
	for _, sv := range s.st.Services {
		if sv.ClusterArn == cl.ClusterArn && sv.Status != "INACTIVE" && (in.LaunchType == "" || sv.LaunchType == in.LaunchType) &&
			(in.SchedulingStrategy == "" || sv.SchedulingStrategy == in.SchedulingStrategy) {
			arns = append(arns, sv.ServiceArn)
		}
	}
	slices.Sort(arns)
	return page("serviceArns", arns, in.NextToken, in.MaxResults, 10), nil
}

// listServicesByNamespace is empty: pogi has no Service Connect namespaces.
func (s *Service) listServicesByNamespace(c *core.Ctx, _ *struct{ Namespace string }) (any, error) {
	return map[string]any{"serviceArns": []string{}}, nil
}

// ---- deployments ----

// deploy makes a PRIMARY deployment of the service's current configuration;
// the previous ones turn ACTIVE until their tasks are replaced. A nil
// serviceConnect keeps the one of the previous PRIMARY.
func (sv *Svc) deploy(now time.Time, serviceConnect json.RawMessage) {
	if serviceConnect == nil && len(sv.Deployments) > 0 {
		serviceConnect = sv.Deployments[0].ServiceConnectConfiguration
	}
	for _, d := range sv.Deployments {
		d.Status = "ACTIVE"
	}
	id := "ecs-svc/" + core.RandString(19, "0123456789")
	d := &Deployment{Id: id, Status: "PRIMARY", TaskDefinition: sv.TaskDefinition, DesiredCount: sv.DesiredCount,
		CreatedAt: core.Time(now), UpdatedAt: core.Time(now), LaunchType: sv.LaunchType, CapacityProviderStrategy: sv.CapacityProviderStrategy,
		PlatformVersion: sv.PlatformVersion, PlatformFamily: sv.PlatformFamily, NetworkConfiguration: sv.NetworkConfiguration,
		ServiceConnectConfiguration: serviceConnect, RolloutState: "IN_PROGRESS", RolloutStateReason: "ECS deployment " + id + " in progress."}
	sv.Deployments = append([]*Deployment{d}, sv.Deployments...)
	sv.CurrentServiceDeployment = sv.arnOf("service-deployment", d)
}

// arnOf is the ARN of a deployment as a service deployment or service revision.
func (sv *Svc) arnOf(kind string, d *Deployment) string {
	account, region := where(sv.ServiceArn)
	return core.ARN("ecs", region, account, kind+"/"+last(sv.ClusterArn)+"/"+sv.ServiceName+"/"+strings.TrimPrefix(d.Id, "ecs-svc/"))
}

// serviceDeployment is d as the service deployment APIs show it; full adds
// what DescribeServiceDeployments tells beyond ListServiceDeployments.
// ponytail: only the deployments a service still has are listed, not its history.
func (sv *Svc) serviceDeployment(d *Deployment, full bool) map[string]any {
	status := map[string]string{"IN_PROGRESS": "IN_PROGRESS", "COMPLETED": "SUCCESSFUL", "FAILED": "STOPPED"}[d.RolloutState]
	if d.Status == "ACTIVE" && status == "IN_PROGRESS" { // superseded before it completed
		status = "STOPPED"
	}
	out := map[string]any{"serviceDeploymentArn": sv.arnOf("service-deployment", d), "serviceArn": sv.ServiceArn, "clusterArn": sv.ClusterArn,
		"createdAt": d.CreatedAt, "startedAt": d.CreatedAt, "status": status, "statusReason": d.RolloutStateReason}
	if status == "SUCCESSFUL" {
		out["finishedAt"] = d.UpdatedAt
	}
	rev := func(d *Deployment) map[string]any {
		return map[string]any{"arn": sv.arnOf("service-revision", d), "requestedTaskCount": d.DesiredCount, "runningTaskCount": d.RunningCount, "pendingTaskCount": d.PendingCount}
	}
	if !full {
		out["targetServiceRevisionArn"] = sv.arnOf("service-revision", d)
		return out
	}
	src := []any{}
	for _, o := range sv.Deployments {
		if o != d {
			src = append(src, rev(o))
		}
	}
	out["targetServiceRevision"], out["sourceServiceRevisions"], out["updatedAt"], out["deploymentConfiguration"] = rev(d), src, d.UpdatedAt, sv.DeploymentConfiguration
	return out
}

func (s *Service) listServiceDeployments(c *core.Ctx, in *struct {
	Cluster, Service string
	Status           []string
	CreatedAt        *struct{ After, Before *core.Time }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.Cluster, in.Service)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	deps := sv.Deployments
	if sv.Status != "ACTIVE" { // deleting a service deletes its service deployments
		deps = nil
	}
	for _, d := range deps {
		v, at := sv.serviceDeployment(d, false), time.Time(d.CreatedAt)
		if w := in.CreatedAt; w != nil && (w.After != nil && at.Before(time.Time(*w.After)) || w.Before != nil && at.After(time.Time(*w.Before))) ||
			len(in.Status) > 0 && !slices.Contains(in.Status, v["status"].(string)) {
			continue
		}
		out = append(out, v)
	}
	return snap(map[string]any{"serviceDeployments": out}), nil
}

func (s *Service) describeServiceDeployments(c *core.Ctx, in *struct{ ServiceDeploymentArns []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := struct {
		ServiceDeployments []map[string]any `json:"serviceDeployments"`
		Failures           []failure        `json:"failures"`
	}{[]map[string]any{}, []failure{}}
	for _, arn := range in.ServiceDeploymentArns {
		_, region, account, res, _ := core.ParseARN(arn)
		if p := strings.Split(res, "/"); len(p) == 4 && p[0] == "service-deployment" {
			if sv := s.st.Services[key(account, region, p[1], p[2])]; sv != nil && sv.Status == "ACTIVE" {
				if d := sv.deployment("ecs-svc/" + p[3]); d != nil {
					out.ServiceDeployments = append(out.ServiceDeployments, sv.serviceDeployment(d, true))
					continue
				}
			}
		}
		out.Failures = append(out.Failures, failure{Arn: arn, Reason: "MISSING"})
	}
	return snap(out), nil
}

func (sv *Svc) deployment(id string) *Deployment {
	for _, d := range sv.Deployments {
		if d.Id == id {
			return d
		}
	}
	return nil
}

// event records a service event, newest first, keeping the last 100.
func (sv *Svc) event(now time.Time, format string, a ...any) {
	e := Event{Id: core.UUID(), CreatedAt: core.Time(now), Message: "(service " + sv.ServiceName + ") " + fmt.Sprintf(format, a...)}
	sv.Events = append([]Event{e}, sv.Events[:min(len(sv.Events), 99)]...)
}

// emit puts an ECS event of the service on the default event bus, as AWS does
// (aws.ecs). It delivers in the background, so a target that calls ECS back
// cannot deadlock the lock held here.
func (s *Service) emit(sv *Svc, detailType, name string, now time.Time, extra map[string]any) {
	account, region := where(sv.ServiceArn)
	detail := map[string]any{"eventType": "INFO", "eventName": name, "clusterArn": sv.ClusterArn}
	if detailType == "ECS Deployment State Change" {
		detail["updatedAt"] = now.UTC().Format(time.RFC3339)
	} else {
		detail["createdAt"] = now.UTC().Format(time.RFC3339)
	}
	maps.Copy(detail, extra)
	body, _ := json.Marshal(detail)
	bus := core.ARN("events", region, account, "event-bus/default")
	msg := &core.Message{Body: string(body), Source: "aws.ecs", DetailType: detailType, From: sv.ServiceArn, Resources: []string{sv.ServiceArn}}
	core.Go("ecs.deliver", func() { s.hub.Deliver(bus, msg) })
}

func (s *Service) emitDeploying(sv *Svc, now time.Time) {
	d := sv.Deployments[0]
	s.emit(sv, "ECS Deployment State Change", "SERVICE_DEPLOYMENT_IN_PROGRESS", now, map[string]any{"deploymentId": d.Id, "reason": d.RolloutStateReason})
}

// tasksOf are the service's tasks that have not STOPPED, oldest first; lock held.
func (s *Service) tasksOf(sv *Svc) []*Task {
	var out []*Task
	for _, t := range s.st.Tasks {
		if t.ClusterArn == sv.ClusterArn && t.Group == "service:"+sv.ServiceName && t.LastStatus != "STOPPED" {
			out = append(out, t)
		}
	}
	slices.SortFunc(out, func(a, b *Task) int {
		return cmp.Or(time.Time(a.CreatedAt).Compare(time.Time(b.CreatedAt)), strings.Compare(a.TaskArn, b.TaskArn))
	})
	return out
}

// reconcile moves a service toward its deployments: it starts and stops
// tasks within maximumPercent and minimumHealthyPercent (a rolling update
// starts new tasks first, then stops old ones), retires drained deployments
// and keeps the counts, rollout state and events; lock held. Tasks that keep
// failing to start are retried with a growing delay.
// The deployment circuit breaker fails a deployment whose tasks keep failing to
// start, and rolls back to the last completed one when asked (see breaker).
// ponytail: no placement or load balancer health gating; tasks count as
// healthy once RUNNING.
func (s *Service) reconcile(sv *Svc, now time.Time) {
	if s.closed || sv.Status == "INACTIVE" {
		return
	}
	if codeDeploy(sv) {
		s.reconcileSets(sv, now)
		return
	}
	if len(sv.Deployments) == 0 {
		return
	}
	account, region := where(sv.ServiceArn)
	mine, primary := s.tasksOf(sv), sv.Deployments[0]
	var started, stopped []string
	why := "Scaling activity initiated by (deployment " + primary.Id + ")"
	stop := func(t *Task) {
		if t.DesiredStatus != "STOPPED" {
			s.stopTask(t, "ServiceSchedulerInitiated", why)
			stopped = append(stopped, "(task "+last(t.TaskArn)+")")
		}
	}
	live := func(dep string) (out []*Task) {
		for _, t := range mine {
			if t.StartedBy == dep && t.DesiredStatus == "RUNNING" {
				out = append(out, t)
			}
		}
		return out
	}
	if sv.Status == "DRAINING" {
		for _, t := range mine {
			stop(t)
		}
		if len(mine) == 0 {
			sv.Status = "INACTIVE"
		}
	} else {
		cl := s.st.Clusters[key(account, region, last(sv.ClusterArn))]
		// stored data may be bad; a negative count must never panic the loop
		desired := max(sv.DesiredCount, 0)
		if sv.SchedulingStrategy == "DAEMON" && cl != nil { // one task per container instance
			desired = 0
			for _, ci := range s.st.Instances {
				if ci.in(cl) && ci.Status == "ACTIVE" {
					desired++
				}
			}
			sv.DesiredCount = desired
		}
		primary.DesiredCount = desired
		if s.breaker(sv, now) { // rolled back: the new primary is reconciled by the call below
			s.reconcile(sv, now)
			return
		}
		pct := func(k string) int { f, _ := sv.DeploymentConfiguration[k].(float64); return int(f) }
		fresh := live(primary.Id)
		for len(fresh) > desired { // scale in, newest first
			stop(fresh[len(fresh)-1])
			fresh = fresh[:len(fresh)-1]
		}
		n := min(desired-len(fresh), desired*pct("maximumPercent")/100-len(mine))
		if backoff := time.Duration(min(1<<min(primary.FailedTasks, 6), 60)) * time.Second; primary.FailedTasks > 0 && now.Sub(primary.failedAt) < backoff ||
			primary.RolloutState == "FAILED" {
			n = 0
		}
		if d := s.st.Defs[key(account, region, last(primary.TaskDefinition))]; d != nil && cl != nil && n > 0 {
			o := &taskOpts{launchType: primary.LaunchType, platformVersion: primary.PlatformVersion, startedBy: primary.Id, group: "service:" + sv.ServiceName,
				strategy: primary.CapacityProviderStrategy, net: primary.NetworkConfiguration, tags: taskTags(sv, d, cl), exec: sv.EnableExecuteCommand, lbs: sv.LoadBalancers}
			for range n {
				started = append(started, "(task "+last(s.newTask(account, region, cl, d, o).TaskArn)+")")
			}
		}
		healthy := 0
		for _, t := range mine {
			if t.LastStatus == "RUNNING" && t.DesiredStatus == "RUNNING" {
				healthy++
			}
		}
		floor := (desired*pct("minimumHealthyPercent") + 99) / 100
		for _, dep := range sv.Deployments[1:] {
			for _, t := range live(dep.Id) {
				if t.LastStatus == "RUNNING" {
					if healthy <= floor {
						continue
					}
					healthy--
				}
				stop(t)
			}
		}
	}
	mine = s.tasksOf(sv)
	sv.Deployments = slices.DeleteFunc(sv.Deployments, func(d *Deployment) bool {
		return d != primary && !slices.ContainsFunc(mine, func(t *Task) bool { return t.StartedBy == d.Id })
	})
	sv.RunningCount, sv.PendingCount = 0, 0
	for _, d := range sv.Deployments {
		d.RunningCount, d.PendingCount = 0, 0
	}
	for _, t := range mine {
		d := sv.deployment(t.StartedBy)
		if d == nil {
			d = &Deployment{}
		}
		if t.LastStatus == "RUNNING" {
			sv.RunningCount, d.RunningCount = sv.RunningCount+1, d.RunningCount+1
		} else {
			sv.PendingCount, d.PendingCount = sv.PendingCount+1, d.PendingCount+1
		}
	}
	if len(stopped) > 0 {
		sv.event(now, "has stopped %d running tasks: %s.", len(stopped), strings.Join(stopped, " "))
	}
	if len(started) > 0 {
		sv.event(now, "has started %d tasks: %s.", len(started), strings.Join(started, " "))
	}
	if sv.Status == "ACTIVE" && len(sv.Deployments) == 1 && primary.RunningCount == primary.DesiredCount && primary.PendingCount == 0 {
		if primary.RolloutState == "IN_PROGRESS" {
			primary.RolloutState, primary.RolloutStateReason, primary.UpdatedAt = "COMPLETED", "ECS deployment "+primary.Id+" completed.", core.Time(now)
			sv.event(now, "(deployment %s) deployment completed.", primary.Id)
			s.emit(sv, "ECS Deployment State Change", "SERVICE_DEPLOYMENT_COMPLETED", now, map[string]any{"deploymentId": primary.Id, "reason": primary.RolloutStateReason})
		}
		if len(sv.Events) == 0 || !strings.HasSuffix(sv.Events[0].Message, "has reached a steady state.") {
			sv.event(now, "has reached a steady state.")
			s.emit(sv, "ECS Service Action", "SERVICE_STEADY_STATE", now, nil)
		}
	}
}

// breakerSwitches makes both switches of a given circuit breaker show, as AWS does.
func breakerSwitches(dc map[string]any) {
	if cb, ok := dc["deploymentCircuitBreaker"].(map[string]any); ok {
		cb["enable"], cb["rollback"] = cb["enable"] == true, cb["rollback"] == true
	}
}

// breaker is the deployment circuit breaker: when the primary deployment is still rolling out and
// its failed tasks reach the threshold (half the desired count, from 3 to 200), the deployment
// FAILS and no more tasks are started for it. With rollback on, the service goes back to the
// newest completed deployment still listed (a new PRIMARY of that revision) and breaker says
// true. Lock held.
func (s *Service) breaker(sv *Svc, now time.Time) (rolledBack bool) {
	cb, _ := sv.DeploymentConfiguration["deploymentCircuitBreaker"].(map[string]any)
	primary := sv.Deployments[0]
	if cb["enable"] != true || primary.RolloutState != "IN_PROGRESS" || primary.FailedTasks < min(max((primary.DesiredCount+1)/2, 3), 200) {
		return false
	}
	primary.RolloutState, primary.RolloutStateReason, primary.UpdatedAt = "FAILED", "ECS deployment circuit breaker: tasks failed to start.", core.Time(now)
	sv.event(now, "(deployment %s) deployment failed: tasks failed to start.", primary.Id)
	s.emit(sv, "ECS Deployment State Change", "SERVICE_DEPLOYMENT_FAILED", now, map[string]any{"eventType": "ERROR", "deploymentId": primary.Id, "reason": primary.RolloutStateReason})
	if cb["rollback"] != true {
		return false
	}
	i := slices.IndexFunc(sv.Deployments[1:], func(d *Deployment) bool { // never back to the revision that just failed
		return d.RolloutState == "COMPLETED" && d.TaskDefinition != primary.TaskDefinition
	})
	if i < 0 {
		return false
	}
	good := sv.Deployments[1+i]
	sv.event(now, "(deployment %s) rolling back to deployment %s.", primary.Id, good.Id)
	sv.TaskDefinition, sv.LaunchType, sv.PlatformVersion = good.TaskDefinition, good.LaunchType, good.PlatformVersion
	sv.CapacityProviderStrategy, sv.NetworkConfiguration = good.CapacityProviderStrategy, good.NetworkConfiguration
	sv.deploy(now, good.ServiceConnectConfiguration)
	s.emitDeploying(sv, now)
	return true
}

// taskTags are the tags a service gives its tasks.
func taskTags(sv *Svc, d *TaskDef, cl *Cluster) []Tag {
	var tags []Tag
	switch sv.PropagateTags {
	case "SERVICE":
		tags = slices.Clone(sv.Tags)
	case "TASK_DEFINITION":
		tags = slices.Clone(d.Tags)
	}
	if sv.EnableECSManagedTags {
		tags = setTags(tags, []Tag{{"aws:ecs:clusterName", cl.ClusterName}, {"aws:ecs:serviceName", sv.ServiceName}})
	}
	return tags
}

// serviceOf is the service that started t, if any; lock held.
func (s *Service) serviceOf(t *Task) *Svc {
	name, ok := strings.CutPrefix(t.Group, "service:")
	if !ok {
		return nil
	}
	account, region := where(t.TaskArn)
	return s.st.Services[key(account, region, last(t.ClusterArn), name)]
}

// registries registers a running task as an instance of its service's Cloud
// Map services (RegisterInstance), or deregisters it, through Cloud Map when
// that runs.
func (s *Service) registries(t *Task, op string) {
	var regs []struct {
		RegistryArn         string
		Port, ContainerPort int
	}
	s.mu.Lock()
	ip, name := t.run.ip, ""
	if sv := s.serviceOf(t); sv != nil {
		name = sv.ServiceName
		json.Unmarshal(sv.ServiceRegistries, &regs)
	}
	s.mu.Unlock()
	account, region := where(t.TaskArn)
	for _, reg := range regs {
		in := map[string]any{"ServiceId": last(reg.RegistryArn), "InstanceId": last(t.TaskArn)}
		if op == "RegisterInstance" {
			attrs := map[string]string{"AWS_INSTANCE_IPV4": ip, "AVAILABILITY_ZONE": t.AvailabilityZone, "REGION": region,
				"ECS_CLUSTER_NAME": t.run.sp.cluster, "ECS_SERVICE_NAME": name, "ECS_TASK_DEFINITION_FAMILY": t.run.sp.family}
			if p := cmp.Or(reg.Port, reg.ContainerPort); p > 0 {
				attrs["AWS_INSTANCE_PORT"] = strconv.Itoa(p)
			}
			in["Attributes"] = attrs
		}
		if err := s.hub.Call(account, region, "Route53AutoNaming_v20170314", op, in, nil); err != nil {
			s.hub.Log.Debug("ecs: service registry", "op", op, "registry", reg.RegistryArn, "err", err)
		}
	}
}

// targets registers a running task with its service's target groups, or
// deregisters it, through ELBv2 when that runs.
func (s *Service) targets(t *Task, op string) {
	r := t.run
	s.mu.Lock()
	ip := r.ip
	s.mu.Unlock()
	for _, lb := range r.sp.lbs {
		if lb.TargetGroupArn == "" || ip == "" { // ponytail: Classic Load Balancers are not registered
			continue
		}
		in := map[string]any{"TargetGroupArn": lb.TargetGroupArn, "Targets": []map[string]any{{"Id": ip, "Port": lb.ContainerPort}}}
		if err := s.hub.Call(r.sp.account, r.sp.region, "ElasticLoadBalancing_v10", op, in, nil); err != nil {
			s.hub.Log.Debug("ecs: load balancer target", "op", op, "targetGroup", lb.TargetGroupArn, "err", err)
			continue
		}
		s.mu.Lock()
		if sv := s.serviceOf(t); sv != nil {
			verb := map[string]string{"RegisterTargets": "registered", "DeregisterTargets": "deregistered"}[op]
			sv.event(time.Now(), "%s 1 targets in (target-group %s)", verb, lb.TargetGroupArn)
		}
		s.mu.Unlock()
	}
}

// rebalanceDefault is AWS's availabilityZoneRebalancing when CreateService leaves it out:
// ENABLED, except for daemon services, which cannot rebalance.
func rebalanceDefault(sched string) string {
	if sched == "DAEMON" {
		return "DISABLED"
	}
	return "ENABLED"
}
