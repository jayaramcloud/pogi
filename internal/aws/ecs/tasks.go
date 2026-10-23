package ecs

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// Task is an ECS task as the API shows it; run drives it while it lives.
type Task struct {
	TaskArn              string          `json:"taskArn"`
	ClusterArn           string          `json:"clusterArn"`
	TaskDefinitionArn    string          `json:"taskDefinitionArn"`
	ContainerInstanceArn string          `json:"containerInstanceArn,omitempty"`
	CapacityProviderName string          `json:"capacityProviderName,omitempty"`
	LaunchType           string          `json:"launchType,omitempty"`
	PlatformVersion      string          `json:"platformVersion,omitempty"`
	PlatformFamily       string          `json:"platformFamily,omitempty"`
	AvailabilityZone     string          `json:"availabilityZone,omitempty"`
	Group                string          `json:"group,omitempty"`
	StartedBy            string          `json:"startedBy,omitempty"`
	Overrides            json.RawMessage `json:"overrides,omitempty"`
	Cpu                  string          `json:"cpu,omitempty"`
	Memory               string          `json:"memory,omitempty"`
	LastStatus           string          `json:"lastStatus"`
	DesiredStatus        string          `json:"desiredStatus"`
	HealthStatus         string          `json:"healthStatus,omitempty"`
	Connectivity         string          `json:"connectivity,omitempty"`
	StopCode             string          `json:"stopCode,omitempty"`
	StoppedReason        string          `json:"stoppedReason,omitempty"`
	Containers           []*Container    `json:"containers"`
	Attachments          []Attachment    `json:"attachments,omitempty"`
	Tags                 []Tag           `json:"tags,omitempty"`
	EnableExecuteCommand bool            `json:"enableExecuteCommand"`
	Version              int             `json:"version"`
	CreatedAt            core.Time       `json:"createdAt"`
	ConnectivityAt       *core.Time      `json:"connectivityAt,omitempty"`
	PullStartedAt        *core.Time      `json:"pullStartedAt,omitempty"`
	PullStoppedAt        *core.Time      `json:"pullStoppedAt,omitempty"`
	StartedAt            *core.Time      `json:"startedAt,omitempty"`
	StoppingAt           *core.Time      `json:"stoppingAt,omitempty"`
	ExecutionStoppedAt   *core.Time      `json:"executionStoppedAt,omitempty"`
	StoppedAt            *core.Time      `json:"stoppedAt,omitempty"`

	run *runner
}

// Container is a container of a task.
type Container struct {
	ContainerArn      string      `json:"containerArn"`
	TaskArn           string      `json:"taskArn"`
	Name              string      `json:"name"`
	Image             string      `json:"image"`
	RuntimeId         string      `json:"runtimeId,omitempty"`
	LastStatus        string      `json:"lastStatus"`
	ExitCode          *int        `json:"exitCode,omitempty"`
	Reason            string      `json:"reason,omitempty"`
	HealthStatus      string      `json:"healthStatus,omitempty"`
	Cpu               string      `json:"cpu,omitempty"`
	Memory            string      `json:"memory,omitempty"`
	MemoryReservation string      `json:"memoryReservation,omitempty"`
	NetworkBindings   []Binding   `json:"networkBindings,omitempty"`
	NetworkInterfaces []NetworkIf `json:"networkInterfaces,omitempty"`
}

// Binding is a published port: the host port on 127.0.0.1 that reaches a container port.
type Binding struct {
	BindIP        string `json:"bindIP"`
	ContainerPort int    `json:"containerPort"`
	HostPort      int    `json:"hostPort"`
	Protocol      string `json:"protocol"`
}

// NetworkIf is a container's address on the task's network interface.
type NetworkIf struct {
	AttachmentId       string `json:"attachmentId"`
	PrivateIpv4Address string `json:"privateIpv4Address"`
}

// Attachment is the elastic network interface of an awsvpc task.
type Attachment struct {
	Id      string `json:"id"`
	Type    string `json:"type"`
	Status  string `json:"status"`
	Details []KV   `json:"details"`
}

// NetConf is an awsvpc network configuration of a task or service.
type NetConf struct {
	AwsvpcConfiguration struct {
		Subnets        []string `json:"subnets"`
		SecurityGroups []string `json:"securityGroups,omitempty"`
		AssignPublicIp string   `json:"assignPublicIp,omitempty"`
	} `json:"awsvpcConfiguration"`
}

// agentPort is where each task's pause container forwards to pogi on the
// task's loopback, the port of the ECS agent's credentials endpoint. SDKs
// fetch credentials over plain HTTP only from loopback addresses.
const agentPort = "51679"

// noEngine is the note of a task that runs as metadata only.
const noEngine = "pogi: no container engine is running, so this task runs as metadata only (start Docker or Podman to run its containers)"

// runner drives the containers of one task in the background.
type runner struct {
	stop      chan struct{} // closed when the task should stop
	token     string        // AWS_CONTAINER_AUTHORIZATION_TOKEN
	sp        *spec
	creds     map[string]string // task role credentials, minted on first request; lock held
	expires   time.Time
	ip        string      // private address once running; written with the lock held
	ports     map[int]int // container port → host port on 127.0.0.1; written with the lock held
	endpoints []string    // hub endpoints to remove when the task stops
	docker    bool        // the task runs containers
	watching  sync.WaitGroup
	once      sync.Once
}

// halt tells the runner to stop the task.
func (r *runner) halt() { r.once.Do(func() { close(r.stop) }) }

func (r *runner) stopped() bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

// spec is what a runner needs, fixed when the task is made.
type spec struct {
	account, region, cluster, id string
	family                       string
	revision                     int
	mode, role, launch           string
	memory                       int // task memory, MiB
	subnets                      []string
	containers                   []containerDef    // overrides applied
	lbs                          []LB              // service target groups to register with
	volumes                      map[string]string // volume name → host path, or "" for a Docker volume of the task
}

// taskOpts are what RunTask, StartTask and services choose about new tasks.
type taskOpts struct {
	launchType, platformVersion, startedBy, group, instance string
	strategy                                                []Strategy
	net                                                     *NetConf
	overrides                                               json.RawMessage
	tags                                                    []Tag
	exec                                                    bool
	lbs                                                     []LB
}

type taskOverride struct {
	ContainerOverrides []struct {
		Name        string   `json:"name"`
		Command     []string `json:"command"`
		Environment []KV     `json:"environment"`
		Memory      int      `json:"memory"`
	} `json:"containerOverrides"`
	TaskRoleArn string `json:"taskRoleArn"`
	Cpu         string `json:"cpu"`
	Memory      string `json:"memory"`
}

// launchType is how a task runs: its launch type, else the first provider
// of its (or its cluster's default) capacity provider strategy.
func launchType(cl *Cluster, o *taskOpts) (launch, provider string) {
	if o.launchType != "" {
		return o.launchType, ""
	}
	st := o.strategy
	if len(st) == 0 {
		st = cl.DefaultCapacityProviderStrategy
	}
	if len(st) == 0 {
		return "EC2", ""
	}
	if provider = st[0].CapacityProvider; fargate(provider) {
		return "FARGATE", provider
	}
	return "EC2", provider
}

// runnable checks that d can run with this launch type and network; errf
// makes the not-found errors (RunTask and CreateService differ).
func runnable(d *TaskDef, launch string, net *NetConf, errf func(string, ...any) error) error {
	if d == nil {
		return errf("TaskDefinition not found.")
	}
	if d.Status != "ACTIVE" {
		return errf("TaskDefinition is inactive")
	}
	var compat []string
	remarshal(d.Def["compatibilities"], &compat)
	switch mode, _ := d.Def["networkMode"].(string); {
	case launch == "FARGATE" && !slices.Contains(compat, "FARGATE"):
		return invalid("Task definition does not support launch_type FARGATE.")
	case mode == "awsvpc" && net == nil:
		return invalid("Network Configuration must be provided when networkMode 'awsvpc' is specified.")
	case mode == "awsvpc" && len(net.AwsvpcConfiguration.Subnets) == 0:
		return invalid("subnets can not be empty.")
	}
	return nil
}

// newTask makes a PROVISIONING task of d in cl and starts its runner; lock held.
func (s *Service) newTask(account, region string, cl *Cluster, d *TaskDef, o *taskOpts) *Task {
	var sp defSpec
	remarshal(d.Def, &sp)
	var ov taskOverride
	json.Unmarshal(o.overrides, &ov)
	for i, cd := range sp.ContainerDefinitions {
		for _, co := range ov.ContainerOverrides {
			if co.Name == cd.Name {
				if co.Command != nil {
					cd.Command = co.Command
				}
				cd.Environment = append(cd.Environment, co.Environment...)
				cd.Memory = cmp.Or(co.Memory, cd.Memory)
			}
		}
		sp.ContainerDefinitions[i] = cd
	}
	id, now := core.NewID(), core.Time(time.Now())
	arn := core.ARN("ecs", region, account, "task/"+cl.ClusterName+"/"+id)
	launch, provider := launchType(cl, o)
	t := &Task{TaskArn: arn, ClusterArn: cl.ClusterArn, TaskDefinitionArn: d.Arn, ContainerInstanceArn: o.instance, CapacityProviderName: provider,
		LaunchType: launch, AvailabilityZone: region + "a", Group: cmp.Or(o.group, "family:"+d.Family), StartedBy: o.startedBy,
		Overrides: o.overrides, Cpu: cmp.Or(ov.Cpu, sp.Cpu), Memory: cmp.Or(ov.Memory, sp.Memory), LastStatus: "PROVISIONING",
		DesiredStatus: "RUNNING", HealthStatus: "UNKNOWN", Tags: o.tags, EnableExecuteCommand: o.exec, Version: 1, CreatedAt: now}
	if t.Overrides == nil {
		t.Overrides = json.RawMessage(`{"containerOverrides":[]}`)
	}
	if launch == "FARGATE" {
		t.PlatformVersion, t.PlatformFamily = cmp.Or(o.platformVersion, "1.4.0"), "Linux"
	}
	for _, cd := range sp.ContainerDefinitions {
		c := &Container{ContainerArn: core.ARN("ecs", region, account, "container/"+cl.ClusterName+"/"+id+"/"+core.UUID()), TaskArn: arn,
			Name: cd.Name, Image: cd.Image, LastStatus: "PENDING", Cpu: strconv.Itoa(cd.Cpu)}
		if cd.Memory > 0 {
			c.Memory = strconv.Itoa(cd.Memory)
		}
		if cd.MemoryReservation > 0 {
			c.MemoryReservation = strconv.Itoa(cd.MemoryReservation)
		}
		if cd.HealthCheck != nil {
			c.HealthStatus = "UNKNOWN"
		}
		t.Containers = append(t.Containers, c)
	}
	var subnets []string
	if o.net != nil {
		subnets = o.net.AwsvpcConfiguration.Subnets
	}
	if sp.NetworkMode == "awsvpc" {
		a := Attachment{Id: core.UUID(), Type: "ElasticNetworkInterface", Status: "PRECREATED", Details: []KV{}}
		if len(subnets) > 0 {
			a.Details = append(a.Details, KV{"subnetId", subnets[0]})
		}
		t.Attachments = []Attachment{a}
	}
	t.run = &runner{stop: make(chan struct{}), token: core.RandHex(32), sp: &spec{account: account, region: region, cluster: cl.ClusterName,
		id: id, family: d.Family, revision: d.Revision, mode: cmp.Or(sp.NetworkMode, "bridge"), role: cmp.Or(ov.TaskRoleArn, sp.TaskRoleArn),
		launch: launch, memory: units(t.Memory), subnets: subnets, containers: sp.ContainerDefinitions, lbs: o.lbs, volumes: map[string]string{}}}
	for _, v := range sp.Volumes {
		t.run.sp.volumes[v.Name] = v.Host.SourcePath
	}
	s.st.Tasks[key(account, region, id)] = t
	core.Go("ecs.run", func() { s.run(t) })
	return t
}

// ---- API ----

type runTaskInput struct {
	Cluster, TaskDefinition, LaunchType, PlatformVersion, StartedBy, Group, PropagateTags string
	Count                                                                                 int
	CapacityProviderStrategy                                                              []Strategy
	NetworkConfiguration                                                                  *NetConf
	Overrides                                                                             json.RawMessage
	Tags                                                                                  []Tag
	EnableECSManagedTags, EnableExecuteCommand                                            bool
	ContainerInstances                                                                    []string
}

// runTask serves RunTask, and StartTask: one task per container instance.
func (s *Service) runTask(c *core.Ctx, in *runTaskInput) (any, error) {
	n := cmp.Or(in.Count, 1)
	if c.Op == "StartTask" {
		n = len(in.ContainerInstances)
	}
	if n < 1 || n > 10 {
		return nil, invalid("The number of tasks must be between 1 and 10.")
	}
	if len(in.Overrides) > 0 && json.Unmarshal(in.Overrides, &taskOverride{}) != nil {
		return nil, invalid("Invalid task overrides.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	d := s.taskDef(c, in.TaskDefinition)
	if err := runnable(d, in.LaunchType, in.NetworkConfiguration, clientErr); err != nil {
		return nil, err
	}
	tags := slices.Clone(in.Tags)
	if in.PropagateTags == "TASK_DEFINITION" {
		tags = setTags(slices.Clone(d.Tags), tags)
	}
	if in.EnableECSManagedTags {
		tags = setTags(tags, []Tag{{"aws:ecs:clusterName", cl.ClusterName}})
	}
	out := struct {
		Tasks    []*Task   `json:"tasks"`
		Failures []failure `json:"failures"`
	}{[]*Task{}, []failure{}}
	o := &taskOpts{launchType: in.LaunchType, platformVersion: in.PlatformVersion, startedBy: in.StartedBy, group: in.Group,
		strategy: in.CapacityProviderStrategy, net: in.NetworkConfiguration, overrides: in.Overrides, tags: tags, exec: in.EnableExecuteCommand}
	for i := range n {
		if c.Op == "StartTask" {
			ci := s.instance(c, cl, in.ContainerInstances[i])
			if ci == nil {
				out.Failures = append(out.Failures, failure{Arn: in.ContainerInstances[i], Reason: "MISSING"})
				continue
			}
			o.instance = ci.ContainerInstanceArn
		}
		out.Tasks = append(out.Tasks, s.newTask(c.Account, c.Region, cl, d, o))
	}
	return snap(out), nil
}

// clusterOf is the cluster named in a long task or service ARN, or "".
func clusterOf(ref string) string {
	if _, _, _, res, ok := core.ParseARN(ref); ok {
		if p := strings.Split(res, "/"); len(p) == 3 {
			return p[1]
		}
	}
	return ""
}

// task finds a task of a cluster by id or ARN; lock held.
func (s *Service) task(c *core.Ctx, cluster, ref string) (*Task, error) {
	cl, err := s.cluster(c, cmp.Or(cluster, clusterOf(ref)))
	if err != nil {
		return nil, err
	}
	if t := s.st.Tasks[key(c.Account, c.Region, last(ref))]; t != nil && t.ClusterArn == cl.ClusterArn {
		return t, nil
	}
	return nil, invalid("The referenced task was not found.")
}

func (s *Service) stopTaskOp(c *core.Ctx, in *struct{ Cluster, Task, Reason string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.task(c, in.Cluster, in.Task)
	if err != nil {
		return nil, err
	}
	s.stopTask(t, "UserInitiated", cmp.Or(in.Reason, "Task stopped by user"))
	return snap(map[string]any{"task": t}), nil
}

func (s *Service) describeTasks(c *core.Ctx, in *struct {
	Cluster string
	Tasks   []string
	Include []string
}) (any, error) {
	if len(in.Tasks) == 0 {
		return nil, invalid("Tasks cannot be empty.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, cmp.Or(in.Cluster, clusterOf(in.Tasks[0])))
	if err != nil {
		return nil, err
	}
	out := struct {
		Tasks    []Task    `json:"tasks"`
		Failures []failure `json:"failures"`
	}{[]Task{}, []failure{}}
	for _, ref := range in.Tasks {
		t := s.st.Tasks[key(c.Account, c.Region, last(ref))]
		if t == nil || t.ClusterArn != cl.ClusterArn {
			out.Failures = append(out.Failures, failure{Arn: c.ARN("ecs", "task/"+cl.ClusterName+"/"+last(ref)), Reason: "MISSING"})
			continue
		}
		v := *t
		if !slices.Contains(in.Include, "TAGS") {
			v.Tags = nil
		}
		out.Tasks = append(out.Tasks, v)
	}
	return snap(out), nil
}

func (s *Service) listTasks(c *core.Ctx, in *struct {
	Cluster, ContainerInstance, Family, StartedBy, ServiceName, DesiredStatus, LaunchType, NextToken string
	MaxResults                                                                                       int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	if in.ServiceName != "" && s.st.Services[key(c.Account, c.Region, cl.ClusterName, last(in.ServiceName))] == nil {
		return nil, errService
	}
	var arns []string
	for _, t := range s.st.Tasks {
		fam, _, _ := strings.Cut(last(t.TaskDefinitionArn), ":")
		if t.ClusterArn == cl.ClusterArn && t.DesiredStatus == cmp.Or(in.DesiredStatus, "RUNNING") &&
			(in.Family == "" || fam == in.Family) && (in.StartedBy == "" || t.StartedBy == in.StartedBy) &&
			(in.ServiceName == "" || t.Group == "service:"+last(in.ServiceName)) && (in.LaunchType == "" || t.LaunchType == in.LaunchType) &&
			(in.ContainerInstance == "" || t.ContainerInstanceArn != "" && last(t.ContainerInstanceArn) == last(in.ContainerInstance)) {
			arns = append(arns, t.TaskArn)
		}
	}
	slices.Sort(arns)
	return page("taskArns", arns, in.NextToken, in.MaxResults, 100), nil
}

// ---- lifecycle ----

// live reports whether t is still in the state, not reset or reloaded; lock held.
func (s *Service) live(t *Task) bool {
	acct, region := where(t.TaskArn)
	return s.st.Tasks[key(acct, region, last(t.TaskArn))] == t
}

func (t *Task) container(name string) *Container {
	for _, c := range t.Containers {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// stopTask asks t to stop for a reason; its runner does the rest. A task that failed counts
// against its deployment here, in the same locked step that takes it out of the running set: if
// the count waited for its containers to be removed (slow under load), a reconcile in between
// would see the task gone but the failure uncounted, start another one, and the circuit breaker
// would trip a task late. Lock held.
func (s *Service) stopTask(t *Task, code, reason string) {
	if t.DesiredStatus == "STOPPED" {
		return
	}
	now := core.Time(time.Now())
	t.DesiredStatus, t.StopCode, t.StoppedReason, t.StoppingAt = "STOPPED", code, reason, &now
	t.Version++
	if code == "TaskFailedToStart" || code == "EssentialContainerExited" {
		if sv := s.serviceOf(t); sv != nil {
			if dep := sv.deployment(t.StartedBy); dep != nil {
				dep.FailedTasks++
				dep.failedAt = time.Now()
			}
		}
	}
	if t.run != nil {
		t.run.halt()
	} else {
		t.halt()
	}
}

// halt marks t STOPPED.
func (t *Task) halt() {
	now := core.Time(time.Now())
	if t.StoppingAt == nil {
		t.StoppingAt = &now
	}
	t.LastStatus, t.DesiredStatus, t.StoppedAt, t.ExecutionStoppedAt = "STOPPED", "STOPPED", &now, &now
	for _, c := range t.Containers {
		c.LastStatus = "STOPPED"
	}
	for i := range t.Attachments {
		t.Attachments[i].Status = "DELETED"
	}
	t.Version++
}

// run takes t from PROVISIONING to RUNNING, waits until it should stop,
// then removes its containers and marks it STOPPED.
func (s *Service) run(t *Task) {
	r := t.run
	if reason := s.launch(t); reason != "" {
		s.mu.Lock()
		if !s.closed {
			s.stopTask(t, "TaskFailedToStart", reason)
		}
		s.mu.Unlock()
	} else if !r.stopped() {
		s.targets(t, "RegisterTargets")
		s.registries(t, "RegisterInstance")
		<-r.stop
		s.targets(t, "DeregisterTargets")
		s.registries(t, "DeregisterInstance")
	}
	s.teardown(r)
	s.finish(t)
}

// finish marks t STOPPED once its containers are gone and lets its service replace it (stopTask
// already counted a failure); lock taken.
func (s *Service) finish(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.live(t) {
		return
	}
	t.halt()
	if sv := s.serviceOf(t); sv != nil {
		s.reconcile(sv, time.Now())
	}
}

// up marks t RUNNING at its private address; lock held.
func (s *Service) up(t *Task, ip string) {
	r, now := t.run, core.Time(time.Now())
	r.ip = ip
	t.LastStatus, t.Connectivity, t.ConnectivityAt, t.StartedAt, t.PullStoppedAt = "RUNNING", "CONNECTED", &now, &now, &now
	if t.PullStartedAt == nil {
		t.PullStartedAt = &now
	}
	t.Version++
	for _, cd := range r.sp.containers {
		c := t.container(cd.Name)
		if c.LastStatus == "PENDING" {
			c.LastStatus = "RUNNING"
		}
		if c.RuntimeId == "" {
			c.RuntimeId = last(t.TaskArn) + "-" + core.RandString(10, "0123456789")
		}
		for _, pm := range cd.PortMappings {
			if hp := r.ports[pm.ContainerPort]; hp > 0 {
				c.NetworkBindings = append(c.NetworkBindings, Binding{"127.0.0.1", pm.ContainerPort, hp, cmp.Or(pm.Protocol, "tcp")})
			}
		}
	}
	if len(t.Attachments) > 0 {
		a, h := &t.Attachments[0], core.RandHex(5)
		mac := "02"
		for i := 0; i < len(h); i += 2 {
			mac += ":" + h[i:i+2]
		}
		a.Status = "ATTACHED"
		a.Details = append(a.Details, KV{"networkInterfaceId", "eni-" + core.RandHex(9)[:17]}, KV{"macAddress", mac},
			KV{"privateDnsName", "ip-" + strings.ReplaceAll(ip, ".", "-") + "." + r.sp.region + ".compute.internal"}, KV{"privateIPv4Address", ip})
		for _, c := range t.Containers {
			c.NetworkInterfaces = []NetworkIf{{a.Id, ip}}
		}
	}
	if sv := s.serviceOf(t); sv != nil {
		s.reconcile(sv, time.Now())
	}
}

// launch starts the containers of t, or without a container engine only
// marks it running, and says why it failed if it did.
func (s *Service) launch(t *Task) string {
	r, sp, d := t.run, t.run.sp, s.hub.Docker()
	if !d.Available() {
		cidr := s.subnetCIDR(sp)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.live(t) && !r.stopped() {
			t.StoppedReason = noEngine
			s.up(t, s.pickIP(cidr))
		}
		return ""
	}
	r.docker = true
	env, reason := s.environment(t)
	if reason != "" {
		return reason
	}
	// the pause container holds the task's network namespace, publishes its
	// ports and forwards the agent port to pogi.
	// ponytail: every network mode gets one namespace per task, so bridge
	// links are moot and two containers of a task cannot share a port.
	pause := &docker.Container{Name: docker.Name("ecs", sp.id), Image: "alpine:3", Service: "ecs", Resource: sp.id, Ports: map[int]int{},
		Cmd: []string{"sh", "-c", "nc -lk -s 127.0.0.1 -p " + agentPort + " -e nc " + s.hostAddr() + " " + s.port() + " & exec sleep infinity"}}
	honoured := false
	for _, cd := range sp.containers {
		for _, pm := range cd.PortMappings {
			if pm.Protocol == "udp" || pm.ContainerPort <= 0 { // ponytail: tcp only, as the container manager publishes
				continue
			}
			if pause.Ports[pm.ContainerPort] = 0; pm.HostPort > 0 && free(pm.HostPort) {
				pause.Ports[pm.ContainerPort], honoured = pm.HostPort, true
			}
		}
	}
	_, err := d.Run(s.ctx, pause)
	if err != nil && honoured { // a host port was taken meanwhile: any free ones
		for p := range pause.Ports {
			pause.Ports[p] = 0
		}
		_, err = d.Run(s.ctx, pause)
	}
	if err != nil {
		return "CannotStartContainerError: " + err.Error()
	}
	info, err := d.Inspect(s.ctx, pause.Name)
	if err != nil {
		return "CannotInspectContainerError: " + err.Error()
	}
	ip, ports := "", map[int]int{}
	for _, n := range info.NetworkSettings.Networks {
		ip = cmp.Or(ip, n.IPAddress)
	}
	for p := range pause.Ports {
		ports[p] = info.HostPort(p)
	}
	s.mu.Lock()
	ok := s.live(t) && !r.stopped()
	if ok {
		now := core.Time(time.Now())
		t.LastStatus, t.PullStartedAt, r.ports = "PENDING", &now, ports
		t.Version++
	}
	s.mu.Unlock()
	if !ok {
		return ""
	}
	for _, cd := range order(sp.containers) {
		if reason := s.await(t, cd); reason != "" || r.stopped() {
			return reason
		}
		logs, reason := s.logStream(sp, cd)
		if reason != "" {
			return reason
		}
		ct := &docker.Container{Name: docker.Name("ecs", sp.id+"-"+cd.Name), Image: cd.Image, Service: "ecs", Resource: sp.id, Labels: labels(sp, cd.Name),
			Env: env[cd.Name], Cmd: cd.Command, Entrypoint: cd.EntryPoint, WorkDir: cd.WorkingDirectory, Network: "container:" + pause.Name,
			Memory: int64(cmp.Or(cd.Memory, sp.memory)) << 20, Binds: sp.binds(cd)}
		// ponytail: cpu units are relative shares in ECS, and the container
		// manager sets only hard limits, so cpu is not limited
		id, err := d.Create(s.ctx, ct)
		if err != nil {
			if strings.Contains(err.Error(), "pull") {
				return "CannotPullContainerError: " + err.Error()
			}
			return "CannotCreateContainerError: " + err.Error()
		}
		if err := d.Start(s.ctx, id); err != nil {
			return "CannotStartContainerError: " + err.Error()
		}
		s.mu.Lock()
		if c := t.container(cd.Name); s.live(t) {
			c.LastStatus, c.RuntimeId = "RUNNING", id
			t.Version++
		}
		s.mu.Unlock()
		r.watching.Add(1)
		core.Go("ecs.watch", func() { s.watch(t, cd, id, logs) })
	}
	s.mu.Lock()
	if ok = s.live(t) && !r.stopped(); ok {
		s.up(t, ip)
	}
	s.mu.Unlock()
	for p, hp := range ports {
		if ok && hp > 0 && ip != "" { // load balancers reach awsvpc targets by private IP and container port
			k := "ip:" + ip + ":" + strconv.Itoa(p)
			s.hub.SetEndpoint(k, "127.0.0.1:"+strconv.Itoa(hp))
			r.endpoints = append(r.endpoints, k)
		}
	}
	return ""
}

// order sorts containers so each starts after those it depends on.
func order(defs []containerDef) []containerDef {
	var out []containerDef
	done := map[string]bool{}
	for len(out) < len(defs) {
		n := len(out)
		for _, cd := range defs {
			if !done[cd.Name] && !slices.ContainsFunc(cd.DependsOn, func(d dependency) bool { return !done[d.ContainerName] }) &&
				!slices.ContainsFunc(cd.VolumesFrom, func(v volumeFrom) bool { return !done[v.SourceContainer] }) { // the source mounts first, so its image files fill a new volume
				out, done[cd.Name] = append(out, cd), true
			}
		}
		if len(out) == n { // a cycle: the rest in definition order, and await times out
			for _, cd := range defs {
				if !done[cd.Name] {
					out, done[cd.Name] = append(out, cd), true
				}
			}
		}
	}
	return out
}

// await waits until the containers cd depends on reach their conditions
// (START, COMPLETE, SUCCESS or HEALTHY) and says why they cannot; it gives up
// quietly when the task stops.
func (s *Service) await(t *Task, cd containerDef) string {
	for _, dep := range cd.DependsOn {
		for start := time.Now(); ; {
			s.mu.Lock()
			c := t.container(dep.ContainerName)
			st, code, health := c.LastStatus, c.ExitCode, c.HealthStatus
			s.mu.Unlock()
			exited := st == "STOPPED"
			done, failed := st != "PENDING", false
			switch dep.Condition {
			case "COMPLETE":
				done = exited
			case "SUCCESS":
				done, failed = exited && code != nil && *code == 0, exited && (code == nil || *code != 0)
			case "HEALTHY":
				done, failed = health == "HEALTHY", health == "UNHEALTHY" || exited
			}
			if failed || time.Since(start) > 3*time.Minute {
				return fmt.Sprintf("DependencyError: container %s did not reach %s, so %s could not start", dep.ContainerName, dep.Condition, cd.Name)
			}
			if done {
				break
			}
			select {
			case <-t.run.stop:
				return ""
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return ""
}

// watch follows a started container: its logs, its health check, then its
// exit, which stops the task when the container is essential.
func (s *Service) watch(t *Task, cd containerDef, id string, logs *logDest) {
	defer t.run.watching.Done()
	d := s.hub.Docker()
	flushed := make(chan struct{})
	core.Go("ecs.watch", func() {
		defer close(flushed)
		if logs != nil {
			d.Logs(s.ctx, id, true, func(line string) { s.putLog(logs, line) })
		}
	})
	if cd.HealthCheck != nil && len(cd.HealthCheck.Command) > 0 {
		core.Go("ecs.health", func() { s.health(t, cd, id) })
	}
	code, err := d.Wait(s.ctx, id)
	select { // the last lines reach CloudWatch Logs before the task stops
	case <-flushed:
	case <-time.After(5 * time.Second):
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.live(t) || err != nil {
		return
	}
	c := t.container(cd.Name)
	c.LastStatus, c.ExitCode = "STOPPED", &code
	t.Version++
	if cd.essential() {
		s.stopTask(t, "EssentialContainerExited", "Essential container in task exited")
	}
}

// health runs a container's health check in it until the container stops,
// as the ECS agent does; a service replaces a task whose essential container
// turns UNHEALTHY.
func (s *Service) health(t *Task, cd containerDef, id string) {
	hc := cd.HealthCheck
	cmd := hc.Command
	switch cmd[0] {
	case "CMD-SHELL":
		cmd = []string{"sh", "-c", strings.Join(cmd[1:], " ")}
	case "CMD":
		cmd = cmd[1:]
	}
	secs := func(n, def int) time.Duration { return time.Duration(cmp.Or(n, def)) * time.Second }
	grace, fails := time.Now().Add(secs(hc.StartPeriod, 0)), 0
	for {
		ctx, cancel := context.WithTimeout(s.ctx, secs(hc.Timeout, 5))
		_, code, err := s.hub.Docker().Exec(ctx, id, cmd...)
		cancel()
		s.mu.Lock()
		c := t.container(cd.Name)
		if !s.live(t) || c.LastStatus == "STOPPED" || t.DesiredStatus == "STOPPED" {
			s.mu.Unlock()
			return
		}
		switch {
		case err == nil && code == 0:
			fails, c.HealthStatus = 0, "HEALTHY"
		case time.Now().After(grace):
			if fails++; fails >= cmp.Or(hc.Retries, 3) {
				c.HealthStatus = "UNHEALTHY"
			}
		}
		t.HealthStatus = taskHealth(t, t.run.sp.containers)
		if t.HealthStatus == "UNHEALTHY" && s.serviceOf(t) != nil {
			s.stopTask(t, "ServiceSchedulerInitiated", "Task failed container health checks")
		}
		s.mu.Unlock()
		select {
		case <-t.run.stop:
			return
		case <-time.After(secs(hc.Interval, 30)):
		}
	}
}

// taskHealth is the health of the essential containers that have checks; lock held.
func taskHealth(t *Task, defs []containerDef) string {
	checked, healthy := 0, 0
	for _, cd := range defs {
		if cd.essential() && cd.HealthCheck != nil {
			checked++
			switch t.container(cd.Name).HealthStatus {
			case "UNHEALTHY":
				return "UNHEALTHY"
			case "HEALTHY":
				healthy++
			}
		}
	}
	if checked > 0 && healthy == checked {
		return "HEALTHY"
	}
	return "UNKNOWN"
}

// teardown stops a task's containers together (the engine sends SIGTERM,
// then SIGKILL after its grace period), lets their watchers record exit codes
// and flush logs, then removes them, the pause container last.
func (s *Service) teardown(r *runner) {
	for _, k := range r.endpoints {
		s.hub.SetEndpoint(k, "")
	}
	if !r.docker {
		return
	}
	d := s.hub.Docker()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	for _, cd := range r.sp.containers {
		wg.Go(func() { d.Stop(ctx, docker.Name("ecs", r.sp.id+"-"+cd.Name)) })
	}
	wg.Wait()
	watched := make(chan struct{})
	go func() { r.watching.Wait(); close(watched) }()
	select {
	case <-watched:
	case <-time.After(15 * time.Second):
	}
	for _, cd := range r.sp.containers {
		d.Remove(ctx, docker.Name("ecs", r.sp.id+"-"+cd.Name))
	}
	d.Remove(ctx, docker.Name("ecs", r.sp.id))
	for name, host := range r.sp.volumes {
		if host == "" {
			d.RemoveVolume(ctx, r.sp.volume(name))
		}
	}
}

// volume is the Docker volume that holds a task volume that has no host path.
func (sp *spec) volume(name string) string { return docker.Name("ecs", sp.id+"-vol-"+name) }

// binds are the mounts of a container: its own mount points, and those of the
// containers it takes volumesFrom. A volume with a host path is a bind mount;
// one with only a name is a Docker volume shared by the containers of the task.
func (sp *spec) binds(cd containerDef) (out []string) {
	add := func(mp mountPoint, ro bool) {
		src, ok := sp.volumes[mp.SourceVolume]
		if !ok || mp.ContainerPath == "" {
			return
		}
		if src == "" {
			src = sp.volume(mp.SourceVolume)
		}
		b := src + ":" + mp.ContainerPath
		if ro || mp.ReadOnly {
			b += ":ro"
		}
		out = append(out, b)
	}
	for _, mp := range cd.MountPoints {
		add(mp, false)
	}
	for _, vf := range cd.VolumesFrom {
		for _, other := range sp.containers {
			if other.Name == vf.SourceContainer {
				for _, mp := range other.MountPoints {
					if !slices.ContainsFunc(cd.MountPoints, func(m mountPoint) bool { return m.ContainerPath == mp.ContainerPath }) {
						add(mp, vf.ReadOnly)
					}
				}
			}
		}
	}
	return out
}

// labels are the Docker labels the ECS agent puts on task containers.
func labels(sp *spec, name string) map[string]string {
	return map[string]string{"com.amazonaws.ecs.cluster": sp.cluster, "com.amazonaws.ecs.container-name": name,
		"com.amazonaws.ecs.task-arn":               core.ARN("ecs", sp.region, sp.account, "task/"+sp.cluster+"/"+sp.id),
		"com.amazonaws.ecs.task-definition-family": sp.family, "com.amazonaws.ecs.task-definition-version": strconv.Itoa(sp.revision)}
}

// hostAddr is where task containers reach pogi.
func (s *Service) hostAddr() string {
	if h := s.hub.Docker().HostAddr(); h != "127.0.0.1" {
		return h
	}
	// ponytail: tasks stay on the pogi network so ports publish the same
	// everywhere; on a Linux engine without a VM they reach pogi only when
	// it listens beyond loopback.
	return "host.docker.internal"
}

// port is pogi's port.
func (s *Service) port() string {
	if u, err := url.Parse(s.hub.URL); err == nil && u.Port() != "" {
		return u.Port()
	}
	return strconv.Itoa(cmp.Or(s.hub.Cfg.Port, 7644))
}

// environment builds each container's variables: the ECS agent's, the
// definition's and the overrides', then secrets from SSM or Secrets Manager.
func (s *Service) environment(t *Task) (map[string][]string, string) {
	r, sp := t.run, t.run.sp
	agent := "http://127.0.0.1:" + agentPort + "/_aws/ecs/"
	out := map[string][]string{}
	for _, cd := range sp.containers {
		env := map[string]string{"AWS_REGION": sp.region, "AWS_DEFAULT_REGION": sp.region, "AWS_EXECUTION_ENV": "AWS_ECS_" + sp.launch,
			"AWS_ENDPOINT_URL": "http://" + s.hostAddr() + ":" + s.port(), "ECS_CONTAINER_METADATA_URI_V4": agent + "metadata/" + sp.id + "/" + cd.Name}
		env["ECS_CONTAINER_METADATA_URI"] = env["ECS_CONTAINER_METADATA_URI_V4"]
		if sp.role != "" {
			env["AWS_CONTAINER_CREDENTIALS_FULL_URI"], env["AWS_CONTAINER_AUTHORIZATION_TOKEN"] = agent+"credentials/"+sp.id, r.token
		} else { // no task role: the test credentials pogi accepts
			env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"] = "test", "test"
		}
		for _, kv := range cd.Environment {
			env[kv.Name] = kv.Value
		}
		for _, sec := range cd.Secrets {
			v, err := s.secret(sp, sec.ValueFrom)
			if err != nil {
				return nil, "ResourceInitializationError: unable to pull secrets or registry auth: unable to retrieve secret " + sec.ValueFrom + ": " + err.Error()
			}
			env[sec.Name] = v
		}
		for k, v := range env {
			out[cd.Name] = append(out[cd.Name], k+"="+v)
		}
	}
	return out, ""
}

// secret resolves a valueFrom: an SSM parameter name or ARN, or a Secrets
// Manager ARN with an optional :json-key:version-stage:version-id suffix.
func (s *Service) secret(sp *spec, from string) (string, error) {
	svc, region, account, res, isARN := core.ParseARN(from)
	if svc == "secretsmanager" {
		p := strings.Split(strings.TrimPrefix(res, "secret:"), ":")
		for len(p) < 4 {
			p = append(p, "")
		}
		in := map[string]string{"SecretId": core.ARN(svc, region, account, "secret:"+p[0])}
		if p[2] != "" {
			in["VersionStage"] = p[2]
		}
		if p[3] != "" {
			in["VersionId"] = p[3]
		}
		var out struct{ SecretString string }
		if err := s.hub.Call(account, region, "secretsmanager", "GetSecretValue", in, &out); err != nil || p[1] == "" {
			return out.SecretString, err
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(out.SecretString), &doc); err != nil {
			return "", err
		}
		v, ok := doc[p[1]]
		if !ok {
			return "", fmt.Errorf("the secret has no JSON key %s", p[1])
		}
		if str, ok := v.(string); ok {
			return str, nil
		}
		b, _ := json.Marshal(v)
		return string(b), nil
	}
	if !isARN {
		account, region = sp.account, sp.region
	}
	var out struct{ Parameter struct{ Value string } }
	err := s.hub.Call(account, region, "AmazonSSM", "GetParameter", map[string]any{"Name": from, "WithDecryption": true}, &out)
	return out.Parameter.Value, err
}

// logDest is the CloudWatch Logs stream of an awslogs container.
type logDest struct{ account, region, group, stream string }

// logStream creates the stream (and with awslogs-create-group the group) of
// an awslogs container. A missing log group fails the task, as on AWS.
func (s *Service) logStream(sp *spec, cd containerDef) (*logDest, string) {
	lc := cd.LogConfiguration
	if lc == nil || lc.LogDriver != "awslogs" || lc.Options["awslogs-group"] == "" {
		return nil, ""
	}
	o := lc.Options
	l := &logDest{sp.account, cmp.Or(o["awslogs-region"], sp.region), o["awslogs-group"], cd.Name + "/" + sp.id}
	if p := o["awslogs-stream-prefix"]; p != "" {
		l.stream = p + "/" + l.stream
	} // ponytail: without a prefix AWS names the stream after the container id
	if o["awslogs-create-group"] == "true" {
		s.hub.Call(l.account, l.region, "Logs_20140328", "CreateLogGroup", map[string]string{"logGroupName": l.group}, nil)
	}
	err := s.hub.Call(l.account, l.region, "Logs_20140328", "CreateLogStream", map[string]string{"logGroupName": l.group, "logStreamName": l.stream}, nil)
	if err == nil {
		return l, ""
	}
	switch e := core.AsError(err); e.Code {
	case "ResourceAlreadyExistsException":
		return l, ""
	case "ServiceNotEnabled":
		s.hub.Log.Debug("ecs: CloudWatch Logs is not enabled, container logs are dropped", "container", cd.Name)
		return nil, ""
	default:
		return nil, "ResourceInitializationError: failed to validate logger args: create stream has been retried 1 times: failed to create Cloudwatch log stream: " +
			e.Code + ": " + e.Msg
	}
}

// putLog sends one line of a container to CloudWatch Logs.
func (s *Service) putLog(l *logDest, line string) {
	if line == "" {
		return
	}
	in := map[string]any{"logGroupName": l.group, "logStreamName": l.stream, "logEvents": []map[string]any{{"timestamp": time.Now().UnixMilli(), "message": line}}}
	if err := s.hub.Call(l.account, l.region, "Logs_20140328", "PutLogEvents", in, nil); err != nil {
		s.hub.Log.Debug("ecs: awslogs", "group", l.group, "stream", l.stream, "err", err)
	}
}

// free reports whether a port is free on 127.0.0.1.
func free(port int) bool {
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err == nil {
		ln.Close()
	}
	return err == nil
}

var cidrRe = regexp.MustCompile(`<cidrBlock>([^<]+)</cidrBlock>`)

// subnetCIDR asks EC2 for the CIDR block of the task's first subnet.
func (s *Service) subnetCIDR(sp *spec) string {
	if len(sp.subnets) > 0 && s.hub.Service("ec2") != nil {
		form := url.Values{"Action": {"DescribeSubnets"}, "Version": {"2016-11-15"}, "SubnetId.1": {sp.subnets[0]}}
		code, _, b := s.hub.Do(sp.account, sp.region, "ec2", http.MethodPost, "/",
			http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}, []byte(form.Encode()))
		if m := cidrRe.FindSubmatch(b); code == http.StatusOK && m != nil {
			return string(m[1])
		}
	}
	return "10.0.0.0/24"
}

// pickIP is an address of cidr that no live task has, for tasks without a
// container engine; lock held.
func (s *Service) pickIP(cidr string) string {
	p, err := netip.ParsePrefix(cidr)
	if err != nil || !p.Addr().Is4() || p.Bits() > 28 {
		p = netip.MustParsePrefix("10.0.0.0/24")
	}
	used := map[string]bool{}
	for _, t := range s.st.Tasks {
		if t.run != nil && t.LastStatus != "STOPPED" {
			used[t.run.ip] = true
		}
	}
	base := p.Masked().Addr().As4()
	n := binary.BigEndian.Uint32(base[:])
	var ip string
	for range 100 { // AWS keeps the first four and the last address of a subnet
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], n+4+uint32(rand.N(1<<(32-p.Bits())-5)))
		if ip = netip.AddrFrom4(b).String(); !used[ip] {
			break
		}
	}
	return ip
}

// ---- the ECS agent's endpoints ----

// agent serves what the ECS agent gives containers, reached through each
// task's pause container: task role credentials at
// /_aws/ecs/credentials/<task> (with the task's authorization token) and
// task metadata (v4) at /_aws/ecs/metadata/<task>/<container>[/task|/stats|/task/stats].
func (s *Service) agent(w http.ResponseWriter, r *http.Request) {
	p := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/_aws/ecs/"), "/"), "/")
	s.mu.Lock()
	var t *Task
	for k, x := range s.st.Tasks {
		if len(p) > 1 && last(k) == p[1] && x.run != nil {
			t = x
		}
	}
	var out any
	status := http.StatusOK
	switch {
	case t == nil:
		status, out = http.StatusNotFound, map[string]string{"code": "NotFound", "message": "no running pogi task has this id"}
	case p[0] == "credentials" && r.Header.Get("Authorization") != t.run.token:
		status, out = http.StatusUnauthorized, map[string]string{"code": "InvalidAuthorization", "message": "the authorization token does not match the task's"}
	case p[0] == "credentials" && t.run.sp.role == "":
		status, out = http.StatusNotFound, map[string]string{"code": "NoCredentials", "message": "the task has no task role"}
	case p[0] == "credentials":
		out = s.credentials(t)
	case p[0] == "metadata" && len(p) > 2 && p[len(p)-1] == "stats":
		out = map[string]any{}
	case p[0] == "metadata" && len(p) > 2 && p[len(p)-1] == "task":
		out = s.taskMeta(t)
	case p[0] == "metadata" && len(p) == 3 && t.container(p[2]) != nil:
		out = s.containerMeta(t, t.container(p[2]))
	default:
		status, out = http.StatusNotFound, map[string]string{"code": "NotFound", "message": "unknown agent path " + r.URL.Path}
	}
	b := snap(out)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(b)
}

// credentials are the task role's session credentials, minted like Lambda's
// and renewed before they expire; lock held.
func (s *Service) credentials(t *Task) map[string]string {
	r := t.run
	if r.creds == nil || time.Until(r.expires) < 15*time.Minute {
		role := r.sp.role
		_, _, account, res, ok := core.ParseARN(role)
		if !ok {
			account, res = r.sp.account, role
		}
		akid, secret := "ASIA"+core.RandString(16, core.UpperNum), core.RandString(40, core.AlphaNum)
		s.hub.AddIdentity(akid, core.Identity{Account: account, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + r.sp.id,
			ARN: core.GlobalARN("sts", r.sp.region, account, "assumed-role/"+last(res)+"/"+r.sp.id), Secret: secret})
		r.expires = time.Now().Add(6 * time.Hour).UTC()
		r.creds = map[string]string{"AccessKeyId": akid, "SecretAccessKey": secret, "Token": core.RandString(180, core.AlphaNum),
			"Expiration": r.expires.Format(time.RFC3339), "RoleArn": role}
	}
	return r.creds
}

// taskMeta is the task metadata (v4) of t; lock held.
func (s *Service) taskMeta(t *Task) map[string]any {
	sp := t.run.sp
	var cs []any
	for _, c := range t.Containers {
		cs = append(cs, s.containerMeta(t, c))
	}
	return map[string]any{"Cluster": t.ClusterArn, "TaskARN": t.TaskArn, "Family": sp.family, "Revision": strconv.Itoa(sp.revision),
		"DesiredStatus": t.DesiredStatus, "KnownStatus": t.LastStatus, "AvailabilityZone": t.AvailabilityZone, "LaunchType": t.LaunchType,
		"Limits": map[string]float64{"CPU": float64(units(t.Cpu)) / 1024, "Memory": float64(units(t.Memory))}, "Containers": cs}
}

// containerMeta is the container metadata (v4) of c; lock held.
func (s *Service) containerMeta(t *Task, c *Container) map[string]any {
	sp := t.run.sp
	return map[string]any{"DockerId": c.RuntimeId, "Name": c.Name, "DockerName": docker.Name("ecs", sp.id+"-"+c.Name), "Image": c.Image,
		"Labels": labels(sp, c.Name), "DesiredStatus": t.DesiredStatus, "KnownStatus": c.LastStatus, "Type": "NORMAL", "ContainerARN": c.ContainerArn,
		"Networks": []any{map[string]any{"NetworkMode": sp.mode, "IPv4Addresses": []string{t.run.ip}}}}
}
