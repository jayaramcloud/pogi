// Package ecs emulates Amazon ECS (JSON 1.1): clusters, capacity providers,
// task definitions, tasks, services and container instances. With a
// container engine, each task runs as real containers that share the network
// namespace of a pause container; without one, tasks run as metadata only.
package ecs

import (
	"cmp"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// Service is the ECS emulator.
type Service struct {
	hub    *core.Hub
	rpc    *core.RPC
	mu     sync.Mutex
	st     state
	ctx    context.Context // engine calls of task runners; Close cancels it
	cancel context.CancelFunc
	stop   func() // the reconcile loop
	closed bool   // shutting down: state stays as it is, for Save
}

type state struct {
	Clusters  map[string]*Cluster  `json:"clusters"`  // account/region/name
	Defs      map[string]*TaskDef  `json:"defs"`      // account/region/family:revision
	Revisions map[string]int       `json:"revisions"` // account/region/family → last revision
	Services  map[string]*Svc      `json:"services"`  // account/region/cluster/name
	Tasks     map[string]*Task     `json:"tasks"`     // account/region/id
	Providers map[string]*Provider `json:"providers"` // account/region/name
	Instances map[string]*Instance `json:"instances"` // account/region/id
	Settings  map[string]string    `json:"settings"`  // account/region/name → value
}

func newState() state {
	return state{Clusters: map[string]*Cluster{}, Defs: map[string]*TaskDef{}, Revisions: map[string]int{}, Services: map[string]*Svc{},
		Tasks: map[string]*Task{}, Providers: map[string]*Provider{}, Instances: map[string]*Instance{}, Settings: map[string]string{}}
}

// KV is a name/value pair: settings, statistics, environment variables, attachment details.
type KV struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Tag is a resource tag.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type failure struct {
	Arn    string `json:"arn,omitempty"`
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

// Strategy is a capacity provider strategy item.
type Strategy struct {
	CapacityProvider string `json:"capacityProvider"`
	Weight           int    `json:"weight,omitempty"`
	Base             int    `json:"base,omitempty"`
}

// Cluster is an ECS cluster; its counts are filled when it is described.
type Cluster struct {
	ClusterArn                        string          `json:"clusterArn"`
	ClusterName                       string          `json:"clusterName"`
	Status                            string          `json:"status"`
	Configuration                     json.RawMessage `json:"configuration,omitempty"`
	ServiceConnectDefaults            json.RawMessage `json:"serviceConnectDefaults,omitempty"`
	Settings                          []KV            `json:"settings,omitempty"`
	Statistics                        []KV            `json:"statistics,omitempty"`
	Tags                              []Tag           `json:"tags,omitempty"`
	CapacityProviders                 []string        `json:"capacityProviders"`
	DefaultCapacityProviderStrategy   []Strategy      `json:"defaultCapacityProviderStrategy"`
	RegisteredContainerInstancesCount int             `json:"registeredContainerInstancesCount"`
	RunningTasksCount                 int             `json:"runningTasksCount"`
	PendingTasksCount                 int             `json:"pendingTasksCount"`
	ActiveServicesCount               int             `json:"activeServicesCount"`
}

// TaskDef is one revision of a task definition family. Def is the wire
// object as registered, so every field round-trips, plus the computed ones.
type TaskDef struct {
	Arn      string         `json:"arn"`
	Family   string         `json:"family"`
	Revision int            `json:"revision"`
	Status   string         `json:"status"`
	Def      map[string]any `json:"def"`
	Tags     []Tag          `json:"tags,omitempty"`
}

// Provider is a capacity provider made with CreateCapacityProvider.
type Provider struct {
	CapacityProviderArn      string          `json:"capacityProviderArn"`
	Name                     string          `json:"name"`
	Status                   string          `json:"status"`
	AutoScalingGroupProvider json.RawMessage `json:"autoScalingGroupProvider,omitempty"`
	UpdateStatus             string          `json:"updateStatus,omitempty"`
	Tags                     []Tag           `json:"tags,omitempty"`
}

// Instance is a registered container instance (metadata only: tasks never run on it).
type Instance struct {
	ContainerInstanceArn string          `json:"containerInstanceArn"`
	Ec2InstanceId        string          `json:"ec2InstanceId"`
	Status               string          `json:"status"`
	AgentConnected       bool            `json:"agentConnected"`
	Version              int             `json:"version"`
	VersionInfo          json.RawMessage `json:"versionInfo,omitempty"`
	RegisteredResources  json.RawMessage `json:"registeredResources,omitempty"`
	RemainingResources   json.RawMessage `json:"remainingResources,omitempty"`
	Attributes           json.RawMessage `json:"attributes,omitempty"`
	RunningTasksCount    int             `json:"runningTasksCount"`
	PendingTasksCount    int             `json:"pendingTasksCount"`
	RegisteredAt         core.Time       `json:"registeredAt"`
	Tags                 []Tag           `json:"tags,omitempty"`
}

// New creates the ECS service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateCluster":               core.H(s.createCluster),
		"DescribeClusters":            core.H(s.describeClusters),
		"ListClusters":                core.H(s.listClusters),
		"DeleteCluster":               core.H(s.deleteCluster),
		"UpdateCluster":               core.H(s.updateCluster),
		"UpdateClusterSettings":       core.H(s.updateCluster),
		"PutClusterCapacityProviders": core.H(s.putClusterCapacityProviders),
		"CreateCapacityProvider":      core.H(s.createCapacityProvider),
		"DescribeCapacityProviders":   core.H(s.describeCapacityProviders),
		"DeleteCapacityProvider":      core.H(s.deleteCapacityProvider),
		"RegisterTaskDefinition":      core.H(s.registerTaskDefinition),
		"DescribeTaskDefinition":      core.H(s.describeTaskDefinition),
		"ListTaskDefinitions":         core.H(s.listTaskDefinitions),
		"ListTaskDefinitionFamilies":  core.H(s.listTaskDefinitionFamilies),
		"DeregisterTaskDefinition":    core.H(s.deregisterTaskDefinition),
		"DeleteTaskDefinitions":       core.H(s.deleteTaskDefinitions),
		"RunTask":                     core.H(s.runTask),
		"StartTask":                   core.H(s.runTask),
		"StopTask":                    core.H(s.stopTaskOp),
		"DescribeTasks":               core.H(s.describeTasks),
		"ListTasks":                   core.H(s.listTasks),
		"CreateService":               core.H(s.createService),
		"UpdateService":               core.H(s.updateService),
		"DeleteService":               core.H(s.deleteService),
		"DescribeServices":            core.H(s.describeServices),
		"ListServices":                core.H(s.listServices),
		"ListServicesByNamespace":     core.H(s.listServicesByNamespace),
		"ListServiceDeployments":      core.H(s.listServiceDeployments),
		"DescribeServiceDeployments":  core.H(s.describeServiceDeployments),
		"TagResource":                 core.H(s.tagResource),
		"UntagResource":               core.H(s.untagResource),
		"ListTagsForResource":         core.H(s.listTagsForResource),
		"PutAccountSetting":           core.H(s.putAccountSetting),
		"PutAccountSettingDefault":    core.H(s.putAccountSetting),
		"DeleteAccountSetting":        core.H(s.putAccountSetting),
		"ListAccountSettings":         core.H(s.listAccountSettings),
		"RegisterContainerInstance":   core.H(s.registerContainerInstance),
		"DeregisterContainerInstance": core.H(s.deregisterContainerInstance),
		"DescribeContainerInstances":  core.H(s.describeContainerInstances),
		"ListContainerInstances":      core.H(s.listContainerInstances),
		"UpdateServicePrimaryTaskSet": core.H(s.taskSets),
		"CreateTaskSet":               core.H(s.taskSets),
		"ExecuteCommand":              core.H(s.executeCommand),
	}}
	if hub != nil {
		hub.Mount("/_aws/ecs/", http.HandlerFunc(s.agent))
		s.stop = core.Every(time.Second, s.Tick)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "ecs", Cloud: core.AWS, Title: "ECS", Targets: []string{"AmazonEC2ContainerServiceV20141113"}, Signing: []string{"ecs"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// Tick keeps services at their desired count and forgets tasks stopped over an hour ago.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, t := range s.st.Tasks {
		if t.StoppedAt != nil && now.Sub(time.Time(*t.StoppedAt)) > time.Hour {
			delete(s.st.Tasks, k)
		}
	}
	for _, sv := range s.st.Services {
		s.reconcile(sv, now)
	}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.drop()
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state without restarting containers: tasks that were running
// are STOPPED ("pogi restarted"), and services start new ones.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	var gone []string
	for _, t := range st.Tasks {
		if t.LastStatus != "STOPPED" {
			gone = append(gone, t.containerNames()...)
			t.StopCode, t.StoppedReason = "TerminationNotice", "pogi restarted"
			t.halt()
		}
	}
	s.mu.Lock()
	s.drop()
	s.st = st
	s.mu.Unlock()
	core.Go("ecs.remove", func() { remove(s.hub, gone) }) // leftovers of the pogi that saved this state
	return nil
}

// Close stops the reconcile loop and removes every task container. Tasks
// keep their state, so a persisted pogi loads them as "pogi restarted".
func (s *Service) Close() error {
	if s.stop != nil {
		s.stop()
	}
	s.mu.Lock()
	s.closed = true
	var names []string
	for _, t := range s.st.Tasks {
		if t.run != nil && t.LastStatus != "STOPPED" {
			names = append(names, t.containerNames()...)
			t.run.halt()
		}
	}
	s.mu.Unlock()
	s.cancel()
	remove(s.hub, names)
	return nil
}

// drop asks the runner of every live task to stop; lock held.
func (s *Service) drop() {
	for _, t := range s.st.Tasks {
		s.stopTask(t, "TerminationNotice", "pogi reset")
	}
}

// remove deletes containers by name, when an engine runs.
func remove(hub *core.Hub, names []string) {
	if hub == nil || len(names) == 0 || !hub.Docker().Available() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Go(func() { hub.Docker().Remove(ctx, n) })
	}
	wg.Wait()
}

// ---- helpers ----

func key(parts ...string) string { return strings.Join(parts, "/") }

// scope is the key prefix of the caller's account and region.
func scope(c *core.Ctx) string { return c.Account + "/" + c.Region + "/" }

// last is the part of a name or ARN after its last '/'.
func last(ref string) string { return ref[strings.LastIndex(ref, "/")+1:] }

// where returns the account and region of an ARN.
func where(arn string) (account, region string) {
	_, region, account, _, _ = core.ParseARN(arn)
	return account, region
}

// snap encodes v under the lock, so responses never race the runners.
func snap(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// remarshal copies in into out through JSON.
func remarshal(in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

// page cuts sorted items after the token (the last item of the previous
// page) into {field: [...], nextToken}.
func page(field string, items []string, token string, max, def int) map[string]any {
	if i := slices.Index(items, token); token != "" && i >= 0 {
		items = items[i+1:]
	}
	if max <= 0 {
		max = def
	}
	out := map[string]any{field: []string{}}
	if len(items) > max {
		items, out["nextToken"] = items[:max], items[max-1]
	}
	if len(items) > 0 {
		out[field] = items
	}
	return out
}

// orEmpty is list, or [] where AWS answers with an empty list rather than nothing.
func orEmpty[T any](list []T) []T {
	if list == nil {
		return []T{}
	}
	return list
}

func clientErr(format string, a ...any) error {
	return core.Errorf(400, "ClientException", format, a...)
}
func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

var (
	errCluster = core.Errorf(400, "ClusterNotFoundException", "Cluster not found.")
	errService = core.Errorf(400, "ServiceNotFoundException", "Service not found.")
	errActive  = core.Errorf(400, "ServiceNotActiveException", "Service was not ACTIVE.")
	nameRe     = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,255}$`)
)

// setTags adds or replaces tags by key.
func setTags(list, add []Tag) []Tag {
	for _, t := range add {
		if i := slices.IndexFunc(list, func(x Tag) bool { return x.Key == t.Key }); i >= 0 {
			list[i].Value = t.Value
		} else {
			list = append(list, t)
		}
	}
	return list
}

// ---- clusters ----

type clusterInput struct {
	Cluster                         string
	ClusterName                     string
	Tags                            []Tag
	Settings                        []KV
	Configuration                   json.RawMessage
	ServiceConnectDefaults          json.RawMessage
	CapacityProviders               []string
	DefaultCapacityProviderStrategy []Strategy
}

func (s *Service) createCluster(c *core.Ctx, in *clusterInput) (any, error) {
	name := cmp.Or(in.ClusterName, "default")
	if !nameRe.MatchString(name) {
		return nil, invalid("Cluster name can only contain letters, numbers, hyphens and underscores, and must be at most 255 characters.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.providersExist(c, in.CapacityProviders); err != nil {
		return nil, err
	}
	k := key(c.Account, c.Region, name)
	cl := s.st.Clusters[k]
	if cl == nil { // CreateCluster of an existing name returns it
		cl = &Cluster{ClusterArn: c.ARN("ecs", "cluster/"+name), ClusterName: name, Status: "ACTIVE", Configuration: in.Configuration,
			ServiceConnectDefaults: in.ServiceConnectDefaults, Settings: in.Settings, Tags: in.Tags,
			CapacityProviders: orEmpty(in.CapacityProviders), DefaultCapacityProviderStrategy: orEmpty(in.DefaultCapacityProviderStrategy)}
		if len(cl.Settings) == 0 {
			cl.Settings = []KV{{"containerInsights", s.setting(c, "containerInsights")}}
		}
		s.st.Clusters[k] = cl
	}
	return snap(map[string]any{"cluster": s.clusterView(cl, allIncludes)}), nil
}

var allIncludes = []string{"ATTACHMENTS", "CONFIGURATIONS", "SETTINGS", "STATISTICS", "TAGS"}

// cluster finds a cluster by name or ARN ("" is default); lock held.
func (s *Service) cluster(c *core.Ctx, ref string) (*Cluster, error) {
	if cl := s.st.Clusters[key(c.Account, c.Region, cmp.Or(last(ref), "default"))]; cl != nil {
		return cl, nil
	}
	return nil, errCluster
}

// clusterView is cl with its counts and the parts include asks for; lock held.
func (s *Service) clusterView(cl *Cluster, include []string) Cluster {
	v := *cl
	n := map[string]int{}
	for _, t := range s.st.Tasks {
		if t.ClusterArn == cl.ClusterArn && t.LastStatus != "STOPPED" {
			state := map[bool]string{true: "running", false: "pending"}[t.LastStatus == "RUNNING"]
			n[state]++
			n[state+t.LaunchType]++
		}
	}
	for _, sv := range s.st.Services {
		if sv.ClusterArn == cl.ClusterArn && sv.Status != "INACTIVE" {
			state := strings.ToLower(sv.Status)
			n[state]++
			n[state+cmp.Or(sv.LaunchType, "EC2")]++
		}
	}
	for _, ci := range s.st.Instances {
		if ci.in(cl) && ci.Status == "ACTIVE" {
			n["instances"]++
		}
	}
	v.RunningTasksCount, v.PendingTasksCount, v.ActiveServicesCount, v.RegisteredContainerInstancesCount = n["running"], n["pending"], n["active"], n["instances"]
	v.Statistics = nil
	if slices.Contains(include, "STATISTICS") {
		for _, st := range []string{"running", "pending", "active", "draining"} {
			noun := map[bool]string{true: "TasksCount", false: "ServiceCount"}[st == "running" || st == "pending"]
			for _, lt := range []string{"EC2", "Fargate"} {
				v.Statistics = append(v.Statistics, KV{st + lt + noun, strconv.Itoa(n[st+strings.ToUpper(lt)])})
			}
		}
	}
	if !slices.Contains(include, "SETTINGS") {
		v.Settings = nil
	}
	if !slices.Contains(include, "CONFIGURATIONS") {
		v.Configuration = nil
	}
	if !slices.Contains(include, "TAGS") {
		v.Tags = nil
	}
	return v
}

func (s *Service) describeClusters(c *core.Ctx, in *struct {
	Clusters []string
	Include  []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := struct {
		Clusters []Cluster `json:"clusters"`
		Failures []failure `json:"failures"`
	}{[]Cluster{}, []failure{}}
	refs := in.Clusters
	if len(refs) == 0 {
		refs = []string{"default"}
	}
	for _, ref := range refs {
		if cl, err := s.cluster(c, ref); err == nil {
			out.Clusters = append(out.Clusters, s.clusterView(cl, in.Include))
		} else {
			out.Failures = append(out.Failures, failure{Arn: c.ARN("ecs", "cluster/"+last(ref)), Reason: "MISSING"})
		}
	}
	return snap(out), nil
}

func (s *Service) listClusters(c *core.Ctx, in *struct {
	NextToken  string
	MaxResults int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var arns []string
	for k, cl := range s.st.Clusters {
		if strings.HasPrefix(k, scope(c)) {
			arns = append(arns, cl.ClusterArn)
		}
	}
	slices.Sort(arns)
	return page("clusterArns", arns, in.NextToken, in.MaxResults, 100), nil
}

func (s *Service) deleteCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	for _, sv := range s.st.Services {
		if sv.ClusterArn == cl.ClusterArn && sv.Status != "INACTIVE" {
			return nil, core.Errorf(400, "ClusterContainsServicesException", "The Cluster cannot be deleted while Services are active.")
		}
	}
	v := s.clusterView(cl, allIncludes)
	switch {
	case v.RunningTasksCount+v.PendingTasksCount > 0:
		return nil, core.Errorf(400, "ClusterContainsTasksException", "The Cluster cannot be deleted while Tasks are active.")
	case v.RegisteredContainerInstancesCount > 0:
		return nil, core.Errorf(400, "ClusterContainsContainerInstancesException", "The Cluster cannot be deleted while Container Instances are active or draining.")
	}
	delete(s.st.Clusters, key(c.Account, c.Region, cl.ClusterName))
	for k, sv := range s.st.Services {
		if sv.ClusterArn == cl.ClusterArn {
			delete(s.st.Services, k)
		}
	}
	for k, t := range s.st.Tasks {
		if t.ClusterArn == cl.ClusterArn {
			delete(s.st.Tasks, k)
		}
	}
	v.Status = "INACTIVE"
	return snap(map[string]any{"cluster": v}), nil
}

// updateCluster serves UpdateCluster and UpdateClusterSettings.
func (s *Service) updateCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	for _, kv := range in.Settings {
		if i := slices.IndexFunc(cl.Settings, func(x KV) bool { return x.Name == kv.Name }); i >= 0 {
			cl.Settings[i].Value = kv.Value
		} else {
			cl.Settings = append(cl.Settings, kv)
		}
	}
	if in.Configuration != nil {
		cl.Configuration = in.Configuration
	}
	if in.ServiceConnectDefaults != nil {
		cl.ServiceConnectDefaults = in.ServiceConnectDefaults
	}
	return snap(map[string]any{"cluster": s.clusterView(cl, allIncludes)}), nil
}

func (s *Service) putClusterCapacityProviders(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	if err := s.providersExist(c, in.CapacityProviders); err != nil {
		return nil, err
	}
	for _, st := range in.DefaultCapacityProviderStrategy {
		if !slices.Contains(in.CapacityProviders, st.CapacityProvider) {
			return nil, invalid("The specified capacity provider strategy cannot contain a capacity provider that is not associated with the cluster. Associate the capacity provider with the cluster or specify a valid capacity provider and try again.")
		}
	}
	cl.CapacityProviders, cl.DefaultCapacityProviderStrategy = orEmpty(in.CapacityProviders), orEmpty(in.DefaultCapacityProviderStrategy)
	return snap(map[string]any{"cluster": s.clusterView(cl, allIncludes)}), nil
}

// ---- capacity providers ----

// fargate is whether name is one of the providers every account has.
func fargate(name string) bool { return name == "FARGATE" || name == "FARGATE_SPOT" }

// providersExist checks capacity provider names; lock held.
func (s *Service) providersExist(c *core.Ctx, names []string) error {
	for _, n := range names {
		if !fargate(n) && s.st.Providers[key(c.Account, c.Region, last(n))] == nil {
			return invalid("The specified capacity provider '%s' is not in an ACTIVE state. Specify a valid capacity provider and try again.", n)
		}
	}
	return nil
}

func (s *Service) createCapacityProvider(c *core.Ctx, in *struct {
	Name                     string
	AutoScalingGroupProvider json.RawMessage
	Tags                     []Tag
}) (any, error) {
	if l := strings.ToLower(in.Name); !nameRe.MatchString(in.Name) || strings.HasPrefix(l, "aws") || strings.HasPrefix(l, "ecs") || strings.HasPrefix(l, "fargate") {
		return nil, invalid("The specified capacity provider name is invalid. Up to 255 letters, numbers, underscores and hyphens are allowed, and the name cannot begin with 'aws', 'ecs' or 'fargate'.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	if s.st.Providers[k] != nil {
		return nil, invalid("The specified capacity provider already exists. To change the configuration of an existing capacity provider, update the capacity provider.")
	}
	p := &Provider{CapacityProviderArn: c.ARN("ecs", "capacity-provider/"+in.Name), Name: in.Name, Status: "ACTIVE",
		AutoScalingGroupProvider: in.AutoScalingGroupProvider, Tags: in.Tags}
	s.st.Providers[k] = p
	return snap(map[string]any{"capacityProvider": p}), nil
}

func (s *Service) describeCapacityProviders(c *core.Ctx, in *struct {
	CapacityProviders []string
	Include           []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := map[string]Provider{}
	for _, n := range []string{"FARGATE", "FARGATE_SPOT"} {
		all[n] = Provider{CapacityProviderArn: c.ARN("ecs", "capacity-provider/"+n), Name: n, Status: "ACTIVE"}
	}
	for k, p := range s.st.Providers {
		if strings.HasPrefix(k, scope(c)) {
			all[p.Name] = *p
		}
	}
	names := in.CapacityProviders
	if len(names) == 0 {
		for n := range all {
			names = append(names, n)
		}
		slices.Sort(names)
	}
	out := struct {
		CapacityProviders []Provider `json:"capacityProviders"`
		Failures          []failure  `json:"failures"`
	}{[]Provider{}, []failure{}}
	for _, n := range names {
		p, ok := all[last(n)]
		if !ok {
			out.Failures = append(out.Failures, failure{Arn: n, Reason: "MISSING"})
			continue
		}
		if !slices.Contains(in.Include, "TAGS") {
			p.Tags = nil
		}
		out.CapacityProviders = append(out.CapacityProviders, p)
	}
	return snap(out), nil
}

func (s *Service) deleteCapacityProvider(c *core.Ctx, in *struct{ CapacityProvider string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, last(in.CapacityProvider))
	p := s.st.Providers[k]
	if p == nil {
		return nil, invalid("The specified capacity provider does not exist. Specify a valid name or ARN and try again.")
	}
	for _, cl := range s.st.Clusters {
		if slices.Contains(cl.CapacityProviders, p.Name) {
			return nil, core.Errorf(400, "ResourceInUseException", "The specified capacity provider is in use and cannot be removed.")
		}
	}
	delete(s.st.Providers, k)
	v := *p
	v.Status, v.UpdateStatus = "INACTIVE", "DELETE_COMPLETE"
	return snap(map[string]any{"capacityProvider": v}), nil
}

// ---- task definitions ----

// containerDef is the part of a container definition pogi runs.
type containerDef struct {
	Name              string        `json:"name"`
	Image             string        `json:"image"`
	Command           []string      `json:"command"`
	EntryPoint        []string      `json:"entryPoint"`
	Environment       []KV          `json:"environment"`
	WorkingDirectory  string        `json:"workingDirectory"`
	Essential         *bool         `json:"essential"`
	Memory            int           `json:"memory"`
	MemoryReservation int           `json:"memoryReservation"`
	Cpu               int           `json:"cpu"`
	PortMappings      []portMapping `json:"portMappings"`
	DependsOn         []dependency  `json:"dependsOn"`
	MountPoints       []mountPoint  `json:"mountPoints"`
	VolumesFrom       []volumeFrom  `json:"volumesFrom"`
	Secrets           []struct {
		Name      string `json:"name"`
		ValueFrom string `json:"valueFrom"`
	} `json:"secrets"`
	HealthCheck *struct {
		Command     []string `json:"command"`
		Interval    int      `json:"interval"`
		Timeout     int      `json:"timeout"`
		Retries     int      `json:"retries"`
		StartPeriod int      `json:"startPeriod"`
	} `json:"healthCheck"`
	LogConfiguration *struct {
		LogDriver string            `json:"logDriver"`
		Options   map[string]string `json:"options"`
	} `json:"logConfiguration"`
}

type volumeFrom struct {
	SourceContainer string `json:"sourceContainer"`
	ReadOnly        bool   `json:"readOnly"`
}

type mountPoint struct {
	SourceVolume  string `json:"sourceVolume"`
	ContainerPath string `json:"containerPath"`
	ReadOnly      bool   `json:"readOnly"`
}

type portMapping struct {
	ContainerPort int    `json:"containerPort"`
	HostPort      int    `json:"hostPort"`
	Protocol      string `json:"protocol"`
}

type dependency struct {
	ContainerName string `json:"containerName"`
	Condition     string `json:"condition"` // START, COMPLETE, SUCCESS or HEALTHY
}

func (cd *containerDef) essential() bool { return cd.Essential == nil || *cd.Essential }

// defSpec is the part of a task definition pogi validates and runs.
type defSpec struct {
	Family                  string         `json:"family"`
	NetworkMode             string         `json:"networkMode"`
	TaskRoleArn             string         `json:"taskRoleArn"`
	ExecutionRoleArn        string         `json:"executionRoleArn"`
	Cpu                     string         `json:"cpu"`
	Memory                  string         `json:"memory"`
	RequiresCompatibilities []string       `json:"requiresCompatibilities"`
	ContainerDefinitions    []containerDef `json:"containerDefinitions"`
	Volumes                 []struct {
		Name string `json:"name"`
		Host struct {
			SourcePath string `json:"sourcePath"`
		} `json:"host"`
	} `json:"volumes"`
}

func (d *defSpec) validate() error {
	if !nameRe.MatchString(d.Family) {
		return clientErr("Family must be 1 to 255 letters, numbers, hyphens and underscores.")
	}
	if len(d.ContainerDefinitions) == 0 {
		return clientErr("Container list cannot be empty.")
	}
	onFargate := slices.Contains(d.RequiresCompatibilities, "FARGATE")
	if onFargate {
		switch cpu, mem := units(d.Cpu), units(d.Memory); {
		case d.NetworkMode != "awsvpc":
			return clientErr("Fargate only supports network mode ‘awsvpc’.")
		case d.Cpu == "":
			return clientErr("Fargate requires that 'cpu' be defined at the task level.")
		case d.Memory == "":
			return clientErr("Fargate requires that 'memory' be defined at the task level.")
		case !fargateSize(cpu, mem):
			return clientErr("No Fargate configuration exists for given values: %s CPU, %s memory. See the Amazon ECS documentation for the valid values.", d.Cpu, d.Memory)
		}
	}
	names, essential := map[string]bool{}, false
	for _, cd := range d.ContainerDefinitions {
		switch {
		case cd.Name == "":
			return clientErr("Container.name should not be null or empty.")
		case !nameRe.MatchString(cd.Name):
			return clientErr("Container.name must be 1 to 255 letters, numbers, hyphens and underscores.")
		case names[cd.Name]:
			return clientErr("Container names must be unique: %s is used twice.", cd.Name)
		case cd.Image == "":
			return clientErr("Container.image should not be null or empty.")
		case d.Memory == "" && cd.Memory == 0 && cd.MemoryReservation == 0:
			return clientErr("Invalid setting for container '%s'. At least one of 'memory' or 'memoryReservation' must be specified.", cd.Name)
		case onFargate && cd.LogConfiguration != nil && cd.LogConfiguration.LogDriver == "awslogs" && d.ExecutionRoleArn == "":
			return clientErr("Fargate requires task definition to have execution role ARN to support log driver awslogs.")
		}
		names[cd.Name], essential = true, essential || cd.essential()
	}
	for _, cd := range d.ContainerDefinitions {
		for _, dep := range cd.DependsOn {
			if !names[dep.ContainerName] || dep.ContainerName == cd.Name {
				return clientErr("Container %s depends on %s, which is not another container of the task definition.", cd.Name, dep.ContainerName)
			}
		}
	}
	if !essential {
		return clientErr("Task definition must contain at least one essential container.")
	}
	return nil
}

// units parses task cpu ("256", "1 vCPU") or memory ("512", "2 GB") into CPU units or MiB.
func units(v string) int {
	v = strings.ToLower(strings.TrimSpace(v))
	for _, u := range []string{"vcpu", "gb"} {
		if n, ok := strings.CutSuffix(v, u); ok {
			f, _ := strconv.ParseFloat(strings.TrimSpace(n), 64)
			return int(f * 1024)
		}
	}
	n, _ := strconv.Atoi(v)
	return n
}

// fargateSize reports whether cpu units and memory MiB are a Fargate task size.
func fargateSize(cpu, mem int) bool {
	gb, whole := mem/1024, mem%1024 == 0
	switch cpu {
	case 256:
		return mem == 512 || mem == 1024 || mem == 2048
	case 512:
		return whole && gb >= 1 && gb <= 4
	case 1024:
		return whole && gb >= 2 && gb <= 8
	case 2048:
		return whole && gb >= 4 && gb <= 16
	case 4096:
		return whole && gb >= 8 && gb <= 30
	case 8192:
		return gb%4 == 0 && whole && gb >= 16 && gb <= 60
	case 16384:
		return gb%8 == 0 && whole && gb >= 32 && gb <= 120
	}
	return false
}

func (s *Service) registerTaskDefinition(c *core.Ctx, in *map[string]any) (any, error) {
	def := *in
	var sp defSpec
	if err := remarshal(def, &sp); err != nil {
		return nil, clientErr("%v", err)
	}
	if err := sp.validate(); err != nil {
		return nil, err
	}
	var tags []Tag
	remarshal(def["tags"], &tags)
	delete(def, "tags")
	s.mu.Lock()
	defer s.mu.Unlock()
	fk := key(c.Account, c.Region, sp.Family)
	s.st.Revisions[fk]++
	rev := s.st.Revisions[fk]
	compat := []string{"EC2"}
	if sp.NetworkMode == "awsvpc" && sp.Cpu != "" && sp.Memory != "" {
		compat = append(compat, "FARGATE")
	}
	for _, r := range sp.RequiresCompatibilities {
		if !slices.Contains(compat, r) {
			compat = append(compat, r)
		}
	}
	d := &TaskDef{Arn: c.ARN("ecs", "task-definition/"+sp.Family+":"+strconv.Itoa(rev)), Family: sp.Family, Revision: rev, Status: "ACTIVE", Def: def, Tags: tags}
	def["taskDefinitionArn"], def["revision"], def["status"], def["compatibilities"] = d.Arn, rev, "ACTIVE", compat
	def["registeredAt"], def["registeredBy"] = core.Time(time.Now()), c.Principal
	s.st.Defs[key(fk+":"+strconv.Itoa(rev))] = d
	return snap(map[string]any{"taskDefinition": def, "tags": orEmpty(tags)}), nil
}

// taskDef finds a revision by family:revision or ARN, or a family's latest
// ACTIVE revision; lock held.
func (s *Service) taskDef(c *core.Ctx, ref string) *TaskDef {
	if _, _, _, res, ok := core.ParseARN(ref); ok {
		ref = strings.TrimPrefix(res, "task-definition/")
	}
	if strings.Contains(ref, ":") {
		return s.st.Defs[key(c.Account, c.Region, ref)]
	}
	for rev := s.st.Revisions[key(c.Account, c.Region, ref)]; rev > 0; rev-- {
		if d := s.st.Defs[key(c.Account, c.Region, ref+":"+strconv.Itoa(rev))]; d != nil && d.Status == "ACTIVE" {
			return d
		}
	}
	return nil
}

func (s *Service) describeTaskDefinition(c *core.Ctx, in *struct {
	TaskDefinition string
	Include        []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.taskDef(c, in.TaskDefinition)
	if d == nil {
		return nil, clientErr("Unable to describe task definition.")
	}
	out := map[string]any{"taskDefinition": d.Def}
	if slices.Contains(in.Include, "TAGS") {
		out["tags"] = orEmpty(d.Tags)
	}
	return snap(out), nil
}

func (s *Service) listTaskDefinitions(c *core.Ctx, in *struct {
	FamilyPrefix, Status, Sort, NextToken string
	MaxResults                            int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var defs []*TaskDef
	for k, d := range s.st.Defs {
		// familyPrefix is the full family name, despite its name
		if strings.HasPrefix(k, scope(c)) && d.Status == cmp.Or(in.Status, "ACTIVE") && (in.FamilyPrefix == "" || d.Family == in.FamilyPrefix) {
			defs = append(defs, d)
		}
	}
	slices.SortFunc(defs, func(a, b *TaskDef) int { return cmp.Or(strings.Compare(a.Family, b.Family), a.Revision-b.Revision) })
	if in.Sort == "DESC" {
		slices.Reverse(defs)
	}
	var arns []string
	for _, d := range defs {
		arns = append(arns, d.Arn)
	}
	return page("taskDefinitionArns", arns, in.NextToken, in.MaxResults, 100), nil
}

func (s *Service) listTaskDefinitionFamilies(c *core.Ctx, in *struct {
	FamilyPrefix, Status, NextToken string
	MaxResults                      int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := map[string]bool{}
	for k, d := range s.st.Defs {
		if strings.HasPrefix(k, scope(c)) && strings.HasPrefix(d.Family, in.FamilyPrefix) {
			active[d.Family] = active[d.Family] || d.Status == "ACTIVE"
		}
	}
	var fams []string
	for f, a := range active {
		if in.Status == "" || in.Status == "ALL" || a == (in.Status == "ACTIVE") {
			fams = append(fams, f)
		}
	}
	slices.Sort(fams)
	return page("families", fams, in.NextToken, in.MaxResults, 100), nil
}

func (s *Service) deregisterTaskDefinition(c *core.Ctx, in *struct{ TaskDefinition string }) (any, error) {
	ref := in.TaskDefinition
	if _, _, _, res, ok := core.ParseARN(ref); ok {
		ref = res
	}
	if _, rev, _ := strings.Cut(ref, ":"); rev == "" {
		return nil, clientErr("Invalid revision number. Number: %s", rev)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.taskDef(c, in.TaskDefinition)
	if d == nil {
		return nil, clientErr("The specified task definition does not exist.")
	}
	if d.Status == "ACTIVE" {
		d.Status, d.Def["status"], d.Def["deregisteredAt"] = "INACTIVE", "INACTIVE", core.Time(time.Now())
	}
	return snap(map[string]any{"taskDefinition": d.Def}), nil
}

func (s *Service) deleteTaskDefinitions(c *core.Ctx, in *struct{ TaskDefinitions []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := struct {
		TaskDefinitions []map[string]any `json:"taskDefinitions"`
		Failures        []failure        `json:"failures"`
	}{[]map[string]any{}, []failure{}}
	for _, ref := range in.TaskDefinitions {
		d := s.taskDef(c, ref)
		switch {
		case d == nil || !strings.Contains(last(ref), ":"):
			out.Failures = append(out.Failures, failure{Arn: ref, Reason: "The specified task definition does not exist."})
		case d.Status == "ACTIVE":
			out.Failures = append(out.Failures, failure{Arn: d.Arn, Reason: "The specified task definition is still in ACTIVE status. Please deregister the target and try again."})
		default:
			delete(s.st.Defs, key(c.Account, c.Region, d.Family+":"+strconv.Itoa(d.Revision)))
			d.Status, d.Def["status"], d.Def["deleteRequestedAt"] = "DELETE_IN_PROGRESS", "DELETE_IN_PROGRESS", core.Time(time.Now())
			out.TaskDefinitions = append(out.TaskDefinitions, d.Def)
		}
	}
	return snap(out), nil
}

// ---- tags ----

// tags finds the tags of any ECS resource by ARN; lock held.
func (s *Service) tags(arn string) (*[]Tag, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	kind, path, _ := strings.Cut(res, "/")
	if ok {
		switch p := strings.Split(path, "/"); kind {
		case "cluster":
			if cl := s.st.Clusters[key(account, region, path)]; cl != nil {
				return &cl.Tags, nil
			}
		case "service":
			if len(p) == 1 { // the short ARN of the default cluster
				p = []string{"default", p[0]}
			}
			if sv := s.st.Services[key(account, region, p[0], p[len(p)-1])]; sv != nil {
				return &sv.Tags, nil
			}
		case "task":
			if t := s.st.Tasks[key(account, region, p[len(p)-1])]; t != nil {
				return &t.Tags, nil
			}
		case "task-definition":
			if d := s.st.Defs[key(account, region, path)]; d != nil {
				return &d.Tags, nil
			}
		case "capacity-provider":
			if pr := s.st.Providers[key(account, region, path)]; pr != nil {
				return &pr.Tags, nil
			}
		case "container-instance":
			if in := s.st.Instances[key(account, region, p[len(p)-1])]; in != nil {
				return &in.Tags, nil
			}
		}
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "The specified resource wasn't found.")
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	*tags = setTags(*tags, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	*tags = slices.DeleteFunc(*tags, func(t Tag) bool { return slices.Contains(in.TagKeys, t.Key) })
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return snap(map[string]any{"tags": orEmpty(*tags)}), nil
}

// ---- account settings ----

var settingDefaults = map[string]string{"serviceLongArnFormat": "enabled", "taskLongArnFormat": "enabled",
	"containerInstanceLongArnFormat": "enabled", "awsvpcTrunking": "disabled", "containerInsights": "disabled",
	"dualStackIPv6": "disabled", "fargateFIPSMode": "disabled", "tagResourceAuthorization": "on",
	"fargateTaskRetirementWaitPeriod": "7", "guardDutyActivate": "off", "defaultLogDriverMode": "non-blocking"}

// setting is an account setting's value; lock held.
func (s *Service) setting(c *core.Ctx, name string) string {
	if v, ok := s.st.Settings[key(c.Account, c.Region, name)]; ok {
		return v
	}
	return settingDefaults[name]
}

// putAccountSetting serves PutAccountSetting, PutAccountSettingDefault and
// DeleteAccountSetting. ponytail: settings are per account and region, not
// per IAM principal.
func (s *Service) putAccountSetting(c *core.Ctx, in *struct{ Name, Value, PrincipalArn string }) (any, error) {
	if _, ok := settingDefaults[in.Name]; !ok {
		return nil, invalid("Invalid setting name: %s", in.Name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	if c.Op == "DeleteAccountSetting" {
		delete(s.st.Settings, k)
	} else {
		s.st.Settings[k] = in.Value
	}
	return map[string]any{"setting": map[string]string{"name": in.Name, "value": s.setting(c, in.Name),
		"principalArn": cmp.Or(in.PrincipalArn, c.Principal), "type": "user"}}, nil
}

func (s *Service) listAccountSettings(c *core.Ctx, in *struct {
	Name, Value, PrincipalArn, NextToken string
	EffectiveSettings                    bool
	MaxResults                           int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]string{}
	for _, n := range slices.Sorted(maps.Keys(settingDefaults)) {
		_, set := s.st.Settings[key(c.Account, c.Region, n)]
		v := s.setting(c, n)
		if (in.Name == "" || in.Name == n) && (in.Value == "" || in.Value == v) && (set || in.EffectiveSettings) {
			out = append(out, map[string]string{"name": n, "value": v, "principalArn": cmp.Or(in.PrincipalArn, c.Principal),
				"type": map[bool]string{true: "user", false: "aws_managed"}[set]})
		}
	}
	return map[string]any{"settings": out}, nil
}

// ---- container instances ----

func (s *Service) registerContainerInstance(c *core.Ctx, in *struct {
	Cluster                  string
	InstanceIdentityDocument string
	TotalResources           json.RawMessage
	VersionInfo              json.RawMessage
	Attributes               json.RawMessage
	Tags                     []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	var doc struct{ InstanceId string }
	json.Unmarshal([]byte(in.InstanceIdentityDocument), &doc)
	id := core.NewID()
	ci := &Instance{ContainerInstanceArn: c.ARN("ecs", "container-instance/"+cl.ClusterName+"/"+id), Ec2InstanceId: cmp.Or(doc.InstanceId, "i-"+core.RandHex(9)[:17]),
		Status: "ACTIVE", AgentConnected: true, Version: 1, VersionInfo: in.VersionInfo, RegisteredResources: in.TotalResources,
		RemainingResources: in.TotalResources, Attributes: in.Attributes, RegisteredAt: core.Time(time.Now()), Tags: in.Tags}
	s.st.Instances[key(c.Account, c.Region, id)] = ci
	return snap(map[string]any{"containerInstance": ci}), nil
}

// instance finds a container instance of cl by id or ARN; lock held.
func (s *Service) instance(c *core.Ctx, cl *Cluster, ref string) *Instance {
	if ci := s.st.Instances[key(c.Account, c.Region, last(ref))]; ci != nil && ci.in(cl) {
		return ci
	}
	return nil
}

// in reports whether the container instance belongs to cl.
func (ci *Instance) in(cl *Cluster) bool {
	return strings.HasPrefix(ci.ContainerInstanceArn, strings.Replace(cl.ClusterArn, ":cluster/", ":container-instance/", 1)+"/")
}

func (s *Service) deregisterContainerInstance(c *core.Ctx, in *struct {
	Cluster, ContainerInstance string
	Force                      bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	ci := s.instance(c, cl, in.ContainerInstance)
	if ci == nil {
		return nil, invalid("The referenced container instance %s was not found.", in.ContainerInstance)
	}
	delete(s.st.Instances, key(c.Account, c.Region, last(ci.ContainerInstanceArn)))
	v := *ci
	v.Status = "INACTIVE"
	return snap(map[string]any{"containerInstance": v}), nil
}

func (s *Service) describeContainerInstances(c *core.Ctx, in *struct {
	Cluster            string
	ContainerInstances []string
	Include            []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	out := struct {
		ContainerInstances []Instance `json:"containerInstances"`
		Failures           []failure  `json:"failures"`
	}{[]Instance{}, []failure{}}
	for _, ref := range in.ContainerInstances {
		ci := s.instance(c, cl, ref)
		if ci == nil {
			out.Failures = append(out.Failures, failure{Arn: ref, Reason: "MISSING"})
			continue
		}
		v := *ci
		for _, t := range s.st.Tasks {
			if t.ContainerInstanceArn == ci.ContainerInstanceArn && t.LastStatus != "STOPPED" {
				if t.LastStatus == "RUNNING" {
					v.RunningTasksCount++
				} else {
					v.PendingTasksCount++
				}
			}
		}
		if !slices.Contains(in.Include, "TAGS") {
			v.Tags = nil
		}
		out.ContainerInstances = append(out.ContainerInstances, v)
	}
	return snap(out), nil
}

func (s *Service) listContainerInstances(c *core.Ctx, in *struct {
	Cluster, Status, Filter, NextToken string
	MaxResults                         int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.Cluster)
	if err != nil {
		return nil, err
	}
	var arns []string
	for k, ci := range s.st.Instances {
		if strings.HasPrefix(k, scope(c)) && ci.in(cl) && (in.Status == "" || ci.Status == in.Status) {
			arns = append(arns, ci.ContainerInstanceArn)
		}
	}
	slices.Sort(arns)
	return page("containerInstanceArns", arns, in.NextToken, in.MaxResults, 100), nil
}

// ---- unsupported ----

func (s *Service) taskSets(c *core.Ctx, _ *struct{}) (any, error) {
	return nil, invalid("pogi does not support task sets (EXTERNAL and CODE_DEPLOY task set operations); services deploy with the ECS rolling controller.")
}

func (s *Service) executeCommand(c *core.Ctx, _ *struct{}) (any, error) {
	return nil, clientErr("pogi does not support ECS Exec. Run `docker exec -it <container> sh` instead: task containers are named pogi-ecs-<task id>-<container name>.")
}

// ---- console ----

// Resources lists clusters, services, tasks and task definitions.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	cls := core.Table{Name: "Clusters", Columns: []string{"Cluster", "Region", "Account", "Services", "Running", "Pending"}}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		cl := s.clusterView(s.st.Clusters[k], nil)
		acct, region := where(cl.ClusterArn)
		cls.Rows = append(cls.Rows, []string{cl.ClusterName, region, acct, strconv.Itoa(cl.ActiveServicesCount),
			strconv.Itoa(cl.RunningTasksCount), strconv.Itoa(cl.PendingTasksCount)})
	}
	svcs := core.Table{Name: "Services", Columns: []string{"Service", "Cluster", "Status", "Desired", "Running", "Pending", "Task definition"}}
	for _, k := range core.SortedKeys(s.st.Services) {
		sv := s.st.Services[k]
		svcs.Rows = append(svcs.Rows, []string{sv.ServiceName, last(sv.ClusterArn), sv.Status, strconv.Itoa(sv.DesiredCount),
			strconv.Itoa(sv.RunningCount), strconv.Itoa(sv.PendingCount), last(sv.TaskDefinition)})
	}
	tasks := core.Table{Name: "Tasks", Columns: []string{"Task", "Cluster", "Task definition", "Status", "Desired", "Group", "Ports"}}
	for _, k := range core.SortedKeys(s.st.Tasks) {
		t := s.st.Tasks[k]
		var ports []string
		for _, c := range t.Containers {
			for _, b := range c.NetworkBindings {
				ports = append(ports, b.BindIP+":"+strconv.Itoa(b.HostPort)+"→"+strconv.Itoa(b.ContainerPort))
			}
		}
		tasks.Rows = append(tasks.Rows, []string{last(t.TaskArn), last(t.ClusterArn), last(t.TaskDefinitionArn), t.LastStatus, t.DesiredStatus,
			t.Group, strings.Join(ports, " ")})
	}
	defs := core.Table{Name: "Task definitions", Columns: []string{"Task definition", "Status", "Network mode", "Containers"}}
	for _, k := range core.SortedKeys(s.st.Defs) {
		d := s.st.Defs[k]
		var sp defSpec
		remarshal(d.Def, &sp)
		var names []string
		for _, cd := range sp.ContainerDefinitions {
			names = append(names, cd.Name)
		}
		defs.Rows = append(defs.Rows, []string{d.Family + ":" + strconv.Itoa(d.Revision), d.Status, cmp.Or(sp.NetworkMode, "bridge"), strings.Join(names, ", ")})
	}
	return []core.Table{cls, svcs, tasks, defs}
}

// Inspect returns the API view of a cluster, service, task or task definition.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Clusters":
		for _, cl := range s.st.Clusters {
			if cl.ClusterName == id {
				return snap(s.clusterView(cl, allIncludes))
			}
		}
	case "Services":
		for _, sv := range s.st.Services {
			if sv.ServiceName == id {
				return snap(sv)
			}
		}
	case "Tasks":
		for k, t := range s.st.Tasks {
			if last(k) == id {
				return snap(t)
			}
		}
	case "Task definitions":
		for _, d := range s.st.Defs {
			if d.Family+":"+strconv.Itoa(d.Revision) == id {
				return snap(map[string]any{"taskDefinition": d.Def, "tags": d.Tags})
			}
		}
	}
	return nil
}

// containerNames are the engine names of a task's containers, pause first.
func (t *Task) containerNames() []string {
	id := last(t.TaskArn)
	out := []string{docker.Name("ecs", id)}
	for _, c := range t.Containers {
		out = append(out, docker.Name("ecs", id+"-"+c.Name))
	}
	return out
}
