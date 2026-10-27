// Package batch emulates AWS Batch over REST-JSON: compute environments, job
// queues, scheduling policies, job definitions and jobs. With a container
// engine every job attempt runs as a container (array jobs, dependencies,
// retries and timeouts included) and logs to CloudWatch Logs; without one,
// jobs wait in RUNNABLE.
package batch

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// NoDocker is the status reason of jobs that wait for a container engine.
const NoDocker = "pogi: Docker is not available, so jobs cannot run"

var (
	errTimeout = errors.New("Job attempt duration exceeded timeout")
	errStopped = errors.New("pogi stopped while the job ran")
)

// Service is the Batch emulator.
type Service struct {
	hub  *core.Hub
	ops  map[string]core.Handler
	stop func()
	wg   sync.WaitGroup // attempts running in containers

	mu     sync.Mutex
	st     state
	runs   map[string]context.CancelCauseFunc // job key → its running attempt
	closed bool
}

type state struct {
	Envs     map[string]*Env    `json:"computeEnvironments"` // account/region/name
	Queues   map[string]*Queue  `json:"jobQueues"`           // account/region/name
	Policies map[string]*Policy `json:"schedulingPolicies"`  // account/region/name
	Defs     map[string]*Def    `json:"jobDefinitions"`      // account/region/name:revision
	Jobs     map[string]*Job    `json:"jobs"`                // account/region/id
}

func newState() state {
	return state{Envs: map[string]*Env{}, Queues: map[string]*Queue{}, Policies: map[string]*Policy{}, Defs: map[string]*Def{}, Jobs: map[string]*Job{}}
}

// Env is a compute environment; its compute resources are kept as given.
type Env struct {
	Name          string            `json:"computeEnvironmentName"`
	Arn           string            `json:"computeEnvironmentArn"`
	Type          string            `json:"type"`
	State         string            `json:"state"`
	Status        string            `json:"status"`
	StatusReason  string            `json:"statusReason,omitempty"`
	Unmanaged     *int              `json:"unmanagedvCpus,omitempty"`
	Resources     map[string]any    `json:"computeResources,omitempty"`
	ServiceRole   string            `json:"serviceRole,omitempty"`
	UpdatePolicy  map[string]any    `json:"updatePolicy,omitempty"`
	Eks           map[string]any    `json:"eksConfiguration,omitempty"`
	Context       string            `json:"context,omitempty"`
	Cluster       string            `json:"ecsClusterArn,omitempty"`
	Orchestration string            `json:"containerOrchestrationType"`
	UUID          string            `json:"uuid"`
	Tags          map[string]string `json:"tags,omitempty"`
}

// Queue is a job queue.
type Queue struct {
	Name         string            `json:"jobQueueName"`
	Arn          string            `json:"jobQueueArn"`
	State        string            `json:"state"`
	Status       string            `json:"status"`
	StatusReason string            `json:"statusReason,omitempty"`
	Priority     int               `json:"priority"`
	Order        []Order           `json:"computeEnvironmentOrder"`
	Policy       string            `json:"schedulingPolicyArn,omitempty"`
	TimeLimits   []any             `json:"jobStateTimeLimitActions,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// Order places a compute environment in a queue.
type Order struct {
	Order int    `json:"order"`
	Env   string `json:"computeEnvironment"`
}

// Policy is a scheduling policy, kept as given.
type Policy struct {
	Name      string            `json:"name"`
	Arn       string            `json:"arn"`
	Fairshare map[string]any    `json:"fairsharePolicy,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// Def is one revision of a job definition; its container, node, EKS and ECS
// properties are kept as given.
type Def struct {
	Name          string            `json:"jobDefinitionName"`
	Arn           string            `json:"jobDefinitionArn"`
	Revision      int               `json:"revision"`
	Status        string            `json:"status"`
	Type          string            `json:"type"`
	Parameters    map[string]string `json:"parameters,omitempty"`
	Retry         *Retry            `json:"retryStrategy,omitempty"`
	Timeout       *Timeout          `json:"timeout,omitempty"`
	Container     json.RawMessage   `json:"containerProperties,omitempty"`
	Node          json.RawMessage   `json:"nodeProperties,omitempty"`
	Eks           json.RawMessage   `json:"eksProperties,omitempty"`
	Ecs           json.RawMessage   `json:"ecsProperties,omitempty"`
	Platform      []string          `json:"platformCapabilities,omitempty"`
	PropagateTags bool              `json:"propagateTags,omitempty"`
	Priority      *int              `json:"schedulingPriority,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
}

// Retry is a retry strategy.
type Retry struct {
	Attempts int      `json:"attempts,omitempty"`
	OnExit   []OnExit `json:"evaluateOnExit,omitempty"`
}

// OnExit decides whether a failed attempt is retried.
type OnExit struct {
	Action         string `json:"action"`
	OnExitCode     string `json:"onExitCode,omitempty"`
	OnReason       string `json:"onReason,omitempty"`
	OnStatusReason string `json:"onStatusReason,omitempty"`
}

// Timeout limits each attempt.
type Timeout struct {
	Seconds int `json:"attemptDurationSeconds,omitempty"`
}

// Job is a job, an array job's parent or one of its children.
type Job struct {
	ID            string            `json:"jobId"`
	Arn           string            `json:"jobArn"`
	Name          string            `json:"jobName"`
	Queue         string            `json:"jobQueue,omitempty"`
	Def           string            `json:"jobDefinition"`
	Status        string            `json:"status"`
	Reason        string            `json:"statusReason,omitempty"`
	Created       int64             `json:"createdAt"`
	Started       int64             `json:"startedAt,omitempty"`
	Stopped       int64             `json:"stoppedAt,omitempty"`
	Attempts      []Attempt         `json:"attempts,omitempty"`
	DependsOn     []Dep             `json:"dependsOn,omitempty"`
	Parameters    map[string]string `json:"parameters,omitempty"`
	Container     *Container        `json:"container,omitempty"`
	Array         *Array            `json:"arrayProperties,omitempty"`
	Retry         *Retry            `json:"retryStrategy,omitempty"`
	Timeout       *Timeout          `json:"timeout,omitempty"`
	Platform      []string          `json:"platformCapabilities,omitempty"`
	PropagateTags bool              `json:"propagateTags,omitempty"`
	Share         string            `json:"shareIdentifier,omitempty"`
	Priority      *int              `json:"schedulingPriority,omitempty"`
	Cancelled     bool              `json:"isCancelled,omitempty"`
	Terminated    bool              `json:"isTerminated,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
	attempt       int64             // when the running attempt's container started
}

// Attempt is how one try of a job ended.
type Attempt struct {
	Container *Container `json:"container"`
	Started   int64      `json:"startedAt,omitempty"`
	Stopped   int64      `json:"stoppedAt,omitempty"`
	Reason    string     `json:"statusReason,omitempty"`
}

// Container is a job's container: its definition's container properties
// with the overrides applied, and how its last attempt ended.
type Container struct {
	Image         string   `json:"image,omitempty"`
	Command       []string `json:"command,omitempty"`
	Environment   []KV     `json:"environment,omitempty"`
	Vcpus         int      `json:"vcpus,omitempty"`
	Memory        int      `json:"memory,omitempty"`
	Resources     []Need   `json:"resourceRequirements,omitempty"`
	JobRole       string   `json:"jobRoleArn,omitempty"`
	ExecutionRole string   `json:"executionRoleArn,omitempty"`
	User          string   `json:"user,omitempty"`
	ExitCode      *int     `json:"exitCode,omitempty"`
	Reason        string   `json:"reason,omitempty"`
	LogStream     string   `json:"logStreamName,omitempty"`
}

// KV is an environment variable.
type KV struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Need is a resource requirement: VCPU, MEMORY (MiB) or GPU.
type Need struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// Dep is a dependency: on a job, N_TO_N on an array job of the same size,
// or SEQUENTIAL among an array job's children.
type Dep struct {
	JobID string `json:"jobId,omitempty"`
	Type  string `json:"type,omitempty"`
}

// Array is an array job's size and its children's statuses, or a child's index.
type Array struct {
	Size    int            `json:"size,omitempty"`
	Index   *int           `json:"index,omitempty"`
	Summary map[string]int `json:"statusSummary,omitempty"`
}

// New creates the Batch service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState(), runs: map[string]context.CancelCauseFunc{}}
	s.ops = map[string]core.Handler{
		"CreateComputeEnvironment":    core.H(s.createEnv),
		"DescribeComputeEnvironments": core.H(s.describeEnvs),
		"UpdateComputeEnvironment":    core.H(s.updateEnv),
		"DeleteComputeEnvironment":    core.H(s.deleteEnv),
		"CreateJobQueue":              core.H(s.createQueue),
		"DescribeJobQueues":           core.H(s.describeQueues),
		"UpdateJobQueue":              core.H(s.updateQueue),
		"DeleteJobQueue":              core.H(s.deleteQueue),
		"CreateSchedulingPolicy":      core.H(s.createPolicy),
		"DescribeSchedulingPolicies":  core.H(s.describePolicies),
		"ListSchedulingPolicies":      core.H(s.listPolicies),
		"UpdateSchedulingPolicy":      core.H(s.updatePolicy),
		"DeleteSchedulingPolicy":      core.H(s.deletePolicy),
		"RegisterJobDefinition":       core.H(s.registerDef),
		"DescribeJobDefinitions":      core.H(s.describeDefs),
		"DeregisterJobDefinition":     core.H(s.deregisterDef),
		"SubmitJob":                   core.H(s.submitJob),
		"DescribeJobs":                core.H(s.describeJobs),
		"ListJobs":                    core.H(s.listJobs),
		"CancelJob":                   core.H(s.cancelJob),
		"TerminateJob":                core.H(s.terminateJob),
	}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "batch", Cloud: core.AWS, Title: "Batch", Signing: []string{"batch"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	out, err := s.serve(c, r)
	if err != nil {
		core.RESTJSONError(w, c, err)
		return
	}
	if out == nil {
		out = struct{}{}
	}
	core.RESTJSON(w, c, http.StatusOK, out)
}

// serve finds the operation: POST /v1/<lowercase name>, or the tags of an ARN.
func (s *Service) serve(c *core.Ctx, r *http.Request) (any, error) {
	body := core.Body(r)
	decode := func(v any) error {
		if len(body) == 0 {
			return nil
		}
		return json.Unmarshal(body, v)
	}
	if arn, ok := strings.CutPrefix(r.URL.EscapedPath(), "/v1/tags/"); ok {
		if c.Op = map[string]string{"GET": "ListTagsForResource", "POST": "TagResource", "DELETE": "UntagResource"}[r.Method]; c.Op != "" {
			if err := c.Intercept(); err != nil {
				return nil, err
			}
			var in struct{ Tags map[string]string }
			if err := decode(&in); err != nil {
				return nil, clientErr("%v", err)
			}
			arn, _ = url.PathUnescape(arn)
			return s.tag(arn, in.Tags, r.URL.Query()["tagKeys"])
		}
	}
	for op, h := range s.ops {
		if r.Method == http.MethodPost && strings.EqualFold(r.URL.Path, "/v1/"+op) {
			c.Op = op
			if err := c.Intercept(); err != nil {
				return nil, err
			}
			return h(c, decode)
		}
	}
	return nil, core.Errorf(http.StatusNotFound, "UnknownOperationException", "pogi does not implement Batch %s %s yet", r.Method, r.URL.Path)
}

func clientErr(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "ClientException", format, a...)
}

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func validName(field, name string) error {
	if !nameRE.MatchString(name) {
		return clientErr("%s must be 1 to 128 letters, numbers, hyphens and underscores, starting with a letter or number", field)
	}
	return nil
}

// ref is the state key of a resource given by name or ARN; kind is its ARN
// resource type (compute-environment, job-queue…).
func ref(account, region, kind, s string) string {
	if _, r, a, res, ok := core.ParseARN(s); ok {
		return a + "/" + r + "/" + strings.TrimPrefix(res, kind+"/")
	}
	return account + "/" + region + "/" + s
}

func prefix(c *core.Ctx) string { return c.Account + "/" + c.Region + "/" }

// snap encodes state for a response while the lock is held.
func snap(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// pick returns the resources named (by name or ARN), or all of the caller's.
func pick[T any](m map[string]*T, c *core.Ctx, kind string, refs []string) []*T {
	out := []*T{}
	if len(refs) == 0 {
		for _, k := range core.SortedKeys(m) {
			if strings.HasPrefix(k, prefix(c)) {
				out = append(out, m[k])
			}
		}
	}
	for _, r := range refs {
		if v := m[ref(c.Account, c.Region, kind, r)]; v != nil {
			out = append(out, v)
		}
	}
	return out
}

// paged is a list response: up to n items (default 100) after the offset
// in token, and the next token when more remain.
func paged[T any](name string, items []T, token string, n int) map[string]any {
	start, _ := strconv.Atoi(token)
	start = min(max(start, 0), len(items))
	if n <= 0 {
		n = 100
	}
	end := min(start+n, len(items))
	out := map[string]any{name: snap(items[start:end])}
	if end < len(items) {
		out["nextToken"] = strconv.Itoa(end)
	}
	return out
}

// ---- compute environments ----

func (s *Service) createEnv(c *core.Ctx, e *Env) (any, error) {
	if err := validName("computeEnvironmentName", e.Name); err != nil {
		return nil, err
	}
	if e.Type != "MANAGED" && e.Type != "UNMANAGED" {
		return nil, clientErr("type must be MANAGED or UNMANAGED")
	}
	if e.Type == "MANAGED" && e.Resources == nil {
		return nil, clientErr("computeResources is required for a MANAGED compute environment")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := prefix(c) + e.Name
	if s.st.Envs[k] != nil {
		return nil, clientErr("Object already exists")
	}
	e.Arn, e.UUID = c.ARN("batch", "compute-environment/"+e.Name), core.UUID()
	e.State, e.Status, e.StatusReason = cmp.Or(e.State, "ENABLED"), "CREATING", ""
	e.Cluster, e.Orchestration = c.ARN("ecs", "cluster/AWSBatch-"+e.Name+"-"+e.UUID), "ECS"
	if e.Eks != nil {
		e.Cluster, e.Orchestration = "", "EKS"
	}
	s.st.Envs[k] = e
	return map[string]string{"computeEnvironmentName": e.Name, "computeEnvironmentArn": e.Arn}, nil
}

// env finds a compute environment by name or ARN. Lock held.
func (s *Service) env(c *core.Ctx, r string) (*Env, error) {
	if e := s.st.Envs[ref(c.Account, c.Region, "compute-environment", r)]; e != nil && e.Status != "DELETING" {
		return e, nil
	}
	return nil, clientErr("Compute environment %s does not exist", r)
}

func (s *Service) describeEnvs(c *core.Ctx, in *struct {
	ComputeEnvironments []string
	MaxResults          int
	NextToken           string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return paged("computeEnvironments", pick(s.st.Envs, c, "compute-environment", in.ComputeEnvironments), in.NextToken, in.MaxResults), nil
}

func (s *Service) updateEnv(c *core.Ctx, in *struct {
	ComputeEnvironment string
	State              string
	UnmanagedvCpus     *int
	ComputeResources   map[string]any
	ServiceRole        string
	UpdatePolicy       map[string]any
	Context            *string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.env(c, in.ComputeEnvironment)
	if err != nil {
		return nil, err
	}
	e.State, e.Unmanaged, e.ServiceRole = cmp.Or(in.State, e.State), cmp.Or(in.UnmanagedvCpus, e.Unmanaged), cmp.Or(in.ServiceRole, e.ServiceRole)
	if in.ComputeResources != nil {
		if e.Resources == nil {
			e.Resources = map[string]any{}
		}
		maps.Copy(e.Resources, in.ComputeResources)
	}
	if in.UpdatePolicy != nil {
		e.UpdatePolicy = in.UpdatePolicy
	}
	if in.Context != nil {
		e.Context = *in.Context
	}
	e.Status = "UPDATING"
	return map[string]string{"computeEnvironmentName": e.Name, "computeEnvironmentArn": e.Arn}, nil
}

func (s *Service) deleteEnv(c *core.Ctx, in *struct{ ComputeEnvironment string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.env(c, in.ComputeEnvironment)
	if err != nil {
		return nil, err
	}
	if e.State != "DISABLED" {
		return nil, clientErr("Cannot delete compute environment %s while it is ENABLED: disable it first", e.Name)
	}
	for _, q := range s.st.Queues {
		if q.Status != "DELETING" && slices.ContainsFunc(q.Order, func(o Order) bool { return o.Env == e.Arn }) {
			return nil, clientErr("Cannot delete, found existing JobQueue relationship")
		}
	}
	e.Status = "DELETING"
	return nil, nil
}

// ---- job queues and scheduling policies ----

func (s *Service) createQueue(c *core.Ctx, q *Queue) (any, error) {
	if err := validName("jobQueueName", q.Name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := prefix(c) + q.Name
	if s.st.Queues[k] != nil {
		return nil, clientErr("Object already exists")
	}
	if err := s.resolve(c, q); err != nil {
		return nil, err
	}
	q.Arn = c.ARN("batch", "job-queue/"+q.Name)
	q.State, q.Status, q.StatusReason = cmp.Or(q.State, "ENABLED"), "CREATING", ""
	s.st.Queues[k] = q
	return map[string]string{"jobQueueName": q.Name, "jobQueueArn": q.Arn}, nil
}

// resolve checks a queue's compute environments and scheduling policy, and
// names the environments by ARN as Batch returns them. Lock held.
func (s *Service) resolve(c *core.Ctx, q *Queue) error {
	for i, o := range q.Order {
		e, err := s.env(c, o.Env)
		if err != nil {
			return err
		}
		q.Order[i].Env = e.Arn
	}
	if q.Policy != "" && s.st.Policies[ref(c.Account, c.Region, "scheduling-policy", q.Policy)] == nil {
		return clientErr("Scheduling policy %s does not exist", q.Policy)
	}
	return nil
}

// queue finds a job queue by name or ARN. Lock held.
func (s *Service) queue(c *core.Ctx, r string) (*Queue, error) {
	if q := s.st.Queues[ref(c.Account, c.Region, "job-queue", r)]; q != nil && q.Status != "DELETING" {
		return q, nil
	}
	return nil, clientErr("Job queue %s does not exist", r)
}

func (s *Service) describeQueues(c *core.Ctx, in *struct {
	JobQueues  []string
	MaxResults int
	NextToken  string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return paged("jobQueues", pick(s.st.Queues, c, "job-queue", in.JobQueues), in.NextToken, in.MaxResults), nil
}

func (s *Service) updateQueue(c *core.Ctx, in *struct {
	JobQueue                 string
	State                    string
	Priority                 *int
	ComputeEnvironmentOrder  []Order
	SchedulingPolicyArn      string
	JobStateTimeLimitActions []any
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.JobQueue)
	if err != nil {
		return nil, err
	}
	next := *q
	next.State, next.Policy = cmp.Or(in.State, q.State), cmp.Or(in.SchedulingPolicyArn, q.Policy)
	if in.Priority != nil {
		next.Priority = *in.Priority
	}
	if in.ComputeEnvironmentOrder != nil {
		next.Order = in.ComputeEnvironmentOrder
	}
	if in.JobStateTimeLimitActions != nil {
		next.TimeLimits = in.JobStateTimeLimitActions
	}
	if err := s.resolve(c, &next); err != nil {
		return nil, err
	}
	next.Status = "UPDATING"
	*q = next
	return map[string]string{"jobQueueName": q.Name, "jobQueueArn": q.Arn}, nil
}

func (s *Service) deleteQueue(c *core.Ctx, in *struct{ JobQueue string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.JobQueue)
	if err != nil {
		return nil, err
	}
	if q.State != "DISABLED" {
		return nil, clientErr("Cannot delete job queue %s while it is ENABLED: disable it first", q.Name)
	}
	q.Status = "DELETING"
	now := time.Now().UnixMilli()
	for k, j := range s.st.Jobs {
		if j.Queue == q.Arn {
			s.halt(k, j, "The job queue was deleted", true, now)
		}
	}
	return nil, nil
}

func (s *Service) createPolicy(c *core.Ctx, p *Policy) (any, error) {
	if err := validName("name", p.Name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := prefix(c) + p.Name
	if s.st.Policies[k] != nil {
		return nil, clientErr("Object already exists")
	}
	p.Arn = c.ARN("batch", "scheduling-policy/"+p.Name)
	s.st.Policies[k] = p
	return map[string]string{"name": p.Name, "arn": p.Arn}, nil
}

func (s *Service) describePolicies(c *core.Ctx, in *struct{ Arns []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"schedulingPolicies": snap(pick(s.st.Policies, c, "scheduling-policy", in.Arns))}, nil
}

func (s *Service) listPolicies(c *core.Ctx, in *struct {
	MaxResults int
	NextToken  string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arns := []map[string]string{}
	for _, p := range pick(s.st.Policies, c, "", nil) {
		arns = append(arns, map[string]string{"arn": p.Arn})
	}
	return paged("schedulingPolicies", arns, in.NextToken, in.MaxResults), nil
}

func (s *Service) updatePolicy(c *core.Ctx, in *struct {
	Arn             string
	FairsharePolicy map[string]any
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.st.Policies[ref(c.Account, c.Region, "scheduling-policy", in.Arn)]
	if p == nil {
		return nil, clientErr("Scheduling policy %s does not exist", in.Arn)
	}
	p.Fairshare = in.FairsharePolicy
	return nil, nil
}

func (s *Service) deletePolicy(c *core.Ctx, in *struct{ Arn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := ref(c.Account, c.Region, "scheduling-policy", in.Arn)
	if s.st.Policies[k] == nil {
		return nil, clientErr("Scheduling policy %s does not exist", in.Arn)
	}
	delete(s.st.Policies, k)
	return nil, nil
}

// ---- job definitions ----

func (s *Service) registerDef(c *core.Ctx, d *Def) (any, error) {
	if err := validName("jobDefinitionName", d.Name); err != nil {
		return nil, err
	}
	if d.Type != "container" && d.Type != "multinode" {
		return nil, clientErr("type must be container or multinode")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p := prefix(c) + d.Name + ":"
	d.Revision = 1
	for k, o := range s.st.Defs {
		if strings.HasPrefix(k, p) {
			d.Revision = max(d.Revision, o.Revision+1)
		}
	}
	d.Status, d.Arn = "ACTIVE", c.ARN("batch", "job-definition/"+d.Name+":"+strconv.Itoa(d.Revision))
	s.st.Defs[p+strconv.Itoa(d.Revision)] = d
	return map[string]any{"jobDefinitionName": d.Name, "jobDefinitionArn": d.Arn, "revision": d.Revision}, nil
}

// def finds a job definition by ARN or name:revision, or the latest ACTIVE
// revision of a name. Lock held.
func (s *Service) def(c *core.Ctx, r string) *Def {
	if d := s.st.Defs[ref(c.Account, c.Region, "job-definition", r)]; d != nil {
		return d
	}
	var last *Def
	for k, d := range s.st.Defs {
		if strings.HasPrefix(k, prefix(c)+r+":") && d.Status == "ACTIVE" && (last == nil || d.Revision > last.Revision) {
			last = d
		}
	}
	return last
}

func (s *Service) describeDefs(c *core.Ctx, in *struct {
	JobDefinitions    []string
	JobDefinitionName string
	Status            string
	MaxResults        int
	NextToken         string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	want := map[string]bool{} // name:revision keys, or bare names for every revision
	for _, r := range in.JobDefinitions {
		want[ref(c.Account, c.Region, "job-definition", r)] = true
	}
	list := []*Def{}
	for k, d := range s.st.Defs {
		switch {
		case !strings.HasPrefix(k, prefix(c)):
		case len(want) > 0 && !want[k] && !want[prefix(c)+d.Name]:
		case in.JobDefinitionName != "" && d.Name != in.JobDefinitionName:
		case in.Status != "" && d.Status != in.Status:
		default:
			list = append(list, d)
		}
	}
	slices.SortFunc(list, func(a, b *Def) int { return cmp.Or(strings.Compare(a.Name, b.Name), a.Revision-b.Revision) })
	return paged("jobDefinitions", list, in.NextToken, in.MaxResults), nil
}

func (s *Service) deregisterDef(c *core.Ctx, in *struct{ JobDefinition string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.def(c, in.JobDefinition)
	if d == nil {
		return nil, clientErr("Job definition %s does not exist", in.JobDefinition)
	}
	d.Status = "INACTIVE"
	return nil, nil
}

// ---- jobs ----

type submitInput struct {
	JobName                    string
	JobQueue                   string
	JobDefinition              string
	ShareIdentifier            string
	SchedulingPriorityOverride *int
	ArrayProperties            *Array
	DependsOn                  []Dep
	Parameters                 map[string]string
	ContainerOverrides         *Container
	RetryStrategy              *Retry
	PropagateTags              *bool
	Timeout                    *Timeout
	Tags                       map[string]string
}

func (s *Service) submitJob(c *core.Ctx, in *submitInput) (any, error) {
	if err := validName("jobName", in.JobName); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.JobQueue)
	if err != nil {
		return nil, err
	}
	if q.State != "ENABLED" {
		return nil, clientErr("Job queue %s is DISABLED and accepts no jobs", q.Name)
	}
	d := s.def(c, in.JobDefinition)
	if d == nil || d.Status != "ACTIVE" {
		return nil, clientErr("Job definition %s does not exist or is INACTIVE", in.JobDefinition)
	}
	size := 0
	if in.ArrayProperties != nil {
		size = in.ArrayProperties.Size
	}
	if size != 0 && (size < 2 || size > 10000) {
		return nil, clientErr("arrayProperties.size must be between 2 and 10000")
	}
	for _, dep := range in.DependsOn {
		switch o := s.st.Jobs[prefix(c)+dep.JobID]; {
		case dep.Type == "SEQUENTIAL" && size > 0:
		case dep.Type == "SEQUENTIAL":
			return nil, clientErr("a SEQUENTIAL dependency needs an array job")
		case o == nil:
			return nil, clientErr("Dependent job %s does not exist", dep.JobID)
		case dep.Type == "N_TO_N" && (o.Array == nil || o.Array.Size != size):
			return nil, clientErr("an N_TO_N dependency needs an array job of the same size, %s is not", dep.JobID)
		}
	}
	params := map[string]string{}
	maps.Copy(params, d.Parameters)
	maps.Copy(params, in.Parameters)
	j := &Job{ID: core.UUID(), Name: in.JobName, Queue: q.Arn, Def: d.Arn, Status: "SUBMITTED", Created: time.Now().UnixMilli(),
		DependsOn: in.DependsOn, Parameters: params, Container: container(d, in.ContainerOverrides, params),
		Retry: cmp.Or(in.RetryStrategy, d.Retry), Timeout: cmp.Or(in.Timeout, d.Timeout), Platform: d.Platform, PropagateTags: d.PropagateTags,
		Share: in.ShareIdentifier, Priority: cmp.Or(in.SchedulingPriorityOverride, d.Priority), Tags: in.Tags}
	if in.PropagateTags != nil {
		j.PropagateTags = *in.PropagateTags
	}
	j.Arn = c.ARN("batch", "job/"+j.ID)
	s.st.Jobs[prefix(c)+j.ID] = j
	if size > 0 {
		j.Array = &Array{Size: size}
	}
	for i := range size { // children <id>:<index> run, the parent follows them
		ch := *j
		n := ":" + strconv.Itoa(i)
		ch.ID, ch.Arn, ch.Array, ch.Tags, ch.DependsOn = j.ID+n, j.Arn+n, &Array{Index: &i}, maps.Clone(j.Tags), nil
		ct := *j.Container
		ch.Container = &ct
		for _, dep := range in.DependsOn {
			switch dep.Type {
			case "N_TO_N":
				ch.DependsOn = append(ch.DependsOn, Dep{JobID: dep.JobID + n})
			case "SEQUENTIAL":
				if i > 0 {
					ch.DependsOn = append(ch.DependsOn, Dep{JobID: j.ID + ":" + strconv.Itoa(i-1)})
				}
			default:
				ch.DependsOn = append(ch.DependsOn, dep)
			}
		}
		s.st.Jobs[prefix(c)+ch.ID] = &ch
	}
	return map[string]string{"jobArn": j.Arn, "jobName": j.Name, "jobId": j.ID}, nil
}

// container is a job's container: its definition's container properties
// with the overrides, and Ref::name parameters put into the command.
func container(d *Def, o *Container, params map[string]string) *Container {
	var ct Container
	json.Unmarshal(d.Container, &ct)
	if o != nil {
		if o.Command != nil {
			ct.Command = o.Command
		}
		ct.Vcpus, ct.Memory = cmp.Or(o.Vcpus, ct.Vcpus), cmp.Or(o.Memory, ct.Memory)
		for _, kv := range o.Environment {
			ct.Environment = append(slices.DeleteFunc(ct.Environment, func(e KV) bool { return e.Name == kv.Name }), kv)
		}
		for _, r := range o.Resources {
			ct.Resources = append(slices.DeleteFunc(ct.Resources, func(n Need) bool { return n.Type == r.Type }), r)
		}
	}
	names := slices.SortedFunc(maps.Keys(params), func(a, b string) int { return len(b) - len(a) }) // Ref::input before Ref::in
	for i, arg := range ct.Command {
		for _, k := range names {
			arg = strings.ReplaceAll(arg, "Ref::"+k, params[k])
		}
		ct.Command[i] = arg
	}
	return &ct
}

func (s *Service) describeJobs(c *core.Ctx, in *struct{ Jobs []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []*Job{}
	for _, id := range in.Jobs {
		if j := s.st.Jobs[ref(c.Account, c.Region, "job", id)]; j != nil {
			list = append(list, j)
		}
	}
	return map[string]any{"jobs": snap(list)}, nil
}

type filter struct {
	Name   string
	Values []string
}

func (s *Service) listJobs(c *core.Ctx, in *struct {
	JobQueue, ArrayJobId, MultiNodeJobId, JobStatus, NextToken string
	MaxResults                                                 int
	Filters                                                    []filter
}) (any, error) {
	if in.JobQueue == "" && in.ArrayJobId == "" && in.MultiNodeJobId == "" {
		return nil, clientErr("one of jobQueue, arrayJobId and multiNodeJobId is required")
	}
	status := cmp.Or(in.JobStatus, "RUNNING") // what Batch lists unless told otherwise or filtering
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := ""
	if q := s.st.Queues[ref(c.Account, c.Region, "job-queue", in.JobQueue)]; q != nil {
		queue = q.Arn
	}
	var list []*Job
	for k, j := range s.st.Jobs {
		child := j.Array != nil && j.Array.Index != nil
		switch {
		case !strings.HasPrefix(k, prefix(c)):
		case in.ArrayJobId != "" && !(child && strings.HasPrefix(j.ID, in.ArrayJobId+":")):
		case in.ArrayJobId == "" && (child || j.Queue != queue):
		case len(in.Filters) == 0 && j.Status != status:
		case !matches(j, in.Filters):
		default:
			list = append(list, j)
		}
	}
	slices.SortFunc(list, func(a, b *Job) int { return cmp.Or(cmp.Compare(b.Created, a.Created), strings.Compare(a.ID, b.ID)) })
	out := make([]*Job, len(list))
	for i, j := range list {
		out[i] = &Job{ID: j.ID, Arn: j.Arn, Name: j.Name, Def: j.Def, Status: j.Status, Reason: j.Reason, Created: j.Created, Started: j.Started, Stopped: j.Stopped}
		if ct := j.Container; ct != nil && (ct.ExitCode != nil || ct.Reason != "") {
			out[i].Container = &Container{ExitCode: ct.ExitCode, Reason: ct.Reason}
		}
		if j.Array != nil {
			out[i].Array = &Array{Size: j.Array.Size, Index: j.Array.Index}
		}
	}
	return paged("jobSummaryList", out, in.NextToken, in.MaxResults), nil
}

// matches applies ListJobs filters: JOB_NAME and JOB_DEFINITION (a trailing
// * matches a prefix), BEFORE_CREATED_AT and AFTER_CREATED_AT (epoch ms).
func matches(j *Job, filters []filter) bool {
	_, _, _, res, _ := core.ParseARN(j.Def)
	def := strings.TrimPrefix(res, "job-definition/")
	name, _, _ := strings.Cut(def, ":")
	for _, f := range filters {
		v := ""
		if len(f.Values) > 0 {
			v = f.Values[0]
		}
		n, _ := strconv.ParseInt(v, 10, 64)
		ok := true
		switch f.Name {
		case "JOB_NAME":
			ok = glob(v, j.Name)
		case "JOB_DEFINITION":
			ok = glob(v, j.Def) || glob(v, def) || glob(v, name)
		case "BEFORE_CREATED_AT":
			ok = j.Created < n
		case "AFTER_CREATED_AT":
			ok = j.Created > n
		}
		if !ok {
			return false
		}
	}
	return true
}

// glob matches Batch's patterns: exact, or a prefix ending in *; empty matches anything.
func glob(p, s string) bool {
	if pre, ok := strings.CutSuffix(p, "*"); ok {
		return strings.HasPrefix(s, pre)
	}
	return p == "" || p == s
}

func (s *Service) cancelJob(c *core.Ctx, in *struct{ JobId, Reason string }) (any, error) {
	return s.stopJob(c, in.JobId, in.Reason, false)
}

func (s *Service) terminateJob(c *core.Ctx, in *struct{ JobId, Reason string }) (any, error) {
	return s.stopJob(c, in.JobId, in.Reason, true)
}

// stopJob halts a job and, for an array job, its children.
func (s *Service) stopJob(c *core.Ctx, id, reason string, terminate bool) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := ref(c.Account, c.Region, "job", id)
	j := s.st.Jobs[k]
	if j == nil {
		return nil, clientErr("Job %s does not exist", id)
	}
	now := time.Now().UnixMilli()
	s.halt(k, j, reason, terminate, now)
	for i := range size(j) {
		if ck := k + ":" + strconv.Itoa(i); s.st.Jobs[ck] != nil {
			s.halt(ck, s.st.Jobs[ck], reason, terminate, now)
		}
	}
	return nil, nil
}

// halt fails a job that has not started (CancelJob), or with terminate
// also stops a running one. Lock held.
func (s *Service) halt(k string, j *Job, reason string, terminate bool, now int64) {
	started := j.Status == "STARTING" || j.Status == "RUNNING"
	if j.Status == "SUCCEEDED" || j.Status == "FAILED" || started && !terminate {
		return
	}
	j.Cancelled, j.Terminated = !terminate, terminate
	switch {
	case started:
		if stop := s.runs[k]; stop != nil {
			stop(errors.New(reason))
		}
	case size(j) == 0: // an array job follows its children
		j.Status, j.Reason, j.Stopped = "FAILED", reason, now
	}
}

// size is an array job's size, 0 for other jobs.
func size(j *Job) int {
	if j.Array == nil {
		return 0
	}
	return j.Array.Size
}

func (s *Service) tag(arn string, add map[string]string, remove []string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, region, account, res, _ := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, "/")
	k := account + "/" + region + "/" + name
	var tags *map[string]string
	switch kind {
	case "compute-environment":
		if v := s.st.Envs[k]; v != nil {
			tags = &v.Tags
		}
	case "job-queue":
		if v := s.st.Queues[k]; v != nil {
			tags = &v.Tags
		}
	case "scheduling-policy":
		if v := s.st.Policies[k]; v != nil {
			tags = &v.Tags
		}
	case "job-definition":
		if v := s.st.Defs[k]; v != nil {
			tags = &v.Tags
		}
	case "job":
		if v := s.st.Jobs[k]; v != nil {
			tags = &v.Tags
		}
	}
	if tags == nil {
		return nil, clientErr("Resource %s does not exist", arn)
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	maps.Copy(*tags, add)
	for _, key := range remove {
		delete(*tags, key)
	}
	return map[string]any{"tags": snap(*tags)}, nil
}

// ---- the scheduler ----

// Tick moves environments and queues to VALID (or away, once deleted) and
// jobs along: SUBMITTED, PENDING until their dependencies succeed, RUNNABLE
// until a container can start. Running attempts end on their own.
func (s *Service) Tick(now time.Time) {
	engine := s.hub.Docker().Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.st.Envs {
		switch e.Status {
		case "CREATING", "UPDATING":
			e.Status, e.StatusReason = "VALID", "ComputeEnvironment Healthy"
		case "DELETING":
			delete(s.st.Envs, k)
		}
	}
	for k, q := range s.st.Queues {
		switch q.Status {
		case "CREATING", "UPDATING":
			q.Status, q.StatusReason = "VALID", "JobQueue Healthy"
		case "DELETING":
			delete(s.st.Queues, k)
		}
	}
	ms := now.UnixMilli()
	for k, j := range s.st.Jobs {
		switch p := k[:strings.LastIndexByte(k, '/')+1]; {
		case j.Status == "SUBMITTED":
			j.Status = "PENDING"
		case j.Status == "SUCCEEDED" || j.Status == "FAILED":
		case size(j) > 0:
			s.follow(p, j, ms)
		case j.Status == "PENDING":
			wait, failed := s.waiting(p, j)
			if failed {
				j.Status, j.Reason, j.Stopped = "FAILED", "Dependent Job failed", ms
			} else if !wait {
				j.Status = "RUNNABLE"
				s.launch(k, j, engine)
			}
		case j.Status == "RUNNABLE":
			s.launch(k, j, engine)
		}
	}
}

// waiting says whether a job still waits for its dependencies, or failed
// because one did. Lock held.
func (s *Service) waiting(prefix string, j *Job) (wait, failed bool) {
	for _, d := range j.DependsOn {
		switch dep := s.st.Jobs[prefix+d.JobID]; {
		case d.JobID == "" || dep == nil:
		case dep.Status == "FAILED":
			return false, true
		case dep.Status != "SUCCEEDED":
			wait = true
		}
	}
	return wait, false
}

// follow sets an array job's status from its children's. Lock held.
func (s *Service) follow(prefix string, j *Job, now int64) {
	sum := map[string]int{"SUBMITTED": 0, "PENDING": 0, "RUNNABLE": 0, "STARTING": 0, "RUNNING": 0, "SUCCEEDED": 0, "FAILED": 0}
	for i := range j.Array.Size {
		if ch := s.st.Jobs[prefix+j.ID+":"+strconv.Itoa(i)]; ch != nil {
			sum[ch.Status]++
		}
	}
	j.Array.Summary = sum
	switch n := j.Array.Size; {
	case sum["SUCCEEDED"] == n:
		j.Status, j.Stopped = "SUCCEEDED", now
	case sum["SUCCEEDED"]+sum["FAILED"] == n:
		j.Status, j.Stopped = "FAILED", now
	case sum["STARTING"]+sum["RUNNING"] > 0:
		j.Status, j.Started = "RUNNING", cmp.Or(j.Started, now)
	}
}

// launch starts a RUNNABLE job's next attempt once Docker runs and its queue
// has an enabled compute environment. Lock held; the attempt runs in the background.
func (s *Service) launch(k string, j *Job, engine bool) {
	if !engine {
		j.Reason = NoDocker
		return
	}
	q, e, best := s.st.Queues[ref("", "", "job-queue", j.Queue)], (*Env)(nil), 0
	if q == nil || q.State != "ENABLED" || s.closed {
		return
	}
	for _, o := range q.Order { // the first enabled compute environment by order
		ce := s.st.Envs[ref("", "", "compute-environment", o.Env)]
		if ce != nil && ce.State == "ENABLED" && (ce.Status == "VALID" || ce.Status == "UPDATING") && (e == nil || o.Order < best) {
			e, best = ce, o.Order
		}
	}
	if e == nil {
		return // it waits, as on Batch without capacity
	}
	j.Status, j.Reason = "STARTING", ""
	n, task := strconv.Itoa(len(j.Attempts)+1), core.NewID()
	_, region, account, res, _ := core.ParseARN(j.Def)
	name, _, _ := strings.Cut(strings.TrimPrefix(res, "job-definition/"), ":")
	j.Container.LogStream, j.Container.ExitCode, j.Container.Reason = name+"/default/"+task, nil, ""

	d := s.hub.Docker()
	host := d.HostAddr()
	ct := &docker.Container{Name: docker.Name("batch", j.ID+"-"+n), Image: j.Container.Image, Service: "batch", Resource: j.ID, Cmd: j.Container.Command}
	if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback only from the host network
		ct.Network = "host"
	}
	ct.Env = append([]string{"AWS_BATCH_JOB_ID=" + j.ID, "AWS_BATCH_JOB_ATTEMPT=" + n, "AWS_BATCH_JQ_NAME=" + q.Name, "AWS_BATCH_CE_NAME=" + e.Name,
		"AWS_REGION=" + region, "AWS_DEFAULT_REGION=" + region, "AWS_ENDPOINT_URL=" + s.endpoint(host)},
		creds(s.hub, j.Container.JobRole, region, account, task)...)
	if j.Array != nil && j.Array.Index != nil {
		ct.Env = append(ct.Env, "AWS_BATCH_JOB_ARRAY_INDEX="+strconv.Itoa(*j.Array.Index))
	}
	for _, kv := range j.Container.Environment {
		ct.Env = append(ct.Env, kv.Name+"="+kv.Value)
	}
	vcpus, mem := float64(j.Container.Vcpus), j.Container.Memory
	for _, r := range j.Container.Resources {
		switch r.Type {
		case "VCPU":
			vcpus, _ = strconv.ParseFloat(r.Value, 64)
		case "MEMORY":
			mem, _ = strconv.Atoi(r.Value)
		}
	}
	// ponytail: vCPUs are capped at this machine's; an engine VM with fewer refuses the container.
	ct.CPUs, ct.Memory = min(vcpus, float64(runtime.NumCPU())), int64(mem)<<20

	ctx, stop := context.WithCancelCause(context.Background())
	s.runs[k] = stop
	timeout := time.Duration(0)
	if j.Timeout != nil {
		timeout = time.Duration(j.Timeout.Seconds) * time.Second
	}
	s.wg.Add(1)
	logs := core.ARN("logs", region, account, "log-group:/aws/batch/job:log-stream:"+j.Container.LogStream)
	subnets := batchSubnets(e)
	core.Go("batch.run", func() {
		if ct.Network != "host" {
			ct.Dns = route53.SubnetDNS(s.hub, account, region, subnets)
		}
		s.run(ctx, k, ct, logs, timeout)
	})
}

// run runs one attempt: the container's output goes to CloudWatch Logs and
// its exit code decides the attempt.
func (s *Service) run(ctx context.Context, k string, ct *docker.Container, logs string, timeout time.Duration) {
	defer s.wg.Done()
	if timeout > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeoutCause(ctx, timeout, errTimeout)
		defer stop()
	}
	d := s.hub.Docker()
	id, err := d.Run(ctx, ct)
	if err != nil {
		s.end(k, nil, "CannotStartContainerError: "+err.Error(), context.Cause(ctx))
		return
	}
	s.mu.Lock()
	if j := s.st.Jobs[k]; j != nil && j.Status == "STARTING" {
		j.Status, j.attempt = "RUNNING", time.Now().UnixMilli()
		j.Started = cmp.Or(j.Started, j.attempt)
	}
	s.mu.Unlock()
	done := make(chan struct{})
	core.Go("batch.run", func() {
		defer close(done)
		d.Logs(context.Background(), id, true, func(line string) { s.hub.Deliver(logs, &core.Message{Body: line}) })
	})
	code, err := d.Wait(ctx, id)
	if err != nil {
		d.Remove(context.Background(), id) // timed out or terminated: stop it now
	}
	<-done
	d.Remove(context.Background(), id)
	var exit *int
	reason := ""
	switch {
	case err == nil:
		exit = &code
	case ctx.Err() == nil:
		reason = err.Error()
	}
	s.end(k, exit, reason, context.Cause(ctx))
}

// end records how an attempt ended, then retries the job or settles it. A
// timeout or termination (cause) is never retried.
func (s *Service) end(k string, code *int, reason string, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runs, k)
	j := s.st.Jobs[k]
	if j == nil || j.Status != "STARTING" && j.Status != "RUNNING" {
		return // reset or reloaded meanwhile
	}
	now, status := time.Now().UnixMilli(), "Essential container in task exited"
	if cause != nil {
		status = cause.Error()
	}
	j.Container.ExitCode, j.Container.Reason = code, reason
	j.Attempts = append(j.Attempts, Attempt{Container: &Container{ExitCode: code, Reason: reason, LogStream: j.Container.LogStream},
		Started: j.attempt, Stopped: now, Reason: status})
	j.attempt = 0
	attempts := 1
	if j.Retry != nil {
		attempts = max(j.Retry.Attempts, 1)
	}
	switch {
	case cause == nil && code != nil && *code == 0:
		j.Status = "SUCCEEDED"
	case cause == nil && len(j.Attempts) < attempts && retry(j.Retry, code, reason, status):
		j.Status = "RUNNABLE"
		return
	default:
		j.Status = "FAILED"
	}
	j.Reason, j.Stopped = status, now
}

// retry applies a retry strategy's evaluateOnExit rules; when none matches, it retries.
func retry(r *Retry, code *int, reason, status string) bool {
	exit := ""
	if code != nil {
		exit = strconv.Itoa(*code)
	}
	for _, e := range r.OnExit {
		if glob(e.OnExitCode, exit) && glob(e.OnReason, reason) && glob(e.OnStatusReason, status) {
			return strings.EqualFold(e.Action, "RETRY")
		}
	}
	return true
}

// endpoint is pogi's URL as containers reach it.
func (s *Service) endpoint(host string) string {
	u, _ := url.Parse(cmp.Or(s.hub.URL, "http://localhost:"+strconv.Itoa(cmp.Or(s.hub.Cfg.Port, 7644))))
	return "http://" + host + ":" + u.Port()
}

// creds are the environment of session credentials minted for a role, the
// way Batch gives a job its job role; without a role, the account's own key.
func creds(h *core.Hub, role, region, account, session string) []string {
	_, _, acct, res, ok := core.ParseARN(role)
	if !ok || !strings.HasPrefix(res, "role/") {
		return []string{"AWS_ACCESS_KEY_ID=" + account, "AWS_SECRET_ACCESS_KEY=test"}
	}
	akid, secret := "ASIA"+core.RandString(16, core.UpperNum), core.RandString(40, core.AlphaNum)
	h.AddIdentity(akid, core.Identity{Account: acct, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + session,
		ARN: core.GlobalARN("sts", region, acct, "assumed-role/"+path.Base(res)+"/"+session), Secret: secret})
	return []string{"AWS_ACCESS_KEY_ID=" + akid, "AWS_SECRET_ACCESS_KEY=" + secret, "AWS_SESSION_TOKEN=" + core.RandString(180, core.AlphaNum)}
}

// ---- lifecycle and console ----

// Close stops the scheduler and the attempts still running, removing their containers.
func (s *Service) Close() error {
	s.stop()
	s.mu.Lock()
	s.closed = true
	for _, stop := range s.runs {
		stop(errStopped)
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	for _, stop := range s.runs {
		stop(errStopped)
	}
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state; attempts that were running have lost their containers and fail.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	for _, j := range st.Jobs {
		if (j.Status == "STARTING" || j.Status == "RUNNING") && size(j) == 0 {
			j.Status, j.Reason, j.Stopped = "FAILED", "pogi restarted while the job ran", now
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	region := func(k string) string { return strings.Split(k, "/")[1] }
	envs := core.Table{Name: "Compute environments", Columns: []string{"Name", "Region", "Type", "State", "Status"}}
	for _, k := range core.SortedKeys(s.st.Envs) {
		e := s.st.Envs[k]
		envs.Rows = append(envs.Rows, []string{e.Name, region(k), e.Type, e.State, e.Status})
	}
	queues := core.Table{Name: "Job queues", Columns: []string{"Name", "Region", "Priority", "State", "Status"}}
	for _, k := range core.SortedKeys(s.st.Queues) {
		q := s.st.Queues[k]
		queues.Rows = append(queues.Rows, []string{q.Name, region(k), strconv.Itoa(q.Priority), q.State, q.Status})
	}
	defs := core.Table{Name: "Job definitions", Columns: []string{"Definition", "Region", "Type", "Status"}}
	for _, k := range core.SortedKeys(s.st.Defs) {
		d := s.st.Defs[k]
		defs.Rows = append(defs.Rows, []string{d.Name + ":" + strconv.Itoa(d.Revision), region(k), d.Type, d.Status})
	}
	jobs := core.Table{Name: "Jobs", Columns: []string{"Job", "Name", "Region", "Status", "Reason"}}
	for _, k := range core.SortedKeys(s.st.Jobs) {
		j := s.st.Jobs[k]
		jobs.Rows = append(jobs.Rows, []string{j.ID, j.Name, region(k), j.Status, j.Reason})
	}
	return []core.Table{envs, queues, defs, jobs}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Compute environments":
		return find(s.st.Envs, id)
	case "Job queues":
		return find(s.st.Queues, id)
	case "Job definitions":
		return find(s.st.Defs, id)
	case "Jobs":
		return find(s.st.Jobs, id)
	}
	return nil
}

// find returns the resource whose key ends in id (name, name:revision or job id).
func find[T any](m map[string]*T, id string) any {
	for k, v := range m {
		if strings.HasSuffix(k, "/"+id) {
			return snap(v)
		}
	}
	return nil
}

// batchSubnets are the subnets of a compute environment's computeResources.
func batchSubnets(e *Env) (out []string) {
	l, _ := e.Resources["subnets"].([]any)
	for _, v := range l {
		if sn, ok := v.(string); ok {
			out = append(out, sn)
		}
	}
	return out
}
