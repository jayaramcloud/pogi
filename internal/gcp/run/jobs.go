package run

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/serverless"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// running is an execution in progress.
type running struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (x *running) stop() {
	x.cancel()
	<-x.done
}

func jobSpec(j *runpb.Job) *runpb.Job {
	j = proto.Clone(j).(*runpb.Job)
	j.Generation, j.ObservedGeneration, j.CreateTime, j.UpdateTime, j.Etag, j.Conditions, j.TerminalCondition = 0, 0, nil, nil, "", nil, nil
	j.ExecutionCount, j.LatestCreatedExecution, j.Reconciling = 0, nil, false
	return j
}

// jobDefaults fills what Cloud Run fills in a job's template.
func jobDefaults(j *runpb.Job) error {
	t := j.GetTemplate().GetTemplate()
	if j.Template == nil || t == nil {
		return gapi.Invalid("Invalid value for job.template.template: required")
	}
	if err := checkContainers(t.Containers, "template.template"); err != nil {
		return err
	}
	if t.Timeout == nil {
		t.Timeout = durationpb.New(600 * time.Second)
	}
	if t.Retries == nil {
		t.Retries = &runpb.TaskTemplate_MaxRetries{MaxRetries: 3}
	}
	if t.ServiceAccount == "" {
		t.ServiceAccount = defaultSA
	}
	if j.Template.TaskCount == 0 {
		j.Template.TaskCount = 1
	}
	return nil
}

func settleJob(j *runpb.Job) {
	j.ObservedGeneration, j.Reconciling = j.Generation, false
	j.Conditions = []*runpb.Condition{condition("Ready", true, "")}
	j.TerminalCondition = j.Conditions[0]
}

func (s *Service) CreateJob(_ context.Context, req *runpb.CreateJobRequest) (*longrunningpb.Operation, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	if err := checkID("job", req.JobId); err != nil {
		return nil, err
	}
	j := proto.Clone(req.Job).(*runpb.Job)
	if err := jobDefaults(j); err != nil {
		return nil, err
	}
	j.Name = req.Parent + "/jobs/" + req.JobId
	j.Uid, j.Generation, j.CreateTime, j.UpdateTime, j.Etag, j.Creator = core.UUID(), 1, timestamppb.Now(), timestamppb.Now(), etag(), "pogi@localhost"
	settleJob(j)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[j.Name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists.", j.Name)
	}
	if !req.ValidateOnly {
		s.jobs[j.Name] = j
	}
	return s.done(req.Parent, proto.Clone(j)), nil
}

func (s *Service) GetJob(_ context.Context, req *runpb.GetJobRequest) (*runpb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v := s.jobs[req.Name]; v != nil {
		return proto.Clone(v).(*runpb.Job), nil
	}
	return nil, notFound("JOB", req.Name)
}

func (s *Service) ListJobs(_ context.Context, req *runpb.ListJobsRequest) (*runpb.ListJobsResponse, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*runpb.Job
	for _, n := range core.SortedKeys(s.jobs) {
		if parent(n) == req.Parent+"/jobs" {
			all = append(all, s.jobs[n])
		}
	}
	lo, hi, next := page(len(all), req.PageSize, req.PageToken)
	out := &runpb.ListJobsResponse{NextPageToken: next}
	for _, v := range all[lo:hi] {
		out.Jobs = append(out.Jobs, proto.Clone(v).(*runpb.Job))
	}
	return out, nil
}

func (s *Service) UpdateJob(ctx context.Context, req *runpb.UpdateJobRequest) (*longrunningpb.Operation, error) {
	in := req.Job
	if in == nil {
		return nil, gapi.Invalid("Invalid value for job: required")
	}
	s.mu.Lock()
	exists := s.jobs[in.Name] != nil
	s.mu.Unlock()
	if !exists {
		if !req.AllowMissing {
			return nil, notFound("JOB", in.Name)
		}
		return s.CreateJob(ctx, &runpb.CreateJobRequest{Parent: parent(parent(in.Name)), JobId: last(in.Name), Job: in, ValidateOnly: req.ValidateOnly})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.jobs[in.Name]
	if cur == nil {
		return nil, notFound("JOB", in.Name)
	}
	next := proto.Clone(cur).(*runpb.Job)
	if in.Template != nil {
		next.Template = proto.Clone(in.Template).(*runpb.ExecutionTemplate)
		if err := jobDefaults(next); err != nil {
			return nil, err
		}
	}
	next.Labels, next.Annotations = in.Labels, in.Annotations
	if !proto.Equal(jobSpec(cur), jobSpec(next)) {
		next.Generation++
		next.UpdateTime, next.Etag = timestamppb.Now(), etag()
	}
	settleJob(next)
	if !req.ValidateOnly {
		s.jobs[next.Name] = next
	}
	return s.done(parent(parent(next.Name)), proto.Clone(next)), nil
}

func (s *Service) DeleteJob(_ context.Context, req *runpb.DeleteJobRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	cur := s.jobs[req.Name]
	if cur == nil {
		s.mu.Unlock()
		return nil, notFound("JOB", req.Name)
	}
	if req.ValidateOnly {
		s.mu.Unlock()
		return s.done(parent(parent(req.Name)), proto.Clone(cur)), nil
	}
	var stop []*running
	for n, e := range s.execs {
		if e.Job == req.Name {
			if x := s.cancel[n]; x != nil {
				stop = append(stop, x)
			}
			delete(s.execs, n)
			delete(s.logs, n)
		}
	}
	delete(s.jobs, req.Name)
	delete(s.iam, req.Name)
	s.mu.Unlock()
	for _, x := range stop {
		x.stop()
	}
	out := proto.Clone(cur).(*runpb.Job)
	out.DeleteTime = timestamppb.Now()
	return s.done(parent(parent(req.Name)), out), nil
}

// ---- executions ----

func (s *Service) GetExecution(_ context.Context, req *runpb.GetExecutionRequest) (*runpb.Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v := s.execs[req.Name]; v != nil {
		return proto.Clone(v).(*runpb.Execution), nil
	}
	return nil, notFound("EXECUTION", req.Name)
}

func (s *Service) ListExecutions(_ context.Context, req *runpb.ListExecutionsRequest) (*runpb.ListExecutionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[req.Parent] == nil {
		return nil, notFound("JOB", req.Parent)
	}
	var all []*runpb.Execution
	for _, n := range core.SortedKeys(s.execs) {
		if s.execs[n].Job == req.Parent {
			all = append(all, s.execs[n])
		}
	}
	lo, hi, next := page(len(all), req.PageSize, req.PageToken)
	out := &runpb.ListExecutionsResponse{NextPageToken: next}
	for _, v := range all[lo:hi] {
		out.Executions = append(out.Executions, proto.Clone(v).(*runpb.Execution))
	}
	return out, nil
}

func (s *Service) DeleteExecution(_ context.Context, req *runpb.DeleteExecutionRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	cur := s.execs[req.Name]
	x := s.cancel[req.Name]
	s.mu.Unlock()
	if cur == nil {
		return nil, notFound("EXECUTION", req.Name)
	}
	if x != nil {
		x.stop()
	}
	s.mu.Lock()
	cur = s.execs[req.Name]
	delete(s.execs, req.Name)
	delete(s.logs, req.Name)
	s.mu.Unlock()
	if cur == nil {
		return nil, notFound("EXECUTION", req.Name)
	}
	out := proto.Clone(cur).(*runpb.Execution)
	out.DeleteTime = timestamppb.Now()
	return s.done(parent(parent(parent(req.Name))), out), nil
}

func (s *Service) CancelExecution(_ context.Context, req *runpb.CancelExecutionRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	cur := s.execs[req.Name]
	x := s.cancel[req.Name]
	s.mu.Unlock()
	switch {
	case cur == nil:
		return nil, notFound("EXECUTION", req.Name)
	case x == nil:
		return nil, status.Errorf(codes.FailedPrecondition, "Execution '%s' has already completed.", last(req.Name))
	}
	x.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done(parent(parent(parent(req.Name))), proto.Clone(s.execs[req.Name])), nil
}

// RunJob starts an execution; the operation finishes with it, once all its tasks have.
func (s *Service) RunJob(_ context.Context, req *runpb.RunJobRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	job := s.jobs[req.Name]
	if job == nil {
		s.mu.Unlock()
		return nil, notFound("JOB", req.Name)
	}
	if req.Etag != "" && req.Etag != job.Etag {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "The etag %s does not match the job's current etag %s.", req.Etag, job.Etag)
	}
	tmpl := proto.Clone(job.Template.Template).(*runpb.TaskTemplate)
	n, par := job.Template.TaskCount, job.Template.Parallelism
	if o := req.Overrides; o != nil {
		if o.TaskCount > 0 {
			n = o.TaskCount
		}
		if o.Timeout != nil {
			tmpl.Timeout = o.Timeout
		}
		for i, co := range o.ContainerOverrides {
			c := tmpl.Containers[0]
			if co.Name != "" || i > 0 {
				for _, x := range tmpl.Containers {
					if x.Name == co.Name {
						c = x
					}
				}
			}
			if co.ClearArgs {
				c.Args = nil
			}
			c.Args = append(c.Args, co.Args...)
			c.Env = append(c.Env, co.Env...)
		}
	}
	name := job.Name + "/executions/" + last(job.Name) + "-" + core.RandString(5, "abcdefghijklmnopqrstuvwxyz")
	ex := &runpb.Execution{Name: name, Uid: core.UUID(), Generation: 1, Labels: job.Template.Labels, Annotations: job.Template.Annotations,
		CreateTime: timestamppb.Now(), StartTime: timestamppb.Now(), Job: job.Name, Parallelism: par, TaskCount: n, Template: tmpl,
		Reconciling: true, ObservedGeneration: 1, Etag: etag(), LaunchStage: job.LaunchStage,
		Conditions: []*runpb.Condition{{Type: "Completed", State: runpb.Condition_CONDITION_RECONCILING, LastTransitionTime: timestamppb.Now()}}}
	if req.ValidateOnly {
		s.mu.Unlock()
		return s.done(parent(parent(job.Name)), ex), nil
	}
	s.execs[name] = ex
	logs := &serverless.Logs{}
	s.logs[name] = logs
	project, loc := splitName(name)
	logs.ToLogging(s.hub, project, "run.googleapis.com%2Fstdout", "cloud_run_job", map[string]string{"job_name": last(job.Name), "location": loc})
	ctx, cancel := context.WithCancel(context.Background())
	x := &running{cancel, make(chan struct{})}
	s.cancel[name] = x
	job.ExecutionCount++
	job.LatestCreatedExecution = &runpb.ExecutionReference{Name: name, CreateTime: ex.CreateTime}
	job.Reconciling = true
	s.mu.Unlock()
	return s.ops.Start(parent(parent(job.Name)), proto.Clone(ex), func() (proto.Message, error) {
		defer close(x.done)
		s.runTasks(ctx, name, logs)
		s.mu.Lock()
		defer s.mu.Unlock()
		return proto.Clone(s.execs[name]), nil
	}), nil
}

// setCompleted sets the Completed condition from the task counts.
func setCompleted(e *runpb.Execution) {
	c := condition("Completed", e.FailedCount == 0 && e.CancelledCount == 0, "")
	switch {
	case e.CancelledCount > 0:
		c.Message = fmt.Sprintf("%d task(s) cancelled", e.CancelledCount)
	case e.FailedCount > 0:
		c.Message = fmt.Sprintf("%d task(s) failed", e.FailedCount)
	}
	e.Conditions = []*runpb.Condition{c}
}

// update changes an execution under the lock.
func (s *Service) update(name string, f func(*runpb.Execution)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.execs[name]; e != nil {
		f(e)
		e.UpdateTime = timestamppb.Now()
	}
}

// runTasks runs the tasks of an execution, parallelism at a time, and records
// how they ended. A failed task is retried up to the template's max retries.
func (s *Service) runTasks(ctx context.Context, name string, logs *serverless.Logs) {
	s.mu.Lock()
	ex := proto.Clone(s.execs[name]).(*runpb.Execution)
	s.mu.Unlock()
	n, par := int(ex.TaskCount), int(ex.Parallelism)
	if par <= 0 || par > n {
		par = n
	}
	sem, wg := make(chan struct{}, par), sync.WaitGroup{}
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		core.Go("run.runTasks", func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.update(name, func(e *runpb.Execution) { e.RunningCount++ })
			res := s.task(ctx, ex, i, logs)
			s.update(name, func(e *runpb.Execution) {
				e.RunningCount--
				switch res {
				case "ok":
					e.SucceededCount++
				case "cancelled":
					e.CancelledCount++
				default:
					e.FailedCount++
				}
			})
		})
	}
	wg.Wait()
	s.mu.Lock()
	delete(s.cancel, name)
	s.mu.Unlock()
	s.update(name, func(e *runpb.Execution) {
		e.Reconciling, e.CompletionTime = false, timestamppb.Now()
		setCompleted(e)
		if j := s.jobs[e.Job]; j != nil {
			j.Reconciling = false
			if j.LatestCreatedExecution.GetName() == name {
				j.LatestCreatedExecution.CompletionTime = e.CompletionTime
				j.LatestCreatedExecution.CompletionStatus = map[bool]runpb.ExecutionReference_CompletionStatus{
					true:  runpb.ExecutionReference_EXECUTION_SUCCEEDED,
					false: runpb.ExecutionReference_EXECUTION_FAILED}[e.FailedCount == 0 && e.CancelledCount == 0]
			}
		}
	})
}

// task runs task i until it succeeds, runs out of retries or is cancelled; it returns "ok", "failed" or "cancelled".
func (s *Service) task(ctx context.Context, ex *runpb.Execution, i int, logs *serverless.Logs) string {
	d := s.hub.Docker()
	if !d.Available() {
		logs.Add("pogi: " + serverless.ErrNoDocker.Error())
		return "failed"
	}
	t := ex.Template
	c := t.Containers[0] // ponytail: the first container only
	project, _ := splitName(ex.Name)
	job, exec := last(ex.Job), last(ex.Name)
	retries := int(t.GetMaxRetries())
	host := d.HostAddr()
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return "cancelled"
		}
		ct := &docker.Container{Name: docker.Name("run", fmt.Sprintf("%s-%d-%d-%s", exec, i, attempt, core.RandHex(2))), Image: c.Image, Service: "run", Resource: exec,
			Cmd: c.Args, Entrypoint: c.Command, WorkDir: c.WorkingDir,
			Env: s.envOf(c, project, "CLOUD_RUN_JOB="+job, "CLOUD_RUN_EXECUTION="+exec, "CLOUD_RUN_TASK_INDEX="+strconv.Itoa(i),
				"CLOUD_RUN_TASK_ATTEMPT="+strconv.Itoa(attempt), "CLOUD_RUN_TASK_COUNT="+strconv.Itoa(int(ex.TaskCount)))}
		if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback only from the host network
			ct.Network = "host"
		}
		code, err := s.attempt(ctx, ct, t.Timeout.AsDuration(), logs)
		switch {
		case ctx.Err() != nil:
			return "cancelled"
		case err == nil && code == 0:
			return "ok"
		case err != nil:
			logs.Add(fmt.Sprintf("pogi: task %d attempt %d: %v", i, attempt, err))
		default:
			logs.Add(fmt.Sprintf("pogi: task %d attempt %d exited with code %d", i, attempt, code))
		}
		if attempt >= retries {
			return "failed"
		}
		s.update(ex.Name, func(e *runpb.Execution) { e.RetriedCount++ })
	}
}

// attempt runs one container to its end and returns its exit code.
func (s *Service) attempt(ctx context.Context, ct *docker.Container, timeout time.Duration, logs *serverless.Logs) (int, error) {
	d := s.hub.Docker()
	id, err := d.Run(ctx, ct)
	if err != nil {
		return -1, err
	}
	defer d.Remove(context.WithoutCancel(ctx), id)
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	code, err := d.Wait(wctx, id)
	if err != nil {
		return -1, fmt.Errorf("%w (timeout %s)", err, timeout)
	}
	d.Logs(context.WithoutCancel(ctx), id, false, logs.Add)
	return code, nil
}
