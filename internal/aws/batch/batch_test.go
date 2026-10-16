package batch_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	batchsvc "github.com/nrndev/pogi/internal/aws/batch"
	"github.com/nrndev/pogi/internal/aws/logs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// setup serves Batch and CloudWatch Logs; without engine, Docker is unreachable.
func setup(t *testing.T, engine bool) (*batchsvc.Service, *batch.Client, *cloudwatchlogs.Client) {
	if !engine {
		t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	}
	var svc *batchsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = batchsvc.New(h); return svc },
		func(h *core.Hub) core.Service { return logs.New(h) })
	cfg := awstest.Config(t, url)
	return svc, batch.NewFromConfig(cfg), cloudwatchlogs.NewFromConfig(cfg)
}

// queue makes a VALID compute environment and job queue named name.
func queue(t *testing.T, svc *batchsvc.Service, c *batch.Client, name string) {
	must(c.CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{ComputeEnvironmentName: aws.String(name), Type: types.CETypeManaged,
		ComputeResources: &types.ComputeResource{Type: types.CRTypeFargate, MaxvCpus: aws.Int32(4), Subnets: []string{"subnet-1"}}}))
	must(c.CreateJobQueue(ctx, &batch.CreateJobQueueInput{JobQueueName: aws.String(name), Priority: aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{{Order: aws.Int32(1), ComputeEnvironment: aws.String(name)}}}))
	svc.Tick(time.Now())
}

func describe(c *batch.Client, id string) types.JobDetail {
	return must(c.DescribeJobs(ctx, &batch.DescribeJobsInput{Jobs: []string{id}})).Jobs[0]
}

func clientErr(t *testing.T, err error) {
	t.Helper()
	var ce *types.ClientException
	if !errors.As(err, &ce) {
		t.Fatalf("want ClientException, got %v", err)
	}
}

func TestWithoutDocker(t *testing.T) {
	svc, c, _ := setup(t, false)
	ce := must(c.CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{ComputeEnvironmentName: aws.String("ce"), Type: types.CETypeManaged,
		ComputeResources: &types.ComputeResource{Type: types.CRTypeFargate, MaxvCpus: aws.Int32(4), Subnets: []string{"subnet-1"}}}))
	if !strings.HasSuffix(*ce.ComputeEnvironmentArn, ":000000000000:compute-environment/ce") {
		t.Fatalf("arn %s", *ce.ComputeEnvironmentArn)
	}
	_, err := c.CreateComputeEnvironment(ctx, &batch.CreateComputeEnvironmentInput{ComputeEnvironmentName: aws.String("ce"), Type: types.CETypeUnmanaged})
	clientErr(t, err)
	envs := must(c.DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{})).ComputeEnvironments
	if len(envs) != 1 || envs[0].Status != types.CEStatusCreating || envs[0].State != types.CEStateEnabled || *envs[0].ComputeResources.MaxvCpus != 4 {
		t.Fatalf("environments %+v", envs)
	}
	must(c.CreateJobQueue(ctx, &batch.CreateJobQueueInput{JobQueueName: aws.String("q"), Priority: aws.Int32(5),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{{Order: aws.Int32(1), ComputeEnvironment: aws.String("ce")}}}))
	svc.Tick(time.Now())
	envs = must(c.DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{ComputeEnvironments: []string{*ce.ComputeEnvironmentArn}})).ComputeEnvironments
	qs := must(c.DescribeJobQueues(ctx, &batch.DescribeJobQueuesInput{JobQueues: []string{"q"}})).JobQueues
	if envs[0].Status != types.CEStatusValid || qs[0].Status != types.JQStatusValid || *qs[0].ComputeEnvironmentOrder[0].ComputeEnvironment != *ce.ComputeEnvironmentArn {
		t.Fatalf("after a tick: %+v %+v", envs[0], qs[0])
	}

	props := &types.ContainerProperties{Image: aws.String("alpine:3"), Command: []string{"echo", "Ref::msg"},
		ResourceRequirements: []types.ResourceRequirement{{Type: types.ResourceTypeVcpu, Value: aws.String("0.5")}}}
	must(c.RegisterJobDefinition(ctx, &batch.RegisterJobDefinitionInput{JobDefinitionName: aws.String("def"), Type: types.JobDefinitionTypeContainer,
		ContainerProperties: props, Parameters: map[string]string{"msg": "default"}}))
	def := must(c.RegisterJobDefinition(ctx, &batch.RegisterJobDefinitionInput{JobDefinitionName: aws.String("def"), Type: types.JobDefinitionTypeContainer,
		ContainerProperties: props, Parameters: map[string]string{"msg": "default"}, RetryStrategy: &types.RetryStrategy{Attempts: aws.Int32(2)}}))
	if *def.Revision != 2 || !strings.HasSuffix(*def.JobDefinitionArn, ":job-definition/def:2") {
		t.Fatalf("second revision %+v", def)
	}
	defs := must(c.DescribeJobDefinitions(ctx, &batch.DescribeJobDefinitionsInput{JobDefinitionName: aws.String("def"), Status: aws.String("ACTIVE")})).JobDefinitions
	if len(defs) != 2 || *defs[1].ContainerProperties.Image != "alpine:3" || *defs[1].RetryStrategy.Attempts != 2 {
		t.Fatalf("definitions %+v", defs)
	}

	// jobs wait in RUNNABLE without Docker; Ref:: parameters fill the command
	job := must(c.SubmitJob(ctx, &batch.SubmitJobInput{JobName: aws.String("one"), JobQueue: aws.String("q"), JobDefinition: aws.String("def"),
		Parameters: map[string]string{"msg": "hello"}}))
	if j := describe(c, *job.JobId); j.Status != types.JobStatusSubmitted || *j.JobDefinition != *def.JobDefinitionArn || j.Container.Command[1] != "hello" {
		t.Fatalf("submitted %+v", j)
	}
	arr := must(c.SubmitJob(ctx, &batch.SubmitJobInput{JobName: aws.String("arr"), JobQueue: aws.String("q"), JobDefinition: aws.String("def:1"),
		ArrayProperties: &types.ArrayProperties{Size: aws.Int32(3)}, DependsOn: []types.JobDependency{{Type: types.ArrayJobDependencySequential}}}))
	after := must(c.SubmitJob(ctx, &batch.SubmitJobInput{JobName: aws.String("after"), JobQueue: aws.String("q"), JobDefinition: aws.String("def"),
		DependsOn: []types.JobDependency{{JobId: job.JobId}}}))
	svc.Tick(time.Now())
	svc.Tick(time.Now())
	if j := describe(c, *job.JobId); j.Status != types.JobStatusRunnable || *j.StatusReason != batchsvc.NoDocker {
		t.Fatalf("without Docker %+v", j)
	}
	if j := describe(c, *after.JobId); j.Status != types.JobStatusPending {
		t.Fatalf("a job waits for its dependency: %s", j.Status)
	}
	children := must(c.ListJobs(ctx, &batch.ListJobsInput{ArrayJobId: arr.JobId, JobStatus: types.JobStatusRunnable})).JobSummaryList
	if len(children) != 1 || *children[0].JobId != *arr.JobId+":0" || *children[0].ArrayProperties.Index != 0 {
		t.Fatalf("SEQUENTIAL children: only the first may run: %+v", children)
	}
	if j := describe(c, *arr.JobId+":1"); j.DependsOn[0].JobId == nil || *j.DependsOn[0].JobId != *arr.JobId+":0" {
		t.Fatalf("child 1 depends on %+v", j.DependsOn)
	}
	listed := must(c.ListJobs(ctx, &batch.ListJobsInput{JobQueue: aws.String("q"), Filters: []types.KeyValuesPair{{Name: aws.String("JOB_NAME"), Values: []string{"a*"}}}})).JobSummaryList
	if len(listed) != 2 {
		t.Fatalf("filtered by name: %+v", listed)
	}

	// cancelling fails the job and so its dependent; terminating an array job fails its children
	must(c.CancelJob(ctx, &batch.CancelJobInput{JobId: job.JobId, Reason: aws.String("no longer needed")}))
	must(c.TerminateJob(ctx, &batch.TerminateJobInput{JobId: arr.JobId, Reason: aws.String("stop")}))
	svc.Tick(time.Now())
	if j := describe(c, *job.JobId); j.Status != types.JobStatusFailed || *j.StatusReason != "no longer needed" || !*j.IsCancelled {
		t.Fatalf("cancelled %+v", j)
	}
	if j := describe(c, *after.JobId); j.Status != types.JobStatusFailed || *j.StatusReason != "Dependent Job failed" {
		t.Fatalf("dependent %+v", j)
	}
	if j := describe(c, *arr.JobId); j.Status != types.JobStatusFailed || j.ArrayProperties.StatusSummary["FAILED"] != 3 {
		t.Fatalf("terminated array job %+v", j.ArrayProperties)
	}

	_, err = c.SubmitJob(ctx, &batch.SubmitJobInput{JobName: aws.String("x"), JobQueue: aws.String("nope"), JobDefinition: aws.String("def")})
	clientErr(t, err)
	_, err = c.SubmitJob(ctx, &batch.SubmitJobInput{JobName: aws.String("x"), JobQueue: aws.String("q"), JobDefinition: aws.String("def"),
		DependsOn: []types.JobDependency{{JobId: aws.String("nope")}}})
	clientErr(t, err)

	must(c.TagResource(ctx, &batch.TagResourceInput{ResourceArn: ce.ComputeEnvironmentArn, Tags: map[string]string{"team": "data", "env": "dev"}}))
	must(c.UntagResource(ctx, &batch.UntagResourceInput{ResourceArn: ce.ComputeEnvironmentArn, TagKeys: []string{"env"}}))
	if tags := must(c.ListTagsForResource(ctx, &batch.ListTagsForResourceInput{ResourceArn: ce.ComputeEnvironmentArn})).Tags; len(tags) != 1 || tags["team"] != "data" {
		t.Fatalf("tags %v", tags)
	}

	// deletes need DISABLED resources, and an environment no queue uses
	_, err = c.DeleteJobQueue(ctx, &batch.DeleteJobQueueInput{JobQueue: aws.String("q")})
	clientErr(t, err)
	must(c.UpdateComputeEnvironment(ctx, &batch.UpdateComputeEnvironmentInput{ComputeEnvironment: aws.String("ce"), State: types.CEStateDisabled}))
	_, err = c.DeleteComputeEnvironment(ctx, &batch.DeleteComputeEnvironmentInput{ComputeEnvironment: aws.String("ce")})
	clientErr(t, err)
	must(c.UpdateJobQueue(ctx, &batch.UpdateJobQueueInput{JobQueue: aws.String("q"), State: types.JQStateDisabled}))
	must(c.DeleteJobQueue(ctx, &batch.DeleteJobQueueInput{JobQueue: aws.String("q")}))
	svc.Tick(time.Now())
	must(c.DeleteComputeEnvironment(ctx, &batch.DeleteComputeEnvironmentInput{ComputeEnvironment: aws.String("ce")}))
	svc.Tick(time.Now())
	if n := len(must(c.DescribeComputeEnvironments(ctx, &batch.DescribeComputeEnvironmentsInput{})).ComputeEnvironments); n != 0 {
		t.Fatalf("%d environments left", n)
	}
	must(c.DeregisterJobDefinition(ctx, &batch.DeregisterJobDefinitionInput{JobDefinition: def.JobDefinitionArn}))
	if d := must(c.DescribeJobDefinitions(ctx, &batch.DescribeJobDefinitionsInput{JobDefinitions: []string{"def:2"}})).JobDefinitions; *d[0].Status != "INACTIVE" {
		t.Fatalf("deregistered %+v", d[0])
	}
}

// TestJobsInDocker runs jobs as containers when a container engine answers.
func TestJobsInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	svc, c, cw := setup(t, true)
	queue(t, svc, c, "dq")
	def := func(name, script string, attempts int32, timeout int32) {
		in := &batch.RegisterJobDefinitionInput{JobDefinitionName: aws.String(name), Type: types.JobDefinitionTypeContainer,
			ContainerProperties: &types.ContainerProperties{Image: aws.String("alpine:3"), Command: []string{"sh", "-c", script},
				ResourceRequirements: []types.ResourceRequirement{{Type: types.ResourceTypeVcpu, Value: aws.String("1")}, {Type: types.ResourceTypeMemory, Value: aws.String("128")}}},
			RetryStrategy: &types.RetryStrategy{Attempts: aws.Int32(attempts)}}
		if timeout > 0 {
			in.Timeout = &types.JobTimeout{AttemptDurationSeconds: aws.Int32(timeout)}
		}
		must(c.RegisterJobDefinition(ctx, in))
	}
	def("hello", `echo "hello from $AWS_BATCH_JOB_ID attempt $AWS_BATCH_JOB_ATTEMPT"; exit 0`, 1, 0)
	def("fail", `echo failing; exit 3`, 2, 0)
	def("idx", `echo "index $AWS_BATCH_JOB_ARRAY_INDEX"`, 1, 0)
	def("step", `sleep 1`, 1, 0)
	def("slow", `sleep 30`, 2, 1)
	def("nap", `sleep 30`, 1, 0)
	submit := func(name, def string, size int32, deps ...string) string {
		in := &batch.SubmitJobInput{JobName: aws.String(name), JobQueue: aws.String("dq"), JobDefinition: aws.String(def)}
		if size > 0 {
			in.ArrayProperties = &types.ArrayProperties{Size: aws.Int32(size)}
		}
		for _, d := range deps {
			in.DependsOn = append(in.DependsOn, types.JobDependency{JobId: aws.String(d)})
		}
		return *must(c.SubmitJob(ctx, in)).JobId
	}
	hello, fail, arr, slow := submit("hello", "hello", 0), submit("fail", "fail", 0), submit("arr", "idx", 3), submit("slow", "slow", 0)
	a := submit("a", "step", 0)
	b := submit("b", "step", 0, a)
	chain := submit("c", "step", 0, b)
	sleeper := submit("sleeper", "nap", 0)
	ids := []string{hello, fail, arr, slow, a, b, chain, sleeper}

	jobs := map[string]types.JobDetail{}
	terminated := false
	for end := time.Now().Add(3 * time.Minute); ; time.Sleep(300 * time.Millisecond) {
		done := 0
		for _, j := range must(c.DescribeJobs(ctx, &batch.DescribeJobsInput{Jobs: ids})).Jobs {
			jobs[*j.JobId] = j
			if j.Status == types.JobStatusSucceeded || j.Status == types.JobStatusFailed {
				done++
			}
			if *j.JobId == sleeper && j.Status == types.JobStatusRunning && !terminated {
				must(c.TerminateJob(ctx, &batch.TerminateJobInput{JobId: aws.String(sleeper), Reason: aws.String("enough")}))
				terminated = true
			}
		}
		if done == len(ids) {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("jobs did not finish: %+v", jobs)
		}
	}

	j := jobs[hello]
	if j.Status != types.JobStatusSucceeded || len(j.Attempts) != 1 || *j.Attempts[0].Container.ExitCode != 0 || !strings.HasPrefix(*j.Container.LogStreamName, "hello/default/") {
		t.Fatalf("hello %+v", j)
	}
	events := must(cw.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{LogGroupName: aws.String("/aws/batch/job"), LogStreamName: j.Container.LogStreamName})).Events
	if len(events) != 1 || *events[0].Message != "hello from "+hello+" attempt 1" {
		t.Fatalf("hello's log %+v", events)
	}
	if j := jobs[fail]; j.Status != types.JobStatusFailed || len(j.Attempts) != 2 || *j.Attempts[1].Container.ExitCode != 3 ||
		*j.StatusReason != "Essential container in task exited" || *j.Attempts[0].Container.LogStreamName == *j.Attempts[1].Container.LogStreamName {
		t.Fatalf("a failing job retries, then fails: %+v", j)
	}
	if j := jobs[arr]; j.Status != types.JobStatusSucceeded || j.ArrayProperties.StatusSummary["SUCCEEDED"] != 3 {
		t.Fatalf("array job %+v", j)
	}
	for i := range 3 {
		ch := describe(c, fmt.Sprintf("%s:%d", arr, i))
		events := must(cw.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{LogGroupName: aws.String("/aws/batch/job"), LogStreamName: ch.Container.LogStreamName})).Events
		if ch.Status != types.JobStatusSucceeded || len(events) != 1 || *events[0].Message != fmt.Sprint("index ", i) {
			t.Fatalf("child %d: %s %+v", i, ch.Status, events)
		}
	}
	if *jobs[b].StartedAt < *jobs[a].StoppedAt || *jobs[chain].StartedAt < *jobs[b].StoppedAt || jobs[chain].Status != types.JobStatusSucceeded {
		t.Fatalf("dependencies ran out of order: a %d-%d, b %d-%d, c %d", *jobs[a].StartedAt, *jobs[a].StoppedAt, *jobs[b].StartedAt, *jobs[b].StoppedAt, *jobs[chain].StartedAt)
	}
	if j := jobs[slow]; j.Status != types.JobStatusFailed || len(j.Attempts) != 1 || *j.StatusReason != "Job attempt duration exceeded timeout" {
		t.Fatalf("a timed out attempt is not retried: %+v", j)
	}
	if j := jobs[sleeper]; j.Status != types.JobStatusFailed || *j.StatusReason != "enough" || !*j.IsTerminated {
		t.Fatalf("terminated %+v", j)
	}
	list, err := d.List(ctx, docker.LabelService+"=batch")
	for _, ct := range list {
		for _, id := range ids {
			if strings.HasPrefix(ct.Labels[docker.LabelResource], id) {
				t.Fatalf("container %v left behind", ct.Names)
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}
