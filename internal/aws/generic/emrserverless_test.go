package generic_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestEMRServerless(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := emrserverless.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	a, err := c.CreateApplication(ctx, &emrserverless.CreateApplicationInput{Name: aws.String("spark1"), ReleaseLabel: aws.String("emr-7.1.0"), Type: aws.String("SPARK"),
		ClientToken: aws.String("tok1"), Tags: map[string]string{"team": "data"}})
	if err != nil || aws.ToString(a.ApplicationId) == "" || aws.ToString(a.Arn) == "" {
		t.Fatalf("create: %v %+v", err, a)
	}
	id := aws.ToString(a.ApplicationId)
	state := func() types.ApplicationState {
		g, err := c.GetApplication(ctx, &emrserverless.GetApplicationInput{ApplicationId: aws.String(id)})
		if err != nil {
			t.Fatal(err)
		}
		return g.Application.State
	}
	if st := state(); st != types.ApplicationStateCreating {
		t.Fatalf("state %s", st)
	}
	lifecycle(t, 0)
	g, _ := c.GetApplication(ctx, &emrserverless.GetApplicationInput{ApplicationId: aws.String(id)})
	if g.Application.State != types.ApplicationStateCreated || aws.ToString(g.Application.Name) != "spark1" || aws.ToString(g.Application.ReleaseLabel) != "emr-7.1.0" || g.Application.Tags["team"] != "data" {
		t.Fatalf("created: %+v", g.Application)
	}
	if _, err := c.UpdateApplication(ctx, &emrserverless.UpdateApplicationInput{ApplicationId: aws.String(id), ClientToken: aws.String("tok2"), MaximumCapacity: &types.MaximumAllowedResources{Cpu: aws.String("8 vCPU"), Memory: aws.String("32 GB")}}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.GetApplication(ctx, &emrserverless.GetApplicationInput{ApplicationId: aws.String(id)}); g.Application.MaximumCapacity == nil || aws.ToString(g.Application.MaximumCapacity.Cpu) != "8 vCPU" {
		t.Fatalf("updated: %+v", g.Application)
	}
	if l, err := c.ListApplications(ctx, &emrserverless.ListApplicationsInput{}); err != nil || len(l.Applications) != 1 || aws.ToString(l.Applications[0].Id) != id {
		t.Fatalf("list: %v %+v", err, l)
	}

	// start and stop
	lifecycle(t, time.Hour)
	if _, err := c.StartApplication(ctx, &emrserverless.StartApplicationInput{ApplicationId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	if st := state(); st != types.ApplicationStateStarting {
		t.Fatalf("starting: %s", st)
	}
	lifecycle(t, 0)
	if st := state(); st != types.ApplicationStateStarted {
		t.Fatalf("started: %s", st)
	}

	// job runs reach SUCCESS
	lifecycle(t, time.Hour)
	jr, err := c.StartJobRun(ctx, &emrserverless.StartJobRunInput{ApplicationId: aws.String(id), ClientToken: aws.String("j1"), Name: aws.String("etl"),
		ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/emr"), Tags: map[string]string{"k": "v"},
		JobDriver: &types.JobDriverMemberSparkSubmit{Value: types.SparkSubmit{EntryPoint: aws.String("s3://code/job.py")}}})
	if err != nil || aws.ToString(jr.JobRunId) == "" {
		t.Fatalf("start job: %v %+v", err, jr)
	}
	get := func() *types.JobRun {
		g, err := c.GetJobRun(ctx, &emrserverless.GetJobRunInput{ApplicationId: aws.String(id), JobRunId: jr.JobRunId})
		if err != nil {
			t.Fatal(err)
		}
		return g.JobRun
	}
	if j := get(); j.State != types.JobRunStateSubmitted || aws.ToString(j.Name) != "etl" || j.Tags["k"] != "v" {
		t.Fatalf("submitted: %+v", j)
	}
	lifecycle(t, 0)
	if j := get(); j.State != types.JobRunStateSuccess || aws.ToString(j.ExecutionRole) != "arn:aws:iam::000000000000:role/emr" {
		t.Fatalf("success: %+v", j)
	}
	if l, err := c.ListJobRuns(ctx, &emrserverless.ListJobRunsInput{ApplicationId: aws.String(id), States: []types.JobRunState{types.JobRunStateSuccess}}); err != nil || len(l.JobRuns) != 1 {
		t.Fatalf("list jobs: %v %+v", err, l)
	}
	jr2, _ := c.StartJobRun(ctx, &emrserverless.StartJobRunInput{ApplicationId: aws.String(id), ClientToken: aws.String("j2"), ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/emr")})
	lifecycle(t, time.Hour)
	jr3, _ := c.StartJobRun(ctx, &emrserverless.StartJobRunInput{ApplicationId: aws.String(id), ClientToken: aws.String("j3"), ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/emr")})
	if _, err := c.CancelJobRun(ctx, &emrserverless.CancelJobRunInput{ApplicationId: aws.String(id), JobRunId: jr3.JobRunId}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.GetJobRun(ctx, &emrserverless.GetJobRunInput{ApplicationId: aws.String(id), JobRunId: jr3.JobRunId}); g.JobRun.State != types.JobRunStateCancelling {
		t.Fatalf("cancelling: %+v", g.JobRun)
	}
	_ = jr2

	// tags, stop, delete, not found
	arn := aws.ToString(a.Arn)
	if _, err := c.TagResource(ctx, &emrserverless.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTagsForResource(ctx, &emrserverless.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(l.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, l)
	}
	if _, err := c.UntagResource(ctx, &emrserverless.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.StopApplication(ctx, &emrserverless.StopApplicationInput{ApplicationId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	if st := state(); st != types.ApplicationStateStopping {
		t.Fatalf("stopping: %s", st)
	}
	lifecycle(t, 0)
	if st := state(); st != types.ApplicationStateStopped {
		t.Fatalf("stopped: %s", st)
	}
	if _, err := c.DeleteApplication(ctx, &emrserverless.DeleteApplicationInput{ApplicationId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetApplication(ctx, &emrserverless.GetApplicationInput{ApplicationId: aws.String(id)})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("get deleted: %v", err)
	}
	if _, err := c.StartJobRun(ctx, &emrserverless.StartJobRunInput{ApplicationId: aws.String(id), ClientToken: aws.String("j4"), ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/x")}); !errors.As(err, &nf) {
		t.Fatalf("job on deleted app: %v", err)
	}
}
