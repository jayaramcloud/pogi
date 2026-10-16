package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestEMR(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := emr.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	step := func(name string) types.StepConfig {
		return types.StepConfig{Name: aws.String(name), ActionOnFailure: types.ActionOnFailureContinue,
			HadoopJarStep: &types.HadoopJarStepConfig{Jar: aws.String("command-runner.jar"), Args: []string{"spark-submit", "job.py"}}}
	}
	run, err := c.RunJobFlow(ctx, &emr.RunJobFlowInput{Name: aws.String("etl"), ReleaseLabel: aws.String("emr-7.1.0"), ServiceRole: aws.String("EMR_DefaultRole"), JobFlowRole: aws.String("EMR_EC2_DefaultRole"),
		Applications: []types.Application{{Name: aws.String("Spark")}}, Steps: []types.StepConfig{step("first")},
		Instances: &types.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true), MasterInstanceType: aws.String("m5.xlarge"), SlaveInstanceType: aws.String("m5.xlarge"), InstanceCount: aws.Int32(3)},
		Tags:      []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}}})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(run.JobFlowId)
	if !strings.HasPrefix(id, "j-") || !strings.Contains(aws.ToString(run.ClusterArn), ":cluster/"+id) {
		t.Fatalf("run: %+v", run)
	}
	cl := func() *types.Cluster {
		d, err := c.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(id)})
		if err != nil {
			t.Fatal(err)
		}
		return d.Cluster
	}
	// STARTING with a PENDING step; then WAITING with a COMPLETED one
	if st := cl().Status.State; st != types.ClusterStateStarting {
		t.Fatalf("state %s", st)
	}
	ls, err := c.ListSteps(ctx, &emr.ListStepsInput{ClusterId: aws.String(id)})
	if err != nil || len(ls.Steps) != 1 || ls.Steps[0].Status.State != types.StepStatePending || aws.ToString(ls.Steps[0].Name) != "first" {
		t.Fatalf("steps: %v %+v", err, ls)
	}
	lifecycle(t, 0)
	got := cl()
	if got.Status.State != types.ClusterStateWaiting || aws.ToString(got.Name) != "etl" || aws.ToString(got.ReleaseLabel) != "emr-7.1.0" || len(got.Applications) != 1 ||
		len(got.Tags) != 1 || aws.ToBool(got.AutoTerminate) || aws.ToString(got.MasterPublicDnsName) == "" {
		t.Fatalf("waiting: %+v", got)
	}
	ls, _ = c.ListSteps(ctx, &emr.ListStepsInput{ClusterId: aws.String(id), StepStates: []types.StepState{types.StepStateCompleted}})
	if len(ls.Steps) != 1 {
		t.Fatalf("completed steps: %+v", ls)
	}
	// more steps; cancel one while it is pending
	lifecycle(t, time.Hour)
	add, err := c.AddJobFlowSteps(ctx, &emr.AddJobFlowStepsInput{JobFlowId: aws.String(id), Steps: []types.StepConfig{step("second")}})
	if err != nil || len(add.StepIds) != 1 {
		t.Fatalf("add steps: %v %+v", err, add)
	}
	if st, err := c.DescribeStep(ctx, &emr.DescribeStepInput{ClusterId: aws.String(id), StepId: aws.String(add.StepIds[0])}); err != nil || st.Step.Status.State != types.StepStatePending ||
		aws.ToString(st.Step.Config.Jar) != "command-runner.jar" {
		t.Fatalf("describe step: %v %+v", err, st)
	}
	cs, err := c.CancelSteps(ctx, &emr.CancelStepsInput{ClusterId: aws.String(id), StepIds: add.StepIds})
	if err != nil || len(cs.CancelStepsInfoList) != 1 || cs.CancelStepsInfoList[0].Status != types.CancelStepsRequestStatusSubmitted {
		t.Fatalf("cancel: %v %+v", err, cs)
	}
	lifecycle(t, 0)
	if st, _ := c.DescribeStep(ctx, &emr.DescribeStepInput{ClusterId: aws.String(id), StepId: aws.String(add.StepIds[0])}); st.Step.Status.State != types.StepStateCancelled {
		t.Fatalf("canceled step: %+v", st)
	}
	// groups, tags, list
	ig, err := c.ListInstanceGroups(ctx, &emr.ListInstanceGroupsInput{ClusterId: aws.String(id)})
	if err != nil || len(ig.InstanceGroups) != 2 || aws.ToInt32(ig.InstanceGroups[1].RunningInstanceCount) != 2 {
		t.Fatalf("groups: %v %+v", err, ig)
	}
	if _, err := c.AddTags(ctx, &emr.AddTagsInput{ResourceId: aws.String(id), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveTags(ctx, &emr.RemoveTagsInput{ResourceId: aws.String(id), TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if tg := cl().Tags; len(tg) != 1 || aws.ToString(tg[0].Key) != "env" {
		t.Fatalf("tags: %+v", tg)
	}
	lc, err := c.ListClusters(ctx, &emr.ListClustersInput{ClusterStates: []types.ClusterState{types.ClusterStateWaiting}})
	if err != nil || len(lc.Clusters) != 1 || aws.ToString(lc.Clusters[0].Id) != id {
		t.Fatalf("list: %v %+v", err, lc)
	}
	// termination protection, then terminate
	c.SetTerminationProtection(ctx, &emr.SetTerminationProtectionInput{JobFlowIds: []string{id}, TerminationProtected: aws.Bool(true)})
	if _, err := c.TerminateJobFlows(ctx, &emr.TerminateJobFlowsInput{JobFlowIds: []string{id}}); err == nil || !strings.Contains(err.Error(), "termination protected") {
		t.Fatalf("protected terminate: %v", err)
	}
	c.SetTerminationProtection(ctx, &emr.SetTerminationProtectionInput{JobFlowIds: []string{id}, TerminationProtected: aws.Bool(false)})
	if _, err := c.TerminateJobFlows(ctx, &emr.TerminateJobFlowsInput{JobFlowIds: []string{id}}); err != nil {
		t.Fatal(err)
	}
	if st := cl().Status.State; st != types.ClusterStateTerminated {
		t.Fatalf("after terminate: %s", st)
	}
	if lc, _ := c.ListClusters(ctx, &emr.ListClustersInput{ClusterStates: []types.ClusterState{types.ClusterStateWaiting}}); len(lc.Clusters) != 0 {
		t.Fatalf("terminated cluster still waiting: %+v", lc)
	}
	// a cluster that does not stay alive ends when its steps are done
	r2, _ := c.RunJobFlow(ctx, &emr.RunJobFlowInput{Name: aws.String("batch"), ReleaseLabel: aws.String("emr-7.1.0"), Steps: []types.StepConfig{step("only")},
		Instances: &types.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(false), MasterInstanceType: aws.String("m5.xlarge")}})
	if d, _ := c.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: r2.JobFlowId}); d.Cluster.Status.State != types.ClusterStateTerminated || d.Cluster.Status.StateChangeReason.Code != types.ClusterStateChangeReasonCodeAllStepsCompleted {
		t.Fatalf("transient cluster: %+v", d.Cluster.Status)
	}
	// typed not-found
	_, err = c.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String("j-NOPE")})
	var ire *types.InvalidRequestException
	if !errors.As(err, &ire) {
		t.Fatalf("unknown cluster: %v", err)
	}

	// security configurations: create, describe, list, delete
	if _, err := c.CreateSecurityConfiguration(ctx, &emr.CreateSecurityConfigurationInput{Name: aws.String("sc1"), SecurityConfiguration: aws.String(`{"EncryptionConfiguration":{"EnableInTransitEncryption":false}}`)}); err != nil {
		t.Fatal(err)
	}
	d, err := c.DescribeSecurityConfiguration(ctx, &emr.DescribeSecurityConfigurationInput{Name: aws.String("sc1")})
	if err != nil || !strings.Contains(aws.ToString(d.SecurityConfiguration), "EncryptionConfiguration") {
		t.Fatalf("describe sc: %v %+v", err, d)
	}
	if l, err := c.ListSecurityConfigurations(ctx, &emr.ListSecurityConfigurationsInput{}); err != nil || len(l.SecurityConfigurations) != 1 {
		t.Fatalf("list sc: %v %+v", err, l)
	}
	if _, err := c.DeleteSecurityConfiguration(ctx, &emr.DeleteSecurityConfigurationInput{Name: aws.String("sc1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeSecurityConfiguration(ctx, &emr.DescribeSecurityConfigurationInput{Name: aws.String("sc1")}); !errors.As(err, &ire) {
		t.Fatalf("deleted sc: %v", err)
	}
}
