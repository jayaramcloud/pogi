package ecs_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// A CODE_DEPLOY service starts with a PRIMARY task set; more task sets scale,
// take over as PRIMARY and drain away; other services refuse task sets.
func TestTaskSets(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := setup(t)
	must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("c")}))
	td := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("app"), Image: aws.String("alpine"), Memory: aws.Int32(64)}}})).TaskDefinition
	cl, svc := aws.String("c"), aws.String("s")
	must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: cl, ServiceName: svc, TaskDefinition: td.TaskDefinitionArn, DesiredCount: aws.Int32(2),
		DeploymentController: &types.DeploymentController{Type: types.DeploymentControllerTypeCodeDeploy}}))
	sets := func() []types.TaskSet {
		return must(c.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: cl, Services: []string{"s"}})).Services[0].TaskSets
	}
	waitSets := func(what string, ok func([]types.TaskSet) bool) []types.TaskSet {
		t.Helper()
		for end := time.Now().Add(20 * time.Second); ; time.Sleep(100 * time.Millisecond) {
			if s := sets(); ok(s) {
				return s
			}
			if time.Now().After(end) {
				t.Fatalf("timed out waiting for %s: %+v", what, sets())
			}
		}
	}
	first := waitSets("a steady primary", func(s []types.TaskSet) bool {
		return len(s) == 1 && s[0].RunningCount == 2 && s[0].StabilityStatus == types.StabilityStatusSteadyState
	})[0]
	if first.Status == nil || *first.Status != "PRIMARY" || first.ComputedDesiredCount != 2 || len(serviceTasks(t, c, "c", "s")) != 2 {
		t.Fatalf("first set %+v", first)
	}

	// a replacement starts at 0%, then scales
	next := must(c.CreateTaskSet(ctx, &ecs.CreateTaskSetInput{Cluster: cl, Service: svc, TaskDefinition: td.TaskDefinitionArn,
		Scale: &types.Scale{Value: 0, Unit: types.ScaleUnitPercent}})).TaskSet
	if *next.Status != "ACTIVE" || next.ComputedDesiredCount != 0 {
		t.Fatalf("replacement %+v", next)
	}
	must(c.UpdateTaskSet(ctx, &ecs.UpdateTaskSetInput{Cluster: cl, Service: svc, TaskSet: next.Id, Scale: &types.Scale{Value: 50, Unit: types.ScaleUnitPercent}}))
	waitSets("half the replacement", func(s []types.TaskSet) bool {
		return len(s) == 2 && s[1].RunningCount == 1 && s[1].StabilityStatus == types.StabilityStatusSteadyState
	})
	must(c.UpdateServicePrimaryTaskSet(ctx, &ecs.UpdateServicePrimaryTaskSetInput{Cluster: cl, Service: svc, PrimaryTaskSet: next.TaskSetArn}))
	if s := sets(); *s[0].Status != "ACTIVE" || *s[1].Status != "PRIMARY" {
		t.Fatalf("after the switch %s %s", *s[0].Status, *s[1].Status)
	}

	// deleting a task set that still has tasks needs force; its tasks stop and it goes away
	_, err := c.DeleteTaskSet(ctx, &ecs.DeleteTaskSetInput{Cluster: cl, Service: svc, TaskSet: first.Id})
	wantErr(t, err, "InvalidParameterException")
	gone := must(c.DeleteTaskSet(ctx, &ecs.DeleteTaskSetInput{Cluster: cl, Service: svc, TaskSet: first.Id, Force: aws.Bool(true)})).TaskSet
	if *gone.Status != "DRAINING" {
		t.Fatalf("deleted set %s", *gone.Status)
	}
	waitSets("the old set to drain away", func(s []types.TaskSet) bool { return len(s) == 1 && *s[0].Id == *next.Id })
	_, err = c.UpdateTaskSet(ctx, &ecs.UpdateTaskSetInput{Cluster: cl, Service: svc, TaskSet: first.Id, Scale: &types.Scale{Value: 10, Unit: types.ScaleUnitPercent}})
	wantErr(t, err, "TaskSetNotFoundException")
	_, err = c.CreateTaskSet(ctx, &ecs.CreateTaskSetInput{Cluster: cl, Service: svc, TaskDefinition: td.TaskDefinitionArn, Scale: &types.Scale{Value: 101, Unit: types.ScaleUnitPercent}})
	wantErr(t, err, "InvalidParameterException")
	_, err = c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: cl, Service: svc, TaskDefinition: td.TaskDefinitionArn})
	wantErr(t, err, "InvalidParameterException")

	// a service on the ECS controller has none
	must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: cl, ServiceName: aws.String("plain"), TaskDefinition: td.TaskDefinitionArn, DesiredCount: aws.Int32(0)}))
	_, err = c.CreateTaskSet(ctx, &ecs.CreateTaskSetInput{Cluster: cl, Service: aws.String("plain"), TaskDefinition: td.TaskDefinitionArn})
	wantErr(t, err, "InvalidParameterException")

	must(c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: cl, Service: svc, Force: aws.Bool(true)}))
}
