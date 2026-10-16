package autoscaling_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	assvc "github.com/nrndev/pogi/internal/aws/autoscaling"
	cwsvc "github.com/nrndev/pogi/internal/aws/cloudwatch"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// elbv2 stands in for ELBv2: it records the targets Auto Scaling registers.
type elbv2 struct {
	mu      sync.Mutex
	targets map[string]bool
}

func (e *elbv2) Info() core.Info {
	return core.Info{Name: "elbv2", Cloud: core.AWS, Targets: []string{"ElasticLoadBalancing_v10"}}
}

func (e *elbv2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TargetGroupArn string
		Targets        []struct{ Id string }
	}
	b, _ := io.ReadAll(r.Body)
	json.Unmarshal(b, &in)
	e.mu.Lock()
	for _, t := range in.Targets {
		e.targets[t.Id] = strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".RegisterTargets")
	}
	e.mu.Unlock()
	w.Write([]byte("{}"))
}

func (e *elbv2) registered() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var ids []string
	for id, on := range e.targets {
		if on {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

type env struct {
	hub *core.Hub
	as  *assvc.Service
	lb  *elbv2
	c   *autoscaling.Client
	ec2 *ec2.Client
	cw  *cloudwatch.Client
}

func serve(t *testing.T) *env {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // instances are metadata here; EC2's own tests run containers
	e := &env{lb: &elbv2{targets: map[string]bool{}}}
	var url string
	e.hub, url = core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) },
		func(h *core.Hub) core.Service { e.as = assvc.New(h); return e.as },
		func(h *core.Hub) core.Service { return cwsvc.New(h) }, func(*core.Hub) core.Service { return e.lb })
	cfg := awstest.Config(t, url)
	e.c, e.ec2, e.cw = autoscaling.NewFromConfig(cfg), ec2.NewFromConfig(cfg), cloudwatch.NewFromConfig(cfg)
	return e
}

func (e *env) group(t *testing.T, name string) types.AutoScalingGroup {
	t.Helper()
	gs := must(e.c.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{AutoScalingGroupNames: []string{name}})).AutoScalingGroups
	if len(gs) != 1 {
		t.Fatalf("groups = %+v", gs)
	}
	return gs[0]
}

// running lists the EC2 instances of a group that run.
func (e *env) running(t *testing.T, name string) []string {
	t.Helper()
	var ids []string
	for _, r := range must(e.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{
		{Name: aws.String("tag:aws:autoscaling:groupName"), Values: []string{name}},
		{Name: aws.String("instance-state-name"), Values: []string{"running"}}}})).Reservations {
		for _, i := range r.Instances {
			ids = append(ids, *i.InstanceId)
		}
	}
	slices.Sort(ids)
	return ids
}

func (e *env) capacity(t *testing.T, name string, want int) types.AutoScalingGroup {
	t.Helper()
	g := e.group(t, name)
	var ids []string
	for _, i := range g.Instances {
		if *i.HealthStatus != "Healthy" || i.LifecycleState != types.LifecycleStateInService {
			t.Fatalf("instance = %+v", i)
		}
		ids = append(ids, *i.InstanceId)
	}
	slices.Sort(ids)
	if *g.DesiredCapacity != int32(want) || len(ids) != want || !slices.Equal(ids, e.running(t, name)) || !slices.Equal(ids, e.lb.registered()) {
		t.Fatalf("want %d: desired %d, group %v, running %v, registered %v", want, *g.DesiredCapacity, ids, e.running(t, name), e.lb.registered())
	}
	return g
}

func TestGroupScaling(t *testing.T) {
	e := serve(t)
	lt := must(e.ec2.CreateLaunchTemplate(ctx, &ec2.CreateLaunchTemplateInput{LaunchTemplateName: aws.String("web"),
		LaunchTemplateData: &ec2types.RequestLaunchTemplateData{ImageId: aws.String("ami-0a1b2c3d4e5f60006"), InstanceType: ec2types.InstanceTypeT3Micro}})).LaunchTemplate
	tg := "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/web/0123456789abcdef"
	must(e.c.CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{AutoScalingGroupName: aws.String("web"),
		MinSize: aws.Int32(1), MaxSize: aws.Int32(4), DesiredCapacity: aws.Int32(2), AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
		LaunchTemplate: &types.LaunchTemplateSpecification{LaunchTemplateId: lt.LaunchTemplateId, Version: aws.String("$Latest")}, TargetGroupARNs: []string{tg},
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("web"), PropagateAtLaunch: aws.Bool(true)}}}))
	g := e.capacity(t, "web", 2)
	if *g.Instances[0].AvailabilityZone == *g.Instances[1].AvailabilityZone || *g.LaunchTemplate.Version != "$Latest" || *g.HealthCheckType != "EC2" ||
		*g.DefaultCooldown != 300 || g.TerminationPolicies[0] != "Default" || !strings.Contains(*g.AutoScalingGroupARN, ":autoScalingGroupName/web") {
		t.Fatalf("group = %+v", g)
	}
	tagged := must(e.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{{Name: aws.String("tag:team"), Values: []string{"web"}}}}))
	if len(tagged.Reservations) != 2 {
		t.Fatalf("propagated tags = %+v", tagged.Reservations)
	}
	_, err := e.c.CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{AutoScalingGroupName: aws.String("web"),
		MinSize: aws.Int32(1), MaxSize: aws.Int32(4), AvailabilityZones: []string{"us-east-1a"},
		LaunchTemplate: &types.LaunchTemplateSpecification{LaunchTemplateId: lt.LaunchTemplateId}})
	wantCode(t, err, "AlreadyExists")

	// simple scaling, by hand
	out := must(e.c.PutScalingPolicy(ctx, &autoscaling.PutScalingPolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("out"),
		AdjustmentType: aws.String("ChangeInCapacity"), ScalingAdjustment: aws.Int32(1)}))
	must(e.c.PutScalingPolicy(ctx, &autoscaling.PutScalingPolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("in"),
		AdjustmentType: aws.String("ChangeInCapacity"), ScalingAdjustment: aws.Int32(-1)}))
	if !strings.Contains(*out.PolicyARN, ":scalingPolicy:") || !strings.HasSuffix(*out.PolicyARN, ":autoScalingGroupName/web:policyName/out") {
		t.Fatalf("policy ARN = %s", *out.PolicyARN)
	}
	must(e.c.ExecutePolicy(ctx, &autoscaling.ExecutePolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("out")}))
	e.capacity(t, "web", 3)
	must(e.c.ExecutePolicy(ctx, &autoscaling.ExecutePolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: out.PolicyARN}))
	e.capacity(t, "web", 4)
	must(e.c.ExecutePolicy(ctx, &autoscaling.ExecutePolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("out")}))
	e.capacity(t, "web", 4) // max
	must(e.c.ExecutePolicy(ctx, &autoscaling.ExecutePolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("in")}))
	must(e.c.ExecutePolicy(ctx, &autoscaling.ExecutePolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("in")}))
	e.capacity(t, "web", 2)

	// step scaling, run by a CloudWatch alarm
	step := must(e.c.PutScalingPolicy(ctx, &autoscaling.PutScalingPolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("step"),
		PolicyType: aws.String("StepScaling"), AdjustmentType: aws.String("ChangeInCapacity"), StepAdjustments: []types.StepAdjustment{
			{MetricIntervalLowerBound: aws.Float64(0), MetricIntervalUpperBound: aws.Float64(20), ScalingAdjustment: aws.Int32(1)},
			{MetricIntervalLowerBound: aws.Float64(20), ScalingAdjustment: aws.Int32(2)}}}))
	must(e.cw.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("cpu-high"), Namespace: aws.String("AWS/EC2"),
		MetricName: aws.String("CPUUtilization"), Statistic: cwtypes.StatisticAverage, Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1),
		Threshold: aws.Float64(70), ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold, AlarmActions: []string{*step.PolicyARN}}))
	must(e.cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("cpu-high"), StateValue: cwtypes.StateValueAlarm,
		StateReason: aws.String("test")}))
	e.capacity(t, "web", 3)
	if p := must(e.c.DescribePolicies(ctx, &autoscaling.DescribePoliciesInput{AutoScalingGroupName: aws.String("web"),
		PolicyNames: []string{"step"}})).ScalingPolicies; len(p) != 1 || *p[0].MetricAggregationType != "Average" || len(p[0].StepAdjustments) != 2 {
		t.Fatalf("policies = %+v", p)
	}

	// target tracking makes its alarms; deleting the policy removes them
	tt := must(e.c.PutScalingPolicy(ctx, &autoscaling.PutScalingPolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("cpu"),
		PolicyType: aws.String("TargetTrackingScaling"), TargetTrackingConfiguration: &types.TargetTrackingConfiguration{TargetValue: aws.Float64(50),
			PredefinedMetricSpecification: &types.PredefinedMetricSpecification{PredefinedMetricType: types.MetricTypeASGAverageCPUUtilization}}}))
	alarms := must(e.cw.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNamePrefix: aws.String("TargetTracking-web-")})).MetricAlarms
	if len(tt.Alarms) != 2 || len(alarms) != 2 || alarms[0].AlarmActions[0] != *tt.PolicyARN || *alarms[0].Dimensions[0].Value != "web" {
		t.Fatalf("tracking alarms = %+v / %+v", tt.Alarms, alarms)
	}
	high := *tt.Alarms[0].AlarmName
	if !strings.Contains(high, "AlarmHigh") {
		high = *tt.Alarms[1].AlarmName
	}
	must(e.cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: &high, StateValue: cwtypes.StateValueAlarm, StateReason: aws.String("test")}))
	e.capacity(t, "web", 4)
	must(e.c.DeletePolicy(ctx, &autoscaling.DeletePolicyInput{AutoScalingGroupName: aws.String("web"), PolicyName: aws.String("cpu")}))
	if a := must(e.cw.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNamePrefix: aws.String("TargetTracking-web-")})).MetricAlarms; len(a) != 0 {
		t.Fatalf("alarms left = %+v", a)
	}

	// an instance that stops running is replaced
	must(e.c.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{AutoScalingGroupName: aws.String("web"), DesiredCapacity: aws.Int32(2)}))
	victim := e.capacity(t, "web", 2).Instances[0].InstanceId
	must(e.ec2.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{*victim}}))
	e.as.Tick(time.Now())
	if g := e.capacity(t, "web", 2); slices.ContainsFunc(g.Instances, func(i types.Instance) bool { return *i.InstanceId == *victim }) {
		t.Fatalf("terminated instance still in the group: %+v", g.Instances)
	}
	_, err = e.c.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{AutoScalingGroupName: aws.String("web"), DesiredCapacity: aws.Int32(9)})
	wantCode(t, err, "ValidationError")

	// a scheduled action
	at := time.Now().Add(time.Hour).UTC()
	must(e.c.PutScheduledUpdateGroupAction(ctx, &autoscaling.PutScheduledUpdateGroupActionInput{AutoScalingGroupName: aws.String("web"),
		ScheduledActionName: aws.String("peak"), StartTime: &at, DesiredCapacity: aws.Int32(3)}))
	e.as.Tick(time.Now())
	e.capacity(t, "web", 2)
	e.as.Tick(at.Add(time.Minute))
	e.capacity(t, "web", 3)
	if a := must(e.c.DescribeScheduledActions(ctx, &autoscaling.DescribeScheduledActionsInput{AutoScalingGroupName: aws.String("web")})); len(a.ScheduledUpdateGroupActions) != 0 {
		t.Fatalf("a one-time action stays: %+v", a.ScheduledUpdateGroupActions)
	}
	must(e.c.PutScheduledUpdateGroupAction(ctx, &autoscaling.PutScheduledUpdateGroupActionInput{AutoScalingGroupName: aws.String("web"),
		ScheduledActionName: aws.String("weekdays"), Recurrence: aws.String("*/5 * * * 1-5,0"), MinSize: aws.Int32(2), DesiredCapacity: aws.Int32(2)}))
	e.as.Tick(time.Now().Add(6 * time.Minute))
	if g := e.capacity(t, "web", 2); *g.MinSize != 2 {
		t.Fatalf("recurring action: min %d", *g.MinSize)
	}
	if a := must(e.c.DescribeScheduledActions(ctx, &autoscaling.DescribeScheduledActionsInput{AutoScalingGroupName: aws.String("web")})); len(a.ScheduledUpdateGroupActions) != 1 {
		t.Fatalf("a recurring action goes: %+v", a.ScheduledUpdateGroupActions)
	}

	acts := must(e.c.DescribeScalingActivities(ctx, &autoscaling.DescribeScalingActivitiesInput{AutoScalingGroupName: aws.String("web")})).Activities
	if !slices.ContainsFunc(acts, func(a types.Activity) bool {
		return a.StatusCode == types.ScalingActivityStatusCodeSuccessful && strings.HasPrefix(*a.Description, "Launching a new EC2 instance: i-")
	}) || !strings.HasPrefix(*acts[0].Description, "Terminating EC2 instance: i-") {
		t.Fatalf("activities = %+v", acts[:min(len(acts), 3)])
	}
	roundTrip(t, e.hub)
	e.capacity(t, "web", 2)

	// Terraform's destroy: drain, then delete
	_, err = e.c.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{AutoScalingGroupName: aws.String("web")})
	wantCode(t, err, "ResourceInUse")
	must(e.c.UpdateAutoScalingGroup(ctx, &autoscaling.UpdateAutoScalingGroupInput{AutoScalingGroupName: aws.String("web"),
		MinSize: aws.Int32(0), MaxSize: aws.Int32(0), DesiredCapacity: aws.Int32(0)}))
	e.capacity(t, "web", 0)
	must(e.c.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{AutoScalingGroupName: aws.String("web")}))
	if gs := must(e.c.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{})).AutoScalingGroups; len(gs) != 0 {
		t.Fatalf("groups = %+v", gs)
	}
}

func TestLaunchConfigurationAndInstances(t *testing.T) {
	e := serve(t)
	must(e.c.CreateLaunchConfiguration(ctx, &autoscaling.CreateLaunchConfigurationInput{LaunchConfigurationName: aws.String("lc"),
		ImageId: aws.String("ami-0a1b2c3d4e5f60002"), InstanceType: aws.String("t3.small")}))
	must(e.c.CreateAutoScalingGroup(ctx, &autoscaling.CreateAutoScalingGroupInput{AutoScalingGroupName: aws.String("app"), LaunchConfigurationName: aws.String("lc"),
		MinSize: aws.Int32(1), MaxSize: aws.Int32(3), AvailabilityZones: []string{"us-east-1c"}}))
	g := e.group(t, "app")
	if len(g.Instances) != 1 || *g.Instances[0].InstanceType != "t3.small" || *g.Instances[0].LaunchConfigurationName != "lc" ||
		*g.Instances[0].AvailabilityZone != "us-east-1c" {
		t.Fatalf("group = %+v", g)
	}
	_, err := e.c.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{LaunchConfigurationName: aws.String("lc")})
	wantCode(t, err, "ResourceInUse")

	// an instance of its own joins, then leaves; a terminated member is replaced unless the group shrinks
	run := must(e.ec2.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String("ami-0a1b2c3d4e5f60002"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1)}))
	own := *run.Instances[0].InstanceId
	must(e.c.AttachInstances(ctx, &autoscaling.AttachInstancesInput{AutoScalingGroupName: aws.String("app"), InstanceIds: []string{own}}))
	if g := e.group(t, "app"); *g.DesiredCapacity != 2 || len(g.Instances) != 2 {
		t.Fatalf("after attach = %+v", g)
	}
	if d := must(e.c.DescribeAutoScalingInstances(ctx, &autoscaling.DescribeAutoScalingInstancesInput{InstanceIds: []string{own}})).AutoScalingInstances; len(d) != 1 ||
		*d[0].AutoScalingGroupName != "app" {
		t.Fatalf("instances = %+v", d)
	}
	must(e.c.DetachInstances(ctx, &autoscaling.DetachInstancesInput{AutoScalingGroupName: aws.String("app"), InstanceIds: []string{own},
		ShouldDecrementDesiredCapacity: aws.Bool(true)}))
	member := *e.group(t, "app").Instances[0].InstanceId
	must(e.c.TerminateInstanceInAutoScalingGroup(ctx, &autoscaling.TerminateInstanceInAutoScalingGroupInput{InstanceId: &member,
		ShouldDecrementDesiredCapacity: aws.Bool(false)}))
	if g := e.group(t, "app"); len(g.Instances) != 1 || *g.Instances[0].InstanceId == member {
		t.Fatalf("after terminate = %+v", g.Instances)
	}
	must(e.c.SuspendProcesses(ctx, &autoscaling.SuspendProcessesInput{AutoScalingGroupName: aws.String("app"), ScalingProcesses: []string{"Launch"}}))
	must(e.c.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{AutoScalingGroupName: aws.String("app"), DesiredCapacity: aws.Int32(3)}))
	if g := e.group(t, "app"); len(g.Instances) != 1 || len(g.SuspendedProcesses) != 1 {
		t.Fatalf("suspended launch still launched: %+v", g)
	}
	must(e.c.ResumeProcesses(ctx, &autoscaling.ResumeProcessesInput{AutoScalingGroupName: aws.String("app")}))
	e.as.Tick(time.Now())
	if g := e.group(t, "app"); len(g.Instances) != 3 {
		t.Fatalf("after resume = %+v", g.Instances)
	}
	must(e.c.PutLifecycleHook(ctx, &autoscaling.PutLifecycleHookInput{AutoScalingGroupName: aws.String("app"), LifecycleHookName: aws.String("drain"),
		LifecycleTransition: aws.String("autoscaling:EC2_INSTANCE_TERMINATING")}))
	if h := must(e.c.DescribeLifecycleHooks(ctx, &autoscaling.DescribeLifecycleHooksInput{AutoScalingGroupName: aws.String("app")})).LifecycleHooks; len(h) != 1 ||
		*h[0].HeartbeatTimeout != 3600 || *h[0].DefaultResult != "ABANDON" {
		t.Fatalf("hooks = %+v", h)
	}
	must(e.c.PutWarmPool(ctx, &autoscaling.PutWarmPoolInput{AutoScalingGroupName: aws.String("app"), MinSize: aws.Int32(1)}))
	if w := must(e.c.DescribeWarmPool(ctx, &autoscaling.DescribeWarmPoolInput{AutoScalingGroupName: aws.String("app")})); w.WarmPoolConfiguration.PoolState != types.WarmPoolStateStopped {
		t.Fatalf("warm pool = %+v", w.WarmPoolConfiguration)
	}
	must(e.c.CreateOrUpdateTags(ctx, &autoscaling.CreateOrUpdateTagsInput{Tags: []types.Tag{{ResourceId: aws.String("app"),
		ResourceType: aws.String("auto-scaling-group"), Key: aws.String("env"), Value: aws.String("dev")}}}))
	if tags := must(e.c.DescribeTags(ctx, &autoscaling.DescribeTagsInput{Filters: []types.Filter{{Name: aws.String("key"), Values: []string{"env"}}}})).Tags; len(tags) != 1 ||
		*tags[0].Value != "dev" {
		t.Fatalf("tags = %+v", tags)
	}
	if l := must(e.c.DescribeAccountLimits(ctx, &autoscaling.DescribeAccountLimitsInput{})); *l.NumberOfAutoScalingGroups != 1 || *l.NumberOfLaunchConfigurations != 1 {
		t.Fatalf("limits = %+v", l)
	}
	must(e.c.DeleteAutoScalingGroup(ctx, &autoscaling.DeleteAutoScalingGroupInput{AutoScalingGroupName: aws.String("app"), ForceDelete: aws.Bool(true)}))
	if ids := e.running(t, "app"); len(ids) != 0 {
		t.Fatalf("instances survive their group: %v", ids)
	}
	must(e.c.DeleteLaunchConfiguration(ctx, &autoscaling.DeleteLaunchConfigurationInput{LaunchConfigurationName: aws.String("lc")}))
}

// roundTrip snapshots, resets and restores the hub: state must survive Save and Load.
func roundTrip(t *testing.T, h *core.Hub) {
	t.Helper()
	b, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if err := h.Restore(b); err != nil {
		t.Fatal(err)
	}
}
