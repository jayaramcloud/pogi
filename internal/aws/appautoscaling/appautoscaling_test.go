package appautoscaling_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aas "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/aws/appautoscaling"
	"github.com/nrndev/pogi/internal/aws/autoscaling"
	cwsvc "github.com/nrndev/pogi/internal/aws/cloudwatch"
	ddbsvc "github.com/nrndev/pogi/internal/aws/dynamodb"
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

// ecs stands in for ECS: one service whose desired count the calls set.
type ecs struct {
	mu      sync.Mutex
	desired int
	calls   []string // the UpdateService payloads
}

func (e *ecs) Info() core.Info {
	return core.Info{Name: "ecs", Cloud: core.AWS, Targets: []string{"AmazonEC2ContainerServiceV20141113"}}
}

func (e *ecs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	e.mu.Lock()
	defer e.mu.Unlock()
	switch r.Header.Get("X-Amz-Target") {
	case "AmazonEC2ContainerServiceV20141113.UpdateService":
		var in struct{ DesiredCount int }
		json.Unmarshal(b, &in)
		e.desired, e.calls = in.DesiredCount, append(e.calls, string(b))
		w.Write([]byte("{}"))
	default: // DescribeServices
		json.NewEncoder(w).Encode(map[string]any{"services": []map[string]any{{"serviceName": "web", "desiredCount": e.desired}}})
	}
}

func (e *ecs) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.desired
}

func TestScalableTargets(t *testing.T) {
	svc := &ecs{desired: 2}
	var app *appautoscaling.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { app = appautoscaling.New(h); return app },
		func(h *core.Hub) core.Service { return autoscaling.New(h) }, func(h *core.Hub) core.Service { return cwsvc.New(h) },
		func(h *core.Hub) core.Service { return ddbsvc.New(h) }, func(*core.Hub) core.Service { return svc })
	cfg := awstest.Config(t, url)
	c, cw := aas.NewFromConfig(cfg), cloudwatch.NewFromConfig(cfg)
	ns, id, dim := types.ServiceNamespaceEcs, aws.String("service/prod/web"), types.ScalableDimensionECSServiceDesiredCount

	_, err := c.RegisterScalableTarget(ctx, &aas.RegisterScalableTargetInput{ServiceNamespace: ns, ResourceId: id,
		ScalableDimension: types.ScalableDimensionDynamoDBTableReadCapacityUnits, MinCapacity: aws.Int32(1), MaxCapacity: aws.Int32(5)})
	wantCode(t, err, "ValidationException")
	reg := must(c.RegisterScalableTarget(ctx, &aas.RegisterScalableTargetInput{ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim,
		MinCapacity: aws.Int32(1), MaxCapacity: aws.Int32(5), Tags: map[string]string{"team": "web"}}))
	if !strings.Contains(*reg.ScalableTargetARN, ":application-autoscaling:us-east-1:000000000000:scalable-target/") {
		t.Fatalf("target ARN = %s", *reg.ScalableTargetARN)
	}
	if ts := must(c.DescribeScalableTargets(ctx, &aas.DescribeScalableTargetsInput{ServiceNamespace: ns})).ScalableTargets; len(ts) != 1 ||
		*ts[0].MaxCapacity != 5 || *ts[0].ResourceId != *id {
		t.Fatalf("targets = %+v", ts)
	}
	if tags := must(c.ListTagsForResource(ctx, &aas.ListTagsForResourceInput{ResourceARN: reg.ScalableTargetARN})).Tags; tags["team"] != "web" {
		t.Fatalf("tags = %+v", tags)
	}

	// a step policy, run by a CloudWatch alarm through Auto Scaling's deliverer
	step := must(c.PutScalingPolicy(ctx, &aas.PutScalingPolicyInput{PolicyName: aws.String("out"), ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim,
		PolicyType: types.PolicyTypeStepScaling, StepScalingPolicyConfiguration: &types.StepScalingPolicyConfiguration{
			AdjustmentType: types.AdjustmentTypeChangeInCapacity, StepAdjustments: []types.StepAdjustment{
				{MetricIntervalLowerBound: aws.Float64(0), ScalingAdjustment: aws.Int32(2)}}}}))
	if !strings.Contains(*step.PolicyARN, ":autoscaling:us-east-1:000000000000:scalingPolicy:") ||
		!strings.HasSuffix(*step.PolicyARN, ":resource/ecs/service/prod/web:policyName/out") {
		t.Fatalf("policy ARN = %s", *step.PolicyARN)
	}
	must(cw.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("busy"), Namespace: aws.String("AWS/ECS"),
		MetricName: aws.String("CPUUtilization"), Statistic: cwtypes.StatisticAverage, Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1),
		Threshold: aws.Float64(80), ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold, AlarmActions: []string{*step.PolicyARN}}))
	must(cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("busy"), StateValue: cwtypes.StateValueAlarm, StateReason: aws.String("test")}))
	if n := svc.count(); n != 4 || !strings.Contains(svc.calls[0], `"cluster":"prod"`) || !strings.Contains(svc.calls[0], `"service":"web"`) {
		t.Fatalf("desired count = %d, calls = %v", n, svc.calls)
	}
	must(cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("busy"), StateValue: cwtypes.StateValueOk, StateReason: aws.String("test")}))
	must(cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("busy"), StateValue: cwtypes.StateValueAlarm, StateReason: aws.String("test")}))
	if n := svc.count(); n != 5 {
		t.Fatalf("desired count = %d, want the max, 5", n)
	}
	acts := must(c.DescribeScalingActivities(ctx, &aas.DescribeScalingActivitiesInput{ServiceNamespace: ns, ResourceId: id})).ScalingActivities
	if len(acts) != 2 || acts[0].StatusCode != types.ScalingActivityStatusCodeSuccessful || !strings.Contains(*acts[0].Cause, "busy") {
		t.Fatalf("activities = %+v", acts)
	}

	// target tracking creates its alarms, deleting the policy removes them
	tt := must(c.PutScalingPolicy(ctx, &aas.PutScalingPolicyInput{PolicyName: aws.String("cpu"), ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim,
		PolicyType: types.PolicyTypeTargetTrackingScaling, TargetTrackingScalingPolicyConfiguration: &types.TargetTrackingScalingPolicyConfiguration{
			TargetValue: aws.Float64(60), PredefinedMetricSpecification: &types.PredefinedMetricSpecification{
				PredefinedMetricType: types.MetricTypeECSServiceAverageCPUUtilization}}}))
	alarms := must(cw.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNamePrefix: aws.String("TargetTracking-service/prod/web-")})).MetricAlarms
	if len(tt.Alarms) != 2 || len(alarms) != 2 || *alarms[0].Namespace != "AWS/ECS" || len(alarms[0].Dimensions) != 2 {
		t.Fatalf("alarms = %+v / %+v", tt.Alarms, alarms)
	}
	if ps := must(c.DescribeScalingPolicies(ctx, &aas.DescribeScalingPoliciesInput{ServiceNamespace: ns})).ScalingPolicies; len(ps) != 2 ||
		*ps[0].TargetTrackingScalingPolicyConfiguration.TargetValue != 60 {
		t.Fatalf("policies = %+v", ps)
	}
	must(c.DeleteScalingPolicy(ctx, &aas.DeleteScalingPolicyInput{PolicyName: aws.String("cpu"), ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim}))
	if a := must(cw.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNamePrefix: aws.String("TargetTracking-")})).MetricAlarms; len(a) != 0 {
		t.Fatalf("alarms left = %+v", a)
	}
	_, err = c.DeleteScalingPolicy(ctx, &aas.DeleteScalingPolicyInput{PolicyName: aws.String("cpu"), ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim})
	wantCode(t, err, "ObjectNotFoundException")

	// a scheduled action lowers the maximum: the service follows
	at := time.Now().Add(time.Hour).UTC()
	must(c.PutScheduledAction(ctx, &aas.PutScheduledActionInput{ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim,
		ScheduledActionName: aws.String("night"), Schedule: aws.String("at(" + at.Format("2006-01-02T15:04:05") + ")"),
		ScalableTargetAction: &types.ScalableTargetAction{MaxCapacity: aws.Int32(3)}}))
	app.Tick(time.Now())
	if n := svc.count(); n != 5 {
		t.Fatalf("the action ran early: %d", n)
	}
	app.Tick(at.Add(time.Minute))
	if n := svc.count(); n != 3 {
		t.Fatalf("desired count = %d after the scheduled action", n)
	}
	if as := must(c.DescribeScheduledActions(ctx, &aas.DescribeScheduledActionsInput{ServiceNamespace: ns})).ScheduledActions; len(as) != 1 ||
		*as[0].ScalableTargetAction.MaxCapacity != 3 {
		t.Fatalf("scheduled actions = %+v", as)
	}

	// DynamoDB capacity goes through UpdateTable
	db := dynamodb.NewFromConfig(cfg)
	must(db.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("orders"),
		AttributeDefinitions:  []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:             []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5)}}))
	must(c.RegisterScalableTarget(ctx, &aas.RegisterScalableTargetInput{ServiceNamespace: types.ServiceNamespaceDynamodb, ResourceId: aws.String("table/orders"),
		ScalableDimension: types.ScalableDimensionDynamoDBTableWriteCapacityUnits, MinCapacity: aws.Int32(10), MaxCapacity: aws.Int32(50)}))
	pt := must(db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("orders")})).Table.ProvisionedThroughput
	if *pt.WriteCapacityUnits != 10 || *pt.ReadCapacityUnits != 5 {
		t.Fatalf("throughput = %+v", pt)
	}

	_, err = c.PutScalingPolicy(ctx, &aas.PutScalingPolicyInput{PolicyName: aws.String("x"), ServiceNamespace: ns, ResourceId: aws.String("service/prod/nope"),
		ScalableDimension: dim, PolicyType: types.PolicyTypeStepScaling, StepScalingPolicyConfiguration: &types.StepScalingPolicyConfiguration{}})
	wantCode(t, err, "ObjectNotFoundException")
	must(c.DeregisterScalableTarget(ctx, &aas.DeregisterScalableTargetInput{ServiceNamespace: ns, ResourceId: id, ScalableDimension: dim}))
	if ts := must(c.DescribeScalableTargets(ctx, &aas.DescribeScalableTargetsInput{ServiceNamespace: ns})).ScalableTargets; len(ts) != 0 {
		t.Fatalf("targets = %+v", ts)
	}
}
