package fis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/fis"
	"github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	cloudwatchsvc "github.com/nrndev/pogi/internal/aws/cloudwatch"
	dynamodbsvc "github.com/nrndev/pogi/internal/aws/dynamodb"
	fissvc "github.com/nrndev/pogi/internal/aws/fis"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
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

type env struct {
	hub *core.Hub
	url string
	svc *fissvc.Service
	fis *fis.Client
	ddb *dynamodb.Client
	cw  *cloudwatch.Client
}

func setup(t *testing.T) env {
	var svc *fissvc.Service
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return dynamodbsvc.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) },
		func(h *core.Hub) core.Service { return cloudwatchsvc.New(h) },
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { svc = fissvc.New(h); return svc })
	cfg := awstest.Config(t, url)
	e := env{hub, url, svc, fis.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), cloudwatch.NewFromConfig(cfg)}
	must(e.ddb.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("orders"), BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}}))
	return e
}

var item = map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}}

// put writes an item and returns the error code, if any.
func put(c *dynamodb.Client) string {
	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders"), Item: item})
	return code(err)
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func throttle(ops, duration string) types.CreateExperimentTemplateActionInput {
	return types.CreateExperimentTemplateActionInput{ActionId: aws.String("aws:fis:inject-api-throttle-error"),
		Parameters: map[string]string{"service": "dynamodb", "operations": ops, "percentage": "100", "duration": duration}}
}

func template(t *testing.T, e env, in *fis.CreateExperimentTemplateInput) string {
	t.Helper()
	in.RoleArn, in.Description = aws.String("arn:aws:iam::000000000000:role/fis"), aws.String("chaos")
	if in.StopConditions == nil {
		in.StopConditions = []types.CreateExperimentTemplateStopConditionInput{{Source: aws.String("none")}}
	}
	return *must(e.fis.CreateExperimentTemplate(ctx, in)).ExperimentTemplate.Id
}

func start(t *testing.T, e env, tmpl string) string {
	t.Helper()
	x := must(e.fis.StartExperiment(ctx, &fis.StartExperimentInput{ExperimentTemplateId: aws.String(tmpl)})).Experiment
	if x.State.Status != types.ExperimentStatusPending {
		t.Fatalf("started %+v", x.State)
	}
	return *x.Id
}

func state(e env, id string) *types.Experiment {
	return must(e.fis.GetExperiment(ctx, &fis.GetExperimentInput{Id: aws.String(id)})).Experiment
}

// TestThrottleDynamoDB is the done-when: an experiment makes DynamoDB throttle.
func TestThrottleDynamoDB(t *testing.T) {
	e := setup(t)
	tmpl := template(t, e, &fis.CreateExperimentTemplateInput{Actions: map[string]types.CreateExperimentTemplateActionInput{"throttle": throttle("PutItem", "PT1M")}})
	if put(e.ddb) != "" {
		t.Fatal("PutItem fails before the experiment")
	}
	id := start(t, e, tmpl)
	now := time.Now()
	e.svc.Tick(now)
	if x := state(e, id); x.State.Status != types.ExperimentStatusRunning || x.Actions["throttle"].State.Status != types.ExperimentActionStatusRunning {
		t.Fatalf("experiment %+v, action %+v", x.State, x.Actions["throttle"].State)
	}
	_, err := e.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders"), Item: item})
	var re interface{ HTTPStatusCode() int }
	if code(err) != "ThrottlingException" || !errors.As(err, &re) || re.HTTPStatusCode() != 400 {
		t.Fatalf("PutItem during the experiment: %v", err)
	}
	if _, err := e.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("orders"), Key: item}); err != nil {
		t.Fatalf("GetItem is not targeted: %v", err)
	}

	e.svc.Tick(now.Add(30 * time.Second))
	if put(e.ddb) != "ThrottlingException" {
		t.Fatal("still throttled half way")
	}
	e.svc.Tick(now.Add(time.Minute))
	x := state(e, id)
	if x.State.Status != types.ExperimentStatusCompleted || x.Actions["throttle"].State.Status != types.ExperimentActionStatusCompleted || x.EndTime == nil {
		t.Fatalf("after its duration: %+v %+v", x.State, x.Actions["throttle"].State)
	}
	if c := put(e.ddb); c != "" || len(e.hub.Chaos.List()) != 0 {
		t.Fatalf("after the experiment PutItem gives %q and %d rules are left", c, len(e.hub.Chaos.List()))
	}

	// stopping removes the faults too
	id = start(t, e, tmpl)
	e.svc.Tick(time.Now())
	if put(e.ddb) != "ThrottlingException" {
		t.Fatal("second run does not throttle")
	}
	stopped := must(e.fis.StopExperiment(ctx, &fis.StopExperimentInput{Id: aws.String(id)})).Experiment
	if stopped.State.Status != types.ExperimentStatusStopped || stopped.Actions["throttle"].State.Status != types.ExperimentActionStatusStopped {
		t.Fatalf("stopped %+v", stopped.State)
	}
	if c := put(e.ddb); c != "" {
		t.Fatalf("after StopExperiment PutItem gives %q", c)
	}
	if list := must(e.fis.ListExperiments(ctx, &fis.ListExperimentsInput{})).Experiments; len(list) != 2 || *list[0].Id != id {
		t.Fatalf("experiments newest first %+v", list)
	}
}

// TestRoleScoped: with role targets, only the calls of those roles' sessions fail.
func TestRoleScoped(t *testing.T) {
	e := setup(t)
	act := throttle("PutItem", "PT5M")
	act.Targets = map[string]string{"Roles": "chaos-roles"}
	tmpl := template(t, e, &fis.CreateExperimentTemplateInput{Actions: map[string]types.CreateExperimentTemplateActionInput{"throttle": act},
		Targets: map[string]types.CreateExperimentTemplateTargetInput{"chaos-roles": {ResourceType: aws.String("aws:iam:role"),
			ResourceArns: []string{"arn:aws:iam::000000000000:role/chaos-target"}, SelectionMode: aws.String("ALL")}}})
	start(t, e, tmpl)
	e.svc.Tick(time.Now())

	as := func(role string) *dynamodb.Client {
		creds := must(sts.NewFromConfig(awstest.Config(t, e.url)).AssumeRole(ctx, &sts.AssumeRoleInput{
			RoleArn: aws.String("arn:aws:iam::000000000000:role/" + role), RoleSessionName: aws.String("load-test")})).Credentials
		return dynamodb.NewFromConfig(awstest.ConfigAs(t, e.url, *creds.AccessKeyId, *creds.SecretAccessKey, *creds.SessionToken))
	}
	if c := put(as("chaos-target")); c != "ThrottlingException" {
		t.Fatalf("the target role's PutItem gives %q", c)
	}
	if c := put(as("bystander")); c != "" {
		t.Fatalf("another role's PutItem gives %q", c)
	}
	if c := put(e.ddb); c != "" {
		t.Fatalf("the account's own PutItem gives %q", c)
	}
}

// TestStopConditionAndOrder: actions run in startAfter order, and an alarm
// in ALARM stops the experiment and its faults.
func TestStopConditionAndOrder(t *testing.T) {
	e := setup(t)
	must(e.cw.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("errors"), Namespace: aws.String("App"), MetricName: aws.String("Errors"),
		Statistic: cwtypes.StatisticSum, Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1), Threshold: aws.Float64(1),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanOrEqualToThreshold}))
	alarm := must(e.cw.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"errors"}})).MetricAlarms[0].AlarmArn
	act := throttle("PutItem", "PT10M")
	act.StartAfter = []string{"warmup"}
	tmpl := template(t, e, &fis.CreateExperimentTemplateInput{
		Actions: map[string]types.CreateExperimentTemplateActionInput{"throttle": act,
			"warmup": {ActionId: aws.String("aws:fis:wait"), Parameters: map[string]string{"duration": "PT1M"}}},
		StopConditions: []types.CreateExperimentTemplateStopConditionInput{{Source: aws.String("aws:cloudwatch:alarm"), Value: alarm}}})
	id := start(t, e, tmpl)
	now := time.Now()
	e.svc.Tick(now)
	if x := state(e, id); x.Actions["warmup"].State.Status != types.ExperimentActionStatusRunning || x.Actions["throttle"].State.Status != types.ExperimentActionStatusPending {
		t.Fatalf("first the wait: %+v %+v", x.Actions["warmup"].State, x.Actions["throttle"].State)
	}
	if put(e.ddb) != "" {
		t.Fatal("throttled before its turn")
	}
	e.svc.Tick(now.Add(time.Minute))
	if put(e.ddb) != "ThrottlingException" {
		t.Fatal("not throttled after the wait")
	}
	must(e.cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("errors"), StateValue: cwtypes.StateValueAlarm, StateReason: aws.String("test")}))
	e.svc.Tick(now.Add(2 * time.Minute))
	x := state(e, id)
	if x.State.Status != types.ExperimentStatusStopped || x.Actions["throttle"].State.Status != types.ExperimentActionStatusStopped {
		t.Fatalf("after the alarm: %+v %+v", x.State, x.Actions["throttle"].State)
	}
	if c := put(e.ddb); c != "" {
		t.Fatalf("after the stop condition PutItem gives %q", c)
	}
}

func TestTemplatesAndLambda(t *testing.T) {
	e := setup(t)
	_, err := e.fis.CreateExperimentTemplate(ctx, &fis.CreateExperimentTemplateInput{RoleArn: aws.String("arn:aws:iam::000000000000:role/fis"),
		Description: aws.String("x"), StopConditions: []types.CreateExperimentTemplateStopConditionInput{{Source: aws.String("none")}},
		Actions: map[string]types.CreateExperimentTemplateActionInput{"a": throttle("PutItem", "five minutes")}})
	if v := (*types.ValidationException)(nil); !errors.As(err, &v) {
		t.Fatalf("want ValidationException for a bad duration, got %v", err)
	}
	fail := types.CreateExperimentTemplateActionInput{ActionId: aws.String("aws:lambda:invocation-error"), Parameters: map[string]string{"duration": "PT1M"},
		Targets: map[string]string{"Functions": "fns"}}
	tmpl := template(t, e, &fis.CreateExperimentTemplateInput{Actions: map[string]types.CreateExperimentTemplateActionInput{"fail": fail},
		Targets: map[string]types.CreateExperimentTemplateTargetInput{"fns": {ResourceType: aws.String("aws:lambda:function"),
			ResourceArns: []string{"arn:aws:lambda:us-east-1:000000000000:function:checkout"}, SelectionMode: aws.String("ALL")}},
		Tags: map[string]string{"team": "sre"}})
	got := must(e.fis.GetExperimentTemplate(ctx, &fis.GetExperimentTemplateInput{Id: aws.String(tmpl)})).ExperimentTemplate
	if got.Tags["team"] != "sre" || *got.Targets["fns"].SelectionMode != "ALL" || got.Actions["fail"].Targets["Functions"] != "fns" || *got.Arn == "" {
		t.Fatalf("template %+v", got)
	}
	// Terraform tags by an ARN without the account
	must(e.fis.TagResource(ctx, &fis.TagResourceInput{ResourceArn: aws.String("arn:aws:fis:us-east-1::experiment-template/" + tmpl), Tags: map[string]string{"env": "dev"}}))
	if tags := must(e.fis.ListTagsForResource(ctx, &fis.ListTagsForResourceInput{ResourceArn: got.Arn})).Tags; len(tags) != 2 || tags["env"] != "dev" {
		t.Fatalf("tags %v", tags)
	}
	must(e.fis.UpdateExperimentTemplate(ctx, &fis.UpdateExperimentTemplateInput{Id: aws.String(tmpl), Description: aws.String("lambda chaos")}))
	if list := must(e.fis.ListExperimentTemplates(ctx, &fis.ListExperimentTemplatesInput{})).ExperimentTemplates; len(list) != 1 || *list[0].Description != "lambda chaos" {
		t.Fatalf("templates %+v", list)
	}
	if acts := must(e.fis.ListActions(ctx, &fis.ListActionsInput{})).Actions; len(acts) < 10 {
		t.Fatalf("actions %+v", acts)
	}
	if a := must(e.fis.GetAction(ctx, &fis.GetActionInput{Id: aws.String("aws:fis:inject-api-throttle-error")})).Action; !*a.Parameters["percentage"].Required ||
		*a.Targets["Roles"].ResourceType != "aws:iam:role" {
		t.Fatalf("action %+v", a)
	}

	// invocations of the target function fail; others reach Lambda
	start(t, e, tmpl)
	e.svc.Tick(time.Now())
	lc := lambda.NewFromConfig(awstest.Config(t, e.url))
	for _, fn := range []string{"checkout", "arn:aws:lambda:us-east-1:000000000000:function:checkout"} {
		if _, err := lc.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String(fn)}); code(err) != "ServiceException" {
			t.Fatalf("invoking %s: %v", fn, err)
		}
	}
	if _, err := lc.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("checkout-v2")}); code(err) != "ResourceNotFoundException" {
		t.Fatalf("another function: %v", err)
	}
	must(e.fis.DeleteExperimentTemplate(ctx, &fis.DeleteExperimentTemplateInput{Id: aws.String(tmpl)}))
	_, err = e.fis.GetExperimentTemplate(ctx, &fis.GetExperimentTemplateInput{Id: aws.String(tmpl)})
	if nf := (*types.ResourceNotFoundException)(nil); !errors.As(err, &nf) {
		t.Fatalf("want ResourceNotFoundException, got %v", err)
	}
}
