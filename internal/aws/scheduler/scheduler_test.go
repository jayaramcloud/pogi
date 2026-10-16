package scheduler_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	schedsvc "github.com/nrndev/pogi/internal/aws/scheduler"
	snssvc "github.com/nrndev/pogi/internal/aws/sns"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

const role = "arn:aws:iam::000000000000:role/scheduler"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func setup(t *testing.T) (*schedsvc.Service, *scheduler.Client, *sqs.Client) {
	var svc *schedsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = schedsvc.New(h); return svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	return svc, scheduler.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
}

// queue creates a queue and returns a function that receives its new bodies.
func queue(sq *sqs.Client, name string) (arn string, bodies func() []string) {
	q := must(sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)}))
	return "arn:aws:sqs:us-east-1:000000000000:" + name, func() []string {
		r := must(sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10}))
		var out []string
		for _, m := range r.Messages {
			out = append(out, *m.Body)
		}
		return out
	}
}

var off = &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff}

func TestOneTimeScheduleFiresAndDeletes(t *testing.T) {
	svc, sc, sq := setup(t)
	arn, bodies := queue(sq, "jobs")
	g := must(sc.CreateScheduleGroup(ctx, &scheduler.CreateScheduleGroupInput{Name: aws.String("batch"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	if tags := must(sc.ListTagsForResource(ctx, &scheduler.ListTagsForResourceInput{ResourceArn: g.ScheduleGroupArn})); len(tags.Tags) != 1 || *tags.Tags[0].Value != "dev" {
		t.Fatalf("tags = %+v", tags.Tags)
	}
	when := time.Now().UTC().Add(time.Minute)
	in := &scheduler.CreateScheduleInput{Name: aws.String("once"), GroupName: aws.String("batch"),
		ScheduleExpression: aws.String("at(" + when.Format("2006-01-02T15:04:05") + ")"), FlexibleTimeWindow: off,
		ActionAfterCompletion: types.ActionAfterCompletionDelete,
		Target:                &types.Target{Arn: aws.String(arn), RoleArn: aws.String(role), Input: aws.String(`{"job":"report"}`)}}
	out := must(sc.CreateSchedule(ctx, in))
	if *out.ScheduleArn != "arn:aws:scheduler:us-east-1:000000000000:schedule/batch/once" {
		t.Fatalf("arn = %s", *out.ScheduleArn)
	}
	var conflict *types.ConflictException
	if _, err := sc.CreateSchedule(ctx, in); !errors.As(err, &conflict) {
		t.Fatalf("duplicate: %v", err)
	}
	got := must(sc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String("once"), GroupName: aws.String("batch")}))
	if got.State != types.ScheduleStateEnabled || *got.GroupName != "batch" || *got.Target.Input != `{"job":"report"}` || got.CreationDate == nil {
		t.Fatalf("schedule = %+v", got)
	}

	svc.Tick(when.Add(-time.Second))
	if b := bodies(); len(b) != 0 {
		t.Fatalf("fired early: %q", b)
	}
	svc.Tick(when.Add(time.Second))
	if b := bodies(); len(b) != 1 || b[0] != `{"job":"report"}` {
		t.Fatalf("delivered %q", b)
	}
	var nf *types.ResourceNotFoundException
	if _, err := sc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String("once"), GroupName: aws.String("batch")}); !errors.As(err, &nf) {
		t.Fatalf("completed schedule should be deleted: %v", err)
	}
}

func TestRecurringScheduleCRUD(t *testing.T) {
	svc, sc, sq := setup(t)
	arn, bodies := queue(sq, "ticks")
	target := &types.Target{Arn: aws.String(arn), RoleArn: aws.String(role), Input: aws.String(`{"from":"<aws.scheduler.schedule-arn>"}`)}
	must(sc.CreateSchedule(ctx, &scheduler.CreateScheduleInput{Name: aws.String("every-minute"),
		ScheduleExpression: aws.String("rate(1 minute)"), FlexibleTimeWindow: off, Target: target}))
	now := time.Now()
	svc.Tick(now.Add(61 * time.Second))
	svc.Tick(now.Add(122 * time.Second))
	want := `{"from":"arn:aws:scheduler:us-east-1:000000000000:schedule/default/every-minute"}`
	if b := bodies(); len(b) != 2 || b[0] != want || b[1] != want {
		t.Fatalf("delivered %q", b)
	}
	snap := must(svc.Save()) // a snapshot keeps the schedule and its next run
	svc.Reset()
	if err := svc.Load(snap); err != nil || len(svc.Resources()[0].Rows) != 1 || svc.Inspect("Schedules", "every-minute") == nil {
		t.Fatalf("load: %v %+v", err, svc.Resources())
	}
	l := must(sc.ListSchedules(ctx, &scheduler.ListSchedulesInput{NamePrefix: aws.String("every")}))
	if len(l.Schedules) != 1 || *l.Schedules[0].GroupName != "default" || *l.Schedules[0].Target.Arn != arn {
		t.Fatalf("list = %+v", l.Schedules)
	}

	must(sc.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{Name: aws.String("every-minute"), ScheduleExpression: aws.String("cron(0 * * * ? *)"),
		FlexibleTimeWindow: off, Target: target, State: types.ScheduleStateDisabled}))
	svc.Tick(now.Add(3 * time.Hour))
	if b := bodies(); len(b) != 0 {
		t.Fatalf("disabled schedule delivered %q", b)
	}
	if got := must(sc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String("every-minute")})); got.State != types.ScheduleStateDisabled || *got.ScheduleExpression != "cron(0 * * * ? *)" {
		t.Fatalf("updated = %+v", got)
	}
	must(sc.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{Name: aws.String("every-minute")}))
	var nf *types.ResourceNotFoundException
	if _, err := sc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String("every-minute")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %v", err)
	}

	if _, err := sc.CreateSchedule(ctx, &scheduler.CreateScheduleInput{Name: aws.String("x"), GroupName: aws.String("missing"),
		ScheduleExpression: aws.String("rate(1 hour)"), FlexibleTimeWindow: off, Target: target}); !errors.As(err, &nf) {
		t.Fatalf("missing group: %v", err)
	}
	var invalid *types.ValidationException
	if _, err := sc.CreateSchedule(ctx, &scheduler.CreateScheduleInput{Name: aws.String("x"),
		ScheduleExpression: aws.String("rate(5 minute)"), FlexibleTimeWindow: off, Target: target}); !errors.As(err, &invalid) {
		t.Fatalf("bad expression: %v", err)
	}
	if _, err := sc.DeleteScheduleGroup(ctx, &scheduler.DeleteScheduleGroupInput{Name: aws.String("default")}); !errors.As(err, &invalid) {
		t.Fatalf("delete default group: %v", err)
	}
	if g := must(sc.ListScheduleGroups(ctx, &scheduler.ListScheduleGroupsInput{})); len(g.ScheduleGroups) != 1 || *g.ScheduleGroups[0].Name != "default" {
		t.Fatalf("groups = %+v", g.ScheduleGroups)
	}
}

// floci #1265 and #1669: universal targets aws-sdk:sns:publish and aws-sdk:sqs:sendMessage run the API call.
func TestUniversalTargets(t *testing.T) {
	var svc *schedsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = schedsvc.New(h); return svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) }, func(h *core.Hub) core.Service { return snssvc.New(h) })
	cfg := awstest.Config(t, url)
	sc, sq := scheduler.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	arn, bodies := queue(sq, "sub")
	_, directBodies := queue(sq, "direct")
	directURL := must(sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("direct")})).QueueUrl
	topic := must(sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("t")})).TopicArn
	must(sns.NewFromConfig(cfg).Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic, Protocol: aws.String("sqs"), Endpoint: aws.String(arn)}))
	when := time.Now().UTC().Add(time.Minute)
	for name, target := range map[string]types.Target{
		"pub":  {Arn: aws.String("arn:aws:scheduler:::aws-sdk:sns:publish"), Input: aws.String(`{"TopicArn":"` + *topic + `","Message":"hello"}`)},
		"send": {Arn: aws.String("arn:aws:scheduler:::aws-sdk:sqs:sendMessage"), Input: aws.String(`{"QueueUrl":"` + *directURL + `","MessageBody":"hi"}`)},
	} {
		target.RoleArn = aws.String(role)
		must(sc.CreateSchedule(ctx, &scheduler.CreateScheduleInput{Name: aws.String(name), FlexibleTimeWindow: off, Target: &target,
			ScheduleExpression: aws.String("at(" + when.Format("2006-01-02T15:04:05") + ")")}))
	}
	svc.Tick(when.Add(time.Second))
	if b := bodies(); len(b) != 1 || !strings.Contains(b[0], `"Message":"hello"`) {
		t.Fatalf("sns:publish delivered %q", b)
	}
	if b := directBodies(); len(b) != 1 || b[0] != "hi" {
		t.Fatalf("sqs:sendMessage delivered %q", b)
	}
}

// floci #4550: a Target.RoleArn that is not an IAM role ARN is refused.
// floci #3951: the Input of a templated target (Lambda here) must be well-formed JSON.
func TestTargetRoleAndInputChecks(t *testing.T) {
	_, sc, _ := setup(t)
	create := func(name, arn, role, input string) error {
		tg := &types.Target{Arn: aws.String(arn), RoleArn: aws.String(role)}
		if input != "" {
			tg.Input = aws.String(input)
		}
		_, err := sc.CreateSchedule(ctx, &scheduler.CreateScheduleInput{Name: aws.String(name), ScheduleExpression: aws.String("rate(5 minutes)"), FlexibleTimeWindow: off, Target: tg})
		return err
	}
	fn := "arn:aws:lambda:us-east-1:000000000000:function:f"
	var ve *types.ValidationException
	for name, err := range map[string]error{
		"badrole": create("badrole", fn, "not-a-role-arn", ""),
		"badjson": create("badjson", fn, role, "not json"),
	} {
		if !errors.As(err, &ve) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// JSON with a context attribute, and non-JSON for a non-templated target, are fine.
	must(0, create("ok1", fn, role, `{"n":<aws.scheduler.attempt-number>}`))
	must(0, create("ok2", "arn:aws:sqs:us-east-1:000000000000:q", role, "plain text"))
}
