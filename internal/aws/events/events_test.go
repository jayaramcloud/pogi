package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/aws/events"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

const defaultBus = "arn:aws:events:us-east-1:000000000000:event-bus/default"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	hub *core.Hub
	svc *events.Service
	eb  *eventbridge.Client
	sq  *sqs.Client
}

func setup(t *testing.T) *env {
	e := &env{}
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { e.svc = events.New(h); return e.svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	e.hub, e.eb, e.sq = h, eventbridge.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	return e
}

// queue creates a queue and returns its URL and ARN.
func (e *env) queue(name string) (string, string) {
	q := must(e.sq.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name)}))
	return *q.QueueUrl, "arn:aws:sqs:us-east-1:000000000000:" + name
}

// bodies receives what arrived in a queue since the last call.
func (e *env) bodies(url string) []string {
	r := must(e.sq.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: 10}))
	var out []string
	for _, m := range r.Messages {
		out = append(out, *m.Body)
	}
	return out
}

func (e *env) rule(name, pattern string, targets ...types.Target) {
	must(e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String(name), EventPattern: aws.String(pattern)}))
	must(e.eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String(name), Targets: targets}))
}

func (e *env) put(entries ...types.PutEventsRequestEntry) *eventbridge.PutEventsOutput {
	return must(e.eb.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: entries}))
}

func entry(source, detail string) types.PutEventsRequestEntry {
	return types.PutEventsRequestEntry{Source: aws.String(source), DetailType: aws.String("Test"), Detail: aws.String(detail)}
}

func target(id, arn string) types.Target {
	return types.Target{Id: aws.String(id), Arn: aws.String(arn)}
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func TestRuleDeliversMatchingEvents(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("orders")
	e.rule("big", `{"source":["shop"],"detail":{"total":[{"numeric":[">",100]}]}}`, target("q", arn))
	big := entry("shop", `{"total": 150}`)
	big.Resources = []string{"order/1"}
	out := e.put(big, entry("shop", `{"total": 5}`), entry("shop", `{not json`))
	if out.FailedEntryCount != 1 || out.Entries[0].EventId == nil || *out.Entries[2].ErrorCode != "MalformedDetail" {
		t.Fatalf("PutEvents = %+v", out.Entries)
	}
	got := e.bodies(url)
	if len(got) != 1 || !strings.HasPrefix(got[0], `{"version":"0","id":"`+*out.Entries[0].EventId+`","detail-type":"Test","source":"shop",`) {
		t.Fatalf("delivered %q", got)
	}
	var ev struct {
		Account, Region, Time string
		Resources             []string
		Detail                struct{ Total int }
	}
	json.Unmarshal([]byte(got[0]), &ev)
	if _, err := time.Parse(time.RFC3339, ev.Time); err != nil || ev.Account != "000000000000" || ev.Region != "us-east-1" ||
		ev.Detail.Total != 150 || len(ev.Resources) != 1 {
		t.Fatalf("envelope = %s", got[0])
	}

	_, err := e.eb.DeleteRule(ctx, &eventbridge.DeleteRuleInput{Name: aws.String("big")})
	if code(err) != "ValidationException" {
		t.Fatalf("deleting a rule with targets: %v", err)
	}
	names := must(e.eb.ListRuleNamesByTarget(ctx, &eventbridge.ListRuleNamesByTargetInput{TargetArn: aws.String(arn)}))
	if len(names.RuleNames) != 1 || names.RuleNames[0] != "big" {
		t.Fatalf("rule names = %v", names.RuleNames)
	}
	must(e.eb.DisableRule(ctx, &eventbridge.DisableRuleInput{Name: aws.String("big")}))
	e.put(big)
	if got := e.bodies(url); len(got) != 0 {
		t.Fatalf("disabled rule delivered %q", got)
	}
	must(e.eb.RemoveTargets(ctx, &eventbridge.RemoveTargetsInput{Rule: aws.String("big"), Ids: []string{"q"}}))
	must(e.eb.DeleteRule(ctx, &eventbridge.DeleteRuleInput{Name: aws.String("big")}))
	var nf *types.ResourceNotFoundException
	if _, err := e.eb.DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: aws.String("big")}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted rule: %v", err)
	}
}

func TestInputTransformation(t *testing.T) {
	e := setup(t)
	pathURL, pathARN := e.queue("path")
	tplURL, tplARN := e.queue("tpl")
	constURL, constARN := e.queue("const")
	tpl := target("tpl", tplARN)
	tpl.InputTransformer = &types.InputTransformer{
		InputPathsMap: map[string]string{"id": "$.detail.order.id", "first": "$.detail.order.items[0]", "src": "$.source", "order": "$.detail.order"},
		InputTemplate: aws.String(`{"id": <id>, "msg": "order <id> from <src>, first item <first>", "order": <order>, "rule": <aws.events.rule-name>}`),
	}
	path, fixed := target("path", pathARN), target("const", constARN)
	path.InputPath, fixed.Input = aws.String("$.detail.order"), aws.String(`{"fixed":true}`)
	e.rule("orders", `{"source":["shop"]}`, path, tpl, fixed)
	e.put(entry("shop", `{"order": {"id": "o-1", "items": [7, 8]}}`))
	for url, want := range map[string]string{
		pathURL:  `{"id":"o-1","items":[7,8]}`,
		tplURL:   `{"id": "o-1", "msg": "order o-1 from shop, first item 7", "order": {"id":"o-1","items":[7,8]}, "rule": "orders"}`,
		constURL: `{"fixed":true}`,
	} {
		if got := e.bodies(url); len(got) != 1 || got[0] != want {
			t.Errorf("got %q\nwant %q", got, want)
		}
	}
	ts := must(e.eb.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{Rule: aws.String("orders")}))
	if len(ts.Targets) != 3 || ts.Targets[2].InputTransformer.InputPathsMap["first"] != "$.detail.order.items[0]" {
		t.Fatalf("targets = %+v", ts.Targets)
	}
}

func TestBusesPatternsAndPermissions(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("audit")
	bus := must(e.eb.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String("audit"),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ops")}}}))
	var dup *types.ResourceAlreadyExistsException
	if _, err := e.eb.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String("audit")}); !errors.As(err, &dup) {
		t.Fatalf("duplicate bus: %v", err)
	}
	d := must(e.eb.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: aws.String("audit")}))
	if *d.Arn != "arn:aws:events:us-east-1:000000000000:event-bus/audit" || *d.Arn != *bus.EventBusArn || *d.Name != "audit" {
		t.Fatalf("describe = %+v", d)
	}
	if l := must(e.eb.ListEventBuses(ctx, &eventbridge.ListEventBusesInput{})); len(l.EventBuses) != 2 {
		t.Fatalf("buses = %d", len(l.EventBuses))
	}

	// The default bus forwards app.billing to the audit bus, whose rule takes every app.* event.
	must(e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("apps"), EventBusName: bus.EventBusArn, EventPattern: aws.String(`{"source":[{"prefix":"app."}]}`)}))
	must(e.eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String("apps"), EventBusName: aws.String("audit"), Targets: []types.Target{target("q", arn)}}))
	e.rule("to-audit", `{"source":["app.billing"]}`, target("bus", *bus.EventBusArn))
	onAudit := entry("app.users", `{}`)
	onAudit.EventBusName = aws.String("audit")
	e.put(entry("app.billing", `{}`), onAudit, entry("app.other", `{}`))
	if got := e.bodies(url); len(got) != 2 {
		t.Fatalf("audit queue got %q", got)
	}

	for n, want := range map[string]bool{"10": true, "9": false} {
		ev := `{"id":"1","source":"x","detail-type":"y","account":"000000000000","region":"us-east-1","time":"2026-01-01T00:00:00Z","resources":[],"detail":{"n":` + n + `}}`
		r := must(e.eb.TestEventPattern(ctx, &eventbridge.TestEventPatternInput{EventPattern: aws.String(`{"detail":{"n":[{"numeric":[">=",10]}]}}`), Event: aws.String(ev)}))
		if r.Result != want {
			t.Errorf("TestEventPattern with n=%s = %v", n, r.Result)
		}
	}
	var badPattern *types.InvalidEventPatternException
	if _, err := e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("bad"), EventPattern: aws.String(`{"source":"not-a-list"}`)}); !errors.As(err, &badPattern) {
		t.Fatalf("invalid pattern: %v", err)
	}
	if _, err := e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("tick"), EventBusName: aws.String("audit"), ScheduleExpression: aws.String("rate(5 minutes)")}); code(err) != "ValidationException" {
		t.Fatalf("schedule on a custom bus: %v", err)
	}
	if _, err := e.eb.DeleteEventBus(ctx, &eventbridge.DeleteEventBusInput{Name: aws.String("default")}); code(err) != "ValidationException" {
		t.Fatalf("delete default bus: %v", err)
	}

	tags := must(e.eb.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{ResourceARN: bus.EventBusArn}))
	if len(tags.Tags) != 1 || *tags.Tags[0].Value != "ops" {
		t.Fatalf("tags = %+v", tags.Tags)
	}
	must(e.eb.PutPermission(ctx, &eventbridge.PutPermissionInput{EventBusName: aws.String("audit"), Action: aws.String("events:PutEvents"),
		Principal: aws.String("111122223333"), StatementId: aws.String("partner")}))
	if d := must(e.eb.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: aws.String("audit")})); !strings.Contains(aws.ToString(d.Policy), `"Sid":"partner"`) {
		t.Fatalf("policy = %v", aws.ToString(d.Policy))
	}
	must(e.eb.RemovePermission(ctx, &eventbridge.RemovePermissionInput{EventBusName: aws.String("audit"), StatementId: aws.String("partner")}))
	if d := must(e.eb.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: aws.String("audit")})); d.Policy != nil {
		t.Fatalf("policy after removal = %v", *d.Policy)
	}
}

func TestArchiveAndReplay(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("orders")
	e.rule("orders", `{"source":["shop"]}`, target("q", arn))
	a := must(e.eb.CreateArchive(ctx, &eventbridge.CreateArchiveInput{ArchiveName: aws.String("shop"),
		EventSourceArn: aws.String(defaultBus), EventPattern: aws.String(`{"source":["shop"]}`)}))
	start := time.Now().Add(-time.Minute)
	e.put(entry("shop", `{"n":1}`), entry("elsewhere", `{"n":2}`))
	if got := e.bodies(url); len(got) != 1 {
		t.Fatalf("live delivery = %q", got)
	}
	if d := must(e.eb.DescribeArchive(ctx, &eventbridge.DescribeArchiveInput{ArchiveName: aws.String("shop")})); d.EventCount != 1 || d.State != types.ArchiveStateEnabled {
		t.Fatalf("archive = %+v", d)
	}
	must(e.eb.StartReplay(ctx, &eventbridge.StartReplayInput{ReplayName: aws.String("again"), EventSourceArn: a.ArchiveArn,
		EventStartTime: aws.Time(start), EventEndTime: aws.Time(time.Now().Add(time.Minute)), Destination: &types.ReplayDestination{Arn: aws.String(defaultBus)}}))
	if got := e.bodies(url); len(got) != 1 || !strings.Contains(got[0], `"detail":{"n":1},"replay-name":"again"`) {
		t.Fatalf("replayed = %q", got)
	}
	if r := must(e.eb.DescribeReplay(ctx, &eventbridge.DescribeReplayInput{ReplayName: aws.String("again")})); r.State != types.ReplayStateCompleted {
		t.Fatalf("replay = %+v", r)
	}
	var ill *types.IllegalStatusException
	if _, err := e.eb.CancelReplay(ctx, &eventbridge.CancelReplayInput{ReplayName: aws.String("again")}); !errors.As(err, &ill) {
		t.Fatalf("cancel finished replay: %v", err)
	}
	if d := must(e.eb.DescribeArchive(ctx, &eventbridge.DescribeArchiveInput{ArchiveName: aws.String("shop")})); d.EventCount != 1 {
		t.Fatalf("replayed events were archived again: %d", d.EventCount)
	}
}

func TestScheduledRule(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("ticks")
	must(e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("every5"), ScheduleExpression: aws.String("rate(5 minutes)")}))
	must(e.eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String("every5"), Targets: []types.Target{target("q", arn)}}))
	now := time.Now()
	e.svc.Tick(now.Add(4 * time.Minute))
	if got := e.bodies(url); len(got) != 0 {
		t.Fatalf("fired early: %q", got)
	}
	e.svc.Tick(now.Add(6 * time.Minute))
	got := e.bodies(url)
	if len(got) != 1 || !strings.Contains(got[0], `"detail-type":"Scheduled Event","source":"aws.events"`) ||
		!strings.Contains(got[0], `"resources":["arn:aws:events:us-east-1:000000000000:rule/every5"],"detail":{}`) {
		t.Fatalf("scheduled event = %q", got)
	}
	e.svc.Tick(now.Add(6 * time.Minute))
	e.svc.Tick(now.Add(11 * time.Minute))
	if got := e.bodies(url); len(got) != 1 {
		t.Fatalf("second period delivered %d", len(got))
	}
	if _, err := e.eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("bad"), ScheduleExpression: aws.String("rate(5 minute)")}); code(err) != "ValidationException" {
		t.Fatalf("bad schedule: %v", err)
	}
}

// Rules survive a snapshot (patterns are recompiled lazily); the console can publish and inspect.
func TestPersistenceAndConsole(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("kept")
	tgt := target("q", arn)
	tgt.RetryPolicy = &types.RetryPolicy{MaximumRetryAttempts: aws.Int32(3)}
	e.rule("kept", `{"source":["console"]}`, tgt)
	snap := must(e.svc.Save())
	e.svc.Reset()
	if err := e.svc.Load(snap); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.PutEvent("000000000000", "us-east-1", "default", "console", "Manual", `{"ok":true}`); err != nil {
		t.Fatal(err)
	}
	if got := e.bodies(url); len(got) != 1 || !strings.Contains(got[0], `"detail-type":"Manual"`) {
		t.Fatalf("after load delivered %q", got)
	}
	ts := must(e.eb.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{Rule: aws.String("kept")}))
	if len(ts.Targets) != 1 || ts.Targets[0].RetryPolicy == nil || *ts.Targets[0].RetryPolicy.MaximumRetryAttempts != 3 {
		t.Fatalf("targets after load = %+v", ts.Targets)
	}
	tables := e.svc.Resources()
	if len(tables) != 2 || len(tables[1].Rows) != 1 || tables[1].Rows[0][0] != "kept" || e.svc.Inspect("Rules", "kept") == nil {
		t.Fatalf("console tables = %+v", tables)
	}
}

// S3 (and Scheduler) publish through the hub with Source and DetailType set.
func TestEventsFromOtherServices(t *testing.T) {
	e := setup(t)
	url, arn := e.queue("uploads")
	e.rule("uploads", `{"source":["aws.s3"],"detail-type":["Object Created"],"detail":{"bucket":{"name":["photos"]}}}`, target("q", arn))
	err := e.hub.Deliver(defaultBus, &core.Message{Source: "aws.s3", DetailType: "Object Created",
		Body: `{"bucket":{"name":"photos"},"object":{"key":"cat.jpg"}}`, Resources: []string{"arn:aws:s3:::photos"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.bodies(url); len(got) != 1 || !strings.Contains(got[0], `"resources":["arn:aws:s3:::photos"],"detail":{"bucket":{"name":"photos"},"object":{"key":"cat.jpg"}}`) {
		t.Fatalf("delivered %q", got)
	}
}

func TestAPIDestination(t *testing.T) {
	e := setup(t)
	type call struct{ method, key, body string }
	calls := make(chan call, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		calls <- call{r.Method, r.Header.Get("X-Api-Key"), string(b)}
	}))
	defer srv.Close()
	conn := must(e.eb.CreateConnection(ctx, &eventbridge.CreateConnectionInput{Name: aws.String("hook"), AuthorizationType: types.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &types.CreateConnectionAuthRequestParameters{ApiKeyAuthParameters: &types.CreateConnectionApiKeyAuthRequestParameters{
			ApiKeyName: aws.String("X-Api-Key"), ApiKeyValue: aws.String("s3cret")}}}))
	if dc := must(e.eb.DescribeConnection(ctx, &eventbridge.DescribeConnectionInput{Name: aws.String("hook")})); dc.ConnectionState != types.ConnectionStateAuthorized ||
		*dc.AuthParameters.ApiKeyAuthParameters.ApiKeyName != "X-Api-Key" {
		t.Fatalf("connection = %+v", dc)
	}
	dest := must(e.eb.CreateApiDestination(ctx, &eventbridge.CreateApiDestinationInput{Name: aws.String("hook"), ConnectionArn: conn.ConnectionArn,
		InvocationEndpoint: aws.String(srv.URL + "/hook"), HttpMethod: types.ApiDestinationHttpMethodPut}))
	api := target("api", *dest.ApiDestinationArn)
	api.Input = aws.String(`{"ping":1}`)
	e.rule("hook", `{"source":["shop"]}`, api)
	e.put(entry("shop", `{}`))
	select {
	case c := <-calls:
		if c != (call{"PUT", "s3cret", `{"ping":1}`}) {
			t.Fatalf("call = %+v", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("API destination was not called")
	}
}

func TestParseSchedule(t *testing.T) {
	at := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	ny, _ := time.LoadLocation("America/New_York")
	for _, c := range []struct {
		expr  string
		after time.Time
		want  string // UTC, "" for never
	}{
		{"rate(5 minutes)", at("2026-10-11T10:00:00Z"), "2026-10-11T10:05:00Z"},
		{"rate(1 day)", at("2026-10-11T10:00:00Z"), "2026-10-12T10:00:00Z"},
		{"cron(0 12 * * ? *)", at("2026-10-11T13:00:00Z"), "2026-10-12T12:00:00Z"},
		{"cron(0/15 * * * ? *)", at("2026-10-11T10:07:00Z"), "2026-10-11T10:15:00Z"},
		{"cron(0 18 ? * MON-FRI *)", at("2026-10-09T19:00:00Z"), "2026-10-12T18:00:00Z"}, // Friday evening → Monday
		{"cron(0 0 L * ? *)", at("2026-02-10T00:00:00Z"), "2026-02-28T00:00:00Z"},
		{"cron(30 9 1 jan,JUL ? 2027)", at("2026-10-11T00:00:00Z"), "2027-01-01T09:30:00Z"},
		{"cron(0 9 * * ? *)", time.Date(2026, 10, 11, 10, 0, 0, 0, ny), "2026-10-12T13:00:00Z"}, // in the caller's zone
		{"cron(30 2 * * ? *)", time.Date(2026, 3, 7, 3, 0, 0, 0, ny), "2026-03-09T06:30:00Z"},   // 02:30 does not exist on 8 March
		{"cron(0 0 1 1 ? 1999)", at("2026-01-01T00:00:00Z"), ""},
		{"at(2026-10-12T08:00:00)", at("2026-10-11T00:00:00Z"), "2026-10-12T08:00:00Z"},
		{"at(2026-10-12T08:00:00)", at("2026-10-13T00:00:00Z"), ""},
	} {
		s, err := events.ParseSchedule(c.expr)
		if err != nil {
			t.Errorf("%s: %v", c.expr, err)
			continue
		}
		got := ""
		if n := s.Next(c.after); !n.IsZero() {
			got = n.UTC().Format(time.RFC3339)
		}
		if got != c.want {
			t.Errorf("%s after %v = %q, want %q", c.expr, c.after, got, c.want)
		}
	}
	for _, bad := range []string{"rate(5 minute)", "rate(1 minutes)", "rate(0 minutes)", "rate(2 weeks)", "cron(0 12 * * * *)",
		"cron(0 12 ? * ? *)", "cron(0 12 1W * ? *)", "cron(0 12 ? * 2#1 *)", "cron(60 * * * ? *)", "cron(0 12 * *)", "at(2026-10-12)", "every(5 minutes)"} {
		if _, err := events.ParseSchedule(bad); err == nil {
			t.Errorf("%s should not parse", bad)
		}
	}
}
