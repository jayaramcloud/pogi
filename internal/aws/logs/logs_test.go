package logs_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	cwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	cwsvc "github.com/nrndev/pogi/internal/aws/cloudwatch"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	logssvc "github.com/nrndev/pogi/internal/aws/logs"
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

func setup(t *testing.T) (*cwl.Client, *logssvc.Service) {
	var svc *logssvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = logssvc.New(h); return svc })
	return cwl.NewFromConfig(awstest.Config(t, url)), svc
}

func put(c *cwl.Client, group, stream string, ts int64, msgs ...string) *cwl.PutLogEventsOutput {
	var evs []types.InputLogEvent
	for i, m := range msgs {
		evs = append(evs, types.InputLogEvent{Timestamp: aws.Int64(ts + int64(i)), Message: aws.String(m)})
	}
	return must(c.PutLogEvents(ctx, &cwl.PutLogEventsInput{LogGroupName: aws.String(group), LogStreamName: aws.String(stream), LogEvents: evs}))
}

func messages[T any](evs []T, msg func(T) *string) string {
	var out []string
	for _, e := range evs {
		out = append(out, aws.ToString(msg(e)))
	}
	return strings.Join(out, ",")
}

func TestGroupsStreamsAndEvents(t *testing.T) {
	c, svc := setup(t)
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("/app/api"), Tags: map[string]string{"team": "web"}}))
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("/app/worker")}))
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("other")}))
	var exists *types.ResourceAlreadyExistsException
	if _, err := c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("/app/api")}); !errors.As(err, &exists) {
		t.Fatalf("want ResourceAlreadyExistsException, got %v", err)
	}
	prefixed := must(c.DescribeLogGroups(ctx, &cwl.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String("/app"), Limit: aws.Int32(1)}))
	if len(prefixed.LogGroups) != 1 || prefixed.NextToken == nil || aws.ToString(prefixed.LogGroups[0].LogGroupName) != "/app/api" ||
		aws.ToString(prefixed.LogGroups[0].Arn) != "arn:aws:logs:us-east-1:000000000000:log-group:/app/api:*" ||
		aws.ToString(prefixed.LogGroups[0].LogGroupArn) != "arn:aws:logs:us-east-1:000000000000:log-group:/app/api" ||
		aws.ToInt64(prefixed.LogGroups[0].CreationTime) < 1e12 || prefixed.LogGroups[0].LogGroupClass != types.LogGroupClassStandard {
		t.Fatalf("describe groups = %+v", prefixed.LogGroups)
	}
	rest := must(c.DescribeLogGroups(ctx, &cwl.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String("/app"), NextToken: prefixed.NextToken}))
	if len(rest.LogGroups) != 1 || aws.ToString(rest.LogGroups[0].LogGroupName) != "/app/worker" || rest.NextToken != nil {
		t.Fatalf("second page = %+v", rest.LogGroups)
	}
	if p := must(c.DescribeLogGroups(ctx, &cwl.DescribeLogGroupsInput{LogGroupNamePattern: aws.String("work")})); len(p.LogGroups) != 1 {
		t.Fatalf("pattern = %d groups", len(p.LogGroups))
	}

	must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("a")}))
	must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("b")}))
	var nf *types.ResourceNotFoundException
	if _, err := c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("/nope"), LogStreamName: aws.String("a")}); !errors.As(err, &nf) {
		t.Fatalf("stream in a missing group: %v", err)
	}
	if _, err := c.PutLogEvents(ctx, &cwl.PutLogEventsInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("zzz"),
		LogEvents: []types.InputLogEvent{{Timestamp: aws.Int64(1), Message: aws.String("x")}}}); !errors.As(err, &nf) {
		t.Fatalf("put to a missing stream: %v", err)
	}

	base := time.Now().Add(-time.Minute).UnixMilli()
	// Out of order on purpose: pogi accepts that and stores events sorted.
	out := must(c.PutLogEvents(ctx, &cwl.PutLogEventsInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("a"), LogEvents: []types.InputLogEvent{
		{Timestamp: aws.Int64(base + 2), Message: aws.String("m2")}, {Timestamp: aws.Int64(base), Message: aws.String("m0")},
		{Timestamp: aws.Int64(base + 1), Message: aws.String("m1")}}}))
	if out.NextSequenceToken == nil || out.RejectedLogEventsInfo != nil {
		t.Fatalf("put = %+v", out)
	}
	put(c, "/app/api", "a", base+3, "m3", "m4")
	put(c, "/app/api", "b", base+10, "other")

	streams := must(c.DescribeLogStreams(ctx, &cwl.DescribeLogStreamsInput{LogGroupName: aws.String("/app/api"), OrderBy: types.OrderByLastEventTime, Descending: aws.Bool(true)}))
	if len(streams.LogStreams) != 2 || aws.ToString(streams.LogStreams[0].LogStreamName) != "b" || aws.ToInt64(streams.LogStreams[1].FirstEventTimestamp) != base ||
		aws.ToInt64(streams.LogStreams[1].LastEventTimestamp) != base+4 || streams.LogStreams[1].LastIngestionTime == nil {
		t.Fatalf("streams = %+v", streams.LogStreams)
	}

	if err := svc.Load(must(svc.Save())); err != nil { // state survives a snapshot round trip
		t.Fatal(err)
	}
	get := func(in *cwl.GetLogEventsInput) *cwl.GetLogEventsOutput {
		in.LogGroupName, in.LogStreamName = aws.String("/app/api"), aws.String("a")
		return must(c.GetLogEvents(ctx, in))
	}
	msg := func(e types.OutputLogEvent) *string { return e.Message }
	first := get(&cwl.GetLogEventsInput{StartFromHead: aws.Bool(true), Limit: aws.Int32(2)})
	second := get(&cwl.GetLogEventsInput{NextToken: first.NextForwardToken, Limit: aws.Int32(2)})
	third := get(&cwl.GetLogEventsInput{NextToken: second.NextForwardToken, Limit: aws.Int32(2)})
	end := get(&cwl.GetLogEventsInput{NextToken: third.NextForwardToken, Limit: aws.Int32(2)})
	back := get(&cwl.GetLogEventsInput{NextToken: third.NextBackwardToken, Limit: aws.Int32(2)})
	if messages(first.Events, msg) != "m0,m1" || messages(second.Events, msg) != "m2,m3" || messages(third.Events, msg) != "m4" ||
		len(end.Events) != 0 || *end.NextForwardToken != *third.NextForwardToken || messages(back.Events, msg) != "m2,m3" ||
		!strings.HasPrefix(*first.NextForwardToken, "f/") || !strings.HasPrefix(*first.NextBackwardToken, "b/") {
		t.Fatalf("pages = %q %q %q %q back %q", messages(first.Events, msg), messages(second.Events, msg), messages(third.Events, msg), messages(end.Events, msg), messages(back.Events, msg))
	}
	if latest := get(&cwl.GetLogEventsInput{Limit: aws.Int32(2)}); messages(latest.Events, msg) != "m3,m4" {
		t.Fatalf("latest = %s", messages(latest.Events, msg))
	}
	if window := get(&cwl.GetLogEventsInput{StartTime: aws.Int64(base + 1), EndTime: aws.Int64(base + 3), StartFromHead: aws.Bool(true)}); messages(window.Events, msg) != "m1,m2" {
		t.Fatalf("window = %s", messages(window.Events, msg))
	}
	var all []string
	pager := cwl.NewGetLogEventsPaginator(c, &cwl.GetLogEventsInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("a"), StartFromHead: aws.Bool(true), Limit: aws.Int32(2)},
		func(o *cwl.GetLogEventsPaginatorOptions) { o.StopOnDuplicateToken = true })
	for pager.HasMorePages() {
		all = append(all, messages(must(pager.NextPage(ctx)).Events, msg))
	}
	if strings.Join(all, "|") != "m0,m1|m2,m3|m4|" {
		t.Fatalf("paginator = %q", all)
	}
	if tail := svc.Tail("000000000000", "us-east-1", "/app/api", base+3, 2); len(tail) != 2 || tail[0].Message != "m4" || tail[1].Stream != "b" {
		t.Fatalf("tail = %+v", tail)
	}

	// Too old and too new events are rejected; retention drops what is past it.
	now := time.Now().UnixMilli()
	if r := put(c, "/app/api", "b", now-15*24*3600*1000, "ancient").RejectedLogEventsInfo; r == nil || aws.ToInt32(r.TooOldLogEventEndIndex) != 1 {
		t.Fatalf("too old = %+v", r)
	}
	rej := must(c.PutLogEvents(ctx, &cwl.PutLogEventsInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("b"), LogEvents: []types.InputLogEvent{
		{Timestamp: aws.Int64(now), Message: aws.String("ok")}, {Timestamp: aws.Int64(now + 3*3600*1000), Message: aws.String("future")}}}))
	if r := rej.RejectedLogEventsInfo; r == nil || aws.ToInt32(r.TooNewLogEventStartIndex) != 1 || r.TooOldLogEventEndIndex != nil {
		t.Fatalf("too new = %+v", r)
	}
	if _, err := c.PutLogEvents(ctx, &cwl.PutLogEventsInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("b"), LogEvents: []types.InputLogEvent{
		{Timestamp: aws.Int64(now - 25*3600*1000), Message: aws.String("a")}, {Timestamp: aws.Int64(now), Message: aws.String("b")}}}); err == nil {
		t.Fatal("a batch spanning more than 24 hours must fail")
	}
	must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("other"), LogStreamName: aws.String("s")}))
	put(c, "other", "s", now-3*24*3600*1000, "three days old")
	put(c, "other", "s", now, "fresh")
	if _, err := c.PutRetentionPolicy(ctx, &cwl.PutRetentionPolicyInput{LogGroupName: aws.String("other"), RetentionInDays: aws.Int32(2)}); err == nil {
		t.Fatal("2 days is not a valid retention")
	}
	must(c.PutRetentionPolicy(ctx, &cwl.PutRetentionPolicyInput{LogGroupName: aws.String("other"), RetentionInDays: aws.Int32(1)}))
	kept := must(c.GetLogEvents(ctx, &cwl.GetLogEventsInput{LogGroupName: aws.String("other"), LogStreamName: aws.String("s")}))
	if messages(kept.Events, msg) != "fresh" {
		t.Fatalf("after retention = %s", messages(kept.Events, msg))
	}
	if g := must(c.DescribeLogGroups(ctx, &cwl.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String("other")})).LogGroups[0]; aws.ToInt32(g.RetentionInDays) != 1 || aws.ToInt64(g.StoredBytes) != int64(len("fresh")+26) {
		t.Fatalf("group after retention = %+v", g)
	}
	must(c.DeleteRetentionPolicy(ctx, &cwl.DeleteRetentionPolicyInput{LogGroupName: aws.String("other")}))

	must(c.TagLogGroup(ctx, &cwl.TagLogGroupInput{LogGroupName: aws.String("/app/api"), Tags: map[string]string{"env": "dev"}}))
	must(c.TagResource(ctx, &cwl.TagResourceInput{ResourceArn: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:/app/api"), Tags: map[string]string{"x": "1"}}))
	must(c.UntagResource(ctx, &cwl.UntagResourceInput{ResourceArn: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:/app/api"), TagKeys: []string{"x"}}))
	must(c.UntagLogGroup(ctx, &cwl.UntagLogGroupInput{LogGroupName: aws.String("/app/api"), Tags: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &cwl.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:logs:us-east-1:000000000000:log-group:/app/api")})).Tags; len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags = %v", tags)
	}
	if tags := must(c.ListTagsLogGroup(ctx, &cwl.ListTagsLogGroupInput{LogGroupName: aws.String("/app/api")})).Tags; len(tags) != 1 {
		t.Fatalf("legacy tags = %v", tags)
	}

	must(c.DeleteLogStream(ctx, &cwl.DeleteLogStreamInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("a")}))
	if _, err := c.GetLogEvents(ctx, &cwl.GetLogEventsInput{LogGroupName: aws.String("/app/api"), LogStreamName: aws.String("a")}); !errors.As(err, &nf) {
		t.Fatalf("deleted stream: %v", err)
	}
	must(c.DeleteLogGroup(ctx, &cwl.DeleteLogGroupInput{LogGroupName: aws.String("/app/api")}))
	if _, err := c.DeleteLogGroup(ctx, &cwl.DeleteLogGroupInput{LogGroupName: aws.String("/app/api")}); !errors.As(err, &nf) {
		t.Fatalf("deleted group: %v", err)
	}
}

func TestFilterPatterns(t *testing.T) {
	c, _ := setup(t)
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("app")}))
	for _, s := range []string{"one", "two"} {
		must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("app"), LogStreamName: aws.String(s)}))
	}
	base := time.Now().Add(-time.Minute).UnixMilli()
	put(c, "app", "one", base, "ERROR timeout talking to db", "INFO all good", "WARN connection reset by peer",
		`{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"}`)
	put(c, "app", "two", base+1, "ERROR disk full", `{"level":"warn","latency":50,"msg":"Create"}`, `{"level":"error","latency":20,"ok":true}`)

	filter := func(pattern string, in *cwl.FilterLogEventsInput) string {
		in.LogGroupName, in.FilterPattern = aws.String("app"), aws.String(pattern)
		var out []string
		for _, e := range must(c.FilterLogEvents(ctx, in)).Events {
			out = append(out, aws.ToString(e.LogStreamName)+":"+strings.Fields(aws.ToString(e.Message))[0])
		}
		return strings.Join(out, " ")
	}
	for _, tc := range []struct{ pattern, want string }{
		{"", `one:ERROR one:INFO two:ERROR one:WARN two:{"level":"warn","latency":50,"msg":"Create"} one:{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"} two:{"level":"error","latency":20,"ok":true}`},
		{"ERROR", "one:ERROR two:ERROR"},
		{"ERROR timeout", "one:ERROR"},
		{`"connection reset"`, "one:WARN"},
		{"ERROR -disk", "one:ERROR"},
		{"?WARN ?INFO", "one:INFO one:WARN"},
		{"%time.ut%", "one:ERROR"},
		{`{ $.level = "error" }`, `one:{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"} two:{"level":"error","latency":20,"ok":true}`},
		{`{ $.latency > 100 }`, `one:{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"}`},
		{`{ ($.level = "error") && ($.latency < 100) }`, `two:{"level":"error","latency":20,"ok":true}`},
		{`{ $.user.id = 7 || $.level = warn }`, `two:{"level":"warn","latency":50,"msg":"Create"} one:{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"}`},
		{`{ $.tags[1] = "b" }`, `one:{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"}`},
		{`{ $.msg = "Update*" }`, `one:{"level":"error","latency":250,"user":{"id":7},"tags":["a","b"],"msg":"UpdateUser"}`},
		{`{ $.ok IS TRUE }`, `two:{"level":"error","latency":20,"ok":true}`},
		{`{ $.msg NOT EXISTS }`, `two:{"level":"error","latency":20,"ok":true}`},
	} {
		if got := filter(tc.pattern, &cwl.FilterLogEventsInput{}); got != tc.want {
			t.Errorf("pattern %s:\n got %s\nwant %s", tc.pattern, got, tc.want)
		}
	}
	if got := filter("ERROR", &cwl.FilterLogEventsInput{LogStreamNames: []string{"two"}}); got != "two:ERROR" {
		t.Errorf("stream names = %s", got)
	}
	if got := filter("", &cwl.FilterLogEventsInput{StartTime: aws.Int64(base + 2), EndTime: aws.Int64(base + 2)}); got != `one:WARN two:{"level":"warn","latency":50,"msg":"Create"}` {
		t.Errorf("time window = %s", got)
	}
	page := must(c.FilterLogEvents(ctx, &cwl.FilterLogEventsInput{LogGroupName: aws.String("app"), Limit: aws.Int32(4)}))
	next := must(c.FilterLogEvents(ctx, &cwl.FilterLogEventsInput{LogGroupName: aws.String("app"), Limit: aws.Int32(4), NextToken: page.NextToken}))
	if len(page.Events) != 4 || page.NextToken == nil || len(next.Events) != 3 || next.NextToken != nil || aws.ToString(page.Events[0].EventId) == "" {
		t.Fatalf("pagination = %d %v / %d %v", len(page.Events), page.NextToken, len(next.Events), next.NextToken)
	}
	var bad *types.InvalidParameterException
	if _, err := c.FilterLogEvents(ctx, &cwl.FilterLogEventsInput{LogGroupName: aws.String("app"), FilterPattern: aws.String(`{ $.level = }`)}); !errors.As(err, &bad) {
		t.Fatalf("bad pattern: %v", err)
	}
}

func TestInsightsAndFilters(t *testing.T) {
	c, svc := setup(t)
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("svc")}))
	must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("svc"), LogStreamName: aws.String("s")}))
	now := time.Now()
	put(c, "svc", "s", now.Add(-time.Minute).UnixMilli(), "ERROR first", "INFO ok", "ERROR second", `{"level":"warn","ms":300}`, `{"level":"info","ms":5}`)

	run := func(q string) *cwl.GetQueryResultsOutput {
		id := must(c.StartQuery(ctx, &cwl.StartQueryInput{LogGroupName: aws.String("svc"), QueryString: aws.String(q),
			StartTime: aws.Int64(now.Add(-time.Hour).Unix()), EndTime: aws.Int64(now.Unix())})).QueryId
		return must(c.GetQueryResults(ctx, &cwl.GetQueryResultsInput{QueryId: id}))
	}
	field := func(row []types.ResultField, name string) string {
		for _, f := range row {
			if aws.ToString(f.Field) == name {
				return aws.ToString(f.Value)
			}
		}
		return ""
	}
	r := run("fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 1")
	if r.Status != types.QueryStatusComplete || len(r.Results) != 1 || field(r.Results[0], "@message") != "ERROR second" ||
		len(field(r.Results[0], "@timestamp")) != len("2006-01-02 15:04:05.000") || r.Statistics.RecordsMatched != 2 || r.Statistics.RecordsScanned != 5 {
		t.Fatalf("query = %+v", r)
	}
	r = run(`fields level, ms | filter ms > 10 and level = "warn"`)
	if len(r.Results) != 1 || field(r.Results[0], "ms") != "300" || field(r.Results[0], "level") != "warn" {
		t.Fatalf("json fields = %+v", r.Results)
	}
	r = run(`filter @message not like "ERROR" | sort @timestamp asc`)
	if len(r.Results) != 3 || field(r.Results[0], "@message") != "INFO ok" {
		t.Fatalf("not like = %+v", r.Results)
	}
	var malformed *types.MalformedQueryException
	if _, err := c.StartQuery(ctx, &cwl.StartQueryInput{LogGroupName: aws.String("svc"), QueryString: aws.String("stats count(*) by bin(5m)"),
		StartTime: aws.Int64(0), EndTime: aws.Int64(now.Unix())}); !errors.As(err, &malformed) {
		t.Fatalf("unsupported query: %v", err)
	}

	must(c.PutMetricFilter(ctx, &cwl.PutMetricFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String("errors"), FilterPattern: aws.String("ERROR"),
		MetricTransformations: []types.MetricTransformation{{MetricName: aws.String("Errors"), MetricNamespace: aws.String("App"), MetricValue: aws.String("1")}}}))
	mf := must(c.DescribeMetricFilters(ctx, &cwl.DescribeMetricFiltersInput{MetricName: aws.String("Errors"), MetricNamespace: aws.String("App")}))
	if len(mf.MetricFilters) != 1 || aws.ToString(mf.MetricFilters[0].LogGroupName) != "svc" || aws.ToString(mf.MetricFilters[0].MetricTransformations[0].MetricValue) != "1" {
		t.Fatalf("metric filters = %+v", mf.MetricFilters)
	}
	if g := must(c.DescribeLogGroups(ctx, &cwl.DescribeLogGroupsInput{})).LogGroups[0]; aws.ToInt32(g.MetricFilterCount) != 1 {
		t.Fatalf("metricFilterCount = %v", g.MetricFilterCount)
	}
	must(c.DeleteMetricFilter(ctx, &cwl.DeleteMetricFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String("errors")}))

	for _, name := range []string{"one", "two"} {
		must(c.PutSubscriptionFilter(ctx, &cwl.PutSubscriptionFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String(name), FilterPattern: aws.String(""),
			DestinationArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:nowhere")}))
	}
	var limit *types.LimitExceededException
	if _, err := c.PutSubscriptionFilter(ctx, &cwl.PutSubscriptionFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String("three"), FilterPattern: aws.String(""),
		DestinationArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:nowhere")}); !errors.As(err, &limit) {
		t.Fatalf("third subscription filter: %v", err)
	}
	sf := must(c.DescribeSubscriptionFilters(ctx, &cwl.DescribeSubscriptionFiltersInput{LogGroupName: aws.String("svc")}))
	if len(sf.SubscriptionFilters) != 2 || sf.SubscriptionFilters[0].Distribution != types.DistributionByLogStream || aws.ToString(sf.SubscriptionFilters[0].LogGroupName) != "svc" {
		t.Fatalf("subscription filters = %+v", sf.SubscriptionFilters)
	}
	if err := svc.Load(must(svc.Save())); err != nil {
		t.Fatal(err)
	}
	put(c, "svc", "s", now.UnixMilli(), "delivered nowhere, logged and dropped")
	must(c.DeleteSubscriptionFilter(ctx, &cwl.DeleteSubscriptionFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String("one")}))
}

func zipped(name, code string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create(name)
	w.Write([]byte(code))
	zw.Close()
	return buf.Bytes()
}

// sink decodes subscription deliveries the way log-shipping Lambdas do.
const sink = `
import base64, gzip, json

def handler(event, context):
    data = json.loads(gzip.decompress(base64.b64decode(event["awslogs"]["data"])))
    for e in data["logEvents"]:
        print("got", data["logGroup"], data["logStream"], data["subscriptionFilters"][0], e["message"])
`

func TestLambdaLogsAndSubscriptions(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return logssvc.New(h) })
	cfg := awstest.Config(t, url)
	c, l := cwl.NewFromConfig(cfg), lambda.NewFromConfig(cfg)
	create := func(name, code string) *string {
		return must(l.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: lambdatypes.RuntimePython312,
			Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
			Code: &lambdatypes.FunctionCode{ZipFile: zipped("app.py", code)}})).FunctionArn
	}
	// waitFor polls FilterLogEvents until the pattern matches, as log delivery is asynchronous.
	waitFor := func(group, pattern string) []types.FilteredLogEvent {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			out, err := c.FilterLogEvents(ctx, &cwl.FilterLogEventsInput{LogGroupName: aws.String(group), FilterPattern: aws.String(pattern)})
			if err == nil && len(out.Events) > 0 {
				return out.Events
			}
		}
		t.Fatalf("no events matching %s in %s", pattern, group)
		return nil
	}

	create("hello", "def handler(event, context):\n    print('hello from lambda', event['n'])\n    return {}\n")
	must(l.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("hello"), Payload: []byte(`{"n":42}`)}))
	evs := waitFor("/aws/lambda/hello", `"hello from lambda 42"`)
	if !strings.Contains(aws.ToString(evs[0].LogStreamName), "[$LATEST]") {
		t.Fatalf("lambda log stream = %s", aws.ToString(evs[0].LogStreamName))
	}
	waitFor("/aws/lambda/hello", "REPORT RequestId")

	sinkARN := create("sink", sink)
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("app")}))
	must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("app"), LogStreamName: aws.String("s")}))
	must(c.PutSubscriptionFilter(ctx, &cwl.PutSubscriptionFilterInput{LogGroupName: aws.String("app"), FilterName: aws.String("ship"),
		FilterPattern: aws.String("ERROR"), DestinationArn: sinkARN}))
	put(c, "app", "s", time.Now().UnixMilli(), "INFO fine", "ERROR boom")
	got := waitFor("/aws/lambda/sink", `"got app"`)
	if len(got) != 1 || !strings.Contains(aws.ToString(got[0].Message), "got app s ship ERROR boom") {
		t.Fatalf("sink saw %+v", got)
	}
}

// A metric filter publishes its metric for matching events, so an alarm on it fires.
func TestMetricFilterPublishesMetrics(t *testing.T) {
	var cw *cwsvc.Service
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return logssvc.New(h) },
		func(h *core.Hub) core.Service { cw = cwsvc.New(h); return cw })
	cfg := awstest.Config(t, url)
	c, m := cwl.NewFromConfig(cfg), cloudwatch.NewFromConfig(cfg)
	must(c.CreateLogGroup(ctx, &cwl.CreateLogGroupInput{LogGroupName: aws.String("svc")}))
	must(c.CreateLogStream(ctx, &cwl.CreateLogStreamInput{LogGroupName: aws.String("svc"), LogStreamName: aws.String("s")}))
	must(c.PutMetricFilter(ctx, &cwl.PutMetricFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String("errors"), FilterPattern: aws.String("ERROR"),
		MetricTransformations: []types.MetricTransformation{{MetricName: aws.String("Errors"), MetricNamespace: aws.String("App"),
			MetricValue: aws.String("1"), DefaultValue: aws.Float64(0), Unit: types.StandardUnitCount}}}))
	must(c.PutMetricFilter(ctx, &cwl.PutMetricFilterInput{LogGroupName: aws.String("svc"), FilterName: aws.String("slow"), FilterPattern: aws.String(`{ $.ms > 0 }`),
		MetricTransformations: []types.MetricTransformation{{MetricName: aws.String("Latency"), MetricNamespace: aws.String("App"),
			MetricValue: aws.String("$.ms"), Dimensions: map[string]string{"Route": "$.route"}}}}))
	now := time.Now().UnixMilli() - 10_000 // in the past, so the alarm window below includes it
	put(c, "svc", "s", now, "ERROR one", "INFO fine", "ERROR two", `{"ms": 40, "route": "/a"}`, `{"ms": 60, "route": "/a"}`, `{"ms": 9}`)
	put(c, "svc", "s", now+10, "INFO only") // no match: the default value (0) is published

	errs := must(m.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{StartTime: aws.Time(time.Now().Add(-5 * time.Minute)), EndTime: aws.Time(time.Now().Add(time.Minute)),
		MetricDataQueries: []cwtypes.MetricDataQuery{
			{Id: aws.String("e"), MetricStat: &cwtypes.MetricStat{Period: aws.Int32(3600), Stat: aws.String("Sum"), Metric: &cwtypes.Metric{Namespace: aws.String("App"), MetricName: aws.String("Errors")}}},
			{Id: aws.String("n"), MetricStat: &cwtypes.MetricStat{Period: aws.Int32(3600), Stat: aws.String("SampleCount"), Metric: &cwtypes.Metric{Namespace: aws.String("App"), MetricName: aws.String("Errors")}}},
			{Id: aws.String("l"), MetricStat: &cwtypes.MetricStat{Period: aws.Int32(3600), Stat: aws.String("Sum"), Metric: &cwtypes.Metric{Namespace: aws.String("App"), MetricName: aws.String("Latency"),
				Dimensions: []cwtypes.Dimension{{Name: aws.String("Route"), Value: aws.String("/a")}}}}},
		}}))
	got := map[string]float64{}
	for _, r := range errs.MetricDataResults {
		for _, v := range r.Values { // the events may straddle an hour boundary
			got[*r.Id] += v
		}
	}
	// two matches (1 each) plus one default 0; the {"ms": 9} event has no route, so it publishes no latency.
	if got["e"] != 2 || got["n"] != 3 || got["l"] != 100 {
		t.Fatalf("metrics = %v", got)
	}

	must(m.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("errors-high"), Namespace: aws.String("App"), MetricName: aws.String("Errors"),
		Statistic: cwtypes.StatisticSum, Period: aws.Int32(3600), EvaluationPeriods: aws.Int32(1), Threshold: aws.Float64(1),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold}))
	cw.Evaluate(time.Now())
	a := must(m.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"errors-high"}})).MetricAlarms[0]
	if a.StateValue != cwtypes.StateValueAlarm {
		t.Fatalf("alarm = %s", aws.ToString(a.StateReason))
	}
}
