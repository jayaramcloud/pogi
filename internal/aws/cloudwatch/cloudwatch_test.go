package cloudwatch_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	cw "github.com/nrndev/pogi/internal/aws/cloudwatch"
	snssvc "github.com/nrndev/pogi/internal/aws/sns"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

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

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// sniffer records the protocol headers the SDK sends and receives.
type sniffer struct {
	mu        sync.Mutex
	protocols map[string]bool
}

func (s *sniffer) Do(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(r)
	if err == nil {
		s.mu.Lock()
		s.protocols[r.Header.Get("Smithy-Protocol")+" "+r.URL.Path+" -> "+resp.Header.Get("Content-Type")] = true
		s.mu.Unlock()
	}
	return resp, err
}

func TestMetricsAlarmsOverCBOR(t *testing.T) {
	var svc *cw.Service
	hub, base := core.ServeTest(t,
		func(h *core.Hub) core.Service { svc = cw.New(h); return svc },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return snssvc.New(h) })
	cfg := awstest.Config(t, base)
	sn := &sniffer{protocols: map[string]bool{}}
	c := cloudwatch.NewFromConfig(cfg, func(o *cloudwatch.Options) { o.HTTPClient = sn })

	minute := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	dims := []types.Dimension{{Name: aws.String("Env"), Value: aws.String("dev")}}
	must(c.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{Namespace: aws.String("App"), MetricData: []types.MetricDatum{
		{MetricName: aws.String("Latency"), Dimensions: dims, Timestamp: aws.Time(minute.Add(5 * time.Second)), Value: aws.Float64(10), Unit: types.StandardUnitMilliseconds},
		{MetricName: aws.String("Latency"), Dimensions: dims, Timestamp: aws.Time(minute.Add(20 * time.Second)), Values: []float64{20, 30}, Counts: []float64{1, 2}, Unit: types.StandardUnitMilliseconds},
		{MetricName: aws.String("Latency"), Dimensions: dims, Timestamp: aws.Time(minute.Add(70 * time.Second)), StatisticValues: &types.StatisticSet{SampleCount: aws.Float64(2), Sum: aws.Float64(100), Minimum: aws.Float64(40), Maximum: aws.Float64(60)}, Unit: types.StandardUnitMilliseconds},
		{MetricName: aws.String("Errors"), Timestamp: aws.Time(minute.Add(10 * time.Second)), Value: aws.Float64(3)},
	}}))
	for p := range sn.protocols {
		if !strings.HasPrefix(p, "rpc-v2-cbor /service/GraniteServiceVersion20100801/operation/PutMetricData -> application/cbor") {
			t.Fatalf("the Go SDK did not speak RPCv2 CBOR: %q", p)
		}
	}
	if len(sn.protocols) == 0 {
		t.Fatal("no request recorded")
	}

	stats := must(c.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{Namespace: aws.String("App"), MetricName: aws.String("Latency"),
		Dimensions: dims, StartTime: aws.Time(minute), EndTime: aws.Time(minute.Add(3 * time.Minute)), Period: aws.Int32(60),
		Statistics:         []types.Statistic{types.StatisticSum, types.StatisticAverage, types.StatisticMinimum, types.StatisticMaximum, types.StatisticSampleCount},
		ExtendedStatistics: []string{"p50"}}))
	if len(stats.Datapoints) != 2 {
		t.Fatalf("datapoints = %+v", stats.Datapoints)
	}
	first := stats.Datapoints[0] // 10, 20, 30, 30
	if *first.Sum != 90 || *first.SampleCount != 4 || *first.Average != 22.5 || *first.Minimum != 10 || *first.Maximum != 30 ||
		first.ExtendedStatistics["p50"] != 20 || first.Unit != types.StandardUnitMilliseconds || !first.Timestamp.Equal(minute) {
		t.Fatalf("first datapoint = %+v", first)
	}
	if second := stats.Datapoints[1]; *second.Sum != 100 || *second.Average != 50 || !second.Timestamp.Equal(minute.Add(time.Minute)) {
		t.Fatalf("second datapoint = %+v", second)
	}

	stat := func(id, name string) types.MetricDataQuery {
		return types.MetricDataQuery{Id: aws.String(id), MetricStat: &types.MetricStat{Period: aws.Int32(60), Stat: aws.String("Sum"),
			Metric: &types.Metric{Namespace: aws.String("App"), MetricName: aws.String(name), Dimensions: map[string][]types.Dimension{"Latency": dims}[name]}}}
	}
	data := must(c.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{StartTime: aws.Time(minute), EndTime: aws.Time(minute.Add(3 * time.Minute)),
		ScanBy: types.ScanByTimestampAscending, MetricDataQueries: []types.MetricDataQuery{
			stat("m1", "Latency"), stat("m2", "Errors"),
			{Id: aws.String("double"), Expression: aws.String("m1 * 2")},
			{Id: aws.String("total"), Expression: aws.String("SUM(METRICS())"), Label: aws.String("all")},
		}}))
	got := map[string][]float64{}
	for _, r := range data.MetricDataResults {
		got[*r.Id] = r.Values
		if r.StatusCode != types.StatusCodeComplete || len(r.Timestamps) != len(r.Values) {
			t.Fatalf("result = %+v", r)
		}
	}
	if want := `{"double":[180,200],"m1":[90,100],"m2":[3],"total":[93,100]}`; string(must(json.Marshal(got))) != want {
		t.Fatalf("GetMetricData = %v, want %s", got, want)
	}

	list := must(c.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("App"),
		Dimensions: []types.DimensionFilter{{Name: aws.String("Env")}}}))
	if len(list.Metrics) != 1 || *list.Metrics[0].MetricName != "Latency" || *list.Metrics[0].Dimensions[0].Value != "dev" {
		t.Fatalf("ListMetrics = %+v", list.Metrics)
	}

	// Errors carry the legacy query code too (awsQueryCompatible).
	_, err := c.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{Namespace: aws.String("AWS/EC2"),
		MetricData: []types.MetricDatum{{MetricName: aws.String("x"), Value: aws.Float64(1)}}})
	var bad *types.InvalidParameterValueException
	if !errors.As(err, &bad) || bad.ErrorCode() != "InvalidParameterValue" {
		t.Fatalf("want InvalidParameterValue, got %v", err)
	}

	// An alarm over the metric notifies an SNS topic, which feeds an SQS queue.
	q := sqs.NewFromConfig(cfg)
	qurl := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("alarms")})).QueueUrl
	topic := must(sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("ops")})).TopicArn
	must(sns.NewFromConfig(cfg).Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic, Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:alarms")}))
	must(c.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("high-cpu"), Namespace: aws.String("App"),
		MetricName: aws.String("CPU"), Statistic: types.StatisticAverage, Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1),
		Threshold: aws.Float64(50), ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
		AlarmActions: []string{*topic}, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ops")}}}))
	alarm := func() types.MetricAlarm {
		out := must(c.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"high-cpu"}}))
		if len(out.MetricAlarms) != 1 {
			t.Fatalf("DescribeAlarms = %+v", out)
		}
		return out.MetricAlarms[0]
	}
	if a := alarm(); a.StateValue != types.StateValueInsufficientData || *a.Threshold != 50 || *a.AlarmArn != "arn:aws:cloudwatch:us-east-1:000000000000:alarm:high-cpu" {
		t.Fatalf("new alarm = %+v", a)
	}
	must(c.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{Namespace: aws.String("App"),
		MetricData: []types.MetricDatum{{MetricName: aws.String("CPU"), Value: aws.Float64(90)}}}))
	roundTrip(t, hub)
	svc.Evaluate(time.Now())
	if a := alarm(); a.StateValue != types.StateValueAlarm || a.StateUpdatedTimestamp == nil || !strings.Contains(*a.StateReason, "Threshold Crossed") {
		t.Fatalf("alarm after evaluation = %+v", a)
	}
	msgs := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qurl, WaitTimeSeconds: 2}))
	var env struct{ Subject, Message string }
	var note struct{ AlarmName, NewStateValue, OldStateValue string }
	if len(msgs.Messages) != 1 || json.Unmarshal([]byte(*msgs.Messages[0].Body), &env) != nil || json.Unmarshal([]byte(env.Message), &note) != nil ||
		env.Subject != `ALARM: "high-cpu" in us-east-1` || note.AlarmName != "high-cpu" || note.NewStateValue != "ALARM" || note.OldStateValue != "INSUFFICIENT_DATA" {
		t.Fatalf("alarm notification = %+v", msgs.Messages)
	}

	must(c.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("high-cpu"), StateValue: types.StateValueOk, StateReason: aws.String("manual")}))
	hist := must(c.DescribeAlarmHistory(ctx, &cloudwatch.DescribeAlarmHistoryInput{AlarmName: aws.String("high-cpu"), HistoryItemType: types.HistoryItemTypeStateUpdate}))
	if len(hist.AlarmHistoryItems) != 2 || !strings.Contains(*hist.AlarmHistoryItems[0].HistorySummary, "ALARM to OK") {
		t.Fatalf("history = %+v", hist.AlarmHistoryItems)
	}
	tags := must(c.ListTagsForResource(ctx, &cloudwatch.ListTagsForResourceInput{ResourceARN: aws.String("arn:aws:cloudwatch:us-east-1:000000000000:alarm:high-cpu")}))
	if len(tags.Tags) != 1 || *tags.Tags[0].Value != "ops" {
		t.Fatalf("tags = %+v", tags.Tags)
	}
	_, err = c.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{AlarmNames: []string{"high-cpu", "nope"}})
	var nf *types.ResourceNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("want ResourceNotFound, got %v", err)
	}
	must(c.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{AlarmNames: []string{"high-cpu"}}))

	must(c.PutDashboard(ctx, &cloudwatch.PutDashboardInput{DashboardName: aws.String("main"), DashboardBody: aws.String(`{"widgets":[]}`)}))
	if d := must(c.GetDashboard(ctx, &cloudwatch.GetDashboardInput{DashboardName: aws.String("main")})); *d.DashboardBody != `{"widgets":[]}` {
		t.Fatalf("dashboard = %+v", d)
	}
	_, err = c.GetDashboard(ctx, &cloudwatch.GetDashboardInput{DashboardName: aws.String("nope")})
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "ResourceNotFound" {
		t.Fatalf("want ResourceNotFound, got %v", err)
	}

	// Python, JS and the CLI speak JSON 1.0; old clients speak Query.
	req, _ := http.NewRequest(http.MethodPost, base, strings.NewReader(`{"Namespace":"App"}`))
	req.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801.ListMetrics")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	resp := must(http.DefaultClient.Do(req))
	body := string(must(io.ReadAll(resp.Body)))
	if resp.Header.Get("Content-Type") != "application/x-amz-json-1.0" || !strings.Contains(body, `"MetricName":"Latency"`) {
		t.Fatalf("JSON 1.0 ListMetrics = %s %s", resp.Header.Get("Content-Type"), body)
	}
	form := url.Values{"Action": {"ListMetrics"}, "Version": {"2010-08-01"}, "Namespace": {"App"}, "Dimensions.member.1.Name": {"Env"}}
	req, _ = http.NewRequest(http.MethodPost, base, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	awstest.Sign(t, req, "monitoring")
	resp = must(http.DefaultClient.Do(req))
	body = string(must(io.ReadAll(resp.Body)))
	if !strings.Contains(body, `<ListMetricsResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/"><ListMetricsResult><Metrics><member><Namespace>App</Namespace><MetricName>Latency</MetricName>`) {
		t.Fatalf("Query ListMetrics = %s", body)
	}
}

// floci #2277: the SDK gzips a PutMetricData body over 10 KB; pogi must read it.
// floci #4577: a metric-math alarm reports its Metrics but no Period the request never sent.
func TestGzipBodyAndMetricMathAlarm(t *testing.T) {
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return cw.New(h) })
	c := cloudwatch.NewFromConfig(awstest.Config(t, base))
	var data []types.MetricDatum
	for i := 0; i < 800; i++ {
		data = append(data, types.MetricDatum{MetricName: aws.String("Metric" + strings.Repeat("x", i%7) + string(rune('A'+i%26))), Value: aws.Float64(1),
			Dimensions: []types.Dimension{{Name: aws.String("d"), Value: aws.String(strings.Repeat("v", 20))}}})
	}
	must(c.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{Namespace: aws.String("Big"), MetricData: data}))
	if l := must(c.ListMetrics(ctx, &cloudwatch.ListMetricsInput{Namespace: aws.String("Big")})); len(l.Metrics) == 0 {
		t.Fatal("the gzipped PutMetricData stored nothing")
	}

	must(c.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("math"), ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods: aws.Int32(1), Threshold: aws.Float64(1), Metrics: []types.MetricDataQuery{
			{Id: aws.String("m1"), ReturnData: aws.Bool(false), MetricStat: &types.MetricStat{Period: aws.Int32(60), Stat: aws.String("Sum"),
				Metric: &types.Metric{Namespace: aws.String("NS"), MetricName: aws.String("M")}}},
			{Id: aws.String("e1"), Expression: aws.String("m1*2"), ReturnData: aws.Bool(true)}}}))
	d := must(c.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{"math"}}))
	if a := d.MetricAlarms[0]; len(a.Metrics) != 2 || a.Period != nil {
		t.Fatalf("metric-math alarm = %d metrics, Period %v", len(a.Metrics), a.Period)
	}
}
