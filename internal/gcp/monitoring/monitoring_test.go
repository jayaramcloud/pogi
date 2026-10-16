package monitoring_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	mpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/nrndev/pogi/internal/core"
	monitoringsvc "github.com/nrndev/pogi/internal/gcp/monitoring"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func opts(url string) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
}

func setup(t *testing.T) (*monitoringsvc.Service, *monitoring.MetricClient, string) {
	t.Helper()
	var svc *monitoringsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = monitoringsvc.New(h); return svc })
	c := must(monitoring.NewMetricClient(ctx, opts(url)...))
	t.Cleanup(func() { c.Close() })
	return svc, c, url
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

const proj = "projects/proj"

// point is one data point of a series, written at end-ago.
func write(t *testing.T, c *monitoring.MetricClient, typ string, labels map[string]string, kind metric.MetricDescriptor_MetricKind, end time.Time, ago time.Duration, v *mpb.TypedValue) {
	t.Helper()
	at := timestamppb.New(end.Add(-ago))
	iv := &mpb.TimeInterval{EndTime: at}
	if kind != metric.MetricDescriptor_GAUGE {
		iv.StartTime = timestamppb.New(end.Add(-time.Hour))
	}
	err := c.CreateTimeSeries(ctx, &mpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*mpb.TimeSeries{{
		Metric: &metric.Metric{Type: typ, Labels: labels}, MetricKind: kind,
		Resource: &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "proj"}},
		Points:   []*mpb.Point{{Interval: iv, Value: v}}}}})
	if err != nil {
		t.Fatal(err)
	}
}

func dbl(f float64) *mpb.TypedValue {
	return &mpb.TypedValue{Value: &mpb.TypedValue_DoubleValue{DoubleValue: f}}
}

func i64(i int64) *mpb.TypedValue {
	return &mpb.TypedValue{Value: &mpb.TypedValue_Int64Value{Int64Value: i}}
}

func list(t *testing.T, c *monitoring.MetricClient, filter string, end time.Time, window time.Duration, agg *mpb.Aggregation) []*mpb.TimeSeries {
	t.Helper()
	it := c.ListTimeSeries(ctx, &mpb.ListTimeSeriesRequest{Name: proj, Filter: filter, Aggregation: agg, View: mpb.ListTimeSeriesRequest_FULL,
		Interval: &mpb.TimeInterval{StartTime: timestamppb.New(end.Add(-window)), EndTime: timestamppb.New(end)}})
	var out []*mpb.TimeSeries
	for {
		ts, err := it.Next()
		if err == iterator.Done {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ts)
	}
}

// values lists a series' points, newest first.
func values(ts *mpb.TimeSeries) []float64 {
	var out []float64
	for _, p := range ts.Points {
		switch v := p.Value.Value.(type) {
		case *mpb.TypedValue_DoubleValue:
			out = append(out, v.DoubleValue)
		case *mpb.TypedValue_Int64Value:
			out = append(out, float64(v.Int64Value))
		}
	}
	return out
}

func eq(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if d := a[i] - b[i]; d > 1e-9 || d < -1e-9 {
			return false
		}
	}
	return true
}

func TestMetricsAndAggregation(t *testing.T) {
	_, c, url := setup(t)
	const typ = "custom.googleapis.com/app/latency"
	d := must(c.CreateMetricDescriptor(ctx, &mpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: &metric.MetricDescriptor{
		Type: typ, MetricKind: metric.MetricDescriptor_GAUGE, ValueType: metric.MetricDescriptor_DOUBLE, Unit: "ms", DisplayName: "latency",
		Labels: []*label.LabelDescriptor{{Key: "host", ValueType: label.LabelDescriptor_STRING}}}}))
	if d.Name != proj+"/metricDescriptors/"+typ {
		t.Fatalf("descriptor = %v", d)
	}
	_, err := c.CreateMetricDescriptor(ctx, &mpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: &metric.MetricDescriptor{Type: "compute.googleapis.com/x"}})
	wantCode(t, err, codes.InvalidArgument)
	if got := must(c.GetMetricDescriptor(ctx, &mpb.GetMetricDescriptorRequest{Name: d.Name})); got.Unit != "ms" {
		t.Fatalf("get = %v", got)
	}
	_, err = c.GetMetricDescriptor(ctx, &mpb.GetMetricDescriptorRequest{Name: proj + "/metricDescriptors/custom.googleapis.com/none"})
	wantCode(t, err, codes.NotFound)

	end := time.Now().Truncate(time.Minute)
	s := time.Second
	// host a: bucket 0 holds 4 and 8, bucket 1 holds 10, bucket 2 holds 2; host b: 2 in bucket 0, 6 in bucket 1.
	for _, w := range []struct {
		host string
		ago  time.Duration
		v    float64
	}{{"a", 150 * s, 2}, {"a", 70 * s, 10}, {"a", 30 * s, 8}, {"a", 10 * s, 4}, {"b", 90 * s, 6}, {"b", 20 * s, 2}} {
		write(t, c, typ, map[string]string{"host": w.host}, metric.MetricDescriptor_GAUGE, end, w.ago, dbl(w.v))
	}
	// Writing an older point, or a second point per series in one request, fails.
	err = c.CreateTimeSeries(ctx, &mpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*mpb.TimeSeries{{
		Metric: &metric.Metric{Type: typ, Labels: map[string]string{"host": "a"}}, Resource: &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "proj"}},
		Points: []*mpb.Point{{Interval: &mpb.TimeInterval{EndTime: timestamppb.New(end.Add(-time.Hour))}, Value: dbl(1)}}}}})
	wantCode(t, err, codes.InvalidArgument)
	err = c.CreateTimeSeries(ctx, &mpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*mpb.TimeSeries{{
		Metric: &metric.Metric{Type: typ}, Points: []*mpb.Point{{Interval: &mpb.TimeInterval{EndTime: timestamppb.Now()}, Value: i64(1)}}}}})
	wantCode(t, err, codes.InvalidArgument) // an INT64 point for a DOUBLE metric

	// Raw points, newest first, and the label filter.
	all := list(t, c, `metric.type = "`+typ+`"`, end, 5*time.Minute, nil)
	if len(all) != 2 {
		t.Fatalf("series = %v", all)
	}
	a := list(t, c, `metric.type = "`+typ+`" AND metric.labels.host = "a"`, end, 5*time.Minute, nil)
	if len(a) != 1 || !eq(values(a[0]), []float64{4, 8, 10, 2}) || a[0].Unit != "ms" || a[0].MetricKind != metric.MetricDescriptor_GAUGE {
		t.Fatalf("host a = %v", a)
	}
	if got := list(t, c, `metric.type = starts_with("custom.googleapis.com/app") AND resource.type = "global"`, end, 5*time.Minute, nil); len(got) != 2 {
		t.Fatalf("starts_with = %d series", len(got))
	}
	if got := list(t, c, `metric.type = "`+typ+`" AND metric.labels.host = one_of("b","c")`, end, 5*time.Minute, nil); len(got) != 1 {
		t.Fatalf("one_of = %d series", len(got))
	}
	if got := list(t, c, `metric.type = "`+typ+`"`, end, 40*s, nil); len(got) != 2 || len(got[0].Points) != 2 && len(got[1].Points) != 1 {
		t.Fatalf("short window = %v", got)
	}

	align := func(al mpb.Aggregation_Aligner, red mpb.Aggregation_Reducer, group ...string) *mpb.Aggregation {
		return &mpb.Aggregation{AlignmentPeriod: durationpb.New(time.Minute), PerSeriesAligner: al, CrossSeriesReducer: red, GroupByFields: group}
	}
	f := `metric.type = "` + typ + `" AND metric.labels.host = "a"`
	for _, tc := range []struct {
		al   mpb.Aggregation_Aligner
		want []float64
	}{
		{mpb.Aggregation_ALIGN_MEAN, []float64{6, 10, 2}}, {mpb.Aggregation_ALIGN_SUM, []float64{12, 10, 2}},
		{mpb.Aggregation_ALIGN_MAX, []float64{8, 10, 2}}, {mpb.Aggregation_ALIGN_MIN, []float64{4, 10, 2}}, {mpb.Aggregation_ALIGN_COUNT, []float64{2, 1, 1}},
	} {
		got := list(t, c, f, end, 3*time.Minute, align(tc.al, mpb.Aggregation_REDUCE_NONE))
		if len(got) != 1 || !eq(values(got[0]), tc.want) {
			t.Errorf("%v = %v, want %v", tc.al, got, tc.want)
		}
	}
	got := list(t, c, f, end, 3*time.Minute, align(mpb.Aggregation_ALIGN_MEAN, mpb.Aggregation_REDUCE_NONE))
	if p := got[0].Points[0]; !p.Interval.EndTime.AsTime().Equal(end) || !p.Interval.StartTime.AsTime().Equal(end.Add(-time.Minute)) {
		t.Fatalf("aligned interval = %v", p.Interval)
	}

	// Across series: the sum per bucket, and per host.
	all = list(t, c, `metric.type = "`+typ+`"`, end, 3*time.Minute, align(mpb.Aggregation_ALIGN_MEAN, mpb.Aggregation_REDUCE_SUM))
	if len(all) != 1 || !eq(values(all[0]), []float64{8, 16, 2}) || len(all[0].Metric.Labels) != 0 {
		t.Fatalf("reduced = %v", all)
	}
	all = list(t, c, `metric.type = "`+typ+`"`, end, 3*time.Minute, align(mpb.Aggregation_ALIGN_MEAN, mpb.Aggregation_REDUCE_MEAN, "metric.label.host"))
	if len(all) != 2 || all[0].Metric.Labels["host"] != "a" || !eq(values(all[1]), []float64{2, 6}) {
		t.Fatalf("grouped = %v", all)
	}
	all = list(t, c, `metric.type = "`+typ+`"`, end, 3*time.Minute, align(mpb.Aggregation_ALIGN_MAX, mpb.Aggregation_REDUCE_COUNT))
	if len(all) != 1 || !eq(values(all[0]), []float64{2, 2, 1}) {
		t.Fatalf("count = %v", all)
	}
	// A reducer without an aligner, and a missing filter or interval, are refused.
	_, err = c.ListTimeSeries(ctx, &mpb.ListTimeSeriesRequest{Name: proj, Filter: `metric.type = "` + typ + `"`,
		Interval: &mpb.TimeInterval{EndTime: timestamppb.Now()}, Aggregation: &mpb.Aggregation{CrossSeriesReducer: mpb.Aggregation_REDUCE_SUM}}).Next()
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.ListTimeSeries(ctx, &mpb.ListTimeSeriesRequest{Name: proj, Interval: &mpb.TimeInterval{EndTime: timestamppb.Now()}}).Next()
	wantCode(t, err, codes.InvalidArgument)

	// Headers only.
	hdr := c.ListTimeSeries(ctx, &mpb.ListTimeSeriesRequest{Name: proj, Filter: `metric.type = "` + typ + `"`, View: mpb.ListTimeSeriesRequest_HEADERS,
		Interval: &mpb.TimeInterval{StartTime: timestamppb.New(end.Add(-time.Hour)), EndTime: timestamppb.New(end)}})
	if ts, err := hdr.Next(); err != nil || len(ts.Points) != 0 {
		t.Fatalf("headers = %v, %v", ts, err)
	}

	// The REST mirror: repeated query parameters, and a metric type with slashes in the path.
	q := neturl.Values{"filter": {`metric.type = "` + typ + `"`}, "interval.endTime": {end.Format(time.RFC3339)}, "interval.startTime": {end.Add(-3 * time.Minute).Format(time.RFC3339)},
		"aggregation.alignmentPeriod": {"60s"}, "aggregation.perSeriesAligner": {"ALIGN_MEAN"}, "aggregation.crossSeriesReducer": {"REDUCE_SUM"},
		"aggregation.groupByFields": {"metric.label.host", "resource.label.project_id"}}
	var rest struct {
		TimeSeries []struct {
			Metric struct{ Labels map[string]string }
			Points []struct{ Value struct{ DoubleValue float64 } }
		}
	}
	httpJSON(t, "GET", url+"/v3/projects/proj/timeSeries?"+q.Encode(), nil, &rest)
	if len(rest.TimeSeries) != 2 {
		t.Fatalf("rest series = %+v", rest)
	}
	var rd struct{ Name, Unit string }
	httpJSON(t, "GET", url+"/v3/projects/proj/metricDescriptors/"+typ, nil, &rd)
	if rd.Unit != "ms" || rd.Name != proj+"/metricDescriptors/"+typ {
		t.Fatalf("rest descriptor = %+v", rd)
	}

	// Listing and deleting descriptors.
	it := c.ListMetricDescriptors(ctx, &mpb.ListMetricDescriptorsRequest{Name: proj, Filter: `metric.type = starts_with("custom.googleapis.com/")`})
	if m, err := it.Next(); err != nil || m.Type != typ {
		t.Fatalf("list descriptors = %v, %v", m, err)
	}
	if err := c.DeleteMetricDescriptor(ctx, &mpb.DeleteMetricDescriptorRequest{Name: d.Name}); err != nil {
		t.Fatal(err)
	}
	wantCode(t, c.DeleteMetricDescriptor(ctx, &mpb.DeleteMetricDescriptorRequest{Name: d.Name}), codes.NotFound)
}

func httpJSON(t *testing.T, method, url string, body any, out any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, url, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s %s = %d %s", method, url, resp.StatusCode, b)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRatesAndAutoDescriptors(t *testing.T) {
	_, c, _ := setup(t)
	end := time.Now().Truncate(time.Minute)
	s := time.Second
	// A cumulative counter that grows by 60 every 30 seconds: 2 per second. Writing it creates its descriptor.
	for i, ago := range []time.Duration{100 * s, 70 * s, 40 * s, 10 * s} {
		write(t, c, "custom.googleapis.com/jobs/done", nil, metric.MetricDescriptor_CUMULATIVE, end, ago, i64(int64(100+60*i)))
	}
	d := must(c.GetMetricDescriptor(ctx, &mpb.GetMetricDescriptorRequest{Name: proj + "/metricDescriptors/custom.googleapis.com/jobs/done"}))
	if d.MetricKind != metric.MetricDescriptor_CUMULATIVE || d.ValueType != metric.MetricDescriptor_INT64 {
		t.Fatalf("auto descriptor = %v", d)
	}
	agg := func(al mpb.Aggregation_Aligner) *mpb.Aggregation {
		return &mpb.Aggregation{AlignmentPeriod: durationpb.New(time.Minute), PerSeriesAligner: al}
	}
	rate := list(t, c, `metric.type = "custom.googleapis.com/jobs/done"`, end, 2*time.Minute, agg(mpb.Aggregation_ALIGN_RATE))
	if len(rate) != 1 || !eq(values(rate[0]), []float64{2, 2}) || rate[0].MetricKind != metric.MetricDescriptor_GAUGE || rate[0].ValueType != metric.MetricDescriptor_DOUBLE {
		t.Fatalf("rate = %v", rate)
	}
	delta := list(t, c, `metric.type = "custom.googleapis.com/jobs/done"`, end, 2*time.Minute, agg(mpb.Aggregation_ALIGN_DELTA))
	if len(delta) != 1 || !eq(values(delta[0]), []float64{120, 60}) {
		t.Fatalf("delta = %v", delta)
	}
	// A DELTA metric's rate is its increments per second.
	must(c.CreateMetricDescriptor(ctx, &mpb.CreateMetricDescriptorRequest{Name: proj, MetricDescriptor: &metric.MetricDescriptor{
		Type: "custom.googleapis.com/jobs/failed", MetricKind: metric.MetricDescriptor_DELTA, ValueType: metric.MetricDescriptor_INT64}}))
	write(t, c, "custom.googleapis.com/jobs/failed", nil, metric.MetricDescriptor_DELTA, end, 50*s, i64(30))
	write(t, c, "custom.googleapis.com/jobs/failed", nil, metric.MetricDescriptor_DELTA, end, 20*s, i64(30))
	if got := list(t, c, `metric.type = "custom.googleapis.com/jobs/failed"`, end, time.Minute, agg(mpb.Aggregation_ALIGN_RATE)); len(got) != 1 || !eq(values(got[0]), []float64{1}) {
		t.Fatalf("delta rate = %v", got)
	}
	// A gauge has no rate.
	write(t, c, "custom.googleapis.com/g", nil, metric.MetricDescriptor_GAUGE, end, 20*s, dbl(1))
	_, err := c.ListTimeSeries(ctx, &mpb.ListTimeSeriesRequest{Name: proj, Filter: `metric.type = "custom.googleapis.com/g"`,
		Interval: &mpb.TimeInterval{StartTime: timestamppb.New(end.Add(-time.Minute)), EndTime: timestamppb.New(end)}, Aggregation: agg(mpb.Aggregation_ALIGN_RATE)}).Next()
	wantCode(t, err, codes.InvalidArgument)
	// Built-in metrics cannot be written.
	err = c.CreateTimeSeries(ctx, &mpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*mpb.TimeSeries{{
		Metric: &metric.Metric{Type: "compute.googleapis.com/instance/uptime"}, Points: []*mpb.Point{{Interval: &mpb.TimeInterval{EndTime: timestamppb.Now()}, Value: dbl(1)}}}}})
	wantCode(t, err, codes.InvalidArgument)
}

func TestResourceDescriptorsAndStoredConfig(t *testing.T) {
	svc, c, url := setup(t)
	rd := must(c.GetMonitoredResourceDescriptor(ctx, &mpb.GetMonitoredResourceDescriptorRequest{Name: proj + "/monitoredResourceDescriptors/gce_instance"}))
	if rd.Type != "gce_instance" || len(rd.Labels) != 3 {
		t.Fatalf("resource descriptor = %v", rd)
	}
	n := 0
	for it := c.ListMonitoredResourceDescriptors(ctx, &mpb.ListMonitoredResourceDescriptorsRequest{Name: proj}); ; n++ {
		if _, err := it.Next(); err != nil {
			break
		}
	}
	if n < 10 {
		t.Fatalf("%d resource descriptors", n)
	}

	nc := must(monitoring.NewNotificationChannelClient(ctx, opts(url)...))
	defer nc.Close()
	ch := must(nc.CreateNotificationChannel(ctx, &mpb.CreateNotificationChannelRequest{Name: proj, NotificationChannel: &mpb.NotificationChannel{
		Type: "email", DisplayName: "oncall", Labels: map[string]string{"email_address": "a@example.com"}}}))
	if !strings.HasPrefix(ch.Name, proj+"/notificationChannels/") || ch.VerificationStatus != mpb.NotificationChannel_VERIFIED || !ch.Enabled.Value {
		t.Fatalf("channel = %v", ch)
	}
	_, err := nc.CreateNotificationChannel(ctx, &mpb.CreateNotificationChannelRequest{Name: proj, NotificationChannel: &mpb.NotificationChannel{Type: "carrier-pigeon"}})
	wantCode(t, err, codes.InvalidArgument)
	ch = must(nc.UpdateNotificationChannel(ctx, &mpb.UpdateNotificationChannelRequest{NotificationChannel: &mpb.NotificationChannel{Name: ch.Name, DisplayName: "pager"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}}}))
	if ch.DisplayName != "pager" || ch.Labels["email_address"] == "" || len(ch.MutationRecords) != 1 {
		t.Fatalf("updated channel = %v", ch)
	}

	ap := must(monitoring.NewAlertPolicyClient(ctx, opts(url)...))
	defer ap.Close()
	pol := must(ap.CreateAlertPolicy(ctx, &mpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: &mpb.AlertPolicy{DisplayName: "high latency",
		Combiner: mpb.AlertPolicy_OR, NotificationChannels: []string{ch.Name}, Conditions: []*mpb.AlertPolicy_Condition{{DisplayName: "p99 > 1s",
			Condition: &mpb.AlertPolicy_Condition_ConditionThreshold{ConditionThreshold: &mpb.AlertPolicy_Condition_MetricThreshold{
				Filter: `metric.type = "custom.googleapis.com/app/latency"`, Comparison: mpb.ComparisonType_COMPARISON_GT, ThresholdValue: 1000, Duration: durationpb.New(time.Minute)}}}}}}))
	if !strings.HasPrefix(pol.Name, proj+"/alertPolicies/") || !strings.HasPrefix(pol.Conditions[0].Name, pol.Name+"/conditions/") || pol.CreationRecord == nil {
		t.Fatalf("policy = %v", pol)
	}
	_, err = ap.CreateAlertPolicy(ctx, &mpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: &mpb.AlertPolicy{}})
	wantCode(t, err, codes.InvalidArgument)
	pol2 := must(ap.UpdateAlertPolicy(ctx, &mpb.UpdateAlertPolicyRequest{AlertPolicy: &mpb.AlertPolicy{Name: pol.Name, DisplayName: "latency"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}}}))
	if pol2.DisplayName != "latency" || len(pol2.Conditions) != 1 {
		t.Fatalf("updated policy = %v", pol2)
	}
	if got := must(ap.ListAlertPolicies(ctx, &mpb.ListAlertPoliciesRequest{Name: proj}).Next()); got.Name != pol.Name {
		t.Fatalf("list = %v", got)
	}

	up := must(monitoring.NewUptimeCheckClient(ctx, opts(url)...))
	defer up.Close()
	chk := must(up.CreateUptimeCheckConfig(ctx, &mpb.CreateUptimeCheckConfigRequest{Parent: proj, UptimeCheckConfig: &mpb.UptimeCheckConfig{DisplayName: "Home page",
		Resource:         &mpb.UptimeCheckConfig_MonitoredResource{MonitoredResource: &monitoredres.MonitoredResource{Type: "uptime_url", Labels: map[string]string{"host": "example.com"}}},
		CheckRequestType: &mpb.UptimeCheckConfig_HttpCheck_{HttpCheck: &mpb.UptimeCheckConfig_HttpCheck{Path: "/"}},
		Period:           durationpb.New(time.Minute), Timeout: durationpb.New(10 * time.Second)}}))
	if !strings.HasPrefix(chk.Name, proj+"/uptimeCheckConfigs/home-page-") {
		t.Fatalf("uptime check = %v", chk)
	}

	// REST over the same state.
	var pols struct {
		AlertPolicies []struct{ DisplayName string }
	}
	httpJSON(t, "GET", url+"/v3/"+proj+"/alertPolicies", nil, &pols)
	if len(pols.AlertPolicies) != 1 || pols.AlertPolicies[0].DisplayName != "latency" {
		t.Fatalf("rest policies = %+v", pols)
	}

	// Everything survives Save and Load, and goes on Reset.
	b := must(svc.Save())
	svc.Reset()
	if _, err := ap.GetAlertPolicy(ctx, &mpb.GetAlertPolicyRequest{Name: pol.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("after reset: %v", err)
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if got := must(ap.GetAlertPolicy(ctx, &mpb.GetAlertPolicyRequest{Name: pol.Name})); got.DisplayName != "latency" {
		t.Fatalf("after load = %v", got)
	}
	for _, del := range []func() error{
		func() error { return ap.DeleteAlertPolicy(ctx, &mpb.DeleteAlertPolicyRequest{Name: pol.Name}) },
		func() error {
			return nc.DeleteNotificationChannel(ctx, &mpb.DeleteNotificationChannelRequest{Name: ch.Name})
		},
		func() error {
			return up.DeleteUptimeCheckConfig(ctx, &mpb.DeleteUptimeCheckConfigRequest{Name: chk.Name})
		},
	} {
		if err := del(); err != nil {
			t.Fatal(err)
		}
		wantCode(t, del(), codes.NotFound)
	}
}

func TestPersistsSeries(t *testing.T) {
	svc, c, _ := setup(t)
	end := time.Now().Truncate(time.Minute)
	write(t, c, "custom.googleapis.com/p", map[string]string{"k": "v"}, metric.MetricDescriptor_GAUGE, end, 10*time.Second, dbl(7))
	b := must(svc.Save())
	svc.Reset()
	if got := list(t, c, `metric.type = "custom.googleapis.com/p"`, end, time.Minute, nil); len(got) != 0 {
		t.Fatalf("after reset: %v", got)
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if got := list(t, c, `metric.type = "custom.googleapis.com/p"`, end, time.Minute, nil); len(got) != 1 || !eq(values(got[0]), []float64{7}) || got[0].Metric.Labels["k"] != "v" {
		t.Fatalf("after load: %v", got)
	}
	if rows := svc.Resources(); len(rows) != 5 || len(rows[1].Rows) != 1 || svc.Inspect("Time series", rows[1].Rows[0][0]) == nil {
		t.Fatalf("console = %v", rows)
	}
}
