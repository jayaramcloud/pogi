package monitoring

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	mpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// seriesKey identifies a time series: its project, metric type and labels,
// and resource type and labels.
func seriesKey(project string, ts *mpb.TimeSeries) string {
	lab := func(m map[string]string) string {
		var out []string
		for _, k := range core.SortedKeys(m) {
			out = append(out, k+"="+m[k])
		}
		return strings.Join(out, ",")
	}
	return project + "|" + ts.Metric.Type + "|" + lab(ts.Metric.Labels) + "|" + ts.Resource.Type + "|" + lab(ts.Resource.Labels)
}

func valueOf(v *mpb.TypedValue) (float64, bool) {
	switch x := v.GetValue().(type) {
	case *mpb.TypedValue_Int64Value:
		return float64(x.Int64Value), true
	case *mpb.TypedValue_DoubleValue:
		return x.DoubleValue, true
	case *mpb.TypedValue_BoolValue:
		if x.BoolValue {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func typeOfValue(v *mpb.TypedValue) metric.MetricDescriptor_ValueType {
	switch v.GetValue().(type) {
	case *mpb.TypedValue_Int64Value:
		return metric.MetricDescriptor_INT64
	case *mpb.TypedValue_DoubleValue:
		return metric.MetricDescriptor_DOUBLE
	case *mpb.TypedValue_BoolValue:
		return metric.MetricDescriptor_BOOL
	case *mpb.TypedValue_StringValue:
		return metric.MetricDescriptor_STRING
	case *mpb.TypedValue_DistributionValue:
		return metric.MetricDescriptor_DISTRIBUTION
	}
	return metric.MetricDescriptor_VALUE_TYPE_UNSPECIFIED
}

// typed makes a point value of the type t from a number.
func typed(t metric.MetricDescriptor_ValueType, f float64) *mpb.TypedValue {
	switch t {
	case metric.MetricDescriptor_INT64:
		return &mpb.TypedValue{Value: &mpb.TypedValue_Int64Value{Int64Value: int64(math.Round(f))}}
	case metric.MetricDescriptor_BOOL:
		return &mpb.TypedValue{Value: &mpb.TypedValue_BoolValue{BoolValue: f != 0}}
	}
	return &mpb.TypedValue{Value: &mpb.TypedValue_DoubleValue{DoubleValue: f}}
}

func (s *Service) CreateTimeSeries(_ context.Context, req *mpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	p, err := project(req.Name)
	if err != nil {
		return nil, err
	}
	if len(req.TimeSeries) == 0 || len(req.TimeSeries) > 200 {
		return nil, gapi.Invalid("Field timeSeries has %d items: it needs between 1 and 200", len(req.TimeSeries))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	type write struct {
		key string
		ts  *mpb.TimeSeries
		pt  *mpb.Point
	}
	var writes []write
	seen := map[string]bool{}
	var newDescs []*metric.MetricDescriptor
	for i, in := range req.TimeSeries {
		bad := func(format string, a ...any) error {
			return gapi.Invalid("Field timeSeries[%d] %s", i, fmt.Sprintf(format, a...))
		}
		ts := proto.Clone(in).(*mpb.TimeSeries)
		if ts.Metric.GetType() == "" {
			return nil, bad("metric.type is required")
		}
		if len(ts.Points) != 1 {
			return nil, bad("points had %d points: only one point per series is allowed in a request", len(ts.Points))
		}
		if ts.Resource == nil {
			ts.Resource = &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": p}}
		}
		pt := ts.Points[0]
		vt := typeOfValue(pt.Value)
		if vt == metric.MetricDescriptor_VALUE_TYPE_UNSPECIFIED || pt.Interval.GetEndTime() == nil {
			return nil, bad("points[0] needs a value and an interval.end_time")
		}
		dn := "projects/" + p + "/metricDescriptors/" + ts.Metric.Type
		d := s.st.descriptors[dn]
		for _, nd := range newDescs {
			if nd.Name == dn {
				d = nd
			}
		}
		if d == nil {
			if !isCustom(ts.Metric.Type) {
				return nil, bad("metric.type: the metric %q is not a custom metric and cannot be written", ts.Metric.Type)
			}
			d = &metric.MetricDescriptor{Name: dn, Type: ts.Metric.Type, MetricKind: ts.MetricKind, ValueType: vt}
			if d.MetricKind == metric.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
				d.MetricKind = metric.MetricDescriptor_GAUGE
			}
			for k := range ts.Metric.Labels {
				d.Labels = append(d.Labels, &labelDesc{Key: k})
			}
			slices.SortFunc(d.Labels, func(a, b *labelDesc) int { return strings.Compare(a.Key, b.Key) })
			newDescs = append(newDescs, d)
		}
		if vt != d.ValueType {
			return nil, bad("points[0].value has type %s: the metric is %s", vt, d.ValueType)
		}
		end := pt.Interval.EndTime.AsTime()
		if st := pt.Interval.StartTime; d.MetricKind == metric.MetricDescriptor_GAUGE {
			if st != nil && !st.AsTime().Equal(end) {
				return nil, bad("points[0].interval: a GAUGE point has no start time, or one equal to its end time")
			}
			pt.Interval.StartTime = pt.Interval.EndTime
		} else if st == nil || !st.AsTime().Before(end) {
			return nil, bad("points[0].interval: a %s point needs a start time before its end time", d.MetricKind)
		}
		ts.MetricKind, ts.ValueType, ts.Unit = d.MetricKind, d.ValueType, d.Unit
		key := seriesKey(p, ts)
		if seen[key] {
			return nil, bad("is a duplicate: only one point per time series is allowed in a request")
		}
		seen[key] = true
		if old := s.st.series[key]; old != nil && len(old.Points) > 0 && !end.After(old.Points[len(old.Points)-1].Interval.EndTime.AsTime()) {
			return nil, bad("points[0]: points must be written in order: this one is not newer than the latest point of the series")
		}
		writes = append(writes, write{key, ts, pt})
	}
	for _, d := range newDescs {
		s.st.descriptors[d.Name] = d
	}
	for _, w := range writes {
		if old := s.st.series[w.key]; old != nil {
			old.Points = append(old.Points, w.pt)
			continue
		}
		w.ts.Points = []*mpb.Point{w.pt}
		s.st.series[w.key] = w.ts
	}
	return &emptypb.Empty{}, nil
}

// seriesFilter returns whether a series matches a filter.
func seriesFilter(expr string) (func(*mpb.TimeSeries) bool, error) {
	f, err := gapi.ParseFilter(expr)
	if err != nil {
		return nil, gapi.Invalid("%v", err)
	}
	return func(ts *mpb.TimeSeries) bool {
		return f.Match(func(field, op, v string) bool {
			var got string
			switch {
			case field == "metric.type":
				got = ts.Metric.Type
			case field == "resource.type":
				got = ts.Resource.Type
			case strings.HasPrefix(field, "metric.labels."):
				got = ts.Metric.Labels[strings.TrimPrefix(field, "metric.labels.")]
			case strings.HasPrefix(field, "resource.labels."):
				got = ts.Resource.Labels[strings.TrimPrefix(field, "resource.labels.")]
			}
			return cmpValue(got, op, v)
		}, func(string) bool { return false })
	}, nil
}

func (s *Service) ListTimeSeries(_ context.Context, req *mpb.ListTimeSeriesRequest) (*mpb.ListTimeSeriesResponse, error) {
	p, err := project(req.Name)
	if err != nil {
		return nil, err
	}
	if req.Filter == "" || !strings.Contains(req.Filter, "metric.type") {
		return nil, gapi.Invalid("Field filter is required and must name a metric.type, e.g. metric.type = %q", "custom.googleapis.com/x")
	}
	match, err := seriesFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	if req.Interval.GetEndTime() == nil {
		return nil, gapi.Invalid("Field interval.end_time is required")
	}
	end := req.Interval.EndTime.AsTime()
	start := end
	if req.Interval.StartTime != nil {
		start = req.Interval.StartTime.AsTime()
	}
	if start.After(end) {
		return nil, gapi.Invalid("Field interval.start_time must not be later than interval.end_time")
	}
	agg, second := req.Aggregation, req.SecondaryAggregation
	if second != nil {
		return nil, gapi.Invalid("pogi does not support secondary_aggregation")
	}
	if agg != nil && agg.CrossSeriesReducer != mpb.Aggregation_REDUCE_NONE && agg.PerSeriesAligner == mpb.Aggregation_ALIGN_NONE {
		return nil, gapi.Invalid("Field aggregation.per_series_aligner is required when a cross_series_reducer is set")
	}

	s.mu.Lock()
	var found []*mpb.TimeSeries
	for _, k := range core.SortedKeys(s.st.series) {
		if ts := s.st.series[k]; strings.HasPrefix(k, p+"|") && match(ts) {
			found = append(found, ts)
		}
	}
	// The points in the interval, copied while the lock is held; alignment
	// also wants the point before it (for rates of cumulative metrics).
	var out []*mpb.TimeSeries
	before := map[*mpb.TimeSeries]*mpb.Point{}
	for _, ts := range found {
		c := &mpb.TimeSeries{Metric: ts.Metric, Resource: ts.Resource, MetricKind: ts.MetricKind, ValueType: ts.ValueType, Unit: ts.Unit}
		var prior *mpb.Point
		for _, pt := range ts.Points {
			switch e := pt.Interval.EndTime.AsTime(); {
			case e.Before(start):
				prior = pt
			case !e.After(end):
				c.Points = append(c.Points, pt)
			}
		}
		if len(c.Points) > 0 {
			out = append(out, c)
			before[c] = prior
		}
	}
	s.mu.Unlock()

	if agg != nil && agg.PerSeriesAligner != mpb.Aggregation_ALIGN_NONE {
		if out, err = align(out, before, agg, start, end); err != nil {
			return nil, err
		}
		if agg.CrossSeriesReducer != mpb.Aggregation_REDUCE_NONE {
			if out, err = reduce(out, agg); err != nil {
				return nil, err
			}
		}
	}
	for _, ts := range out { // newest first
		slices.Reverse(ts.Points)
	}
	if req.View == mpb.ListTimeSeriesRequest_HEADERS {
		for _, ts := range out {
			ts.Points = nil
		}
	}
	items, next := gapi.Page(out, req.PageSize, req.PageToken)
	return &mpb.ListTimeSeriesResponse{TimeSeries: items, NextPageToken: next}, nil
}

type num struct {
	end time.Time
	v   float64
}

// aligners compute one aligned value from the points of a bucket (oldest
// first); base is the point before the first, for cumulative metrics, and
// period the bucket's length. ok false drops the bucket.
func aligner(a mpb.Aggregation_Aligner, kind metric.MetricDescriptor_MetricKind, vt metric.MetricDescriptor_ValueType) (f func(pts []num, base *num, period time.Duration) (float64, bool), out metric.MetricDescriptor_ValueType, outKind metric.MetricDescriptor_MetricKind, err error) {
	out, outKind = vt, kind
	switch a {
	case mpb.Aggregation_ALIGN_MIN, mpb.Aggregation_ALIGN_MAX, mpb.Aggregation_ALIGN_SUM, mpb.Aggregation_ALIGN_NEXT_OLDER, mpb.Aggregation_ALIGN_INTERPOLATE:
		red := map[mpb.Aggregation_Aligner]string{mpb.Aggregation_ALIGN_MIN: "min", mpb.Aggregation_ALIGN_MAX: "max", mpb.Aggregation_ALIGN_SUM: "sum"}[a]
		f = func(pts []num, _ *num, _ time.Duration) (float64, bool) {
			if red == "" {
				return pts[len(pts)-1].v, true
			}
			return fold(red, vals(pts)), true
		}
	case mpb.Aggregation_ALIGN_MEAN, mpb.Aggregation_ALIGN_STDDEV, mpb.Aggregation_ALIGN_PERCENTILE_99, mpb.Aggregation_ALIGN_PERCENTILE_95,
		mpb.Aggregation_ALIGN_PERCENTILE_50, mpb.Aggregation_ALIGN_PERCENTILE_05:
		name := map[mpb.Aggregation_Aligner]string{mpb.Aggregation_ALIGN_MEAN: "mean", mpb.Aggregation_ALIGN_STDDEV: "stddev", mpb.Aggregation_ALIGN_PERCENTILE_99: "p99",
			mpb.Aggregation_ALIGN_PERCENTILE_95: "p95", mpb.Aggregation_ALIGN_PERCENTILE_50: "p50", mpb.Aggregation_ALIGN_PERCENTILE_05: "p05"}[a]
		out = metric.MetricDescriptor_DOUBLE
		f = func(pts []num, _ *num, _ time.Duration) (float64, bool) { return fold(name, vals(pts)), true }
	case mpb.Aggregation_ALIGN_COUNT:
		out = metric.MetricDescriptor_INT64
		f = func(pts []num, _ *num, _ time.Duration) (float64, bool) { return float64(len(pts)), true }
	case mpb.Aggregation_ALIGN_COUNT_TRUE, mpb.Aggregation_ALIGN_COUNT_FALSE, mpb.Aggregation_ALIGN_FRACTION_TRUE:
		if vt != metric.MetricDescriptor_BOOL {
			return nil, 0, 0, gapi.Invalid("The aligner %s needs a BOOL metric", a)
		}
		out = metric.MetricDescriptor_INT64
		if a == mpb.Aggregation_ALIGN_FRACTION_TRUE {
			out = metric.MetricDescriptor_DOUBLE
		}
		f = func(pts []num, _ *num, _ time.Duration) (float64, bool) {
			t := 0.0
			for _, p := range pts {
				t += p.v
			}
			switch a {
			case mpb.Aggregation_ALIGN_COUNT_TRUE:
				return t, true
			case mpb.Aggregation_ALIGN_COUNT_FALSE:
				return float64(len(pts)) - t, true
			}
			return t / float64(len(pts)), true
		}
	case mpb.Aggregation_ALIGN_RATE, mpb.Aggregation_ALIGN_DELTA:
		if kind == metric.MetricDescriptor_GAUGE {
			return nil, 0, 0, gapi.Invalid("The aligner %s is not valid for GAUGE metrics", a)
		}
		rate := a == mpb.Aggregation_ALIGN_RATE
		if rate {
			out, outKind = metric.MetricDescriptor_DOUBLE, metric.MetricDescriptor_GAUGE
		} else {
			outKind = metric.MetricDescriptor_DELTA
		}
		f = func(pts []num, base *num, period time.Duration) (float64, bool) {
			if kind == metric.MetricDescriptor_DELTA { // the points hold increments
				t := fold("sum", vals(pts))
				if rate {
					t /= period.Seconds()
				}
				return t, true
			}
			// Cumulative: the growth since the previous point (or the first one in the bucket).
			first := pts[0]
			if base != nil {
				first = *base
			}
			last := pts[len(pts)-1]
			d := last.v - first.v
			if !rate {
				return d, true
			}
			if dt := last.end.Sub(first.end).Seconds(); dt > 0 {
				return d / dt, true
			}
			return 0, false
		}
	default:
		return nil, 0, 0, gapi.Invalid("pogi does not support the aligner %s", a)
	}
	return f, out, outKind, nil
}

func vals(pts []num) []float64 {
	out := make([]float64, len(pts))
	for i, p := range pts {
		out[i] = p.v
	}
	return out
}

// fold reduces numbers: min, max, sum, mean, stddev (sample), p50, p95, p99, p05 (nearest rank).
func fold(op string, v []float64) float64 {
	switch op {
	case "min":
		return slices.Min(v)
	case "max":
		return slices.Max(v)
	case "sum", "mean", "stddev":
		t := 0.0
		for _, x := range v {
			t += x
		}
		mean := t / float64(len(v))
		switch op {
		case "sum":
			return t
		case "mean":
			return mean
		}
		if len(v) < 2 {
			return 0
		}
		ss := 0.0
		for _, x := range v {
			ss += (x - mean) * (x - mean)
		}
		return math.Sqrt(ss / float64(len(v)-1))
	}
	s := slices.Sorted(slices.Values(v))
	q := map[string]float64{"p05": .05, "p50": .5, "p95": .95, "p99": .99}[op]
	return s[max(int(math.Ceil(q*float64(len(s))))-1, 0)]
}

// maxBuckets bounds how many points an alignment makes per series.
const maxBuckets = 100000

// align turns each series into one point per alignment period, the periods
// ending at the interval's end: (end-(k+1)p, end-kp].
func align(in []*mpb.TimeSeries, before map[*mpb.TimeSeries]*mpb.Point, agg *mpb.Aggregation, start, end time.Time) ([]*mpb.TimeSeries, error) {
	period := agg.AlignmentPeriod.AsDuration()
	if period < time.Second {
		return nil, gapi.Invalid("Field aggregation.alignment_period is required, and must be at least 1 second, when an aligner is set")
	}
	n := int(math.Ceil(float64(end.Sub(start)) / float64(period)))
	if n > maxBuckets {
		return nil, gapi.Invalid("The interval has %d alignment periods: at most %d are allowed", n, maxBuckets)
	}
	n = max(n, 1)
	var out []*mpb.TimeSeries
	for _, ts := range in {
		f, vt, kind, err := aligner(agg.PerSeriesAligner, ts.MetricKind, ts.ValueType)
		if err != nil {
			return nil, err
		}
		res := &mpb.TimeSeries{Metric: ts.Metric, Resource: ts.Resource, MetricKind: kind, ValueType: vt, Unit: ts.Unit}
		buckets := make([][]num, n)
		for _, pt := range ts.Points {
			v, ok := valueOf(pt.Value)
			if !ok {
				continue
			}
			e := pt.Interval.EndTime.AsTime()
			k := int(end.Sub(e) / period) // the bucket (end-(k+1)p, end-kp] holds e, except at an exact boundary
			if end.Sub(e)%period == 0 && k > 0 {
				k--
			}
			if k >= 0 && k < n {
				buckets[k] = append(buckets[k], num{e, v})
			}
		}
		var base *num
		if b := before[ts]; b != nil {
			v, _ := valueOf(b.Value)
			base = &num{b.Interval.EndTime.AsTime(), v}
		}
		for k := n - 1; k >= 0; k-- { // oldest bucket first
			if len(buckets[k]) == 0 {
				continue
			}
			if v, ok := f(buckets[k], base, period); ok {
				be := end.Add(-time.Duration(k) * period)
				res.Points = append(res.Points, &mpb.Point{
					Interval: &mpb.TimeInterval{StartTime: timestamppb.New(be.Add(-period)), EndTime: timestamppb.New(be)}, Value: typed(vt, v)})
			}
			if p := buckets[k][len(buckets[k])-1]; kindHasBase(ts.MetricKind) {
				base = &p
			}
		}
		if len(res.Points) > 0 {
			out = append(out, res)
		}
	}
	return out, nil
}

func kindHasBase(k metric.MetricDescriptor_MetricKind) bool {
	return k == metric.MetricDescriptor_CUMULATIVE
}

// reduce combines the aligned series that share the group-by fields, point by point.
func reduce(in []*mpb.TimeSeries, agg *mpb.Aggregation) ([]*mpb.TimeSeries, error) {
	op, ok := map[mpb.Aggregation_Reducer]string{mpb.Aggregation_REDUCE_MEAN: "mean", mpb.Aggregation_REDUCE_MIN: "min", mpb.Aggregation_REDUCE_MAX: "max",
		mpb.Aggregation_REDUCE_SUM: "sum", mpb.Aggregation_REDUCE_STDDEV: "stddev", mpb.Aggregation_REDUCE_COUNT: "count",
		mpb.Aggregation_REDUCE_COUNT_TRUE: "count_true", mpb.Aggregation_REDUCE_COUNT_FALSE: "count_false", mpb.Aggregation_REDUCE_FRACTION_TRUE: "fraction_true",
		mpb.Aggregation_REDUCE_PERCENTILE_99: "p99", mpb.Aggregation_REDUCE_PERCENTILE_95: "p95", mpb.Aggregation_REDUCE_PERCENTILE_50: "p50",
		mpb.Aggregation_REDUCE_PERCENTILE_05: "p05"}[agg.CrossSeriesReducer]
	if !ok {
		return nil, gapi.Invalid("pogi does not support the reducer %s", agg.CrossSeriesReducer)
	}
	type group struct {
		ts     *mpb.TimeSeries
		points map[int64][]float64
		ints   bool
	}
	groups := map[string]*group{}
	var order []string
	for _, ts := range in {
		g := &mpb.TimeSeries{Metric: &metric.Metric{Type: ts.Metric.Type, Labels: map[string]string{}},
			Resource: &monitoredres.MonitoredResource{Type: ts.Resource.Type, Labels: map[string]string{}}, MetricKind: ts.MetricKind, ValueType: ts.ValueType, Unit: ts.Unit}
		var key []string
		for _, f := range agg.GroupByFields {
			switch {
			case strings.HasPrefix(f, "metric.label."), strings.HasPrefix(f, "metric.labels."):
				k := f[strings.Index(f, "label"):]
				k = strings.TrimPrefix(strings.TrimPrefix(k, "labels."), "label.")
				if v, ok := ts.Metric.Labels[k]; ok {
					g.Metric.Labels[k] = v
					key = append(key, "m."+k+"="+v)
				}
			case strings.HasPrefix(f, "resource.label."), strings.HasPrefix(f, "resource.labels."):
				k := f[strings.Index(f, "label"):]
				k = strings.TrimPrefix(strings.TrimPrefix(k, "labels."), "label.")
				if v, ok := ts.Resource.Labels[k]; ok {
					g.Resource.Labels[k] = v
					key = append(key, "r."+k+"="+v)
				}
			}
		}
		k := ts.Metric.Type + "|" + ts.Resource.Type + "|" + strings.Join(key, ",")
		gr := groups[k]
		if gr == nil {
			gr = &group{ts: g, points: map[int64][]float64{}, ints: ts.ValueType == metric.MetricDescriptor_INT64}
			groups[k] = gr
			order = append(order, k)
		}
		for _, pt := range ts.Points {
			v, _ := valueOf(pt.Value)
			t := pt.Interval.EndTime.AsTime().UnixNano()
			gr.points[t] = append(gr.points[t], v)
		}
	}
	period := agg.AlignmentPeriod.AsDuration()
	var out []*mpb.TimeSeries
	for _, k := range order {
		g := groups[k]
		vt := metric.MetricDescriptor_DOUBLE
		switch {
		case strings.HasPrefix(op, "count"):
			vt = metric.MetricDescriptor_INT64
		case g.ints && slices.Contains([]string{"sum", "min", "max"}, op):
			vt = metric.MetricDescriptor_INT64
		}
		g.ts.ValueType = vt
		if g.ts.MetricKind == metric.MetricDescriptor_CUMULATIVE {
			g.ts.MetricKind = metric.MetricDescriptor_GAUGE
		}
		times := make([]int64, 0, len(g.points))
		for t := range g.points {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		for _, t := range times {
			v := g.points[t]
			var r float64
			switch op {
			case "count":
				r = float64(len(v))
			case "count_true", "count_false", "fraction_true":
				tr := 0.0
				for _, x := range v {
					tr += x
				}
				r = map[string]float64{"count_true": tr, "count_false": float64(len(v)) - tr, "fraction_true": tr / float64(len(v))}[op]
			default:
				r = fold(op, v)
			}
			e := time.Unix(0, t)
			g.ts.Points = append(g.ts.Points, &mpb.Point{Interval: &mpb.TimeInterval{StartTime: timestamppb.New(e.Add(-period)), EndTime: timestamppb.New(e)}, Value: typed(vt, r)})
		}
		out = append(out, g.ts)
	}
	return out, nil
}

func (s *Service) CreateServiceTimeSeries(ctx context.Context, req *mpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	return s.CreateTimeSeries(ctx, req)
}
