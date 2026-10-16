// Package cloudwatch emulates Amazon CloudWatch metrics, alarms and
// dashboards over every protocol its SDKs speak: RPCv2-CBOR (Go, Java),
// JSON 1.0 (Python, JS, CLI) and the legacy Query protocol. Metric alarms are
// evaluated in the background and fire their actions through the hub.
package cloudwatch

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const (
	retention  = 15 * 24 * time.Hour // CloudWatch keeps high-resolution data for 15 days at most
	maxPoints  = 100_000             // per metric
	maxHistory = 100                 // per alarm
	tick       = 10 * time.Second
)

// Service is the CloudWatch emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex
	st   state
	stop chan struct{}
	once sync.Once
}

type state struct {
	Metrics    map[string]*Metric    `json:"metrics"`    // account/region/namespace/name/dims
	Alarms     map[string]*stored    `json:"alarms"`     // account/region/name
	Dashboards map[string]*Dashboard `json:"dashboards"` // account/name (dashboards are global)
}

// Dimension is a metric dimension.
type Dimension struct {
	Name  string
	Value string
}

// Tag is a CloudWatch resource tag.
type Tag struct {
	Key   string
	Value string
}

// Metric is one time series.
type Metric struct {
	Account    string      `json:"account"`
	Region     string      `json:"region"`
	Namespace  string      `json:"namespace"`
	Name       string      `json:"name"`
	Dimensions []Dimension `json:"dimensions,omitempty"`
	Points     []Point     `json:"points"`
}

// Point is one datum: a value, values with counts, or a statistic set.
type Point struct {
	T      time.Time `json:"t"`
	N      float64   `json:"n"`
	Sum    float64   `json:"sum"`
	Min    float64   `json:"min"`
	Max    float64   `json:"max"`
	Values []float64 `json:"values,omitempty"` // samples of a Values/Counts datum, for percentiles
	Counts []float64 `json:"counts,omitempty"`
	Unit   string    `json:"unit,omitempty"`
}

// Alarm is a metric or composite alarm in its MetricAlarm/CompositeAlarm wire
// shape; composite alarms set AlarmRule.
type Alarm struct {
	AlarmName                          string
	AlarmArn                           string
	AlarmDescription                   string `json:",omitempty"`
	AlarmConfigurationUpdatedTimestamp core.Time
	ActionsEnabled                     bool
	OKActions                          []string `json:",omitempty"`
	AlarmActions                       []string `json:",omitempty"`
	InsufficientDataActions            []string `json:",omitempty"`
	StateValue                         string
	StateReason                        string `json:",omitempty"`
	StateReasonData                    string `json:",omitempty"`
	StateUpdatedTimestamp              core.Time
	StateTransitionedTimestamp         core.Time
	MetricName                         string      `json:",omitempty"`
	Namespace                          string      `json:",omitempty"`
	Statistic                          string      `json:",omitempty"`
	ExtendedStatistic                  string      `json:",omitempty"`
	Dimensions                         []Dimension `json:",omitempty"`
	Period                             int         `json:",omitempty"`
	Unit                               string      `json:",omitempty"`
	EvaluationPeriods                  int         `json:",omitempty"`
	DatapointsToAlarm                  int         `json:",omitempty"`
	Threshold                          *float64    `json:",omitempty"`
	ComparisonOperator                 string      `json:",omitempty"`
	TreatMissingData                   string      `json:",omitempty"`
	EvaluateLowSampleCountPercentile   string      `json:",omitempty"`
	Metrics                            []query     `json:",omitempty"`
	ThresholdMetricId                  string      `json:",omitempty"`
	AlarmRule                          string      `json:",omitempty"`
}

// stored is an alarm with what the API does not return inline.
type stored struct {
	Alarm
	Account string            `json:"account"`
	Region  string            `json:"region"`
	Tags    map[string]string `json:"tags,omitempty"`
	History []HistoryItem     `json:"history,omitempty"`
}

// HistoryItem is one DescribeAlarmHistory entry.
type HistoryItem struct {
	AlarmName       string
	AlarmType       string
	Timestamp       core.Time
	HistoryItemType string
	HistorySummary  string
	HistoryData     string
}

// Dashboard is a stored dashboard.
type Dashboard struct {
	Name     string    `json:"name"`
	Account  string    `json:"account"`
	Body     string    `json:"body"`
	Modified time.Time `json:"modified"`
}

type metricID struct {
	Namespace  string
	MetricName string
	Dimensions []Dimension `json:",omitempty"`
}

type metricStat struct {
	Metric metricID
	Period int
	Stat   string
	Unit   string `json:",omitempty"`
}

// query is a MetricDataQuery (GetMetricData and metric-math alarms).
type query struct {
	Id         string
	MetricStat *metricStat `json:",omitempty"`
	Expression string      `json:",omitempty"`
	Label      string      `json:",omitempty"`
	ReturnData *bool       `json:",omitempty"`
	Period     int         `json:",omitempty"`
	AccountId  string      `json:",omitempty"`
}

// New creates the CloudWatch service and starts alarm evaluation.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, stop: make(chan struct{})}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.0", XMLNS: "http://monitoring.amazonaws.com/doc/2010-08-01/", QueryCodes: queryCodes, Ops: map[string]core.Handler{
		"PutMetricData":           core.H(s.putMetricData),
		"GetMetricData":           core.H(s.getMetricData),
		"GetMetricStatistics":     core.H(s.getMetricStatistics),
		"ListMetrics":             core.H(s.listMetrics),
		"PutMetricAlarm":          core.H(s.putMetricAlarm),
		"PutCompositeAlarm":       core.H(s.putCompositeAlarm),
		"DescribeAlarms":          core.H(s.describeAlarms),
		"DescribeAlarmsForMetric": core.H(s.describeAlarmsForMetric),
		"DeleteAlarms":            core.H(s.deleteAlarms),
		"SetAlarmState":           core.H(s.setAlarmState),
		"EnableAlarmActions":      core.H(s.enableActions(true)),
		"DisableAlarmActions":     core.H(s.enableActions(false)),
		"DescribeAlarmHistory":    core.H(s.describeAlarmHistory),
		"PutDashboard":            core.H(s.putDashboard),
		"GetDashboard":            core.H(s.getDashboard),
		"ListDashboards":          core.H(s.listDashboards),
		"DeleteDashboards":        core.H(s.deleteDashboards),
		"TagResource":             core.H(s.tagResource),
		"UntagResource":           core.H(s.untagResource),
		"ListTagsForResource":     core.H(s.listTagsForResource),
	}}
	core.Go("cloudwatch.loop", func() { s.loop() })
	return s
}

// queryCodes maps error shapes to the legacy codes of this awsQueryCompatible service.
var queryCodes = map[string]string{
	"InvalidParameterValueException":       "InvalidParameterValue;Sender",
	"InvalidParameterCombinationException": "InvalidParameterCombination;Sender",
	"MissingRequiredParameterException":    "MissingParameter;Sender",
	"DashboardNotFoundError":               "ResourceNotFound;Sender",
	"DashboardInvalidInputError":           "InvalidParameterInput;Sender",
	"InvalidFormatFault":                   "InvalidFormat;Sender",
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudwatch", Cloud: core.AWS, Title: "CloudWatch", Targets: []string{"GraniteServiceVersion20100801"},
		Signing: []string{"monitoring"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Metrics: map[string]*Metric{}, Alarms: map[string]*stored{}, Dashboards: map[string]*Dashboard{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Metrics: map[string]*Metric{}, Alarms: map[string]*stored{}, Dashboards: map[string]*Dashboard{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// Close stops alarm evaluation; the hub may call it more than once.
func (s *Service) Close() error {
	s.once.Do(func() { close(s.stop) })
	return nil
}

func (s *Service) loop() {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			func() { defer core.Recover("cloudwatch.evaluate"); s.Evaluate(now) }() // a panic skips one tick
		}
	}
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValueException", format, a...)
}

func missing(name string) error {
	return core.Errorf(400, "MissingRequiredParameterException", "The parameter %s is required.", name)
}

func combination(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterCombinationException", format, a...)
}

func noAlarm(name string) error {
	return core.Errorf(404, "ResourceNotFound", "Alarm %s not found", name)
}

func scope(c *core.Ctx) string { return c.Account + "/" + c.Region + "/" }

// metricKey identifies a time series; dimensions are order-insensitive.
func metricKey(account, region string, id metricID) string {
	k := account + "/" + region + "/" + id.Namespace + "\x00" + id.MetricName
	for _, d := range sortDims(id.Dimensions) {
		k += "\x00" + d.Name + "=" + d.Value
	}
	return k
}

func sortDims(d []Dimension) []Dimension {
	d = slices.Clone(d)
	sort.Slice(d, func(i, j int) bool { return d[i].Name < d[j].Name })
	return d
}

// ---- metrics ----

type datum struct {
	MetricName      string
	Dimensions      []Dimension
	Timestamp       *core.Time
	Value           *float64
	StatisticValues *struct{ SampleCount, Sum, Minimum, Maximum float64 }
	Values          []float64
	Counts          []float64
	Unit            string
}

func (s *Service) putMetricData(c *core.Ctx, in *struct {
	Namespace        string
	MetricData       []datum
	EntityMetricData []struct{ MetricData []datum }
}) (any, error) {
	if in.Namespace == "" {
		return nil, missing("Namespace")
	}
	if strings.HasPrefix(in.Namespace, "AWS/") && !c.Internal { // only pogi's own services publish AWS/ metrics
		return nil, invalid("The value AWS/ for parameter Namespace is invalid.")
	}
	data := in.MetricData
	for _, e := range in.EntityMetricData {
		data = append(data, e.MetricData...)
	}
	points := make([]Point, len(data))
	for i, d := range data {
		p, err := toPoint(d, fmt.Sprintf("MetricData.member.%d.", i+1))
		if err != nil {
			return nil, err
		}
		points[i] = p
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, d := range data {
		id := metricID{in.Namespace, d.MetricName, sortDims(d.Dimensions)}
		k := metricKey(c.Account, c.Region, id)
		m := s.st.Metrics[k]
		if m == nil {
			m = &Metric{Account: c.Account, Region: c.Region, Namespace: id.Namespace, Name: id.MetricName, Dimensions: id.Dimensions}
			s.st.Metrics[k] = m
		}
		m.Points = append(m.Points, points[i])
		m.trim(now)
	}
	return nil, nil
}

func toPoint(d datum, f string) (Point, error) {
	p := Point{T: time.Now().UTC(), Unit: d.Unit}
	if d.MetricName == "" {
		return p, missing(f + "MetricName")
	}
	if d.Timestamp != nil {
		p.T = time.Time(*d.Timestamp).UTC()
	}
	n := 0
	for _, set := range []bool{d.Value != nil, len(d.Values) > 0, d.StatisticValues != nil} {
		if set {
			n++
		}
	}
	switch {
	case n > 1:
		return p, combination("The parameters %sValue, %sValues and %sStatisticValues are mutually exclusive and you have specified more than one.", f, f, f)
	case d.Value != nil:
		p.N, p.Sum, p.Min, p.Max = 1, *d.Value, *d.Value, *d.Value
	case len(d.Values) > 0:
		if len(d.Counts) > 0 && len(d.Counts) != len(d.Values) {
			return p, invalid("The parameters %sValues and %sCounts must be of the same size.", f, f)
		}
		p.Values, p.Counts, p.Min, p.Max = d.Values, d.Counts, math.Inf(1), math.Inf(-1)
		for i, v := range d.Values {
			c := 1.0
			if len(d.Counts) > 0 {
				c = d.Counts[i]
			}
			p.N, p.Sum, p.Min, p.Max = p.N+c, p.Sum+v*c, min(p.Min, v), max(p.Max, v)
		}
	case d.StatisticValues != nil:
		v := d.StatisticValues
		p.N, p.Sum, p.Min, p.Max = v.SampleCount, v.Sum, v.Minimum, v.Maximum
	default:
		return p, combination("At least one of the parameters %sValue, %sValues or %sStatisticValues must be specified.", f, f, f)
	}
	return p, nil
}

// trim drops points past retention or over the cap; appends are roughly in
// time order, so old points sit at the front. Lock held.
func (m *Metric) trim(now time.Time) {
	cut := 0
	for cut < len(m.Points) && now.Sub(m.Points[cut].T) > retention {
		cut++
	}
	if n := len(m.Points) - cut; n > maxPoints {
		cut += n - maxPoints
	}
	m.Points = m.Points[cut:]
}

// agg accumulates the points of one bucket.
type agg struct {
	n, sum, min, max float64
	vals, cnts       []float64 // samples for percentiles
	unit             string
}

func (a *agg) add(p *Point) {
	a.n, a.sum, a.min, a.max = a.n+p.N, a.sum+p.Sum, min(a.min, p.Min), max(a.max, p.Max)
	if a.unit == "" {
		a.unit = unitOf(p)
	}
	switch {
	case len(p.Values) > 0:
		a.vals = append(a.vals, p.Values...)
		for i := range p.Values {
			c := 1.0
			if len(p.Counts) > 0 {
				c = p.Counts[i]
			}
			a.cnts = append(a.cnts, c)
		}
	case p.N == 1:
		a.vals, a.cnts = append(a.vals, p.Min), append(a.cnts, 1)
	}
}

func unitOf(p *Point) string {
	if p.Unit == "" {
		return "None"
	}
	return p.Unit
}

// stat computes Sum, Average, Minimum, Maximum, SampleCount or pNN.N.
func (a *agg) stat(name string) (float64, bool) {
	switch name {
	case "SampleCount":
		return a.n, true
	case "Sum":
		return a.sum, true
	case "Average":
		return a.sum / a.n, a.n > 0
	case "Minimum":
		return a.min, true
	case "Maximum":
		return a.max, true
	}
	pct, err := strconv.ParseFloat(strings.TrimPrefix(name, "p"), 64)
	if !strings.HasPrefix(name, "p") || err != nil || len(a.vals) == 0 {
		return 0, false // ponytail: other extended statistics (TM, WM, IQM…) yield no data
	}
	idx := make([]int, len(a.vals))
	total := 0.0
	for i := range idx {
		idx[i], total = i, total+a.cnts[i]
	}
	sort.Slice(idx, func(i, j int) bool { return a.vals[idx[i]] < a.vals[idx[j]] })
	seen := 0.0
	for _, i := range idx {
		if seen += a.cnts[i]; seen >= pct/100*total {
			return a.vals[i], true
		}
	}
	return a.vals[idx[len(idx)-1]], true
}

// bucketFn maps a datapoint time to its bucket for a period; ok=false skips it.
type bucketFn func(t time.Time, period int64) (key int64, ok bool)

// aligned buckets [start, end) into period windows keyed by their epoch start.
func aligned(start, end time.Time) bucketFn {
	return func(t time.Time, period int64) (int64, bool) {
		u := t.Unix()
		return u - u%period, !t.Before(start) && t.Before(end)
	}
}

// windows buckets the n periods ending at now, keyed 0 (newest) to n-1, the
// way alarms evaluate.
func windows(now time.Time, n int64) bucketFn {
	return func(t time.Time, period int64) (int64, bool) {
		k := int64(now.Sub(t) / (time.Duration(period) * time.Second))
		return k, !t.After(now) && k < n
	}
}

func (m *Metric) aggregate(unit string, period int64, bucket bucketFn) map[int64]*agg {
	out := map[int64]*agg{}
	if period <= 0 {
		return out
	}
	for i := range m.Points {
		p := &m.Points[i]
		k, ok := bucket(p.T, period)
		if !ok || unit != "" && unit != unitOf(p) {
			continue
		}
		a := out[k]
		if a == nil {
			a = &agg{min: math.Inf(1), max: math.Inf(-1)}
			out[k] = a
		}
		a.add(p)
	}
	return out
}

// evalQueries computes the series of every query: metric stats first, then
// expressions. It returns id → bucket → value and per-id problems. Lock held.
func (s *Service) evalQueries(account, region string, qs []query, bucket bucketFn) (map[string]map[int64]float64, map[string]string) {
	series, problems := map[string]map[int64]float64{}, map[string]string{}
	var stats []string
	for _, q := range qs {
		if q.MetricStat == nil {
			continue
		}
		out := map[int64]float64{}
		if m := s.st.Metrics[metricKey(account, region, q.MetricStat.Metric)]; m != nil {
			for k, a := range m.aggregate(q.MetricStat.Unit, int64(q.MetricStat.Period), bucket) {
				if v, ok := a.stat(q.MetricStat.Stat); ok {
					out[k] = v
				}
			}
		}
		series[q.Id] = out
		stats = append(stats, q.Id)
	}
	for _, q := range qs {
		if q.MetricStat == nil && q.Expression != "" {
			if v, ok := expr(q.Expression, series, stats); ok {
				series[q.Id] = v
			} else {
				problems[q.Id] = "pogi evaluates only SUM/AVG/MIN/MAX(METRICS()), ids and 'a op b' arithmetic"
			}
		}
	}
	return series, problems
}

var (
	aggExpr = regexp.MustCompile(`^(SUM|AVG|MIN|MAX)\(METRICS\(\)\)$`)
	binExpr = regexp.MustCompile(`^([\w.]+)\s*([-+*/])\s*([\w.]+)$`)
)

// expr evaluates the metric math pogi supports.
func expr(e string, series map[string]map[int64]float64, stats []string) (map[int64]float64, bool) {
	e = strings.TrimSpace(e)
	if v, ok := series[e]; ok {
		return v, true
	}
	if m := aggExpr.FindStringSubmatch(e); m != nil {
		out, n := map[int64]float64{}, map[int64]float64{}
		for _, id := range stats {
			for k, v := range series[id] {
				switch old, seen := out[k], n[k] > 0; {
				case !seen:
					out[k] = v
				case m[1] == "MIN":
					out[k] = min(old, v)
				case m[1] == "MAX":
					out[k] = max(old, v)
				default:
					out[k] = old + v
				}
				n[k]++
			}
		}
		if m[1] == "AVG" {
			for k := range out {
				out[k] /= n[k]
			}
		}
		return out, true
	}
	m := binExpr.FindStringSubmatch(e)
	if m == nil {
		return nil, false
	}
	l, lc, ok1 := operand(m[1], series)
	r, rc, ok2 := operand(m[3], series)
	if !ok1 || !ok2 || l == nil && r == nil {
		return nil, false
	}
	keys := l
	if keys == nil {
		keys = r
	}
	out := map[int64]float64{}
	for k := range keys {
		x, y, ok := lc, rc, true
		if l != nil {
			x, ok = l[k]
		}
		if r != nil && ok {
			y, ok = r[k]
		}
		switch {
		case !ok:
		case m[2] == "+":
			out[k] = x + y
		case m[2] == "-":
			out[k] = x - y
		case m[2] == "*":
			out[k] = x * y
		case y != 0:
			out[k] = x / y
		}
	}
	return out, true
}

func operand(s string, series map[string]map[int64]float64) (map[int64]float64, float64, bool) {
	if v, ok := series[s]; ok {
		return v, 0, true
	}
	f, err := strconv.ParseFloat(s, 64)
	return nil, f, err == nil
}

type message struct{ Code, Value string }

type dataResult struct {
	Id         string
	Label      string
	Timestamps []core.Time
	Values     []float64
	StatusCode string
	Messages   []message `json:",omitempty"`
}

func (s *Service) getMetricData(c *core.Ctx, in *struct {
	MetricDataQueries []query
	StartTime         core.Time
	EndTime           core.Time
	ScanBy            string
}) (any, error) {
	start, end := time.Time(in.StartTime), time.Time(in.EndTime)
	if len(in.MetricDataQueries) == 0 {
		return nil, missing("MetricDataQueries")
	}
	if !start.Before(end) {
		return nil, invalid("The parameter StartTime must be less than the parameter EndTime.")
	}
	for i, q := range in.MetricDataQueries {
		if (q.MetricStat == nil) == (q.Expression == "") {
			return nil, invalid("Exactly one of MetricStat and Expression must be specified in MetricDataQueries.member.%d.", i+1)
		}
		if q.MetricStat != nil && q.MetricStat.Period <= 0 {
			return nil, invalid("The parameter MetricDataQueries.member.%d.MetricStat.Period must be greater than 0.", i+1)
		}
	}
	s.mu.Lock()
	series, problems := s.evalQueries(c.Account, c.Region, in.MetricDataQueries, aligned(start, end))
	s.mu.Unlock()
	out := &struct{ MetricDataResults []dataResult }{[]dataResult{}}
	for _, q := range in.MetricDataQueries {
		if q.ReturnData != nil && !*q.ReturnData {
			continue
		}
		r := dataResult{Id: q.Id, Label: q.Label, StatusCode: "Complete", Timestamps: []core.Time{}, Values: []float64{}}
		if r.Label == "" {
			r.Label = q.Id
			if q.MetricStat != nil {
				r.Label = q.MetricStat.Metric.MetricName
			}
		}
		keys := slices.Sorted(maps.Keys(series[q.Id]))
		if in.ScanBy != "TimestampAscending" {
			slices.Reverse(keys)
		}
		for _, k := range keys {
			r.Timestamps = append(r.Timestamps, core.Time(time.Unix(k, 0).UTC()))
			r.Values = append(r.Values, series[q.Id][k])
		}
		if p, ok := problems[q.Id]; ok {
			r.Messages = []message{{"Unsupported", p}}
		}
		out.MetricDataResults = append(out.MetricDataResults, r)
	}
	return out, nil
}

type datapoint struct {
	Timestamp          core.Time
	SampleCount        *float64           `json:",omitempty"`
	Average            *float64           `json:",omitempty"`
	Sum                *float64           `json:",omitempty"`
	Minimum            *float64           `json:",omitempty"`
	Maximum            *float64           `json:",omitempty"`
	Unit               string             `json:",omitempty"`
	ExtendedStatistics map[string]float64 `json:",omitempty"`
}

func (s *Service) getMetricStatistics(c *core.Ctx, in *struct {
	Namespace          string
	MetricName         string
	Dimensions         []Dimension
	StartTime          core.Time
	EndTime            core.Time
	Period             int
	Statistics         []string
	ExtendedStatistics []string
	Unit               string
}) (any, error) {
	start, end := time.Time(in.StartTime), time.Time(in.EndTime)
	switch {
	case in.Namespace == "":
		return nil, missing("Namespace")
	case in.MetricName == "":
		return nil, missing("MetricName")
	case in.Period <= 0 || in.Period > 60 && in.Period%60 != 0:
		return nil, invalid("The parameter Period must be a multiple of 60.")
	case len(in.Statistics) == 0 && len(in.ExtendedStatistics) == 0:
		return nil, combination("Must specify either Statistics or ExtendedStatistics.")
	case !start.Before(end):
		return nil, invalid("The parameter StartTime must be less than the parameter EndTime.")
	}
	s.mu.Lock()
	var aggs map[int64]*agg
	if m := s.st.Metrics[metricKey(c.Account, c.Region, metricID{in.Namespace, in.MetricName, in.Dimensions})]; m != nil {
		aggs = m.aggregate(in.Unit, int64(in.Period), aligned(start, end))
	}
	s.mu.Unlock()
	out := &struct {
		Label      string
		Datapoints []datapoint
	}{in.MetricName, []datapoint{}}
	for _, k := range slices.Sorted(maps.Keys(aggs)) {
		a := aggs[k]
		dp := datapoint{Timestamp: core.Time(time.Unix(k, 0).UTC()), Unit: a.unit}
		fields := map[string]**float64{"SampleCount": &dp.SampleCount, "Average": &dp.Average, "Sum": &dp.Sum, "Minimum": &dp.Minimum, "Maximum": &dp.Maximum}
		for _, st := range in.Statistics {
			if v, ok := a.stat(st); ok && fields[st] != nil {
				*fields[st] = &v
			}
		}
		for _, st := range in.ExtendedStatistics {
			if v, ok := a.stat(st); ok {
				if dp.ExtendedStatistics == nil {
					dp.ExtendedStatistics = map[string]float64{}
				}
				dp.ExtendedStatistics[st] = v
			}
		}
		out.Datapoints = append(out.Datapoints, dp)
	}
	return out, nil
}

func (s *Service) listMetrics(c *core.Ctx, in *struct {
	Namespace      string
	MetricName     string
	Dimensions     []struct{ Name, Value string }
	RecentlyActive string
}) (any, error) {
	since := time.Time{}
	if in.RecentlyActive != "" {
		since = time.Now().Add(-3 * time.Hour) // PT3H is the only accepted value
	}
	out := &struct{ Metrics []metricID }{[]metricID{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Metrics) {
		m := s.st.Metrics[k]
		if m.Account != c.Account || m.Region != c.Region || in.Namespace != "" && m.Namespace != in.Namespace ||
			in.MetricName != "" && m.Name != in.MetricName || len(m.Points) == 0 || m.Points[len(m.Points)-1].T.Before(since) {
			continue
		}
		ok := true
		for _, f := range in.Dimensions {
			ok = ok && slices.ContainsFunc(m.Dimensions, func(d Dimension) bool { return d.Name == f.Name && (f.Value == "" || d.Value == f.Value) })
		}
		if ok {
			out.Metrics = append(out.Metrics, metricID{m.Namespace, m.Name, m.Dimensions})
		}
	}
	return out, nil
}

// ---- alarms ----

func (s *Service) putMetricAlarm(c *core.Ctx, in *struct {
	AlarmName                        string
	AlarmDescription                 string
	ActionsEnabled                   *bool
	OKActions                        []string
	AlarmActions                     []string
	InsufficientDataActions          []string
	MetricName                       string
	Namespace                        string
	Statistic                        string
	ExtendedStatistic                string
	Dimensions                       []Dimension
	Period                           int
	Unit                             string
	EvaluationPeriods                int
	DatapointsToAlarm                int
	Threshold                        *float64
	ComparisonOperator               string
	TreatMissingData                 string
	EvaluateLowSampleCountPercentile string
	Metrics                          []query
	Tags                             []Tag
	ThresholdMetricId                string
}) (any, error) {
	switch {
	case in.AlarmName == "":
		return nil, missing("AlarmName")
	case in.ComparisonOperator == "":
		return nil, missing("ComparisonOperator")
	case in.EvaluationPeriods < 1:
		return nil, invalid("The parameter EvaluationPeriods must be greater than or equal to 1.")
	case in.DatapointsToAlarm > in.EvaluationPeriods:
		return nil, invalid("DatapointsToAlarm must be less than or equal to EvaluationPeriods.")
	case (in.MetricName == "") == (len(in.Metrics) == 0):
		return nil, combination("Exactly one of MetricName and Metrics must be specified.")
	case in.MetricName != "" && (in.Statistic == "") == (in.ExtendedStatistic == ""):
		return nil, combination("Exactly one of Statistic and ExtendedStatistic must be specified.")
	case in.MetricName != "" && in.Period <= 0:
		return nil, missing("Period")
	case in.Threshold == nil && in.ThresholdMetricId == "":
		return nil, missing("Threshold")
	}
	s.upsert(c, Alarm{
		AlarmName: in.AlarmName, AlarmDescription: in.AlarmDescription, ActionsEnabled: in.ActionsEnabled == nil || *in.ActionsEnabled,
		OKActions: in.OKActions, AlarmActions: in.AlarmActions, InsufficientDataActions: in.InsufficientDataActions,
		MetricName: in.MetricName, Namespace: in.Namespace, Statistic: in.Statistic, ExtendedStatistic: in.ExtendedStatistic,
		Dimensions: in.Dimensions, Period: in.Period, Unit: in.Unit, EvaluationPeriods: in.EvaluationPeriods,
		DatapointsToAlarm: in.DatapointsToAlarm, Threshold: in.Threshold, ComparisonOperator: in.ComparisonOperator,
		TreatMissingData: in.TreatMissingData, EvaluateLowSampleCountPercentile: in.EvaluateLowSampleCountPercentile,
		Metrics: in.Metrics, ThresholdMetricId: in.ThresholdMetricId,
	}, in.Tags)
	return nil, nil
}

func (s *Service) putCompositeAlarm(c *core.Ctx, in *struct {
	AlarmName               string
	AlarmRule               string
	AlarmDescription        string
	ActionsEnabled          *bool
	AlarmActions            []string
	OKActions               []string
	InsufficientDataActions []string
	Tags                    []Tag
}) (any, error) {
	if in.AlarmName == "" {
		return nil, missing("AlarmName")
	}
	if in.AlarmRule == "" {
		return nil, missing("AlarmRule")
	}
	// ponytail: composite alarms are stored, not evaluated; SetAlarmState drives them.
	s.upsert(c, Alarm{AlarmName: in.AlarmName, AlarmRule: in.AlarmRule, AlarmDescription: in.AlarmDescription,
		ActionsEnabled: in.ActionsEnabled == nil || *in.ActionsEnabled, AlarmActions: in.AlarmActions,
		OKActions: in.OKActions, InsufficientDataActions: in.InsufficientDataActions}, in.Tags)
	return nil, nil
}

// upsert creates an alarm, or replaces its configuration and keeps its state
// (and tags) as PutMetricAlarm does.
func (s *Service) upsert(c *core.Ctx, cfg Alarm, tags []Tag) {
	now := core.Time(time.Now().UTC())
	s.mu.Lock()
	defer s.mu.Unlock()
	k := scope(c) + cfg.AlarmName
	a, verb := s.st.Alarms[k], "updated"
	if a == nil {
		a, verb = &stored{Account: c.Account, Region: c.Region, Tags: tagMap(tags)}, "created"
		a.StateValue, a.StateReason, a.StateUpdatedTimestamp, a.StateTransitionedTimestamp = "INSUFFICIENT_DATA", "Unchecked: Initial alarm creation", now, now
		s.st.Alarms[k] = a
	}
	cfg.StateValue, cfg.StateReason, cfg.StateReasonData = a.StateValue, a.StateReason, a.StateReasonData
	cfg.StateUpdatedTimestamp, cfg.StateTransitionedTimestamp = a.StateUpdatedTimestamp, a.StateTransitionedTimestamp
	cfg.AlarmArn, cfg.AlarmConfigurationUpdatedTimestamp = c.ARN("cloudwatch", "alarm:"+cfg.AlarmName), now
	a.Alarm = cfg
	a.record("ConfigurationUpdate", fmt.Sprintf("Alarm %q %s", cfg.AlarmName, verb), map[string]any{"version": "1.0", "type": verb})
}

func tagMap(tags []Tag) map[string]string {
	m := map[string]string{}
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

func (a *stored) kind() string {
	if a.AlarmRule != "" {
		return "CompositeAlarm"
	}
	return "MetricAlarm"
}

func (a *stored) record(typ, summary string, data any) {
	b, _ := json.Marshal(data)
	a.History = append(a.History, HistoryItem{AlarmName: a.AlarmName, AlarmType: a.kind(), Timestamp: core.Time(time.Now().UTC()),
		HistoryItemType: typ, HistorySummary: summary, HistoryData: string(b)})
	if len(a.History) > maxHistory {
		a.History = a.History[len(a.History)-maxHistory:]
	}
}

func (s *Service) describeAlarms(c *core.Ctx, in *struct {
	AlarmNames      []string
	AlarmNamePrefix string
	AlarmTypes      []string
	StateValue      string
	ActionPrefix    string
}) (any, error) {
	types := in.AlarmTypes
	if len(types) == 0 {
		types = []string{"MetricAlarm"} // AWS returns only metric alarms by default
	}
	out := &struct{ MetricAlarms, CompositeAlarms []Alarm }{[]Alarm{}, []Alarm{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Alarms) {
		a := s.st.Alarms[k]
		actions := slices.Concat(a.AlarmActions, a.OKActions, a.InsufficientDataActions)
		if a.Account != c.Account || a.Region != c.Region || !slices.Contains(types, a.kind()) ||
			len(in.AlarmNames) > 0 && !slices.Contains(in.AlarmNames, a.AlarmName) || !strings.HasPrefix(a.AlarmName, in.AlarmNamePrefix) ||
			in.StateValue != "" && a.StateValue != in.StateValue ||
			in.ActionPrefix != "" && !slices.ContainsFunc(actions, func(x string) bool { return strings.HasPrefix(x, in.ActionPrefix) }) {
			continue
		}
		if a.AlarmRule != "" {
			out.CompositeAlarms = append(out.CompositeAlarms, a.Alarm)
		} else {
			out.MetricAlarms = append(out.MetricAlarms, a.Alarm)
		}
	}
	return out, nil
}

func (s *Service) describeAlarmsForMetric(c *core.Ctx, in *struct {
	MetricName        string
	Namespace         string
	Statistic         string
	ExtendedStatistic string
	Dimensions        []Dimension
	Period            int
	Unit              string
}) (any, error) {
	out := &struct{ MetricAlarms []Alarm }{[]Alarm{}}
	want := metricKey(c.Account, c.Region, metricID{in.Namespace, in.MetricName, in.Dimensions})
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Alarms) {
		a := s.st.Alarms[k]
		if a.MetricName == "" || a.Namespace != in.Namespace || a.MetricName != in.MetricName ||
			len(in.Dimensions) > 0 && metricKey(a.Account, a.Region, metricID{a.Namespace, a.MetricName, a.Dimensions}) != want ||
			a.Account != c.Account || a.Region != c.Region || in.Statistic != "" && a.Statistic != in.Statistic ||
			in.ExtendedStatistic != "" && a.ExtendedStatistic != in.ExtendedStatistic || in.Period != 0 && a.Period != in.Period ||
			in.Unit != "" && a.Unit != in.Unit {
			continue
		}
		out.MetricAlarms = append(out.MetricAlarms, a.Alarm)
	}
	return out, nil
}

func (s *Service) deleteAlarms(c *core.Ctx, in *struct{ AlarmNames []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range in.AlarmNames {
		if s.st.Alarms[scope(c)+n] == nil {
			return nil, noAlarm(n) // all or nothing, as in AWS
		}
	}
	for _, n := range in.AlarmNames {
		delete(s.st.Alarms, scope(c)+n)
	}
	return nil, nil
}

func (s *Service) enableActions(on bool) func(*core.Ctx, *struct{ AlarmNames []string }) (any, error) {
	return func(c *core.Ctx, in *struct{ AlarmNames []string }) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, n := range in.AlarmNames {
			if a := s.st.Alarms[scope(c)+n]; a != nil {
				a.ActionsEnabled = on
			}
		}
		return nil, nil
	}
}

func (s *Service) setAlarmState(c *core.Ctx, in *struct {
	AlarmName       string
	StateValue      string
	StateReason     string
	StateReasonData string
}) (any, error) {
	if !slices.Contains([]string{"OK", "ALARM", "INSUFFICIENT_DATA"}, in.StateValue) {
		return nil, invalid("The value %s for parameter StateValue is invalid.", in.StateValue)
	}
	if in.StateReasonData != "" && !json.Valid([]byte(in.StateReasonData)) {
		return nil, core.Errorf(400, "InvalidFormatFault", "Data was not syntactically valid JSON")
	}
	s.mu.Lock()
	a := s.st.Alarms[scope(c)+in.AlarmName]
	if a == nil {
		s.mu.Unlock()
		return nil, noAlarm(in.AlarmName)
	}
	out := s.transition(a, in.StateValue, in.StateReason, in.StateReasonData, time.Now().UTC())
	s.mu.Unlock()
	s.fire(out)
	return nil, nil
}

func (s *Service) describeAlarmHistory(c *core.Ctx, in *struct {
	AlarmName       string
	AlarmTypes      []string
	HistoryItemType string
	StartDate       *core.Time
	EndDate         *core.Time
	MaxRecords      int
	ScanBy          string
}) (any, error) {
	items := []HistoryItem{}
	s.mu.Lock()
	for _, a := range s.st.Alarms {
		if a.Account != c.Account || a.Region != c.Region || in.AlarmName != "" && a.AlarmName != in.AlarmName ||
			len(in.AlarmTypes) > 0 && !slices.Contains(in.AlarmTypes, a.kind()) {
			continue
		}
		for _, h := range a.History {
			t := time.Time(h.Timestamp)
			if in.HistoryItemType != "" && h.HistoryItemType != in.HistoryItemType ||
				in.StartDate != nil && t.Before(time.Time(*in.StartDate)) || in.EndDate != nil && t.After(time.Time(*in.EndDate)) {
				continue
			}
			items = append(items, h)
		}
	}
	s.mu.Unlock()
	sort.SliceStable(items, func(i, j int) bool {
		ti, tj := time.Time(items[i].Timestamp), time.Time(items[j].Timestamp)
		if in.ScanBy == "TimestampAscending" {
			return ti.Before(tj)
		}
		return ti.After(tj)
	})
	if in.MaxRecords > 0 && len(items) > in.MaxRecords {
		items = items[:in.MaxRecords]
	}
	return &struct{ AlarmHistoryItems []HistoryItem }{items}, nil
}

type delivery struct {
	arn string
	msg *core.Message
}

// transition sets an alarm's state and returns the actions to run when the
// state changed; lock held.
func (s *Service) transition(a *stored, state, reason, data string, now time.Time) []delivery {
	old := a.StateValue
	a.StateReason, a.StateReasonData, a.StateUpdatedTimestamp = reason, data, core.Time(now)
	if state == old {
		return nil
	}
	a.StateValue, a.StateTransitionedTimestamp = state, core.Time(now)
	a.record("StateUpdate", fmt.Sprintf("Alarm updated from %s to %s", old, state), map[string]any{"version": "1.0",
		"oldState": map[string]string{"stateValue": old}, "newState": map[string]string{"stateValue": state, "stateReason": reason}})
	if !a.ActionsEnabled {
		return nil
	}
	arns := a.InsufficientDataActions
	switch state {
	case "ALARM":
		arns = a.AlarmActions
	case "OK":
		arns = a.OKActions
	}
	var out []delivery
	for _, arn := range arns {
		out = append(out, delivery{arn, notification(&a.Alarm, old, arn, now)})
		a.record("Action", "Published a notification to "+arn, map[string]string{"actionState": "Succeeded", "notificationResource": arn})
	}
	return out
}

// notification builds what an alarm action receives: the alarm event for
// Lambda, the SNS alarm message (as JSON) for everything else.
func notification(a *Alarm, old, target string, now time.Time) *core.Message {
	_, region, account, _, _ := core.ParseARN(a.AlarmArn)
	ts := now.UTC().Format("2006-01-02T15:04:05.000-0700")
	if svc, _, _, _, _ := core.ParseARN(target); svc == "lambda" {
		b, _ := json.Marshal(map[string]any{"source": "aws.cloudwatch", "alarmArn": a.AlarmArn, "accountId": account, "time": ts, "region": region,
			"alarmData": map[string]any{"alarmName": a.AlarmName,
				"state":         map[string]any{"value": a.StateValue, "reason": a.StateReason, "reasonData": a.StateReasonData, "timestamp": ts},
				"previousState": map[string]any{"value": old},
				"configuration": map[string]any{"description": a.AlarmDescription, "metrics": a.Metrics}}})
		return &core.Message{Body: string(b)}
	}
	dims := []map[string]string{}
	for _, d := range a.Dimensions {
		dims = append(dims, map[string]string{"name": d.Name, "value": d.Value})
	}
	b, _ := json.Marshal(map[string]any{"AlarmName": a.AlarmName, "AlarmDescription": a.AlarmDescription, "AWSAccountId": account,
		"AlarmConfigurationUpdatedTimestamp": time.Time(a.AlarmConfigurationUpdatedTimestamp).UTC().Format("2006-01-02T15:04:05.000-0700"),
		"NewStateValue":                      a.StateValue, "NewStateReason": a.StateReason, "StateChangeTime": ts, "Region": region, "AlarmArn": a.AlarmArn,
		"OldStateValue": old, "OKActions": a.OKActions, "AlarmActions": a.AlarmActions, "InsufficientDataActions": a.InsufficientDataActions,
		"Trigger": map[string]any{"MetricName": a.MetricName, "Namespace": a.Namespace, "StatisticType": "Statistic",
			"Statistic": strings.ToUpper(a.Statistic), "Dimensions": dims, "Period": a.Period, "EvaluationPeriods": a.EvaluationPeriods,
			"DatapointsToAlarm": a.DatapointsToAlarm, "ComparisonOperator": a.ComparisonOperator, "Threshold": a.Threshold,
			"TreatMissingData": a.TreatMissingData}})
	subject := fmt.Sprintf("%s: %q in %s", a.StateValue, a.AlarmName, region)
	return &core.Message{Body: string(b), Subject: subject[:min(len(subject), 99)]} // SNS subjects stay under 100 characters
}

func (s *Service) fire(ds []delivery) {
	for _, d := range ds {
		if err := s.hub.Deliver(d.arn, d.msg); err != nil {
			s.hub.Log.Warn("cloudwatch alarm action failed", "target", d.arn, "err", err)
		}
	}
}

// Evaluate evaluates every metric alarm at now and runs the actions of the
// alarms that change state. The background loop calls it every 10 seconds.
func (s *Service) Evaluate(now time.Time) {
	var out []delivery
	changed := false
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.Alarms) {
		a := s.st.Alarms[k]
		if state, reason, data := s.evaluate(a, now); state != "" && state != a.StateValue {
			out, changed = append(out, s.transition(a, state, reason, data, now)...), true
		}
	}
	s.mu.Unlock()
	if changed {
		s.hub.MarkDirty() // background changes are not requests; persist them too
	}
	s.fire(out)
}

// evaluate computes an alarm's state over its last EvaluationPeriods periods;
// "" leaves the state alone. Lock held.
func (s *Service) evaluate(a *stored, now time.Time) (state, reason, data string) {
	if a.AlarmRule != "" || a.Threshold == nil || a.Period <= 0 && len(a.Metrics) == 0 {
		return "", "", "" // ponytail: composite and anomaly-band alarms are not evaluated
	}
	n := a.EvaluationPeriods
	qs, id, stat, period := a.Metrics, "", a.Statistic+a.ExtendedStatistic, a.Period
	if a.MetricName != "" {
		qs = []query{{Id: "m", MetricStat: &metricStat{metricID{a.Namespace, a.MetricName, a.Dimensions}, a.Period, stat, a.Unit}}}
	}
	for _, q := range qs {
		if q.MetricStat != nil && period == 0 { // a metric-math alarm has no Period of its own
			period = q.MetricStat.Period
		}
		if id == "" || q.ReturnData != nil && *q.ReturnData {
			id = q.Id
		}
	}
	series, _ := s.evalQueries(a.Account, a.Region, qs, windows(now, int64(n)))
	thr, recent := *a.Threshold, []float64{}
	breaching, unknown := 0, 0
	for k := range n {
		v, ok := series[id][int64(k)]
		if !ok {
			unknown++
			continue
		}
		recent = append(recent, v)
		if breaches(a.ComparisonOperator, v, thr) {
			breaching++
		}
	}
	switch a.TreatMissingData {
	case "breaching":
		breaching += unknown
	case "notBreaching":
	case "ignore":
		if unknown == n {
			return "", "", ""
		}
	default:
		if unknown == n {
			return "INSUFFICIENT_DATA", fmt.Sprintf("Insufficient Data: %d datapoints were unknown.", n), ""
		}
	}
	m := a.DatapointsToAlarm
	if m == 0 {
		m = n
	}
	state = "OK"
	if breaching >= m {
		state = "ALARM"
	}
	b, _ := json.Marshal(map[string]any{"version": "1.0", "queryDate": now.UTC().Format("2006-01-02T15:04:05.000-0700"),
		"statistic": stat, "period": period, "recentDatapoints": recent, "threshold": thr})
	return state, fmt.Sprintf("Threshold Crossed: %d out of the last %d datapoints %v breached the threshold (%s %g).",
		breaching, n, recent, a.ComparisonOperator, thr), string(b)
}

func breaches(op string, v, t float64) bool {
	switch op {
	case "GreaterThanThreshold":
		return v > t
	case "GreaterThanOrEqualToThreshold":
		return v >= t
	case "LessThanThreshold":
		return v < t
	case "LessThanOrEqualToThreshold":
		return v <= t
	}
	return false
}

// ---- dashboards ----

func dashboardARN(c *core.Ctx, name string) string {
	return c.GlobalARN("cloudwatch", "dashboard/"+name)
}

func noDashboard(name string) error {
	return core.Errorf(404, "DashboardNotFoundError", "Dashboard %s does not exist", name)
}

func (s *Service) putDashboard(c *core.Ctx, in *struct{ DashboardName, DashboardBody string }) (any, error) {
	if in.DashboardName == "" {
		return nil, missing("DashboardName")
	}
	var body map[string]any
	if json.Unmarshal([]byte(in.DashboardBody), &body) != nil {
		return nil, core.Errorf(400, "DashboardInvalidInputError", "The field DashboardBody must be a valid JSON object")
	}
	s.mu.Lock()
	s.st.Dashboards[c.Account+"/"+in.DashboardName] = &Dashboard{in.DashboardName, c.Account, in.DashboardBody, time.Now().UTC()}
	s.mu.Unlock()
	return map[string]any{"DashboardValidationMessages": []any{}}, nil
}

func (s *Service) getDashboard(c *core.Ctx, in *struct{ DashboardName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.st.Dashboards[c.Account+"/"+in.DashboardName]
	if d == nil {
		return nil, noDashboard(in.DashboardName)
	}
	return map[string]string{"DashboardArn": dashboardARN(c, d.Name), "DashboardBody": d.Body, "DashboardName": d.Name}, nil
}

type dashboardEntry struct {
	DashboardName string
	DashboardArn  string
	LastModified  core.Time
	Size          int
}

func (s *Service) listDashboards(c *core.Ctx, in *struct{ DashboardNamePrefix string }) (any, error) {
	out := &struct{ DashboardEntries []dashboardEntry }{[]dashboardEntry{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Dashboards) {
		if d := s.st.Dashboards[k]; d.Account == c.Account && strings.HasPrefix(d.Name, in.DashboardNamePrefix) {
			out.DashboardEntries = append(out.DashboardEntries, dashboardEntry{d.Name, dashboardARN(c, d.Name), core.Time(d.Modified), len(d.Body)})
		}
	}
	return out, nil
}

func (s *Service) deleteDashboards(c *core.Ctx, in *struct{ DashboardNames []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range in.DashboardNames {
		if s.st.Dashboards[c.Account+"/"+n] == nil {
			return nil, noDashboard(n)
		}
	}
	for _, n := range in.DashboardNames {
		delete(s.st.Dashboards, c.Account+"/"+n)
	}
	return nil, nil
}

// ---- tags ----

// alarmByARN finds a tagged resource (alarms are the only taggable kind here); lock held.
func (s *Service) alarmByARN(arn string) (*stored, error) {
	if _, region, account, res, ok := core.ParseARN(arn); ok && strings.HasPrefix(res, "alarm:") {
		if a := s.st.Alarms[account+"/"+region+"/"+strings.TrimPrefix(res, "alarm:")]; a != nil {
			return a, nil
		}
	}
	return nil, core.Errorf(404, "ResourceNotFoundException", "Resource %s not found", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceARN string
	Tags        []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.alarmByARN(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	if a.Tags == nil {
		a.Tags = map[string]string{}
	}
	maps.Copy(a.Tags, tagMap(in.Tags))
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceARN string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.alarmByARN(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(a.Tags, k)
	}
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *struct{ ResourceARN string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.alarmByARN(in.ResourceARN)
	if err != nil {
		return nil, err
	}
	out := &struct{ Tags []Tag }{[]Tag{}}
	for _, k := range core.SortedKeys(a.Tags) {
		out.Tags = append(out.Tags, Tag{k, a.Tags[k]})
	}
	return out, nil
}

// TaggedResources lists alarms (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, a := range s.st.Alarms {
		if a.Account == account && a.Region == region {
			out[a.AlarmArn] = maps.Clone(a.Tags)
		}
	}
	return out
}

// ---- console ----

// Resources lists metrics and alarms.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	mt := core.Table{Name: "Metrics", Columns: []string{"Namespace", "Name", "Dimensions", "Points"}}
	for _, k := range core.SortedKeys(s.st.Metrics) {
		m := s.st.Metrics[k]
		var dims []string
		for _, d := range m.Dimensions {
			dims = append(dims, d.Name+"="+d.Value)
		}
		mt.Rows = append(mt.Rows, []string{m.Namespace, m.Name, strings.Join(dims, ","), strconv.Itoa(len(m.Points))})
	}
	at := core.Table{Name: "Alarms", Columns: []string{"Name", "State", "Metric", "Threshold"}}
	for _, k := range core.SortedKeys(s.st.Alarms) {
		a := s.st.Alarms[k]
		metric, thr := a.Namespace+" "+a.MetricName, ""
		switch {
		case a.AlarmRule != "":
			metric = a.AlarmRule
		case a.MetricName == "":
			metric = "metric math"
		}
		if a.Threshold != nil {
			thr = a.ComparisonOperator + " " + strconv.FormatFloat(*a.Threshold, 'g', -1, 64)
		}
		at.Rows = append(at.Rows, []string{a.AlarmName, a.StateValue, metric, thr})
	}
	return []core.Table{mt, at}
}

// Inspect returns an alarm (by name) or the metrics of a namespace with their recent points.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Alarms":
		for _, k := range core.SortedKeys(s.st.Alarms) {
			if a := s.st.Alarms[k]; a.AlarmName == id {
				return map[string]any{"alarm": a.Alarm, "tags": maps.Clone(a.Tags), "history": slices.Clone(a.History)}
			}
		}
	case "Metrics":
		var out []map[string]any
		for _, k := range core.SortedKeys(s.st.Metrics) {
			if m := s.st.Metrics[k]; m.Namespace == id {
				out = append(out, map[string]any{"name": m.Name, "dimensions": m.Dimensions, "points": slices.Clone(m.Points[max(0, len(m.Points)-100):])})
			}
		}
		if out != nil {
			return out
		}
	}
	return nil
}
