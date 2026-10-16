package autoscaling

// What Auto Scaling shares with Application Auto Scaling: the policy shapes,
// step and percent arithmetic, target tracking's alarms, and reading the
// alarm action that runs a policy.

import (
	"encoding/json"
	"math"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

const cloudwatch = "GraniteServiceVersion20100801" // CloudWatch's in-process target

// Dimension is a CloudWatch metric dimension.
type Dimension struct{ Name, Value string }

// Predefined is a target tracking policy's predefined metric.
type Predefined struct {
	PredefinedMetricType string
	ResourceLabel        string `json:",omitempty"`
}

// Customized is a target tracking policy's own CloudWatch metric.
// ponytail: metric math (Metrics) is not kept.
type Customized struct {
	MetricName, Namespace string
	Dimensions            []Dimension `json:",omitempty"`
	Statistic             string      `json:",omitempty"`
	Unit                  string      `json:",omitempty"`
}

// Tracking is a target tracking configuration of either API; Application
// Auto Scaling adds the cooldowns.
type Tracking struct {
	PredefinedMetricSpecification *Predefined `json:",omitempty"`
	CustomizedMetricSpecification *Customized `json:",omitempty"`
	TargetValue                   float64
	DisableScaleIn                bool
	ScaleOutCooldown              *int `json:",omitempty"`
	ScaleInCooldown               *int `json:",omitempty"`
}

// StepAdjustment is one step of a step scaling policy.
type StepAdjustment struct {
	MetricIntervalLowerBound *float64 `json:",omitempty"`
	MetricIntervalUpperBound *float64 `json:",omitempty"`
	ScalingAdjustment        int
}

// Alarm is an alarm a target tracking policy made.
type Alarm struct{ AlarmName, AlarmARN string }

// Metric is the CloudWatch metric target tracking alarms watch.
type Metric struct {
	Namespace, MetricName, Statistic string
	Dimensions                       []Dimension
}

// RequestsPerTarget is ALBRequestCountPerTarget's metric for a resource
// label (app/<lb>/<id>/targetgroup/<tg>/<id>).
func RequestsPerTarget(label string) Metric {
	_, tg, _ := strings.Cut(label, "/targetgroup/")
	return Metric{"AWS/ApplicationELB", "RequestCountPerTarget", "Sum", []Dimension{{"TargetGroup", "targetgroup/" + tg}}}
}

// Adjust applies a ChangeInCapacity, ExactCapacity or
// PercentChangeInCapacity adjustment, rounding percentages as AWS does.
func Adjust(capacity int, kind string, adj, minMagnitude int) int {
	switch kind {
	case "ExactCapacity":
		return adj
	case "PercentChangeInCapacity":
		d := float64(capacity) * float64(adj) / 100
		n := int(d) // toward zero: above 1 rounds down, below -1 up
		if n == 0 && d != 0 {
			n = int(math.Copysign(1, d))
		}
		if n > 0 && n < minMagnitude {
			n = minMagnitude
		} else if n < 0 && -n < minMagnitude {
			n = -minMagnitude
		}
		return capacity + n
	}
	return capacity + adj
}

// Step finds the adjustment whose interval holds breach (the metric minus
// the alarm threshold); a bound is inclusive on the threshold's side.
func Step(steps []StepAdjustment, breach float64) (int, bool) {
	for _, st := range steps {
		lo, hi := math.Inf(-1), math.Inf(1)
		if st.MetricIntervalLowerBound != nil {
			lo = *st.MetricIntervalLowerBound
		}
		if st.MetricIntervalUpperBound != nil {
			hi = *st.MetricIntervalUpperBound
		}
		if breach >= 0 && lo <= breach && breach < hi || breach < 0 && lo < breach && breach <= hi {
			return st.ScalingAdjustment, true
		}
	}
	return 0, false
}

// Track is target tracking's new capacity: proportional to ratio (metric
// over target) when known, else one more for the high alarm, one fewer for
// the low one.
func Track(capacity int, ratio float64, high, noScaleIn bool) int {
	switch {
	case !high && noScaleIn:
		return capacity
	case ratio > 0 && high:
		return max(int(math.Ceil(float64(capacity)*ratio)), capacity+1)
	case ratio > 0:
		return min(int(math.Ceil(float64(capacity)*ratio)), capacity)
	case high:
		return capacity + 1
	}
	return capacity - 1
}

// Signal is what a CloudWatch alarm action tells a policy.
type Signal struct {
	AlarmName, NewStateValue string
	High                     bool    // the alarm fires on values above its threshold
	Value, Threshold         float64 // Value only when Known
	Known                    bool
}

// ReadSignal reads an alarm notification. The metric value is the average
// of the datapoints the alarm last evaluated; SetAlarmState leaves none.
func ReadSignal(hub *core.Hub, account, region string, m *core.Message) Signal {
	var n struct {
		AlarmName, NewStateValue string
		Trigger                  struct {
			ComparisonOperator string
			Threshold          float64
		}
	}
	json.Unmarshal([]byte(m.Body), &n)
	a := Signal{AlarmName: n.AlarmName, NewStateValue: n.NewStateValue, High: strings.HasPrefix(n.Trigger.ComparisonOperator, "Greater"),
		Threshold: n.Trigger.Threshold}
	var out struct {
		MetricAlarms []struct{ StateReasonData string }
	}
	if hub.Call(account, region, cloudwatch, "DescribeAlarms", map[string]any{"AlarmNames": []string{n.AlarmName}}, &out) == nil && len(out.MetricAlarms) > 0 {
		var d struct{ RecentDatapoints []float64 }
		json.Unmarshal([]byte(out.MetricAlarms[0].StateReasonData), &d)
		for _, v := range d.RecentDatapoints {
			a.Value += v / float64(len(d.RecentDatapoints))
		}
		a.Known = len(d.RecentDatapoints) > 0
	}
	return a
}

// Breach is how far the metric is past the threshold; unknown counts as
// just past it, in the alarm's direction.
func (a Signal) Breach() float64 {
	switch {
	case a.Known:
		return a.Value - a.Threshold
	case a.High:
		return 1e-9
	}
	return -1e-9
}

// Ratio is the metric over a tracking target, 0 when unknown.
func (a Signal) Ratio(target float64) float64 {
	if !a.Known || target == 0 {
		return 0
	}
	return a.Value / target
}

// TrackingAlarms creates target tracking's alarms on a metric, as AWS
// names them: high (3 minutes above the target) scales out, low (15 minutes
// under 90% of it) scales in. Without CloudWatch there are none.
func TrackingAlarms(hub *core.Hub, account, region, prefix, policyARN string, m Metric, target float64, scaleIn bool) []Alarm {
	var out []Alarm
	for _, a := range []struct {
		name, op string
		periods  int
		limit    float64
	}{{"AlarmHigh", "GreaterThanThreshold", 3, target}, {"AlarmLow", "LessThanThreshold", 15, target * 0.9}} {
		if a.name == "AlarmLow" && !scaleIn {
			continue
		}
		name := "TargetTracking-" + prefix + "-" + a.name + "-" + core.UUID()
		in := map[string]any{"AlarmName": name, "AlarmActions": []string{policyARN}, "Namespace": m.Namespace, "MetricName": m.MetricName,
			"Dimensions": m.Dimensions, "Statistic": m.Statistic, "Period": 60, "EvaluationPeriods": a.periods, "Threshold": a.limit,
			"ComparisonOperator": a.op, "AlarmDescription": "DO NOT EDIT OR DELETE. For TargetTrackingScaling policy " + policyARN + "."}
		if hub.Call(account, region, cloudwatch, "PutMetricAlarm", in, nil) == nil {
			out = append(out, Alarm{name, core.ARN("cloudwatch", region, account, "alarm:"+name)})
		}
	}
	return out
}

// DeleteAlarms deletes the alarms a policy made.
func DeleteAlarms(hub *core.Hub, account, region string, alarms []Alarm) {
	var names []string
	for _, a := range alarms {
		names = append(names, a.AlarmName)
	}
	if len(names) > 0 {
		hub.Call(account, region, cloudwatch, "DeleteAlarms", map[string]any{"AlarmNames": names}, nil)
	}
}
