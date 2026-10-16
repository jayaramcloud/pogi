package generic

import (
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

// Cost Explorer. Cost categories and anomaly monitors and subscriptions are
// the engine's CRUD; the cost and usage, forecast and dimension queries read
// the made-up bill of the cost model (costmodel.go).
func init() {
	// ---- cost categories
	catPost := func(c *Call, r *Resource, out map[string]any) {
		if str(r.Data["EffectiveStart"]) == "" {
			r.Data["EffectiveStart"] = costMonthStart(time.Now().UTC()).Format("2006-01-02T15:04:05Z")
		}
		r.Data["NumberOfRules"] = float64(len(asList(r.Data["Rules"])))
		r.Data["ProcessingStatus"] = []any{map[string]any{"Component": "COST_EXPLORER", "Status": "APPLIED"}}
		out["EffectiveStart"] = r.Data["EffectiveStart"]
	}
	costWrapped("ce", "CreateCostCategoryDefinition", "", catPost)
	costWrapped("ce", "UpdateCostCategoryDefinition", "", catPost)
	Override("ce", "DeleteCostCategoryDefinition", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err == nil {
			out["EffectiveEnd"] = costMonthStart(time.Now().UTC()).AddDate(0, 1, 0).Format("2006-01-02T15:04:05Z")
		}
		return out, err
	})
	Override("ce", "GetCostCategories", func(c *Call) (map[string]any, error) {
		var vals []string
		for _, r := range c.List("CostCategoryDefinition") {
			if name := str(c.In["CostCategoryName"]); name == "" {
				vals = append(vals, str(r.Data["Name"]))
			} else if str(r.Data["Name"]) == name {
				for _, rule := range asList(r.Data["Rules"]) {
					if m, _ := rule.(map[string]any); str(m["Value"]) != "" {
						vals = append(vals, str(m["Value"]))
					}
				}
			}
		}
		if s := strings.ToLower(str(c.In["SearchString"])); s != "" {
			vals = slices.DeleteFunc(vals, func(v string) bool { return !strings.Contains(strings.ToLower(v), s) })
		}
		slices.Sort(vals)
		vals = slices.Compact(vals)
		key := "CostCategoryNames"
		if str(c.In["CostCategoryName"]) != "" {
			key = "CostCategoryValues"
		}
		return map[string]any{key: costStrs(vals), "ReturnSize": float64(len(vals)), "TotalSize": float64(len(vals))}, nil
	})

	// ---- anomaly monitors and subscriptions
	stamp := func(c *Call, r *Resource, out map[string]any) {
		r.Data["CreationDate"], r.Data["LastUpdatedDate"] = costIsoNow(), costIsoNow()
		r.Data["DimensionalValueCount"] = 0.0
	}
	costWrapped("ce", "CreateAnomalyMonitor", "AnomalyMonitor", stamp)
	Override("ce", "UpdateAnomalyMonitor", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, costAsCode(err, "UnknownMonitorException")
		}
		c.Res.Data["LastUpdatedDate"] = costIsoNow()
		c.Put(c.Res)
		return out, nil
	})
	costAsCodeOf("ce", "UnknownMonitorException", "DeleteAnomalyMonitor")
	Override("ce", "GetAnomalyMonitors", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		return out, costKeepArns(out, "AnomalyMonitors", "MonitorArn", c.In["MonitorArnList"], "UnknownMonitorException")
	})
	Override("ce", "CreateAnomalySubscription", func(c *Call) (map[string]any, error) {
		costHoist(c, "AnomalySubscription")
		for _, a := range asList(c.In["MonitorArnList"]) { // a subscription watches monitors that exist
			if c.Get("AnomalyMonitor", str(a)) == nil {
				return nil, costApiErr("UnknownMonitorException", "Monitor %s does not exist", str(a))
			}
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		c.Res.Data["AccountId"] = c.Account
		c.Put(c.Res)
		return out, nil
	})
	costAsCodeOf("ce", "UnknownSubscriptionException", "UpdateAnomalySubscription", "DeleteAnomalySubscription")
	Override("ce", "GetAnomalySubscriptions", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		if err := costKeepArns(out, "AnomalySubscriptions", "SubscriptionArn", c.In["SubscriptionArnList"], "UnknownSubscriptionException"); err != nil {
			return nil, err
		}
		if m := str(c.In["MonitorArn"]); m != "" {
			subs := asList(out["AnomalySubscriptions"])
			out["AnomalySubscriptions"] = slices.DeleteFunc(subs, func(s any) bool {
				sm, _ := s.(map[string]any)
				return !slices.ContainsFunc(asList(sm["MonitorArnList"]), func(a any) bool { return str(a) == m })
			})
		}
		return out, nil
	})

	// ---- queries
	Override("ce", "GetCostAndUsage", ceCostAndUsage)
	Override("ce", "GetDimensionValues", ceDimensionValues)
	Override("ce", "GetCostForecast", ceCostForecast)
}

// ceMetrics are the metrics GetCostAndUsage takes; all but the quantities are the cost.
var ceMetrics = []string{"BlendedCost", "UnblendedCost", "AmortizedCost", "NetAmortizedCost", "NetUnblendedCost", "NormalizedUsageAmount", "UsageQuantity"}

func costIsQuantity(metric string) bool {
	return metric == "UsageQuantity" || metric == "NormalizedUsageAmount"
}

// ceRange reads and checks the TimePeriod of a call.
func ceRange(c *Call) (from, to time.Time, err error) {
	tp, _ := c.In["TimePeriod"].(map[string]any)
	from, ok1 := costParseTime(tp["Start"])
	to, ok2 := costParseTime(tp["End"])
	switch {
	case !ok1 || !ok2:
		return from, to, c.Invalid("TimePeriod Start and End must be dates (YYYY-MM-DD) or, for hourly data, date and hour")
	case !from.Before(to):
		return from, to, c.Invalid("Start date (and hour) should be before end date (and hour)")
	}
	return from, to, nil
}

func costFmtPeriod(t time.Time, gran string) string {
	if gran == "HOURLY" {
		return t.Format("2006-01-02T15:04:05Z")
	}
	return t.Format("2006-01-02")
}

// ceLines is the made-up bill of the calling account for a period, with the call's Filter applied.
func ceLines(c *Call, from, to time.Time) []*costLine {
	filter, _ := c.In["Filter"].(map[string]any)
	var out []*costLine
	for _, l := range costLines(c.Hub(), c.Account, from, to) {
		if costMatches(filter, l) {
			out = append(out, l)
		}
	}
	return out
}

func ceCostAndUsage(c *Call) (map[string]any, error) {
	from, to, err := ceRange(c)
	if err != nil {
		return nil, err
	}
	gran := str(c.In["Granularity"])
	var metrics []string
	for _, m := range asList(c.In["Metrics"]) {
		if !slices.Contains(ceMetrics, str(m)) {
			return nil, c.Invalid("Metric %s is not supported; use one of %s", str(m), strings.Join(ceMetrics, ", "))
		}
		metrics = append(metrics, str(m))
	}
	if len(metrics) == 0 {
		return nil, c.Invalid("Metrics must have at least one value")
	}
	groupBy := asList(c.In["GroupBy"])
	if len(groupBy) > 2 {
		return nil, c.Invalid("GroupBy takes at most two definitions")
	}
	lines := ceLines(c, from, to)
	values := func(ls []*costLine) map[string]any {
		out := map[string]any{}
		for _, m := range metrics {
			if costIsQuantity(m) {
				out[m] = costAmount(costSumCost(ls, true), "N/A")
			} else {
				out[m] = costAmount(costSumCost(ls, false), "USD")
			}
		}
		return out
	}
	now := time.Now().UTC()
	results := []any{}
	for ps := from; ps.Before(to); {
		pe := costMinTime(costNextPeriod(costPeriodStart(ps, gran), gran), to)
		var in []*costLine
		for _, l := range lines {
			if !l.At.Before(ps) && l.At.Before(pe) {
				in = append(in, l)
			}
		}
		r := map[string]any{"TimePeriod": map[string]any{"Start": costFmtPeriod(ps, gran), "End": costFmtPeriod(pe, gran)}, "Estimated": pe.After(now),
			"Total": map[string]any{}, "Groups": []any{}}
		if len(groupBy) == 0 {
			r["Total"] = values(in)
		} else {
			by := map[string][]*costLine{}
			keys := map[string][]any{}
			for _, l := range in {
				var ks []any
				for _, g := range groupBy {
					gm, _ := g.(map[string]any)
					switch str(gm["Type"]) {
					case "TAG":
						ks = append(ks, str(gm["Key"])+"$")
					case "DIMENSION":
						ks = append(ks, l.dim(str(gm["Key"])))
					default:
						ks = append(ks, "No "+str(gm["Key"]))
					}
				}
				k := strings.Join(costStrsOf(ks), "\x00")
				by[k], keys[k] = append(by[k], l), ks
			}
			groups := []any{}
			for _, k := range costSortedMapKeys(by) {
				groups = append(groups, map[string]any{"Keys": keys[k], "Metrics": values(by[k])})
			}
			r["Groups"] = groups
		}
		results = append(results, r)
		ps = pe
	}
	out := map[string]any{"ResultsByTime": results, "DimensionValueAttributes": []any{}}
	if len(groupBy) > 0 {
		out["GroupDefinitions"] = groupBy
	}
	return out, nil
}

func ceDimensionValues(c *Call) (map[string]any, error) {
	from, to, err := ceRange(c)
	if err != nil {
		return nil, err
	}
	dim := str(c.In["Dimension"])
	set := map[string]bool{}
	for _, l := range ceLines(c, from, to) {
		if v := l.dim(dim); v != "" && strings.Contains(strings.ToLower(v), strings.ToLower(str(c.In["SearchString"]))) {
			set[v] = true
		}
	}
	vals := costSortedKeys(set)
	if sb, _ := c.In["SortBy"].([]any); len(sb) > 0 {
		if m, _ := sb[0].(map[string]any); str(m["SortOrder"]) == "DESCENDING" {
			slices.Reverse(vals)
		}
	}
	var items []any
	for _, v := range vals {
		items = append(items, map[string]any{"Value": v, "Attributes": map[string]any{}})
	}
	page, next := costPageOf(items, c.In["NextPageToken"], c.In["MaxResults"], 1000)
	if page == nil {
		page = []any{}
	}
	return map[string]any{"DimensionValues": page, "ReturnSize": float64(len(page)), "TotalSize": float64(len(items)), "NextPageToken": next}, nil
}

// ceCostForecast fits a line (least squares) through the daily totals of the 30
// days up to now and extends it over the period asked for, a day at a time. The
// prediction interval is the spread of the fit's residuals, as wide as the
// interval level (80, the default, is 1.28 standard deviations, 95 is 1.96).
func ceCostForecast(c *Call) (map[string]any, error) {
	from, to, err := ceRange(c)
	if err != nil {
		return nil, err
	}
	today := costDay(time.Now().UTC())
	if from.Before(today) {
		return nil, c.Invalid("Start date must be today or later to forecast")
	}
	gran, metric := str(c.In["Granularity"]), str(c.In["Metric"])
	histFrom := today.AddDate(0, 0, -30)
	daily := map[time.Time]float64{}
	for _, l := range ceLines(c, histFrom, today.AddDate(0, 0, 1)) {
		if metric == "USAGE_QUANTITY" {
			daily[costDay(l.At)] += l.Qty
		} else {
			daily[costDay(l.At)] += l.Cost
		}
	}
	const n = 31
	var sx, sy, sxx, sxy float64
	for i := range n {
		x, y := float64(i), daily[histFrom.AddDate(0, 0, i)]
		sx, sy, sxx, sxy = sx+x, sy+y, sxx+x*x, sxy+x*y
	}
	slope := (n*sxy - sx*sy) / (n*sxx - sx*sx)
	icpt := (sy - slope*sx) / n
	var ss float64
	for i := range n {
		r := daily[histFrom.AddDate(0, 0, i)] - (icpt + slope*float64(i))
		ss += r * r
	}
	sd := math.Sqrt(ss / (n - 2))
	z := 1.28
	if lvl, ok := c.In["PredictionIntervalLevel"].(float64); ok {
		switch {
		case lvl <= 51:
			z = 0.67
		case lvl <= 80:
			z = 1.28
		case lvl <= 90:
			z = 1.64
		case lvl <= 95:
			z = 1.96
		default:
			z = 2.58
		}
	}
	var total float64
	results := []any{}
	for ps := from; ps.Before(to); {
		pe := costMinTime(costNextPeriod(costPeriodStart(ps, gran), gran), to)
		var mean float64
		days := 0
		for d := ps; d.Before(pe); d = d.AddDate(0, 0, 1) {
			mean += math.Max(0, icpt+slope*d.Sub(histFrom).Hours()/24)
			days++
		}
		if gran == "HOURLY" {
			days = 1
		}
		spread := z * sd * math.Sqrt(float64(max(days, 1)))
		total += mean
		results = append(results, map[string]any{"TimePeriod": map[string]any{"Start": costFmtPeriod(ps, gran), "End": costFmtPeriod(pe, gran)},
			"MeanValue": costMoney(mean), "PredictionIntervalLowerBound": costMoney(math.Max(0, mean-spread)), "PredictionIntervalUpperBound": costMoney(mean + spread)})
		ps = pe
	}
	unit := "USD"
	if metric == "USAGE_QUANTITY" {
		unit = "N/A"
	}
	return map[string]any{"Total": costAmount(total, unit), "ForecastResultsByTime": results}, nil
}

// keepArns leaves the items of out[member] whose key is in arns (all, when arns is empty) and fails with code for one that is not there.
func costKeepArns(out map[string]any, member, key string, arns any, code string) error {
	want := asList(arns)
	if len(want) == 0 {
		return nil
	}
	items := asList(out[member])
	have := map[string]bool{}
	for _, it := range items {
		m, _ := it.(map[string]any)
		have[str(m[key])] = true
	}
	for _, a := range want {
		if !have[str(a)] {
			return costApiErr(code, "%s does not exist", str(a))
		}
	}
	out[member] = slices.DeleteFunc(items, func(it any) bool {
		m, _ := it.(map[string]any)
		return !slices.ContainsFunc(want, func(a any) bool { return str(a) == str(m[key]) })
	})
	return nil
}

func costStrsOf(l []any) []string {
	out := make([]string, len(l))
	for i, v := range l {
		out[i] = str(v)
	}
	return out
}

func costSortedMapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
