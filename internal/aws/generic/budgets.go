package generic

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Budgets. A budget is one resource that keeps its budget structure and its
// notifications (each with its subscribers) in its data; actions are resources
// of their own. Spend is the cost model's (costmodel.go): the actual spend of a
// budget is what its filters select from the current period of its time unit,
// the forecast is that spend projected over the whole period.
//
// A ticker (and Service.Tick, for tests) checks every notification: when the
// spend crosses its threshold the notification goes to ALARM and each SNS
// subscriber gets a message through the hub; when the spend falls back it
// returns to OK. Email subscribers are not mailed (ponytail: SES is not in this
// path; deliver to an SNS topic instead).
func init() {
	Override("budgets", "CreateBudget", func(c *Call) (map[string]any, error) {
		b := costCmap(c.In["Budget"])
		if b["BudgetLimit"] == nil && b["PlannedBudgetLimits"] == nil && (str(b["BudgetType"]) == "COST" || str(b["BudgetType"]) == "USAGE") {
			return nil, c.Invalid("BudgetLimit or PlannedBudgetLimits is required for a %s budget", str(b["BudgetType"]))
		}
		costHoist(c, "Budget")
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		start := costPeriodStart(now, str(b["TimeUnit"]))
		if b["TimePeriod"] == nil {
			b["TimePeriod"] = map[string]any{"Start": start.Format(time.RFC3339), "End": "2087-06-15T00:00:00Z"}
		}
		if b["CostTypes"] == nil {
			b["CostTypes"] = map[string]any{"IncludeCredit": true, "IncludeDiscount": true, "IncludeOtherSubscription": true, "IncludeRecurring": true,
				"IncludeRefund": true, "IncludeSubscription": true, "IncludeSupport": true, "IncludeTax": true, "IncludeUpfront": true, "UseAmortized": false, "UseBlended": false}
		}
		b["LastUpdatedTime"] = now.Format(time.RFC3339)
		var notifs []any
		for _, n := range asList(c.In["NotificationsWithSubscribers"]) {
			nm := costCmap(n)
			notifs = append(notifs, map[string]any{"Notification": costWithNotificationDefaults(costCmap(nm["Notification"])), "Subscribers": nm["Subscribers"]})
		}
		r := c.Res
		r.Data = map[string]any{"Budget": b, "Notifications": notifs}
		c.Put(r)
		return out, nil
	})
	Override("budgets", "DescribeBudget", func(c *Call) (map[string]any, error) {
		r := c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"]))
		if r == nil {
			return nil, c.NotFound("%s", costNoBudget(c))
		}
		return map[string]any{"Budget": budgetView(c, r)}, nil
	})
	Override("budgets", "DescribeBudgets", func(c *Call) (map[string]any, error) {
		var items []any
		for _, r := range c.List("Budget") {
			if r.IDs["AccountId"] == str(c.In["AccountId"]) {
				items = append(items, budgetView(c, r))
			}
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Budgets": page, "NextToken": next}, nil
	})
	Override("budgets", "UpdateBudget", func(c *Call) (map[string]any, error) {
		nb := costCmap(c.In["NewBudget"])
		r := c.Get("Budget", str(c.In["AccountId"]), str(nb["BudgetName"]))
		if r == nil {
			return nil, c.NotFound("Unable to get budget: %s. Please make sure it exists.", str(nb["BudgetName"]))
		}
		old := costCmap(r.Data["Budget"])
		for _, k := range []string{"CostTypes", "TimePeriod"} { // what an update leaves out stays
			if nb[k] == nil {
				nb[k] = old[k]
			}
		}
		nb["LastUpdatedTime"] = time.Now().UTC().Format(time.RFC3339)
		r.Data["Budget"] = nb
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("budgets", "DeleteBudget", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		for _, a := range c.List("BudgetAction") { // its actions go with it
			if a.IDs["AccountId"] == str(c.In["AccountId"]) && a.IDs["BudgetName"] == str(c.In["BudgetName"]) {
				c.Svc.Delete(c.Account, c.Region, "BudgetAction", a.Key)
			}
		}
		return out, nil
	})

	// ---- notifications and subscribers
	Override("budgets", "CreateNotification", func(c *Call) (map[string]any, error) {
		return costEditNotifications(c, func(ns []any) ([]any, error) {
			n := costWithNotificationDefaults(costCmap(c.In["Notification"]))
			if costFindNotification(ns, n) >= 0 {
				return nil, c.Exists("The notification already exists")
			}
			return append(ns, map[string]any{"Notification": n, "Subscribers": c.In["Subscribers"]}), nil
		})
	})
	Override("budgets", "DeleteNotification", func(c *Call) (map[string]any, error) {
		return costEditNotifications(c, func(ns []any) ([]any, error) {
			i, err := costNotificationAt(c, ns, c.In["Notification"])
			if err != nil {
				return nil, err
			}
			return slices.Delete(ns, i, i+1), nil
		})
	})
	Override("budgets", "UpdateNotification", func(c *Call) (map[string]any, error) {
		return costEditNotifications(c, func(ns []any) ([]any, error) {
			i, err := costNotificationAt(c, ns, c.In["OldNotification"])
			if err != nil {
				return nil, err
			}
			nn := costWithNotificationDefaults(costCmap(c.In["NewNotification"]))
			if j := costFindNotification(ns, nn); j >= 0 && j != i {
				return nil, c.Exists("The notification already exists")
			}
			costCmap(ns[i])["Notification"] = nn
			return ns, nil
		})
	})
	Override("budgets", "DescribeNotificationsForBudget", func(c *Call) (map[string]any, error) {
		r := c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"]))
		if r == nil {
			return nil, c.NotFound("%s", costNoBudget(c))
		}
		var items []any
		for _, n := range asList(r.Data["Notifications"]) {
			items = append(items, costCmap(n)["Notification"])
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Notifications": page, "NextToken": next}, nil
	})
	Override("budgets", "DescribeBudgetNotificationsForAccount", func(c *Call) (map[string]any, error) {
		var items []any
		for _, r := range c.List("Budget") {
			if r.IDs["AccountId"] != str(c.In["AccountId"]) {
				continue
			}
			var ns []any
			for _, n := range asList(r.Data["Notifications"]) {
				ns = append(ns, costCmap(n)["Notification"])
			}
			items = append(items, map[string]any{"BudgetName": r.IDs["BudgetName"], "Notifications": ns})
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"BudgetNotificationsForAccount": page, "NextToken": next}, nil
	})
	Override("budgets", "CreateSubscriber", func(c *Call) (map[string]any, error) {
		return costEditNotifications(c, func(ns []any) ([]any, error) {
			i, err := costNotificationAt(c, ns, c.In["Notification"])
			if err != nil {
				return nil, err
			}
			n := costCmap(ns[i])
			subs := asList(n["Subscribers"])
			if costFindSubscriber(subs, costCmap(c.In["Subscriber"])) >= 0 {
				return nil, c.Exists("The subscriber already exists")
			}
			n["Subscribers"] = append(subs, c.In["Subscriber"])
			return ns, nil
		})
	})
	Override("budgets", "DeleteSubscriber", func(c *Call) (map[string]any, error) {
		return costEditNotifications(c, func(ns []any) ([]any, error) {
			i, err := costNotificationAt(c, ns, c.In["Notification"])
			if err != nil {
				return nil, err
			}
			n := costCmap(ns[i])
			subs := asList(n["Subscribers"])
			j := costFindSubscriber(subs, costCmap(c.In["Subscriber"]))
			switch {
			case j < 0:
				return nil, c.NotFound("The subscriber does not exist")
			case len(subs) == 1:
				return nil, c.Invalid("A notification must have at least one subscriber")
			}
			n["Subscribers"] = slices.Delete(subs, j, j+1)
			return ns, nil
		})
	})
	Override("budgets", "UpdateSubscriber", func(c *Call) (map[string]any, error) {
		return costEditNotifications(c, func(ns []any) ([]any, error) {
			i, err := costNotificationAt(c, ns, c.In["Notification"])
			if err != nil {
				return nil, err
			}
			n := costCmap(ns[i])
			subs := asList(n["Subscribers"])
			j := costFindSubscriber(subs, costCmap(c.In["OldSubscriber"]))
			if j < 0 {
				return nil, c.NotFound("The subscriber does not exist")
			}
			if k := costFindSubscriber(subs, costCmap(c.In["NewSubscriber"])); k >= 0 && k != j {
				return nil, c.Exists("The subscriber already exists")
			}
			subs[j] = c.In["NewSubscriber"]
			return ns, nil
		})
	})
	Override("budgets", "DescribeSubscribersForNotification", func(c *Call) (map[string]any, error) {
		r := c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"]))
		if r == nil {
			return nil, c.NotFound("%s", costNoBudget(c))
		}
		i, err := costNotificationAt(c, asList(r.Data["Notifications"]), c.In["Notification"])
		if err != nil {
			return nil, err
		}
		page, next := costPageOf(asList(costCmap(asList(r.Data["Notifications"])[i])["Subscribers"]), c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Subscribers": page, "NextToken": next}, nil
	})

	// ---- actions
	Override("budgets", "CreateBudgetAction", func(c *Call) (map[string]any, error) {
		if c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"])) == nil {
			return nil, c.NotFound("%s", costNoBudget(c))
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["Status"] = "STANDBY"
		r.Data["History"] = []any{costActionEvent("CREATE_ACTION", "STANDBY", "")}
		c.Put(r)
		return out, nil
	})
	Override("budgets", "DescribeBudgetAction", func(c *Call) (map[string]any, error) {
		r := c.Get("BudgetAction", str(c.In["AccountId"]), str(c.In["BudgetName"]), str(c.In["ActionId"]))
		if r == nil {
			return nil, c.NotFound("The action %s does not exist", str(c.In["ActionId"]))
		}
		return map[string]any{"AccountId": r.IDs["AccountId"], "BudgetName": r.IDs["BudgetName"], "Action": costActionView(r)}, nil
	})
	Override("budgets", "UpdateBudgetAction", func(c *Call) (map[string]any, error) {
		r := c.Get("BudgetAction", str(c.In["AccountId"]), str(c.In["BudgetName"]), str(c.In["ActionId"]))
		if r == nil {
			return nil, c.NotFound("The action %s does not exist", str(c.In["ActionId"]))
		}
		old := costActionView(r)
		for _, k := range []string{"ActionThreshold", "ApprovalModel", "Definition", "ExecutionRoleArn", "NotificationType", "Subscribers"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		r.Data["History"] = append(asList(r.Data["History"]), costActionEvent("UPDATE_ACTION", str(r.Data["Status"]), ""))
		c.Put(r)
		return map[string]any{"AccountId": r.IDs["AccountId"], "BudgetName": r.IDs["BudgetName"], "OldAction": old, "NewAction": costActionView(r)}, nil
	})
	Override("budgets", "DeleteBudgetAction", func(c *Call) (map[string]any, error) {
		r := c.Get("BudgetAction", str(c.In["AccountId"]), str(c.In["BudgetName"]), str(c.In["ActionId"]))
		if r == nil {
			return nil, c.NotFound("The action %s does not exist", str(c.In["ActionId"]))
		}
		c.Svc.Delete(c.Account, c.Region, "BudgetAction", r.Key)
		return map[string]any{"AccountId": r.IDs["AccountId"], "BudgetName": r.IDs["BudgetName"], "Action": costActionView(r)}, nil
	})
	actions := func(c *Call) map[string]any {
		var items []any
		for _, r := range c.List("BudgetAction") {
			if r.IDs["AccountId"] == str(c.In["AccountId"]) && (str(c.In["BudgetName"]) == "" || r.IDs["BudgetName"] == str(c.In["BudgetName"])) {
				items = append(items, costActionView(r))
			}
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Actions": page, "NextToken": next}
	}
	Override("budgets", "DescribeBudgetActionsForAccount", func(c *Call) (map[string]any, error) { return actions(c), nil })
	Override("budgets", "DescribeBudgetActionsForBudget", func(c *Call) (map[string]any, error) {
		if c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"])) == nil {
			return nil, c.NotFound("%s", costNoBudget(c))
		}
		return actions(c), nil
	})
	// Executing an action records the result (pogi applies no IAM policy, SCP or SSM document).
	Override("budgets", "ExecuteBudgetAction", func(c *Call) (map[string]any, error) {
		r := c.Get("BudgetAction", str(c.In["AccountId"]), str(c.In["BudgetName"]), str(c.In["ActionId"]))
		if r == nil {
			return nil, c.NotFound("The action %s does not exist", str(c.In["ActionId"]))
		}
		status, event := "EXECUTION_SUCCESS", "EXECUTE_ACTION"
		switch str(c.In["ExecutionType"]) {
		case "REVERSE_BUDGET_ACTION":
			status = "REVERSE_SUCCESS"
		case "RESET_BUDGET_ACTION":
			status = "STANDBY"
		}
		r.Data["Status"] = status
		r.Data["History"] = append(asList(r.Data["History"]), costActionEvent(event, status, str(c.In["ExecutionType"])))
		c.Put(r)
		return map[string]any{"AccountId": r.IDs["AccountId"], "BudgetName": r.IDs["BudgetName"], "ActionId": r.IDs["ActionId"], "ExecutionType": c.In["ExecutionType"]}, nil
	})
	Override("budgets", "DescribeBudgetActionHistories", func(c *Call) (map[string]any, error) {
		r := c.Get("BudgetAction", str(c.In["AccountId"]), str(c.In["BudgetName"]), str(c.In["ActionId"]))
		if r == nil {
			return nil, c.NotFound("The action %s does not exist", str(c.In["ActionId"]))
		}
		var items []any
		for _, h := range asList(r.Data["History"]) {
			hm := costCmap(h)
			hm["ActionHistoryDetails"] = map[string]any{"Message": str(hm["Message"]), "Action": costActionView(r)}
			delete(hm, "Message")
			items = append(items, hm)
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"ActionHistories": page, "NextToken": next}, nil
	})

	// ---- performance history: the budget against the cost model, period by period
	Override("budgets", "DescribeBudgetPerformanceHistory", func(c *Call) (map[string]any, error) {
		r := c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"]))
		if r == nil {
			return nil, c.NotFound("%s", costNoBudget(c))
		}
		b := costCmap(r.Data["Budget"])
		unit, now := str(b["TimeUnit"]), time.Now().UTC()
		first := costPeriodStart(now, unit)
		for range map[string]int{"DAILY": 13, "MONTHLY": 11, "QUARTERLY": 3, "ANNUALLY": 1}[unit] {
			first = costPrevPeriod(first, unit)
		}
		last := now
		if tp := costCmap(c.In["TimePeriod"]); tp != nil {
			if t, ok := costParseTime(tp["Start"]); ok {
				first = costPeriodStart(t, unit)
			}
			if t, ok := costParseTime(tp["End"]); ok {
				last = t
			}
		}
		lines := budgetLines(c.Hub(), c.Account, b, first, last)
		limit := budgetLimit(b)
		var rows []any
		for ps := first; !ps.After(last) && unit != "CUSTOM"; ps = costNextPeriod(ps, unit) {
			pe := costNextPeriod(ps, unit)
			var in []*costLine
			for _, l := range lines {
				if !l.At.Before(ps) && l.At.Before(pe) {
					in = append(in, l)
				}
			}
			rows = append(rows, map[string]any{"TimePeriod": map[string]any{"Start": ps.Format(time.RFC3339), "End": pe.Format(time.RFC3339)},
				"BudgetedAmount": costAmount(limit, budgetUnit(b)), "ActualAmount": costAmount(costSumCost(in, str(b["BudgetType"]) == "USAGE"), budgetUnit(b))})
		}
		page, next := costPageOf(rows, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"BudgetPerformanceHistory": map[string]any{"BudgetName": b["BudgetName"], "BudgetType": b["BudgetType"], "TimeUnit": b["TimeUnit"],
			"CostFilters": b["CostFilters"], "CostTypes": b["CostTypes"], "BudgetedAndActualAmountsList": page}, "NextToken": next}, nil
	})

	starters["budgets"] = append(starters["budgets"], func(s *Service) { s.Every("budgets", time.Minute, s.costCheckBudgets) })
}

// ---- helpers

func costCmap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func costNoBudget(c *Call) string {
	return fmt.Sprintf("Unable to get budget: %s. Please make sure it exists.", str(c.In["BudgetName"]))
}

func costWithNotificationDefaults(n map[string]any) map[string]any {
	if n["ThresholdType"] == nil {
		n["ThresholdType"] = "PERCENTAGE"
	}
	if n["NotificationState"] == nil {
		n["NotificationState"] = "OK"
	}
	return n
}

// findNotification is the index of the notification in ns that n names: same type, operator, threshold and threshold type.
func costFindNotification(ns []any, n map[string]any) int {
	n = costWithNotificationDefaults(n)
	return slices.IndexFunc(ns, func(x any) bool {
		e := costCmap(costCmap(x)["Notification"])
		return str(e["NotificationType"]) == str(n["NotificationType"]) && str(e["ComparisonOperator"]) == str(n["ComparisonOperator"]) &&
			str(e["Threshold"]) == str(n["Threshold"]) && str(e["ThresholdType"]) == str(n["ThresholdType"])
	})
}

func costNotificationAt(c *Call, ns []any, n any) (int, error) {
	i := costFindNotification(ns, costCmap(n))
	if i < 0 {
		return 0, c.NotFound("The notification does not exist")
	}
	return i, nil
}

func costFindSubscriber(subs []any, s map[string]any) int {
	return slices.IndexFunc(subs, func(x any) bool {
		e := costCmap(x)
		return str(e["Address"]) == str(s["Address"]) && str(e["SubscriptionType"]) == str(s["SubscriptionType"])
	})
}

// editNotifications runs change on the notifications of the budget the call names and stores the result.
func costEditNotifications(c *Call, change func(ns []any) ([]any, error)) (map[string]any, error) {
	r := c.Get("Budget", str(c.In["AccountId"]), str(c.In["BudgetName"]))
	if r == nil {
		return nil, c.NotFound("%s", costNoBudget(c))
	}
	ns, err := change(asList(r.Data["Notifications"]))
	if err != nil {
		return nil, err
	}
	r.Data["Notifications"] = ns
	c.Put(r)
	return map[string]any{}, nil
}

func costActionEvent(event, status, msg string) map[string]any {
	return map[string]any{"Timestamp": time.Now().UTC().Format(time.RFC3339), "Status": status, "EventType": event, "Message": msg}
}

// actionView is the Action structure of an action resource.
func costActionView(r *Resource) map[string]any {
	v := map[string]any{"ActionId": r.IDs["ActionId"], "BudgetName": r.IDs["BudgetName"], "Status": r.Data["Status"]}
	for _, k := range []string{"ActionThreshold", "ActionType", "ApprovalModel", "Definition", "ExecutionRoleArn", "NotificationType", "Subscribers"} {
		v[k] = r.Data[k]
	}
	return v
}

// ---- spend

func costPrevPeriod(t time.Time, unit string) time.Time {
	switch unit {
	case "DAILY":
		return t.AddDate(0, 0, -1)
	case "QUARTERLY":
		return t.AddDate(0, -3, 0)
	case "ANNUALLY":
		return t.AddDate(-1, 0, 0)
	}
	return t.AddDate(0, -1, 0)
}

func budgetLimit(b map[string]any) float64 {
	f, _ := strconv.ParseFloat(str(costCmap(b["BudgetLimit"])["Amount"]), 64)
	return f
}

func budgetUnit(b map[string]any) string {
	if u := str(costCmap(b["BudgetLimit"])["Unit"]); u != "" {
		return u
	}
	return "USD"
}

// budgetLines is the bill of the account for [from, to) that a budget's filters select.
func budgetLines(h *core.Hub, account string, b map[string]any, from, to time.Time) []*costLine {
	var out []*costLine
	filter, _ := b["FilterExpression"].(map[string]any)
	cf := costCmap(b["CostFilters"])
	for _, l := range costLines(h, account, from, to) {
		ok := costMatches(filter, l)
		for k, vs := range cf {
			if d, known := budgetFilterKeys[k]; known && len(asList(vs)) > 0 {
				ok = ok && slices.ContainsFunc(asList(vs), func(v any) bool { return str(v) == l.dim(d) })
			}
		}
		if ok {
			out = append(out, l)
		}
	}
	return out
}

// budgetSpend is the actual and forecast spend of a budget in the current period of its time unit.
func budgetSpend(h *core.Hub, account string, b map[string]any, now time.Time) (actual, forecast float64) {
	unit := str(b["TimeUnit"])
	var start, end time.Time
	if tp := costCmap(b["TimePeriod"]); unit == "CUSTOM" {
		start, _ = costParseTime(tp["Start"])
		end, _ = costParseTime(tp["End"])
	} else {
		start = costPeriodStart(now, unit)
		end = costNextPeriod(start, unit)
	}
	if t := str(b["BudgetType"]); t != "COST" && t != "USAGE" {
		return 0, 0 // utilisation and coverage budgets have no data here
	}
	actual = costSumCost(budgetLines(h, account, b, start, costMinTime(now.Add(time.Hour), end)), str(b["BudgetType"]) == "USAGE")
	forecast = actual
	if el := now.Sub(start); el > 0 && end.After(now) {
		forecast = actual * float64(end.Sub(start)) / float64(el)
	}
	return
}

// budgetView is the Budget structure of a budget resource, with its calculated spend.
func budgetView(c *Call, r *Resource) map[string]any {
	b := map[string]any{}
	for k, v := range costCmap(r.Data["Budget"]) {
		b[k] = v
	}
	actual, forecast := budgetSpend(c.Hub(), c.Account, b, time.Now().UTC())
	u := budgetUnit(b)
	b["CalculatedSpend"] = map[string]any{"ActualSpend": costAmount(actual, u), "ForecastedSpend": costAmount(forecast, u)}
	return b
}

// checkBudgets moves the notifications of every budget to ALARM or OK by its spend as of now, and tells the SNS subscribers of the ones that just crossed.
func (s *Service) costCheckBudgets(now time.Time) {
	type send struct{ to, subject, body string }
	var sends []send
	s.mu.Lock()
	for _, r := range s.st.Res {
		if r.Type != "Budget" {
			continue
		}
		b := costCmap(r.Data["Budget"])
		actual, forecast := budgetSpend(s.h, r.Account, b, now)
		limit, u := budgetLimit(b), budgetUnit(b)
		for _, x := range asList(r.Data["Notifications"]) {
			nm := costCmap(x)
			n := costCmap(nm["Notification"])
			thr, _ := strconv.ParseFloat(str(n["Threshold"]), 64)
			if str(n["ThresholdType"]) != "ABSOLUTE_VALUE" {
				thr = limit * thr / 100
			}
			spend, kind := actual, "actual"
			if str(n["NotificationType"]) == "FORECASTED" {
				spend, kind = forecast, "forecasted"
			}
			breach := false
			switch str(n["ComparisonOperator"]) {
			case "GREATER_THAN":
				breach = spend > thr
			case "LESS_THAN":
				breach = spend < thr
			default:
				breach = spend == thr
			}
			switch {
			case breach && str(n["NotificationState"]) != "ALARM":
				n["NotificationState"] = "ALARM"
				for _, sub := range asList(nm["Subscribers"]) {
					if sm := costCmap(sub); str(sm["SubscriptionType"]) == "SNS" {
						sends = append(sends, send{str(sm["Address"]), "AWS Budgets: " + r.IDs["BudgetName"] + " has exceeded your alert threshold",
							fmt.Sprintf("AWS Budget Notification %s\nAWS Account %s\n\nYou created a budget named %q. The budget limit is %s %s. The %s spend is %s %s, which is above the alert threshold of %s %s.",
								now.Format("January 02, 2006"), r.Account, r.IDs["BudgetName"], costMoney(limit), u, kind, costMoney(spend), u, costMoney(thr), u)})
					}
				}
			case !breach && str(n["NotificationState"]) == "ALARM":
				n["NotificationState"] = "OK"
			}
		}
	}
	s.mu.Unlock()
	for _, m := range sends {
		if err := s.h.Deliver(m.to, &core.Message{Subject: m.subject, Body: m.body}); err != nil {
			s.h.Log.Warn("budgets: notification not delivered", "to", m.to, "err", err)
		}
	}
}
