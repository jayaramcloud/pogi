package generic

import (
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The cost services (ce, budgets, bcm-pricing-calculator) are global: one set
// of resources for every region and ARNs without a region.
func init() {
	for _, n := range []string{"ce", "budgets", "bcm-pricing-calculator"} {
		globalServices[n] = true
	}
	for k, v := range map[string]string{
		"ce/AnomalyMonitor":                       "anomalymonitor/{tail}",
		"ce/AnomalySubscription":                  "anomalysubscription/{tail}",
		"ce/CostCategoryDefinition":               "costcategory/{tail}",
		"budgets/Budget":                          "budget/{BudgetName}",
		"budgets/BudgetAction":                    "budget/{BudgetName}/action/{ActionId}",
		"bcm-data-exports/Export":                 "export/{tail}",
		"bcm-pricing-calculator/BillEstimate":     "bill-estimate/{identifier}",
		"bcm-pricing-calculator/BillScenario":     "bill-scenario/{identifier}",
		"bcm-pricing-calculator/WorkloadEstimate": "workload-estimate/{identifier}",
	} {
		arnTemplates[k] = v
	}
}

// hoist copies the members of the structure input[member] to the top of the
// input, so that the engine finds the identifiers a wrapper structure hides
// (PutReportDefinition keeps the report's name inside ReportDefinition).
func costHoist(c *Call, member string) {
	if m, ok := c.In[member].(map[string]any); ok {
		for k, v := range m {
			if _, has := c.In[k]; !has {
				c.In[k] = v
			}
		}
	}
}

// wrapped overrides a create or update call whose input is a wrapper structure: the engine
// runs on the hoisted members, then post, if any, adjusts the stored resource (c.Res) and the output.
func costWrapped(service, op, member string, post func(c *Call, r *Resource, out map[string]any)) {
	Override(service, op, func(c *Call) (map[string]any, error) {
		costHoist(c, member)
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		if post != nil && c.Res != nil {
			post(c, c.Res, out)
			c.Put(c.Res)
		}
		return out, nil
	})
}

// asCode gives a not-found error the code its API uses for the case (UnknownMonitorException).
func costAsCode(err error, code string) error {
	if err == nil {
		return nil
	}
	if e := core.AsError(err); strings.Contains(e.Code, "NotFound") {
		return core.Errorf(e.Status, code, "%s", e.Msg)
	}
	return err
}

// asCodeOf overrides op to run the engine and rename its not-found error.
func costAsCodeOf(service, code string, ops ...string) {
	for _, op := range ops {
		Override(service, op, func(c *Call) (map[string]any, error) {
			out, err := c.Default()
			return out, costAsCode(err, code)
		})
	}
}

func costIsoNow() string { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }

// amount is a {Amount, Unit} structure.
func costAmount(f float64, unit string) map[string]any {
	return map[string]any{"Amount": costMoney(f), "Unit": unit}
}

// apiErr is an error of the API's own, by code.
func costApiErr(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }
