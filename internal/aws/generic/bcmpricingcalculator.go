package generic

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/pricelist"
	"github.com/nrndev/pogi/internal/core"
)

// BCM Pricing Calculator. Workload estimates, bill scenarios and bill
// estimates are the engine's CRUD; their usage lines and modifications are
// batches of items kept in the parent's data, priced from the embedded price
// list (internal/aws/pricelist): a usage line costs its amount times the unit
// price of its usage type, in the region its usage type names. A bill estimate
// prices the usage modifications of its bill scenario; its historical cost is 0
// (ponytail: no baseline, the estimate is the cost of the modifications alone).
func init() {
	const svc = "bcm-pricing-calculator"
	creat := func(c *Call, r *Resource, status string) {
		t := r.Created
		r.Data["status"] = status
		r.Data["expiresAt"] = t.AddDate(3, 0, 0).Format(time.RFC3339)
		c.Put(r)
	}
	// AWS mints a UUID as the id of an estimate or scenario; the engine would take the name for it.
	mint := func(c *Call) { c.In["identifier"] = core.UUID() }
	// ---- workload estimates
	Override(svc, "CreateWorkloadEstimate", func(c *Call) (map[string]any, error) {
		mint(c)
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["totalCost"], r.Data["costCurrency"], r.Data["rateTimestamp"] = 0.0, "USD", r.Created.Format(time.RFC3339)
		if r.Data["rateType"] == nil {
			r.Data["rateType"] = "BEFORE_DISCOUNTS"
		}
		creat(c, r, "VALID")
		return c.Fill(r), nil
	})
	calcBatch(calcKind{svc: svc, parent: "WorkloadEstimate", parentIn: "workloadEstimateId", store: "usage",
		create: "BatchCreateWorkloadEstimateUsage", update: "BatchUpdateWorkloadEstimateUsage", del: "BatchDeleteWorkloadEstimateUsage", list: "ListWorkloadEstimateUsage",
		createIn: "usage", updateIn: "usage", delIn: "ids", priced: true,
		build: func(e map[string]any, p pricelist.Product) map[string]any {
			return map[string]any{"quantity": map[string]any{"amount": e["amount"], "unit": p.Unit}, "currency": "USD", "status": "VALID",
				"cost": costToFloat(e["amount"]) * p.Price, "location": p.Attrs["location"]}
		},
		change: func(it, e map[string]any) {
			if e["amount"] != nil {
				q := costCmap(it["quantity"])
				q["amount"] = e["amount"]
				it["cost"] = costToFloat(e["amount"]) * costUnitPrice(it)
			}
		},
		after: func(r *Resource) {
			var total float64
			for _, it := range asList(r.Data["usage"]) {
				total += costToFloat(costCmap(it)["cost"])
			}
			r.Data["totalCost"], r.Data["status"] = total, "VALID"
		}})

	// ---- bill scenarios
	Override(svc, "CreateBillScenario", func(c *Call) (map[string]any, error) {
		mint(c)
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		m := costMonthStart(time.Now().UTC())
		r.Data["billInterval"] = map[string]any{"start": m.Format(time.RFC3339), "end": m.AddDate(0, 1, 0).Format(time.RFC3339)}
		creat(c, r, "READY")
		return c.Fill(r), nil
	})
	calcBatch(calcKind{svc: svc, parent: "BillScenario", parentIn: "billScenarioId", store: "usageMods",
		create: "BatchCreateBillScenarioUsageModification", update: "BatchUpdateBillScenarioUsageModification", del: "BatchDeleteBillScenarioUsageModification", list: "ListBillScenarioUsageModifications",
		createIn: "usageModifications", updateIn: "usageModifications", delIn: "ids", priced: true,
		build: func(e map[string]any, p pricelist.Product) map[string]any {
			return map[string]any{"quantities": costQuantities(e["amounts"], p.Unit), "location": p.Attrs["location"], "unit": p.Unit}
		},
		change: func(it, e map[string]any) {
			if e["amounts"] != nil {
				it["quantities"] = costQuantities(e["amounts"], str(it["unit"]))
			}
		}})
	calcBatch(calcKind{svc: svc, parent: "BillScenario", parentIn: "billScenarioId", store: "commitMods",
		create: "BatchCreateBillScenarioCommitmentModification", update: "BatchUpdateBillScenarioCommitmentModification", del: "BatchDeleteBillScenarioCommitmentModification", list: "ListBillScenarioCommitmentModifications",
		createIn: "commitmentModifications", updateIn: "commitmentModifications", delIn: "ids"})

	// ---- bill estimates: the scenario's usage modifications, priced
	Override(svc, "CreateBillEstimate", func(c *Call) (map[string]any, error) {
		sc := c.Get("BillScenario", str(c.In["billScenarioId"]))
		if sc == nil {
			return nil, c.NotFound("Bill scenario %s does not exist", str(c.In["billScenarioId"]))
		}
		mint(c)
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		var lines []any
		var total float64
		byService := map[string]float64{}
		for _, m := range asList(sc.Data["usageMods"]) {
			it := costCmap(m)
			p, ok := calcPrice(str(it["serviceCode"]), str(it["usageType"]), str(it["operation"]))
			if !ok {
				continue
			}
			var qty float64
			for _, q := range asList(it["quantities"]) {
				qty += costToFloat(costCmap(q)["amount"])
			}
			cost := qty * p.Price
			total += cost
			byService[str(it["serviceCode"])] += cost
			lines = append(lines, map[string]any{"id": core.UUID(), "lineItemId": core.UUID(), "lineItemType": "Usage", "serviceCode": it["serviceCode"], "usageType": it["usageType"],
				"operation": it["operation"], "location": it["location"], "usageAccountId": it["usageAccountId"], "payerAccountId": c.Account,
				"estimatedCost": map[string]any{"amount": cost, "currency": "USD"}, "historicalCost": map[string]any{"amount": 0.0, "currency": "USD"},
				"estimatedUsageQuantity": map[string]any{"amount": qty, "unit": p.Unit}, "historicalUsageQuantity": map[string]any{"amount": 0.0, "unit": p.Unit}})
		}
		diffs := map[string]any{}
		for _, s := range costSortedMapKeys(byService) {
			diffs[s] = map[string]any{"estimatedCost": map[string]any{"amount": byService[s], "currency": "USD"}, "historicalCost": map[string]any{"amount": 0.0, "currency": "USD"}}
		}
		r.Data["billInterval"] = sc.Data["billInterval"]
		r.Data["costSummary"] = map[string]any{"serviceCostDifferences": diffs,
			"totalCostDifference": map[string]any{"estimatedCost": map[string]any{"amount": total, "currency": "USD"}, "historicalCost": map[string]any{"amount": 0.0, "currency": "USD"}}}
		r.Data["lineItems"], r.Data["inputUsage"], r.Data["inputCommitments"] = lines, sc.Data["usageMods"], sc.Data["commitMods"]
		creat(c, r, "COMPLETE")
		return c.Fill(r), nil
	})
	for op, store := range map[string]string{"ListBillEstimateLineItems": "lineItems", "ListBillEstimateInputUsageModifications": "inputUsage",
		"ListBillEstimateCommitments": "commitments", "ListBillEstimateInputCommitmentModifications": "inputCommitments"} {
		Override(svc, op, func(c *Call) (map[string]any, error) {
			r := c.Get("BillEstimate", str(c.In["billEstimateId"]))
			if r == nil {
				return nil, c.NotFound("Bill estimate %s does not exist", str(c.In["billEstimateId"]))
			}
			page, next := costPageOf(costFilterItems(asList(r.Data[store]), c.In["filters"]), c.In["nextToken"], c.In["maxResults"], 100)
			return map[string]any{"items": page, "nextToken": next}, nil
		})
	}
}

// ---- usage items

type calcKind struct {
	svc, parent, parentIn, store string
	create, update, del, list    string
	createIn, updateIn, delIn    string
	priced                       bool                                                       // entries name usage the price list must have
	build                        func(e map[string]any, p pricelist.Product) map[string]any // the fields an entry adds to its item
	change                       func(it, e map[string]any)                                 // applies an update entry to an item
	after                        func(r *Resource)                                          // recomputes the parent
}

// calcPrice finds the price of usage in the list.
func calcPrice(serviceCode, usageType, operation string) (pricelist.Product, bool) {
	s := pricelist.Lookup(serviceCode)
	if s == nil {
		return pricelist.Product{}, false
	}
	if p, ok := s.Price("", usageType, operation); ok {
		return p, true
	}
	return s.Price("", usageType, "")
}

func costUnitPrice(it map[string]any) float64 {
	p, _ := calcPrice(str(it["serviceCode"]), str(it["usageType"]), str(it["operation"]))
	return p.Price
}

func costToFloat(v any) float64 {
	f, _ := v.(float64)
	return f
}

// quantities turns the amounts of a usage modification into its quantities, with the unit.
func costQuantities(amounts any, unit string) []any {
	out := []any{}
	for _, a := range asList(amounts) {
		am := costCmap(a)
		out = append(out, map[string]any{"startHour": am["startHour"], "amount": am["amount"], "unit": unit})
	}
	return out
}

// calcBatch registers the batch create, update, delete and list operations of one kind of item.
func calcBatch(k calcKind) {
	parent := func(c *Call) (*Resource, error) {
		r := c.Get(k.parent, str(c.In[k.parentIn]))
		if r == nil {
			return nil, c.NotFound("%s %s does not exist", k.parent, str(c.In[k.parentIn]))
		}
		return r, nil
	}
	finish := func(c *Call, r *Resource, items []any) {
		r.Data[k.store] = items
		if k.after != nil {
			k.after(r)
		}
		c.Put(r)
	}
	errs := func(code, msg, field, val string) map[string]any {
		return map[string]any{"errorCode": code, "errorMessage": msg, field: val}
	}
	Override(k.svc, k.create, func(c *Call) (map[string]any, error) {
		r, err := parent(c)
		if err != nil {
			return nil, err
		}
		items := asList(r.Data[k.store])
		var made, bad []any
		for _, x := range asList(c.In[k.createIn]) {
			e := costCmap(x)
			if slices.ContainsFunc(items, func(i any) bool { return str(costCmap(i)["key"]) == str(e["key"]) }) {
				bad = append(bad, errs("CONFLICT", "An item with key "+str(e["key"])+" already exists", "key", str(e["key"])))
				continue
			}
			it := map[string]any{"id": core.UUID()}
			for f, v := range e {
				it[f] = v
			}
			delete(it, "amount")
			if k.priced {
				p, ok := calcPrice(str(e["serviceCode"]), str(e["usageType"]), str(e["operation"]))
				if !ok {
					bad = append(bad, errs("BAD_REQUEST", "No price for "+str(e["serviceCode"])+" "+str(e["usageType"])+" "+str(e["operation"])+" in the price list", "key", str(e["key"])))
					continue
				}
				for f, v := range k.build(e, p) {
					it[f] = v
				}
			}
			items, made = append(items, it), append(made, it)
		}
		finish(c, r, items)
		return map[string]any{"items": made, "errors": costOrEmpty(bad)}, nil
	})
	Override(k.svc, k.update, func(c *Call) (map[string]any, error) {
		r, err := parent(c)
		if err != nil {
			return nil, err
		}
		items := asList(r.Data[k.store])
		var done, bad []any
		for _, x := range asList(c.In[k.updateIn]) {
			e := costCmap(x)
			i := slices.IndexFunc(items, func(i any) bool { return str(costCmap(i)["id"]) == str(e["id"]) })
			if i < 0 {
				bad = append(bad, errs("NOT_FOUND", "Item "+str(e["id"])+" does not exist", "id", str(e["id"])))
				continue
			}
			it := costCmap(items[i])
			if e["group"] != nil {
				it["group"] = e["group"]
			}
			if k.change != nil {
				k.change(it, e)
			}
			done = append(done, it)
		}
		finish(c, r, items)
		return map[string]any{"items": done, "errors": costOrEmpty(bad)}, nil
	})
	Override(k.svc, k.del, func(c *Call) (map[string]any, error) {
		r, err := parent(c)
		if err != nil {
			return nil, err
		}
		items := asList(r.Data[k.store])
		var bad []any
		for _, id := range asList(c.In[k.delIn]) {
			i := slices.IndexFunc(items, func(i any) bool { return str(costCmap(i)["id"]) == str(id) })
			if i < 0 {
				bad = append(bad, errs("NOT_FOUND", "Item "+str(id)+" does not exist", "id", str(id)))
				continue
			}
			items = slices.Delete(items, i, i+1)
		}
		finish(c, r, items)
		return map[string]any{"errors": costOrEmpty(bad)}, nil
	})
	Override(k.svc, k.list, func(c *Call) (map[string]any, error) {
		r, err := parent(c)
		if err != nil {
			return nil, err
		}
		page, next := costPageOf(costFilterItems(asList(r.Data[k.store]), c.In["filters"]), c.In["nextToken"], c.In["maxResults"], 100)
		return map[string]any{"items": page, "nextToken": next}, nil
	})
}

func costOrEmpty(l []any) []any {
	if l == nil {
		return []any{}
	}
	return l
}

var calcFilterField = map[string]string{"USAGE_ACCOUNT_ID": "usageAccountId", "SERVICE_CODE": "serviceCode", "USAGE_TYPE": "usageType", "OPERATION": "operation",
	"LOCATION": "location", "USAGE_GROUP": "group", "LINE_ITEM_TYPE": "lineItemType"}

// filterItems keeps the items every filter of a list call lets through: the filter names the member, the values are alternatives.
func costFilterItems(items []any, filters any) []any {
	fs := asList(filters)
	if len(fs) == 0 {
		return items
	}
	return slices.DeleteFunc(slices.Clone(items), func(it any) bool {
		for _, f := range fs {
			fm := costCmap(f)
			have, hit := str(costCmap(it)[calcFilterField[str(fm["name"])]]), false
			for _, v := range asList(fm["values"]) {
				switch w := str(v); str(fm["matchOption"]) {
				case "STARTS_WITH":
					hit = hit || strings.HasPrefix(have, w)
				case "CONTAINS":
					hit = hit || strings.Contains(have, w)
				default:
					hit = hit || have == w
				}
			}
			if !hit {
				return true
			}
		}
		return false
	})
}
