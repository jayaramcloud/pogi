package monitor

// Azure Resource Graph: POST /providers/Microsoft.ResourceGraph/resources runs a KQL query over the
// Resource Manager's resources (the Resources table) and its subscriptions and resource groups
// (ResourceContainers), with the KQL subset in kql.go.
//
// ponytail: only those two tables; the others (PolicyResources, AdvisorResources, ...) are unknown tables.

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// graphCols are the columns of both tables, in Resource Graph's order.
var graphCols = []col{{"id", "string"}, {"name", "string"}, {"type", "string"}, {"tenantId", "string"},
	{"kind", "string"}, {"location", "string"}, {"resourceGroup", "string"}, {"subscriptionId", "string"},
	{"managedBy", "string"}, {"sku", "dynamic"}, {"plan", "dynamic"}, {"properties", "dynamic"}, {"tags", "dynamic"},
	{"identity", "dynamic"}, {"zones", "dynamic"}, {"extendedLocation", "dynamic"}}

// tabular operators a query may start with: Resource Graph then reads the Resources table.
var graphOps = map[string]bool{"where": true, "project": true, "project-away": true, "project-keep": true,
	"project-rename": true, "extend": true, "summarize": true, "count": true, "limit": true, "take": true, "top": true,
	"order": true, "sort": true, "distinct": true}

// graphRel makes a table of resources. Resource Graph lower-cases the type and the resource group.
func graphRel(items []map[string]any) *rel {
	r := &rel{cols: graphCols}
	for _, it := range items {
		id, _ := arm.ParseID(str(it, "id"))
		sub := id.Subscription
		if sub == "" { // a subscription in ResourceContainers
			sub = str(it, "subscriptionId")
		}
		row := []any{str(it, "id"), str(it, "name"), strings.ToLower(str(it, "type")), arm.TenantID, str(it, "kind"),
			str(it, "location"), strings.ToLower(id.ResourceGroup), sub, str(it, "managedBy"), it["sku"], it["plan"],
			it["properties"], it["tags"], it["identity"], it["zones"], it["extendedLocation"]}
		if strings.EqualFold(str(it, "type"), "Microsoft.Resources/resourceGroups") {
			row[2] = "microsoft.resources/subscriptions/resourcegroups"
		}
		if row[2] == "" { // a subscription
			row[1], row[2] = str(it, "displayName"), "microsoft.resources/subscriptions"
			row[11] = map[string]any{"state": it["state"], "subscriptionPolicies": it["subscriptionPolicies"]}
		}
		if row[12] == nil {
			row[12] = map[string]any{}
		}
		r.rows = append(r.rows, row)
	}
	return r
}

// graphQuery is the resources action. The body is Resource Graph's QueryRequest.
func (s *Service) graphQuery(c *core.Ctx, _ arm.ID, body map[string]any) (any, error) {
	bad := func(f string, a ...any) error { return core.Errorf(http.StatusBadRequest, "BadRequest", f, a...) }
	src, _ := body["query"].(string)
	if strings.TrimSpace(src) == "" {
		return nil, bad("The query is missing: send it as \"query\".")
	}
	var subs []string
	if l, ok := body["subscriptions"].([]any); ok {
		for _, v := range l {
			if sub, ok := v.(string); ok {
				subs = append(subs, sub)
			}
		}
	}
	opts, _ := body["options"].(map[string]any)
	num := func(name string, def, lo, hi int) (int, error) {
		v, ok := opts[name]
		if !ok || v == nil {
			return def, nil
		}
		f, ok := v.(float64)
		if !ok || f != float64(int(f)) || int(f) < lo || int(f) > hi {
			return 0, bad("The option %s must be a whole number from %d to %d.", name, lo, hi)
		}
		return int(f), nil
	}
	top, err := num("$top", 100, 1, 1000)
	if err != nil {
		return nil, err
	}
	skip, err := num("$skip", 0, 0, 1<<31-1)
	if err != nil {
		return nil, err
	}
	if tok, _ := opts["$skipToken"].(string); tok != "" {
		n, err := strconv.Atoi(tok)
		if err != nil || n < 0 {
			return nil, bad("The $skipToken '%s' is not one this service gave out.", tok)
		}
		skip = n
	}
	format := "objectArray"
	if f, _ := opts["resultFormat"].(string); f != "" {
		if f != "table" && f != "objectArray" {
			return nil, bad("The resultFormat '%s' is not valid: use table or objectArray.", f)
		}
		format = f
	}
	if first, _, _ := strings.Cut(strings.TrimSpace(src), " "); graphOps[first] {
		src = "Resources | " + src
	}
	q, err := parseKQL(src)
	if err != nil {
		return nil, bad("The query is invalid: %v", err)
	}
	a, ok := s.hub.Service("arm").(*arm.Service)
	if !ok {
		return nil, core.Errorf(http.StatusServiceUnavailable, "ServiceUnavailable", "The Resource Manager is not running.")
	}
	res, conts := a.Inventory(c, subs)
	out, err := q.run(&env{now: time.Now().UTC(), tables: func(name string) (*rel, bool) {
		switch strings.ToLower(name) {
		case "resources":
			return graphRel(res), true
		case "resourcecontainers":
			return graphRel(conts), true
		}
		return nil, false
	}})
	if err != nil {
		return nil, bad("The query is invalid: %v", err)
	}
	total := len(out.rows)
	rows := out.rows[min(skip, total):min(skip+top, total)]
	resp := map[string]any{"totalRecords": total, "count": len(rows), "resultTruncated": "false", "facets": []any{}}
	if skip+len(rows) < total {
		resp["$skipToken"] = strconv.Itoa(skip + len(rows))
	}
	cell := func(v any, typ string) any {
		if v == nil && typ == "string" {
			return ""
		}
		return jsonVal(v)
	}
	if format == "table" {
		types := map[string]string{"int": "integer", "long": "integer", "real": "number", "bool": "boolean", "dynamic": "object"}
		cols := make([]map[string]any, len(out.cols))
		for i, cl := range out.cols {
			cols[i] = map[string]any{"name": cl.Name, "type": cmpOr(types[cl.Type], "string")}
		}
		data := make([][]any, len(rows))
		for i, row := range rows {
			data[i] = make([]any, len(row))
			for j, v := range row {
				data[i][j] = cell(v, out.cols[j].Type)
			}
		}
		resp["data"] = map[string]any{"columns": cols, "rows": data}
		return resp, nil
	}
	data := make([]map[string]any, len(rows))
	for i, row := range rows {
		data[i] = make(map[string]any, len(row))
		for j, v := range row {
			data[i][out.cols[j].Name] = cell(v, out.cols[j].Type)
		}
	}
	resp["data"] = data
	return resp, nil
}
