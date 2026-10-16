package generic

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// BCM Data Exports: exports are the engine's CRUD around the Export wrapper
// (their ARN carries the export's name). Once a day the ticker, or
// Service.Tick in tests, runs every export: the month-to-date bill goes into the
// export's S3 bucket as CSV (costreport.go) and an execution is recorded:
//
//	<prefix>/<export>/data/BILLING_PERIOD=<yyyy-mm>/<export>-00001.csv[.gz]
//	<prefix>/<export>/metadata/BILLING_PERIOD=<yyyy-mm>/<export>-Manifest.json
//
// with the execution id after the export's name when the export keeps a new
// version each time (CREATE_NEW_REPORT). The query is not parsed (ponytail: every
// export has the same columns) and Parquet is written as CSV.
func init() {
	stamp := func(c *Call, r *Resource, out map[string]any) {
		now := time.Now().UTC().Format(time.RFC3339)
		exp := costCmap(r.Data["Export"])
		exp["ExportArn"] = r.ARN
		r.Data["ExportName"] = exp["Name"]
		st, _ := r.Data["ExportStatus"].(map[string]any)
		if st == nil {
			st = map[string]any{"StatusCode": "HEALTHY", "CreatedAt": now, "LastRefreshedAt": now}
		}
		st["LastUpdatedAt"] = now
		r.Data["ExportStatus"] = st
	}
	valid := func(c *Call) error {
		exp := costCmap(c.In["Export"])
		dest := costCmap(costCmap(exp["DestinationConfigurations"])["S3Destination"])
		if !costBucketExists(c.Hub(), c.Account, str(dest["S3Region"]), str(dest["S3Bucket"])) {
			return c.Invalid("The S3 bucket %s does not exist or pogi cannot write to it", str(dest["S3Bucket"]))
		}
		return nil
	}
	Override("bcm-data-exports", "CreateExport", func(c *Call) (map[string]any, error) {
		exp := costCmap(c.In["Export"])
		if err := valid(c); err != nil {
			return nil, err
		}
		for _, r := range c.List("Export") {
			if str(r.Data["ExportName"]) == str(exp["Name"]) {
				return nil, c.Invalid("An export named %s already exists", str(exp["Name"]))
			}
		}
		costHoist(c, "Export")
		c.In["ExportArn"] = core.ARN("bcm-data-exports", c.Region, c.Account, "export/"+str(exp["Name"])+"-"+core.UUID())
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		stamp(c, c.Res, out)
		c.Res.Data["Executions"] = []any{}
		c.Put(c.Res)
		return out, nil
	})
	Override("bcm-data-exports", "UpdateExport", func(c *Call) (map[string]any, error) {
		if err := valid(c); err != nil {
			return nil, err
		}
		costHoist(c, "Export")
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		stamp(c, c.Res, out)
		c.Put(c.Res)
		return out, nil
	})
	starters["bcm-data-exports"] = append(starters["bcm-data-exports"], func(s *Service) { s.Every("exports", time.Minute, s.costDeliverExports) })
	Override("bcm-data-exports", "ListExecutions", func(c *Call) (map[string]any, error) {
		r := c.Get("Export", str(c.In["ExportArn"]))
		if r == nil {
			return nil, c.NotFound("Export %s does not exist", str(c.In["ExportArn"]))
		}
		page, next := costPageOf(asList(r.Data["Executions"]), c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Executions": page, "NextToken": next}, nil
	})
	Override("bcm-data-exports", "GetExecution", func(c *Call) (map[string]any, error) {
		r := c.Get("Export", str(c.In["ExportArn"]))
		if r == nil {
			return nil, c.NotFound("Export %s does not exist", str(c.In["ExportArn"]))
		}
		for _, e := range asList(r.Data["Executions"]) {
			if em := costCmap(e); str(em["ExecutionId"]) == str(c.In["ExecutionId"]) {
				return map[string]any{"ExecutionId": em["ExecutionId"], "ExecutionStatus": em["ExecutionStatus"], "Export": r.Data["Export"]}, nil
			}
		}
		return nil, c.NotFound("Execution %s does not exist", str(c.In["ExecutionId"]))
	})

	// ---- the tables an export can query
	prop := func(name, desc, def string, valid ...string) map[string]any {
		return map[string]any{"Name": name, "Description": desc, "DefaultValue": def, "ValidValues": costStrs(valid)}
	}
	granularity := prop("TIME_GRANULARITY", "The time granularity of the data", "HOURLY", "HOURLY", "DAILY", "MONTHLY")
	view := prop("BILLING_VIEW_ARN", "The billing view the data comes from", "")
	tables := map[string]map[string]any{
		"COST_AND_USAGE_REPORT": {"Description": "Cost and Usage Report data", "TableProperties": []any{granularity, prop("INCLUDE_RESOURCES", "Include resource ids", "FALSE", "TRUE", "FALSE"), view}},
		"FOCUS_1_2_AWS":         {"Description": "FOCUS 1.2 data with AWS columns", "TableProperties": []any{granularity, view}},
	}
	Override("bcm-data-exports", "ListTables", func(c *Call) (map[string]any, error) {
		var items []any
		for _, n := range costSortedMapKeys(tables) {
			items = append(items, map[string]any{"TableName": n, "Description": tables[n]["Description"], "TableProperties": tables[n]["TableProperties"]})
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 100)
		return map[string]any{"Tables": page, "NextToken": next}, nil
	})
	Override("bcm-data-exports", "GetTable", func(c *Call) (map[string]any, error) {
		t := tables[str(c.In["TableName"])]
		if t == nil {
			return nil, c.NotFound("Table %s does not exist", str(c.In["TableName"]))
		}
		var cols []any
		for _, h := range costReportHeader {
			typ := "String"
			if strings.HasSuffix(h, "UsageAmount") || strings.HasSuffix(h, "UnblendedCost") {
				typ = "Double"
			}
			cols = append(cols, map[string]any{"Name": h, "Type": typ, "Description": "pogi cost model: " + h})
		}
		props := costCmap(c.In["TableProperties"])
		if props == nil {
			props = map[string]any{}
		}
		return map[string]any{"TableName": c.In["TableName"], "Description": t["Description"], "Schema": cols, "TableProperties": props}, nil
	})
}

// deliverExports runs the exports that have not run today as of now.
func (s *Service) costDeliverExports(now time.Time) {
	for _, r := range s.costAllOf("Export") {
		if last, ok := costParseTime(r.Data["LastRun"]); ok && !last.Before(costDay(now)) {
			continue
		}
		exp := costCmap(r.Data["Export"])
		dest := costCmap(costCmap(exp["DestinationConfigurations"])["S3Destination"])
		cfg := costCmap(dest["S3OutputConfigurations"])
		name, gz := str(exp["Name"]), str(cfg["Compression"]) == "GZIP"
		id := core.UUID()
		base := strings.Trim(strings.Trim(str(dest["S3Prefix"]), "/")+"/"+name, "/")
		if str(cfg["Overwrite"]) != "OVERWRITE_REPORT" {
			base += "/" + id
		}
		period := "BILLING_PERIOD=" + now.Format("2006-01")
		file := base + "/data/" + period + "/" + name + "-00001.csv"
		if gz {
			file += ".gz"
		}
		manifest, _ := json.Marshal(map[string]any{"exportArn": r.ARN, "executionId": id, "dataFiles": []string{file}, "billingPeriod": now.Format("2006-01")})
		status := costPutObject(s.h, r.Account, str(dest["S3Region"]), str(dest["S3Bucket"]), file, costReportCSV(s.h, r.Account, now, gz))
		if status < 300 {
			status = costPutObject(s.h, r.Account, str(dest["S3Region"]), str(dest["S3Bucket"]), base+"/metadata/"+period+"/"+name+"-Manifest.json", manifest)
		}
		code, reason := "DELIVERY_SUCCESS", ""
		if status >= 300 {
			code, reason = "DELIVERY_IN_PROCESS", "INSUFFICIENT_PERMISSION"
		}
		stamp := now.UTC().Format(time.RFC3339)
		s.costMutate(r, func(cur *Resource) {
			cur.Data["LastRun"] = stamp
			es := map[string]any{"StatusCode": code, "CreatedAt": stamp, "CompletedAt": stamp, "LastUpdatedAt": stamp}
			if reason != "" {
				es["StatusReason"] = reason
			}
			cur.Data["Executions"] = append(asList(cur.Data["Executions"]), map[string]any{"ExecutionId": id, "ExecutionStatus": es})
			st := costCmap(cur.Data["ExportStatus"])
			st["LastRefreshedAt"] = stamp
		})
	}
}
