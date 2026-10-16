package generic

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Cost and Usage Report: report definitions are the engine's CRUD around the
// ReportDefinition wrapper. Once a report is due (each hour, day or month, by
// its TimeUnit) the ticker, or Service.Tick in tests, writes the month-to-date
// bill as CSV into the report's S3 bucket (costreport.go):
//
//	<prefix>/<report>/<yyyymmdd>-<yyyymmdd>/<report>-00001.csv[.gz]   plus <report>-Manifest.json
//
// with an assembly-id folder in between when the report keeps a new version
// each time (CREATE_NEW_REPORT).
func init() {
	arnTemplates["cur/ReportDefinition"] = "definition/{ReportName}"
	post := func(c *Call, r *Resource, out map[string]any) {
		d := costCmap(r.Data["ReportDefinition"])
		for k, v := range map[string]any{"ReportVersioning": "CREATE_NEW_REPORT", "RefreshClosedReports": true, "AdditionalArtifacts": []any{}} {
			if d[k] == nil {
				d[k] = v
			}
			r.Data[k] = d[k]
		}
	}
	for _, op := range []string{"PutReportDefinition", "ModifyReportDefinition"} {
		Override("cur", op, func(c *Call) (map[string]any, error) {
			d := costCmap(c.In["ReportDefinition"])
			if (str(d["Format"]) == "Parquet") != (str(d["Compression"]) == "Parquet") {
				return nil, c.Invalid("Parquet is both a format and a compression: use it for both or for neither")
			}
			if !costBucketExists(c.Hub(), c.Account, str(d["S3Region"]), str(d["S3Bucket"])) {
				return nil, c.Invalid("Failed to verify customer bucket permission.")
			}
			if op == "PutReportDefinition" && c.Get("ReportDefinition", str(d["ReportName"])) != nil { // the engine's Put replaces
				return nil, c.Exists("Report name %s already exists", str(d["ReportName"]))
			}
			costHoist(c, "ReportDefinition")
			out, err := c.Default()
			if err != nil {
				return nil, err
			}
			post(c, c.Res, out)
			c.Put(c.Res)
			return out, nil
		})
	}
	starters["cur"] = append(starters["cur"], func(s *Service) { s.Every("cur", time.Minute, s.costDeliverReports) })
	Override("cur", "DeleteReportDefinition", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err == nil {
			out["ResponseMessage"] = "Successfully deleted report definition with name: " + str(c.In["ReportName"])
		}
		return out, err
	})
	// Tags are addressed by report name, which the engine does not take for an identifier of a tag call.
	tagged := func(c *Call) (*Resource, error) {
		r := c.Get("ReportDefinition", str(c.In["ReportName"]))
		if r == nil {
			return nil, c.NotFound("Report definition %s does not exist", str(c.In["ReportName"]))
		}
		return r, nil
	}
	Override("cur", "TagResource", func(c *Call) (map[string]any, error) {
		r, err := tagged(c)
		if err != nil {
			return nil, err
		}
		applyTags(r, tagMap(c.In["Tags"]), nil)
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("cur", "UntagResource", func(c *Call) (map[string]any, error) {
		r, err := tagged(c)
		if err != nil {
			return nil, err
		}
		var keys []string
		for _, k := range asList(c.In["TagKeys"]) {
			keys = append(keys, str(k))
		}
		applyTags(r, nil, keys)
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("cur", "ListTagsForResource", func(c *Call) (map[string]any, error) {
		r, err := tagged(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"Tags": tagsAs(c.op.out.Members["Tags"].sh, r.Tags)}, nil
	})
}

// deliverReports writes the reports that are due as of now.
func (s *Service) costDeliverReports(now time.Time) {
	for _, r := range s.costAllOf("ReportDefinition") {
		d := costCmap(r.Data["ReportDefinition"])
		period := costPeriodStart(now, str(d["TimeUnit"]))
		if last, ok := costParseTime(r.Data["LastPeriod"]); ok && !last.Before(period) {
			continue
		}
		name, gz := str(d["ReportName"]), str(d["Compression"]) == "GZIP"
		dir := strings.Trim(strings.Join([]string{strings.Trim(str(d["S3Prefix"]), "/"), name, costBillingPeriod(now)}, "/"), "/")
		if str(d["ReportVersioning"]) != "OVERWRITE_REPORT" {
			dir += "/" + core.RandHex(8) // the assembly id
		}
		file, suffix := dir+"/"+name+"-00001.csv", ""
		if gz {
			suffix = ".gz"
		}
		manifest, _ := json.Marshal(map[string]any{"assemblyId": dir, "account": r.Account, "reportName": name, "bucket": str(d["S3Bucket"]),
			"billingPeriod": map[string]string{"start": costMonthStart(now).Format("20060102T150405.000Z"), "end": costMonthStart(now).AddDate(0, 1, 0).Format("20060102T150405.000Z")},
			"reportKeys":    []string{file + suffix}, "compression": str(d["Compression"])})
		status := costPutObject(s.h, r.Account, str(d["S3Region"]), str(d["S3Bucket"]), file+suffix, costReportCSV(s.h, r.Account, now, gz))
		if status < 300 {
			status = costPutObject(s.h, r.Account, str(d["S3Region"]), str(d["S3Bucket"]), dir+"/"+name+"-Manifest.json", manifest)
		}
		result := "SUCCESS"
		switch {
		case status == 404:
			result = "ERROR_NO_BUCKET"
		case status >= 300:
			result = "ERROR_PERMISSIONS"
		}
		s.costMutate(r, func(cur *Resource) {
			cur.Data["LastPeriod"] = period.Format(time.RFC3339)
			rd := costCmap(cur.Data["ReportDefinition"])
			rd["ReportStatus"] = map[string]any{"lastDelivery": now.Format("2006-01-02T15:04:05Z"), "lastStatus": result}
			cur.Data["ReportStatus"] = rd["ReportStatus"]
		})
	}
}
