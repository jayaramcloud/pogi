package generic

import "github.com/nrndev/pogi/internal/core"

// Access Analyzer: analyzers keep their archive rules, including the ones
// given inline when the analyzer is created. No findings are produced (ponytail).

func init() {
	Hook("accessanalyzer", "CreateAnalyzer", func(c *Call) {
		for _, ir := range asList(c.In["archiveRules"]) {
			rule := ir.(map[string]any)
			now := c.now()
			name := str(c.In["analyzerName"])
			c.Put(&Resource{Type: "ArchiveRule", Key: name + "/" + str(rule["ruleName"]), IDs: map[string]string{"analyzerName": name, "ruleName": str(rule["ruleName"])}, Name: str(rule["ruleName"]),
				ARN: core.ARN("access-analyzer", c.Region, c.Account, "analyzer/"+name+"/archive-rule/"+str(rule["ruleName"])), Account: c.Account, Region: c.key(), Created: now, Updated: now,
				Data: map[string]any{"analyzerName": name, "ruleName": rule["ruleName"], "filter": rule["filter"]}})
		}
	})
	Hook("accessanalyzer", "DeleteAnalyzer", func(c *Call) {
		for _, r := range c.List("ArchiveRule") {
			if r.IDs["analyzerName"] == c.Res.IDs["analyzerName"] {
				c.Svc.Delete(c.Account, c.Region, "ArchiveRule", r.Key)
			}
		}
	})
	Override("accessanalyzer", "ListArchiveRules", func(c *Call) (map[string]any, error) {
		if c.Get("Analyzer", str(c.In["analyzerName"])) == nil {
			return nil, c.NotFound("Analyzer %s not found", str(c.In["analyzerName"]))
		}
		return c.Default()
	})
}
