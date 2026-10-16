package generic

import (
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Security Hub: the hub is one record per account and region, with its
// standards subscriptions and product subscriptions. Findings are not kept
// (ponytail: BatchImportFindings and GetFindings use the engine's empty defaults).

// shStandards are the standards DescribeStandards lists: name, version, enabled by default.
var shStandards = []struct {
	key, name, version string
	def                bool
}{
	{"aws-foundational-security-best-practices", "AWS Foundational Security Best Practices", "1.0.0", true},
	{"cis-aws-foundations-benchmark", "CIS AWS Foundations Benchmark", "1.2.0", true},
	{"cis-aws-foundations-benchmark", "CIS AWS Foundations Benchmark", "1.4.0", false},
	{"pci-dss", "PCI DSS", "3.2.1", false},
	{"nist-800-53", "NIST Special Publication 800-53 Revision 5", "5.0.0", false},
}

func shStandardsARN(c *Call, key, version string) string {
	if key == "cis-aws-foundations-benchmark" && version == "1.2.0" {
		return "arn:" + core.Partition(c.Region) + ":securityhub:::ruleset/" + key + "/v/" + version
	}
	return "arn:" + core.Partition(c.Region) + ":securityhub:" + c.Region + "::standards/" + key + "/v/" + version
}

func shKnown(c *Call, arn string) bool {
	for _, s := range shStandards {
		if shStandardsARN(c, s.key, s.version) == arn {
			return true
		}
	}
	return false
}

func shHub(c *Call) (*Resource, error) {
	if r := c.Get("Hub", "default"); r != nil {
		return r, nil
	}
	return nil, c.Err("InvalidAccessException", "Account %s is not subscribed to AWS Security Hub", c.Account)
}

func shSubscribe(c *Call, standardsARN string, input any) *Resource {
	key := standardsARN
	if i := strings.Index(standardsARN, "standards/"); i >= 0 {
		key = standardsARN[i+len("standards/"):]
	} else if i := strings.Index(standardsARN, "ruleset/"); i >= 0 {
		key = standardsARN[i+len("ruleset/"):]
	}
	arn := core.ARN("securityhub", c.Region, c.Account, "subscription/"+key)
	if r := c.Get("StandardsSubscription", arn); r != nil {
		return r
	}
	now := c.now()
	r := &Resource{Type: "StandardsSubscription", Key: arn, ARN: arn, Name: arn, Account: c.Account, Region: c.key(), Created: now, Updated: now,
		Data: map[string]any{"StandardsSubscriptionArn": arn, "StandardsArn": standardsARN, "StandardsInput": input, "StandardsStatus": "READY"}}
	if input == nil {
		r.Data["StandardsInput"] = map[string]any{}
	}
	c.Put(r)
	return r
}

func shSub(r *Resource) map[string]any {
	return map[string]any{"StandardsSubscriptionArn": r.Data["StandardsSubscriptionArn"], "StandardsArn": r.Data["StandardsArn"], "StandardsInput": r.Data["StandardsInput"], "StandardsStatus": r.Data["StandardsStatus"]}
}

func init() {
	Override("securityhub", "EnableSecurityHub", func(c *Call) (map[string]any, error) {
		if c.Get("Hub", "default") != nil {
			return nil, c.Err("ResourceConflictException", "Account %s is already subscribed to Security Hub", c.Account)
		}
		gen := str(c.In["ControlFindingGenerator"])
		if gen == "" {
			gen = "SECURITY_CONTROL"
		}
		arn := core.ARN("securityhub", c.Region, c.Account, "hub/default")
		now := c.now()
		r := &Resource{Type: "Hub", Key: "default", ARN: arn, Name: "default", Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"HubArn": arn, "SubscribedAt": now.Format("2006-01-02T15:04:05.000Z"), "AutoEnableControls": true, "ControlFindingGenerator": gen}}
		_, add, _ := takeTags(c.op.in, c.In)
		applyTags(r, add, nil)
		c.Put(r)
		if v, ok := c.In["EnableDefaultStandards"].(bool); !ok || v {
			for _, s := range shStandards {
				if s.def {
					shSubscribe(c, shStandardsARN(c, s.key, s.version), nil)
				}
			}
		}
		return map[string]any{}, nil
	})
	Override("securityhub", "DescribeHub", func(c *Call) (map[string]any, error) {
		r, err := shHub(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"HubArn": r.ARN, "SubscribedAt": r.Data["SubscribedAt"], "AutoEnableControls": r.Data["AutoEnableControls"], "ControlFindingGenerator": r.Data["ControlFindingGenerator"]}, nil
	})
	Override("securityhub", "UpdateSecurityHubConfiguration", func(c *Call) (map[string]any, error) {
		r, err := shHub(c)
		if err != nil {
			return nil, err
		}
		for _, k := range []string{"AutoEnableControls", "ControlFindingGenerator"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		r.Updated = c.now()
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("securityhub", "DisableSecurityHub", func(c *Call) (map[string]any, error) {
		r, err := shHub(c)
		if err != nil {
			return nil, err
		}
		for _, typ := range []string{"StandardsSubscription", "ProductSubscription"} {
			for _, s := range c.List(typ) {
				c.Svc.Delete(c.Account, c.Region, typ, s.Key)
			}
		}
		c.Svc.Delete(c.Account, c.Region, "Hub", r.Key)
		return map[string]any{}, nil
	})

	Override("securityhub", "DescribeStandards", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, s := range shStandards {
			out = append(out, map[string]any{"StandardsArn": shStandardsARN(c, s.key, s.version), "Name": s.name, "Description": s.name + " v" + s.version, "EnabledByDefault": s.def})
		}
		return map[string]any{"Standards": out}, nil
	})
	Override("securityhub", "BatchEnableStandards", func(c *Call) (map[string]any, error) {
		if _, err := shHub(c); err != nil {
			return nil, err
		}
		out := []any{}
		for _, q := range asList(c.In["StandardsSubscriptionRequests"]) {
			m := q.(map[string]any)
			if !shKnown(c, str(m["StandardsArn"])) {
				return nil, c.Err("InvalidInputException", "Invalid standards ARN %s", str(m["StandardsArn"]))
			}
			out = append(out, shSub(shSubscribe(c, str(m["StandardsArn"]), m["StandardsInput"])))
		}
		return map[string]any{"StandardsSubscriptions": out}, nil
	})
	Override("securityhub", "BatchDisableStandards", func(c *Call) (map[string]any, error) {
		if _, err := shHub(c); err != nil {
			return nil, err
		}
		out := []any{}
		for _, a := range asList(c.In["StandardsSubscriptionArns"]) {
			if r := c.Get("StandardsSubscription", str(a)); r != nil {
				c.Svc.Delete(c.Account, c.Region, "StandardsSubscription", r.Key)
				r.Data["StandardsStatus"] = "DELETING"
				out = append(out, shSub(r))
			}
		}
		return map[string]any{"StandardsSubscriptions": out}, nil
	})
	Override("securityhub", "GetEnabledStandards", func(c *Call) (map[string]any, error) {
		if _, err := shHub(c); err != nil {
			return nil, err
		}
		want := asList(c.In["StandardsSubscriptionArns"])
		out := []any{}
		for _, r := range c.List("StandardsSubscription") {
			if len(want) == 0 || slices.ContainsFunc(want, func(a any) bool { return str(a) == r.ARN }) {
				out = append(out, shSub(r))
			}
		}
		return map[string]any{"StandardsSubscriptions": out}, nil
	})

	Override("securityhub", "EnableImportFindingsForProduct", func(c *Call) (map[string]any, error) {
		if _, err := shHub(c); err != nil {
			return nil, err
		}
		p := str(c.In["ProductArn"])
		arn := core.ARN("securityhub", c.Region, c.Account, "product-subscription/"+p[strings.Index(p, "product/")+len("product/"):])
		if c.Get("ProductSubscription", arn) != nil {
			return nil, c.Err("ResourceConflictException", "Product %s is already enabled", p)
		}
		now := c.now()
		c.Put(&Resource{Type: "ProductSubscription", Key: arn, ARN: arn, Name: arn, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: map[string]any{"ProductArn": p}})
		return map[string]any{"ProductSubscriptionArn": arn}, nil
	})
	Override("securityhub", "DisableImportFindingsForProduct", func(c *Call) (map[string]any, error) {
		if !c.Svc.Delete(c.Account, c.Region, "ProductSubscription", str(c.In["ProductSubscriptionArn"])) {
			return nil, c.NotFound("Product subscription %s not found", str(c.In["ProductSubscriptionArn"]))
		}
		return map[string]any{}, nil
	})
	Override("securityhub", "ListEnabledProductsForImport", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("ProductSubscription") {
			out = append(out, r.ARN)
		}
		return map[string]any{"ProductSubscriptions": out}, nil
	})
	Override("securityhub", "DescribeProducts", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, p := range []struct{ id, name string }{{"aws/guardduty", "GuardDuty"}, {"aws/inspector", "Inspector"}, {"aws/macie", "Macie"}} {
			a := core.ARN("securityhub", c.Region, "", "product/"+p.id)
			if c.In["ProductArn"] == nil || c.In["ProductArn"] == a {
				out = append(out, map[string]any{"ProductArn": a, "ProductName": p.name, "CompanyName": "Amazon", "Description": p.name + " findings", "IntegrationTypes": []any{"SEND_FINDINGS_TO_SECURITY_HUB"}})
			}
		}
		return map[string]any{"Products": out}, nil
	})

	// Action targets and insights have ARNs of their own shape.
	Override("securityhub", "CreateActionTarget", func(c *Call) (map[string]any, error) {
		c.In["ActionTargetArn"] = core.ARN("securityhub", c.Region, c.Account, "action/custom/"+str(c.In["Id"]))
		return c.Default()
	})
	Override("securityhub", "CreateInsight", func(c *Call) (map[string]any, error) {
		c.In["InsightArn"] = core.ARN("securityhub", c.Region, c.Account, "insight/"+c.Account+"/custom/"+core.UUID())
		return c.Default()
	})
}
