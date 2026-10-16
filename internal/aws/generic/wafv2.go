package generic

import (
	"cmp"
	"encoding/json"
	"net/netip"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// WAF v2: web ACLs, rule groups, IP sets and regex pattern sets with lock
// tokens that change on every update, associations, logging configurations and
// a computed capacity. Requests are not filtered (ponytail: the rules are only stored).

// wafKinds are the four entities that share one shape of API.
var wafKinds = []struct{ typ, path string }{{"WebACL", "webacl"}, {"RuleGroup", "rulegroup"}, {"IPSet", "ipset"}, {"RegexPatternSet", "regexpatternset"}}

// wafFind is the entity a request names, by name, scope and id or by ARN.
func wafFind(c *Call, typ string) *Resource {
	if arn := str(c.In["ARN"]); arn != "" {
		for _, r := range c.List(typ) {
			if r.ARN == arn {
				return r
			}
		}
		return nil
	}
	return c.Get(typ, str(c.In["Name"]), str(c.In["Scope"]), str(c.In["Id"]))
}

// wafAddresses checks the CIDR blocks of an IP set (a bare IP needs /32 or /128)
// and that they fit its IP address version.
func wafAddresses(c *Call, version string) error {
	for _, a := range asList(c.In["Addresses"]) {
		p, err := netip.ParsePrefix(str(a))
		if err != nil || version == "IPV4" && !p.Addr().Is4() || version == "IPV6" && p.Addr().Is4() {
			return core.Errorf(400, "WAFInvalidParameterException", "Error reason: One or more of the IP addresses is not a valid %s CIDR block, parameter: Addresses (%s)", cmp.Or(version, "IP"), str(a))
		}
	}
	return nil
}

func wafNotFound(c *Call, typ string) error {
	return c.NotFound("AWS WAF couldn't perform the operation because your resource doesn't exist: %s %s", typ, str(c.In["Name"]))
}

func wafLock(c *Call, r *Resource) error {
	if str(c.In["LockToken"]) != str(r.Data["LockToken"]) {
		return core.Errorf(400, "WAFOptimisticLockException", "AWS WAF couldn't save your changes because someone changed the resource after you started to edit it. Reapply your changes.")
	}
	return nil
}

// wafEntity is an entity as Get describes it.
func wafEntity(r *Resource) map[string]any {
	out := map[string]any{}
	for k, v := range r.Data {
		if k != "LockToken" && k != "Scope" {
			out[k] = v
		}
	}
	out["Id"], out["Name"], out["ARN"] = r.IDs["Id"], r.IDs["Name"], r.ARN
	if r.Type == "WebACL" || r.Type == "RuleGroup" {
		out["LabelNamespace"] = "awswaf:" + r.Account + ":" + strings.ToLower(r.Type) + ":" + r.IDs["Name"] + ":"
	}
	if r.Type == "WebACL" {
		out["ManagedByFirewallManager"] = false
		if out["Capacity"] == nil {
			out["Capacity"] = 0.0
		}
	}
	return out
}

// wafCapacity is the capacity units a rule list uses.
// ponytail: a flat cost per statement type, not AWS's per-field-and-transform rule.
func wafCapacity(c *Call, rules []any) float64 {
	var sum func(v any) float64
	sum = func(v any) float64 {
		var n float64
		switch x := v.(type) {
		case []any:
			for _, e := range x {
				n += sum(e)
			}
		case map[string]any:
			for k, e := range x {
				switch k {
				case "Statement", "Statements", "AndStatement", "OrStatement", "NotStatement":
					n += sum(e)
				case "RateBasedStatement":
					m, _ := e.(map[string]any)
					n += 2 + sum(m["ScopeDownStatement"])
				case "ByteMatchStatement", "GeoMatchStatement", "IPSetReferenceStatement", "LabelMatchStatement", "SizeConstraintStatement", "AsnMatchStatement":
					n++
				case "RegexMatchStatement":
					n += 3
				case "RegexPatternSetReferenceStatement":
					n += 25
				case "SqliMatchStatement":
					n += 20
				case "XssMatchStatement":
					n += 40
				case "ManagedRuleGroupStatement":
					n += 50
				case "RuleGroupReferenceStatement":
					m, _ := e.(map[string]any)
					for _, g := range c.List("RuleGroup") {
						if g.ARN == str(m["ARN"]) {
							n += asFloat(g.Data["Capacity"])
						}
					}
				}
			}
		}
		return n
	}
	return sum(rules)
}

func asFloat(v any) float64 {
	f, _ := v.(float64)
	return f
}

func init() {
	for _, k := range wafKinds {
		Override("wafv2", "Create"+k.typ, func(c *Call) (map[string]any, error) {
			if k.typ == "IPSet" {
				if err := wafAddresses(c, str(c.In["IPAddressVersion"])); err != nil {
					return nil, err
				}
			}
			for _, r := range c.List(k.typ) {
				if r.IDs["Name"] == str(c.In["Name"]) && r.IDs["Scope"] == str(c.In["Scope"]) {
					return nil, c.Exists("AWS WAF could not perform the operation because some resource in your request is a duplicate of an existing one: %s", str(c.In["Name"]))
				}
			}
			id := core.UUID()
			region, sc := c.Region, "regional"
			if str(c.In["Scope"]) == "CLOUDFRONT" {
				region, sc = "us-east-1", "global"
			}
			data, add, _ := takeTags(c.op.in, c.In)
			now, name := c.now(), str(c.In["Name"])
			r := &Resource{Type: k.typ, Key: name + "/" + str(c.In["Scope"]) + "/" + id, IDs: map[string]string{"Name": name, "Scope": str(c.In["Scope"]), "Id": id}, Name: name, ID: id,
				ARN: core.ARN("wafv2", region, c.Account, sc+"/"+k.path+"/"+name+"/"+id), Account: c.Account, Region: c.key(), Data: data, Created: now, Updated: now}
			r.Data["LockToken"] = core.UUID()
			if k.typ == "WebACL" {
				r.Data["Capacity"] = wafCapacity(c, asList(c.In["Rules"]))
			}
			applyTags(r, add, nil)
			c.Put(r)
			c.Res = r
			return map[string]any{"Summary": map[string]any{"Name": r.IDs["Name"], "Id": id, "Description": r.Data["Description"], "LockToken": r.Data["LockToken"], "ARN": r.ARN}}, nil
		})
		Override("wafv2", "Get"+k.typ, func(c *Call) (map[string]any, error) {
			r := wafFind(c, k.typ)
			if r == nil {
				return nil, wafNotFound(c, k.typ)
			}
			return map[string]any{k.typ: wafEntity(r), "LockToken": r.Data["LockToken"]}, nil
		})
		Override("wafv2", "Update"+k.typ, func(c *Call) (map[string]any, error) {
			r := wafFind(c, k.typ)
			if r == nil {
				return nil, wafNotFound(c, k.typ)
			}
			if err := wafLock(c, r); err != nil {
				return nil, err
			}
			if k.typ == "IPSet" {
				if err := wafAddresses(c, str(r.Data["IPAddressVersion"])); err != nil {
					return nil, err
				}
			}
			// An update replaces the whole configuration.
			data := map[string]any{}
			for n, v := range c.In {
				if n != "Id" && n != "Name" && n != "Scope" && n != "LockToken" {
					data[n] = v
				}
			}
			if r.Type == "RuleGroup" && data["Capacity"] == nil {
				data["Capacity"] = r.Data["Capacity"]
			}
			if r.Type == "WebACL" {
				data["Capacity"] = wafCapacity(c, asList(c.In["Rules"]))
			}
			data["LockToken"] = core.UUID()
			r.Data, r.Updated = data, c.now()
			c.Put(r)
			return map[string]any{"NextLockToken": data["LockToken"]}, nil
		})
		Override("wafv2", "Delete"+k.typ, func(c *Call) (map[string]any, error) {
			r := wafFind(c, k.typ)
			if r == nil {
				return nil, wafNotFound(c, k.typ)
			}
			if err := wafLock(c, r); err != nil {
				return nil, err
			}
			if used := wafUsers(c, r); used {
				return nil, core.Errorf(400, "WAFAssociatedItemException", "AWS WAF couldn't delete the %s because it's still in use.", k.typ)
			}
			c.Svc.mu.Lock()
			delete(c.Svc.st.Res, r.skey())
			c.Svc.mu.Unlock()
			return map[string]any{}, nil
		})
	}

	// A resource holds at most 50 tags, counting the ones it already has.
	Override("wafv2", "TagResource", func(c *Call) (map[string]any, error) {
		add, targets := tagMap(c.In["Tags"]), c.targets()
		c.Svc.mu.Lock()
		over := false
		for _, r := range targets {
			n := len(add)
			for k := range r.Tags {
				if _, ok := add[k]; !ok {
					n++
				}
			}
			over = over || n > 50
		}
		c.Svc.mu.Unlock()
		if over {
			return nil, c.Err("WAFLimitsExceededException", "A resource can have at most 50 tags")
		}
		return c.Default()
	})

	// A web ACL is associated with resources (load balancers, API stages) by ARN.
	Override("wafv2", "AssociateWebACL", func(c *Call) (map[string]any, error) {
		var acl *Resource
		for _, r := range c.List("WebACL") {
			if r.ARN == str(c.In["WebACLArn"]) {
				acl = r
			}
		}
		if acl == nil {
			return nil, c.NotFound("AWS WAF couldn't perform the operation because your resource doesn't exist: %s", str(c.In["WebACLArn"]))
		}
		now := c.now()
		c.Put(&Resource{Type: "WebACLAssociation", Key: str(c.In["ResourceArn"]), Name: str(c.In["ResourceArn"]), Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"WebACLArn": acl.ARN}})
		return map[string]any{}, nil
	})
	Override("wafv2", "DisassociateWebACL", func(c *Call) (map[string]any, error) {
		if !c.Svc.Delete(c.Account, c.Region, "WebACLAssociation", str(c.In["ResourceArn"])) {
			return nil, c.NotFound("AWS WAF couldn't perform the operation because your resource doesn't exist: %s", str(c.In["ResourceArn"]))
		}
		return map[string]any{}, nil
	})
	Override("wafv2", "GetWebACLForResource", func(c *Call) (map[string]any, error) {
		a := c.Get("WebACLAssociation", str(c.In["ResourceArn"]))
		if a == nil {
			return map[string]any{}, nil
		}
		for _, r := range c.List("WebACL") {
			if r.ARN == a.Data["WebACLArn"] {
				return map[string]any{"WebACL": wafEntity(r)}, nil
			}
		}
		return map[string]any{}, nil
	})
	Override("wafv2", "ListResourcesForWebACL", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, a := range c.List("WebACLAssociation") {
			if a.Data["WebACLArn"] == c.In["WebACLArn"] {
				out = append(out, a.Key)
			}
		}
		return map[string]any{"ResourceArns": out}, nil
	})
	Override("wafv2", "CheckCapacity", func(c *Call) (map[string]any, error) {
		return map[string]any{"Capacity": wafCapacity(c, asList(c.In["Rules"]))}, nil
	})

	// A logging configuration is keyed by the ARN inside its structure.
	Override("wafv2", "PutLoggingConfiguration", func(c *Call) (map[string]any, error) {
		lc, _ := c.In["LoggingConfiguration"].(map[string]any)
		arn := str(lc["ResourceArn"])
		now := c.now()
		c.Put(&Resource{Type: "LoggingConfiguration", Key: arn, Name: arn, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: lc})
		return map[string]any{"LoggingConfiguration": lc}, nil
	})
	Override("wafv2", "GetLoggingConfiguration", func(c *Call) (map[string]any, error) {
		r := c.Get("LoggingConfiguration", str(c.In["ResourceArn"]))
		if r == nil {
			return nil, c.NotFound("AWS WAF couldn't perform the operation because your resource doesn't exist: %s", str(c.In["ResourceArn"]))
		}
		return map[string]any{"LoggingConfiguration": r.Data}, nil
	})
	Override("wafv2", "DeleteLoggingConfiguration", func(c *Call) (map[string]any, error) {
		if !c.Svc.Delete(c.Account, c.Region, "LoggingConfiguration", str(c.In["ResourceArn"])) {
			return nil, c.NotFound("AWS WAF couldn't perform the operation because your resource doesn't exist: %s", str(c.In["ResourceArn"]))
		}
		return map[string]any{}, nil
	})
	Override("wafv2", "ListLoggingConfigurations", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("LoggingConfiguration") {
			out = append(out, r.Data)
		}
		return map[string]any{"LoggingConfigurations": out}, nil
	})
}

// wafUsers says whether another entity or an association refers to r.
func wafUsers(c *Call, r *Resource) bool {
	for _, k := range wafKinds {
		for _, o := range c.List(k.typ) {
			if b, _ := json.Marshal(o.Data); o.ARN != r.ARN && strings.Contains(string(b), r.ARN) {
				return true
			}
		}
	}
	for _, a := range c.List("WebACLAssociation") {
		if a.Data["WebACLArn"] == r.ARN {
			return true
		}
	}
	return false
}
