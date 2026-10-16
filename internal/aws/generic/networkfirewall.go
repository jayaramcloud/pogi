package generic

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Network Firewall: rule groups, firewall policies and firewalls, each found by
// name or ARN and changed under an update token that rotates with every change
// (a stale one is an InvalidTokenException). A firewall is READY at once with a
// synced endpoint per subnet. No traffic is filtered (ponytail).

const nfSvc = "network-firewall"

var nfKinds = map[string]struct{ name, arn, resp, path string }{
	"RuleGroup":      {"RuleGroupName", "RuleGroupArn", "RuleGroupResponse", "stateful-rulegroup"},
	"FirewallPolicy": {"FirewallPolicyName", "FirewallPolicyArn", "FirewallPolicyResponse", "firewall-policy"},
	"Firewall":       {"FirewallName", "FirewallArn", "", "firewall"},
}

func nfFind(c *Call, typ string) (*Resource, error) {
	k := nfKinds[typ]
	for _, r := range c.List(typ) {
		if a := str(c.In[k.arn]); a != "" && r.ARN == a || a == "" && r.Name == str(c.In[k.name]) && r.Name != "" {
			return r, nil
		}
	}
	return nil, c.NotFound("Resource %s %s not found", typ, str(c.In[k.name])+str(c.In[k.arn]))
}

// nfToken checks the update token a request carries against the resource's, if it carries one.
func nfToken(c *Call, r *Resource) error {
	if t := str(c.In["UpdateToken"]); t != "" && t != str(r.Data["UpdateToken"]) {
		return c.Err("InvalidTokenException", "The token you provided is stale or is not valid for the current state of the resource. Describe the resource to get a fresh token.")
	}
	return nil
}

func nfTags(r *Resource) []any {
	out := []any{}
	for _, k := range core.SortedKeys(r.Tags) {
		out = append(out, map[string]any{"Key": k, "Value": r.Tags[k]})
	}
	return out
}

// nfPolicyUsers are the firewalls that use a policy.
func nfPolicyUsers(c *Call, policyARN string) int {
	n := 0
	for _, f := range c.List("Firewall") {
		if f.Data["FirewallPolicyArn"] == policyARN {
			n++
		}
	}
	return n
}

// nfResponse is the descriptive summary of a rule group or a policy.
func nfResponse(c *Call, r *Resource) map[string]any {
	out := map[string]any{"Description": r.Data["Description"], "LastModifiedTime": r.Updated.Format(time.RFC3339Nano), "Tags": nfTags(r), "NumberOfAssociations": 0.0}
	if r.Type == "RuleGroup" {
		out["RuleGroupArn"], out["RuleGroupName"], out["RuleGroupId"], out["RuleGroupStatus"] = r.ARN, r.Name, r.ID, "ACTIVE"
		out["Type"], out["Capacity"], out["ConsumedCapacity"] = r.Data["Type"], r.Data["Capacity"], 0.0
		n := 0
		for _, p := range c.List("FirewallPolicy") {
			if b, _ := json.Marshal(p.Data["FirewallPolicy"]); strings.Contains(string(b), r.ARN) {
				n++
			}
		}
		out["NumberOfAssociations"] = float64(n)
	} else {
		out["FirewallPolicyArn"], out["FirewallPolicyName"], out["FirewallPolicyId"], out["FirewallPolicyStatus"] = r.ARN, r.Name, r.ID, "ACTIVE"
		out["NumberOfAssociations"] = float64(nfPolicyUsers(c, r.ARN))
	}
	return out
}

func nfFirewall(r *Resource) map[string]any {
	out := map[string]any{"FirewallName": r.Name, "FirewallArn": r.ARN, "FirewallId": r.ID, "Tags": nfTags(r)}
	for _, k := range []string{"FirewallPolicyArn", "VpcId", "SubnetMappings", "DeleteProtection", "SubnetChangeProtection", "FirewallPolicyChangeProtection", "Description", "EnabledAnalysisTypes"} {
		if v, ok := r.Data[k]; ok {
			out[k] = v
		}
	}
	return out
}

func nfStatus(c *Call, r *Resource, status string) map[string]any {
	states := map[string]any{}
	for i, m := range asList(r.Data["SubnetMappings"]) {
		subnet := str(m.(map[string]any)["SubnetId"])
		states[fmt.Sprintf("%s%c", c.Region, 'a'+i)] = map[string]any{"Attachment": map[string]any{"SubnetId": subnet, "EndpointId": "vpce-" + r.ID[:8] + fmt.Sprint(i), "Status": "READY"}}
	}
	return map[string]any{"Status": status, "ConfigurationSyncStateSummary": "IN_SYNC", "SyncStates": states}
}

func init() {
	// Rule groups and policies share one shape of API.
	for typ, k := range nfKinds {
		if typ == "Firewall" {
			continue
		}
		cfg := typ // the configuration member is named like the type
		Override(nfSvc, "Create"+typ, func(c *Call) (map[string]any, error) {
			name := str(c.In[k.name])
			for _, r := range c.List(typ) {
				if r.Name == name {
					return nil, c.Exists("A %s named %s already exists", typ, name)
				}
			}
			path := k.path
			if typ == "RuleGroup" && c.In["Type"] == "STATELESS" {
				path = "stateless-rulegroup"
			}
			data, add, _ := takeTags(c.op.in, c.In)
			id, now := core.UUID(), c.now()
			data["UpdateToken"] = core.UUID()
			r := &Resource{Type: typ, Key: name, IDs: map[string]string{k.name: name}, Name: name, ID: id, ARN: core.ARN(nfSvc, c.Region, c.Account, path+"/"+name), Account: c.Account, Region: c.key(), Data: data, Created: now, Updated: now}
			applyTags(r, add, nil)
			if c.In["DryRun"] != true { // a dry run only checks the request
				c.Put(r)
				c.Res = r
			}
			return map[string]any{k.resp: nfResponse(c, r), "UpdateToken": data["UpdateToken"]}, nil
		})
		Override(nfSvc, "Describe"+typ, func(c *Call) (map[string]any, error) {
			r, err := nfFind(c, typ)
			if err != nil {
				return nil, err
			}
			out := map[string]any{k.resp: nfResponse(c, r), "UpdateToken": r.Data["UpdateToken"]}
			if v, ok := r.Data[cfg]; ok {
				out[cfg] = v
			}
			return out, nil
		})
		Override(nfSvc, "Update"+typ, func(c *Call) (map[string]any, error) {
			r, err := nfFind(c, typ)
			if err != nil {
				return nil, err
			}
			if err := nfToken(c, r); err != nil {
				return nil, err
			}
			if c.In["DryRun"] == true {
				return map[string]any{k.resp: nfResponse(c, r), "UpdateToken": r.Data["UpdateToken"]}, nil
			}
			for n, v := range c.In {
				if n != "UpdateToken" && n != k.name && n != k.arn && n != "DryRun" {
					r.Data[n] = v
				}
			}
			r.Data["UpdateToken"], r.Updated = core.UUID(), c.now()
			c.Put(r)
			return map[string]any{k.resp: nfResponse(c, r), "UpdateToken": r.Data["UpdateToken"]}, nil
		})
		Override(nfSvc, "Delete"+typ, func(c *Call) (map[string]any, error) {
			r, err := nfFind(c, typ)
			if err != nil {
				return nil, err
			}
			if n := nfResponse(c, r)["NumberOfAssociations"].(float64); n > 0 {
				return nil, c.Err("InvalidOperationException", "Unable to delete the object because it is still in use in %d other resources", int(n))
			}
			c.Svc.Delete(c.Account, c.Region, typ, r.Key)
			return map[string]any{k.resp: nfResponse(c, r)}, nil
		})
	}
	Override(nfSvc, "ListRuleGroups", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("RuleGroup") {
			if c.In["Type"] == nil || c.In["Type"] == r.Data["Type"] {
				out = append(out, map[string]any{"Name": r.Name, "Arn": r.ARN})
			}
		}
		return map[string]any{"RuleGroups": out}, nil
	})
	Override(nfSvc, "ListFirewallPolicies", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("FirewallPolicy") {
			out = append(out, map[string]any{"Name": r.Name, "Arn": r.ARN})
		}
		return map[string]any{"FirewallPolicies": out}, nil
	})

	// Firewalls.
	Override(nfSvc, "CreateFirewall", func(c *Call) (map[string]any, error) {
		name := str(c.In["FirewallName"])
		for _, r := range c.List("Firewall") {
			if r.Name == name {
				return nil, c.Exists("A firewall named %s already exists", name)
			}
		}
		var policy *Resource
		for _, p := range c.List("FirewallPolicy") {
			if p.ARN == str(c.In["FirewallPolicyArn"]) {
				policy = p
			}
		}
		if policy == nil {
			return nil, c.NotFound("Firewall policy %s not found", str(c.In["FirewallPolicyArn"]))
		}
		data, add, _ := takeTags(c.op.in, c.In)
		for _, k := range []string{"DeleteProtection", "SubnetChangeProtection", "FirewallPolicyChangeProtection"} {
			if data[k] == nil {
				data[k] = false
			}
		}
		data["UpdateToken"] = core.UUID()
		id, now := core.UUID(), c.now()
		r := &Resource{Type: "Firewall", Key: name, IDs: map[string]string{"FirewallName": name}, Name: name, ID: id, ARN: core.ARN(nfSvc, c.Region, c.Account, "firewall/"+name), Account: c.Account, Region: c.key(), Data: data, Created: now, Updated: now}
		applyTags(r, add, nil)
		c.Put(r)
		c.Res = r
		return map[string]any{"Firewall": nfFirewall(r), "FirewallStatus": nfStatus(c, r, "READY")}, nil
	})
	Override(nfSvc, "DescribeFirewall", func(c *Call) (map[string]any, error) {
		r, err := nfFind(c, "Firewall")
		if err != nil {
			return nil, err
		}
		return map[string]any{"Firewall": nfFirewall(r), "FirewallStatus": nfStatus(c, r, "READY"), "UpdateToken": r.Data["UpdateToken"]}, nil
	})
	Override(nfSvc, "ListFirewalls", func(c *Call) (map[string]any, error) {
		vpcs := asList(c.In["VpcIds"])
		out := []any{}
		for _, r := range c.List("Firewall") {
			if len(vpcs) == 0 || slices.ContainsFunc(vpcs, func(v any) bool { return v == r.Data["VpcId"] }) {
				out = append(out, map[string]any{"FirewallName": r.Name, "FirewallArn": r.ARN})
			}
		}
		return map[string]any{"Firewalls": out}, nil
	})
	Override(nfSvc, "DeleteFirewall", func(c *Call) (map[string]any, error) {
		r, err := nfFind(c, "Firewall")
		if err != nil {
			return nil, err
		}
		if r.Data["DeleteProtection"] == true {
			return nil, c.Err("InvalidOperationException", "Unable to delete the firewall because delete protection is on")
		}
		c.Svc.Delete(c.Account, c.Region, "Firewall", r.Key)
		return map[string]any{"Firewall": nfFirewall(r), "FirewallStatus": nfStatus(c, r, "DELETING")}, nil
	})
	// The firewall's own settings each have an update call that echoes the firewall's name and a new token.
	for op, field := range map[string]string{"UpdateFirewallDescription": "Description", "UpdateFirewallDeleteProtection": "DeleteProtection", "UpdateFirewallPolicyChangeProtection": "FirewallPolicyChangeProtection",
		"UpdateSubnetChangeProtection": "SubnetChangeProtection", "AssociateFirewallPolicy": "FirewallPolicyArn"} {
		Override(nfSvc, op, func(c *Call) (map[string]any, error) {
			r, err := nfFind(c, "Firewall")
			if err != nil {
				return nil, err
			}
			if err := nfToken(c, r); err != nil {
				return nil, err
			}
			if field == "FirewallPolicyArn" && !slices.ContainsFunc(c.List("FirewallPolicy"), func(p *Resource) bool { return p.ARN == str(c.In[field]) }) {
				return nil, c.NotFound("Firewall policy %s not found", str(c.In[field]))
			}
			r.Data[field], r.Data["UpdateToken"], r.Updated = c.In[field], core.UUID(), c.now()
			c.Put(r)
			return map[string]any{"FirewallArn": r.ARN, "FirewallName": r.Name, field: r.Data[field], "UpdateToken": r.Data["UpdateToken"]}, nil
		})
	}
	Override(nfSvc, "AssociateSubnets", func(c *Call) (map[string]any, error) {
		r, err := nfFind(c, "Firewall")
		if err != nil {
			return nil, err
		}
		if err := nfToken(c, r); err != nil {
			return nil, err
		}
		r.Data["SubnetMappings"] = append(asList(r.Data["SubnetMappings"]), asList(c.In["SubnetMappings"])...)
		r.Data["UpdateToken"] = core.UUID()
		c.Put(r)
		return map[string]any{"FirewallArn": r.ARN, "FirewallName": r.Name, "SubnetMappings": r.Data["SubnetMappings"], "UpdateToken": r.Data["UpdateToken"]}, nil
	})
	Override(nfSvc, "DisassociateSubnets", func(c *Call) (map[string]any, error) {
		r, err := nfFind(c, "Firewall")
		if err != nil {
			return nil, err
		}
		if err := nfToken(c, r); err != nil {
			return nil, err
		}
		r.Data["SubnetMappings"] = slices.DeleteFunc(slices.Clone(asList(r.Data["SubnetMappings"])), func(m any) bool {
			return slices.ContainsFunc(asList(c.In["SubnetIds"]), func(s any) bool { return s == m.(map[string]any)["SubnetId"] })
		})
		r.Data["UpdateToken"] = core.UUID()
		c.Put(r)
		return map[string]any{"FirewallArn": r.ARN, "FirewallName": r.Name, "SubnetMappings": r.Data["SubnetMappings"], "UpdateToken": r.Data["UpdateToken"]}, nil
	})

	// Logging belongs to a firewall.
	Override(nfSvc, "DescribeLoggingConfiguration", func(c *Call) (map[string]any, error) {
		r, err := nfFind(c, "Firewall")
		if err != nil {
			return nil, err
		}
		out := map[string]any{"FirewallArn": r.ARN, "LoggingConfiguration": r.Data["LoggingConfiguration"]}
		if out["LoggingConfiguration"] == nil {
			out["LoggingConfiguration"] = map[string]any{"LogDestinationConfigs": []any{}}
		}
		return out, nil
	})
	Override(nfSvc, "UpdateLoggingConfiguration", func(c *Call) (map[string]any, error) {
		r, err := nfFind(c, "Firewall")
		if err != nil {
			return nil, err
		}
		r.Data["LoggingConfiguration"], r.Updated = c.In["LoggingConfiguration"], c.now()
		c.Put(r)
		return map[string]any{"FirewallArn": r.ARN, "FirewallName": r.Name, "LoggingConfiguration": r.Data["LoggingConfiguration"]}, nil
	})
}
