package generic

import "github.com/nrndev/pogi/internal/core"

// WorkSpaces: DescribeTags reads the tags of an IP group (the Terraform provider reads them this way).
// UpdateRulesOfIpGroup replaces the whole list of rules of the group, as AWS does (the engine
// would merge it into the group's members otherwise).
func init() {
	Override("workspaces", "UpdateRulesOfIpGroup", func(c *Call) (map[string]any, error) {
		id := str(c.In["GroupId"])
		r := c.Get("IpGroup", id)
		if r == nil {
			return nil, c.NotFound("The IP group %s does not exist", id)
		}
		r.Data["UserRules"] = c.In["UserRules"]
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("workspaces", "DescribeTags", func(c *Call) (map[string]any, error) {
		id := str(c.In["ResourceId"])
		r := c.Get("IpGroup", id)
		if r == nil {
			return nil, c.NotFound("The resource %s does not exist", id)
		}
		var tags []any
		for _, k := range core.SortedKeys(r.Tags) {
			tags = append(tags, map[string]any{"Key": k, "Value": r.Tags[k]})
		}
		return map[string]any{"TagList": tags}, nil
	})
}
