package generic

// Resource Groups: a group's ARN is group/<name>. A second group with a name in use is a BadRequestException,
// the error the real service declares. The group's query is stored, and no resource is ever matched to it.
func init() {
	arnTemplates["resource-groups/Group"] = "group/{Name}"
	Override("resource-groups", "CreateGroup", func(c *Call) (map[string]any, error) {
		if c.Get("Group", str(c.In["Name"])) != nil {
			return nil, c.Err("BadRequestException", "Group %s already exists.", str(c.In["Name"]))
		}
		return c.Default()
	})
	for _, op := range []string{"GetGroupQuery", "UpdateGroupQuery"} {
		Override("resource-groups", op, func(c *Call) (map[string]any, error) {
			g := rgFind(c)
			if g == nil {
				return nil, c.Err("NotFoundException", "Group not found.")
			}
			if op == "UpdateGroupQuery" {
				g.Data["ResourceQuery"] = c.In["ResourceQuery"]
				g.Updated = c.now()
				c.Put(g)
			}
			return map[string]any{"GroupQuery": map[string]any{"GroupName": g.Name, "ResourceQuery": g.Data["ResourceQuery"]}}, nil
		})
	}
}

// A group has a configuration (its list of items) even when it was made with a query.
func init() {
	Override("resource-groups", "GetGroupConfiguration", func(c *Call) (map[string]any, error) {
		g := rgFind(c)
		if g == nil {
			return nil, c.Err("NotFoundException", "Group not found.")
		}
		items, ok := g.Data["Configuration"].([]any)
		if !ok {
			items = []any{}
		}
		return map[string]any{"GroupConfiguration": map[string]any{"Configuration": items, "Status": "UPDATE_COMPLETE"}}, nil
	})
}

// rgFind is the group a call names, by name or ARN, in Group (or the older GroupName).
func rgFind(c *Call) *Resource {
	id := str(c.In["Group"])
	if id == "" {
		id = str(c.In["GroupName"])
	}
	for _, r := range c.List("Group") {
		if r.Name == id || r.ARN == id {
			return r
		}
	}
	return nil
}
