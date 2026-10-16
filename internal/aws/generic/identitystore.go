package generic

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Identity Store: the directory behind IAM Identity Center. Users and groups
// need unique names, lookups by name, filters and attribute-path updates.

func init() {
	// Ids look like <10 hex digits>-<uuid>; the engine would pick another shape the pattern allows.
	newID := func() string { return randHex(10) + "-" + core.UUID() }
	unique := func(typ, attr string) func(*Call) (map[string]any, error) {
		return func(c *Call) (map[string]any, error) {
			v := str(c.In[attr])
			for _, r := range c.List(typ) {
				if r.IDs["IdentityStoreId"] == str(c.In["IdentityStoreId"]) && strings.EqualFold(str(r.Data[attr]), v) {
					return nil, c.Exists("%s %s already exists", attr, v)
				}
			}
			c.In[typ+"Id"] = newID()
			return c.Default()
		}
	}
	Override("identitystore", "CreateUser", unique("User", "UserName"))
	Override("identitystore", "CreateGroup", unique("Group", "DisplayName"))
	Override("identitystore", "CreateGroupMembership", func(c *Call) (map[string]any, error) {
		c.In["MembershipId"] = newID()
		return c.Default()
	})

	for _, p := range []struct{ typ, attr, op string }{{"User", "UserName", "GetUserId"}, {"Group", "DisplayName", "GetGroupId"}} {
		Override("identitystore", p.op, func(c *Call) (map[string]any, error) {
			alt, _ := c.In["AlternateIdentifier"].(map[string]any)
			attr, _ := alt["UniqueAttribute"].(map[string]any)
			path, val := str(attr["AttributePath"]), attr["AttributeValue"]
			if path == "" || !strings.EqualFold(path, p.attr) {
				return nil, c.Invalid("Unsupported attribute path %q", path)
			}
			for _, r := range c.List(p.typ) {
				if r.IDs["IdentityStoreId"] == str(c.In["IdentityStoreId"]) && str(r.Data[p.attr]) == str(val) {
					return map[string]any{"IdentityStoreId": r.IDs["IdentityStoreId"], p.typ + "Id": r.ID}, nil
				}
			}
			return nil, c.NotFound("%s %s not found", p.typ, str(val))
		})
	}
	Override("identitystore", "GetGroupMembershipId", func(c *Call) (map[string]any, error) {
		mid, _ := c.In["MemberId"].(map[string]any)
		for _, r := range c.List("GroupMembership") {
			if m, _ := r.Data["MemberId"].(map[string]any); str(r.Data["GroupId"]) == str(c.In["GroupId"]) && str(m["UserId"]) == str(mid["UserId"]) {
				return map[string]any{"IdentityStoreId": r.IDs["IdentityStoreId"], "MembershipId": r.ID}, nil
			}
		}
		return nil, c.NotFound("membership not found")
	})
	Override("identitystore", "IsMemberInGroups", func(c *Call) (map[string]any, error) {
		mid, _ := c.In["MemberId"].(map[string]any)
		var res []any
		for _, g := range asList(c.In["GroupIds"]) {
			in := false
			for _, r := range c.List("GroupMembership") {
				if m, _ := r.Data["MemberId"].(map[string]any); str(r.Data["GroupId"]) == str(g) && str(m["UserId"]) == str(mid["UserId"]) {
					in = true
				}
			}
			res = append(res, map[string]any{"GroupId": g, "MemberId": mid, "MembershipExists": in})
		}
		return map[string]any{"Results": res}, nil
	})
	Override("identitystore", "ListGroupMembershipsForMember", func(c *Call) (map[string]any, error) {
		mid, _ := c.In["MemberId"].(map[string]any)
		var out []any
		for _, r := range c.List("GroupMembership") {
			if m, _ := r.Data["MemberId"].(map[string]any); r.IDs["IdentityStoreId"] == str(c.In["IdentityStoreId"]) && str(m["UserId"]) == str(mid["UserId"]) {
				out = append(out, map[string]any{"IdentityStoreId": r.IDs["IdentityStoreId"], "MembershipId": r.ID, "GroupId": r.Data["GroupId"], "MemberId": m})
			}
		}
		return map[string]any{"GroupMemberships": out}, nil
	})

	// The deprecated Filters member narrows a list to one attribute value.
	for op, list := range map[string]string{"ListUsers": "Users", "ListGroups": "Groups"} {
		Override("identitystore", op, func(c *Call) (map[string]any, error) {
			out, err := c.Default()
			if err != nil {
				return nil, err
			}
			for _, f := range asList(c.In["Filters"]) {
				fm, _ := f.(map[string]any)
				var keep []any
				for _, it := range asList(out[list]) {
					im, _ := it.(map[string]any)
					for k, v := range im {
						if strings.EqualFold(k, str(fm["AttributePath"])) && str(v) == str(fm["AttributeValue"]) {
							keep = append(keep, it)
						}
					}
				}
				out[list] = keep
			}
			return out, nil
		})
	}

	// Updates are lists of attribute paths ("displayName", "name.givenName") and values.
	for _, typ := range []string{"User", "Group"} {
		Override("identitystore", "Update"+typ, func(c *Call) (map[string]any, error) {
			r := c.Get(typ, str(c.In["IdentityStoreId"]), str(c.In[typ+"Id"]))
			if r == nil {
				return nil, c.NotFound("%s %s not found", typ, str(c.In[typ+"Id"]))
			}
			for _, o := range asList(c.In["Operations"]) {
				om, _ := o.(map[string]any)
				setPath(r.Data, str(om["AttributePath"]), om["AttributeValue"])
			}
			r.Updated = c.now()
			c.Put(r)
			return map[string]any{}, nil
		})
	}

	Override("identitystore", "ListIdentityStores", func(c *Call) (map[string]any, error) {
		in := ssoInstance(c)
		return map[string]any{"IdentityStores": []any{map[string]any{"IdentityStoreId": in.Data["IdentityStoreId"], "Arn": core.ARN("identitystore", c.Region, c.Account, "identitystore/"+str(in.Data["IdentityStoreId"]))}}}, nil
	})
	Override("identitystore", "DescribeIdentityStore", func(c *Call) (map[string]any, error) {
		return map[string]any{"IdentityStoreId": c.In["IdentityStoreId"], "IdentityStoreArn": core.ARN("identitystore", c.Region, c.Account, "identitystore/"+str(c.In["IdentityStoreId"]))}, nil
	})
}

// setPath sets a dotted attribute path ("name.givenName") in stored data, whose keys are capitalised member names.
func setPath(data map[string]any, path string, v any) {
	var key string
	for i, p := range strings.Split(path, ".") {
		key = strings.ToUpper(p[:1]) + p[1:]
		if i < strings.Count(path, ".") {
			sub, _ := data[key].(map[string]any)
			if sub == nil {
				sub = map[string]any{}
				data[key] = sub
			}
			data = sub
		}
	}
	data[key] = v
}
