package generic

import (
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Detective: one behaviour graph per account and region, with member accounts
// of their own (the engine would key every member by its graph alone).

func detectiveGraph(c *Call) (*Resource, error) {
	r := c.Get("Graph", str(c.In["GraphArn"]))
	if r == nil {
		return nil, c.NotFound("The requested resource %s was not found", str(c.In["GraphArn"]))
	}
	return r, nil
}

func detectiveMember(c *Call, graph, account string) *Resource {
	return c.Get("Member", graph, account)
}

func detectiveMemberOut(m *Resource) map[string]any {
	return map[string]any{"AccountId": m.Data["AccountId"], "EmailAddress": m.Data["EmailAddress"], "GraphArn": m.Data["GraphArn"], "AdministratorId": m.Account, "MasterId": m.Account,
		"Status": "INVITED", "InvitationType": "INVITATION", "InvitedTime": m.Created.Format(time.RFC3339Nano), "UpdatedTime": m.Updated.Format(time.RFC3339Nano)}
}

func init() {
	Override("detective", "CreateGraph", func(c *Call) (map[string]any, error) {
		if len(c.List("Graph")) > 0 {
			return nil, c.Exists("This account already has a behavior graph in this Region")
		}
		c.In["GraphArn"] = core.ARN("detective", c.Region, c.Account, "graph:"+randHex(32))
		return c.Default()
	})
	// A graph lists as its ARN and creation time.
	Override("detective", "ListGraphs", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, g := range c.List("Graph") {
			out = append(out, map[string]any{"Arn": g.ARN, "CreatedTime": g.Created.Format(time.RFC3339Nano)})
		}
		return map[string]any{"GraphList": out}, nil
	})
	Hook("detective", "DeleteGraph", func(c *Call) {
		for _, m := range c.List("Member") {
			if m.Data["GraphArn"] == c.Res.ARN {
				c.Svc.Delete(c.Account, c.Region, "Member", m.Key)
			}
		}
	})

	Override("detective", "CreateMembers", func(c *Call) (map[string]any, error) {
		g, err := detectiveGraph(c)
		if err != nil {
			return nil, err
		}
		out := []any{}
		for _, a := range asList(c.In["Accounts"]) {
			am := a.(map[string]any)
			now := c.now()
			m := &Resource{Type: "Member", Key: g.ARN + "/" + str(am["AccountId"]), IDs: map[string]string{"GraphArn": g.ARN, "AccountId": str(am["AccountId"])}, Name: str(am["AccountId"]),
				Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: map[string]any{"AccountId": am["AccountId"], "EmailAddress": am["EmailAddress"], "GraphArn": g.ARN}}
			c.Put(m)
			out = append(out, detectiveMemberOut(m))
		}
		return map[string]any{"Members": out, "UnprocessedAccounts": []any{}}, nil
	})
	Override("detective", "ListMembers", func(c *Call) (map[string]any, error) {
		g, err := detectiveGraph(c)
		if err != nil {
			return nil, err
		}
		out := []any{}
		for _, m := range c.List("Member") {
			if m.Data["GraphArn"] == g.ARN {
				out = append(out, detectiveMemberOut(m))
			}
		}
		return map[string]any{"MemberDetails": out}, nil
	})
	Override("detective", "GetMembers", func(c *Call) (map[string]any, error) {
		g, err := detectiveGraph(c)
		if err != nil {
			return nil, err
		}
		out, missing := []any{}, []any{}
		for _, id := range asList(c.In["AccountIds"]) {
			if m := detectiveMember(c, g.ARN, str(id)); m != nil {
				out = append(out, detectiveMemberOut(m))
			} else {
				missing = append(missing, map[string]any{"AccountId": id, "Reason": "Account is not a member of the behavior graph"})
			}
		}
		return map[string]any{"MemberDetails": out, "UnprocessedAccounts": missing}, nil
	})
	Override("detective", "DeleteMembers", func(c *Call) (map[string]any, error) {
		g, err := detectiveGraph(c)
		if err != nil {
			return nil, err
		}
		out, missing := []any{}, []any{}
		for _, id := range asList(c.In["AccountIds"]) {
			if c.Svc.Delete(c.Account, c.Region, "Member", g.ARN+"/"+str(id)) {
				out = append(out, id)
			} else {
				missing = append(missing, map[string]any{"AccountId": id, "Reason": "Account is not a member of the behavior graph"})
			}
		}
		return map[string]any{"AccountIds": out, "UnprocessedAccounts": missing}, nil
	})
}
