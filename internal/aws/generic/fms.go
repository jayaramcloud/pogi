package generic

import "github.com/nrndev/pogi/internal/core"

// Firewall Manager: policies, lists and resource sets are the engine's. A policy
// gets an id when it is first put (the engine only finds one inside the Policy
// structure), and the administrator account is one setting per account.

func init() {
	Override("fms", "PutPolicy", func(c *Call) (map[string]any, error) {
		p, _ := c.In["Policy"].(map[string]any)
		if p != nil {
			if str(p["PolicyId"]) == "" {
				p["PolicyId"] = core.UUID()
			}
			p["PolicyUpdateToken"] = "1"
		}
		return c.Default()
	})
	admin := func(c *Call) (map[string]any, error) {
		a := str(c.In["AdminAccount"])
		if a == "" {
			return nil, c.Invalid("AdminAccount is required")
		}
		kSave(c, "AdminAccount", map[string]any{"AdminAccount": a})
		return map[string]any{}, nil
	}
	Override("fms", "AssociateAdminAccount", admin)
	Override("fms", "PutAdminAccount", admin)
	Override("fms", "GetAdminAccount", func(c *Call) (map[string]any, error) {
		r := kSetting(c, "AdminAccount")
		if r == nil {
			return nil, c.NotFound("No administrator account is set for account %s", c.Account)
		}
		return map[string]any{"AdminAccount": r.Data["AdminAccount"], "RoleStatus": "READY"}, nil
	})
	Override("fms", "DisassociateAdminAccount", func(c *Call) (map[string]any, error) {
		if !c.Svc.Delete(c.Account, c.Region, "AdminAccount", "default") {
			return nil, c.NotFound("No administrator account is set for account %s", c.Account)
		}
		return map[string]any{}, nil
	})
}
