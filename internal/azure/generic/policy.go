package generic

import (
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Policy: definitions at subscription scope, assignments at any scope. Nothing is evaluated: no resource is
// ever denied or reported non-compliant.

func init() {
	Override("Microsoft.Authorization/policyDefinitions", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		armres.Defaults(armres.Props(r), map[string]any{"policyType": "Custom", "mode": "Indexed"})
		return nil
	}})
	Override("Microsoft.Authorization/policyAssignments", Hook{View: func(s *Service, id arm.ID, r map[string]any) {
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"enforcementMode": "Default"})
		p["scope"] = id.String()
		if i := strings.LastIndex(strings.ToLower(id.String()), "/providers/microsoft.authorization/policyassignments/"); i >= 0 {
			p["scope"] = id.String()[:i]
		}
	}})
}
