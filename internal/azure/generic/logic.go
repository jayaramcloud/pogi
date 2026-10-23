package generic

import (
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Logic Apps: a workflow is stored with its definition and state. Triggers never fire and no run starts.

func init() {
	Override("Microsoft.Logic/workflows", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"state": "Enabled", "definition": map[string]any{}})
		// Azure returns the definition's parameters as an object, never null; azurerm reads it as one.
		if def, _ := p["definition"].(map[string]any); def != nil && def["parameters"] == nil {
			def["parameters"] = map[string]any{}
		}
		p["createdTime"], p["changedTime"] = now(), now()
		if old, ok := s.Raw(id); ok {
			// Azure returns the definition's parameters as an object, never null; azurerm reads it as one.
			if def, _ := p["definition"].(map[string]any); def != nil && def["parameters"] == nil {
				def["parameters"] = map[string]any{}
			}
			p["createdTime"] = armres.Props(old)["createdTime"]
		}
		p["version"] = core.RandHex(16) // a new version for every change
		return nil
	}})
}
