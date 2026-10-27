package generic

import (
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Policy: definitions at subscription scope, assignments at any scope. The Resource Manager evaluates them
// (internal/azure/arm/policy.go); here a definition's rule and an assignment's parameters are checked when stored.

func init() {
	Override("Microsoft.Authorization/policyDefinitions", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"policyType": "Custom", "mode": "Indexed"})
		return arm.CheckPolicyRule(p["policyRule"])
	}})
	Override("Microsoft.Authorization/policyAssignments", Hook{Put: checkAssignment, View: func(s *Service, id arm.ID, r map[string]any) {
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"enforcementMode": "Default"})
		p["scope"] = id.String()
		if i := strings.LastIndex(strings.ToLower(id.String()), "/providers/microsoft.authorization/policyassignments/"); i >= 0 {
			p["scope"] = id.String()[:i]
		}
	}})
}

// checkAssignment refuses an assignment that leaves out a parameter its definition needs (one with no default
// value) or names one the definition does not have. A definition pogi does not hold (a built-in one) is not checked.
func checkAssignment(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	did, ok := arm.ParseID(armres.Str(p, "policyDefinitionId"))
	def, found := s.Raw(did)
	if !ok || !found || !strings.EqualFold(did.Type(), "Microsoft.Authorization/policyDefinitions") {
		return nil
	}
	want, _ := armres.Props(def)["parameters"].(map[string]any)
	got, _ := p["parameters"].(map[string]any)
	has := func(m map[string]any, name string) (any, bool) {
		for k, v := range m {
			if strings.EqualFold(k, name) {
				return v, true
			}
		}
		return nil, false
	}
	for name := range got {
		if _, ok := has(want, name); !ok {
			return core.Errorf(http.StatusBadRequest, "UndefinedPolicyParameter", "The policy assignment '%s' has the parameter '%s', which its definition '%s' does not define.", id.Name(), name, did.Name())
		}
	}
	for _, name := range core.SortedKeys(want) {
		d, _ := want[name].(map[string]any)
		if _, hasDefault := d["defaultValue"]; hasDefault {
			continue
		}
		if _, ok := has(got, name); !ok {
			return core.Errorf(http.StatusBadRequest, "MissingPolicyParameter", "The policy assignment '%s' is missing the parameter '%s' that its definition '%s' needs.", id.Name(), name, did.Name())
		}
	}
	return nil
}
