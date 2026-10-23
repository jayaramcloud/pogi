package generic

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Static Web Apps: a site has a default host name and an API key (the deployment token). No site is served and
// no build runs.

func init() {
	Override("Microsoft.Web/staticSites", Hook{
		Created: http.StatusAccepted,
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			if armres.Str(r, "location") == "" { // the spec does not mark the type as tracked, Azure still wants a location
				return core.Errorf(http.StatusBadRequest, "LocationRequired", "The location property is required for this definition.")
			}
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Free", "tier": "Free"}, "kind": "app"})
			armres.Defaults(armres.Props(r), map[string]any{"stagingEnvironmentPolicy": "Enabled", "allowConfigFileUpdates": true,
				"provider": "None", "enterpriseGradeCdnStatus": "Disabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			h := hash8(id.String())
			n, _ := strconv.ParseUint(h[:1], 16, 8)
			p["defaultHostname"] = fmt.Sprintf("%s-%s.%d.azurestaticapps.net", id.Name(), h, 1+n%5)
			p["customDomains"] = []any{}
			r["properties"] = p
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"listSecrets": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				k, _ := accessKeys(id)
				return map[string]any{"properties": map[string]any{"apiKey": k[:40]}}, nil
			},
			"listAppSettings": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return settings(s, id, "appsettings"), nil
			},
			"listFunctionAppSettings": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return settings(s, id, "functionappsettings"), nil
			},
		},
	})
	// The settings are the user's own pairs: nothing of ours goes among them.
	Override("Microsoft.Web/staticSites/config", Hook{Created: http.StatusOK, View: func(s *Service, id arm.ID, r map[string]any) {
		delete(armres.Props(r), "provisioningState")
	}})
}

// settings answers listAppSettings and listFunctionAppSettings with what config/<name> holds.
func settings(s *Service, id arm.ID, name string) map[string]any {
	out := map[string]any{}
	if cfg, ok := s.Raw(id.Child("config", name)); ok {
		for k, v := range armres.Props(cfg) {
			if k != "provisioningState" {
				out[k] = v
			}
		}
	}
	return map[string]any{"properties": out}
}
