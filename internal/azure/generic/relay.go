package generic

import (
	"fmt"
	"net/http"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Relay: a namespace starts with the RootManageSharedAccessKey rule (azurerm reads its keys), rules answer
// listKeys. No listener or sender can connect.

func relayEndpoint(id arm.ID) string {
	return fmt.Sprintf("sb://%s.servicebus.windows.net/", id.Names[0])
}

func init() {
	const ns = "Microsoft.Relay/namespaces"
	Override(ns, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Standard", "tier": "Standard"}})
			p := armres.Props(r)
			if old, ok := s.Raw(id); ok {
				p["createdAt"] = armres.Props(old)["createdAt"]
			} else {
				p["createdAt"] = now()
			}
			p["updatedAt"] = now()
			p["status"] = "Active"
			p["metricId"] = fmt.Sprintf("%s:%s", id.Subscription, id.Name())
			p["serviceBusEndpoint"] = fmt.Sprintf("https://%s.servicebus.windows.net:443/", id.Name())
			return rootRule(s, c, id, "authorizationRules")
		},
	})
	listKeys := map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": listKeysRule(relayEndpoint)}
	// the client accepts only 200 for these
	Override(ns+"/authorizationRules", Hook{Actions: listKeys, Created: http.StatusOK})
	Override(ns+"/hybridConnections/authorizationRules", Hook{Actions: listKeys, Created: http.StatusOK})
	Override(ns+"/hybridConnections", Hook{Created: http.StatusOK, Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"requiresClientAuthorization": true, "userMetadata": ""})
		p["listenerCount"] = 0
		p["createdAt"], p["updatedAt"] = now(), now()
		if old, ok := s.Raw(id); ok {
			p["createdAt"] = armres.Props(old)["createdAt"]
		}
		return nil
	}})
}
