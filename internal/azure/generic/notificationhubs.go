package generic

import (
	"fmt"
	"net/http"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Notification Hubs: a namespace starts with the RootManageSharedAccessKey rule, rules answer listKeys. No
// notification is sent.

func nhEndpoint(id arm.ID) string { return fmt.Sprintf("sb://%s.servicebus.windows.net/", id.Names[0]) }

func init() {
	const ns = "Microsoft.NotificationHubs/namespaces"
	Override(ns, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Free"}})
			p := armres.Props(r)
			armres.Defaults(p, map[string]any{"namespaceType": "NotificationHub", "enabled": true, "status": "Created", "createdAt": now(), "critical": false})
			p["name"] = id.Name()
			p["updatedAt"] = now()
			p["serviceBusEndpoint"] = fmt.Sprintf("https://%s.servicebus.windows.net:443/", id.Name())
			return rootRule(s, c, id, "authorizationRules")
		},
	})
	listKeys := map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": listKeysRule(nhEndpoint)}
	Override(ns+"/authorizationRules", Hook{Actions: listKeys, Untracked: true, Created: http.StatusOK})
	Override(ns+"/notificationHubs/authorizationRules", Hook{Actions: listKeys, Untracked: true, Created: http.StatusOK})
	Override(ns+"/notificationHubs", Hook{
		// the push service credentials of a hub: none are set
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"pnsCredentials": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return map[string]any{"properties": map[string]any{}}, nil
			}},
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Props(r)["name"] = id.Name()
			return nil
		}})
}
