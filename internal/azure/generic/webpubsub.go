package generic

import (
	"fmt"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Web PubSub: a service has a host name and access keys; hubs hold their settings. No client can connect.

func init() {
	Override("Microsoft.SignalRService/webPubSub", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Standard_S1", "tier": "Standard", "capacity": 1}})
			armres.Defaults(armres.Props(r), map[string]any{"publicNetworkAccess": "Enabled", "disableLocalAuth": false,
				"disableAadAuth": false, "tls": map[string]any{"clientCertEnabled": false}})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			p["hostName"], p["hostNamePrefix"] = id.Name()+".webpubsub.azure.com", id.Name()
			p["publicPort"], p["serverPort"], p["version"] = 443, 443, "1.0"
			r["properties"] = p
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"listKeys": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				p, q := accessKeys(id)
				cs := func(k string) string {
					return fmt.Sprintf("Endpoint=https://%s.webpubsub.azure.com;AccessKey=%s;Version=1.0;", id.Name(), k)
				}
				return map[string]any{"primaryKey": p, "secondaryKey": q, "primaryConnectionString": cs(p), "secondaryConnectionString": cs(q)}, nil
			},
		},
	})
	Override("Microsoft.SignalRService/webPubSub/hubs", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		armres.Defaults(armres.Props(r), map[string]any{"anonymousConnectPolicy": "deny", "eventHandlers": []any{}})
		return nil
	}})
}
