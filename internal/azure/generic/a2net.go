package generic

import (
	"fmt"
	"net/url"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Networking, second batch: Network Watcher, DDoS Protection, Virtual Network Manager, Route Server,
// ExpressRoute, VPN Gateway, Web Application Firewall policies and Application Gateway for Containers.
// The engine stores and reads them back; no traffic flows and no tunnel, circuit or peering comes up.

func init() {
	// A connection's shared key is read back from its own path: GET .../connections/<name>/sharedkey.
	Override("Microsoft.Network/connections", Hook{Extra: func(s *Service) []arm.Provider {
		return []arm.Provider{{Type: "Microsoft.Network/connections/sharedkey", Doc: func(c *core.Ctx, parent arm.ID, _ url.Values) (any, error) {
			conn, ok := s.Raw(parent)
			if !ok {
				return nil, core.Errorf(404, "ResourceNotFound", "connection %s was not found", parent.Name())
			}
			return map[string]any{"value": armres.Props(conn)["sharedKey"]}, nil
		}}}
	}})

	// VPN Gateway: the IP configurations get their ids, as Azure gives them.
	Override("Microsoft.Network/virtualNetworkGateways", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		inlineIDs(armres.Props(r), id)
		return nil
	}})

	// ExpressRoute: a circuit has its service key and waits for the provider, as a new one does.
	Override("Microsoft.Network/expressRouteCircuits", Hook{View: func(s *Service, id arm.ID, r map[string]any) {
		armres.Defaults(armres.Props(r), map[string]any{"serviceKey": guid("er/" + id.String()), "serviceProviderProvisioningState": "NotProvisioned"})
	}})

	// Application Gateway for Containers: Terraform reads the frontend's host name.
	Override("Microsoft.ServiceNetworking/trafficControllers/frontends", Hook{View: func(s *Service, id arm.ID, r map[string]any) {
		armres.Props(r)["fqdn"] = fmt.Sprintf("%s.%s.alb.azure.com", id.Name(), guid("alb/" + id.String())[:8])
	}})
}
