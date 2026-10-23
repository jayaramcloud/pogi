package generic

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Networking services (Front Door, Application Gateway, Load Balancer, Firewall, Bastion, Traffic Manager,
// Private Link, Virtual WAN). The engine stores and reads them back; the hooks below fill what Azure fills
// and Terraform reads: ids of inline parts, host names, states. No traffic flows through any of them.

func init() {
	// Front Door: Terraform waits for the endpoint's host name and a domain's validation token.
	Override("Microsoft.Cdn/profiles", Hook{View: func(s *Service, id arm.ID, r map[string]any) {
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"originResponseTimeoutSeconds": 60})
		p["frontDoorId"], p["resourceState"] = guid("fd/"+id.String()), "Active"
	}})
	Override("Microsoft.Cdn/profiles/afdEndpoints", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"enabledState": "Enabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			armres.Props(r)["hostName"] = fmt.Sprintf("%s-%s.z01.azurefd.net", id.Name(), guid("afd/" + id.String())[:8])
		}})
	Override("Microsoft.Cdn/profiles/customDomains", Hook{View: func(s *Service, id arm.ID, r map[string]any) {
		p := armres.Props(r)
		p["domainValidationState"] = "Approved"
		p["validationProperties"] = map[string]any{"validationToken": guid("dv/" + id.String()), "expirationDate": "2099-01-01T00:00:00Z"}
	}})

	// Application Gateway: inline parts get ids, as Azure gives them (Terraform builds references from them).
	Override("Microsoft.Network/applicationGateways", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			inlineIDs(armres.Props(r), id)
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			p["operationalState"], p["resourceGuid"] = "Running", guid("agw/"+id.String())
		}})

	// Load Balancer: a backend pool is a resource of its own that the balancer lists inline. Pools sent
	// inline become resources, so that both ways of making one give the same answer.
	Override("Microsoft.Network/loadBalancers", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			p := armres.Props(r)
			inlineIDs(p, id)
			pools, _ := p["backendAddressPools"].([]any)
			delete(p, "backendAddressPools")
			for _, x := range pools {
				pool, _ := x.(map[string]any)
				name, _ := pool["name"].(string)
				if name == "" {
					continue
				}
				if pid := id.Child("backendAddressPools", name); !s.Has(pid) {
					if err := s.Create(c, pid, map[string]any{"properties": pool["properties"]}); err != nil {
						return err
					}
				}
			}
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			p["resourceGuid"] = guid("lb/" + id.String())
			// Azure always returns these lists, empty when there is nothing; azurerm appends to them without checking.
			for _, list := range []string{"frontendIPConfigurations", "probes", "loadBalancingRules", "inboundNatRules", "inboundNatPools", "outboundRules"} {
				if p[list] == nil {
					p[list] = []any{}
				}
			}
			pools := []any{}
			for _, ch := range s.Children(id) {
				if strings.EqualFold(armres.Str(ch, "type"), "Microsoft.Network/loadBalancers/backendAddressPools") {
					pools = append(pools, ch)
				}
			}
			p["backendAddressPools"] = pools
		}})

	// Firewall: each IP configuration gets a private address, as the firewall takes one from its subnet.
	Override("Microsoft.Network/azureFirewalls", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		p := armres.Props(r)
		inlineIDs(p, id)
		cfgs, _ := p["ipConfigurations"].([]any)
		for i, x := range cfgs {
			if cfg, _ := x.(map[string]any); cfg != nil {
				cp := armres.Props(cfg)
				armres.Defaults(cp, map[string]any{"privateIPAddress": fmt.Sprintf("10.0.1.%d", 4+i)})
				cfg["properties"] = cp
			}
		}
		return nil
	}})
	Override("Microsoft.Network/firewallPolicies", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		armres.Defaults(armres.Props(r), map[string]any{"threatIntelMode": "Alert"})
		return nil
	}})

	// Bastion: Terraform reads the DNS name.
	Override("Microsoft.Network/bastionHosts", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			inlineIDs(armres.Props(r), id)
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			armres.Props(r)["dnsName"] = "bst-" + guid("bst/"+id.String()) + ".bastion.azure.com"
		}})

	// Traffic Manager: the profile's FQDN, its endpoints inline, and each endpoint's status.
	Override("Microsoft.Network/trafficmanagerprofiles", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"profileStatus": "Enabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			if dns, _ := p["dnsConfig"].(map[string]any); dns != nil {
				dns["fqdn"] = fmt.Sprintf("%v.trafficmanager.net", dns["relativeName"])
			}
			p["endpoints"] = s.Children(id)
		}})
	for _, t := range []string{"AzureEndpoints", "ExternalEndpoints", "NestedEndpoints"} {
		Override("Microsoft.Network/trafficmanagerprofiles/"+t, Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"endpointStatus": "Enabled", "endpointMonitorStatus": "Online"})
			return nil
		}})
	}

	// Private Link: a private endpoint's connections are approved when they point at something of the user's
	// (automatic) and pending when they are manual; a service has its alias.
	Override("Microsoft.Network/privateEndpoints", Hook{Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		p := armres.Props(r)
		for key, status := range map[string]string{"privateLinkServiceConnections": "Approved", "manualPrivateLinkServiceConnections": "Pending"} {
			conns, _ := p[key].([]any)
			for _, x := range conns {
				if cn, _ := x.(map[string]any); cn != nil {
					cp := armres.Defaults(armres.Props(cn), map[string]any{"privateLinkServiceConnectionState": map[string]any{"status": status, "description": "", "actionsRequired": "None"}})
					cn["properties"] = cp
				}
			}
		}
		inlineIDs(p, id)
		return nil
	}})
	Override("Microsoft.Network/privateLinkServices", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			inlineIDs(armres.Props(r), id)
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			armres.Props(r)["alias"] = fmt.Sprintf("%s.%s.%s.azure.privatelinkservice", id.Name(), guid("pls/" + id.String())[:8], armres.Str(r, "location"))
		}})

	Override("Microsoft.Network/virtualWans", Hook{View: func(s *Service, id arm.ID, r map[string]any) {
		armres.Defaults(armres.Props(r), map[string]any{"office365LocalBreakoutCategory": "None"})
	}})
	// Virtual WAN: a hub reaches routingState Provisioned at once and has Azure's router ASN.
	Override("Microsoft.Network/virtualHubs", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"virtualRouterAsn": 65515})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) { armres.Props(r)["routingState"] = "Provisioned" }})
}

// inlineIDs gives every named part of a list property (frontendPorts, probes, ...) the id Azure gives it,
// <resource id>/<list>/<name>, when the client sent none.
func inlineIDs(p map[string]any, id arm.ID) {
	for list, v := range p {
		items, _ := v.([]any)
		for _, x := range items {
			it, _ := x.(map[string]any)
			if name, _ := it["name"].(string); name != "" && it["id"] == nil {
				it["id"] = id.String() + "/" + list + "/" + name
			}
		}
	}
}

// guid is a UUID made from a seed, so a resource shows the same one on every read.
func guid(seed string) string {
	h := fmt.Sprintf("%x", sha256.Sum256([]byte("pogi:net/"+seed)))
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
