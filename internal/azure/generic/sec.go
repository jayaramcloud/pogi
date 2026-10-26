package generic

import (
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Security, identity and management services: Confidential Ledger, Trusted Signing, Purview,
// Defender for Cloud, Chaos Studio and the like. Nothing runs behind any of them. They store what a client sends,
// and fill the addresses and defaults that clients read back; no address answers.

// regionCode is the short region name that Trusted Signing puts in its address.
func regionCode(location string) string {
	l := strings.ToLower(strings.ReplaceAll(location, " ", ""))
	if c, ok := map[string]string{"eastus": "eus", "eastus2": "eus2", "westus": "wus", "westus2": "wus2", "westus3": "wus3",
		"centralus": "cus", "northeurope": "neu", "westeurope": "weu", "uksouth": "uks"}[l]; ok {
		return c
	}
	return l
}

// withProps sets the computed properties of a resource, those that clients read and a PUT cannot set.
func withProps(r map[string]any, computed map[string]any) {
	p := armres.Props(r)
	for k, v := range computed {
		p[k] = v
	}
	r["properties"] = p
}

// defenderSettings are the settings that every subscription has from the start, switched off.
var defenderSettings = map[string]string{"mcas": "DataExportSettings", "wdatp": "DataExportSettings",
	"wdatp_exclude_linux_public_preview": "DataExportSettings", "wdatp_unified_sso": "DataExportSettings", "sentinel": "AlertSyncSettings"}

func init() {
	Override("Microsoft.ConfidentialLedger/ledgers", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			withProps(r, map[string]any{"ledgerName": id.Name(), "ledgerUri": "https://" + id.Name() + ".confidential-ledger.azure.com",
				"identityServiceUri": "https://identity.confidential-ledger.core.azure.com/ledgerIdentity/" + id.Name()})
		},
	})
	Override("Microsoft.CodeSigning/codeSigningAccounts", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"sku": map[string]any{"name": "Basic"}})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			withProps(r, map[string]any{"accountUri": "https://" + regionCode(armres.Str(r, "location")) + ".codesigning.azure.net"})
		},
	})
	Override("Microsoft.Purview/accounts", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Standard", "capacity": 1}})
			armres.Defaults(armres.Props(r), map[string]any{"publicNetworkAccess": "Enabled", "managedResourceGroupName": "managed-rg-" + id.Name()})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			host := "https://" + id.Name() + ".purview.azure.com"
			p := armres.Props(r)
			rg, _ := p["managedResourceGroupName"].(string)
			withProps(r, map[string]any{"friendlyName": id.Name(),
				"endpoints":        map[string]any{"catalog": host + "/catalog", "guardian": host + "/guardian", "scan": host + "/scan"},
				"managedResources": map[string]any{"resourceGroup": "/subscriptions/" + id.Subscription + "/resourceGroups/" + rg}})
		},
	})
	// Advisor makes its own recommendations, which pogi never does: the list is always empty.
	Override("Microsoft.Advisor/configurations", Hook{Extra: func(*Service) []arm.Provider {
		return []arm.Provider{{Type: "Microsoft.Advisor/recommendations",
			Get:  func(c *core.Ctx, id arm.ID) (map[string]any, error) { return nil, arm.NotFound(id) },
			List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) { return nil, nil }}}
	}})
	// A domain service is ready when its replica sets run; the addresses of their domain controllers are made up.
	Override("Microsoft.AAD/domainServices", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			p := armres.Props(r)
			armres.Defaults(p, map[string]any{"sku": "Standard", "filteredSync": "Disabled",
				"tenantId": arm.TenantID, "deploymentId": guid(id.String())})
			sets, _ := p["replicaSets"].([]any)
			for _, v := range sets {
				if m, _ := v.(map[string]any); m != nil {
					armres.Defaults(m, map[string]any{"location": armres.Str(r, "location"), "serviceStatus": "Running",
						"replicaSetId":              guid(id.String() + armres.Str(m, "subnetId")),
						"domainControllerIpAddress": []any{"10.0.0.4", "10.0.0.5"}, "externalAccessIpAddress": ""})
				}
			}
			return nil
		},
	})
	// azurerm's client of application definitions does not poll a delete
	Override("Microsoft.Solutions/applicationDefinitions", Hook{SyncDelete: true})
	Override("Microsoft.Security/settings", Hook{
		Default: func(id arm.ID) map[string]any {
			kind, ok := defenderSettings[strings.ToLower(id.Name())]
			if !ok {
				return nil
			}
			return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "kind": kind, "properties": map[string]any{"enabled": false}}
		},
	})
	Override("Microsoft.Chaos/targets/capabilities", Hook{Created: http.StatusOK}) // the clients accept only 200
	Override("Microsoft.Chaos/targets", Hook{Created: http.StatusOK, Extra: func(*Service) []arm.Provider { return chaosCatalog() }})
}

// chaosTargetTypes are some of the target types of Chaos Studio, with the capabilities that each has. azurerm asks for
// them in the region before it makes a target or a capability, and refuses one that is not listed.
var chaosTargetTypes = []struct {
	name         string
	capabilities []string
}{
	{"Microsoft-Agent", []string{"CPUPressure-1.0", "PhysicalMemoryPressure-1.0", "VirtualMemoryPressure-1.0", "DiskIOPressure-1.0",
		"KillProcess-1.0", "StopService-1.0", "NetworkLatency-1.0", "NetworkDisconnect-1.0", "DnsFailure-1.0", "TimeChange-1.0"}},
	{"Microsoft-VirtualMachine", []string{"Shutdown-1.0"}},
	{"Microsoft-VirtualMachineScaleSet", []string{"Shutdown-1.0"}},
	{"Microsoft-KeyVault", []string{"DenyAccess-1.0"}},
	{"Microsoft-CosmosDB", []string{"Failover-1.0"}},
	{"Microsoft-NetworkSecurityGroup", []string{"SecurityRule-1.0"}},
}

// chaosCatalog serves the regions, the target types of a region and the capability types of a target type.
func chaosCatalog() []arm.Provider {
	const ns = "Microsoft.Chaos/locations"
	item := func(id arm.ID, typ, name string) map[string]any {
		return map[string]any{"id": id.String() + "/" + name, "name": name, "type": typ, "properties": map[string]any{"displayName": name}}
	}
	find := func(name string) (capabilities []string, ok bool) {
		for _, t := range chaosTargetTypes {
			if strings.EqualFold(t.name, name) {
				return t.capabilities, true
			}
		}
		return nil, false
	}
	return []arm.Provider{
		{Type: ns + "/targetTypes",
			Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
				if _, ok := find(id.Name()); !ok {
					return nil, arm.NotFound(id)
				}
				return item(id.Parent(), id.Type(), id.Name()), nil
			},
			List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
				var out []map[string]any
				for _, t := range chaosTargetTypes {
					out = append(out, item(parent, ns+"/targetTypes", t.name))
				}
				return out, nil
			}},
		{Type: ns + "/targetTypes/capabilityTypes",
			Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
				caps, _ := find(id.Names[1])
				if !slices.Contains(caps, id.Name()) {
					return nil, arm.NotFound(id)
				}
				return item(id.Parent(), id.Type(), id.Name()), nil
			},
			List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
				caps, _ := find(parent.Names[1])
				var out []map[string]any
				for _, name := range caps {
					out = append(out, item(parent, ns+"/targetTypes/capabilityTypes", name))
				}
				return out, nil
			}},
	}
}
