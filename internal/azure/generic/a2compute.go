package generic

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Compute and workplace services (group a2-compute): Azure Arc, Azure Local, Azure VMware Solution, Red Hat
// OpenShift, Fleet Manager, Service Fabric, Virtual Desktop, DevTest Labs, Dev Box, Load Testing, Managed
// DevOps Pools and Center for SAP solutions. These hooks fill the host names, endpoints and states that
// Terraform and the SDKs read back. Nothing runs behind a resource.

// computed sets read-only properties on every read: values that are not the client's own.
func computed(r map[string]any, kv map[string]any) {
	p := armres.Props(r)
	for k, v := range kv {
		p[k] = v
	}
	r["properties"] = p
}

func init() {
	Override("Microsoft.HybridCompute/privateLinkScopes", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"publicNetworkAccess": "Disabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"privateLinkScopeId": guid(id.String()), "privateEndpointConnections": []any{}})
		},
	})

	Override("Microsoft.AzureStackHCI/clusters", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"status": "NotYetRegistered", "cloudId": guid(id.String()),
				"resourceProviderObjectId": guid(id.String() + "/rp"), "serviceEndpoint": "https://dp.stackhci.azure.com",
				"trialDaysRemaining": 60, "billingModel": "Trial", "registrationTimestamp": "2026-01-01T00:00:00Z",
				"reportedProperties": map[string]any{"clusterName": id.Name(), "clusterVersion": "10.2402.0.0", "nodes": []any{}}})
		},
	})

	Override("Microsoft.AVS/privateClouds", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"internet": "Disabled", "availability": map[string]any{"strategy": "SingleZone"}})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			h := hash8(id.String())
			computed(r, map[string]any{
				"endpoints": map[string]any{"nsxtManager": "https://10.0.0.2/", "vcsa": "https://10.0.0.3/", "hcxCloudManager": "https://10.0.0.9/"},
				"circuit": map[string]any{"primarySubnet": "192.168.48.0/30", "secondarySubnet": "192.168.48.4/30",
					"expressRouteID": id.String() + "/circuit", "expressRoutePrivatePeeringID": id.String() + "/circuit/peerings/AzurePrivatePeering"},
				"nsxtCertificateThumbprint": strings.ToUpper(h + h + h + h + h), "vcenterCertificateThumbprint": strings.ToUpper(h + h + h + h + h),
				"provisioningNetwork": "192.168.50.0/24", "vmotionNetwork": "192.168.51.0/24", "hcxCloudManagerEndpoint": "https://10.0.0.9/",
			})
			if p := armres.Props(r); p["managementCluster"] != nil {
				mc, _ := p["managementCluster"].(map[string]any)
				armres.Defaults(mc, map[string]any{"clusterId": 1, "hosts": []any{}})
			}
		},
	})
	Override("Microsoft.AVS/privateClouds/clusters", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"clusterId": 2, "hosts": []any{}})
		},
	})
	Override("Microsoft.AVS/privateClouds/authorizations", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"expressRouteAuthorizationId": id.String() + "/expressRouteAuthorization",
				"expressRouteAuthorizationKey": guid(id.String() + "/key"), "expressRouteId": id.Parent().String() + "/circuit"})
		},
	})

	Override("Microsoft.RedHatOpenShift/openShiftClusters", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			h := hash8(id.String())
			api, _ := p["apiserverProfile"].(map[string]any)
			if api == nil {
				api = map[string]any{}
			}
			armres.Defaults(api, map[string]any{"visibility": "Public"})
			api["url"], api["ip"] = fmt.Sprintf("https://api.%s.%s.aroapp.io:6443/", id.Name(), h), "20.0.0.10"
			p["apiserverProfile"] = api
			p["consoleProfile"] = map[string]any{"url": fmt.Sprintf("https://console-openshift-console.apps.%s.%s.aroapp.io/", id.Name(), h)}
			ing, _ := p["ingressProfiles"].([]any)
			for i, v := range ing {
				if m, _ := v.(map[string]any); m != nil {
					m["ip"] = fmt.Sprintf("20.0.0.%d", 20+i)
				}
			}
			r["properties"] = p
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"listCredentials": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return map[string]any{"kubeadminUsername": "kubeadmin", "kubeadminPassword": armres.Key(id.String() + "/kubeadmin")[:20]}, nil
			},
			"listAdminCredentials": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				kc := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: %s\n  cluster:\n    server: https://api.%s.aroapp.io:6443/\n", id.Name(), id.Name())
				return map[string]any{"kubeconfig": base64.StdEncoding.EncodeToString([]byte(kc))}, nil
			},
		},
	})

	Override("Microsoft.ServiceFabric/clusters", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"upgradeMode": "Automatic", "clusterState": "Ready", "addOnFeatures": []any{}})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"clusterId": guid(id.String()), "clusterState": "Ready",
				"clusterEndpoint": fmt.Sprintf("https://%s.%s.servicefabric.azure.com", id.Name(), armres.Str(r, "location"))})
		},
	})

	Override("Microsoft.DevTestLab/labs", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			sa := func(n string) string {
				return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Storage/storageAccounts/%s", id.Subscription, id.ResourceGroup, n)
			}
			computed(r, map[string]any{"uniqueIdentifier": guid(id.String()), "createdDate": "2026-01-01T00:00:00Z",
				"labStorageType": "Premium", "vaultName": fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.KeyVault/vaults/%s", id.Subscription, id.ResourceGroup, "dtl"+hash8(id.String())),
				"artifactsStorageAccount": sa("a" + hash8(id.String())), "defaultStorageAccount": sa("d" + hash8(id.String())),
				"defaultPremiumStorageAccount": sa("p" + hash8(id.String())), "premiumDataDiskStorageAccount": sa("q" + hash8(id.String()))})
		},
	})
	Override("Microsoft.DevTestLab/labs/virtualnetworks", Hook{
		Untracked: true, // a lab's network has no location of its own
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"uniqueIdentifier": guid(id.String()), "createdDate": "2026-01-01T00:00:00Z"})
		},
	})

	Override("Microsoft.DevCenter/devcenters", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"devCenterUri": fmt.Sprintf("https://%s-%s.%s.devcenter.azure.com", guid(id.String()), id.Name(), armres.Str(r, "location"))})
		},
	})
	Override("Microsoft.DevCenter/projects", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"devCenterUri": fmt.Sprintf("https://%s-%s.%s.devcenter.azure.com", guid(id.String()), id.Name(), armres.Str(r, "location"))})
		},
	})
	Override("Microsoft.DevCenter/networkConnections", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"healthCheckStatus": "Passed"})
		},
	})
	Override("Microsoft.DevCenter/devcenters/attachednetworks", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"healthCheckStatus": "Passed"})
		},
	})
	Override("Microsoft.DevCenter/projects/pools", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"devBoxDefinitionType": "Reference", "licenseType": "Windows_Client",
				"singleSignOnStatus": "Disabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) { computed(r, map[string]any{"healthStatus": "Healthy"}) },
	})
	Override("Microsoft.DevCenter/devcenters/devboxdefinitions", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"hibernateSupport": "Disabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"validationStatus": "Succeeded"})
		},
	})

	Override("Microsoft.LoadTestService/loadTests", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			computed(r, map[string]any{"dataPlaneURI": fmt.Sprintf("%s.%s.cnt-prod.loadtesting.azure.com", guid(id.String()), armres.Str(r, "location"))})
		},
	})
}
