package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/servicenetworking/armservicenetworking"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

// The networking products of the second batch, each driven with Microsoft's own client: the main resource is
// made and read back, then the ways it fails.
func TestNetworkingBatch2(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "aznetworkwatcher", "azddos", "aznetworkmanager", "azvirtualwan", "azrouteserver",
		"azexpressroute", "azvpngateway", "azwaf", "azappgwcontainers")
	loc := to.Ptr("eastus")

	// Network Watcher (the client's create is a plain call)
	nw := armtest.Must(armnetwork.NewWatchersClient(sub, e.Cred, e.Opts))
	w := armtest.Must(nw.CreateOrUpdate(ctx, rg, "nw", armnetwork.Watcher{Location: loc}, nil)).Watcher
	if *w.Type != "Microsoft.Network/networkWatchers" || *w.Properties.ProvisioningState != armnetwork.ProvisioningStateSucceeded {
		t.Fatalf("watcher: %+v", w)
	}
	_, err := nw.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = nw.CreateOrUpdate(ctx, rg, "nolocation", armnetwork.Watcher{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")

	// DDoS Protection
	ddos := armtest.Must(armnetwork.NewDdosProtectionPlansClient(sub, e.Cred, e.Opts))
	d := armtest.Must(armtest.Must(ddos.BeginCreateOrUpdate(ctx, rg, "plan", armnetwork.DdosProtectionPlan{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).DdosProtectionPlan
	if *d.Type != "Microsoft.Network/ddosProtectionPlans" {
		t.Fatalf("plan: %+v", d)
	}
	_, err = ddos.BeginCreateOrUpdate(ctx, rg, "nolocation", armnetwork.DdosProtectionPlan{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = ddos.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// Virtual Network Manager has no client in the armnetwork version of go.mod: the generic resources client.
	res := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	mgr := armresources.GenericResource{Location: loc, Properties: map[string]any{"networkManagerScopes": map[string]any{"subscriptions": []string{"/subscriptions/" + sub}}, "networkManagerScopeAccesses": []string{"Connectivity"}}}
	m := armtest.Must(armtest.Must(res.BeginCreateOrUpdate(ctx, rg, "Microsoft.Network", "", "networkManagers", "nm", "2024-05-01", mgr, nil)).PollUntilDone(ctx, armtest.Fast))
	if *m.Type != "Microsoft.Network/networkManagers" {
		t.Fatalf("manager: %+v", m)
	}
	armtest.Must(armtest.Must(res.BeginCreateOrUpdate(ctx, rg, "Microsoft.Network", "networkManagers/nm", "networkGroups", "ng", "2024-05-01", armresources.GenericResource{Properties: map[string]any{}}, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err = res.BeginCreateOrUpdate(ctx, rg, "Microsoft.Network", "networkManagers/nope", "networkGroups", "ng", "2024-05-01", armresources.GenericResource{Properties: map[string]any{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = res.BeginCreateOrUpdate(ctx, rg, "Microsoft.Network", "", "networkManagers", "noscope", "2024-05-01", armresources.GenericResource{Location: loc, Properties: map[string]any{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")

	// Route Server: a Virtual Hub with BGP peers
	hubs := armtest.Must(armnetwork.NewVirtualHubsClient(sub, e.Cred, e.Opts))
	bgp := armtest.Must(armnetwork.NewVirtualHubBgpConnectionClient(sub, e.Cred, e.Opts))
	armtest.Must(armtest.Must(hubs.BeginCreateOrUpdate(ctx, rg, "rs", armnetwork.VirtualHub{Location: loc, Properties: &armnetwork.VirtualHubProperties{SKU: to.Ptr("Standard")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	peer := armtest.Must(armtest.Must(bgp.BeginCreateOrUpdate(ctx, rg, "rs", "peer", armnetwork.BgpConnection{Properties: &armnetwork.BgpConnectionProperties{PeerAsn: to.Ptr[int64](65501), PeerIP: to.Ptr("10.0.9.4")}}, nil)).PollUntilDone(ctx, armtest.Fast)).BgpConnection
	if *peer.Properties.PeerAsn != 65501 || *peer.Type != "Microsoft.Network/virtualHubs/bgpConnections" {
		t.Fatalf("peer: %+v", peer)
	}
	_, err = bgp.BeginCreateOrUpdate(ctx, rg, "nohub", "peer", armnetwork.BgpConnection{Properties: &armnetwork.BgpConnectionProperties{PeerAsn: to.Ptr[int64](1), PeerIP: to.Ptr("10.0.9.4")}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")

	// ExpressRoute: a circuit with its service key, and an authorization under it
	circuits := armtest.Must(armnetwork.NewExpressRouteCircuitsClient(sub, e.Cred, e.Opts))
	auths := armtest.Must(armnetwork.NewExpressRouteCircuitAuthorizationsClient(sub, e.Cred, e.Opts))
	c := armtest.Must(armtest.Must(circuits.BeginCreateOrUpdate(ctx, rg, "er", armnetwork.ExpressRouteCircuit{Location: loc,
		SKU: &armnetwork.ExpressRouteCircuitSKU{Name: to.Ptr("Standard_MeteredData"), Tier: to.Ptr(armnetwork.ExpressRouteCircuitSKUTierStandard), Family: to.Ptr(armnetwork.ExpressRouteCircuitSKUFamilyMeteredData)},
		Properties: &armnetwork.ExpressRouteCircuitPropertiesFormat{ServiceProviderProperties: &armnetwork.ExpressRouteCircuitServiceProviderProperties{
			ServiceProviderName: to.Ptr("Equinix"), PeeringLocation: to.Ptr("Silicon Valley"), BandwidthInMbps: to.Ptr[int32](50)}}}, nil)).PollUntilDone(ctx, armtest.Fast)).ExpressRouteCircuit
	if len(*c.Properties.ServiceKey) != 36 || *c.Properties.ServiceProviderProvisioningState != armnetwork.ServiceProviderProvisioningStateNotProvisioned {
		t.Fatalf("circuit: %+v", c.Properties)
	}
	if again := armtest.Must(circuits.Get(ctx, rg, "er", nil)).ExpressRouteCircuit; *again.Properties.ServiceKey != *c.Properties.ServiceKey {
		t.Fatal("service key changes between reads")
	}
	armtest.Must(armtest.Must(auths.BeginCreateOrUpdate(ctx, rg, "er", "auth", armnetwork.ExpressRouteCircuitAuthorization{}, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err = auths.BeginCreateOrUpdate(ctx, rg, "nocircuit", "auth", armnetwork.ExpressRouteCircuitAuthorization{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = circuits.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// VPN Gateway: gateway, local gateway and a connection whose shared key reads back
	gws := armtest.Must(armnetwork.NewVirtualNetworkGatewaysClient(sub, e.Cred, e.Opts))
	locals := armtest.Must(armnetwork.NewLocalNetworkGatewaysClient(sub, e.Cred, e.Opts))
	conns := armtest.Must(armnetwork.NewVirtualNetworkGatewayConnectionsClient(sub, e.Cred, e.Opts))
	gw := armtest.Must(armtest.Must(gws.BeginCreateOrUpdate(ctx, rg, "vpngw", armnetwork.VirtualNetworkGateway{Location: loc, Properties: &armnetwork.VirtualNetworkGatewayPropertiesFormat{
		GatewayType: to.Ptr(armnetwork.VirtualNetworkGatewayTypeVPN), VPNType: to.Ptr(armnetwork.VPNTypeRouteBased),
		SKU:              &armnetwork.VirtualNetworkGatewaySKU{Name: to.Ptr(armnetwork.VirtualNetworkGatewaySKUNameVPNGw1), Tier: to.Ptr(armnetwork.VirtualNetworkGatewaySKUTierVPNGw1)},
		IPConfigurations: []*armnetwork.VirtualNetworkGatewayIPConfiguration{{Name: to.Ptr("default")}}}}, nil)).PollUntilDone(ctx, armtest.Fast)).VirtualNetworkGateway
	if *gw.Properties.GatewayType != armnetwork.VirtualNetworkGatewayTypeVPN || *gw.Properties.IPConfigurations[0].ID != *gw.ID+"/ipConfigurations/default" {
		t.Fatalf("gateway: %+v", gw.Properties)
	}
	site := armtest.Must(armtest.Must(locals.BeginCreateOrUpdate(ctx, rg, "site", armnetwork.LocalNetworkGateway{Location: loc, Properties: &armnetwork.LocalNetworkGatewayPropertiesFormat{
		GatewayIPAddress: to.Ptr("203.0.113.10"), LocalNetworkAddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("192.168.0.0/24")}}}}, nil)).PollUntilDone(ctx, armtest.Fast)).LocalNetworkGateway
	conn := armtest.Must(armtest.Must(conns.BeginCreateOrUpdate(ctx, rg, "conn", armnetwork.VirtualNetworkGatewayConnection{Location: loc, Properties: &armnetwork.VirtualNetworkGatewayConnectionPropertiesFormat{
		ConnectionType: to.Ptr(armnetwork.VirtualNetworkGatewayConnectionTypeIPsec), SharedKey: to.Ptr("secret-key"),
		VirtualNetworkGateway1: &armnetwork.VirtualNetworkGateway{ID: gw.ID}, LocalNetworkGateway2: &armnetwork.LocalNetworkGateway{ID: site.ID}}}, nil)).PollUntilDone(ctx, armtest.Fast)).VirtualNetworkGatewayConnection
	if *conn.Type != "Microsoft.Network/connections" {
		t.Fatalf("connection: %+v", conn)
	}
	if k := armtest.Must(conns.GetSharedKey(ctx, rg, "conn", nil)); *k.Value != "secret-key" {
		t.Fatalf("shared key: %v", *k.Value)
	}
	_, err = conns.GetSharedKey(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = gws.BeginCreateOrUpdate(ctx, rg, "nolocation", armnetwork.VirtualNetworkGateway{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = locals.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// Web Application Firewall policy (the client's create is a plain call)
	wafs := armtest.Must(armnetwork.NewWebApplicationFirewallPoliciesClient(sub, e.Cred, e.Opts))
	waf := armtest.Must(wafs.CreateOrUpdate(ctx, rg, "waf", armnetwork.WebApplicationFirewallPolicy{Location: loc, Properties: &armnetwork.WebApplicationFirewallPolicyPropertiesFormat{
		PolicySettings: &armnetwork.PolicySettings{Mode: to.Ptr(armnetwork.WebApplicationFirewallModePrevention)},
		ManagedRules:   &armnetwork.ManagedRulesDefinition{ManagedRuleSets: []*armnetwork.ManagedRuleSet{{RuleSetType: to.Ptr("OWASP"), RuleSetVersion: to.Ptr("3.2")}}}}}, nil)).WebApplicationFirewallPolicy
	if *waf.Properties.PolicySettings.Mode != armnetwork.WebApplicationFirewallModePrevention || *waf.Properties.ManagedRules.ManagedRuleSets[0].RuleSetVersion != "3.2" {
		t.Fatalf("policy: %+v", waf.Properties)
	}
	_, err = wafs.CreateOrUpdate(ctx, rg, "nomanaged", armnetwork.WebApplicationFirewallPolicy{Location: loc, Properties: &armnetwork.WebApplicationFirewallPolicyPropertiesFormat{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = wafs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// Application Gateway for Containers
	tcs := armtest.Must(armservicenetworking.NewTrafficControllerInterfaceClient(sub, e.Cred, e.Opts))
	fes := armtest.Must(armservicenetworking.NewFrontendsInterfaceClient(sub, e.Cred, e.Opts))
	assocs := armtest.Must(armservicenetworking.NewAssociationsInterfaceClient(sub, e.Cred, e.Opts))
	tc := armtest.Must(armtest.Must(tcs.BeginCreateOrUpdate(ctx, rg, "alb", armservicenetworking.TrafficController{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).TrafficController
	if *tc.Type != "Microsoft.ServiceNetworking/trafficControllers" {
		t.Fatalf("traffic controller: %+v", tc)
	}
	fe := armtest.Must(armtest.Must(fes.BeginCreateOrUpdate(ctx, rg, "alb", "fe", armservicenetworking.Frontend{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).Frontend
	if !strings.HasPrefix(*fe.Properties.Fqdn, "fe.") || !strings.HasSuffix(*fe.Properties.Fqdn, ".alb.azure.com") {
		t.Fatalf("frontend: %+v", fe.Properties)
	}
	armtest.Must(armtest.Must(assocs.BeginCreateOrUpdate(ctx, rg, "alb", "assoc", armservicenetworking.Association{Location: loc, Properties: &armservicenetworking.AssociationProperties{
		AssociationType: to.Ptr(armservicenetworking.AssociationTypeSubnets), Subnet: &armservicenetworking.AssociationSubnet{ID: to.Ptr("/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Network/virtualNetworks/v/subnets/s")}}}, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err = assocs.BeginCreateOrUpdate(ctx, rg, "alb", "notype", armservicenetworking.Association{Location: loc, Properties: &armservicenetworking.AssociationProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = fes.BeginCreateOrUpdate(ctx, rg, "nocontroller", "fe", armservicenetworking.Frontend{Location: loc}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = tcs.BeginCreateOrUpdate(ctx, rg, "nolocation", armservicenetworking.TrafficController{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = tcs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}
