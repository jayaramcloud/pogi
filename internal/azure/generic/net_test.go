package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/cdn/armcdn/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/trafficmanager/armtrafficmanager/v2"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

func TestFrontDoor(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azfrontdoor")
	profiles := armtest.Must(armcdn.NewProfilesClient(sub, e.Cred, e.Opts))
	eps := armtest.Must(armcdn.NewAFDEndpointsClient(sub, e.Cred, e.Opts))
	groups := armtest.Must(armcdn.NewAFDOriginGroupsClient(sub, e.Cred, e.Opts))
	origins := armtest.Must(armcdn.NewAFDOriginsClient(sub, e.Cred, e.Opts))
	routes := armtest.Must(armcdn.NewRoutesClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(profiles.BeginCreate(ctx, rg, "fd", armcdn.Profile{Location: to.Ptr("global"),
		SKU: &armcdn.SKU{Name: to.Ptr(armcdn.SKUNamePremiumAzureFrontDoor)}}, nil))
	p := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Profile
	if *p.Type != "Microsoft.Cdn/profiles" || *p.Properties.ProvisioningState != armcdn.ProfileProvisioningStateSucceeded ||
		*p.Properties.FrontDoorID == "" || *p.SKU.Name != armcdn.SKUNamePremiumAzureFrontDoor {
		t.Fatalf("profile: %+v", p.Properties)
	}
	ep := armtest.Must(armtest.Must(eps.BeginCreate(ctx, rg, "fd", "ep", armcdn.AFDEndpoint{Location: to.Ptr("global")}, nil)).PollUntilDone(ctx, armtest.Fast)).AFDEndpoint
	if h := *ep.Properties.HostName; !strings.HasPrefix(h, "ep-") || !strings.HasSuffix(h, ".azurefd.net") {
		t.Fatalf("host name %q", h)
	}
	if again := armtest.Must(eps.Get(ctx, rg, "fd", "ep", nil)).AFDEndpoint; *again.Properties.HostName != *ep.Properties.HostName {
		t.Fatal("host name changes between reads")
	}
	armtest.Must(armtest.Must(groups.BeginCreate(ctx, rg, "fd", "og", armcdn.AFDOriginGroup{Properties: &armcdn.AFDOriginGroupProperties{}}, nil)).PollUntilDone(ctx, armtest.Fast))
	armtest.Must(armtest.Must(origins.BeginCreate(ctx, rg, "fd", "og", "o1", armcdn.AFDOrigin{Properties: &armcdn.AFDOriginProperties{HostName: to.Ptr("origin.example.test")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	armtest.Must(armtest.Must(routes.BeginCreate(ctx, rg, "fd", "ep", "r1", armcdn.Route{Properties: &armcdn.RouteProperties{
		OriginGroup: &armcdn.ResourceReference{ID: p.ID}, PatternsToMatch: []*string{to.Ptr("/*")}}}, nil)).PollUntilDone(ctx, armtest.Fast))
	if n := drain(t, routes.NewListByEndpointPager(rg, "fd", "ep", nil), func(r armcdn.RoutesClientListByEndpointResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("routes: %d", n)
	}

	// failures
	_, err := profiles.BeginCreate(ctx, rg, "nolocation", armcdn.Profile{SKU: &armcdn.SKU{Name: to.Ptr(armcdn.SKUNameStandardAzureFrontDoor)}}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = profiles.BeginCreate(ctx, rg, "nosku", armcdn.Profile{Location: to.Ptr("global")}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = origins.BeginCreate(ctx, rg, "fd", "og", "nohost", armcdn.AFDOrigin{Properties: &armcdn.AFDOriginProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = eps.BeginCreate(ctx, rg, "nosuchprofile", "ep", armcdn.AFDEndpoint{Location: to.Ptr("global")}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = eps.Get(ctx, rg, "fd", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(eps.BeginDelete(ctx, rg, "fd", "nope", nil)) // delete of nothing succeeds, as in Azure
}

func TestLoadBalancer(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azloadbalancer")
	lbs := armtest.Must(armnetwork.NewLoadBalancersClient(sub, e.Cred, e.Opts))
	pools := armtest.Must(armnetwork.NewLoadBalancerBackendAddressPoolsClient(sub, e.Cred, e.Opts))
	lb := armtest.Must(armtest.Must(lbs.BeginCreateOrUpdate(ctx, rg, "lb", armnetwork.LoadBalancer{Location: to.Ptr("eastus"),
		SKU: &armnetwork.LoadBalancerSKU{Name: to.Ptr(armnetwork.LoadBalancerSKUNameStandard)},
		Properties: &armnetwork.LoadBalancerPropertiesFormat{
			FrontendIPConfigurations: []*armnetwork.FrontendIPConfiguration{{Name: to.Ptr("front")}},
			BackendAddressPools:      []*armnetwork.BackendAddressPool{{Name: to.Ptr("inline")}},
			Probes:                   []*armnetwork.Probe{{Name: to.Ptr("p"), Properties: &armnetwork.ProbePropertiesFormat{Port: to.Ptr[int32](80), Protocol: to.Ptr(armnetwork.ProbeProtocolTCP)}}},
		}}, nil)).PollUntilDone(ctx, armtest.Fast)).LoadBalancer
	if *lb.Properties.ProvisioningState != armnetwork.ProvisioningStateSucceeded || *lb.Properties.Probes[0].ID != *lb.ID+"/probes/p" ||
		*lb.Properties.FrontendIPConfigurations[0].ID != *lb.ID+"/frontendIPConfigurations/front" {
		t.Fatalf("load balancer: %+v", lb.Properties)
	}
	// a pool sent inline is a resource of its own, and the balancer lists it
	pool := armtest.Must(pools.Get(ctx, rg, "lb", "inline", nil)).BackendAddressPool
	if *pool.Type != "Microsoft.Network/loadBalancers/backendAddressPools" {
		t.Fatalf("pool: %+v", pool)
	}
	got := armtest.Must(lbs.Get(ctx, rg, "lb", nil)).LoadBalancer
	if len(got.Properties.BackendAddressPools) != 1 || *got.Properties.BackendAddressPools[0].Name != "inline" || len(got.Properties.LoadBalancingRules) != 0 {
		t.Fatalf("pools: %+v", got.Properties)
	}
	// putting the balancer back as it was read keeps the pool once
	armtest.Must(armtest.Must(lbs.BeginCreateOrUpdate(ctx, rg, "lb", got, nil)).PollUntilDone(ctx, armtest.Fast))
	if n := drain(t, pools.NewListPager(rg, "lb", nil), func(r armnetwork.LoadBalancerBackendAddressPoolsClientListResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("pools: %d", n)
	}

	_, err := lbs.BeginCreateOrUpdate(ctx, rg, "nolocation", armnetwork.LoadBalancer{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = lbs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = pools.Get(ctx, rg, "lb", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = pools.Get(ctx, rg, "nolb", "inline", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(lbs.BeginDelete(ctx, rg, "nope", nil))
}

func TestTrafficManager(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "aztrafficmanager")
	profiles := armtest.Must(armtrafficmanager.NewProfilesClient(sub, e.Cred, e.Opts))
	eps := armtest.Must(armtrafficmanager.NewEndpointsClient(sub, e.Cred, e.Opts))
	p := armtest.Must(profiles.CreateOrUpdate(ctx, rg, "tm", armtrafficmanager.Profile{Location: to.Ptr("global"), Properties: &armtrafficmanager.ProfileProperties{
		TrafficRoutingMethod: to.Ptr(armtrafficmanager.TrafficRoutingMethodWeighted),
		DNSConfig:            &armtrafficmanager.DNSConfig{RelativeName: to.Ptr("pogi-tm"), TTL: to.Ptr[int64](60)},
		MonitorConfig:        &armtrafficmanager.MonitorConfig{Protocol: to.Ptr(armtrafficmanager.MonitorProtocolHTTP), Port: to.Ptr[int64](80), Path: to.Ptr("/")}}}, nil)).Profile
	if *p.Properties.DNSConfig.Fqdn != "pogi-tm.trafficmanager.net" || *p.Properties.ProfileStatus != armtrafficmanager.ProfileStatusEnabled {
		t.Fatalf("profile: %+v", p.Properties)
	}
	ep := armtest.Must(eps.CreateOrUpdate(ctx, rg, "tm", armtrafficmanager.EndpointTypeExternalEndpoints, "ext", armtrafficmanager.Endpoint{
		Properties: &armtrafficmanager.EndpointProperties{Target: to.Ptr("www.example.test"), Weight: to.Ptr[int64](50)}}, nil)).Endpoint
	if *ep.Properties.EndpointStatus != armtrafficmanager.EndpointStatusEnabled || *ep.Properties.Weight != 50 {
		t.Fatalf("endpoint: %+v", ep.Properties)
	}
	if got := armtest.Must(profiles.Get(ctx, rg, "tm", nil)).Profile; len(got.Properties.Endpoints) != 1 || *got.Properties.Endpoints[0].Name != "ext" {
		t.Fatalf("endpoints of the profile: %+v", got.Properties.Endpoints)
	}
	armtest.Must(eps.Delete(ctx, rg, "tm", armtrafficmanager.EndpointTypeExternalEndpoints, "ext", nil))
	_, err := eps.Get(ctx, rg, "tm", armtrafficmanager.EndpointTypeExternalEndpoints, "ext", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = eps.CreateOrUpdate(ctx, rg, "nothere", armtrafficmanager.EndpointTypeExternalEndpoints, "ext", armtrafficmanager.Endpoint{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = profiles.CreateOrUpdate(ctx, rg, "nolocation", armtrafficmanager.Profile{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = profiles.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(profiles.Delete(ctx, rg, "tm", nil))
	armtest.Must(profiles.Delete(ctx, rg, "tm", nil))
}

// Application Gateway, Firewall, Bastion, Private Link and Virtual WAN: each is stored with what Azure fills in.
func TestNetworkingThin(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azappgateway", "azfirewall", "azbastion", "azprivatelink", "azvirtualwan")
	loc := to.Ptr("eastus")
	agws := armtest.Must(armnetwork.NewApplicationGatewaysClient(sub, e.Cred, e.Opts))
	gw := armtest.Must(armtest.Must(agws.BeginCreateOrUpdate(ctx, rg, "agw", armnetwork.ApplicationGateway{Location: loc, Properties: &armnetwork.ApplicationGatewayPropertiesFormat{
		FrontendPorts: []*armnetwork.ApplicationGatewayFrontendPort{{Name: to.Ptr("http"), Properties: &armnetwork.ApplicationGatewayFrontendPortPropertiesFormat{Port: to.Ptr[int32](80)}}}}}, nil)).PollUntilDone(ctx, armtest.Fast)).ApplicationGateway
	if *gw.Properties.OperationalState != armnetwork.ApplicationGatewayOperationalStateRunning || *gw.Properties.FrontendPorts[0].ID != *gw.ID+"/frontendPorts/http" {
		t.Fatalf("gateway: %+v", gw.Properties)
	}

	pols := armtest.Must(armnetwork.NewFirewallPoliciesClient(sub, e.Cred, e.Opts))
	groups := armtest.Must(armnetwork.NewFirewallPolicyRuleCollectionGroupsClient(sub, e.Cred, e.Opts))
	fws := armtest.Must(armnetwork.NewAzureFirewallsClient(sub, e.Cred, e.Opts))
	pol := armtest.Must(armtest.Must(pols.BeginCreateOrUpdate(ctx, rg, "pol", armnetwork.FirewallPolicy{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).FirewallPolicy
	grp := armtest.Must(armtest.Must(groups.BeginCreateOrUpdate(ctx, rg, "pol", "g1", armnetwork.FirewallPolicyRuleCollectionGroup{
		Properties: &armnetwork.FirewallPolicyRuleCollectionGroupProperties{Priority: to.Ptr[int32](200)}}, nil)).PollUntilDone(ctx, armtest.Fast)).FirewallPolicyRuleCollectionGroup
	if *grp.Type != "Microsoft.Network/firewallPolicies/ruleCollectionGroups" || *grp.Properties.Priority != 200 {
		t.Fatalf("rule collection group: %+v", grp)
	}
	fw := armtest.Must(armtest.Must(fws.BeginCreateOrUpdate(ctx, rg, "fw", armnetwork.AzureFirewall{Location: loc, Properties: &armnetwork.AzureFirewallPropertiesFormat{
		FirewallPolicy:   &armnetwork.SubResource{ID: pol.ID},
		IPConfigurations: []*armnetwork.AzureFirewallIPConfiguration{{Name: to.Ptr("c1")}, {Name: to.Ptr("c2")}}}}, nil)).PollUntilDone(ctx, armtest.Fast)).AzureFirewall
	if *fw.Properties.IPConfigurations[0].Properties.PrivateIPAddress == *fw.Properties.IPConfigurations[1].Properties.PrivateIPAddress {
		t.Fatalf("firewall addresses: %+v", fw.Properties.IPConfigurations)
	}

	bastions := armtest.Must(armnetwork.NewBastionHostsClient(sub, e.Cred, e.Opts))
	b := armtest.Must(armtest.Must(bastions.BeginCreateOrUpdate(ctx, rg, "bst", armnetwork.BastionHost{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).BastionHost
	if !strings.HasSuffix(*b.Properties.DNSName, ".bastion.azure.com") {
		t.Fatalf("bastion: %+v", b.Properties)
	}

	pls := armtest.Must(armnetwork.NewPrivateLinkServicesClient(sub, e.Cred, e.Opts))
	pes := armtest.Must(armnetwork.NewPrivateEndpointsClient(sub, e.Cred, e.Opts))
	svc := armtest.Must(armtest.Must(pls.BeginCreateOrUpdate(ctx, rg, "pls", armnetwork.PrivateLinkService{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).PrivateLinkService
	if !strings.HasSuffix(*svc.Properties.Alias, ".eastus.azure.privatelinkservice") {
		t.Fatalf("alias: %q", *svc.Properties.Alias)
	}
	pe := armtest.Must(armtest.Must(pes.BeginCreateOrUpdate(ctx, rg, "pe", armnetwork.PrivateEndpoint{Location: loc, Properties: &armnetwork.PrivateEndpointProperties{
		PrivateLinkServiceConnections:       []*armnetwork.PrivateLinkServiceConnection{{Name: to.Ptr("auto"), Properties: &armnetwork.PrivateLinkServiceConnectionProperties{PrivateLinkServiceID: svc.ID}}},
		ManualPrivateLinkServiceConnections: []*armnetwork.PrivateLinkServiceConnection{{Name: to.Ptr("manual"), Properties: &armnetwork.PrivateLinkServiceConnectionProperties{PrivateLinkServiceID: svc.ID}}}}}, nil)).PollUntilDone(ctx, armtest.Fast)).PrivateEndpoint
	if s := *pe.Properties.PrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status; s != "Approved" {
		t.Fatalf("automatic connection: %s", s)
	}
	if s := *pe.Properties.ManualPrivateLinkServiceConnections[0].Properties.PrivateLinkServiceConnectionState.Status; s != "Pending" {
		t.Fatalf("manual connection: %s", s)
	}

	wans := armtest.Must(armnetwork.NewVirtualWansClient(sub, e.Cred, e.Opts))
	hubs := armtest.Must(armnetwork.NewVirtualHubsClient(sub, e.Cred, e.Opts))
	wan := armtest.Must(armtest.Must(wans.BeginCreateOrUpdate(ctx, rg, "wan", armnetwork.VirtualWAN{Location: loc}, nil)).PollUntilDone(ctx, armtest.Fast)).VirtualWAN
	hub := armtest.Must(armtest.Must(hubs.BeginCreateOrUpdate(ctx, rg, "hub", armnetwork.VirtualHub{Location: loc, Properties: &armnetwork.VirtualHubProperties{
		VirtualWan: &armnetwork.SubResource{ID: wan.ID}, AddressPrefix: to.Ptr("10.30.0.0/24")}}, nil)).PollUntilDone(ctx, armtest.Fast)).VirtualHub
	if *hub.Properties.RoutingState != armnetwork.RoutingStateProvisioned || *hub.Properties.VirtualRouterAsn != 65515 {
		t.Fatalf("hub: %+v", hub.Properties)
	}

	// failures: a missing location, a child of nothing, nothing to get or delete
	_, err := agws.BeginCreateOrUpdate(ctx, rg, "nolocation", armnetwork.ApplicationGateway{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = groups.BeginCreateOrUpdate(ctx, rg, "nopolicy", "g", armnetwork.FirewallPolicyRuleCollectionGroup{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = hubs.BeginCreateOrUpdate(ctx, rg, "nolocation", armnetwork.VirtualHub{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = bastions.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = pes.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(pls.BeginDelete(ctx, rg, "nope", nil))
	_, err = wans.BeginDelete(ctx, "missing-group", "w", nil)
	armtest.WantErr(t, err, 404, "ResourceGroupNotFound")
}
