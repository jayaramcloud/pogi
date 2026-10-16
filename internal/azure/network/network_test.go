package network_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azarm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// fast makes the SDK poll long-running operations at once, not every second.
var fast = &runtime.PollUntilDoneOptions{Frequency: 10 * time.Millisecond}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// start runs pogi with the Resource Manager, the sign-in services and the network
// service, and returns client options that trust pogi's CA and sign in with client credentials.
func start(t *testing.T) (azcore.TokenCredential, *azarm.ClientOptions, *pogi.Running) {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"arm", "entra", "graph", "aznetwork"}})
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	cred := must(azidentity.NewClientSecretCredential(arm.TenantID, "app", "secret",
		&azidentity.ClientSecretCredentialOptions{ClientOptions: co, DisableInstanceDiscovery: true}))
	return cred, &azarm.ClientOptions{ClientOptions: co}, srv
}

func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || re.ErrorCode != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
}

const (
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
	loc = "westeurope"
)

func TestVirtualNetwork(t *testing.T) {
	t.Parallel()
	cred, opts, _ := start(t)
	vnets, subnets := must(armnetwork.NewVirtualNetworksClient(sub, cred, opts)), must(armnetwork.NewSubnetsClient(sub, cred, opts))

	// 1. create, with one subnet inline
	vn := armnetwork.VirtualNetwork{Location: to.Ptr(loc), Properties: &armnetwork.VirtualNetworkPropertiesFormat{
		AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.0.0.0/16")}},
		Subnets:      []*armnetwork.Subnet{{Name: to.Ptr("inline"), Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr("10.0.0.0/24")}}},
	}}
	created := must(must(vnets.BeginCreateOrUpdate(ctx, rg, "vn1", vn, nil)).PollUntilDone(ctx, fast))
	if *created.Name != "vn1" || *created.Properties.ProvisioningState != "Succeeded" || len(created.Properties.Subnets) != 1 || created.Etag == nil {
		t.Fatalf("%+v", created)
	}
	if *created.ID != "/subscriptions/"+sub+"/resourceGroups/"+rg+"/providers/Microsoft.Network/virtualNetworks/vn1" {
		t.Fatal(*created.ID)
	}

	// 2. subnets: a second one, inside the space; outside it; overlapping; invalid CIDR
	sn := must(must(subnets.BeginCreateOrUpdate(ctx, rg, "vn1", "app", armnetwork.Subnet{Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr("10.0.1.0/24")}}, nil)).PollUntilDone(ctx, fast))
	if *sn.Properties.AddressPrefix != "10.0.1.0/24" || *sn.Properties.PrivateEndpointNetworkPolicies != "Disabled" {
		t.Fatalf("%+v", sn.Properties)
	}
	bad := func(prefix, code string) {
		t.Helper()
		p, err := subnets.BeginCreateOrUpdate(ctx, rg, "vn1", "bad", armnetwork.Subnet{Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr(prefix)}}, nil)
		if err == nil {
			_, err = p.PollUntilDone(ctx, fast)
		}
		wantErr(t, err, 400, code)
	}
	bad("192.168.0.0/24", "NetcfgInvalidSubnet")
	bad("10.0.1.128/25", "NetcfgSubnetRangesOverlap")
	bad("10.0.2.1/24", "InvalidCIDRNotation")
	bad("nonsense", "InvalidCIDRNotation")

	// 3. get shows both subnets inline; get one subnet; list
	got := must(vnets.Get(ctx, rg, "vn1", nil))
	if len(got.Properties.Subnets) != 2 {
		t.Fatalf("%d subnets", len(got.Properties.Subnets))
	}
	if g := must(subnets.Get(ctx, rg, "vn1", "inline", nil)); *g.Properties.AddressPrefix != "10.0.0.0/24" {
		t.Fatal(*g.Properties.AddressPrefix)
	}
	names := []string{}
	for p := subnets.NewListPager(rg, "vn1", nil); p.More(); {
		for _, s := range must(p.NextPage(ctx)).Value {
			names = append(names, *s.Name)
		}
	}
	if strings.Join(names, ",") != "app,inline" {
		t.Fatal(names)
	}
	var all []string
	for p := vnets.NewListPager(rg, nil); p.More(); {
		for _, v := range must(p.NextPage(ctx)).Value {
			all = append(all, *v.Name)
		}
	}
	for p := vnets.NewListAllPager(nil); p.More(); {
		for _, v := range must(p.NextPage(ctx)).Value {
			all = append(all, *v.Name)
		}
	}
	if strings.Join(all, ",") != "vn1,vn1" {
		t.Fatal(all)
	}

	// 4. update: tags keep the subnets; shrinking the space below a subnet fails
	tagged := must(vnets.UpdateTags(ctx, rg, "vn1", armnetwork.TagsObject{Tags: map[string]*string{"env": to.Ptr("test")}}, nil))
	if *tagged.Tags["env"] != "test" || len(tagged.Properties.Subnets) != 2 || *tagged.Location != loc {
		t.Fatalf("%+v", tagged)
	}
	shrunk := vn
	shrunk.Properties = &armnetwork.VirtualNetworkPropertiesFormat{AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.0.0.0/24")}}}
	p, err := vnets.BeginCreateOrUpdate(ctx, rg, "vn1", shrunk, nil)
	if err == nil {
		_, err = p.PollUntilDone(ctx, fast)
	}
	wantErr(t, err, 400, "NetcfgInvalidSubnet")
	if g := must(vnets.Get(ctx, rg, "vn1", nil)); len(g.Properties.Subnets) != 2 || *g.Properties.AddressSpace.AddressPrefixes[0] != "10.0.0.0/16" {
		t.Fatal("the failed update changed the network")
	}
	grown := vn
	grown.Properties = &armnetwork.VirtualNetworkPropertiesFormat{AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.0.0.0/16"), to.Ptr("172.16.0.0/16")}}}
	grown.Properties.Subnets = nil
	must(must(vnets.BeginCreateOrUpdate(ctx, rg, "vn1", grown, nil)).PollUntilDone(ctx, fast))
	must(must(subnets.BeginCreateOrUpdate(ctx, rg, "vn1", "second", armnetwork.Subnet{Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr("172.16.5.0/24")}}, nil)).PollUntilDone(ctx, fast))

	// 5. a location is required
	_, err = vnets.BeginCreateOrUpdate(ctx, rg, "nolocation", armnetwork.VirtualNetwork{}, nil)
	wantErr(t, err, 400, "LocationRequired")

	// 6. delete, then the typed not-found
	must(must(subnets.BeginDelete(ctx, rg, "vn1", "second", nil)).PollUntilDone(ctx, fast))
	_, err = subnets.Get(ctx, rg, "vn1", "second", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(vnets.BeginDelete(ctx, rg, "vn1", nil)).PollUntilDone(ctx, fast))
	_, err = vnets.Get(ctx, rg, "vn1", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = subnets.Get(ctx, rg, "vn1", "app", nil)
	wantErr(t, err, 404, "ResourceNotFound")
}

func TestSecurityGroup(t *testing.T) {
	t.Parallel()
	cred, opts, _ := start(t)
	nsgs, rules := must(armnetwork.NewSecurityGroupsClient(sub, cred, opts)), must(armnetwork.NewSecurityRulesClient(sub, cred, opts))

	rule := func(prio int32, dir armnetwork.SecurityRuleDirection) armnetwork.SecurityRule {
		return armnetwork.SecurityRule{Properties: &armnetwork.SecurityRulePropertiesFormat{
			Protocol: to.Ptr(armnetwork.SecurityRuleProtocolTCP), Access: to.Ptr(armnetwork.SecurityRuleAccessAllow), Direction: to.Ptr(dir), Priority: to.Ptr(prio),
			SourceAddressPrefix: to.Ptr("*"), SourcePortRange: to.Ptr("*"), DestinationAddressPrefix: to.Ptr("*"), DestinationPortRange: to.Ptr("443")}}
	}
	created := must(must(nsgs.BeginCreateOrUpdate(ctx, rg, "nsg1", armnetwork.SecurityGroup{Location: to.Ptr(loc), Properties: &armnetwork.SecurityGroupPropertiesFormat{
		SecurityRules: []*armnetwork.SecurityRule{{Name: to.Ptr("https"), Properties: rule(100, armnetwork.SecurityRuleDirectionInbound).Properties}}}}, nil)).PollUntilDone(ctx, fast))
	if len(created.Properties.SecurityRules) != 1 || len(created.Properties.DefaultSecurityRules) != 6 {
		t.Fatalf("%d rules, %d default", len(created.Properties.SecurityRules), len(created.Properties.DefaultSecurityRules))
	}

	// a rule: create, same priority in the other direction is fine, same direction conflicts, bad priority
	must(must(rules.BeginCreateOrUpdate(ctx, rg, "nsg1", "out", rule(100, armnetwork.SecurityRuleDirectionOutbound), nil)).PollUntilDone(ctx, fast))
	p, err := rules.BeginCreateOrUpdate(ctx, rg, "nsg1", "dup", rule(100, armnetwork.SecurityRuleDirectionInbound), nil)
	if err == nil {
		_, err = p.PollUntilDone(ctx, fast)
	}
	wantErr(t, err, 400, "SecurityRuleConflict")
	_, err = rules.BeginCreateOrUpdate(ctx, rg, "nsg1", "low", rule(99, armnetwork.SecurityRuleDirectionInbound), nil)
	wantErr(t, err, 400, "SecurityRuleInvalidPriority")
	updated := must(must(rules.BeginCreateOrUpdate(ctx, rg, "nsg1", "https", rule(200, armnetwork.SecurityRuleDirectionInbound), nil)).PollUntilDone(ctx, fast))
	if *updated.Properties.Priority != 200 {
		t.Fatal(*updated.Properties.Priority)
	}

	var names []string
	for p := rules.NewListPager(rg, "nsg1", nil); p.More(); {
		for _, r := range must(p.NextPage(ctx)).Value {
			names = append(names, *r.Name)
		}
	}
	if strings.Join(names, ",") != "https,out" {
		t.Fatal(names)
	}
	tagged := must(nsgs.UpdateTags(ctx, rg, "nsg1", armnetwork.TagsObject{Tags: map[string]*string{"a": to.Ptr("b")}}, nil))
	if *tagged.Tags["a"] != "b" || len(tagged.Properties.SecurityRules) != 2 {
		t.Fatalf("%+v", tagged)
	}

	must(must(rules.BeginDelete(ctx, rg, "nsg1", "out", nil)).PollUntilDone(ctx, fast))
	_, err = rules.Get(ctx, rg, "nsg1", "out", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(nsgs.BeginDelete(ctx, rg, "nsg1", nil)).PollUntilDone(ctx, fast))
	_, err = nsgs.Get(ctx, rg, "nsg1", nil)
	wantErr(t, err, 404, "ResourceNotFound")
}

func TestPublicIPAndNIC(t *testing.T) {
	t.Parallel()
	cred, opts, _ := start(t)
	vnets, subnets, ips, nics, nsgs := must(armnetwork.NewVirtualNetworksClient(sub, cred, opts)), must(armnetwork.NewSubnetsClient(sub, cred, opts)), must(armnetwork.NewPublicIPAddressesClient(sub, cred, opts)), must(armnetwork.NewInterfacesClient(sub, cred, opts)), must(armnetwork.NewSecurityGroupsClient(sub, cred, opts))

	// public IPs come from the fake range, one each, and a DNS label is unique
	pip := func(name, label string) armnetwork.PublicIPAddress {
		a := armnetwork.PublicIPAddress{Location: to.Ptr(loc), SKU: &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)},
			Properties: &armnetwork.PublicIPAddressPropertiesFormat{PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic)}}
		if label != "" {
			a.Properties.DNSSettings = &armnetwork.PublicIPAddressDNSSettings{DomainNameLabel: to.Ptr(label)}
		}
		return must(must(ips.BeginCreateOrUpdate(ctx, rg, name, a, nil)).PollUntilDone(ctx, fast)).PublicIPAddress
	}
	ip1, ip2 := pip("ip1", "pogi-one"), pip("ip2", "")
	fake := netip.MustParsePrefix("198.18.0.0/15")
	for _, ip := range []armnetwork.PublicIPAddress{ip1, ip2} {
		if a, err := netip.ParseAddr(*ip.Properties.IPAddress); err != nil || !fake.Contains(a) {
			t.Fatalf("address %v", *ip.Properties.IPAddress)
		}
	}
	if *ip1.Properties.IPAddress == *ip2.Properties.IPAddress || *ip1.Properties.DNSSettings.Fqdn != "pogi-one.westeurope.cloudapp.azure.com" {
		t.Fatalf("%+v", ip1.Properties)
	}
	_, err := ips.BeginCreateOrUpdate(ctx, rg, "ip3", armnetwork.PublicIPAddress{Location: to.Ptr(loc), Properties: &armnetwork.PublicIPAddressPropertiesFormat{
		DNSSettings: &armnetwork.PublicIPAddressDNSSettings{DomainNameLabel: to.Ptr("POGI-one")}}}, nil)
	wantErr(t, err, 400, "DnsRecordInUse")
	again := pip("ip1", "pogi-one") // an update keeps the address
	if *again.Properties.IPAddress != *ip1.Properties.IPAddress {
		t.Fatal("the address changed")
	}
	if tagged := must(ips.UpdateTags(ctx, rg, "ip1", armnetwork.TagsObject{Tags: map[string]*string{"k": to.Ptr("v")}}, nil)); *tagged.Tags["k"] != "v" {
		t.Fatal(tagged.Tags)
	}

	// the network, its subnet, and a security group to attach
	must(must(vnets.BeginCreateOrUpdate(ctx, rg, "vn", armnetwork.VirtualNetwork{Location: to.Ptr(loc), Properties: &armnetwork.VirtualNetworkPropertiesFormat{
		AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.1.0.0/16")}}}}, nil)).PollUntilDone(ctx, fast))
	snet := must(must(subnets.BeginCreateOrUpdate(ctx, rg, "vn", "s", armnetwork.Subnet{Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr("10.1.1.0/29")}}, nil)).PollUntilDone(ctx, fast))
	nsg := must(must(nsgs.BeginCreateOrUpdate(ctx, rg, "nsg", armnetwork.SecurityGroup{Location: to.Ptr(loc)}, nil)).PollUntilDone(ctx, fast))

	nic := func(name, addr string, publicIP *armnetwork.PublicIPAddress) (armnetwork.InterfacesClientCreateOrUpdateResponse, error) {
		cfg := &armnetwork.InterfaceIPConfigurationPropertiesFormat{Subnet: &armnetwork.Subnet{ID: snet.ID}, PrivateIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodDynamic)}
		if addr != "" {
			cfg.PrivateIPAddress, cfg.PrivateIPAllocationMethod = to.Ptr(addr), to.Ptr(armnetwork.IPAllocationMethodStatic)
		}
		if publicIP != nil {
			cfg.PublicIPAddress = &armnetwork.PublicIPAddress{ID: publicIP.ID}
		}
		p, err := nics.BeginCreateOrUpdate(ctx, rg, name, armnetwork.Interface{Location: to.Ptr(loc), Properties: &armnetwork.InterfacePropertiesFormat{
			NetworkSecurityGroup: &armnetwork.SecurityGroup{ID: nsg.ID},
			IPConfigurations:     []*armnetwork.InterfaceIPConfiguration{{Name: to.Ptr("ipconfig1"), Properties: cfg}}}}, nil)
		if err != nil {
			return armnetwork.InterfacesClientCreateOrUpdateResponse{}, err
		}
		return p.PollUntilDone(ctx, fast)
	}
	private := func(r armnetwork.InterfacesClientCreateOrUpdateResponse) string {
		return *r.Properties.IPConfigurations[0].Properties.PrivateIPAddress
	}

	// dynamic addresses skip the four Azure keeps, and are handed out in turn
	n1 := must(nic("nic1", "", &ip1))
	n2 := must(nic("nic2", "", nil))
	if private(n1) != "10.1.1.4" || private(n2) != "10.1.1.5" || *n1.Properties.MacAddress == "" {
		t.Fatal(private(n1), private(n2))
	}
	if again := must(nic("nic1", "", &ip1)); private(again) != "10.1.1.4" || *again.Properties.MacAddress != *n1.Properties.MacAddress {
		t.Fatal("an update moved the interface")
	}
	// static: valid, taken, outside the subnet, reserved
	if n3 := must(nic("nic3", "10.1.1.6", nil)); private(n3) != "10.1.1.6" {
		t.Fatal(private(n3))
	}
	_, err = nic("nic4", "10.1.1.6", nil)
	wantErr(t, err, 400, "PrivateIPAddressIsAllocated")
	_, err = nic("nic4", "10.1.2.9", nil)
	wantErr(t, err, 400, "PrivateIPAddressNotInSubnet")
	_, err = nic("nic4", "10.1.1.2", nil)
	wantErr(t, err, 400, "PrivateIPAddressIsReserved")
	// a /29 has 8 addresses, 4..6 usable: 4, 5 and 6 are taken
	_, err = nic("nic5", "", nil)
	wantErr(t, err, 400, "SubnetIsFull")
	// references must exist
	bogus := armnetwork.PublicIPAddress{ID: to.Ptr(*ip1.ID + "x")}
	_, err = nic("nic6", "", &bogus)
	wantErr(t, err, 400, "InvalidResourceReference")

	// list and tags
	var names []string
	for p := nics.NewListPager(rg, nil); p.More(); {
		for _, n := range must(p.NextPage(ctx)).Value {
			names = append(names, *n.Name)
		}
	}
	if strings.Join(names, ",") != "nic1,nic2,nic3" {
		t.Fatal(names)
	}
	if tagged := must(nics.UpdateTags(ctx, rg, "nic1", armnetwork.TagsObject{Tags: map[string]*string{"k": to.Ptr("v")}}, nil)); *tagged.Tags["k"] != "v" || private(armnetwork.InterfacesClientCreateOrUpdateResponse(tagged)) != "10.1.1.4" {
		t.Fatalf("%+v", tagged)
	}

	// what is in use cannot be deleted; once the interfaces are gone it can
	for _, c := range []struct {
		del  func() error
		code string
	}{
		{func() error { _, err := subnets.BeginDelete(ctx, rg, "vn", "s", nil); return err }, "InUseSubnetCannotBeDeleted"},
		{func() error { _, err := vnets.BeginDelete(ctx, rg, "vn", nil); return err }, "InUseSubnetCannotBeDeleted"},
		{func() error { _, err := ips.BeginDelete(ctx, rg, "ip1", nil); return err }, "PublicIPAddressCannotBeDeleted"},
		{func() error { _, err := nsgs.BeginDelete(ctx, rg, "nsg", nil); return err }, "InUseNetworkSecurityGroupCannotBeDeleted"},
	} {
		wantErr(t, c.del(), 400, c.code)
	}
	for _, n := range names {
		must(must(nics.BeginDelete(ctx, rg, n, nil)).PollUntilDone(ctx, fast))
	}
	_, err = nics.Get(ctx, rg, "nic1", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(ips.BeginDelete(ctx, rg, "ip1", nil)).PollUntilDone(ctx, fast))
	must(must(nsgs.BeginDelete(ctx, rg, "nsg", nil)).PollUntilDone(ctx, fast))
	must(must(vnets.BeginDelete(ctx, rg, "vn", nil)).PollUntilDone(ctx, fast))
	_, err = ips.Get(ctx, rg, "ip1", nil)
	wantErr(t, err, 404, "ResourceNotFound")
}

func TestRouteTableAndNAT(t *testing.T) {
	t.Parallel()
	cred, opts, _ := start(t)
	tables, routes, nats, ips, vnets, subnets := must(armnetwork.NewRouteTablesClient(sub, cred, opts)), must(armnetwork.NewRoutesClient(sub, cred, opts)), must(armnetwork.NewNatGatewaysClient(sub, cred, opts)), must(armnetwork.NewPublicIPAddressesClient(sub, cred, opts)), must(armnetwork.NewVirtualNetworksClient(sub, cred, opts)), must(armnetwork.NewSubnetsClient(sub, cred, opts))

	rt := must(must(tables.BeginCreateOrUpdate(ctx, rg, "rt", armnetwork.RouteTable{Location: to.Ptr(loc), Properties: &armnetwork.RouteTablePropertiesFormat{
		Routes: []*armnetwork.Route{{Name: to.Ptr("all"), Properties: &armnetwork.RoutePropertiesFormat{AddressPrefix: to.Ptr("0.0.0.0/0"), NextHopType: to.Ptr(armnetwork.RouteNextHopTypeInternet)}}}}}, nil)).PollUntilDone(ctx, fast))
	if len(rt.Properties.Routes) != 1 || *rt.Properties.DisableBgpRoutePropagation {
		t.Fatalf("%+v", rt.Properties)
	}
	r := armnetwork.Route{Properties: &armnetwork.RoutePropertiesFormat{AddressPrefix: to.Ptr("10.9.0.0/16"), NextHopType: to.Ptr(armnetwork.RouteNextHopTypeVirtualAppliance), NextHopIPAddress: to.Ptr("10.0.0.4")}}
	if got := must(must(routes.BeginCreateOrUpdate(ctx, rg, "rt", "appliance", r, nil)).PollUntilDone(ctx, fast)); *got.Properties.NextHopIPAddress != "10.0.0.4" {
		t.Fatal(got)
	}
	r.Properties.NextHopIPAddress = nil
	_, err := routes.BeginCreateOrUpdate(ctx, rg, "rt", "noip", r, nil)
	wantErr(t, err, 400, "InvalidRequestFormat")
	r.Properties.AddressPrefix = to.Ptr("10.9.0.1/16")
	_, err = routes.BeginCreateOrUpdate(ctx, rg, "rt", "badcidr", r, nil)
	wantErr(t, err, 400, "InvalidCIDRNotation")
	var names []string
	for p := routes.NewListPager(rg, "rt", nil); p.More(); {
		for _, x := range must(p.NextPage(ctx)).Value {
			names = append(names, *x.Name)
		}
	}
	if strings.Join(names, ",") != "all,appliance" {
		t.Fatal(names)
	}
	if tagged := must(tables.UpdateTags(ctx, rg, "rt", armnetwork.TagsObject{Tags: map[string]*string{"k": to.Ptr("v")}}, nil)); *tagged.Tags["k"] != "v" || len(tagged.Properties.Routes) != 2 {
		t.Fatalf("%+v", tagged)
	}

	// a NAT gateway with a public IP, attached to a subnet together with the route table
	pip := must(must(ips.BeginCreateOrUpdate(ctx, rg, "natip", armnetwork.PublicIPAddress{Location: to.Ptr(loc), SKU: &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)},
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic)}}, nil)).PollUntilDone(ctx, fast))
	_, err = nats.BeginCreateOrUpdate(ctx, rg, "nat", armnetwork.NatGateway{Location: to.Ptr(loc), Properties: &armnetwork.NatGatewayPropertiesFormat{
		PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(*pip.ID + "-missing")}}}}, nil)
	wantErr(t, err, 400, "InvalidResourceReference")
	nat := must(must(nats.BeginCreateOrUpdate(ctx, rg, "nat", armnetwork.NatGateway{Location: to.Ptr(loc), Properties: &armnetwork.NatGatewayPropertiesFormat{
		PublicIPAddresses: []*armnetwork.SubResource{{ID: pip.ID}}}}, nil)).PollUntilDone(ctx, fast))
	if *nat.Properties.IdleTimeoutInMinutes != 4 || *nat.SKU.Name != armnetwork.NatGatewaySKUNameStandard || len(nat.Properties.PublicIPAddresses) != 1 {
		t.Fatalf("%+v", nat)
	}
	must(must(vnets.BeginCreateOrUpdate(ctx, rg, "nvn", armnetwork.VirtualNetwork{Location: to.Ptr(loc), Properties: &armnetwork.VirtualNetworkPropertiesFormat{
		AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.2.0.0/16")}}}}, nil)).PollUntilDone(ctx, fast))
	sn := must(must(subnets.BeginCreateOrUpdate(ctx, rg, "nvn", "s", armnetwork.Subnet{Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr("10.2.0.0/24"),
		NatGateway: &armnetwork.SubResource{ID: nat.ID}, RouteTable: &armnetwork.RouteTable{ID: rt.ID}}}, nil)).PollUntilDone(ctx, fast))
	if *sn.Properties.NatGateway.ID != *nat.ID {
		t.Fatal("nat gateway not attached")
	}
	var natNames []string
	for p := nats.NewListPager(rg, nil); p.More(); {
		for _, n := range must(p.NextPage(ctx)).Value {
			natNames = append(natNames, *n.Name)
		}
	}
	if strings.Join(natNames, ",") != "nat" {
		t.Fatal(natNames)
	}
	if tagged := must(nats.UpdateTags(ctx, rg, "nat", armnetwork.TagsObject{Tags: map[string]*string{"k": to.Ptr("v")}}, nil)); *tagged.Tags["k"] != "v" {
		t.Fatal(tagged.Tags)
	}

	_, err = nats.BeginDelete(ctx, rg, "nat", nil)
	wantErr(t, err, 400, "InUseNatGatewayCannotBeDeleted")
	_, err = tables.BeginDelete(ctx, rg, "rt", nil)
	wantErr(t, err, 400, "InUseRouteTableCannotBeDeleted")
	must(must(vnets.BeginDelete(ctx, rg, "nvn", nil)).PollUntilDone(ctx, fast))
	must(must(nats.BeginDelete(ctx, rg, "nat", nil)).PollUntilDone(ctx, fast))
	must(must(routes.BeginDelete(ctx, rg, "rt", "appliance", nil)).PollUntilDone(ctx, fast))
	must(must(tables.BeginDelete(ctx, rg, "rt", nil)).PollUntilDone(ctx, fast))
	_, err = nats.Get(ctx, rg, "nat", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = tables.Get(ctx, rg, "rt", nil)
	wantErr(t, err, 404, "ResourceNotFound")
}

func TestPrivateDNS(t *testing.T) {
	t.Parallel()
	cred, opts, _ := start(t)
	f := must(armprivatedns.NewClientFactory(sub, cred, opts))
	zones, records, links := f.NewPrivateZonesClient(), f.NewRecordSetsClient(), f.NewVirtualNetworkLinksClient()
	vnets := must(armnetwork.NewVirtualNetworksClient(sub, cred, opts))

	_, err := zones.BeginCreateOrUpdate(ctx, rg, "nodots", armprivatedns.PrivateZone{Location: to.Ptr("global")}, nil)
	wantErr(t, err, 400, "InvalidZoneName")
	z := must(must(zones.BeginCreateOrUpdate(ctx, rg, "corp.example.com", armprivatedns.PrivateZone{Location: to.Ptr("global")}, nil)).PollUntilDone(ctx, fast))
	if *z.Location != "global" || *z.Properties.NumberOfRecordSets != 1 || *z.Properties.MaxNumberOfRecordSets != 25000 {
		t.Fatalf("%+v", z.Properties)
	}

	// record sets: the SOA is there; A and CNAME are created; validation; ALL and typed lists
	soa := must(records.Get(ctx, rg, "corp.example.com", armprivatedns.RecordTypeSOA, "@", nil))
	if *soa.Properties.Fqdn != "corp.example.com." || soa.Properties.SoaRecord == nil {
		t.Fatalf("%+v", soa.Properties)
	}
	a := must(records.CreateOrUpdate(ctx, rg, "corp.example.com", armprivatedns.RecordTypeA, "db", armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
		TTL: to.Ptr[int64](300), ARecords: []*armprivatedns.ARecord{{IPv4Address: to.Ptr("10.1.1.4")}}}}, nil))
	if *a.Properties.Fqdn != "db.corp.example.com." || *a.Properties.ARecords[0].IPv4Address != "10.1.1.4" || *a.Type != "Microsoft.Network/privateDnsZones/A" {
		t.Fatalf("%+v", a)
	}
	_, err = records.CreateOrUpdate(ctx, rg, "corp.example.com", armprivatedns.RecordTypeA, "bad", armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
		ARecords: []*armprivatedns.ARecord{{IPv4Address: to.Ptr("999.1.1.4")}}}}, nil)
	wantErr(t, err, 400, "InvalidRequestFormat")
	must(records.CreateOrUpdate(ctx, rg, "corp.example.com", armprivatedns.RecordTypeCNAME, "www", armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
		CnameRecord: &armprivatedns.CnameRecord{Cname: to.Ptr("db.corp.example.com")}}}, nil))
	must(records.CreateOrUpdate(ctx, rg, "corp.example.com", armprivatedns.RecordTypeTXT, "info", armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{
		TxtRecords: []*armprivatedns.TxtRecord{{Value: []*string{to.Ptr("hello")}}}}}, nil))
	if n := count(records.NewListPager(rg, "corp.example.com", nil)); n != 4 { // SOA, A, CNAME, TXT
		t.Fatalf("%d record sets", n)
	}
	if n := count(records.NewListByTypePager(rg, "corp.example.com", armprivatedns.RecordTypeA, nil)); n != 1 {
		t.Fatalf("%d A record sets", n)
	}
	if upd := must(records.Update(ctx, rg, "corp.example.com", armprivatedns.RecordTypeA, "db", armprivatedns.RecordSet{Properties: &armprivatedns.RecordSetProperties{TTL: to.Ptr[int64](60)}}, nil)); *upd.Properties.TTL != 60 || len(upd.Properties.ARecords) != 1 {
		t.Fatalf("%+v", upd.Properties)
	}

	// a link to a network that exists, and one to a network that does not
	vn := must(must(vnets.BeginCreateOrUpdate(ctx, rg, "dnsvn", armnetwork.VirtualNetwork{Location: to.Ptr(loc), Properties: &armnetwork.VirtualNetworkPropertiesFormat{
		AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.3.0.0/16")}}}}, nil)).PollUntilDone(ctx, fast))
	_, err = links.BeginCreateOrUpdate(ctx, rg, "corp.example.com", "bogus", armprivatedns.VirtualNetworkLink{Location: to.Ptr("global"), Properties: &armprivatedns.VirtualNetworkLinkProperties{
		VirtualNetwork: &armprivatedns.SubResource{ID: to.Ptr(*vn.ID + "-missing")}}}, nil)
	wantErr(t, err, 400, "InvalidResourceReference")
	l := must(must(links.BeginCreateOrUpdate(ctx, rg, "corp.example.com", "link", armprivatedns.VirtualNetworkLink{Location: to.Ptr("global"), Properties: &armprivatedns.VirtualNetworkLinkProperties{
		VirtualNetwork: &armprivatedns.SubResource{ID: vn.ID}, RegistrationEnabled: to.Ptr(true)}}, nil)).PollUntilDone(ctx, fast))
	if *l.Properties.VirtualNetworkLinkState != armprivatedns.VirtualNetworkLinkStateCompleted {
		t.Fatalf("%+v", l.Properties)
	}
	z2 := must(zones.Get(ctx, rg, "corp.example.com", nil))
	if *z2.Properties.NumberOfRecordSets != 4 || *z2.Properties.NumberOfVirtualNetworkLinks != 1 || *z2.Properties.NumberOfVirtualNetworkLinksWithRegistration != 1 {
		t.Fatalf("%+v", z2.Properties)
	}
	if tagged := must(zones.BeginUpdate(ctx, rg, "corp.example.com", armprivatedns.PrivateZone{Tags: map[string]*string{"k": to.Ptr("v")}}, nil)); tagged == nil {
		t.Fatal("no poller")
	} else if got := must(tagged.PollUntilDone(ctx, fast)); *got.Tags["k"] != "v" || *got.Properties.NumberOfRecordSets != 4 {
		t.Fatalf("%+v", got)
	}
	if n := count(zones.NewListByResourceGroupPager(rg, nil)); n != 1 {
		t.Fatalf("%d zones", n)
	}
	if n := count(zones.NewListPager(nil)); n != 1 {
		t.Fatalf("%d zones", n)
	}
	if n := count(links.NewListPager(rg, "corp.example.com", nil)); n != 1 {
		t.Fatalf("%d links", n)
	}

	// delete
	must(records.Delete(ctx, rg, "corp.example.com", armprivatedns.RecordTypeA, "db", nil))
	_, err = records.Get(ctx, rg, "corp.example.com", armprivatedns.RecordTypeA, "db", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(links.BeginDelete(ctx, rg, "corp.example.com", "link", nil)).PollUntilDone(ctx, fast))
	must(must(zones.BeginDelete(ctx, rg, "corp.example.com", nil)).PollUntilDone(ctx, fast))
	_, err = zones.Get(ctx, rg, "corp.example.com", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = records.Get(ctx, rg, "corp.example.com", armprivatedns.RecordTypeCNAME, "www", nil)
	wantErr(t, err, 404, "ResourceNotFound")
}

// count walks a pager and counts its items.
func count[T any](p *runtime.Pager[T]) (n int) {
	for p.More() {
		n += reflect.ValueOf(must(p.NextPage(ctx))).FieldByName("Value").Len()
	}
	return n
}

func TestPersistence(t *testing.T) {
	t.Parallel()
	cred, opts, srv := start(t)
	ips := must(armnetwork.NewPublicIPAddressesClient(sub, cred, opts))
	first := must(must(ips.BeginCreateOrUpdate(ctx, rg, "p", armnetwork.PublicIPAddress{Location: to.Ptr(loc)}, nil)).PollUntilDone(ctx, fast))
	svc := srv.Hub.Service("aznetwork").(core.Stateful)
	b := must(svc.Save())
	svc.Reset()
	if _, err := ips.Get(ctx, rg, "p", nil); err == nil {
		t.Fatal("reset left the address")
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if got := must(ips.Get(ctx, rg, "p", nil)); *got.Properties.IPAddress != *first.Properties.IPAddress {
		t.Fatal("the address did not come back")
	}
	// the next address after a restore is a new one
	second := must(must(ips.BeginCreateOrUpdate(ctx, rg, "q", armnetwork.PublicIPAddress{Location: to.Ptr(loc)}, nil)).PollUntilDone(ctx, fast))
	if *second.Properties.IPAddress == *first.Properties.IPAddress {
		t.Fatal("address reused")
	}
	if rows := srv.Hub.Service("aznetwork").(core.Browsable).Resources()[0].Rows; len(rows) != 2 {
		t.Fatal(rows)
	}
}
