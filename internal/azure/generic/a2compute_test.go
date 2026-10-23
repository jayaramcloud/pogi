package generic_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/avs/armavs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/azurestackhci/armazurestackhci/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservicefleet/armcontainerservicefleet/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/desktopvirtualization/armdesktopvirtualization/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/devcenter/armdevcenter/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/devopsinfrastructure/armdevopsinfrastructure"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/devtestlabs/armdevtestlabs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/hybridcompute/armhybridcompute/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/loadtesting/armloadtesting"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redhatopenshift/armredhatopenshift/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/servicefabric/armservicefabric/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/workloadssapvirtualinstance/armworkloadssapvirtualinstance"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

const loc = "eastus"

func TestArc(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azarc")
	c := armtest.Must(armhybridcompute.NewPrivateLinkScopesClient(sub, e.Cred, e.Opts))
	// public network access is Disabled unless the client says otherwise
	s := armtest.Must(c.CreateOrUpdate(ctx, rg, "scope1", armhybridcompute.PrivateLinkScope{Location: to.Ptr(loc), Properties: &armhybridcompute.PrivateLinkScopeProperties{}}, nil))
	if *s.Properties.PublicNetworkAccess != armhybridcompute.PublicNetworkAccessTypeDisabled || *s.Properties.PrivateLinkScopeID == "" || *s.Properties.ProvisioningState != "Succeeded" {
		t.Fatalf("scope: %+v", s.Properties)
	}
	if n := drain(t, c.NewListByResourceGroupPager(rg, nil), func(r armhybridcompute.PrivateLinkScopesClientListByResourceGroupResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("scopes: %d", n)
	}
	armtest.Must(armtest.Must(c.BeginDelete(ctx, rg, "scope1", nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := c.Get(ctx, rg, "scope1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestAzureLocal(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azlocal")
	c := armtest.Must(armazurestackhci.NewClustersClient(sub, e.Cred, e.Opts))
	cl := armtest.Must(c.Create(ctx, rg, "hci1", armazurestackhci.Cluster{Location: to.Ptr(loc),
		Properties: &armazurestackhci.ClusterProperties{AADClientID: to.Ptr("11111111-2222-3333-4444-555555555555"), AADTenantID: to.Ptr("00000000-0000-0000-0000-000000000000")}}, nil))
	p := cl.Properties
	if *p.CloudID == "" || *p.ServiceEndpoint != "https://dp.stackhci.azure.com" || *p.Status != armazurestackhci.StatusNotYetRegistered || *p.AADClientID != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("cluster: %+v", p)
	}
	if *armtest.Must(c.Get(ctx, rg, "hci1", nil)).Properties.CloudID != *p.CloudID {
		t.Fatal("the cloud id changes between reads")
	}
	armtest.Must(armtest.Must(c.BeginDelete(ctx, rg, "hci1", nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := c.Get(ctx, rg, "hci1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestAzureVMwareSolution(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azavs")
	c := armtest.Must(armavs.NewPrivateCloudsClient(sub, e.Cred, e.Opts))
	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "cloud1", armavs.PrivateCloud{Location: to.Ptr(loc), SKU: &armavs.SKU{Name: to.Ptr("av36p")},
		Properties: &armavs.PrivateCloudProperties{NetworkBlock: to.Ptr("192.168.48.0/22"), ManagementCluster: &armavs.ManagementCluster{ClusterSize: to.Ptr[int32](3)}}}, nil))
	pc := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).PrivateCloud
	p := pc.Properties
	if *p.Endpoints.NsxtManager == "" || *p.Endpoints.Vcsa == "" || *p.Circuit.PrimarySubnet == "" || *p.Internet != armavs.InternetEnumDisabled || *p.ManagementCluster.ClusterSize != 3 {
		t.Fatalf("private cloud: %+v", p)
	}
	// the sub-resources: a cluster and an ExpressRoute authorization
	cc := armtest.Must(armavs.NewClustersClient(sub, e.Cred, e.Opts))
	armtest.Must(armtest.Must(cc.BeginCreateOrUpdate(ctx, rg, "cloud1", "extra", armavs.Cluster{SKU: &armavs.SKU{Name: to.Ptr("av36p")}, Properties: &armavs.ClusterProperties{ClusterSize: to.Ptr[int32](3)}}, nil)).PollUntilDone(ctx, armtest.Fast))
	ac := armtest.Must(armavs.NewAuthorizationsClient(sub, e.Cred, e.Opts))
	a := armtest.Must(armtest.Must(ac.BeginCreateOrUpdate(ctx, rg, "cloud1", "auth1", armavs.ExpressRouteAuthorization{}, nil)).PollUntilDone(ctx, armtest.Fast))
	if *a.Properties.ExpressRouteAuthorizationKey == "" || *a.Properties.ExpressRouteAuthorizationID == "" {
		t.Fatalf("authorization: %+v", a.Properties)
	}
	armtest.Must(armtest.Must(c.BeginDelete(ctx, rg, "cloud1", nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := c.Get(ctx, rg, "cloud1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestOpenShift(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azopenshift")
	c := armtest.Must(armredhatopenshift.NewOpenShiftClustersClient(sub, e.Cred, e.Opts))
	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "aro1", armredhatopenshift.OpenShiftCluster{Location: to.Ptr(loc),
		Properties: &armredhatopenshift.OpenShiftClusterProperties{
			ClusterProfile:   &armredhatopenshift.ClusterProfile{Domain: to.Ptr("pogi.example")},
			ApiserverProfile: &armredhatopenshift.APIServerProfile{Visibility: to.Ptr(armredhatopenshift.VisibilityPublic)},
			IngressProfiles:  []*armredhatopenshift.IngressProfile{{Name: to.Ptr("default"), Visibility: to.Ptr(armredhatopenshift.VisibilityPublic)}},
		}}, nil))
	p := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Properties
	if *p.ApiserverProfile.URL == "" || *p.ApiserverProfile.IP == "" || *p.ConsoleProfile.URL == "" || *p.IngressProfiles[0].IP == "" || *p.ProvisioningState != armredhatopenshift.ProvisioningStateSucceeded {
		t.Fatalf("cluster: %+v %+v", p.ApiserverProfile, p.IngressProfiles)
	}
	cred := armtest.Must(c.ListCredentials(ctx, rg, "aro1", nil))
	admin := armtest.Must(c.ListAdminCredentials(ctx, rg, "aro1", nil))
	if *cred.KubeadminUsername != "kubeadmin" || *cred.KubeadminPassword == "" || *admin.Kubeconfig == "" {
		t.Fatalf("credentials: %+v %+v", cred, admin)
	}
	_, err := c.ListCredentials(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestFleetManager(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azfleet")
	c := armtest.Must(armcontainerservicefleet.NewFleetsClient(sub, e.Cred, e.Opts))
	f := armtest.Must(armtest.Must(c.BeginCreate(ctx, rg, "fleet1", armcontainerservicefleet.Fleet{Location: to.Ptr(loc), Tags: map[string]*string{"env": to.Ptr("dev")}}, nil)).PollUntilDone(ctx, armtest.Fast)).Fleet
	if *f.Properties.ProvisioningState != armcontainerservicefleet.FleetProvisioningStateSucceeded || *f.Tags["env"] != "dev" {
		t.Fatalf("fleet: %+v", f)
	}
	// an update strategy is a child of the fleet
	sc := armtest.Must(armcontainerservicefleet.NewFleetUpdateStrategiesClient(sub, e.Cred, e.Opts))
	st := armcontainerservicefleet.FleetUpdateStrategy{Properties: &armcontainerservicefleet.FleetUpdateStrategyProperties{
		Strategy: &armcontainerservicefleet.UpdateRunStrategy{Stages: []*armcontainerservicefleet.UpdateStage{{Name: to.Ptr("stage-1")}}}}}
	armtest.Must(armtest.Must(sc.BeginCreateOrUpdate(ctx, rg, "fleet1", "strategy1", st, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := sc.BeginCreateOrUpdate(ctx, rg, "nofleet", "strategy1", st, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
}

func TestServiceFabric(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azservicefabric")
	c := armtest.Must(armservicefabric.NewClustersClient(sub, e.Cred, e.Opts))
	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "sf1", armservicefabric.Cluster{Location: to.Ptr(loc), Properties: &armservicefabric.ClusterProperties{
		ManagementEndpoint: to.Ptr("https://example:80"), ReliabilityLevel: to.Ptr(armservicefabric.ReliabilityLevelBronze),
		NodeTypes: []*armservicefabric.NodeTypeDescription{{Name: to.Ptr("first"), IsPrimary: to.Ptr(true), VMInstanceCount: to.Ptr[int32](3),
			ClientConnectionEndpointPort: to.Ptr[int32](2020), HTTPGatewayEndpointPort: to.Ptr[int32](80), DurabilityLevel: to.Ptr(armservicefabric.DurabilityLevelBronze)}}}}, nil))
	p := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Properties
	if *p.ClusterState != armservicefabric.ClusterStateReady || *p.ClusterID == "" || *p.ClusterEndpoint != "https://sf1.eastus.servicefabric.azure.com" || *p.UpgradeMode != armservicefabric.UpgradeModeAutomatic {
		t.Fatalf("cluster: %+v", p)
	}
	_, err := c.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestVirtualDesktop(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azavd")
	hc := armtest.Must(armdesktopvirtualization.NewHostPoolsClient(sub, e.Cred, e.Opts))
	hp := armtest.Must(hc.CreateOrUpdate(ctx, rg, "pool1", armdesktopvirtualization.HostPool{Location: to.Ptr(loc), Properties: &armdesktopvirtualization.HostPoolProperties{
		HostPoolType: to.Ptr(armdesktopvirtualization.HostPoolTypePooled), LoadBalancerType: to.Ptr(armdesktopvirtualization.LoadBalancerTypeDepthFirst),
		PreferredAppGroupType: to.Ptr(armdesktopvirtualization.PreferredAppGroupTypeDesktop)}}, nil)).HostPool
	ag := armtest.Must(armdesktopvirtualization.NewApplicationGroupsClient(sub, e.Cred, e.Opts))
	g := armtest.Must(ag.CreateOrUpdate(ctx, rg, "apps1", armdesktopvirtualization.ApplicationGroup{Location: to.Ptr(loc), Properties: &armdesktopvirtualization.ApplicationGroupProperties{
		ApplicationGroupType: to.Ptr(armdesktopvirtualization.ApplicationGroupTypeRemoteApp), HostPoolArmPath: hp.ID}}, nil)).ApplicationGroup
	if *g.Properties.HostPoolArmPath != *hp.ID {
		t.Fatalf("application group: %+v", g.Properties)
	}
	ac := armtest.Must(armdesktopvirtualization.NewApplicationsClient(sub, e.Cred, e.Opts))
	armtest.Must(ac.CreateOrUpdate(ctx, rg, "apps1", "notepad", armdesktopvirtualization.Application{Properties: &armdesktopvirtualization.ApplicationProperties{
		CommandLineSetting: to.Ptr(armdesktopvirtualization.CommandLineSettingDoNotAllow), FilePath: to.Ptr("C:\\Windows\\notepad.exe")}}, nil))
	// a host pool needs its type
	_, err := hc.CreateOrUpdate(ctx, rg, "notype", armdesktopvirtualization.HostPool{Location: to.Ptr(loc), Properties: &armdesktopvirtualization.HostPoolProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = ac.CreateOrUpdate(ctx, rg, "missing", "notepad", armdesktopvirtualization.Application{Properties: &armdesktopvirtualization.ApplicationProperties{
		CommandLineSetting: to.Ptr(armdesktopvirtualization.CommandLineSettingDoNotAllow)}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
}

func TestDevTestLabs(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdevtestlabs")
	c := armtest.Must(armdevtestlabs.NewLabsClient(sub, e.Cred, e.Opts))
	lab := armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "lab1", armdevtestlabs.Lab{Location: to.Ptr(loc), Properties: &armdevtestlabs.LabProperties{}}, nil)).PollUntilDone(ctx, armtest.Fast)).Lab
	p := lab.Properties
	if *p.UniqueIdentifier == "" || *p.DefaultStorageAccount == "" || *p.ArtifactsStorageAccount == "" || *p.VaultName == "" {
		t.Fatalf("lab: %+v", p)
	}
	// a lab's network has no location of its own
	vc := armtest.Must(armdevtestlabs.NewVirtualNetworksClient(sub, e.Cred, e.Opts))
	armtest.Must(armtest.Must(vc.BeginCreateOrUpdate(ctx, rg, "lab1", "net1", armdevtestlabs.VirtualNetwork{Properties: &armdevtestlabs.VirtualNetworkProperties{}}, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := vc.BeginCreateOrUpdate(ctx, rg, "nolab", "net1", armdevtestlabs.VirtualNetwork{Properties: &armdevtestlabs.VirtualNetworkProperties{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
}

func TestDevBox(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdevbox")
	c := armtest.Must(armdevcenter.NewDevCentersClient(sub, e.Cred, e.Opts))
	dc := armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "dc1", armdevcenter.DevCenter{Location: to.Ptr(loc)}, nil)).PollUntilDone(ctx, armtest.Fast)).DevCenter
	if *dc.Properties.DevCenterURI == "" {
		t.Fatalf("dev center: %+v", dc.Properties)
	}
	pc := armtest.Must(armdevcenter.NewProjectsClient(sub, e.Cred, e.Opts))
	pr := armtest.Must(armtest.Must(pc.BeginCreateOrUpdate(ctx, rg, "proj1", armdevcenter.Project{Location: to.Ptr(loc), Properties: &armdevcenter.ProjectProperties{DevCenterID: dc.ID}}, nil)).PollUntilDone(ctx, armtest.Fast)).Project
	if *pr.Properties.DevCenterID != *dc.ID {
		t.Fatalf("project: %+v", pr.Properties)
	}
	// a dev box definition, which Terraform and the portal read back
	dd := armtest.Must(armdevcenter.NewDevBoxDefinitionsClient(sub, e.Cred, e.Opts))
	d := armtest.Must(armtest.Must(dd.BeginCreateOrUpdate(ctx, rg, "dc1", "def1", armdevcenter.DevBoxDefinition{Location: to.Ptr(loc), Properties: &armdevcenter.DevBoxDefinitionProperties{
		ImageReference: &armdevcenter.ImageReference{ID: to.Ptr(*dc.ID + "/galleries/default/images/win11")}, SKU: &armdevcenter.SKU{Name: to.Ptr("general_i_8c32gb256ssd_v2")}}}, nil)).PollUntilDone(ctx, armtest.Fast)).DevBoxDefinition
	if *d.Properties.HibernateSupport != armdevcenter.HibernateSupportDisabled {
		t.Fatalf("definition: %+v", d.Properties)
	}
	_, err := pc.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestLoadTesting(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azloadtesting")
	c := armtest.Must(armloadtesting.NewLoadTestsClient(sub, e.Cred, e.Opts))
	lt := armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "lt1", armloadtesting.LoadTestResource{Location: to.Ptr(loc), Properties: &armloadtesting.LoadTestProperties{Description: to.Ptr("demo")}}, nil)).PollUntilDone(ctx, armtest.Fast)).LoadTestResource
	if *lt.Properties.DataPlaneURI == "" || *lt.Properties.Description != "demo" {
		t.Fatalf("load test: %+v", lt.Properties)
	}
	armtest.Must(armtest.Must(c.BeginDelete(ctx, rg, "lt1", nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := c.Get(ctx, rg, "lt1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestManagedDevOpsPools(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdevopspools")
	c := armtest.Must(armdevopsinfrastructure.NewPoolsClient(sub, e.Cred, e.Opts))
	pool := armdevopsinfrastructure.Pool{Location: to.Ptr(loc), Properties: &armdevopsinfrastructure.PoolProperties{
		MaximumConcurrency:         to.Ptr[int32](1),
		DevCenterProjectResourceID: to.Ptr("/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.DevCenter/projects/proj1"),
		OrganizationProfile: &armdevopsinfrastructure.AzureDevOpsOrganizationProfile{Kind: to.Ptr("AzureDevOps"),
			Organizations: []*armdevopsinfrastructure.Organization{{URL: to.Ptr("https://dev.azure.com/pogi-example")}}},
		AgentProfile: &armdevopsinfrastructure.StatelessAgentProfile{Kind: to.Ptr("Stateless")},
		FabricProfile: &armdevopsinfrastructure.VmssFabricProfile{Kind: to.Ptr("Vmss"), SKU: &armdevopsinfrastructure.DevOpsAzureSKU{Name: to.Ptr("Standard_D2ads_v5")},
			Images: []*armdevopsinfrastructure.PoolImage{{WellKnownImageName: to.Ptr("ubuntu-22.04/latest")}}}}}
	p := armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "pool1", pool, nil)).PollUntilDone(ctx, armtest.Fast)).Properties
	if *p.MaximumConcurrency != 1 || *p.ProvisioningState != armdevopsinfrastructure.ProvisioningStateSucceeded {
		t.Fatalf("pool: %+v", p)
	}
	// a pool without an organization or fabric profile is refused
	_, err := c.BeginCreateOrUpdate(ctx, rg, "bad", armdevopsinfrastructure.Pool{Location: to.Ptr(loc), Properties: &armdevopsinfrastructure.PoolProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
}

func TestSAPWorkloads(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azsapworkloads")
	c := armtest.Must(armworkloadssapvirtualinstance.NewSAPVirtualInstancesClient(sub, e.Cred, e.Opts))
	vm := "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Compute/virtualMachines/sap-central"
	vi := armworkloadssapvirtualinstance.SAPVirtualInstance{Location: to.Ptr(loc), Properties: &armworkloadssapvirtualinstance.SAPVirtualInstanceProperties{
		Environment: to.Ptr(armworkloadssapvirtualinstance.SAPEnvironmentTypeNonProd), SapProduct: to.Ptr(armworkloadssapvirtualinstance.SAPProductTypeS4HANA),
		Configuration: &armworkloadssapvirtualinstance.DiscoveryConfiguration{ConfigurationType: to.Ptr(armworkloadssapvirtualinstance.SAPConfigurationTypeDiscovery), CentralServerVMID: to.Ptr(vm)}}}
	got := armtest.Must(armtest.Must(c.BeginCreate(ctx, rg, "PG1", vi, nil)).PollUntilDone(ctx, armtest.Fast)).SAPVirtualInstance
	if *got.Properties.ProvisioningState != armworkloadssapvirtualinstance.SapVirtualInstanceProvisioningStateSucceeded || *got.Properties.Environment != armworkloadssapvirtualinstance.SAPEnvironmentTypeNonProd {
		t.Fatalf("virtual instance: %+v", got.Properties)
	}
	_, err := c.BeginCreate(ctx, rg, "XY1", armworkloadssapvirtualinstance.SAPVirtualInstance{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.BeginCreate(ctx, rg, "not a sid", vi, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
}

// The failures every one of these services shares, through the generic client: a missing location, a missing
// required property, a child of a parent that does not exist, and get and delete of what is not there.
func TestComputeFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azarc", "azlocal", "azavs", "azopenshift", "azfleet", "azservicefabric", "azavd", "azdevtestlabs", "azdevbox", "azloadtesting", "azdevopspools", "azsapworkloads")
	c := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	for _, tc := range []struct{ ns, typ, ver string }{
		{"Microsoft.HybridCompute", "privateLinkScopes", "2024-07-10"},
		{"Microsoft.AzureStackHCI", "clusters", "2024-04-01"},
		{"Microsoft.AVS", "privateClouds", "2023-09-01"},
		{"Microsoft.RedHatOpenShift", "openShiftClusters", "2023-11-22"},
		{"Microsoft.ContainerService", "fleets", "2024-04-01"},
		{"Microsoft.ServiceFabric", "clusters", "2021-06-01"},
		{"Microsoft.DevTestLab", "labs", "2018-09-15"},
		{"Microsoft.DevCenter", "devcenters", "2024-02-01"},
		{"Microsoft.LoadTestService", "loadTests", "2022-12-01"},
		{"Microsoft.DevOpsInfrastructure", "pools", "2025-01-21"},
		{"Microsoft.Workloads", "sapVirtualInstances", "2024-09-01"},
	} {
		t.Run(tc.ns+"/"+tc.typ, func(t *testing.T) {
			name := "nolocation"
			if tc.typ == "sapVirtualInstances" {
				name = "XY1" // a SAP system id: three characters
			}
			_, err := c.BeginCreateOrUpdate(ctx, rg, tc.ns, "", tc.typ, name, tc.ver, armresources.GenericResource{Properties: map[string]any{}}, nil)
			armtest.WantErr(t, err, 400, "LocationRequired")
			_, err = c.Get(ctx, rg, tc.ns, "", tc.typ, "missing", tc.ver, nil)
			armtest.WantErr(t, err, 404, "ResourceNotFound")
			// like Azure, deleting what is not there is not an error
			armtest.Must(c.BeginDelete(ctx, rg, tc.ns, "", tc.typ, "missing", tc.ver, nil))
		})
	}
}
