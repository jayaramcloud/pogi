package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/analysisservices/armanalysisservices"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/datashare/armdatashare"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/deviceprovisioningservices/armdeviceprovisioningservices"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/digitaltwins/armdigitaltwins"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/elasticsan/armelasticsan"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/fabric/armfabric"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/hdinsight/armhdinsight"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/healthcareapis/armhealthcareapis/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/netapp/armnetapp/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/oracledatabase/armoracledatabase/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/powerbidedicated/armpowerbidedicated/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redisenterprise/armredisenterprise/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storagecache/armstoragecache/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storagemover/armstoragemover/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storagesync/armstoragesync/v2"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

var a2dServices = []string{"aznetapp", "azmanagedlustre", "azelasticsan", "azstoragemover", "azfilesync", "azdatashare",
	"azanalysisservices", "azhdinsight", "azpowerbiembedded", "azfabric", "azmanagedredis", "azoracle", "azhealthcare", "azdigitaltwins", "aziotdps"}

// a2dDone starts a long-running call and waits for it.
func a2dDone[T any](p *runtime.Poller[T], err error) T {
	return armtest.Must(armtest.Must(p, err).PollUntilDone(ctx, armtest.Fast))
}

func TestNetAppFiles(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	accts := armtest.Must(armnetapp.NewAccountsClient(sub, e.Cred, e.Opts))
	pools := armtest.Must(armnetapp.NewPoolsClient(sub, e.Cred, e.Opts))
	vols := armtest.Must(armnetapp.NewVolumesClient(sub, e.Cred, e.Opts))

	a := a2dDone(accts.BeginCreateOrUpdate(ctx, rg, "na1", armnetapp.Account{Location: to.Ptr(eus)}, nil)).Account
	if *a.Name != "na1" || *a.Type != "Microsoft.NetApp/netAppAccounts" {
		t.Fatalf("account: %+v", a)
	}
	pool := a2dDone(pools.BeginCreateOrUpdate(ctx, rg, "na1", "pool1", armnetapp.CapacityPool{Location: to.Ptr(eus),
		Properties: &armnetapp.PoolProperties{ServiceLevel: to.Ptr(armnetapp.ServiceLevelStandard), Size: to.Ptr(int64(4398046511104))}}, nil)).CapacityPool
	if *pool.Properties.QosType != armnetapp.QosTypeAuto || *pool.Properties.PoolID == "" {
		t.Fatalf("pool: %+v", pool.Properties)
	}
	v := a2dDone(vols.BeginCreateOrUpdate(ctx, rg, "na1", "pool1", "vol1", armnetapp.Volume{Location: to.Ptr(eus), Properties: &armnetapp.VolumeProperties{
		CreationToken: to.Ptr("vol1"), UsageThreshold: to.Ptr(int64(107374182400)), SubnetID: to.Ptr("/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Network/virtualNetworks/v/subnets/s")}}, nil)).Volume
	if len(v.Properties.MountTargets) != 1 || *v.Properties.MountTargets[0].IPAddress == "" || *v.Type != "Microsoft.NetApp/netAppAccounts/capacityPools/volumes" {
		t.Fatalf("volume: %+v", v.Properties)
	}
	if n := drain(t, pools.NewListPager(rg, "na1", nil), func(r armnetapp.PoolsClientListResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("pools: %d", n)
	}

	_, err := accts.BeginCreateOrUpdate(ctx, rg, "na2", armnetapp.Account{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = pools.BeginCreateOrUpdate(ctx, rg, "nonetapp", "pool1", armnetapp.CapacityPool{Location: to.Ptr(eus), Properties: &armnetapp.PoolProperties{
		ServiceLevel: to.Ptr(armnetapp.ServiceLevelStandard), Size: to.Ptr(int64(4398046511104))}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = pools.BeginCreateOrUpdate(ctx, rg, "na1", "pool2", armnetapp.CapacityPool{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent") // the pool's size and service level are required
	_, err = accts.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = vols.Get(ctx, rg, "na1", "pool1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(vols.BeginDelete(ctx, rg, "na1", "pool1", "vol1", nil))
	_, err = vols.Get(ctx, rg, "na1", "pool1", "vol1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestManagedLustre(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	c := armtest.Must(armstoragecache.NewAmlFilesystemsClient(sub, e.Cred, e.Opts))
	body := armstoragecache.AmlFilesystem{Location: to.Ptr(eus), Zones: []*string{to.Ptr("1")}, SKU: &armstoragecache.SKUName{Name: to.Ptr("AMLFS-Durable-Premium-125")},
		Properties: &armstoragecache.AmlFilesystemProperties{StorageCapacityTiB: to.Ptr(float32(16)),
			FilesystemSubnet:  to.Ptr("/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Network/virtualNetworks/v/subnets/s"),
			MaintenanceWindow: &armstoragecache.AmlFilesystemPropertiesMaintenanceWindow{DayOfWeek: to.Ptr(armstoragecache.MaintenanceDayOfWeekTypeFriday), TimeOfDayUTC: to.Ptr("22:00")}}}
	f := a2dDone(c.BeginCreateOrUpdate(ctx, rg, "lfs1", body, nil)).AmlFilesystem
	if *f.Properties.ProvisioningState != "Succeeded" || *f.Properties.ClientInfo.MgsAddress == "" || *f.Properties.Health.State != "Available" {
		t.Fatalf("filesystem: %+v", f.Properties)
	}
	_, err := c.BeginCreateOrUpdate(ctx, rg, "lfs2", armstoragecache.AmlFilesystem{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.BeginCreateOrUpdate(ctx, rg, "-bad-", body, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = c.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(c.BeginDelete(ctx, rg, "lfs1", nil))
	_, err = c.Get(ctx, rg, "lfs1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestElasticSAN(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	sans := armtest.Must(armelasticsan.NewElasticSansClient(sub, e.Cred, e.Opts))
	groups := armtest.Must(armelasticsan.NewVolumeGroupsClient(sub, e.Cred, e.Opts))
	vols := armtest.Must(armelasticsan.NewVolumesClient(sub, e.Cred, e.Opts))

	san := a2dDone(sans.BeginCreate(ctx, rg, "san1", armelasticsan.ElasticSan{Location: to.Ptr(eus), Properties: &armelasticsan.Properties{
		BaseSizeTiB: to.Ptr(int64(2)), ExtendedCapacitySizeTiB: to.Ptr(int64(0)), SKU: &armelasticsan.SKU{Name: to.Ptr(armelasticsan.SKUNamePremiumLRS)}}}, nil)).ElasticSan
	if *san.Properties.TotalSizeTiB != 2 || *san.Properties.TotalIops != 10000 {
		t.Fatalf("san: %+v", san.Properties)
	}
	armtest.Must(armtest.Must(groups.BeginCreate(ctx, rg, "san1", "vg1", armelasticsan.VolumeGroup{Properties: &armelasticsan.VolumeGroupProperties{
		ProtocolType: to.Ptr(armelasticsan.StorageTargetTypeIscsi)}}, nil)).PollUntilDone(ctx, armtest.Fast))
	v := a2dDone(vols.BeginCreate(ctx, rg, "san1", "vg1", "vol1", armelasticsan.Volume{Properties: &armelasticsan.VolumeProperties{SizeGiB: to.Ptr(int64(1))}}, nil)).Volume
	if *v.Properties.StorageTarget.TargetPortalPort != 3260 || *v.Properties.VolumeID == "" {
		t.Fatalf("volume: %+v", v.Properties)
	}

	_, err := sans.BeginCreate(ctx, rg, "san2", armelasticsan.ElasticSan{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = sans.BeginCreate(ctx, rg, "-", armelasticsan.ElasticSan{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = groups.BeginCreate(ctx, rg, "nosan", "vg1", armelasticsan.VolumeGroup{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = vols.Get(ctx, rg, "san1", "vg1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(vols.BeginDelete(ctx, rg, "san1", "vg1", "vol1", nil))
	_, err = vols.Get(ctx, rg, "san1", "vg1", "vol1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestStorageMover(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	ms := armtest.Must(armstoragemover.NewStorageMoversClient(sub, e.Cred, e.Opts))
	ps := armtest.Must(armstoragemover.NewProjectsClient(sub, e.Cred, e.Opts))
	es := armtest.Must(armstoragemover.NewEndpointsClient(sub, e.Cred, e.Opts))

	m := armtest.Must(ms.CreateOrUpdate(ctx, rg, "mover1", armstoragemover.StorageMover{Location: to.Ptr(eus)}, nil)).StorageMover
	if *m.Properties.ProvisioningState != armstoragemover.ProvisioningStateSucceeded {
		t.Fatalf("mover: %+v", m.Properties)
	}
	armtest.Must(ps.CreateOrUpdate(ctx, rg, "mover1", "proj1", armstoragemover.Project{Properties: &armstoragemover.ProjectProperties{Description: to.Ptr("p")}}, nil))
	ep := armtest.Must(es.CreateOrUpdate(ctx, rg, "mover1", "src", armstoragemover.Endpoint{Properties: &armstoragemover.NfsMountEndpointProperties{
		EndpointType: to.Ptr(armstoragemover.EndpointTypeNfsMount), Host: to.Ptr("192.168.0.1"), Export: to.Ptr("/export")}}, nil)).Endpoint
	if *ep.Type != "Microsoft.StorageMover/storageMovers/endpoints" {
		t.Fatalf("endpoint: %+v", ep)
	}

	_, err := ms.CreateOrUpdate(ctx, rg, "mover2", armstoragemover.StorageMover{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = ps.CreateOrUpdate(ctx, rg, "nomover", "proj1", armstoragemover.Project{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = ms.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(ps.BeginDelete(ctx, rg, "mover1", "proj1", nil))
	_, err = ps.Get(ctx, rg, "mover1", "proj1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestFileSync(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	ss := armtest.Must(armstoragesync.NewServicesClient(sub, e.Cred, e.Opts))
	gs := armtest.Must(armstoragesync.NewSyncGroupsClient(sub, e.Cred, e.Opts))

	s := a2dDone(ss.BeginCreate(ctx, rg, "sync1", armstoragesync.ServiceCreateParameters{Location: to.Ptr(eus), Properties: &armstoragesync.ServiceCreateParametersProperties{}}, nil)).Service
	if *s.Properties.StorageSyncServiceUID == "" {
		t.Fatalf("service: %+v", s.Properties)
	}
	armtest.Must(gs.Create(ctx, rg, "sync1", "group1", armstoragesync.SyncGroupCreateParameters{Properties: map[string]any{}}, nil))
	if n := drain(t, gs.NewListByStorageSyncServicePager(rg, "sync1", nil), func(r armstoragesync.SyncGroupsClientListByStorageSyncServiceResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("sync groups: %d", n)
	}

	_, err := ss.BeginCreate(ctx, rg, "sync2", armstoragesync.ServiceCreateParameters{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = gs.Create(ctx, rg, "nosync", "group1", armstoragesync.SyncGroupCreateParameters{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = ss.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = gs.Get(ctx, rg, "sync1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(gs.Delete(ctx, rg, "sync1", "group1", nil))
	_, err = gs.Get(ctx, rg, "sync1", "group1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestDataShare(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	as := armtest.Must(armdatashare.NewAccountsClient(sub, e.Cred, e.Opts))
	ss := armtest.Must(armdatashare.NewSharesClient(sub, e.Cred, e.Opts))

	a := a2dDone(as.BeginCreate(ctx, rg, "share1", armdatashare.Account{Location: to.Ptr(eus), Identity: &armdatashare.Identity{Type: to.Ptr(armdatashare.TypeSystemAssigned)}}, nil)).Account
	if *a.Properties.ProvisioningState != armdatashare.ProvisioningStateSucceeded || *a.Identity.PrincipalID == "" {
		t.Fatalf("account: %+v", a)
	}
	sh := armtest.Must(ss.Create(ctx, rg, "share1", "s1", armdatashare.Share{Properties: &armdatashare.ShareProperties{ShareKind: to.Ptr(armdatashare.ShareKindCopyBased), Description: to.Ptr("d")}}, nil)).Share
	if *sh.Name != "s1" || *sh.Properties.Description != "d" {
		t.Fatalf("share: %+v", sh)
	}

	_, err := as.BeginCreate(ctx, rg, "share2", armdatashare.Account{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = ss.Create(ctx, rg, "noaccount", "s1", armdatashare.Share{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = as.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = ss.Get(ctx, rg, "share1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(ss.BeginDelete(ctx, rg, "share1", "s1", nil))
	_, err = ss.Get(ctx, rg, "share1", "s1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestAnalysisServices(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	c := armtest.Must(armanalysisservices.NewServersClient(sub, e.Cred, e.Opts))
	srv := a2dDone(c.BeginCreate(ctx, rg, "aas1", armanalysisservices.Server{Location: to.Ptr(eus), SKU: &armanalysisservices.ResourceSKU{Name: to.Ptr("B1")},
		Properties: &armanalysisservices.ServerProperties{AsAdministrators: &armanalysisservices.ServerAdministrators{Members: []*string{to.Ptr("admin@pogi.test")}}}}, nil)).Server
	if *srv.Properties.ServerFullName != "asazure://eastus.asazure.windows.net/aas1" || *srv.Properties.State != armanalysisservices.StateSucceeded {
		t.Fatalf("server: %+v", srv.Properties)
	}
	_, err := c.BeginCreate(ctx, rg, "aas2", armanalysisservices.Server{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.BeginCreate(ctx, rg, "Bad-Name", armanalysisservices.Server{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = c.GetDetails(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(c.BeginDelete(ctx, rg, "aas1", nil))
	_, err = c.GetDetails(ctx, rg, "aas1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestHDInsight(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	cs := armtest.Must(armhdinsight.NewClustersClient(sub, e.Cred, e.Opts))
	cfgs := armtest.Must(armhdinsight.NewConfigurationsClient(sub, e.Cred, e.Opts))

	body := armhdinsight.ClusterCreateParametersExtended{Location: to.Ptr(eus), Properties: &armhdinsight.ClusterCreateProperties{
		ClusterVersion: to.Ptr("5.1"), OSType: to.Ptr(armhdinsight.OSTypeLinux), Tier: to.Ptr(armhdinsight.TierStandard),
		ClusterDefinition: &armhdinsight.ClusterDefinition{Kind: to.Ptr("hadoop"), Configurations: map[string]any{
			"gateway": map[string]any{"restAuthCredential.isEnabled": true, "restAuthCredential.username": "gw", "restAuthCredential.password": "Secret123!"}}},
		StorageProfile: &armhdinsight.StorageProfile{Storageaccounts: []*armhdinsight.StorageAccount{{Name: to.Ptr("acct.blob.core.windows.net"), IsDefault: to.Ptr(true),
			Container: to.Ptr("c1"), Key: to.Ptr("a2V5")}}}}}
	c := a2dDone(cs.BeginCreate(ctx, rg, "hdi1", body, nil)).Cluster
	if *c.Properties.ClusterState != "Running" || len(c.Properties.ConnectivityEndpoints) != 2 || *c.Properties.ConnectivityEndpoints[0].Location != "hdi1.azurehdinsight.net" {
		t.Fatalf("cluster: %+v", c.Properties)
	}
	g := armtest.Must(cfgs.Get(ctx, rg, "hdi1", "gateway", nil))
	if *g.Value["restAuthCredential.username"] != "gw" || *g.Value["restAuthCredential.isEnabled"] != "true" {
		t.Fatalf("gateway: %+v", g.Value)
	}
	core := armtest.Must(cfgs.Get(ctx, rg, "hdi1", "core-site", nil))
	if *core.Value["fs.defaultFS"] != "wasbs://c1@acct.blob.core.windows.net" {
		t.Fatalf("core-site: %+v", core.Value)
	}
	if c.Properties.ClusterDefinition.Configurations != nil {
		t.Fatalf("passwords must stay out of the cluster: %v", c.Properties.ClusterDefinition.Configurations)
	}

	_, err := cs.BeginCreate(ctx, rg, "hdi2", armhdinsight.ClusterCreateParametersExtended{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = cs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = cfgs.Get(ctx, rg, "nope", "gateway", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(cs.BeginDelete(ctx, rg, "hdi1", nil))
	_, err = cs.Get(ctx, rg, "hdi1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestPowerBIEmbeddedAndFabric(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	pb := armtest.Must(armpowerbidedicated.NewCapacitiesClient(sub, e.Cred, e.Opts))
	c := a2dDone(pb.BeginCreate(ctx, rg, "pbi1", armpowerbidedicated.DedicatedCapacity{Location: to.Ptr(eus), SKU: &armpowerbidedicated.CapacitySKU{Name: to.Ptr("A1")},
		Properties: &armpowerbidedicated.DedicatedCapacityProperties{Administration: &armpowerbidedicated.DedicatedCapacityAdministrators{Members: []*string{to.Ptr("admin@pogi.test")}}}}, nil)).DedicatedCapacity
	if string(*c.Properties.State) != "Succeeded" {
		t.Fatalf("capacity: %+v", c.Properties)
	}
	_, err := pb.BeginCreate(ctx, rg, "pbi2", armpowerbidedicated.DedicatedCapacity{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = pb.BeginCreate(ctx, rg, "Bad-Name", armpowerbidedicated.DedicatedCapacity{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = pb.GetDetails(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	fc := armtest.Must(armfabric.NewCapacitiesClient(sub, e.Cred, e.Opts))
	f := a2dDone(fc.BeginCreateOrUpdate(ctx, rg, "fab1", armfabric.Capacity{Location: to.Ptr(eus), SKU: &armfabric.RpSKU{Name: to.Ptr("F2"), Tier: to.Ptr(armfabric.RpSKUTierFabric)},
		Properties: &armfabric.CapacityProperties{Administration: &armfabric.CapacityAdministration{Members: []*string{to.Ptr("admin@pogi.test")}}}}, nil)).Capacity
	if string(*f.Properties.State) != "Active" || string(*f.Properties.ProvisioningState) != "Succeeded" {
		t.Fatalf("fabric capacity: %+v", f.Properties)
	}
	_, err = fc.BeginCreateOrUpdate(ctx, rg, "fab2", armfabric.Capacity{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = fc.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(fc.BeginDelete(ctx, rg, "fab1", nil))
	_, err = fc.Get(ctx, rg, "fab1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestManagedRedis(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	cs := armtest.Must(armredisenterprise.NewClient(sub, e.Cred, e.Opts))
	dbs := armtest.Must(armredisenterprise.NewDatabasesClient(sub, e.Cred, e.Opts))

	c := a2dDone(cs.BeginCreate(ctx, rg, "redis1", armredisenterprise.Cluster{Location: to.Ptr(eus), SKU: &armredisenterprise.SKU{Name: to.Ptr(armredisenterprise.SKUNameBalancedB0)}}, nil)).Cluster
	if *c.Properties.ResourceState != armredisenterprise.ResourceStateRunning || *c.Properties.HostName != "redis1.eastus.redis.azure.net" {
		t.Fatalf("cluster: %+v", c.Properties)
	}
	db := a2dDone(dbs.BeginCreate(ctx, rg, "redis1", "default", armredisenterprise.Database{Properties: &armredisenterprise.DatabaseCreateProperties{}}, nil)).Database
	if *db.Properties.Port != 10000 || *db.Properties.ResourceState != armredisenterprise.ResourceStateRunning {
		t.Fatalf("database: %+v", db.Properties)
	}
	k1 := armtest.Must(dbs.ListKeys(ctx, rg, "redis1", "default", nil))
	k2 := armtest.Must(dbs.ListKeys(ctx, rg, "redis1", "default", nil))
	if *k1.PrimaryKey == "" || *k1.PrimaryKey == *k1.SecondaryKey || *k1.PrimaryKey != *k2.PrimaryKey {
		t.Fatal("keys must be two different values that stay the same across calls")
	}

	_, err := cs.BeginCreate(ctx, rg, "redis2", armredisenterprise.Cluster{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = dbs.BeginCreate(ctx, rg, "noredis", "default", armredisenterprise.Database{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = dbs.ListKeys(ctx, rg, "redis1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = cs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(dbs.BeginDelete(ctx, rg, "redis1", "default", nil))
	_, err = dbs.Get(ctx, rg, "redis1", "default", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestOracleDatabaseAtAzure(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	c := armtest.Must(armoracledatabase.NewCloudExadataInfrastructuresClient(sub, e.Cred, e.Opts))
	body := armoracledatabase.CloudExadataInfrastructure{Location: to.Ptr(eus), Zones: []*string{to.Ptr("1")}, Properties: &armoracledatabase.CloudExadataInfrastructureProperties{
		ComputeCount: to.Ptr(int32(2)), StorageCount: to.Ptr(int32(3)), Shape: to.Ptr("Exadata.X9M"), DisplayName: to.Ptr("exa1")}}
	x := a2dDone(c.BeginCreateOrUpdate(ctx, rg, "exa1", body, nil)).CloudExadataInfrastructure
	if string(*x.Properties.LifecycleState) != "Available" {
		t.Fatalf("exadata: %+v", x.Properties)
	}
	_, err := c.BeginCreateOrUpdate(ctx, rg, "exa2", armoracledatabase.CloudExadataInfrastructure{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.BeginCreateOrUpdate(ctx, rg, "exa3", armoracledatabase.CloudExadataInfrastructure{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent") // the properties are required
	_, err = c.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(c.BeginDelete(ctx, rg, "exa1", nil))
	_, err = c.Get(ctx, rg, "exa1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestHealthDataServices(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	ws := armtest.Must(armhealthcareapis.NewWorkspacesClient(sub, e.Cred, e.Opts))
	fhir := armtest.Must(armhealthcareapis.NewFhirServicesClient(sub, e.Cred, e.Opts))
	dicom := armtest.Must(armhealthcareapis.NewDicomServicesClient(sub, e.Cred, e.Opts))

	w := a2dDone(ws.BeginCreateOrUpdate(ctx, rg, "hw1", armhealthcareapis.Workspace{Location: to.Ptr(eus)}, nil)).Workspace
	if *w.Properties.ProvisioningState != armhealthcareapis.ProvisioningStateSucceeded {
		t.Fatalf("workspace: %+v", w.Properties)
	}
	a2dDone(fhir.BeginCreateOrUpdate(ctx, rg, "hw1", "fhir1", armhealthcareapis.FhirService{Location: to.Ptr(eus), Kind: to.Ptr(armhealthcareapis.FhirServiceKindFhirR4)}, nil))
	d := a2dDone(dicom.BeginCreateOrUpdate(ctx, rg, "hw1", "dicom1", armhealthcareapis.DicomService{Location: to.Ptr(eus)}, nil)).DicomService
	if *d.Properties.ServiceURL != "https://hw1-dicom1.dicom.azurehealthcareapis.com" {
		t.Fatalf("dicom: %+v", d.Properties)
	}

	_, err := ws.BeginCreateOrUpdate(ctx, rg, "hw2", armhealthcareapis.Workspace{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = ws.BeginCreateOrUpdate(ctx, rg, "ab", armhealthcareapis.Workspace{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName") // 3 to 24 characters
	_, err = fhir.BeginCreateOrUpdate(ctx, rg, "nows", "fhir1", armhealthcareapis.FhirService{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = dicom.Get(ctx, rg, "hw1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(fhir.BeginDelete(ctx, rg, "fhir1", "hw1", nil)) // this client takes the service's name before the workspace's
	_, err = fhir.Get(ctx, rg, "hw1", "fhir1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestDigitalTwins(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	c := armtest.Must(armdigitaltwins.NewClient(sub, e.Cred, e.Opts))
	d := a2dDone(c.BeginCreateOrUpdate(ctx, rg, "twins1", armdigitaltwins.Description{Location: to.Ptr(eus)}, nil)).Description
	if *d.Properties.HostName != "twins1.api.eastus.digitaltwins.azure.net" || string(*d.Properties.ProvisioningState) != "Succeeded" {
		t.Fatalf("instance: %+v", d.Properties)
	}
	_, err := c.BeginCreateOrUpdate(ctx, rg, "twins2", armdigitaltwins.Description{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.BeginCreateOrUpdate(ctx, rg, "tw", armdigitaltwins.Description{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName") // 3 to 63 characters
	_, err = c.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(c.BeginDelete(ctx, rg, "twins1", nil))
	_, err = c.Get(ctx, rg, "twins1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestIoTHubDeviceProvisioning(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, a2dServices...)
	c := armtest.Must(armdeviceprovisioningservices.NewIotDpsResourceClient(sub, e.Cred, e.Opts))
	body := armdeviceprovisioningservices.ProvisioningServiceDescription{Location: to.Ptr(eus), Properties: &armdeviceprovisioningservices.IotDpsPropertiesDescription{},
		SKU: &armdeviceprovisioningservices.IotDpsSKUInfo{Name: to.Ptr(armdeviceprovisioningservices.IotDpsSKUS1), Capacity: to.Ptr(int64(1))}}
	d := a2dDone(c.BeginCreateOrUpdate(ctx, rg, "dps1", body, nil)).ProvisioningServiceDescription
	if *d.Properties.ServiceOperationsHostName != "dps1.azure-devices-provisioning.net" || !strings.HasPrefix(*d.Properties.IDScope, "0ne") || *d.Properties.State != armdeviceprovisioningservices.StateActive {
		t.Fatalf("service: %+v", d.Properties)
	}
	var keys []*armdeviceprovisioningservices.SharedAccessSignatureAuthorizationRuleAccessRightsDescription
	for pg := c.NewListKeysPager("dps1", rg, nil); pg.More(); {
		keys = append(keys, armtest.Must(pg.NextPage(ctx)).Value...)
	}
	if len(keys) != 1 || *keys[0].KeyName != "provisioningserviceowner" || *keys[0].PrimaryKey == *keys[0].SecondaryKey {
		t.Fatalf("keys: %+v", keys)
	}

	_, err := c.BeginCreateOrUpdate(ctx, rg, "dps2", armdeviceprovisioningservices.ProvisioningServiceDescription{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.Get(ctx, "nope", rg, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	a2dDone(c.BeginDelete(ctx, "dps1", rg, nil))
	_, err = c.Get(ctx, "dps1", rg, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}
