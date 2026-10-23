package generic_test

import (
	"bytes"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/cloudquotas/apiv1"
	"cloud.google.com/go/cloudquotas/apiv1/cloudquotaspb"
	"cloud.google.com/go/edgecontainer/apiv1"
	"cloud.google.com/go/edgecontainer/apiv1/edgecontainerpb"
	"cloud.google.com/go/gkemulticloud/apiv1/gkemulticloudpb"
	"github.com/nrndev/pogi"
	"google.golang.org/api/appengine/v1"
	"google.golang.org/api/config/v1"
	"google.golang.org/api/domains/v1"
	"google.golang.org/api/gkebackup/v1"
	"google.golang.org/api/hypercomputecluster/v1"
	"google.golang.org/api/networkmanagement/v1"
	"google.golang.org/api/networksecurity/v1"
	"google.golang.org/api/networkservices/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/osconfig/v1"
	"google.golang.org/api/vmwareengine/v1"
	"google.golang.org/api/workstations/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func startNet(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"networkmanagement", "networksecurity", "networkservices", "domains",
		"vmwareengine", "edgecontainer", "gkemulticloud", "gkebackup", "appengine", "workstations", "hypercomputecluster",
		"osconfig", "cloudquotas", "gcpconfig", "operations"}})
}

func TestNetworkManagement(t *testing.T) {
	svc := must(networkmanagement.NewService(ctx, opts(startNet(t), "/networkmanagement/")...))
	const parent = "projects/p/locations/global"
	tests := svc.Projects.Locations.Global.ConnectivityTests
	test := func() *networkmanagement.ConnectivityTest {
		return &networkmanagement.ConnectivityTest{Protocol: "TCP", Labels: map[string]string{"env": "dev"},
			Source:      &networkmanagement.Endpoint{IpAddress: "10.0.0.5", ProjectId: "p"},
			Destination: &networkmanagement.Endpoint{IpAddress: "10.0.1.7", ProjectId: "p", Port: 443}}
	}
	if op := must(tests.Create(parent, test()).TestId("t1").Do()); !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(tests.Get(parent + "/connectivityTests/t1").Do())
	if got.Name != parent+"/connectivityTests/t1" || got.CreateTime == "" || got.Destination.Port != 443 || got.Labels["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
	must(tests.Patch(parent+"/connectivityTests/t1", &networkmanagement.ConnectivityTest{Description: "d"}).UpdateMask("description").Do())
	if got = must(tests.Get(parent + "/connectivityTests/t1").Do()); got.Description != "d" || got.Protocol != "TCP" {
		t.Fatalf("%+v", got)
	}
	if l := must(tests.List(parent).Do()); len(l.Resources) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := tests.Create(parent, test()).TestId("t1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = tests.Create(parent, test()).Do()
	wantCode(t, err, 400, "testId")
	_, err = tests.Create(parent, test()).TestId("Bad Id").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = tests.Create(parent, &networkmanagement.ConnectivityTest{Source: test().Source}).TestId("t2").Do()
	wantCode(t, err, 400, "destination")
	_, err = tests.Get(parent + "/connectivityTests/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = tests.Delete(parent + "/connectivityTests/nope").Do()
	wantCode(t, err, 404, "nope")
	must(tests.Delete(parent + "/connectivityTests/t1").Do())
}

func TestCloudNGFW(t *testing.T) {
	svc := must(networksecurity.NewService(ctx, opts(startNet(t), "/networksecurity/")...))
	const parent = "organizations/123456789012/locations/us-central1-a"
	eps := svc.Organizations.Locations.FirewallEndpoints
	must(eps.Create(parent, &networksecurity.FirewallEndpoint{Labels: map[string]string{"env": "dev"}}).FirewallEndpointId("fw1").Do())
	got := must(eps.Get(parent + "/firewallEndpoints/fw1").Do())
	if got.Name != parent+"/firewallEndpoints/fw1" || got.State != "ACTIVE" || got.Labels["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
	_, err := eps.Create(parent, &networksecurity.FirewallEndpoint{}).FirewallEndpointId("fw1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = eps.Create(parent, &networksecurity.FirewallEndpoint{}).Do()
	wantCode(t, err, 400, "firewallEndpointId")
	_, err = eps.Get(parent + "/firewallEndpoints/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = eps.Delete(parent + "/firewallEndpoints/nope").Do()
	wantCode(t, err, 404, "nope")
	must(eps.Delete(parent + "/firewallEndpoints/fw1").Do())
}

func TestCloudServiceMesh(t *testing.T) {
	svc := must(networkservices.NewService(ctx, opts(startNet(t), "/networkservices/")...))
	const parent = "projects/p/locations/global"
	meshes := svc.Projects.Locations.Meshes
	must(meshes.Create(parent, &networkservices.Mesh{Description: "m", InterceptionPort: 15001}).MeshId("m1").Do())
	got := must(meshes.Get(parent + "/meshes/m1").Do())
	if got.Name != parent+"/meshes/m1" || got.InterceptionPort != 15001 || got.SelfLink == "" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	must(meshes.Patch(parent+"/meshes/m1", &networkservices.Mesh{Description: "d2"}).UpdateMask("description").Do())
	if got = must(meshes.Get(parent + "/meshes/m1").Do()); got.Description != "d2" || got.InterceptionPort != 15001 {
		t.Fatalf("%+v", got)
	}
	// a route is made in the same project
	routes := svc.Projects.Locations.HttpRoutes
	must(routes.Create(parent, &networkservices.HttpRoute{Hostnames: []string{"example.com"}, Meshes: []string{got.Name},
		Rules: []*networkservices.HttpRouteRouteRule{{}}}).HttpRouteId("r1").Do())
	if l := must(meshes.List(parent).Do()); len(l.Meshes) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := meshes.Create(parent, &networkservices.Mesh{}).MeshId("m1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = meshes.Create(parent, &networkservices.Mesh{}).Do()
	wantCode(t, err, 400, "meshId")
	_, err = meshes.Get(parent + "/meshes/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = meshes.Delete(parent + "/meshes/nope").Do()
	wantCode(t, err, 404, "nope")
	must(meshes.Delete(parent + "/meshes/m1").Do())
}

func TestCloudDomains(t *testing.T) {
	svc := must(domains.NewService(ctx, opts(startNet(t), "/domains/")...))
	const parent = "projects/p/locations/global"
	contact := &domains.Contact{Email: "a@example.com", PhoneNumber: "+11234567890",
		PostalAddress: &domains.PostalAddress{RegionCode: "US", PostalCode: "94043", Recipients: []string{"Pogi"}}}
	req := func(domain string) *domains.RegisterDomainRequest {
		return &domains.RegisterDomainRequest{YearlyPrice: &domains.Money{CurrencyCode: "USD", Units: 12},
			Registration: &domains.Registration{DomainName: domain, Labels: map[string]string{"env": "dev"}, ContactSettings: &domains.ContactSettings{
				Privacy: "REDACTED_CONTACT_DATA", RegistrantContact: contact, AdminContact: contact, TechnicalContact: contact}}}
	}
	regs := svc.Projects.Locations.Registrations
	op := must(regs.Register(parent, req("Pogi-Example.com")).Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(regs.Get(parent + "/registrations/pogi-example.com").Do())
	if got.State != "ACTIVE" || got.DomainName != "Pogi-Example.com" || got.ExpireTime == "" || got.ManagementSettings.TransferLockState != "LOCKED" || got.Labels["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
	must(regs.Patch(parent+"/registrations/pogi-example.com", &domains.Registration{Labels: map[string]string{"env": "prod"}}).UpdateMask("labels").Do())
	if got = must(regs.Get(parent + "/registrations/pogi-example.com").Do()); got.Labels["env"] != "prod" {
		t.Fatalf("%+v", got)
	}
	_, err := regs.Register(parent, req("pogi-example.com")).Do()
	wantCode(t, err, 409, "already exists")
	_, err = regs.Register(parent, req("nodot")).Do()
	wantCode(t, err, 400, "Invalid domain name")
	r := req("a.example.com")
	r.YearlyPrice = nil
	_, err = regs.Register(parent, r).Do()
	wantCode(t, err, 400, "yearlyPrice")
	r = req("b.example.com")
	r.Registration.ContactSettings = nil
	_, err = regs.Register(parent, r).Do()
	wantCode(t, err, 400, "contactSettings")
	_, err = regs.Register(parent, &domains.RegisterDomainRequest{}).Do()
	wantCode(t, err, 400, "registration")
	_, err = regs.Get(parent + "/registrations/nope.com").Do()
	wantCode(t, err, 404, "nope.com")
	_, err = regs.Delete(parent + "/registrations/nope.com").Do()
	wantCode(t, err, 404, "nope.com")
	must(regs.Delete(parent + "/registrations/pogi-example.com").Do())
}

func TestVMwareEngine(t *testing.T) {
	svc := must(vmwareengine.NewService(ctx, opts(startNet(t), "/vmwareengine/")...))
	const parent = "projects/p/locations/global"
	nets := svc.Projects.Locations.VmwareEngineNetworks
	must(nets.Create(parent, &vmwareengine.VmwareEngineNetwork{Type: "STANDARD", Description: "d"}).VmwareEngineNetworkId("n1").Do())
	got := must(nets.Get(parent + "/vmwareEngineNetworks/n1").Do())
	if got.Name != parent+"/vmwareEngineNetworks/n1" || got.Type != "STANDARD" || got.State != "ACTIVE" || got.Uid == "" {
		t.Fatalf("%+v", got)
	}
	_, err := nets.Create(parent, &vmwareengine.VmwareEngineNetwork{Type: "STANDARD"}).VmwareEngineNetworkId("n1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = nets.Create(parent, &vmwareengine.VmwareEngineNetwork{}).VmwareEngineNetworkId("n2").Do()
	wantCode(t, err, 400, "type")
	_, err = nets.Create(parent, &vmwareengine.VmwareEngineNetwork{Type: "STANDARD"}).Do()
	wantCode(t, err, 400, "vmwareEngineNetworkId")
	_, err = nets.Get(parent + "/vmwareEngineNetworks/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = nets.Delete(parent + "/vmwareEngineNetworks/nope").Do()
	wantCode(t, err, 404, "nope")
	must(nets.Delete(parent + "/vmwareEngineNetworks/n1").Do())
}

func TestDistributedCloud(t *testing.T) {
	srv := startNet(t)
	c := must(edgecontainer.NewRESTClient(ctx, option.WithEndpoint(srv.URL+"/edgecontainer"), option.WithoutAuthentication()))
	defer c.Close()
	const parent = "projects/p/locations/us-central1"
	cluster := func() *edgecontainerpb.Cluster {
		return &edgecontainerpb.Cluster{Labels: map[string]string{"env": "dev"},
			Authorization: &edgecontainerpb.Authorization{AdminUsers: &edgecontainerpb.ClusterUser{Username: "a@example.com"}},
			Networking:    &edgecontainerpb.ClusterNetworking{ClusterIpv4CidrBlocks: []string{"10.200.0.0/16"}, ServicesIpv4CidrBlocks: []string{"10.225.0.0/16"}},
			Fleet:         &edgecontainerpb.Fleet{Project: "projects/p"}}
	}
	create := func(id string, cl *edgecontainerpb.Cluster) error {
		op, err := c.CreateCluster(ctx, &edgecontainerpb.CreateClusterRequest{Parent: parent, ClusterId: id, Cluster: cl})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	if err := create("c1", cluster()); err != nil {
		t.Fatal(err)
	}
	got := must(c.GetCluster(ctx, &edgecontainerpb.GetClusterRequest{Name: parent + "/clusters/c1"}))
	if got.Name != parent+"/clusters/c1" || got.Labels["env"] != "dev" || got.CreateTime == nil || got.Fleet.Project != "projects/p" {
		t.Fatalf("%+v", got)
	}
	// a node pool is made under the cluster
	op := must(c.CreateNodePool(ctx, &edgecontainerpb.CreateNodePoolRequest{Parent: parent + "/clusters/c1", NodePoolId: "np1",
		NodePool: &edgecontainerpb.NodePool{NodeLocation: "zone1", NodeCount: 3}}))
	must(op.Wait(ctx))
	if np := must(c.GetNodePool(ctx, &edgecontainerpb.GetNodePoolRequest{Name: parent + "/clusters/c1/nodePools/np1"})); np.NodeCount != 3 {
		t.Fatalf("%+v", np)
	}
	up := must(c.UpdateCluster(ctx, &edgecontainerpb.UpdateClusterRequest{Cluster: &edgecontainerpb.Cluster{Name: parent + "/clusters/c1", Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}}}))
	if cl := must(up.Wait(ctx)); cl.Labels["env"] != "prod" {
		t.Fatalf("%+v", cl)
	}
	wantGRPC := func(err error, want string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("want %q, got %v", want, err)
		}
	}
	wantGRPC(create("c1", cluster()), "already exists")
	wantGRPC(create("c2", &edgecontainerpb.Cluster{}), "authorization")
	wantGRPC(create("Bad Id", cluster()), "Invalid id")
	_, err := c.GetCluster(ctx, &edgecontainerpb.GetClusterRequest{Name: parent + "/clusters/nope"})
	wantGRPC(err, "nope")
	_, err = c.DeleteCluster(ctx, &edgecontainerpb.DeleteClusterRequest{Name: parent + "/clusters/nope"})
	wantGRPC(err, "nope")
	if err := must(c.DeleteCluster(ctx, &edgecontainerpb.DeleteClusterRequest{Name: parent + "/clusters/c1"})).Wait(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetNodePool(ctx, &edgecontainerpb.GetNodePoolRequest{Name: parent + "/clusters/c1/nodePools/np1"})
	wantGRPC(err, "np1") // the pool went with its cluster
}

// The Go client of GKE Multi-Cloud speaks gRPC only, so this test sends the REST requests itself and
// reads each answer into the client's own message types.
func TestGKEMultiCloud(t *testing.T) {
	srv := startNet(t)
	const parent = "projects/p/locations/us-central1"
	do := func(method, path string, in, out proto.Message) (int, string) {
		t.Helper()
		var body io.Reader
		if in != nil {
			body = bytes.NewReader(must(protojson.Marshal(in)))
		}
		req := must(http.NewRequest(method, srv.URL+"/gkemulticloud/v1/"+path, body))
		req.Header.Set("Content-Type", "application/json")
		resp := must(http.DefaultClient.Do(req))
		defer resp.Body.Close()
		b := must(io.ReadAll(resp.Body))
		if resp.StatusCode == 200 && out != nil {
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, out); err != nil {
				t.Fatalf("%s %s: %v\n%s", method, path, err, b)
			}
		}
		return resp.StatusCode, string(b)
	}
	wantErr := func(code int, body string, wantCode int, want string) {
		t.Helper()
		if code != wantCode || !strings.Contains(body, want) {
			t.Fatalf("want %d %q, got %d %s", wantCode, want, code, body)
		}
	}
	good := func() *gkemulticloudpb.AzureClient {
		return &gkemulticloudpb.AzureClient{TenantId: "12345678-1234-1234-1234-123456789111", ApplicationId: "12345678-1234-1234-1234-123456789111"}
	}
	var op longrunningpb.Operation
	if code, b := do("POST", parent+"/azureClients?azureClientId=a1", good(), &op); code != 200 || !op.Done {
		t.Fatalf("%d %s", code, b)
	}
	var got gkemulticloudpb.AzureClient
	do("GET", parent+"/azureClients/a1", nil, &got)
	if got.Name != parent+"/azureClients/a1" || got.TenantId == "" || got.Uid == "" || got.CreateTime == nil {
		t.Fatalf("%+v", &got)
	}
	// an AWS cluster and its node pool go through the same engine
	if code, b := do("POST", parent+"/awsClusters?awsClusterId=w1", &gkemulticloudpb.AwsCluster{AwsRegion: "us-east-1", Description: "d"}, &op); code != 200 {
		t.Fatalf("%d %s", code, b)
	}
	if code, b := do("POST", parent+"/awsClusters/w1/awsNodePools?awsNodePoolId=np1", &gkemulticloudpb.AwsNodePool{Version: "1.30"}, &op); code != 200 {
		t.Fatalf("%d %s", code, b)
	}
	var list gkemulticloudpb.ListAwsClustersResponse
	if do("GET", parent+"/awsClusters", nil, &list); len(list.AwsClusters) != 1 || list.AwsClusters[0].AwsRegion != "us-east-1" {
		t.Fatalf("%+v", &list)
	}
	code, b := do("POST", parent+"/azureClients?azureClientId=a1", good(), nil)
	wantErr(code, b, 409, "already exists")
	code, b = do("POST", parent+"/azureClients?azureClientId=a2", &gkemulticloudpb.AzureClient{}, nil)
	wantErr(code, b, 400, "tenantId")
	code, b = do("POST", parent+"/azureClients?azureClientId=Bad+Id", good(), nil)
	wantErr(code, b, 400, "Invalid id")
	code, b = do("POST", parent+"/azureClients", good(), nil)
	wantErr(code, b, 400, "azureClientId")
	code, b = do("GET", parent+"/azureClients/nope", nil, nil)
	wantErr(code, b, 404, "nope")
	code, b = do("DELETE", parent+"/azureClients/nope", nil, nil)
	wantErr(code, b, 404, "nope")
	if code, b = do("DELETE", parent+"/azureClients/a1", nil, &op); code != 200 {
		t.Fatalf("%d %s", code, b)
	}
	code, b = do("GET", parent+"/azureClients/a1", nil, nil)
	wantErr(code, b, 404, "a1")
}

func TestBackupForGKE(t *testing.T) {
	svc := must(gkebackup.NewService(ctx, opts(startNet(t), "/gkebackup/")...))
	const parent = "projects/p/locations/us-central1"
	plans := svc.Projects.Locations.BackupPlans
	plan := func() *gkebackup.BackupPlan {
		return &gkebackup.BackupPlan{Cluster: "projects/p/locations/us-central1/clusters/c", Labels: map[string]string{"env": "dev"},
			BackupConfig: &gkebackup.BackupConfig{AllNamespaces: true, IncludeVolumeData: true}}
	}
	must(plans.Create(parent, plan()).BackupPlanId("bp1").Do())
	got := must(plans.Get(parent + "/backupPlans/bp1").Do())
	if got.Name != parent+"/backupPlans/bp1" || got.Uid == "" || got.Etag == "" || !got.BackupConfig.AllNamespaces {
		t.Fatalf("%+v", got)
	}
	// a backup is made under the plan
	op := must(svc.Projects.Locations.BackupPlans.Backups.Create(parent+"/backupPlans/bp1", &gkebackup.Backup{}).BackupId("b1").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	_, err := plans.Create(parent, plan()).BackupPlanId("bp1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = plans.Create(parent, plan()).Do()
	wantCode(t, err, 400, "backupPlanId")
	_, err = plans.Create(parent, &gkebackup.BackupPlan{}).BackupPlanId("bp2").Do()
	wantCode(t, err, 400, "cluster")
	_, err = plans.Get(parent + "/backupPlans/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = plans.Delete(parent + "/backupPlans/nope").Do()
	wantCode(t, err, 404, "nope")
	must(plans.Delete(parent + "/backupPlans/bp1").Do())
}

func TestAppEngine(t *testing.T) {
	svc := must(appengine.NewService(ctx, opts(startNet(t), "/appengine/")...))
	op := must(svc.Apps.Create(&appengine.Application{Id: "p", LocationId: "us-central"}).Do())
	if op.Name == "" || !op.Done {
		t.Fatalf("%+v", op)
	}
	// the operation is served where its name says: apps/p/operations/<id>
	if got := must(svc.Apps.Operations.Get("p", op.Name[len("apps/p/operations/"):]).Do()); !got.Done || got.Name != op.Name {
		t.Fatalf("%+v", got)
	}
	app := must(svc.Apps.Get("p").Do())
	if app.Name != "apps/p" || app.DefaultHostname != "p.appspot.com" || app.ServingStatus != "SERVING" || app.LocationId != "us-central" {
		t.Fatalf("%+v", app)
	}
	must(svc.Apps.Patch("p", &appengine.Application{ServingStatus: "USER_DISABLED"}).UpdateMask("servingStatus").Do())
	if app = must(svc.Apps.Get("p").Do()); app.ServingStatus != "USER_DISABLED" || app.LocationId != "us-central" {
		t.Fatalf("%+v", app)
	}
	_, err := svc.Apps.Create(&appengine.Application{Id: "p", LocationId: "us-central"}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = svc.Apps.Create(&appengine.Application{LocationId: "us-central"}).Do()
	wantCode(t, err, 400, "id")
	_, err = svc.Apps.Create(&appengine.Application{Id: "q"}).Do()
	wantCode(t, err, 400, "locationId")
	_, err = svc.Apps.Get("nope").Do()
	wantCode(t, err, 404, "nope")
}

func TestCloudWorkstations(t *testing.T) {
	svc := must(workstations.NewService(ctx, opts(startNet(t), "/workstations/")...))
	const parent = "projects/p/locations/us-central1"
	clusters := svc.Projects.Locations.WorkstationClusters
	cl := func() *workstations.WorkstationCluster {
		return &workstations.WorkstationCluster{Network: "projects/p/global/networks/default", Subnetwork: "projects/p/regions/us-central1/subnetworks/default"}
	}
	must(clusters.Create(parent, cl()).WorkstationClusterId("c1").Do())
	got := must(clusters.Get(parent + "/workstationClusters/c1").Do())
	if got.Name != parent+"/workstationClusters/c1" || got.Uid == "" || got.Network == "" {
		t.Fatalf("%+v", got)
	}
	cfgs := clusters.WorkstationConfigs
	must(cfgs.Create(parent+"/workstationClusters/c1", &workstations.WorkstationConfig{IdleTimeout: "600s"}).WorkstationConfigId("cfg1").Do())
	if c := must(cfgs.Get(parent + "/workstationClusters/c1/workstationConfigs/cfg1").Do()); c.IdleTimeout != "600s" {
		t.Fatalf("%+v", c)
	}
	_, err := clusters.Create(parent, cl()).WorkstationClusterId("c1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = clusters.Create(parent, cl()).Do()
	wantCode(t, err, 400, "workstationClusterId")
	_, err = clusters.Create(parent, &workstations.WorkstationCluster{}).WorkstationClusterId("c2").Do()
	wantCode(t, err, 400, "network")
	_, err = cfgs.Create(parent+"/workstationClusters/nope", &workstations.WorkstationConfig{}).WorkstationConfigId("x").Do()
	wantCode(t, err, 404, "nope")
	_, err = clusters.Get(parent + "/workstationClusters/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = clusters.Delete(parent + "/workstationClusters/nope").Do()
	wantCode(t, err, 404, "nope")
	must(clusters.Delete(parent + "/workstationClusters/c1").Do())
}

func TestClusterDirector(t *testing.T) {
	svc := must(hypercomputecluster.NewService(ctx, opts(startNet(t), "/hypercomputecluster/")...))
	const parent = "projects/p/locations/us-central1"
	clusters := svc.Projects.Locations.Clusters
	must(clusters.Create(parent, &hypercomputecluster.Cluster{Description: "d", Labels: map[string]string{"env": "dev"}}).ClusterId("c1").Do())
	got := must(clusters.Get(parent + "/clusters/c1").Do())
	if got.Name != parent+"/clusters/c1" || got.Description != "d" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	must(clusters.Patch(parent+"/clusters/c1", &hypercomputecluster.Cluster{Description: "d2"}).UpdateMask("description").Do())
	if got = must(clusters.Get(parent + "/clusters/c1").Do()); got.Description != "d2" {
		t.Fatalf("%+v", got)
	}
	_, err := clusters.Create(parent, &hypercomputecluster.Cluster{}).ClusterId("c1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = clusters.Create(parent, &hypercomputecluster.Cluster{}).Do()
	wantCode(t, err, 400, "clusterId")
	_, err = clusters.Get(parent + "/clusters/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = clusters.Delete(parent + "/clusters/nope").Do()
	wantCode(t, err, 404, "nope")
	must(clusters.Delete(parent + "/clusters/c1").Do())
}

func TestVMManager(t *testing.T) {
	svc := must(osconfig.NewService(ctx, opts(startNet(t), "/osconfig/")...))
	pd := svc.Projects.PatchDeployments
	dep := func() *osconfig.PatchDeployment {
		return &osconfig.PatchDeployment{InstanceFilter: &osconfig.PatchInstanceFilter{All: true},
			OneTimeSchedule: &osconfig.OneTimeSchedule{ExecuteTime: "2030-01-01T00:00:00Z"}}
	}
	got := must(pd.Create("projects/p", dep()).PatchDeploymentId("pd1").Do())
	if got.Name != "projects/p/patchDeployments/pd1" || got.CreateTime == "" || !got.InstanceFilter.All {
		t.Fatalf("%+v", got)
	}
	if got = must(pd.Get("projects/p/patchDeployments/pd1").Do()); got.OneTimeSchedule.ExecuteTime == "" {
		t.Fatalf("%+v", got)
	}
	if l := must(pd.List("projects/p").Do()); len(l.PatchDeployments) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := pd.Create("projects/p", dep()).PatchDeploymentId("pd1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = pd.Create("projects/p", dep()).Do()
	wantCode(t, err, 400, "patchDeploymentId")
	_, err = pd.Create("projects/p", &osconfig.PatchDeployment{}).PatchDeploymentId("pd2").Do()
	wantCode(t, err, 400, "instanceFilter")
	_, err = pd.Get("projects/p/patchDeployments/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = pd.Delete("projects/p/patchDeployments/nope").Do()
	wantCode(t, err, 404, "nope")
	must(pd.Delete("projects/p/patchDeployments/pd1").Do())
}

func TestCloudQuotas(t *testing.T) {
	srv := startNet(t)
	c := must(cloudquotas.NewRESTClient(ctx, option.WithEndpoint(srv.URL+"/cloudquotas"), option.WithoutAuthentication()))
	defer c.Close()
	adj := must(cloudquotas.NewQuotaAdjusterSettingsManagerRESTClient(ctx, option.WithEndpoint(srv.URL+"/cloudquotas"), option.WithoutAuthentication()))
	defer adj.Close()
	const parent = "projects/p/locations/global"
	pref := func() *cloudquotaspb.QuotaPreference {
		return &cloudquotaspb.QuotaPreference{Service: "compute.googleapis.com", QuotaId: "CPUS-per-project-region",
			Dimensions: map[string]string{"region": "us-central1"}, QuotaConfig: &cloudquotaspb.QuotaConfig{PreferredValue: 200}}
	}
	got := must(c.CreateQuotaPreference(ctx, &cloudquotaspb.CreateQuotaPreferenceRequest{Parent: parent, QuotaPreferenceId: "qp1", QuotaPreference: pref()}))
	if got.Name != parent+"/quotaPreferences/qp1" || got.QuotaConfig.PreferredValue != 200 || got.QuotaConfig.GrantedValue.GetValue() != 200 || got.CreateTime == nil {
		t.Fatalf("%+v", got)
	}
	wantErr := func(err error, want string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("want %q, got %v", want, err)
		}
	}
	_, err := c.CreateQuotaPreference(ctx, &cloudquotaspb.CreateQuotaPreferenceRequest{Parent: parent, QuotaPreferenceId: "qp1", QuotaPreference: pref()})
	wantErr(err, "already exists")
	_, err = c.CreateQuotaPreference(ctx, &cloudquotaspb.CreateQuotaPreferenceRequest{Parent: parent, QuotaPreferenceId: "qp2", QuotaPreference: &cloudquotaspb.QuotaPreference{Service: "compute.googleapis.com"}})
	wantErr(err, "quotaId")
	_, err = c.GetQuotaPreference(ctx, &cloudquotaspb.GetQuotaPreferenceRequest{Name: parent + "/quotaPreferences/nope"})
	wantErr(err, "nope")

	// the adjuster settings are there from the start, off, and a PATCH changes them
	name := parent + "/quotaAdjusterSettings"
	s := must(adj.GetQuotaAdjusterSettings(ctx, &cloudquotaspb.GetQuotaAdjusterSettingsRequest{Name: name}))
	if s.Name != name || s.Enablement != cloudquotaspb.QuotaAdjusterSettings_DISABLED {
		t.Fatalf("%+v", s)
	}
	s = must(adj.UpdateQuotaAdjusterSettings(ctx, &cloudquotaspb.UpdateQuotaAdjusterSettingsRequest{
		QuotaAdjusterSettings: &cloudquotaspb.QuotaAdjusterSettings{Name: name, Enablement: cloudquotaspb.QuotaAdjusterSettings_ENABLED},
		UpdateMask:            &fieldmaskpb.FieldMask{Paths: []string{"enablement"}}}))
	if s.Enablement != cloudquotaspb.QuotaAdjusterSettings_ENABLED {
		t.Fatalf("%+v", s)
	}
	if s = must(adj.GetQuotaAdjusterSettings(ctx, &cloudquotaspb.GetQuotaAdjusterSettingsRequest{Name: name})); s.Enablement != cloudquotaspb.QuotaAdjusterSettings_ENABLED {
		t.Fatalf("%+v", s)
	}
}

func TestInfrastructureManager(t *testing.T) {
	svc := must(config.NewService(ctx, opts(startNet(t), "/config/")...))
	const parent = "projects/p/locations/us-central1"
	deps := svc.Projects.Locations.Deployments
	dep := func() *config.Deployment {
		return &config.Deployment{ServiceAccount: "projects/p/serviceAccounts/s@p.iam.gserviceaccount.com", Labels: map[string]string{"env": "dev"},
			TerraformBlueprint: &config.TerraformBlueprint{GitSource: &config.GitSource{Repo: "https://example.com/r.git", Ref: "main"}}}
	}
	if op := must(deps.Create(parent, dep()).DeploymentId("d1").Do()); !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(deps.Get(parent + "/deployments/d1").Do())
	if got.Name != parent+"/deployments/d1" || got.State != "ACTIVE" || got.TerraformBlueprint.GitSource.Ref != "main" || got.Labels["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
	_, err := deps.Create(parent, dep()).DeploymentId("d1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = deps.Create(parent, dep()).Do()
	wantCode(t, err, 400, "deploymentId")
	_, err = deps.Create(parent, &config.Deployment{}).DeploymentId("d2").Do()
	wantCode(t, err, 400, "serviceAccount")
	_, err = deps.Get(parent + "/deployments/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = deps.Delete(parent + "/deployments/nope").Do()
	wantCode(t, err, 404, "nope")
	must(deps.Delete(parent + "/deployments/d1").Do())
}
