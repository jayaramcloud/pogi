package generic_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	alloydb "google.golang.org/api/alloydb/v1"
	composer "google.golang.org/api/composer/v1"
	dataflow "google.golang.org/api/dataflow/v1b3"
	dataproc "google.golang.org/api/dataproc/v1"
	datastream "google.golang.org/api/datastream/v1"
	memcache "google.golang.org/api/memcache/v1"
	ncc "google.golang.org/api/networkconnectivity/v1"
	vpcaccess "google.golang.org/api/vpcaccess/v1"
)

func startData(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"dataproc", "composer", "dataflow", "memcache", "vpcaccess",
		"networkconnectivity", "alloydb", "datastream", "operations"}})
}

func TestDataproc(t *testing.T) {
	svc := must(dataproc.NewService(ctx, opts(startData(t), "/dataproc/")...))
	clusters := svc.Projects.Regions.Clusters
	cl := func() *dataproc.Cluster {
		return &dataproc.Cluster{ClusterName: "c1", Labels: map[string]string{"env": "dev"}, Config: &dataproc.ClusterConfig{
			MasterConfig: &dataproc.InstanceGroupConfig{NumInstances: 1}, WorkerConfig: &dataproc.InstanceGroupConfig{NumInstances: 2}}}
	}
	op := must(clusters.Create("p", "us-central1", cl()).Do())
	if !op.Done || !strings.HasPrefix(op.Name, "projects/p/regions/us-central1/operations/") {
		t.Fatalf("%+v", op)
	}
	if polled := must(svc.Projects.Regions.Operations.Get(op.Name).Do()); !polled.Done {
		t.Fatalf("%+v", polled)
	}
	got := must(clusters.Get("p", "us-central1", "c1").Do())
	if got.Status.State != "RUNNING" || got.ProjectId != "p" || got.ClusterUuid == "" || got.Config.SoftwareConfig.ImageVersion == "" ||
		got.Config.WorkerConfig.InstanceNames[1] != "c1-w-1" || got.Config.MasterConfig.InstanceNames[0] != "c1-m" || got.Labels["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
	if l := must(clusters.List("p", "us-central1").Do()); len(l.Clusters) != 1 {
		t.Fatalf("%+v", l)
	}
	if l := must(clusters.List("p", "europe-west1").Do()); len(l.Clusters) != 0 {
		t.Fatalf("regions are separate: %+v", l)
	}

	// failures
	_, err := clusters.Create("p", "us-central1", cl()).Do()
	wantCode(t, err, 409, "already exists")
	_, err = clusters.Create("p", "us-central1", &dataproc.Cluster{}).Do()
	wantCode(t, err, 400, "clusterName")
	_, err = clusters.Create("p", "us-central1", &dataproc.Cluster{ClusterName: "Bad Name!"}).Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = clusters.Get("p", "us-central1", "nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = clusters.Delete("p", "us-central1", "nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = clusters.Stop("p", "us-central1", "c1", &dataproc.StopClusterRequest{}).Do()
	wantCode(t, err, 501, "stop")

	if op = must(clusters.Delete("p", "us-central1", "c1").Do()); !op.Done {
		t.Fatalf("%+v", op)
	}
	_, err = clusters.Get("p", "us-central1", "c1").Do()
	wantCode(t, err, 404, "c1")
}

func TestComposer(t *testing.T) {
	svc := must(composer.NewService(ctx, opts(startData(t), "/composer/")...))
	envs := svc.Projects.Locations.Environments
	parent := "projects/p/locations/us-central1"
	name := parent + "/environments/env1"
	op := must(envs.Create(parent, &composer.Environment{Name: name, Labels: map[string]string{"a": "b"}}).Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(envs.Get(name).Do())
	if got.State != "RUNNING" || got.Name != name || got.Uuid == "" || got.Labels["a"] != "b" {
		t.Fatalf("%+v", got)
	}
	must(envs.Patch(name, &composer.Environment{Labels: map[string]string{"a": "c"}}).UpdateMask("labels").Do())
	if got = must(envs.Get(name).Do()); got.Labels["a"] != "c" {
		t.Fatalf("%+v", got)
	}

	// failures: the name is the only id, so it must be there and sit under the parent
	_, err := envs.Create(parent, &composer.Environment{Name: name}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = envs.Create(parent, &composer.Environment{}).Do()
	wantCode(t, err, 400, "name")
	_, err = envs.Create(parent, &composer.Environment{Name: "projects/p/locations/other/environments/x"}).Do()
	wantCode(t, err, 400, "name")
	_, err = envs.Get(parent + "/environments/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = envs.Delete(parent + "/environments/nope").Do()
	wantCode(t, err, 404, "nope")
	if op = must(envs.Delete(name).Do()); !op.Done {
		t.Fatalf("%+v", op)
	}
}

func TestDataflow(t *testing.T) {
	svc := must(dataflow.NewService(ctx, opts(startData(t), "/dataflow/")...))
	loc := svc.Projects.Locations
	req := func() *dataflow.CreateJobFromTemplateRequest {
		return &dataflow.CreateJobFromTemplateRequest{JobName: "wc", GcsPath: "gs://dataflow-templates/latest/Word_Count", Parameters: map[string]string{"output": "gs://b/o"}}
	}
	job := must(loc.Templates.Create("p", "us-central1", req()).Do())
	if job.Id == "" || job.Name != "wc" || job.CurrentState != "JOB_STATE_RUNNING" || job.Location != "us-central1" || job.ProjectId != "p" {
		t.Fatalf("%+v", job)
	}
	if got := must(loc.Jobs.Get("p", "us-central1", job.Id).Do()); got.CurrentState != "JOB_STATE_RUNNING" || got.Name != "wc" {
		t.Fatalf("%+v", got)
	}
	if l := must(loc.Jobs.List("p", "us-central1").Do()); len(l.Jobs) != 1 {
		t.Fatalf("%+v", l)
	}

	// failures
	_, err := loc.Templates.Create("p", "us-central1", req()).Do()
	wantCode(t, err, 409, "active job")
	_, err = loc.Templates.Create("p", "us-central1", &dataflow.CreateJobFromTemplateRequest{GcsPath: "gs://x/y"}).Do()
	wantCode(t, err, 400, "jobName")
	_, err = loc.Templates.Create("p", "us-central1", &dataflow.CreateJobFromTemplateRequest{JobName: "x"}).Do()
	wantCode(t, err, 400, "gcsPath")
	_, err = loc.Templates.Create("p", "us-central1", &dataflow.CreateJobFromTemplateRequest{JobName: "x", GcsPath: "/tmp/t"}).Do()
	wantCode(t, err, 400, "gs://")
	_, err = loc.Jobs.Get("p", "us-central1", "nope").Do()
	wantCode(t, err, 404, "nope")

	// cancelling ends the job, so its name can be used again
	must(loc.Jobs.Update("p", "us-central1", job.Id, &dataflow.Job{RequestedState: "JOB_STATE_CANCELLED"}).Do())
	if got := must(loc.Jobs.Get("p", "us-central1", job.Id).Do()); got.CurrentState != "JOB_STATE_CANCELLED" {
		t.Fatalf("%+v", got)
	}
	must(loc.Templates.Create("p", "us-central1", req()).Do())
}

func TestMemcache(t *testing.T) {
	svc := must(memcache.NewService(ctx, opts(startData(t), "/memcache/")...))
	insts := svc.Projects.Locations.Instances
	parent := "projects/p/locations/us-central1"
	in := func() *memcache.Instance {
		return &memcache.Instance{NodeCount: 1, NodeConfig: &memcache.NodeConfig{CpuCount: 1, MemorySizeMb: 1024}}
	}
	must(insts.Create(parent, in()).InstanceId("m1").Do())
	got := must(insts.Get(parent + "/instances/m1").Do())
	if got.State != "READY" || got.NodeCount != 1 || got.Name != parent+"/instances/m1" {
		t.Fatalf("%+v", got)
	}
	_, err := insts.Create(parent, in()).InstanceId("m1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = insts.Create(parent, in()).Do()
	wantCode(t, err, 400, "instanceId")
	_, err = insts.Get(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = insts.Delete(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	must(insts.Delete(parent + "/instances/m1").Do())
}

func TestVPCAccess(t *testing.T) {
	svc := must(vpcaccess.NewService(ctx, opts(startData(t), "/vpcaccess/")...))
	cons := svc.Projects.Locations.Connectors
	parent := "projects/p/locations/us-central1"
	c := func() *vpcaccess.Connector {
		return &vpcaccess.Connector{Network: "default", IpCidrRange: "10.8.0.0/28"}
	}
	must(cons.Create(parent, c()).ConnectorId("vc1").Do())
	got := must(cons.Get(parent + "/connectors/vc1").Do())
	if got.State != "READY" || got.IpCidrRange != "10.8.0.0/28" {
		t.Fatalf("%+v", got)
	}
	_, err := cons.Create(parent, c()).ConnectorId("vc1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = cons.Create(parent, c()).ConnectorId("bad id!").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = cons.Get(parent + "/connectors/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = cons.Delete(parent + "/connectors/nope").Do()
	wantCode(t, err, 404, "nope")
	must(cons.Delete(parent + "/connectors/vc1").Do())
}

func TestNetworkConnectivity(t *testing.T) {
	svc := must(ncc.NewService(ctx, opts(startData(t), "/networkconnectivity/")...))
	loc := svc.Projects.Locations
	proj := "projects/p"
	must(loc.Global.Hubs.Create(proj+"/locations/global", &ncc.Hub{Description: "h"}).HubId("hub1").Do())
	hub := must(loc.Global.Hubs.Get(proj + "/locations/global/hubs/hub1").Do())
	if hub.State != "ACTIVE" || hub.Description != "h" || hub.UniqueId == "" {
		t.Fatalf("%+v", hub)
	}
	must(loc.Spokes.Create(proj+"/locations/global", &ncc.Spoke{Hub: hub.Name, LinkedVpcNetwork: &ncc.LinkedVpcNetwork{Uri: "projects/p/global/networks/default"}}).SpokeId("sp1").Do())
	sp := must(loc.Spokes.Get(proj + "/locations/global/spokes/sp1").Do())
	if sp.State != "ACTIVE" || sp.Hub != hub.Name {
		t.Fatalf("%+v", sp)
	}
	_, err := loc.Global.Hubs.Create(proj+"/locations/global", &ncc.Hub{}).HubId("hub1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = loc.Global.Hubs.Create(proj+"/locations/global", &ncc.Hub{}).Do()
	wantCode(t, err, 400, "hubId")
	_, err = loc.Spokes.Get(proj + "/locations/global/spokes/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = loc.Global.Hubs.Delete(proj + "/locations/global/hubs/nope").Do()
	wantCode(t, err, 404, "nope")
	must(loc.Spokes.Delete(proj + "/locations/global/spokes/sp1").Do())
	must(loc.Global.Hubs.Delete(proj + "/locations/global/hubs/hub1").Do())
}

func TestAlloyDB(t *testing.T) {
	svc := must(alloydb.NewService(ctx, opts(startData(t), "/alloydb/")...))
	clusters := svc.Projects.Locations.Clusters
	parent := "projects/p/locations/us-central1"
	// the requestId query parameter is not the cluster's id
	must(clusters.Create(parent, &alloydb.Cluster{NetworkConfig: &alloydb.NetworkConfig{Network: "projects/p/global/networks/default"}}).ClusterId("c1").RequestId("r1").Do())
	c := must(clusters.Get(parent + "/clusters/c1").Do())
	if c.State != "READY" || c.ClusterType != "PRIMARY" || c.Name != parent+"/clusters/c1" || c.Etag != "" {
		t.Fatalf("%+v", c)
	}
	inst := must(clusters.Instances.Create(c.Name, &alloydb.Instance{InstanceType: "PRIMARY"}).InstanceId("i1").RequestId("r2").Do())
	if !inst.Done {
		t.Fatalf("%+v", inst)
	}
	if got := must(clusters.Instances.Get(c.Name + "/instances/i1").Do()); got.State != "READY" || got.InstanceType != "PRIMARY" {
		t.Fatalf("%+v", got)
	}
	_, err := clusters.Create(parent, &alloydb.Cluster{}).ClusterId("c1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = clusters.Create(parent, &alloydb.Cluster{}).RequestId("r").Do()
	wantCode(t, err, 400, "clusterId")
	_, err = clusters.Instances.Create(parent+"/clusters/missing", &alloydb.Instance{}).InstanceId("i").Do()
	wantCode(t, err, 404, "missing")
	_, err = clusters.Get(parent + "/clusters/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = clusters.Delete(parent + "/clusters/nope").Do()
	wantCode(t, err, 404, "nope")
	must(clusters.Delete(c.Name).Force(true).Do())
	_, err = clusters.Instances.Get(c.Name + "/instances/i1").Do()
	wantCode(t, err, 404, "i1")
}

func TestDatastream(t *testing.T) {
	svc := must(datastream.NewService(ctx, opts(startData(t), "/datastream/")...))
	profiles := svc.Projects.Locations.ConnectionProfiles
	parent := "projects/p/locations/us-central1"
	p := func() *datastream.ConnectionProfile {
		return &datastream.ConnectionProfile{DisplayName: "d", Labels: map[string]string{"env": "dev"},
			GcsProfile: &datastream.GcsProfile{Bucket: "b", RootPath: "/x"}}
	}
	op := must(profiles.Create(parent, p()).ConnectionProfileId("cp1").Do())
	var resp map[string]any
	json.Unmarshal(op.Response, &resp)
	if !op.Done || resp["name"] != parent+"/connectionProfiles/cp1" {
		t.Fatalf("%+v %v", op, resp)
	}
	got := must(profiles.Get(parent + "/connectionProfiles/cp1").Do())
	if got.DisplayName != "d" || got.GcsProfile.Bucket != "b" || got.Labels["env"] != "dev" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	_, err := profiles.Create(parent, p()).ConnectionProfileId("cp1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = profiles.Create(parent, p()).Do()
	wantCode(t, err, 400, "connectionProfileId")
	_, err = profiles.Get(parent + "/connectionProfiles/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = profiles.Delete(parent + "/connectionProfiles/nope").Do()
	wantCode(t, err, 404, "nope")
	must(profiles.Delete(parent + "/connectionProfiles/cp1").Do())
}
