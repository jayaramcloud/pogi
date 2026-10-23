package generic_test

import (
	"testing"

	lustre "cloud.google.com/go/lustre/apiv1"
	lustrepb "cloud.google.com/go/lustre/apiv1/lustrepb"
	memorystore "cloud.google.com/go/memorystore/apiv1"
	memorystorepb "cloud.google.com/go/memorystore/apiv1/memorystorepb"
	"github.com/nrndev/pogi"
	analyticshub "google.golang.org/api/analyticshub/v1"
	backupdr "google.golang.org/api/backupdr/v1"
	biglake "google.golang.org/api/biglake/v1"
	dataform "google.golang.org/api/dataform/v1"
	datafusion "google.golang.org/api/datafusion/v1"
	datamigration "google.golang.org/api/datamigration/v1"
	"google.golang.org/api/iterator"
	metastore "google.golang.org/api/metastore/v1"
	netapp "google.golang.org/api/netapp/v1"
	"google.golang.org/api/option"
	oracledatabase "google.golang.org/api/oracledatabase/v1"
	parallelstore "google.golang.org/api/parallelstore/v1"
	transcoder "google.golang.org/api/transcoder/v1"
)

func startG2Data(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"analyticshub", "biglake", "datafusion", "dataform", "metastore",
		"datamigration", "memorystore", "oracledatabase", "backupdr", "netapp", "parallelstore", "lustre", "transcoder", "operations"}})
}

// crud drives one resource the way a user does, and checks the ways it fails: create, get, list, delete,
// a second create, an empty and a bad id, and a get or delete of something that is not there.
// base is the resource's collection path; get returns the name the server gave it.
func crud(t *testing.T, base string, create func(id string) error, get func(id string) (string, error),
	list func() (int, error), del func(id string) error) {
	t.Helper()
	if err := create("r1"); err != nil {
		t.Fatal(err)
	}
	if name, err := get("r1"); err != nil || name != base+"/r1" {
		t.Fatalf("get: %q %v", name, err)
	}
	if n, err := list(); err != nil || n != 1 {
		t.Fatalf("list: %d %v", n, err)
	}
	wantCode(t, create("r1"), 409, "already exists")
	wantCode(t, create(""), 400, "Id")
	wantCode(t, create("Bad Id!"), 400, "Invalid id")
	_, err := get("nope")
	wantCode(t, err, 404, "nope")
	wantCode(t, del("nope"), 404, "nope")
	if err := del("r1"); err != nil {
		t.Fatal(err)
	}
	_, err = get("r1")
	wantCode(t, err, 404, "r1")
	if n, err := list(); err != nil || n != 0 {
		t.Fatalf("list after delete: %d %v", n, err)
	}
}

const pl = "projects/p/locations/us-central1"

func TestAnalyticsHub(t *testing.T) {
	c := must(analyticshub.NewService(ctx, opts(startG2Data(t), "/analyticshub/")...)).Projects.Locations.DataExchanges
	crud(t, pl+"/dataExchanges",
		func(id string) error {
			_, err := c.Create(pl, &analyticshub.DataExchange{DisplayName: "x"}).DataExchangeId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/dataExchanges/" + id).Do()
			if r == nil {
				return "", err
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.DataExchanges), err },
		func(id string) error { _, err := c.Delete(pl + "/dataExchanges/" + id).Do(); return err })
}

func TestBigLake(t *testing.T) {
	c := must(biglake.NewService(ctx, opts(startG2Data(t), "/biglake/")...)).Projects.Locations.Catalogs
	crud(t, pl+"/catalogs",
		func(id string) error { _, err := c.Create(pl, &biglake.Catalog{}).CatalogId(id).Do(); return err },
		func(id string) (string, error) {
			r, err := c.Get(pl + "/catalogs/" + id).Do()
			if r == nil {
				return "", err
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.Catalogs), err },
		func(id string) error { _, err := c.Delete(pl + "/catalogs/" + id).Do(); return err })
}

func TestDataFusion(t *testing.T) {
	c := must(datafusion.NewService(ctx, opts(startG2Data(t), "/datafusion/")...)).Projects.Locations.Instances
	crud(t, pl+"/instances",
		func(id string) error {
			op, err := c.Create(pl, &datafusion.Instance{Type: "BASIC"}).InstanceId(id).Do()
			if err == nil && !op.Done {
				t.Fatalf("%+v", op)
			}
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/instances/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.State != "ACTIVE" || r.Type != "BASIC" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.Instances), err },
		func(id string) error { _, err := c.Delete(pl + "/instances/" + id).Do(); return err })
}

func TestDataform(t *testing.T) {
	c := must(dataform.NewService(ctx, opts(startG2Data(t), "/dataform/")...)).Projects.Locations.Repositories
	crud(t, pl+"/repositories",
		func(id string) error {
			_, err := c.Create(pl, &dataform.Repository{Labels: map[string]string{"env": "dev"}}).RepositoryId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/repositories/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.Labels["env"] != "dev" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.Repositories), err },
		func(id string) error { _, err := c.Delete(pl + "/repositories/" + id).Do(); return err })
}

func TestDataprocMetastore(t *testing.T) {
	c := must(metastore.NewService(ctx, opts(startG2Data(t), "/metastore/")...)).Projects.Locations.Services
	crud(t, pl+"/services",
		func(id string) error {
			_, err := c.Create(pl, &metastore.Service{HiveMetastoreConfig: &metastore.HiveMetastoreConfig{Version: "3.1.2"}}).ServiceId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/services/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.State != "ACTIVE" || r.HiveMetastoreConfig.Version != "3.1.2" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.Services), err },
		func(id string) error { _, err := c.Delete(pl + "/services/" + id).Do(); return err })
}

func TestDatabaseMigration(t *testing.T) {
	c := must(datamigration.NewService(ctx, opts(startG2Data(t), "/datamigration/")...)).Projects.Locations.ConnectionProfiles
	crud(t, pl+"/connectionProfiles",
		func(id string) error {
			_, err := c.Create(pl, &datamigration.ConnectionProfile{DisplayName: "d", Mysql: &datamigration.MySqlConnectionProfile{Host: "10.0.0.5", Port: 3306}}).ConnectionProfileId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/connectionProfiles/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.State != "READY" || r.Mysql.Host != "10.0.0.5" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.ConnectionProfiles), err },
		func(id string) error { _, err := c.Delete(pl + "/connectionProfiles/" + id).Do(); return err })
}

func TestOracleDatabase(t *testing.T) {
	c := must(oracledatabase.NewService(ctx, opts(startG2Data(t), "/oracledatabase/")...)).Projects.Locations.CloudExadataInfrastructures
	crud(t, pl+"/cloudExadataInfrastructures",
		func(id string) error {
			_, err := c.Create(pl, &oracledatabase.CloudExadataInfrastructure{DisplayName: "d",
				Properties: &oracledatabase.CloudExadataInfrastructureProperties{Shape: "Exadata.X9M", ComputeCount: 2, StorageCount: 3}}).CloudExadataInfrastructureId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/cloudExadataInfrastructures/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.Properties.Shape != "Exadata.X9M" || r.CreateTime == "" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.CloudExadataInfrastructures), err },
		func(id string) error { _, err := c.Delete(pl + "/cloudExadataInfrastructures/" + id).Do(); return err })
}

func TestBackupDR(t *testing.T) {
	c := must(backupdr.NewService(ctx, opts(startG2Data(t), "/backupdr/")...)).Projects.Locations.BackupVaults
	crud(t, pl+"/backupVaults",
		func(id string) error {
			_, err := c.Create(pl, &backupdr.BackupVault{BackupMinimumEnforcedRetentionDuration: "100000s"}).BackupVaultId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/backupVaults/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.State != "ACTIVE" || r.BackupMinimumEnforcedRetentionDuration != "100000s" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.BackupVaults), err },
		func(id string) error { _, err := c.Delete(pl + "/backupVaults/" + id).Do(); return err })
}

func TestNetApp(t *testing.T) {
	c := must(netapp.NewService(ctx, opts(startG2Data(t), "/netapp/")...)).Projects.Locations.StoragePools
	crud(t, pl+"/storagePools",
		func(id string) error {
			_, err := c.Create(pl, &netapp.StoragePool{ServiceLevel: "PREMIUM", CapacityGib: 2048, Network: "projects/p/global/networks/default"}).StoragePoolId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/storagePools/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.State != "READY" || r.CapacityGib != 2048 {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.StoragePools), err },
		func(id string) error { _, err := c.Delete(pl + "/storagePools/" + id).Do(); return err })
}

func TestParallelstore(t *testing.T) {
	c := must(parallelstore.NewService(ctx, opts(startG2Data(t), "/parallelstore/")...)).Projects.Locations.Instances
	z := "projects/p/locations/us-central1-a"
	crud(t, z+"/instances",
		func(id string) error {
			_, err := c.Create(z, &parallelstore.Instance{CapacityGib: 12000, Network: "projects/p/global/networks/default"}).InstanceId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(z + "/instances/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.State != "ACTIVE" || r.CapacityGib != 12000 {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(z).Do(); return len(r.Instances), err },
		func(id string) error { _, err := c.Delete(z + "/instances/" + id).Do(); return err })
}

func TestTranscoder(t *testing.T) {
	c := must(transcoder.NewService(ctx, opts(startG2Data(t), "/transcoder/")...)).Projects.Locations.JobTemplates
	crud(t, pl+"/jobTemplates",
		func(id string) error {
			_, err := c.Create(pl, &transcoder.JobTemplate{Config: &transcoder.JobConfig{
				MuxStreams: []*transcoder.MuxStream{{Key: "sd", Container: "mp4", ElementaryStreams: []string{"v0"}}}}}).JobTemplateId(id).Do()
			return err
		},
		func(id string) (string, error) {
			r, err := c.Get(pl + "/jobTemplates/" + id).Do()
			if r == nil {
				return "", err
			}
			if r.Config.MuxStreams[0].Key != "sd" {
				t.Fatalf("%+v", r)
			}
			return r.Name, err
		},
		func() (int, error) { r, err := c.List(pl).Do(); return len(r.JobTemplates), err },
		func(id string) error { _, err := c.Delete(pl + "/jobTemplates/" + id).Do(); return err })
}

func TestTranscoderJob(t *testing.T) {
	c := must(transcoder.NewService(ctx, opts(startG2Data(t), "/transcoder/")...)).Projects.Locations.Jobs
	j := must(c.Create(pl, &transcoder.Job{InputUri: "gs://in/a.mp4", OutputUri: "gs://out/", TemplateId: "preset/web-hd"}).Do())
	if j.State != "SUCCEEDED" || j.Name == "" || j.InputUri != "gs://in/a.mp4" { // a job's id is made by the server
		t.Fatalf("%+v", j)
	}
	if got := must(c.Get(j.Name).Do()); got.Name != j.Name {
		t.Fatalf("%+v", got)
	}
	must(c.Delete(j.Name).Do())
	_, err := c.Get(j.Name).Do()
	wantCode(t, err, 404, "jobs")
}

// Memorystore for Valkey and Managed Lustre have no client in google.golang.org/api: the tests use the REST client of
// cloud.google.com/go, which waits for the operation it gets back.
func TestMemorystoreValkey(t *testing.T) {
	srv := startG2Data(t)
	c := must(memorystore.NewRESTClient(ctx, option.WithEndpoint(srv.URL+"/memorystore"), option.WithoutAuthentication()))
	defer c.Close()
	crud(t, pl+"/instances",
		func(id string) error {
			op, err := c.CreateInstance(ctx, &memorystorepb.CreateInstanceRequest{Parent: pl, InstanceId: id,
				Instance: &memorystorepb.Instance{ShardCount: 1}})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		},
		func(id string) (string, error) {
			r, err := c.GetInstance(ctx, &memorystorepb.GetInstanceRequest{Name: pl + "/instances/" + id})
			if err != nil {
				return "", err
			}
			if r.State != memorystorepb.Instance_ACTIVE || r.ShardCount != 1 || r.Uid == "" {
				t.Fatalf("%+v", r)
			}
			return r.Name, nil
		},
		func() (n int, err error) {
			it := c.ListInstances(ctx, &memorystorepb.ListInstancesRequest{Parent: pl})
			for _, err = it.Next(); err == nil; _, err = it.Next() {
				n++
			}
			if err == iterator.Done {
				err = nil
			}
			return n, err
		},
		func(id string) error {
			op, err := c.DeleteInstance(ctx, &memorystorepb.DeleteInstanceRequest{Name: pl + "/instances/" + id})
			if err != nil {
				return err
			}
			return op.Wait(ctx)
		})
}

func TestManagedLustre(t *testing.T) {
	srv := startG2Data(t)
	c := must(lustre.NewRESTClient(ctx, option.WithEndpoint(srv.URL+"/lustre"), option.WithoutAuthentication()))
	defer c.Close()
	z := "projects/p/locations/us-central1-a"
	crud(t, z+"/instances",
		func(id string) error {
			op, err := c.CreateInstance(ctx, &lustrepb.CreateInstanceRequest{Parent: z, InstanceId: id,
				Instance: &lustrepb.Instance{Filesystem: "lfs", CapacityGib: 18000, Network: "projects/p/global/networks/default"}})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		},
		func(id string) (string, error) {
			r, err := c.GetInstance(ctx, &lustrepb.GetInstanceRequest{Name: z + "/instances/" + id})
			if err != nil {
				return "", err
			}
			if r.State != lustrepb.Instance_ACTIVE || r.CapacityGib != 18000 {
				t.Fatalf("%+v", r)
			}
			return r.Name, nil
		},
		func() (n int, err error) {
			it := c.ListInstances(ctx, &lustrepb.ListInstancesRequest{Parent: z})
			for _, err = it.Next(); err == nil; _, err = it.Next() {
				n++
			}
			if err == iterator.Done {
				err = nil
			}
			return n, err
		},
		func(id string) error {
			op, err := c.DeleteInstance(ctx, &lustrepb.DeleteInstanceRequest{Name: z + "/instances/" + id})
			if err != nil {
				return err
			}
			return op.Wait(ctx)
		})
}
