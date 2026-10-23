package generic_test

import (
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	bqdt "google.golang.org/api/bigquerydatatransfer/v1"
	"google.golang.org/api/dataplex/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/looker/v1"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/storagetransfer/v1"
)

func startMore(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"looker", "privateca", "healthcare", "dataplex", "bigquerydatatransfer", "storagetransfer", "operations"}})
}

// parent is the project and location a resource goes under.
const parent = "projects/p/locations/us-central1"

func TestLooker(t *testing.T) {
	svc := must(looker.NewService(ctx, opts(startMore(t), "/looker/")...))
	insts := svc.Projects.Locations.Instances
	op := must(insts.Create(parent, &looker.Instance{PlatformEdition: "LOOKER_CORE_STANDARD_ANNUAL"}).InstanceId("l1").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(insts.Get(parent + "/instances/l1").Do())
	if got.State != "ACTIVE" || got.Name != parent+"/instances/l1" || got.PlatformEdition == "" {
		t.Fatalf("%+v", got)
	}
	_, err := insts.Create(parent, &looker.Instance{}).InstanceId("l1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = insts.Create(parent, &looker.Instance{}).Do()
	wantCode(t, err, 400, "instanceId")
	_, err = insts.Create(parent, &looker.Instance{}).InstanceId("Bad Id").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = insts.Get(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = insts.Delete(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	must(insts.Delete(parent + "/instances/l1").Do())
}

func TestPrivateCA(t *testing.T) {
	svc := must(privateca.NewService(ctx, opts(startMore(t), "/privateca/")...))
	pools := svc.Projects.Locations.CaPools
	must(pools.Create(parent, &privateca.CaPool{Tier: "ENTERPRISE"}).CaPoolId("pool").Do())
	pool := parent + "/caPools/pool"
	ca := func() *privateca.CertificateAuthority {
		return &privateca.CertificateAuthority{Type: "SELF_SIGNED", Lifetime: "315360000s", KeySpec: &privateca.KeyVersionSpec{Algorithm: "RSA_PKCS1_4096_SHA256"},
			Config: &privateca.CertificateConfig{SubjectConfig: &privateca.SubjectConfig{Subject: &privateca.Subject{CommonName: "ca"}}}}
	}
	cas := pools.CertificateAuthorities
	must(cas.Create(pool, ca()).CertificateAuthorityId("ca1").Do())
	got := must(cas.Get(pool + "/certificateAuthorities/ca1").Do())
	if got.State != "STAGED" || got.Tier != "" && got.Tier != "ENTERPRISE" {
		t.Fatalf("%+v", got)
	}
	op := must(cas.Enable(got.Name, &privateca.EnableCertificateAuthorityRequest{}).Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	if got = must(cas.Get(got.Name).Do()); got.State != "ENABLED" {
		t.Fatalf("%+v", got)
	}
	must(cas.Disable(got.Name, &privateca.DisableCertificateAuthorityRequest{}).Do())
	if got = must(cas.Get(got.Name).Do()); got.State != "DISABLED" {
		t.Fatalf("%+v", got)
	}

	_, err := pools.Create(parent, &privateca.CaPool{}).CaPoolId("p2").Do()
	wantCode(t, err, 400, "tier")
	_, err = pools.Create(parent, &privateca.CaPool{Tier: "DEVOPS"}).CaPoolId("pool").Do()
	wantCode(t, err, 409, "already exists")
	_, err = pools.Create(parent, &privateca.CaPool{Tier: "DEVOPS"}).Do()
	wantCode(t, err, 400, "caPoolId")
	bad := ca()
	bad.KeySpec = nil
	_, err = cas.Create(pool, bad).CertificateAuthorityId("ca2").Do()
	wantCode(t, err, 400, "keySpec")
	_, err = cas.Create(parent+"/caPools/nopool", ca()).CertificateAuthorityId("ca2").Do()
	wantCode(t, err, 404, "nopool")
	_, err = cas.Enable(pool+"/certificateAuthorities/nope", &privateca.EnableCertificateAuthorityRequest{}).Do()
	wantCode(t, err, 404, "nope")
	_, err = cas.Get(pool + "/certificateAuthorities/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = pools.Delete(parent + "/caPools/nope").Do()
	wantCode(t, err, 404, "nope")
}

func TestHealthcare(t *testing.T) {
	svc := must(healthcare.NewService(ctx, opts(startMore(t), "/healthcare/")...))
	ds := svc.Projects.Locations.Datasets
	must(ds.Create(parent, &healthcare.Dataset{TimeZone: "UTC"}).DatasetId("d1").Do())
	got := must(ds.Get(parent + "/datasets/d1").Do())
	if got.Name != parent+"/datasets/d1" || got.TimeZone != "UTC" {
		t.Fatalf("%+v", got)
	}
	// patch answers the dataset itself, not an operation
	if got = must(ds.Patch(got.Name, &healthcare.Dataset{TimeZone: "Asia/Kolkata"}).UpdateMask("timeZone").Do()); got.TimeZone != "Asia/Kolkata" {
		t.Fatalf("%+v", got)
	}
	_, err := ds.Create(parent, &healthcare.Dataset{}).DatasetId("d1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = ds.Create(parent, &healthcare.Dataset{}).Do()
	wantCode(t, err, 400, "datasetId")
	_, err = ds.Get(parent + "/datasets/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = ds.Delete(parent + "/datasets/nope").Do()
	wantCode(t, err, 404, "nope")
	must(ds.Delete(got.Name).Do())
}

func TestDataplex(t *testing.T) {
	svc := must(dataplex.NewService(ctx, opts(startMore(t), "/dataplex/")...))
	lakes := svc.Projects.Locations.Lakes
	must(lakes.Create(parent, &dataplex.GoogleCloudDataplexV1Lake{DisplayName: "Lake", Labels: map[string]string{"env": "dev"}}).LakeId("lake1").Do())
	got := must(lakes.Get(parent + "/lakes/lake1").Do())
	if got.State != "ACTIVE" || got.DisplayName != "Lake" || got.Uid == "" || got.Labels["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
	_, err := lakes.Create(parent, &dataplex.GoogleCloudDataplexV1Lake{}).LakeId("lake1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = lakes.Create(parent, &dataplex.GoogleCloudDataplexV1Lake{}).LakeId("Bad Id").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = lakes.Get(parent + "/lakes/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = lakes.Delete(parent + "/lakes/nope").Do()
	wantCode(t, err, 404, "nope")
	must(lakes.Delete(got.Name).Do())
}

func TestBigQueryDataTransfer(t *testing.T) {
	svc := must(bqdt.NewService(ctx, opts(startMore(t), "/bigquerydatatransfer/")...))
	cfgs := svc.Projects.Locations.TransferConfigs
	in := func() *bqdt.TransferConfig {
		return &bqdt.TransferConfig{DisplayName: "q", DataSourceId: "scheduled_query", DestinationDatasetId: "ds", Schedule: "every 24 hours",
			Params: googleapi.RawMessage(`{"query":"SELECT 1"}`)}
	}
	// the server names a config: no id is sent
	a := must(cfgs.Create(parent, in()).Do())
	b := must(cfgs.Create(parent, in()).Do())
	if a.Name == b.Name || !strings.HasPrefix(a.Name, parent+"/transferConfigs/") {
		t.Fatalf("%q %q", a.Name, b.Name)
	}
	got := must(cfgs.Get(a.Name).Do())
	if got.DisplayName != "q" || got.DataSourceId != "scheduled_query" || got.State != "" {
		t.Fatalf("%+v", got)
	}
	got = must(cfgs.Patch(a.Name, &bqdt.TransferConfig{DisplayName: "renamed"}).UpdateMask("displayName").Do())
	if got.DisplayName != "renamed" || got.DataSourceId != "scheduled_query" {
		t.Fatalf("%+v", got)
	}
	if l := must(cfgs.List(parent).Do()); len(l.TransferConfigs) != 2 {
		t.Fatalf("%+v", l)
	}
	_, err := cfgs.Get(parent + "/transferConfigs/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = cfgs.Delete(parent + "/transferConfigs/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = cfgs.Patch(a.Name, in()).UpdateMask("bogus").Do()
	wantCode(t, err, 400, "bogus")
	must(cfgs.Delete(a.Name).Do())
	_, err = cfgs.Get(a.Name).Do()
	wantCode(t, err, 404, a.Name)
}

func TestStorageTransfer(t *testing.T) {
	svc := must(storagetransfer.NewService(ctx, opts(startMore(t), "/storagetransfer/")...))
	jobs := svc.TransferJobs
	spec := &storagetransfer.TransferSpec{GcsDataSource: &storagetransfer.GcsData{BucketName: "src"}, GcsDataSink: &storagetransfer.GcsData{BucketName: "dst"}}
	a := must(jobs.Create(&storagetransfer.TransferJob{ProjectId: "p", Description: "copy", TransferSpec: spec}).Do())
	if a.Status != "ENABLED" || len(a.Name) < len("transferJobs/1") || a.Name[:13] != "transferJobs/" || a.CreationTime == "" {
		t.Fatalf("%+v", a)
	}
	named := must(jobs.Create(&storagetransfer.TransferJob{ProjectId: "p", Name: "transferJobs/mine", TransferSpec: spec}).Do())
	if named.Name != "transferJobs/mine" {
		t.Fatalf("%+v", named)
	}
	got := must(jobs.Get(a.Name, "p").Do())
	if got.Description != "copy" || got.TransferSpec.GcsDataSink.BucketName != "dst" {
		t.Fatalf("%+v", got)
	}
	l := must(jobs.List(`{"projectId":"p"}`).Do())
	if len(l.TransferJobs) != 2 {
		t.Fatalf("%+v", l)
	}
	if l = must(jobs.List(`{"projectId":"other"}`).Do()); len(l.TransferJobs) != 0 {
		t.Fatalf("%+v", l)
	}

	// patch with a mask, then DELETED keeps the job (as the real API does)
	up := &storagetransfer.UpdateTransferJobRequest{ProjectId: "p", UpdateTransferJobFieldMask: "description",
		TransferJob: &storagetransfer.TransferJob{Description: "new", Status: "DISABLED"}}
	if got = must(jobs.Patch(a.Name, up).Do()); got.Description != "new" || got.Status != "ENABLED" {
		t.Fatalf("%+v", got)
	}
	up.UpdateTransferJobFieldMask = "status"
	up.TransferJob = &storagetransfer.TransferJob{Status: "DELETED"}
	if got = must(jobs.Patch(a.Name, up).Do()); got.Status != "DELETED" || got.DeletionTime == "" {
		t.Fatalf("%+v", got)
	}
	if got = must(jobs.Get(a.Name, "p").Do()); got.Status != "DELETED" {
		t.Fatalf("%+v", got)
	}
	must(jobs.Delete(named.Name, "p").Do())

	// failures
	_, err := jobs.Create(&storagetransfer.TransferJob{Description: "no project"}).Do()
	wantCode(t, err, 400, "projectId")
	_, err = jobs.Create(&storagetransfer.TransferJob{ProjectId: "p", Name: "transferJobs/" + a.Name[13:]}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = jobs.Create(&storagetransfer.TransferJob{ProjectId: "p", Name: "badname"}).Do()
	wantCode(t, err, 400, "job name")
	_, err = jobs.Get("transferJobs/nope", "p").Do()
	wantCode(t, err, 404, "nope")
	_, err = jobs.Get(a.Name, "other").Do()
	wantCode(t, err, 404, a.Name)
	_, err = jobs.Get(a.Name, "").Do()
	wantCode(t, err, 400, "projectId")
	_, err = jobs.Delete("transferJobs/nope", "p").Do()
	wantCode(t, err, 404, "nope")
	_, err = jobs.Patch(a.Name, &storagetransfer.UpdateTransferJobRequest{ProjectId: "p", UpdateTransferJobFieldMask: "bogus",
		TransferJob: &storagetransfer.TransferJob{}}).Do()
	wantCode(t, err, 400, "bogus")
	_, err = jobs.List("not json").Do()
	wantCode(t, err, 400, "filter")
}
