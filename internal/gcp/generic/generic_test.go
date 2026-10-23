package generic_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	cm "google.golang.org/api/certificatemanager/v1"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/file/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var ctx = context.Background()

func start(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"dns", "file", "certificatemanager", "operations"}})
}

func opts(srv *pogi.Running, prefix string) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(srv.URL + prefix), option.WithoutAuthentication()}
}

// wantCode fails unless err is a Google API error with this HTTP status and a message holding want.
func wantCode(t *testing.T, err error, code int, want string) {
	t.Helper()
	var ge *googleapi.Error
	if !errors.As(err, &ge) || ge.Code != code || !strings.Contains(ge.Message, want) {
		t.Fatalf("want %d %q, got %v", code, want, err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestDNS(t *testing.T) {
	svc := must(dns.NewService(ctx, opts(start(t), "/")...))
	zones := svc.ManagedZones
	z := must(zones.Create("p", &dns.ManagedZone{Name: "example", DnsName: "example.com.", Description: "d", Labels: map[string]string{"env": "dev"},
		PrivateVisibilityConfig: &dns.ManagedZonePrivateVisibilityConfig{ForceSendFields: []string{"Networks"}}}).Do())
	if z.PrivateVisibilityConfig != nil { // Terraform sends it for public zones; Cloud DNS does not keep it
		t.Fatalf("%+v", z.PrivateVisibilityConfig)
	}
	if z.Id == 0 || z.Kind != "dns#managedZone" || len(z.NameServers) != 4 || z.Visibility != "public" || z.CreationTime == "" || z.Labels["env"] != "dev" {
		t.Fatalf("%+v", z)
	}
	if got := must(zones.Get("p", "example").Do()); got.Id != z.Id {
		t.Fatalf("%+v", got)
	}

	// failures
	_, err := zones.Create("p", &dns.ManagedZone{Name: "example", DnsName: "example.com."}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = zones.Create("p", &dns.ManagedZone{DnsName: "x.com."}).Do()
	wantCode(t, err, 400, `"name"`)
	_, err = zones.Create("p", &dns.ManagedZone{Name: "x", DnsName: "no-dot.com"}).Do()
	wantCode(t, err, 400, "dnsName")
	_, err = zones.Create("p", &dns.ManagedZone{Name: "Bad_Name", DnsName: "x.com."}).Do()
	wantCode(t, err, 400, "name")
	_, err = zones.Get("p", "nope").Do()
	wantCode(t, err, 404, "nope")
	err = zones.Delete("p", "nope").Do()
	wantCode(t, err, 404, "nope")

	// record sets go in through changes
	rr := &dns.ResourceRecordSet{Name: "www.example.com.", Type: "A", Ttl: 300, Rrdatas: []string{"1.2.3.4"}}
	ch := must(svc.Changes.Create("p", "example", &dns.Change{Additions: []*dns.ResourceRecordSet{rr}}).Do())
	if ch.Status != "done" || ch.Id == "" || ch.Kind != "dns#change" {
		t.Fatalf("%+v", ch)
	}
	_, err = svc.Changes.Create("p", "example", &dns.Change{Additions: []*dns.ResourceRecordSet{rr}}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = svc.Changes.Create("p", "example", &dns.Change{Deletions: []*dns.ResourceRecordSet{{Name: "no.example.com.", Type: "A", Rrdatas: []string{"1.1.1.1"}}}}).Do()
	wantCode(t, err, 404, "does not exist")
	_, err = svc.Changes.Create("p", "gone", &dns.Change{Additions: []*dns.ResourceRecordSet{rr}}).Do()
	wantCode(t, err, 404, "gone")
	_, err = svc.Changes.Create("p", "example", &dns.Change{}).Do()
	wantCode(t, err, 400, "at least one")
	must(svc.Changes.Create("p", "example", &dns.Change{Additions: []*dns.ResourceRecordSet{{Name: "mail.example.com.", Type: "MX", Ttl: 60, Rrdatas: []string{"10 m.example.com."}}}}).Do())
	if l := must(svc.ResourceRecordSets.List("p", "example").Do()); len(l.Rrsets) != 2 {
		t.Fatalf("%+v", l.Rrsets)
	}
	if l := must(svc.ResourceRecordSets.List("p", "example").Name("www.example.com.").Type("A").Do()); len(l.Rrsets) != 1 || l.Rrsets[0].Rrdatas[0] != "1.2.3.4" {
		t.Fatalf("%+v", l.Rrsets)
	}
	if l := must(svc.ResourceRecordSets.List("p", "example").MaxResults(1).Do()); len(l.Rrsets) != 1 || l.NextPageToken == "" {
		t.Fatalf("%+v", l)
	}
	// a change replaces a record set: deletion and addition together
	rr2 := &dns.ResourceRecordSet{Name: "www.example.com.", Type: "A", Ttl: 60, Rrdatas: []string{"5.6.7.8"}}
	must(svc.Changes.Create("p", "example", &dns.Change{Deletions: []*dns.ResourceRecordSet{rr}, Additions: []*dns.ResourceRecordSet{rr2}}).Do())
	if got := must(svc.ResourceRecordSets.Get("p", "example", "www.example.com.", "A").Do()); got.Rrdatas[0] != "5.6.7.8" {
		t.Fatalf("%+v", got)
	}
	if l := must(svc.Changes.List("p", "example").Do()); len(l.Changes) != 3 {
		t.Fatalf("%+v", l.Changes)
	}
	must(svc.ResourceRecordSets.Delete("p", "example", "mail.example.com.", "MX").Do())
	_, err = svc.ResourceRecordSets.Get("p", "example", "mail.example.com.", "MX").Do()
	wantCode(t, err, 404, "mail.example.com.")

	// patch answers with a finished operation that can be read back
	op := must(zones.Patch("p", "example", &dns.ManagedZone{Description: "new"}).Do())
	if op.Status != "done" || op.Id == "" {
		t.Fatalf("%+v", op)
	}
	if got := must(svc.ManagedZoneOperations.Get("p", "example", op.Id).Do()); got.Status != "done" {
		t.Fatalf("%+v", got)
	}
	if got := must(zones.Get("p", "example").Do()); got.Description != "new" || got.DnsName != "example.com." {
		t.Fatalf("%+v", got)
	}

	// pages
	must(zones.Create("p", &dns.ManagedZone{Name: "second", DnsName: "second.com."}).Do())
	l := must(zones.List("p").MaxResults(1).Do())
	if len(l.ManagedZones) != 1 || l.ManagedZones[0].Name != "example" || l.NextPageToken == "" {
		t.Fatalf("%+v", l)
	}
	if l = must(zones.List("p").MaxResults(1).PageToken(l.NextPageToken).Do()); len(l.ManagedZones) != 1 || l.ManagedZones[0].Name != "second" || l.NextPageToken != "" {
		t.Fatalf("%+v", l)
	}
	if err := zones.Delete("p", "example").Do(); err != nil {
		t.Fatal(err)
	}
	_, err = zones.Get("p", "example").Do()
	wantCode(t, err, 404, "example")
	if l := must(zones.List("p").Do()); len(l.ManagedZones) != 1 {
		t.Fatalf("%+v", l)
	}
}

func TestFilestore(t *testing.T) {
	srv := start(t)
	svc := must(file.NewService(ctx, opts(srv, "/file/")...))
	insts := svc.Projects.Locations.Instances
	parent := "projects/p/locations/us-central1-b"
	inst := func() *file.Instance {
		return &file.Instance{Tier: "BASIC_HDD", Description: "d", Labels: map[string]string{"env": "prod"},
			FileShares: []*file.FileShareConfig{{Name: "share", CapacityGb: 1024}}, Networks: []*file.NetworkConfig{{Network: "default", Modes: []string{"MODE_IPV4"}}}}
	}

	op := must(insts.Create(parent, inst()).InstanceId("fs1").Do())
	if !op.Done || !strings.HasPrefix(op.Name, "projects/p/locations/us-central1-b/operations/") {
		t.Fatalf("%+v", op)
	}
	var resp map[string]any
	json.Unmarshal(op.Response, &resp)
	if resp["@type"] != "type.googleapis.com/google.cloud.file.v1.Instance" || resp["name"] != parent+"/instances/fs1" || resp["state"] != "READY" {
		t.Fatalf("%v", resp)
	}
	polled := must(svc.Projects.Locations.Operations.Get(op.Name).Do())
	if !polled.Done || polled.Name != op.Name || polled.Metadata == nil {
		t.Fatalf("%+v", polled)
	}
	got := must(insts.Get(parent + "/instances/fs1").Do())
	if got.State != "READY" || got.Name != parent+"/instances/fs1" || got.CreateTime == "" || got.Etag == "" || got.Labels["env"] != "prod" ||
		got.Networks[0].IpAddresses[0] == "" {
		t.Fatalf("%+v", got)
	}

	// failures
	_, err := insts.Create(parent, inst()).InstanceId("fs1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = insts.Create(parent, inst()).Do()
	wantCode(t, err, 400, "instanceId")
	_, err = insts.Create(parent, inst()).InstanceId("bad id!").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = insts.Create(parent, &file.Instance{Tier: "BASIC_HDD"}).InstanceId("fs2").Do()
	wantCode(t, err, 400, "fileShares")
	_, err = insts.Get(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = insts.Delete(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = insts.Patch(parent+"/instances/fs1", &file.Instance{Description: "x"}).UpdateMask("nosuchfield").Do()
	wantCode(t, err, 400, "nosuchfield")
	_, err = insts.Patch(parent+"/instances/nope", &file.Instance{Description: "x"}).UpdateMask("description").Do()
	wantCode(t, err, 404, "nope")
	_, err = insts.Restore(parent+"/instances/fs1", &file.RestoreInstanceRequest{}).Do()
	wantCode(t, err, 501, "file.projects.locations.instances.restore")
	snap := svc.Projects.Locations.Instances.Snapshots
	_, err = snap.Create(parent+"/instances/missing", &file.Snapshot{}).SnapshotId("s").Do()
	wantCode(t, err, 404, "missing")

	// patch: only the masked fields change; no mask means the fields sent
	must(insts.Patch(parent+"/instances/fs1", &file.Instance{Description: "masked", Labels: map[string]string{"env": "dev"}}).UpdateMask("description").Do())
	got = must(insts.Get(parent + "/instances/fs1").Do())
	if got.Description != "masked" || got.Labels["env"] != "prod" {
		t.Fatalf("%+v", got)
	}
	before := got.Etag
	must(insts.Patch(parent+"/instances/fs1", &file.Instance{Labels: map[string]string{"env": "dev"}}).Do())
	got = must(insts.Get(parent + "/instances/fs1").Do())
	if got.Description != "masked" || got.Labels["env"] != "dev" || got.Etag == before {
		t.Fatalf("%+v", got)
	}

	// lists: pages and filters
	a := inst()
	a.Labels = map[string]string{"env": "prod"}
	must(insts.Create(parent, a).InstanceId("fs3").Do())
	must(insts.Create("projects/p/locations/europe-west1-b", inst()).InstanceId("other").Do())
	l := must(insts.List(parent).PageSize(1).Do())
	if len(l.Instances) != 1 || l.Instances[0].Name != parent+"/instances/fs1" || l.NextPageToken == "" {
		t.Fatalf("%+v", l)
	}
	if l = must(insts.List(parent).PageSize(1).PageToken(l.NextPageToken).Do()); len(l.Instances) != 1 || l.Instances[0].Name != parent+"/instances/fs3" || l.NextPageToken != "" {
		t.Fatalf("%+v", l)
	}
	if l = must(insts.List(parent).Filter("labels.env=prod").Do()); len(l.Instances) != 1 || l.Instances[0].Name != parent+"/instances/fs3" {
		t.Fatalf("%+v", l)
	}
	_, err = insts.List(parent).Filter("a = ((").Do()
	wantCode(t, err, 400, "filter")

	// delete: an operation, then gone
	del := must(insts.Delete(parent + "/instances/fs1").Do())
	json.Unmarshal(del.Response, &resp)
	if !del.Done || resp["@type"] != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("%+v %v", del, resp)
	}
	_, err = insts.Get(parent + "/instances/fs1").Do()
	wantCode(t, err, 404, "fs1")
	if _, err = insts.Delete(parent + "/instances/fs1").Do(); err == nil {
		t.Fatal("a second delete worked")
	}

	// the same path under another API's host is not Filestore's, and without the prefix on localhost it still is
	req := func(host, path string) (int, string) {
		r, _ := http.NewRequest("GET", srv.URL+path, nil)
		r.Host = host
		resp := must(http.DefaultClient.Do(r))
		defer resp.Body.Close()
		var b struct{ Error struct{ Message string } }
		json.NewDecoder(resp.Body).Decode(&b)
		return resp.StatusCode, b.Error.Message
	}
	if code, msg := req("redis.googleapis.com", "/v1/"+parent+"/instances/fs3"); code != 404 || !strings.Contains(msg, "redis.googleapis.com") {
		t.Fatalf("redis host: %d %s", code, msg)
	}
	if code, _ := req("file.googleapis.com", "/v1/"+parent+"/instances/fs3"); code != 200 {
		t.Fatalf("file host: %d", code)
	}
	if code, _ := req("", "/v1/"+parent+"/instances/fs3"); code != 200 {
		t.Fatalf("no prefix on localhost: %d", code)
	}
}

func TestUnlisted(t *testing.T) {
	srv := start(t)
	call := func(method, path string) (int, string) {
		r, _ := http.NewRequest(method, srv.URL+path, nil)
		if strings.HasPrefix(path, "/v1/") {
			r.Host = "dataflow.googleapis.com"
		}
		resp := must(http.DefaultClient.Do(r))
		defer resp.Body.Close()
		var b struct {
			Error struct{ Status, Message string }
		}
		json.NewDecoder(resp.Body).Decode(&b)
		return resp.StatusCode, b.Error.Status + " " + b.Error.Message
	}
	// an API that is not listed
	if code, msg := call("GET", "/v1/projects/p/jobs"); code != 501 || !strings.Contains(msg, "/v1/projects/p/jobs") {
		t.Fatalf("%d %s", code, msg)
	}
	// a method the listed API does not have
	if code, msg := call("DELETE", "/file/v1/projects/p/locations/l/instances"); code != 501 || !strings.Contains(msg, "UNIMPLEMENTED") || !strings.Contains(msg, "/v1/projects/p/locations/l/instances") {
		t.Fatalf("%d %s", code, msg)
	}
	// the operations of an unknown name
	if code, _ := call("GET", "/file/v1/projects/p/locations/l/operations/operation-nope"); code != 404 {
		t.Fatalf("%d", code)
	}
}

func TestCertificateManager(t *testing.T) {
	svc := must(cm.NewService(ctx, opts(start(t), "/certificatemanager/")...))
	locs := svc.Projects.Locations
	parent := "projects/p/locations/global"

	auth := must(locs.DnsAuthorizations.Create(parent, &cm.DnsAuthorization{Domain: "example.com", Labels: map[string]string{"a": "b"}}).DnsAuthorizationId("auth1").Do())
	if !auth.Done {
		t.Fatalf("%+v", auth)
	}
	da := must(locs.DnsAuthorizations.Get(parent + "/dnsAuthorizations/auth1").Do())
	if da.Domain != "example.com" || da.Type != "FIXED_RECORD" || da.DnsResourceRecord == nil || da.DnsResourceRecord.Type != "CNAME" ||
		!strings.HasSuffix(da.DnsResourceRecord.Name, ".example.com.") || da.CreateTime == "" {
		t.Fatalf("%+v", da)
	}
	cert := &cm.Certificate{Managed: &cm.ManagedCertificate{Domains: []string{"example.com"}, DnsAuthorizations: []string{da.Name}}}
	must(locs.Certificates.Create(parent, cert).CertificateId("cert1").Do())
	c := must(locs.Certificates.Get(parent + "/certificates/cert1").Do())
	if c.Managed.State != "ACTIVE" || c.Scope != "DEFAULT" || c.Managed.DnsAuthorizations[0] != da.Name {
		t.Fatalf("%+v", c)
	}
	must(locs.CertificateMaps.Create(parent, &cm.CertificateMap{Description: "m"}).CertificateMapId("map1").Do())
	must(locs.CertificateMaps.CertificateMapEntries.Create(parent+"/certificateMaps/map1", &cm.CertificateMapEntry{Hostname: "example.com", Certificates: []string{c.Name}}).CertificateMapEntryId("e1").Do())
	if l := must(locs.CertificateMaps.CertificateMapEntries.List(parent + "/certificateMaps/map1").Do()); len(l.CertificateMapEntries) != 1 {
		t.Fatalf("%+v", l)
	}
	if l := must(locs.Certificates.List(parent).Do()); len(l.Certificates) != 1 {
		t.Fatalf("%+v", l)
	}

	// failures
	_, err := locs.DnsAuthorizations.Create(parent, &cm.DnsAuthorization{}).DnsAuthorizationId("auth2").Do()
	wantCode(t, err, 400, "domain")
	_, err = locs.Certificates.Create(parent, cert).CertificateId("cert1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = locs.CertificateMaps.CertificateMapEntries.Create(parent+"/certificateMaps/nomap", &cm.CertificateMapEntry{Hostname: "x"}).CertificateMapEntryId("e").Do()
	wantCode(t, err, 404, "nomap")
	_, err = locs.Certificates.Patch(parent+"/certificates/cert1", &cm.Certificate{Description: "x"}).UpdateMask("bogus.path").Do()
	wantCode(t, err, 400, "bogus.path")
	must(locs.Certificates.Patch(parent+"/certificates/cert1", &cm.Certificate{Description: "patched"}).UpdateMask("description").Do())
	if c = must(locs.Certificates.Get(parent + "/certificates/cert1").Do()); c.Description != "patched" || c.Managed == nil {
		t.Fatalf("%+v", c)
	}
	// deleting a map removes its entries
	must(locs.CertificateMaps.Delete(parent + "/certificateMaps/map1").Do())
	_, err = locs.CertificateMaps.CertificateMapEntries.Get(parent + "/certificateMaps/map1/certificateMapEntries/e1").Do()
	wantCode(t, err, 404, "e1")
}

// Resources survive a save and a load, and are gone after a reset.
func TestPersistence(t *testing.T) {
	srv := start(t)
	svc := must(file.NewService(ctx, opts(srv, "/file/")...))
	parent := "projects/p/locations/l"
	must(svc.Projects.Locations.Instances.Create(parent, &file.Instance{Tier: "BASIC_HDD", Description: "kept",
		FileShares: []*file.FileShareConfig{{Name: "s"}}, Networks: []*file.NetworkConfig{{Network: "default"}}}).InstanceId("kept").Do())
	st := srv.Hub.Service("file").(core.Stateful)
	saved := must(st.Save())
	st.Reset()
	_, err := svc.Projects.Locations.Instances.Get(parent + "/instances/kept").Do()
	wantCode(t, err, 404, "kept")
	if err := st.Load(saved); err != nil {
		t.Fatal(err)
	}
	if got := must(svc.Projects.Locations.Instances.Get(parent + "/instances/kept").Do()); got.Description != "kept" {
		t.Fatalf("%+v", got)
	}
}
