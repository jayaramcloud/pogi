package compute_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	computev1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

const (
	project = "proj"
	zone    = "us-central1-a"
)

func start(t *testing.T, dockerHost string) *pogi.Running {
	t.Helper()
	return pogi.Start(t, pogi.Config{Services: []string{"compute"}, DockerHost: dockerHost,
		DockerImages: map[string]string{"compute.debian": "alpine:latest"}}) // alpine has wget and a small pull
}

func opts(srv *pogi.Running) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()}
}

func code(err error) int {
	var ae *apierror.APIError
	if errors.As(err, &ae) {
		return ae.HTTPCode()
	}
	return 0
}

// waitFor waits for a Compute Engine operation and fails on its error.
func waitFor(op *compute.Operation, err error) {
	check(err)
	check(op.Wait(ctx))
}

func instance(name, script string) *computepb.Instance {
	i := &computepb.Instance{Name: proto.String(name), MachineType: proto.String("zones/" + zone + "/machineTypes/e2-micro"),
		Disks: []*computepb.AttachedDisk{{Boot: proto.Bool(true), AutoDelete: proto.Bool(true), InitializeParams: &computepb.AttachedDiskInitializeParams{
			SourceImage: proto.String("projects/debian-cloud/global/images/family/debian-12"), DiskSizeGb: proto.Int64(12)}}},
		NetworkInterfaces: []*computepb.NetworkInterface{{Network: proto.String("global/networks/default"),
			AccessConfigs: []*computepb.AccessConfig{{Name: proto.String("External NAT")}}}},
		ServiceAccounts: []*computepb.ServiceAccount{{Email: proto.String("sa@proj.iam.gserviceaccount.com"), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}},
		Labels:          map[string]string{"env": "test"}}
	i.Metadata = &computepb.Metadata{Items: []*computepb.Items{{Key: proto.String("owner"), Value: proto.String("ada")}}}
	if script != "" {
		i.Metadata.Items = append(i.Metadata.Items, &computepb.Items{Key: proto.String("startup-script"), Value: proto.String(script)})
	}
	return i
}

func get(t *testing.T, url string, header bool) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if header {
		req.Header.Set("Metadata-Flavor", "Google")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func TestStaticLists(t *testing.T) {
	srv := start(t, "unix:///nonexistent")
	zones := must(compute.NewZonesRESTClient(ctx, opts(srv)...))
	z := must(zones.Get(ctx, &computepb.GetZoneRequest{Project: project, Zone: zone}))
	if z.GetName() != zone || z.GetStatus() != "UP" {
		t.Fatalf("zone = %v", z)
	}
	n := 0
	for it := zones.List(ctx, &computepb.ListZonesRequest{Project: project, Filter: proto.String(`name = "us-east1-b"`)}); ; n++ {
		if _, err := it.Next(); err != nil {
			if err != iterator.Done {
				t.Fatal(err)
			}
			break
		}
	}
	if n != 1 {
		t.Fatalf("filtered zones = %d", n)
	}
	mt := must(compute.NewMachineTypesRESTClient(ctx, opts(srv)...))
	m := must(mt.Get(ctx, &computepb.GetMachineTypeRequest{Project: project, Zone: zone, MachineType: "e2-standard-4"}))
	if m.GetGuestCpus() != 4 || m.GetMemoryMb() != 16384 {
		t.Fatalf("machine type = %v", m)
	}
	im := must(compute.NewImagesRESTClient(ctx, opts(srv)...))
	img := must(im.GetFromFamily(ctx, &computepb.GetFromFamilyImageRequest{Project: "debian-cloud", Family: "debian-12"}))
	if img.GetFamily() != "debian-12" || !strings.Contains(img.GetSelfLink(), "projects/debian-cloud/global/images/debian-12") {
		t.Fatalf("image = %v", img)
	}
	if _, err := mt.Get(ctx, &computepb.GetMachineTypeRequest{Project: project, Zone: zone, MachineType: "nope"}); code(err) != 404 {
		t.Fatalf("missing machine type: %v", err)
	}
}

// Instances without a container engine: metadata only, RUNNING at once.
func TestInstancesWithoutDocker(t *testing.T) {
	srv := start(t, "unix:///nonexistent")
	instances := must(compute.NewInstancesRESTClient(ctx, opts(srv)...))
	disks := must(compute.NewDisksRESTClient(ctx, opts(srv)...))

	waitFor(instances.Insert(ctx, &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: instance("web", "")}))
	i := must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "web"}))
	nic := i.GetNetworkInterfaces()[0]
	if i.GetStatus() != "RUNNING" || !strings.HasSuffix(i.GetMachineType(), "/machineTypes/e2-micro") || !strings.HasPrefix(nic.GetNetworkIP(), "10.128.") ||
		nic.GetAccessConfigs()[0].GetNatIP() == "" || len(i.GetDisks()) != 1 || i.GetDisks()[0].GetDiskSizeGb() != 12 || i.GetLabels()["env"] != "test" {
		t.Fatalf("instance = %v", i)
	}
	// the boot disk was made from the image
	d := must(disks.Get(ctx, &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "web"}))
	if d.GetSizeGb() != 12 || !strings.Contains(d.GetSourceImage(), "/projects/debian-cloud/global/images/debian-12-") || len(d.GetUsers()) != 1 {
		t.Fatalf("disk = %v", d)
	}
	if _, err := instances.Insert(ctx, &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: instance("web", "")}); code(err) != 409 {
		t.Fatalf("duplicate: %v", err)
	}
	bad := instance("bad", "")
	bad.MachineType = proto.String("e2-nonsense")
	if _, err := instances.Insert(ctx, &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: bad}); code(err) != 400 {
		t.Fatalf("bad machine type: %v", err)
	}
	if _, err := disks.Get(ctx, &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "bad"}); code(err) != 404 {
		t.Fatalf("a failed insert left a disk: %v", err)
	}

	// list, and the aggregated list
	waitFor(instances.Insert(ctx, &computepb.InsertInstanceRequest{Project: project, Zone: "us-east1-b", InstanceResource: instance("db", "")}))
	names := map[string]bool{}
	for it := instances.List(ctx, &computepb.ListInstancesRequest{Project: project, Zone: zone}); ; {
		x, err := it.Next()
		if err == iterator.Done {
			break
		}
		check(err)
		names[x.GetName()] = true
	}
	if len(names) != 1 || !names["web"] {
		t.Fatalf("list = %v", names)
	}
	all := map[string]bool{}
	for it := instances.AggregatedList(ctx, &computepb.AggregatedListInstancesRequest{Project: project}); ; {
		x, err := it.Next()
		if err == iterator.Done {
			break
		}
		check(err)
		for _, in := range x.Value.GetInstances() {
			all[x.Key+"/"+in.GetName()] = true
		}
	}
	if len(all) != 2 || !all["zones/"+zone+"/web"] || !all["zones/us-east1-b/db"] {
		t.Fatalf("aggregated = %v", all)
	}

	// setMetadata takes the fingerprint it read; a stale one is refused
	md := i.GetMetadata()
	md.Items = append(md.Items, &computepb.Items{Key: proto.String("added"), Value: proto.String("yes")})
	waitFor(instances.SetMetadata(ctx, &computepb.SetMetadataInstanceRequest{Project: project, Zone: zone, Instance: "web", MetadataResource: md}))
	if _, err := instances.SetMetadata(ctx, &computepb.SetMetadataInstanceRequest{Project: project, Zone: zone, Instance: "web", MetadataResource: md}); code(err) != 412 {
		t.Fatalf("stale fingerprint: %v", err)
	}
	waitFor(instances.SetLabels(ctx, &computepb.SetLabelsInstanceRequest{Project: project, Zone: zone, Instance: "web", InstancesSetLabelsRequestResource: &computepb.InstancesSetLabelsRequest{
		LabelFingerprint: i.LabelFingerprint, Labels: map[string]string{"env": "prod"}}}))
	i = must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "web"}))
	if i.GetLabels()["env"] != "prod" || len(i.GetMetadata().GetItems()) != 2 {
		t.Fatalf("after updates: %v", i)
	}

	// the metadata server
	base := srv.URL + "/_gcp/metadata/" + project + "/" + zone + "/web/computeMetadata/v1/"
	if c, _, _ := get(t, base+"instance/name", false); c != 403 {
		t.Fatalf("without the header: %d", c)
	}
	for path, want := range map[string]string{"instance/name": "web", "project/project-id": project, "instance/attributes/owner": "ada",
		"instance/zone": "/zones/" + zone, "instance/hostname": "web." + zone + ".c.proj.internal", "instance/service-accounts/default/email": "sa@proj.iam.gserviceaccount.com"} {
		c, body, h := get(t, base+path, true)
		if c != 200 || !strings.HasSuffix(body, want) || h.Get("Metadata-Flavor") != "Google" {
			t.Fatalf("%s = %d %q", path, c, body)
		}
	}
	if _, body, _ := get(t, base+"instance/attributes/", true); body != "added\nowner" {
		t.Fatalf("attributes = %q", body)
	}
	_, body, _ := get(t, base+"instance/service-accounts/default/token", true)
	var tok struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	check(json.Unmarshal([]byte(body), &tok))
	if at, ok := gapi.ParseAccessToken(tok.AccessToken); !ok || at.Email != "sa@proj.iam.gserviceaccount.com" || tok.TokenType != "Bearer" {
		t.Fatalf("token = %s", body)
	}
	if c, body, _ := get(t, base+"instance/service-accounts/default/identity?audience=https://x", true); c != 200 || strings.Count(body, ".") != 2 {
		t.Fatalf("identity = %d %q", c, body)
	}
	_, body, _ = get(t, base+"instance/?recursive=true", true)
	var rec struct {
		Name              string
		MachineType       string
		NetworkInterfaces []struct{ IP string }
		Attributes        map[string]string
		ServiceAccounts   map[string]struct{ Email string }
	}
	check(json.Unmarshal([]byte(body), &rec))
	if rec.Name != "web" || !strings.HasSuffix(rec.MachineType, "e2-micro") || len(rec.NetworkInterfaces) != 1 || rec.Attributes["owner"] != "ada" ||
		rec.ServiceAccounts["default"].Email == "" {
		t.Fatalf("recursive = %s", body)
	}
	// with two instances the plain path cannot say whose it is
	if c, _, _ := get(t, srv.URL+"/computeMetadata/v1/instance/name", true); c != 404 {
		t.Fatalf("ambiguous: %d", c)
	}

	// guest attributes, written by the instance and read through the API
	req, _ := http.NewRequest("PUT", base+"instance/guest-attributes/pogi/name", strings.NewReader("web"))
	req.Header.Set("Metadata-Flavor", "Google")
	resp := must(http.DefaultClient.Do(req))
	resp.Body.Close()
	ga := must(instances.GetGuestAttributes(ctx, &computepb.GetGuestAttributesInstanceRequest{Project: project, Zone: zone, Instance: "web", QueryPath: proto.String("pogi/")}))
	if it := ga.GetQueryValue().GetItems(); len(it) != 1 || it[0].GetKey() != "name" || it[0].GetValue() != "web" {
		t.Fatalf("guest attributes = %v", ga)
	}
	if _, err := instances.GetGuestAttributes(ctx, &computepb.GetGuestAttributesInstanceRequest{Project: project, Zone: zone, Instance: "web", QueryPath: proto.String("none/")}); code(err) != 404 {
		t.Fatalf("missing guest attributes: %v", err)
	}

	// stop, start, reset, delete
	waitFor(instances.Stop(ctx, &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "web"}))
	if s := must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "web"})).GetStatus(); s != "TERMINATED" {
		t.Fatalf("after stop: %s", s)
	}
	waitFor(instances.Start(ctx, &computepb.StartInstanceRequest{Project: project, Zone: zone, Instance: "web"}))
	waitFor(instances.Reset(ctx, &computepb.ResetInstanceRequest{Project: project, Zone: zone, Instance: "web"}))
	if s := must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "web"})).GetStatus(); s != "RUNNING" {
		t.Fatalf("after start: %s", s)
	}
	if _, err := disks.Delete(ctx, &computepb.DeleteDiskRequest{Project: project, Zone: zone, Disk: "web"}); code(err) != 400 {
		t.Fatalf("deleting an attached disk: %v", err)
	}
	waitFor(instances.Delete(ctx, &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "web"}))
	if _, err := instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "web"}); code(err) != 404 {
		t.Fatalf("deleted instance: %v", err)
	}
	if _, err := disks.Get(ctx, &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "web"}); code(err) != 404 {
		t.Fatalf("autoDelete left the disk: %v", err)
	}
	// the one that is left is the plain path's
	if _, body, _ := get(t, srv.URL+"/computeMetadata/v1/instance/name", true); body != "db" {
		t.Fatalf("plain path = %q", body)
	}
}

// Disks, networks, subnetworks, firewalls, addresses and the operations that report them.
func TestNetworkAndDiskResources(t *testing.T) {
	srv := start(t, "unix:///nonexistent")
	networks := must(compute.NewNetworksRESTClient(ctx, opts(srv)...))
	subnets := must(compute.NewSubnetworksRESTClient(ctx, opts(srv)...))
	firewalls := must(compute.NewFirewallsRESTClient(ctx, opts(srv)...))
	addresses := must(compute.NewAddressesRESTClient(ctx, opts(srv)...))
	disks := must(compute.NewDisksRESTClient(ctx, opts(srv)...))
	zops := must(compute.NewZoneOperationsRESTClient(ctx, opts(srv)...))

	// the project has a default network
	if n := must(networks.Get(ctx, &computepb.GetNetworkRequest{Project: project, Network: "default"})); !n.GetAutoCreateSubnetworks() {
		t.Fatalf("default network = %v", n)
	}
	waitFor(networks.Insert(ctx, &computepb.InsertNetworkRequest{Project: project, NetworkResource: &computepb.Network{Name: proto.String("vpc")}}))
	waitFor(subnets.Insert(ctx, &computepb.InsertSubnetworkRequest{Project: project, Region: "us-central1", SubnetworkResource: &computepb.Subnetwork{
		Name: proto.String("sub"), Network: proto.String("global/networks/vpc"), IpCidrRange: proto.String("10.9.0.0/24")}}))
	sub := must(subnets.Get(ctx, &computepb.GetSubnetworkRequest{Project: project, Region: "us-central1", Subnetwork: "sub"}))
	if sub.GetGatewayAddress() != "10.9.0.1" || !strings.HasSuffix(sub.GetNetwork(), "/global/networks/vpc") {
		t.Fatalf("subnetwork = %v", sub)
	}
	if _, err := subnets.Insert(ctx, &computepb.InsertSubnetworkRequest{Project: project, Region: "us-central1", SubnetworkResource: &computepb.Subnetwork{
		Name: proto.String("clash"), Network: proto.String("global/networks/vpc"), IpCidrRange: proto.String("10.9.0.128/25")}}); code(err) != 400 {
		t.Fatalf("overlapping subnetwork: %v", err)
	}
	if _, err := networks.Delete(ctx, &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"}); code(err) != 400 {
		t.Fatalf("deleting a network in use: %v", err)
	}

	// an instance on the subnetwork gets an address from its range
	instances := must(compute.NewInstancesRESTClient(ctx, opts(srv)...))
	i := instance("app", "")
	i.NetworkInterfaces = []*computepb.NetworkInterface{{Subnetwork: proto.String("regions/us-central1/subnetworks/sub")}}
	waitFor(instances.Insert(ctx, &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: i}))
	got := must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "app"}))
	if ip := got.GetNetworkInterfaces()[0].GetNetworkIP(); !strings.HasPrefix(ip, "10.9.0.") {
		t.Fatalf("instance address = %s", ip)
	}
	if _, err := subnets.Delete(ctx, &computepb.DeleteSubnetworkRequest{Project: project, Region: "us-central1", Subnetwork: "sub"}); code(err) != 400 {
		t.Fatalf("deleting a subnetwork in use: %v", err)
	}
	waitFor(instances.Delete(ctx, &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "app"}))
	waitFor(subnets.Delete(ctx, &computepb.DeleteSubnetworkRequest{Project: project, Region: "us-central1", Subnetwork: "sub"}))
	waitFor(networks.Delete(ctx, &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"}))
	if _, err := networks.Get(ctx, &computepb.GetNetworkRequest{Project: project, Network: "vpc"}); code(err) != 404 {
		t.Fatalf("deleted network: %v", err)
	}

	// firewalls: insert, patch, list
	waitFor(firewalls.Insert(ctx, &computepb.InsertFirewallRequest{Project: project, FirewallResource: &computepb.Firewall{Name: proto.String("ssh"),
		Allowed: []*computepb.Allowed{{IPProtocol: proto.String("tcp"), Ports: []string{"22"}}}, SourceRanges: []string{"0.0.0.0/0"}}}))
	waitFor(firewalls.Patch(ctx, &computepb.PatchFirewallRequest{Project: project, Firewall: "ssh", FirewallResource: &computepb.Firewall{Priority: proto.Int32(500)}}))
	fw := must(firewalls.Get(ctx, &computepb.GetFirewallRequest{Project: project, Firewall: "ssh"}))
	if fw.GetPriority() != 500 || fw.GetDirection() != "INGRESS" || len(fw.GetAllowed()) != 1 || !strings.HasSuffix(fw.GetNetwork(), "/networks/default") {
		t.Fatalf("firewall = %v", fw)
	}
	if _, err := firewalls.Insert(ctx, &computepb.InsertFirewallRequest{Project: project, FirewallResource: &computepb.Firewall{Name: proto.String("empty")}}); code(err) != 400 {
		t.Fatalf("firewall without rules: %v", err)
	}

	// addresses
	waitFor(addresses.Insert(ctx, &computepb.InsertAddressRequest{Project: project, Region: "us-central1", AddressResource: &computepb.Address{Name: proto.String("ip")}}))
	a := must(addresses.Get(ctx, &computepb.GetAddressRequest{Project: project, Region: "us-central1", Address: "ip"}))
	if a.GetStatus() != "RESERVED" || !strings.HasPrefix(a.GetAddress(), "34.") {
		t.Fatalf("address = %v", a)
	}

	// a disk, resized labels, and the operations list
	waitFor(disks.Insert(ctx, &computepb.InsertDiskRequest{Project: project, Zone: zone, DiskResource: &computepb.Disk{Name: proto.String("data"), SizeGb: proto.Int64(20),
		Type: proto.String("zones/" + zone + "/diskTypes/pd-ssd")}}))
	d := must(disks.Get(ctx, &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "data"}))
	if d.GetSizeGb() != 20 || !strings.HasSuffix(d.GetType(), "/diskTypes/pd-ssd") || d.GetStatus() != "READY" {
		t.Fatalf("disk = %v", d)
	}
	waitFor(disks.SetLabels(ctx, &computepb.SetLabelsDiskRequest{Project: project, Zone: zone, Resource: "data", ZoneSetLabelsRequestResource: &computepb.ZoneSetLabelsRequest{
		LabelFingerprint: d.LabelFingerprint, Labels: map[string]string{"a": "b"}}}))
	ops := 0
	for it := zops.List(ctx, &computepb.ListZoneOperationsRequest{Project: project, Zone: zone}); ; ops++ {
		op, err := it.Next()
		if err == iterator.Done {
			break
		}
		check(err)
		if op.GetStatus() != computepb.Operation_DONE {
			t.Fatalf("operation = %v", op)
		}
	}
	if ops < 4 {
		t.Fatalf("zone operations = %d", ops)
	}
	waitFor(disks.Delete(ctx, &computepb.DeleteDiskRequest{Project: project, Zone: zone, Disk: "data"}))
	if _, err := zops.Get(ctx, &computepb.GetZoneOperationRequest{Project: project, Zone: zone, Operation: "operation-nope"}); code(err) != 404 {
		t.Fatalf("missing operation: %v", err)
	}
}

// With an engine an instance is a container: its startup script runs, and talks to the metadata server through
// GCE_METADATA_HOST.
func TestInstanceContainer(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI")
	}
	srv := start(t, "")
	d := srv.Hub.Docker()
	if !d.Available() {
		t.Skip("no container engine")
	}
	instances := must(compute.NewInstancesRESTClient(ctx, opts(srv)...))
	script := `#!/bin/sh
echo started > /tmp/started
wget -q -O /tmp/name --header 'Metadata-Flavor: Google' "http://$GCE_METADATA_HOST/computeMetadata/v1/instance/name"
wget -q -O /tmp/token --header 'Metadata-Flavor: Google' "http://$GCE_METADATA_HOST/computeMetadata/v1/instance/service-accounts/default/token"
wget -q -O /tmp/owner --header 'Metadata-Flavor: Google' "http://$GCE_METADATA_HOST/computeMetadata/v1/instance/attributes/owner"
NAME=$(cat /tmp/name)
printf 'PUT /computeMetadata/v1/instance/guest-attributes/pogi/name HTTP/1.0\r\nMetadata-Flavor: Google\r\nContent-Length: %s\r\n\r\n%s' "${#NAME}" "$NAME" | nc -w 3 "${GCE_METADATA_HOST%:*}" "${GCE_METADATA_HOST##*:}" > /dev/null
`
	name := "vm-" + strings.ToLower(core.RandHex(3))
	req := func(f func(*computepb.Instance)) *computepb.InsertInstanceRequest {
		i := instance(name, script)
		if f != nil {
			f(i)
		}
		return &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: i}
	}
	waitFor(instances.Insert(ctx, req(nil)))
	i := must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: name}))
	ctr := docker.Name("compute", strconv.FormatUint(i.GetId(), 10))
	t.Cleanup(func() { d.Remove(ctx, ctr) })
	if i.GetStatus() != "RUNNING" {
		t.Fatalf("status = %s", i.GetStatus())
	}
	read := func(file string) string {
		t.Helper()
		for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			out, code, err := d.Exec(ctx, ctr, "cat", file)
			if err == nil && code == 0 && len(out) > 0 {
				return string(out)
			}
			if time.Now().After(end) {
				t.Fatalf("%s never appeared: %q %d %v", file, out, code, err)
			}
		}
	}
	if got := read("/tmp/started"); got != "started\n" {
		t.Fatalf("started = %q", got)
	}
	if got := read("/tmp/name"); got != name {
		t.Fatalf("the instance reports %q, want %q", got, name)
	}
	if got := read("/tmp/owner"); got != "ada" {
		t.Fatalf("owner = %q", got)
	}
	// the instance reports its own name through a guest attribute
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		ga, err := instances.GetGuestAttributes(ctx, &computepb.GetGuestAttributesInstanceRequest{Project: project, Zone: zone, Instance: name, VariableKey: proto.String("pogi/name")})
		if err == nil {
			if ga.GetVariableValue() != name {
				t.Fatalf("guest attribute = %q, want %q", ga.GetVariableValue(), name)
			}
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the instance never reported its name: %v", err)
		}
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	check(json.Unmarshal([]byte(read("/tmp/token")), &tok))
	if at, ok := gapi.ParseAccessToken(tok.AccessToken); !ok || at.Email != "sa@proj.iam.gserviceaccount.com" {
		t.Fatalf("token = %q", tok.AccessToken)
	}
	if out, _, _ := d.Exec(ctx, ctr, "sh", "-c", "echo $GCE_METADATA_HOST"); !strings.Contains(string(out), ":") {
		t.Fatalf("GCE_METADATA_HOST = %q", out)
	}
	if info := must(d.Inspect(ctx, ctr)); info.Config.Labels[docker.LabelService] != "compute" {
		t.Fatalf("labels = %v", info.Config.Labels)
	}

	// stop, start: the container stops and runs again, and the script runs again
	waitFor(instances.Stop(ctx, &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: name}))
	if info := must(d.Inspect(ctx, ctr)); info.State.Running {
		t.Fatal("the container still runs after stop")
	}
	if s := must(instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: name})).GetStatus(); s != "TERMINATED" {
		t.Fatalf("after stop: %s", s)
	}
	waitFor(instances.Start(ctx, &computepb.StartInstanceRequest{Project: project, Zone: zone, Instance: name}))
	if info := must(d.Inspect(ctx, ctr)); !info.State.Running {
		t.Fatal("the container does not run after start")
	}
	if got := read("/tmp/started"); got != "started\n" {
		t.Fatalf("after start: %q", got)
	}

	// delete removes the container and the boot disk
	waitFor(instances.Delete(ctx, &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: name}))
	if _, err := d.Inspect(ctx, ctr); err == nil {
		t.Fatal("the container survives delete")
	}
	if _, err := instances.Get(ctx, &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: name}); code(err) != 404 {
		t.Fatalf("deleted instance: %v", err)
	}
}

// The discovery-based client (google.golang.org/api/compute/v1, what the Terraform provider uses), the typed
// not-found error, and Save, Reset and Load.
func TestDiscoveryClientAndPersistence(t *testing.T) {
	srv := start(t, "unix:///nonexistent")
	svc := must(computev1.NewService(ctx, option.WithEndpoint(srv.URL+"/compute/v1/"), option.WithoutAuthentication()))
	op := must(svc.Instances.Insert(project, zone, &computev1.Instance{Name: "d1", MachineType: "zones/" + zone + "/machineTypes/e2-small",
		Disks: []*computev1.AttachedDisk{{Boot: true, AutoDelete: true, InitializeParams: &computev1.AttachedDiskInitializeParams{
			SourceImage: "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"}}},
		NetworkInterfaces: []*computev1.NetworkInterface{{Network: "global/networks/default"}}}).Do())
	if op = must(svc.ZoneOperations.Wait(project, zone, op.Name).Do()); op.Status != "DONE" || op.Error != nil || op.OperationType != "insert" {
		t.Fatalf("operation = %+v", op)
	}
	i := must(svc.Instances.Get(project, zone, "d1").Do())
	if i.Status != "RUNNING" || i.Kind != "compute#instance" || !strings.HasSuffix(i.MachineType, "/machineTypes/e2-small") {
		t.Fatalf("instance = %+v", i)
	}
	d := must(svc.Disks.Get(project, zone, "d1").Do())
	if !strings.Contains(d.SourceImage, "/projects/ubuntu-os-cloud/global/images/ubuntu-2204-") {
		t.Fatalf("boot disk image = %s", d.SourceImage)
	}
	list := must(svc.Instances.List(project, zone).Filter("status = RUNNING").Do())
	if len(list.Items) != 1 || list.Kind != "compute#instanceList" {
		t.Fatalf("list = %+v", list)
	}

	_, err := svc.Instances.Get(project, zone, "nope").Do()
	var ge *googleapi.Error
	if !errors.As(err, &ge) || ge.Code != 404 || len(ge.Errors) != 1 || ge.Errors[0].Reason != "notFound" || !strings.Contains(ge.Message, "projects/proj/zones/"+zone+"/instances/nope") {
		t.Fatalf("not found: %#v", err)
	}
	if _, err = svc.Instances.Get(project, "mars-central1-a", "d1").Do(); !errors.As(err, &ge) || ge.Code != 404 {
		t.Fatalf("unknown zone: %v", err)
	}

	st := srv.Hub.Service("compute").(core.Stateful)
	b := must(st.Save())
	st.Reset()
	if _, err := svc.Instances.Get(project, zone, "d1").Do(); err == nil {
		t.Fatal("reset left the instance")
	}
	check(st.Load(b))
	if got := must(svc.Instances.Get(project, zone, "d1").Do()); got.Id != i.Id || got.Status != "RUNNING" {
		t.Fatalf("after load: %+v", got)
	}
	// the console's view of it
	tables := srv.Hub.Resources("compute")
	if rows := tables["compute"]; len(rows) == 0 || rows[0].Name != "Instances" || len(rows[0].Rows) != 1 || !strings.HasSuffix(rows[0].Rows[0][0], "/instances/d1") {
		t.Fatalf("resources = %+v", tables)
	}
}
