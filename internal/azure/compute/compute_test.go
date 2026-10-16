package compute_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azarm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
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

const (
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
	loc = "westeurope"
)

// start runs pogi with the Resource Manager, the sign-in services, the network service and compute, and returns
// client options that trust pogi's CA and sign in with client credentials.
func start(t *testing.T, dockerHost string) (azcore.TokenCredential, *azarm.ClientOptions, *pogi.Running) {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"arm", "entra", "graph", "aznetwork", "azcompute"}, DockerHost: dockerHost,
		DockerImages: map[string]string{"azcompute.ubuntu": "alpine:latest", "azcompute.debian": "alpine:latest"}}) // alpine: wget and a small pull
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

// nic makes a virtual network, a subnet and a network interface with a fixed address.
func nic(cred azcore.TokenCredential, opts *azarm.ClientOptions, name, addr string) armnetwork.Interface {
	vnets, subnets, nics := must(armnetwork.NewVirtualNetworksClient(sub, cred, opts)), must(armnetwork.NewSubnetsClient(sub, cred, opts)), must(armnetwork.NewInterfacesClient(sub, cred, opts))
	if _, err := vnets.Get(ctx, rg, "vn", nil); err != nil {
		must(must(vnets.BeginCreateOrUpdate(ctx, rg, "vn", armnetwork.VirtualNetwork{Location: to.Ptr(loc), Properties: &armnetwork.VirtualNetworkPropertiesFormat{
			AddressSpace: &armnetwork.AddressSpace{AddressPrefixes: []*string{to.Ptr("10.1.0.0/16")}}}}, nil)).PollUntilDone(ctx, fast))
		must(must(subnets.BeginCreateOrUpdate(ctx, rg, "vn", "s", armnetwork.Subnet{Properties: &armnetwork.SubnetPropertiesFormat{AddressPrefix: to.Ptr("10.1.1.0/24")}}, nil)).PollUntilDone(ctx, fast))
	}
	sn := must(subnets.Get(ctx, rg, "vn", "s", nil))
	return must(must(nics.BeginCreateOrUpdate(ctx, rg, name, armnetwork.Interface{Location: to.Ptr(loc), Properties: &armnetwork.InterfacePropertiesFormat{
		IPConfigurations: []*armnetwork.InterfaceIPConfiguration{{Name: to.Ptr("ipconfig1"), Properties: &armnetwork.InterfaceIPConfigurationPropertiesFormat{
			Subnet: &sn.Subnet, PrivateIPAddress: to.Ptr(addr), PrivateIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic)}}}}}, nil)).PollUntilDone(ctx, fast)).Interface
}

// vm is a VM of an Ubuntu image on the given interface.
func vm(nicID *string, customData string) armcompute.VirtualMachine {
	v := armcompute.VirtualMachine{Location: to.Ptr(loc), Tags: map[string]*string{"env": to.Ptr("test")}, Properties: &armcompute.VirtualMachineProperties{
		HardwareProfile: &armcompute.HardwareProfile{VMSize: to.Ptr(armcompute.VirtualMachineSizeTypesStandardB1S)},
		StorageProfile: &armcompute.StorageProfile{
			ImageReference: &armcompute.ImageReference{Publisher: to.Ptr("Canonical"), Offer: to.Ptr("0001-com-ubuntu-server-jammy"), SKU: to.Ptr("22_04-lts-gen2"), Version: to.Ptr("latest")},
			OSDisk:         &armcompute.OSDisk{CreateOption: to.Ptr(armcompute.DiskCreateOptionTypesFromImage), ManagedDisk: &armcompute.ManagedDiskParameters{StorageAccountType: to.Ptr(armcompute.StorageAccountTypesStandardLRS)}},
		},
		OSProfile:      &armcompute.OSProfile{ComputerName: to.Ptr("host1"), AdminUsername: to.Ptr("azureuser"), AdminPassword: to.Ptr("P@ssw0rd-1234")},
		NetworkProfile: &armcompute.NetworkProfile{NetworkInterfaces: []*armcompute.NetworkInterfaceReference{{ID: nicID, Properties: &armcompute.NetworkInterfaceReferenceProperties{Primary: to.Ptr(true)}}}}}}
	if customData != "" {
		v.Properties.OSProfile.CustomData = to.Ptr(base64.StdEncoding.EncodeToString([]byte(customData)))
	}
	return v
}

func imds(t *testing.T, url, header string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if header != "" {
		req.Header.Set("Metadata", header)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// Without a container engine: the VM is running at once, and the rest of the plane works.
func TestVirtualMachinesWithoutDocker(t *testing.T) {
	t.Parallel()
	cred, opts, srv := start(t, "unix:///nonexistent")
	vms, disks, sets, keys := must(armcompute.NewVirtualMachinesClient(sub, cred, opts)), must(armcompute.NewDisksClient(sub, cred, opts)),
		must(armcompute.NewAvailabilitySetsClient(sub, cred, opts)), must(armcompute.NewSSHPublicKeysClient(sub, cred, opts))
	n := nic(cred, opts, "nic1", "10.1.1.10")

	// the checks of a PUT
	bad := vm(n.ID, "")
	bad.Properties.OSProfile.AdminUsername = nil
	_, err := vms.BeginCreateOrUpdate(ctx, rg, "bad", bad, nil)
	wantErr(t, err, 400, "InvalidParameter")
	bad = vm(to.Ptr(*n.ID+"-nope"), "")
	_, err = vms.BeginCreateOrUpdate(ctx, rg, "bad", bad, nil)
	wantErr(t, err, 400, "InvalidResourceReference")
	bad = vm(n.ID, "")
	bad.Location = nil
	_, err = vms.BeginCreateOrUpdate(ctx, rg, "bad", bad, nil)
	wantErr(t, err, 400, "LocationRequired")
	bad = vm(n.ID, "")
	bad.Properties.AvailabilitySet = &armcompute.SubResource{ID: to.Ptr("/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Compute/availabilitySets/missing")}
	_, err = vms.BeginCreateOrUpdate(ctx, rg, "bad", bad, nil)
	wantErr(t, err, 400, "InvalidResourceReference")

	// an availability set, a managed data disk and an SSH key to use
	as := must(sets.CreateOrUpdate(ctx, rg, "as1", armcompute.AvailabilitySet{Location: to.Ptr(loc), SKU: &armcompute.SKU{Name: to.Ptr("Aligned")},
		Properties: &armcompute.AvailabilitySetProperties{PlatformFaultDomainCount: to.Ptr(int32(2))}}, nil))
	if *as.Properties.PlatformFaultDomainCount != 2 || *as.Properties.PlatformUpdateDomainCount != 5 || *as.SKU.Name != "Aligned" {
		t.Fatalf("availability set = %+v", as.Properties)
	}
	data := must(must(disks.BeginCreateOrUpdate(ctx, rg, "data1", armcompute.Disk{Location: to.Ptr(loc), SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesStandardLRS)},
		Properties: &armcompute.DiskProperties{DiskSizeGB: to.Ptr(int32(16)), CreationData: &armcompute.CreationData{CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty)}}}, nil)).PollUntilDone(ctx, fast)).Disk
	if *data.Properties.DiskState != armcompute.DiskStateUnattached || *data.Properties.DiskSizeGB != 16 || *data.SKU.Tier != "Standard" {
		t.Fatalf("disk = %+v", data.Properties)
	}
	_, err = disks.BeginCreateOrUpdate(ctx, rg, "nocreate", armcompute.Disk{Location: to.Ptr(loc), Properties: &armcompute.DiskProperties{CreationData: &armcompute.CreationData{}}}, nil)
	wantErr(t, err, 400, "InvalidParameter")
	must(keys.Create(ctx, rg, "key1", armcompute.SSHPublicKeyResource{Location: to.Ptr(loc)}, nil))
	pair := must(keys.GenerateKeyPair(ctx, rg, "key1", nil))
	if k1 := must(keys.Get(ctx, rg, "key1", nil)); k1.Properties.PublicKey == nil || *k1.Properties.PublicKey != *pair.PublicKey {
		t.Fatalf("generateKeyPair did not keep the public key: %+v", k1.Properties)
	}
	key := must(keys.Create(ctx, rg, "key2", armcompute.SSHPublicKeyResource{Location: to.Ptr(loc), Properties: &armcompute.SSHPublicKeyResourceProperties{PublicKey: pair.PublicKey}}, nil))
	if *key.Properties.PublicKey != *pair.PublicKey || !strings.HasPrefix(*pair.PublicKey, "ssh-rsa ") || !strings.Contains(*pair.PrivateKey, "PRIVATE KEY") {
		t.Fatalf("keys: %+v %+v", pair, key)
	}
	_, err = keys.Create(ctx, rg, "key3", armcompute.SSHPublicKeyResource{Location: to.Ptr(loc), Properties: &armcompute.SSHPublicKeyResourceProperties{PublicKey: to.Ptr("not a key")}}, nil)
	wantErr(t, err, 400, "InvalidParameter")

	// create
	v := vm(n.ID, "")
	v.Properties.AvailabilitySet = &armcompute.SubResource{ID: as.ID}
	v.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{{Lun: to.Ptr(int32(0)), CreateOption: to.Ptr(armcompute.DiskCreateOptionTypesAttach), ManagedDisk: &armcompute.ManagedDiskParameters{ID: data.ID}}}
	v.Properties.OSProfile.LinuxConfiguration = &armcompute.LinuxConfiguration{DisablePasswordAuthentication: to.Ptr(false),
		SSH: &armcompute.SSHConfiguration{PublicKeys: []*armcompute.SSHPublicKey{{Path: to.Ptr("/home/azureuser/.ssh/authorized_keys"), KeyData: pair.PublicKey}}}}
	v.Identity = &armcompute.VirtualMachineIdentity{Type: to.Ptr(armcompute.ResourceIdentityTypeSystemAssigned)}
	created := must(must(vms.BeginCreateOrUpdate(ctx, rg, "vm1", v, nil)).PollUntilDone(ctx, fast)).VirtualMachine
	p := created.Properties
	if *created.Name != "vm1" || *p.ProvisioningState != "Succeeded" || p.VMID == nil || *p.OSProfile.ComputerName != "host1" || p.OSProfile.AdminPassword != nil ||
		*p.OSProfile.AdminUsername != "azureuser" || *p.OSProfile.LinuxConfiguration.SSH.PublicKeys[0].KeyData != *pair.PublicKey ||
		*p.StorageProfile.OSDisk.OSType != armcompute.OperatingSystemTypesLinux || created.Identity == nil || *created.Identity.PrincipalID == "" ||
		*created.Tags["env"] != "test" || len(p.StorageProfile.DataDisks) != 1 || *p.StorageProfile.DataDisks[0].DiskSizeGB != 16 {
		t.Fatalf("vm = %+v", p)
	}
	// the OS disk exists, attached, and the data disk is attached too
	osName := *p.StorageProfile.OSDisk.Name
	if !strings.HasPrefix(osName, "vm1_OsDisk_1_") {
		t.Fatalf("os disk name %s", osName)
	}
	osd := must(disks.Get(ctx, rg, osName, nil))
	if *osd.ManagedBy != *created.ID || *osd.Properties.DiskState != armcompute.DiskStateAttached || *osd.Properties.DiskSizeGB != 30 || *osd.Properties.OSType != armcompute.OperatingSystemTypesLinux {
		t.Fatalf("os disk = %+v", osd)
	}
	if d := must(disks.Get(ctx, rg, "data1", nil)); *d.ManagedBy != *created.ID {
		t.Fatalf("data disk = %+v", d)
	}
	_, err = disks.BeginDelete(ctx, rg, "data1", nil)
	wantErr(t, err, 409, "OperationNotAllowed")
	_, err = sets.Delete(ctx, rg, "as1", nil)
	wantErr(t, err, 409, "OperationNotAllowed")
	if a := must(sets.Get(ctx, rg, "as1", nil)); len(a.Properties.VirtualMachines) != 1 || *a.Properties.VirtualMachines[0].ID != *created.ID {
		t.Fatalf("set vms = %+v", a.Properties.VirtualMachines)
	}

	// instance view: running; powerOff, deallocate, start, restart
	state := func() string {
		got := must(vms.Get(ctx, rg, "vm1", &armcompute.VirtualMachinesClientGetOptions{Expand: to.Ptr(armcompute.InstanceViewTypesInstanceView)}))
		for _, s := range got.Properties.InstanceView.Statuses {
			if strings.HasPrefix(*s.Code, "PowerState/") {
				return strings.TrimPrefix(*s.Code, "PowerState/")
			}
		}
		return ""
	}
	if s := state(); s != "running" {
		t.Fatalf("power state %q", s)
	}
	must(vms.BeginPowerOff(ctx, rg, "vm1", nil)).PollUntilDone(ctx, fast)
	if s := state(); s != "stopped" {
		t.Fatalf("after powerOff: %q", s)
	}
	must(vms.BeginDeallocate(ctx, rg, "vm1", nil)).PollUntilDone(ctx, fast)
	if s := state(); s != "deallocated" {
		t.Fatalf("after deallocate: %q", s)
	}
	must(vms.BeginStart(ctx, rg, "vm1", nil)).PollUntilDone(ctx, fast)
	must(vms.BeginRestart(ctx, rg, "vm1", nil)).PollUntilDone(ctx, fast)
	if s := state(); s != "running" {
		t.Fatalf("after start: %q", s)
	}

	// a Windows image has no container and runs as metadata
	w := vm(n.ID, "")
	w.Properties.StorageProfile.ImageReference = &armcompute.ImageReference{Publisher: to.Ptr("MicrosoftWindowsServer"), Offer: to.Ptr("WindowsServer"), SKU: to.Ptr("2022-datacenter"), Version: to.Ptr("latest")}
	w.Properties.OSProfile.WindowsConfiguration = &armcompute.WindowsConfiguration{}
	wv := must(must(vms.BeginCreateOrUpdate(ctx, rg, "win1", w, nil)).PollUntilDone(ctx, fast)).VirtualMachine
	if *wv.Properties.StorageProfile.OSDisk.OSType != armcompute.OperatingSystemTypesWindows {
		t.Fatalf("windows vm = %+v", wv.Properties.StorageProfile.OSDisk)
	}
	// an update (tags, size) keeps the VM and its disks
	v.Tags["env"] = to.Ptr("prod")
	v.Properties.HardwareProfile.VMSize = to.Ptr(armcompute.VirtualMachineSizeTypesStandardB2S)
	upd := must(must(vms.BeginCreateOrUpdate(ctx, rg, "vm1", v, nil)).PollUntilDone(ctx, fast)).VirtualMachine
	if *upd.Properties.VMID != *p.VMID || *upd.Properties.StorageProfile.OSDisk.Name != osName || *upd.Tags["env"] != "prod" || *upd.Properties.HardwareProfile.VMSize != armcompute.VirtualMachineSizeTypesStandardB2S {
		t.Fatalf("update = %+v", upd.Properties)
	}
	if got := must(vms.Get(ctx, rg, "vm1", nil)); len(got.Properties.StorageProfile.DataDisks) != 1 {
		t.Fatalf("update lost the data disk: %+v", got.Properties.StorageProfile)
	}
	var names []string
	for pg := vms.NewListPager(rg, nil); pg.More(); {
		for _, x := range must(pg.NextPage(ctx)).Value {
			names = append(names, *x.Name)
		}
	}
	if len(names) != 2 || names[0] != "vm1" || names[1] != "win1" {
		t.Fatalf("list = %v", names)
	}
	for pg := vms.NewListAllPager(nil); pg.More(); {
		if n := len(must(pg.NextPage(ctx)).Value); n != 2 {
			t.Fatalf("list all = %d", n)
		}
	}

	// IMDS: the headers, the version, the document, the NIC's address, text format and the token
	base := srv.URL + "/_azure/imds/" + *p.VMID + "/metadata/"
	if c, _ := imds(t, base+"instance?api-version=2021-02-01", ""); c != 400 {
		t.Fatalf("without the Metadata header: %d", c)
	}
	if c, body := imds(t, base+"instance", "true"); c != 400 || !strings.Contains(body, "api-version") {
		t.Fatalf("without api-version: %d %s", c, body)
	}
	c, body := imds(t, base+"instance?api-version=2021-02-01", "true")
	var doc struct {
		Compute struct {
			Name, VMSize, ResourceGroupName, SubscriptionID, VMID, Location, Publisher, Offer, SKU, OSType, Tags string
			OSProfile                                                                                            struct{ AdminUsername, ComputerName string }
			PublicKeys                                                                                           []struct{ KeyData string }
		}
		Network struct {
			Interface []struct {
				IPv4 struct {
					IPAddress []struct{ PrivateIPAddress string }
					Subnet    []struct{ Address, Prefix string }
				}
			}
		}
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil || c != 200 {
		t.Fatalf("imds = %d %s", c, body)
	}
	cp := doc.Compute
	if cp.Name != "vm1" || cp.VMSize != "Standard_B2s" || cp.ResourceGroupName != rg || cp.SubscriptionID != sub || cp.VMID != *p.VMID || cp.Location != loc || cp.Publisher != "Canonical" ||
		cp.OSType != "Linux" || cp.Tags != "env:prod" || cp.OSProfile.AdminUsername != "azureuser" || len(cp.PublicKeys) != 1 {
		t.Fatalf("compute = %+v", cp)
	}
	if len(doc.Network.Interface) != 1 || doc.Network.Interface[0].IPv4.IPAddress[0].PrivateIPAddress != "10.1.1.10" || doc.Network.Interface[0].IPv4.Subnet[0].Address != "10.1.1.0" || doc.Network.Interface[0].IPv4.Subnet[0].Prefix != "24" {
		t.Fatalf("network = %+v", doc.Network)
	}
	if c, body := imds(t, base+"instance/compute/vmSize?api-version=2021-02-01&format=text", "true"); c != 200 || body != "Standard_B2s" {
		t.Fatalf("text = %d %q", c, body)
	}
	if c, _ := imds(t, base+"instance/compute/nope?api-version=2021-02-01", "true"); c != 404 {
		t.Fatalf("missing leaf: %d", c)
	}
	if c, _ := imds(t, srv.URL+"/metadata/instance?api-version=2021-02-01", "true"); c != 404 { // two VMs: the plain path cannot say whose
		t.Fatalf("ambiguous plain path: %d", c)
	}
	// the managed identity endpoint is Entra's, reached through the VM's prefix
	c, body = imds(t, base+"identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/", "true")
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal([]byte(body), &tok); c != 200 || strings.Count(tok.AccessToken, ".") != 2 {
		t.Fatalf("identity token = %d %s", c, body)
	}
	if c, _ := imds(t, base+"identity/oauth2/token?api-version=2018-02-01&resource=x", ""); c != 400 {
		t.Fatalf("token without the header: %d", c)
	}

	// delete: the OS disk stays, detached; a disk marked Delete goes with the VM
	must(vms.BeginDelete(ctx, rg, "win1", nil)).PollUntilDone(ctx, fast)
	if c, body := imds(t, srv.URL+"/metadata/instance/compute/name?api-version=2021-02-01&format=text", "true"); c != 200 || body != "vm1" { // one VM left: the plain path is its
		t.Fatalf("plain path = %d %q", c, body)
	}
	must(vms.BeginDelete(ctx, rg, "vm1", nil)).PollUntilDone(ctx, fast)
	_, err = vms.Get(ctx, rg, "vm1", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	if d := must(disks.Get(ctx, rg, osName, nil)); d.ManagedBy != nil || *d.Properties.DiskState != armcompute.DiskStateUnattached {
		t.Fatalf("os disk after delete = %+v", d)
	}
	must(sets.Delete(ctx, rg, "as1", nil))
	must(must(disks.BeginDelete(ctx, rg, osName, nil)).PollUntilDone(ctx, fast))
	_, err = disks.Get(ctx, rg, osName, nil)
	wantErr(t, err, 404, "ResourceNotFound")
	// a disk can grow and cannot shrink
	grown := must(must(disks.BeginCreateOrUpdate(ctx, rg, "data1", armcompute.Disk{Location: to.Ptr(loc), SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesStandardLRS)},
		Properties: &armcompute.DiskProperties{DiskSizeGB: to.Ptr(int32(32)), CreationData: &armcompute.CreationData{CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty)}}}, nil)).PollUntilDone(ctx, fast))
	if *grown.Properties.DiskSizeGB != 32 || *grown.Properties.DiskState != armcompute.DiskStateUnattached {
		t.Fatalf("grown = %+v", grown.Properties)
	}
	_, err = disks.BeginCreateOrUpdate(ctx, rg, "data1", armcompute.Disk{Location: to.Ptr(loc), Properties: &armcompute.DiskProperties{DiskSizeGB: to.Ptr(int32(8)),
		CreationData: &armcompute.CreationData{CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty)}}}, nil)
	wantErr(t, err, 400, "OperationNotAllowed")
}

// With an engine the VM is a container: customData runs at boot and reads the VM's own IMDS.
func TestVirtualMachineContainer(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI")
	}
	cred, opts, srv := start(t, "")
	d := srv.Hub.Docker()
	if !d.Available() {
		t.Skip("no container engine")
	}
	vms, disks := must(armcompute.NewVirtualMachinesClient(sub, cred, opts)), must(armcompute.NewDisksClient(sub, cred, opts))
	n := nic(cred, opts, "nic1", "10.1.1.20")
	script := `#!/bin/sh
echo booted > /tmp/booted
wget -q -O /tmp/name --header 'Metadata: true' "$AZURE_POD_IDENTITY_AUTHORITY_HOST/metadata/instance/compute/name?api-version=2021-02-01&format=text"
wget -q -O /tmp/ip --header 'Metadata: true' "$AZURE_POD_IDENTITY_AUTHORITY_HOST/metadata/instance/network/interface/0/ipv4/ipAddress/0/privateIpAddress?api-version=2021-02-01&format=text"
wget -q -O /tmp/token --header 'Metadata: true' "$AZURE_POD_IDENTITY_AUTHORITY_HOST/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/"
wget -q -O /tmp/msi --header "X-IDENTITY-HEADER: $IDENTITY_HEADER" "$IDENTITY_ENDPOINT?api-version=2019-08-01&resource=https://management.azure.com/"
`
	name := "vm-" + strings.ToLower(core.RandHex(3))
	created := must(must(vms.BeginCreateOrUpdate(ctx, rg, name, vm(n.ID, script), nil)).PollUntilDone(ctx, fast)).VirtualMachine
	ctr := docker.Name("azcompute", *created.Properties.VMID)
	t.Cleanup(func() { d.Remove(ctx, ctr) })
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
	state := func() string {
		got := must(vms.Get(ctx, rg, name, &armcompute.VirtualMachinesClientGetOptions{Expand: to.Ptr(armcompute.InstanceViewTypesInstanceView)}))
		return *got.Properties.InstanceView.Statuses[len(got.Properties.InstanceView.Statuses)-1].Code
	}
	if s := state(); s != "PowerState/running" {
		t.Fatalf("state = %s", s)
	}
	if got := read("/tmp/booted"); got != "booted\n" {
		t.Fatalf("booted = %q", got)
	}
	if got := read("/tmp/name"); got != name {
		t.Fatalf("the VM reports %q, want %q", got, name)
	}
	if got := read("/tmp/ip"); got != "10.1.1.20" {
		t.Fatalf("private ip = %q", got)
	}
	for _, f := range []string{"/tmp/token", "/tmp/msi"} {
		var tok struct {
			AccessToken string `json:"access_token"`
		}
		if json.Unmarshal([]byte(read(f)), &tok); strings.Count(tok.AccessToken, ".") != 2 {
			t.Fatalf("%s = %q", f, read(f))
		}
	}
	if info := must(d.Inspect(ctx, ctr)); info.Config.Labels[docker.LabelService] != "azcompute" || !info.State.Running {
		t.Fatalf("container = %+v", info)
	}

	// power off stops the container, start runs it again (customData does not run twice), deallocate stops it
	must(vms.BeginPowerOff(ctx, rg, name, nil)).PollUntilDone(ctx, fast)
	if info := must(d.Inspect(ctx, ctr)); info.State.Running {
		t.Fatal("the container still runs after powerOff")
	}
	if s := state(); s != "PowerState/stopped" {
		t.Fatalf("after powerOff: %s", s)
	}
	must(vms.BeginStart(ctx, rg, name, nil)).PollUntilDone(ctx, fast)
	if info := must(d.Inspect(ctx, ctr)); !info.State.Running {
		t.Fatal("the container does not run after start")
	}
	if out, _, _ := d.Exec(ctx, ctr, "sh", "-c", "cat /tmp/booted && ls /var/lib/cloud/instance"); !strings.Contains(string(out), "boot-finished") {
		t.Fatalf("files after start: %q", out)
	}
	must(vms.BeginRestart(ctx, rg, name, nil)).PollUntilDone(ctx, fast)
	must(vms.BeginDeallocate(ctx, rg, name, nil)).PollUntilDone(ctx, fast)
	if info := must(d.Inspect(ctx, ctr)); info.State.Running {
		t.Fatal("the container still runs after deallocate")
	}
	if s := state(); s != "PowerState/deallocated" {
		t.Fatalf("after deallocate: %s", s)
	}

	// delete removes the container; the OS disk stays until it is deleted
	must(vms.BeginDelete(ctx, rg, name, nil)).PollUntilDone(ctx, fast)
	if _, err := d.Inspect(ctx, ctr); err == nil {
		t.Fatal("the container survives delete")
	}
	_, err := vms.Get(ctx, rg, name, nil)
	wantErr(t, err, 404, "ResourceNotFound")
	must(must(disks.BeginDelete(ctx, rg, *created.Properties.StorageProfile.OSDisk.Name, nil)).PollUntilDone(ctx, fast))
}

// cloud-config customData runs its runcmd list; the state survives Save and Load.
func TestCloudConfigAndPersistence(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI")
	}
	cred, opts, srv := start(t, "")
	d := srv.Hub.Docker()
	if !d.Available() {
		t.Skip("no container engine")
	}
	vms := must(armcompute.NewVirtualMachinesClient(sub, cred, opts))
	n := nic(cred, opts, "nic1", "10.1.1.30")
	cc := "#cloud-config\nruncmd:\n  - echo cloud > /tmp/cloud\n  - [ sh, -c, \"echo list > /tmp/list\" ]\n"
	created := must(must(vms.BeginCreateOrUpdate(ctx, rg, "cc", vm(n.ID, cc), nil)).PollUntilDone(ctx, fast)).VirtualMachine
	ctr := docker.Name("azcompute", *created.Properties.VMID)
	t.Cleanup(func() { d.Remove(ctx, ctr) })
	for _, f := range []struct{ file, want string }{{"/tmp/cloud", "cloud\n"}, {"/tmp/list", "list\n"}} {
		for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			out, code, _ := d.Exec(ctx, ctr, "cat", f.file)
			if code == 0 && string(out) == f.want {
				break
			}
			if time.Now().After(end) {
				t.Fatalf("%s = %q", f.file, out)
			}
		}
	}
	svc := srv.Hub.Service("azcompute").(core.Stateful)
	b := must(svc.Save())
	svc.Reset()
	if _, err := vms.Get(ctx, rg, "cc", nil); err == nil {
		t.Fatal("reset left the VM")
	}
	if _, _, err := d.Exec(ctx, ctr, "true"); err == nil {
		t.Fatal("reset left the container")
	}
	must(0, svc.Load(b))
	got := must(vms.Get(ctx, rg, "cc", nil))
	if *got.Properties.VMID != *created.Properties.VMID {
		t.Fatal("the VM did not come back")
	}
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if out, code, _ := d.Exec(ctx, ctr, "cat", "/tmp/cloud"); code == 0 && string(out) == "cloud\n" {
			break
		} else if time.Now().After(end) {
			t.Fatalf("the VM did not boot again: %q", out)
		}
	}
}
