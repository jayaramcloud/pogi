package aks_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azarm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v6"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/aks"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// fast makes the SDK poll long-running operations at once, not every second.
var fast = &runtime.PollUntilDoneOptions{Frequency: 100 * time.Millisecond}

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

// start runs pogi with the Resource Manager, the sign-in services and AKS, and
// returns the clients of the SDK, which trust pogi's CA and sign in with client credentials.
func start(t *testing.T, cfg pogi.Config) (*pogi.Running, *armcontainerservice.ManagedClustersClient, *armcontainerservice.AgentPoolsClient) {
	t.Helper()
	cfg.Services = []string{"arm", "entra", "graph", "azaks"}
	srv := pogi.Start(t, cfg)
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	cred := must(azidentity.NewClientSecretCredential(arm.TenantID, "app", "secret",
		&azidentity.ClientSecretCredentialOptions{ClientOptions: co, DisableInstanceDiscovery: true}))
	opts := &azarm.ClientOptions{ClientOptions: co}
	return srv, must(armcontainerservice.NewManagedClustersClient(sub, cred, opts)), must(armcontainerservice.NewAgentPoolsClient(sub, cred, opts))
}

func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || re.ErrorCode != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
}

func cluster(pools ...string) armcontainerservice.ManagedCluster {
	mc := armcontainerservice.ManagedCluster{Location: to.Ptr(loc), Tags: map[string]*string{"env": to.Ptr("test")},
		Identity:   &armcontainerservice.ManagedClusterIdentity{Type: to.Ptr(armcontainerservice.ResourceIdentityTypeSystemAssigned)},
		Properties: &armcontainerservice.ManagedClusterProperties{DNSPrefix: to.Ptr("pogi")}}
	for i, n := range pools {
		mc.Properties.AgentPoolProfiles = append(mc.Properties.AgentPoolProfiles, &armcontainerservice.ManagedClusterAgentPoolProfile{
			Name: to.Ptr(n), Count: to.Ptr(int32(i + 2)), VMSize: to.Ptr("Standard_DS2_v2"), Mode: to.Ptr(armcontainerservice.AgentPoolModeSystem)})
	}
	return mc
}

// noEngine runs pogi as if no container engine were installed.
var noEngine = pogi.Config{DockerHost: "/nonexistent/docker.sock"}

// TestMetadata covers the management plane with no cluster behind it: the SDK
// creates, reads, lists, tags and deletes clusters and agent pools, asks for the
// kubeconfigs, and the API server's webhook accepts the kubeconfig's token only.
func TestMetadata(t *testing.T) {
	srv, clusters, pools := start(t, noEngine)

	mc := must(must(clusters.BeginCreateOrUpdate(ctx, rg, "web", cluster("system", "work"), nil)).PollUntilDone(ctx, fast))
	p := mc.Properties
	if *mc.Name != "web" || *mc.Type != "Microsoft.ContainerService/managedClusters" || *p.ProvisioningState != "Succeeded" || *p.PowerState.Code != armcontainerservice.CodeRunning ||
		*p.Fqdn == "" || *p.DNSPrefix != "pogi" || *p.KubernetesVersion == "" || *p.NodeResourceGroup != "MC_pogi_web_westeurope" ||
		len(p.AgentPoolProfiles) != 2 || *p.AgentPoolProfiles[0].Name != "system" || *p.AgentPoolProfiles[1].Count != 3 || *mc.Tags["env"] != "test" ||
		*mc.Identity.Type != armcontainerservice.ResourceIdentityTypeSystemAssigned {
		t.Fatalf("cluster: %+v %+v", mc, p)
	}
	got := must(clusters.Get(ctx, rg, "web", nil))
	if *got.ID != *mc.ID {
		t.Fatalf("get: %v", *got.ID)
	}
	_, err := clusters.Get(ctx, rg, "nope", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = clusters.BeginCreateOrUpdate(ctx, rg, "bad name", cluster("a"), nil)
	wantErr(t, err, 400, "InvalidParameter")
	_, err = clusters.BeginCreateOrUpdate(ctx, rg, "nopool", armcontainerservice.ManagedCluster{Properties: &armcontainerservice.ManagedClusterProperties{}}, nil)
	wantErr(t, err, 400, "LocationRequired")
	must(must(clusters.BeginCreateOrUpdate(ctx, rg, "api", cluster("system"), nil)).PollUntilDone(ctx, fast))

	// list, tags
	var names []string
	for pg := clusters.NewListByResourceGroupPager(rg, nil); pg.More(); {
		for _, c := range must(pg.NextPage(ctx)).Value {
			names = append(names, *c.Name)
		}
	}
	if len(names) != 2 || names[0] != "api" || names[1] != "web" {
		t.Fatalf("list: %v", names)
	}
	n := 0
	for pg := clusters.NewListPager(nil); pg.More(); {
		n += len(must(pg.NextPage(ctx)).Value)
	}
	if n != 2 {
		t.Fatalf("list by subscription: %d", n)
	}
	tagged := must(must(clusters.BeginUpdateTags(ctx, rg, "web", armcontainerservice.TagsObject{Tags: map[string]*string{"env": to.Ptr("prod")}}, nil)).PollUntilDone(ctx, fast))
	if *tagged.Tags["env"] != "prod" || len(tagged.Properties.AgentPoolProfiles) != 2 {
		t.Fatalf("tagged: %+v", tagged)
	}

	// agent pools
	list := func() (out []string) {
		for pg := pools.NewListPager(rg, "web", nil); pg.More(); {
			for _, ap := range must(pg.NextPage(ctx)).Value {
				out = append(out, *ap.Name)
			}
		}
		return out
	}
	if l := list(); len(l) != 2 || l[0] != "system" || l[1] != "work" {
		t.Fatalf("pools: %v", l)
	}
	ap := must(must(pools.BeginCreateOrUpdate(ctx, rg, "web", "gpu", armcontainerservice.AgentPool{Properties: &armcontainerservice.ManagedClusterAgentPoolProfileProperties{
		Count: to.Ptr(int32(4)), VMSize: to.Ptr("Standard_NC6")}}, nil)).PollUntilDone(ctx, fast))
	if *ap.Name != "gpu" || *ap.Properties.Count != 4 || *ap.Properties.ProvisioningState != "Succeeded" || *ap.Properties.Mode != armcontainerservice.AgentPoolModeUser ||
		*ap.Properties.OSType != armcontainerservice.OSTypeLinux || *ap.Type != "Microsoft.ContainerService/managedClusters/agentPools" {
		t.Fatalf("pool: %+v", ap.Properties)
	}
	ap = must(must(pools.BeginCreateOrUpdate(ctx, rg, "web", "gpu", armcontainerservice.AgentPool{Properties: &armcontainerservice.ManagedClusterAgentPoolProfileProperties{
		Count: to.Ptr(int32(6))}}, nil)).PollUntilDone(ctx, fast))
	if *ap.Properties.Count != 6 || *ap.Properties.VMSize != "Standard_NC6" {
		t.Fatalf("updated pool: %+v", ap.Properties)
	}
	if l := list(); len(l) != 3 {
		t.Fatalf("pools: %v", l)
	}
	if mc := must(clusters.Get(ctx, rg, "web", nil)); len(mc.Properties.AgentPoolProfiles) != 3 {
		t.Fatalf("pools of the cluster: %v", mc.Properties.AgentPoolProfiles)
	}
	_, err = pools.BeginCreateOrUpdate(ctx, rg, "nope", "gpu", armcontainerservice.AgentPool{}, nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = pools.BeginCreateOrUpdate(ctx, rg, "web", "Bad_Pool", armcontainerservice.AgentPool{}, nil)
	wantErr(t, err, 400, "InvalidParameter")
	must(must(pools.BeginDelete(ctx, rg, "web", "gpu", nil)).PollUntilDone(ctx, fast))
	_, err = pools.Get(ctx, rg, "web", "gpu", nil)
	wantErr(t, err, 404, "ResourceNotFound")

	// credentials: the call `az aks get-credentials` makes
	user := must(clusters.ListClusterUserCredentials(ctx, rg, "web", nil))
	admin := must(clusters.ListClusterAdminCredentials(ctx, rg, "web", nil))
	if len(user.Kubeconfigs) != 1 || *user.Kubeconfigs[0].Name != "clusterUser" || len(admin.Kubeconfigs) != 1 || *admin.Kubeconfigs[0].Name != "clusterAdmin" {
		t.Fatalf("credentials: %+v %+v", user, admin)
	}
	cfg := string(user.Kubeconfigs[0].Value)
	if !strings.Contains(cfg, "server: https://"+*p.Fqdn) || !strings.Contains(cfg, "user: clusterUser_pogi_web") || !strings.Contains(cfg, "kind: Config") {
		t.Fatalf("kubeconfig:\n%s", cfg)
	}
	_, err = clusters.ListClusterUserCredentials(ctx, rg, "nope", nil)
	wantErr(t, err, 404, "ResourceNotFound")

	// the API server's webhook takes the token of the kubeconfig, and no other
	_, token, _ := strings.Cut(cfg, "token: ")
	token, _, _ = strings.Cut(token, "\n")
	review := func(tok string) bool {
		body, _ := json.Marshal(map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "spec": map[string]any{"token": tok}})
		res := must(http.Post(srv.URL+"/_azure/aks/authn"+strings.ToLower(*mc.ID), "application/json", bytes.NewReader(body)))
		defer res.Body.Close()
		var out struct{ Status struct{ Authenticated bool } }
		json.NewDecoder(res.Body).Decode(&out)
		return out.Status.Authenticated
	}
	if !review(token) || review(token+"x") || review("") {
		t.Fatal("the webhook accepts the cluster's token only")
	}

	// state survives Save and Load; the token stays the same
	svc := srv.Hub.Service("azaks").(*aks.Service)
	if tables := svc.Resources(); len(tables) != 1 || len(tables[0].Rows) != 2 {
		t.Fatalf("resources: %v", tables)
	}
	if b, _ := json.Marshal(svc.Inspect("Clusters", "web")); !strings.Contains(string(b), "Docker is not available") || strings.Contains(string(b), token) {
		t.Fatalf("inspect: %s", b)
	}
	b := must(svc.Save())
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if !review(token) {
		t.Fatal("the token after Load")
	}

	// delete
	must(must(clusters.BeginDelete(ctx, rg, "web", nil)).PollUntilDone(ctx, fast))
	_, err = clusters.Get(ctx, rg, "web", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = pools.Get(ctx, rg, "web", "system", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	if review(token) {
		t.Fatal("the token of a deleted cluster")
	}
	svc.Reset()
	if tables := svc.Resources(); len(tables[0].Rows) != 0 {
		t.Fatalf("after Reset: %v", tables)
	}
}

// TestKubectl runs a cluster on k3s: the PUT answers Creating and the SDK's
// poller waits for Succeeded, then kubectl uses the kubeconfig of
// listClusterUserCredential (and the admin one) to get nodes and apply a ConfigMap.
func TestKubectl(t *testing.T) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("needs kubectl")
	}
	srv, clusters, pools := start(t, pogi.Config{})
	if testing.Short() || !srv.Hub.Docker().Available() {
		t.Skip("needs a running container engine")
	}
	name := "e2e" + core.RandHex(3)
	poller := must(clusters.BeginCreateOrUpdate(ctx, rg, name, cluster("system"), nil))
	if mc := must(clusters.Get(ctx, rg, name, nil)); *mc.Properties.ProvisioningState != "Creating" {
		t.Fatalf("cluster while it starts: %v", *mc.Properties.ProvisioningState)
	}
	mc := must(poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: time.Second}))
	if *mc.Properties.ProvisioningState != "Succeeded" || strings.HasSuffix(*mc.Properties.Fqdn, "azmk8s.io") {
		t.Fatalf("running cluster: %+v", mc.Properties)
	}
	must(must(pools.BeginCreateOrUpdate(ctx, rg, name, "extra", armcontainerservice.AgentPool{Properties: &armcontainerservice.ManagedClusterAgentPoolProfileProperties{Count: to.Ptr(int32(2))}}, nil)).PollUntilDone(ctx, fast))

	dir := t.TempDir()
	kubectl := func(kubeconfig string, args ...string) (string, error) {
		out, err := exec.Command("kubectl", append([]string{"--kubeconfig", kubeconfig}, args...)...).CombinedOutput()
		return string(out), err
	}
	check := func(admin bool) {
		t.Helper()
		var cfg []byte
		if admin {
			cfg = must(clusters.ListClusterAdminCredentials(ctx, rg, name, nil)).Kubeconfigs[0].Value
		} else {
			cfg = must(clusters.ListClusterUserCredentials(ctx, rg, name, nil)).Kubeconfigs[0].Value
		}
		kc := filepath.Join(dir, "kubeconfig")
		os.WriteFile(kc, cfg, 0o600)
		out, err := kubectl(kc, "get", "nodes", "--no-headers")
		if lines := strings.Split(strings.TrimSpace(out), "\n"); err != nil || len(lines) != 1 || !strings.Contains(lines[0], " Ready ") {
			t.Fatalf("kubectl get nodes: %v\n%s", err, out)
		}
		cm := filepath.Join(dir, "cm.yaml")
		os.WriteFile(cm, []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: pogi-e2e\n  namespace: default\ndata:\n  hello: world\n"), 0o600)
		if out, err := kubectl(kc, "apply", "-f", cm); err != nil {
			t.Fatalf("kubectl apply: %v\n%s", err, out)
		}
		if out, err := kubectl(kc, "get", "configmap", "pogi-e2e", "-o", "jsonpath={.data.hello}"); err != nil || out != "world" {
			t.Fatalf("kubectl get configmap: %v\n%s", err, out)
		}
	}
	check(false)
	check(true)

	// another token is refused
	cfg := must(clusters.ListClusterUserCredentials(ctx, rg, name, nil)).Kubeconfigs[0].Value
	bad := filepath.Join(dir, "bad")
	os.WriteFile(bad, bytes.Replace(cfg, []byte("token: pogi-aks-"), []byte("token: pogi-aks-x"), 1), 0o600)
	if out, err := kubectl(bad, "get", "nodes"); err == nil || !strings.Contains(out, "Unauthorized") {
		t.Fatalf("a bad token: %v\n%s", err, out)
	}

	// persisted state restarts the container; the kubeconfig names its new port
	svc := srv.Hub.Service("azaks").(*aks.Service)
	b := must(svc.Save())
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(4 * time.Minute); ; time.Sleep(time.Second) {
		got := must(clusters.Get(ctx, rg, name, nil))
		if *got.Properties.ProvisioningState == "Succeeded" {
			break
		}
		if *got.Properties.ProvisioningState == "Failed" || time.Now().After(end) {
			t.Fatalf("after Load: %v", *got.Properties.ProvisioningState)
		}
	}
	check(false)

	// delete removes the container
	must(must(clusters.BeginDelete(ctx, rg, name, nil)).PollUntilDone(ctx, fast))
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if _, err := srv.Hub.Docker().Inspect(ctx, "pogi-aks-"+rg+"-"+name); err != nil {
			break
		}
		if time.Now().After(end) {
			t.Fatal("DeleteCluster leaves its container")
		}
	}
}
