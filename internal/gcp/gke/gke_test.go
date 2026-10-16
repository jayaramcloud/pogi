package gke_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	mk "cloud.google.com/go/managedkafka/apiv1"
	mkpb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gke"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

const loc = "projects/p/locations/us-central1"

// start runs pogi and returns a gRPC client and a REST client of it.
func start(t *testing.T, cfg pogi.Config) (*pogi.Running, *container.ClusterManagerClient, *container.ClusterManagerClient) {
	t.Helper()
	cfg.Services = []string{"container", "managedkafka", "operations", "gcpoauth"}
	s := pogi.Start(t, cfg)
	g := must(container.NewClusterManagerClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	r := must(container.NewClusterManagerRESTClient(ctx, option.WithEndpoint(s.URL), option.WithoutAuthentication()))
	t.Cleanup(func() { g.Close(); r.Close() })
	return s, g, r
}

func newCluster(name string) *containerpb.CreateClusterRequest {
	return &containerpb.CreateClusterRequest{Parent: loc, Cluster: &containerpb.Cluster{Name: name, InitialNodeCount: 2,
		ResourceLabels: map[string]string{"env": "test"}}}
}

// noEngine runs pogi as if no container engine were installed.
var noEngine = pogi.Config{DockerHost: "/nonexistent/docker.sock"}

// TestMetadata covers the API with no cluster behind it, over gRPC and over
// REST: clusters, node pools and operations, and the paths Managed Kafka shares.
func TestMetadata(t *testing.T) {
	s, c, rest := start(t, noEngine)
	name := loc + "/clusters/web"

	op := must(c.CreateCluster(ctx, newCluster("web")))
	if op.OperationType != containerpb.Operation_CREATE_CLUSTER || op.Status != containerpb.Operation_DONE || !strings.HasSuffix(op.TargetLink, name) {
		t.Fatalf("create operation: %v", op)
	}
	if got := must(c.GetOperation(ctx, &containerpb.GetOperationRequest{Name: loc + "/operations/" + op.Name})); got.Name != op.Name || got.Status != containerpb.Operation_DONE {
		t.Fatalf("get operation: %v", got)
	}
	cl := must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name}))
	if cl.Name != "web" || cl.Location != "us-central1" || cl.Status != containerpb.Cluster_RUNNING || cl.StatusMessage == "" || cl.Id == "" ||
		cl.CurrentNodeCount != 2 || len(cl.NodePools) != 1 || cl.NodePools[0].Name != "default-pool" || cl.NodePools[0].Status != containerpb.NodePool_RUNNING ||
		cl.ResourceLabels["env"] != "test" || !strings.HasSuffix(cl.SelfLink, name) {
		t.Fatalf("cluster: %v", cl)
	}
	_, err := c.CreateCluster(ctx, newCluster("web"))
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateCluster(ctx, newCluster("Bad_Name"))
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc + "/clusters/nope"})
	wantCode(t, err, codes.NotFound)
	must(c.CreateCluster(ctx, newCluster("api")))

	// REST sees the same clusters, and answers errors the Google way
	list := must(rest.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: loc}))
	if len(list.Clusters) != 2 || list.Clusters[0].Name != "api" || list.Clusters[1].Name != "web" {
		t.Fatalf("list: %v", list)
	}
	_, err = rest.GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc + "/clusters/nope"})
	wantCode(t, err, codes.NotFound)

	// update, labels
	up := must(rest.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{Name: name, Update: &containerpb.ClusterUpdate{DesiredMonitoringService: "none"}}))
	if up.OperationType != containerpb.Operation_UPDATE_CLUSTER {
		t.Fatalf("update operation: %v", up)
	}
	must(rest.SetLabels(ctx, &containerpb.SetLabelsRequest{Name: name, ResourceLabels: map[string]string{"env": "prod"}}))
	cl = must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name}))
	if cl.MonitoringService != "none" || cl.ResourceLabels["env"] != "prod" || len(cl.ResourceLabels) != 1 {
		t.Fatalf("updated cluster: %v", cl)
	}

	// node pools
	np := &containerpb.NodePool{Name: "gpu", InitialNodeCount: 3, Config: &containerpb.NodeConfig{MachineType: "n1-standard-4"}}
	must(rest.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{Parent: name, NodePool: np}))
	_, err = c.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{Parent: name, NodePool: np})
	wantCode(t, err, codes.AlreadyExists)
	pool := must(c.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: name + "/nodePools/gpu"}))
	if pool.Config.MachineType != "n1-standard-4" || pool.InitialNodeCount != 3 || pool.Status != containerpb.NodePool_RUNNING || pool.Version == "" {
		t.Fatalf("node pool: %v", pool)
	}
	must(rest.UpdateNodePool(ctx, &containerpb.UpdateNodePoolRequest{Name: name + "/nodePools/gpu", NodeVersion: "1.36.0-gke.1", ImageType: "UBUNTU_CONTAINERD"}))
	must(rest.SetNodePoolSize(ctx, &containerpb.SetNodePoolSizeRequest{Name: name + "/nodePools/gpu", NodeCount: 5}))
	pools := must(c.ListNodePools(ctx, &containerpb.ListNodePoolsRequest{Parent: name}))
	if len(pools.NodePools) != 2 || pools.NodePools[1].Version != "1.36.0-gke.1" || pools.NodePools[1].Config.ImageType != "UBUNTU_CONTAINERD" || pools.NodePools[1].InitialNodeCount != 5 {
		t.Fatalf("node pools: %v", pools)
	}
	if cl = must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})); cl.CurrentNodeCount != 7 {
		t.Fatalf("node count: %d", cl.CurrentNodeCount)
	}
	must(c.DeleteNodePool(ctx, &containerpb.DeleteNodePoolRequest{Name: name + "/nodePools/gpu"}))
	_, err = rest.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: name + "/nodePools/gpu"})
	wantCode(t, err, codes.NotFound)
	ops := must(c.ListOperations(ctx, &containerpb.ListOperationsRequest{Parent: loc}))
	if len(ops.Operations) < 8 {
		t.Fatalf("operations: %d", len(ops.Operations))
	}

	// Managed Kafka's clusters live at the same paths and are not GKE's
	kc := must(mk.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer kc.Close()
	kafka := must(must(kc.CreateCluster(ctx, &mkpb.CreateClusterRequest{Parent: loc, ClusterId: "events", Cluster: &mkpb.Cluster{
		CapacityConfig: &mkpb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3 << 30},
		PlatformConfig: &mkpb.Cluster_GcpConfig{GcpConfig: &mkpb.GcpConfig{AccessConfig: &mkpb.AccessConfig{
			NetworkConfigs: []*mkpb.NetworkConfig{{Subnet: "projects/p/regions/us-central1/subnetworks/default"}}}}}}})).Wait(ctx))
	if kafka.Name != loc+"/clusters/events" {
		t.Fatalf("kafka cluster: %v", kafka)
	}
	if got := must(kc.GetCluster(ctx, &mkpb.GetClusterRequest{Name: loc + "/clusters/events"})); got.Name != kafka.Name {
		t.Fatalf("kafka get: %v", got)
	}
	if got := must(rest.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})); got.Name != "web" {
		t.Fatalf("gke get next to a kafka cluster: %v", got)
	}

	// delete: the cluster is gone at once, with its node pools
	del := must(rest.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name}))
	if del.OperationType != containerpb.Operation_DELETE_CLUSTER || del.Status != containerpb.Operation_DONE {
		t.Fatalf("delete operation: %v", del)
	}
	_, err = c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})
	wantCode(t, err, codes.NotFound)
	_, err = c.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	wantCode(t, err, codes.NotFound)
	_, err = c.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: name + "/nodePools/default-pool"})
	wantCode(t, err, codes.NotFound)

	// state survives Save and Load; a cluster that ran on k3s turns ERROR with no engine
	svc := s.Hub.Service("container").(*gke.Service)
	b := must(svc.Save())
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if got := must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc + "/clusters/api"})); got.Status != containerpb.Cluster_RUNNING {
		t.Fatalf("after Load: %v", got)
	}
	svc.Reset()
	if l := must(c.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: loc})); len(l.Clusters) != 0 {
		t.Fatalf("after Reset: %v", l)
	}
	if tables := svc.Resources(); len(tables) != 1 || len(tables[0].Rows) != 0 {
		t.Fatalf("resources: %v", tables)
	}
}

// TestKubectl runs a cluster on k3s and reaches it two ways: with the
// kubeconfig of /_gcp/gke/kubeconfig (`pogi kubeconfig gke`) and with one built
// the gcloud way, an exec plugin that hands kubectl a pogi access token.
func TestKubectl(t *testing.T) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("needs kubectl")
	}
	s, c, _ := start(t, pogi.Config{})
	if testing.Short() || !s.Hub.Docker().Available() {
		t.Skip("needs a running container engine")
	}
	name := "e2e-" + core.RandHex(3)
	op := must(c.CreateCluster(ctx, newCluster(name)))
	if op.Status != containerpb.Operation_RUNNING {
		t.Fatalf("create operation: %v", op)
	}
	if cl := must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc + "/clusters/" + name})); cl.Status != containerpb.Cluster_PROVISIONING || cl.Endpoint != "" {
		t.Fatalf("cluster while it starts: %v", cl)
	}
	var cl *containerpb.Cluster
	for end := time.Now().Add(4 * time.Minute); ; time.Sleep(time.Second) {
		got := must(c.GetOperation(ctx, &containerpb.GetOperationRequest{Name: loc + "/operations/" + op.Name}))
		if got.Status == containerpb.Operation_DONE {
			if got.Error != nil {
				b, _ := json.Marshal(s.Hub.Service("container").(core.Inspectable).Inspect("Clusters", name))
				t.Fatalf("operation failed: %v\n%s", got.Error, b)
			}
			cl = must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc + "/clusters/" + name}))
			break
		}
		if time.Now().After(end) {
			t.Fatal("the operation does not finish")
		}
	}
	if cl.Status != containerpb.Cluster_RUNNING || cl.Endpoint == "" || cl.MasterAuth.ClusterCaCertificate == "" || cl.NodePools[0].Status != containerpb.NodePool_RUNNING {
		t.Fatalf("running cluster: %v", cl)
	}

	dir := t.TempDir()
	kubectl := func(kubeconfig string, args ...string) (string, error) {
		cmd := exec.Command("kubectl", append([]string{"--kubeconfig", kubeconfig}, args...)...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	check := func(kubeconfig string) {
		t.Helper()
		out, err := kubectl(kubeconfig, "get", "nodes", "--no-headers")
		if lines := strings.Split(strings.TrimSpace(out), "\n"); err != nil || len(lines) != 1 || !strings.Contains(lines[0], " Ready ") {
			t.Fatalf("kubectl get nodes: %v\n%s", err, out)
		}
		cm := filepath.Join(dir, "cm.yaml")
		os.WriteFile(cm, []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: pogi-e2e\n  namespace: default\ndata:\n  hello: world\n"), 0o600)
		if out, err := kubectl(kubeconfig, "apply", "-f", cm); err != nil {
			t.Fatalf("kubectl apply: %v\n%s", err, out)
		}
		if out, err := kubectl(kubeconfig, "get", "configmap", "pogi-e2e", "-o", "jsonpath={.data.hello}"); err != nil || out != "world" {
			t.Fatalf("kubectl get configmap: %v\n%s", err, out)
		}
	}

	// the static kubeconfig of `pogi kubeconfig gke`
	static := filepath.Join(dir, "static")
	fetch := func(cl *containerpb.Cluster) string {
		res := must(http.Get(s.URL + "/_gcp/gke/kubeconfig/p/us-central1/" + name))
		cfg := string(must(io.ReadAll(res.Body)))
		res.Body.Close()
		if res.StatusCode != 200 || !strings.Contains(cfg, "server: https://"+cl.Endpoint) || !strings.Contains(cfg, "token: ya29.pogi-") {
			t.Fatalf("kubeconfig: %d\n%s", res.StatusCode, cfg)
		}
		os.WriteFile(static, []byte(cfg), 0o600)
		return cfg
	}
	cfg := fetch(cl)
	check(static)

	// the gcloud way: a kubeconfig with an exec plugin that prints an access token fetched from pogi's token endpoint
	tok := must(http.PostForm(s.URL+"/token", url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"x"}, "scope": {"https://www.googleapis.com/auth/cloud-platform"}}))
	var tr struct {
		AccessToken string `json:"access_token"`
	}
	json.NewDecoder(tok.Body).Decode(&tr)
	tok.Body.Close()
	script := filepath.Join(dir, "gke-gcloud-auth-plugin")
	os.WriteFile(script, []byte(`#!/bin/sh
cat <<EOF
{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":{"token":"`+tr.AccessToken+`"}}
EOF
`), 0o700)
	gcloud := filepath.Join(dir, "gcloud")
	os.WriteFile(gcloud, []byte("apiVersion: v1\nkind: Config\nclusters:\n- name: gke\n  cluster:\n    server: https://"+cl.Endpoint+"\n    certificate-authority-data: "+cl.MasterAuth.ClusterCaCertificate+
		"\nusers:\n- name: gke\n  user:\n    exec:\n      apiVersion: client.authentication.k8s.io/v1beta1\n      command: "+script+"\n      interactiveMode: Never\n"+
		"contexts:\n- name: gke\n  context:\n    cluster: gke\n    user: gke\ncurrent-context: gke\n"), 0o600)
	if !strings.HasPrefix(tr.AccessToken, "ya29.pogi-") {
		t.Fatalf("token: %q", tr.AccessToken)
	}
	check(gcloud)

	// anything else is refused
	bad := filepath.Join(dir, "bad")
	os.WriteFile(bad, []byte(strings.Replace(cfg, "ya29.pogi-", "ya29.nope-", 1)), 0o600)
	if out, err := kubectl(bad, "get", "nodes"); err == nil || !strings.Contains(out, "Unauthorized") {
		t.Fatalf("a bad token: %v\n%s", err, out)
	}

	// persisted state restarts the container; the same token still works
	svc := s.Hub.Service("container").(*gke.Service)
	b := must(svc.Save())
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(4 * time.Minute); ; time.Sleep(time.Second) {
		got := must(c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc + "/clusters/" + name}))
		if got.Status == containerpb.Cluster_RUNNING {
			fetch(got) // the published port is another one
			break
		}
		if got.Status == containerpb.Cluster_ERROR || time.Now().After(end) {
			t.Fatalf("after Load: %v", got)
		}
	}
	check(static)

	// delete removes the container
	must(c.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: loc + "/clusters/" + name}))
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if _, err := s.Hub.Docker().Inspect(ctx, "pogi-gke-p-us-central1-"+name); err != nil {
			break
		}
		if time.Now().After(end) {
			t.Fatal("DeleteCluster leaves its container")
		}
	}
	if res := must(http.Get(s.URL + "/_gcp/gke/kubeconfig/p/us-central1/" + name)); res.StatusCode != 404 {
		t.Fatalf("kubeconfig of a deleted cluster: %d", res.StatusCode)
	}
}
