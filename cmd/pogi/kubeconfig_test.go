package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestKubeconfigGKE creates a GKE cluster, prints its kubeconfig with
// `pogi kubeconfig gke` and gets its nodes with kubectl.
func TestKubeconfigGKE(t *testing.T) {
	if err := kubeconfig([]string{"aks", "x"}); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("an unknown cloud: %v", err)
	}
	s := pogi.Start(t, pogi.Config{Services: []string{"container", "operations", "gcpoauth"}})
	if err := kubeconfig([]string{"gke", "nope", "--endpoint", s.URL}); err == nil || !strings.Contains(err.Error(), "no cluster") {
		t.Fatalf("a missing cluster: %v", err)
	}
	if _, err := exec.LookPath("kubectl"); err != nil || testing.Short() || !s.Hub.Docker().Available() {
		t.Skip("needs kubectl and a running container engine")
	}
	ctx := context.Background()
	c, err := container.NewClusterManagerClient(ctx, option.WithEndpoint(strings.TrimPrefix(s.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const parent = "projects/pogi/locations/us-central1"
	name := "cli-" + core.RandHex(3)
	op, err := c.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: &containerpb.Cluster{Name: name, InitialNodeCount: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(4 * time.Minute); ; time.Sleep(time.Second) {
		got, err := c.GetOperation(ctx, &containerpb.GetOperationRequest{Name: parent + "/operations/" + op.Name})
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == containerpb.Operation_DONE {
			if got.Error != nil {
				t.Fatalf("cluster: %v", got.Error)
			}
			break
		}
		if time.Now().After(end) {
			t.Fatal("the cluster does not start")
		}
	}
	out := stdout(t, func() error { return kubeconfig([]string{"gke", name, "--endpoint", s.URL}) })
	kc := filepath.Join(t.TempDir(), "kubeconfig")
	os.WriteFile(kc, []byte(out), 0o600)
	if got, err := exec.Command("kubectl", "--kubeconfig", kc, "get", "nodes", "--no-headers").CombinedOutput(); err != nil || !strings.Contains(string(got), " Ready ") {
		t.Fatalf("kubectl get nodes: %v\n%s", err, got)
	}
}
