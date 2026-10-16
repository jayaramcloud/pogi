package test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	ar "cloud.google.com/go/artifactregistry/apiv1"
	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func docker(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// TestRegistryPushRun pushes an image with docker to an Azure Container Registry and to an Artifact Registry
// repository, both of which are the one registry on pogi's registry port, and has a container group of
// Container Instances run each of them by the name the cloud gives it.
func TestRegistryPushRun(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	e := armtest.Start(t, pogi.Config{ECRPort: port}, "ecr", "azacr", "azaci", "artifactregistry", "operations")
	if !e.Srv.Hub.Docker().Available() {
		t.Skip("no container engine")
	}
	ctx := context.Background()
	sub, rg := core.AzureSubscription, core.AzureResourceGroup

	// the two registries
	regs := armtest.Must(armcontainerregistry.NewRegistriesClient(sub, e.Cred, e.Opts))
	reg := armtest.Must(armtest.Must(regs.BeginCreate(ctx, rg, "pogipush", armcontainerregistry.Registry{Location: to.Ptr("eastus"),
		SKU: &armcontainerregistry.SKU{Name: to.Ptr(armcontainerregistry.SKUNameBasic)}}, nil)).PollUntilDone(ctx, armtest.Fast))
	login := *reg.Properties.LoginServer
	if login != fmt.Sprintf("pogipush.azurecr.localhost:%d", port) {
		t.Fatalf("login server %s", login)
	}
	gar := armtest.Must(ar.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(e.Srv.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer gar.Close()
	repo := "projects/pushproj/locations/us-central1/repositories/apps"
	armtest.Must(gar.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{Parent: "projects/pushproj/locations/us-central1", RepositoryId: "apps",
		Repository: &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_DOCKER}}))
	registry := fmt.Sprintf("http://127.0.0.1:%d", port)
	for end := time.Now().Add(3 * time.Minute); ; time.Sleep(200 * time.Millisecond) { // a forwarder container on Docker Desktop: its image may need a pull
		if resp, err := http.Get(registry + "/v2/"); err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(end) {
			t.Fatal("the registry port does not answer")
		}
	}

	// docker push: to the registry's own host, and to the path the repository has
	acrImage := fmt.Sprintf("%s/web:1", login)
	garImage := fmt.Sprintf("localhost:%d/pushproj/apps/web:1", port)
	for _, img := range []string{acrImage, garImage} {
		docker(t, "tag", "nginx:alpine", img)
		t.Cleanup(func() { exec.Command("docker", "rmi", "-f", img).Run() })
		docker(t, "push", img)
	}
	var found []string
	for it := gar.ListDockerImages(ctx, &artifactregistrypb.ListDockerImagesRequest{Parent: repo}); ; {
		img, err := it.Next()
		if err != nil {
			break
		}
		found = append(found, img.Uri[:strings.IndexByte(img.Uri, '@')]+" "+strings.Join(img.Tags, ","))
	}
	if len(found) != 1 || found[0] != "us-central1-docker.pkg.dev/pushproj/apps/web 1" {
		t.Fatalf("Artifact Registry images %v", found)
	}
	if resp, err := http.Get(registry + "/v2/pogipush/web/tags/list"); err != nil {
		t.Fatal(err)
	} else if b, _ := io.ReadAll(resp.Body); !strings.Contains(string(b), `"1"`) {
		t.Fatalf("Container Registry tags: %s", b)
	}

	// container groups run them by the cloud's names, which pogi pulls from its registry
	groups := armtest.Must(armcontainerinstance.NewContainerGroupsClient(sub, e.Cred, e.Opts))
	for name, image := range map[string]string{"fromacr": "pogipush.azurecr.io/web:1", "fromgar": "us-central1-docker.pkg.dev/pushproj/apps/web:1"} {
		body := armcontainerinstance.ContainerGroup{Location: to.Ptr("eastus"), Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
			OSType: to.Ptr(armcontainerinstance.OperatingSystemTypesLinux), Containers: []*armcontainerinstance.Container{{Name: to.Ptr("web"), Properties: &armcontainerinstance.ContainerProperties{
				Image: to.Ptr(image), Resources: &armcontainerinstance.ResourceRequirements{Requests: &armcontainerinstance.ResourceRequests{CPU: to.Ptr(0.5), MemoryInGB: to.Ptr(0.5)}}}}},
			IPAddress: &armcontainerinstance.IPAddress{Type: to.Ptr(armcontainerinstance.ContainerGroupIPAddressTypePublic),
				Ports: []*armcontainerinstance.Port{{Port: to.Ptr[int32](80), Protocol: to.Ptr(armcontainerinstance.ContainerGroupNetworkProtocolTCP)}}}}}
		armtest.Must(armtest.Must(groups.BeginCreateOrUpdate(ctx, rg, name, body, nil)).PollUntilDone(ctx, armtest.Fast))
		n := name
		t.Cleanup(func() { groups.BeginDelete(ctx, rg, n, nil) })
		var code int
		var page string
		for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
			resp, err := http.Get(e.Srv.URL + "/_azure/aci/" + name + "/")
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if code, page = resp.StatusCode, string(b); code == 200 {
				break
			}
		}
		if code != 200 || !strings.Contains(page, "Welcome to nginx") {
			t.Fatalf("group %s (%s) answers %d %.80q", name, image, code, page)
		}
	}

	// deleting the registry and the repository takes their images
	armtest.Must(regs.BeginDelete(ctx, rg, "pogipush", nil)).PollUntilDone(ctx, armtest.Fast)
	armtest.Must(gar.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{Name: repo})).Wait(ctx)
	for _, p := range []string{"/v2/pogipush/web/tags/list", "/v2/pushproj/apps/web/tags/list"} {
		if resp, err := http.Get(registry + p); err != nil || resp.StatusCode != 404 {
			t.Fatalf("%s after the delete: %v %v", p, err, resp)
		}
	}
}
