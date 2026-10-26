package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The GKE form puts the parent first and complete, and a cluster name plus node count is enough to create one.
func TestGKECreateForm(t *testing.T) {
	c := start(t, "container")
	page := c.get("/_pogi/new/container/ClusterManager.CreateCluster")
	want(t, page, "Create cluster", "projects/pogi/locations/us-central1")
	if strings.Index(page, `name="parent"`) > strings.Index(page, `name="cluster.name"`) || !strings.Contains(page, `name="parent"`) {
		t.Fatalf("parent is not before the cluster fields")
	}
	c.ok("/_pogi/new/container/ClusterManager.CreateCluster", url.Values{
		"parent": {"projects/pogi/locations/us-central1"}, "cluster.name": {"c1"}, "cluster.initialNodeCount": {"1"}})
	want(t, c.get("/_pogi/resources/container"), "c1")
}

func TestEmptyStateWithoutCreate(t *testing.T) {
	c := start(t, "container")
	want(t, c.get("/_pogi/resources/container"), "Create one here")
}

// A page with no Create button must not say "Create one here".
func TestEmptyStateNoCreateButton(t *testing.T) {
	c := start(t, "azemail")
	page := c.get("/_pogi/resources/azemail")
	want(t, page, "Create one with your CLI, SDK or Terraform")
	wantNot(t, page, "Create one here")
}

// Saving an Azure resource says what it was and opens the page of the service that lists it.
func TestAzureCreateResult(t *testing.T) {
	c := start(t, "arm", "azaks")
	v := url.Values{
		"type": {"Microsoft.ContainerService/managedClusters"},
		"path": {"/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/pogi/providers/Microsoft.ContainerService/managedClusters/aks1"},
		"body": {`{"location":"eastus","properties":{"dnsPrefix":"aks1","agentPoolProfiles":[{"name":"np","count":1,"vmSize":"Standard_DS2_v2","mode":"System"}]}}`},
	}
	req, _ := http.NewRequest("POST", c.base+"/_pogi/arm/new", strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", "drawer")
	res, err := c.c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	got := res.Header.Get("HX-Trigger") + res.Header.Get("HX-Location")
	want(t, got, "Managed cluster aks1 saved", `/_pogi/resources/azaks`)
}
