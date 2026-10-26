package web_test

import (
	"github.com/nrndev/pogi/internal/docker/dockertest"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

// vpcCount is how many VPCs and subnets pogi's EC2 holds (the default VPC is one of them).
func (c *console) vpcCount() (vpcs, subnets int) {
	for _, t := range c.srv.Hub.Service("ec2").(core.Browsable).Resources() {
		switch t.Name {
		case "VPCs":
			vpcs = len(t.Rows)
		case "Subnets":
			subnets = len(t.Rows)
		}
	}
	return
}

// An EKS cluster made with no subnets gets a VPC and two subnets first, and the message says so.
// A failed create (the name is taken) leaves no VPC behind.
func TestEKSCreateMakesVPC(t *testing.T) {
	dockertest.Start(t, 0, true) // no engine: the clusters stay metadata, so no container is left behind
	c := start(t, "eks", "ec2")
	form := url.Values{"name": {"k1"}, "version": {"1.35"}, "roleArn": {"arn:aws:iam::000000000000:role/eks"}}
	status, h, body := c.hxPost("/_pogi/new/eks/CreateCluster", "drawer", form)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	want(t, h.Get("HX-Trigger"), "Cluster k1 created", "No subnets were picked", "first made VPC vpc-")
	vpcs, subnets := c.vpcCount() // the cluster's own VPC and its two subnets are in
	want(t, c.get("/_pogi/resources/ec2?tab=Subnets"), "10.0.1.0/24", "10.0.2.0/24")
	_, _, body = c.hxPost("/_pogi/new/eks/CreateCluster", "drawer", form) // the name is taken
	want(t, body, "k1")
	if v, s := c.vpcCount(); v != vpcs || s != subnets {
		t.Fatalf("a failed create left a VPC behind: %d VPCs, %d subnets", v, s)
	}
	form.Set("name", "k2")
	form.Set("subnetIds.0", c.firstSubnet())
	_, h, _ = c.hxPost("/_pogi/new/eks/CreateCluster", "drawer", form)
	wantNot(t, h.Get("HX-Trigger"), "No subnets were picked")
	if v, _ := c.vpcCount(); v != vpcs {
		t.Fatalf("a create with a subnet made a VPC: %d", v)
	}
}

func (c *console) firstSubnet() string {
	for _, t := range c.srv.Hub.Service("ec2").(core.Browsable).Resources() {
		if t.Name == "Subnets" {
			return t.Rows[0][0]
		}
	}
	c.t.Fatal("no subnet")
	return ""
}

// An AKS cluster is made from four fields, and its resource group is made first when it is missing.
func TestAKSCreate(t *testing.T) {
	dockertest.Start(t, 0, true) // no engine: the clusters stay metadata, so no container is left behind
	c := start(t, "arm", "azaks")
	form := url.Values{"name": {"aks1"}, "resourceGroup": {"newrg"}, "location": {"westeurope"}, "nodeCount": {"2"}}
	status, h, body := c.hxPost("/_pogi/new/azaks/CreateCluster", "drawer", form)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	want(t, h.Get("HX-Trigger"), "Cluster aks1 created", "Resource group newrg did not exist")
	want(t, c.get("/_pogi/resources/azaks"), "aks1", "newrg", "westeurope")

	form.Set("name", "aks2") // the group exists now
	_, h, _ = c.hxPost("/_pogi/new/azaks/CreateCluster", "drawer", form)
	want(t, h.Get("HX-Trigger"), "Cluster aks2 created")
	wantNot(t, h.Get("HX-Trigger"), "did not exist")

	for field, bad := range map[string]string{"resourceGroup": "no/slash", "location": "East US", "nodeCount": "0"} {
		f := url.Values{"name": {"aks3"}, "resourceGroup": {"newrg"}, "location": {"westeurope"}, "nodeCount": {"1"}}
		f.Set(field, bad)
		_, _, body := c.hxPost("/_pogi/new/azaks/CreateCluster", "drawer", f)
		want(t, body, `id="apierr"`)
	}
}

// The form defaults come from the scope, and GKE has one "Create cluster", the short one.
func TestGKECreateShort(t *testing.T) {
	dockertest.Start(t, 0, true) // no engine: the clusters stay metadata, so no container is left behind
	c := start(t, "container")
	page := c.get("/_pogi/new/container/CreateCluster")
	want(t, page, `value="us-central1"`, `name="nodeCount"`)
	c.ok("/_pogi/new/container/CreateCluster", url.Values{"name": {"g1"}, "location": {"us-central1"}, "nodeCount": {"1"}})
	want(t, c.get("/_pogi/resources/container"), "g1")
	_, _, body := c.hxPost("/_pogi/new/container/CreateCluster", "drawer", url.Values{"name": {"g2"}, "location": {"../x"}, "nodeCount": {"1"}})
	want(t, body, "is not valid")
	page = c.get("/_pogi/resources/container")
	want(t, page, "/_pogi/new/container/CreateCluster")
	wantNot(t, page, "ClusterManager.CreateCluster")
}

// The kubeconfig download: unknown cluster is 404, a cluster with no API server yet is 409, both say why.
func TestKubeconfigErrors(t *testing.T) {
	dockertest.Start(t, 0, true) // no engine: the clusters stay metadata, so no container is left behind
	c := start(t, "eks", "container", "azaks", "arm")
	get := func(path string) (int, string) {
		res, err := c.c.Get(c.base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	for _, svc := range []string{"eks", "container", "azaks"} {
		if st, msg := get("/_pogi/kubeconfig/" + svc + "/nope"); st != 404 || !strings.Contains(msg, "nope") {
			t.Fatalf("%s unknown cluster: %d %q", svc, st, msg)
		}
	}
	if st, _ := get("/_pogi/kubeconfig/s3/x"); st != 404 {
		t.Fatalf("a service with no clusters: %d", st)
	}
	// fresh clusters have no API server yet
	c.hxPost("/_pogi/new/eks/CreateCluster", "drawer", url.Values{"name": {"e1"}, "version": {"1.35"}, "roleArn": {"arn:aws:iam::000000000000:role/eks"}})
	c.hxPost("/_pogi/new/container/CreateCluster", "drawer", url.Values{"name": {"g1"}, "location": {"us-central1"}, "nodeCount": {"1"}})
	c.hxPost("/_pogi/new/azaks/CreateCluster", "drawer", url.Values{"name": {"a1"}, "resourceGroup": {"rg1"}, "location": {"eastus"}, "nodeCount": {"1"}})
	for svc, name := range map[string]string{"eks": "e1", "container": "g1", "azaks": "a1"} {
		if st, msg := get("/_pogi/kubeconfig/" + svc + "/" + name); st != 409 || !strings.Contains(msg, name) && !strings.Contains(msg, "API server") {
			t.Fatalf("%s cluster not ready: %d %q", svc, st, msg)
		}
		d := c.drawer("/_pogi/resources/" + svc + "/Clusters/" + name)
		want(t, d, "Connect with kubectl", "Available when the cluster is ready")
		wantNot(t, d, "Download kubeconfig")
	}
}
