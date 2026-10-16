package aci_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

var (
	ctx = armtest.Ctx
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
)

// start runs pogi with Container Instances and skips the test without a container engine.
func start(t *testing.T) (*armtest.Env, *armcontainerinstance.ContainerGroupsClient) {
	t.Helper()
	e := armtest.Start(t, pogi.Config{}, "azaci")
	if !e.Srv.Hub.Docker().Available() {
		t.Skip("no container engine")
	}
	return e, armtest.Must(armcontainerinstance.NewContainerGroupsClient(sub, e.Cred, e.Opts))
}

// get requests path of host through pogi's port (no DNS needed).
func get(e *armtest.Env, host, path string) (int, string) {
	addr := strings.TrimPrefix(e.Srv.URL, "http://")
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, n, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, n, addr)
	}}}
	resp, err := c.Get("http://" + host + ":" + addr[strings.LastIndexByte(addr, ':')+1:] + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// answers waits for the service at host to answer with a body that has want.
func answers(t *testing.T, e *armtest.Env, host, path, want string) {
	t.Helper()
	var code int
	var body string
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if code, body = get(e, host, path); code == 200 && strings.Contains(body, want) {
			return
		}
	}
	t.Fatalf("%s%s = %d %q, want %q", host, path, code, body, want)
}

func container(name, image string, cmd ...string) *armcontainerinstance.Container {
	c := &armcontainerinstance.Container{Name: to.Ptr(name), Properties: &armcontainerinstance.ContainerProperties{Image: to.Ptr(image),
		Resources: &armcontainerinstance.ResourceRequirements{Requests: &armcontainerinstance.ResourceRequests{CPU: to.Ptr(0.5), MemoryInGB: to.Ptr(0.5)}}}}
	for _, a := range cmd {
		c.Properties.Command = append(c.Properties.Command, to.Ptr(a))
	}
	return c
}

func group(restart armcontainerinstance.ContainerGroupRestartPolicy, cs ...*armcontainerinstance.Container) armcontainerinstance.ContainerGroup {
	return armcontainerinstance.ContainerGroup{Location: to.Ptr("eastus"), Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
		OSType: to.Ptr(armcontainerinstance.OperatingSystemTypesLinux), RestartPolicy: to.Ptr(restart), Containers: cs}}
}

// state polls until the group is in a state, or fails.
func state(t *testing.T, cg *armcontainerinstance.ContainerGroupsClient, name, want string) armcontainerinstance.ContainerGroup {
	t.Helper()
	var got armcontainerinstance.ContainerGroup
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		got = armtest.Must(cg.Get(ctx, rg, name, nil)).ContainerGroup
		if got.Properties.InstanceView != nil && *got.Properties.InstanceView.State == want {
			return got
		}
	}
	t.Fatalf("group %s is %v, want %s", name, got.Properties.InstanceView, want)
	return got
}

func TestContainerGroup(t *testing.T) {
	e, cg := start(t)
	logs := armtest.Must(armcontainerinstance.NewContainersClient(sub, e.Cred, e.Opts))

	// 1. a group of nginx and a sidecar that reaches it on localhost; the ports of the group are published
	web := container("web", "nginx:alpine")
	web.Properties.Ports = []*armcontainerinstance.ContainerPort{{Port: to.Ptr[int32](80)}}
	web.Properties.EnvironmentVariables = []*armcontainerinstance.EnvironmentVariable{{Name: to.Ptr("GREETING"), Value: to.Ptr("hi")}, {Name: to.Ptr("SECRET"), SecureValue: to.Ptr("s3cret")}}
	side := container("side", "alpine:3", "sh", "-c", "echo sidecar-up; sleep 2; wget -qO- http://localhost:80/ | head -3; sleep 3600")
	body := group(armcontainerinstance.ContainerGroupRestartPolicyAlways, web, side)
	body.Tags = map[string]*string{"env": to.Ptr("test")}
	body.Properties.IPAddress = &armcontainerinstance.IPAddress{Type: to.Ptr(armcontainerinstance.ContainerGroupIPAddressTypePublic), DNSNameLabel: to.Ptr("pogi-aci-web"),
		Ports: []*armcontainerinstance.Port{{Port: to.Ptr[int32](80), Protocol: to.Ptr(armcontainerinstance.ContainerGroupNetworkProtocolTCP)}}}
	poller := armtest.Must(cg.BeginCreateOrUpdate(ctx, rg, "web", body, nil))
	t.Cleanup(func() { cg.BeginDelete(ctx, rg, "web", nil) })
	created := armtest.Must(poller.PollUntilDone(ctx, armtest.Fast))
	p := created.Properties
	if *created.Name != "web" || *p.ProvisioningState != "Succeeded" || *p.InstanceView.State != "Running" || *p.IPAddress.IP != "127.0.0.1" ||
		!strings.HasPrefix(*p.IPAddress.Fqdn, "pogi-aci-web.eastus.azurecontainer.localhost:") || len(p.Containers) != 2 {
		t.Fatalf("%+v %+v", created, p)
	}
	st := p.Containers[0].Properties.InstanceView
	if *st.CurrentState.State != "Running" || st.CurrentState.StartTime == nil || *st.RestartCount != 0 {
		t.Fatalf("%+v", st)
	}
	if p.Containers[0].Properties.EnvironmentVariables[1].SecureValue != nil {
		t.Fatal("a secure value is returned")
	}

	// 2. the group answers on its port, by its fqdn and by path
	answers(t, e, "pogi-aci-web.eastus.azurecontainer.localhost", "/", "Welcome to nginx")
	answers(t, e, "localhost", "/_azure/aci/web/", "Welcome to nginx")
	if code, _ := get(e, "nobody.eastus.azurecontainer.localhost", "/"); code != 404 {
		t.Fatalf("an unknown group answers %d", code)
	}

	// 3. logs: the sidecar's own output, and what it read from nginx through the shared localhost
	var out string
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end) && !strings.Contains(out, "DOCTYPE"); time.Sleep(300 * time.Millisecond) {
		out = *armtest.Must(logs.ListLogs(ctx, rg, "web", "side", nil)).Content
	}
	if !strings.HasPrefix(out, "sidecar-up\n") || !strings.Contains(out, "<!DOCTYPE html>") {
		t.Fatalf("logs = %q", out)
	}
	if tail := *armtest.Must(logs.ListLogs(ctx, rg, "web", "side", &armcontainerinstance.ContainersClientListLogsOptions{Tail: to.Ptr[int32](1)})).Content; strings.Count(tail, "\n") != 1 {
		t.Fatalf("tail = %q", tail)
	}
	_, err := logs.ListLogs(ctx, rg, "web", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// 4. an update of the tags leaves the containers alone
	up := armtest.Must(cg.Update(ctx, rg, "web", armcontainerinstance.Resource{Tags: map[string]*string{"env": to.Ptr("prod")}}, nil))
	if *up.Tags["env"] != "prod" || !up.Properties.Containers[0].Properties.InstanceView.CurrentState.StartTime.Equal(*st.CurrentState.StartTime) {
		t.Fatalf("%+v", up)
	}

	// 5. stop, start, restart
	armtest.Must(cg.Stop(ctx, rg, "web", nil))
	stopped := state(t, cg, "web", "Stopped")
	if *stopped.Properties.Containers[0].Properties.InstanceView.CurrentState.State != "Terminated" {
		t.Fatalf("%+v", stopped.Properties.Containers[0].Properties.InstanceView)
	}
	if code, _ := get(e, "pogi-aci-web.eastus.azurecontainer.localhost", "/"); code == 200 {
		t.Fatal("a stopped group answers")
	}
	armtest.Must(cg.BeginStart(ctx, rg, "web", nil)).PollUntilDone(ctx, armtest.Fast)
	state(t, cg, "web", "Running")
	answers(t, e, "pogi-aci-web.eastus.azurecontainer.localhost", "/", "Welcome to nginx")
	armtest.Must(cg.BeginRestart(ctx, rg, "web", nil)).PollUntilDone(ctx, armtest.Fast)
	state(t, cg, "web", "Running")
	answers(t, e, "pogi-aci-web.eastus.azurecontainer.localhost", "/", "Welcome to nginx")

	// 6. listing and deleting; the containers are gone with the group
	n := 0
	for pg := cg.NewListByResourceGroupPager(rg, nil); pg.More(); {
		n += len(armtest.Must(pg.NextPage(ctx)).Value)
	}
	for pg := cg.NewListPager(nil); pg.More(); {
		n += len(armtest.Must(pg.NextPage(ctx)).Value)
	}
	if n != 2 {
		t.Fatalf("groups listed %d times", n)
	}
	armtest.Must(cg.BeginDelete(ctx, rg, "web", nil)).PollUntilDone(ctx, armtest.Fast)
	_, err = cg.Get(ctx, rg, "web", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	left := armtest.Must(e.Srv.Hub.Docker().List(ctx, "pogi.service=aci", "pogi.resource=web"))
	if len(left) != 0 {
		t.Fatalf("containers left: %v", left)
	}
}

func TestRestartPolicy(t *testing.T) {
	e, cg := start(t)
	_ = e
	run := func(name, restart string, code string) armcontainerinstance.ContainerGroup {
		t.Helper()
		body := group(armcontainerinstance.ContainerGroupRestartPolicy(restart), container("job", "alpine:3", "sh", "-c", "echo ran; exit "+code))
		armtest.Must(armtest.Must(cg.BeginCreateOrUpdate(ctx, rg, name, body, nil)).PollUntilDone(ctx, armtest.Fast))
		t.Cleanup(func() { cg.BeginDelete(ctx, rg, name, nil) })
		return body
	}
	// Never: it ends Succeeded or Failed by its exit code
	run("ok", "Never", "0")
	run("bad", "Never", "3")
	okG, badG := state(t, cg, "ok", "Succeeded"), state(t, cg, "bad", "Failed")
	if cs := okG.Properties.Containers[0].Properties.InstanceView.CurrentState; *cs.State != "Terminated" || *cs.ExitCode != 0 || *cs.DetailStatus != "Completed" {
		t.Fatalf("%+v", cs)
	}
	if cs := badG.Properties.Containers[0].Properties.InstanceView.CurrentState; *cs.ExitCode != 3 || *cs.DetailStatus != "Error" {
		t.Fatalf("%+v", cs)
	}
	// OnFailure runs a failing container again, and leaves a successful one alone
	run("retry", "OnFailure", "1")
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		g := armtest.Must(cg.Get(ctx, rg, "retry", nil))
		if *g.Properties.Containers[0].Properties.InstanceView.RestartCount >= 1 {
			break
		}
		if time.Now().After(end) {
			t.Fatal("a failing container with OnFailure was not restarted")
		}
	}
	run("once", "OnFailure", "0")
	state(t, cg, "once", "Succeeded")
	if n := *armtest.Must(cg.Get(ctx, rg, "once", nil)).Properties.Containers[0].Properties.InstanceView.RestartCount; n != 0 {
		t.Fatalf("restarts = %d", n)
	}
}

func TestErrors(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azaci")
	cg := armtest.Must(armcontainerinstance.NewContainerGroupsClient(sub, e.Cred, e.Opts))
	invalid := func(name string, body armcontainerinstance.ContainerGroup, code string) {
		t.Helper()
		_, err := cg.BeginCreateOrUpdate(ctx, rg, name, body, nil)
		armtest.WantErr(t, err, 400, code)
	}
	invalid("empty", group(armcontainerinstance.ContainerGroupRestartPolicyAlways), "InvalidContainerGroup")
	invalid("twice", group(armcontainerinstance.ContainerGroupRestartPolicyAlways, container("a", "alpine:3"), container("a", "alpine:3")), "InvalidContainerGroup")
	invalid("policy", group("Sometimes", container("a", "alpine:3")), "InvalidContainerGroup")
	_, err := cg.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	if _, err = cg.Stop(ctx, rg, "missing", nil); err == nil {
		t.Fatal("stopping a group that does not exist worked")
	}
	if e.Srv.Hub.Docker().Available() { // an image nobody can pull is the group's failure, not the service's
		invalid("noimage", group(armcontainerinstance.ContainerGroupRestartPolicyAlways, container("a", "pogi-test-no-such-registry.invalid/x:1")), "InaccessibleImage")
		if _, err := cg.Get(ctx, rg, "noimage", nil); err == nil {
			t.Fatal("a group that did not start exists")
		}
	}
}
