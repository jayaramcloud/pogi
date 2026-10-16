package containerapps_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v3"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

var (
	ctx = armtest.Ctx
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
)

type env struct {
	*armtest.Env
	envs   *armappcontainers.ManagedEnvironmentsClient
	apps   *armappcontainers.ContainerAppsClient
	revs   *armappcontainers.ContainerAppsRevisionsClient
	jobs   *armappcontainers.JobsClient
	execs  *armappcontainers.JobsExecutionsClient
	envID  string
	domain string
	docker bool
}

// start runs pogi with Container Apps and makes an environment, "pogienv".
func start(t *testing.T) *env {
	t.Helper()
	e := &env{Env: armtest.Start(t, pogi.Config{}, "azcontainerapps")}
	e.envs = armtest.Must(armappcontainers.NewManagedEnvironmentsClient(sub, e.Cred, e.Opts))
	e.apps = armtest.Must(armappcontainers.NewContainerAppsClient(sub, e.Cred, e.Opts))
	e.revs = armtest.Must(armappcontainers.NewContainerAppsRevisionsClient(sub, e.Cred, e.Opts))
	e.jobs = armtest.Must(armappcontainers.NewJobsClient(sub, e.Cred, e.Opts))
	e.execs = armtest.Must(armappcontainers.NewJobsExecutionsClient(sub, e.Cred, e.Opts))
	made := armtest.Must(armtest.Must(e.envs.BeginCreateOrUpdate(ctx, rg, "pogienv", armappcontainers.ManagedEnvironment{Location: to.Ptr("eastus"),
		Tags: map[string]*string{"env": to.Ptr("test")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	e.envID, e.domain, e.docker = *made.ID, *made.Properties.DefaultDomain, e.Srv.Hub.Docker().Available()
	return e
}

func (e *env) needDocker(t *testing.T) {
	t.Helper()
	if !e.docker {
		t.Skip("no container engine")
	}
}

// get requests path of host through pogi's port (no DNS needed).
func (e *env) get(host, path string) (int, string) {
	addr := strings.TrimPrefix(e.Srv.URL, "http://")
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, n, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, n, addr)
	}}, Timeout: 30 * time.Second}
	resp, err := c.Get("http://" + host + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// serve returns nginx's container: index is written to the page it serves.
func serve(index string) *armappcontainers.Container {
	c := &armappcontainers.Container{Name: to.Ptr("main"), Image: to.Ptr("nginx:alpine"),
		Resources: &armappcontainers.ContainerResources{CPU: to.Ptr(0.25), Memory: to.Ptr("0.5Gi")}}
	if index != "" {
		c.Command = []*string{to.Ptr("/bin/sh"), to.Ptr("-c")}
		c.Args = []*string{to.Ptr("echo " + index + " > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'")}
	}
	return c
}

func (e *env) app(mode armappcontainers.ActiveRevisionsMode, suffix string, c *armappcontainers.Container, traffic ...*armappcontainers.TrafficWeight) armappcontainers.ContainerApp {
	return armappcontainers.ContainerApp{Location: to.Ptr("eastus"), Properties: &armappcontainers.ContainerAppProperties{EnvironmentID: to.Ptr(e.envID),
		Configuration: &armappcontainers.Configuration{ActiveRevisionsMode: to.Ptr(mode),
			Ingress: &armappcontainers.Ingress{External: to.Ptr(true), TargetPort: to.Ptr[int32](80), Traffic: traffic},
			Secrets: []*armappcontainers.Secret{{Name: to.Ptr("token"), Value: to.Ptr("s3cret")}}},
		Template: &armappcontainers.Template{RevisionSuffix: to.Ptr(suffix), Containers: []*armappcontainers.Container{c},
			Scale: &armappcontainers.Scale{MinReplicas: to.Ptr[int32](0), MaxReplicas: to.Ptr[int32](3)}}}}
}

// answers waits for the host to answer path with a body that has want.
func (e *env) answers(t *testing.T, host, want string) {
	t.Helper()
	var code int
	var body string
	for end := time.Now().Add(60 * time.Second); time.Now().Before(end); time.Sleep(300 * time.Millisecond) {
		if code, body = e.get(host, "/"); code == 200 && strings.Contains(body, want) {
			return
		}
	}
	t.Fatalf("%s = %d %q, want %q", host, code, body, want)
}

func TestEnvironment(t *testing.T) {
	e := start(t)
	got := armtest.Must(e.envs.Get(ctx, rg, "pogienv", nil))
	if *got.Properties.ProvisioningState != armappcontainers.EnvironmentProvisioningStateSucceeded || *got.Tags["env"] != "test" ||
		!strings.HasPrefix(*got.Properties.DefaultDomain, "pogienv.azurecontainerapps.localhost:") || *got.Properties.StaticIP != "127.0.0.1" {
		t.Fatalf("%+v", got)
	}
	n := 0
	for pg := e.envs.NewListByResourceGroupPager(rg, nil); pg.More(); {
		n += len(armtest.Must(pg.NextPage(ctx)).Value)
	}
	for pg := e.envs.NewListBySubscriptionPager(nil); pg.More(); {
		n += len(armtest.Must(pg.NextPage(ctx)).Value)
	}
	if n != 2 {
		t.Fatalf("environments listed %d times", n)
	}
	// an app needs its environment; an environment in use cannot be deleted
	bad := e.app("Single", "", serve(""))
	bad.Properties.EnvironmentID = to.Ptr(strings.Replace(e.envID, "pogienv", "ghost", 1))
	_, err := e.apps.BeginCreateOrUpdate(ctx, rg, "orphan", bad, nil)
	armtest.WantErr(t, err, 400, "ManagedEnvironmentNotFound")
	noContainer := e.app("Single", "", serve(""))
	noContainer.Properties.Template.Containers = nil
	_, err = e.apps.BeginCreateOrUpdate(ctx, rg, "empty", noContainer, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "tenant", e.app("Single", "", serve("")), nil)).PollUntilDone(ctx, armtest.Fast))
	_, err = e.envs.BeginDelete(ctx, rg, "pogienv", nil)
	armtest.WantErr(t, err, 400, "ManagedEnvironmentHasContainerApps")
	armtest.Must(e.apps.BeginDelete(ctx, rg, "tenant", nil)).PollUntilDone(ctx, armtest.Fast)
	armtest.Must(e.envs.BeginDelete(ctx, rg, "pogienv", nil)).PollUntilDone(ctx, armtest.Fast)
	_, err = e.envs.Get(ctx, rg, "pogienv", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestContainerApp(t *testing.T) {
	e := start(t)
	e.needDocker(t)

	// 1. create: ingress with a fqdn, a first revision, secrets by name only
	created := armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "web", e.app("Single", "one", serve("")), nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.apps.BeginDelete(ctx, rg, "web", nil) })
	p := created.Properties
	fqdn := *p.Configuration.Ingress.Fqdn
	if *p.ProvisioningState != armappcontainers.ContainerAppProvisioningStateSucceeded || *p.LatestRevisionName != "web--one" || !strings.HasPrefix(fqdn, "web.pogienv.azurecontainerapps.localhost:") ||
		*p.Configuration.Secrets[0].Name != "token" || p.Configuration.Secrets[0].Value != nil || *p.Configuration.ActiveRevisionsMode != "Single" {
		t.Fatalf("%+v\n%+v", created, p)
	}
	sec := armtest.Must(e.apps.ListSecrets(ctx, rg, "web", nil))
	if len(sec.Value) != 1 || *sec.Value[0].Value != "s3cret" {
		t.Fatalf("secrets %+v", sec)
	}

	// 2. it answers through its ingress: the container starts on the first request
	host := strings.Split(fqdn, ":")[0]
	e.answers(t, host, "Welcome to nginx")
	if code, body := e.get("localhost", "/_azure/containerapps/pogienv/web/"); code != 200 || !strings.Contains(body, "Welcome to nginx") {
		t.Fatalf("path form = %d %q", code, body)
	}
	if code, _ := e.get("ghost.pogienv.azurecontainerapps.localhost", "/"); code != 404 {
		t.Fatalf("an unknown app answers %d", code)
	}

	// 3. Single mode: a new template is a new revision with all the traffic, and the old one is inactive
	up := e.app("Single", "two", serve("two"))
	armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "web", up, nil)).PollUntilDone(ctx, armtest.Fast))
	e.answers(t, host, "two")
	revs := map[string]*armappcontainers.Revision{}
	for pg := e.revs.NewListRevisionsPager(rg, "web", nil); pg.More(); {
		for _, r := range armtest.Must(pg.NextPage(ctx)).Value {
			revs[*r.Name] = r
		}
	}
	if len(revs) != 2 || *revs["web--one"].Properties.Active || !*revs["web--two"].Properties.Active || *revs["web--two"].Properties.TrafficWeight != 100 {
		t.Fatalf("revisions %+v %+v", revs["web--one"].Properties, revs["web--two"].Properties)
	}
	// the same template again makes no revision
	armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "web", up, nil)).PollUntilDone(ctx, armtest.Fast))
	n := 0
	for pg := e.revs.NewListRevisionsPager(rg, "web", nil); pg.More(); {
		n += len(armtest.Must(pg.NextPage(ctx)).Value)
	}
	if n != 2 {
		t.Fatalf("%d revisions after an update that changed nothing", n)
	}

	// 4. Multiple mode: two revisions split traffic by weight
	armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "web", e.app("Multiple", "two", serve("two"), &armappcontainers.TrafficWeight{RevisionName: to.Ptr("web--two"), Weight: to.Ptr[int32](50)},
		&armappcontainers.TrafficWeight{RevisionName: to.Ptr("web--three"), Weight: to.Ptr[int32](50)}), nil)).PollUntilDone(ctx, armtest.Fast))
	armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "web", e.app("Multiple", "three", serve("three"), &armappcontainers.TrafficWeight{RevisionName: to.Ptr("web--two"), Weight: to.Ptr[int32](50)},
		&armappcontainers.TrafficWeight{LatestRevision: to.Ptr(true), Weight: to.Ptr[int32](50)}), nil)).PollUntilDone(ctx, armtest.Fast))
	r2, r3 := armtest.Must(e.revs.GetRevision(ctx, rg, "web", "web--two", nil)), armtest.Must(e.revs.GetRevision(ctx, rg, "web", "web--three", nil))
	if *r2.Properties.TrafficWeight != 50 || *r3.Properties.TrafficWeight != 50 || !*r2.Properties.Active || !*r3.Properties.Active {
		t.Fatalf("%+v %+v", r2.Properties, r3.Properties)
	}
	seen := map[string]int{}
	e.answers(t, host, "two")
	e.answers(t, host, "three")
	for i := 0; i < 60; i++ {
		if _, body := e.get(host, "/"); body == "two" || body == "three" {
			seen[body]++
		}
	}
	if seen["two"] < 5 || seen["three"] < 5 || seen["two"]+seen["three"] != 60 {
		t.Fatalf("a 50/50 split answered %v", seen)
	}
	// a revision has its own address
	for i := 0; i < 5; i++ {
		if _, body := e.get("web--two."+strings.SplitN(host, ".", 2)[1], "/"); body != "two" {
			t.Fatalf("revision two's own address answered %q", body)
		}
	}

	// 5. deactivating a revision takes it out; everything goes to the other; activating brings it back
	armtest.Must(e.revs.DeactivateRevision(ctx, rg, "web", "web--two", nil))
	for i := 0; i < 10; i++ {
		if _, body := e.get(host, "/"); body != "three" {
			t.Fatalf("after deactivating two, %q answered", body)
		}
	}
	armtest.Must(e.revs.ActivateRevision(ctx, rg, "web", "web--two", nil))
	armtest.Must(e.revs.RestartRevision(ctx, rg, "web", "web--three", nil))
	e.answers(t, host, "three")

	// 6. stop and start the app
	armtest.Must(armtest.Must(e.apps.BeginStop(ctx, rg, "web", nil)).PollUntilDone(ctx, armtest.Fast))
	if code, _ := e.get(host, "/"); code != 404 {
		t.Fatalf("a stopped app answers %d", code)
	}
	if got := armtest.Must(e.apps.Get(ctx, rg, "web", nil)); *got.Properties.RunningStatus != armappcontainers.ContainerAppRunningStatusStopped {
		t.Fatalf("%+v", got.Properties.RunningStatus)
	}
	armtest.Must(armtest.Must(e.apps.BeginStart(ctx, rg, "web", nil)).PollUntilDone(ctx, armtest.Fast))
	e.answers(t, host, "three")

	// 7. delete takes the revisions and the containers
	armtest.Must(e.apps.BeginDelete(ctx, rg, "web", nil)).PollUntilDone(ctx, armtest.Fast)
	_, err := e.apps.Get(ctx, rg, "web", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	if left := armtest.Must(e.Srv.Hub.Docker().List(ctx, "pogi.service=containerapps", "pogi.resource=web--three")); len(left) != 0 {
		t.Fatalf("containers left: %v", left)
	}
}

func TestSecretsAndEnv(t *testing.T) {
	e := start(t)
	e.needDocker(t)
	c := &armappcontainers.Container{Name: to.Ptr("main"), Image: to.Ptr("nginx:alpine"),
		Command: []*string{to.Ptr("/bin/sh"), to.Ptr("-c")},
		Args:    []*string{to.Ptr("echo \"$GREETING $TOKEN $CONTAINER_APP_NAME\" > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'")},
		Env:     []*armappcontainers.EnvironmentVar{{Name: to.Ptr("GREETING"), Value: to.Ptr("hello")}, {Name: to.Ptr("TOKEN"), SecretRef: to.Ptr("token")}}}
	body := e.app("Single", "", c)
	armtest.Must(armtest.Must(e.apps.BeginCreateOrUpdate(ctx, rg, "envapp", body, nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.apps.BeginDelete(ctx, rg, "envapp", nil) })
	e.answers(t, "envapp.pogienv.azurecontainerapps.localhost:"+e.Srv.URL[strings.LastIndexByte(e.Srv.URL, ':')+1:], "hello s3cret envapp")
	// an update that leaves the secret value out (a PATCH is a GET merged with changes) keeps it
	armtest.Must(armtest.Must(e.apps.BeginUpdate(ctx, rg, "envapp", armappcontainers.ContainerApp{Tags: map[string]*string{"a": to.Ptr("b")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	if sec := armtest.Must(e.apps.ListSecrets(ctx, rg, "envapp", nil)); *sec.Value[0].Value != "s3cret" {
		t.Fatalf("%+v", sec)
	}
	if n := len(armtest.Must(e.revs.GetRevision(ctx, rg, "envapp", *armtest.Must(e.apps.Get(ctx, rg, "envapp", nil)).Properties.LatestRevisionName, nil)).Properties.Template.Containers); n != 1 {
		t.Fatal("revision lost its container")
	}
}

func job(e *env, image string, retries int32, count int32, args string) armappcontainers.Job {
	return armappcontainers.Job{Location: to.Ptr("eastus"), Properties: &armappcontainers.JobProperties{EnvironmentID: to.Ptr(e.envID),
		Configuration: &armappcontainers.JobConfiguration{TriggerType: to.Ptr(armappcontainers.TriggerTypeManual), ReplicaTimeout: to.Ptr[int32](60), ReplicaRetryLimit: to.Ptr(retries),
			ManualTriggerConfig: &armappcontainers.JobConfigurationManualTriggerConfig{Parallelism: to.Ptr[int32](count), ReplicaCompletionCount: to.Ptr(count)}},
		Template: &armappcontainers.JobTemplate{Containers: []*armappcontainers.Container{{Name: to.Ptr("job"), Image: to.Ptr(image),
			Command: []*string{to.Ptr("sh"), to.Ptr("-c")}, Args: []*string{to.Ptr(args)}}}}}}
}

// execution waits until the job's only execution is in status.
func execution(t *testing.T, e *env, name string, status armappcontainers.JobExecutionRunningState) *armappcontainers.JobExecution {
	t.Helper()
	var last []*armappcontainers.JobExecution
	for end := time.Now().Add(60 * time.Second); time.Now().Before(end); time.Sleep(300 * time.Millisecond) {
		last = nil
		for pg := e.execs.NewListPager(rg, name, nil); pg.More(); {
			last = append(last, armtest.Must(pg.NextPage(ctx)).Value...)
		}
		if len(last) == 1 && *last[0].Properties.Status == status {
			return last[0]
		}
	}
	for _, x := range last {
		t.Logf("%s: %v", *x.Name, *x.Properties.Status)
	}
	t.Fatalf("executions of %s: %d, want one %s", name, len(last), status)
	return nil
}

func TestJobs(t *testing.T) {
	e := start(t)
	e.needDocker(t)

	// 1. create; a manual job runs when started, and its execution ends Succeeded
	made := armtest.Must(armtest.Must(e.jobs.BeginCreateOrUpdate(ctx, rg, "ok", job(e, "alpine:3", 0, 1, "echo working; exit 0"), nil)).PollUntilDone(ctx, armtest.Fast))
	if *made.Properties.ProvisioningState != armappcontainers.JobProvisioningStateSucceeded || *made.Properties.Configuration.TriggerType != armappcontainers.TriggerTypeManual {
		t.Fatalf("%+v", made.Properties)
	}
	t.Cleanup(func() { e.jobs.BeginDelete(ctx, rg, "ok", nil) })
	if n := len(armtest.Must(e.execs.NewListPager(rg, "ok", nil).NextPage(ctx)).Value); n != 0 {
		t.Fatalf("%d executions before a start", n)
	}
	started := armtest.Must(armtest.Must(e.jobs.BeginStart(ctx, rg, "ok", nil)).PollUntilDone(ctx, armtest.Fast))
	if !strings.HasPrefix(*started.Name, "ok-") || !strings.HasSuffix(*started.ID, "/jobs/ok/executions/"+*started.Name) {
		t.Fatalf("%+v", started)
	}
	ex := execution(t, e, "ok", armappcontainers.JobExecutionRunningStateSucceeded)
	if *ex.Name != *started.Name || ex.Properties.StartTime == nil || ex.Properties.EndTime == nil || *ex.Properties.Template.Containers[0].Image != "alpine:3" {
		t.Fatalf("%+v", ex.Properties)
	}

	// 2. a failing job ends Failed, after its retries
	armtest.Must(armtest.Must(e.jobs.BeginCreateOrUpdate(ctx, rg, "bad", job(e, "alpine:3", 1, 1, "echo nope; exit 7"), nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.jobs.BeginDelete(ctx, rg, "bad", nil) })
	armtest.Must(armtest.Must(e.jobs.BeginStart(ctx, rg, "bad", nil)).PollUntilDone(ctx, armtest.Fast))
	execution(t, e, "bad", armappcontainers.JobExecutionRunningStateFailed)

	// 3. several replicas, and the job's template replaced for one run
	armtest.Must(armtest.Must(e.jobs.BeginCreateOrUpdate(ctx, rg, "many", job(e, "alpine:3", 0, 3, "sleep 1"), nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.jobs.BeginDelete(ctx, rg, "many", nil) })
	armtest.Must(armtest.Must(e.jobs.BeginStart(ctx, rg, "many", &armappcontainers.JobsClientBeginStartOptions{Template: &armappcontainers.JobExecutionTemplate{
		Containers: []*armappcontainers.JobExecutionContainer{{Name: to.Ptr("job"), Image: to.Ptr("alpine:3"), Command: []*string{to.Ptr("sh"), to.Ptr("-c")}, Args: []*string{to.Ptr("exit 4")}}}}})).PollUntilDone(ctx, armtest.Fast))
	execution(t, e, "many", armappcontainers.JobExecutionRunningStateFailed)

	// 4. errors: a job needs a timeout and a known trigger; starting a job that is not there
	noTimeout := job(e, "alpine:3", 0, 1, "true")
	noTimeout.Properties.Configuration.ReplicaTimeout = nil
	_, err := e.jobs.BeginCreateOrUpdate(ctx, rg, "bad2", noTimeout, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = e.jobs.BeginStart(ctx, rg, "ghost", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// 5. delete takes the executions
	armtest.Must(e.jobs.BeginDelete(ctx, rg, "ok", nil)).PollUntilDone(ctx, armtest.Fast)
	_, err = e.jobs.Get(ctx, rg, "ok", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestJobStop(t *testing.T) {
	e := start(t)
	e.needDocker(t)
	armtest.Must(armtest.Must(e.jobs.BeginCreateOrUpdate(ctx, rg, "slow", job(e, "alpine:3", 0, 1, "sleep 300"), nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.jobs.BeginDelete(ctx, rg, "slow", nil) })
	armtest.Must(armtest.Must(e.jobs.BeginStart(ctx, rg, "slow", nil)).PollUntilDone(ctx, armtest.Fast))
	ex := execution(t, e, "slow", armappcontainers.JobExecutionRunningStateRunning)
	armtest.Must(e.jobs.BeginStopExecution(ctx, rg, "slow", *ex.Name, nil))
	execution(t, e, "slow", armappcontainers.JobExecutionRunningStateStopped)
}
