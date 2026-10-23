package appservice_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

var (
	ctx = armtest.Ctx
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
)

func must[T any](v T, err error) T { return armtest.Must(v, err) }

type env struct {
	*armtest.Env
	plans  *armappservice.PlansClient
	apps   *armappservice.WebAppsClient
	docker bool
}

// start runs pogi with App Service (and Functions, which owns the Microsoft.Web provider).
func start(t *testing.T, cfg ...pogi.Config) *env {
	t.Helper()
	var c pogi.Config
	if len(cfg) > 0 {
		c = cfg[0]
	}
	e := &env{Env: armtest.Start(t, c, "azappservice", "azfunctions")}
	e.plans = must(armappservice.NewPlansClient(sub, e.Cred, e.Opts))
	e.apps = must(armappservice.NewWebAppsClient(sub, e.Cred, e.Opts))
	e.docker = e.Srv.Hub.Docker().Available()
	return e
}

func (e *env) needDocker(t *testing.T) {
	t.Helper()
	if !e.docker {
		t.Skip("no container engine")
	}
}

// plan makes a plan; a Linux one is reserved.
func (e *env) plan(t *testing.T, name string, linux bool) string {
	t.Helper()
	p := must(must(e.plans.BeginCreateOrUpdate(ctx, rg, name, armappservice.Plan{Location: to.Ptr("eastus"),
		SKU: &armappservice.SKUDescription{Name: to.Ptr("B1")}, Properties: &armappservice.PlanProperties{Reserved: to.Ptr(linux)}}, nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.plans.Delete(ctx, rg, name, nil) })
	return *p.ID
}

// site is a web app of the plan with a runtime (linuxFxVersion) and settings.
func site(plan, kind, fx string, settings map[string]string) armappservice.Site {
	cfg := &armappservice.SiteConfig{LinuxFxVersion: to.Ptr(fx)}
	for k, v := range settings {
		cfg.AppSettings = append(cfg.AppSettings, &armappservice.NameValuePair{Name: to.Ptr(k), Value: to.Ptr(v)})
	}
	return armappservice.Site{Location: to.Ptr("eastus"), Kind: to.Ptr(kind), Properties: &armappservice.SiteProperties{ServerFarmID: to.Ptr(plan), SiteConfig: cfg}}
}

func (e *env) create(t *testing.T, name string, s armappservice.Site) armappservice.WebAppsClientCreateOrUpdateResponse {
	t.Helper()
	r := must(must(e.apps.BeginCreateOrUpdate(ctx, rg, name, s, nil)).PollUntilDone(ctx, armtest.Fast))
	t.Cleanup(func() { e.apps.Delete(ctx, rg, name, nil) })
	return r
}

// request calls host through pogi's port (no DNS needed).
func (e *env) request(method, host, path string, body []byte) (int, string) {
	addr := strings.TrimPrefix(e.Srv.URL, "http://")
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, n, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, n, addr)
	}}, Timeout: 2 * time.Minute}
	req, _ := http.NewRequest(method, "http://"+host+path, bytes.NewReader(body))
	resp, err := c.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func (e *env) get(host, path string) (int, string) { return e.request("GET", host, path, nil) }

// answers waits for the host to answer path with a body that has want.
func (e *env) answers(t *testing.T, host, path, want string) {
	t.Helper()
	var code int
	var body string
	for end := time.Now().Add(2 * time.Minute); time.Now().Before(end); time.Sleep(300 * time.Millisecond) {
		if code, body = e.get(host, path); code == 200 && strings.Contains(body, want) {
			return
		}
	}
	t.Fatalf("%s%s = %d %q, want %q", host, path, code, body, want)
}

// running lists the containers pogi runs for an app.
func (e *env) running(app string) int {
	l, _ := e.Srv.Hub.Docker().List(ctx, "pogi.service=azappservice", "pogi.resource="+app)
	return len(l)
}

func hostOf(s armappservice.WebAppsClientCreateOrUpdateResponse) string {
	return strings.Split(*s.Properties.DefaultHostName, ":")[0]
}

func appSettings(t *testing.T, e *env, name string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for k, v := range must(e.apps.ListApplicationSettings(ctx, rg, name, nil)).Properties {
		got[k] = *v
	}
	return got
}

// ---- a container image ----

func TestContainerImage(t *testing.T) {
	e := start(t)
	e.needDocker(t)
	plan := e.plan(t, "imgplan", true)
	app := e.create(t, "imgapp", site(plan, "app,linux,container", "DOCKER|nginx:alpine", map[string]string{"WEBSITES_PORT": "80", "GREETING": "hello"}))

	p := app.Properties
	if *app.Name != "imgapp" || *app.Type != "Microsoft.Web/sites" || *p.State != "Running" || *p.Reserved != true ||
		!strings.HasPrefix(*p.DefaultHostName, "imgapp.azurewebsites.localhost:") || *p.AvailabilityState != armappservice.SiteAvailabilityStateNormal ||
		*p.ServerFarmID != plan || len(p.HostNames) != 1 {
		t.Fatalf("site: %+v", p)
	}
	if p.SiteConfig != nil {
		t.Fatalf("a GET of a site has no siteConfig: %+v", p.SiteConfig)
	}
	cfg := must(e.apps.GetConfiguration(ctx, rg, "imgapp", nil))
	if *cfg.Properties.LinuxFxVersion != "DOCKER|nginx:alpine" || *cfg.Properties.NumberOfWorkers != 1 {
		t.Fatalf("config: %+v", cfg.Properties)
	}
	if got := appSettings(t, e, "imgapp"); len(got) != 2 || got["GREETING"] != "hello" {
		t.Fatalf("settings: %v", got)
	}
	creds := must(must(e.apps.BeginListPublishingCredentials(ctx, rg, "imgapp", nil)).PollUntilDone(ctx, armtest.Fast))
	if *creds.Properties.PublishingUserName != "$imgapp" || *creds.Properties.PublishingPassword == "" {
		t.Fatalf("credentials: %+v", creds.Properties)
	}

	// it answers at its host name and on pogi's own address
	host := hostOf(app)
	e.answers(t, host, "/", "Welcome to nginx")
	if code, body := e.get("localhost", "/_azure/appservice/imgapp/"); code != 200 || !strings.Contains(body, "Welcome to nginx") {
		t.Fatalf("path form = %d %q", code, body)
	}
	if code, _ := e.get("ghost.azurewebsites.localhost", "/"); code != 404 {
		t.Fatalf("an unknown app answers %d", code)
	}
	n := 0
	for pg := e.apps.NewListByResourceGroupPager(rg, nil); pg.More(); {
		for _, s := range must(pg.NextPage(ctx)).Value {
			if *s.Name == "imgapp" {
				n++
			}
		}
	}
	if n != 1 {
		t.Fatalf("listed %d times", n)
	}

	// stop: the container goes and the host says so; start: it comes back
	must(e.apps.Stop(ctx, rg, "imgapp", nil))
	if s := must(e.apps.Get(ctx, rg, "imgapp", nil)); *s.Properties.State != "Stopped" {
		t.Fatalf("state after stop: %v", *s.Properties.State)
	}
	if code, body := e.get(host, "/"); code != 403 || !strings.Contains(body, "stopped") {
		t.Fatalf("stopped app = %d %q", code, body)
	}
	if n := e.running("imgapp"); n != 0 {
		t.Fatalf("%d containers after stop", n)
	}
	must(e.apps.Start(ctx, rg, "imgapp", nil))
	e.answers(t, host, "/", "Welcome to nginx")
	must(e.apps.Restart(ctx, rg, "imgapp", nil))
	e.answers(t, host, "/", "Welcome to nginx")

	// delete: no container, no host
	must(e.apps.Delete(ctx, rg, "imgapp", nil))
	if n := e.running("imgapp"); n != 0 {
		t.Fatalf("%d containers after delete", n)
	}
	if code, _ := e.get(host, "/"); code != 404 {
		t.Fatalf("deleted app answers %d", code)
	}
	_, err := e.apps.Get(ctx, rg, "imgapp", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

// ---- zip-deployed code ----

const server = `import os, http.server

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = ("%s on %s" % (os.environ.get("GREETING", "none"), os.environ["PORT"])).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

http.server.HTTPServer(("", int(os.environ["PORT"])), H).serve_forever()
`

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	for name, body := range files {
		io.WriteString(must(zw.Create(name)), body)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestZipDeployPython(t *testing.T) {
	e := start(t)
	e.needDocker(t)
	plan := e.plan(t, "zipplan", true)
	app := e.create(t, "zipapp", site(plan, "app,linux", "PYTHON|3.12", map[string]string{"GREETING": "one"}))
	host := hostOf(app)

	// no code yet: the app says what is missing
	if code, body := e.get(host, "/"); code != 503 || !strings.Contains(body, "no code") {
		t.Fatalf("before the deploy = %d %q", code, body)
	}
	// a bad zip is refused, and nothing changes
	scm := strings.Replace(host, ".azurewebsites", ".scm.azurewebsites", 1)
	if code, body := e.request("POST", scm, "/api/zipdeploy", []byte("not a zip")); code != 400 || !strings.Contains(body, "not a zip") {
		t.Fatalf("bad zip = %d %q", code, body)
	}
	if code, body := e.request("POST", scm, "/api/zipdeploy", zipOf(t, map[string]string{"../evil.py": "x"})); code != 400 {
		t.Fatalf("zip with an entry outside the app = %d %q", code, body)
	}

	if code, body := e.request("POST", scm, "/api/zipdeploy", zipOf(t, map[string]string{"app.py": server})); code != 200 {
		t.Fatalf("zip deploy = %d %q", code, body)
	}
	e.answers(t, host, "/", "one on 8080")
	if code, body := e.get(scm, "/api/deployments/latest"); code != 200 || !strings.Contains(body, `"complete":true`) {
		t.Fatalf("deployment status = %d %q", code, body)
	}

	// changing an app setting restarts the app with it
	must(e.apps.UpdateApplicationSettings(ctx, rg, "zipapp", armappservice.StringDictionary{Properties: map[string]*string{"GREETING": to.Ptr("two"), "WEBSITES_PORT": to.Ptr("9000")}}, nil))
	e.answers(t, host, "/", "two on 9000")

	// a new zip replaces the code
	other := strings.Replace(server, `"%s on %s" % (os.environ.get("GREETING", "none"), os.environ["PORT"])`, `"v2 " + os.environ["GREETING"]`, 1)
	e.request("POST", scm, "/api/zipdeploy", zipOf(t, map[string]string{"app.py": other}))
	e.answers(t, host, "/", "v2 two")

	must(e.apps.Delete(ctx, rg, "zipapp", nil))
	if n := e.running("zipapp"); n != 0 {
		t.Fatalf("%d containers after delete", n)
	}
}

// ---- failures ----

func TestResourceErrors(t *testing.T) {
	e := start(t, pogi.Config{DockerHost: "/nonexistent/docker.sock"})
	plan := e.plan(t, "errplan", true)

	// a plan that is not there, none at all, a bad name
	_, err := e.apps.BeginCreateOrUpdate(ctx, rg, "orphan", site(plan+"x", "app,linux", "DOCKER|nginx", nil), nil)
	armtest.WantErr(t, err, 400, "BadRequest")
	nop := site("", "app,linux", "DOCKER|nginx", nil)
	nop.Properties.ServerFarmID = nil
	_, err = e.apps.BeginCreateOrUpdate(ctx, rg, "noplan", nop, nil)
	armtest.WantErr(t, err, 400, "BadRequest")
	for _, name := range []string{"-lead", "trail-", "a", "under_score", strings.Repeat("x", 61)} {
		_, err = e.apps.BeginCreateOrUpdate(ctx, rg, name, site(plan, "app,linux", "DOCKER|nginx", nil), nil)
		if err == nil {
			t.Errorf("name %q was accepted", name)
		}
	}

	// get, configure and delete what is not there
	_, err = e.apps.Get(ctx, rg, "ghost", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = e.apps.GetConfiguration(ctx, rg, "ghost", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = e.apps.UpdateApplicationSettings(ctx, rg, "ghost", armappservice.StringDictionary{Properties: map[string]*string{"A": to.Ptr("b")}}, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = e.apps.Stop(ctx, rg, "ghost", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	if _, err = e.apps.Delete(ctx, rg, "ghost", nil); err != nil {
		t.Fatalf("delete of a missing app: %v", err)
	}

	// a site, its settings, and a backup it never had
	e.create(t, "errapp", site(plan, "app,linux", "DOCKER|nginx", map[string]string{"A": "b"}))
	if got := appSettings(t, e, "errapp"); got["A"] != "b" || len(got) != 1 {
		t.Fatalf("settings %v", got)
	}
	_, err = e.apps.GetBackupConfiguration(ctx, rg, "errapp", nil)
	armtest.WantErr(t, err, 404, "BackupNotFound")
}

func TestWithoutDocker(t *testing.T) {
	e := start(t, pogi.Config{DockerHost: "/nonexistent/docker.sock"})
	plan := e.plan(t, "nodplan", true)

	app := e.create(t, "nodocker", site(plan, "app,linux,container", "DOCKER|nginx:alpine", nil))
	if *app.Properties.State != "Running" {
		t.Fatalf("the resource works without Docker: %+v", app.Properties)
	}
	if code, body := e.get(hostOf(app), "/"); code != 503 || !strings.Contains(body, "no container engine") {
		t.Fatalf("request = %d %q", code, body)
	}
	// stack pogi cannot run
	java := e.create(t, "javaapp", site(plan, "app,linux", "JAVA|17-java17", nil))
	if code, body := e.get(hostOf(java), "/"); code != 503 || !strings.Contains(body, "JAVA") {
		t.Fatalf("unsupported stack = %d %q", code, body)
	}
	// stopped is stopped, with or without Docker
	must(e.apps.Stop(ctx, rg, "nodocker", nil))
	if code, body := e.get(hostOf(app), "/"); code != 403 || !strings.Contains(body, "stopped") {
		t.Fatalf("stopped = %d %q", code, body)
	}
	// a PUT does not start a stopped app
	e.create(t, "nodocker", site(plan, "app,linux,container", "DOCKER|nginx:alpine", nil))
	if s := must(e.apps.Get(ctx, rg, "nodocker", nil)); *s.Properties.State != "Stopped" {
		t.Fatalf("state %v", *s.Properties.State)
	}
}

func TestWindowsApp(t *testing.T) {
	e := start(t)
	plan := e.plan(t, "winplan", false)
	app := e.create(t, "winapp", armappservice.Site{Location: to.Ptr("eastus"), Kind: to.Ptr("app"),
		Properties: &armappservice.SiteProperties{ServerFarmID: to.Ptr(plan), SiteConfig: &armappservice.SiteConfig{NetFrameworkVersion: to.Ptr("v8.0")}}})
	if *app.Properties.Reserved || *app.Properties.State != "Running" {
		t.Fatalf("%+v", app.Properties)
	}
	if cfg := must(e.apps.GetConfiguration(ctx, rg, "winapp", nil)); *cfg.Properties.NetFrameworkVersion != "v8.0" {
		t.Fatalf("config %+v", cfg.Properties)
	}
	if code, body := e.get(hostOf(app), "/"); code != 503 || !strings.Contains(body, "Windows") || !strings.Contains(body, "metadata only") {
		t.Fatalf("windows app = %d %q", code, body)
	}
}

// An image that cannot be pulled: the app reports why, and the request does not hang.
func TestImageNotPulled(t *testing.T) {
	e := start(t)
	e.needDocker(t)
	plan := e.plan(t, "pullplan", true)
	app := e.create(t, "pullapp", site(plan, "app,linux,container", "DOCKER|localhost:1/nothing/here:1", nil))
	start := time.Now()
	code, body := e.get(hostOf(app), "/")
	if code != 503 || !strings.Contains(body, "localhost:1/nothing/here") {
		t.Fatalf("request = %d %q", code, body)
	}
	if d := time.Since(start); d > time.Minute {
		t.Fatalf("took %s to fail", d)
	}
	if n := e.running("pullapp"); n != 0 {
		t.Fatalf("%d containers", n)
	}
	// the console says why
	if info, _ := e.Srv.Hub.Service("azappservice").(core.Inspectable).Inspect("Web apps", "pullapp").(map[string]any); info["error"] == nil {
		t.Fatalf("inspect has no error: %v", info)
	}
}
