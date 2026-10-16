package functions_test

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/functions"
	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
	"go.yaml.in/yaml/v3"
)

// fixture is a hub with the service and a fake Functions host that records what it receives.
type fixture struct {
	t    *testing.T
	hub  *core.Hub
	svc  *functions.Service
	url  string
	mu   sync.Mutex
	seen []*http.Request
	body []string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t}
	f.hub, f.url = core.ServeTest(t, func(h *core.Hub) core.Service { f.svc = functions.New(h); return f.svc })
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.seen, f.body = append(f.seen, r), append(f.body, string(b))
		f.mu.Unlock()
		switch {
		case r.URL.Path == "/api/hello":
			w.Header().Set("X-From", "fake-host")
			w.WriteHeader(http.StatusTeapot)
			io.WriteString(w, "hello "+r.URL.Query().Get("name"))
		case strings.HasPrefix(r.URL.Path, "/admin/functions/"), strings.HasPrefix(r.URL.Path, "/runtime/webhooks/eventgrid"):
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	f.hub.SetEndpoint("fake", strings.TrimPrefix(fake.URL, "http://"))
	return f
}

func (f *fixture) last() (*http.Request, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[len(f.seen)-1], f.body[len(f.body)-1]
}

func (f *fixture) app(name string) {
	f.t.Helper()
	if err := f.svc.CreateApp(name, "python", "", nil); err != nil {
		f.t.Fatal(err)
	}
	addr, _ := f.hub.Endpoint("fake")
	f.svc.Fake(name, addr, "master")
}

func (f *fixture) do(method, path, host string, body io.Reader, hdr ...string) (*http.Response, string) {
	f.t.Helper()
	req, _ := http.NewRequest(method, f.url+path, body)
	if host != "" {
		req.Host = host
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// Both URL forms reach the host, with the caller's key swapped for the host's own.
func TestProxy(t *testing.T) {
	f := newFixture(t)
	f.app("hello")
	for _, c := range []struct{ name, path, host string }{
		{"path form", "/_azure/functions/hello/api/hello?name=x&code=whatever", ""},
		{"host form", "/api/hello?name=x&code=whatever", "hello.azurewebsites.localhost:7644"},
	} {
		t.Run(c.name, func(t *testing.T) {
			resp, body := f.do("GET", c.path, c.host, nil, "x-functions-key", "caller's")
			if resp.StatusCode != http.StatusTeapot || body != "hello x" || resp.Header.Get("X-From") != "fake-host" {
				t.Fatalf("response not passed through: %d %q %v", resp.StatusCode, body, resp.Header)
			}
			r, _ := f.last()
			if r.Header.Get("x-functions-key") != "master" || r.URL.Query().Has("code") || r.URL.Path != "/api/hello" {
				t.Fatalf("host got key %q, url %s", r.Header.Get("x-functions-key"), r.URL)
			}
		})
	}

	resp, body := f.do("GET", "/_azure/functions/nope/api/hello", "", nil)
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(body, "ResourceNotFound") {
		t.Fatalf("unknown app: %d %s", resp.StatusCode, body)
	}
	resp, _ = f.do("POST", "/_azure/functions/hello/admin/functions/tick", "", strings.NewReader(`{"input":"x"}`))
	if r, b := f.last(); resp.StatusCode != http.StatusAccepted || r.URL.Path != "/admin/functions/tick" || b != `{"input":"x"}` {
		t.Fatalf("admin call not passed through: %d %s %s", resp.StatusCode, r.URL, b)
	}
}

func TestZipDeploy(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.CreateApp("zapp", "python", "", nil); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range map[string]string{"function_app.py": "print(1)", "host.json": "{}", "sub/x.txt": "x"} {
		w, _ := zw.Create(name)
		io.WriteString(w, content)
	}
	zw.Close()

	resp, body := f.do("POST", "/api/zipdeploy", "zapp.scm.azurewebsites.localhost", bytes.NewReader(buf.Bytes()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scm zipdeploy: %d %s", resp.StatusCode, body)
	}
	resp, _ = f.do("POST", "/_azure/functions/zapp/api/zipdeploy?isAsync=true", "", bytes.NewReader(buf.Bytes()))
	loc := resp.Header.Get("Location")
	if resp.StatusCode != http.StatusAccepted || !strings.HasSuffix(loc, "/_azure/functions/zapp/api/deployments/latest") {
		t.Fatalf("async zipdeploy: %d %q", resp.StatusCode, loc)
	}
	if resp, body := f.do("GET", "/api/deployments/latest", "zapp.scm.azurewebsites.localhost", nil); resp.StatusCode != 200 || !strings.Contains(body, `"complete":true`) {
		t.Fatalf("deployment status: %d %s", resp.StatusCode, body)
	}
	insp := f.svc.Inspect("Function apps", "zapp").(map[string]any)
	dir := insp["app"].(*functions.App).Path
	if b, err := os.ReadFile(filepath.Join(dir, "sub", "x.txt")); err != nil || string(b) != "x" {
		t.Fatalf("zip not extracted into %s: %v %q", dir, err, b)
	}

	// a zip entry that climbs out of the app is refused
	buf.Reset()
	zw = zip.NewWriter(&buf)
	w, _ := zw.Create("../evil")
	io.WriteString(w, "x")
	zw.Close()
	if resp, _ := f.do("POST", "/api/zipdeploy", "zapp.scm.azurewebsites.localhost", bytes.NewReader(buf.Bytes())); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("zip slip: %d", resp.StatusCode)
	}
	if resp, _ := f.do("POST", "/api/zipdeploy", "zapp.scm.azurewebsites.localhost", strings.NewReader("not a zip")); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("not a zip: %d", resp.StatusCode)
	}

	if err := f.svc.DeleteApp("zapp"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("zip deploy left behind: %v", err)
	}
}

// The Event Grid hook posts the event to the host's webhook for that function.
func TestEventGridDelivery(t *testing.T) {
	f := newFixture(t)
	f.app("grid")
	id := "/subscriptions/s/resourceGroups/g/providers/Microsoft.Web/sites/grid/functions/OnEvent"
	events := `[{"id":"1","eventType":"x","subject":"s","data":{},"dataVersion":"1","metadataVersion":"1","eventTime":"2026-01-01T00:00:00Z","topic":"t"}]`
	if err := f.hub.Deliver(id, &core.Message{Body: events}); err != nil {
		t.Fatal(err)
	}
	r, body := f.last()
	if r.URL.Path != "/runtime/webhooks/eventgrid" || r.URL.Query().Get("functionName") != "OnEvent" ||
		r.Header.Get("aeg-event-type") != "Notification" || r.Header.Get("x-functions-key") != "master" || body != events {
		t.Fatalf("host got %s %v %s", r.URL, r.Header, body)
	}
	if err := f.hub.Deliver("/subscriptions/s/resourceGroups/g/providers/Microsoft.Web/sites/grid", &core.Message{Body: "[]"}); err == nil {
		t.Fatal("a site is not a function")
	}
	if err := f.hub.Deliver("/subscriptions/s/resourceGroups/g/providers/Microsoft.Web/sites/nope/functions/f", &core.Message{Body: "[]"}); err == nil {
		t.Fatal("unknown app")
	}
	if err := f.hub.Deliver(id, &core.Message{Body: `{"specversion":"1.0"}`}); err != nil {
		t.Fatal(err)
	}
	if r, _ := f.last(); r.Header.Get("Content-Type") != "application/cloudevents+json" {
		t.Fatalf("a CloudEvent keeps its schema: %s", r.Header.Get("Content-Type"))
	}
}

// With no Core Tools and no container engine the apps are metadata only.
func TestMetadataOnly(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // no func
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return functions.New(h) })
	hub.Cfg.DockerHost = "unix:///nonexistent/docker.sock"
	svc := hub.Service("azfunctions").(*functions.Service)
	if err := svc.CreateApp("meta", "python", t.TempDir(), map[string]string{"A": "b"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateApp("nocode", "python", "", nil); err != nil {
		t.Fatal(err)
	}
	if resp, _ := http.Get(url + "/_azure/functions/nocode/api/hello"); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("an app without code: %d", resp.StatusCode)
	}
	resp, err := http.Get(url + "/_azure/functions/meta/api/hello")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "metadata only") {
		t.Fatalf("want 503 saying metadata only, got %d %s", resp.StatusCode, b)
	}
	if err := hub.Deliver("/subscriptions/s/resourceGroups/g/providers/Microsoft.Web/sites/meta/functions/f", &core.Message{Body: "[]"}); err == nil {
		t.Fatal("delivery must fail without a host")
	}
}

func TestAppLifecycle(t *testing.T) {
	f := newFixture(t)
	code := t.TempDir()
	for _, bad := range [][3]string{{"", "python", ""}, {"ok", "cobol", ""}, {"ok", "python", filepath.Join(code, "missing")}, {"-x", "python", ""}} {
		if err := f.svc.CreateApp(bad[0], bad[1], bad[2], nil); err == nil {
			t.Fatalf("CreateApp%v succeeded", bad)
		}
	}
	if err := f.svc.CreateApp("one", "python", code, map[string]string{"K": "v"}); err != nil {
		t.Fatal(err)
	}
	if core.AsError(f.svc.CreateApp("ONE", "python", "", nil)).Status != http.StatusConflict {
		t.Fatal("names are case-insensitive")
	}
	if err := f.svc.UpdateApp("one", "node", "", map[string]string{"K": "w"}); err != nil {
		t.Fatal(err)
	}
	a := f.svc.Inspect("Function apps", "one").(map[string]any)["app"].(*functions.App)
	if a.Runtime != "node" || a.Settings["K"] != "w" || a.Path != code {
		t.Fatalf("update: %+v", a)
	}
	if core.AsError(f.svc.UpdateApp("none", "", "", nil)).Status != http.StatusNotFound || f.svc.DeleteApp("none") == nil {
		t.Fatal("missing apps are 404")
	}

	rows := f.svc.Resources()[0].Rows
	if len(rows) != 1 || rows[0][0] != "one" || rows[0][3] != "stopped" {
		t.Fatalf("resources: %v", rows)
	}

	// Save/Load/Reset keep metadata, not processes
	b, err := f.svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	f.svc.Reset()
	if len(f.svc.Resources()[0].Rows) != 0 {
		t.Fatal("reset left apps")
	}
	if err := f.svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if a := f.svc.Inspect("Function apps", "one").(map[string]any)["app"].(*functions.App); a.Settings["K"] != "w" {
		t.Fatalf("load: %+v", a)
	}
	if err := f.svc.DeleteApp("one"); err != nil {
		t.Fatal(err)
	}
}

// The host's settings: pogi's storage, the runtime, then the app's own.
func TestHostSettings(t *testing.T) {
	f := newFixture(t)
	for _, k := range []string{"azblob", "azqueue", "aztable"} {
		f.hub.SetEndpoint("port:"+k, "127.0.0.1:"+map[string]string{"azblob": "1", "azqueue": "2", "aztable": "3"}[k])
	}
	f.svc.CreateApp("e1", "python", "", map[string]string{"MY": "setting", "AzureWebJobsStorage": "UseDevelopmentStorage=true"})
	f.svc.CreateApp("e2", "node", "", map[string]string{"AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=real"})
	e := f.svc.Env("e1", "host.docker.internal")
	for _, want := range []string{"BlobEndpoint=http://host.docker.internal:1/devstoreaccount1;", "QueueEndpoint=http://host.docker.internal:2/devstoreaccount1;",
		"TableEndpoint=http://host.docker.internal:3/devstoreaccount1;", "AccountKey=" + core.AzuriteKey} {
		if !strings.Contains(e["AzureWebJobsStorage"], want) {
			t.Errorf("AzureWebJobsStorage lacks %s: %s", want, e["AzureWebJobsStorage"])
		}
	}
	if e["MY"] != "setting" || e["FUNCTIONS_WORKER_RUNTIME"] != "python" {
		t.Errorf("env: %v", e)
	}
	if e := f.svc.Env("e2", "x"); e["AzureWebJobsStorage"] != "DefaultEndpointsProtocol=https;AccountName=real" || e["FUNCTIONS_WORKER_RUNTIME"] != "node" {
		t.Errorf("the app's own storage setting wins: %v", e)
	}
}

func TestBackgroundTriggers(t *testing.T) {
	for _, c := range []struct {
		file, code string
		want       bool
	}{
		{"function_app.py", "@app.route(route='x')\ndef f(): pass", false},
		{"function_app.py", "@app.timer_trigger(schedule='* * * * * *', arg_name='t')", true},
		{"function_app.py", "@app.queue_trigger(arg_name='m', queue_name='q', connection='C')", true},
		{"f/function.json", `{"bindings":[{"type":"timerTrigger","name":"t"}]}`, true},
		{"src/app.js", "app.timer('x', {schedule: '* * * * * *'})", true},
		{"src/app.js", "app.http('x', {})", false},
		{".python_packages/x.py", "timer_trigger", false},
	} {
		dir := t.TempDir()
		os.MkdirAll(filepath.Dir(filepath.Join(dir, c.file)), 0o755)
		os.WriteFile(filepath.Join(dir, c.file), []byte(c.code), 0o644)
		if got := functions.Background(dir); got != c.want {
			t.Errorf("%s %q: got %v", c.file, c.code, got)
		}
	}
}

func TestInspect(t *testing.T) {
	f := newFixture(t)
	f.app("insp")
	m := f.svc.Inspect("Function apps", "insp").(map[string]any)
	if !strings.HasSuffix(m["url"].(string), "/api/") || !strings.Contains(m["url"].(string), "insp.azurewebsites.localhost") || m["host"] != "running (fake)" {
		t.Fatalf("%v", m)
	}
	if f.svc.Inspect("Function apps", "none") != nil || f.svc.Inspect("Other", "insp") != nil {
		t.Fatal("unknown rows are nil")
	}
	if _, err := json.Marshal(m); err != nil {
		t.Fatal(err)
	}
}

// pogi.yaml seeds function apps; a path is relative to the file.
func TestSeed(t *testing.T) {
	dir := t.TempDir()
	os.Mkdir(filepath.Join(dir, "code"), 0o755)
	f := &config.File{Path: filepath.Join(dir, "pogi.yaml")}
	src := "resources:\n  azure:\n    functions:\n      apps:\n        - {name: api, runtime: python, path: code, settings: {GREETING: hi, N: 3}}\n"
	if err := yaml.Unmarshal([]byte(src), f); err != nil {
		t.Fatal(err)
	}
	srv := pogi.Start(t, pogi.Config{File: f, Services: []string{"azfunctions"}})
	svc := srv.Hub.Service("azfunctions").(*functions.Service)
	a := svc.Inspect("Function apps", "api").(map[string]any)["app"].(*functions.App)
	if a.Runtime != "python" || a.Path != filepath.Join(dir, "code") || a.Settings["GREETING"] != "hi" || a.Settings["N"] != "3" {
		t.Fatalf("seeded app: %+v", a)
	}
	srv.Reset("azfunctions") // seeded again after a reset
	if svc.Inspect("Function apps", "api") == nil {
		t.Fatal("not reseeded")
	}
}
