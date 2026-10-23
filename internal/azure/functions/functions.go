// Package functions emulates Azure Functions apps by running the real
// Functions host. pogi gives the host its settings (its own Blob, Queue and
// Table endpoints as AzureWebJobsStorage) and does not re-implement any
// trigger: the host runs timers, queues and the rest itself. It is Azure
// Functions Core Tools (`func host start`) when func is on PATH, else the
// official mcr.microsoft.com/azure-functions/<runtime> image in Docker, else
// the apps are metadata only and invocations get 503.
//
// HTTP triggers are reachable at http://<app>.azurewebsites.localhost:<port>/api/<fn>
// and at /_azure/functions/<app>/api/<fn>; any function key is accepted.
// Code comes from a local folder or a Kudu-style zip deploy (POST /api/zipdeploy on
// <app>.scm.azurewebsites.localhost, or /_azure/functions/<app>/api/zipdeploy).
// Event Grid events reach a function through the hub (Microsoft.Web provider).
// The Go methods CreateApp, UpdateApp, DeleteApp and Deploy are for the
// Azure resource manager, which finds the service with Hub.Service("azfunctions").
package functions

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Azure Functions emulator.
type Service struct {
	h  *core.Hub
	mu sync.Mutex
	st state

	hosts map[string]*host // running (or starting) hosts by app
	logs  map[string]*ring

	probe sync.Mutex // guards ip
	ip    string     // this machine as containers see it, once looked up
}

type state struct {
	Apps map[string]*App `json:"apps"`
	ARM  armres.Store    `json:"arm,omitempty"` // plans and sites as the resource manager has them
}

// App is a function app. Hosts are not state: they start again on demand.
type App struct {
	Name     string            `json:"name"`
	Runtime  string            `json:"runtime"`
	Path     string            `json:"path,omitempty"` // code folder: the caller's own, or pogi's copy of a zip deploy
	Zip      bool              `json:"zip,omitempty"`  // Path is pogi's copy
	Settings map[string]string `json:"settings,omitempty"`
	Created  time.Time         `json:"created"`
}

// New creates the Azure Functions service.
func New(h *core.Hub) *Service {
	s := &Service{h: h}
	s.Reset()
	h.OnDeliver("microsoft.web", s.deliver)
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azfunctions", Cloud: core.Azure, Title: "Azure Functions", Protocol: "azure-json", Match: match}
}

const pathPrefix = "/_azure/functions/"

const hostSuffix = ".azurewebsites.localhost"

// target splits a request into the app and the path on that app's host.
// scm is the Kudu site, <app>.scm.azurewebsites.localhost.
func target(r *http.Request) (app string, scm bool, path string, ok bool) {
	if rest, found := strings.CutPrefix(r.URL.Path, pathPrefix); found {
		app, p, _ := strings.Cut(rest, "/")
		return app, false, "/" + p, app != ""
	}
	host := strings.ToLower(r.Host)
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.HasSuffix(host, "]") {
		host = host[:i]
	}
	if name, found := strings.CutSuffix(host, hostSuffix); found && name != "" {
		app, scm = strings.CutSuffix(name, ".scm")
		return app, scm, r.URL.Path, app != "" && !strings.Contains(app, ".")
	}
	return "", false, "", false
}

func match(r *http.Request) bool { _, _, _, ok := target(r); return ok }

// BodyLimit raises the upload limit for a zip deploy.
func (s *Service) BodyLimit(r *http.Request) int64 {
	if _, _, path, ok := target(r); ok && path == "/api/zipdeploy" && (r.Method == http.MethodPost || r.Method == http.MethodPut) {
		return s.h.CodeLimit(false)
	}
	return 0
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	name, scm, path, _ := target(r)
	switch {
	case path == "/api/zipdeploy" && (r.Method == http.MethodPost || r.Method == http.MethodPut):
		c.Op = "ZipDeploy"
	case (scm || strings.HasPrefix(r.URL.Path, pathPrefix)) && strings.HasPrefix(path, "/api/deployments"):
		c.Op = "GetDeployment"
	default:
		c.Op = "InvokeFunction"
	}
	if err := c.Intercept(); err != nil {
		reply(w, c, err)
		return
	}
	a := s.app(name)
	if a == nil {
		reply(w, c, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The function app %q was not found.", name))
		return
	}
	switch c.Op {
	case "ZipDeploy":
		if err := s.Deploy(name, core.Body(r)); err != nil {
			reply(w, c, err)
			return
		}
		if r.URL.Query().Get("isAsync") == "true" {
			w.Header().Set("Location", "http://"+r.Host+strings.TrimSuffix(r.URL.Path, "zipdeploy")+"deployments/latest")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusOK)
	case "GetDeployment": // what `az functionapp deployment source config-zip` polls
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": "latest", "status": 4, "complete": true, "active": true, "site_name": name})
	default:
		s.proxy(w, r, c, name, path)
	}
}

// reply writes an Azure-style JSON error.
func reply(w http.ResponseWriter, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-ms-request-id", c.RequestID)
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": e.Code, "message": e.Msg}})
}

// proxy sends the request to the app's host, starting it first. The caller's
// function key (or none) is replaced with the host's master key.
func (s *Service) proxy(w http.ResponseWriter, r *http.Request, c *core.Ctx, name, path string) {
	h, err := s.host(r.Context(), name)
	if err != nil {
		reply(w, c, core.Errorf(http.StatusServiceUnavailable, "ServiceUnavailable", "pogi: the Functions host of %s is unavailable: %v", name, err))
		return
	}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host, pr.Out.URL.Path, pr.Out.URL.RawPath = "http", h.addr, path, ""
			if q := pr.In.URL.Query(); q.Has("code") {
				q.Del("code")
				pr.Out.URL.RawQuery = q.Encode()
			}
			pr.Out.Header.Set("x-functions-key", h.key)
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			reply(w, c, core.Errorf(http.StatusBadGateway, "BadGateway", "pogi: the Functions host of %s did not answer: %v", name, err))
		},
	}
	rp.ServeHTTP(w, r)
}

func (s *Service) app(name string) *App {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Apps[strings.ToLower(name)]; a != nil {
		cp := *a
		return &cp
	}
	return nil
}

// host returns the app's running host, starting it on first use and waiting
// for it to answer.
func (s *Service) host(ctx context.Context, name string) (*host, error) {
	s.mu.Lock()
	a := s.st.Apps[strings.ToLower(name)]
	if a == nil {
		s.mu.Unlock()
		return nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The function app %q was not found.", name)
	}
	key := strings.ToLower(a.Name)
	h := s.hosts[key]
	if h == nil {
		app := *a
		hctx, cancel := context.WithCancel(context.Background())
		h = &host{ready: make(chan struct{}), cancel: cancel}
		s.hosts[key] = h
		logs := s.ring(key)
		core.Go("functions.host", func() {
			defer close(h.ready)
			h.err = errors.New("internal error starting the function host") // stays if start panics
			defer func() {
				if h.err != nil {
					logs.add("pogi: " + h.err.Error())
					if h.stop != nil {
						h.stop()
					}
					s.mu.Lock()
					if s.hosts[key] == h {
						delete(s.hosts, key)
					}
					s.mu.Unlock()
				}
			}()
			h.err = s.start(hctx, app, h, logs)
		})
	}
	s.mu.Unlock()
	select {
	case <-h.ready:
		return h, h.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) ring(key string) *ring { // lock held
	if s.logs[key] == nil {
		s.logs[key] = &ring{}
	}
	return s.logs[key]
}

// stopHost stops an app's host, if any.
func (s *Service) stopHost(name string) {
	s.mu.Lock()
	key := strings.ToLower(name)
	h := s.hosts[key]
	delete(s.hosts, key)
	s.mu.Unlock()
	if h != nil {
		h.cancel()
		<-h.ready
		if h.err == nil && h.stop != nil {
			h.stop()
		}
	}
}

// background matches code that has a trigger other than HTTP, whose host must
// run without anybody calling it (a timer fires by itself).
// ponytail: a text scan of the code (Python decorators, function.json,
// Node app.timer(…)); the host's own function list would be exact.
var background = regexp.MustCompile(`(?i)(timer|queue|service_?bus\w*|event_?hub|blob|cosmos_?db)_?trigger|app\.(timer|storageQueue|serviceBus\w*|eventHub|storageBlob|cosmosDB)\(`)

func hasBackgroundTrigger(dir string) bool {
	found := false
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		switch {
		case err != nil || found:
			return filepath.SkipAll
		case d.IsDir():
			if n := d.Name(); n == "node_modules" || n == ".python_packages" || n == ".git" || n == "bin" || n == "obj" {
				return filepath.SkipDir
			}
		case slices.Contains([]string{".py", ".json", ".js", ".mjs", ".ts", ".cs", ".java", ".ps1"}, filepath.Ext(p)):
			if b, err := os.ReadFile(p); err == nil && len(b) < 1<<20 && background.Match(b) {
				found = true
			}
		}
		return nil
	})
	return found
}

// autostart starts the host of an app whose code has background triggers.
func (s *Service) autostart(name string) {
	a := s.app(name)
	if a == nil || a.Path == "" || !hasBackgroundTrigger(a.Path) {
		return
	}
	core.Go("functions.autostart", func() {
		if _, err := s.host(context.Background(), name); err != nil {
			s.h.Log.Warn("azure functions: host did not start", slog.String("app", name), "err", err)
		}
	})
}

// ---- Go API, for the resource manager and seeding ----

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{0,58}$`)

func normalize(runtime, path string) (string, string, error) {
	runtime = strings.ToLower(strings.TrimSpace(runtime))
	if runtime == "dotnet" {
		runtime = "dotnet-isolated"
	}
	if _, ok := images[runtime]; !ok && runtime != "" {
		return "", "", core.Errorf(http.StatusBadRequest, "InvalidRuntime", "runtime %q is not one of python, node, dotnet-isolated, java, powershell", runtime)
	}
	if path != "" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", "", err
		}
		if fi, err := os.Stat(abs); err != nil || !fi.IsDir() {
			return "", "", core.Errorf(http.StatusBadRequest, "InvalidCodePath", "code path %q is not a directory", path)
		}
		path = abs
	}
	return runtime, path, nil
}

// CreateApp adds a function app. path is a code folder (may be empty when the
// code is deployed as a zip later); settings are the app settings.
func (s *Service) CreateApp(name, runtime, path string, settings map[string]string) error {
	if !nameRE.MatchString(name) {
		return core.Errorf(http.StatusBadRequest, "InvalidResourceName", "%q is not a valid function app name", name)
	}
	if runtime == "" {
		return core.Errorf(http.StatusBadRequest, "InvalidRuntime", "a runtime is required")
	}
	runtime, path, err := normalize(runtime, path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.st.Apps[strings.ToLower(name)] != nil {
		s.mu.Unlock()
		return core.Errorf(http.StatusConflict, "Conflict", "The function app %q already exists.", name)
	}
	s.st.Apps[strings.ToLower(name)] = &App{Name: name, Runtime: runtime, Path: path, Settings: settings, Created: time.Now().UTC()}
	s.mu.Unlock()
	s.autostart(name)
	return nil
}

// UpdateApp changes an app: an empty runtime or path and nil settings stay as they are.
// A running host restarts on its next use with the new settings and code.
func (s *Service) UpdateApp(name, runtime, path string, settings map[string]string) error {
	runtime, path, err := normalize(runtime, path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	a := s.st.Apps[strings.ToLower(name)]
	if a == nil {
		s.mu.Unlock()
		return notFound(name)
	}
	if runtime != "" {
		a.Runtime = runtime
	}
	if path != "" {
		a.Path, a.Zip = path, false
	}
	if settings != nil {
		a.Settings = settings
	}
	s.mu.Unlock()
	s.stopHost(name)
	s.autostart(name)
	return nil
}

// DeleteApp removes an app, stopping its host and deleting a zip deploy.
func (s *Service) DeleteApp(name string) error {
	s.stopHost(name)
	s.mu.Lock()
	a := s.st.Apps[strings.ToLower(name)]
	if a == nil {
		s.mu.Unlock()
		return notFound(name)
	}
	delete(s.st.Apps, strings.ToLower(name))
	delete(s.logs, strings.ToLower(name))
	s.mu.Unlock()
	if a.Zip {
		os.RemoveAll(a.Path)
	}
	return nil
}

func notFound(name string) error {
	return core.Errorf(http.StatusNotFound, "ResourceNotFound", "The function app %q was not found.", name)
}

// Deploy replaces an app's code with the contents of a zip.
func (s *Service) Deploy(name string, zipData []byte) error {
	a := s.app(name)
	if a == nil {
		return notFound(name)
	}
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return core.Errorf(http.StatusBadRequest, "BadRequest", "the deployment is not a zip file: %v", err)
	}
	s.stopHost(name)
	dir := filepath.Join(s.h.Cfg.DataDir, "azfunctions", strings.ToLower(a.Name))
	os.RemoveAll(dir)
	if err := Extract(zr, dir); err != nil {
		os.RemoveAll(dir)
		return core.Errorf(http.StatusBadRequest, "BadRequest", "%v", err)
	}
	s.mu.Lock()
	if cur := s.st.Apps[strings.ToLower(name)]; cur != nil {
		cur.Path, cur.Zip = dir, true
	}
	s.mu.Unlock()
	s.autostart(name)
	return nil
}

// maxUnzipped caps the total unpacked size of one app zip (1 GiB).
const maxUnzipped = 1 << 30

// Extract unpacks a zip into dir, refusing entries outside it and more than 1 GiB in all.
func Extract(zr *zip.Reader, dir string) error {
	if err := core.CheckZipReader(zr); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	left := int64(maxUnzipped) // bytes still allowed, across all entries
	for _, f := range zr.File {
		dst := filepath.Join(dir, filepath.FromSlash(f.Name))
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(dst, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644|f.Mode()&0o111)
		if err == nil {
			var n int64
			n, err = io.Copy(out, io.LimitReader(rc, left+1))
			out.Close()
			if left -= n; left < 0 {
				err = fmt.Errorf("the app is over the %d MB unzipped limit", maxUnzipped>>20)
			}
		}
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// ---- Event Grid ----

// deliver hands an Event Grid event to an Event Grid–triggered function:
// /subscriptions/s/resourceGroups/g/providers/Microsoft.Web/sites/<app>/functions/<fn>.
func (s *Service) deliver(id string, m *core.Message) error {
	_, rest, _ := strings.Cut(strings.ToLower(id), "/providers/microsoft.web/sites/")
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[1] != "functions" {
		return fmt.Errorf("%s is not a function: want …/Microsoft.Web/sites/<app>/functions/<fn>", id)
	}
	// the function name keeps its case: the host matches it ignoring case, but a path from the caller reads better
	fn := id[len(id)-len(parts[2]):]
	h, err := s.host(context.Background(), parts[0])
	if err != nil {
		return err
	}
	ct := "application/json"
	if strings.Contains(m.Body, `"specversion"`) && strings.HasPrefix(strings.TrimSpace(m.Body), "{") {
		ct = "application/cloudevents+json"
	}
	req, _ := http.NewRequest(http.MethodPost, "http://"+h.addr+"/runtime/webhooks/eventgrid?functionName="+fn, strings.NewReader(m.Body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set("aeg-event-type", "Notification")
	req.Header.Set("x-functions-key", h.key)
	if t := core.Traceparent(m.Trace); t != "" {
		req.Header.Set("traceparent", t)
	}
	resp, err := hostClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("the Functions host answered %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}

// ---- state, console ----

func (s *Service) Reset() {
	s.mu.Lock()
	apps := s.st.Apps
	hosts := s.hosts
	s.st = state{Apps: map[string]*App{}}
	s.hosts, s.logs = map[string]*host{}, map[string]*ring{}
	s.mu.Unlock()
	stopAll(hosts)
	for _, a := range apps {
		if a.Zip {
			os.RemoveAll(a.Path)
		}
	}
}

func stopAll(hosts map[string]*host) {
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		core.Go("functions.stopAll", func() {
			defer wg.Done()
			h.cancel()
			<-h.ready
			if h.err == nil && h.stop != nil {
				h.stop()
			}
		})
	}
	wg.Wait()
}

// Close stops every host.
func (s *Service) Close() error {
	s.mu.Lock()
	hosts := s.hosts
	s.hosts = map[string]*host{}
	s.mu.Unlock()
	stopAll(hosts)
	return nil
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Apps == nil {
		st.Apps = map[string]*App{}
	}
	s.mu.Lock()
	hosts := s.hosts
	s.st, s.hosts = st, map[string]*host{}
	s.mu.Unlock()
	stopAll(hosts)
	for name := range st.Apps {
		s.autostart(name)
	}
	return nil
}

const table = "Function apps"

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: table, Columns: []string{"Name", "Runtime", "Code", "Host"}}
	for _, k := range core.SortedKeys(s.st.Apps) {
		a := s.st.Apps[k]
		t.Rows = append(t.Rows, []string{a.Name, a.Runtime, cmpOr(a.Path, "(none)"), s.state(k)})
	}
	return []core.Table{t}
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// state says what the app's host is doing; lock held.
func (s *Service) state(key string) string {
	h := s.hosts[key]
	if h == nil {
		return "stopped"
	}
	select {
	case <-h.ready:
		return "running (" + h.how + ")"
	default:
		return "starting"
	}
}

func (s *Service) Inspect(tbl, id string) any {
	if tbl != table {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Apps[strings.ToLower(id)]
	if a == nil {
		return nil
	}
	out := map[string]any{"app": a, "host": s.state(strings.ToLower(a.Name)),
		"url": "http://" + strings.ToLower(a.Name) + hostSuffix + ":" + portOf(s.h.URL) + "/api/"}
	if g := s.logs[strings.ToLower(a.Name)]; g != nil {
		out["logs"] = g.tail()
	}
	return out
}

func portOf(u string) string { return u[strings.LastIndexByte(u, ':')+1:] }

// hostClient calls the user's function host; a host that never answers must not hold pogi forever.
var hostClient = &http.Client{Timeout: 2 * time.Minute}
