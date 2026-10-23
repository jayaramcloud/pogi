// Package appservice emulates Azure App Service web apps: the sites of
// Microsoft.Web/sites that are not function apps, and their configuration
// children (config/web, appsettings, connectionstrings, publishing
// credentials, ...). The Functions package owns the Microsoft.Web/sites type
// and hands the web apps to Sites() of this service.
//
// A Linux web app runs in Docker:
//   - linuxFxVersion DOCKER|<image>: that image, with the app settings as its
//     environment, serving WEBSITES_PORT (80 by default);
//   - NODE|<version> and PYTHON|<version>: the code of a zip deploy (POST
//     /api/zipdeploy on <app>.scm.azurewebsites.localhost) in the official
//     node or python image, started with the app command line (else npm start
//     or server.js, app.py), serving WEBSITES_PORT (8080 by default).
//
// The app is served at http://<app>.azurewebsites.localhost:<port>/ and at
// /_azure/appservice/<app>/... on pogi's port, and starts when it is created
// or changed, and again on a request that finds it down. It stops when it is
// stopped or deleted, and when pogi shuts down.
//
// Without Docker the web apps are metadata only: requests get 503. A Windows
// web app is always metadata only.
//
// ponytail: one container per app, no scaling, slots, custom domains, TLS
// bindings, backups or authentication (the settings are stored, not applied);
// one stack per language version; no .NET, Java, PHP or Ruby stacks.
package appservice

import (
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/azure/functions"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

const (
	typSite   = "Microsoft.Web/sites"
	typPlan   = "Microsoft.Web/serverfarms"
	typConfig = typSite + "/config"
	typPolicy = typSite + "/basicPublishingCredentialsPolicies"

	hostSuffix = ".azurewebsites.localhost"
	pathPrefix = "/_azure/appservice/"
)

type state struct {
	ARM  armres.Store      `json:"arm"`            // web apps and their configuration
	Code map[string]string `json:"code,omitempty"` // lower-cased app ID → folder of its zip deploy
}

// Service is the App Service web app emulator.
type Service struct {
	hub    *core.Hub
	mu     sync.Mutex
	st     state
	insts  map[string]*inst            // running apps by lower-cased ID
	logs   map[string]*serverless.Logs // by lower-cased ID
	fails  map[string]string           // why an app did not start, by lower-cased ID
	ctx    context.Context
	cancel context.CancelFunc
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azappservice", Cloud: core.Azure, Title: "App Service", Protocol: "azure-json", Match: s.match}
}

func key(id arm.ID) string { return strings.ToLower(id.String()) }

func bad(code, format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, code, format, a...)
}

// ---- data plane ----

// target is what a request names: the app, whether it is the Kudu (scm) site, and the path on it.
type target struct {
	app, path string
	scm       bool
}

func parseTarget(r *http.Request) (t target, ok bool) {
	if rest, found := strings.CutPrefix(r.URL.Path, pathPrefix); found {
		app, p, _ := strings.Cut(rest, "/")
		return target{app: app, path: "/" + p}, app != ""
	}
	h, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		h = r.Host
	}
	label, found := strings.CutSuffix(strings.ToLower(h), hostSuffix)
	if !found || label == "" {
		return t, false
	}
	t.app, t.scm = strings.CutSuffix(label, ".scm")
	t.path = r.URL.Path
	return t, t.app != "" && !strings.Contains(t.app, ".")
}

// match claims the requests of the web apps this service has; those of others (function apps) pass on.
func (s *Service) match(r *http.Request) bool {
	t, ok := parseTarget(r)
	if !ok {
		return false
	}
	_, found := s.find(t.app)
	return found
}

// find returns the web app of a name (host names are global, so the name alone tells).
func (s *Service) find(name string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.ARM) {
		if r := s.st.ARM[k]; strings.EqualFold(armres.Str(r, "type"), typSite) && strings.EqualFold(armres.Str(r, "name"), name) {
			return armres.Clone(r), true
		}
	}
	return nil, false
}

// BodyLimit raises the upload limit for a zip deploy.
func (s *Service) BodyLimit(r *http.Request) int64 {
	if t, ok := parseTarget(r); ok && isDeploy(t, r) {
		return s.hub.CodeLimit(false)
	}
	return 0
}

func isDeploy(t target, r *http.Request) bool {
	return (r.Method == http.MethodPost || r.Method == http.MethodPut) &&
		(t.path == "/api/zipdeploy" || t.path == "/api/publish" && r.URL.Query().Get("type") == "zip")
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	t, _ := parseTarget(r)
	switch {
	case isDeploy(t, r):
		c.Op = "ZipDeploy"
	case strings.HasPrefix(t.path, "/api/deployments") && (t.scm || strings.HasPrefix(r.URL.Path, pathPrefix)):
		c.Op = "GetDeployment"
	default:
		c.Op = "Invoke"
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	app, ok := s.find(t.app)
	if !ok {
		http.Error(w, "404 Not Found: no web app named "+t.app, http.StatusNotFound)
		return
	}
	id, _ := arm.ParseID(armres.Str(app, "id"))
	switch c.Op {
	case "ZipDeploy":
		if err := s.deploy(id, core.Body(r)); err != nil {
			http.Error(w, err.Error(), core.AsError(err).Status)
			return
		}
		if r.URL.Query().Get("isAsync") == "true" {
			w.Header().Set("Location", "http://"+r.Host+strings.TrimSuffix(r.URL.Path, t.path)+"/api/deployments/latest")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusOK)
	case "GetDeployment": // what `az webapp deploy` polls
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": "latest", "status": 4, "complete": true, "active": true, "site_name": t.app})
	default:
		s.invoke(w, r, id, app, t)
	}
}

// invoke passes a request to the app's container, starting it when it is down.
func (s *Service) invoke(w http.ResponseWriter, r *http.Request, id arm.ID, app map[string]any, t target) {
	switch {
	case t.scm:
		http.Error(w, "404 Not Found: pogi serves the deployment API of the Kudu site (/api/zipdeploy), nothing else", http.StatusNotFound)
	case armres.Props(app)["state"] == "Stopped":
		http.Error(w, "Error 403 - This web app is stopped.", http.StatusForbidden)
	default:
		x, err := s.instance(id)
		if err != nil {
			http.Error(w, "503 Service Unavailable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		r = r.Clone(r.Context())
		r.URL.Path, r.URL.RawPath = t.path, ""
		serverless.Proxy(w, r, x.in.Addr)
	}
}

// ---- deploying code ----

// deploy replaces the code of an app with a zip, and restarts the app.
func (s *Service) deploy(id arm.ID, data []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return bad("BadRequest", "the deployment is not a zip file: %v", err)
	}
	k := key(id)
	root := ""
	if dd := s.hub.Cfg.DataDir; dd != "" { // under the data dir: if pogi runs in a container, the engine can then find it
		root = filepath.Join(dd, "azappservice")
		os.MkdirAll(root, 0o755)
	}
	dir, err := os.MkdirTemp(root, "code-")
	if err != nil {
		return err
	}
	if err := functions.Extract(zr, dir); err != nil {
		os.RemoveAll(dir)
		return bad("BadRequest", "%v", err)
	}
	s.mu.Lock()
	old := s.st.Code[k]
	s.st.Code[k] = dir
	x := s.take(k)
	s.mu.Unlock()
	x.stop()
	if old != "" {
		os.RemoveAll(old)
	}
	s.warm(id)
	return nil
}

// ---- state and console ----

func (s *Service) Reset() {
	s.Close()
	s.mu.Lock()
	code := s.st.Code
	s.st = state{Code: map[string]string{}}
	s.insts, s.logs, s.fails = map[string]*inst{}, map[string]*serverless.Logs{}, map[string]string{}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mu.Unlock()
	for _, d := range code {
		os.RemoveAll(d)
	}
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
	if st.Code == nil {
		st.Code = map[string]string{}
	}
	s.Close()
	s.mu.Lock()
	s.st = st
	s.insts, s.logs, s.fails = map[string]*inst{}, map[string]*serverless.Logs{}, map[string]string{}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	var ids []arm.ID
	for _, r := range st.ARM {
		if id, ok := arm.ParseID(armres.Str(r, "id")); ok && strings.EqualFold(id.Type(), typSite) && armres.Props(r)["state"] != "Stopped" {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	for _, id := range ids { // the containers did not survive the restart
		s.warm(id)
	}
	return nil
}

// Close stops the containers.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	insts := s.insts
	s.insts = map[string]*inst{}
	s.mu.Unlock()
	for _, x := range insts {
		x.stop()
	}
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Web apps", Columns: []string{"Name", "Kind", "Plan", "State", "Container"}}
	for _, k := range core.SortedKeys(s.st.ARM) {
		r := s.st.ARM[k]
		if !strings.EqualFold(armres.Str(r, "type"), typSite) {
			continue
		}
		p := armres.Props(r)
		plan := armres.Str(p, "serverFarmId")
		t.Rows = append(t.Rows, []string{armres.Str(r, "name"), armres.Str(r, "kind"), plan[strings.LastIndexByte(plan, '/')+1:],
			cmp.Or(armres.Str(p, "state"), "Running"), map[bool]string{true: "running", false: "none"}[s.insts[k] != nil]})
	}
	return []core.Table{t}
}

// Inspect shows a web app as the API returns it, with its configuration, why it did not start, and its logs.
func (s *Service) Inspect(table, name string) any {
	if table != "Web apps" {
		return nil
	}
	app, ok := s.find(name)
	if !ok {
		return nil
	}
	id, _ := arm.ParseID(armres.Str(app, "id"))
	k := key(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{"app": app, "settings": s.settings(id)}
	if cfg, ok := s.st.ARM.Get(id.Child("config", "web")); ok {
		out["config"] = armres.Props(cfg)
	}
	if f := s.fails[k]; f != "" {
		out["error"] = f
	}
	if l := s.logs[k]; l != nil {
		out["logs"] = l.Lines()
	}
	return out
}

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{0,58}[a-zA-Z0-9]$`)
