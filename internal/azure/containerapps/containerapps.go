// Package containerapps emulates Azure Container Apps: the management plane
// of Microsoft.App/managedEnvironments, containerApps (with their revisions)
// and jobs (with their executions), all arm.Providers, and the containers.
//
// An environment is metadata with a default domain, <env>.azurecontainerapps.localhost:<pogi port>.
//
// A container app runs as Docker containers. Each change to its template makes
// a revision, <app>--<suffix>; in Single mode the new revision takes all traffic,
// in Multiple mode configuration.ingress.traffic splits it by weight, by revision
// or by latestRevision. With ingress the app is served at
// http://<app>.<env>.azurecontainerapps.localhost:<port>/ (its fqdn; a revision at
// <revision>.<env>... ) and at /_azure/containerapps/<env>/<app>/... on pogi's
// port, and proxied like Cloud Run: the container of a revision starts on its first
// request (at once with minReplicas above 0, and for apps without ingress) and runs
// until the revision is deactivated or the app deleted. Secrets and env are
// resolved into the containers' environment; scale settings are stored.
//
// A job runs on the start action: replicaCompletionCount containers, parallelism at a
// time, each retried replicaRetryLimit times, within replicaTimeout. Its executions
// are Running, then Succeeded, or Failed.
//
// Without a container engine the apps and jobs are metadata only: requests get 503
// and executions fail.
//
// ponytail: the first container of a template only, no sidecars or init containers;
// one replica per revision and no scaling; no volumes, probes, Dapr, custom domains,
// certificates, authentication or labels; schedule and event triggers of jobs are
// stored and never fire; internal ingress is served like external.
package containerapps

import (
	"cmp"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

const (
	typEnv  = "Microsoft.App/managedEnvironments"
	typApp  = "Microsoft.App/containerApps"
	typRev  = typApp + "/revisions"
	typJob  = "Microsoft.App/jobs"
	typExec = typJob + "/executions"

	hostSuffix = ".azurecontainerapps.localhost"
	pathPrefix = "/_azure/containerapps/"
)

type state struct {
	ARM    armres.Store      `json:"arm"`
	Latest map[string]string `json:"latest"` // lower-cased app ID → name of its latest revision
	Hash   map[string]string `json:"hash"`   // lower-cased revision ID → hash of its template
}

// Service is the Container Apps emulator.
type Service struct {
	hub    *core.Hub
	mu     sync.Mutex
	st     state
	insts  map[string]*inst            // running revisions by lower-cased revision ID
	logs   map[string]*serverless.Logs // by lower-cased revision or execution ID
	runs   map[string]*execRun         // executions in progress, by lower-cased ID
	ctx    context.Context
	cancel context.CancelFunc
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.Reset()
	arm.Register(h, armres.Stored{Type: typEnv, Async: true, Mu: &s.mu, St: &s.st.ARM, Put: s.putEnv, View: s.viewEnv, Delete: nil}.Provider())
	env := arm.Lookup(h, typEnv)
	del := env.Delete
	env.Delete = func(c *core.Ctx, id arm.ID) error {
		if s.inUse(id) {
			return core.Errorf(http.StatusBadRequest, "ManagedEnvironmentHasContainerApps", "The managed environment '%s' cannot be deleted while it has container apps or jobs.", id.Name())
		}
		return del(c, id)
	}
	app := armres.Stored{Type: typApp, Async: true, Mu: &s.mu, St: &s.st.ARM, Put: s.putApp, View: s.viewApp, Delete: s.deleteApp,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listSecrets": s.listSecrets("secrets"), "start": s.setRunning(true), "stop": s.setRunning(false)}}.Provider()
	app.LongActions = []string{"start", "stop"}
	arm.Register(h, app)
	rev := armres.Stored{Type: typRev, Mu: &s.mu, St: &s.st.ARM, View: s.viewRev,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"activate": s.activate, "deactivate": s.deactivate, "restart": s.restartRev}}.Provider()
	rev.Put, rev.Delete = nil, nil // a revision comes of a change to its app
	arm.Register(h, rev)
	job := armres.Stored{Type: typJob, Async: true, Mu: &s.mu, St: &s.st.ARM, Put: s.putJob, View: s.viewJob, Delete: s.deleteJob,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"start": s.startJob, "stop": s.stopJob, "listSecrets": s.listSecrets("secrets")}}.Provider()
	arm.Register(h, job)
	ex := armres.Stored{Type: typExec, Mu: &s.mu, St: &s.st.ARM,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"stop": s.stopExecution}}.Provider()
	ex.Put, ex.Delete = nil, nil
	arm.Register(h, ex)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azcontainerapps", Cloud: core.Azure, Title: "Container Apps", Protocol: "azure-json", Match: match}
}

// match claims the data plane: <app>.<env>.azurecontainerapps.localhost and /_azure/containerapps/.
func match(r *http.Request) bool {
	h, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		h = r.Host
	}
	return strings.HasSuffix(strings.ToLower(h), hostSuffix) || strings.HasPrefix(r.URL.Path, pathPrefix)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.serveData(w, r) }

func key(id arm.ID) string { return strings.ToLower(id.String()) }

func invalid(code, format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, code, format, a...)
}

// obj is m[k] as an object, which it makes in m when it is not one.
func obj(m map[string]any, k string) map[string]any {
	if o, ok := m[k].(map[string]any); ok {
		return o
	}
	o := map[string]any{}
	m[k] = o
	return o
}

// objs is m[k] as a list of objects.
func objs(m map[string]any, k string) []map[string]any {
	var out []map[string]any
	l, _ := m[k].([]any)
	for _, x := range l {
		if o, _ := x.(map[string]any); o != nil {
			out = append(out, o)
		}
	}
	return out
}

func num(v any) float64 { f, _ := v.(float64); return f }

// port is pogi's port as the caller reaches it.
func (s *Service) port(c *core.Ctx) string {
	_, p, _ := net.SplitHostPort(cmp.Or(c.Host, armres.HostOf(s.hub.URL)))
	return cmp.Or(p, serverless.Port(s.hub))
}

// ---- environments ----

func (s *Service) putEnv(c *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	for _, k := range []string{"defaultDomain", "staticIp", "provisioningState", "eventStreamEndpoint"} {
		delete(p, k)
	}
	return nil
}

func (s *Service) viewEnv(c *core.Ctx, r map[string]any) map[string]any {
	p := armres.Props(r)
	p["defaultDomain"] = strings.ToLower(armres.Str(r, "name")) + hostSuffix + ":" + s.port(c)
	p["staticIp"], p["provisioningState"] = "127.0.0.1", "Succeeded"
	armres.Defaults(p, map[string]any{"zoneRedundant": false, "eventStreamEndpoint": ""})
	return r
}

// inUse reports whether an environment still has apps or jobs.
func (s *Service) inUse(env arm.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.st.ARM {
		if t := armres.Str(r, "type"); strings.EqualFold(t, typApp) || strings.EqualFold(t, typJob) {
			if strings.EqualFold(envOf(r), env.String()) {
				return true
			}
		}
	}
	return false
}

// envOf is the ID of the environment an app or job lives in.
func envOf(r map[string]any) string {
	p := armres.Props(r)
	return cmp.Or(armres.Str(p, "environmentId"), armres.Str(p, "managedEnvironmentId"))
}

// environment finds the environment an app or job names.
func (s *Service) environment(r map[string]any) (map[string]any, error) {
	id, ok := arm.ParseID(envOf(r))
	if !ok || !strings.EqualFold(id.Type(), typEnv) {
		return nil, invalid("InvalidRequestContent", "properties.environmentId must be the resource ID of a managed environment.")
	}
	s.mu.Lock()
	env, found := s.st.ARM.Get(id)
	s.mu.Unlock()
	if !found {
		return nil, invalid("ManagedEnvironmentNotFound", "The managed environment '%s' was not found.", id.Name())
	}
	return env, nil
}

// ---- secrets ----

// listSecrets answers the POST that returns the secret values, {"value": [{name, value}]}; list is the
// configuration's key.
func (s *Service) listSecrets(list string) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		s.mu.Lock()
		r, ok := s.st.ARM.Get(id)
		s.mu.Unlock()
		if !ok {
			return nil, arm.NotFound(id)
		}
		out := []any{}
		for _, sec := range objs(armres.Props(r)["configuration"].(map[string]any), list) {
			out = append(out, map[string]any{"name": sec["name"], "value": sec["value"]})
		}
		return map[string]any{"value": out}, nil
	}
}

// restoreSecrets completes the secrets of a PATCH, which is a GET (without the secret values) merged with
// the changes, from the stored ones.
func restoreSecrets(old, cfg map[string]any) {
	for _, sec := range objs(cfg, "secrets") {
		if sec["value"] == nil && sec["keyVaultUrl"] == nil {
			for _, o := range objs(old, "secrets") {
				if o["name"] == sec["name"] {
					sec["value"] = o["value"]
				}
			}
		}
	}
}

// hideSecrets removes the values of secrets from a configuration, as the API does.
func hideSecrets(cfg map[string]any) {
	for _, sec := range objs(cfg, "secrets") {
		delete(sec, "value")
	}
}

// ---- state and console ----

func (s *Service) Reset() {
	s.Close()
	s.mu.Lock()
	s.st = state{Latest: map[string]string{}, Hash: map[string]string{}}
	s.insts, s.logs, s.runs = map[string]*inst{}, map[string]*serverless.Logs{}, map[string]*execRun{}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mu.Unlock()
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
	if st.Latest == nil {
		st.Latest = map[string]string{}
	}
	if st.Hash == nil {
		st.Hash = map[string]string{}
	}
	s.Close()
	s.mu.Lock()
	s.st = st
	s.insts, s.logs, s.runs = map[string]*inst{}, map[string]*serverless.Logs{}, map[string]*execRun{}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	for _, r := range s.st.ARM { // the containers did not survive the restart
		if strings.EqualFold(armres.Str(r, "type"), typExec) {
			if p := armres.Props(r); p["status"] == "Running" {
				p["status"], p["endTime"] = "Failed", nowStamp()
			}
		}
	}
	s.mu.Unlock()
	return nil
}

// Close stops the containers and the executions in progress.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	insts, runs := s.insts, s.runs
	s.insts, s.runs = map[string]*inst{}, map[string]*execRun{}
	s.mu.Unlock()
	for _, x := range insts {
		x.stop()
	}
	for _, x := range runs {
		x.cancel()
	}
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	tabs := map[string]*core.Table{
		typEnv:  {Name: "Environments", Columns: []string{"Name", "Location", "Default domain"}},
		typApp:  {Name: "Container apps", Columns: []string{"Name", "Environment", "Latest revision", "Mode"}},
		typRev:  {Name: "Revisions", Columns: []string{"Name", "Active", "Traffic", "Image", "Running"}},
		typJob:  {Name: "Jobs", Columns: []string{"Name", "Environment", "Trigger"}},
		typExec: {Name: "Executions", Columns: []string{"Name", "Status", "Started", "Finished"}},
	}
	for _, k := range core.SortedKeys(s.st.ARM) {
		r := s.st.ARM[k]
		p := armres.Props(r)
		t, name := tabs[armres.Str(r, "type")], armres.Str(r, "name")
		if t == nil {
			continue
		}
		switch armres.Str(r, "type") {
		case typEnv:
			t.Rows = append(t.Rows, []string{name, armres.Str(r, "location"), name + hostSuffix})
		case typApp:
			cfg, _ := p["configuration"].(map[string]any)
			t.Rows = append(t.Rows, []string{name, lastSeg(envOf(r)), s.st.Latest[k], cmp.Or(armres.Str(cfg, "activeRevisionsMode"), "Single")})
		case typRev:
			t.Rows = append(t.Rows, []string{name, boolStr(p["active"]), "", firstImage(p), boolStr(s.insts[k] != nil)})
		case typJob:
			cfg, _ := p["configuration"].(map[string]any)
			t.Rows = append(t.Rows, []string{name, lastSeg(envOf(r)), armres.Str(cfg, "triggerType")})
		case typExec:
			t.Rows = append(t.Rows, []string{name, armres.Str(p, "status"), armres.Str(p, "startTime"), armres.Str(p, "endTime")})
		}
	}
	var out []core.Table
	for _, typ := range []string{typEnv, typApp, typRev, typJob, typExec} {
		out = append(out, *tabs[typ])
	}
	return out
}

func lastSeg(id string) string { return id[strings.LastIndexByte(id, '/')+1:] }

func boolStr(v any) string {
	if b, _ := v.(bool); b {
		return "yes"
	}
	return "no"
}

func firstImage(p map[string]any) string {
	t, _ := p["template"].(map[string]any)
	for _, c := range objs(t, "containers") {
		return armres.Str(c, "image")
	}
	return ""
}

// Inspect shows a resource as the API returns it, with the output of its containers.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	typ := map[string]string{"Environments": typEnv, "Container apps": typApp, "Revisions": typRev, "Jobs": typJob, "Executions": typExec}[table]
	for k, r := range s.st.ARM {
		if armres.Str(r, "name") == id && armres.Str(r, "type") == typ {
			out := armres.Clone(r)
			if l := s.logs[k]; l != nil {
				out["logs"] = l.Lines()
			}
			return out
		}
	}
	return nil
}
