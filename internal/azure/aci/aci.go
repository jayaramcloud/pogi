// Package aci emulates Azure Container Instances: the management plane of
// Microsoft.ContainerInstance/containerGroups (an arm.Provider) and the
// containers of a group, which run in Docker: image, command, environment,
// ports, memory and CPU limits, restart policy. Without a container engine
// the groups are metadata only and stay Pending.
//
// A group's containers share the network of its first container, so they reach
// each other on localhost; the ports the group declares are published on
// 127.0.0.1. The group's ipAddress is 127.0.0.1, and an HTTP service is also
// served at http://<dnsNameLabel>.<location>.azurecontainer.localhost:<pogi port>/
// (the fqdn; the group's name when it has no label) and at
// /_azure/aci/<group>/... on pogi's port.
//
// The instanceView of the group and of each container follows the containers:
// Running, Terminated with the exit code, restarted per restartPolicy, and
// Stopped by the stop action. Images on Azure Container Registry, Artifact Registry and ECR
// hosts are pulled from pogi's registry (see docker.Options.ECR).
//
// ponytail: no exec or attach, no volumes (emptyDir, secret, Azure Files, git),
// probes, GPUs, virtual networks, init containers or identities; a restart of
// the first container drops the others' shared network until the group restarts.
package aci

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

const typ = "Microsoft.ContainerInstance/containerGroups"

const (
	hostSuffix = ".azurecontainer.localhost"
	pathPrefix = "/_azure/aci/"
)

type state struct {
	ARM     armres.Store    `json:"arm"`
	Stopped map[string]bool `json:"stopped,omitempty"` // groups stopped by the stop action, by lower-cased ID
}

// Service is the Container Instances emulator.
type Service struct {
	hub    *core.Hub
	mu     sync.Mutex
	st     state
	runs   map[string]*run // running groups by lower-cased ID
	ctx    context.Context
	cancel context.CancelFunc
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.Reset()
	act := map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"start": s.start, "stop": s.stop, "restart": s.restart}
	p := armres.Stored{Type: typ, Async: true, Mu: &s.mu, St: &s.st.ARM, Put: s.put, Delete: s.delete, View: s.view, Actions: act}.Provider()
	p.NoContent = []string{"start", "stop", "restart"} // the SDKs expect 204 of all three
	arm.Register(h, p)
	arm.Register(h, arm.Provider{Type: typ + "/containers", Get: s.getContainer, List: s.listContainers})
	arm.Register(h, arm.Provider{Type: typ + "/containers/logs", Doc: s.logs})
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azaci", Cloud: core.Azure, Title: "Container Instances", Protocol: "azure-json", Match: match}
}

// match claims the data plane: <label>.<location>.azurecontainer.localhost and /_azure/aci/.
func match(r *http.Request) bool {
	h, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		h = r.Host
	}
	return strings.HasSuffix(strings.ToLower(h), hostSuffix) || strings.HasPrefix(r.URL.Path, pathPrefix)
}

func (s *Service) Close() error {
	s.cancel()
	s.mu.Lock()
	runs := s.runs
	s.runs = map[string]*run{}
	s.mu.Unlock()
	for _, r := range runs {
		r.remove(s.hub.Docker())
	}
	return nil
}

func key(id arm.ID) string { return strings.ToLower(id.String()) }

// ---- the group document ----

// group is the part of a container group that decides what runs.
type group struct {
	Location   string
	Properties struct {
		Containers []struct {
			Name       string
			Properties struct {
				Image                string
				Command              []string
				Ports                []port
				EnvironmentVariables []struct{ Name, Value, SecureValue string }
				Resources            struct {
					Requests struct{ CPU, MemoryInGB float64 }
				}
			}
		}
		IPAddress struct {
			Ports        []port
			Type         string
			DNSNameLabel string `json:"dnsNameLabel"`
		}
		RestartPolicy string
		OSType        string
	}
}

type port struct {
	Port     int
	Protocol string
}

func parse(r map[string]any) (*group, error) {
	b, _ := json.Marshal(r)
	g := &group{}
	if err := json.Unmarshal(b, g); err != nil {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidRequestContent", "The request content was invalid and could not be deserialized: %v", err)
	}
	return g, nil
}

// hash identifies what a group runs: a PUT of the same one keeps its containers.
func (g *group) hash() string {
	b, _ := json.Marshal(g.Properties)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func (g *group) restartPolicy() string { return cmp.Or(g.Properties.RestartPolicy, "Always") }

func invalid(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "InvalidContainerGroup", format, a...)
}

func (g *group) validate(name string) error {
	if len(g.Properties.Containers) == 0 {
		return invalid("The container group '%s' must contain at least one container.", name)
	}
	seen := map[string]bool{}
	for _, c := range g.Properties.Containers {
		if c.Name == "" || c.Properties.Image == "" {
			return invalid("Every container of the group '%s' needs a name and an image.", name)
		}
		if seen[c.Name] {
			return invalid("The container name '%s' is used twice in the group '%s'.", c.Name, name)
		}
		seen[c.Name] = true
	}
	if rp := g.Properties.RestartPolicy; rp != "" && !slices.Contains([]string{"Always", "OnFailure", "Never"}, rp) {
		return invalid("The restart policy '%s' is not valid: use Always, OnFailure or Never.", rp)
	}
	if t := g.Properties.IPAddress.Type; t != "" && t != "Public" && t != "Private" {
		return invalid("The IP address type '%s' is not valid: use Public or Private.", t)
	}
	return nil
}

// ---- ARM ----

// restoreSecrets completes the properties of a PATCH, which is a GET (without the secure values of environment variables
// and the passwords of registry credentials) merged with the changes, from the stored group; a variable that has
// neither a value nor a secure value is not a valid one anyway. It also drops the computed instanceView.
func restoreSecrets(old, p map[string]any) {
	list := func(m map[string]any, k string) []map[string]any {
		var out []map[string]any
		l, _ := m[k].([]any)
		for _, x := range l {
			if e, _ := x.(map[string]any); e != nil {
				out = append(out, e)
			}
		}
		return out
	}
	for _, c := range list(p, "containers") {
		cp := armres.Props(c)
		delete(cp, "instanceView")
		for _, oc := range list(old, "containers") {
			if armres.Str(oc, "name") != armres.Str(c, "name") {
				continue
			}
			for _, e := range list(cp, "environmentVariables") {
				for _, oe := range list(armres.Props(oc), "environmentVariables") {
					if armres.Str(oe, "name") == armres.Str(e, "name") && e["value"] == nil && e["secureValue"] == nil {
						e["secureValue"] = oe["secureValue"]
					}
				}
			}
		}
	}
	for _, cr := range list(p, "imageRegistryCredentials") {
		for _, o := range list(old, "imageRegistryCredentials") {
			if armres.Str(o, "server") == armres.Str(cr, "server") && cr["password"] == nil {
				cr["password"] = o["password"]
			}
		}
	}
}

// put runs the group's containers, unless a group of the same containers runs already.
func (s *Service) put(c *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	delete(p, "instanceView") // computed, and in the body of a PATCH, which is a GET merged with changes
	delete(p, "provisioningState")
	s.mu.Lock()
	prev, _ := s.st.ARM.Get(id)
	s.mu.Unlock()
	restoreSecrets(armres.Props(prev), p)
	g, err := parse(r)
	if err != nil {
		return err
	}
	if err := g.validate(id.Name()); err != nil {
		return err
	}
	if g.Properties.IPAddress.Type == "" && (len(g.Properties.IPAddress.Ports) > 0 || g.Properties.IPAddress.DNSNameLabel != "") {
		p["ipAddress"].(map[string]any)["type"] = "Public"
	}
	k := key(id)
	s.mu.Lock()
	old := s.runs[k]
	s.mu.Unlock()
	if old != nil && old.spec == g.hash() {
		return nil
	}
	if old != nil {
		s.teardown(k)
	}
	run, err := s.launch(id, g)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.runs[k] = run
	delete(s.st.Stopped, k)
	s.mu.Unlock()
	return nil
}

func (s *Service) delete(c *core.Ctx, id arm.ID) {
	k := key(id)
	s.teardown(k)
	s.mu.Lock()
	delete(s.st.Stopped, k)
	s.mu.Unlock()
}

// view adds what the containers say to the stored group.
func (s *Service) view(c *core.Ctx, r map[string]any) map[string]any {
	id, _ := arm.ParseID(armres.Str(r, "id"))
	g, _ := parse(r)
	p := armres.Props(r)
	p["provisioningState"] = "Succeeded"
	if ip, _ := p["ipAddress"].(map[string]any); ip != nil {
		ip["ip"] = "127.0.0.1"
		if l := g.Properties.IPAddress.DNSNameLabel; l != "" {
			ip["fqdn"] = l + "." + armres.Str(r, "location") + hostSuffix + ":" + s.port(c)
			ip["dnsNameLabelReusePolicy"] = cmp.Or(ip["dnsNameLabelReusePolicy"], "Unsecure")
		}
	}
	armres.Defaults(p, map[string]any{"restartPolicy": "Always", "osType": "Linux", "sku": "Standard"})
	s.mu.Lock()
	run := s.runs[key(id)]
	stopped := s.st.Stopped[key(id)]
	s.mu.Unlock()
	cs, _ := p["containers"].([]any)
	for _, x := range cs {
		m, _ := x.(map[string]any)
		cp := armres.Props(m)
		delete(cp, "environmentVariablesSecure")
		if env, _ := cp["environmentVariables"].([]any); env != nil {
			for _, e := range env {
				delete(e.(map[string]any), "secureValue")
			}
		}
		cp["instanceView"] = run.containerView(armres.Str(m, "name"))
	}
	p["instanceView"] = map[string]any{"state": run.groupState(g.restartPolicy(), stopped), "events": []any{}}
	if creds, _ := p["imageRegistryCredentials"].([]any); creds != nil {
		for _, x := range creds {
			delete(x.(map[string]any), "password")
		}
	}
	return r
}

// port is pogi's port as the caller reaches it.
func (s *Service) port(c *core.Ctx) string {
	_, p, _ := net.SplitHostPort(cmp.Or(c.Host, armres.HostOf(s.hub.URL)))
	return cmp.Or(p, serverless.Port(s.hub))
}

func (s *Service) stored(id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.st.ARM.Get(id)
	if !ok {
		return nil, arm.NotFound(id)
	}
	return r, nil
}

// ---- actions ----

func (s *Service) start(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	r, err := s.stored(id)
	if err != nil {
		return nil, err
	}
	k := key(id)
	s.mu.Lock()
	run := s.runs[k]
	s.mu.Unlock()
	if run != nil && s.hub.Docker().Available() {
		if err := s.resume(k, run); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return nil, s.relaunch(k, id, r)
}

func (s *Service) stop(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	if _, err := s.stored(id); err != nil {
		return nil, err
	}
	k := key(id)
	s.mu.Lock()
	run := s.runs[k]
	s.st.Stopped[k] = true
	s.mu.Unlock()
	if run != nil {
		run.halt(s.hub.Docker())
	}
	return nil, nil
}

func (s *Service) restart(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	r, err := s.stored(id)
	if err != nil {
		return nil, err
	}
	return nil, s.relaunch(key(id), id, r)
}

// relaunch replaces the containers of a stored group with new ones.
func (s *Service) relaunch(k string, id arm.ID, r map[string]any) error {
	g, err := parse(r)
	if err != nil {
		return err
	}
	s.teardown(k)
	run, err := s.launch(id, g)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.runs[k] = run
	delete(s.st.Stopped, k)
	s.mu.Unlock()
	return nil
}

// ---- containers and logs ----

func (s *Service) containerDoc(id arm.ID, r map[string]any, name string) map[string]any {
	s.mu.Lock()
	run := s.runs[key(id)]
	s.mu.Unlock()
	return map[string]any{"id": id.Child("containers", name).String(), "name": name, "type": typ + "/containers",
		"properties": map[string]any{"instanceView": run.containerView(name)}}
}

func (s *Service) getContainer(c *core.Ctx, id arm.ID) (map[string]any, error) {
	r, err := s.stored(id.Parent())
	if err != nil {
		return nil, err
	}
	g, _ := parse(r)
	for _, x := range g.Properties.Containers {
		if strings.EqualFold(x.Name, id.Name()) {
			return s.containerDoc(id.Parent(), r, x.Name), nil
		}
	}
	return nil, arm.NotFound(id)
}

func (s *Service) listContainers(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	r, err := s.stored(parent)
	if err != nil {
		return nil, err
	}
	g, _ := parse(r)
	var out []map[string]any
	for _, x := range g.Properties.Containers {
		out = append(out, s.containerDoc(parent, r, x.Name))
	}
	return out, nil
}

// logs answers GET .../containers/<name>/logs with the container's output, the last ?tail= lines of it.
func (s *Service) logs(c *core.Ctx, parent arm.ID, q url.Values) (any, error) {
	if _, err := s.getContainer(c, parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	run := s.runs[key(parent.Parent())]
	s.mu.Unlock()
	lines := run.logs(s.hub.Docker(), parent.Name())
	if t, err := strconv.Atoi(q.Get("tail")); err == nil && t >= 0 && t < len(lines) {
		lines = lines[len(lines)-t:]
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	return map[string]any{"content": content}, nil
}

// ---- data plane ----

// find returns the running group a request is for, and the host port of its first port; lock held.
func (s *Service) find(r *http.Request) (*run, int) {
	label := ""
	if rest, ok := strings.CutPrefix(r.URL.Path, pathPrefix); ok {
		label, _, _ = strings.Cut(rest, "/")
	} else {
		label, _, _ = strings.Cut(r.Host, ".")
	}
	for _, k := range core.SortedKeys(s.runs) {
		run := s.runs[k]
		if strings.EqualFold(run.label, label) || strings.EqualFold(run.name, label) {
			return run, run.firstPort()
		}
	}
	return nil, 0
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "Invoke"
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	s.mu.Lock()
	run, p := s.find(r)
	s.mu.Unlock()
	switch {
	case run == nil:
		http.Error(w, "404 Not Found: no running container group answers to this name; use http://<dnsNameLabel>.<location>"+hostSuffix+":<port>/ or "+pathPrefix+"<group>/", http.StatusNotFound)
		return
	case p == 0:
		http.Error(w, "503 Service Unavailable: the group has no published port", http.StatusServiceUnavailable)
		return
	}
	if rest, ok := strings.CutPrefix(r.URL.Path, pathPrefix); ok {
		_, path, _ := strings.Cut(rest, "/")
		r = r.Clone(r.Context())
		r.URL.Path, r.URL.RawPath = "/"+path, ""
	}
	serverless.Proxy(w, r, "127.0.0.1:"+strconv.Itoa(p))
}

// ---- state and console ----

func (s *Service) Reset() {
	s.mu.Lock()
	runs := s.runs
	s.runs, s.st = map[string]*run{}, state{Stopped: map[string]bool{}}
	s.mu.Unlock()
	for _, r := range runs {
		r.remove(s.hub.Docker())
	}
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores the groups and starts the ones that were running again, in the background.
func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Stopped == nil {
		st.Stopped = map[string]bool{}
	}
	s.mu.Lock()
	s.st = st
	var todo []string
	for _, k := range core.SortedKeys(st.ARM) {
		if !st.Stopped[k] && s.runs[k] == nil {
			todo = append(todo, k)
		}
	}
	s.mu.Unlock()
	core.Go("aci.Load", func() {
		for _, k := range todo {
			if id, ok := arm.ParseID(k); ok && s.ctx.Err() == nil {
				if r, err := s.stored(id); err == nil {
					if err := s.relaunch(k, id, r); err != nil {
						s.hub.Log.Warn("container group did not start again", "group", id.Name(), "err", err)
					}
				}
			}
		}
	})
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Container groups", Columns: []string{"Name", "Location", "State", "Images", "Ports"}}
	for _, k := range core.SortedKeys(s.st.ARM) {
		r := s.st.ARM[k]
		g, _ := parse(r)
		var imgs []string
		for _, c := range g.Properties.Containers {
			imgs = append(imgs, c.Properties.Image)
		}
		t.Rows = append(t.Rows, []string{armres.Str(r, "name"), g.Location, s.runs[k].groupState(g.restartPolicy(), s.st.Stopped[k]),
			strings.Join(imgs, ", "), s.runs[k].portList()})
	}
	return []core.Table{t}
}

// Inspect shows a group with its containers' ids and the host ports that publish its ports.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, r := range s.st.ARM {
		if strings.EqualFold(armres.Str(r, "name"), id) {
			out := armres.Clone(r)
			if run := s.runs[k]; run != nil {
				out["pogi"] = run.inspect()
			}
			return out
		}
	}
	return nil
}
