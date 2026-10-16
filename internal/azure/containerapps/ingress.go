package containerapps

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

// inst is the container of a revision, started on its first request or when the app is made.
type inst struct {
	ready chan struct{}
	addr  string // host:port that pogi reaches it at; empty for a container without ingress
	id    string
	err   error
	d     *docker.Client
}

// stop removes the container; it is safe on nil.
func (x *inst) stop() {
	if x == nil {
		return
	}
	<-x.ready
	if x.id != "" {
		serverless.CloseIdle()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		x.d.Remove(ctx, x.id)
	}
}

// take removes a revision's instance from the table, to be stopped without the lock; lock held.
func (s *Service) take(k string) *inst {
	x := s.insts[k]
	delete(s.insts, k)
	return x
}

// instance returns the running container of a revision, starting it when needed.
// ponytail: one container per revision and no scaling; it runs until the revision is deactivated or its app deleted.
func (s *Service) instance(app, rev map[string]any) (*inst, error) {
	id, _ := arm.ParseID(armres.Str(rev, "id"))
	k := key(id)
	s.mu.Lock()
	x := s.insts[k]
	if x != nil {
		s.mu.Unlock()
		<-x.ready
		return x, x.err
	}
	x = &inst{ready: make(chan struct{}), d: s.hub.Docker()}
	s.insts[k] = x
	logs := s.logs[k]
	if logs == nil {
		logs = &serverless.Logs{}
		s.logs[k] = logs
	}
	ctx := s.ctx
	s.mu.Unlock()
	s.start(ctx, x, app, rev, logs)
	if x.err != nil {
		s.mu.Lock()
		if s.insts[k] == x {
			delete(s.insts, k)
		}
		s.mu.Unlock()
	}
	close(x.ready)
	return x, x.err
}

// warm starts a revision's container ahead of its first request, for an app with replicas to keep or without ingress.
func (s *Service) warm(app, rev map[string]any) {
	if _, err := s.instance(app, rev); err != nil {
		s.hub.Log.Warn("container app revision did not start", "revision", armres.Str(rev, "name"), "err", err)
	}
}

// start runs the first container of the revision's template and waits until it listens, when it has ingress.
func (s *Service) start(ctx context.Context, x *inst, app, rev map[string]any, logs *serverless.Logs) {
	d := x.d
	if !d.Available() {
		x.err = serverless.ErrNoDocker
		return
	}
	p := armres.Props(app)
	tpl, _ := armres.Props(rev)["template"].(map[string]any)
	cs := objs(tpl, "containers")
	if len(cs) == 0 {
		x.err = fmt.Errorf("revision %s has no container", armres.Str(rev, "name"))
		return
	}
	c := cs[0] // ponytail: the first container only, sidecars and init containers do not run
	cfg, _ := p["configuration"].(map[string]any)
	ing, _ := cfg["ingress"].(map[string]any)
	port := int(num(ing["targetPort"]))
	appName, revName := armres.Str(app, "name"), armres.Str(rev, "name")
	envName := lastSeg(envOf(app))
	env := append(envList(c, secretsOf(p)), "CONTAINER_APP_NAME="+appName, "CONTAINER_APP_REVISION="+revName, "CONTAINER_APP_REPLICA_NAME="+revName+"-0",
		"CONTAINER_APP_ENV_DNS_SUFFIX="+strings.ToLower(envName)+hostSuffix, "CONTAINER_APP_HOSTNAME="+revName)
	mem, cpus := limits(c)
	ct := &docker.Container{Name: docker.Name("containerapps", revName+"-"+core.RandHex(3)), Image: armres.Str(c, "image"), Service: "containerapps", Resource: revName,
		Env: env, Cmd: strs(c["args"]), Entrypoint: strs(c["command"]), Memory: mem, CPUs: cpus}
	if port > 0 {
		ct.Ports = map[int]int{port: 0}
	}
	pull, cancel := context.WithTimeout(ctx, 10*time.Minute) // an image may need pulling
	defer cancel()
	id, err := d.Run(pull, ct)
	if err != nil {
		x.err = err
		return
	}
	x.id = id
	core.Go("containerapps.logs", func() { d.Logs(context.Background(), id, true, logs.Add) })
	if port == 0 {
		return
	}
	info, err := d.Inspect(pull, id)
	if err != nil || info.HostPort(port) == 0 {
		x.err = fmt.Errorf("container port %d is not published (%v)", port, err)
		return
	}
	x.addr = d.Addr(info.HostPort(port))
	err = serverless.WaitReady(x.addr, 2*time.Minute, func() bool {
		i, err := d.Inspect(ctx, id)
		return err == nil && i.State.Running
	})
	if err != nil {
		tail := logs.Lines()
		x.err = fmt.Errorf("container %s: %w\n%s", armres.Str(c, "image"), err, strings.Join(tail[max(0, len(tail)-10):], "\n"))
	}
}

// ---- data plane ----

// target is what a data-plane request names.
type target struct {
	app, env, rev string // rev is set for a revision's own address
	path          string // what the container sees
}

func parseTarget(r *http.Request) (t target, ok bool) {
	if rest, found := strings.CutPrefix(r.URL.Path, pathPrefix); found {
		p := strings.SplitN(rest, "/", 3)
		if len(p) < 2 {
			return t, false
		}
		t.env, t.app, t.path = p[0], p[1], "/"
		if len(p) == 3 {
			t.path += p[2]
		}
	} else {
		h, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			h = r.Host
		}
		label, found := strings.CutSuffix(strings.ToLower(h), hostSuffix)
		if !found {
			return t, false
		}
		t.app, t.env, _ = strings.Cut(label, ".")
		t.path = r.URL.Path
	}
	t.app = strings.ToLower(t.app)
	if app, rev, ok := strings.Cut(t.app, "--"); ok { // <app>--<suffix>: a revision
		t.app, t.rev = app, app+"--"+rev
	}
	t.env = strings.ToLower(t.env)
	return t, t.app != ""
}

// route finds the app and revision a target names; lock held.
func (s *Service) route(t target) (app, rev map[string]any, err error) {
	for _, k := range core.SortedKeys(s.st.ARM) {
		r := s.st.ARM[k]
		if armres.Str(r, "type") == typApp && strings.EqualFold(armres.Str(r, "name"), t.app) &&
			(t.env == "" || strings.EqualFold(lastSeg(envOf(r)), t.env)) {
			app = armres.Clone(r)
			break
		}
	}
	if app == nil {
		return nil, nil, errors.New("no container app named " + t.app)
	}
	p := armres.Props(app)
	cfg, _ := p["configuration"].(map[string]any)
	if _, ok := cfg["ingress"].(map[string]any); !ok {
		return app, nil, errors.New(t.app + " has no ingress")
	}
	if p["runningStatus"] == "Stopped" {
		return app, nil, errors.New(t.app + " is stopped")
	}
	id, _ := arm.ParseID(armres.Str(app, "id"))
	name := t.rev
	if name == "" { // by traffic weight
		w := s.weights(app)
		total := 0.0
		for _, n := range core.SortedKeys(w) {
			if r, ok := s.st.ARM.Get(id.Child("revisions", n)); ok && armres.Props(r)["active"] == true {
				total += w[n]
			}
		}
		if total == 0 {
			return app, nil, errors.New("no revision of " + t.app + " receives traffic")
		}
		pick := rand.Float64() * total
		for _, n := range core.SortedKeys(w) {
			r, ok := s.st.ARM.Get(id.Child("revisions", n))
			if !ok || armres.Props(r)["active"] != true {
				continue
			}
			if pick -= w[n]; pick < 0 {
				name = n
				break
			}
		}
	}
	rev, ok := s.st.ARM.Get(id.Child("revisions", name))
	if !ok || armres.Props(rev)["active"] != true {
		return app, nil, errors.New("no active revision " + name)
	}
	return app, rev, nil
}

func (s *Service) serveData(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "Invoke"
	t, ok := parseTarget(r)
	if !ok {
		http.Error(w, "pogi: use http://<app>.<environment>"+hostSuffix+":<port>/ or "+pathPrefix+"<environment>/<app>/", http.StatusNotFound)
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	s.mu.Lock()
	app, rev, err := s.route(t)
	s.mu.Unlock()
	if err != nil {
		http.Error(w, "404 Not Found: "+err.Error(), http.StatusNotFound)
		return
	}
	x, err := s.instance(app, rev)
	if err != nil || x.addr == "" {
		msg := "the revision has no ingress port"
		if err != nil {
			msg = err.Error()
		}
		http.Error(w, "503 Service Unavailable: "+msg, http.StatusServiceUnavailable)
		return
	}
	r = r.Clone(r.Context())
	r.URL.Path, r.URL.RawPath = t.path, ""
	serverless.Proxy(w, r, x.addr)
}
