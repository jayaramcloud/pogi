package run

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

const hostSuffix = "run.localhost"

// isData is traffic for a service's containers: the Host is
// "<service>.run.localhost" or "<tag>---<service>.run.localhost", or the path
// is /_gcp/run/<project>/<region>/<service>/....
func isData(r *http.Request) bool {
	if _, ok := serverless.Label(r.Host, hostSuffix); ok {
		return true
	}
	return strings.HasPrefix(r.URL.Path, "/_gcp/run/")
}

// target is what a data-plane request names.
type target struct {
	project, region string // empty for host-style names
	service, tag    string
	path            string // what the container sees
}

func parseTarget(r *http.Request) (t target, ok bool) {
	label, host := serverless.Label(r.Host, hostSuffix)
	if !host {
		rest, found := strings.CutPrefix(r.URL.Path, "/_gcp/run/")
		p := strings.SplitN(rest, "/", 4)
		if !found || len(p) < 3 {
			return t, false
		}
		t.project, t.region, label = p[0], p[1], p[2]
		t.path = "/"
		if len(p) == 4 {
			t.path += p[3]
		}
	} else {
		t.path = r.URL.Path
	}
	t.tag, t.service = "", label
	if tag, svc, ok := strings.Cut(label, "---"); ok {
		t.tag, t.service = tag, svc
	}
	return t, t.service != ""
}

// find returns the service a target names; lock held.
func (s *Service) find(t target) *runpb.Service {
	for _, n := range core.SortedKeys(s.svcs) {
		p, l := splitName(n)
		if last(n) == t.service && (t.project == "" || t.project == p && t.region == l) {
			return s.svcs[n]
		}
	}
	return nil
}

// pick chooses the revision of svc for a request: the tag's, else one by traffic percent; lock held.
func (s *Service) pick(svc *runpb.Service, tag string) *runpb.Revision {
	resolve := func(t *runpb.TrafficTarget) *runpb.Revision {
		if t.Type == latestType {
			return s.revs[svc.LatestReadyRevision]
		}
		return s.revs[svc.Name+"/revisions/"+t.Revision]
	}
	total := int32(0)
	for _, t := range svc.Traffic {
		if tag != "" && t.Tag == tag {
			return resolve(t)
		}
		total += t.Percent
	}
	if tag != "" || total == 0 {
		return nil
	}
	n := rand.Int32N(total)
	for _, t := range svc.Traffic {
		if n -= t.Percent; n < 0 {
			return resolve(t)
		}
	}
	return nil
}

func (s *Service) serveData(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "Invoke"
	t, ok := parseTarget(r)
	if !ok {
		http.Error(w, "pogi: use http://<service>.run.localhost:<port>/ or /_gcp/run/<project>/<region>/<service>/", http.StatusNotFound)
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	s.mu.Lock()
	svc := s.find(t)
	var rev *runpb.Revision
	if svc != nil {
		rev = s.pick(svc, t.tag)
	}
	s.mu.Unlock()
	switch {
	case svc == nil:
		http.Error(w, "404 Not Found: no Cloud Run service named "+t.service, http.StatusNotFound)
		return
	case rev == nil:
		http.Error(w, "404 Not Found: no revision serves "+t.service+" (tag "+t.tag+")", http.StatusNotFound)
		return
	}
	in, err := s.instance(rev)
	if err != nil {
		http.Error(w, "503 Service Unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), rev.Timeout.AsDuration())
	defer cancel()
	r = r.Clone(ctx)
	r.URL.Path, r.URL.RawPath = t.path, ""
	serverless.Proxy(w, r, in.Addr)
}

// inst is the container of a revision, started on its first request.
type inst struct {
	ready chan struct{}
	in    *serverless.Instance
	err   error
}

func (x *inst) stop() {
	<-x.ready
	if x.in != nil {
		x.in.Stop()
	}
}

// instance returns the running container of rev, starting it when needed.
// ponytail: one container per revision and no scaling; it runs until the
// revision or service is deleted or pogi resets.
func (s *Service) instance(rev *runpb.Revision) (*serverless.Instance, error) {
	s.mu.Lock()
	x := s.insts[rev.Name]
	if x != nil {
		s.mu.Unlock()
		<-x.ready
		return x.in, x.err
	}
	x = &inst{ready: make(chan struct{})}
	s.insts[rev.Name] = x
	logs := s.logs[rev.Name]
	if logs == nil {
		logs = &serverless.Logs{}
		s.logs[rev.Name] = logs
		p, loc := splitName(rev.Name)
		svc := last(parent(parent(rev.Name)))
		logs.ToLogging(s.hub, p, "run.googleapis.com%2Fstdout", "cloud_run_revision",
			map[string]string{"service_name": svc, "revision_name": last(rev.Name), "location": loc, "configuration_name": svc})
	}
	s.mu.Unlock()
	x.in, x.err = s.start(rev, logs)
	if x.err != nil {
		s.mu.Lock()
		if s.insts[rev.Name] == x {
			delete(s.insts, rev.Name)
		}
		s.mu.Unlock()
	}
	close(x.ready)
	return x.in, x.err
}

// envOf is the container's variables in the order they apply: pogi's endpoints, the revision's, then Cloud Run's.
func (s *Service) envOf(c *runpb.Container, project string, system ...string) []string {
	env := serverless.Env(s.hub, s.hub.Docker().HostAddr(), project)
	for _, e := range c.Env {
		if _, ok := e.Values.(*runpb.EnvVar_Value); ok { // ponytail: secret and other value sources are not resolved
			env = append(env, e.Name+"="+e.GetValue())
		}
	}
	return append(env, system...)
}

func (s *Service) start(rev *runpb.Revision, logs *serverless.Logs) (*serverless.Instance, error) {
	if len(rev.Containers) == 0 {
		return nil, fmt.Errorf("revision %s has no container", rev.Name)
	}
	c := rev.Containers[0] // ponytail: the ingress container only, sidecars do not run
	project, _ := splitName(rev.Name)
	svc, rv := last(rev.Service), last(rev.Name)
	want := 8080
	if len(c.Ports) > 0 && c.Ports[0].ContainerPort > 0 {
		want = int(c.Ports[0].ContainerPort)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // an image may need pulling
	defer cancel()
	return serverless.Start(ctx, s.hub, want, func(port int) *docker.Container {
		return &docker.Container{Name: docker.Name("run", rv+"-"+core.RandHex(3)), Image: c.Image, Service: "run", Resource: rv,
			Cmd: c.Args, Entrypoint: c.Command, WorkDir: c.WorkingDir,
			Env: s.envOf(c, project, "PORT="+strconv.Itoa(port), "K_SERVICE="+svc, "K_REVISION="+rv, "K_CONFIGURATION="+svc)}
	}, logs, 2*time.Minute)
}

// deliver is Hub.Deliver for "//run.googleapis.com/projects/<p>/locations/<l>/services/<s>":
// the message becomes a POST to the service's container, the way Eventarc and
// Cloud Tasks reach Cloud Run:
//   - the body is Message.Body;
//   - the path is Message.Subject, "/" when empty (it may carry a query string);
//   - every Message.Attributes key becomes a request header ("ce-type",
//     "content-type", ...), so a CloudEvent in binary mode is ce-* headers;
//   - a response other than 2xx is returned as an error.
//
// The revision comes from the service's traffic split. The container starts
// if it is not running.
func (s *Service) deliver(name string, m *core.Message) error {
	name, _ = url.PathUnescape(name)
	s.mu.Lock()
	svc := s.svcs[name]
	var rev *runpb.Revision
	if svc != nil {
		rev = s.pick(svc, "")
	}
	s.mu.Unlock()
	switch {
	case svc == nil:
		return fmt.Errorf("Cloud Run service %s does not exist", name)
	case rev == nil:
		return fmt.Errorf("no revision of %s receives traffic", name)
	}
	in, err := s.instance(rev)
	if err != nil {
		return err
	}
	return serverless.Deliver(in.Addr, m)
}
