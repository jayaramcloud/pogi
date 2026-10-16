package aci

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

// run is the containers of one group.
type run struct {
	name, label, spec string
	restart           string

	mu     sync.Mutex
	ctx    context.Context // the watchers'; cancelled when the group stops
	cancel context.CancelFunc
	cs     []*ctr
	order  []int       // the group's published ports, as declared
	ports  map[int]int // group port → host port on 127.0.0.1
	halted bool
}

// ctr is one container of the group.
type ctr struct {
	name, id          string
	state             string // Running, Terminated or Waiting
	detail            string
	exit, restarts    int
	started, finished time.Time
}

func (c *ctr) running(detail string) {
	c.state, c.detail, c.started = "Running", detail, time.Now().UTC()
}

func (c *ctr) terminated(code int, detail string) {
	c.state, c.exit, c.detail, c.finished = "Terminated", code, detail, time.Now().UTC()
}

func containerName(id arm.ID, name string) string {
	n := id.Name() + "-" + name
	if !strings.EqualFold(id.ResourceGroup, core.AzureResourceGroup) {
		n += "-" + id.ResourceGroup
	}
	return docker.Name("aci", n)
}

// launch creates and starts the containers of a group; the first one owns the network the others join.
func (s *Service) launch(id arm.ID, g *group) (*run, error) {
	r := &run{name: id.Name(), label: g.Properties.IPAddress.DNSNameLabel, spec: g.hash(), restart: g.restartPolicy(), ports: map[int]int{}}
	r.ctx, r.cancel = context.WithCancel(s.ctx)
	for _, c := range g.Properties.Containers {
		r.cs = append(r.cs, &ctr{name: c.Name, state: "Waiting", detail: "Pending"})
	}
	for _, p := range g.Properties.IPAddress.Ports {
		if (p.Protocol == "" || strings.EqualFold(p.Protocol, "TCP")) && !slices.Contains(r.order, p.Port) {
			r.order = append(r.order, p.Port)
		}
	}
	d := s.hub.Docker()
	if !d.Available() {
		for _, c := range r.cs {
			c.detail = "no container engine is running"
		}
		return r, nil
	}
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Minute) // an image may need pulling
	defer cancel()
	first := ""
	for i, c := range g.Properties.Containers {
		p := c.Properties
		ct := &docker.Container{Name: containerName(id, c.Name), Image: p.Image, Service: "aci", Resource: id.Name(), Entrypoint: p.Command,
			Memory: int64(p.Resources.Requests.MemoryInGB * (1 << 30)), CPUs: p.Resources.Requests.CPU}
		for _, e := range p.EnvironmentVariables {
			ct.Env = append(ct.Env, e.Name+"="+cmp.Or(e.Value, e.SecureValue))
		}
		if i == 0 {
			ct.Ports = map[int]int{}
			for _, p := range r.order {
				ct.Ports[p] = 0
			}
		} else {
			ct.Network = "container:" + first
		}
		d.Remove(ctx, ct.Name) // what an earlier run left
		id, err := d.Create(ctx, ct)
		if err != nil {
			r.remove(d)
			return nil, core.Errorf(400, "InaccessibleImage", "The image '%s' in container group '%s' is not accessible. Please check the image and the registry credential. (%v)", p.Image, r.name, err)
		}
		r.cs[i].id = id
		if err := d.Start(ctx, id); err != nil {
			r.remove(d)
			return nil, core.Errorf(400, "ContainerGroupDeploymentFailed", "The container '%s' of group '%s' did not start: %v", c.Name, r.name, err)
		}
		r.cs[i].running("")
		if i == 0 {
			first = id
			if err := r.publish(ctx, d); err != nil {
				r.remove(d)
				return nil, err
			}
		}
	}
	for _, c := range r.cs {
		core.Go("aci.watch", func() { s.watch(r, r.ctx, c) })
	}
	return r, nil
}

// publish reads the host ports of the first container, which the engine picks anew each time it starts.
func (r *run) publish(ctx context.Context, d *docker.Client) error {
	info, err := d.Inspect(ctx, r.cs[0].id)
	if err != nil {
		return err
	}
	r.mu.Lock()
	for _, p := range r.order {
		r.ports[p] = info.HostPort(p)
	}
	r.mu.Unlock()
	return nil
}

// watch follows a container: it records the exit and restarts the container as the restart policy says.
func (s *Service) watch(r *run, ctx context.Context, c *ctr) {
	d := s.hub.Docker()
	for {
		code, err := d.Wait(ctx, c.id)
		if ctx.Err() != nil || err != nil {
			return
		}
		r.mu.Lock()
		c.terminated(code, map[bool]string{true: "Completed", false: "Error"}[code == 0])
		again := r.restart == "Always" || r.restart == "OnFailure" && code != 0
		r.mu.Unlock()
		if !again {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		if err := d.Start(ctx, c.id); err != nil {
			return
		}
		r.mu.Lock()
		c.running("")
		c.restarts++
		r.mu.Unlock()
		if c == r.cs[0] {
			r.publish(ctx, d)
		}
	}
}

// halt stops the containers, which stay for their logs.
func (r *run) halt(d *docker.Client) {
	r.mu.Lock()
	r.halted = true
	r.cancel()
	cs := slices.Clone(r.cs)
	r.mu.Unlock()
	for _, c := range cs {
		if c.id != "" {
			d.Stop(context.Background(), c.id)
		}
	}
	r.mu.Lock()
	for _, c := range cs {
		if c.id != "" {
			c.terminated(c.exit, "Stopped")
		}
	}
	r.mu.Unlock()
}

// resume starts a halted group's containers again.
func (s *Service) resume(k string, r *run) error {
	d := s.hub.Docker()
	r.mu.Lock()
	r.halted = false
	r.ctx, r.cancel = context.WithCancel(s.ctx)
	ctx, cs := r.ctx, slices.Clone(r.cs)
	r.mu.Unlock()
	for _, c := range cs {
		if c.id == "" {
			continue
		}
		if err := d.Start(ctx, c.id); err != nil {
			return core.Errorf(400, "ContainerGroupDeploymentFailed", "The container '%s' of group '%s' did not start: %v", c.name, r.name, err)
		}
		r.mu.Lock()
		c.running("")
		r.mu.Unlock()
		if c == cs[0] {
			r.publish(ctx, d)
		}
		core.Go("aci.watch", func() { s.watch(r, ctx, c) })
	}
	s.mu.Lock()
	delete(s.st.Stopped, k)
	s.mu.Unlock()
	return nil
}

// remove deletes the containers.
func (r *run) remove(d *docker.Client) {
	if r == nil {
		return
	}
	r.cancel()
	r.mu.Lock()
	cs := slices.Clone(r.cs)
	r.mu.Unlock()
	serverless.CloseIdle()
	for _, c := range cs {
		if c.id != "" {
			d.Remove(context.Background(), c.id)
		}
	}
}

// teardown forgets a group's run and deletes its containers.
func (s *Service) teardown(k string) {
	s.mu.Lock()
	r := s.runs[k]
	delete(s.runs, k)
	s.mu.Unlock()
	r.remove(s.hub.Docker())
}

func (r *run) firstPort() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.order) == 0 {
		return 0
	}
	return r.ports[r.order[0]]
}

func (r *run) portList() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, p := range r.order {
		out = append(out, fmt.Sprintf("%d→%d", p, r.ports[p]))
	}
	return strings.Join(out, ", ")
}

// groupState is the state the group's instanceView reports.
func (r *run) groupState(restart string, stopped bool) string {
	if stopped {
		return "Stopped"
	}
	if r == nil {
		return "Pending"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	running, waiting, failed := 0, 0, false
	for _, c := range r.cs {
		switch c.state {
		case "Running":
			running++
		case "Waiting":
			waiting++
		default:
			failed = failed || c.exit != 0
		}
	}
	switch {
	case waiting > 0:
		return "Pending"
	case running > 0 || restart == "Always":
		return "Running"
	case failed:
		return "Failed"
	}
	return "Succeeded"
}

func stamp(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Format(time.RFC3339)
}

// containerView is a container's instanceView.
func (r *run) containerView(name string) map[string]any {
	st := map[string]any{"state": "Waiting", "detailStatus": "Pending"}
	out := map[string]any{"restartCount": 0, "currentState": st, "events": []any{}}
	if r == nil {
		return out
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.cs {
		if !strings.EqualFold(c.name, name) {
			continue
		}
		st["state"], st["detailStatus"] = c.state, c.detail
		if !c.started.IsZero() {
			st["startTime"] = stamp(c.started)
		}
		if c.state == "Terminated" {
			st["exitCode"], st["finishTime"] = c.exit, stamp(c.finished)
		}
		out["restartCount"] = c.restarts
	}
	return out
}

// logs is what the container wrote.
func (r *run) logs(d *docker.Client, name string) []string {
	if r == nil || !d.Available() {
		return nil
	}
	r.mu.Lock()
	id := ""
	for _, c := range r.cs {
		if strings.EqualFold(c.name, name) {
			id = c.id
		}
	}
	r.mu.Unlock()
	var lines []string
	if id != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		d.Logs(ctx, id, false, func(l string) { lines = append(lines, l) })
	}
	return lines
}

func (r *run) inspect() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var cs []map[string]any
	for _, c := range r.cs {
		cs = append(cs, map[string]any{"name": c.name, "container": c.id, "state": c.state + " " + c.detail, "restarts": strconv.Itoa(c.restarts)})
	}
	ports := map[string]int{}
	for p, h := range r.ports {
		ports[strconv.Itoa(p)] = h
	}
	return map[string]any{"containers": cs, "hostPorts": ports}
}
