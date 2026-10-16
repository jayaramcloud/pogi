package containerapps

import (
	"cmp"
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

// execRun is an execution in progress.
type execRun struct {
	cancel  context.CancelFunc
	stopped atomic.Bool
}

func (s *Service) putJob(c *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	if _, err := s.environment(r); err != nil {
		return err
	}
	for _, k := range []string{"provisioningState", "eventStreamEndpoint", "outboundIpAddresses"} {
		delete(p, k)
	}
	p["environmentId"] = envOf(r)
	delete(p, "managedEnvironmentId")
	if err := validateTemplate(p); err != nil {
		return err
	}
	s.mu.Lock()
	old, _ := s.st.ARM.Get(id)
	s.mu.Unlock()
	cfg := obj(p, "configuration")
	if oc, ok := armres.Props(old)["configuration"].(map[string]any); ok {
		restoreSecrets(oc, cfg)
	}
	trigger := cfg["triggerType"]
	if trigger == nil {
		trigger = "Manual"
		cfg["triggerType"] = trigger
	}
	switch trigger {
	case "Manual", "Schedule", "Event":
	default:
		return invalid("InvalidRequestContent", "configuration.triggerType must be Manual, Schedule or Event, not '%v'.", trigger)
	}
	if num(cfg["replicaTimeout"]) <= 0 {
		return invalid("InvalidRequestContent", "configuration.replicaTimeout is required: the seconds a replica may run.")
	}
	return nil
}

func (s *Service) viewJob(c *core.Ctx, r map[string]any) map[string]any {
	p := armres.Props(r)
	cfg := obj(p, "configuration")
	hideSecrets(cfg)
	armres.Defaults(cfg, map[string]any{"replicaRetryLimit": 0})
	if cfg["triggerType"] == "Manual" {
		m := obj(cfg, "manualTriggerConfig")
		armres.Defaults(m, map[string]any{"parallelism": 1, "replicaCompletionCount": 1})
	}
	p["managedEnvironmentId"] = p["environmentId"]
	p["provisioningState"], p["outboundIpAddresses"], p["eventStreamEndpoint"] = "Succeeded", []any{"127.0.0.1"}, ""
	armres.Defaults(p, map[string]any{"workloadProfileName": "Consumption"})
	return r
}

func (s *Service) deleteJob(c *core.Ctx, id arm.ID) {
	k := key(id)
	s.mu.Lock()
	var cancel []*execRun
	for name, x := range s.runs {
		if strings.HasPrefix(name, k+"/executions/") {
			cancel = append(cancel, x)
		}
	}
	s.mu.Unlock()
	for _, x := range cancel {
		x.stopped.Store(true)
		x.cancel()
	}
}

// startJob is the start action: it makes an execution and runs it in the background. The body may be a template
// whose containers replace the job's.
func (s *Service) startJob(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
	s.mu.Lock()
	job, ok := s.st.ARM.Get(id)
	s.mu.Unlock()
	if !ok {
		return nil, arm.NotFound(id)
	}
	p := armres.Props(job)
	tpl := obj(p, "template")
	if len(objs(body, "containers")) > 0 { // the body is a template, whose containers replace the job's
		tpl = body
	}
	if err := validateTemplate(map[string]any{"template": tpl}); err != nil {
		return nil, err
	}
	cfg := obj(p, "configuration")
	name := id.Name() + "-" + randName(7)
	eid := id.Child("executions", name)
	exec := armres.Envelope(eid, map[string]any{"properties": map[string]any{"status": "Running", "startTime": nowStamp(),
		"template": map[string]any{"containers": toExecContainers(tpl)}}})
	ctx, cancel := context.WithCancel(s.ctx)
	run := &execRun{cancel: cancel}
	k := key(eid)
	s.mu.Lock()
	s.st.ARM.Put(eid, exec)
	s.runs[k] = run
	logs := &serverless.Logs{}
	s.logs[k] = logs
	s.mu.Unlock()
	m, _ := cfg["manualTriggerConfig"].(map[string]any)
	n, par := max(int(num(m["replicaCompletionCount"])), 1), max(int(num(m["parallelism"])), 1)
	retries, timeout := int(num(cfg["replicaRetryLimit"])), time.Duration(num(cfg["replicaTimeout"]))*time.Second
	first := objs(tpl, "containers")[0]
	core.Go("containerapps.execute", func() { s.execute(ctx, run, eid, job, first, n, par, retries, timeout, logs) })
	return map[string]any{"id": eid.String(), "name": name}, nil
}

func randName(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.IntN(len(chars))]
	}
	return string(b)
}

// toExecContainers is how an execution shows the containers it runs.
func toExecContainers(tpl map[string]any) []any {
	var out []any
	for _, c := range objs(tpl, "containers") {
		e := map[string]any{"name": c["name"], "image": c["image"]}
		for _, k := range []string{"args", "command", "resources"} {
			if c[k] != nil {
				e[k] = c[k]
			}
		}
		out = append(out, e)
	}
	return out
}

// execute runs the replicas of an execution and records how it ended.
func (s *Service) execute(ctx context.Context, run *execRun, eid arm.ID, job, c map[string]any, n, par, retries int, timeout time.Duration, logs *serverless.Logs) {
	defer run.cancel()
	var failed atomic.Bool
	var wg sync.WaitGroup
	sem := make(chan struct{}, par)
	secrets := secretsOf(armres.Props(job))
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		core.Go("containerapps.execute", func() {
			defer func() { <-sem; wg.Done() }()
			if !s.replica(ctx, eid, job, c, i, secrets, retries, timeout, logs) {
				failed.Store(true)
			}
		})
	}
	wg.Wait()
	status := "Succeeded"
	switch {
	case run.stopped.Load():
		status = "Stopped"
	case failed.Load():
		status = "Failed"
	}
	s.mu.Lock()
	if exec, ok := s.st.ARM.Get(eid); ok {
		props := armres.Props(exec)
		props["status"], props["endTime"] = status, nowStamp()
		s.st.ARM.Put(eid, exec)
	}
	delete(s.runs, key(eid))
	s.mu.Unlock()
}

// replica runs one replica's container to completion, again after a failure up to retries times.
func (s *Service) replica(ctx context.Context, eid arm.ID, job, c map[string]any, i int, secrets map[string]string, retries int, timeout time.Duration, logs *serverless.Logs) bool {
	d := s.hub.Docker()
	if !d.Available() {
		logs.Add("no container engine is running")
		return false
	}
	jobName, exec := armres.Str(job, "name"), eid.Name()
	env := append(envList(c, secrets), "CONTAINER_APP_JOB_NAME="+jobName, "CONTAINER_APP_JOB_EXECUTION_NAME="+exec,
		"CONTAINER_APP_REPLICA_NAME="+exec+"-"+randName(5))
	mem, cpus := limits(c)
	for attempt := 0; attempt <= retries && ctx.Err() == nil; attempt++ {
		ct := &docker.Container{Name: docker.Name("containerapps", exec+"-"+randName(6)), Image: armres.Str(c, "image"), Service: "containerapps", Resource: exec,
			Env: env, Cmd: strs(c["args"]), Entrypoint: strs(c["command"]), Memory: mem, CPUs: cpus}
		code, err := s.runOnce(ctx, d, ct, cmp.Or(timeout, 30*time.Minute), logs)
		if err != nil {
			logs.Add(err.Error())
		}
		if err == nil && code == 0 {
			return true
		}
	}
	return false
}

// runOnce runs a container until it exits or the time is up, and returns its exit code.
func (s *Service) runOnce(ctx context.Context, d *docker.Client, ct *docker.Container, timeout time.Duration, logs *serverless.Logs) (int, error) {
	pull, cancel := context.WithTimeout(ctx, 10*time.Minute) // an image may need pulling
	id, err := d.Create(pull, ct)
	cancel()
	if err != nil {
		return -1, err
	}
	defer d.Remove(context.Background(), id)
	if err := d.Start(ctx, id); err != nil {
		return -1, err
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	code, err := d.Wait(wctx, id)
	lctx, lcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer lcancel()
	d.Logs(lctx, id, false, logs.Add)
	return code, err
}

// stopExecution stops one execution, if it is running.
func (s *Service) stopExecution(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	s.mu.Lock()
	run := s.runs[key(id)]
	s.mu.Unlock()
	if run != nil {
		run.stopped.Store(true)
		run.cancel()
	}
	return nil, nil
}

// stopJob stops the executions of a job that are running.
func (s *Service) stopJob(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	k := key(id)
	s.mu.Lock()
	_, ok := s.st.ARM.Get(id)
	var runs []*execRun
	for name, x := range s.runs {
		if strings.HasPrefix(name, k+"/executions/") {
			runs = append(runs, x)
		}
	}
	s.mu.Unlock()
	if !ok {
		return nil, arm.NotFound(id)
	}
	for _, x := range runs {
		x.stopped.Store(true)
		x.cancel()
	}
	return nil, nil
}
