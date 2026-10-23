package appservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

// inst is the container of an app, started when the app is made or changed, or by a request.
type inst struct {
	ready chan struct{}
	in    *serverless.Instance
	err   error
}

// stop removes the container; it is safe on nil.
func (x *inst) stop() {
	if x == nil {
		return
	}
	<-x.ready
	if x.in != nil {
		x.in.Stop()
	}
}

// take removes an app's instance from the table, to be stopped without the lock; lock held.
func (s *Service) take(k string) *inst {
	x := s.insts[k]
	delete(s.insts, k)
	return x
}

// instance returns the app's running container, starting it when it is down.
// ponytail: one container per app; a start that failed is tried again by the next request.
func (s *Service) instance(id arm.ID) (*inst, error) {
	k := key(id)
	s.mu.Lock()
	x := s.insts[k]
	if x != nil {
		s.mu.Unlock()
		<-x.ready
		return x, x.err
	}
	x = &inst{ready: make(chan struct{})}
	s.insts[k] = x
	logs := s.logs[k]
	if logs == nil {
		logs = &serverless.Logs{}
		s.logs[k] = logs
	}
	ctx := s.ctx
	s.mu.Unlock()
	x.in, x.err = s.start(ctx, id, logs)
	s.mu.Lock()
	if x.err != nil {
		s.fails[k] = x.err.Error()
		if s.insts[k] == x {
			delete(s.insts, k)
		}
	} else {
		delete(s.fails, k)
	}
	s.mu.Unlock()
	close(x.ready)
	return x, x.err
}

// warm starts the app in the background, unless it is stopped.
func (s *Service) warm(id arm.ID) {
	core.Go("appservice.warm", func() {
		s.mu.Lock()
		r, ok := s.st.ARM.Get(id)
		s.mu.Unlock()
		if !ok || armres.Props(r)["state"] == "Stopped" || !isLinux(r) {
			return
		}
		if _, err := s.instance(id); err != nil {
			s.hub.Log.Warn("web app did not start", "app", id.Name(), "err", err)
		}
	})
}

// settings are the app settings as strings; lock held.
func (s *Service) settings(id arm.ID) map[string]string {
	out := map[string]string{}
	if r, ok := s.st.ARM.Get(id.Child("config", "appsettings")); ok {
		for k, v := range armres.Props(r) {
			out[k], _ = v.(string)
		}
	}
	return out
}

// spec is what runs for an app.
type spec struct {
	image   string
	port    int
	cmd     []string
	workDir string
	bind    string // code folder to mount, "" for an image
}

// isLinux says whether an app is a Linux one; the plan's reserved flag decides, as for the real service.
func isLinux(app map[string]any) bool {
	return strings.Contains(armres.Str(app, "kind"), "linux") || armres.Props(app)["reserved"] == true
}

const wwwroot = "/home/site/wwwroot"

// what to run when the app has no startup command
const (
	nodeStart   = `if grep -q '"start"' package.json 2>/dev/null; then exec npm start; fi; for f in server.js index.js app.js; do [ -f "$f" ] && exec node "$f"; done; echo "pogi: no start script, server.js, index.js or app.js" >&2; exit 1`
	pythonStart = `[ -f requirements.txt ] && pip install -q -r requirements.txt; exec python app.py`
)

// plan works out what to run from linuxFxVersion, the startup command and the settings.
func (s *Service) plan(id arm.ID, app map[string]any, settings map[string]string) (spec, error) {
	if !isLinux(app) {
		return spec{}, errors.New("pogi runs Linux web apps only: a Windows web app is metadata only")
	}
	var cfg map[string]any
	s.mu.Lock()
	if r, ok := s.st.ARM.Get(id.Child("config", "web")); ok {
		cfg = armres.Props(r)
	}
	code := s.st.Code[key(id)]
	s.mu.Unlock()
	stack, version, _ := strings.Cut(armres.Str(cfg, "linuxFxVersion"), "|")
	startup := armres.Str(cfg, "appCommandLine")
	sp := spec{port: 80}
	if p, err := strconv.Atoi(settings["WEBSITES_PORT"]); err == nil && p > 0 && p < 65536 {
		sp.port = p
	} else if settings["WEBSITES_PORT"] != "" {
		return sp, fmt.Errorf("WEBSITES_PORT is %q, not a port", settings["WEBSITES_PORT"])
	}
	_, portSet := settings["WEBSITES_PORT"]
	switch strings.ToUpper(stack) {
	case "DOCKER":
		if version == "" {
			return sp, errors.New("linuxFxVersion DOCKER| names no image")
		}
		sp.image, sp.cmd = version, strings.Fields(startup) // ponytail: split on spaces, no quoting
		return sp, nil
	case "NODE", "PYTHON":
	case "":
		return sp, errors.New("the app has no runtime: set linuxFxVersion to DOCKER|<image>, NODE|<version> or PYTHON|<version>")
	default:
		return sp, fmt.Errorf("pogi runs DOCKER, NODE and PYTHON apps; %s is not one of them (the app is metadata only)", stack)
	}
	if code == "" {
		return sp, errors.New("the app has no code: deploy a zip to /api/zipdeploy on its scm host")
	}
	if !portSet {
		sp.port = 8080
	}
	version = strings.TrimSuffix(strings.ToLower(version), "-lts")
	if strings.EqualFold(stack, "NODE") {
		sp.image = "node:" + cmpOr(version, "lts") + "-slim"
		sp.cmd = []string{"sh", "-c", cmpOr(startup, nodeStart)}
	} else {
		sp.image = "python:" + cmpOr(version, "3") + "-slim"
		sp.cmd = []string{"sh", "-c", cmpOr(startup, pythonStart)}
	}
	sp.workDir, sp.bind = wwwroot, filepath.Clean(code)+":"+wwwroot
	return sp, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// start runs the app's container and waits until it listens.
func (s *Service) start(ctx context.Context, id arm.ID, logs *serverless.Logs) (*serverless.Instance, error) {
	s.mu.Lock()
	app, ok := s.st.ARM.Get(id)
	settings := s.settings(id)
	s.mu.Unlock()
	if !ok {
		return nil, arm.NotFound(id)
	}
	sp, err := s.plan(id, app, settings)
	if err != nil {
		return nil, err
	}
	if sp.bind != "" {
		if _, err := os.Stat(strings.SplitN(sp.bind, ":", 2)[0]); err != nil {
			return nil, errors.New("the deployed code is gone: deploy it again")
		}
	}
	name := armres.Str(app, "name")
	env := []string{"WEBSITE_SITE_NAME=" + name, "WEBSITE_HOSTNAME=" + strings.ToLower(name) + hostSuffix, "WEBSITES_PORT=" + strconv.Itoa(sp.port), "PORT=" + strconv.Itoa(sp.port)}
	for _, k := range core.SortedKeys(settings) {
		env = append(env, k+"="+settings[k])
	}
	pull, cancel := context.WithTimeout(ctx, 10*time.Minute) // an image may need pulling
	defer cancel()
	return serverless.Start(pull, s.hub, sp.port, func(hostPort int) *docker.Container {
		e := env
		if hostPort != sp.port { // sharing the host's network: the app must listen where pogi looks
			e = append(append([]string(nil), env...), "WEBSITES_PORT="+strconv.Itoa(hostPort), "PORT="+strconv.Itoa(hostPort))
		}
		ct := &docker.Container{Name: docker.Name("appservice", name+"-"+core.RandHex(3)), Image: sp.image, Service: "azappservice", Resource: name,
			Env: e, Cmd: sp.cmd, WorkDir: sp.workDir}
		if sp.bind != "" {
			ct.Binds = []string{sp.bind}
		}
		return ct
	}, logs, 2*time.Minute)
}
