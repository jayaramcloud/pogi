package functions

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/serverless"
)

// SDKDir is where "pogi lambda-sdks" installs the Functions Framework (Python
// packages below python/), the way Lambda's runtimes find boto3.
func SDKDir(dataDir string) string { return filepath.Join(dataDir, functionsDir) }

// proc is a running function: a Functions Framework process or a container.
type proc struct {
	ready chan struct{}
	addr  string
	err   error
	stopf func()
}

func (p *proc) stop() {
	<-p.ready
	if p.stopf != nil {
		p.stopf()
	}
}

// ensure returns where the function listens, starting it when needed.
func (s *Service) ensure(name string) (string, error) {
	s.mu.Lock()
	p := s.procs[name]
	if p != nil {
		s.mu.Unlock()
		<-p.ready
		return p.addr, p.err
	}
	fn := s.fns[name]
	if fn == nil {
		s.mu.Unlock()
		return "", fmt.Errorf("function %s does not exist", name)
	}
	p = &proc{ready: make(chan struct{})}
	s.procs[name] = p
	logs := &serverless.Logs{}
	s.logs[name] = logs
	if parts := strings.Split(name, "/"); len(parts) >= 6 {
		logs.ToLogging(s.hub, parts[1], "cloudfunctions.googleapis.com%2Fcloud-functions", "cloud_function", map[string]string{"function_name": parts[5], "region": parts[3]})
	}
	def := fn.p
	s.mu.Unlock()
	p.addr, p.stopf, p.err = s.start(def, logs)
	if p.err != nil {
		s.mu.Lock()
		if s.procs[name] == p {
			delete(s.procs, name)
		}
		s.mu.Unlock()
	}
	close(p.ready)
	return p.addr, p.err
}

func (s *Service) stopProc(name string) {
	s.mu.Lock()
	p := s.procs[name]
	delete(s.procs, name)
	s.mu.Unlock()
	if p != nil {
		p.stop()
	}
}

// ---- running ----

// source returns a directory holding the function's code. cleanup removes
// what pogi made for it.
func (s *Service) source(f *functionspb.Function) (dir string, cleanup func(), err error) {
	ss := f.BuildConfig.GetSource().GetStorageSource()
	if ss.GetObject() == "" {
		return "", nil, fmt.Errorf("the function has no storage_source (repository sources are not supported)")
	}
	var zipped []byte
	if ss.Bucket == "" { // a local path: a directory or a zip file
		p := strings.TrimPrefix(ss.Object, "file://")
		fi, err := os.Stat(p)
		switch {
		case err != nil:
			return "", nil, fmt.Errorf("source %s: %w", ss.Object, err)
		case fi.IsDir():
			return p, func() {}, nil
		}
		if zipped, err = os.ReadFile(p); err != nil {
			return "", nil, err
		}
	} else {
		r, _ := http.NewRequest(http.MethodGet, "http://localhost/storage/v1/b/"+ss.Bucket+"/o/"+url.PathEscape(ss.Object)+"?alt=media", nil)
		code, body := s.hub.DoService("gcs", r)
		if code != http.StatusOK {
			return "", nil, fmt.Errorf("source gs://%s/%s: %d %s", ss.Bucket, ss.Object, code, strings.TrimSpace(string(body)))
		}
		zipped = body
	}
	root := filepath.Join(s.hub.Cfg.DataDir, functionsDir)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", nil, err
	}
	if dir, err = os.MkdirTemp(root, "src-"); err != nil {
		return "", nil, err
	}
	cleanup = func() { os.RemoveAll(dir) }
	if err := unzip(zipped, dir); err != nil {
		cleanup()
		return "", nil, err
	}
	return dir, cleanup, nil
}

// maxUnzipped caps the total unpacked size of one source zip (500 MB).
const maxUnzipped = 500 << 20

func unzip(b []byte, dir string) error {
	zr, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return fmt.Errorf("source is not a zip file: %w", err)
	}
	if err := core.CheckZipReader(zr); err != nil {
		return err
	}
	left := int64(maxUnzipped) // bytes still allowed, across all entries
	for _, f := range zr.File {
		dst := filepath.Join(dir, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(dst, 0o755)
			continue
		}
		os.MkdirAll(filepath.Dir(dst), 0o755)
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err == nil {
			var n int64
			n, err = io.Copy(out, io.LimitReader(rc, left+1))
			out.Close()
			if left -= n; left < 0 {
				err = fmt.Errorf("the source is over the %d MB unzipped limit", maxUnzipped>>20)
			}
		}
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// frameworkCmd is how to run the Functions Framework natively: the command
// and the PYTHONPATH it needs, or ok false when it is not installed (or
// --lambda docker asks for containers).
func (s *Service) frameworkCmd(runtime string) (cmd []string, pythonPath string, ok bool) {
	if s.hub.Cfg.Lambda == "docker" {
		return nil, "", false
	}
	sdk := filepath.Join(SDKDir(s.hub.Cfg.DataDir), "python")
	if _, err := os.Stat(filepath.Join(sdk, "functions_framework")); err == nil {
		for _, n := range []string{"python3." + strings.TrimPrefix(runtime, "python3"), "python3", "python"} {
			if p, err := exec.LookPath(n); err == nil {
				return []string{p, "-m", "functions_framework"}, sdk, true
			}
		}
	}
	if p, err := exec.LookPath("functions-framework"); err == nil {
		return []string{p}, "", true
	}
	return nil, "", false
}

// start runs a function and waits until it listens.
func (s *Service) start(f *functionspb.Function, logs *serverless.Logs) (addr string, stop func(), err error) {
	dir, cleanup, err := s.source(f)
	if err != nil {
		return "", nil, err
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()
	signature := "http"
	if f.EventTrigger != nil {
		signature = "cloudevent"
	}
	cmd, pypath, native := s.frameworkCmd(f.BuildConfig.Runtime)
	if native {
		return s.startNative(f, dir, cleanup, cmd, pypath, signature, logs)
	}
	if s.hub.Cfg.Lambda == "native" {
		return "", nil, fmt.Errorf("--lambda native, but the Functions Framework is not installed: run `pogi lambda-sdks` or pip install functions-framework")
	}
	if !s.hub.Docker().Available() {
		return "", nil, fmt.Errorf("neither the Functions Framework (run `pogi lambda-sdks`) nor a container engine is available")
	}
	return s.startDocker(f, dir, cleanup, signature, logs)
}

var requirement = regexp.MustCompile(`(?m)^\s*[^#\s]`)

// env is the function's environment: pogi's endpoints, its own variables, then the Cloud Functions ones.
func (s *Service) env(f *functionspb.Function, host string, signature string) []string {
	project, _ := splitName(f.Name)
	id := last(f.Name)
	env := serverless.Env(s.hub, host, project)
	for _, k := range core.SortedKeys(f.ServiceConfig.GetEnvironmentVariables()) {
		env = append(env, k+"="+f.ServiceConfig.EnvironmentVariables[k])
	}
	return append(env, "FUNCTION_TARGET="+f.BuildConfig.EntryPoint, "FUNCTION_SIGNATURE_TYPE="+signature, "K_SERVICE="+id,
		"K_REVISION="+f.ServiceConfig.GetRevision(), "PYTHONUNBUFFERED=1")
}

func (s *Service) startNative(f *functionspb.Function, dir string, cleanup func(), cmd []string, pypath, signature string, logs *serverless.Logs) (string, func(), error) {
	deps := filepath.Join(dir, ".pogi-deps")
	if b, _ := os.ReadFile(filepath.Join(dir, "requirements.txt")); requirement.Match(b) {
		py, err := exec.LookPath("python3")
		if err != nil {
			return "", nil, fmt.Errorf("requirements.txt needs python3 and pip: %w", err)
		}
		pip := exec.Command(py, "-m", "pip", "install", "-q", "--disable-pip-version-check", "--target", deps, "-r", "requirements.txt")
		pip.Dir, pip.Stdout, pip.Stderr = dir, logs, logs
		if err := pip.Run(); err != nil {
			return "", nil, fmt.Errorf("pip install -r requirements.txt: %w", err)
		}
	}
	port := serverless.FreePort()
	ctx, cancel := context.WithCancel(context.Background())
	c := exec.CommandContext(ctx, cmd[0], append(cmd[1:], "--target", f.BuildConfig.EntryPoint, "--port", strconv.Itoa(port), "--signature-type", signature)...)
	core.OwnGroup(c)
	c.Cancel = func() error { return core.InterruptGroup(c) } // gunicorn's master stops its workers on SIGINT
	c.WaitDelay = 5 * time.Second
	c.Dir, c.Stdout, c.Stderr = dir, logs, logs
	c.Env = []string{"PYTHONPATH=" + strings.Join([]string{deps, dir, pypath}, string(os.PathListSeparator))}
	for _, k := range []string{"PATH", "HOME", "TMPDIR", "LANG", "SYSTEMROOT"} {
		if v, ok := os.LookupEnv(k); ok {
			c.Env = append(c.Env, k+"="+v)
		}
	}
	c.Env = append(c.Env, s.env(f, "127.0.0.1", signature)...)
	if err := c.Start(); err != nil {
		cancel()
		return "", nil, err
	}
	exited := make(chan struct{})
	go func() { c.Wait(); close(exited) }()
	stop := func() {
		serverless.CloseIdle()
		cancel()
		<-exited
		core.KillGroup(c) // workers that outlived the master
		s.hub.Closed("cloudfunctions", "process", "stopped", last(f.Name))
		cleanup()
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	err := serverless.WaitReady(addr, 2*time.Minute, func() bool {
		select {
		case <-exited:
			return false
		default:
			return true
		}
	})
	if err != nil {
		stop()
		tail := logs.Lines()
		return "", nil, fmt.Errorf("functions framework: %w\n%s", err, strings.Join(tail[max(0, len(tail)-10):], "\n"))
	}
	return addr, stop, nil
}

// startDocker runs the function in a python image with the Functions
// Framework installed at start (and the requirements.txt of the source).
// ponytail: pip runs on every start; bake an image if that gets slow. The
// source is bind-mounted, so a remote engine cannot see it.
func (s *Service) startDocker(f *functionspb.Function, dir string, cleanup func(), signature string, logs *serverless.Logs) (string, func(), error) {
	img := "python:3." + strings.TrimPrefix(f.BuildConfig.Runtime, "python3") + "-slim"
	id := last(f.Name)
	script := "pip install -q --disable-pip-version-check --no-cache-dir --target /deps functions-framework"
	if b, _ := os.ReadFile(filepath.Join(dir, "requirements.txt")); requirement.Match(b) {
		script += " -r requirements.txt"
	}
	in, err := serverless.Start(context.Background(), s.hub, 8080, func(port int) *docker.Container {
		return &docker.Container{Name: docker.Name("functions", id+"-"+core.RandHex(3)), Image: img, Service: "cloudfunctions", Resource: id,
			Cmd:     []string{"sh", "-c", script + " && exec python -m functions_framework --target \"$FUNCTION_TARGET\" --port " + strconv.Itoa(port) + " --signature-type " + signature},
			WorkDir: "/workspace", Binds: []string{dir + ":/workspace"},
			Env: append(s.env(f, s.hub.Docker().HostAddr(), signature), "PYTHONPATH=/deps:/workspace")}
	}, logs, 10*time.Minute)
	if err != nil {
		return "", nil, err
	}
	return in.Addr, func() { in.Stop(); cleanup() }, nil
}

// ---- triggers ----

// pushURL is where a topic's push subscription delivers messages for a function.
func (s *Service) pushURL(name string) string {
	project, region := splitName(name)
	return s.hub.URL + "/_gcp/functions/_pubsub/" + project + "/" + region + "/" + last(name)
}

// pubsub calls pogi's Pub/Sub REST API in-process.
func (s *Service) pubsub(method, path string, body any) (int, string) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, "http://localhost"+path, r)
	req.Header.Set("Content-Type", "application/json")
	code, resp := s.hub.DoService("pubsub", req)
	return code, strings.TrimSpace(string(resp))
}

// wireTrigger makes a push subscription for a Pub/Sub trigger.
// ponytail: Cloud Storage and other events need Eventarc to deliver them (Hub.Deliver).
func (s *Service) wireTrigger(name string) error {
	s.mu.Lock()
	fn := s.fns[name]
	var t *functionspb.EventTrigger
	if fn != nil {
		t = fn.p.EventTrigger
	}
	s.mu.Unlock()
	if t == nil || t.EventType != pubsubEvent {
		return nil
	}
	project, _ := splitName(t.PubsubTopic)
	sub := "projects/" + project + "/subscriptions/gcf-" + last(name) + "-" + last(t.TriggerRegion) + "-" + last(t.PubsubTopic)
	push := map[string]any{"pushEndpoint": s.pushURL(name)}
	code, msg := s.pubsub(http.MethodPut, "/v1/"+sub, map[string]any{"topic": t.PubsubTopic, "pushConfig": push, "ackDeadlineSeconds": 60})
	if code == http.StatusConflict { // a redeploy: the endpoint may have moved
		code, msg = s.pubsub(http.MethodPost, "/v1/"+sub+":modifyPushConfig", map[string]any{"pushConfig": push})
	}
	if code != http.StatusOK {
		return fmt.Errorf("Pub/Sub subscription for %s: %d %s", t.PubsubTopic, code, msg)
	}
	s.mu.Lock()
	if fn := s.fns[name]; fn != nil {
		fn.sub = sub
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) unwire(fn *function) {
	if fn.sub != "" {
		s.pubsub(http.MethodDelete, "/v1/"+fn.sub, nil)
	}
}

// push receives a Pub/Sub push for a function and calls it with the message
// as a CloudEvent in binary mode, the way Eventarc does.
func (s *Service) push(w http.ResponseWriter, r *http.Request, name string) {
	body := core.Body(r)
	var env struct {
		Message struct{ MessageID, PublishTime string }
	}
	json.Unmarshal(body, &env)
	s.mu.Lock()
	fn := s.fns[name]
	var t *functionspb.EventTrigger
	if fn != nil {
		t = fn.p.EventTrigger
	}
	s.mu.Unlock()
	if t == nil {
		http.Error(w, "no function "+name+" with a Pub/Sub trigger", http.StatusNotFound)
		return
	}
	addr, err := s.ensure(name)
	if err == nil {
		attr := func(v string) core.MessageAttribute { return core.MessageAttribute{DataType: "String", StringValue: v} }
		err = serverless.Deliver(addr, &core.Message{Body: string(body), Subject: "/", Attributes: map[string]core.MessageAttribute{
			"ce-specversion": attr("1.0"), "ce-id": attr(env.Message.MessageID), "ce-time": attr(env.Message.PublishTime),
			"ce-type": attr(pubsubEvent), "ce-source": attr("//pubsub.googleapis.com/" + t.PubsubTopic), "content-type": attr("application/json")}})
	}
	if err != nil {
		s.hub.Log.Warn("cloudfunctions: pubsub delivery failed", "function", name, "err", err)
		http.Error(w, err.Error(), http.StatusBadGateway) // Pub/Sub retries
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deliver is Hub.Deliver for "//cloudfunctions.googleapis.com/projects/<p>/locations/<l>/functions/<f>"
// (Eventarc's Cloud Storage events and other targets): the message becomes a
// POST to the function, with the same mapping as Cloud Run's:
//   - the body is Message.Body;
//   - the path is Message.Subject, "/" when empty;
//   - every Message.Attributes key becomes a request header ("ce-type",
//     "content-type", ...), so a CloudEvent in binary mode is ce-* headers;
//   - a response other than 2xx is returned as an error.
//
// The function starts if it is not running.
func (s *Service) deliver(name string, m *core.Message) error {
	name, _ = url.PathUnescape(name)
	addr, err := s.ensure(name)
	if err != nil {
		return err
	}
	return serverless.Deliver(addr, m)
}

// ---- data plane ----

// isData is traffic for a function: the Host is "<function>.cloudfunctions.localhost"
// or the path is /_gcp/functions/....
func isData(r *http.Request) bool {
	if _, ok := serverless.Label(r.Host, hostSuffix); ok {
		return true
	}
	return strings.HasPrefix(r.URL.Path, "/_gcp/functions/")
}

func (s *Service) serveData(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "Invoke"
	path := r.URL.Path
	var project, region, id string
	if label, ok := serverless.Label(r.Host, hostSuffix); ok {
		id = label
	} else {
		p := strings.SplitN(strings.TrimPrefix(path, "/_gcp/functions/"), "/", 5)
		if p[0] == "_pubsub" && len(p) == 4 && r.Method == http.MethodPost {
			c.Op = "PubsubPush"
			s.push(w, r, "projects/"+p[1]+"/locations/"+p[2]+"/functions/"+p[3])
			return
		}
		if len(p) < 3 {
			http.Error(w, "pogi: use /_gcp/functions/<project>/<region>/<function>/", http.StatusNotFound)
			return
		}
		project, region, id, path = p[0], p[1], p[2], "/"
		if len(p) > 3 {
			path += strings.Join(p[3:], "/")
		}
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	s.mu.Lock()
	var fn *function
	for _, n := range core.SortedKeys(s.fns) {
		if p, l := splitName(n); last(n) == id && (project == "" || p == project && l == region) {
			fn = s.fns[n]
			break
		}
	}
	s.mu.Unlock()
	switch {
	case fn == nil:
		http.Error(w, "404 Not Found: no function named "+id, http.StatusNotFound)
		return
	case fn.p.State == functionspb.Function_FAILED:
		http.Error(w, "503 Service Unavailable: the function failed to deploy", http.StatusServiceUnavailable)
		return
	}
	addr, err := s.ensure(fn.p.Name)
	if err != nil {
		http.Error(w, "503 Service Unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(fn.p.ServiceConfig.GetTimeoutSeconds())*time.Second)
	defer cancel()
	r = r.Clone(ctx)
	r.URL.Path, r.URL.RawPath = path, ""
	serverless.Proxy(w, r, addr)
}
