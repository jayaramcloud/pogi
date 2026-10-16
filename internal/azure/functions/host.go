package functions

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// images are the official Functions host images by runtime, under
// mcr.microsoft.com/azure-functions/. Override one with docker.images in
// pogi.yaml (azfunctions.python: …).
var images = map[string]string{
	"python":          "python:4-python3.12",
	"node":            "node:4-node22",
	"dotnet-isolated": "dotnet-isolated:4-dotnet-isolated8.0",
	"java":            "java:4-java17",
	"powershell":      "powershell:4-powershell7.4",
}

// ring keeps the last lines a host wrote, for the console.
type ring struct {
	mu    sync.Mutex
	lines []string
	part  string // an unfinished line (process output arrives in chunks)
}

const maxLines = 500

func (g *ring) add(line string) {
	g.mu.Lock()
	g.lines = append(g.lines, line)
	if len(g.lines) > maxLines {
		g.lines = g.lines[len(g.lines)-maxLines:]
	}
	g.mu.Unlock()
}

// Write makes a ring an io.Writer for a process's output.
func (g *ring) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.part += string(p)
	for {
		line, rest, ok := strings.Cut(g.part, "\n")
		if !ok {
			return len(p), nil
		}
		g.part = rest
		g.lines = append(g.lines, strings.TrimRight(line, "\r"))
	}
}

func (g *ring) tail() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.lines...)
}

// host is one running Functions host: a Core Tools process or a container.
// ready closes once start finished; then err or addr is set.
type host struct {
	ready  chan struct{}
	cancel context.CancelFunc
	err    error
	addr   string // host:port pogi dials
	key    string // master key, which pogi sends in place of the caller's
	how    string // "func" or "docker"
	stop   func() // set before ready closes
}

// start runs the app's host and waits until it answers. Core Tools is
// preferred; the official image is the fallback.
func (s *Service) start(ctx context.Context, a App, h *host, logs *ring) error {
	if a.Path == "" {
		return errors.New("the app has no code: set a code path or deploy a zip")
	}
	h.key = core.RandHex(16)
	var exited <-chan struct{}
	var err error
	if fn, e := exec.LookPath("func"); e == nil {
		h.how = "func"
		exited, err = s.runFunc(ctx, fn, a, h, logs)
	} else if d := s.h.Docker(); d.Available() {
		h.how = "docker"
		exited, err = s.runContainer(ctx, a, h, logs)
	} else {
		return errors.New("pogi cannot run Azure Functions here: install Azure Functions Core Tools (func) or start Docker; the app exists as metadata only")
	}
	if err != nil {
		return err
	}
	return s.await(ctx, h, exited, logs)
}

// await polls the host's status until it has loaded its functions.
// ponytail: waits up to 5 minutes (the image may be pulling, or an emulated
// amd64 host starting slowly); no per-app timeout setting.
func (s *Service) await(ctx context.Context, h *host, exited <-chan struct{}, logs *ring) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+h.addr+"/admin/host/status", nil)
		req.Header.Set("x-functions-key", h.key)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			var st struct{ State string }
			json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if st.State == "Running" || st.State == "Error" { // Error: the host answers, with its own message
				return nil
			}
		}
		select {
		case <-exited:
			return fmt.Errorf("the Functions host exited: %s", lastLines(logs, 5))
		case <-ctx.Done():
			return fmt.Errorf("the Functions host did not start: %w: %s", ctx.Err(), lastLines(logs, 5))
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func lastLines(g *ring, n int) string {
	l := g.tail()
	return strings.Join(l[max(0, len(l)-n):], " | ")
}

// runFunc starts `func host start` in the app's directory, with its settings as environment.
// It runs in its own process group, and stopping it signals the whole group,
// so the worker processes of the host go too. Not tested here (no func installed).
func (s *Service) runFunc(ctx context.Context, fn string, a App, h *host, logs *ring) (<-chan struct{}, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	env := s.env(a, "127.0.0.1")
	cmd := exec.Command(fn, "host", "start", "--port", strconv.Itoa(port))
	cmd.Dir = a.Path
	cmd.Env = os.Environ()
	for _, k := range core.SortedKeys(env) {
		cmd.Env = append(cmd.Env, k+"="+env[k])
	}
	cmd.Stdout, cmd.Stderr = logs, logs
	cmd.WaitDelay = 10 * time.Second
	core.OwnGroup(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	h.addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	exited := make(chan struct{})
	go func() { cmd.Wait(); close(exited) }()
	h.stop = func() {
		if core.InterruptGroup(cmd) != nil {
			core.KillGroup(cmd)
		}
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
		}
		core.KillGroup(cmd) // workers that outlived the host
		s.h.Closed("azfunctions", "process", "stopped", a.Name)
	}
	return exited, nil
}

// runContainer starts the official image with the code mounted at
// /home/site/wwwroot and a secrets file that gives pogi a master key.
func (s *Service) runContainer(ctx context.Context, a App, h *host, logs *ring) (<-chan struct{}, error) {
	d := s.h.Docker()
	img, ok := images[a.Runtime]
	if !ok {
		return nil, fmt.Errorf("unsupported runtime %q", a.Runtime)
	}
	root := "" // under the data dir when there is one: if pogi runs in a container, the engine can then find it
	if dd := s.h.Cfg.DataDir; dd != "" {
		root = filepath.Join(dd, "azfunctions")
		os.MkdirAll(root, 0o755)
	}
	secrets, err := os.MkdirTemp(root, "pogi-azfunctions-")
	if err != nil {
		return nil, err
	}
	keys := `{"masterKey":{"name":"master","value":"` + h.key + `","encrypted":false},"functionKeys":[],"systemKeys":[]}`
	if err := os.WriteFile(filepath.Join(secrets, "host.json"), []byte(keys), 0o666); err != nil {
		os.RemoveAll(secrets)
		return nil, err
	}
	os.Chmod(secrets, 0o777) // the host runs as another user
	os.Chmod(filepath.Join(secrets, "host.json"), 0o666)

	image := "mcr.microsoft.com/azure-functions/" + img
	hostAddr := d.HostAddr()
	if hostAddr != "127.0.0.1" {
		hostAddr = s.hostIP(ctx, image, hostAddr)
	}
	env := s.env(a, hostAddr)
	ct := &docker.Container{
		Name: docker.Name("azfunctions", a.Name+"-"+core.RandHex(3)), Image: image,
		Service: "azfunctions", Resource: a.Name,
		Binds: []string{a.Path + ":/home/site/wwwroot:ro", secrets + ":/azure-functions-host/Secrets"},
		Ports: map[int]int{80: 0},
	}
	env["AzureWebJobsScriptRoot"] = "/home/site/wwwroot"
	env["AzureWebJobsSecretStorageType"] = "files"
	if hostAddr == "127.0.0.1" { // an engine on this kernel cannot route its bridge to pogi's loopback ports
		port, err := freePort()
		if err != nil {
			os.RemoveAll(secrets)
			return nil, err
		}
		ct.Network, ct.Ports = "host", nil
		env["ASPNETCORE_URLS"] = "http://127.0.0.1:" + strconv.Itoa(port)
		h.addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	}
	for _, k := range core.SortedKeys(env) {
		ct.Env = append(ct.Env, k+"="+env[k])
	}
	id, err := d.Run(ctx, ct)
	if err != nil {
		os.RemoveAll(secrets)
		return nil, err
	}
	h.stop = func() {
		d.Remove(context.Background(), id)
		os.RemoveAll(secrets)
	}
	if h.addr == "" {
		info, err := d.Inspect(ctx, id)
		if err != nil || info.HostPort(80) == 0 {
			h.stop()
			return nil, fmt.Errorf("the Functions container has no published port: %v", err)
		}
		h.addr = d.Addr(info.HostPort(80))
	}
	core.Go("functions.logs", func() { d.Logs(context.Background(), id, true, logs.add) }) // ends when the container is removed
	exited := make(chan struct{})
	go func() { d.Wait(context.Background(), id); close(exited) }()
	return exited, nil
}

// hostIP is the address of this machine as the container sees it, as an IP.
// The host's Azure Storage client takes "http://name:port/account" as a path-style
// account only for an IP (or Azurite's ports), so host.docker.internal would make
// it read the account as the container and fail to create its lock containers.
// A throwaway container of the same image looks the name up; the answer is
// kept, and name is used when the lookup fails.
func (s *Service) hostIP(ctx context.Context, image, name string) string {
	s.probe.Lock()
	defer s.probe.Unlock()
	if s.ip != "" {
		return s.ip
	}
	d := s.h.Docker()
	id, err := d.Run(ctx, &docker.Container{Name: docker.Name("azfunctions", "probe-"+core.RandHex(3)), Image: image, Service: "azfunctions",
		Resource: "probe", Entrypoint: []string{"getent"}, Cmd: []string{"ahostsv4", "host.docker.internal"}})
	if err != nil {
		return name
	}
	defer d.Remove(context.WithoutCancel(ctx), id)
	d.Wait(ctx, id)
	d.Logs(ctx, id, false, func(line string) {
		if f := strings.Fields(line); len(f) > 0 && s.ip == "" && net.ParseIP(f[0]) != nil {
			s.ip = f[0]
		}
	})
	return cmp.Or(s.ip, name)
}

// env is what the host runs with: pogi's storage, the runtime, then the app's
// own settings verbatim. The app's AzureWebJobsStorage wins unless it is
// missing or the Azurite shorthand (UseDevelopmentStorage=true), which names a
// storage emulator the host cannot reach. host is where the host reaches pogi.
func (s *Service) env(a App, host string) map[string]string {
	env := map[string]string{
		"FUNCTIONS_WORKER_RUNTIME":    a.Runtime,
		"FUNCTIONS_EXTENSION_VERSION": "~4",
		"WEBSITE_SITE_NAME":           a.Name,
		// the host's own log lines go to the console, which pogi captures for Inspect
		"AzureFunctionsJobHost__logging__console__isEnabled": "true",
	}
	if conn := s.storage(host); conn != "" {
		env["AzureWebJobsStorage"] = conn
	}
	for k, v := range a.Settings {
		if k == "AzureWebJobsStorage" && (v == "" || strings.EqualFold(v, "UseDevelopmentStorage=true")) {
			continue
		}
		env[k] = v
	}
	return env
}

// storage is the Azurite-style connection string for pogi's Blob, Queue and
// Table ports as host reaches them; "" when those services have no port.
func (s *Service) storage(host string) string {
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + core.AzuriteKey + ";"
	for _, kind := range [][2]string{{"Blob", "azblob"}, {"Queue", "azqueue"}, {"Table", "aztable"}} {
		addr, ok := s.h.Endpoint("port:" + kind[1])
		if !ok {
			return ""
		}
		_, port, _ := net.SplitHostPort(addr)
		conn += kind[0] + "Endpoint=http://" + net.JoinHostPort(host, port) + "/devstoreaccount1;"
	}
	return conn
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}
