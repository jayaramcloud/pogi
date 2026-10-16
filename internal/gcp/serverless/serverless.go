// Package serverless is what Cloud Run and Cloud Functions share: running an
// HTTP container, waiting until it listens, proxying requests to it and the
// message-to-HTTP mapping of Hub.Deliver.
package serverless

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// ErrNoDocker is why a container cannot run.
var ErrNoDocker = errors.New("no container engine is running (start Docker, Podman, Colima or OrbStack, or set DOCKER_HOST)")

// Port is the port pogi listens on.
func Port(h *core.Hub) string {
	if u, err := url.Parse(h.URL); err == nil && u.Port() != "" {
		return u.Port()
	}
	if h.Cfg.Port > 0 {
		return strconv.Itoa(h.Cfg.Port)
	}
	return "7644"
}

// Label is the part of host before suffix: "<label>.<suffix>[:port]".
func Label(host, suffix string) (string, bool) {
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	return strings.CutSuffix(strings.ToLower(h), "."+suffix)
}

// FreePort returns a port nobody listens on.
func FreePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// Logs keeps the last lines a container or process wrote.
type Logs struct {
	mu    sync.Mutex
	lines []string
	tee   func(string)
}

// ToLogging also writes every line to Cloud Logging, when it is enabled, as
// an entry of the log logID (URL-encoded, e.g. "run.googleapis.com%2Fstdout")
// of a monitored resource.
func (l *Logs) ToLogging(h *core.Hub, project, logID, resourceType string, labels map[string]string) {
	if h.Service("logging") == nil {
		return
	}
	attrs := map[string]core.MessageAttribute{"resource.type": {StringValue: resourceType}, "resource.label.project_id": {StringValue: project}}
	for k, v := range labels {
		attrs["resource.label."+k] = core.MessageAttribute{StringValue: v}
	}
	target := "//logging.googleapis.com/projects/" + project + "/logs/" + logID
	l.mu.Lock()
	l.tee = func(line string) { h.Deliver(target, &core.Message{Body: line, Attributes: attrs}) }
	l.mu.Unlock()
}

const maxLines = 1000

// Add records one line.
func (l *Logs) Add(s string) {
	l.mu.Lock()
	l.lines = append(l.lines, s)
	if len(l.lines) > maxLines {
		l.lines = l.lines[len(l.lines)-maxLines:]
	}
	tee := l.tee
	l.mu.Unlock()
	if tee != nil {
		tee(s)
	}
}

// Lines returns a copy of the recorded lines.
func (l *Logs) Lines() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// Write makes Logs an io.Writer for processes' output.
// ponytail: a line split across two writes is recorded as two lines.
func (l *Logs) Write(p []byte) (int, error) {
	for _, s := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		l.Add(s)
	}
	return len(p), nil
}

// WaitReady blocks until something accepts connections on addr and keeps
// them open (a published container port accepts them before the container
// listens, then closes them), alive reports false, or d passes.
func WaitReady(addr string, d time.Duration, alive func() bool) error {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		if alive != nil && !alive() {
			return errors.New("it exited before listening")
		}
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			continue
		}
		c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, err = c.Read(make([]byte, 1))
		c.Close()
		if ne, ok := err.(net.Error); err == nil || ok && ne.Timeout() {
			return nil
		}
	}
	return fmt.Errorf("nothing listened on %s within %s", addr, d)
}

// Instance is a running container.
type Instance struct {
	Addr string // host:port that pogi reaches it at
	ID   string
	d    *docker.Client
}

// Start runs the container build makes and waits until it listens. want is the
// port it serves in a bridge network; on an engine without a VM (Linux) the
// container shares the host's network and serves on a free port instead, so
// build is called with the port to use. Its output goes to logs.
func Start(ctx context.Context, h *core.Hub, want int, build func(port int) *docker.Container, logs *Logs, wait time.Duration) (*Instance, error) {
	d := h.Docker()
	if !d.Available() {
		return nil, ErrNoDocker
	}
	port, shared := want, d.HostAddr() == "127.0.0.1"
	if shared {
		port = FreePort()
	}
	ct := build(port)
	if shared {
		ct.Network = "host"
	} else {
		ct.Ports = map[int]int{port: 0}
	}
	id, err := d.Run(ctx, ct)
	if err != nil {
		return nil, err
	}
	in := &Instance{ID: id, d: d, Addr: "127.0.0.1:" + strconv.Itoa(port)}
	if !shared {
		info, err := d.Inspect(ctx, id)
		if err != nil || info.HostPort(port) == 0 {
			in.Stop()
			return nil, fmt.Errorf("container port %d is not published (%v)", port, err)
		}
		in.Addr = d.Addr(info.HostPort(port))
	}
	if logs != nil {
		core.Go("serverless.logs", func() { d.Logs(context.Background(), id, true, logs.Add) })
	}
	err = WaitReady(in.Addr, wait, func() bool {
		i, err := d.Inspect(ctx, id)
		return err == nil && i.State.Running
	})
	if err != nil {
		in.Stop()
		tail := logs.Lines()
		return nil, fmt.Errorf("container %s: %w\n%s", ct.Image, err, strings.Join(tail[max(0, len(tail)-10):], "\n"))
	}
	return in, nil
}

// Stop removes the container.
func (i *Instance) Stop() {
	CloseIdle()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	i.d.Remove(ctx, i.ID)
}

// Env is the environment that points Google client libraries in a
// container (or process) on host at pogi.
func Env(h *core.Hub, host, project string) []string {
	hp := host + ":" + Port(h)
	return []string{"GOOGLE_CLOUD_PROJECT=" + project, "GCLOUD_PROJECT=" + project, "GCP_PROJECT=" + project,
		"STORAGE_EMULATOR_HOST=http://" + hp, "PUBSUB_EMULATOR_HOST=" + hp, "FIRESTORE_EMULATOR_HOST=" + hp, "DATASTORE_EMULATOR_HOST=" + hp}
}

var upstream = &http.Transport{
	DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	MaxIdleConnsPerHost: 32,
	IdleConnTimeout:     time.Minute,
}

// CloseIdle drops the kept-alive connections to containers and processes,
// which would hold a server open while it shuts down.
func CloseIdle() { upstream.CloseIdleConnections() }

// Proxy passes r to addr with the Host kept.
// ponytail: the request body is buffered (pogi's gateway reads it anyway).
func Proxy(w http.ResponseWriter, r *http.Request, addr string) {
	body := core.Body(r)
	(&httputil.ReverseProxy{
		Transport: upstream,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host, pr.Out.Host = "http", addr, pr.In.Host
			pr.SetXForwarded()
			pr.Out.Body, pr.Out.ContentLength, pr.Out.TransferEncoding = http.NoBody, 0, nil
			if len(body) > 0 {
				pr.Out.Body, pr.Out.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			}
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if errors.Is(err, context.DeadlineExceeded) {
				http.Error(w, "504 Gateway Timeout: the request exceeded the timeout", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, "503 Service Unavailable: "+err.Error(), http.StatusServiceUnavailable)
		},
	}).ServeHTTP(w, r)
}

// Deliver is the mapping of core.Message onto an HTTP endpoint that Cloud Run
// and Cloud Functions use for Hub.Deliver (Eventarc, Cloud Tasks, ...):
//   - the request is a POST of Message.Body to addr;
//   - its path is Message.Subject (default "/"), and may carry a query string;
//   - each Message.Attributes key becomes a header, set from StringValue (or
//     BinaryValue), so "ce-type" and "content-type" make a CloudEvent in
//     binary mode; without content-type the body is application/json when it
//     is JSON and text/plain otherwise;
//   - a non-2xx response, or none, is an error.
func Deliver(addr string, m *core.Message) error {
	path := m.Subject
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, strings.NewReader(m.Body))
	if err != nil {
		return err
	}
	for k, a := range m.Attributes {
		v := a.StringValue
		if v == "" {
			v = string(a.BinaryValue)
		}
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", map[bool]string{true: "application/json", false: "text/plain"}[json.Valid([]byte(m.Body))])
	}
	if t := core.Traceparent(m.Trace); t != "" {
		req.Header.Set("traceparent", t)
	}
	resp, err := upstream.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s answered %s: %s", addr, resp.Status, bytes.TrimSpace(b))
	}
	return nil
}
