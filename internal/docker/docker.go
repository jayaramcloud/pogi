// Package docker drives a container engine (Docker, Podman, Colima or
// OrbStack) through the Docker Engine API over its socket, with only the
// standard library. A Client finds its engine on first use; without one,
// Available is false and services fall back to metadata only.
package docker

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Labels on every container pogi runs.
const (
	LabelManaged  = "pogi.managed"  // "true"
	LabelOwner    = "pogi.owner"    // the Client that made it: host/pid/random
	LabelService  = "pogi.service"  // e.g. rds
	LabelResource = "pogi.resource" // e.g. the DB instance id
	LabelData     = "pogi.data"     // with saved state: the absolute data dir the container belongs to
)

// maxAPI is the newest Engine API (1.x) pogi speaks: Docker 29's.
const maxAPI = 54

var errNoEngine = errors.New("no container engine is running (start Docker, Podman, Colima or OrbStack, or set DOCKER_HOST)")

// CallLimit is how long one engine call (create, start, inspect, remove, exec) may take before pogi gives up on it.
// Pulls have their own limits; Logs and Wait follow their contexts. Tests shorten it.
var CallLimit = 2 * time.Minute

// engineError is a failure of the connection to the engine, as a plain sentence with the raw error after it.
type engineError struct {
	plain string
	err   error
}

func (e *engineError) Error() string {
	if e.err == nil {
		return e.plain
	}
	return e.plain + " — " + e.err.Error()
}
func (e *engineError) Unwrap() error { return e.err }

// errTimeout is a call that got no answer within CallLimit.
var errTimeout = &engineError{plain: "Docker stopped answering"}

// lost says what a connection error means: the engine isn't there (a dial failed) or it went away
// in the middle of a call. Other errors come back unchanged.
func lost(err error) error {
	var oe *net.OpError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &oe) && oe.Op == "dial":
		return &engineError{"Docker isn't running", err}
	case errors.As(err, &oe), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return &engineError{"Docker stopped answering", err}
	}
	return err
}

// Options configure a Client; the zero value finds the engine the docker CLI would.
type Options struct {
	Host    string            // engine socket or URL; empty: $DOCKER_HOST, then the usual sockets
	Network string            // network of managed containers; default "pogi"
	Images  map[string]string // image overrides by "<service>.<image name>", e.g. rds.postgres
	Keep    bool              // state is persisted: Close stops containers (the next run starts them again) and Sweep leaves them
	DataDir string            // with Keep: the absolute data dir; containers are labelled with it, so a later run finds them
	ECR     string            // pogi's registry, which serves the images of the registries in registryHosts, e.g. localhost:4510
	Log     *slog.Logger
}

// registryHosts are the registries of the clouds that pogi's own registry stands in for: an image on one is
// pulled from pogi's registry (Options.ECR) instead, under the path it has on the cloud's registry. An Azure
// Container Registry's name is the first element of that path, as in pogi's registry's names.
var registryHosts = []struct {
	re *regexp.Regexp
	to string // follows pogi's registry in the new reference
}{
	{regexp.MustCompile(`^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com(\.cn)?/`), ""}, // ECR
	{regexp.MustCompile(`^([a-z0-9]+)\.azurecr\.(?:io|cn|us)/`), "${1}/"},             // Azure Container Registry
	{regexp.MustCompile(`^[a-z0-9-]+-docker\.pkg\.dev/`), ""},                         // Artifact Registry
}

// localImage is the image reference with a cloud registry's host replaced by registry.
func localImage(image, registry string) string {
	for _, h := range registryHosts {
		if h.re.MatchString(image) {
			return h.re.ReplaceAllString(image, registry+"/"+h.to)
		}
	}
	return image
}

// Engine is the container engine a Client talks to.
type Engine struct {
	Name    string // Docker Desktop, OrbStack, Colima, Podman or Docker
	Socket  string // where it answered: unix:///… or tcp://…
	Version string // engine version
	API     string // Engine API version in use
}

// Client runs the containers of one pogi instance.
type Client struct {
	opt   Options
	owner string

	mu      sync.Mutex
	conn    *conn          // nil until an engine answers
	checked time.Time      // last lookup that found nothing
	netOK   bool           // the network exists
	addrs   map[int]string // nested: where the container that publishes a host port is (see Addr)

	tasks Tasks // downloads and starts, for the console

	nestMu   sync.Mutex
	nestDone bool
	self     *self // pogi's own container, when nested
}

type conn struct {
	Engine
	http *http.Client
	base string // http://docker/v1.54
	arch string // the engine's CPU, as Go names it: amd64, arm64
}

// New returns a Client; it looks for the engine on first use.
func New(o Options) *Client {
	o.Network = cmp.Or(o.Network, "pogi")
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	host, _ := os.Hostname()
	c := &Client{opt: o, owner: host + "/" + strconv.Itoa(os.Getpid()) + "/" + rand.Text()[:8]}
	if o.DataDir != "" { // marks the data dir as in use, so no sweep or clean takes its stopped containers
		if os.MkdirAll(o.DataDir, 0o700) == nil {
			os.WriteFile(filepath.Join(o.DataDir, ownerFile), []byte(c.owner), 0o600)
		}
	}
	return c
}

// ownerFile in a data dir names the pogi that uses it now.
const ownerFile = ".pogi-owner"

// DataInUse reports whether a pogi that still runs uses the data dir.
func DataInUse(dir string) bool {
	b, err := os.ReadFile(filepath.Join(dir, ownerFile))
	return err == nil && alive(strings.TrimSpace(string(b)))
}

// Name is the container name of a resource, pogi-<service>-<resource>, with
// the characters Docker refuses replaced by '-'.
func Name(service, resource string) string {
	return "pogi-" + strings.Map(func(r rune) rune {
		if 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9' || strings.ContainsRune("_.-", r) {
			return r
		}
		return '-'
	}, service+"-"+resource)
}

// Available reports whether an engine answers.
func (c *Client) Available() bool { return c.find() != nil }

// Engine describes the engine in use; ok is false when none answers.
func (c *Client) Engine() (e Engine, ok bool) {
	if cn := c.find(); cn != nil {
		return cn.Engine, true
	}
	return Engine{}, false
}

// HostAddr is the host containers reach pogi's listeners at. Engines behind
// a VM (Docker Desktop, OrbStack, Colima, Podman machine) forward
// host.docker.internal to this machine's loopback. A Linux engine on this
// kernel cannot route its bridges to 127.0.0.1, so there it is 127.0.0.1 and
// a container that calls pogi needs Network "host".
//
// Nested (pogi runs in a container), it is pogi's address on the pogi network.
func (c *Client) HostAddr() string {
	if s := c.nest(); s != nil && s.ip != "" {
		return s.ip
	}
	if cn := c.find(); cn != nil && cn.native() {
		return "127.0.0.1"
	}
	return "host.docker.internal"
}

// native: an engine on this Linux kernel, no VM between pogi and its containers.
func (cn *conn) native() bool {
	return runtime.GOOS == "linux" && (cn.Name == "Docker" || cn.Name == "Podman")
}

// find returns the engine connection; while none answers it looks again at
// most every two seconds (an engine may start after pogi).
// ponytail: an engine found once is kept; if it stops, calls fail with its
// connection error until pogi restarts. Drop c.conn on such errors if that bites.
func (c *Client) find() *conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil || time.Since(c.checked) < 2*time.Second {
		return c.conn
	}
	for _, s := range sockets(c.opt.Host) {
		if cn, err := dial(s); err == nil {
			c.conn = cn
			return cn
		}
	}
	c.checked = time.Now()
	return nil
}

// sockets lists where to look for an engine: the configured host, else
// $DOCKER_HOST, else the sockets of Docker, Docker Desktop, OrbStack, Colima
// and Podman.
func sockets(host string) []string {
	if host = cmp.Or(host, os.Getenv("DOCKER_HOST")); host != "" {
		if strings.HasPrefix(host, "/") {
			host = "unix://" + host
		}
		return []string{host}
	}
	home, _ := os.UserHomeDir()
	list := []string{"/var/run/docker.sock", filepath.Join(home, ".docker/run/docker.sock"), filepath.Join(home, ".orbstack/run/docker.sock"),
		filepath.Join(home, ".colima/default/docker.sock"), filepath.Join(home, ".colima/docker.sock")}
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		list = append(list, filepath.Join(d, "podman/podman.sock"))
	}
	list = append(list, "/run/podman/podman.sock")
	for i, p := range list {
		list[i] = "unix://" + p
	}
	return list
}

// tlsConfig is the client TLS setup of a tcp engine like the docker CLI's: ca.pem, cert.pem and key.pem
// from $DOCKER_CERT_PATH (default ~/.docker), and the server checked only when DOCKER_TLS_VERIFY is set.
// It returns nil when TLS is not wanted: a tcp:// address with neither DOCKER_TLS_VERIFY nor https.
func tlsConfig(scheme string) (*tls.Config, error) {
	verify := os.Getenv("DOCKER_TLS_VERIFY") != ""
	if !verify && scheme != "https" {
		return nil, nil
	}
	dir := os.Getenv("DOCKER_CERT_PATH")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".docker")
	}
	cfg := &tls.Config{InsecureSkipVerify: !verify}
	if ca, err := os.ReadFile(filepath.Join(dir, "ca.pem")); err == nil {
		cfg.RootCAs = x509.NewCertPool()
		cfg.RootCAs.AppendCertsFromPEM(ca)
	} else if verify && !os.IsNotExist(err) {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, "cert.pem")); err == nil {
		cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// dial pings the engine at sock and negotiates the API version.
// ponytail: unix sockets and tcp (plain or TLS) only; Windows named pipes and ssh:// are not supported.
func dial(sock string) (*conn, error) {
	u, err := url.Parse(sock)
	if err != nil {
		return nil, err
	}
	tr, host, scheme := &http.Transport{}, u.Host, "http"
	switch u.Scheme {
	case "unix":
		if _, err := os.Stat(u.Path); err != nil {
			return nil, err
		}
		tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", u.Path)
		}
		host = "docker"
	case "tcp", "http", "https":
		cfg, err := tlsConfig(u.Scheme)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			tr.TLSClientConfig, scheme = cfg, "https"
		}
	default:
		return nil, fmt.Errorf("unsupported engine address %s", sock)
	}
	cn := &conn{http: &http.Client{Transport: tr}, base: scheme + "://" + host}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var v struct {
		Version, ApiVersion, Arch string
		Platform                  struct{ Name string }
		Components                []struct{ Name string }
	}
	if err := cn.do(ctx, "GET", "/_ping", nil, nil); err != nil {
		return nil, err
	}
	if err := cn.do(ctx, "GET", "/version", nil, &v); err != nil {
		return nil, err
	}
	_, minor, _ := strings.Cut(v.ApiVersion, ".")
	n, _ := strconv.Atoi(minor)
	cn.Engine = Engine{Name: "Docker", Socket: sock, Version: v.Version, API: "1." + strconv.Itoa(min(n, maxAPI))}
	cn.base += "/v" + cn.API
	cn.arch = v.Arch
	switch {
	case strings.Contains(v.Platform.Name, "Docker Desktop"):
		cn.Name = "Docker Desktop"
	case strings.Contains(sock, "orbstack"):
		cn.Name = "OrbStack"
	case strings.Contains(sock, "colima"):
		cn.Name = "Colima"
	case strings.Contains(sock, "podman") || slices.ContainsFunc(v.Components, func(c struct{ Name string }) bool { return strings.Contains(c.Name, "Podman") }):
		cn.Name = "Podman"
	}
	return cn, nil
}

// apiError is an engine's refusal.
type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return "docker: " + e.msg }

// EngineDown reports whether err is the engine not answering (not running, cut off, or too slow).
func EngineDown(err error) bool {
	var e *engineError
	return errors.Is(err, errNoEngine) || errors.As(err, &e)
}

// NotFound reports whether the engine said the container (or image) doesn't exist.
func NotFound(err error) bool { return status(err) == http.StatusNotFound }

func status(err error) int {
	var e *apiError
	if errors.As(err, &e) {
		return e.status
	}
	return 0
}

// call sends a request: in is JSON, or a tar stream as an io.Reader; hdr are
// header name/value pairs. A status of 400 or more is an error with the
// engine's message.
func (cn *conn) call(ctx context.Context, method, path string, in any, hdr ...string) (*http.Response, error) {
	var body io.Reader
	ctype := "application/json"
	switch v := in.(type) {
	case nil:
	case io.Reader:
		body, ctype = v, "application/x-tar"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, cn.base+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", ctype)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		if hdr[i+1] != "" {
			req.Header.Set(hdr[i], hdr[i+1])
		}
	}
	resp, err := cn.http.Do(req)
	if err != nil {
		if context.Cause(ctx) == errTimeout {
			return nil, errTimeout
		}
		if ctx.Err() != nil { // the caller gave up
			return nil, err
		}
		return nil, lost(err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		var e struct{ Message string }
		json.Unmarshal(b, &e)
		return nil, &apiError{resp.StatusCode, cmp.Or(e.Message, strings.TrimSpace(string(b)), resp.Status)}
	}
	return resp, nil
}

// do is call for a request without a stream; out (when not nil) gets the JSON answer.
// It gives up after CallLimit.
func (cn *conn) do(ctx context.Context, method, path string, in, out any) error {
	ctx, cancel := context.WithTimeoutCause(ctx, CallLimit, errTimeout)
	defer cancel()
	return cn.run(ctx, method, path, in, out)
}

// run is do without the time limit, for calls that take as long as they take (Wait, Commit).
func (cn *conn) run(ctx context.Context, method, path string, in, out any) error {
	resp, err := cn.call(ctx, method, path, in)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
	} else {
		err = json.NewDecoder(resp.Body).Decode(out)
	}
	if err != nil && context.Cause(ctx) == errTimeout {
		return errTimeout
	}
	return lost(err)
}

func (c *Client) send(ctx context.Context, method, path string, in any, hdr ...string) (*http.Response, error) {
	cn := c.find()
	if cn == nil {
		return nil, errNoEngine
	}
	return cn.call(ctx, method, path, in, hdr...)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	cn := c.find()
	if cn == nil {
		return errNoEngine
	}
	return cn.do(ctx, method, path, in, out)
}

// long is do without the time limit.
func (c *Client) long(ctx context.Context, method, path string, in, out any) error {
	cn := c.find()
	if cn == nil {
		return errNoEngine
	}
	return cn.run(ctx, method, path, in, out)
}

// Container is what Create makes. Only Image is required.
type Container struct {
	Name       string            // see Name; empty: the engine picks one
	Image      string            // pulled when missing; docker.images in pogi.yaml may override it
	Service    string            // pogi.service label, and the prefix of image overrides
	Resource   string            // pogi.resource label
	Labels     map[string]string // more labels
	Env        []string          // KEY=value
	Cmd        []string
	Entrypoint []string
	WorkDir    string
	Ports      map[int]int // container tcp port → host port on 127.0.0.1; 0 picks a free one
	Binds      []string    // host-path:container-path[:ro], or volume:container-path
	ExtraHosts []string    // name:ip
	Network    string      // default: the pogi network; "host" shares the host's
	Aliases    []string    // more DNS names on the network
	Memory     int64       // bytes; 0: no limit
	CPUs       float64     // 0: no limit

	Privileged bool              // every device and capability of the host (k3s)
	CapAdd     []string          // more capabilities, e.g. NET_ADMIN
	Tmpfs      map[string]string // container path → mount options ("" for defaults)
}

// Resolve is the image a service runs for image: the docker.images override of "<service>.<image name>", and
// the images of cloud registries from pogi's own. Create uses it, and so does pogi pull.
func (c *Client) Resolve(service, image string) string {
	if o := c.opt.Images[service+"."+imageName(image)]; o != "" {
		image = o
	}
	if c.opt.ECR != "" {
		image = localImage(image, c.opt.ECR)
	}
	return image
}

// Create makes a container, pulling its image when missing, and returns its id.
func (c *Client) Create(ctx context.Context, ct *Container) (string, error) {
	cn := c.find()
	if cn == nil {
		return "", errNoEngine
	}
	image := c.Resolve(ct.Service, ct.Image)
	switch err := c.do(ctx, "GET", "/images/"+image+"/json", nil, nil); {
	case EngineDown(err): // the other calls would wait their turn to fail too
		return "", err
	case status(err) == http.StatusNotFound:
		if err := c.pullFor(ctx, image, ct.Service, ct.Resource); err != nil {
			return "", err
		}
	}
	network := cmp.Or(ct.Network, c.opt.Network)
	if network == c.opt.Network {
		if err := c.ensureNetwork(ctx); err != nil {
			return "", err
		}
	}
	labels := map[string]string{}
	maps.Copy(labels, ct.Labels)
	labels[LabelManaged], labels[LabelOwner], labels[LabelService], labels[LabelResource] = "true", c.owner, ct.Service, ct.Resource
	if c.opt.DataDir != "" {
		labels[LabelData] = c.opt.DataDir
	}
	for _, b := range ct.Binds { // make named volumes here, so they carry the labels too
		if v := volumeName(b); v != "" {
			if err := c.do(ctx, "POST", "/volumes/create", map[string]any{"Name": v, "Labels": labels}, nil); err != nil {
				return "", err
			}
		}
	}
	binds, mounts, copies := c.rebinds(cn, ct.Binds) // nested: pogi's folders as the engine sees them
	hosts := ct.ExtraHosts
	if network != "host" && !strings.HasPrefix(network, "container:") && (cn.Name == "Docker" || cn.Name == "Colima") { // the others resolve it themselves; a shared namespace has the hosts of its owner
		hosts = append(slices.Clip(hosts), "host.docker.internal:host-gateway")
	}
	exposed, bindings := map[string]struct{}{}, map[string][]map[string]string{}
	for p, h := range ct.Ports {
		k, hp := strconv.Itoa(p)+"/tcp", ""
		if h != 0 {
			hp = strconv.Itoa(h)
		}
		exposed[k], bindings[k] = struct{}{}, []map[string]string{{"HostIp": "127.0.0.1", "HostPort": hp}}
	}
	body := map[string]any{"Image": image, "Env": ct.Env, "Cmd": ct.Cmd, "Entrypoint": ct.Entrypoint, "WorkingDir": ct.WorkDir,
		"Labels": labels, "ExposedPorts": exposed,
		"HostConfig": map[string]any{"PortBindings": bindings, "Binds": binds, "Mounts": mounts, "ExtraHosts": hosts, "NetworkMode": network,
			"Memory": ct.Memory, "NanoCpus": int64(ct.CPUs * 1e9), "Privileged": ct.Privileged, "CapAdd": ct.CapAdd, "Tmpfs": ct.Tmpfs}}
	if network != "host" && !strings.HasPrefix(network, "container:") {
		body["NetworkingConfig"] = map[string]any{"EndpointsConfig": map[string]any{network: map[string]any{"Aliases": ct.Aliases}}}
	}
	var out struct{ ID string } // "Id"; json matches names in any case
	if err := c.do(ctx, "POST", "/containers/create?name="+url.QueryEscape(ct.Name), body, &out); err != nil {
		return "", err
	}
	for _, cp := range copies {
		if err := c.copyIn(ctx, out.ID, cp[0], cp[1]); err != nil {
			c.Remove(context.WithoutCancel(ctx), out.ID)
			return "", err
		}
	}
	return out.ID, nil
}

// Run creates a container and starts it; one that fails to start is removed.
func (c *Client) Run(ctx context.Context, ct *Container) (string, error) {
	id, err := c.Create(ctx, ct)
	if err != nil {
		return "", err
	}
	if err := c.Start(ctx, id); err != nil {
		c.Remove(context.WithoutCancel(ctx), id)
		return "", err
	}
	return id, nil
}

// ensureNetwork creates the pogi network unless it exists, once per Client.
func (c *Client) ensureNetwork(ctx context.Context) error {
	c.mu.Lock()
	ok := c.netOK
	c.mu.Unlock()
	if ok {
		return nil
	}
	err := c.do(ctx, "GET", "/networks/"+c.opt.Network, nil, nil)
	if status(err) == http.StatusNotFound {
		err = c.do(ctx, "POST", "/networks/create", map[string]any{"Name": c.opt.Network, "CheckDuplicate": true}, nil)
		if status(err) == http.StatusConflict { // made meanwhile by another pogi
			err = nil
		}
	}
	if err == nil {
		c.mu.Lock()
		c.netOK = true
		c.mu.Unlock()
	}
	return err
}

// Pull pulls an image (its latest tag when it names none) with the
// credentials of ~/.docker/config.json, logging progress.
func (c *Client) Pull(ctx context.Context, image string) error { return c.pullFor(ctx, image, "", "") }

// pullFor is Pull for the resource that needs the image; the console shows the download in its row.
func (c *Client) pullFor(ctx context.Context, image, service, resource string) (err error) {
	if !strings.Contains(image, "@") && !strings.Contains(image[strings.LastIndex(image, "/")+1:], ":") {
		image += ":latest" // without a tag the engine pulls every tag
	}
	tr := c.Track(service, resource, "download", image)
	defer func() { tr.End(err) }()
	cn := c.find()
	if cn == nil {
		return errNoEngine
	}
	err = c.pull(ctx, cn, image, "", tr)
	if p := emulated(cn.arch, err); p != "" {
		c.opt.Log.Warn("image has no build for this machine's CPU; pulling the "+p+" build, which runs under emulation (slower)", "image", image)
		err = c.pull(ctx, cn, image, p, tr)
	}
	return err
}

// emulated is the platform to pull an image for when it has no build for the
// engine's CPU: linux/amd64 on an arm64 engine (Apple Silicon), which Docker
// Desktop, OrbStack, Colima and Podman run under emulation. Some images, like
// the Azure Functions hosts, ship only amd64. It is "" otherwise.
func emulated(arch string, err error) string {
	if arch != "arm64" || err == nil {
		return ""
	}
	if m := err.Error(); strings.Contains(m, "no matching manifest") || strings.Contains(m, "no match for platform") ||
		strings.Contains(m, "no image found in manifest list") { // Docker; Docker with the containerd store; Podman
		return "linux/amd64"
	}
	return ""
}

// pull pulls one image, for platform when it is not "" (os/arch). It fails when no bytes arrive for
// stallLimit, or after pullLimit in all.
func (c *Client) pull(ctx context.Context, cn *conn, image, platform string, tr *Tracker) error {
	start := time.Now()
	c.opt.Log.Info("pulling image", "image", image)
	q := url.Values{"fromImage": {image}}
	if platform != "" {
		q.Set("platform", platform)
	}
	pctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	defer time.AfterFunc(pullLimit, func() { cancel(errTooLong) }).Stop()
	stall := time.AfterFunc(stallLimit, func() { cancel(errStalled) })
	defer stall.Stop()
	fail := func(err error) error {
		err = lost(err) // an engine that went away is not the registry's fault
		if ctx.Err() == nil && pctx.Err() != nil {
			err = context.Cause(pctx)
		}
		return &pullError{image, err}
	}
	resp, err := cn.call(pctx, "POST", "/images/create?"+q.Encode(), nil, "X-Registry-Auth", auth(image))
	if err != nil {
		return fail(err)
	}
	defer resp.Body.Close()
	ls := layers{}
	for dec := json.NewDecoder(&stallReader{resp.Body, stall}); ; {
		var m struct {
			ID, Status, Progress, Error string
			ProgressDetail              struct{ Current, Total int64 }
		}
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return fail(err)
		}
		if m.Error != "" {
			return fail(errors.New(m.Error))
		}
		if m.ID != "" {
			switch d := m.ProgressDetail; {
			case d.Total > 0 && m.Status == "Downloading": // not "Extracting", which counts again from 0
				tr.Progress(ls.add(m.ID, d.Current, d.Total))
			case m.Status == "Download complete" || m.Status == "Pull complete" || m.Status == "Already exists":
				if v, ok := ls[m.ID]; ok { // the layer is whole: its done is its total
					tr.Progress(ls.add(m.ID, v[1], v[1]))
				}
			}
		}
		if m.Progress == "" {
			c.opt.Log.Debug("pulling image", "image", image, "layer", m.ID, "status", m.Status)
		}
	}
	c.opt.Log.Info("pulled image", "image", image, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// HasImage reports whether the engine holds an image, without pulling it.
func (c *Client) HasImage(ctx context.Context, image string) (bool, error) {
	err := c.do(ctx, "GET", "/images/"+image+"/json", nil, nil)
	if status(err) == http.StatusNotFound {
		return false, nil
	}
	return err == nil, err
}

// Commit saves a container's filesystem as the image repo:tag.
func (c *Client) Commit(ctx context.Context, id, repo, tag string) error {
	q := url.Values{"container": {id}, "repo": {repo}, "tag": {tag}}
	return c.long(ctx, "POST", "/commit?"+q.Encode(), map[string]any{}, nil)
}

// RemoveImage deletes an image by name; a missing one, or one a container
// still uses, is no error.
func (c *Client) RemoveImage(ctx context.Context, image string) error {
	err := c.do(ctx, "DELETE", "/images/"+image, nil, nil)
	if s := status(err); s == http.StatusNotFound || s == http.StatusConflict {
		return nil
	}
	return err
}

// imageName is an image's last path part without tag or digest: postgres,
// valkey (valkey/valkey:8-alpine), python (public.ecr.aws/lambda/python:3.12).
func imageName(image string) string {
	n := image[strings.LastIndex(image, "/")+1:]
	n, _, _ = strings.Cut(n, "@")
	n, _, _ = strings.Cut(n, ":")
	return n
}

// Start starts a created or stopped container, and returns once the engine
// says which host port publishes each of its ports (see published).
func (c *Client) Start(ctx context.Context, id string) error {
	defer c.forget()
	if err := c.do(ctx, "POST", "/containers/"+id+"/start", nil, nil); err != nil {
		return err
	}
	return c.published(ctx, id)
}

// Stop stops a container, killing it after its stop timeout.
func (c *Client) Stop(ctx context.Context, id string) error {
	return c.do(ctx, "POST", "/containers/"+id+"/stop", nil, nil)
}

// Restart stops a container and starts it again, and returns once the engine
// says which host port publishes each of its ports (see published).
func (c *Client) Restart(ctx context.Context, id string) error {
	defer c.forget()
	if err := c.do(ctx, "POST", "/containers/"+id+"/restart", nil, nil); err != nil {
		return err
	}
	return c.published(ctx, id)
}

// publishWait is how long published waits for the engine to name the host ports.
var publishWait = 30 * time.Second

// published waits until the engine names a host port for every port the
// container publishes. Docker fills them in a moment after start returns, so
// under load an Inspect right after start can read port 0 for one of them:
// an ActiveMQ reboot then waited on 127.0.0.1:0 until its 5-minute limit.
// A container that is no longer running is left to the caller to explain.
func (c *Client) published(ctx context.Context, id string) error {
	end := time.Now().Add(publishWait)
	for {
		in, err := c.Inspect(ctx, id)
		if err != nil {
			return err
		}
		missing, nm := "", in.HostConfig.NetworkMode
		for k, bs := range in.HostConfig.PortBindings {
			if len(bs) > 0 && in.hostPort(k) == 0 && nm != "host" && !strings.HasPrefix(nm, "container:") { // those publish nothing
				missing = k
			}
		}
		switch {
		case missing == "" || !in.State.Running:
			return nil
		case time.Now().After(end):
			return fmt.Errorf("docker started %s but did not publish its port %s within %v", id, missing, publishWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// RemoveLeftover removes the container called name, so that this pogi can make one of that name: a leftover of
// a crashed run, or one it replaces. A container of another pogi that still runs, or one pogi did not make,
// stays, and the error says so: two pogis on one machine must not take each other's containers.
func (c *Client) RemoveLeftover(ctx context.Context, name string) error {
	i, err := c.Inspect(ctx, name)
	if NotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !c.mine(i.Config.Labels) {
		return fmt.Errorf("the container %s belongs to another pogi that is still running, or was not made by pogi: stop that one first", name)
	}
	return c.Remove(ctx, name)
}

// mine says whether this Client may take a container (by its labels): one it or its data dir made, or an orphan.
func (c *Client) mine(labels map[string]string) bool {
	own := labels[LabelOwner] == c.owner || c.opt.DataDir != "" && labels[LabelData] == c.opt.DataDir
	return labels[LabelManaged] == "true" && own || Orphan(labels)
}

// Remove deletes a container, running or not, with its anonymous volumes;
// a missing one is no error. When another call is already removing it
// (409), Remove waits until it is gone: until then its named volumes are
// still in use, and removing them right after would fail (a leaked volume).
func (c *Client) Remove(ctx context.Context, id string) error {
	defer c.forget()
	err := c.do(ctx, "DELETE", "/containers/"+id+"?force=1&v=1", nil, nil)
	switch status(err) {
	case http.StatusNotFound:
		return nil
	case http.StatusConflict:
		ctx, cancel := context.WithTimeout(ctx, CallLimit) // like any other engine call
		defer cancel()
		for {
			if _, err := c.Inspect(ctx, id); NotFound(err) {
				return nil
			} else if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	return err
}

// RemoveVolume deletes a named volume; a missing or busy one is no error.
func (c *Client) RemoveVolume(ctx context.Context, name string) error {
	err := c.do(ctx, "DELETE", "/volumes/"+url.PathEscape(name)+"?force=1", nil, nil)
	if s := status(err); s == http.StatusNotFound || s == http.StatusConflict {
		return nil
	}
	return err
}

// HasVolume reports whether a named volume exists.
func (c *Client) HasVolume(ctx context.Context, name string) bool {
	return c.do(ctx, "GET", "/volumes/"+url.PathEscape(name), nil, nil) == nil
}

// volumeName is the named volume a bind ("volume:/path"), or "" for a host path.
func volumeName(bind string) string {
	src, _, _ := strings.Cut(bind, ":")
	if len(src) < 2 || strings.ContainsAny(src, `/\~`) { // one letter is a Windows drive
		return ""
	}
	return src
}

// Volume is a Docker volume in Volumes.
type Volume struct {
	Name   string
	Labels map[string]string
}

// Volumes returns the volumes that carry every label (key=value).
func (c *Client) Volumes(ctx context.Context, labels ...string) ([]Volume, error) {
	f, _ := json.Marshal(map[string][]string{"label": labels})
	var out struct{ Volumes []Volume }
	err := c.do(ctx, "GET", "/volumes?filters="+url.QueryEscape(string(f)), nil, &out)
	return out.Volumes, err
}

// Info is what Inspect tells about a container.
type Info struct {
	ID    string `json:"Id"`
	Name  string
	Image string
	State struct {
		Status    string // created, running, exited…
		Running   bool
		ExitCode  int
		OOMKilled bool // the engine killed it for lack of memory
	}
	Config struct {
		Labels map[string]string
	}
	HostConfig struct {
		NetworkMode  string
		PortBindings map[string][]struct{ HostIp, HostPort string } // what was asked for: HostPort "" means any free one
	}
	Mounts          []Mount
	NetworkSettings struct {
		Ports    map[string][]struct{ HostIp, HostPort string }
		Networks map[string]struct {
			IPAddress string
			Aliases   []string
		}
	}
}

// HostPort is the host port that publishes a container's tcp port, or 0.
func (i *Info) HostPort(port int) int { return i.hostPort(strconv.Itoa(port) + "/tcp") }

// hostPort is the host port that publishes key ("5432/tcp"), or 0.
func (i *Info) hostPort(key string) int {
	for _, b := range i.NetworkSettings.Ports[key] {
		if p, _ := strconv.Atoi(b.HostPort); p > 0 {
			return p
		}
	}
	return 0
}

// Inspect describes a container: its state and the host ports it got.
func (c *Client) Inspect(ctx context.Context, id string) (*Info, error) {
	var i Info
	if err := c.do(ctx, "GET", "/containers/"+id+"/json", nil, &i); err != nil {
		return nil, err
	}
	return &i, nil
}

// Wait blocks until a container stops and returns its exit code.
func (c *Client) Wait(ctx context.Context, id string) (int, error) {
	var out struct{ StatusCode int }
	err := c.long(ctx, "POST", "/containers/"+id+"/wait", nil, &out)
	return out.StatusCode, err
}

// Watch calls exited once, with a plain sentence, when the container stops on its own: "killed: out of memory
// (raise Docker's memory limit)", or "stopped: exit <code> — <last log line>". It says nothing when ctx ends, or
// when the engine can't be asked (then nothing is known). Call it after the container starts.
func (c *Client) Watch(ctx context.Context, id string, exited func(note string)) {
	go func() {
		code, err := c.Wait(ctx, id)
		if err != nil || ctx.Err() != nil {
			return
		}
		ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if in, err := c.Inspect(ictx, id); err == nil {
			switch {
			case in.State.Running: // restarted since: whoever restarted it watches it again
				return
			case in.State.OOMKilled:
				exited("killed: out of memory (raise Docker's memory limit)")
				return
			}
		}
		note := "stopped: exit " + strconv.Itoa(code)
		if l := c.lastLine(ictx, id); l != "" {
			note += " — " + l
		}
		exited(note)
	}()
}

// lastLine is the last line a container wrote, or "".
func (c *Client) lastLine(ctx context.Context, id string) string {
	resp, err := c.send(ctx, "GET", "/containers/"+id+"/logs?stdout=1&stderr=1&tail=5", nil)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	last := ""
	w := &lines{fn: func(l string) {
		if l = strings.TrimSpace(l); l != "" {
			last = l
		}
	}}
	demux(resp.Body, w, w)
	w.flush()
	return last
}

// Logs calls line with each line a container wrote to stdout or stderr;
// with follow it reads on until the container stops (or ctx ends).
// ponytail: containers made with a TTY send raw streams; pogi makes none.
func (c *Client) Logs(ctx context.Context, id string, follow bool, line func(string)) error {
	q := "?stdout=1&stderr=1"
	if follow {
		q += "&follow=1"
	}
	resp, err := c.send(ctx, "GET", "/containers/"+id+"/logs"+q, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, errs := &lines{fn: line}, &lines{fn: line}
	err = demux(resp.Body, out, errs)
	out.flush()
	errs.flush()
	return err
}

// Exec runs a command in a running container and returns its output
// (stdout and stderr together) and exit code.
func (c *Client) Exec(ctx context.Context, id string, cmd ...string) ([]byte, int, error) {
	var x struct{ ID string }
	if err := c.do(ctx, "POST", "/containers/"+id+"/exec", map[string]any{"Cmd": cmd, "AttachStdout": true, "AttachStderr": true}, &x); err != nil {
		return nil, -1, err
	}
	resp, err := c.send(ctx, "POST", "/exec/"+x.ID+"/start", map[string]bool{"Detach": false, "Tty": false})
	if err != nil {
		return nil, -1, err
	}
	var out bytes.Buffer
	err = demux(resp.Body, &out, &out)
	resp.Body.Close()
	for i := 0; err == nil; i++ { // the stream can end a moment before the engine records the exit
		var st struct {
			Running  bool
			ExitCode int
		}
		if err = c.do(ctx, "GET", "/exec/"+x.ID+"/json", nil, &st); err == nil && !st.Running {
			return out.Bytes(), st.ExitCode, nil
		}
		if i == 100 {
			err = fmt.Errorf("docker: %s closed its output but still runs", cmd[0])
		}
		time.Sleep(20 * time.Millisecond)
	}
	return out.Bytes(), -1, err
}

// Summary is a container in a List.
type Summary struct {
	ID     string `json:"Id"`
	Names  []string
	Image  string
	State  string
	Labels map[string]string
	Ports  []struct {
		PrivatePort, PublicPort int
		Type                    string
	}
	NetworkSettings struct {
		Networks map[string]struct{ IPAddress string }
	}
}

// List returns the containers, running or not, that carry every label (key=value).
func (c *Client) List(ctx context.Context, labels ...string) ([]Summary, error) {
	f, _ := json.Marshal(map[string][]string{"label": labels})
	var out []Summary
	err := c.do(ctx, "GET", "/containers/json?all=1&filters="+url.QueryEscape(string(f)), nil, &out)
	return out, err
}

// CopyTo unpacks a tar archive into a directory of a container (created or running).
func (c *Client) CopyTo(ctx context.Context, id, dir string, tar io.Reader) error {
	return c.do(ctx, "PUT", "/containers/"+id+"/archive?path="+url.QueryEscape(dir), tar, nil)
}

// CopyFrom returns a tar archive of a file or directory in a container
// (created, running or stopped); the caller closes it.
func (c *Client) CopyFrom(ctx context.Context, id, path string) (io.ReadCloser, error) {
	resp, err := c.send(ctx, "GET", "/containers/"+id+"/archive?path="+url.QueryEscape(path), nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Sweep removes the containers and volumes that a pogi which no longer runs
// left behind; with persisted state they stay, as services resume them. pogi
// calls it at startup.
func (c *Client) Sweep() {
	if c.opt.Keep || !c.Available() {
		return
	}
	ctx := context.Background()
	list, err := c.List(ctx, LabelManaged+"=true")
	n := 0
	for _, s := range list {
		if s.Labels[LabelData] == "" && Orphan(s.Labels) && c.Remove(ctx, s.ID) == nil { // saved-state ones wait for their data dir's next run, or pogi clean
			n++
		}
	}
	vols, _ := c.Volumes(ctx, LabelManaged+"=true")
	for _, v := range vols {
		if v.Labels[LabelData] == "" && Orphan(v.Labels) {
			c.RemoveVolume(ctx, v.Name)
		}
	}
	if err != nil || n > 0 {
		c.opt.Log.Info("removed containers a previous pogi left behind", "count", n, "err", err)
	}
}

// alive reports whether the pogi owning a container may still run. Owners
// are host/pid/random, and only this host's processes can be checked.
// ponytail: a pogi that ran in a container or on another host is never swept.
func alive(owner string) bool {
	p := strings.Split(owner, "/")
	host, _ := os.Hostname()
	if len(p) != 3 || p[0] != host {
		return true
	}
	pid, err := strconv.Atoi(p[1])
	if err != nil {
		return true
	}
	proc, err := os.FindProcess(pid)
	return err == nil && !errors.Is(proc.Signal(syscall.Signal(0)), os.ErrProcessDone)
}

// Orphan reports whether something pogi made (a container or volume, by its
// labels) was left by a pogi that no longer runs. Nothing without the
// managed label counts, and nothing of a data dir in use: a container that a
// later run took over keeps the label of the run that made it.
func Orphan(labels map[string]string) bool {
	if labels[LabelManaged] != "true" || alive(labels[LabelOwner]) {
		return false
	}
	return labels[LabelData] == "" || !DataInUse(labels[LabelData])
}

// Mine lists this pogi's containers, running or not: with a data dir all
// those of it (an earlier run made some), else those this Client made.
// It finds none when no engine was ever used.
func (c *Client) Mine(ctx context.Context) []Summary {
	if !c.used() {
		return nil
	}
	label := LabelOwner + "=" + c.owner
	if c.opt.DataDir != "" {
		label = LabelData + "=" + c.opt.DataDir
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // a dead engine must not hold up shutdown
	defer cancel()
	list, _ := c.List(ctx, label)
	return list
}

// used reports whether an engine has answered before.
func (c *Client) used() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

// Close ends this pogi's containers (services remove their own transient
// ones before). With persisted state it stops them, and the next run starts
// them again; otherwise it removes them, and the volumes this Client made.
func (c *Client) Close() {
	if !c.used() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, s := range c.Mine(ctx) {
		switch {
		case !c.opt.Keep:
			wg.Go(func() { c.Remove(ctx, s.ID) })
		case s.State == "running":
			wg.Go(func() { c.Stop(ctx, s.ID) })
		}
	}
	wg.Wait()
	if c.opt.Keep {
		return
	}
	vols, _ := c.Volumes(ctx, LabelOwner+"="+c.owner)
	for _, v := range vols {
		c.RemoveVolume(ctx, v.Name)
	}
}

// demux copies an engine stream of stdcopy frames (a stream byte, three zero
// bytes and a big-endian length, then the payload) to stdout and stderr.
func demux(r io.Reader, stdout, stderr io.Writer) error {
	var h [8]byte
	for {
		if _, err := io.ReadFull(r, h[:]); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		w := stdout
		if h[0] != 1 {
			w = stderr
		}
		if _, err := io.CopyN(w, r, int64(binary.BigEndian.Uint32(h[4:]))); err != nil {
			return err
		}
	}
}

// lines calls fn with each line written to it; flush sends a last partial one.
type lines struct {
	fn  func(string)
	buf []byte
}

func (l *lines) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			if len(l.buf) > 1<<20 { // a runaway line
				l.flush()
			}
			return len(p), nil
		}
		l.fn(strings.TrimSuffix(string(l.buf[:i]), "\r"))
		l.buf = l.buf[i+1:]
	}
}

func (l *lines) flush() {
	if len(l.buf) > 0 {
		l.fn(string(l.buf))
		l.buf = nil
	}
}
