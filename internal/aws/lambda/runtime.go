package lambda

import (
	"archive/zip"
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

//go:embed bootstrap/python.py bootstrap/node.mjs
var bootstraps embed.FS

// spec is everything needed to run one function version.
type spec struct {
	key                string // pool key; changes whenever code or config change
	name, version, arn string
	account, region    string
	runtime, handler   string
	packageType, image string
	imageConfig        *ImageConfig
	code               []byte // zip
	codeDir            string // hot-reload directory (used instead of code)
	codeSha            string
	layers             [][]byte
	env                map[string]string
	role               string // execution role; the function's credentials act as it
	timeout            time.Duration
	memory             int
	vpcSubnets         []string // VpcConfig.SubnetIds: where the function's container resolves private zones
}

type invocation struct {
	id, arn, clientCtx string // arn as invoked: instances serve a version, whatever alias called it
	trace              string // the flow this run belongs to, passed to the runtime
	payload            []byte
	deadline           time.Time
	done               chan outcome
}

type outcome struct {
	body    []byte
	errType string // "" on success
	logs    string
}

type instance struct {
	id      string
	pool    *pool
	spec    *spec
	ln      net.Listener
	srv     *http.Server
	cmd     *exec.Cmd
	docker  string // container name
	next    chan *invocation
	dead    chan struct{}
	killed  sync.Once
	gone    atomic.Bool  // set by kill: the instance must never be pooled again
	born    time.Time    // when the instance started
	asked   atomic.Int64 // when the runtime first asked for an invocation (GET …/invocation/next), in Unix nanoseconds
	stream  string
	mu      sync.Mutex
	current *invocation
	logs    bytes.Buffer  // logs of the current invocation
	out     io.Writer     // a native runtime's output pipe; flush writes its mark here
	marks   int           // marks flush has written
	seen    int           // marks the output reader has met
	flushed chan struct{} // closed when the reader has met every mark written
	initErr []byte
	used    time.Time
	ext     extensions // the Extensions API, see extensions.go
	reason  string     // why the instance is stopped, when not an idle spin-down: "timeout"
}

// pool keeps warm instances per function version. Each instance serves one
// invocation at a time; concurrent invocations start more of them.
type pool struct {
	svc   *Service
	mu    sync.Mutex
	idle  map[string][]*instance
	count map[string]int // live instances, busy or idle
	floor map[string]int // instances provisioned concurrency keeps started
	all   map[*instance]bool
	stop  chan struct{}
}

func newPool(s *Service) *pool {
	return &pool{svc: s, idle: map[string][]*instance{}, count: map[string]int{}, floor: map[string]int{}, all: map[*instance]bool{}, stop: make(chan struct{})}
}

// warm is how many idle instances a function version keeps (pogi.yaml lambda.warmPool).
// ponytail: per version, not per function; an alias splitting traffic keeps two pools.
func (p *pool) warm() int {
	if n := p.svc.hub.Cfg.WarmPool; n > 0 {
		return n
	}
	return 4
}

// keepAlive is how long an idle instance stays warm (LocalStack: LAMBDA_KEEPALIVE_MS).
func keepAlive() time.Duration {
	if ms, err := strconv.Atoi(os.Getenv("LAMBDA_KEEPALIVE_MS")); err == nil {
		return time.Duration(ms) * time.Millisecond
	}
	return 10 * time.Minute
}

// expire drops idle instances whose runtime exited, then stops those idle
// for longer than keepAlive, oldest first, down to the floor provisioned
// concurrency keeps (which the next provision tops up again).
func (p *pool) expire(now time.Time) {
	p.mu.Lock()
	var old []*instance
	for key, list := range p.idle {
		spare := p.count[key] - p.floor[key]
		keep := list[:0]
		for _, in := range list { // oldest first: release appends
			select {
			case <-in.dead:
				old = append(old, in)
				spare--
				continue
			default:
			}
			if spare > 0 && now.Sub(in.used) > keepAlive() {
				old = append(old, in)
				spare--
			} else {
				keep = append(keep, in)
			}
		}
		p.idle[key] = keep
	}
	p.mu.Unlock()
	for _, in := range old {
		p.kill(in)
	}
}

func (p *pool) setFloor(floor map[string]int) {
	p.mu.Lock()
	p.floor = floor
	p.mu.Unlock()
}

// fill starts idle instances of s until it has n, trying at most n starts
// (a runtime that dies at once is retried on the next Tick, not in a loop).
func (p *pool) fill(s *spec, n int) error {
	for range n {
		p.mu.Lock()
		if p.count[s.key] >= n {
			p.mu.Unlock()
			return nil
		}
		p.count[s.key]++
		p.mu.Unlock()
		in, err := p.start(s)
		if err != nil {
			p.mu.Lock()
			p.count[s.key]--
			p.mu.Unlock()
			return err
		}
		p.release(in)
	}
	return nil
}

// close kills every instance.
func (p *pool) close() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.mu.Lock()
	var all []*instance
	for in := range p.all {
		all = append(all, in)
	}
	p.mu.Unlock()
	for _, in := range all {
		p.kill(in)
	}
}

// invalidate kills the instances of a function (code or config changed, or deleted).
func (p *pool) invalidate(prefix string) {
	p.mu.Lock()
	var hit []*instance
	for in := range p.all {
		if strings.HasPrefix(in.spec.key, prefix) {
			hit = append(hit, in)
		}
	}
	p.mu.Unlock()
	for _, in := range hit {
		p.kill(in)
	}
}

func (p *pool) kill(in *instance) {
	in.killed.Do(func() {
		in.gone.Store(true)
		in.ext.shutdown(in.shutdownReason())
		if in.cmd != nil && in.cmd.Process != nil {
			core.KillGroup(in.cmd)
			p.svc.hub.Closed("lambda", "process", "stopped", in.spec.name)
		}
		if in.docker != "" {
			p.svc.hub.Docker().Remove(context.Background(), in.docker)
		}
		in.srv.Close()
		p.mu.Lock()
		delete(p.all, in)
		p.count[in.spec.key]--
		list := p.idle[in.spec.key]
		for i, x := range list {
			if x == in {
				p.idle[in.spec.key] = append(list[:i], list[i+1:]...)
				break
			}
		}
		p.mu.Unlock()
	})
}

// acquire returns an idle instance or starts one; Service.admit has already
// bounded how many run at once.
func (p *pool) acquire(s *spec) (*instance, error) {
	for {
		p.mu.Lock()
		if list := p.idle[s.key]; len(list) > 0 {
			in := list[len(list)-1]
			p.idle[s.key] = list[:len(list)-1]
			p.mu.Unlock()
			select {
			case <-in.dead:
				p.kill(in)
				continue
			default:
				return in, nil
			}
		}
		p.count[s.key]++
		p.mu.Unlock()
		in, err := p.start(s)
		if err != nil {
			p.mu.Lock()
			p.count[s.key]--
			p.mu.Unlock()
		}
		return in, err
	}
}

// release makes an instance idle again, or stops it when the version
// already keeps as many idle ones as its warm pool and provisioned floor.
func (p *pool) release(in *instance) {
	in.used = time.Now()
	if in.gone.Load() { // killed (for example after a timeout) while it ran: dead may not be closed yet
		return
	}
	select {
	case <-in.dead:
		p.kill(in)
		return
	default:
	}
	k := in.spec.key
	p.mu.Lock()
	if len(p.idle[k]) >= p.floor[k]+p.warm() {
		p.mu.Unlock()
		p.kill(in)
		return
	}
	p.idle[k] = append(p.idle[k], in)
	p.mu.Unlock()
}

// run executes one invocation on a warm or new instance.
func (p *pool) run(s *spec, payload []byte, clientCtx, trace string) (outcome, error) {
	in, err := p.acquire(s)
	if err != nil {
		return outcome{}, err
	}
	inv := &invocation{id: core.UUID(), arn: s.arn, payload: payload, clientCtx: clientCtx, trace: trace, done: make(chan outcome, 1)}
	initWait := p.initLimit()
	if in.docker != "" {
		initWait = max(initWait, 5*time.Minute) // first run may pull the image
	}
	select {
	case in.next <- inv: // the runtime picked it up (unbuffered)
	case <-in.dead:
		p.kill(in)
		return p.initFailure(in), nil
	case <-time.After(initWait):
		p.kill(in)
		return outcome{}, core.Errorf(502, "ServiceException", "function %s did not start within %s%s", s.name, initWait, in.waiting())
	}
	start := time.Now()
	in.log(fmt.Sprintf("START RequestId: %s Version: %s", inv.id, s.version))
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	var out outcome
	select {
	case out = <-inv.done:
	case <-timer.C:
		msg := fmt.Sprintf("%s %s Task timed out after %.2f seconds", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), inv.id, s.timeout.Seconds())
		in.log(msg)
		out = outcome{body: []byte(`{"errorMessage":"` + msg + `"}`), errType: "Unhandled"}
		in.mu.Lock()
		in.reason = "timeout"
		in.mu.Unlock()
		p.kill(in)
	case <-in.dead:
		out = outcome{body: []byte(`{"errorMessage":"RequestId: ` + inv.id + ` Error: Runtime exited","errorType":"Runtime.ExitError"}`), errType: "Unhandled"}
		p.kill(in)
	}
	d := time.Since(start)
	in.flush()
	in.log("END RequestId: " + inv.id)
	in.log(fmt.Sprintf("REPORT RequestId: %s\tDuration: %.2f ms\tBilled Duration: %d ms\tMemory Size: %d MB\tMax Memory Used: %d MB",
		inv.id, float64(d.Microseconds())/1000, d.Milliseconds()+1, s.memory, min(s.memory, 64)))
	in.mu.Lock()
	out.logs = in.logs.String()
	in.logs.Reset()
	in.current = nil
	in.mu.Unlock()
	p.release(in)
	return out, nil
}

func (p *pool) initFailure(in *instance) outcome {
	in.mu.Lock()
	defer in.mu.Unlock()
	body := in.initErr
	if len(body) == 0 {
		body = []byte(`{"errorMessage":"RequestId: init Error: Runtime exited during init","errorType":"Runtime.ExitError"}`)
	}
	return outcome{body: body, errType: "Unhandled", logs: in.logs.String()}
}

// log records a line for the current invocation, the console and CloudWatch Logs.
func (in *instance) log(line string) {
	in.mu.Lock()
	if in.logs.Len() < 1<<20 {
		in.logs.WriteString(line + "\n")
	}
	in.mu.Unlock()
	in.pool.svc.logLine(in.spec, in.stream, line)
}

// mark is what flush writes into a runtime's output pipe. The NUL bytes keep it apart from printed text.
const mark = "\x00pogi:flush\x00"

// flush waits until the output reader has logged everything the runtime printed so far. A function
// prints, then posts its response; the reader may not have read those lines yet when the response
// arrives, and END would beat them into the log. The pipe keeps order, so once the reader meets a mark
// written now, every line before it is logged.
func (in *instance) flush() {
	if in.out == nil {
		return // a container's output comes through Docker's log stream instead (see dockerRun)
	}
	in.mu.Lock()
	in.marks++
	flushed := make(chan struct{})
	in.flushed = flushed
	in.mu.Unlock()
	if _, err := io.WriteString(in.out, mark+"\n"); err != nil {
		flushed = nil // the runtime has exited: in.dead closes once the reader reaches the end of its output
	}
	select {
	case <-flushed:
	case <-in.dead:
	case <-time.After(5 * time.Second): // a reader this far behind would hold up the response; log what it has
	}
}

// readOutput logs each line a runtime prints, until its output ends. A line longer than the buffer
// comes out in pieces, so the reader never stops early. A line ending in mark answers a flush; any text
// before the mark is a line the runtime had not finished.
func (in *instance) readOutput(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	sc.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if n, line, err := bufio.ScanLines(data, atEOF); n > 0 || len(data) < 1<<20 {
			return n, line, err
		}
		return len(data), data, nil
	})
	for sc.Scan() {
		line, marked := strings.CutSuffix(sc.Text(), mark)
		if line != "" || !marked {
			in.log(line)
		}
		if marked {
			in.mu.Lock()
			if in.seen++; in.seen >= in.marks && in.flushed != nil {
				close(in.flushed)
				in.flushed = nil
			}
			in.mu.Unlock()
		}
	}
}

// start launches a runtime process (or container) serving its own Runtime API.
func (p *pool) start(s *spec) (*instance, error) {
	select {
	case <-p.stop: // a retry or provisioning that outlived Close
		return nil, core.Errorf(503, "ServiceException", "Lambda is shutting down")
	default:
	}
	mode, err := p.mode(s)
	if err != nil {
		return nil, err
	}
	in := &instance{id: core.RandHex(16), pool: p, spec: s, next: make(chan *invocation), dead: make(chan struct{}), used: time.Now(), born: time.Now()}
	in.stream = time.Now().UTC().Format("2006/01/02") + "/[" + s.version + "]" + in.id
	listen := "127.0.0.1"
	if mode == "docker" {
		listen = p.svc.hub.Docker().ListenAddr() // the container calls it
	}
	in.ln, err = net.Listen("tcp", net.JoinHostPort(listen, "0"))
	if err != nil {
		return nil, err
	}
	in.srv = &http.Server{Handler: in.api(), ReadHeaderTimeout: 10 * time.Second}
	go in.srv.Serve(in.ln)
	port := strconv.Itoa(in.ln.Addr().(*net.TCPAddr).Port)

	endpoint := p.svc.hub.URL
	if endpoint == "" {
		endpoint = "http://localhost:" + strconv.Itoa(cmp.Or(p.svc.hub.Cfg.Port, 7644))
	}
	env := map[string]string{
		"_HANDLER": s.handler, "AWS_LAMBDA_FUNCTION_NAME": s.name, "AWS_LAMBDA_FUNCTION_VERSION": s.version,
		"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": strconv.Itoa(s.memory), "AWS_LAMBDA_LOG_GROUP_NAME": "/aws/lambda/" + s.name,
		"AWS_LAMBDA_LOG_STREAM_NAME": in.stream, "AWS_REGION": s.region, "AWS_DEFAULT_REGION": s.region,
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_EXECUTION_ENV": "AWS_Lambda_" + s.runtime,
		"AWS_LAMBDA_INITIALIZATION_TYPE": "on-demand", "TZ": ":UTC",
	}
	if _, _, account, res, ok := core.ParseARN(s.role); ok && strings.HasPrefix(res, "role/") {
		// session credentials for the execution role, as Lambda provides
		akid, secret := "ASIA"+core.RandString(16, core.UpperNum), core.RandString(40, core.AlphaNum)
		p.svc.hub.AddIdentity(akid, core.Identity{Account: account, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + s.name,
			ARN: core.GlobalARN("sts", s.region, account, "assumed-role/"+res[strings.LastIndex(res, "/")+1:]+"/"+s.name), Secret: secret})
		env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"], env["AWS_SESSION_TOKEN"] = akid, secret, core.RandString(180, core.AlphaNum)
	}
	var cmd *exec.Cmd
	if mode == "docker" {
		err = p.dockerRun(s, in, env, port, endpoint)
	} else {
		cmd, err = p.nativeCmd(s, env, port, endpoint)
	}
	if err != nil {
		in.srv.Close()
		return nil, err
	}
	if mode != "docker" {
		opt, _ := p.layerDir(s)
		p.startExtensions(s, in, env, opt)
	}
	if cmd == nil { // dockerRun started the container
		return in, nil
	}
	// stdout and stderr share one pipe; pogi keeps its write end too, for flush's marks
	r, w, err := os.Pipe()
	if err != nil {
		in.srv.Close()
		return nil, err
	}
	cmd.Stdout, cmd.Stderr, in.out = w, w, w
	core.OwnGroup(cmd) // the runtime's own children go with it when it is killed
	if err := cmd.Start(); err != nil {
		r.Close()
		w.Close()
		in.srv.Close()
		return nil, core.Errorf(502, "InvalidRuntimeException", "cannot start %s runtime: %v", s.runtime, err)
	}
	in.cmd = cmd
	p.mu.Lock()
	p.all[in] = true
	p.mu.Unlock()
	core.Go("lambda.start", func() {
		cmd.Wait()
		w.Close() // now the reader meets the end of the output once it has read the rest
	})
	core.Go("lambda.start", func() {
		defer close(in.dead)
		in.readOutput(r)
		r.Close()
	})
	return in, nil
}

// api serves the Lambda Runtime API for one instance.
func (in *instance) api() http.Handler {
	m := http.NewServeMux()
	in.mountExtensions(m)
	m.HandleFunc("GET /2018-06-01/runtime/invocation/next", func(w http.ResponseWriter, r *http.Request) {
		in.asked.CompareAndSwap(0, time.Now().UnixNano())
		in.ext.waitInit(r.Context(), in.dead, in.pool.initLimit())
		select {
		case inv := <-in.next:
			inv.deadline = time.Now().Add(in.spec.timeout)
			trace := core.TraceHeader(cmp.Or(inv.trace, core.NewTrace()))
			in.ext.invoke(inv, trace)
			in.mu.Lock()
			in.current = inv
			in.mu.Unlock()
			h := w.Header()
			h.Set("Content-Type", "application/json") // without it Go guesses text/plain, and the Python runtime hands the handler raw bytes
			h.Set("Lambda-Runtime-Aws-Request-Id", inv.id)
			h.Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(inv.deadline.UnixMilli(), 10))
			h.Set("Lambda-Runtime-Invoked-Function-Arn", inv.arn)
			h.Set("Lambda-Runtime-Trace-Id", trace)
			if inv.clientCtx != "" {
				h.Set("Lambda-Runtime-Client-Context", inv.clientCtx)
			}
			w.Write(inv.payload)
		case <-r.Context().Done():
		case <-in.dead:
		}
	})
	finish := func(w http.ResponseWriter, r *http.Request, errType string) {
		body, _ := io.ReadAll(r.Body)
		in.mu.Lock()
		inv := in.current
		in.mu.Unlock()
		if inv == nil || inv.id != r.PathValue("id") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		inv.done <- outcome{body: body, errType: errType}
		accepted(w)
	}
	m.HandleFunc("POST /2018-06-01/runtime/invocation/{id}/response", func(w http.ResponseWriter, r *http.Request) { finish(w, r, "") })
	m.HandleFunc("POST /2018-06-01/runtime/invocation/{id}/error", func(w http.ResponseWriter, r *http.Request) { finish(w, r, "Unhandled") })
	m.HandleFunc("POST /2018-06-01/runtime/init/error", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		in.mu.Lock()
		in.initErr = body
		in.mu.Unlock()
		accepted(w)
	})
	return m
}

// accepted answers a Runtime API post the way AWS does: 202 and {"status":"OK"}
// (some runtime clients, such as .NET, read the body).
func accepted(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	io.WriteString(w, `{"status":"OK"}`)
}

// mode picks native or docker for a function, per --lambda (auto | native | docker).
// Container images always run in Docker.
func (p *pool) mode(s *spec) (string, error) {
	if s.packageType == "Image" {
		if !p.dockerAvailable() {
			return "", core.Errorf(502, "InvalidRuntimeException",
				"function %s is the container image %s, which runs only in Docker, and no container engine is running (start Docker, Podman, Colima or OrbStack)", s.name, s.image)
		}
		return "docker", nil
	}
	native := p.nativeOK(s)
	switch p.svc.hub.Cfg.Lambda {
	case "native":
		if !native {
			return "", core.Errorf(502, "InvalidRuntimeException", "runtime %s cannot run natively here (pogi runs python and nodejs natively; use --lambda docker or auto)", s.runtime)
		}
		return "native", nil
	case "docker":
		if !p.dockerAvailable() {
			return "", core.Errorf(502, "InvalidRuntimeException", "--lambda docker is set but Docker is not available")
		}
		return "docker", nil
	}
	if native {
		return "native", nil
	}
	if p.dockerAvailable() {
		return "docker", nil
	}
	return "", core.Errorf(502, "InvalidRuntimeException",
		"runtime %s needs Docker, or Python or Node.js installed where pogi runs (pogi runs python* and nodejs* natively when they are installed, as in the ghcr.io/nrndev/pogi image; provided.* runs natively only for binaries built for %s/%s)", s.runtime, runtime.GOOS, runtime.GOARCH)
}

func (p *pool) nativeOK(s *spec) bool {
	switch {
	case strings.HasPrefix(s.runtime, "python"):
		return interpreter(s.runtime, "python3", "python") != ""
	case strings.HasPrefix(s.runtime, "nodejs"):
		return interpreter("node") != ""
	case strings.HasPrefix(s.runtime, "provided"), s.runtime == "go1.x":
		dir, err := p.codeDir(s)
		if err != nil {
			return false
		}
		return nativeBinary(filepath.Join(dir, "bootstrap"))
	}
	return false
}

// nativeBinary reports whether an executable was built for this OS.
func nativeBinary(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	magic := make([]byte, 4)
	io.ReadFull(f, magic)
	switch runtime.GOOS {
	case "linux":
		return bytes.Equal(magic, []byte{0x7f, 'E', 'L', 'F'})
	case "darwin":
		return bytes.Equal(magic, []byte{0xcf, 0xfa, 0xed, 0xfe}) || bytes.Equal(magic, []byte{0xca, 0xfe, 0xba, 0xbe})
	}
	return false
}

func interpreter(names ...string) string {
	for _, n := range names {
		if p, err := exec.LookPath(n); err == nil {
			return p
		}
	}
	return ""
}

// initLimit is how long a new environment may take to start (lambda.initTimeout); AWS's limit is 10 seconds.
func (p *pool) initLimit() time.Duration { return cmp.Or(p.svc.hub.Cfg.LambdaInit, 10*time.Second) }

func (p *pool) dockerAvailable() bool { return p.svc.hub.Docker().Available() }

func (p *pool) nativeCmd(s *spec, env map[string]string, port, endpoint string) (*exec.Cmd, error) {
	dir, err := p.codeDir(s)
	if err != nil {
		return nil, err
	}
	opt, err := p.layerDir(s)
	if err != nil {
		return nil, err
	}
	var cmd *exec.Cmd
	switch {
	case strings.HasPrefix(s.runtime, "python"):
		boot, err := p.bootstrap("python.py")
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(interpreter(s.runtime, "python3", "python"), "-u", boot)
		paths := []string{dir}
		if opt != "" {
			paths = append(paths, filepath.Join(opt, "python"))
			m, _ := filepath.Glob(filepath.Join(opt, "python", "lib", "python*", "site-packages"))
			paths = append(paths, m...)
		}
		paths = append(paths, filepath.Join(SDKDir(p.svc.hub.Cfg.DataDir), "python")) // boto3, as the runtime has
		env["PYTHONPATH"] = strings.Join(paths, string(os.PathListSeparator))
	case strings.HasPrefix(s.runtime, "nodejs"):
		boot, err := p.bootstrap("node.mjs")
		if err != nil {
			return nil, err
		}
		cmd = exec.Command(interpreter("node"), boot)
		// layers, then the AWS SDK the runtime has; code under the data dir also
		// finds the SDK by walking up to <data>/lambda/node_modules (ES modules too)
		paths := []string{filepath.Join(SDKDir(p.svc.hub.Cfg.DataDir), "node_modules")}
		if opt != "" {
			paths = append([]string{filepath.Join(opt, "nodejs", "node_modules")}, paths...)
		}
		env["NODE_PATH"] = strings.Join(paths, string(os.PathListSeparator))
	default:
		cmd = exec.Command(filepath.Join(dir, "bootstrap"))
	}
	env["LAMBDA_TASK_ROOT"] = dir
	env["AWS_LAMBDA_RUNTIME_API"] = "127.0.0.1:" + port
	env["AWS_ENDPOINT_URL"] = endpoint
	if ca, _, _ := p.svc.hub.CertPaths(); exists(ca) {
		env["NODE_EXTRA_CA_CERTS"] = ca // trust pogi's https port
	}
	if opt != "" {
		env["POGI_LAYERS_DIR"] = opt
	}
	cmd.Dir = dir
	cmd.Env = hostEnv()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	for k, v := range s.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd, nil
}

// hostEnv passes only what interpreters need, so host AWS settings never leak in.
func hostEnv() []string {
	var out []string
	for _, k := range []string{"PATH", "HOME", "TMPDIR", "TEMP", "TMP", "LANG", "SYSTEMROOT", "USERPROFILE"} {
		if v, ok := os.LookupEnv(k); ok {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// runtimeImage maps a Lambda runtime to its AWS base image: python3.12 →
// python:3.12, nodejs20.x → nodejs:20, java21, dotnet8, ruby3.3, provided.al2023.
func runtimeImage(rt string) string {
	for prefix, repo := range map[string]string{"python": "python", "nodejs": "nodejs", "java": "java", "dotnet": "dotnet", "ruby": "ruby"} {
		if v, ok := strings.CutPrefix(rt, prefix); ok {
			return "public.ecr.aws/lambda/" + repo + ":" + strings.TrimSuffix(v, ".x")
		}
	}
	if v, ok := strings.CutPrefix(rt, "provided."); ok {
		return "public.ecr.aws/lambda/provided:" + v
	}
	if rt == "provided" { // Amazon Linux 1
		return "public.ecr.aws/lambda/provided:alami"
	}
	return "public.ecr.aws/lambda/provided:al2023"
}

// dockerRun runs a function in its AWS base image (or its own image),
// pointing the image's runtime client at this instance's Runtime API; the
// container's output is the instance's log until it stops.
func (p *pool) dockerRun(s *spec, in *instance, env map[string]string, port, endpoint string) error {
	d := p.svc.hub.Docker()
	host := d.HostAddr()
	ct := &docker.Container{Name: docker.Name("lambda", in.id), Image: s.image, Service: "lambda", Resource: s.name}
	if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback only from the host network
		ct.Network = "host"
	}
	u, _ := url.Parse(endpoint)
	env["AWS_LAMBDA_RUNTIME_API"] = host + ":" + port
	env["AWS_ENDPOINT_URL"] = "http://" + host + ":" + u.Port()
	env["LAMBDA_TASK_ROOT"] = "/var/task"
	if s.packageType == "Image" { // the image's entrypoint knows its handler and runtime
		delete(env, "_HANDLER")
		delete(env, "AWS_EXECUTION_ENV")
		if ic := s.imageConfig; ic != nil {
			ct.Entrypoint, ct.Cmd, ct.WorkDir = ic.EntryPoint, ic.Command, ic.WorkingDirectory
		}
	}
	for k, v := range env {
		ct.Env = append(ct.Env, k+"="+v)
	}
	for k, v := range s.env {
		ct.Env = append(ct.Env, k+"="+v)
	}
	if s.packageType != "Image" {
		dir, err := p.codeDir(s)
		if err != nil {
			return err
		}
		ct.Binds = []string{dir + ":/var/task:ro"}
		if opt, err := p.layerDir(s); err == nil && opt != "" {
			ct.Binds = append(ct.Binds, opt+":/opt:ro")
		}
		ct.Image, ct.Cmd = runtimeImage(s.runtime), []string{s.handler}
		opt, _ := p.layerDir(s)
		var n int
		ct.Entrypoint, n = dockerExtensions(opt)
		in.ext.init(n)
	}
	if ct.Network != "host" {
		ct.Dns = route53.SubnetDNS(p.svc.hub, s.account, s.region, s.vpcSubnets)
	}
	if _, err := d.Run(context.Background(), ct); err != nil {
		return core.Errorf(502, "InvalidRuntimeException", "cannot start %s in Docker: %v", ct.Image, err)
	}
	in.docker = ct.Name
	p.mu.Lock()
	p.all[in] = true
	p.mu.Unlock()
	// ponytail: nothing orders Docker's log stream with the Runtime API, so a container's last lines can
	// land after END. flush would need its mark written into the container's own stdout (a docker exec
	// per invoke, and a shell in the image); add that if container logs come up short.
	core.Go("lambda.dockerRun", func() {
		defer close(in.dead)
		d.Logs(context.Background(), ct.Name, true, in.log)
	})
	return nil
}

// codeDir extracts a function's zip once per code hash (or returns the hot-reload dir).
func (p *pool) codeDir(s *spec) (string, error) {
	if s.codeDir != "" {
		return s.codeDir, nil
	}
	return p.extract("code", s.codeSha, [][]byte{s.code})
}

// layerDir merges a function's layers into one /opt-like directory.
func (p *pool) layerDir(s *spec) (string, error) {
	if len(s.layers) == 0 {
		return "", nil
	}
	h := sha256.New()
	for _, l := range s.layers {
		h.Write(l)
	}
	return p.extract("layers", hex.EncodeToString(h.Sum(nil)), s.layers)
}

func (p *pool) extract(kind, sum string, zips [][]byte) (string, error) {
	dir := filepath.Join(p.svc.hub.Cfg.DataDir, "lambda", kind, strings.NewReplacer("/", "_", "+", "-", "=", "").Replace(sum))
	if _, err := os.Stat(dir); err == nil {
		return dir, nil
	}
	tmp := dir + ".tmp" + core.RandHex(4)
	for _, z := range zips {
		if err := unzip(z, tmp); err != nil {
			os.RemoveAll(tmp)
			return "", core.Errorf(400, "InvalidParameterValueException", "Could not unzip uploaded file: %v", err)
		}
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dir); err != nil && !errors.Is(err, os.ErrExist) {
		os.RemoveAll(tmp)
		if _, statErr := os.Stat(dir); statErr != nil {
			return "", err
		}
	}
	return dir, nil
}

// maxUnzipped caps the total unpacked size of one code zip (250 MB, the AWS Lambda limit).
// It is a var so a test can lower it.
var maxUnzipped int64 = 250 << 20

func unzip(data []byte, dest string) error {
	if len(data) == 0 {
		return nil
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	left := maxUnzipped // bytes still allowed, across all entries
	for _, f := range zr.File {
		path := filepath.Join(dest, f.Name)
		if !strings.HasPrefix(path, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal path %q in zip", f.Name)
		}
		if f.FileInfo().IsDir() {
			os.MkdirAll(path, 0o755)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		mode := f.Mode().Perm()
		if mode == 0 {
			mode = 0o644
		}
		if strings.HasSuffix(f.Name, "bootstrap") || strings.HasPrefix(f.Name, "extensions/") {
			mode |= 0o755
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			rc.Close()
			return err
		}
		n, err := io.Copy(out, io.LimitReader(rc, left+1))
		rc.Close()
		out.Close()
		if err != nil {
			return err
		}
		if left -= n; left < 0 {
			return fmt.Errorf("the code is over the %d MB unzipped limit", maxUnzipped>>20)
		}
	}
	return nil
}

// bootstrap writes an embedded runtime client to disk once.
func (p *pool) bootstrap(name string) (string, error) {
	b, _ := bootstraps.ReadFile("bootstrap/" + name)
	sum := sha256.Sum256(b)
	path := filepath.Join(p.svc.hub.Cfg.DataDir, "lambda", "runtime", hex.EncodeToString(sum[:6])+"-"+name)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	tmp := path + core.RandHex(4)
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

func exists(path string) bool { _, err := os.Stat(path); return err == nil }

// SDKDir holds the AWS SDKs native runtimes lack (see `pogi lambda-sdks`):
// node_modules for Node.js, python/ for boto3. Function code is unpacked
// below it, so Node's module lookup finds the SDK without NODE_PATH.
func SDKDir(dataDir string) string { return filepath.Join(dataDir, "lambda") }

// waiting says what a function that did not start in time was waiting for, and when each part got ready
// after the instance started, so the error points at the slow part.
func (in *instance) waiting() string {
	after := func(t time.Time) string { return t.Sub(in.born).Round(100 * time.Millisecond).String() }
	out := ": its runtime never asked for an invocation"
	if n := in.asked.Load(); n != 0 {
		out = ": its runtime asked for an invocation after " + after(time.Unix(0, n))
	}
	e := &in.ext
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.expected == 0 {
		return out
	}
	out += fmt.Sprintf("; %d extensions started, %d registered", e.expected, len(e.list))
	for _, id := range core.SortedKeys(e.list) {
		x := e.list[id]
		out += "; " + x.name + " registered after " + after(x.registered)
		if !x.ready.IsZero() {
			out += " and finished its init after " + after(x.ready)
		}
	}
	return out
}
