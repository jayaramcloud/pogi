package core

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/docker"
	"google.golang.org/grpc"
)

// Config is the runtime configuration shared by all services.
type Config struct {
	Host         string   // listen host
	Port         int      // main port
	ECRPort      int      // ECR registry port, localhost:<ECRPort> (pogi.New: 4510, Start: none); <0: main port only
	SMTPPort     int      // SES SMTP port, localhost:<SMTPPort> (pogi.New: 2525, Start: none); <0: off
	AMQPPort     int      // AMQP 1.0 port for Service Bus and Event Hubs, plain TCP (pogi.New: 5672, Start: a random port); <=0: off
	MQTTPort     int      // IoT MQTT broker port, plain TCP (pogi.New: 1883, Start: none); <0: WebSocket at /mqtt only
	TransferPort int      // Transfer Family SFTP port, opened with the first server (pogi.New: 2222, Start: a free port)
	Region       string   // default AWS region
	DataDir      string   // ~/.pogi
	Persist      bool     // --persist: hybrid storage when Storage is empty
	Services     []string // enabled services or clouds; empty means all
	Lambda       string   // auto | native | docker
	IAM          string   // off | soft | strict
	Version      string
	NoTLS        bool // plain HTTP only on the main port
	NoPorts      bool // do not open the Azurite-style extra ports

	Hostname      string            // "localhost"; e.g. "aws.localhost" for virtual-host style URLs
	TLSCert       string            // custom certificate and key files; empty: pogi's local CA
	TLSKey        string            //
	CORSOrigins   []string          // extra browser origins allowed on every API
	CORSHeaders   []string          // extra request headers allowed from those origins
	AllowExternal bool              // permit a non-loopback Host
	MaxUploadMB   int               // largest upload in MiB; 0 means 10
	Storage       string            // memory | hybrid | persistent | wal; Persist means hybrid
	FlushInterval time.Duration     // hybrid flush period
	StorageModes  map[string]string // per-service storage mode
	ValidateSigs  bool              // check SigV4 signatures
	WarmPool      int               // Lambda environments kept warm per function
	LambdaInit    time.Duration     // longest a new Lambda environment may take to start; 0 means 10s, AWS's init limit
	DockerHost    string            // container engine socket or URL; empty: auto-detect
	DockerNetwork string            // network for managed containers
	DockerImages  map[string]string // image overrides, e.g. "rds.postgres"
	AIEndpoint    string            // OpenAI-compatible endpoint for Bedrock and friends
	AIKey         string            //
	AIModels      map[string]string // model id glob → local model
	AITimeout     time.Duration     // longest one model call; 0 means 5 minutes
	SMTPRelay     string            // smtp://[user:pass@]host:port that SES also delivers every message to (Mailpit, MailHog)
	File          *config.File      // pogi.yaml, for seeding, identities, chaos and stacks
}

// Hub owns the services of one pogi instance and routes requests to them.
// There are no globals: several hubs can live in one process (tests).
type Hub struct {
	closeOnce sync.Once // Close runs once
	Cfg       Config
	Log       *slog.Logger
	Requests  *RequestLog
	Spans     *SpanLog // steps of flows that are not requests: deliveries, invocations, redrives
	Chaos     *Chaos
	Started   time.Time
	Init      *InitHooks
	URL       string // base URL once listening, e.g. http://localhost:7644
	GRPC      *grpc.Server

	// Ready is set once serving, just before "Ready." is printed (pogi wait).
	Ready atomic.Bool

	// Authorize evaluates IAM for a request, by cloud: each cloud's IAM service sets its own.
	// It records its decision in Ctx.IAM and returns an error to deny; it runs in every mode
	// and only strict mode denies (see Hub.Check).
	Authorize map[string]func(c *Ctx) *Error

	// CheckInput checks the input limits of a request to an AWS service before the service sees it
	// (set by pogi.New). It answers a bad request itself and reports true; false lets the service run.
	CheckInput func(svc Service, c *Ctx, w http.ResponseWriter, r *http.Request) bool

	// Seed applies a pogi.yaml `resources:` fragment (set by pogi.New; used by MCP).
	Seed func(fragment []byte) error

	services  []Service
	byName    map[string]Service
	targets   map[string]Service
	signing   map[string]Service
	matchers  []Service
	mounts    []mount
	emulators []emulatorRoute

	iamMode atomic.Pointer[string] // set by SetIAMMode; Cfg.IAM until then

	mu         sync.RWMutex
	deliver    map[string]DeliverFunc
	invoke     InvokeFunc
	invokeFrom InvokeFromFunc
	idents     map[string]Identity
	resets     []func(services []string)
	addrs      map[string]string
	stores     map[string]*store // services kept on disk; set by OpenStorage
	lock       *os.File          // the data folder lock, held from OpenStorage to Close
	probs      problems          // pogi-wide problems for status and the bell
	docker     func() *docker.Client
	amqp       func() *amqp.Server
	shut       shutdownLog // what shutdown stopped; see ShutdownList
}

type mount struct {
	prefix string
	h      http.Handler
}

// NewHub creates an empty hub with the admin and compatibility endpoints mounted.
func NewHub(cfg Config, log *slog.Logger) *Hub {
	if cfg.Region == "" {
		cfg.Region = DefaultRegion
	}
	if cfg.Hostname == "" {
		cfg.Hostname = "localhost"
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &Hub{
		Cfg: cfg, Log: log, Requests: NewRequestLog(1000), Spans: NewSpanLog(5000), Chaos: &Chaos{}, Started: time.Now(),
		Authorize: map[string]func(*Ctx) *Error{},
		Init:      &InitHooks{},
		byName:    map[string]Service{}, targets: map[string]Service{}, signing: map[string]Service{},
		deliver: map[string]DeliverFunc{}, idents: map[string]Identity{},
	}
	h.GRPC = h.newGRPC()
	h.amqp = sync.OnceValue(func() *amqp.Server { return amqp.NewServer(amqp.Options{Log: h.Log}) })
	h.docker = sync.OnceValue(func() *docker.Client {
		c, ecr := h.Cfg, ""
		if c.ECRPort > 0 {
			ecr = "localhost:" + strconv.Itoa(c.ECRPort)
		}
		keep, dir := c.Persist || (c.Storage != "" && c.Storage != "memory"), ""
		if keep && c.DataDir != "" { // containers carry the data dir, so the next run finds them
			dir, _ = filepath.Abs(c.DataDir)
		}
		return docker.New(docker.Options{Host: c.DockerHost, Network: c.DockerNetwork, Images: c.DockerImages,
			Keep: keep, DataDir: dir, ECR: ecr, Log: h.Log})
	})
	admin := http.NewCrossOriginProtection().Handler(h.adminMux())
	for _, p := range []string{"/_pogi/api/", "/_localstack/", "/moto-api/"} {
		h.Mount(p, admin)
	}
	h.Mount("/emulator/v1/", http.HandlerFunc(h.serveEmulator))
	return h
}

// Docker is the container engine client of this instance, made on first
// use; Available says whether an engine runs.
func (h *Hub) Docker() *docker.Client { return h.docker() }

// AMQP is the AMQP 1.0 broker that Service Bus and Event Hubs add their
// resolvers to.
func (h *Hub) AMQP() *amqp.Server { return h.amqp() }

// Register adds a service to the routing tables.
func (h *Hub) Register(s Service) {
	info := s.Info()
	h.services = append(h.services, s)
	h.byName[info.Name] = s
	for _, t := range info.Targets {
		h.targets[t] = s
	}
	for _, n := range info.Signing {
		h.signing[n] = s
	}
	if info.Match != nil {
		h.matchers = append(h.matchers, s)
	}
	if g, ok := s.(GRPCService); ok {
		g.RegisterGRPC(h.GRPC)
	}
}

// Mount serves an internal handler under prefix, outside the request log.
// The longest matching prefix wins.
func (h *Hub) Mount(prefix string, handler http.Handler) {
	h.mounts = append(h.mounts, mount{prefix, handler})
	slices.SortStableFunc(h.mounts, func(a, b mount) int { return len(b.prefix) - len(a.prefix) })
}

// Services returns the registered services in registration order.
func (h *Hub) Services() []Service { return h.services }

// Service returns a service by name, or nil.
func (h *Hub) Service(name string) Service { return h.byName[name] }

// ServeHTTP is the gateway: it finds the service, builds the Ctx and logs the call.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if ok, why := h.browserOK(r); !ok {
		h.refuse(w, r, why)
		return
	}
	for _, m := range h.mounts {
		if strings.HasPrefix(r.URL.Path, m.prefix) {
			m.h.ServeHTTP(w, r)
			return
		}
	}
	if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		h.GRPC.ServeHTTP(w, r) // Google Cloud gRPC APIs share the main port (h2c)
		return
	}
	c := &Ctx{Hub: h, RequestID: NewID(), Host: r.Host, Path: r.URL.Path, Region: h.Cfg.Region, Trace: TraceFrom(r.Header)}
	akid, region, signing := sigv4(r)
	if region != "" {
		c.Region = region
	}
	c.AccessKey = akid
	c.Account, c.Principal = h.accountFor(akid, c.Region)

	svc := h.route(r, signing)
	if svc == nil {
		if r.Method == http.MethodGet && r.URL.Path == "/" && strings.Contains(r.Header.Get("Accept"), "text/html") {
			http.Redirect(w, r, "/_pogi/", http.StatusFound)
			return
		}
		if h.CORS(w, r) {
			return // a preflight; S3, which gets them otherwise, is off
		}
		http.Error(w, "pogi: no enabled service handles this request", http.StatusNotFound)
		return
	}
	// S3 (bucket CORS) and host- or path-routed traffic (API Gateway, Lambda
	// URLs, Cognito's hosted UI) answer CORS themselves.
	if info := svc.Info(); info.Name != "s3" && (info.Match == nil || !info.Match(r)) && h.CORS(w, r) {
		return
	}
	h.serve(svc, c, w, r)
}

// Handler serves one service on its own port (Azurite-style), with the same
// context, logging and chaos as the gateway. The account is the first path
// segment (devstoreaccount1).
func (h *Hub) Handler(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ok, why := h.browserOK(r); !ok {
			h.refuse(w, r, why)
			return
		}
		account, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		h.serve(svc, &Ctx{Hub: h, RequestID: NewID(), Host: r.Host, Region: h.Cfg.Region, Account: account, Trace: TraceFrom(r.Header)}, w, r)
	})
}

func (h *Hub) serve(svc Service, c *Ctx, w http.ResponseWriter, r *http.Request) {
	info := svc.Info()
	c.Cloud, c.Service = info.Cloud, info.Name
	c.Bearer, _ = strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	c.RawQuery, c.Header = r.URL.RawQuery, r.Header
	c.Internal = r.Header.Get(internalHeader) != "" && r.RemoteAddr == "" // in-process only, never from the network
	c.ctx = r.Context()
	r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, c))
	if h.Cfg.ValidateSigs {
		c.req = r.Clone(r.Context()) // as received: services may rewrite the URL (SQS)
	}
	if c.Internal {
		if r.Header.Get(ConsoleHeader) != "" {
			h.serveConsole(svc, c, w, r)
			return
		}
		svc.ServeHTTP(w, r)
		return
	}
	h.limitBody(svc, w, r, c)
	defer func() { // a handler that left the body unread must not meet its deadline later
		if c.bodyErr == nil || c.bodyErr.Status != http.StatusRequestTimeout {
			http.NewResponseController(w).SetReadDeadline(time.Time{})
		}
	}()
	rec := &recorder{ResponseWriter: w}
	// persistent mode: a change is on disk before the client hears of it, so
	// the response waits in a buffer (a stream arrives at the end).
	st := h.stores[info.Name]
	held := st != nil && st.mode == "persistent" && r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions
	if held {
		rec.ResponseWriter = &bufferWriter{header: w.Header()}
	}
	start := time.Now()
	h.guard(svc, rec, r, c)
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	if held {
		h.sync(st, false)
		b := rec.ResponseWriter.(*bufferWriter)
		if b.status != 0 {
			w.WriteHeader(b.status)
		}
		w.Write(b.buf.Bytes())
	}
	h.Requests.Add(Entry{
		Time: start, Cloud: c.Cloud, Service: c.Service, Op: c.Op, Method: r.Method,
		Path: redactPath(r.URL.RequestURI()), Status: rec.status, Ms: float64(time.Since(start).Microseconds()) / 1000,
		Account: c.Account, Region: c.Region, RequestID: c.RequestID, AccessKey: c.AccessKey, Principal: c.Principal,
		Error: c.ErrCode, Chaos: c.Chaos, IAM: c.IAM, Trace: c.Trace,
		ReqHeaders: flatHeaders(r.Header), ReqBody: preview(c.body, r.Header.Get("Content-Type"), r.URL.Path),
		RespBody: preview(rec.body.Bytes(), rec.Header().Get("Content-Type"), r.URL.Path),
	})
}

// serveConsole serves a write the console made in-process and logs it, marked as from the console.
// Chaos and IAM do not apply to the console, so it skips them, as other internal calls do.
func (h *Hub) serveConsole(svc Service, c *Ctx, w http.ResponseWriter, r *http.Request) {
	rec := &recorder{ResponseWriter: w}
	start := time.Now()
	svc.ServeHTTP(rec, r)
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	op := c.Op
	if _, t, ok := strings.Cut(r.Header.Get("X-Amz-Target"), "."); op == "" && ok {
		op = t
	}
	h.Requests.Add(Entry{
		Time: start, Cloud: c.Cloud, Service: c.Service, Op: cmp.Or(op, r.Method), Method: r.Method, Console: true,
		Path: redactPath(r.URL.RequestURI()), Status: rec.status, Ms: float64(time.Since(start).Microseconds()) / 1000,
		Account: c.Account, Region: c.Region, RequestID: c.RequestID, Error: c.ErrCode,
		RespBody: preview(rec.body.Bytes(), rec.Header().Get("Content-Type"), r.URL.Path),
	})
}

// LogConsole records a write the console made straight on a service (not through the hub), marked
// "console". err is the write's result. Every such write goes through it: web's UI.write.
func (h *Hub) LogConsole(service, op, target string, took time.Duration, err error) {
	e := Entry{Time: time.Now().Add(-took), Service: service, Op: op, Method: http.MethodPost, Path: target, Status: http.StatusOK,
		Ms: float64(took.Microseconds()) / 1000, Console: true, Account: DefaultAccount, Region: h.Cfg.Region}
	if s := h.byName[service]; s != nil {
		e.Cloud = s.Info().Cloud
	}
	if err != nil {
		ce := AsError(err)
		e.Status, e.Error, e.RespBody = ce.Status, ce.Code, ce.Msg
	}
	h.Requests.Add(e)
}

// route picks the service, following the smithy protocol-detection order.
func (h *Hub) route(r *http.Request, signing string) Service {
	if signing == "" && !strings.HasPrefix(r.Header.Get("Authorization"), "AWS ") {
		googlePrefix(r) // never an AWS request: an S3 bucket may be named "redis"
	}
	if t := r.Header.Get("X-Amz-Target"); t != "" {
		prefix, _, _ := strings.Cut(t, ".")
		if s := h.targets[prefix]; s != nil {
			return s
		}
	}
	if strings.HasPrefix(r.Header.Get("Smithy-Protocol"), "rpc-v2") {
		// POST /service/{ServiceName}/operation/{Op}
		if p := strings.Split(r.URL.Path, "/"); len(p) >= 5 && p[1] == "service" {
			if s := h.targets[p[2]]; s != nil {
				return s
			}
		}
	}
	wrongHost := false
	for _, s := range h.matchers {
		info := s.Info()
		if info.Cloud != AWS && (signing != "" || strings.HasPrefix(r.Header.Get("Authorization"), "AWS ")) {
			continue // a signed AWS request is for AWS, even where its path looks Google's or Azure's (MSK's /v1/operations/..., a bucket named "subscriptions")
		}
		if info.Match(r) {
			if googleHostOK(info.Name, r.Host) {
				return s
			}
			wrongHost = true
		}
	}
	if s := h.signing[signing]; s != nil {
		return s
	}
	if r.Header.Get("Authorization") == "" && r.Method == http.MethodGet && r.URL.Path == "/" &&
		strings.Contains(r.Header.Get("Accept"), "text/html") {
		return nil // a browser: show the console
	}
	switch { // S3 owns anonymous and presigned traffic, but no other cloud's
	case wrongHost:
		return unknownAPI{GCP, true, ""}
	case looksAWS(r, signing):
		return unknownAPI{AWS, false, ""}
	case looksGoogle(r):
		return unknownAPI{GCP, false, ""}
	}
	if k := azureKind(r); k != "" && signing == "" && !strings.HasPrefix(r.Header.Get("Authorization"), "AWS") {
		return unknownAPI{Azure, false, k}
	}
	return h.byName["s3"]
}

// sigv4 extracts the access key, region and signing name from SigV4 auth.
func sigv4(r *http.Request) (akid, region, service string) {
	cred := ""
	switch a := r.Header.Get("Authorization"); {
	case strings.HasPrefix(a, "AWS4-"):
		if i := strings.Index(a, "Credential="); i >= 0 {
			cred = a[i+len("Credential="):]
			if j := strings.IndexAny(cred, ", "); j >= 0 {
				cred = cred[:j]
			}
		}
	case strings.HasPrefix(a, "AWS "): // SigV2
		akid, _, _ = strings.Cut(strings.TrimPrefix(a, "AWS "), ":")
		return akid, "", ""
	default:
		if r.URL.RawQuery != "" {
			cred = r.URL.Query().Get("X-Amz-Credential")
		}
	}
	p := strings.Split(cred, "/")
	if len(p) >= 4 {
		return p[0], p[2], p[3]
	}
	return "", "", ""
}

// Identity is who an access key belongs to.
type Identity struct {
	Account string    `json:"account"`
	ARN     string    `json:"arn"`
	UserID  string    `json:"userId"`
	Secret  string    `json:"-"` // secret access key, for signature checking
	Expires time.Time `json:"-"` // temporary (STS) credentials stop working at this time; zero means never
}

// AddIdentity maps an access key (e.g. from STS AssumeRole) to an identity.
func (h *Hub) AddIdentity(akid string, id Identity) {
	h.mu.Lock()
	h.idents[akid] = id
	h.mu.Unlock()
}

// SetEndpoint records where pogi reaches a container-backed resource, for
// load balancers and proxies: "ip:10.0.1.5:80" or "instance:i-0abc:80" →
// "127.0.0.1:49153". An empty addr removes it.
func (h *Hub) SetEndpoint(key, addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.addrs == nil {
		h.addrs = map[string]string{}
	}
	if addr == "" {
		delete(h.addrs, key)
		return
	}
	h.addrs[key] = addr
}

// Endpoint returns the address recorded by SetEndpoint.
func (h *Hub) Endpoint(key string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	a, ok := h.addrs[key]
	return a, ok
}

// RemoveIdentity forgets an access key (deleted or deactivated IAM keys).
func (h *Hub) RemoveIdentity(akid string) {
	h.mu.Lock()
	delete(h.idents, akid)
	h.mu.Unlock()
}

// Identity looks up a registered access key.
func (h *Hub) Identity(akid string) (Identity, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	id, ok := h.idents[akid]
	return id, ok
}

// accountFor follows LocalStack: a 12-digit key is the account, else the default.
func (h *Hub) accountFor(akid, region string) (account, principal string) {
	if id, ok := h.Identity(akid); ok {
		return id.Account, id.ARN
	}
	account = DefaultAccount
	if len(akid) == 12 && strings.Trim(akid, "0123456789") == "" {
		account = akid
	}
	return account, GlobalARN("iam", region, account, "root")
}

// Message is an event handed from one service to another through the hub.
type Message struct {
	Body       string
	Attributes map[string]MessageAttribute
	Subject    string
	GroupID    string
	DedupID    string
	Trace      string   // the flow this message belongs to (Ctx.Trace of the request that caused it)
	From       string   // resource that sent it (ARN or full resource name), for the span log and the map
	Source     string   // EventBridge source
	DetailType string   // EventBridge detail-type
	Resources  []string // EventBridge resources
}

// MessageAttribute is an SQS/SNS-style typed attribute.
type MessageAttribute struct {
	DataType    string
	StringValue string `json:",omitempty"`
	BinaryValue []byte `json:",omitempty"`
}

// FunctionError is returned by Hub.Invoke when the function itself failed;
// Payload holds its error JSON.
type FunctionError struct{ Payload []byte }

func (e *FunctionError) Error() string { return "function error: " + string(e.Payload) }

// DeliverFunc receives a message for a resource ARN of one service.
type DeliverFunc func(arn string, m *Message) error

// InvokeFunc runs a function synchronously (Lambda).
type InvokeFunc func(arn string, payload []byte) ([]byte, error)

// OnDeliver registers the deliverer for one service's targets: an AWS
// service ("sqs", "sns"), a Google API ("pubsub", "run", "cloudfunctions")
// or an Azure resource provider ("microsoft.servicebus", "microsoft.eventhub").
func (h *Hub) OnDeliver(service string, f DeliverFunc) {
	h.mu.Lock()
	h.deliver[service] = f
	h.mu.Unlock()
}

// Deliver routes a message to the service that owns the target:
//   - an AWS ARN, to its service;
//   - a Google full resource name ("//pubsub.googleapis.com/projects/p/topics/t"),
//     to the API ("pubsub") with the name after the host ("projects/p/topics/t");
//   - an Azure resource ID ("/subscriptions/s/resourceGroups/g/providers/Microsoft.ServiceBus/namespaces/n/queues/q"),
//     to the lower-cased provider ("microsoft.servicebus") with the ID unchanged.
func (h *Hub) Deliver(arn string, m *Message) error {
	svc, ok := DeliverTarget(arn)
	if !ok {
		return fmt.Errorf("invalid delivery target %q", arn)
	}
	target := arn
	if t, google := strings.CutPrefix(arn, "//"); google {
		_, arn, _ = strings.Cut(t, "/")
	}
	h.mu.RLock()
	f := h.deliver[svc]
	h.mu.RUnlock()
	if f == nil {
		return fmt.Errorf("service %q is not enabled, cannot deliver to %s", svc, arn)
	}
	start := time.Now()
	err := f(arn, m)
	if svc != "lambda" { // Lambda records the invocation itself, with its real duration
		h.Span(SpanDeliver, svc, "deliver", m.From, target, m.Trace, start, err)
	}
	return err
}

// DeliverTarget names the service Deliver hands a target to.
func DeliverTarget(target string) (service string, ok bool) {
	if svc, _, _, _, ok := ParseARN(target); ok {
		return svc, true
	}
	if t, google := strings.CutPrefix(target, "//"); google {
		host, name, _ := strings.Cut(t, "/")
		api, isGoogle := strings.CutSuffix(host, ".googleapis.com")
		return api, isGoogle && api != "" && strings.HasPrefix(name, "projects/")
	}
	if lower := strings.ToLower(target); strings.HasPrefix(lower, "/subscriptions/") {
		_, rest, found := strings.Cut(lower, "/providers/")
		provider, _, _ := strings.Cut(rest, "/")
		return provider, found && provider != ""
	}
	return "", false
}

// OnInvoke registers the synchronous function invoker.
func (h *Hub) OnInvoke(f InvokeFunc) {
	h.mu.Lock()
	h.invoke = f
	h.mu.Unlock()
}

// InvokeFromFunc runs a function for a caller that is part of a flow: trace
// is passed on to the function runtime and from names the caller.
type InvokeFromFunc func(trace, from, arn string, payload []byte) ([]byte, error)

// OnInvokeFrom registers the invoker that takes a trace (Lambda).
func (h *Hub) OnInvokeFrom(f InvokeFromFunc) {
	h.mu.Lock()
	h.invokeFrom = f
	h.mu.Unlock()
}

// Invoke runs a function synchronously and returns its result.
func (h *Hub) Invoke(arn string, payload []byte) ([]byte, error) {
	return h.InvokeFrom("", "", arn, payload)
}

// InvokeFrom is Invoke inside a flow: trace reaches the function, and the
// invocation shows in the flow as a step from from (a resource id).
func (h *Hub) InvokeFrom(trace, from, arn string, payload []byte) ([]byte, error) {
	h.mu.RLock()
	f, ff := h.invoke, h.invokeFrom
	h.mu.RUnlock()
	if ff != nil {
		return ff(trace, from, arn, payload)
	}
	if f == nil {
		return nil, fmt.Errorf("lambda is not enabled, cannot invoke %s", arn)
	}
	return f(arn, payload)
}

// recorder captures the status and the start of the body for the request log.
type recorder struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

const previewMax = 32 << 10

func (r *recorder) WriteHeader(s int) {
	if r.status == 0 {
		r.status = s
	}
	r.ResponseWriter.WriteHeader(s)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if room := previewMax - r.body.Len(); room > 0 {
		r.body.Write(b[:min(room, len(b))])
	}
	return r.ResponseWriter.Write(b)
}

func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func preview(b []byte, contentType, path string) string {
	if len(b) == 0 {
		return ""
	}
	cut := b[:min(len(b), previewMax)]
	probe := cut // tolerate a rune split by the cut
	for i := 0; i < 3 && len(probe) > 0 && !utf8.Valid(probe); i++ {
		probe = probe[:len(probe)-1]
	}
	if !utf8.Valid(probe) || strings.HasPrefix(contentType, "image/") {
		return fmt.Sprintf("[binary, %d bytes]", len(b))
	}
	// redact before cutting, so a secret on the boundary is not half shown
	text := redact(string(b[:min(len(b), previewMax+4096)]), path)
	if len(cut) < len(b) {
		return text[:min(len(text), previewMax)] + fmt.Sprintf("\n… [%d bytes total]", len(b))
	}
	return text
}

// secretName matches the names of fields that hold secrets: any *password, any access, id, refresh, session,
// auth, security or api token, and a few known secret names. The request log hides their values and keeps the names.
const secretName = `[\w.-]*password|(?:access|id|refresh|session|auth|security|api|bearer)[_-]?token|` +
	`secret_hash|secretstring|secretbinary|secretaccesskey|clientsecret`

var (
	secretJSON = regexp.MustCompile(`(?i)("(?:` + secretName + `)"\s*:\s*)"(?:[^"\\]|\\.)*"`)
	secretForm = regexp.MustCompile(`(?i)((?:^|&)(?:` + secretName + `)=)[^&]*`)
	secretXML  = regexp.MustCompile(`(?i)(<(?:` + secretName + `)>)[^<]*`)
	// Google Secret Manager payload, Lambda environment variables, and a "value" field.
	secretPayload = regexp.MustCompile(`("payload"\s*:\s*\{\s*"data"\s*:\s*)"(?:[^"\\]|\\.)*"`)
	secretEnv     = regexp.MustCompile(`(?i)"variables"\s*:\s*\{[^{}]*\}`)
	envPair       = regexp.MustCompile(`("(?:[^"\\]|\\.)*"\s*:\s*)"(?:[^"\\]|\\.)*"`)
	secretValue   = regexp.MustCompile(`(?i)("value"\s*:\s*)"(?:[^"\\]|\\.)*"`)
	// a query parameter or header whose name says it holds a secret or key
	secretParam = regexp.MustCompile(`(?i)^(?:[\w.-]*(?:password|secret|signature)|[\w.-]*(?:^|[-_]|api)key|(?:[\w.-]*[-_])?token)$`)
)

// redact hides secret values in JSON, form-encoded and XML bodies. path is the request path: Key Vault
// secrets are plain "value" fields, and SSM ones are "Value" only when the text says SecureString.
func redact(s, path string) string {
	s = secretJSON.ReplaceAllString(s, `$1"[redacted]"`)
	s = secretForm.ReplaceAllString(s, `${1}[redacted]`)
	s = secretXML.ReplaceAllString(s, `${1}[redacted]`)
	s = secretPayload.ReplaceAllString(s, `$1"[redacted]"`)
	s = secretEnv.ReplaceAllStringFunc(s, func(m string) string { return envPair.ReplaceAllString(m, `$1"[redacted]"`) })
	if strings.Contains(path, "/secrets/") || strings.Contains(s, `"SecureString"`) {
		s = secretValue.ReplaceAllString(s, `$1"[redacted]"`)
	}
	return s
}

// redactPath hides the values of secret query parameters, such as X-Amz-Security-Token and key.
func redactPath(uri string) string {
	path, q, ok := strings.Cut(uri, "?")
	if !ok {
		return uri
	}
	parts := strings.Split(q, "&")
	for i, p := range parts {
		if k, _, ok := strings.Cut(p, "="); ok {
			if name, err := url.QueryUnescape(k); err == nil && secretParam.MatchString(name) {
				parts[i] = k + "=[redacted]"
			}
		}
	}
	return path + "?" + strings.Join(parts, "&")
}

func flatHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if k == "Authorization" || secretParam.MatchString(k) {
			out[k] = "[redacted]"
			continue
		}
		out[k] = strings.Join(v, ", ")
	}
	return out
}

// IAMMode is the IAM enforcement mode, off | soft | strict. It starts as
// Cfg.IAM and the console can change it while pogi runs.
func (h *Hub) IAMMode() string {
	if m := h.iamMode.Load(); m != nil {
		return *m
	}
	return h.Cfg.IAM
}

// SetIAMMode switches IAM enforcement for the requests that follow.
func (h *Hub) SetIAMMode(mode string) { h.iamMode.Store(&mode) }
