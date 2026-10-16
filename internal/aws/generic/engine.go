package generic

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Factory builds the service of one embedded model.
type Factory struct {
	Name string
	New  func(*core.Hub) core.Service
}

// Services returns a factory for every embedded model, sorted by name. The
// caller registers them after the hand-written services and skips names that
// already exist.
func Services() []Factory {
	var out []Factory
	for _, n := range core.SortedKeys(loadIndex()) {
		out = append(out, Factory{n, func(h *core.Hub) core.Service { return New(h, n) }})
	}
	return out
}

// globalServices keep one set of resources for every region, with region-less ARNs.
var globalServices = map[string]bool{"globalaccelerator": true, "route53": true, "account": true}

// arnSep is the separator after the first segment of an ARN's resource part (default "/").
var arnSep = map[string]string{"backup": ":"}

// arnService is the service part of ARNs where it differs from the endpoint prefix.
var arnService = map[string]string{"bedrock-agent": "bedrock", "bedrock-agent-runtime": "bedrock", "bedrock-runtime": "bedrock", "bedrock-agentcore-control": "bedrock-agentcore", "sagemaker": "sagemaker"}

// arnTemplates are the resource parts of ARNs that the default (<type>/<id>, nested as
// <parent>/<id>/<type>/<id>) gets wrong, by "<service>/<Type>"; {Member} is an identifier.
var arnTemplates = map[string]string{
	"codeartifact/Repository":                "repository/{domain}/{repository}",
	"bedrock-agent/AgentAlias":               "agent-alias/{agentId}/{agentAliasId}",
	"bedrock-agentcore-control/AgentRuntime": "runtime/{agentRuntimeId}",
	"textract/Adapter":                       "/adapters/{AdapterId}",
}

// Service is one generated service. Its model is parsed on the first request.
type Service struct {
	h    *core.Hub
	meta *meta
	info core.Info

	once sync.Once
	m    *model
	merr error

	mu    sync.Mutex
	st    state
	ticks map[string]func(time.Time)
	stops []func()
}

// New makes the service for a model name (the botocore directory, e.g. "efs").
func New(h *core.Hub, name string) *Service {
	md := loadIndex()[name]
	if md == nil {
		panic("generic: no model " + name)
	}
	s := &Service{h: h, meta: md, st: state{Res: map[string]*Resource{}}}
	signing := md.SigningName
	if signing == "" {
		signing = md.EndpointPrefix
	}
	title := md.Title
	if title == "" {
		title = md.ServiceID
	}
	s.info = core.Info{Name: name, Cloud: core.AWS, Title: title, Protocol: strings.Replace(md.Protocol, "ec2", "query", 1)}
	// A name a hand-written service already routes on stays with it.
	takenSigning, takenTarget := false, false
	target := md.TargetPrefix
	if md.Protocol == "query" || md.Protocol == "ec2" {
		target = strings.ReplaceAll(md.ServiceID, " ", "") + "_" + strings.ReplaceAll(md.APIVersion, "-", "")
	}
	if h != nil {
		for _, o := range h.Services() {
			i := o.Info()
			takenSigning = takenSigning || slices.Contains(i.Signing, signing)
			takenTarget = takenTarget || slices.Contains(i.Targets, target)
		}
	}
	if !takenTarget {
		s.info.Targets = []string{target}
	}
	switch {
	case !takenSigning:
		s.info.Signing = []string{signing}
	case md.Protocol == "query" || md.Protocol == "ec2":
		// Query services that share a signing name (Classic ELB and ELB v2) tell apart by Version.
		s.info.Match = func(r *http.Request) bool { return matchQuery(r, signing, md.APIVersion) }
	case strings.HasPrefix(md.Protocol, "rest"):
		// REST services that share a signing name (Bedrock, Bedrock Runtime and the agent APIs all
		// sign as "bedrock") tell apart by whether the method and path are one of their operations.
		s.info.Match = func(r *http.Request) bool {
			if a := r.Header.Get("Authorization"); strings.Contains(a, "/aws4_request") && !strings.Contains(a, "/"+signing+"/aws4_request") {
				return false // signed for another service that has the same paths (AgentCore and Agents both have /tags/{arn})
			}
			m, err := s.model()
			if err != nil {
				return false
			}
			op, _ := m.match(r)
			return op != nil
		}
	}
	if m := matchers[name]; m != nil {
		s.info.Match = m
	}
	if h != nil {
		for _, f := range starters[name] {
			f(s)
		}
	}
	return s
}

// matchers claim requests for a service whose signing name a hand-written
// service owns (Lambda microVMs beside Lambda), by service name.
var matchers = map[string]func(*http.Request) bool{}

// starters run when a service is made: the services with background work start it here (Service.Every).
var starters = map[string][]func(*Service){}

// resumers run when a service loads its stored resources: the services with containers start them again (Service.resume).
var resumers = map[string]func(*Service){}

func (s *Service) Info() core.Info { return s.info }

// Every starts the background work of a service (budget alerts, report
// delivery) on a ticker, once per name. Tick runs it all once, for tests, and
// Close stops it.
func (s *Service) Every(name string, d time.Duration, f func(now time.Time)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ticks[name] != nil {
		return
	}
	if s.ticks == nil {
		s.ticks = map[string]func(time.Time){}
	}
	// AWS bills by UTC day and month: a local clock would put a run after local midnight but
	// before UTC midnight in the wrong period.
	utc := func(now time.Time) { f(now.UTC()) }
	s.ticks[name] = utc
	s.stops = append(s.stops, core.Every(d, utc))
}

// Tick runs the background work once as of now.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	var fs []func(time.Time)
	for _, n := range core.SortedKeys(s.ticks) {
		fs = append(fs, s.ticks[n])
	}
	s.mu.Unlock()
	for _, f := range fs {
		f(now)
	}
}

// Close stops the background work and runs the OnClose hooks; the hub calls it on shutdown.
func (s *Service) Close() error {
	s.runClosers()
	s.mu.Lock()
	stops := s.stops
	s.stops, s.ticks = nil, nil
	s.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
	return nil
}

// matchQuery claims a Query request signed for signing whose Version is version.
func matchQuery(r *http.Request, signing, version string) bool {
	if r.Method != http.MethodPost || !strings.Contains(r.Header.Get("Authorization"), "/"+signing+"/aws4_request") ||
		r.Header.Get("X-Amz-Target") != "" || r.Body == nil {
		return false
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(b), r.Body), r.Body}
	form, _ := url.ParseQuery(string(b))
	return form.Get("Version") == version
}

func (s *Service) model() (*model, error) {
	s.once.Do(func() { s.m, s.merr = loadModel(s.meta) })
	return s.m, s.merr
}

func (s *Service) region(r string) string {
	if globalServices[s.meta.Name] {
		return ""
	}
	return r
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if raw := rawFor(s.meta.Name); raw != nil && raw(s, w, r) {
		return
	}
	c := core.C(r)
	body := core.Body(r)
	m, err := s.model()
	if err != nil {
		core.WriteJSONError(w, c, "1.0", core.Errorf(500, "InternalFailure", "model: %v", err))
		return
	}
	op, in, wire, err := m.parse(r, body)
	if op != nil {
		c.Op = op.Name
	}
	if err != nil {
		m.writeErr(w, c, wire, err)
		return
	}
	if err := c.Intercept(); err != nil {
		m.writeErr(w, c, wire, err)
		return
	}
	call := &Call{Svc: s, Op: op.Name, In: in, Core: c, Account: c.Account, Region: c.Region, op: op, m: m}
	out, err := s.run(call)
	if err != nil {
		m.writeErr(w, c, wire, err)
		return
	}
	if call.Stream != nil {
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		call.Stream(w)
		return
	}
	s.respond(w, c, m, wire, op, out)
}

// run validates a call, then runs its override or the default behaviour, then its hooks.
func (s *Service) run(c *Call) (map[string]any, error) {
	if c.op.in != nil {
		if msg := validate(c.op.in, c.In, true, nil); msg != "" {
			return nil, c.m.modelErr(c.op, eInvalid, "", msg)
		}
	}
	var out map[string]any
	var err error
	if fn := override(s.meta.Name, c.Op); fn != nil {
		out, err = fn(c)
	} else {
		out, err = c.Default()
	}
	if err == nil {
		for _, h := range hooksFor(s.meta.Name, c.Op) {
			h(c)
		}
	}
	return out, err
}

// ---- overrides and hooks

var reg struct {
	sync.RWMutex
	over  map[string]func(*Call) (map[string]any, error)
	hooks map[string][]func(*Call)
}

// Override replaces the behaviour of one operation of a service (the botocore
// directory name, e.g. "efs"). fn gets the parsed input and returns the output
// in member names; it may call c.Default() for the engine's own result and
// change it.
func Override(service, op string, fn func(*Call) (map[string]any, error)) {
	reg.Lock()
	defer reg.Unlock()
	if reg.over == nil {
		reg.over = map[string]func(*Call) (map[string]any, error){}
	}
	reg.over[service+"/"+op] = fn
}

// Hook runs fn after an operation succeeds, with c.Res set to the resource it
// created, updated or deleted (nil when it touched none): for side effects.
func Hook(service, op string, fn func(*Call)) {
	reg.Lock()
	defer reg.Unlock()
	if reg.hooks == nil {
		reg.hooks = map[string][]func(*Call){}
	}
	reg.hooks[service+"/"+op] = append(reg.hooks[service+"/"+op], fn)
}

func override(service, op string) func(*Call) (map[string]any, error) {
	reg.RLock()
	defer reg.RUnlock()
	return reg.over[service+"/"+op]
}

func hooksFor(service, op string) []func(*Call) {
	reg.RLock()
	defer reg.RUnlock()
	return reg.hooks[service+"/"+op]
}

// Call is one operation in flight, as an override or hook sees it.
type Call struct {
	Svc     *Service
	Op      string
	In      map[string]any // the input, keyed by the model's member names
	Core    *core.Ctx      // nil for Service.Do
	Account string
	Region  string
	Res     *Resource // set by the default behaviour

	// Stream, when an override sets it, writes the whole response itself (event streams).
	Stream func(http.ResponseWriter)

	op *operation
	m  *model
}

// Hub is the hub of the service, for cross-service calls.
func (c *Call) Hub() *core.Hub { return c.Svc.h }

// Default runs the engine's behaviour for the operation.
func (c *Call) Default() (map[string]any, error) {
	switch c.op.kind {
	case kCreate:
		return c.create()
	case kRead:
		return c.read()
	case kList:
		return c.list()
	case kUpdate:
		return c.update()
	case kDelete:
		return c.del()
	case kTag, kUntag:
		return c.tag()
	case kListTags:
		return c.listTags()
	}
	return c.other()
}

// Fill builds an output the way the default behaviour does, from a resource (or nil).
func (c *Call) Fill(r *Resource) map[string]any { return c.fill(c.op.out, r) }

// Get returns a copy of the resource of a type whose identifiers (in the order
// of the type's identifier members) are ids, or nil.
func (c *Call) Get(typ string, ids ...string) *Resource {
	return c.Svc.Get(c.Account, c.Region, typ, strings.Join(ids, "/"))
}

// List returns copies of the resources of a type in this account and region.
func (c *Call) List(typ string) []*Resource { return c.Svc.List(c.Account, c.Region, typ) }

// Put stores a resource changed by an override.
func (c *Call) Put(r *Resource) {
	c.Svc.mu.Lock()
	c.Svc.st.Res[r.skey()] = r
	c.Svc.mu.Unlock()
}

// NotFound, Exists and Invalid are the model's errors for those cases.
func (c *Call) NotFound(format string, a ...any) error {
	return c.m.modelErr(c.op, eNotFound, c.typeName(), fmt.Sprintf(format, a...))
}
func (c *Call) Exists(format string, a ...any) error {
	return c.m.modelErr(c.op, eExists, c.typeName(), fmt.Sprintf(format, a...))
}
func (c *Call) Invalid(format string, a ...any) error {
	return c.m.modelErr(c.op, eInvalid, "", fmt.Sprintf(format, a...))
}

// Err is the model's error shape of that name (with its code and HTTP status), for errors no pattern finds.
func (c *Call) Err(shape, format string, a ...any) error {
	msg := fmt.Sprintf(format, a...)
	if sh := c.m.shapes[shape]; sh != nil {
		return c.m.shapeErr(sh, msg)
	}
	return core.Errorf(http.StatusBadRequest, shape, "%s", msg)
}

func (c *Call) typeName() string {
	if c.op.typ != nil {
		return c.op.typ.name
	}
	return ""
}

// ---- API for tests and Cloud Control

// Types lists the inferred resource types.
func (s *Service) Types() []string {
	m, err := s.model()
	if err != nil {
		return nil
	}
	var out []string
	for _, t := range m.types {
		out = append(out, t.name)
	}
	slices.Sort(out)
	return out
}

// Do runs an operation in-process (no signature, chaos or IAM checks). The
// input uses the model's member names; the output does too.
func (s *Service) Do(account, region, op string, in map[string]any) (map[string]any, error) {
	m, err := s.model()
	if err != nil {
		return nil, err
	}
	o := m.ops[op]
	if o == nil {
		return nil, core.Errorf(400, "UnknownOperationException", "no operation %s", op)
	}
	if in == nil {
		in = map[string]any{}
	}
	return s.run(&Call{Svc: s, Op: op, In: in, Account: account, Region: region, op: o, m: m})
}

// List returns copies of the resources of a type.
func (s *Service) List(account, region, typ string) []*Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Resource
	for _, r := range s.listLocked(account, s.region(region), typ) {
		out = append(out, r.clone())
	}
	return out
}

// Get returns a copy of a resource by its key (identifiers joined with "/"), or nil.
func (s *Service) Get(account, region, typ, key string) *Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.st.Res[skey(account, s.region(region), typ, key)]; r != nil {
		return r.clone()
	}
	return nil
}

// Delete removes a resource by key and reports whether it existed.
func (s *Service) Delete(account, region, typ, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := skey(account, s.region(region), typ, key)
	_, ok := s.st.Res[k]
	delete(s.st.Res, k)
	return ok
}
