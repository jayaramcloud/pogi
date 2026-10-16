// Package functions emulates Cloud Functions (v2 API) over gRPC and its REST
// mirror, and runs Python functions with the Functions Framework: natively
// when it is installed (see "pogi lambda-sdks" or put functions-framework on
// PATH), otherwise in a python container.
//
// A function answers at http://<function>.cloudfunctions.localhost:<port>/ and
// at /_gcp/functions/<project>/<region>/<function>/... on pogi's port. A
// Pub/Sub trigger is a push subscription of the topic that pogi points at an
// internal endpoint, which wraps each message as a
// google.cloud.pubsub.topic.v1.messagePublished CloudEvent. Other events
// (Cloud Storage through Eventarc) arrive by Hub.Deliver to the key
// "cloudfunctions" with the function's name as the target.
package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/serverless"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Cloud Functions emulator.
type Service struct {
	functionspb.UnimplementedFunctionServiceServer
	hub  *core.Hub
	ops  *gapi.Ops
	rest *gapi.REST

	mu    sync.Mutex
	fns   map[string]*function
	procs map[string]*proc
	logs  map[string]*serverless.Logs
}

// function is a stored function and the Pub/Sub subscription that feeds it.
type function struct {
	p   *functionspb.Function
	sub string
}

const (
	pubsubEvent  = "google.cloud.pubsub.topic.v1.messagePublished"
	storagePfx   = "google.cloud.storage.object.v1."
	hostSuffix   = "cloudfunctions.localhost"
	defaultSA    = "pogi-compute@developer.gserviceaccount.com"
	functionsDir = "functions"
)

var (
	parentRE = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
	idRE     = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)
	pyRE     = regexp.MustCompile(`^python3(\d+)$`)
)

// New creates the Cloud Functions service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, ops: gapi.Operations(h)}
	s.Reset()
	s.rest = &gapi.REST{Versions: []string{"v2", "v2beta"}, Collections: []string{"functions", "runtimes"}, Desc: &functionspb.FunctionService_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"POST locations/*/functions":                       {Method: "CreateFunction", Field: "parent", Body: "function"},
			"GET locations/*/functions":                        {Method: "ListFunctions", Field: "parent"},
			"GET locations/*/functions/*":                      {Method: "GetFunction", Field: "name"},
			"PATCH locations/*/functions/*":                    {Method: "UpdateFunction", Field: "function.name", Body: "function"},
			"DELETE locations/*/functions/*":                   {Method: "DeleteFunction", Field: "name"},
			"POST locations/*/functions:generateUploadUrl":     {Method: "GenerateUploadUrl", Field: "parent"},
			"POST locations/*/functions/*:generateDownloadUrl": {Method: "GenerateDownloadUrl", Field: "name"},
			"GET locations/*/runtimes":                         {Method: "ListRuntimes", Field: "parent"},
		}}
	h.OnDeliver("cloudfunctions", s.deliver)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudfunctions", Cloud: core.GCP, Title: "Cloud Functions", Match: s.match,
		Targets: []string{"google.cloud.functions.v2.FunctionService"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { functionspb.RegisterFunctionServiceServer(g, s) }

func (s *Service) match(r *http.Request) bool { return s.rest.Match(r) || isData(r) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isData(r) {
		s.serveData(w, r)
		return
	}
	s.rest.ServeHTTP(w, r)
}

// Reset stops every function and forgets all of them.
func (s *Service) Reset() {
	s.Close()
	s.mu.Lock()
	s.fns, s.procs, s.logs = map[string]*function{}, map[string]*proc{}, map[string]*serverless.Logs{}
	s.mu.Unlock()
}

// Close stops the running functions.
func (s *Service) Close() error {
	s.mu.Lock()
	procs := s.procs
	s.procs = map[string]*proc{}
	s.mu.Unlock()
	for _, p := range procs {
		p.stop()
	}
	return nil
}

// ---- persistence ----

type savedFn struct {
	Function json.RawMessage `json:"function"`
	Sub      string          `json:"sub,omitempty"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []savedFn{}
	for _, n := range core.SortedKeys(s.fns) {
		b, _ := protojson.Marshal(s.fns[n].p)
		out = append(out, savedFn{b, s.fns[n].sub})
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in []savedFn
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	fns := map[string]*function{}
	for _, x := range in {
		p := &functionspb.Function{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(x.Function, p); err != nil {
			return err
		}
		fns[p.Name] = &function{p, x.Sub}
	}
	s.Reset()
	s.mu.Lock()
	s.fns = fns // processes start again on the first request
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

func last(name string) string { return name[strings.LastIndexByte(name, '/')+1:] }

func parent(name string) string { return name[:strings.LastIndexByte(name, '/')] }

func splitName(name string) (project, region string) {
	p := strings.Split(name, "/")
	if len(p) >= 4 {
		return p[1], p[3]
	}
	return "", ""
}

func (s *Service) url(id string) string {
	return "http://" + id + "." + hostSuffix + ":" + serverless.Port(s.hub) + "/"
}

func notFound(name string) error {
	return gapi.NotFound("Function '%s' does not exist.", name)
}

func opMeta(name, verb string, typ functionspb.OperationType) *functionspb.OperationMetadata {
	return &functionspb.OperationMetadata{CreateTime: timestamppb.Now(), Target: name, Verb: verb, ApiVersion: "v2", OperationType: typ}
}

func page(n int, size int32, token string) (lo, hi int, next string) {
	lo, _ = strconv.Atoi(token)
	lo = min(max(lo, 0), n)
	if size <= 0 {
		size = 100
	}
	hi = min(lo+int(size), n)
	if hi < n {
		next = strconv.Itoa(hi)
	}
	return
}

func rev(n int) string {
	return fmt.Sprintf("%05d-%s", n, core.RandString(3, "abcdefghijklmnopqrstuvwxyz"))
}

// ---- API ----

// check validates a function and fills what Cloud Functions fills in.
func (s *Service) check(f *functionspb.Function) error {
	bc := f.BuildConfig
	switch {
	case bc == nil || bc.Runtime == "":
		return gapi.Invalid("Invalid value for build_config.runtime: required")
	case !pyRE.MatchString(bc.Runtime):
		return gapi.Invalid("Runtime %q is not supported by pogi: it runs python runtimes (python39 ... python313)", bc.Runtime) // ponytail: no node, go, java
	case bc.EntryPoint == "":
		return gapi.Invalid("Invalid value for build_config.entry_point: required")
	case bc.Source.GetStorageSource().GetObject() == "":
		return gapi.Invalid("Invalid value for build_config.source: a storage_source with an object is required (see generateUploadUrl)")
	}
	if t := f.EventTrigger; t != nil {
		switch {
		case t.EventType == "":
			return gapi.Invalid("Invalid value for event_trigger.event_type: required")
		case t.EventType == pubsubEvent && t.PubsubTopic == "":
			return gapi.Invalid("Invalid value for event_trigger.pubsub_topic: required for %s", pubsubEvent)
		case strings.HasPrefix(t.EventType, storagePfx):
			ok := false
			for _, e := range t.EventFilters {
				ok = ok || e.Attribute == "bucket" && e.Value != ""
			}
			if !ok {
				return gapi.Invalid("Invalid value for event_trigger.event_filters: a bucket filter is required for %s", t.EventType)
			}
		}
	}
	return nil
}

// fill sets the generated fields of a new or updated function.
func (s *Service) fill(f *functionspb.Function, revision int) {
	project, region := splitName(f.Name)
	id := last(f.Name)
	f.Environment, f.Url = functionspb.Environment_GEN_2, s.url(id)
	f.BuildConfig.Build = fmt.Sprintf("projects/%s/locations/%s/builds/%s", project, region, core.UUID())
	sc := f.ServiceConfig
	if sc == nil {
		sc = &functionspb.ServiceConfig{}
		f.ServiceConfig = sc
	}
	sc.Service = fmt.Sprintf("projects/%s/locations/%s/services/%s", project, region, id)
	sc.Uri, sc.Revision = f.Url, id+"-"+rev(revision)
	sc.AllTrafficOnLatestRevision = true
	if sc.TimeoutSeconds == 0 {
		sc.TimeoutSeconds = 60
	}
	if sc.AvailableMemory == "" {
		sc.AvailableMemory = "256M"
	}
	if sc.AvailableCpu == "" {
		sc.AvailableCpu = "0.1666"
	}
	if sc.MaxInstanceCount == 0 {
		sc.MaxInstanceCount = 100
	}
	if sc.MaxInstanceRequestConcurrency == 0 {
		sc.MaxInstanceRequestConcurrency = 1
	}
	if sc.IngressSettings == 0 {
		sc.IngressSettings = functionspb.ServiceConfig_ALLOW_ALL
	}
	if sc.SecurityLevel == 0 {
		sc.SecurityLevel = functionspb.ServiceConfig_SECURE_ALWAYS
	}
	if sc.ServiceAccountEmail == "" {
		sc.ServiceAccountEmail = defaultSA
	}
	if t := f.EventTrigger; t != nil {
		if t.Trigger == "" {
			t.Trigger = fmt.Sprintf("projects/%s/locations/%s/triggers/%s-%s", project, region, id, core.RandHex(3))
		}
		t.TriggerRegion = region
		t.Service = sc.Service
		if t.ServiceAccountEmail == "" {
			t.ServiceAccountEmail = defaultSA
		}
		if t.RetryPolicy == 0 {
			t.RetryPolicy = functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY
		}
	}
}

func (s *Service) CreateFunction(_ context.Context, req *functionspb.CreateFunctionRequest) (*longrunningpb.Operation, error) {
	if !parentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<region>", req.Parent)
	}
	if !idRE.MatchString(req.FunctionId) {
		return nil, gapi.Invalid("Invalid function ID %q: lowercase letters, digits and '-', starting with a letter, at most 63 characters", req.FunctionId)
	}
	if req.Function == nil {
		return nil, gapi.Invalid("Invalid value for function: required")
	}
	f := proto.Clone(req.Function).(*functionspb.Function)
	if err := s.check(f); err != nil {
		return nil, err
	}
	f.Name = req.Parent + "/functions/" + req.FunctionId
	f.CreateTime, f.UpdateTime, f.State = timestamppb.Now(), timestamppb.Now(), functionspb.Function_DEPLOYING
	s.fill(f, 1)
	s.mu.Lock()
	if s.fns[f.Name] != nil {
		s.mu.Unlock()
		return nil, gapi.Exists("Function '%s' already exists.", f.Name)
	}
	s.fns[f.Name] = &function{p: f}
	s.mu.Unlock()
	return s.deploy(f.Name, req.Parent, "create", functionspb.OperationType_CREATE_FUNCTION), nil
}

// deploy starts (or restarts) the function in an operation: the "build" is
// fetching the source, and the function is active once it listens.
func (s *Service) deploy(name, parent, verb string, typ functionspb.OperationType) *longrunningpb.Operation {
	return s.ops.Start(parent, opMeta(name, verb, typ), func() (proto.Message, error) {
		s.stopProc(name)
		err := s.wireTrigger(name)
		if err == nil {
			_, err = s.ensure(name)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		fn := s.fns[name]
		if fn == nil {
			return nil, gapi.NotFound("Function '%s' was deleted.", name)
		}
		fn.p.UpdateTime = timestamppb.Now()
		if err != nil {
			fn.p.State = functionspb.Function_FAILED
			fn.p.StateMessages = []*functionspb.StateMessage{{Severity: functionspb.StateMessage_ERROR, Type: "DeployError", Message: err.Error()}}
			return nil, gapi.Invalid("Function failed to deploy: %v", err)
		}
		fn.p.State, fn.p.StateMessages = functionspb.Function_ACTIVE, nil
		return proto.Clone(fn.p), nil
	})
}

func (s *Service) GetFunction(_ context.Context, req *functionspb.GetFunctionRequest) (*functionspb.Function, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fn := s.fns[req.Name]; fn != nil {
		return proto.Clone(fn.p).(*functionspb.Function), nil
	}
	return nil, notFound(req.Name)
}

func (s *Service) ListFunctions(_ context.Context, req *functionspb.ListFunctionsRequest) (*functionspb.ListFunctionsResponse, error) {
	if !parentRE.MatchString(req.Parent) && !strings.HasSuffix(req.Parent, "/locations/-") {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<region>", req.Parent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*functionspb.Function
	for _, n := range core.SortedKeys(s.fns) {
		if p := parent(n); p == req.Parent+"/functions" || strings.HasSuffix(req.Parent, "/locations/-") && strings.HasPrefix(p, strings.TrimSuffix(req.Parent, "-")) {
			all = append(all, s.fns[n].p)
		}
	}
	lo, hi, next := page(len(all), req.PageSize, req.PageToken)
	out := &functionspb.ListFunctionsResponse{NextPageToken: next}
	for _, f := range all[lo:hi] {
		out.Functions = append(out.Functions, proto.Clone(f).(*functionspb.Function))
	}
	return out, nil
}

func (s *Service) UpdateFunction(_ context.Context, req *functionspb.UpdateFunctionRequest) (*longrunningpb.Operation, error) {
	in := req.Function
	if in == nil {
		return nil, gapi.Invalid("Invalid value for function: required")
	}
	s.mu.Lock()
	fn := s.fns[in.Name]
	if fn == nil {
		s.mu.Unlock()
		return nil, notFound(in.Name)
	}
	next := proto.Clone(fn.p).(*functionspb.Function)
	paths := req.UpdateMask.GetPaths()
	has := func(field string) bool {
		if len(paths) == 0 {
			return true
		}
		for _, p := range paths {
			if p == field || strings.HasPrefix(p, field+".") {
				return true
			}
		}
		return false
	}
	if in.BuildConfig != nil && has("build_config") {
		next.BuildConfig = proto.Clone(in.BuildConfig).(*functionspb.BuildConfig)
	}
	if in.ServiceConfig != nil && has("service_config") {
		next.ServiceConfig = proto.Clone(in.ServiceConfig).(*functionspb.ServiceConfig)
	}
	if in.EventTrigger != nil && has("event_trigger") {
		next.EventTrigger = proto.Clone(in.EventTrigger).(*functionspb.EventTrigger)
	}
	if has("labels") {
		next.Labels = in.Labels
	}
	if has("description") {
		next.Description = in.Description
	}
	if err := s.check(next); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	n, _ := strconv.Atoi(strings.SplitN(strings.TrimPrefix(last(fn.p.ServiceConfig.GetRevision()), last(next.Name)+"-"), "-", 2)[0])
	s.fill(next, n+1)
	next.State, next.UpdateTime = functionspb.Function_DEPLOYING, timestamppb.Now()
	fn.p = next
	s.mu.Unlock()
	return s.deploy(next.Name, parent(parent(next.Name)), "update", functionspb.OperationType_UPDATE_FUNCTION), nil
}

func (s *Service) DeleteFunction(_ context.Context, req *functionspb.DeleteFunctionRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	fn := s.fns[req.Name]
	if fn == nil {
		s.mu.Unlock()
		return nil, notFound(req.Name)
	}
	delete(s.fns, req.Name)
	delete(s.logs, req.Name)
	s.mu.Unlock()
	s.stopProc(req.Name)
	s.unwire(fn)
	return s.ops.Done(parent(parent(req.Name)), opMeta(req.Name, "delete", functionspb.OperationType_DELETE_FUNCTION), nil), nil
}

// GenerateUploadUrl makes an object name in a bucket of the project; a PUT to
// the URL stores the source zip there.
func (s *Service) GenerateUploadUrl(_ context.Context, req *functionspb.GenerateUploadUrlRequest) (*functionspb.GenerateUploadUrlResponse, error) {
	if !parentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<region>", req.Parent)
	}
	project, region := splitName(req.Parent)
	bucket, object := "gcf-v2-uploads-"+project+"-"+region, core.UUID()+".zip"
	body, _ := json.Marshal(map[string]string{"name": bucket})
	r, _ := http.NewRequest(http.MethodPost, "http://localhost/storage/v1/b?project="+project, strings.NewReader(string(body)))
	r.Header.Set("Content-Type", "application/json")
	if code, msg := s.hub.DoService("gcs", r); code != http.StatusOK && code != http.StatusConflict {
		return nil, gapi.Invalid("Cannot create the upload bucket (%d): %s", code, strings.TrimSpace(string(msg)))
	}
	return &functionspb.GenerateUploadUrlResponse{
		UploadUrl:     s.hub.URL + "/" + bucket + "/" + object + "?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Signature=pogi",
		StorageSource: &functionspb.StorageSource{Bucket: bucket, Object: object}}, nil
}

func (s *Service) GenerateDownloadUrl(_ context.Context, req *functionspb.GenerateDownloadUrlRequest) (*functionspb.GenerateDownloadUrlResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn := s.fns[req.Name]
	if fn == nil {
		return nil, notFound(req.Name)
	}
	src := fn.p.BuildConfig.GetSource().GetStorageSource()
	return &functionspb.GenerateDownloadUrlResponse{DownloadUrl: s.hub.URL + "/" + src.GetBucket() + "/" + src.GetObject() + "?X-Goog-Signature=pogi"}, nil
}

func (s *Service) ListRuntimes(_ context.Context, req *functionspb.ListRuntimesRequest) (*functionspb.ListRuntimesResponse, error) {
	out := &functionspb.ListRuntimesResponse{}
	for _, v := range []string{"9", "10", "11", "12", "13"} {
		out.Runtimes = append(out.Runtimes, &functionspb.ListRuntimesResponse_Runtime{Name: "python3" + v, DisplayName: "Python 3." + v,
			Stage: functionspb.ListRuntimesResponse_GA, Environment: functionspb.Environment_GEN_2})
	}
	return out, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "functions", Columns: []string{"Name", "Runtime", "State", "Trigger", "URL"}}
	for _, n := range core.SortedKeys(s.fns) {
		f := s.fns[n].p
		trig := "HTTP"
		if f.EventTrigger != nil {
			trig = f.EventTrigger.EventType
		}
		t.Rows = append(t.Rows, []string{n, f.BuildConfig.GetRuntime(), f.State.String(), trig, f.Url})
	}
	return []core.Table{t}
}

// Inspect shows a function as the API returns it, with its output.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	fn, logs := s.fns[id], s.logs[id]
	s.mu.Unlock()
	if table != "functions" || fn == nil {
		return nil
	}
	b, _ := protojson.Marshal(fn.p)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	out["logs"] = logs.Lines()
	return out
}
