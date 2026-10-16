// Package run emulates Google Cloud Run's Admin API v2 over gRPC and its REST
// mirror (services, revisions, jobs, executions) and runs the containers.
//
// A service is reachable at http://<service>.run.localhost:<port>/ (a traffic
// tag adds http://<tag>---<service>.run.localhost:<port>/) and at
// /_gcp/run/<project>/<region>/<service>/... on pogi's port. The container of
// a revision starts on its first request and runs until the service is
// deleted. Jobs run one container per task and finish when all tasks have.
// Without a container engine services and jobs are metadata only: requests get
// 503 and executions fail.
package run

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/serverless"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Cloud Run emulator.
type Service struct {
	runpb.UnimplementedServicesServer
	runpb.UnimplementedRevisionsServer
	runpb.UnimplementedJobsServer
	runpb.UnimplementedExecutionsServer

	hub  *core.Hub
	ops  *gapi.Ops
	rest *gapi.REST

	mu    sync.Mutex
	svcs  map[string]*runpb.Service
	revs  map[string]*runpb.Revision
	jobs  map[string]*runpb.Job
	execs map[string]*runpb.Execution
	seq   map[string]int // revisions made per service
	iam   map[string]*iampb.Policy

	insts  map[string]*inst            // running revision containers by revision name
	logs   map[string]*serverless.Logs // by revision or execution name
	cancel map[string]*running         // executions in progress
}

// New creates the Cloud Run service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, ops: gapi.Operations(h)}
	s.Reset()
	svc, jobs, revs, exes := &runpb.Services_ServiceDesc, &runpb.Jobs_ServiceDesc, &runpb.Revisions_ServiceDesc, &runpb.Executions_ServiceDesc
	s.rest = &gapi.REST{Versions: []string{"v2"}, Collections: []string{"services", "jobs"}, Desc: svc, Impl: s, Routes: map[string]gapi.Route{
		"POST locations/*/services":                      {Method: "CreateService", Field: "parent", Body: "service"},
		"GET locations/*/services":                       {Method: "ListServices", Field: "parent"},
		"GET locations/*/services/*":                     {Method: "GetService", Field: "name"},
		"PATCH locations/*/services/*":                   {Method: "UpdateService", Field: "service.name", Body: "service"},
		"DELETE locations/*/services/*":                  {Method: "DeleteService", Field: "name"},
		"GET locations/*/services/*:getIamPolicy":        {Method: "GetIamPolicy", Field: "resource"},
		"POST locations/*/services/*:setIamPolicy":       {Method: "SetIamPolicy", Field: "resource"},
		"POST locations/*/services/*:testIamPermissions": {Method: "TestIamPermissions", Field: "resource"},
		"GET locations/*/services/*/revisions":           {Method: "ListRevisions", Field: "parent", Desc: revs},
		"GET locations/*/services/*/revisions/*":         {Method: "GetRevision", Field: "name", Desc: revs},
		"DELETE locations/*/services/*/revisions/*":      {Method: "DeleteRevision", Field: "name", Desc: revs},
		"POST locations/*/jobs":                          {Method: "CreateJob", Field: "parent", Body: "job", Desc: jobs},
		"GET locations/*/jobs":                           {Method: "ListJobs", Field: "parent", Desc: jobs},
		"GET locations/*/jobs/*":                         {Method: "GetJob", Field: "name", Desc: jobs},
		"PATCH locations/*/jobs/*":                       {Method: "UpdateJob", Field: "job.name", Body: "job", Desc: jobs},
		"DELETE locations/*/jobs/*":                      {Method: "DeleteJob", Field: "name", Desc: jobs},
		"POST locations/*/jobs/*:run":                    {Method: "RunJob", Field: "name", Desc: jobs},
		"GET locations/*/jobs/*:getIamPolicy":            {Method: "GetIamPolicy", Field: "resource", Desc: jobs},
		"POST locations/*/jobs/*:setIamPolicy":           {Method: "SetIamPolicy", Field: "resource", Desc: jobs},
		"POST locations/*/jobs/*:testIamPermissions":     {Method: "TestIamPermissions", Field: "resource", Desc: jobs},
		"GET locations/*/jobs/*/executions":              {Method: "ListExecutions", Field: "parent", Desc: exes},
		"GET locations/*/jobs/*/executions/*":            {Method: "GetExecution", Field: "name", Desc: exes},
		"DELETE locations/*/jobs/*/executions/*":         {Method: "DeleteExecution", Field: "name", Desc: exes},
		"POST locations/*/jobs/*/executions/*:cancel":    {Method: "CancelExecution", Field: "name", Desc: exes},
	}}
	// ponytail: no Tasks API (GetTask, ListTasks) and no worker pools.
	h.OnDeliver("run", s.deliver)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "run", Cloud: core.GCP, Title: "Cloud Run", Match: s.match,
		Targets: []string{"google.cloud.run.v2.Services", "google.cloud.run.v2.Revisions", "google.cloud.run.v2.Jobs", "google.cloud.run.v2.Executions"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	runpb.RegisterServicesServer(g, s)
	runpb.RegisterRevisionsServer(g, s)
	runpb.RegisterJobsServer(g, s)
	runpb.RegisterExecutionsServer(g, s)
}

// match claims the Admin API and the data plane (hosts and paths).
func (s *Service) match(r *http.Request) bool {
	return s.rest.Match(r) || isData(r)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isData(r) {
		s.serveData(w, r)
		return
	}
	s.rest.ServeHTTP(w, r)
}

// Reset stops every container and forgets all state.
func (s *Service) Reset() {
	s.Close()
	s.mu.Lock()
	s.svcs, s.revs, s.jobs, s.execs = map[string]*runpb.Service{}, map[string]*runpb.Revision{}, map[string]*runpb.Job{}, map[string]*runpb.Execution{}
	s.seq, s.iam = map[string]int{}, map[string]*iampb.Policy{}
	s.insts, s.logs, s.cancel = map[string]*inst{}, map[string]*serverless.Logs{}, map[string]*running{}
	s.mu.Unlock()
}

// Close stops the containers and cancels running executions.
func (s *Service) Close() error {
	s.mu.Lock()
	insts, runs := s.insts, s.cancel
	s.insts, s.cancel = map[string]*inst{}, map[string]*running{}
	s.mu.Unlock()
	for _, x := range insts {
		x.stop()
	}
	for _, x := range runs {
		x.stop()
	}
	return nil
}

// ---- persistence ----

type saved struct {
	Services   []json.RawMessage          `json:"services"`
	Revisions  []json.RawMessage          `json:"revisions"`
	Jobs       []json.RawMessage          `json:"jobs"`
	Executions []json.RawMessage          `json:"executions"`
	Seq        map[string]int             `json:"seq"`
	IAM        map[string]json.RawMessage `json:"iam"`
}

func dump[T proto.Message](m map[string]T) []json.RawMessage {
	var out []json.RawMessage
	for _, k := range core.SortedKeys(m) {
		b, _ := protojson.Marshal(m[k])
		out = append(out, b)
	}
	return out
}

func load[T proto.Message](raw []json.RawMessage, mk func() T, name func(T) string) (map[string]T, error) {
	out := map[string]T{}
	for _, b := range raw {
		m := mk()
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, m); err != nil {
			return nil, err
		}
		out[name(m)] = m
	}
	return out, nil
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := saved{dump(s.svcs), dump(s.revs), dump(s.jobs), dump(s.execs), s.seq, map[string]json.RawMessage{}}
	for k, p := range s.iam {
		in.IAM[k], _ = protojson.Marshal(p)
	}
	return json.Marshal(in)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	svcs, err := load(in.Services, func() *runpb.Service { return &runpb.Service{} }, (*runpb.Service).GetName)
	if err != nil {
		return err
	}
	revs, err := load(in.Revisions, func() *runpb.Revision { return &runpb.Revision{} }, (*runpb.Revision).GetName)
	if err != nil {
		return err
	}
	jobs, err := load(in.Jobs, func() *runpb.Job { return &runpb.Job{} }, (*runpb.Job).GetName)
	if err != nil {
		return err
	}
	execs, err := load(in.Executions, func() *runpb.Execution { return &runpb.Execution{} }, (*runpb.Execution).GetName)
	if err != nil {
		return err
	}
	iam := map[string]*iampb.Policy{}
	for k, raw := range in.IAM {
		p := &iampb.Policy{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, p); err != nil {
			return err
		}
		iam[k] = p
	}
	for _, e := range execs { // the containers did not survive the restart
		if e.Reconciling {
			e.Reconciling, e.CompletionTime = false, timestamppb.Now()
			e.RunningCount, e.CancelledCount = 0, e.CancelledCount+e.RunningCount
			setCompleted(e)
		}
	}
	s.Reset()
	s.mu.Lock()
	s.svcs, s.revs, s.jobs, s.execs, s.iam = svcs, revs, jobs, execs, iam
	if in.Seq != nil {
		s.seq = in.Seq
	}
	s.mu.Unlock()
	return nil
}

// ---- names, pages, operations ----

var (
	parentRE = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)$`)
	idRE     = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,47}[a-z0-9])?$`)
)

// last is the part of a resource name after its last slash.
func last(name string) string { return name[strings.LastIndexByte(name, '/')+1:] }

func parent(name string) string { return name[:strings.LastIndexByte(name, '/')] }

// splitName returns the project and region of "projects/p/locations/l/...".
func splitName(name string) (project, region string) {
	p := strings.Split(name, "/")
	if len(p) >= 4 {
		return p[1], p[3]
	}
	return "", ""
}

func notFound(kind, name string) error {
	p, l := splitName(name)
	return gapi.NotFound("Resource '%s' of kind '%s' in region '%s' in project '%s' does not exist.", last(name), kind, l, p)
}

func checkParent(p string) error {
	if !parentRE.MatchString(p) {
		return gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<region>", p)
	}
	return nil
}

func checkID(kind, id string) error {
	if !idRE.MatchString(id) {
		return gapi.Invalid("Invalid %s ID %q: lowercase letters, digits and '-', starting with a letter, at most 49 characters", kind, id)
	}
	return nil
}

// page returns the slice bounds of one page of n items.
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

// done answers a mutation with a finished operation under parent.
func (s *Service) done(parent string, resp proto.Message) *longrunningpb.Operation {
	return s.ops.Done(parent, nil, resp)
}

func etag() string { return `"` + core.RandHex(8) + `"` }

func condition(typ string, ok bool, msg string) *runpb.Condition {
	st := runpb.Condition_CONDITION_SUCCEEDED
	if !ok {
		st = runpb.Condition_CONDITION_FAILED
	}
	return &runpb.Condition{Type: typ, State: st, Message: msg, LastTransitionTime: timestamppb.Now()}
}

// ---- IAM (stored as given; pogi does not enforce it) ----

func (s *Service) GetIamPolicy(_ context.Context, r *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.iam[r.Resource]; p != nil {
		return proto.Clone(p).(*iampb.Policy), nil
	}
	return &iampb.Policy{Version: 1, Etag: []byte("ACAB")}, nil
}

func (s *Service) SetIamPolicy(_ context.Context, r *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if r.Policy == nil {
		return nil, gapi.Invalid("The policy is required")
	}
	p := proto.Clone(r.Policy).(*iampb.Policy)
	p.Etag = []byte(core.RandHex(4))
	s.mu.Lock()
	s.iam[r.Resource] = p
	s.mu.Unlock()
	return proto.Clone(p).(*iampb.Policy), nil
}

func (s *Service) TestIamPermissions(_ context.Context, r *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return &iampb.TestIamPermissionsResponse{Permissions: r.Permissions}, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := []core.Table{{Name: "services", Columns: []string{"Name", "URI", "Latest revision", "Traffic"}},
		{Name: "revisions", Columns: []string{"Name", "Service", "Image", "Running"}},
		{Name: "jobs", Columns: []string{"Name", "Tasks", "Executions"}},
		{Name: "executions", Columns: []string{"Name", "Job", "Succeeded", "Failed", "Running"}}}
	for _, n := range core.SortedKeys(s.svcs) {
		v := s.svcs[n]
		var tr []string
		for _, x := range v.TrafficStatuses {
			tr = append(tr, strconv.Itoa(int(x.Percent))+"% "+x.Revision)
		}
		t[0].Rows = append(t[0].Rows, []string{n, v.Uri, last(v.LatestReadyRevision), strings.Join(tr, ", ")})
	}
	for _, n := range core.SortedKeys(s.revs) {
		v := s.revs[n]
		img := ""
		if len(v.Containers) > 0 {
			img = v.Containers[0].Image
		}
		t[1].Rows = append(t[1].Rows, []string{n, v.Service, img, strconv.FormatBool(s.insts[n] != nil)})
	}
	for _, n := range core.SortedKeys(s.jobs) {
		v := s.jobs[n]
		t[2].Rows = append(t[2].Rows, []string{n, strconv.Itoa(int(v.Template.GetTaskCount())), strconv.Itoa(int(v.ExecutionCount))})
	}
	for _, n := range core.SortedKeys(s.execs) {
		v := s.execs[n]
		t[3].Rows = append(t[3].Rows, []string{n, v.Job, strconv.Itoa(int(v.SucceededCount)), strconv.Itoa(int(v.FailedCount)), strconv.Itoa(int(v.RunningCount))})
	}
	return t
}

// Inspect shows a resource as the API returns it, with the output of its containers.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	var m proto.Message
	switch table {
	case "services":
		m = s.svcs[id]
	case "revisions":
		m = s.revs[id]
	case "jobs":
		m = s.jobs[id]
	case "executions":
		m = s.execs[id]
	}
	logs := s.logs[id]
	s.mu.Unlock()
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}
	b, _ := protojson.Marshal(m)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	if lines := logs.Lines(); lines != nil {
		out["logs"] = lines
	}
	return out
}
