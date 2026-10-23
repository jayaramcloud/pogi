// Package workflows emulates Google Cloud Workflows (v1) and the Workflow Executions API (v1) over gRPC
// and their REST mirrors. An execution really runs the workflow's YAML: main and subworkflows, assign,
// call (http.*, sys.*, subworkflows), switch, for, try/except/retry, return, raise and next, with
// ${...} expressions and the common standard library functions. A step type, connector or function
// outside that subset (parallel, googleapis.*, events.*) fails the execution with an error that names it.
package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	wpb "cloud.google.com/go/workflows/apiv1/workflowspb"
	epb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service serves both Workflows and Workflow Executions.
type Service struct {
	wpb.UnimplementedWorkflowsServer
	epb.UnimplementedExecutionsServer
	hub    *core.Hub
	ops    *gapi.Ops
	client *http.Client
	rest   *gapi.REST
	mu     sync.Mutex
	wfs    map[string]*workflow
	execs  map[string]*execution
}

type workflow struct {
	p    *wpb.Workflow
	revs []*wpb.Workflow // every revision, oldest first
}

type execution struct {
	p      *epb.Execution
	wf     string
	cancel context.CancelFunc
}

// New creates the service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, ops: gapi.Operations(h), client: gapi.Client(h), wfs: map[string]*workflow{}, execs: map[string]*execution{}}
	const w = "locations/*/workflows"
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"workflows"}, Desc: &wpb.Workflows_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"GET " + w:                             {Method: "ListWorkflows", Field: "parent"},
			"POST " + w:                            {Method: "CreateWorkflow", Field: "parent", Body: "workflow"},
			"GET " + w + "/*":                      {Method: "GetWorkflow", Field: "name"},
			"PATCH " + w + "/*":                    {Method: "UpdateWorkflow", Field: "workflow.name", Body: "workflow"},
			"DELETE " + w + "/*":                   {Method: "DeleteWorkflow", Field: "name"},
			"GET " + w + "/*:listRevisions":        {Method: "ListWorkflowRevisions", Field: "name"},
			"GET " + w + "/*/executions":           {Method: "ListExecutions", Field: "parent", Desc: &epb.Executions_ServiceDesc},
			"POST " + w + "/*/executions":          {Method: "CreateExecution", Field: "parent", Body: "execution", Desc: &epb.Executions_ServiceDesc},
			"GET " + w + "/*/executions/*":         {Method: "GetExecution", Field: "name", Desc: &epb.Executions_ServiceDesc},
			"POST " + w + "/*/executions/*:cancel": {Method: "CancelExecution", Field: "name", Desc: &epb.Executions_ServiceDesc},
		}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "workflows", Cloud: core.GCP, Title: "Workflows", Match: s.rest.Match,
		Targets: []string{"google.cloud.workflows.v1.Workflows", "google.cloud.workflows.executions.v1.Executions"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	wpb.RegisterWorkflowsServer(g, s)
	epb.RegisterExecutionsServer(g, s)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

func (s *Service) Close() error { s.Reset(); return nil }

// Reset stops the running executions and forgets everything.
func (s *Service) Reset() {
	s.mu.Lock()
	for _, x := range s.execs {
		x.cancel()
	}
	s.wfs, s.execs = map[string]*workflow{}, map[string]*execution{}
	s.mu.Unlock()
}

// ---- persistence ----

type saved struct {
	Workflows  []savedWorkflow   `json:"workflows"`
	Executions []json.RawMessage `json:"executions"`
}

type savedWorkflow struct {
	Latest   json.RawMessage   `json:"latest"`
	Revision []json.RawMessage `json:"revisions"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{Workflows: []savedWorkflow{}, Executions: []json.RawMessage{}}
	for _, n := range core.SortedKeys(s.wfs) {
		w := s.wfs[n]
		sw := savedWorkflow{Revision: []json.RawMessage{}}
		sw.Latest, _ = protojson.Marshal(w.p)
		for _, r := range w.revs {
			b, _ := protojson.Marshal(r)
			sw.Revision = append(sw.Revision, b)
		}
		out.Workflows = append(out.Workflows, sw)
	}
	for _, n := range core.SortedKeys(s.execs) {
		b, _ := protojson.Marshal(s.execs[n].p)
		out.Executions = append(out.Executions, b)
	}
	return json.Marshal(out)
}

// Load restores what Save wrote. An execution that was running is marked FAILED: its run is gone.
func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	wfs, execs := map[string]*workflow{}, map[string]*execution{}
	for _, sw := range in.Workflows {
		w := &workflow{p: &wpb.Workflow{}}
		if err := protojson.Unmarshal(sw.Latest, w.p); err != nil {
			return err
		}
		for _, raw := range sw.Revision {
			r := &wpb.Workflow{}
			if err := protojson.Unmarshal(raw, r); err != nil {
				return err
			}
			w.revs = append(w.revs, r)
		}
		wfs[w.p.Name] = w
	}
	for _, raw := range in.Executions {
		p := &epb.Execution{}
		if err := protojson.Unmarshal(raw, p); err != nil {
			return err
		}
		x := &execution{p: p, wf: p.Name[:strings.Index(p.Name, "/executions/")], cancel: func() {}}
		if p.State == epb.Execution_ACTIVE {
			s.fail(x, newErr("pogi restarted while the execution was running"))
		}
		execs[p.Name] = x
	}
	s.mu.Lock()
	s.wfs, s.execs = wfs, execs
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

var (
	locRE  = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
	wfRE   = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/workflows/[^/]+$`)
	exRE   = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/workflows/[^/]+/executions/[^/]+$`)
	idRE   = regexp.MustCompile(`^[A-Za-z]([-_A-Za-z0-9]{0,62}[A-Za-z0-9])?$`)
	nameRE = regexp.MustCompile(`^(projects/[^/]+/locations/[^/]+)/workflows/([^/]+)`)
)

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

func notFound(name string) error { return gapi.NotFound("Resource '%s' not found.", name) }

func (s *Service) meta(target, verb string) *wpb.OperationMetadata {
	now := timestamppb.Now()
	return &wpb.OperationMetadata{CreateTime: now, EndTime: now, Target: target, Verb: verb, ApiVersion: "v1"}
}

func revisionID(n int) string { return fmt.Sprintf("%06d-%s", n, core.RandHex(2)[:3]) }

// check validates a workflow's fields and source.
func check(p *wpb.Workflow) error {
	if len(p.Description) > 1000 {
		return gapi.Invalid("Workflow.description must be at most 1000 characters")
	}
	if p.GetSourceContents() == "" {
		return gapi.Invalid("Workflow.source_contents must be set")
	}
	if _, err := parseSource(p.GetSourceContents()); err != nil {
		return gapi.Invalid("Invalid workflow source: %v", err)
	}
	return nil
}

// ---- workflows ----

func (s *Service) CreateWorkflow(_ context.Context, r *wpb.CreateWorkflowRequest) (*longrunningpb.Operation, error) {
	if !locRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("parent must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>")
	}
	if !idRE.MatchString(r.WorkflowId) {
		return nil, gapi.Invalid("workflow_id %q is not valid: use 1 to 64 letters, digits, underscores or hyphens, starting with a letter and ending with a letter or digit", r.WorkflowId)
	}
	if r.Workflow == nil {
		return nil, gapi.Invalid("workflow must be set")
	}
	p := clone(r.Workflow)
	if err := check(p); err != nil {
		return nil, err
	}
	name := r.Parent + "/workflows/" + r.WorkflowId
	now := timestamppb.Now()
	p.Name, p.State, p.CreateTime, p.UpdateTime, p.RevisionCreateTime = name, wpb.Workflow_ACTIVE, now, now, now
	p.RevisionId = revisionID(1)
	s.mu.Lock()
	if s.wfs[name] != nil {
		s.mu.Unlock()
		return nil, gapi.Exists("Resource '%s' already exists.", name)
	}
	s.wfs[name] = &workflow{p: p, revs: []*wpb.Workflow{clone(p)}}
	s.mu.Unlock()
	return s.ops.Done(r.Parent, s.meta(name, "create"), clone(p)), nil
}

func (s *Service) GetWorkflow(_ context.Context, r *wpb.GetWorkflowRequest) (*wpb.Workflow, error) {
	if !wfRE.MatchString(r.Name) {
		return nil, gapi.Invalid("name must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.wfs[r.Name]
	if w == nil {
		return nil, notFound(r.Name)
	}
	if r.RevisionId == "" {
		return clone(w.p), nil
	}
	for _, rev := range w.revs {
		if rev.RevisionId == r.RevisionId {
			return clone(rev), nil
		}
	}
	return nil, notFound(r.Name + "@" + r.RevisionId)
}

func (s *Service) ListWorkflows(_ context.Context, r *wpb.ListWorkflowsRequest) (*wpb.ListWorkflowsResponse, error) {
	if !locRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("parent must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>")
	}
	if r.Filter != "" || r.OrderBy != "" {
		return nil, status.Errorf(codes.Unimplemented, "pogi does not support filter or order_by when listing workflows")
	}
	s.mu.Lock()
	var all []*wpb.Workflow
	for _, n := range core.SortedKeys(s.wfs) {
		if strings.HasPrefix(n, r.Parent+"/workflows/") {
			all = append(all, clone(s.wfs[n].p))
		}
	}
	s.mu.Unlock()
	page, next := gapi.Page(all, r.PageSize, r.PageToken)
	return &wpb.ListWorkflowsResponse{Workflows: page, NextPageToken: next}, nil
}

func (s *Service) ListWorkflowRevisions(_ context.Context, r *wpb.ListWorkflowRevisionsRequest) (*wpb.ListWorkflowRevisionsResponse, error) {
	s.mu.Lock()
	w := s.wfs[r.Name]
	var all []*wpb.Workflow
	if w != nil {
		for i := len(w.revs) - 1; i >= 0; i-- { // newest first
			all = append(all, clone(w.revs[i]))
		}
	}
	s.mu.Unlock()
	if w == nil {
		return nil, notFound(r.Name)
	}
	page, next := gapi.Page(all, r.PageSize, r.PageToken)
	return &wpb.ListWorkflowRevisionsResponse{Workflows: page, NextPageToken: next}, nil
}

func (s *Service) UpdateWorkflow(_ context.Context, r *wpb.UpdateWorkflowRequest) (*longrunningpb.Operation, error) {
	if r.Workflow == nil || !wfRE.MatchString(r.Workflow.Name) {
		return nil, gapi.Invalid("workflow.name must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>")
	}
	name := r.Workflow.Name
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.wfs[name]
	if w == nil {
		return nil, notFound(name)
	}
	next := clone(w.p)
	if err := gapi.Update(next, r.Workflow, r.GetUpdateMask().GetPaths(), "description", "labels", "service_account", "source_contents",
		"call_log_level", "user_env_vars", "crypto_key_name", "execution_history_level"); err != nil {
		return nil, err
	}
	if err := check(next); err != nil {
		return nil, err
	}
	now := timestamppb.Now()
	next.UpdateTime = now
	if next.GetSourceContents() != w.p.GetSourceContents() || next.ServiceAccount != w.p.ServiceAccount {
		next.RevisionId, next.RevisionCreateTime = revisionID(len(w.revs)+1), now
		w.revs = append(w.revs, clone(next))
	} else {
		w.revs[len(w.revs)-1] = clone(next) // the latest revision shows the current labels and description
	}
	w.p = next
	return s.ops.Done(strings.TrimSuffix(name[:strings.Index(name, "/workflows/")], "/"), s.meta(name, "update"), clone(next)), nil
}

func (s *Service) DeleteWorkflow(_ context.Context, r *wpb.DeleteWorkflowRequest) (*longrunningpb.Operation, error) {
	if !wfRE.MatchString(r.Name) {
		return nil, gapi.Invalid("name must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>")
	}
	s.mu.Lock()
	if s.wfs[r.Name] == nil {
		s.mu.Unlock()
		return nil, notFound(r.Name)
	}
	delete(s.wfs, r.Name)
	for n, x := range s.execs {
		if x.wf == r.Name {
			x.cancel()
			delete(s.execs, n)
		}
	}
	s.mu.Unlock()
	return s.ops.Done(nameRE.FindStringSubmatch(r.Name)[1], s.meta(r.Name, "delete"), &emptypb.Empty{}), nil
}

// ---- executions ----

func (s *Service) CreateExecution(_ context.Context, r *epb.CreateExecutionRequest) (*epb.Execution, error) {
	if !wfRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("parent must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>")
	}
	in := r.Execution
	if in == nil {
		in = &epb.Execution{}
	}
	var arg any
	if in.Argument != "" {
		if len(in.Argument) > 32<<10 {
			return nil, gapi.Invalid("argument is larger than the limit of 32 KB")
		}
		var err error
		if arg, err = decodeJSON([]byte(in.Argument)); err != nil {
			return nil, gapi.Invalid("argument is not valid JSON: %v", err)
		}
	}
	s.mu.Lock()
	w := s.wfs[r.Parent]
	if w == nil {
		s.mu.Unlock()
		return nil, notFound(r.Parent)
	}
	rev := w.p
	prog, err := parseSource(rev.GetSourceContents())
	if err != nil { // it was checked when the workflow was saved
		s.mu.Unlock()
		return nil, gapi.Invalid("Invalid workflow source: %v", err)
	}
	id := core.NewID()
	id = id[:8] + "-" + id[8:12] + "-" + id[12:16] + "-" + id[16:20] + "-" + id[20:]
	ctx, cancel := context.WithCancel(context.Background())
	x := &execution{wf: r.Parent, cancel: cancel, p: &epb.Execution{
		Name: r.Parent + "/executions/" + id, StartTime: timestamppb.Now(), State: epb.Execution_ACTIVE, Argument: in.Argument,
		WorkflowRevisionId: rev.RevisionId, CallLogLevel: in.CallLogLevel, Labels: in.Labels,
	}}
	if x.p.Labels == nil {
		x.p.Labels = rev.Labels
	}
	s.execs[x.p.Name] = x
	env := &runEnv{ctx: ctx, svc: s, prog: prog, user: rev.UserEnvVars, meta: map[string]string{
		"GOOGLE_CLOUD_PROJECT_ID":            strings.Split(r.Parent, "/")[1],
		"GOOGLE_CLOUD_LOCATION":              strings.Split(r.Parent, "/")[3],
		"GOOGLE_CLOUD_WORKFLOW_ID":           strings.Split(r.Parent, "/")[5],
		"GOOGLE_CLOUD_WORKFLOW_REVISION_ID":  rev.RevisionId,
		"GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID": id,
	}}
	out := clone(x.p)
	s.mu.Unlock()
	core.Go("workflows.run", func() {
		defer cancel()
		res, err := env.runMain(arg)
		s.finish(x, res, err)
	})
	return out, nil
}

// finish records how a run ended, unless the execution was cancelled meanwhile.
func (s *Service) finish(x *execution, result any, err error) {
	if err == nil {
		b, merr := json.Marshal(result)
		if merr == nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			if x.p.State == epb.Execution_ACTIVE {
				s.end(x, epb.Execution_SUCCEEDED)
				x.p.Result = string(b)
			}
			return
		}
		err = newErr("the result cannot be encoded as JSON: "+merr.Error(), "ValueError")
	}
	if we, ok := err.(*wfErr); ok {
		s.fail(x, we)
	}
}

func (s *Service) fail(x *execution, we *wfErr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if x.p.State != epb.Execution_ACTIVE {
		return
	}
	s.end(x, epb.Execution_FAILED)
	payload, _ := json.Marshal(we.val)
	e := &epb.Execution_Error{Payload: string(payload), Context: we.Error(), StackTrace: &epb.Execution_StackTrace{}}
	for _, f := range we.stack {
		e.StackTrace.Elements = append(e.StackTrace.Elements, &epb.Execution_StackTraceElement{Step: f.step, Routine: f.routine})
	}
	if len(we.stack) > 0 {
		e.Context += fmt.Sprintf("\nin step %s, routine %s", we.stack[0].step, we.stack[0].routine)
	}
	x.p.Error = e
}

// end moves an ACTIVE execution to a final state (the caller holds the lock).
func (s *Service) end(x *execution, st epb.Execution_State) {
	if x.p.State != epb.Execution_ACTIVE {
		return
	}
	now := time.Now()
	x.p.State, x.p.EndTime = st, timestamppb.New(now)
	x.p.Duration = durationpb.New(now.Sub(x.p.StartTime.AsTime()))
}

// view is the execution as a client asks for it: BASIC has the name, times, state and revision only.
func view(x *execution, v epb.ExecutionView, dflt epb.ExecutionView) *epb.Execution {
	if v == epb.ExecutionView_EXECUTION_VIEW_UNSPECIFIED {
		v = dflt
	}
	p := clone(x.p)
	if v == epb.ExecutionView_BASIC {
		p = &epb.Execution{Name: p.Name, StartTime: p.StartTime, EndTime: p.EndTime, Duration: p.Duration, State: p.State, WorkflowRevisionId: p.WorkflowRevisionId}
	}
	return p
}

func (s *Service) GetExecution(_ context.Context, r *epb.GetExecutionRequest) (*epb.Execution, error) {
	if !exRE.MatchString(r.Name) {
		return nil, gapi.Invalid("name must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>/executions/<EXECUTION_ID>")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x := s.execs[r.Name]
	if x == nil {
		return nil, notFound(r.Name)
	}
	return view(x, r.View, epb.ExecutionView_FULL), nil
}

func (s *Service) ListExecutions(_ context.Context, r *epb.ListExecutionsRequest) (*epb.ListExecutionsResponse, error) {
	if !wfRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("parent must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>")
	}
	if r.Filter != "" || r.OrderBy != "" {
		return nil, status.Errorf(codes.Unimplemented, "pogi does not support filter or order_by when listing executions")
	}
	s.mu.Lock()
	if s.wfs[r.Parent] == nil {
		s.mu.Unlock()
		return nil, notFound(r.Parent)
	}
	var xs []*execution
	for _, x := range s.execs {
		if x.wf == r.Parent {
			xs = append(xs, x)
		}
	}
	sort.Slice(xs, func(i, j int) bool { // newest first
		a, b := xs[i].p.StartTime.AsTime(), xs[j].p.StartTime.AsTime()
		return a.After(b) || a.Equal(b) && xs[i].p.Name < xs[j].p.Name
	})
	var all []*epb.Execution
	for _, x := range xs {
		all = append(all, view(x, r.View, epb.ExecutionView_BASIC))
	}
	s.mu.Unlock()
	page, next := gapi.Page(all, r.PageSize, r.PageToken)
	return &epb.ListExecutionsResponse{Executions: page, NextPageToken: next}, nil
}

func (s *Service) CancelExecution(_ context.Context, r *epb.CancelExecutionRequest) (*epb.Execution, error) {
	if !exRE.MatchString(r.Name) {
		return nil, gapi.Invalid("name must be formatted: projects/<PROJECT_ID>/locations/<LOCATION_ID>/workflows/<WORKFLOW_ID>/executions/<EXECUTION_ID>")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x := s.execs[r.Name]
	if x == nil {
		return nil, notFound(r.Name)
	}
	if x.p.State != epb.Execution_ACTIVE {
		return nil, status.Errorf(codes.FailedPrecondition, "Execution %s is already %s and cannot be cancelled.", r.Name, x.p.State)
	}
	s.end(x, epb.Execution_CANCELLED)
	x.cancel()
	return clone(x.p), nil
}
