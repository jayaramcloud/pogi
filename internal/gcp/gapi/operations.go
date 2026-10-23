package gapi

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi/internal/core"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Ops is the shared google.longrunning.Operations service: every GCP
// service that returns an Operation stores it here, and clients poll it by
// name over gRPC or REST (GET /v1|v2/projects/p/locations/l/operations/o).
//
// ponytail: operations are in memory only and never expire; persist them
// if a restart mid-operation ever matters.
type Ops struct {
	longrunningpb.UnimplementedOperationsServer
	rest *REST
	top  *Mux // operations/<id> without a project: Resource Manager and Service Usage
	mu   sync.Mutex
	ops  map[string]*op
	js   map[string]map[string]any               // operations whose result is a JSON resource (DoneJSON), by name
	back []func() longrunningpb.OperationsClient // vendor emulators that keep operations of their own
}

type op struct {
	p    *longrunningpb.Operation
	done chan struct{}
}

// NewOps creates the operations service.
func NewOps(*core.Hub) *Ops {
	o := &Ops{ops: map[string]*op{}, js: map[string]map[string]any{}}
	o.rest = &REST{Versions: []string{"v1", "v1beta1", "v1alpha1", "v2", "v2beta", "v3"}, Collections: []string{"operations"},
		Desc: &longrunningpb.Operations_ServiceDesc, Impl: o, Routes: map[string]Route{
			"GET operations":                       {Method: "ListOperations", Field: "name"},
			"GET operations/*":                     {Method: "GetOperation", Field: "name"},
			"DELETE operations/*":                  {Method: "DeleteOperation", Field: "name"},
			"POST operations/*:cancel":             {Method: "CancelOperation", Field: "name"},
			"POST operations/*:wait":               {Method: "WaitOperation", Field: "name"},
			"GET locations/*/operations":           {Method: "ListOperations", Field: "name"},
			"GET locations/*/operations/*":         {Method: "GetOperation", Field: "name"},
			"DELETE locations/*/operations/*":      {Method: "DeleteOperation", Field: "name"},
			"POST locations/*/operations/*:cancel": {Method: "CancelOperation", Field: "name"},
			"POST locations/*/operations/*:wait":   {Method: "WaitOperation", Field: "name"},
			"GET regions/*/operations":             {Method: "ListOperations", Field: "name"}, // Dataproc
			"GET regions/*/operations/*":           {Method: "GetOperation", Field: "name"},
			"DELETE regions/*/operations/*":        {Method: "DeleteOperation", Field: "name"},
			"POST regions/*/operations/*:cancel":   {Method: "CancelOperation", Field: "name"},
			"POST regions/*/operations/*:wait":     {Method: "WaitOperation", Field: "name"},
		}}
	var top []Rule
	for _, v := range o.rest.Versions {
		top = append(top,
			Rule{HTTP: "GET /" + v + "/{name=operations}", Method: "ListOperations"},
			Rule{HTTP: "GET /" + v + "/{name=operations/*}", Method: "GetOperation"},
			Rule{HTTP: "DELETE /" + v + "/{name=operations/*}", Method: "DeleteOperation"},
			Rule{HTTP: "POST /" + v + "/{name=operations/*}:cancel", Method: "CancelOperation"},
			Rule{HTTP: "POST /" + v + "/{name=operations/*}:wait", Method: "WaitOperation"})
	}
	o.top = NewMux(o, top...)
	return o
}

// Delegate adds a backend for the operations that are not stored here: the
// emulator behind a service (Spanner's) keeps its own. client returns nil
// while the backend is not running, and then it knows no operation.
func (o *Ops) Delegate(client func() longrunningpb.OperationsClient) {
	o.mu.Lock()
	o.back = append(o.back, client)
	o.mu.Unlock()
}

// backends are the running delegates.
func (o *Ops) backends() (out []longrunningpb.OperationsClient) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, f := range o.back {
		if c := f(); c != nil {
			out = append(out, c)
		}
	}
	return out
}

// Operations returns the hub's operations service, or a detached one when
// "operations" is disabled (clients then can't poll, but done ops still work).
func Operations(h *core.Hub) *Ops {
	if o, ok := h.Service("operations").(*Ops); ok {
		return o
	}
	return NewOps(h)
}

func (o *Ops) Info() core.Info {
	return core.Info{Name: "operations", Cloud: core.GCP, Title: "Operations", Match: func(r *http.Request) bool { return o.rest.Match(r) || o.top.Match(r) || o.jsonOp(r) != nil }, // the last: a DoneJSON operation under any parent, apps/a/operations/o of App Engine
		Targets: []string{"google.longrunning.Operations"}}
}

func (o *Ops) RegisterGRPC(g *grpc.Server) { longrunningpb.RegisterOperationsServer(g, o) }

func (o *Ops) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if js := o.jsonOp(r); js != nil {
		c := core.C(r)
		c.Op = "GetOperation"
		if err := c.Intercept(); err != nil {
			e := core.AsError(err)
			JSONErr(w, c, e.Status, e.Code, e.Msg)
			return
		}
		JSON(w, js)
		return
	}
	if o.top.Match(r) {
		o.top.ServeHTTP(w, r)
		return
	}
	o.rest.ServeHTTP(w, r)
}

func (o *Ops) Reset() {
	o.mu.Lock()
	o.ops = map[string]*op{}
	o.js = map[string]map[string]any{}
	o.mu.Unlock()
}

// Done records a finished operation under parent (projects/p/locations/l, or
// "" for a top-level operations/<id>) with resp as its result; meta may be nil.
func (o *Ops) Done(parent string, meta, resp proto.Message) *longrunningpb.Operation {
	p := o.add(parent, meta)
	o.finish(p.Name, resp, nil)
	return o.get(p.Name)
}

// DoneJSON records a finished operation under parent whose metadata and response are JSON objects
// (a generic REST engine's resources, which no proto message describes). A GET or :wait of it answers
// {"name", "metadata", "done": true, "response"}; list, cancel and delete treat it like any other.
func (o *Ops) DoneJSON(parent string, meta, resp map[string]any) map[string]any {
	p := o.add(parent, nil)
	o.finish(p.Name, nil, nil)
	js := map[string]any{"name": p.Name, "done": true, "response": resp}
	if meta != nil {
		js["metadata"] = meta
	}
	o.mu.Lock()
	o.js[p.Name] = js
	o.mu.Unlock()
	return js
}

// jsonOp returns the DoneJSON operation a GET or :wait request names, or nil.
func (o *Ops) jsonOp(r *http.Request) map[string]any {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if wait := strings.TrimSuffix(p, ":wait"); r.Method == http.MethodPost && wait != p {
		p = wait
	} else if r.Method != http.MethodGet {
		return nil
	}
	_, name, _ := strings.Cut(p, "/") // the path without its version
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.js[name]
}

// Start records a running operation and finishes it with run's result in
// the background; Get and Wait see it as done once run returns.
func (o *Ops) Start(parent string, meta proto.Message, run func() (proto.Message, error)) *longrunningpb.Operation {
	p := o.add(parent, meta)
	core.Go("gapi.Start", func() {
		resp, err := run()
		o.finish(p.Name, resp, err)
	})
	return p
}

func (o *Ops) add(parent string, meta proto.Message) *longrunningpb.Operation {
	p := &longrunningpb.Operation{Name: strings.TrimPrefix(parent+"/operations/operation-"+core.NewID(), "/")}
	if meta != nil {
		p.Metadata, _ = anypb.New(meta)
	}
	o.mu.Lock()
	o.ops[p.Name] = &op{p: p, done: make(chan struct{})}
	o.mu.Unlock()
	return proto.Clone(p).(*longrunningpb.Operation)
}

func (o *Ops) finish(name string, resp proto.Message, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	x := o.ops[name]
	if x == nil || x.p.Done {
		return
	}
	x.p.Done = true
	if err != nil {
		x.p.Result = &longrunningpb.Operation_Error{Error: status.Convert(err).Proto()}
	} else if resp != nil {
		a, _ := anypb.New(resp)
		x.p.Result = &longrunningpb.Operation_Response{Response: a}
	} else {
		a, _ := anypb.New(&emptypb.Empty{})
		x.p.Result = &longrunningpb.Operation_Response{Response: a}
	}
	close(x.done)
}

func (o *Ops) get(name string) *longrunningpb.Operation {
	o.mu.Lock()
	defer o.mu.Unlock()
	if x := o.ops[name]; x != nil {
		return proto.Clone(x.p).(*longrunningpb.Operation)
	}
	return nil
}

func (o *Ops) GetOperation(ctx context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	if p := o.get(r.Name); p != nil {
		return p, nil
	}
	for _, c := range o.backends() {
		if p, err := c.GetOperation(ctx, r); err == nil {
			return p, nil
		}
	}
	return nil, NotFound("Operation %s not found.", r.Name)
}

func (o *Ops) ListOperations(ctx context.Context, r *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	out := &longrunningpb.ListOperationsResponse{}
	for _, c := range o.backends() {
		if l, err := c.ListOperations(ctx, r); err == nil {
			out.Operations = append(out.Operations, l.Operations...)
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	parent := strings.TrimSuffix(r.Name, "/operations")
	if parent == "operations" {
		parent = ""
	}
	prefix := strings.TrimPrefix(parent+"/operations/", "/")
	for _, n := range core.SortedKeys(o.ops) {
		if strings.HasPrefix(n, prefix) {
			out.Operations = append(out.Operations, proto.Clone(o.ops[n].p).(*longrunningpb.Operation))
		}
	}
	return out, nil
}

func (o *Ops) DeleteOperation(ctx context.Context, r *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	o.mu.Lock()
	_, ok := o.ops[r.Name]
	delete(o.ops, r.Name)
	delete(o.js, r.Name)
	o.mu.Unlock()
	if ok {
		return &emptypb.Empty{}, nil
	}
	for _, c := range o.backends() {
		if _, err := c.DeleteOperation(ctx, r); err == nil {
			return &emptypb.Empty{}, nil
		}
	}
	return nil, NotFound("Operation %s not found.", r.Name)
}

// CancelOperation marks a running operation CANCELLED; the work itself runs on.
func (o *Ops) CancelOperation(ctx context.Context, r *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	if o.get(r.Name) == nil {
		for _, c := range o.backends() {
			if _, err := c.CancelOperation(ctx, r); err == nil {
				return &emptypb.Empty{}, nil
			}
		}
		return nil, NotFound("Operation %s not found.", r.Name)
	}
	o.mu.Lock()
	x := o.ops[r.Name]
	if !x.p.Done {
		x.p.Done = true
		x.p.Result = &longrunningpb.Operation_Error{Error: &spb.Status{Code: 1, Message: "Operation was cancelled."}}
		close(x.done)
	}
	o.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (o *Ops) WaitOperation(ctx context.Context, r *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	o.mu.Lock()
	x := o.ops[r.Name]
	o.mu.Unlock()
	if x == nil {
		for _, c := range o.backends() {
			if p, err := c.WaitOperation(ctx, r); err == nil {
				return p, nil
			}
		}
		return nil, NotFound("Operation %s not found.", r.Name)
	}
	wait := time.Minute
	if r.Timeout != nil {
		wait = r.Timeout.AsDuration()
	}
	select {
	case <-x.done:
	case <-time.After(wait):
	case <-ctx.Done():
	}
	return o.get(r.Name), nil
}
