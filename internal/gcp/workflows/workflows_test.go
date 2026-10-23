package workflows_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	workflows "cloud.google.com/go/workflows/apiv1"
	wpb "cloud.google.com/go/workflows/apiv1/workflowspb"
	executions "cloud.google.com/go/workflows/executions/apiv1"
	epb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	wf "github.com/nrndev/pogi/internal/gcp/workflows"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

const parent = "projects/proj/locations/us-central1"

type env struct {
	w   *workflows.Client
	e   *executions.Client
	url string
}

func setup(t *testing.T) *env {
	t.Helper()
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return gapi.NewOps(h) },
		func(h *core.Hub) core.Service { return wf.New(h) })
	opts := []option.ClientOption{option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
	w, err := workflows.NewClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	e, err := executions.NewClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close(); e.Close() })
	return &env{w, e, url}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func (v *env) create(t *testing.T, id, src string) *wpb.Workflow {
	t.Helper()
	op, err := v.w.CreateWorkflow(ctx, &wpb.CreateWorkflowRequest{Parent: parent, WorkflowId: id,
		Workflow: &wpb.Workflow{SourceCode: &wpb.Workflow_SourceContents{SourceContents: src}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := op.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// start creates an execution and waits for it to leave ACTIVE.
func (v *env) run(t *testing.T, id, src, arg string) *epb.Execution {
	t.Helper()
	v.create(t, id, src)
	x, err := v.e.CreateExecution(ctx, &epb.CreateExecutionRequest{Parent: parent + "/workflows/" + id, Execution: &epb.Execution{Argument: arg}})
	if err != nil {
		t.Fatal(err)
	}
	return v.wait(t, x.Name)
}

func (v *env) wait(t *testing.T, name string) *epb.Execution {
	t.Helper()
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		x, err := v.e.GetExecution(ctx, &epb.GetExecutionRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if x.State != epb.Execution_ACTIVE {
			return x
		}
	}
	t.Fatal("execution still ACTIVE")
	return nil
}

func ok(t *testing.T, x *epb.Execution, result string) {
	t.Helper()
	if x.State != epb.Execution_SUCCEEDED || x.Result != result {
		t.Fatalf("state %v result %s error %v, want SUCCEEDED %s", x.State, x.Result, x.Error, result)
	}
}

func failed(t *testing.T, x *epb.Execution, in ...string) {
	t.Helper()
	if x.State != epb.Execution_FAILED || x.Error == nil {
		t.Fatalf("state %v result %s, want FAILED", x.State, x.Result)
	}
	for _, s := range in {
		if !strings.Contains(x.Error.Payload+x.Error.Context, s) {
			t.Fatalf("error %v does not mention %q", x.Error, s)
		}
	}
}

func TestLanguage(t *testing.T) {
	v := setup(t)
	ok(t, v.run(t, "lang", `
main:
  params: [args]
  steps:
    - init:
        assign:
          - total: 0
          - items: ["a", "b", "c"]
          - m: {name: "${args.name}", n: 1}
          - m.extra: [1, 2]
          - m.extra[1]: 20
          - dd: '${json.decode("{\"k\": [1,2]}")}'
    - sum:
        for:
          value: v
          index: i
          in: ${items}
          steps:
            - add:
                assign:
                  - total: ${total + i}
    - range:
        for:
          value: n
          range: [1, 4]
          steps:
            - skip:
                switch:
                  - condition: ${n == 2}
                    next: continue
                  - condition: ${n == 4}
                    next: break
            - add2:
                assign:
                  - total: ${total + n * 10}
    - pick:
        switch:
          - condition: ${total > 100}
            return: big
          - condition: ${"x" in m}
            next: end
          - condition: true
            steps:
              - sub:
                  call: helper
                  args: {a: 2}
                  result: h
    - fin:
        return:
          total: ${total}
          h: ${h}
          upper: ${text.to_upper(m.name)}
          len: ${len(items) + len(keys(m))}
          missing: ${default(map.get(m, "zz"), "dflt")}
          deep: ${m.extra[1]}
          enc: '${json.encode({"a": [1, 2.5, null]})}'
          dec: ${dd.k[1]}
          div: ${7 / 2}
          idiv: ${7 // 2}
          cat: ${"a" + "b"}
          sub: ${text.substring("hello", 1, 3)}
          proj: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          fmt: ${time.format(0)}
          notin: ${not ("q" in m) and (1 < 2 or false)}
helper:
  params: [a, {b: 5}]
  steps:
    - r:
        return: ${a * b}
`, `{"name": "pogi"}`), `{"cat":"ab","dec":2,"deep":20,"div":3.5,"enc":"{\"a\":[1,2.5,null]}","fmt":"1970-01-01T00:00:00Z","h":10,"idiv":3,"len":6,"missing":"dflt","notin":true,"proj":"proj","sub":"el","total":43,"upper":"POGI"}`)
}

func TestJumpAndReturnForms(t *testing.T) {
	v := setup(t)
	// A bare list of steps is main; next jumps back and forth.
	ok(t, v.run(t, "jump", `
- start:
    assign: [{n: 0}]
- again:
    assign: [{n: "${n + 1}"}]
- check:
    switch:
      - condition: ${n < 5}
        next: again
- done:
    return: ${n}
`, ""), "5")
	ok(t, v.run(t, "early", `
- a:
    return: 1
    next: end
- b:
    return: 2
`, ""), "1")
}

func TestHTTPAndRetry(t *testing.T) {
	var tries atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/flaky":
			if tries.Add(1) == 1 {
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok": true, "tries": 2}`)
		case "/missing":
			http.Error(w, "nope", 404)
		case "/slow":
			time.Sleep(2 * time.Second)
		default:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"method": r.Method, "body": string(b), "q": r.URL.Query().Get("k"),
				"h": r.Header.Get("X-Test"), "ct": r.Header.Get("Content-Type")})
		}
	}))
	t.Cleanup(srv.Close)
	v := setup(t)
	ok(t, v.run(t, "http", `
main:
  params: [args]
  steps:
    - get:
        call: http.get
        args:
          url: ${args.base + "/echo"}
          query: {k: "v 1"}
          headers: {X-Test: "yes"}
        result: g
    - post:
        call: http.post
        args:
          url: ${args.base + "/echo"}
          body: {a: 1}
        result: p
    - put:
        call: http.put
        args: {url: "${args.base + \"/echo\"}", body: "raw"}
        result: u
    - patch:
        call: http.patch
        args: {url: "${args.base + \"/echo\"}"}
        result: pa
    - del:
        call: http.delete
        args: {url: "${args.base + \"/echo\"}"}
        result: d
    - req:
        call: http.request
        args: {method: HEAD, url: "${args.base + \"/echo\"}"}
        result: r
    - retried:
        try:
          call: http.get
          args: {url: "${args.base + \"/flaky\"}"}
          result: f
        retry:
          predicate: ${http.default_retry_predicate}
          max_retries: 3
          backoff: {initial_delay: 0.01, max_delay: 0.1, multiplier: 2}
    - notfound:
        try:
          call: http.get
          args: {url: "${args.base + \"/missing\"}"}
        except:
          as: e
          steps:
            - keep:
                assign: [{code: "${e.code}"}, {tag: "${e.tags[0]}"}]
    - timeout:
        try:
          call: http.get
          args: {url: "${args.base + \"/slow\"}", timeout: 0.2}
        except:
          as: e
          steps:
            - keep2:
                assign: [{tmo: "${e.tags[0]}"}]
    - out:
        return:
          get: ${g.body.method + g.body.q + g.body.h}
          post: ${p.body.body + p.body.ct}
          put: ${u.body.body}
          patch: ${pa.body.method}
          del: ${d.body.method}
          req: ${r.code}
          f: ${f.body.tries}
          tries: ${f.code}
          code: ${code}
          tag: ${tag}
          tmo: ${tmo}
`, `{"base": "`+srv.URL+`"}`), `{"code":404,"del":"DELETE","f":2,"get":"GETv 1yes","patch":"PATCH","post":"{\"a\":1}application/json","put":"raw","req":200,"tag":"HttpError","tmo":"TimeoutError","tries":200}`)
	if tries.Load() != 2 {
		t.Fatalf("flaky endpoint called %d times, want 2", tries.Load())
	}
}

// A workflow reaches pogi's own services through a *.googleapis.com URL.
func TestCallsPogi(t *testing.T) {
	v := setup(t)
	v.create(t, "target", "- s:\n    return: 1\n")
	ok(t, v.run(t, "caller", `
- get:
    call: http.get
    args:
      url: https://workflows.googleapis.com/v1/projects/proj/locations/us-central1/workflows/target
    result: r
- out:
    return: ${r.body.state + " " + r.body.name}
`, ""), `"ACTIVE `+parent+`/workflows/target"`)
}

func TestRetryPolicies(t *testing.T) {
	v := setup(t)
	// A subworkflow predicate, and a retry that runs out and reaches except.
	ok(t, v.run(t, "retry", `
main:
  steps:
    - n:
        assign: [{n: 0}]
    - go:
        try:
          steps:
            - bump:
                assign: [{n: "${n + 1}"}]
            - maybe:
                switch:
                  - condition: ${n < 3}
                    raise: {message: "again", code: 7}
        retry:
          predicate: ${again}
          max_retries: 5
          backoff: {initial_delay: 0.001, max_delay: 0.01, multiplier: 2}
    - gaveup:
        try:
          raise: {code: 9}
        retry:
          predicate: ${again}
          max_retries: 2
          backoff: {initial_delay: 0.001, max_delay: 0.01, multiplier: 2}
        except:
          as: e
          steps:
            - r:
                return: ${[n, e.code]}
again:
  params: [e]
  steps:
    - r:
        return: ${e.code == 7 or e.code == 9}
`, ""), `[3,9]`)
}

func TestExecutionFailures(t *testing.T) {
	v := setup(t)
	for name, c := range map[string]struct{ src, want string }{
		"parallel":  {"- p:\n    parallel:\n      branches: []\n", "parallel"},
		"connector": {"- c:\n    call: googleapis.storage.v1.buckets.get\n    args: {bucket: b}\n", "googleapis.storage.v1.buckets.get"},
		"events":    {"- c:\n    call: events.create_callback_endpoint\n", "events.create_callback_endpoint"},
		"func":      {"- c:\n    assign: [{x: \"${base64.encode(1)}\"}]\n", "base64.encode"},
		"raise":     {"- r:\n    raise: \"boom\"\n", "boom"},
		"raisemap":  {"- r:\n    raise: {code: 3, message: \"bad\"}\n", "bad"},
		"keyerror":  {"- r:\n    assign: [{m: {}}, {x: \"${m.nope}\"}]\n", "nope"},
		"nosub":     {"- r:\n    call: ghost\n", "ghost"},
		// unsupported features are not caught by try/except
		"nocatch": {"- t:\n    try:\n      parallel:\n        branches: []\n    except:\n      as: e\n      steps:\n        - r:\n            return: caught\n", "parallel"},
	} {
		t.Run(name, func(t *testing.T) {
			x := v.run(t, "f"+name, c.src, "")
			failed(t, x, c.want)
			if x.Error.StackTrace == nil || len(x.Error.StackTrace.Elements) == 0 || x.Error.StackTrace.Elements[0].Routine != "main" {
				t.Fatalf("no step in the error: %v", x.Error)
			}
		})
	}
	// The error names the step.
	x := v.run(t, "fstep", "- first:\n    assign: [{a: 1}]\n- second:\n    raise: oops\n", "")
	if x.Error.StackTrace.Elements[0].Step != "second" || !strings.Contains(x.Error.Context, "second") {
		t.Fatalf("error %v", x.Error)
	}
}

func TestStepCap(t *testing.T) {
	v := setup(t)
	x := v.run(t, "cap", "- loop:\n    assign: [{a: 1}]\n    next: loop\n", "")
	failed(t, x, "limit of 100000 steps")
	x = v.run(t, "capfor", "- l:\n    for:\n      value: v\n      range: [1, 1000000]\n      steps:\n        - s:\n            assign: [{a: 1}]\n", "")
	failed(t, x, "iterations")
}

func TestWorkflowAPI(t *testing.T) {
	v := setup(t)
	w := v.create(t, "w1", "- s:\n    return: 1\n")
	if w.Name != parent+"/workflows/w1" || w.State != wpb.Workflow_ACTIVE || !strings.HasPrefix(w.RevisionId, "000001-") {
		t.Fatalf("created %v", w)
	}
	// create errors
	req := func(id, src string) error {
		_, err := v.w.CreateWorkflow(ctx, &wpb.CreateWorkflowRequest{Parent: parent, WorkflowId: id,
			Workflow: &wpb.Workflow{SourceCode: &wpb.Workflow_SourceContents{SourceContents: src}}})
		return err
	}
	wantCode(t, req("w1", "- s:\n    return: 1\n"), codes.AlreadyExists)
	for _, id := range []string{"", "1abc", "has space", "-x", "x-", strings.Repeat("a", 65)} {
		wantCode(t, req(id, "- s:\n    return: 1\n"), codes.InvalidArgument)
	}
	for _, src := range []string{"", "main: [", "- a: 1\n", "foo:\n  steps:\n    - a:\n        return: 1\n", "- a:\n    return: 1\n  b:\n    return: 2\n", "42"} {
		wantCode(t, req("bad", src), codes.InvalidArgument)
	}
	_, err := v.w.CreateWorkflow(ctx, &wpb.CreateWorkflowRequest{Parent: "nope", WorkflowId: "x"})
	wantCode(t, err, codes.InvalidArgument)
	if _, err := v.w.GetWorkflow(ctx, &wpb.GetWorkflowRequest{Name: parent + "/workflows/ghost"}); status.Code(err) != codes.NotFound {
		t.Fatalf("get missing: %v", err)
	}

	// update: a new source makes a revision, a new description does not
	up := func(wf *wpb.Workflow, mask ...string) *wpb.Workflow {
		op, err := v.w.UpdateWorkflow(ctx, &wpb.UpdateWorkflowRequest{Workflow: wf, UpdateMask: &fieldmaskpb.FieldMask{Paths: mask}})
		if err != nil {
			t.Fatal(err)
		}
		r, err := op.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	d := up(&wpb.Workflow{Name: w.Name, Description: "hi"}, "description")
	if d.RevisionId != w.RevisionId || d.Description != "hi" {
		t.Fatalf("description update: %v", d)
	}
	n := up(&wpb.Workflow{Name: w.Name, SourceCode: &wpb.Workflow_SourceContents{SourceContents: "- s:\n    return: 2\n"}}, "source_contents")
	if !strings.HasPrefix(n.RevisionId, "000002-") || n.Description != "hi" {
		t.Fatalf("source update: %v", n)
	}
	_, err = v.w.UpdateWorkflow(ctx, &wpb.UpdateWorkflowRequest{Workflow: &wpb.Workflow{Name: w.Name,
		SourceCode: &wpb.Workflow_SourceContents{SourceContents: "{"}}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"source_contents"}}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = v.w.UpdateWorkflow(ctx, &wpb.UpdateWorkflowRequest{Workflow: &wpb.Workflow{Name: w.Name}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = v.w.UpdateWorkflow(ctx, &wpb.UpdateWorkflowRequest{Workflow: &wpb.Workflow{Name: parent + "/workflows/ghost"}})
	wantCode(t, err, codes.NotFound)

	// revisions: newest first, and a revision can be fetched by id
	revs := v.w.ListWorkflowRevisions(ctx, &wpb.ListWorkflowRevisionsRequest{Name: w.Name})
	var ids []string
	for r, err := revs.Next(); err == nil; r, err = revs.Next() {
		ids = append(ids, r.RevisionId)
	}
	if len(ids) != 2 || ids[0] != n.RevisionId || ids[1] != w.RevisionId {
		t.Fatalf("revisions %v", ids)
	}
	old, err := v.w.GetWorkflow(ctx, &wpb.GetWorkflowRequest{Name: w.Name, RevisionId: w.RevisionId})
	if err != nil || old.GetSourceContents() != "- s:\n    return: 1\n" {
		t.Fatalf("old revision: %v %v", old, err)
	}
	// an execution uses the latest source
	ok(t, v.exec(t, w.Name), "2")

	// list and delete
	v.create(t, "w2", "- s:\n    return: 1\n")
	it := v.w.ListWorkflows(ctx, &wpb.ListWorkflowsRequest{Parent: parent, PageSize: 1})
	var names []string
	for r, err := it.Next(); err == nil; r, err = it.Next() {
		names = append(names, r.Name)
	}
	if len(names) != 2 || names[0] != parent+"/workflows/w1" {
		t.Fatalf("list %v", names)
	}
	del, err := v.w.DeleteWorkflow(ctx, &wpb.DeleteWorkflowRequest{Name: w.Name})
	if err != nil || del.Wait(ctx) != nil {
		t.Fatal(err)
	}
	_, err = v.w.DeleteWorkflow(ctx, &wpb.DeleteWorkflowRequest{Name: w.Name})
	wantCode(t, err, codes.NotFound)
}

func (v *env) exec(t *testing.T, workflow string) *epb.Execution {
	x, err := v.e.CreateExecution(ctx, &epb.CreateExecutionRequest{Parent: workflow})
	if err != nil {
		t.Fatal(err)
	}
	return v.wait(t, x.Name)
}

func TestExecutionAPI(t *testing.T) {
	v := setup(t)
	v.create(t, "sleepy", "- s:\n    call: sys.sleep\n    args: {seconds: 30}\n- r:\n    return: done\n")
	w := parent + "/workflows/sleepy"
	x, err := v.e.CreateExecution(ctx, &epb.CreateExecutionRequest{Parent: w, Execution: &epb.Execution{Argument: `{"a":1}`}})
	if err != nil {
		t.Fatal(err)
	}
	if x.State != epb.Execution_ACTIVE || x.Argument != `{"a":1}` || x.WorkflowRevisionId == "" || !strings.HasPrefix(x.Name, w+"/executions/") {
		t.Fatalf("created %v", x)
	}
	got, _ := v.e.GetExecution(ctx, &epb.GetExecutionRequest{Name: x.Name})
	if got.State != epb.Execution_ACTIVE {
		t.Fatalf("running: %v", got)
	}
	c, err := v.e.CancelExecution(ctx, &epb.CancelExecutionRequest{Name: x.Name})
	if err != nil || c.State != epb.Execution_CANCELLED || c.EndTime == nil {
		t.Fatalf("cancel: %v %v", c, err)
	}
	time.Sleep(50 * time.Millisecond)
	if got = v.wait(t, x.Name); got.State != epb.Execution_CANCELLED || got.Result != "" {
		t.Fatalf("after cancel: %v", got)
	}
	_, err = v.e.CancelExecution(ctx, &epb.CancelExecutionRequest{Name: x.Name})
	wantCode(t, err, codes.FailedPrecondition)

	// views: BASIC leaves the argument out, FULL has it
	b, _ := v.e.GetExecution(ctx, &epb.GetExecutionRequest{Name: x.Name, View: epb.ExecutionView_BASIC})
	if b.Argument != "" || b.State != epb.Execution_CANCELLED {
		t.Fatalf("basic view: %v", b)
	}
	it := v.e.ListExecutions(ctx, &epb.ListExecutionsRequest{Parent: w})
	l, err := it.Next()
	if err != nil || l.Name != x.Name || l.Argument != "" {
		t.Fatalf("list: %v %v", l, err)
	}
	it = v.e.ListExecutions(ctx, &epb.ListExecutionsRequest{Parent: w, View: epb.ExecutionView_FULL})
	if l, _ = it.Next(); l.Argument != `{"a":1}` {
		t.Fatalf("full list: %v", l)
	}

	// errors
	_, err = v.e.CreateExecution(ctx, &epb.CreateExecutionRequest{Parent: parent + "/workflows/ghost"})
	wantCode(t, err, codes.NotFound)
	_, err = v.e.CreateExecution(ctx, &epb.CreateExecutionRequest{Parent: w, Execution: &epb.Execution{Argument: "{nope"}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = v.e.CreateExecution(ctx, &epb.CreateExecutionRequest{Parent: "bad"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = v.e.GetExecution(ctx, &epb.GetExecutionRequest{Name: w + "/executions/ghost"})
	wantCode(t, err, codes.NotFound)
	_, err = v.e.GetExecution(ctx, &epb.GetExecutionRequest{Name: "bad"})
	wantCode(t, err, codes.InvalidArgument)
	it = v.e.ListExecutions(ctx, &epb.ListExecutionsRequest{Parent: parent + "/workflows/ghost"})
	_, err = it.Next()
	wantCode(t, err, codes.NotFound)
}

// The REST mirror, on the hosts and prefixes the Google clients and Terraform use.
func TestREST(t *testing.T) {
	v := setup(t)
	do := func(method, host, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, v.url+path, strings.NewReader(body))
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	const base = "/v1/" + parent
	code, op := do("POST", "workflows.googleapis.com", base+"/workflows?workflowId=r1", `{"sourceContents": "- s:\n    return: [1, 2]\n"}`)
	if code != 200 || op["done"] != true {
		t.Fatalf("create: %d %v", code, op)
	}
	if code, _ = do("GET", "workflows.googleapis.com", "/workflows/v1/"+parent+"/workflows/r1", ""); code != 200 {
		t.Fatalf("prefixed get: %d", code)
	}
	if code, _ = do("POST", "workflows.googleapis.com", base+"/workflows?workflowId=r1", `{"sourceContents": "- s:\n    return: 1\n"}`); code != 409 {
		t.Fatalf("duplicate: %d", code)
	}
	if code, _ = do("POST", "workflows.googleapis.com", base+"/workflows?workflowId=r2", `{"sourceContents": "{"}`); code != 400 {
		t.Fatalf("bad source: %d", code)
	}
	code, x := do("POST", "workflowexecutions.googleapis.com", base+"/workflows/r1/executions", `{"argument": "{}"}`)
	if code != 200 {
		t.Fatalf("create execution: %d %v", code, x)
	}
	name := x["name"].(string)
	var got map[string]any
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if _, got = do("GET", "workflowexecutions.googleapis.com", "/v1/"+name, ""); got["state"] != "ACTIVE" {
			break
		}
	}
	if got["state"] != "SUCCEEDED" || got["result"] != "[1,2]" {
		t.Fatalf("execution: %v", got)
	}
	if _, got = do("GET", "workflowexecutions.googleapis.com", "/v1/"+name+"?view=BASIC", ""); got["result"] != nil {
		t.Fatalf("basic view: %v", got)
	}
	if code, got = do("GET", "workflows.googleapis.com", base+"/workflows/r1:listRevisions", ""); code != 200 || len(got["workflows"].([]any)) != 1 {
		t.Fatalf("revisions: %d %v", code, got)
	}
	if code, got = do("PATCH", "workflows.googleapis.com", base+"/workflows/r1?updateMask=description", `{"description": "d"}`); code != 200 || got["done"] != true {
		t.Fatalf("patch: %d %v", code, got)
	}
	if code, _ = do("GET", "workflowexecutions.googleapis.com", base+"/workflows/ghost/executions", ""); code != 404 {
		t.Fatalf("missing: %d", code)
	}
}

func TestExpressionErrors(t *testing.T) {
	v := setup(t)
	for name, e := range map[string]string{
		"syntax":   "1 +",
		"types":    `"a" - 1`,
		"zero":     "1 // 0",
		"index":    "[1][5]",
		"undefvar": "ghost",
		"badchar":  "1 = 2",
	} {
		t.Run(name, func(t *testing.T) {
			x := v.run(t, "e"+name, "- r:\n    assign: [{x: '${"+e+"}'}]\n", "")
			failed(t, x)
		})
	}
}
