package run_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runv2 "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

const parent = "projects/proj/locations/us-central1"

func endpoint(srv *pogi.Running) string { return strings.TrimPrefix(srv.URL, "http://") }

func opts(srv *pogi.Running) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint(srv)), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
}

// docker skips the test without a container engine and returns pogi with one.
func start(t *testing.T) *pogi.Running {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("no docker CLI")
	}
	srv := pogi.Start(t, pogi.Config{Services: []string{"run", "operations"}})
	if !srv.Hub.Docker().Available() {
		t.Skip("no container engine")
	}
	return srv
}

// image builds a tiny image from a Dockerfile in a temp dir: busybox's httpd
// serves /index.html (the tag) and two CGI scripts, env and rec/last (which
// record the environment of the last POST).
func image(t *testing.T, tag string) string {
	t.Helper()
	dir := t.TempDir()
	name := "pogi-test-run-" + strings.ToLower(core.RandHex(4))
	files := map[string]string{
		"Dockerfile": `FROM busybox:latest
COPY www /www
RUN chmod +x /www/cgi-bin/*
CMD ["sh", "-c", "exec httpd -f -p $PORT -h /www"]
`,
		"www/index.html":   tag,
		"www/cgi-bin/env":  "#!/bin/sh\necho 'Content-type: text/plain'\necho\nenv\n",
		"www/cgi-bin/rec":  "#!/bin/sh\nenv > /tmp/last\ncat >> /tmp/last\necho 'Content-type: text/plain'\necho\necho recorded\n",
		"www/cgi-bin/last": "#!/bin/sh\necho 'Content-type: text/plain'\necho\ncat /tmp/last\n",
		"www/cgi-bin/boom": "#!/bin/sh\necho 'Status: 500 Boom'\necho 'Content-type: text/plain'\necho\necho boom\n",
	}
	for n, c := range files {
		p := filepath.Join(dir, n)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if out, err := exec.Command("docker", "build", "-q", "-t", name, dir).CombinedOutput(); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}
	t.Cleanup(func() { exec.Command("docker", "rmi", "-f", name).Run() })
	return name
}

// get requests path of the service host through pogi's port (no DNS needed).
func get(srv *pogi.Running, host, path string) (int, string) {
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, n, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, n, endpoint(srv))
	}}}
	resp, err := c.Get("http://" + host + ":" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:] + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func template(img string, env ...*runpb.EnvVar) *runpb.RevisionTemplate {
	return &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: img, Env: env,
		Ports: []*runpb.ContainerPort{{ContainerPort: 8080}}}}}
}

func TestServices(t *testing.T) {
	srv := start(t)
	v1, v2 := image(t, "hello-v1"), image(t, "hello-v2")
	c := must(runv2.NewServicesClient(ctx, opts(srv)...))
	defer c.Close()
	revs := must(runv2.NewRevisionsClient(ctx, opts(srv)...))
	defer revs.Close()

	op := must(c.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent, ServiceId: "web",
		Service: &runpb.Service{Labels: map[string]string{"env": "test"},
			Template: template(v1, &runpb.EnvVar{Name: "GREETING", Values: &runpb.EnvVar_Value{Value: "hi there"}})}}))
	svc := must(op.Wait(ctx))
	port := srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	if svc.Name != parent+"/services/web" || svc.Uri != "http://web.run.localhost:"+port || len(svc.Urls) != 1 || svc.Etag == "" ||
		svc.Generation != 1 || svc.ObservedGeneration != 1 || svc.Labels["env"] != "test" {
		t.Fatalf("created = %v", svc)
	}
	rev1 := svc.LatestReadyRevision
	if !strings.HasPrefix(rev1, svc.Name+"/revisions/web-00001-") || svc.LatestCreatedRevision != rev1 ||
		svc.TerminalCondition.GetType() != "Ready" || svc.TerminalCondition.State != runpb.Condition_CONDITION_SUCCEEDED ||
		len(svc.TrafficStatuses) != 1 || svc.TrafficStatuses[0].Percent != 100 || svc.TrafficStatuses[0].Type != runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST {
		t.Fatalf("status = %v", svc)
	}
	_, err := c.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent, ServiceId: "web", Service: &runpb.Service{Template: template(v1)}})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate = %v", err)
	}
	_, err = c.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent, ServiceId: "noimage", Service: &runpb.Service{Template: template("")}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no image = %v", err)
	}

	// the host name and the path both reach the container (it starts on the first request)
	if code, body := get(srv, "web.run.localhost", "/index.html"); code != 200 || body != "hello-v1" {
		t.Fatalf("host = %d %q", code, body)
	}
	if code, body := get(srv, "127.0.0.1", "/_gcp/run/proj/us-central1/web/index.html"); code != 200 || body != "hello-v1" {
		t.Fatalf("path = %d %q", code, body)
	}
	_, env := get(srv, "web.run.localhost", "/cgi-bin/env")
	for _, want := range []string{"K_SERVICE=web", "K_CONFIGURATION=web", "K_REVISION=" + strings.TrimPrefix(rev1, svc.Name+"/revisions/"), "PORT=8080",
		"GREETING=hi there", "GOOGLE_CLOUD_PROJECT=proj"} {
		if !strings.Contains(env, want) {
			t.Errorf("env lacks %q:\n%s", want, env)
		}
	}
	if code, _ := get(srv, "other.run.localhost", "/"); code != 404 {
		t.Fatalf("unknown service = %d", code)
	}
	if code, _ := get(srv, "web.run.localhost", "/cgi-bin/boom"); code != 500 {
		t.Fatalf("a container's 500 is passed on, got %d", code)
	}

	// a template change makes revision 2; all traffic follows it
	svc.Template.Containers[0].Image = v2
	svc = must(must(c.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: svc})).Wait(ctx))
	rev2 := svc.LatestReadyRevision
	if !strings.HasPrefix(rev2, svc.Name+"/revisions/web-00002-") || svc.Generation != 2 || svc.ObservedGeneration != 2 {
		t.Fatalf("updated = %v", svc)
	}
	if _, body := get(srv, "web.run.localhost", "/index.html"); body != "hello-v2" {
		t.Fatalf("after update: %q", body)
	}
	// no change, no revision
	again := must(must(c.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: svc})).Wait(ctx))
	if again.LatestReadyRevision != rev2 || again.Generation != 2 {
		t.Fatalf("no-op update = %v", again)
	}
	list := revs.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: svc.Name})
	var names []string
	for r, err := list.Next(); err == nil; r, err = list.Next() {
		names = append(names, r.Name)
	}
	if len(names) != 2 || names[0] != rev2 || names[1] != rev1 {
		t.Fatalf("revisions = %v", names)
	}

	// a 50/50 split between the two revisions, and a tag on the old one
	short := func(r string) string { return r[strings.LastIndexByte(r, '/')+1:] }
	svc.Traffic = []*runpb.TrafficTarget{
		{Type: runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION, Revision: short(rev1), Percent: 50, Tag: "old"},
		{Type: runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST, Percent: 50}}
	svc = must(must(c.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: svc})).Wait(ctx))
	if svc.LatestReadyRevision != rev2 || len(svc.TrafficStatuses) != 2 || svc.TrafficStatuses[0].Uri != "http://old---web.run.localhost:"+port {
		t.Fatalf("split = %v", svc)
	}
	seen := map[string]int{}
	for range 60 {
		_, body := get(srv, "web.run.localhost", "/index.html")
		seen[body]++
	}
	if seen["hello-v1"] < 10 || seen["hello-v2"] < 10 || seen["hello-v1"]+seen["hello-v2"] != 60 {
		t.Fatalf("a 50/50 split answered %v", seen)
	}
	for range 5 {
		if _, body := get(srv, "old---web.run.localhost", "/index.html"); body != "hello-v1" {
			t.Fatalf("tag old answered %q", body)
		}
	}
	if code, _ := get(srv, "nope---web.run.localhost", "/"); code != 404 {
		t.Fatalf("unknown tag = %d", code)
	}
	// bad splits and revision deletes
	svc.Traffic = []*runpb.TrafficTarget{{Type: runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST, Percent: 60}}
	if _, err := c.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: svc}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("60%% = %v", err)
	}
	if _, err := revs.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: rev1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete a revision with traffic = %v", err)
	}

	l := c.ListServices(ctx, &runpb.ListServicesRequest{Parent: parent})
	if s, err := l.Next(); err != nil || s.Name != svc.Name {
		t.Fatalf("list = %v %v", s, err)
	}
	if tbl := srv.Hub.Service("run").(core.Browsable).Resources(); len(tbl[1].Rows) != 2 || tbl[1].Rows[0][3] != "true" {
		t.Fatalf("console = %v", tbl)
	}

	// delete: the service, its revisions and containers go
	must(must(c.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: svc.Name})).Wait(ctx))
	if _, err := c.GetService(ctx, &runpb.GetServiceRequest{Name: svc.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete = %v", err)
	}
	if code, _ := get(srv, "web.run.localhost", "/index.html"); code != 404 {
		t.Fatalf("data plane after delete = %d", code)
	}
	for _, r := range []string{rev1, rev2} {
		if out, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=pogi.resource="+short(r)).Output(); len(bytes.TrimSpace(out)) != 0 {
			t.Fatalf("container of %s left: %s", short(r), out)
		}
	}
}

func TestDeliver(t *testing.T) {
	srv := start(t)
	c := must(runv2.NewServicesClient(ctx, opts(srv)...))
	defer c.Close()
	must(must(c.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent, ServiceId: "sink", Service: &runpb.Service{Template: template(image(t, "x"))}})).Wait(ctx))

	attr := func(v string) core.MessageAttribute { return core.MessageAttribute{DataType: "String", StringValue: v} }
	target := "//run.googleapis.com/" + parent + "/services/sink"
	err := srv.Hub.Deliver(target, &core.Message{Body: `{"hello":"world"}`, Subject: "/cgi-bin/rec", Attributes: map[string]core.MessageAttribute{
		"ce-type": attr("com.example.thing"), "ce-id": attr("42"), "content-type": attr("application/cloudevents+json")}})
	if err != nil {
		t.Fatal(err)
	}
	_, last := get(srv, "sink.run.localhost", "/cgi-bin/last")
	for _, want := range []string{"REQUEST_METHOD=POST", "HTTP_CE_TYPE=com.example.thing", "HTTP_CE_ID=42", "CONTENT_TYPE=application/cloudevents+json", `{"hello":"world"}`} {
		if !strings.Contains(last, want) {
			t.Errorf("delivered request lacks %q:\n%s", want, last)
		}
	}
	if err := srv.Hub.Deliver(target, &core.Message{Body: "x", Subject: "/cgi-bin/boom"}); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("a 500 should fail the delivery, got %v", err)
	}
	if err := srv.Hub.Deliver("//run.googleapis.com/"+parent+"/services/missing", &core.Message{}); err == nil {
		t.Fatal("delivering to a missing service should fail")
	}
}

func TestJobs(t *testing.T) {
	srv := start(t)
	img := image(t, "x")
	jobs := must(runv2.NewJobsClient(ctx, opts(srv)...))
	defer jobs.Close()
	execs := must(runv2.NewExecutionsClient(ctx, opts(srv)...))
	defer execs.Close()

	mk := func(id, script string, n, parallel, retries int32) *runpb.Job {
		return must(must(jobs.CreateJob(ctx, &runpb.CreateJobRequest{Parent: parent, JobId: id, Job: &runpb.Job{Template: &runpb.ExecutionTemplate{
			TaskCount: n, Parallelism: parallel, Template: &runpb.TaskTemplate{Retries: &runpb.TaskTemplate_MaxRetries{MaxRetries: retries},
				Timeout:    durationpb.New(time.Minute),
				Containers: []*runpb.Container{{Image: img, Command: []string{"sh", "-c"}, Args: []string{script}}}}}}})).Wait(ctx))
	}
	job := mk("batch", `echo "task $CLOUD_RUN_TASK_INDEX of $CLOUD_RUN_TASK_COUNT in $CLOUD_RUN_EXECUTION attempt $CLOUD_RUN_TASK_ATTEMPT"`, 3, 2, 0)
	if job.Name != parent+"/jobs/batch" || job.Template.TaskCount != 3 || job.TerminalCondition.GetType() != "Ready" {
		t.Fatalf("job = %v", job)
	}
	ex := must(must(jobs.RunJob(ctx, &runpb.RunJobRequest{Name: job.Name})).Wait(ctx))
	if ex.TaskCount != 3 || ex.SucceededCount != 3 || ex.FailedCount != 0 || ex.RunningCount != 0 || ex.CompletionTime == nil ||
		ex.Job != job.Name || !strings.HasPrefix(ex.Name, job.Name+"/executions/batch-") {
		t.Fatalf("execution = %v", ex)
	}
	if c := ex.Conditions[0]; c.Type != "Completed" || c.State != runpb.Condition_CONDITION_SUCCEEDED {
		t.Fatalf("conditions = %v", ex.Conditions)
	}
	got := must(execs.GetExecution(ctx, &runpb.GetExecutionRequest{Name: ex.Name}))
	if got.SucceededCount != 3 {
		t.Fatalf("get = %v", got)
	}
	logs := fmt.Sprint(srv.Hub.Service("run").(core.Inspectable).Inspect("executions", ex.Name))
	exShort := ex.Name[strings.LastIndexByte(ex.Name, '/')+1:]
	for i := range 3 {
		if want := fmt.Sprintf("task %d of 3 in %s attempt 0", i, exShort); !strings.Contains(logs, want) {
			t.Errorf("logs lack %q: %s", want, logs)
		}
	}
	if j := must(jobs.GetJob(ctx, &runpb.GetJobRequest{Name: job.Name})); j.ExecutionCount != 1 || j.LatestCreatedExecution.GetName() != ex.Name {
		t.Fatalf("job after run = %v", j)
	}

	// overrides run one task; a failing task is retried, then fails the execution
	ex = must(must(jobs.RunJob(ctx, &runpb.RunJobRequest{Name: job.Name, Overrides: &runpb.RunJobRequest_Overrides{TaskCount: 1}})).Wait(ctx))
	if ex.TaskCount != 1 || ex.SucceededCount != 1 {
		t.Fatalf("overridden = %v", ex)
	}
	bad := mk("flaky", "exit 3", 2, 0, 1)
	ex = must(must(jobs.RunJob(ctx, &runpb.RunJobRequest{Name: bad.Name})).Wait(ctx))
	if ex.FailedCount != 2 || ex.SucceededCount != 0 || ex.RetriedCount != 2 || ex.Conditions[0].State != runpb.Condition_CONDITION_FAILED {
		t.Fatalf("failed execution = %v", ex)
	}
	n := 0
	it := execs.ListExecutions(ctx, &runpb.ListExecutionsRequest{Parent: job.Name})
	for _, err := it.Next(); err == nil; _, err = it.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("executions of batch = %d", n)
	}

	// cancel a running execution
	slow := mk("slow", "sleep 120", 2, 0, 0)
	run := must(jobs.RunJob(ctx, &runpb.RunJobRequest{Name: slow.Name}))
	meta := must(run.Metadata())
	deadline := time.Now().Add(30 * time.Second)
	for {
		cur := must(execs.GetExecution(ctx, &runpb.GetExecutionRequest{Name: meta.Name}))
		if cur.RunningCount == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tasks never ran: %v", cur)
		}
		time.Sleep(200 * time.Millisecond)
	}
	ex = must(must(execs.CancelExecution(ctx, &runpb.CancelExecutionRequest{Name: meta.Name})).Wait(ctx))
	if ex.CancelledCount != 2 || ex.RunningCount != 0 || ex.CompletionTime == nil {
		t.Fatalf("cancelled = %v", ex)
	}
	if _, err := execs.CancelExecution(ctx, &runpb.CancelExecutionRequest{Name: meta.Name}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cancel twice = %v", err)
	}

	// delete the execution, then the job
	must(must(execs.DeleteExecution(ctx, &runpb.DeleteExecutionRequest{Name: meta.Name})).Wait(ctx))
	if _, err := execs.GetExecution(ctx, &runpb.GetExecutionRequest{Name: meta.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("get deleted execution = %v", err)
	}
	must(must(jobs.DeleteJob(ctx, &runpb.DeleteJobRequest{Name: job.Name})).Wait(ctx))
	if _, err := jobs.GetJob(ctx, &runpb.GetJobRequest{Name: job.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("get deleted job = %v", err)
	}
}

// Without a container engine the API still works, and requests get 503.
func TestNoEngine(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"run", "operations"}, DockerHost: "tcp://127.0.0.1:1"})
	c := must(runv2.NewServicesClient(ctx, opts(srv)...))
	defer c.Close()
	svc := must(must(c.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent, ServiceId: "web", Service: &runpb.Service{Template: template("any/image:1")}})).Wait(ctx))
	if svc.Uri == "" {
		t.Fatalf("service = %v", svc)
	}
	if code, body := get(srv, "web.run.localhost", "/"); code != 503 || !strings.Contains(body, "container engine") {
		t.Fatalf("no engine = %d %q", code, body)
	}
	jobs := must(runv2.NewJobsClient(ctx, opts(srv)...))
	defer jobs.Close()
	job := must(must(jobs.CreateJob(ctx, &runpb.CreateJobRequest{Parent: parent, JobId: "j", Job: &runpb.Job{Template: &runpb.ExecutionTemplate{
		Template: &runpb.TaskTemplate{Containers: []*runpb.Container{{Image: "any/image:1"}}}}}})).Wait(ctx))
	if ex := must(must(jobs.RunJob(ctx, &runpb.RunJobRequest{Name: job.Name})).Wait(ctx)); ex.FailedCount != 1 {
		t.Fatalf("execution = %v", ex)
	}
}

// The REST mirror: create, get and poll the operation with plain net/http.
func TestREST(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"run", "operations"}, DockerHost: "tcp://127.0.0.1:1"})
	do := func(method, path, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, op := do("POST", "/v2/"+parent+"/services?serviceId=api", `{"template":{"containers":[{"image":"example/api:1"}]},"labels":{"a":"b"}}`)
	name, _ := op["name"].(string)
	if code != 200 || !strings.HasPrefix(name, parent+"/operations/") {
		t.Fatalf("create = %d %v", code, op)
	}
	var done map[string]any
	for range 50 {
		if _, done = do("GET", "/v2/"+name, ""); done["done"] == true {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	resp, _ := done["response"].(map[string]any)
	if done["done"] != true || resp["name"] != parent+"/services/api" || resp["@type"] != "type.googleapis.com/google.cloud.run.v2.Service" {
		t.Fatalf("operation = %v", done)
	}
	code, svc := do("GET", "/v2/"+parent+"/services/api", "")
	if code != 200 || svc["uri"] == "" || svc["terminalCondition"].(map[string]any)["type"] != "Ready" || svc["observedGeneration"] != "1" {
		t.Fatalf("get = %d %v", code, svc)
	}
	code, list := do("GET", "/v2/"+parent+"/services", "")
	if code != 200 || len(list["services"].([]any)) != 1 {
		t.Fatalf("list = %d %v", code, list)
	}
	code, revs := do("GET", "/v2/"+parent+"/services/api/revisions", "")
	if code != 200 || len(revs["revisions"].([]any)) != 1 {
		t.Fatalf("revisions = %d %v", code, revs)
	}
	code, patched := do("PATCH", "/v2/"+parent+"/services/api?updateMask=labels", `{"labels":{"a":"c"}}`)
	if code != 200 || !strings.HasPrefix(patched["name"].(string), parent+"/operations/") {
		t.Fatalf("patch = %d %v", code, patched)
	}
	if _, svc = do("GET", "/v2/"+parent+"/services/api", ""); svc["labels"].(map[string]any)["a"] != "c" || svc["generation"] != "2" {
		t.Fatalf("after patch = %v", svc)
	}
	if code, e := do("GET", "/v2/"+parent+"/services/missing", ""); code != 404 || e["error"].(map[string]any)["status"] != "NOT_FOUND" {
		t.Fatalf("missing = %d %v", code, e)
	}
	if code, e := do("POST", "/v2/"+parent+"/services/api:setIamPolicy", `{}`); code != 400 || e["error"].(map[string]any)["status"] != "INVALID_ARGUMENT" {
		t.Fatalf("setIamPolicy without a policy = %d %v", code, e)
	}
	// gcloud functions deploy --allow-unauthenticated binds the invoker role over the v1 API
	invoker := `{"policy":{"bindings":[{"role":"roles/run.invoker","members":["allUsers"]}]}}`
	if code, p := do("POST", "/v1/"+parent+"/services/api:setIamPolicy", invoker); code != 200 || p["etag"] == nil {
		t.Fatalf("v1 setIamPolicy = %d %v", code, p)
	}
	if code, p := do("GET", "/v1/"+parent+"/services/api:getIamPolicy", ""); code != 200 || len(p["bindings"].([]any)) != 1 {
		t.Fatalf("v1 getIamPolicy = %d %v", code, p)
	}
	if code, _ := do("POST", "/v2/"+parent+"/jobs?jobId=j", "null"); code != 400 {
		t.Fatalf("a null body = %d, want a 400, not a panic", code)
	}
	if code, _ := do("DELETE", "/v2/"+parent+"/services/api", ""); code != 200 {
		t.Fatalf("delete = %d", code)
	}
	if code, _ := do("GET", "/v2/"+parent+"/services/api", ""); code != 404 {
		t.Fatalf("get deleted = %d", code)
	}
}

// Save and Load keep services, revisions and jobs.
func TestPersistence(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"run", "operations"}, DockerHost: "tcp://127.0.0.1:1"})
	c := must(runv2.NewServicesClient(ctx, opts(srv)...))
	defer c.Close()
	must(must(c.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent, ServiceId: "keep", Service: &runpb.Service{Template: template("a/b:1")}})).Wait(ctx))
	st := srv.Hub.Service("run").(core.Stateful)
	b := must(st.Save())
	st.Reset()
	if _, err := c.GetService(ctx, &runpb.GetServiceRequest{Name: parent + "/services/keep"}); status.Code(err) != codes.NotFound {
		t.Fatalf("after reset = %v", err)
	}
	if err := st.Load(b); err != nil {
		t.Fatal(err)
	}
	svc := must(c.GetService(ctx, &runpb.GetServiceRequest{Name: parent + "/services/keep"}))
	if !strings.Contains(svc.LatestReadyRevision, "keep-00001-") {
		t.Fatalf("after load = %v", svc)
	}
}

func TestKnative(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"run", "operations"}, DockerHost: "tcp://127.0.0.1:1"})
	do := func(method, host, path, body string) (int, map[string]any) {
		if !strings.HasPrefix(path, "/v") {
			path = "/apis/serving.knative.dev/v1/namespaces/pogi/services" + path
		}
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	const regional, plain = "us-central1-run.localhost", "run.localhost"
	body := func(nonce, container string) string {
		return `{"apiVersion":"serving.knative.dev/v1","kind":"Service","metadata":{"annotations":{"run.googleapis.com/client-name":"gcloud"},"labels":{},"name":"hello","namespace":"pogi"},"spec":{"template":{"metadata":{"annotations":{"run.googleapis.com/startup-cpu-boost":"True"},"labels":{"client.knative.dev/nonce":"` + nonce + `"}},"spec":{"containers":[` + container + `],"nodeSelector":{}}}},"status":{"address":{}}}`
	}
	nginx := `{"image":"nginx:alpine","name":"","ports":[{"containerPort":80}]}`
	if code, _ := do("GET", regional, "/hello", ""); code != 404 {
		t.Fatalf("get before create = %d", code)
	}
	code, svc := do("POST", regional, "", body("aaa", nginx))
	meta, _ := svc["metadata"].(map[string]any)
	st, _ := svc["status"].(map[string]any)
	conds, _ := st["conditions"].([]any)
	if code != 200 || meta["labels"].(map[string]any)["cloud.googleapis.com/location"] != "us-central1" || st["url"] == "" ||
		len(conds) == 0 || conds[0].(map[string]any)["status"] != "True" ||
		!strings.Contains(fmt.Sprint(svc["spec"]), "containerPort:80") {
		t.Fatalf("create = %d %v", code, svc)
	}
	if _, v2 := do("GET", plain, "/v2/projects/pogi/locations/us-central1/services/hello", ""); v2["name"] != "projects/pogi/locations/us-central1/services/hello" {
		t.Fatalf("v2 get = %v", v2)
	}
	first := st["latestCreatedRevisionName"]
	code, svc = do("PUT", regional, "/hello", body("bbb", nginx))
	meta, _ = svc["metadata"].(map[string]any)
	st, _ = svc["status"].(map[string]any)
	if code != 200 || meta["generation"] != float64(2) || st["latestCreatedRevisionName"] == first {
		t.Fatalf("replace = %d %v", code, svc)
	}
	if code, list := do("GET", plain, "", ""); code != 200 || len(list["items"].([]any)) != 1 || list["kind"] != "ServiceList" {
		t.Fatalf("list = %d %v", code, list)
	}
	// gcloud run services list without --region
	if code, list := do("GET", plain, "/v1/projects/pogi/locations/-/services", ""); code != 200 || len(list["items"].([]any)) != 1 {
		t.Fatalf("v1 list of every region = %d %v", code, list)
	}
	if code, _ := do("GET", plain, "/hello", ""); code != 400 {
		t.Fatalf("get without region = %d", code)
	}
	if code, e := do("POST", regional, "", body("c", `{"image":"x","env":[{"name":"A","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]}`)); code != 400 {
		t.Fatalf("valueFrom = %d %v", code, e)
	}
	if code, del := do("DELETE", regional, "/hello", ""); code != 200 || del["status"] != "Success" {
		t.Fatalf("delete = %d %v", code, del)
	}
	if code, _ := do("GET", regional, "/hello", ""); code != 404 {
		t.Fatalf("get after delete = %d", code)
	}
}
