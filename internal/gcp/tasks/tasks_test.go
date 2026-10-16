package tasks_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	pb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/tasks"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ctx = context.Background()

const parent = "projects/proj/locations/us-central1"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

// setup starts pogi with Cloud Tasks and returns its clock-less service (Tick is the test's), a client and the URL.
func setup(t *testing.T) (*tasks.Service, *cloudtasks.Client, string) {
	t.Helper()
	var svc *tasks.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = tasks.New(h); svc.Manual(); return svc })
	c := must(cloudtasks.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { c.Close() })
	return svc, c, url
}

// target is an HTTP handler that records requests and answers with status(n) for the nth call.
type target struct {
	*httptest.Server
	mu     sync.Mutex
	calls  []*http.Request
	bodies []string
	status func(n int) int
	block  chan struct{} // when set, handlers wait for it to close
}

func newTarget(t *testing.T, status func(n int) int) *target {
	tg := &target{status: status}
	tg.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		tg.mu.Lock()
		tg.calls, tg.bodies = append(tg.calls, r), append(tg.bodies, string(b))
		n, block := len(tg.calls), tg.block
		tg.mu.Unlock()
		if block != nil {
			<-block
		}
		w.WriteHeader(tg.status(n))
	}))
	t.Cleanup(tg.Close)
	return tg
}

func (tg *target) count() int {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return len(tg.calls)
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func mkQueue(t *testing.T, c *cloudtasks.Client, id string, q *pb.Queue) *pb.Queue {
	t.Helper()
	q.Name = parent + "/queues/" + id
	return must(c.CreateQueue(ctx, &pb.CreateQueueRequest{Parent: parent, Queue: q}))
}

func httpTask(url string) *pb.Task {
	return &pb.Task{MessageType: &pb.Task_HttpRequest{HttpRequest: &pb.HttpRequest{Url: url, Body: []byte("hello"),
		Headers: map[string]string{"X-Custom": "yes"}}}}
}

func TestQueues(t *testing.T) {
	_, c, _ := setup(t)
	q := mkQueue(t, c, "jobs", &pb.Queue{})
	if q.State != pb.Queue_RUNNING || q.RateLimits.MaxDispatchesPerSecond != 500 || q.RateLimits.MaxConcurrentDispatches != 1000 ||
		q.RetryConfig.MaxAttempts != 100 || q.RetryConfig.MaxBackoff.Seconds != 3600 {
		t.Fatalf("defaults = %v", q)
	}
	_, err := c.CreateQueue(ctx, &pb.CreateQueueRequest{Parent: parent, Queue: &pb.Queue{Name: q.Name}})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateQueue(ctx, &pb.CreateQueueRequest{Parent: parent, Queue: &pb.Queue{Name: "nope"}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.GetQueue(ctx, &pb.GetQueueRequest{Name: parent + "/queues/missing"})
	wantCode(t, err, codes.NotFound)
	mkQueue(t, c, "alpha", &pb.Queue{})

	var names []string
	for it := c.ListQueues(ctx, &pb.ListQueuesRequest{Parent: parent}); ; {
		n, err := it.Next()
		if err == iterator.Done {
			break
		}
		names = append(names, must(n, err).Name)
	}
	if len(names) != 2 || !strings.HasSuffix(names[0], "alpha") {
		t.Fatalf("list = %v", names)
	}

	up := must(c.UpdateQueue(ctx, &pb.UpdateQueueRequest{
		Queue: &pb.Queue{Name: q.Name, RateLimits: &pb.RateLimits{MaxDispatchesPerSecond: 5, MaxConcurrentDispatches: 2},
			RetryConfig: &pb.RetryConfig{MaxAttempts: 3, MinBackoff: durationpb.New(time.Second)}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits.max_dispatches_per_second", "retry_config.max_attempts"}}}))
	if up.RateLimits.MaxDispatchesPerSecond != 5 || up.RateLimits.MaxConcurrentDispatches != 1000 || up.RetryConfig.MaxAttempts != 3 ||
		up.RetryConfig.MinBackoff.AsDuration() != 100*time.Millisecond {
		t.Fatalf("update = %v", up)
	}

	if p := must(c.PauseQueue(ctx, &pb.PauseQueueRequest{Name: q.Name})); p.State != pb.Queue_PAUSED {
		t.Fatalf("pause = %v", p)
	}
	if p := must(c.ResumeQueue(ctx, &pb.ResumeQueueRequest{Name: q.Name})); p.State != pb.Queue_RUNNING {
		t.Fatalf("resume = %v", p)
	}
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: httpTask("http://127.0.0.1:1/x")}))
	if p := must(c.PurgeQueue(ctx, &pb.PurgeQueueRequest{Name: q.Name})); p.PurgeTime == nil {
		t.Fatalf("purge = %v", p)
	}
	if _, err := c.ListTasks(ctx, &pb.ListTasksRequest{Parent: q.Name}).Next(); err != iterator.Done {
		t.Fatalf("no tasks expected, got %v", err)
	}
}

func TestTasks(t *testing.T) {
	_, c, _ := setup(t)
	q := mkQueue(t, c, "q1", &pb.Queue{})
	_, err := c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: parent + "/queues/missing", Task: httpTask("http://x.test/")})
	wantCode(t, err, codes.NotFound)
	_, err = c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: &pb.Task{}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: httpTask("ftp://x.test/")})
	wantCode(t, err, codes.InvalidArgument)

	auto := must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: httpTask("http://x.test/a")}))
	if !strings.HasPrefix(auto.Name, q.Name+"/tasks/") || auto.CreateTime == nil || auto.GetHttpRequest().HttpMethod != pb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		t.Fatalf("auto = %v", auto)
	}
	named := httpTask("http://x.test/b")
	named.Name = q.Name + "/tasks/my-task"
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: named}))
	_, err = c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: named})
	wantCode(t, err, codes.AlreadyExists)
	if g := must(c.GetTask(ctx, &pb.GetTaskRequest{Name: named.Name})); g.GetHttpRequest().Url != "http://x.test/b" {
		t.Fatalf("get = %v", g)
	}

	// A deleted name stays taken: it is a tombstone.
	must(0, c.DeleteTask(ctx, &pb.DeleteTaskRequest{Name: named.Name}))
	_, err = c.GetTask(ctx, &pb.GetTaskRequest{Name: named.Name})
	wantCode(t, err, codes.NotFound)
	_, err = c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: named})
	wantCode(t, err, codes.AlreadyExists)
	wantCode(t, c.DeleteTask(ctx, &pb.DeleteTaskRequest{Name: named.Name}), codes.NotFound)

	n := 0
	for it := c.ListTasks(ctx, &pb.ListTasksRequest{Parent: q.Name}); ; n++ {
		if _, err := it.Next(); err == iterator.Done {
			break
		}
	}
	if n != 1 {
		t.Fatalf("listed %d tasks, want 1", n)
	}
}

// A task whose handler fails twice is retried with doubling backoff, carries
// Cloud Tasks' headers and an OIDC token, and is deleted when the handler answers 2xx.
func TestRetries(t *testing.T) {
	svc, c, _ := setup(t)
	tg := newTarget(t, func(n int) int {
		if n < 3 {
			return 500
		}
		return 200
	})
	q := mkQueue(t, c, "retry", &pb.Queue{RetryConfig: &pb.RetryConfig{MinBackoff: durationpb.New(10 * time.Second),
		MaxBackoff: durationpb.New(100 * time.Second), MaxDoublings: 3}})
	task := httpTask(tg.URL + "/work")
	task.Name = q.Name + "/tasks/t1"
	task.GetHttpRequest().AuthorizationHeader = &pb.HttpRequest_OidcToken{OidcToken: &pb.OidcToken{ServiceAccountEmail: "sa@proj.iam.gserviceaccount.com"}}
	task.GetHttpRequest().HttpMethod = pb.HttpMethod_PUT
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: task}))

	t0 := time.Now()
	attempt := func(at time.Time, calls int) *pb.Task {
		t.Helper()
		svc.Tick(at)
		waitFor(t, "a response", func() bool {
			g, err := c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})
			return tg.count() == calls && (err != nil || g.GetLastAttempt().GetResponseTime() != nil)
		})
		g, _ := c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})
		return g
	}
	g := attempt(t0, 1)
	if g.DispatchCount != 1 || g.ResponseCount != 1 || g.FirstAttempt == nil || g.LastAttempt.ResponseStatus.Code != int32(codes.Internal) {
		t.Fatalf("after attempt 1: %v", g)
	}
	if d := g.ScheduleTime.AsTime().Sub(t0); d != 10*time.Second {
		t.Fatalf("first backoff = %v, want 10s", d)
	}
	svc.Tick(t0.Add(9 * time.Second)) // not due yet
	if g = must(c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})); g.DispatchCount != 1 {
		t.Fatalf("retried too early: %v", g)
	}
	t1 := t0.Add(10 * time.Second)
	g = attempt(t1, 2)
	if d := g.ScheduleTime.AsTime().Sub(t1); d != 20*time.Second {
		t.Fatalf("second backoff = %v, want 20s (doubled)", d)
	}
	svc.Tick(t1.Add(19 * time.Second))
	if tg.count() != 2 {
		t.Fatal("retried before the doubled backoff")
	}
	svc.Tick(t1.Add(20 * time.Second))
	waitFor(t, "the task to finish", func() bool {
		_, err := c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})
		return tg.count() == 3 && status.Code(err) == codes.NotFound
	})

	tg.mu.Lock()
	defer tg.mu.Unlock()
	for i, r := range tg.calls {
		want := map[string]string{"X-Cloudtasks-Queuename": "retry", "X-Cloudtasks-Taskname": "t1", "X-Custom": "yes",
			"X-Cloudtasks-Taskretrycount": string(rune('0' + i)), "X-Cloudtasks-Taskexecutioncount": string(rune('0' + i))}
		for k, v := range want {
			if r.Header.Get(k) != v {
				t.Errorf("attempt %d: header %s = %q, want %q", i+1, k, r.Header.Get(k), v)
			}
		}
		if r.Method != http.MethodPut || r.URL.Path != "/work" || tg.bodies[i] != "hello" || r.Header.Get("X-CloudTasks-TaskETA") == "" ||
			r.UserAgent() != "Google-Cloud-Tasks" {
			t.Errorf("attempt %d: %s %s %q ua=%q eta=%q", i+1, r.Method, r.URL.Path, tg.bodies[i], r.UserAgent(), r.Header.Get("X-CloudTasks-TaskETA"))
		}
	}
	claims := verifyJWT(t, strings.TrimPrefix(tg.calls[2].Header.Get("Authorization"), "Bearer "))
	if claims["aud"] != tg.URL+"/work" || claims["email"] != "sa@proj.iam.gserviceaccount.com" || claims["iss"] != "https://accounts.google.com" {
		t.Fatalf("claims = %v", claims)
	}
}

func verifyJWT(t *testing.T, tok string) map[string]any {
	t.Helper()
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		t.Fatalf("not a JWT: %q", tok)
	}
	sig, _ := base64.RawURLEncoding.DecodeString(p[2])
	sum := sha256.Sum256([]byte(p[0] + "." + p[1]))
	if err := rsa.VerifyPKCS1v15(gapi.PublicKey(), crypto.SHA256, sum[:], sig); err != nil {
		t.Fatalf("bad signature: %v", err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(p[1])
	var claims map[string]any
	json.Unmarshal(raw, &claims)
	return claims
}

func TestAuthAudienceAndOAuth(t *testing.T) {
	svc, c, _ := setup(t)
	tg := newTarget(t, func(int) int { return 200 })
	q := mkQueue(t, c, "auth", &pb.Queue{})
	a := httpTask(tg.URL + "/oidc")
	a.GetHttpRequest().AuthorizationHeader = &pb.HttpRequest_OidcToken{OidcToken: &pb.OidcToken{ServiceAccountEmail: "sa@p.iam", Audience: "https://api.example"}}
	b := httpTask(tg.URL + "/oauth")
	b.GetHttpRequest().AuthorizationHeader = &pb.HttpRequest_OauthToken{OauthToken: &pb.OAuthToken{ServiceAccountEmail: "sa@p.iam"}}
	for _, task := range []*pb.Task{a, b} {
		must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: task}))
	}
	svc.Tick(time.Now().Add(time.Second))
	waitFor(t, "two calls", func() bool { return tg.count() == 2 })
	tg.mu.Lock()
	defer tg.mu.Unlock()
	for _, r := range tg.calls {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if r.URL.Path == "/oidc" && verifyJWT(t, tok)["aud"] != "https://api.example" {
			t.Errorf("oidc audience wrong: %v", verifyJWT(t, tok))
		}
		if r.URL.Path == "/oauth" && !strings.HasPrefix(tok, "ya29.") {
			t.Errorf("oauth token = %q", tok)
		}
	}
}

func TestRateLimits(t *testing.T) {
	svc, c, _ := setup(t)
	tg := newTarget(t, func(int) int { return 200 })
	q := mkQueue(t, c, "slow", &pb.Queue{RateLimits: &pb.RateLimits{MaxDispatchesPerSecond: 1, MaxBurstSize: 1}})
	for range 3 {
		must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: httpTask(tg.URL)}))
	}
	t0 := time.Now().Add(time.Second)
	svc.Tick(t0)
	svc.Tick(t0.Add(500 * time.Millisecond)) // half a token: nothing
	waitFor(t, "one dispatch", func() bool { return tg.count() == 1 })
	svc.Tick(t0.Add(time.Second))
	waitFor(t, "a second dispatch", func() bool { return tg.count() == 2 })
	svc.Tick(t0.Add(1500 * time.Millisecond))
	time.Sleep(50 * time.Millisecond)
	if tg.count() != 2 {
		t.Fatalf("dispatched %d tasks in 1.5s at 1 per second", tg.count())
	}
	svc.Tick(t0.Add(2 * time.Second))
	waitFor(t, "a third dispatch", func() bool { return tg.count() == 3 })

	// Max concurrent dispatches: the second task waits for the first to finish.
	tg2 := newTarget(t, func(int) int { return 200 })
	tg2.block = make(chan struct{})
	q2 := mkQueue(t, c, "serial", &pb.Queue{RateLimits: &pb.RateLimits{MaxDispatchesPerSecond: 100, MaxConcurrentDispatches: 1}})
	for range 2 {
		must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q2.Name, Task: httpTask(tg2.URL)}))
	}
	now := time.Now().Add(time.Second)
	svc.Tick(now)
	waitFor(t, "the first call", func() bool { return tg2.count() == 1 })
	svc.Tick(now.Add(time.Second))
	time.Sleep(50 * time.Millisecond)
	if tg2.count() != 1 {
		t.Fatal("second dispatch ran beside the first despite maxConcurrentDispatches=1")
	}
	close(tg2.block)
	waitFor(t, "the first to finish", func() bool {
		svc.Tick(now.Add(2 * time.Second))
		return tg2.count() == 2
	})
}

func TestPausedAndScheduled(t *testing.T) {
	svc, c, _ := setup(t)
	tg := newTarget(t, func(int) int { return 200 })
	q := mkQueue(t, c, "p", &pb.Queue{})
	later := httpTask(tg.URL)
	later.ScheduleTime = timestamppb.New(time.Now().Add(time.Hour))
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: later}))
	now := must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: httpTask(tg.URL)}))
	must(c.PauseQueue(ctx, &pb.PauseQueueRequest{Name: q.Name}))
	svc.Tick(time.Now().Add(time.Minute))
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 0 {
		t.Fatal("paused queue dispatched")
	}
	must(c.ResumeQueue(ctx, &pb.ResumeQueueRequest{Name: q.Name}))
	svc.Tick(time.Now().Add(time.Minute)) // the hour-away task is still not due
	waitFor(t, "the due task", func() bool { return tg.count() == 1 })
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 1 {
		t.Fatal("scheduled task ran early")
	}
	// RunTask ignores the schedule and answers once the handler has.
	var left string
	for it := c.ListTasks(ctx, &pb.ListTasksRequest{Parent: q.Name}); ; {
		g, err := it.Next()
		if err != nil {
			break
		}
		left = g.Name
	}
	if left == "" || left == now.Name {
		t.Fatalf("listed %q", left)
	}
	ran := must(c.RunTask(ctx, &pb.RunTaskRequest{Name: left}))
	if ran.DispatchCount != 1 || ran.ResponseCount != 1 || tg.count() != 2 {
		t.Fatalf("run = %v, calls %d", ran, tg.count())
	}
	_, err := c.GetTask(ctx, &pb.GetTaskRequest{Name: left})
	wantCode(t, err, codes.NotFound)
}

func TestMaxAttemptsDropsTask(t *testing.T) {
	svc, c, _ := setup(t)
	tg := newTarget(t, func(int) int { return 503 })
	q := mkQueue(t, c, "drop", &pb.Queue{RetryConfig: &pb.RetryConfig{MaxAttempts: 2, MinBackoff: durationpb.New(time.Second)}})
	task := httpTask(tg.URL)
	task.Name = q.Name + "/tasks/doomed"
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: task}))
	t0 := time.Now()
	svc.Tick(t0)
	waitFor(t, "attempt 1", func() bool {
		g, _ := c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})
		return g.GetLastAttempt().GetResponseTime() != nil
	})
	svc.Tick(t0.Add(time.Second))
	waitFor(t, "the task to be dropped", func() bool {
		_, err := c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})
		return tg.count() == 2 && status.Code(err) == codes.NotFound
	})
}

func TestREST(t *testing.T) {
	svc, _, url := setup(t)
	tg := newTarget(t, func(int) int { return 200 })
	do := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, url+path, strings.NewReader(body))
		resp := must(http.DefaultClient.Do(req))
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, q := do("POST", "/v2/"+parent+"/queues", `{"name":"`+parent+`/queues/rest","rateLimits":{"maxDispatchesPerSecond":7}}`)
	if code != 200 || q["state"] != "RUNNING" || q["rateLimits"].(map[string]any)["maxDispatchesPerSecond"] != float64(7) {
		t.Fatalf("create queue: %d %v", code, q)
	}
	if code, e := do("POST", "/v2/"+parent+"/queues", `{"name":"`+parent+`/queues/rest"}`); code != 409 || e["error"].(map[string]any)["status"] != "ALREADY_EXISTS" {
		t.Fatalf("duplicate: %d %v", code, e)
	}
	body := base64.StdEncoding.EncodeToString([]byte("ping"))
	code, task := do("POST", "/v2/"+parent+"/queues/rest/tasks",
		`{"task":{"httpRequest":{"url":"`+tg.URL+`/rest","httpMethod":"POST","body":"`+body+`"}}}`)
	if code != 200 || !strings.Contains(task["name"].(string), "/queues/rest/tasks/") {
		t.Fatalf("create task: %d %v", code, task)
	}
	if code, l := do("GET", "/v2/"+parent+"/queues/rest/tasks", ""); code != 200 || len(l["tasks"].([]any)) != 1 {
		t.Fatalf("list tasks: %d %v", code, l)
	}
	if code, p := do("POST", "/v2/"+parent+"/queues/rest:pause", ``); code != 200 || p["state"] != "PAUSED" {
		t.Fatalf("pause: %d %v", code, p)
	}
	do("POST", "/v2/"+parent+"/queues/rest:resume", ``)
	svc.Tick(time.Now().Add(time.Second))
	waitFor(t, "REST task delivery", func() bool { return tg.count() == 1 })
	if tg.bodies[0] != "ping" {
		t.Fatalf("body = %q", tg.bodies[0])
	}
	if code, _ := do("PATCH", "/v2/"+parent+"/queues/rest?updateMask=rateLimits.maxDispatchesPerSecond", `{"rateLimits":{"maxDispatchesPerSecond":9}}`); code != 200 {
		t.Fatalf("patch: %d", code)
	}
	if code, g := do("GET", "/v2/"+parent+"/queues/rest", ""); code != 200 || g["rateLimits"].(map[string]any)["maxDispatchesPerSecond"] != float64(9) {
		t.Fatalf("get: %d %v", code, g)
	}
	if code, _ := do("DELETE", "/v2/"+parent+"/queues/rest", ""); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := do("GET", "/v2/"+parent+"/queues/rest", ""); code != 404 {
		t.Fatalf("get after delete: %d", code)
	}
}

func TestPersistence(t *testing.T) {
	svc, c, _ := setup(t)
	q := mkQueue(t, c, "keep", &pb.Queue{})
	task := httpTask("http://x.test/p")
	task.Name = q.Name + "/tasks/saved"
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: task}))
	b := must(svc.Save())
	svc.Reset()
	_, err := c.GetQueue(ctx, &pb.GetQueueRequest{Name: q.Name})
	wantCode(t, err, codes.NotFound)
	if err := svc.Load(bytes.Clone(b)); err != nil {
		t.Fatal(err)
	}
	if g := must(c.GetTask(ctx, &pb.GetTaskRequest{Name: task.Name})); g.GetHttpRequest().Url != "http://x.test/p" {
		t.Fatalf("loaded = %v", g)
	}
	if rows := svc.Resources(); len(rows) != 2 || len(rows[1].Rows) != 1 || svc.Inspect("Tasks", task.Name) == nil {
		t.Fatalf("resources = %v", rows)
	}
}

// A *.run.localhost URL reaches pogi's own port with its Host header, no DNS involved.
func TestRunLocalhostURL(t *testing.T) {
	var svc *tasks.Service
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = tasks.New(h); svc.Manual(); return svc })
	hosts := make(chan string, 1)
	hub.Mount("/probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hosts <- r.Host }))
	c := must(cloudtasks.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	defer c.Close()
	q := mkQueue(t, c, "run", &pb.Queue{})
	must(c.CreateTask(ctx, &pb.CreateTaskRequest{Parent: q.Name, Task: httpTask("http://my-svc.run.localhost:7644/probe")}))
	svc.Tick(time.Now().Add(time.Second))
	select {
	case h := <-hosts:
		if h != "my-svc.run.localhost:7644" {
			t.Fatalf("host = %q", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the task never reached pogi")
	}
}
