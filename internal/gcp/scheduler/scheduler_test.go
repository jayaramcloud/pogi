package scheduler_test

import (
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

	ps "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	scheduler "cloud.google.com/go/scheduler/apiv1"
	pb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/pubsub"
	sched "github.com/nrndev/pogi/internal/gcp/scheduler"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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

// setup starts pogi with Pub/Sub and Cloud Scheduler; the scheduler's clock is the test's (Tick).
func setup(t *testing.T) (*sched.Service, *scheduler.CloudSchedulerClient, *ps.Client, string) {
	t.Helper()
	var svc *sched.Service
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return pubsub.New(h) },
		func(h *core.Hub) core.Service { svc = sched.New(h); svc.Manual(); return svc })
	c := must(scheduler.NewCloudSchedulerClient(ctx, option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { c.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", strings.TrimPrefix(url, "http://"))
	pc := must(ps.NewClient(ctx, "proj"))
	t.Cleanup(func() { pc.Close() })
	return svc, c, pc, url
}

type target struct {
	*httptest.Server
	mu     sync.Mutex
	calls  []*http.Request
	bodies []string
	status func(n int) int
}

func newTarget(t *testing.T, status func(n int) int) *target {
	tg := &target{status: status}
	tg.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		tg.mu.Lock()
		tg.calls, tg.bodies = append(tg.calls, r), append(tg.bodies, string(b))
		n := len(tg.calls)
		tg.mu.Unlock()
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

func mkJob(t *testing.T, c *scheduler.CloudSchedulerClient, id string, j *pb.Job) *pb.Job {
	t.Helper()
	j.Name = parent + "/jobs/" + id
	return must(c.CreateJob(ctx, &pb.CreateJobRequest{Parent: parent, Job: j}))
}

func httpJob(schedule, url string) *pb.Job {
	return &pb.Job{Schedule: schedule, Target: &pb.Job_HttpTarget{HttpTarget: &pb.HttpTarget{Uri: url, Body: []byte(`{"tick":true}`),
		Headers: map[string]string{"Content-Type": "application/json"}}}}
}

func TestJobs(t *testing.T) {
	_, c, _, _ := setup(t)
	j := mkJob(t, c, "nightly", &pb.Job{Schedule: "30 9 * * 1", TimeZone: "Asia/Kolkata",
		Target: &pb.Job_HttpTarget{HttpTarget: &pb.HttpTarget{Uri: "http://example.test/hook"}}})
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if next := j.ScheduleTime.AsTime().In(loc); next.Weekday() != time.Monday || next.Hour() != 9 || next.Minute() != 30 {
		t.Fatalf("scheduleTime = %v", next)
	}
	if j.State != pb.Job_ENABLED || j.UserUpdateTime == nil || j.RetryConfig != nil || j.AttemptDeadline.Seconds != 180 ||
		j.GetHttpTarget().HttpMethod != pb.HttpMethod_POST && j.GetHttpTarget().HttpMethod != pb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		t.Fatalf("defaults = %v", j)
	}
	_, err := c.CreateJob(ctx, &pb.CreateJobRequest{Parent: parent, Job: &pb.Job{Name: j.Name, Schedule: "* * * * *", Target: j.Target}})
	wantCode(t, err, codes.AlreadyExists)
	for name, bad := range map[string]*pb.Job{
		"bad cron":  {Schedule: "61 * * * *", Target: j.Target},
		"bad zone":  {Schedule: "* * * * *", TimeZone: "Mars/Olympus", Target: j.Target},
		"no target": {Schedule: "* * * * *"},
		"bad topic": {Schedule: "* * * * *", Target: &pb.Job_PubsubTarget{PubsubTarget: &pb.PubsubTarget{TopicName: "nope"}}},
		"bad uri":   {Schedule: "* * * * *", Target: &pb.Job_HttpTarget{HttpTarget: &pb.HttpTarget{Uri: "ftp://x"}}},
	} {
		bad.Name = parent + "/jobs/bad"
		if _, err := c.CreateJob(ctx, &pb.CreateJobRequest{Parent: parent, Job: bad}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
	_, err = c.GetJob(ctx, &pb.GetJobRequest{Name: parent + "/jobs/missing"})
	wantCode(t, err, codes.NotFound)

	up := must(c.UpdateJob(ctx, &pb.UpdateJobRequest{Job: &pb.Job{Name: j.Name, Schedule: "0 0 * * *", Description: "ignored"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule"}}}))
	if up.Schedule != "0 0 * * *" || up.Description != "" || up.TimeZone != "Asia/Kolkata" || up.GetHttpTarget() == nil {
		t.Fatalf("update = %v", up)
	}
	mkJob(t, c, "alpha", httpJob("* * * * *", "http://example.test/a"))
	var names []string
	for it := c.ListJobs(ctx, &pb.ListJobsRequest{Parent: parent}); ; {
		x, err := it.Next()
		if err == iterator.Done {
			break
		}
		names = append(names, must(x, err).Name)
	}
	if len(names) != 2 || !strings.HasSuffix(names[0], "alpha") {
		t.Fatalf("list = %v", names)
	}
	must(0, c.DeleteJob(ctx, &pb.DeleteJobRequest{Name: j.Name}))
	_, err = c.GetJob(ctx, &pb.GetJobRequest{Name: j.Name})
	wantCode(t, err, codes.NotFound)
}

func TestFiresHTTPTarget(t *testing.T) {
	svc, c, _, _ := setup(t)
	tg := newTarget(t, func(int) int { return 200 })
	j := httpJob("* * * * *", tg.URL+"/cron")
	j.GetHttpTarget().AuthorizationHeader = &pb.HttpTarget_OidcToken{OidcToken: &pb.OidcToken{ServiceAccountEmail: "sched@proj.iam.gserviceaccount.com", Audience: "https://svc.example"}}
	created := mkJob(t, c, "tick", j)
	due := created.ScheduleTime.AsTime()
	if !due.After(time.Now()) || time.Until(due) > time.Minute {
		t.Fatalf("scheduleTime = %v", due)
	}

	svc.Tick(due.Add(-time.Second)) // not yet
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 0 {
		t.Fatal("fired early")
	}
	svc.Tick(due)
	waitFor(t, "the job to fire", func() bool {
		g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: created.Name}))
		return tg.count() == 1 && g.Status != nil
	})
	g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: created.Name}))
	if g.Status.Code != 0 || g.LastAttemptTime == nil || !g.LastAttemptTime.AsTime().Equal(due) || !g.ScheduleTime.AsTime().Equal(due.Add(time.Minute)) {
		t.Fatalf("after firing: %v", g)
	}
	svc.Tick(due.Add(30 * time.Second)) // the same minute: nothing
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 1 {
		t.Fatalf("fired twice in one minute: %d", tg.count())
	}
	svc.Tick(due.Add(time.Minute))
	waitFor(t, "the second run", func() bool { return tg.count() == 2 })

	r := tg.calls[0]
	if r.Method != http.MethodPost || r.URL.Path != "/cron" || tg.bodies[0] != `{"tick":true}` || r.Header.Get("Content-Type") != "application/json" ||
		r.Header.Get("X-CloudScheduler-JobName") != "tick" || r.Header.Get("X-CloudScheduler") != "true" ||
		r.Header.Get("X-CloudScheduler-ScheduleTime") != due.UTC().Format(time.RFC3339) || r.UserAgent() != "Google-Cloud-Scheduler" {
		t.Fatalf("request = %s %s %q %v", r.Method, r.URL.Path, tg.bodies[0], r.Header)
	}
	p := strings.Split(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), ".")
	sig, _ := base64.RawURLEncoding.DecodeString(p[2])
	sum := sha256.Sum256([]byte(p[0] + "." + p[1]))
	if err := rsa.VerifyPKCS1v15(gapi.PublicKey(), crypto.SHA256, sum[:], sig); err != nil {
		t.Fatalf("token signature: %v", err)
	}
	var claims map[string]any
	raw, _ := base64.RawURLEncoding.DecodeString(p[1])
	json.Unmarshal(raw, &claims)
	if claims["aud"] != "https://svc.example" || claims["email"] != "sched@proj.iam.gserviceaccount.com" {
		t.Fatalf("claims = %v", claims)
	}
}

func TestFiresPubsubTarget(t *testing.T) {
	svc, c, pc, _ := setup(t)
	topic := must(pc.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/proj/topics/ticks"})).Name
	sub := must(pc.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{Name: "projects/proj/subscriptions/ticks-sub", Topic: topic})).Name
	created := mkJob(t, c, "to-topic", &pb.Job{Schedule: "*/5 * * * *", Target: &pb.Job_PubsubTarget{PubsubTarget: &pb.PubsubTarget{
		TopicName: topic, Data: []byte("wake up"), Attributes: map[string]string{"origin": "scheduler"}}}})
	svc.Tick(created.ScheduleTime.AsTime())

	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var got *ps.Message
	err := pc.Subscriber(sub).Receive(rctx, func(_ context.Context, m *ps.Message) {
		got = m
		m.Ack()
		cancel()
	})
	if err != nil || got == nil {
		t.Fatalf("receive: %v, message %v", err, got)
	}
	if string(got.Data) != "wake up" || got.Attributes["origin"] != "scheduler" {
		t.Fatalf("message = %q %v", got.Data, got.Attributes)
	}
	waitFor(t, "the status", func() bool {
		g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: created.Name}))
		return g.LastAttemptTime != nil && g.Status != nil && g.Status.Code == 0
	})

	// A topic that does not exist is a failed run with NOT_FOUND.
	missing := mkJob(t, c, "to-nowhere", &pb.Job{Schedule: "* * * * *", Target: &pb.Job_PubsubTarget{PubsubTarget: &pb.PubsubTarget{
		TopicName: "projects/proj/topics/nowhere", Data: []byte("x")}}})
	g := must(c.RunJob(ctx, &pb.RunJobRequest{Name: missing.Name}))
	if g.Status.GetCode() != int32(codes.NotFound) {
		t.Fatalf("status = %v", g.Status)
	}
}

func TestPauseResume(t *testing.T) {
	svc, c, _, _ := setup(t)
	tg := newTarget(t, func(int) int { return 200 })
	j := mkJob(t, c, "pausable", httpJob("* * * * *", tg.URL))
	p := must(c.PauseJob(ctx, &pb.PauseJobRequest{Name: j.Name}))
	if p.State != pb.Job_PAUSED || p.ScheduleTime != nil {
		t.Fatalf("paused = %v", p)
	}
	svc.Tick(time.Now().Add(10 * time.Minute))
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 0 {
		t.Fatal("a paused job fired")
	}
	r := must(c.ResumeJob(ctx, &pb.ResumeJobRequest{Name: j.Name}))
	if r.State != pb.Job_ENABLED || r.ScheduleTime == nil {
		t.Fatalf("resumed = %v", r)
	}
	svc.Tick(r.ScheduleTime.AsTime())
	waitFor(t, "the resumed job", func() bool { return tg.count() == 1 })
}

func TestRunJobAndAppEngine(t *testing.T) {
	_, c, _, _ := setup(t)
	tg := newTarget(t, func(int) int { return 404 })
	j := mkJob(t, c, "manual", httpJob("0 0 1 1 *", tg.URL))
	r := must(c.RunJob(ctx, &pb.RunJobRequest{Name: j.Name}))
	if tg.count() != 1 || r.Status.GetCode() != int32(codes.NotFound) || r.LastAttemptTime == nil {
		t.Fatalf("run = %v, calls %d", r, tg.count())
	}
	ae := mkJob(t, c, "gae", &pb.Job{Schedule: "* * * * *", Target: &pb.Job_AppEngineHttpTarget{AppEngineHttpTarget: &pb.AppEngineHttpTarget{RelativeUri: "/tasks/x"}}})
	if g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: ae.Name})); g.GetAppEngineHttpTarget().RelativeUri != "/tasks/x" {
		t.Fatalf("app engine target not stored: %v", g)
	}
	if g := must(c.RunJob(ctx, &pb.RunJobRequest{Name: ae.Name})); g.Status.GetCode() != int32(codes.Unimplemented) {
		t.Fatalf("app engine run status = %v", g.Status)
	}
}

func TestRetries(t *testing.T) {
	svc, c, _, _ := setup(t)
	tg := newTarget(t, func(n int) int {
		if n < 3 {
			return 500
		}
		return 200
	})
	j := httpJob("0 0 1 1 *", tg.URL)
	j.RetryConfig = &pb.RetryConfig{RetryCount: 3, MinBackoffDuration: durationpb.New(10 * time.Second), MaxDoublings: 3}
	created := mkJob(t, c, "retrying", j)
	due := created.ScheduleTime.AsTime()
	step := func(at time.Time, calls int) {
		t.Helper()
		svc.Tick(at)
		waitFor(t, "an attempt", func() bool {
			g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: created.Name}))
			return tg.count() == calls && g.Status != nil && g.LastAttemptTime.AsTime().Equal(at)
		})
	}
	step(due, 1)
	svc.Tick(due.Add(9 * time.Second))
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 1 {
		t.Fatal("retried before the 10s backoff")
	}
	step(due.Add(10*time.Second), 2)
	svc.Tick(due.Add(29 * time.Second))
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 2 {
		t.Fatal("retried before the doubled 20s backoff")
	}
	step(due.Add(30*time.Second), 3)
	if g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: created.Name})); g.Status.Code != 0 {
		t.Fatalf("final status = %v", g.Status)
	}
	svc.Tick(due.Add(time.Hour)) // succeeded: no more retries
	time.Sleep(30 * time.Millisecond)
	if tg.count() != 3 {
		t.Fatalf("calls = %d", tg.count())
	}
}

func TestREST(t *testing.T) {
	svc, _, _, url := setup(t)
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
	code, j := do("POST", "/v1/"+parent+"/jobs", `{"name":"`+parent+`/jobs/rest","schedule":"*/10 * * * *","timeZone":"America/New_York",`+
		`"httpTarget":{"uri":"`+tg.URL+`/rest","httpMethod":"PUT","body":"`+base64.StdEncoding.EncodeToString([]byte("x"))+`"}}`)
	if code != 200 || j["state"] != "ENABLED" || j["scheduleTime"] == nil || j["timeZone"] != "America/New_York" {
		t.Fatalf("create: %d %v", code, j)
	}
	if code, e := do("POST", "/v1/"+parent+"/jobs", `{"name":"`+parent+`/jobs/rest","schedule":"* * * * *","httpTarget":{"uri":"http://x.test"}}`); code != 409 || e["error"].(map[string]any)["status"] != "ALREADY_EXISTS" {
		t.Fatalf("duplicate: %d %v", code, e)
	}
	if code, e := do("POST", "/v1/"+parent+"/jobs", `{"name":"`+parent+`/jobs/bad","schedule":"nonsense","httpTarget":{"uri":"http://x.test"}}`); code != 400 || e["error"].(map[string]any)["status"] != "INVALID_ARGUMENT" {
		t.Fatalf("invalid: %d %v", code, e)
	}
	if code, l := do("GET", "/v1/"+parent+"/jobs", ""); code != 200 || len(l["jobs"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, l)
	}
	if code, r := do("POST", "/v1/"+parent+"/jobs/rest:run", ``); code != 200 || r["lastAttemptTime"] == nil {
		t.Fatalf("run: %d %v", code, r)
	}
	if tg.count() != 1 || tg.calls[0].Method != http.MethodPut || tg.bodies[0] != "x" {
		t.Fatalf("target saw %d calls %v", tg.count(), tg.bodies)
	}
	if code, p := do("POST", "/v1/"+parent+"/jobs/rest:pause", ``); code != 200 || p["state"] != "PAUSED" {
		t.Fatalf("pause: %d %v", code, p)
	}
	if code, p := do("PATCH", "/v1/"+parent+"/jobs/rest?updateMask=description", `{"description":"hi"}`); code != 200 || p["description"] != "hi" || p["state"] != "ENABLED" && p["state"] != "PAUSED" {
		t.Fatalf("patch: %d %v", code, p)
	}
	if code, _ := do("DELETE", "/v1/"+parent+"/jobs/rest", ""); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := do("GET", "/v1/"+parent+"/jobs/rest", ""); code != 404 {
		t.Fatalf("get after delete: %d", code)
	}
	_ = svc
}

func TestPersistence(t *testing.T) {
	svc, c, _, _ := setup(t)
	j := mkJob(t, c, "keep", httpJob("0 3 * * *", "http://x.test/k"))
	b := must(svc.Save())
	svc.Reset()
	_, err := c.GetJob(ctx, &pb.GetJobRequest{Name: j.Name})
	wantCode(t, err, codes.NotFound)
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	g := must(c.GetJob(ctx, &pb.GetJobRequest{Name: j.Name}))
	if g.Schedule != "0 3 * * *" || g.ScheduleTime == nil {
		t.Fatalf("loaded = %v", g)
	}
	if rows := svc.Resources(); len(rows) != 1 || len(rows[0].Rows) != 1 || svc.Inspect("Jobs", j.Name) == nil {
		t.Fatalf("resources = %v", rows)
	}
}
