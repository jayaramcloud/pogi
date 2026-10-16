// Package scheduler emulates Google Cloud Scheduler (v1) over gRPC and its
// REST mirror: unix-cron jobs with time zones that fire at HTTP targets (with
// OIDC or OAuth tokens) and Pub/Sub topics, with retries, pause, resume and
// run. App Engine targets are stored and never run.
package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Cloud Scheduler emulator.
type Service struct {
	pb.UnimplementedCloudSchedulerServer
	hub    *core.Hub
	client *http.Client
	rest   *gapi.REST
	stop   func()
	mu     sync.Mutex
	jobs   map[string]*job
}

type job struct {
	p        *pb.Job
	cron     *cron
	next     time.Time // next scheduled run; zero while paused
	retryAt  time.Time // next retry of a failed run, if any
	attempts int       // attempts so far in the current run
	firstAt  time.Time // when the current run first tried
}

// New creates the Cloud Scheduler service and starts its clock.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, client: gapi.Client(h), jobs: map[string]*job{}}
	j := "locations/*/jobs"
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"jobs"}, Desc: &pb.CloudScheduler_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"GET " + j:                {Method: "ListJobs", Field: "parent"},
			"POST " + j:               {Method: "CreateJob", Field: "parent", Body: "job"},
			"GET " + j + "/*":         {Method: "GetJob", Field: "name"},
			"PATCH " + j + "/*":       {Method: "UpdateJob", Field: "job.name", Body: "job"},
			"DELETE " + j + "/*":      {Method: "DeleteJob", Field: "name"},
			"POST " + j + "/*:pause":  {Method: "PauseJob", Field: "name"},
			"POST " + j + "/*:resume": {Method: "ResumeJob", Field: "name"},
			"POST " + j + "/*:run":    {Method: "RunJob", Field: "name"},
		}}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudscheduler", Cloud: core.GCP, Title: "Cloud Scheduler", Match: s.rest.Match,
		Targets: []string{"google.cloud.scheduler.v1.CloudScheduler"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterCloudSchedulerServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

// Close stops the clock.
func (s *Service) Close() error { s.stop(); return nil }

// Manual stops the background clock so a test can drive Tick itself.
func (s *Service) Manual() { s.stop() }

func (s *Service) Reset() {
	s.mu.Lock()
	s.jobs = map[string]*job{}
	s.mu.Unlock()
}

// ---- persistence ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []json.RawMessage{}
	for _, n := range core.SortedKeys(s.jobs) {
		b, _ := protojson.Marshal(s.jobs[n].p)
		out = append(out, b)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in []json.RawMessage
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	jobs := map[string]*job{}
	for _, raw := range in {
		p := &pb.Job{}
		if err := protojson.Unmarshal(raw, p); err != nil {
			return err
		}
		j, err := newJob(p, time.Now())
		if err != nil {
			return err
		}
		jobs[p.Name] = j
	}
	s.mu.Lock()
	s.jobs = jobs
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

var (
	locRE   = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
	jobRE   = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/jobs/[A-Za-z0-9_-]{1,500}$`)
	topicRE = regexp.MustCompile(`^projects/[^/]+/topics/[^/]+$`)
)

const jobFormat = `Job name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/jobs/<JOB_ID>"`

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

// newJob validates p, fills the defaults Scheduler picks and schedules it
// when it is enabled.
func newJob(p *pb.Job, now time.Time) (*job, error) {
	if p.Schedule == "" {
		return nil, gapi.Invalid("Job.schedule must be set")
	}
	if p.TimeZone == "" {
		p.TimeZone = "Etc/UTC"
	}
	loc, err := time.LoadLocation(p.TimeZone)
	if err != nil {
		return nil, gapi.Invalid("Invalid time zone %q", p.TimeZone)
	}
	c, err := parseCron(p.Schedule, loc)
	if err != nil {
		return nil, gapi.Invalid("Invalid schedule %q: %v", p.Schedule, err)
	}
	switch t := p.Target.(type) {
	case *pb.Job_HttpTarget:
		u, err := url.Parse(t.HttpTarget.GetUri())
		if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
			return nil, gapi.Invalid("Invalid http_target.uri %q: want an absolute http or https URL", t.HttpTarget.GetUri())
		}
		if o := t.HttpTarget.GetOidcToken(); o != nil && o.ServiceAccountEmail == "" {
			return nil, gapi.Invalid("http_target.oidc_token.service_account_email must be set")
		}
		if o := t.HttpTarget.GetOauthToken(); o != nil && o.ServiceAccountEmail == "" {
			return nil, gapi.Invalid("http_target.oauth_token.service_account_email must be set")
		}
	case *pb.Job_PubsubTarget:
		if !topicRE.MatchString(t.PubsubTarget.GetTopicName()) {
			return nil, gapi.Invalid("pubsub_target.topic_name must be formatted: projects/<PROJECT_ID>/topics/<TOPIC_ID>")
		}
	case *pb.Job_AppEngineHttpTarget:
	default:
		return nil, gapi.Invalid("Job must have a target: http_target, pubsub_target or app_engine_http_target")
	}
	if p.AttemptDeadline == nil {
		p.AttemptDeadline = durationpb.New(3 * time.Minute)
	}
	if p.State == pb.Job_STATE_UNSPECIFIED {
		p.State = pb.Job_ENABLED
	}
	j := &job{p: p, cron: c}
	j.schedule(now)
	return j, nil
}

// schedule sets the next run (none while paused).
func (j *job) schedule(now time.Time) {
	j.next, j.retryAt, j.attempts = time.Time{}, time.Time{}, 0
	j.p.ScheduleTime = nil
	if j.p.State == pb.Job_ENABLED {
		j.next = j.cron.next(now)
		j.p.ScheduleTime = timestamppb.New(j.next)
	}
}

func (s *Service) job(name string) (*job, error) {
	if !jobRE.MatchString(name) {
		return nil, gapi.Invalid("%s", jobFormat)
	}
	j := s.jobs[name]
	if j == nil {
		return nil, gapi.NotFound("Job not found.")
	}
	return j, nil
}

// ---- jobs ----

func (s *Service) CreateJob(_ context.Context, req *pb.CreateJobRequest) (*pb.Job, error) {
	p := clone(req.GetJob())
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<PROJECT_ID>/locations/<LOCATION_ID>", req.Parent)
	}
	if p.Name == "" {
		return nil, gapi.Invalid("Job.name must be set")
	}
	if !jobRE.MatchString(p.Name) || !strings.HasPrefix(p.Name, req.Parent+"/jobs/") {
		return nil, gapi.Invalid("%s", jobFormat)
	}
	now := time.Now()
	p.State, p.Status, p.LastAttemptTime, p.UserUpdateTime = pb.Job_ENABLED, nil, nil, timestamppb.New(now)
	j, err := newJob(p, now)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[p.Name] != nil {
		return nil, gapi.Exists("Job already exists.")
	}
	s.jobs[p.Name] = j
	return clone(p), nil
}

func (s *Service) GetJob(_ context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.job(req.Name)
	if err != nil {
		return nil, err
	}
	return clone(j.p), nil
}

func (s *Service) ListJobs(_ context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<PROJECT_ID>/locations/<LOCATION_ID>", req.Parent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*pb.Job
	for _, n := range core.SortedKeys(s.jobs) {
		if strings.HasPrefix(n, req.Parent+"/jobs/") {
			all = append(all, clone(s.jobs[n].p))
		}
	}
	out := &pb.ListJobsResponse{}
	out.Jobs, out.NextPageToken = gapi.Page(all, req.PageSize, req.PageToken)
	return out, nil
}

var updatable = []string{"description", "schedule", "time_zone", "retry_config", "attempt_deadline", "http_target",
	"pubsub_target", "app_engine_http_target"}

func (s *Service) UpdateJob(_ context.Context, req *pb.UpdateJobRequest) (*pb.Job, error) {
	in := req.GetJob()
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, err := s.job(in.GetName())
	if err != nil {
		return nil, err
	}
	p := clone(cur.p)
	if err := gapi.Update(p, in, req.GetUpdateMask().GetPaths(), updatable...); err != nil {
		return nil, err
	}
	if cur.p.GetTarget() != nil && p.GetTarget() == nil {
		return nil, gapi.Invalid("Job must have a target")
	}
	p.UserUpdateTime = timestamppb.Now()
	j, err := newJob(p, time.Now())
	if err != nil {
		return nil, err
	}
	s.jobs[p.Name] = j
	return clone(p), nil
}

func (s *Service) DeleteJob(_ context.Context, req *pb.DeleteJobRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.job(req.Name); err != nil {
		return nil, err
	}
	delete(s.jobs, req.Name)
	return &emptypb.Empty{}, nil
}

func (s *Service) setState(name string, st pb.Job_State) (*pb.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.job(name)
	if err != nil {
		return nil, err
	}
	if j.p.State != st {
		j.p.State = st
		j.schedule(time.Now())
	}
	return clone(j.p), nil
}

func (s *Service) PauseJob(_ context.Context, req *pb.PauseJobRequest) (*pb.Job, error) {
	return s.setState(req.Name, pb.Job_PAUSED)
}

func (s *Service) ResumeJob(_ context.Context, req *pb.ResumeJobRequest) (*pb.Job, error) {
	return s.setState(req.Name, pb.Job_ENABLED)
}

// RunJob runs the job now and returns it once the target has answered.
// ponytail: Google returns before the target answers; this waits.
func (s *Service) RunJob(_ context.Context, req *pb.RunJobRequest) (*pb.Job, error) {
	s.mu.Lock()
	j, err := s.job(req.Name)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	now := time.Now()
	j.attempts, j.retryAt = 0, time.Time{}
	run := s.begin(j, now, now)
	s.mu.Unlock()
	run()
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(j.p), nil
}

// ---- firing ----

// Tick fires the jobs that are due at now. A background clock calls it every
// second; tests call it with a time of their own after Manual.
func (s *Service) Tick(now time.Time) {
	var runs []func()
	s.mu.Lock()
	for _, n := range core.SortedKeys(s.jobs) {
		j := s.jobs[n]
		if j.p.State != pb.Job_ENABLED {
			continue
		}
		switch {
		case !j.next.IsZero() && !j.next.After(now):
			at := j.next
			j.next = j.cron.next(now) // runs missed while pogi was busy or stopped are skipped
			j.p.ScheduleTime = timestamppb.New(j.next)
			j.attempts, j.retryAt = 0, time.Time{}
			runs = append(runs, s.begin(j, now, at))
		case !j.retryAt.IsZero() && !j.retryAt.After(now):
			j.retryAt = time.Time{}
			runs = append(runs, s.begin(j, now, now))
		}
	}
	s.mu.Unlock()
	for _, run := range runs {
		core.Go("scheduler.run", func() { run() })
	}
}

// begin books an attempt of j at now for the run scheduled at at (lock held)
// and returns the function that makes the call and records the result.
func (s *Service) begin(j *job, now, at time.Time) func() {
	p := clone(j.p)
	j.attempts++
	if j.attempts == 1 {
		j.firstAt = now
	}
	j.p.LastAttemptTime = timestamppb.New(now)
	return func() {
		st := s.deliver(p, at)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.jobs[p.Name] != j {
			return // deleted or replaced
		}
		j.p.Status = st
		rc := retryConfig(j.p.RetryConfig)
		wait := backoff(rc, j.attempts)
		if st.Code != 0 && j.attempts <= int(rc.RetryCount) &&
			(rc.MaxRetryDuration.AsDuration() <= 0 || now.Add(wait).Sub(j.firstAt) <= rc.MaxRetryDuration.AsDuration()) {
			j.retryAt = now.Add(wait)
		}
	}
}

// retryConfig fills in the defaults of a job's retry settings. They are applied when a run fails and
// never stored: Cloud Scheduler leaves retryConfig out of a job that does not set it, and the
// Terraform provider would plan to remove one it finds there.
func retryConfig(rc *pb.RetryConfig) *pb.RetryConfig {
	if rc == nil {
		rc = &pb.RetryConfig{}
	}
	rc = clone(rc)
	if rc.MinBackoffDuration == nil {
		rc.MinBackoffDuration = durationpb.New(5 * time.Second)
	}
	if rc.MaxBackoffDuration == nil {
		rc.MaxBackoffDuration = durationpb.New(time.Hour)
	}
	if rc.MaxDoublings == 0 {
		rc.MaxDoublings = 16 // ponytail: proto3 can't tell 0 from unset, so 0 doublings is unreachable
	}
	return rc
}

// backoff is the wait after n failed attempts: min_backoff doubled
// max_doublings times, then growing linearly by that step, capped at max_backoff.
func backoff(rc *pb.RetryConfig, n int) time.Duration {
	lo, hi, d := rc.MinBackoffDuration.AsDuration(), rc.MaxBackoffDuration.AsDuration(), int(rc.MaxDoublings)
	w := float64(lo)
	if n-1 <= d {
		w *= math.Pow(2, float64(n-1))
	} else {
		w *= math.Pow(2, float64(d)) * float64(n-d)
	}
	return time.Duration(min(w, float64(hi)))
}

// deliver sends the job's payload to its target and reports the outcome as a google.rpc.Status.
func (s *Service) deliver(p *pb.Job, scheduled time.Time) *spb.Status {
	switch t := p.Target.(type) {
	case *pb.Job_HttpTarget:
		code, err := s.send(p, t.HttpTarget, scheduled)
		return gapi.Status(code, err)
	case *pb.Job_PubsubTarget:
		attrs := map[string]core.MessageAttribute{}
		for k, v := range t.PubsubTarget.Attributes {
			attrs[k] = core.MessageAttribute{DataType: "String", StringValue: v}
		}
		err := s.hub.Deliver("//pubsub.googleapis.com/"+t.PubsubTarget.TopicName,
			&core.Message{Body: string(t.PubsubTarget.Data), Attributes: attrs})
		if err != nil {
			return status.Convert(err).Proto()
		}
		return &spb.Status{}
	}
	return &spb.Status{Code: int32(codes.Unimplemented), Message: "pogi does not run App Engine targets"}
}

func (s *Service) send(p *pb.Job, t *pb.HttpTarget, scheduled time.Time) (int, error) {
	timeout := p.AttemptDeadline.AsDuration()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	method := t.HttpMethod.String()
	if t.HttpMethod == pb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, t.Uri, bytes.NewReader(t.Body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Google-Cloud-Scheduler")
	for k, v := range t.Headers {
		req.Header.Set(k, v)
	}
	if len(t.Body) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	req.Header.Set("X-CloudScheduler", "true")
	req.Header.Set("X-CloudScheduler-JobName", p.Name[strings.LastIndex(p.Name, "/")+1:])
	req.Header.Set("X-CloudScheduler-ScheduleTime", scheduled.UTC().Format(time.RFC3339))
	if o := t.GetOidcToken(); o != nil {
		aud := o.Audience
		if aud == "" {
			aud = t.Uri
		}
		req.Header.Set("Authorization", "Bearer "+gapi.OIDCToken(o.ServiceAccountEmail, aud))
	} else if t.GetOauthToken() != nil {
		req.Header.Set("Authorization", "Bearer "+gapi.OAuthToken())
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// ---- console ----

func targetOf(p *pb.Job) string {
	switch t := p.Target.(type) {
	case *pb.Job_HttpTarget:
		return t.HttpTarget.HttpMethod.String() + " " + t.HttpTarget.Uri
	case *pb.Job_PubsubTarget:
		return t.PubsubTarget.TopicName
	}
	return "App Engine"
}

// Resources lists jobs.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Jobs", Columns: []string{"Name", "State", "Schedule", "Time zone", "Target", "Next run", "Last attempt"}}
	for _, n := range core.SortedKeys(s.jobs) {
		p := s.jobs[n].p
		next, last := "-", "-"
		if p.ScheduleTime != nil {
			next = p.ScheduleTime.AsTime().Format(time.DateTime)
		}
		if p.LastAttemptTime != nil {
			last = p.LastAttemptTime.AsTime().Format(time.DateTime) + " (" + strconv.Itoa(int(p.Status.GetCode())) + ")"
		}
		t.Rows = append(t.Rows, []string{n, p.State.String(), p.Schedule, p.TimeZone, targetOf(p), next, last})
	}
	return []core.Table{t}
}

// Inspect returns a job as the API shows it.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j := s.jobs[id]; table == "Jobs" && j != nil {
		b, _ := protojson.Marshal(j.p)
		return json.RawMessage(b)
	}
	return nil
}
