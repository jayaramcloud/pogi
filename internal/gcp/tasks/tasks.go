// Package tasks emulates Google Cloud Tasks (v2) over gRPC and its REST
// mirror: queues (rate limits, retry config, pause, resume, purge) and HTTP
// tasks that a dispatch loop delivers with Google's headers, OIDC and OAuth
// tokens, exponential backoff and tombstoned task names.
package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// tombstone is how long a deleted or executed task name stays taken.
// ponytail: Google holds names for about an hour; not configurable.
const tombstone = time.Hour

// Service is the Cloud Tasks emulator.
type Service struct {
	pb.UnimplementedCloudTasksServer
	hub    *core.Hub
	client *http.Client
	rest   *gapi.REST
	stop   func()
	mu     sync.Mutex
	queues map[string]*queue
}

type queue struct {
	p        *pb.Queue
	tasks    map[string]*task
	tomb     map[string]time.Time // names that cannot be reused, until
	tokens   float64              // rate limit bucket
	last     time.Time
	inflight int
}

type task struct {
	p       *pb.Task
	next    time.Time // when the next attempt is due
	running bool
}

// New creates the Cloud Tasks service and starts its dispatch loop.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, client: gapi.Client(h), queues: map[string]*queue{}}
	q, t := "locations/*/queues", "locations/*/queues/*/tasks"
	s.rest = &gapi.REST{Versions: []string{"v2"}, Collections: []string{"queues"}, Desc: &pb.CloudTasks_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"GET " + q:                {Method: "ListQueues", Field: "parent"},
			"POST " + q:               {Method: "CreateQueue", Field: "parent", Body: "queue"},
			"GET " + q + "/*":         {Method: "GetQueue", Field: "name"},
			"PATCH " + q + "/*":       {Method: "UpdateQueue", Field: "queue.name", Body: "queue"},
			"DELETE " + q + "/*":      {Method: "DeleteQueue", Field: "name"},
			"POST " + q + "/*:purge":  {Method: "PurgeQueue", Field: "name"},
			"POST " + q + "/*:pause":  {Method: "PauseQueue", Field: "name"},
			"POST " + q + "/*:resume": {Method: "ResumeQueue", Field: "name"},
			"GET " + t:                {Method: "ListTasks", Field: "parent"},
			"POST " + t:               {Method: "CreateTask", Field: "parent"},
			"GET " + t + "/*":         {Method: "GetTask", Field: "name"},
			"DELETE " + t + "/*":      {Method: "DeleteTask", Field: "name"},
			"POST " + t + "/*:run":    {Method: "RunTask", Field: "name"},
		}}
	s.stop = core.Every(100*time.Millisecond, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudtasks", Cloud: core.GCP, Title: "Cloud Tasks", Match: s.rest.Match,
		Targets: []string{"google.cloud.tasks.v2.CloudTasks"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterCloudTasksServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

// Close stops the dispatch loop.
func (s *Service) Close() error { s.stop(); return nil }

// Manual stops the background clock so a test can drive Tick itself.
func (s *Service) Manual() { s.stop() }

func (s *Service) Reset() {
	s.mu.Lock()
	s.queues = map[string]*queue{}
	s.mu.Unlock()
}

// ---- persistence ----

type savedQueue struct {
	Queue json.RawMessage      `json:"queue"`
	Tasks []json.RawMessage    `json:"tasks,omitempty"`
	Tomb  map[string]time.Time `json:"tombstones,omitempty"`
}

func pj(m proto.Message) json.RawMessage {
	b, _ := protojson.Marshal(m)
	return b
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []savedQueue{}
	for _, n := range core.SortedKeys(s.queues) {
		q := s.queues[n]
		sq := savedQueue{Queue: pj(q.p), Tomb: q.tomb}
		for _, t := range sortedTasks(q) {
			sq.Tasks = append(sq.Tasks, pj(t.p))
		}
		out = append(out, sq)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in []savedQueue
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	queues := map[string]*queue{}
	for _, sq := range in {
		q := newQueue(&pb.Queue{})
		if err := protojson.Unmarshal(sq.Queue, q.p); err != nil {
			return err
		}
		if sq.Tomb != nil {
			q.tomb = sq.Tomb
		}
		for _, raw := range sq.Tasks {
			t := &task{p: &pb.Task{}}
			if err := protojson.Unmarshal(raw, t.p); err != nil {
				return err
			}
			t.next = t.p.ScheduleTime.AsTime()
			q.tasks[t.p.Name] = t
		}
		queues[q.p.Name] = q
	}
	s.mu.Lock()
	s.queues = queues
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

var (
	locRE   = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
	queueRE = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/queues/[A-Za-z0-9-]{1,100}$`)
	taskRE  = regexp.MustCompile(`^(projects/[^/]+/locations/[^/]+/queues/[A-Za-z0-9-]{1,100})/tasks/[A-Za-z0-9_-]{1,500}$`)
)

const queueFormat = `Queue name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>"`

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

func newQueue(p *pb.Queue) *queue {
	return &queue{p: p, tasks: map[string]*task{}, tomb: map[string]time.Time{}}
}

// defaults fills the rate limits and retry config Cloud Tasks picks when they are unset.
func defaults(p *pb.Queue) {
	if p.RateLimits == nil {
		p.RateLimits = &pb.RateLimits{}
	}
	rl := p.RateLimits
	if rl.MaxDispatchesPerSecond == 0 {
		rl.MaxDispatchesPerSecond = 500
	}
	if rl.MaxBurstSize == 0 {
		rl.MaxBurstSize = int32(min(max(math.Ceil(rl.MaxDispatchesPerSecond), 1), 100))
	}
	if rl.MaxConcurrentDispatches == 0 {
		rl.MaxConcurrentDispatches = 1000
	}
	if p.RetryConfig == nil {
		p.RetryConfig = &pb.RetryConfig{}
	}
	rc := p.RetryConfig
	if rc.MaxAttempts == 0 {
		rc.MaxAttempts = 100
	}
	if rc.MinBackoff == nil {
		rc.MinBackoff = durationpb.New(100 * time.Millisecond)
	}
	if rc.MaxBackoff == nil {
		rc.MaxBackoff = durationpb.New(time.Hour)
	}
	if rc.MaxDoublings == 0 {
		rc.MaxDoublings = 16 // ponytail: proto3 can't tell 0 from unset, so 0 doublings is unreachable
	}
	if p.State == pb.Queue_STATE_UNSPECIFIED {
		p.State = pb.Queue_RUNNING
	}
}

func (s *Service) queue(name string) (*queue, error) {
	if !queueRE.MatchString(name) {
		return nil, gapi.Invalid("%s", queueFormat)
	}
	q := s.queues[name]
	if q == nil {
		return nil, gapi.NotFound("Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.")
	}
	return q, nil
}

func (s *Service) task(name string) (*queue, *task, error) {
	m := taskRE.FindStringSubmatch(name)
	if m == nil {
		return nil, nil, gapi.Invalid(`Task name must be formatted: "projects/<PROJECT_ID>/locations/<LOCATION_ID>/queues/<QUEUE_ID>/tasks/<TASK_ID>"`)
	}
	q, err := s.queue(m[1])
	if err != nil {
		return nil, nil, err
	}
	t := q.tasks[name]
	if t == nil {
		return nil, nil, gapi.NotFound("The task no longer exists, though a task with this name existed recently. The task either successfully completed or was deleted.")
	}
	return q, t, nil
}

func sortedTasks(q *queue) []*task {
	out := make([]*task, 0, len(q.tasks))
	for _, n := range core.SortedKeys(q.tasks) {
		out = append(out, q.tasks[n])
	}
	return out
}

// view copies a task for a response; BASIC leaves out App Engine bodies.
func view(p *pb.Task, v pb.Task_View) *pb.Task {
	out := clone(p)
	if v == pb.Task_VIEW_UNSPECIFIED {
		v = pb.Task_BASIC
	}
	out.View = v
	if v == pb.Task_BASIC && out.GetAppEngineHttpRequest() != nil {
		out.GetAppEngineHttpRequest().Body = nil
	}
	return out
}

// ---- queues ----

func (s *Service) CreateQueue(_ context.Context, req *pb.CreateQueueRequest) (*pb.Queue, error) {
	p := clone(req.GetQueue())
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<PROJECT_ID>/locations/<LOCATION_ID>", req.Parent)
	}
	if !queueRE.MatchString(p.Name) || !strings.HasPrefix(p.Name, req.Parent+"/queues/") {
		return nil, gapi.Invalid("%s", queueFormat)
	}
	p.State, p.PurgeTime = pb.Queue_RUNNING, nil
	defaults(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queues[p.Name] != nil {
		return nil, gapi.Exists("Queue already exists")
	}
	s.queues[p.Name] = newQueue(p)
	return clone(p), nil
}

func (s *Service) GetQueue(_ context.Context, req *pb.GetQueueRequest) (*pb.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(req.Name)
	if err != nil {
		return nil, err
	}
	return clone(q.p), nil
}

func (s *Service) ListQueues(_ context.Context, req *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<PROJECT_ID>/locations/<LOCATION_ID>", req.Parent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*pb.Queue
	for _, n := range core.SortedKeys(s.queues) {
		if strings.HasPrefix(n, req.Parent+"/queues/") {
			all = append(all, clone(s.queues[n].p))
		}
	}
	out := &pb.ListQueuesResponse{}
	out.Queues, out.NextPageToken = gapi.Page(all, req.PageSize, req.PageToken)
	return out, nil
}

// UpdateQueue also creates the queue when it does not exist, as Google does.
func (s *Service) UpdateQueue(_ context.Context, req *pb.UpdateQueueRequest) (*pb.Queue, error) {
	in := req.GetQueue()
	if !queueRE.MatchString(in.GetName()) {
		return nil, gapi.Invalid("%s", queueFormat)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[in.Name]
	if q == nil {
		p := clone(in)
		p.State, p.PurgeTime = pb.Queue_RUNNING, nil
		defaults(p)
		s.queues[p.Name] = newQueue(p)
		return clone(p), nil
	}
	if err := gapi.Update(q.p, in, req.GetUpdateMask().GetPaths(), "app_engine_routing_override", "http_target",
		"rate_limits", "retry_config", "stackdriver_logging_config"); err != nil {
		return nil, err
	}
	defaults(q.p)
	return clone(q.p), nil
}

func (s *Service) DeleteQueue(_ context.Context, req *pb.DeleteQueueRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.queue(req.Name); err != nil {
		return nil, err
	}
	delete(s.queues, req.Name)
	return &emptypb.Empty{}, nil
}

func (s *Service) PurgeQueue(_ context.Context, req *pb.PurgeQueueRequest) (*pb.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(req.Name)
	if err != nil {
		return nil, err
	}
	for n, t := range q.tasks {
		if !t.running {
			delete(q.tasks, n)
		}
	}
	q.p.PurgeTime = timestamppb.Now()
	return clone(q.p), nil
}

func (s *Service) setState(name string, st pb.Queue_State) (*pb.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(name)
	if err != nil {
		return nil, err
	}
	q.p.State = st
	return clone(q.p), nil
}

func (s *Service) PauseQueue(_ context.Context, req *pb.PauseQueueRequest) (*pb.Queue, error) {
	return s.setState(req.Name, pb.Queue_PAUSED)
}

func (s *Service) ResumeQueue(_ context.Context, req *pb.ResumeQueueRequest) (*pb.Queue, error) {
	return s.setState(req.Name, pb.Queue_RUNNING)
}

// ---- tasks ----

func (s *Service) CreateTask(_ context.Context, req *pb.CreateTaskRequest) (*pb.Task, error) {
	if req.GetTask() == nil {
		return nil, gapi.Invalid("Task must be set")
	}
	p := clone(req.Task)
	if hr := p.GetHttpRequest(); hr != nil {
		if err := checkHTTP(hr); err != nil {
			return nil, err
		}
	} else if p.GetAppEngineHttpRequest() == nil {
		return nil, gapi.Invalid("Task.message_type must be set: http_request or app_engine_http_request")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(req.Parent)
	if err != nil {
		return nil, err
	}
	if p.Name == "" {
		for p.Name == "" || q.tasks[p.Name] != nil {
			p.Name = req.Parent + "/tasks/" + strconv.FormatUint(rand.Uint64()>>1, 10)
		}
	}
	if m := taskRE.FindStringSubmatch(p.Name); m == nil || m[1] != req.Parent {
		return nil, gapi.Invalid("Task name must be formatted as %s/tasks/<TASK_ID>", req.Parent)
	}
	if _, taken := q.tomb[p.Name]; taken || q.tasks[p.Name] != nil {
		return nil, gapi.Exists("Requested entity already exists")
	}
	now := time.Now()
	if p.ScheduleTime == nil {
		p.ScheduleTime = timestamppb.New(now)
	}
	if p.DispatchDeadline == nil {
		p.DispatchDeadline = durationpb.New(10 * time.Minute)
	}
	p.CreateTime, p.DispatchCount, p.ResponseCount, p.FirstAttempt, p.LastAttempt = timestamppb.New(now), 0, 0, nil, nil
	q.tasks[p.Name] = &task{p: p, next: p.ScheduleTime.AsTime()}
	return view(p, req.ResponseView), nil
}

func checkHTTP(hr *pb.HttpRequest) error {
	u, err := url.Parse(hr.Url)
	if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
		return gapi.Invalid("Invalid url %q: want an absolute http or https URL", hr.Url)
	}
	if o := hr.GetOidcToken(); o != nil && o.ServiceAccountEmail == "" {
		return gapi.Invalid("HttpRequest.oidc_token.service_account_email must be set")
	}
	if o := hr.GetOauthToken(); o != nil && o.ServiceAccountEmail == "" {
		return gapi.Invalid("HttpRequest.oauth_token.service_account_email must be set")
	}
	return nil
}

func (s *Service) GetTask(_ context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, t, err := s.task(req.Name)
	if err != nil {
		return nil, err
	}
	return view(t.p, req.ResponseView), nil
}

func (s *Service) ListTasks(_ context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(req.Parent)
	if err != nil {
		return nil, err
	}
	var all []*pb.Task
	for _, t := range sortedTasks(q) {
		all = append(all, view(t.p, req.ResponseView))
	}
	out := &pb.ListTasksResponse{}
	out.Tasks, out.NextPageToken = gapi.Page(all, req.PageSize, req.PageToken)
	return out, nil
}

func (s *Service) DeleteTask(_ context.Context, req *pb.DeleteTaskRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, _, err := s.task(req.Name)
	if err != nil {
		return nil, err
	}
	delete(q.tasks, req.Name)
	q.tomb[req.Name] = time.Now().Add(tombstone)
	return &emptypb.Empty{}, nil
}

// RunTask dispatches the task now, ignoring rate limits and a paused queue,
// and returns it once the handler has answered.
// ponytail: Google returns before the handler answers; this waits.
func (s *Service) RunTask(_ context.Context, req *pb.RunTaskRequest) (*pb.Task, error) {
	s.mu.Lock()
	q, t, err := s.task(req.Name)
	if err == nil && t.running {
		err = gapi.Invalid("Task is already running")
	}
	if err == nil && t.p.GetHttpRequest() == nil {
		err = gapi.Invalid("pogi dispatches HTTP tasks only")
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	run := s.begin(q, t, time.Now())
	s.mu.Unlock()
	return view(run(), req.ResponseView), nil
}

// ---- dispatch ----

// Tick dispatches the tasks that are due at now, within each queue's rate
// limits and concurrency. A background clock calls it; tests call it with a
// time of their own after Manual.
func (s *Service) Tick(now time.Time) {
	var runs []func() *pb.Task
	s.mu.Lock()
	for _, n := range core.SortedKeys(s.queues) {
		q := s.queues[n]
		for name, until := range q.tomb {
			if now.After(until) {
				delete(q.tomb, name)
			}
		}
		if q.p.State != pb.Queue_RUNNING {
			continue
		}
		rl := q.p.RateLimits
		if q.last.IsZero() {
			q.last, q.tokens = now, float64(rl.MaxBurstSize)
		} else if now.After(q.last) {
			q.tokens = min(float64(rl.MaxBurstSize), q.tokens+rl.MaxDispatchesPerSecond*now.Sub(q.last).Seconds())
			q.last = now
		}
		var due []*task
		for _, t := range q.tasks {
			if !t.running && !t.next.After(now) && t.p.GetHttpRequest() != nil {
				due = append(due, t)
			}
		}
		slices.SortFunc(due, func(a, b *task) int {
			if c := a.next.Compare(b.next); c != 0 {
				return c
			}
			return strings.Compare(a.p.Name, b.p.Name)
		})
		for _, t := range due {
			if q.inflight >= int(rl.MaxConcurrentDispatches) || q.tokens < 1 {
				break
			}
			q.tokens--
			runs = append(runs, s.begin(q, t, now))
		}
	}
	s.mu.Unlock()
	for _, run := range runs {
		core.Go("tasks.run", func() { run() })
	}
}

// begin books an attempt of t at now (lock held) and returns the function
// that makes the HTTP call and records its result (lock not held).
func (s *Service) begin(q *queue, t *task, now time.Time) func() *pb.Task {
	p := t.p
	t.running = true
	q.inflight++
	hr := clone(p.GetHttpRequest())
	h := http.Header{"User-Agent": {"Google-Cloud-Tasks"}}
	for k, v := range hr.Headers {
		h.Set(k, v)
	}
	if len(hr.Body) > 0 && h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/octet-stream")
	}
	short := func(n string) string { return n[strings.LastIndex(n, "/")+1:] }
	h.Set("X-CloudTasks-QueueName", short(q.p.Name))
	h.Set("X-CloudTasks-TaskName", short(p.Name))
	h.Set("X-CloudTasks-TaskRetryCount", strconv.Itoa(int(p.DispatchCount)))
	h.Set("X-CloudTasks-TaskExecutionCount", strconv.Itoa(int(p.ResponseCount)))
	h.Set("X-CloudTasks-TaskETA", strconv.FormatFloat(float64(p.ScheduleTime.AsTime().UnixMicro())/1e6, 'f', 6, 64))
	if p.DispatchCount > 0 {
		last := p.LastAttempt.GetResponseStatus()
		h.Set("X-CloudTasks-TaskPreviousResponse", strconv.Itoa(int(last.GetCode())))
		h.Set("X-CloudTasks-TaskRetryReason", last.GetMessage())
	}
	if o := hr.GetOidcToken(); o != nil {
		h.Set("Authorization", "Bearer "+gapi.OIDCToken(o.ServiceAccountEmail, or(o.Audience, hr.Url)))
	} else if o := hr.GetOauthToken(); o != nil {
		h.Set("Authorization", "Bearer "+gapi.OAuthToken())
	}
	p.DispatchCount++
	p.LastAttempt = &pb.Attempt{ScheduleTime: p.ScheduleTime, DispatchTime: timestamppb.New(now)}
	if p.DispatchCount == 1 {
		p.FirstAttempt = clone(p.LastAttempt)
	}
	timeout := p.DispatchDeadline.AsDuration()
	return func() *pb.Task {
		start := time.Now()
		code, err := s.send(hr, h, timeout)
		return s.finish(q, t, now, now.Add(time.Since(start)), code, err)
	}
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// send makes one HTTP call; code is 0 when there was no response.
func (s *Service) send(hr *pb.HttpRequest, h http.Header, timeout time.Duration) (code int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	method := hr.HttpMethod.String()
	if hr.HttpMethod == pb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, hr.Url, bytes.NewReader(hr.Body))
	if err != nil {
		return 0, err
	}
	req.Header = h
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// finish records an attempt's result: a 2xx deletes the task, anything else
// schedules a retry unless the retry config is used up. It returns the task
// as the attempt left it.
func (s *Service) finish(q *queue, t *task, dispatched, responded time.Time, code int, err error) *pb.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := t.p
	t.running = false
	q.inflight--
	if code != 0 {
		p.ResponseCount++
	}
	p.LastAttempt.ResponseTime, p.LastAttempt.ResponseStatus = timestamppb.New(responded), gapi.Status(code, err)
	if p.DispatchCount == 1 {
		p.FirstAttempt = clone(p.LastAttempt)
	}
	snap := view(p, pb.Task_FULL)
	if s.queues[q.p.Name] != q || q.tasks[p.Name] != t {
		return snap // deleted or purged while running
	}
	rc := q.p.RetryConfig
	n := int(p.DispatchCount)
	wait := backoff(rc, n)
	if code/100 == 2 || rc.MaxAttempts > 0 && n >= int(rc.MaxAttempts) ||
		rc.MaxRetryDuration.AsDuration() > 0 && responded.Add(wait).Sub(p.FirstAttempt.DispatchTime.AsTime()) > rc.MaxRetryDuration.AsDuration() {
		delete(q.tasks, p.Name)
		q.tomb[p.Name] = responded.Add(tombstone)
		return snap
	}
	t.next = dispatched.Add(wait)
	p.ScheduleTime = timestamppb.New(t.next)
	return snap
}

// backoff is the wait after n failed attempts: min_backoff doubled
// max_doublings times, then growing linearly by that step, capped at max_backoff.
func backoff(rc *pb.RetryConfig, n int) time.Duration {
	lo, hi, d := rc.MinBackoff.AsDuration(), rc.MaxBackoff.AsDuration(), int(rc.MaxDoublings)
	w := float64(lo)
	if n-1 <= d {
		w *= math.Pow(2, float64(n-1))
	} else {
		w *= math.Pow(2, float64(d)) * float64(n-d)
	}
	return time.Duration(min(w, float64(hi)))
}

// ---- console ----

// Resources lists queues and tasks.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	qt := core.Table{Name: "Queues", Columns: []string{"Name", "State", "Tasks", "Max dispatches/s", "Max concurrent"}}
	tt := core.Table{Name: "Tasks", Columns: []string{"Name", "URL", "Method", "Dispatches", "Responses", "Scheduled"}}
	for _, n := range core.SortedKeys(s.queues) {
		q := s.queues[n]
		qt.Rows = append(qt.Rows, []string{n, q.p.State.String(), strconv.Itoa(len(q.tasks)),
			strconv.FormatFloat(q.p.RateLimits.MaxDispatchesPerSecond, 'f', -1, 64), strconv.Itoa(int(q.p.RateLimits.MaxConcurrentDispatches))})
		for _, t := range sortedTasks(q) {
			hr := t.p.GetHttpRequest()
			tt.Rows = append(tt.Rows, []string{t.p.Name, hr.GetUrl(), hr.GetHttpMethod().String(), strconv.Itoa(int(t.p.DispatchCount)),
				strconv.Itoa(int(t.p.ResponseCount)), t.p.ScheduleTime.AsTime().Format(time.DateTime)})
		}
	}
	return []core.Table{qt, tt}
}

// Inspect returns a queue or a task as the API shows it.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Queues":
		if q := s.queues[id]; q != nil {
			return pj(q.p)
		}
	case "Tasks":
		if _, t, err := s.task(id); err == nil {
			return pj(view(t.p, pb.Task_FULL))
		}
	}
	return nil
}
