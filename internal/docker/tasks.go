package docker

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Task is background work for the console: an image download, or a container
// start that has not yet answered. Done and Total are bytes (Total 0: not known).
type Task struct {
	ID       string    `json:"id"`
	Service  string    `json:"service"`
	Resource string    `json:"resource"`
	Kind     string    `json:"kind"` // download or start
	What     string    `json:"what"` // the image
	Done     int64     `json:"done"`
	Total    int64     `json:"total"`
	State    string    `json:"state"` // running, done or failed
	Error    string    `json:"error"` // a plain sentence; Docker's own text may follow " — "
	Since    time.Time `json:"since"`

	ended time.Time
}

const maxTasks = 200 // kept in memory; the oldest finished one goes first

// Finished is when a done or failed task ended.
func (t Task) Finished() time.Time { return t.ended }

// Tasks is the list of a Client's tasks.
type Tasks struct {
	mu   sync.Mutex
	list []*Task
	seq  int
	now  func() time.Time
}

// Tracker updates one task.
type Tracker struct {
	ts *Tasks
	t  *Task
}

// Tasks returns the tasks of this client.
func (c *Client) Tasks() *Tasks { return &c.tasks }

// Track starts a task: kind is "download" or "start", what the image.
func (c *Client) Track(service, resource, kind, what string) *Tracker {
	return c.tasks.begin(service, resource, kind, what)
}

func (ts *Tasks) clock() time.Time {
	if ts.now != nil {
		return ts.now()
	}
	return time.Now()
}

func (ts *Tasks) begin(service, resource, kind, what string) *Tracker {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.seq++
	t := &Task{ID: "t-" + strconv.Itoa(ts.seq), Service: service, Resource: resource, Kind: kind, What: what, State: "running", Since: ts.clock().UTC()}
	if len(ts.list) >= maxTasks {
		i := 0
		for j, o := range ts.list {
			if o.State != "running" {
				i = j
				break
			}
		}
		ts.list = append(ts.list[:i], ts.list[i+1:]...) // none finished: the oldest
	}
	ts.list = append(ts.list, t)
	return &Tracker{ts, t}
}

// Progress records the bytes done of total (0: not known).
func (tr *Tracker) Progress(done, total int64) {
	tr.ts.mu.Lock()
	defer tr.ts.mu.Unlock()
	if tr.t.State == "running" {
		tr.t.Done, tr.t.Total = done, total
	}
}

// End finishes the task; err (when not nil) is shown as a plain sentence. Only the first call counts.
func (tr *Tracker) End(err error) {
	tr.ts.mu.Lock()
	defer tr.ts.mu.Unlock()
	if tr.t.State != "running" {
		return
	}
	tr.t.ended = tr.ts.clock()
	if err != nil {
		tr.t.State, tr.t.Error = "failed", Plain(err)
		return
	}
	tr.t.State = "done"
	if tr.t.Total > 0 {
		tr.t.Done = tr.t.Total
	}
}

// Recent lists the running tasks and those finished within the last maxAge, oldest first.
// The console's task list uses a minute; the bell looks back further.
func (ts *Tasks) Recent(maxAge time.Duration) []Task {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	now, out := ts.clock(), []Task{}
	for _, t := range ts.list {
		if t.State == "running" || now.Sub(t.ended) <= maxAge {
			out = append(out, *t)
		}
	}
	return out
}

// List is what GET /_pogi/tasks shows: running tasks, and finished ones for 60 seconds.
func (ts *Tasks) List() []Task { return ts.Recent(time.Minute) }

// Limits of a download: it fails when no bytes arrive for stallLimit, or takes longer than pullLimit in all.
var (
	stallLimit = 2 * time.Minute
	pullLimit  = 30 * time.Minute

	errStalled = errors.New("the download stalled: no data for 2 minutes")
	errTooLong = errors.New("the download took longer than 30 minutes")
)

// stallReader restarts a timer whenever bytes arrive.
type stallReader struct {
	io.ReadCloser
	t *time.Timer
}

func (s *stallReader) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if n > 0 {
		s.t.Reset(stallLimit)
	}
	return n, err
}

// pullError is a failed pull of an image.
type pullError struct {
	image string
	err   error
}

func (e *pullError) Error() string { return "pull " + e.image + ": " + e.err.Error() }
func (e *pullError) Unwrap() error { return e.err }

// layers adds up the progress of an image's layers.
type layers map[string][2]int64 // id → done, total

func (l layers) add(id string, done, total int64) (sumDone, sumTotal int64) {
	l[id] = [2]int64{done, total}
	for _, v := range l {
		sumDone, sumTotal = sumDone+v[0], sumTotal+v[1]
	}
	if sumTotal < sumDone { // some layer has no size yet: the total is not known
		sumTotal = 0
	}
	return
}

// Plain turns an engine failure into a plain sentence, with Docker's own text after " — ".
// The sentences: Docker isn't running, the image wasn't found, the registry can't be reached,
// the disk is full, and the two download limits.
func Plain(err error) string {
	if err == nil {
		return ""
	}
	raw := err.Error()
	switch {
	case errors.Is(err, errNoEngine):
		return "Docker isn't running"
	case errors.Is(err, errStalled):
		return errStalled.Error()
	}
	var ee *engineError
	if errors.As(err, &ee) {
		return ee.plain
	}
	switch {
	case errors.Is(err, errTooLong):
		return errTooLong.Error()
	}
	var pe *pullError
	image := ""
	if errors.As(err, &pe) {
		image, raw = pe.image, pe.err.Error()
	}
	raw = strings.TrimPrefix(raw, "docker: ")
	m := strings.ToLower(raw)
	has := func(s ...string) bool {
		for _, x := range s {
			if strings.Contains(m, x) {
				return true
			}
		}
		return false
	}
	switch {
	case has("no space left on device", "disk quota", "not enough space"):
		return "The disk is full — " + raw
	case has("manifest unknown", "no such image", "pull access denied", "repository does not exist") || pe != nil && has("not found"):
		if image != "" {
			return "image not found: " + image + " — " + raw
		}
		return "image not found — " + raw
	case pe != nil && has("no such host", "connection refused", "i/o timeout", "dial tcp", "tls handshake", "connection reset",
		"network is unreachable", "client.timeout", "temporary failure in name resolution", "eof", "context deadline exceeded"):
		return "The registry can't be reached — " + raw
	case errors.Is(err, context.Canceled):
		return "stopped"
	}
	return raw
}
