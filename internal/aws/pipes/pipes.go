// Package pipes emulates Amazon EventBridge Pipes (REST-JSON): pipes poll
// SQS, Kinesis and DynamoDB Streams, filter with event patterns, enrich
// through Lambda, Step Functions, API Gateway or API destinations, and
// deliver to the targets EventBridge rules reach.
package pipes

import (
	"bytes"
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the EventBridge Pipes emulator.
type Service struct {
	stop func()
	hub  *core.Hub
	mux  *http.ServeMux
	mu   sync.Mutex
	st   state
	curs map[string]*cursor // pipe key → its place in the source
}

type state struct {
	Pipes map[string]*Pipe `json:"pipes"` // account/region/name
}

// Pipe is stored in the DescribePipe shape. The parameter objects are kept
// as sent, so every member round-trips, including those pogi does not act on.
type Pipe struct {
	Arn                            string
	Name                           string
	Description                    string `json:",omitempty"`
	DesiredState                   string // RUNNING, STOPPED or DELETED
	CurrentState                   string
	StateReason                    string `json:",omitempty"`
	RoleArn                        string
	Source                         string
	SourceParameters               params `json:",omitempty"`
	Enrichment                     string `json:",omitempty"`
	EnrichmentParameters           params `json:",omitempty"`
	Target                         string
	TargetParameters               params            `json:",omitempty"`
	LogConfiguration               json.RawMessage   `json:",omitempty"`
	KmsKeyIdentifier               string            `json:",omitempty"`
	Tags                           map[string]string `json:",omitempty"`
	CreationTime, LastModifiedTime float64
}

// params is a SourceParameters, EnrichmentParameters or TargetParameters object.
type params map[string]json.RawMessage

// decode reads the members pogi acts on into v.
func (p params) decode(v any) {
	b, _ := json.Marshal(p)
	json.Unmarshal(b, v)
}

// summary is what the state-changing operations return.
func (p *Pipe) summary() map[string]any {
	return map[string]any{"Arn": p.Arn, "Name": p.Name, "DesiredState": p.DesiredState, "CurrentState": p.CurrentState,
		"CreationTime": p.CreationTime, "LastModifiedTime": p.LastModifiedTime}
}

// New creates the Pipes service and starts its clock.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	for route, op := range map[string]struct {
		name string
		fn   func(*core.Ctx, *http.Request) (any, error)
	}{
		"POST /v1/pipes/{Name}":       {"CreatePipe", s.createPipe},
		"GET /v1/pipes/{Name}":        {"DescribePipe", s.describePipe},
		"PUT /v1/pipes/{Name}":        {"UpdatePipe", s.updatePipe},
		"DELETE /v1/pipes/{Name}":     {"DeletePipe", s.deletePipe},
		"GET /v1/pipes":               {"ListPipes", s.listPipes},
		"POST /v1/pipes/{Name}/start": {"StartPipe", s.setState("RUNNING", "STARTING")},
		"POST /v1/pipes/{Name}/stop":  {"StopPipe", s.setState("STOPPED", "STOPPING")},
		"POST /tags/{arn...}":         {"TagResource", s.tagResource},
		"GET /tags/{arn...}":          {"ListTagsForResource", s.listTags},
		"DELETE /tags/{arn...}":       {"UntagResource", s.untagResource},
		"/":                           {"Unknown", s.unknown},
	} {
		s.mux.HandleFunc(route, s.handle(op.name, op.fn))
	}
	s.stop = core.Every(250*time.Millisecond, s.Tick)
	return s
}

// Close stops the clock and with it the pollers.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Info() core.Info {
	return core.Info{Name: "pipes", Cloud: core.AWS, Title: "EventBridge Pipes", Signing: []string{"pipes"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// handle runs one operation and writes its REST-JSON result or error.
func (s *Service) handle(op string, fn func(*core.Ctx, *http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		var out any
		err := c.Intercept()
		if err == nil {
			out, err = fn(c, r)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if out == nil {
			out = struct{}{}
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	}
}

func (s *Service) unknown(c *core.Ctx, r *http.Request) (any, error) {
	return nil, core.Errorf(404, "UnknownOperationException", "pogi does not implement this Pipes request (%s %s)", r.Method, r.URL.Path)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Pipes: map[string]*Pipe{}}
	s.curs = map[string]*cursor{}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Pipes == nil {
		st.Pipes = map[string]*Pipe{}
	}
	s.mu.Lock()
	s.st, s.curs = st, map[string]*cursor{}
	s.mu.Unlock()
	return nil
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func notFound(format string, a ...any) error {
	return core.Errorf(404, "NotFoundException", format, a...)
}

func conflict(format string, a ...any) error {
	return core.Errorf(409, "ConflictException", format, a...)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, v) != nil {
		return invalid("The request body is not valid JSON.")
	}
	return nil
}

var nameRe = regexp.MustCompile(`^[\.\-_A-Za-z0-9]{1,64}$`)

// check validates a pipe as created or updated.
func check(p *Pipe) error {
	src, _, _, res, _ := core.ParseARN(p.Source)
	var sp source
	p.SourceParameters.decode(&sp)
	switch {
	case !nameRe.MatchString(p.Name):
		return invalid("Pipe name %s is not valid.", p.Name)
	case p.RoleArn == "":
		return invalid("RoleArn is required.")
	case p.Target == "":
		return invalid("Target is required.")
	case p.DesiredState != "RUNNING" && p.DesiredState != "STOPPED":
		return invalid("DesiredState %s is not valid: use RUNNING or STOPPED.", p.DesiredState)
	case src != "sqs" && src != "kinesis" && !(src == "dynamodb" && strings.Contains(res, "/stream/")):
		return invalid("Source %s is not supported: pogi pipes poll SQS queues, Kinesis streams and DynamoDB streams.", p.Source)
	case src != "sqs" && sp.of(src).StartingPosition == "":
		return invalid("StartingPosition is required in the SourceParameters of a stream source.")
	}
	if p.Enrichment != "" {
		svc, _, _, res, _ := core.ParseARN(p.Enrichment)
		if svc != "lambda" && svc != "states" && svc != "execute-api" && !(svc == "events" && strings.HasPrefix(res, "api-destination/")) {
			return invalid("Enrichment %s is not supported: use a Lambda function, a Step Functions state machine, an API Gateway route or an API destination.", p.Enrichment)
		}
	}
	if sp.FilterCriteria != nil {
		for _, f := range sp.FilterCriteria.Filters {
			if _, err := pattern.Compile(f.Pattern); err != nil {
				return invalid("Invalid filter pattern %s: %v", f.Pattern, err)
			}
		}
	}
	return nil
}

// settled refuses a change while the pipe is between states.
func settled(p *Pipe) error {
	if strings.HasSuffix(p.CurrentState, "ING") && p.CurrentState != "RUNNING" {
		return conflict("Pipe %s is %s; try again once it has settled.", p.Name, p.CurrentState)
	}
	return nil
}

func (s *Service) pipe(c *core.Ctx, name string) (*Pipe, error) {
	if p := s.st.Pipes[key(c.Account, c.Region, name)]; p != nil {
		return p, nil
	}
	return nil, notFound("Pipe %s does not exist.", name)
}

func (s *Service) createPipe(c *core.Ctx, r *http.Request) (any, error) {
	p := &Pipe{}
	if err := decode(r, p); err != nil {
		return nil, err
	}
	p.Name, p.DesiredState, p.StateReason = r.PathValue("Name"), cmp.Or(p.DesiredState, "RUNNING"), ""
	p.SourceParameters, p.EnrichmentParameters, p.TargetParameters = merge(nil, p.SourceParameters), merge(nil, p.EnrichmentParameters), merge(nil, p.TargetParameters)
	if err := check(p); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, p.Name)
	if s.st.Pipes[k] != nil {
		return nil, conflict("Pipe %s already exists.", p.Name)
	}
	now := core.Unix(time.Now())
	p.Arn, p.CurrentState, p.CreationTime, p.LastModifiedTime = c.ARN("pipes", "pipe/"+p.Name), "CREATING", now, now
	s.st.Pipes[k] = p
	return p.summary(), nil
}

func (s *Service) describePipe(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pipe(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	b, _ := json.Marshal(p) // a copy: the pollers write the state
	return json.RawMessage(b), nil
}

// updatePipe changes the members sent. In a parameters object each member
// sent replaces the old one, as in AWS; the source itself cannot change.
func (s *Service) updatePipe(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Description, DesiredState, Enrichment, Target, RoleArn, KmsKeyIdentifier *string
		SourceParameters, EnrichmentParameters, TargetParameters                 params
		LogConfiguration                                                         json.RawMessage
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pipe(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	if err := settled(p); err != nil {
		return nil, err
	}
	u := *p
	for dst, v := range map[*string]*string{&u.Description: in.Description, &u.DesiredState: in.DesiredState, &u.Enrichment: in.Enrichment,
		&u.Target: in.Target, &u.RoleArn: in.RoleArn, &u.KmsKeyIdentifier: in.KmsKeyIdentifier} {
		if v != nil {
			*dst = *v
		}
	}
	u.SourceParameters = merge(p.SourceParameters, in.SourceParameters)
	u.EnrichmentParameters = merge(p.EnrichmentParameters, in.EnrichmentParameters)
	u.TargetParameters = merge(p.TargetParameters, in.TargetParameters)
	if in.LogConfiguration != nil {
		u.LogConfiguration = in.LogConfiguration
	}
	if err := check(&u); err != nil {
		return nil, err
	}
	u.CurrentState, u.StateReason, u.LastModifiedTime = "UPDATING", "", core.Unix(time.Now())
	*p = u
	return p.summary(), nil
}

// merge applies UpdatePipe's parameters to the old ones. A member sent
// empty, the way Terraform clears one, goes; a stream source keeps its
// starting position, which cannot change.
func merge(old, upd params) params {
	out := maps.Clone(old)
	if out == nil {
		out = params{}
	}
	for k, v := range upd {
		switch string(bytes.TrimSpace(v)) {
		case `""`, "{}", "[]", "null":
			delete(out, k)
			continue
		}
		if k == "KinesisStreamParameters" || k == "DynamoDBStreamParameters" {
			var o, n map[string]json.RawMessage
			json.Unmarshal(old[k], &o)
			if json.Unmarshal(v, &n) == nil && n != nil {
				for _, f := range []string{"StartingPosition", "StartingPositionTimestamp"} {
					if o[f] != nil {
						n[f] = o[f]
					}
				}
				v, _ = json.Marshal(n)
			}
		}
		out[k] = v
	}
	return out
}

// setState is StartPipe and StopPipe: the pipe passes through via on its
// way to desired, which the clock completes.
func (s *Service) setState(desired, via string) func(*core.Ctx, *http.Request) (any, error) {
	return func(c *core.Ctx, r *http.Request) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		p, err := s.pipe(c, r.PathValue("Name"))
		if err != nil {
			return nil, err
		}
		if err := settled(p); err != nil {
			return nil, err
		}
		p.DesiredState, p.CurrentState, p.StateReason, p.LastModifiedTime = desired, via, "", core.Unix(time.Now())
		return p.summary(), nil
	}
}

func (s *Service) deletePipe(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pipe(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	p.DesiredState, p.CurrentState, p.LastModifiedTime = "DELETED", "DELETING", core.Unix(time.Now())
	return p.summary(), nil
}

func (s *Service) listPipes(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for k, p := range s.st.Pipes {
		if strings.HasPrefix(k, key(c.Account, c.Region, q.Get("NamePrefix"))) &&
			(q.Get("DesiredState") == "" || p.DesiredState == q.Get("DesiredState")) &&
			(q.Get("CurrentState") == "" || p.CurrentState == q.Get("CurrentState")) &&
			strings.HasPrefix(p.Source, q.Get("SourcePrefix")) && strings.HasPrefix(p.Target, q.Get("TargetPrefix")) {
			m := p.summary()
			m["Source"], m["Target"], m["Enrichment"], m["StateReason"] = p.Source, p.Target, p.Enrichment, p.StateReason
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["Name"].(string) < out[j]["Name"].(string) })
	out = out[sort.Search(len(out), func(i int) bool { return out[i]["Name"].(string) > q.Get("NextToken") }):]
	res := map[string]any{"Pipes": out}
	if limit, _ := strconv.Atoi(q.Get("Limit")); limit > 0 && len(out) > limit {
		res["Pipes"], res["NextToken"] = out[:limit], out[limit-1]["Name"]
	}
	if out == nil {
		res["Pipes"] = []any{}
	}
	return res, nil
}

// ---- tags ----

func (s *Service) tagged(arn string) (*Pipe, error) {
	_, region, account, res, _ := core.ParseARN(arn)
	if name, ok := strings.CutPrefix(res, "pipe/"); ok {
		if p := s.st.Pipes[key(account, region, name)]; p != nil {
			return p, nil
		}
	}
	return nil, notFound("Resource %s does not exist.", arn)
}

func (s *Service) tagResource(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Tags map[string]string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	if p.Tags == nil {
		p.Tags = map[string]string{}
	}
	maps.Copy(p.Tags, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(p.Tags, k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	tags := map[string]string{}
	maps.Copy(tags, p.Tags)
	return map[string]any{"tags": tags}, nil
}

// ---- lifecycle ----

// starting maps the states a pipe passes on its way to RUNNING to the one
// it lands in when its source cannot be opened.
var starting = map[string]string{"CREATING": "CREATE_FAILED", "UPDATING": "UPDATE_FAILED", "STARTING": "START_FAILED"}

// Tick moves pipes along their lifecycle and polls each running one. A
// goroutine calls it every 250ms; tests may call it directly.
func (s *Service) Tick(now time.Time) {
	type job struct {
		key string
		p   Pipe
		cur *cursor
	}
	var jobs []job
	s.mu.Lock()
	for k, p := range s.st.Pipes {
		switch {
		case p.CurrentState == "DELETING":
			delete(s.st.Pipes, k)
			delete(s.curs, k)
		case p.DesiredState == "STOPPED":
			p.CurrentState = "STOPPED"
		case p.CurrentState == "RUNNING" || starting[p.CurrentState] != "": // the poller reports RUNNING once the source is open
			cur := s.curs[k]
			if cur == nil {
				cur = &cursor{}
				s.curs[k] = cur
			}
			if !cur.busy {
				cur.busy = true
				jobs = append(jobs, job{k, *p, cur})
			}
		}
	}
	s.mu.Unlock()
	for _, j := range jobs {
		core.Go("pipes.poll", func() { s.poll(j.key, j.p, j.cur) })
	}
}

// poll opens the source if needed, reports the pipe RUNNING (or failed),
// then moves one batch and records the outcome as the pipe's StateReason.
func (s *Service) poll(k string, p Pipe, cur *cursor) {
	err := s.open(&p, cur)
	s.mu.Lock()
	q := s.st.Pipes[k]
	if s.curs[k] != cur { // deleted meanwhile
		q = nil
	}
	if q != nil && starting[q.CurrentState] != "" && q.DesiredState == "RUNNING" {
		if err != nil {
			q.CurrentState, q.StateReason = starting[q.CurrentState], err.Error()
		} else {
			q.CurrentState = "RUNNING"
		}
	}
	s.mu.Unlock()
	if err == nil {
		err = s.pull(&p, cur)
	}
	s.mu.Lock()
	cur.busy = false
	if q != nil && q.CurrentState == "RUNNING" {
		q.StateReason = ""
		if err != nil {
			q.StateReason = err.Error()
		}
	}
	s.mu.Unlock()
}

// ---- console ----

// Resources lists pipes for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Pipes", Columns: []string{"Name", "Region", "State", "Source", "Enrichment", "Target", "Reason"}}
	for _, k := range core.SortedKeys(s.st.Pipes) {
		p := s.st.Pipes[k]
		t.Rows = append(t.Rows, []string{p.Name, strings.Split(k, "/")[1], p.CurrentState, p.Source, p.Enrichment, p.Target, p.StateReason})
	}
	return []core.Table{t}
}

// Inspect returns a pipe as DescribePipe does.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Pipes) {
		if p := s.st.Pipes[k]; table == "Pipes" && p.Name == id {
			b, _ := json.Marshal(p)
			return json.RawMessage(b)
		}
	}
	return nil
}
