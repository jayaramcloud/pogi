// Package logs emulates Amazon CloudWatch Logs over JSON 1.1: log groups and
// streams, events with retention, filter patterns, metric and subscription
// filters, tags and a Logs Insights subset. It is also the sink for Lambda
// function logs, which arrive through the hub one line at a time.
package logs

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the CloudWatch Logs emulator.
type Service struct {
	hub     *core.Hub
	rpc     *core.RPC
	mu      sync.Mutex
	st      state
	queries []*query // recent Logs Insights queries, not persisted
}

type state struct {
	Groups map[string]*Group `json:"groups"` // account/region/name
	Seq    int64             `json:"seq"`    // last event id
}

// Group is a log group.
type Group struct {
	Name      string                   `json:"name"`
	Account   string                   `json:"account"`
	Region    string                   `json:"region"`
	Created   int64                    `json:"created"`             // epoch millis
	Retention int                      `json:"retention,omitempty"` // days; 0 keeps events forever
	KmsKeyID  string                   `json:"kmsKeyId,omitempty"`
	Class     string                   `json:"class"`
	Tags      map[string]string        `json:"tags"`
	Streams   map[string]*Stream       `json:"streams"`
	Metrics   map[string]*MetricFilter `json:"metrics"`
	Subs      map[string]*SubFilter    `json:"subs"`
}

// Stream is a log stream; its events are sorted by (timestamp, id).
type Stream struct {
	Name     string  `json:"name"`
	Created  int64   `json:"created"`
	Ingested int64   `json:"ingested,omitempty"`
	Uploads  int64   `json:"uploads,omitempty"` // PutLogEvents calls, for sequence tokens
	Events   []event `json:"events"`
}

type event struct {
	ID int64  `json:"i"`
	T  int64  `json:"t"` // timestamp, epoch millis
	In int64  `json:"n"` // ingestion time
	M  string `json:"m"`
}

// MetricFilter publishes its metric to CloudWatch for each event its pattern matches.
type MetricFilter struct {
	Name            string           `json:"filterName"`
	Pattern         string           `json:"filterPattern"`
	Transformations []Transformation `json:"metricTransformations"`
	Created         int64            `json:"creationTime"`
}

// Transformation is a metric filter's metric (API shape).
type Transformation struct {
	MetricName      string            `json:"metricName"`
	MetricNamespace string            `json:"metricNamespace"`
	MetricValue     string            `json:"metricValue"`
	DefaultValue    *float64          `json:"defaultValue,omitempty"`
	Dimensions      map[string]string `json:"dimensions,omitempty"`
	Unit            string            `json:"unit,omitempty"`
}

// SubFilter forwards matching events to a Lambda, Kinesis or Firehose destination.
type SubFilter struct {
	Name         string  `json:"filterName"`
	Pattern      string  `json:"filterPattern"`
	Destination  string  `json:"destinationArn"`
	Role         string  `json:"roleArn,omitempty"`
	Distribution string  `json:"distribution"`
	Created      int64   `json:"creationTime"`
	match        matcher // compiled lazily after Load
}

const (
	hour      = int64(time.Hour / time.Millisecond)
	day       = 24 * hour
	maxEvents = 10000 // per stream; the oldest are dropped
)

// New creates the CloudWatch Logs service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateLogGroup":              core.H(s.createLogGroup),
		"DeleteLogGroup":              core.H(s.deleteLogGroup),
		"DescribeLogGroups":           core.H(s.describeLogGroups),
		"PutRetentionPolicy":          core.H(s.putRetentionPolicy),
		"DeleteRetentionPolicy":       core.H(s.deleteRetentionPolicy),
		"CreateLogStream":             core.H(s.createLogStream),
		"DeleteLogStream":             core.H(s.deleteLogStream),
		"DescribeLogStreams":          core.H(s.describeLogStreams),
		"PutLogEvents":                core.H(s.putLogEvents),
		"GetLogEvents":                core.H(s.getLogEvents),
		"FilterLogEvents":             core.H(s.filterLogEvents),
		"TagLogGroup":                 core.H(s.tagLogGroup),
		"UntagLogGroup":               core.H(s.untagLogGroup),
		"ListTagsLogGroup":            core.H(s.listTagsLogGroup),
		"TagResource":                 core.H(s.tagResource),
		"UntagResource":               core.H(s.untagResource),
		"ListTagsForResource":         core.H(s.listTagsForResource),
		"PutMetricFilter":             core.H(s.putMetricFilter),
		"DescribeMetricFilters":       core.H(s.describeMetricFilters),
		"DeleteMetricFilter":          core.H(s.deleteMetricFilter),
		"PutSubscriptionFilter":       core.H(s.putSubscriptionFilter),
		"DescribeSubscriptionFilters": core.H(s.describeSubscriptionFilters),
		"DeleteSubscriptionFilter":    core.H(s.deleteSubscriptionFilter),
		"StartQuery":                  core.H(s.startQuery),
		"GetQueryResults":             core.H(s.getQueryResults),
		"StopQuery":                   core.H(s.stopQuery),
		"DescribeQueries":             core.H(s.describeQueries),
	}}
	if hub != nil {
		hub.OnDeliver("logs", s.deliver)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "logs", Cloud: core.AWS, Title: "CloudWatch Logs", Targets: []string{"Logs_20140328"}, Signing: []string{"logs"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Groups: map[string]*Group{}}
	s.queries = nil
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
	if st.Groups == nil {
		st.Groups = map[string]*Group{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func invalidParam(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

func notFound(what string) error {
	return core.Errorf(400, "ResourceNotFoundException", "The specified %s does not exist.", what)
}

func (g *Group) arn() string { return core.ARN("logs", g.Region, g.Account, "log-group:"+g.Name) }

func (s *Service) newGroup(account, region, name string, now int64) *Group {
	g := &Group{Name: name, Account: account, Region: region, Created: now, Class: "STANDARD", Tags: map[string]string{},
		Streams: map[string]*Stream{}, Metrics: map[string]*MetricFilter{}, Subs: map[string]*SubFilter{}}
	s.st.Groups[key(account, region, name)] = g
	return g
}

// group finds a log group by name or ARN and applies its retention. Lock held.
func (s *Service) group(c *core.Ctx, name, ident string) (*Group, error) {
	if name != "" && ident != "" {
		return nil, invalidParam("LogGroup name and LogGroup ARN are mutually exclusive parameters.")
	}
	ref, account, region := name+ident, c.Account, c.Region
	if _, r, a, res, ok := core.ParseARN(ref); ok {
		ref, account, region = strings.TrimSuffix(strings.TrimPrefix(res, "log-group:"), ":*"), a, r
	}
	g := s.st.Groups[key(account, region, ref)]
	if g == nil {
		return nil, notFound("log group")
	}
	g.expire(time.Now().UnixMilli())
	return g, nil
}

// groups lists the caller's log groups by name. Lock held.
func (s *Service) groups(c *core.Ctx) []*Group {
	var out []*Group
	for _, g := range s.st.Groups {
		if g.Account == c.Account && g.Region == c.Region {
			g.expire(time.Now().UnixMilli())
			out = append(out, g)
		}
	}
	slices.SortFunc(out, func(a, b *Group) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// expire drops events older than the retention period. Lock held.
func (g *Group) expire(now int64) {
	if g.Retention == 0 {
		return
	}
	cut := now - int64(g.Retention)*day
	for _, st := range g.Streams {
		if i := sort.Search(len(st.Events), func(i int) bool { return st.Events[i].T >= cut }); i > 0 {
			st.Events = st.Events[i:]
		}
	}
}

// page slices n items for an offset nextToken and a page size (def when unset).
func page(n int, token string, size, def int) (start, end int, next string, err error) {
	if token != "" {
		if start, err = strconv.Atoi(token); err != nil || start < 0 {
			return 0, 0, "", invalidParam("The specified nextToken is invalid.")
		}
	}
	if size <= 0 {
		size = def
	}
	start = min(start, n)
	end = min(start+size, n)
	if end < n {
		next = strconv.Itoa(end)
	}
	return start, end, next, nil
}

// ---- log groups ----

var (
	groupNameRe = regexp.MustCompile(`^[.\-_/#A-Za-z0-9]{1,512}$`)
	retentions  = []int{1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653}
)

func (s *Service) createLogGroup(c *core.Ctx, in *struct {
	LogGroupName, KmsKeyId, LogGroupClass string
	Tags                                  map[string]string
}) (any, error) {
	if !groupNameRe.MatchString(in.LogGroupName) {
		return nil, invalidParam("1 validation error detected: Value '%s' at 'logGroupName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\.\\-_/#A-Za-z0-9]+", in.LogGroupName)
	}
	if !slices.Contains([]string{"", "STANDARD", "INFREQUENT_ACCESS", "DELIVERY"}, in.LogGroupClass) {
		return nil, invalidParam("Invalid logGroupClass %s.", in.LogGroupClass)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Groups[key(c.Account, c.Region, in.LogGroupName)] != nil {
		return nil, core.Errorf(400, "ResourceAlreadyExistsException", "The specified log group already exists")
	}
	g := s.newGroup(c.Account, c.Region, in.LogGroupName, time.Now().UnixMilli())
	g.KmsKeyID = in.KmsKeyId
	if in.LogGroupClass != "" {
		g.Class = in.LogGroupClass
	}
	maps.Copy(g.Tags, in.Tags)
	return nil, nil
}

func (s *Service) deleteLogGroup(c *core.Ctx, in *struct{ LogGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	delete(s.st.Groups, key(g.Account, g.Region, g.Name))
	return nil, nil
}

type groupOut struct {
	LogGroupName      string `json:"logGroupName"`
	CreationTime      int64  `json:"creationTime"`
	RetentionInDays   int    `json:"retentionInDays,omitempty"`
	MetricFilterCount int    `json:"metricFilterCount"`
	Arn               string `json:"arn"`
	StoredBytes       int64  `json:"storedBytes"`
	KmsKeyID          string `json:"kmsKeyId,omitempty"`
	LogGroupClass     string `json:"logGroupClass"`
	LogGroupArn       string `json:"logGroupArn"`
}

func (g *Group) out() groupOut {
	var stored int64 // AWS counts 26 bytes of overhead per event
	for _, st := range g.Streams {
		for _, e := range st.Events {
			stored += int64(len(e.M)) + 26
		}
	}
	return groupOut{g.Name, g.Created, g.Retention, len(g.Metrics), g.arn() + ":*", stored, g.KmsKeyID, g.Class, g.arn()}
}

func (s *Service) describeLogGroups(c *core.Ctx, in *struct {
	LogGroupNamePrefix, LogGroupNamePattern, LogGroupClass, NextToken string
	LogGroupIdentifiers                                               []string
	Limit                                                             int
}) (any, error) {
	if in.LogGroupNamePrefix != "" && in.LogGroupNamePattern != "" {
		return nil, invalidParam("LogGroup name prefix and LogGroup name pattern are mutually exclusive parameters.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []groupOut
	for _, g := range s.groups(c) {
		if strings.HasPrefix(g.Name, in.LogGroupNamePrefix) && strings.Contains(g.Name, in.LogGroupNamePattern) &&
			(in.LogGroupClass == "" || g.Class == in.LogGroupClass) &&
			(len(in.LogGroupIdentifiers) == 0 || slices.ContainsFunc(in.LogGroupIdentifiers, func(id string) bool {
				return id == g.Name || strings.TrimSuffix(id, ":*") == g.arn()
			})) {
			all = append(all, g.out())
		}
	}
	start, end, next, err := page(len(all), in.NextToken, in.Limit, 50)
	if err != nil {
		return nil, err
	}
	return &struct {
		LogGroups []groupOut `json:"logGroups"`
		NextToken string     `json:"nextToken,omitempty"`
	}{append([]groupOut{}, all[start:end]...), next}, nil
}

func (s *Service) putRetentionPolicy(c *core.Ctx, in *struct {
	LogGroupName    string
	RetentionInDays int
}) (any, error) {
	if !slices.Contains(retentions, in.RetentionInDays) {
		return nil, invalidParam("1 validation error detected: Value '%d' at 'retentionInDays' failed to satisfy constraint: Member must satisfy enum value set: %v", in.RetentionInDays, retentions)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	g.Retention = in.RetentionInDays
	g.expire(time.Now().UnixMilli())
	return nil, nil
}

func (s *Service) deleteRetentionPolicy(c *core.Ctx, in *struct{ LogGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	g.Retention = 0
	return nil, nil
}

// ---- log streams ----

func (s *Service) createLogStream(c *core.Ctx, in *struct{ LogGroupName, LogStreamName string }) (any, error) {
	if n := in.LogStreamName; n == "" || len(n) > 512 || strings.ContainsAny(n, ":*") {
		return nil, invalidParam("Invalid log stream name %q: 1 to 512 characters, no ':' or '*'.", n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	if g.Streams[in.LogStreamName] != nil {
		return nil, core.Errorf(400, "ResourceAlreadyExistsException", "The specified log stream already exists")
	}
	g.Streams[in.LogStreamName] = &Stream{Name: in.LogStreamName, Created: time.Now().UnixMilli()}
	return nil, nil
}

func (s *Service) deleteLogStream(c *core.Ctx, in *struct{ LogGroupName, LogStreamName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	if g.Streams[in.LogStreamName] == nil {
		return nil, notFound("log stream")
	}
	delete(g.Streams, in.LogStreamName)
	return nil, nil
}

type streamOut struct {
	LogStreamName       string `json:"logStreamName"`
	CreationTime        int64  `json:"creationTime"`
	FirstEventTimestamp int64  `json:"firstEventTimestamp,omitempty"`
	LastEventTimestamp  int64  `json:"lastEventTimestamp,omitempty"`
	LastIngestionTime   int64  `json:"lastIngestionTime,omitempty"`
	UploadSequenceToken string `json:"uploadSequenceToken,omitempty"`
	Arn                 string `json:"arn"`
	StoredBytes         int64  `json:"storedBytes"` // always 0 in AWS since 2019
}

func (s *Service) describeLogStreams(c *core.Ctx, in *struct {
	LogGroupName, LogGroupIdentifier, LogStreamNamePrefix, OrderBy, NextToken string
	Descending                                                                bool
	Limit                                                                     int
}) (any, error) {
	if in.OrderBy == "LastEventTime" && in.LogStreamNamePrefix != "" {
		return nil, invalidParam("Cannot order by LastEventTime with a logStreamNamePrefix.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, in.LogGroupIdentifier)
	if err != nil {
		return nil, err
	}
	var all []streamOut
	for _, st := range g.Streams {
		if !strings.HasPrefix(st.Name, in.LogStreamNamePrefix) {
			continue
		}
		o := streamOut{LogStreamName: st.Name, CreationTime: st.Created, LastIngestionTime: st.Ingested, Arn: g.arn() + ":log-stream:" + st.Name}
		if n := len(st.Events); n > 0 {
			o.FirstEventTimestamp, o.LastEventTimestamp = st.Events[0].T, st.Events[n-1].T
		}
		if st.Uploads > 0 {
			o.UploadSequenceToken = strconv.FormatInt(st.Uploads, 10)
		}
		all = append(all, o)
	}
	slices.SortFunc(all, func(a, b streamOut) int {
		if in.OrderBy == "LastEventTime" && a.LastEventTimestamp != b.LastEventTimestamp {
			return cmp.Compare(a.LastEventTimestamp, b.LastEventTimestamp)
		}
		return strings.Compare(a.LogStreamName, b.LogStreamName)
	})
	if in.Descending {
		slices.Reverse(all)
	}
	start, end, next, err := page(len(all), in.NextToken, in.Limit, 50)
	if err != nil {
		return nil, err
	}
	return &struct {
		LogStreams []streamOut `json:"logStreams"`
		NextToken  string      `json:"nextToken,omitempty"`
	}{append([]streamOut{}, all[start:end]...), next}, nil
}

// ---- events ----

type inputEvent struct {
	Timestamp int64
	Message   string
}

// cmpEvent orders events by timestamp, then id (arrival).
func cmpEvent(a, b event) int {
	if c := cmp.Compare(a.T, b.T); c != 0 {
		return c
	}
	return cmp.Compare(a.ID, b.ID)
}

// token is an event's position, used as a pagination cursor.
func (e event) token() string { return strconv.FormatInt(e.T, 10) + "/" + strconv.FormatInt(e.ID, 10) }

func parseToken(s string) (event, error) {
	t, id, ok := strings.Cut(s, "/")
	ts, err1 := strconv.ParseInt(t, 10, 64)
	n, err2 := strconv.ParseInt(id, 10, 64)
	if !ok || err1 != nil || err2 != nil {
		return event{}, invalidParam("The specified nextToken is invalid.")
	}
	return event{T: ts, ID: n}, nil
}

// add stores events (sorted by timestamp) in a stream and returns the
// subscription deliveries they cause. Lock held.
func (s *Service) add(g *Group, st *Stream, in []inputEvent, now int64) fanout {
	if len(in) == 0 {
		return fanout{}
	}
	evs := make([]event, len(in))
	for i, e := range in {
		s.st.Seq++
		evs[i] = event{ID: s.st.Seq, T: e.Timestamp, In: now, M: e.Message}
	}
	inOrder := len(st.Events) == 0 || st.Events[len(st.Events)-1].T <= evs[0].T
	st.Events = append(st.Events, evs...)
	if !inOrder { // stable, so equal timestamps stay in id order
		sort.SliceStable(st.Events, func(i, j int) bool { return st.Events[i].T < st.Events[j].T })
	}
	if n := len(st.Events); n > maxEvents {
		st.Events = st.Events[n-maxEvents:]
	}
	st.Ingested = now
	return fanout{g.subscribed(st.Name, evs), g.metricPuts(evs)}
}

func (s *Service) putLogEvents(c *core.Ctx, in *struct {
	LogGroupName, LogStreamName, SequenceToken string
	LogEvents                                  []inputEvent
}) (any, error) {
	evs := in.LogEvents
	if len(evs) == 0 {
		return nil, invalidParam("1 validation error detected: Value at 'logEvents' failed to satisfy constraint: Member must have length greater than or equal to 1")
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].Timestamp < evs[j].Timestamp })
	if evs[len(evs)-1].Timestamp-evs[0].Timestamp > day {
		return nil, invalidParam("The batch of log events in a single PutLogEvents request cannot span more than 24 hours.")
	}
	now := time.Now().UnixMilli()
	s.mu.Lock()
	g, err := s.group(c, in.LogGroupName, "")
	var st *Stream
	if err == nil {
		if st = g.Streams[in.LogStreamName]; st == nil {
			err = notFound("log stream")
		}
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	// Events older than 14 days or the retention period, or more than 2 hours ahead, are rejected.
	var rejected struct {
		TooNewLogEventStartIndex *int `json:"tooNewLogEventStartIndex,omitempty"`
		TooOldLogEventEndIndex   *int `json:"tooOldLogEventEndIndex,omitempty"`
		ExpiredLogEventEndIndex  *int `json:"expiredLogEventEndIndex,omitempty"`
	}
	lo, hi := 0, len(evs)
	for lo < hi && evs[lo].Timestamp < now-14*day {
		lo++
	}
	if old := lo; old > 0 {
		rejected.TooOldLogEventEndIndex = &old
	}
	if old := lo; g.Retention > 0 {
		for lo < hi && evs[lo].Timestamp < now-int64(g.Retention)*day {
			lo++
		}
		if end := lo; end > old {
			rejected.ExpiredLogEventEndIndex = &end
		}
	}
	for hi > lo && evs[hi-1].Timestamp > now+2*hour {
		hi--
	}
	if start := hi; hi < len(evs) {
		rejected.TooNewLogEventStartIndex = &start
	}
	ds := s.add(g, st, evs[lo:hi], now)
	st.Uploads++
	out := map[string]any{"nextSequenceToken": strconv.FormatInt(st.Uploads, 10)}
	s.mu.Unlock()
	s.send(ds)
	if rejected.TooOldLogEventEndIndex != nil || rejected.ExpiredLogEventEndIndex != nil || rejected.TooNewLogEventStartIndex != nil {
		out["rejectedLogEventsInfo"] = rejected
	}
	return out, nil
}

type outputEvent struct {
	Timestamp     int64  `json:"timestamp"`
	Message       string `json:"message"`
	IngestionTime int64  `json:"ingestionTime"`
}

func (s *Service) getLogEvents(c *core.Ctx, in *struct {
	LogGroupName, LogGroupIdentifier, LogStreamName, NextToken string
	StartTime, EndTime                                         int64
	Limit                                                      int
	StartFromHead                                              bool
}) (any, error) {
	limit := in.Limit
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, in.LogGroupIdentifier)
	if err != nil {
		return nil, err
	}
	st := g.Streams[in.LogStreamName]
	if st == nil {
		return nil, notFound("log stream")
	}
	evs := st.Events // window: startTime inclusive, endTime exclusive
	lo := sort.Search(len(evs), func(i int) bool { return evs[i].T >= in.StartTime })
	hi := len(evs)
	if in.EndTime > 0 {
		hi = max(lo, sort.Search(len(evs), func(i int) bool { return evs[i].T >= in.EndTime }))
	}
	evs = evs[lo:hi]
	// Tokens are f/<position> (after it) and b/<position> (before it); at either
	// end the same token comes back, which is how SDK paginators stop.
	var pg []event
	fwd, bwd := "f/0/0", "b/0/0"
	switch dir, pos, _ := strings.Cut(in.NextToken, "/"); {
	case in.NextToken == "" && in.StartFromHead:
		pg = evs[:min(limit, len(evs))]
	case in.NextToken == "":
		pg = evs[max(0, len(evs)-limit):]
	case dir == "f" || dir == "b":
		at, err := parseToken(pos)
		if err != nil {
			return nil, err
		}
		fwd, bwd = "f/"+pos, "b/"+pos
		if dir == "f" {
			i := sort.Search(len(evs), func(i int) bool { return cmpEvent(evs[i], at) > 0 })
			pg = evs[i:min(i+limit, len(evs))]
		} else {
			j := sort.Search(len(evs), func(i int) bool { return cmpEvent(evs[i], at) >= 0 })
			pg = evs[max(0, j-limit):j]
		}
	default:
		return nil, invalidParam("The specified nextToken is invalid.")
	}
	out := struct {
		Events            []outputEvent `json:"events"`
		NextForwardToken  string        `json:"nextForwardToken"`
		NextBackwardToken string        `json:"nextBackwardToken"`
	}{Events: []outputEvent{}, NextForwardToken: fwd, NextBackwardToken: bwd}
	if len(pg) > 0 {
		out.NextForwardToken, out.NextBackwardToken = "f/"+pg[len(pg)-1].token(), "b/"+pg[0].token()
	}
	for _, e := range pg {
		out.Events = append(out.Events, outputEvent{e.T, e.M, e.In})
	}
	return &out, nil
}

type filteredEvent struct {
	LogStreamName string `json:"logStreamName"`
	Timestamp     int64  `json:"timestamp"`
	Message       string `json:"message"`
	IngestionTime int64  `json:"ingestionTime"`
	EventID       string `json:"eventId"`
}

func (s *Service) filterLogEvents(c *core.Ctx, in *struct {
	LogGroupName, LogGroupIdentifier, LogStreamNamePrefix, FilterPattern, NextToken string
	LogStreamNames                                                                  []string
	StartTime, EndTime                                                              int64
	Limit                                                                           int
}) (any, error) {
	if len(in.LogStreamNames) > 0 && in.LogStreamNamePrefix != "" {
		return nil, invalidParam("logStreamNames and logStreamNamePrefix are mutually exclusive parameters.")
	}
	match, err := compile(in.FilterPattern)
	if err != nil {
		return nil, err
	}
	var after event // results are ordered by (timestamp, id); the token is the last one returned
	if in.NextToken != "" {
		if after, err = parseToken(in.NextToken); err != nil {
			return nil, err
		}
	}
	limit := in.Limit
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, in.LogGroupIdentifier)
	if err != nil {
		return nil, err
	}
	type hit struct {
		stream string
		e      event
	}
	var hits []hit
	for name, st := range g.Streams {
		if !strings.HasPrefix(name, in.LogStreamNamePrefix) || len(in.LogStreamNames) > 0 && !slices.Contains(in.LogStreamNames, name) {
			continue
		}
		for _, e := range st.Events {
			if e.T >= in.StartTime && (in.EndTime == 0 || e.T <= in.EndTime) && (in.NextToken == "" || cmpEvent(e, after) > 0) && match(e.M) {
				hits = append(hits, hit{name, e})
			}
		}
	}
	slices.SortFunc(hits, func(a, b hit) int { return cmpEvent(a.e, b.e) })
	out := struct {
		Events    []filteredEvent `json:"events"`
		NextToken string          `json:"nextToken,omitempty"`
	}{Events: []filteredEvent{}}
	if len(hits) > limit {
		hits = hits[:limit]
		out.NextToken = hits[limit-1].e.token()
	}
	for _, h := range hits {
		out.Events = append(out.Events, filteredEvent{h.stream, h.e.T, h.e.M, h.e.In, strconv.FormatInt(h.e.ID, 10)})
	}
	return &out, nil
}

// ---- Lambda logs and subscription filters ----

// deliver appends one line to the stream an ARN names
// (arn:<partition>:logs:<region>:<account>:log-group:<group>:log-stream:<stream>),
// creating the group and stream on demand. Lambda sends every log line here.
func (s *Service) deliver(arn string, m *core.Message) error {
	_, region, account, res, ok := core.ParseARN(arn)
	group, stream, _ := strings.Cut(strings.TrimPrefix(res, "log-group:"), ":log-stream:")
	group = strings.TrimSuffix(group, ":*")
	if !ok || !strings.HasPrefix(res, "log-group:") || group == "" {
		return fmt.Errorf("CloudWatch Logs accepts log-group ARNs, not %s", arn)
	}
	if stream == "" {
		stream = "default"
	}
	now := time.Now().UnixMilli()
	s.mu.Lock()
	g := s.st.Groups[key(account, region, group)]
	if g == nil {
		g = s.newGroup(account, region, group, now)
	}
	st := g.Streams[stream]
	if st == nil {
		st = &Stream{Name: stream, Created: now}
		g.Streams[stream] = st
	}
	ds := s.add(g, st, []inputEvent{{now, m.Body}}, now)
	s.mu.Unlock()
	s.hub.MarkDirty()
	s.send(ds)
	return nil
}

// fanout is what new events trigger once the lock is released.
type fanout struct {
	subs    []delivery
	metrics []metricPut
}

// metricPut is one PutMetricData call for a metric filter.
type metricPut struct {
	account, region, namespace string
	datum                      map[string]any
}

// delivery is a batch of events for one subscription filter.
type delivery struct {
	dest, filter, owner, group, stream string
	events                             []event
}

// subscribed collects what the group's subscription filters want from new events. Lock held.
func (g *Group) subscribed(stream string, evs []event) []delivery {
	var out []delivery
	for _, f := range g.Subs {
		if f.match == nil {
			if f.match, _ = compile(f.Pattern); f.match == nil {
				f.match = func(string) bool { return false }
			}
		}
		var hit []event
		for _, e := range evs {
			if f.match(e.M) {
				hit = append(hit, e)
			}
		}
		if len(hit) > 0 {
			out = append(out, delivery{f.Destination, f.Name, g.Account, g.Name, stream, hit})
		}
	}
	return out
}

// metricPuts turns new events into CloudWatch datums, one per matching event.
// When a batch has no match, a filter's defaultValue is published instead.
// A metricValue is a number or a JSON selector such as $.latency; a dimension
// value is a selector. An event that lacks the field publishes nothing. Lock held.
// ponytail: space-delimited ($status) selectors are not read, and defaultValue is
// per batch, not per minute with a timer. Add both if someone needs them.
func (g *Group) metricPuts(evs []event) []metricPut {
	var out []metricPut
	for _, f := range g.Metrics {
		match, err := compile(f.Pattern)
		if err != nil {
			continue
		}
		tr := f.Transformations[0]
		hit := false
		for _, e := range evs {
			if !match(e.M) {
				continue
			}
			hit = true
			var doc any
			json.Unmarshal([]byte(e.M), &doc)
			value, ok := 0.0, true
			if strings.HasPrefix(tr.MetricValue, "$") {
				v, found := lookup(doc, tr.MetricValue)
				value, ok = num(v)
				ok = ok && found
			} else if value, err = strconv.ParseFloat(tr.MetricValue, 64); err != nil {
				ok = false
			}
			var dims []map[string]string
			for _, name := range core.SortedKeys(tr.Dimensions) {
				v, found := lookup(doc, tr.Dimensions[name])
				ok = ok && found
				dims = append(dims, map[string]string{"Name": name, "Value": str(v)})
			}
			if ok {
				out = append(out, g.metricPut(tr, value, dims, float64(e.T)/1000))
			}
		}
		if !hit && tr.DefaultValue != nil && len(evs) > 0 {
			out = append(out, g.metricPut(tr, *tr.DefaultValue, nil, float64(time.Now().UnixMilli())/1000))
		}
	}
	return out
}

func (g *Group) metricPut(tr Transformation, value float64, dims []map[string]string, ts float64) metricPut {
	d := map[string]any{"MetricName": tr.MetricName, "Value": value, "Timestamp": ts}
	if len(dims) > 0 {
		d["Dimensions"] = dims
	}
	if tr.Unit != "" {
		d["Unit"] = tr.Unit
	}
	return metricPut{g.Account, g.Region, tr.MetricNamespace, d}
}

// send delivers subscription batches as CloudWatch Logs does: gzipped JSON,
// wrapped as {"awslogs":{"data":base64}} for Lambda and raw for Kinesis and Firehose.
// ponytail: one delivery per PutLogEvents call or Lambda log line, no time-window batching.
func (s *Service) send(f fanout) {
	for _, m := range f.metrics {
		if err := s.hub.Call(m.account, m.region, "GraniteServiceVersion20100801", "PutMetricData",
			map[string]any{"Namespace": m.namespace, "MetricData": []any{m.datum}}, nil); err != nil {
			s.hub.Log.Debug("logs metric filter publish failed", "namespace", m.namespace, "err", err)
		}
	}
	s.sendSubs(f.subs)
}

func (s *Service) sendSubs(ds []delivery) {
	type logEvent struct {
		ID        string `json:"id"`
		Timestamp int64  `json:"timestamp"`
		Message   string `json:"message"`
	}
	for _, d := range ds {
		evs := make([]logEvent, len(d.events))
		for i, e := range d.events {
			evs[i] = logEvent{strconv.FormatInt(e.ID, 10), e.T, e.M}
		}
		raw, _ := json.Marshal(map[string]any{"messageType": "DATA_MESSAGE", "owner": d.owner, "logGroup": d.group, "logStream": d.stream,
			"subscriptionFilters": []string{d.filter}, "logEvents": evs})
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		zw.Write(raw)
		zw.Close()
		body := buf.String()
		if svc, _, _, _, _ := core.ParseARN(d.dest); svc == "lambda" {
			body = `{"awslogs":{"data":"` + base64.StdEncoding.EncodeToString(buf.Bytes()) + `"}}`
		}
		if err := s.hub.Deliver(d.dest, &core.Message{Body: body}); err != nil {
			s.hub.Log.Warn("logs subscription delivery failed", "destination", d.dest, "err", err)
		}
	}
}

// ---- tags ----

// tag edits the tags of a log group named by name or ARN and returns them.
func (s *Service) tag(c *core.Ctx, ref string, add map[string]string, remove []string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, ref, "")
	if err != nil {
		return nil, err
	}
	maps.Copy(g.Tags, add)
	for _, k := range remove {
		delete(g.Tags, k)
	}
	return map[string]any{"tags": maps.Clone(g.Tags)}, nil
}

func (s *Service) tagLogGroup(c *core.Ctx, in *struct {
	LogGroupName string
	Tags         map[string]string
}) (any, error) {
	_, err := s.tag(c, in.LogGroupName, in.Tags, nil)
	return nil, err
}

func (s *Service) untagLogGroup(c *core.Ctx, in *struct {
	LogGroupName string
	Tags         []string
}) (any, error) {
	_, err := s.tag(c, in.LogGroupName, nil, in.Tags)
	return nil, err
}

func (s *Service) listTagsLogGroup(c *core.Ctx, in *struct{ LogGroupName string }) (any, error) {
	return s.tag(c, in.LogGroupName, nil, nil)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        map[string]string
}) (any, error) {
	_, err := s.tag(c, in.ResourceArn, in.Tags, nil)
	return nil, err
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	_, err := s.tag(c, in.ResourceArn, nil, in.TagKeys)
	return nil, err
}

func (s *Service) listTagsForResource(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	return s.tag(c, in.ResourceArn, nil, nil)
}

// ---- metric and subscription filters ----

func (s *Service) putMetricFilter(c *core.Ctx, in *struct {
	LogGroupName, FilterName, FilterPattern string
	MetricTransformations                   []Transformation
}) (any, error) {
	if in.FilterName == "" || len(in.MetricTransformations) != 1 {
		return nil, invalidParam("A metric filter needs a filterName and exactly one metricTransformation.")
	}
	if _, err := compile(in.FilterPattern); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	created := time.Now().UnixMilli()
	if old := g.Metrics[in.FilterName]; old != nil {
		created = old.Created
	}
	g.Metrics[in.FilterName] = &MetricFilter{in.FilterName, in.FilterPattern, in.MetricTransformations, created}
	return nil, nil
}

func (s *Service) describeMetricFilters(c *core.Ctx, in *struct {
	LogGroupName, FilterNamePrefix, MetricName, MetricNamespace, NextToken string
	Limit                                                                  int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := s.groups(c)
	if in.LogGroupName != "" {
		g, err := s.group(c, in.LogGroupName, "")
		if err != nil {
			return nil, err
		}
		groups = []*Group{g}
	}
	type out struct {
		MetricFilter
		LogGroupName string `json:"logGroupName"`
	}
	var all []out
	for _, g := range groups {
		for _, name := range core.SortedKeys(g.Metrics) {
			f := g.Metrics[name]
			if strings.HasPrefix(name, in.FilterNamePrefix) && (in.MetricName == "" || slices.ContainsFunc(f.Transformations, func(t Transformation) bool {
				return t.MetricName == in.MetricName && (in.MetricNamespace == "" || t.MetricNamespace == in.MetricNamespace)
			})) {
				all = append(all, out{*f, g.Name})
			}
		}
	}
	start, end, next, err := page(len(all), in.NextToken, in.Limit, 50)
	if err != nil {
		return nil, err
	}
	return &struct {
		MetricFilters []out  `json:"metricFilters"`
		NextToken     string `json:"nextToken,omitempty"`
	}{append([]out{}, all[start:end]...), next}, nil
}

func (s *Service) deleteMetricFilter(c *core.Ctx, in *struct{ LogGroupName, FilterName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	if g.Metrics[in.FilterName] == nil {
		return nil, notFound("metric filter")
	}
	delete(g.Metrics, in.FilterName)
	return nil, nil
}

func (s *Service) putSubscriptionFilter(c *core.Ctx, in *struct {
	LogGroupName, FilterName, FilterPattern, DestinationArn, RoleArn, Distribution string
}) (any, error) {
	match, err := compile(in.FilterPattern)
	if err != nil {
		return nil, err
	}
	if _, _, _, _, ok := core.ParseARN(in.DestinationArn); !ok || in.FilterName == "" {
		return nil, invalidParam("A subscription filter needs a filterName and a destinationArn.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	created := time.Now().UnixMilli()
	if old := g.Subs[in.FilterName]; old != nil {
		created = old.Created
	} else if len(g.Subs) >= 2 {
		return nil, core.Errorf(400, "LimitExceededException", "Resource limit exceeded.")
	}
	dist := in.Distribution
	if dist == "" {
		dist = "ByLogStream"
	}
	g.Subs[in.FilterName] = &SubFilter{in.FilterName, in.FilterPattern, in.DestinationArn, in.RoleArn, dist, created, match}
	return nil, nil
}

func (s *Service) describeSubscriptionFilters(c *core.Ctx, in *struct {
	LogGroupName, FilterNamePrefix, NextToken string
	Limit                                     int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	type out struct {
		SubFilter
		LogGroupName string `json:"logGroupName"`
	}
	var all []out
	for _, name := range core.SortedKeys(g.Subs) {
		if strings.HasPrefix(name, in.FilterNamePrefix) {
			all = append(all, out{*g.Subs[name], g.Name})
		}
	}
	start, end, next, err := page(len(all), in.NextToken, in.Limit, 50)
	if err != nil {
		return nil, err
	}
	return &struct {
		SubscriptionFilters []out  `json:"subscriptionFilters"`
		NextToken           string `json:"nextToken,omitempty"`
	}{append([]out{}, all[start:end]...), next}, nil
}

func (s *Service) deleteSubscriptionFilter(c *core.Ctx, in *struct{ LogGroupName, FilterName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.LogGroupName, "")
	if err != nil {
		return nil, err
	}
	if g.Subs[in.FilterName] == nil {
		return nil, notFound("subscription filter")
	}
	delete(g.Subs, in.FilterName)
	return nil, nil
}

// ---- Logs Insights ----

// query is a finished Logs Insights query; queries run synchronously in StartQuery.
type query struct {
	id, text, group string
	created         int64
	results         [][]resultField
	stats           queryStats
}

func (s *Service) startQuery(c *core.Ctx, in *struct {
	LogGroupName                       string
	LogGroupNames, LogGroupIdentifiers []string
	StartTime, EndTime                 int64 // epoch seconds
	QueryString                        string
	Limit                              int
}) (any, error) {
	q, err := parseQuery(in.QueryString)
	if err != nil {
		return nil, core.Errorf(400, "MalformedQueryException", "%v", err)
	}
	names := append(slices.Clone(in.LogGroupNames), in.LogGroupIdentifiers...)
	if in.LogGroupName != "" {
		names = append(names, in.LogGroupName)
	}
	if len(names) == 0 {
		return nil, invalidParam("Specify a log group name or identifier.")
	}
	if in.EndTime < in.StartTime {
		return nil, invalidParam("End time should not be before the start time.")
	}
	limit := in.Limit
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var groups []*Group
	for _, n := range names {
		g, err := s.group(c, n, "")
		if err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	results, stats := q.run(groups, in.StartTime*1000, in.EndTime*1000+999, limit)
	id := core.UUID()
	// ponytail: the last 100 queries are kept in memory.
	s.queries = append(s.queries, &query{id, in.QueryString, groups[0].Name, time.Now().UnixMilli(), results, stats})
	if len(s.queries) > 100 {
		s.queries = s.queries[1:]
	}
	return map[string]string{"queryId": id}, nil
}

// query finds a recent query by id. Lock held.
func (s *Service) query(id string) (*query, error) {
	for _, q := range s.queries {
		if q.id == id {
			return q, nil
		}
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "Query %s does not exist.", id)
}

func (s *Service) getQueryResults(c *core.Ctx, in *struct{ QueryId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.query(in.QueryId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": "Complete", "queryLanguage": "CWLI", "results": q.results, "statistics": q.stats}, nil
}

func (s *Service) stopQuery(c *core.Ctx, in *struct{ QueryId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.query(in.QueryId)
	if err != nil {
		return nil, err
	}
	return map[string]bool{"success": false}, nil // it already finished
}

func (s *Service) describeQueries(c *core.Ctx, in *struct{ LogGroupName, Status string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type info struct {
		QueryID      string `json:"queryId"`
		QueryString  string `json:"queryString"`
		Status       string `json:"status"`
		CreateTime   int64  `json:"createTime"`
		LogGroupName string `json:"logGroupName"`
	}
	out := []info{}
	for _, q := range slices.Backward(s.queries) {
		if (in.LogGroupName == "" || q.group == in.LogGroupName) && (in.Status == "" || in.Status == "Complete") {
			out = append(out, info{q.id, q.text, "Complete", q.created, q.group})
		}
	}
	return map[string]any{"queries": out}, nil
}

// ---- console ----

// Event is a log event as the console shows it.
type Event struct {
	Timestamp int64  `json:"timestamp"` // epoch millis
	Stream    string `json:"stream"`
	Message   string `json:"message"`
}

// Tail returns a group's events newer than after (epoch millis) across its
// streams, oldest first, keeping the last limit (all when limit <= 0).
func (s *Service) Tail(account, region, group string, after int64, limit int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Groups[key(account, region, group)]
	if g == nil {
		return nil
	}
	return g.tail(after, limit)
}

// tail is Tail with the lock held.
func (g *Group) tail(after int64, limit int) []Event {
	g.expire(time.Now().UnixMilli())
	type hit struct {
		stream string
		e      event
	}
	var hits []hit
	for name, st := range g.Streams {
		i := sort.Search(len(st.Events), func(i int) bool { return st.Events[i].T > after })
		for _, e := range st.Events[i:] {
			hits = append(hits, hit{name, e})
		}
	}
	slices.SortFunc(hits, func(a, b hit) int { return cmpEvent(a.e, b.e) })
	if limit > 0 && len(hits) > limit {
		hits = hits[len(hits)-limit:]
	}
	out := make([]Event, len(hits))
	for i, h := range hits {
		out[i] = Event{h.e.T, h.stream, h.e.M}
	}
	return out
}

// Resources lists log groups for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Log groups", Columns: []string{"Name", "Region", "Account", "Streams", "Events", "Retention"}}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		n := 0
		for _, st := range g.Streams {
			n += len(st.Events)
		}
		retention := "never expire"
		if g.Retention > 0 {
			retention = strconv.Itoa(g.Retention) + " days"
		}
		t.Rows = append(t.Rows, []string{g.Name, g.Region, g.Account, strconv.Itoa(len(g.Streams)), strconv.Itoa(n), retention})
	}
	return []core.Table{t}
}

// Inspect returns a log group's metadata, streams, filters and its last 100 events.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Groups) {
		if g := s.st.Groups[k]; g.Name == id {
			events := g.tail(0, 100)
			return map[string]any{"logGroup": g.out(), "streams": core.SortedKeys(g.Streams), "tags": maps.Clone(g.Tags),
				"metricFilters": core.SortedKeys(g.Metrics), "subscriptionFilters": core.SortedKeys(g.Subs), "events": events}
		}
	}
	return nil
}

// TaggedResources lists log groups (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, g := range s.st.Groups {
		if g.Account == account && g.Region == region {
			out[g.arn()] = maps.Clone(g.Tags)
		}
	}
	return out
}
