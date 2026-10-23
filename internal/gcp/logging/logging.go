// Package logging emulates Google Cloud Logging (v2) over gRPC and its REST
// mirror: log entries (write, list with the Logging query language, tail),
// logs, sinks that deliver to Pub/Sub, Cloud Storage and BigQuery, exclusions,
// log-based metrics and log buckets. Cloud Run, Cloud Functions and Cloud Run
// jobs send their output here through Hub.Deliver.
package logging

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	lpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxEntries bounds the entries kept; the oldest go first.
// ponytail: one global cap, no per-bucket retention; make it per bucket (retentionDays) if tests need expiry.
const maxEntries = 50000

// Service is the Cloud Logging emulator.
type Service struct {
	lpb.UnimplementedLoggingServiceV2Server
	lpb.UnimplementedConfigServiceV2Server
	lpb.UnimplementedMetricsServiceV2Server
	hub  *core.Hub
	rest *gapi.REST
	mu   sync.Mutex
	st   state
	tail map[*tailer]bool
}

type state struct {
	entries    []*lpb.LogEntry // oldest write first
	sinks      map[string]*lpb.LogSink
	exclusions map[string]*lpb.LogExclusion
	metrics    map[string]*lpb.LogMetric
	buckets    map[string]*lpb.LogBucket
}

func newState() state {
	return state{sinks: map[string]*lpb.LogSink{}, exclusions: map[string]*lpb.LogExclusion{}, metrics: map[string]*lpb.LogMetric{}, buckets: map[string]*lpb.LogBucket{}}
}

// New creates the Cloud Logging service. Hub.Deliver to
// //logging.googleapis.com/projects/<p>/logs/<id> writes an entry (see deliver).
func New(h *core.Hub) *Service {
	s := &Service{hub: h, st: newState(), tail: map[*tailer]bool{}}
	h.OnDeliver("logging", s.deliver)
	cfg, met, log := &lpb.ConfigServiceV2_ServiceDesc, &lpb.MetricsServiceV2_ServiceDesc, &lpb.LoggingServiceV2_ServiceDesc
	b := "locations/*/buckets"
	s.rest = &gapi.REST{Versions: []string{"v2"}, Collections: []string{"logs", "sinks", "exclusions", "metrics", "buckets"}, Desc: cfg, Impl: s,
		Routes: map[string]gapi.Route{
			"GET logs":                  {Method: "ListLogs", Field: "parent", Desc: log},
			"DELETE logs/*":             {Method: "DeleteLog", Field: "log_name", Desc: log},
			"GET sinks":                 {Method: "ListSinks", Field: "parent"},
			"POST sinks":                {Method: "CreateSink", Field: "parent", Body: "sink"},
			"GET sinks/*":               {Method: "GetSink", Field: "sink_name"},
			"PUT sinks/*":               {Method: "UpdateSink", Field: "sink_name", Body: "sink"},
			"PATCH sinks/*":             {Method: "UpdateSink", Field: "sink_name", Body: "sink"},
			"DELETE sinks/*":            {Method: "DeleteSink", Field: "sink_name"},
			"GET exclusions":            {Method: "ListExclusions", Field: "parent"},
			"POST exclusions":           {Method: "CreateExclusion", Field: "parent", Body: "exclusion"},
			"GET exclusions/*":          {Method: "GetExclusion", Field: "name"},
			"PATCH exclusions/*":        {Method: "UpdateExclusion", Field: "name", Body: "exclusion"},
			"DELETE exclusions/*":       {Method: "DeleteExclusion", Field: "name"},
			"GET metrics":               {Method: "ListLogMetrics", Field: "parent", Desc: met},
			"POST metrics":              {Method: "CreateLogMetric", Field: "parent", Body: "metric", Desc: met},
			"GET metrics/*":             {Method: "GetLogMetric", Field: "metric_name", Desc: met},
			"PUT metrics/*":             {Method: "UpdateLogMetric", Field: "metric_name", Body: "metric", Desc: met},
			"DELETE metrics/*":          {Method: "DeleteLogMetric", Field: "metric_name", Desc: met},
			"GET " + b:                  {Method: "ListBuckets", Field: "parent"},
			"POST " + b:                 {Method: "CreateBucket", Field: "parent", Body: "bucket"},
			"GET " + b + "/*":           {Method: "GetBucket", Field: "name"},
			"PATCH " + b + "/*":         {Method: "UpdateBucket", Field: "name", Body: "bucket"},
			"DELETE " + b + "/*":        {Method: "DeleteBucket", Field: "name"},
			"POST " + b + "/*:undelete": {Method: "UndeleteBucket", Field: "name"},
		}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "logging", Cloud: core.GCP, Title: "Cloud Logging", Match: s.match,
		Targets: []string{"google.logging.v2.LoggingServiceV2", "google.logging.v2.ConfigServiceV2", "google.logging.v2.MetricsServiceV2"}}
}

// match claims /v2/projects/<p>/{logs,sinks,...} and the two top-level entries calls.
func (s *Service) match(r *http.Request) bool {
	return s.rest.Match(r) || r.Method == http.MethodPost && (r.URL.Path == "/v2/entries:write" || r.URL.Path == "/v2/entries:list")
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	lpb.RegisterLoggingServiceV2Server(g, s)
	lpb.RegisterConfigServiceV2Server(g, s)
	lpb.RegisterMetricsServiceV2Server(g, s)
}

// ServeHTTP serves the REST mirror; entries:write and entries:list have no
// project in their path, so they are made here.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v2/entries:write":
		req := &lpb.WriteLogEntriesRequest{}
		s.entries(w, r, "WriteLogEntries", req, func() (proto.Message, error) { return s.WriteLogEntries(r.Context(), req) })
	case "/v2/entries:list":
		req := &lpb.ListLogEntriesRequest{}
		s.entries(w, r, "ListLogEntries", req, func() (proto.Message, error) { return s.ListLogEntries(r.Context(), req) })
	default:
		s.rest.ServeHTTP(w, r)
	}
}

func (s *Service) entries(w http.ResponseWriter, r *http.Request, op string, req proto.Message, call func() (proto.Message, error)) {
	c := core.C(r)
	c.Op = op
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(core.Body(r), req); err != nil {
		gapi.Err(w, c, gapi.Invalid("Invalid JSON payload received. %s", gapi.PlainProto(core.Body(r), err)))
		return
	}
	out, err := call()
	if err != nil {
		gapi.Err(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

// ---- persistence ----

type saved struct {
	Entries    [][]byte          `json:"entries,omitempty"` // proto wire format: payloads may be Any of unknown types
	Sinks      map[string][]byte `json:"sinks,omitempty"`
	Exclusions map[string][]byte `json:"exclusions,omitempty"`
	Metrics    map[string][]byte `json:"metrics,omitempty"`
	Buckets    map[string][]byte `json:"buckets,omitempty"`
}

func wire[M proto.Message](m map[string]M) map[string][]byte {
	out := map[string][]byte{}
	for k, v := range m {
		out[k], _ = proto.Marshal(v)
	}
	return out
}

func unwire[M proto.Message](in map[string][]byte, mk func() M) (map[string]M, error) {
	out := map[string]M{}
	for k, b := range in {
		m := mk()
		if err := proto.Unmarshal(b, m); err != nil {
			return nil, err
		}
		out[k] = m
	}
	return out, nil
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{Sinks: wire(s.st.sinks), Exclusions: wire(s.st.exclusions), Metrics: wire(s.st.metrics), Buckets: wire(s.st.buckets)}
	for _, e := range s.st.entries {
		b, _ := proto.Marshal(e)
		out.Entries = append(out.Entries, b)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	st := newState()
	var err error
	if st.sinks, err = unwire(in.Sinks, func() *lpb.LogSink { return &lpb.LogSink{} }); err != nil {
		return err
	}
	if st.exclusions, err = unwire(in.Exclusions, func() *lpb.LogExclusion { return &lpb.LogExclusion{} }); err != nil {
		return err
	}
	if st.metrics, err = unwire(in.Metrics, func() *lpb.LogMetric { return &lpb.LogMetric{} }); err != nil {
		return err
	}
	if st.buckets, err = unwire(in.Buckets, func() *lpb.LogBucket { return &lpb.LogBucket{} }); err != nil {
		return err
	}
	for _, raw := range in.Entries {
		e := &lpb.LogEntry{}
		if err := proto.Unmarshal(raw, e); err != nil {
			return err
		}
		st.entries = append(st.entries, e)
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- console ----

// Resources lists logs, recent entries and configuration.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := map[string]int{}
	for _, e := range s.st.entries {
		counts[e.LogName]++
	}
	logs := core.Table{Name: "Logs", Log: true, Columns: []string{"Log", "Entries"}}
	for _, n := range core.SortedKeys(counts) {
		logs.Rows = append(logs.Rows, []string{n, strconv.Itoa(counts[n])})
	}
	ents := core.Table{Name: "Entries", Log: true, Columns: []string{"Time", "Severity", "Log", "Resource", "Message"}}
	for i := len(s.st.entries) - 1; i >= 0 && len(ents.Rows) < 200; i-- {
		e := s.st.entries[i]
		ents.Rows = append(ents.Rows, []string{e.Timestamp.AsTime().Format(time.DateTime), e.Severity.String(), e.LogName, e.Resource.GetType(), preview(e)})
	}
	sinks := core.Table{Name: "Sinks", Columns: []string{"Name", "Destination", "Filter", "Disabled"}}
	for _, n := range core.SortedKeys(s.st.sinks) {
		k := s.st.sinks[n]
		sinks.Rows = append(sinks.Rows, []string{n, k.Destination, k.Filter, strconv.FormatBool(k.Disabled)})
	}
	excl := core.Table{Name: "Exclusions", Columns: []string{"Name", "Filter", "Disabled"}}
	for _, n := range core.SortedKeys(s.st.exclusions) {
		x := s.st.exclusions[n]
		excl.Rows = append(excl.Rows, []string{n, x.Filter, strconv.FormatBool(x.Disabled)})
	}
	met := core.Table{Name: "Log-based metrics", Columns: []string{"Name", "Filter"}}
	for _, n := range core.SortedKeys(s.st.metrics) {
		met.Rows = append(met.Rows, []string{n, s.st.metrics[n].Filter})
	}
	buckets := core.Table{Name: "Buckets", Columns: []string{"Name", "Retention days", "State"}}
	for _, n := range core.SortedKeys(s.st.buckets) {
		bk := s.st.buckets[n]
		buckets.Rows = append(buckets.Rows, []string{n, strconv.Itoa(int(bk.RetentionDays)), bk.LifecycleState.String()})
	}
	return []core.Table{logs, ents, sinks, excl, met, buckets}
}

// Inspect returns the JSON of a sink, exclusion, metric or bucket, or of the
// newest entry with an insert id.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m proto.Message
	switch table {
	case "Sinks":
		m = s.st.sinks[id]
	case "Exclusions":
		m = s.st.exclusions[id]
	case "Log-based metrics":
		m = s.st.metrics[id]
	case "Buckets":
		m = s.st.buckets[id]
	}
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}
	b, _ := protojson.Marshal(m)
	return json.RawMessage(b)
}

// preview is the one-line text of an entry.
func preview(e *lpb.LogEntry) string {
	switch p := e.Payload.(type) {
	case *lpb.LogEntry_TextPayload:
		return p.TextPayload
	case *lpb.LogEntry_JsonPayload:
		b, _ := protojson.Marshal(p.JsonPayload)
		return string(b)
	}
	return ""
}
