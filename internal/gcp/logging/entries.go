package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	lpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var logNameRE = regexp.MustCompile(`^((?:projects|organizations|folders|billingAccounts)/[^/]+)/logs/.+$`)

// parent is the projects/<p> (or organizations/<o>, ...) a log name belongs to.
func parent(logName string) string {
	if m := logNameRE.FindStringSubmatch(logName); m != nil {
		return m[1]
	}
	return ""
}

func (s *Service) WriteLogEntries(_ context.Context, req *lpb.WriteLogEntriesRequest) (*lpb.WriteLogEntriesResponse, error) {
	now := time.Now()
	var in []*lpb.LogEntry
	for i, e := range req.Entries {
		e = proto.Clone(e).(*lpb.LogEntry)
		if e.LogName == "" {
			e.LogName = req.LogName
		}
		if logNameRE.FindStringSubmatch(e.LogName) == nil {
			return nil, gapi.Invalid("Invalid log name in entries[%d]: %q (want projects/<project>/logs/<log>)", i, e.LogName)
		}
		if e.Resource == nil {
			e.Resource = proto.Clone(req.Resource).(*monitoredres.MonitoredResource)
		}
		if e.Resource == nil {
			e.Resource = &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": strings.Split(e.LogName, "/")[1]}}
		}
		for k, v := range req.Labels {
			if _, ok := e.Labels[k]; !ok {
				if e.Labels == nil {
					e.Labels = map[string]string{}
				}
				e.Labels[k] = v
			}
		}
		if e.Timestamp == nil {
			e.Timestamp = timestamppb.New(now)
		}
		if e.InsertId == "" {
			e.InsertId = core.RandHex(8)
		}
		e.ReceiveTimestamp = timestamppb.New(now)
		in = append(in, e)
	}
	if !req.DryRun {
		s.store(in)
	}
	return &lpb.WriteLogEntriesResponse{}, nil
}

// store keeps entries (the exclusions that match drop them from storage), hands
// them to tails and delivers them to the sinks. Called without the lock.
func (s *Service) store(in []*lpb.LogEntry) {
	if len(in) == 0 {
		return
	}
	s.mu.Lock()
	type sink struct {
		s       *lpb.LogSink
		project string
		f       func(*lpb.LogEntry) bool
	}
	var sinks []sink
	for _, n := range core.SortedKeys(s.st.sinks) {
		if k := s.st.sinks[n]; !k.Disabled {
			if f, err := entryFilter(k.Filter); err == nil {
				sinks = append(sinks, sink{k, sinkParent(n), f})
			}
		}
	}
	var excl []func(*lpb.LogEntry) bool
	for _, x := range s.st.exclusions {
		if f, err := entryFilter(x.Filter); err == nil && !x.Disabled {
			excl = append(excl, f)
		}
	}
	type delivery struct {
		sink  *lpb.LogSink
		entry *lpb.LogEntry
	}
	var out []delivery
	for _, e := range in {
		kept := true
		for _, f := range excl {
			kept = kept && !f(e)
		}
		if kept {
			s.st.entries = append(s.st.entries, e)
		}
		for t := range s.tail {
			t.offer(e)
		}
		for _, k := range sinks {
			if parent(e.LogName) == k.project && k.f(e) && !excluded(k.s, e) {
				out = append(out, delivery{k.s, e})
			}
		}
	}
	if n := len(s.st.entries) - maxEntries; n > 0 {
		s.st.entries = append([]*lpb.LogEntry(nil), s.st.entries[n:]...)
	}
	s.mu.Unlock()
	for _, d := range out {
		s.send(d.sink, d.entry)
	}
}

// excluded is whether one of a sink's own exclusions drops the entry.
func excluded(k *lpb.LogSink, e *lpb.LogEntry) bool {
	for _, x := range k.Exclusions {
		if f, err := entryFilter(x.Filter); err == nil && !x.Disabled && f(e) {
			return true
		}
	}
	return false
}

// ---- filters ----

// severities are the rank of each severity name; numbers are accepted too.
func severityRank(v string) (int, bool) {
	if n, ok := ltype.LogSeverity_value[strings.ToUpper(v)]; ok {
		return int(n), true
	}
	n, err := strconv.ParseInt(v, 10, 32) // LogSeverity is an int32: a bigger number is not a severity, not a wrapped one
	return int(n), err == nil
}

// entryJSON is the entry as the REST API shows it, for filters to walk.
func entryJSON(e *lpb.LogEntry) map[string]any {
	b, err := protojson.Marshal(e)
	if err != nil { // a payload of an unknown type
		c := proto.Clone(e).(*lpb.LogEntry)
		c.Payload = nil
		b, _ = protojson.Marshal(c)
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// lookup finds the value at a dotted path; a key may itself hold dots (labels."a.b").
func lookup(m map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	for i := len(parts); i >= 1; i-- {
		v, ok := m[strings.Join(parts[:i], ".")]
		if !ok {
			continue
		}
		if i == len(parts) {
			return v, true
		}
		if child, isMap := v.(map[string]any); isMap {
			if r, found := lookup(child, strings.Join(parts[i:], ".")); found {
				return r, true
			}
		}
	}
	return nil, false
}

func asText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// contains reports whether the lower-cased needle appears in any string of v.
func contains(v any, needle string) bool {
	switch x := v.(type) {
	case map[string]any:
		for _, c := range x {
			if contains(c, needle) {
				return true
			}
		}
	case []any:
		for _, c := range x {
			if contains(c, needle) {
				return true
			}
		}
	default:
		return strings.Contains(strings.ToLower(asText(v)), needle)
	}
	return false
}

// textFields are what a bare word searches: the payloads and the labels.
var textFields = []string{"textPayload", "jsonPayload", "protoPayload", "labels", "resource", "logName"}

// compare orders got and want: as severities, timestamps, numbers or text.
func compare(field string, got any, want string) (int, bool) {
	g := asText(got)
	switch {
	case field == "severity":
		a, ok1 := severityRank(g)
		b, ok2 := severityRank(want)
		return a - b, ok1 && ok2
	case field == "timestamp" || field == "receiveTimestamp":
		a, err1 := time.Parse(time.RFC3339Nano, g)
		b, err2 := time.Parse(time.RFC3339Nano, want)
		if err2 != nil {
			b, err2 = time.Parse("2006-01-02", want)
		}
		if err1 == nil && err2 == nil {
			return a.Compare(b), true
		}
	}
	a, err1 := strconv.ParseFloat(g, 64)
	b, err2 := strconv.ParseFloat(want, 64)
	if err1 == nil && err2 == nil {
		switch {
		case a < b:
			return -1, true
		case a > b:
			return 1, true
		}
		return 0, true
	}
	return strings.Compare(strings.ToLower(g), strings.ToLower(want)), true
}

// entryFilter compiles a Logging query. An error is an invalid filter.
func entryFilter(expr string) (func(*lpb.LogEntry) bool, error) {
	f, err := gapi.ParseFilter(expr)
	if err != nil {
		return nil, gapi.Invalid("%v", err)
	}
	return func(e *lpb.LogEntry) bool {
		m := entryJSON(e)
		return f.Match(func(field, op, want string) bool {
			if op == "()" {
				args := strings.Split(want, "\x00")
				switch strings.ToLower(field) {
				case "log_id":
					return e.LogName != "" && (strings.HasSuffix(e.LogName, "/logs/"+args[0]) || strings.HasSuffix(e.LogName, "/logs/"+url.PathEscape(args[0])))
				case "search":
					return contains(m, strings.ToLower(args[0]))
				}
				return false
			}
			got, ok := lookup(m, field)
			if field == "severity" && !ok {
				got, ok = "DEFAULT", true
			}
			if op == ":" && want == "*" {
				return ok
			}
			if !ok {
				return false
			}
			switch op {
			case "=":
				c, _ := compare(field, got, want)
				return c == 0
			case "!=":
				c, _ := compare(field, got, want)
				return c != 0
			case ">", ">=", "<", "<=":
				c, comparable := compare(field, got, want)
				return comparable && (op == ">" && c > 0 || op == ">=" && c >= 0 || op == "<" && c < 0 || op == "<=" && c <= 0)
			case ":":
				if field == "severity" || field == "timestamp" {
					c, _ := compare(field, got, want)
					return c == 0
				}
				return contains(got, strings.ToLower(want))
			case "=~", "!~":
				re, err := regexp.Compile(want)
				return err == nil && re.MatchString(asText(got)) == (op == "=~")
			}
			return false
		}, func(word string) bool {
			word = strings.ToLower(word)
			for _, k := range textFields {
				if v, ok := m[k]; ok && contains(v, word) {
					return true
				}
			}
			return false
		})
	}, nil
}

// ---- list ----

// find lists the stored entries of the parents (projects/<p>) that pass the filter.
func (s *Service) find(parents []string, filter string) ([]*lpb.LogEntry, error) {
	if len(parents) == 0 {
		return nil, gapi.Invalid("resource_names is required")
	}
	for _, p := range parents {
		if !regexp.MustCompile(`^(projects|organizations|folders|billingAccounts)/[^/]+$`).MatchString(p) {
			return nil, gapi.Invalid("Invalid resource name %q (want projects/<project>)", p)
		}
	}
	f, err := entryFilter(filter)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	all := append([]*lpb.LogEntry(nil), s.st.entries...)
	s.mu.Unlock()
	var out []*lpb.LogEntry
	for _, e := range all {
		if slicesContains(parents, parent(e.LogName)) && f(e) {
			out = append(out, e)
		}
	}
	return out, nil
}

func slicesContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func (s *Service) ListLogEntries(_ context.Context, req *lpb.ListLogEntriesRequest) (*lpb.ListLogEntriesResponse, error) {
	got, err := s.find(req.ResourceNames, req.Filter)
	if err != nil {
		return nil, err
	}
	desc := false
	switch strings.ToLower(strings.TrimSpace(req.OrderBy)) {
	case "", "timestamp", "timestamp asc":
	case "timestamp desc":
		desc = true
	default:
		return nil, gapi.Invalid("Invalid order_by %q: pogi orders by timestamp, asc or desc", req.OrderBy)
	}
	sort.SliceStable(got, func(i, j int) bool {
		a, b := got[i].Timestamp.AsTime(), got[j].Timestamp.AsTime()
		return desc && a.After(b) || !desc && a.Before(b)
	})
	items, next := gapi.Page(got, min(max(req.PageSize, 0), 1000), req.PageToken)
	if req.PageSize == 0 {
		items, next = gapi.Page(got, 1000, req.PageToken)
	}
	out := make([]*lpb.LogEntry, len(items))
	for i, e := range items {
		out[i] = proto.Clone(e).(*lpb.LogEntry)
	}
	return &lpb.ListLogEntriesResponse{Entries: out, NextPageToken: next}, nil
}

func (s *Service) ListLogs(_ context.Context, req *lpb.ListLogsRequest) (*lpb.ListLogsResponse, error) {
	parents := append([]string{req.Parent}, req.ResourceNames...)
	s.mu.Lock()
	seen := map[string]bool{}
	for _, e := range s.st.entries {
		if slicesContains(parents, parent(e.LogName)) {
			seen[e.LogName] = true
		}
	}
	s.mu.Unlock()
	names, next := gapi.Page(core.SortedKeys(seen), req.PageSize, req.PageToken)
	return &lpb.ListLogsResponse{LogNames: names, NextPageToken: next}, nil
}

func (s *Service) DeleteLog(_ context.Context, req *lpb.DeleteLogRequest) (*emptypb.Empty, error) {
	if logNameRE.FindStringSubmatch(req.LogName) == nil {
		return nil, gapi.Invalid("Invalid log name %q", req.LogName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.st.entries[:0:0]
	for _, e := range s.st.entries {
		if e.LogName != req.LogName {
			kept = append(kept, e)
		}
	}
	if len(kept) == len(s.st.entries) {
		return nil, gapi.NotFound("Log %s does not exist", req.LogName)
	}
	s.st.entries = kept
	return &emptypb.Empty{}, nil
}

// ---- tail ----

// tailer is one TailLogEntries stream.
type tailer struct {
	parents []string
	match   func(*lpb.LogEntry) bool
	ch      chan *lpb.LogEntry
	dropped chan struct{}
}

// offer queues an entry for the stream; when it is full the entry is dropped
// and counted, which the stream reports as suppression.
func (t *tailer) offer(e *lpb.LogEntry) {
	if !slicesContains(t.parents, parent(e.LogName)) || !t.match(e) {
		return
	}
	select {
	case t.ch <- e:
	default:
		select {
		case t.dropped <- struct{}{}:
		default:
		}
	}
}

func (s *Service) TailLogEntries(stream lpb.LoggingServiceV2_TailLogEntriesServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(req.ResourceNames) == 0 {
		return gapi.Invalid("resource_names is required")
	}
	f, err := entryFilter(req.Filter)
	if err != nil {
		return err
	}
	t := &tailer{parents: req.ResourceNames, match: f, ch: make(chan *lpb.LogEntry, 1000), dropped: make(chan struct{}, 1)}
	s.mu.Lock()
	s.tail[t] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.tail, t)
		s.mu.Unlock()
	}()
	gone := make(chan struct{}) // the client closes its side
	core.Go("logging.TailLogEntries", func() {
		defer close(gone)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	})
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-gone:
			return nil
		case <-tick.C:
		}
		var batch []*lpb.LogEntry
		for drain := true; drain; {
			select {
			case e := <-t.ch:
				batch = append(batch, proto.Clone(e).(*lpb.LogEntry))
			default:
				drain = false
			}
		}
		resp := &lpb.TailLogEntriesResponse{Entries: batch}
		select {
		case <-t.dropped:
			resp.SuppressionInfo = []*lpb.TailLogEntriesResponse_SuppressionInfo{{Reason: lpb.TailLogEntriesResponse_SuppressionInfo_NOT_CONSUMED}}
		default:
		}
		if len(batch) > 0 || len(resp.SuppressionInfo) > 0 {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// ---- Hub.Deliver ----

// deliver writes the entry a Deliver makes: target is projects/<p>/logs/<id>,
// Body the text (or the JSON object, which becomes jsonPayload with its
// "severity" key as the severity). Attributes "resource.type",
// "resource.label.<k>", "label.<k>" and "severity" fill the rest.
func (s *Service) deliver(target string, m *core.Message) error {
	if logNameRE.FindStringSubmatch(target) == nil {
		return fmt.Errorf("invalid log name %q: want projects/<project>/logs/<id>", target)
	}
	e := &lpb.LogEntry{LogName: target, Resource: &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{}}}
	for k, a := range m.Attributes {
		switch {
		case k == "resource.type":
			e.Resource.Type = a.StringValue
		case strings.HasPrefix(k, "resource.label."):
			e.Resource.Labels[strings.TrimPrefix(k, "resource.label.")] = a.StringValue
		case strings.HasPrefix(k, "label."):
			if e.Labels == nil {
				e.Labels = map[string]string{}
			}
			e.Labels[strings.TrimPrefix(k, "label.")] = a.StringValue
		case k == "severity":
			if n, ok := severityRank(a.StringValue); ok {
				e.Severity = ltype.LogSeverity(n)
			}
		}
	}
	if len(e.Resource.Labels) == 0 {
		e.Resource.Labels["project_id"] = strings.Split(target, "/")[1]
	}
	var obj map[string]any
	if json.Unmarshal([]byte(m.Body), &obj) == nil && obj != nil {
		if st, err := structpb.NewStruct(obj); err == nil {
			e.Payload = &lpb.LogEntry_JsonPayload{JsonPayload: st}
			if sv, ok := obj["severity"].(string); ok {
				if n, ok := severityRank(sv); ok {
					e.Severity = ltype.LogSeverity(n)
				}
			}
		}
	}
	if e.Payload == nil {
		e.Payload = &lpb.LogEntry_TextPayload{TextPayload: m.Body}
	}
	now := time.Now()
	e.Timestamp, e.ReceiveTimestamp, e.InsertId = timestamppb.New(now), timestamppb.New(now), core.RandHex(8)
	s.store([]*lpb.LogEntry{e})
	return nil
}
