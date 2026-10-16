package logging

import (
	"context"
	"hash/fnv"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	lpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	projectRE = regexp.MustCompile(`^projects/[^/]+$`)
	idRE      = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)
)

func checkParent(p string) error {
	if !projectRE.MatchString(p) {
		return gapi.Invalid("Invalid parent %q (want projects/<project>)", p)
	}
	return nil
}

// ---- sinks ----

// destinations are the prefixes a sink's destination can have.
var destinations = []string{"storage.googleapis.com/", "bigquery.googleapis.com/projects/", "pubsub.googleapis.com/projects/", "logging.googleapis.com/projects/"}

func checkSink(k *lpb.LogSink) error {
	if !idRE.MatchString(k.Name) {
		return gapi.Invalid("Invalid sink name %q: use 1-100 letters, digits, - _ or .", k.Name)
	}
	ok := false
	for _, d := range destinations {
		ok = ok || strings.HasPrefix(k.Destination, d) && len(k.Destination) > len(d)
	}
	if !ok {
		return gapi.Invalid("Invalid destination %q: want storage.googleapis.com/<bucket>, bigquery.googleapis.com/projects/<p>/datasets/<d>, pubsub.googleapis.com/projects/<p>/topics/<t> or logging.googleapis.com/projects/<p>/locations/<l>/buckets/<b>", k.Destination)
	}
	if _, err := entryFilter(k.Filter); err != nil {
		return err
	}
	for _, x := range k.Exclusions {
		if _, err := entryFilter(x.Filter); err != nil {
			return err
		}
	}
	return nil
}

// writerIdentity is the service account a sink writes as; a unique one has the project number in it.
func writerIdentity(project string, unique bool) string {
	if !unique {
		return "serviceAccount:cloud-logs@system.gserviceaccount.com"
	}
	h := fnv.New64a()
	h.Write([]byte(project))
	return "serviceAccount:service-" + strconv.FormatUint(h.Sum64()%900000000000+100000000000, 10) + "@gcp-sa-logging.iam.gserviceaccount.com"
}

func (s *Service) CreateSink(_ context.Context, req *lpb.CreateSinkRequest) (*lpb.LogSink, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	k := proto.Clone(req.Sink).(*lpb.LogSink)
	if err := checkSink(k); err != nil {
		return nil, err
	}
	key := req.Parent + "/sinks/" + k.Name // the sink keeps its short name, as at Google
	k.WriterIdentity = writerIdentity(req.Parent, req.UniqueWriterIdentity)
	k.OutputVersionFormat = lpb.LogSink_V2
	k.CreateTime, k.UpdateTime = timestamppb.Now(), timestamppb.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.sinks[key]; ok {
		return nil, gapi.Exists("Sink %s already exists", key)
	}
	s.st.sinks[key] = k
	return proto.Clone(k).(*lpb.LogSink), nil
}

func (s *Service) GetSink(_ context.Context, req *lpb.GetSinkRequest) (*lpb.LogSink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.st.sinks[req.SinkName]
	if !ok {
		return nil, gapi.NotFound("Sink %s does not exist", req.SinkName)
	}
	return proto.Clone(k).(*lpb.LogSink), nil
}

func (s *Service) ListSinks(_ context.Context, req *lpb.ListSinksRequest) (*lpb.ListSinksResponse, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	var all []*lpb.LogSink
	for _, n := range core.SortedKeys(s.st.sinks) {
		if strings.HasPrefix(n, req.Parent+"/sinks/") {
			all = append(all, proto.Clone(s.st.sinks[n]).(*lpb.LogSink))
		}
	}
	s.mu.Unlock()
	items, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &lpb.ListSinksResponse{Sinks: items, NextPageToken: next}, nil
}

func (s *Service) UpdateSink(_ context.Context, req *lpb.UpdateSinkRequest) (*lpb.LogSink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.st.sinks[req.SinkName]
	if !ok {
		return nil, gapi.NotFound("Sink %s does not exist", req.SinkName)
	}
	k := proto.Clone(old).(*lpb.LogSink)
	if err := gapi.Update(k, req.Sink, req.UpdateMask.GetPaths(), "destination", "filter", "description", "disabled", "exclusions", "include_children", "bigquery_options"); err != nil {
		return nil, err
	}
	if err := checkSink(&lpb.LogSink{Name: "x", Destination: k.Destination, Filter: k.Filter, Exclusions: k.Exclusions}); err != nil {
		return nil, err
	}
	if req.UniqueWriterIdentity {
		k.WriterIdentity = writerIdentity(sinkParent(req.SinkName), true)
	}
	k.UpdateTime = timestamppb.Now()
	s.st.sinks[req.SinkName] = k
	return proto.Clone(k).(*lpb.LogSink), nil
}

func sinkParent(name string) string { return strings.Join(strings.Split(name, "/")[:2], "/") }

func (s *Service) DeleteSink(_ context.Context, req *lpb.DeleteSinkRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.sinks[req.SinkName]; !ok {
		return nil, gapi.NotFound("Sink %s does not exist", req.SinkName)
	}
	delete(s.st.sinks, req.SinkName)
	return &emptypb.Empty{}, nil
}

// send delivers an entry to a sink's destination through the hub.
func (s *Service) send(k *lpb.LogSink, e *lpb.LogEntry) {
	e = proto.Clone(e).(*lpb.LogEntry)
	b, err := protojson.Marshal(e)
	if err != nil { // a payload of an unknown type
		e.Payload = nil
		b, _ = protojson.Marshal(e)
	}
	dest, logID := k.Destination, e.LogName[strings.Index(e.LogName, "/logs/")+len("/logs/"):]
	switch {
	case strings.HasPrefix(dest, "pubsub.googleapis.com/"):
		err = s.hub.Deliver("//"+dest, &core.Message{Body: string(b)})
	case strings.HasPrefix(dest, "bigquery.googleapis.com/"):
		// The table is the log's name with / and . as _, as BigQuery sinks do (the log id is URL-encoded).
		id, _ := url.PathUnescape(logID)
		err = s.hub.Deliver("//"+dest, &core.Message{Body: string(b), Subject: strings.NewReplacer("/", "_", ".", "_", "-", "_").Replace(id)})
	case strings.HasPrefix(dest, "storage.googleapis.com/"):
		bucket, _, _ := strings.Cut(strings.TrimPrefix(dest, "storage.googleapis.com/"), "/")
		ts := e.Timestamp.AsTime().UTC()
		name := logID + "/" + ts.Format("2006/01/02") + "/" + ts.Format("15:04:05.000000000") + "_" + e.InsertId + ".json"
		req, _ := http.NewRequest(http.MethodPost, "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+url.QueryEscape(name), strings.NewReader(string(b)))
		req.Header.Set("Content-Type", "application/json")
		if st, body := s.hub.DoService("gcs", req); st/100 != 2 {
			err = &core.Error{Status: st, Code: "storage", Msg: strings.TrimSpace(string(body))}
		}
	default: // a log bucket: entries are all stored already
	}
	if err != nil && s.hub.Log != nil {
		s.hub.Log.Warn("logging: cannot deliver to sink", "sink", k.Name, "destination", dest, "err", err)
	}
}

// ---- exclusions ----

func (s *Service) CreateExclusion(_ context.Context, req *lpb.CreateExclusionRequest) (*lpb.LogExclusion, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	x := proto.Clone(req.Exclusion).(*lpb.LogExclusion)
	if !idRE.MatchString(x.Name) {
		return nil, gapi.Invalid("Invalid exclusion name %q", x.Name)
	}
	if _, err := entryFilter(x.Filter); err != nil {
		return nil, err
	}
	key := req.Parent + "/exclusions/" + x.Name
	x.CreateTime, x.UpdateTime = timestamppb.Now(), timestamppb.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.exclusions[key]; ok {
		return nil, gapi.Exists("Exclusion %s already exists", key)
	}
	s.st.exclusions[key] = x
	return proto.Clone(x).(*lpb.LogExclusion), nil
}

func (s *Service) GetExclusion(_ context.Context, req *lpb.GetExclusionRequest) (*lpb.LogExclusion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, ok := s.st.exclusions[req.Name]
	if !ok {
		return nil, gapi.NotFound("Exclusion %s does not exist", req.Name)
	}
	return proto.Clone(x).(*lpb.LogExclusion), nil
}

func (s *Service) ListExclusions(_ context.Context, req *lpb.ListExclusionsRequest) (*lpb.ListExclusionsResponse, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	var all []*lpb.LogExclusion
	for _, n := range core.SortedKeys(s.st.exclusions) {
		if strings.HasPrefix(n, req.Parent+"/exclusions/") {
			all = append(all, proto.Clone(s.st.exclusions[n]).(*lpb.LogExclusion))
		}
	}
	s.mu.Unlock()
	items, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &lpb.ListExclusionsResponse{Exclusions: items, NextPageToken: next}, nil
}

func (s *Service) UpdateExclusion(_ context.Context, req *lpb.UpdateExclusionRequest) (*lpb.LogExclusion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.st.exclusions[req.Name]
	if !ok {
		return nil, gapi.NotFound("Exclusion %s does not exist", req.Name)
	}
	x := proto.Clone(old).(*lpb.LogExclusion)
	if len(req.UpdateMask.GetPaths()) == 0 {
		return nil, gapi.Invalid("update_mask is required")
	}
	if err := gapi.Update(x, req.Exclusion, req.UpdateMask.GetPaths(), "description", "filter", "disabled"); err != nil {
		return nil, err
	}
	if _, err := entryFilter(x.Filter); err != nil {
		return nil, err
	}
	x.UpdateTime = timestamppb.Now()
	s.st.exclusions[req.Name] = x
	return proto.Clone(x).(*lpb.LogExclusion), nil
}

func (s *Service) DeleteExclusion(_ context.Context, req *lpb.DeleteExclusionRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.exclusions[req.Name]; !ok {
		return nil, gapi.NotFound("Exclusion %s does not exist", req.Name)
	}
	delete(s.st.exclusions, req.Name)
	return &emptypb.Empty{}, nil
}

// ---- log-based metrics ----

// ponytail: log-based metrics are stored, not counted; they could write
// logging.googleapis.com/user/<name> points to Monitoring on each matching entry.

func (s *Service) CreateLogMetric(_ context.Context, req *lpb.CreateLogMetricRequest) (*lpb.LogMetric, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	m := proto.Clone(req.Metric).(*lpb.LogMetric)
	if !idRE.MatchString(m.Name) {
		return nil, gapi.Invalid("Invalid metric name %q", m.Name)
	}
	if m.Filter == "" {
		return nil, gapi.Invalid("Field metric.filter is required")
	}
	if _, err := entryFilter(m.Filter); err != nil {
		return nil, err
	}
	key := req.Parent + "/metrics/" + m.Name
	m.CreateTime, m.UpdateTime = timestamppb.Now(), timestamppb.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.metrics[key]; ok {
		return nil, gapi.Exists("Metric %s already exists", key)
	}
	s.st.metrics[key] = m
	return proto.Clone(m).(*lpb.LogMetric), nil
}

func (s *Service) GetLogMetric(_ context.Context, req *lpb.GetLogMetricRequest) (*lpb.LogMetric, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.st.metrics[req.MetricName]
	if !ok {
		return nil, gapi.NotFound("Metric %s does not exist", req.MetricName)
	}
	return proto.Clone(m).(*lpb.LogMetric), nil
}

func (s *Service) ListLogMetrics(_ context.Context, req *lpb.ListLogMetricsRequest) (*lpb.ListLogMetricsResponse, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	var all []*lpb.LogMetric
	for _, n := range core.SortedKeys(s.st.metrics) {
		if strings.HasPrefix(n, req.Parent+"/metrics/") {
			all = append(all, proto.Clone(s.st.metrics[n]).(*lpb.LogMetric))
		}
	}
	s.mu.Unlock()
	items, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &lpb.ListLogMetricsResponse{Metrics: items, NextPageToken: next}, nil
}

// UpdateLogMetric replaces a metric, or creates it.
func (s *Service) UpdateLogMetric(_ context.Context, req *lpb.UpdateLogMetricRequest) (*lpb.LogMetric, error) {
	if !regexp.MustCompile(`^projects/[^/]+/metrics/[A-Za-z0-9_.-]{1,100}$`).MatchString(req.MetricName) {
		return nil, gapi.Invalid("Invalid metric name %q", req.MetricName)
	}
	m := proto.Clone(req.Metric).(*lpb.LogMetric)
	if _, err := entryFilter(m.Filter); err != nil || m.Filter == "" {
		return nil, gapi.Invalid("Field metric.filter is required and must be valid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m.Name, m.UpdateTime = req.MetricName[strings.LastIndex(req.MetricName, "/")+1:], timestamppb.Now()
	m.CreateTime = timestamppb.Now()
	if old, ok := s.st.metrics[req.MetricName]; ok {
		m.CreateTime = old.CreateTime
	}
	s.st.metrics[req.MetricName] = m
	return proto.Clone(m).(*lpb.LogMetric), nil
}

func (s *Service) DeleteLogMetric(_ context.Context, req *lpb.DeleteLogMetricRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.metrics[req.MetricName]; !ok {
		return nil, gapi.NotFound("Metric %s does not exist", req.MetricName)
	}
	delete(s.st.metrics, req.MetricName)
	return &emptypb.Empty{}, nil
}

// ---- buckets ----

var bucketParentRE = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)

// ensureBuckets adds the two buckets every project has, in the location. Lock held.
func (s *Service) ensureBuckets(parent string) {
	for _, b := range []struct {
		id   string
		days int32
		desc string
	}{{"_Default", 30, "Default bucket"}, {"_Required", 400, "Audit bucket"}} {
		if n := parent + "/buckets/" + b.id; s.st.buckets[n] == nil {
			s.st.buckets[n] = &lpb.LogBucket{Name: n, Description: b.desc, RetentionDays: b.days, LifecycleState: lpb.LifecycleState_ACTIVE,
				CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now(), Locked: b.id == "_Required"}
		}
	}
}

func (s *Service) ListBuckets(_ context.Context, req *lpb.ListBucketsRequest) (*lpb.ListBucketsResponse, error) {
	if !bucketParentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q (want projects/<project>/locations/<location>)", req.Parent)
	}
	s.mu.Lock()
	s.ensureBuckets(req.Parent)
	var all []*lpb.LogBucket
	for _, n := range core.SortedKeys(s.st.buckets) {
		if strings.HasPrefix(n, req.Parent+"/buckets/") {
			all = append(all, proto.Clone(s.st.buckets[n]).(*lpb.LogBucket))
		}
	}
	s.mu.Unlock()
	items, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &lpb.ListBucketsResponse{Buckets: items, NextPageToken: next}, nil
}

func (s *Service) bucket(name string) (*lpb.LogBucket, error) {
	if i := strings.Index(name, "/buckets/"); i > 0 {
		s.ensureBuckets(name[:i])
	}
	b, ok := s.st.buckets[name]
	if !ok {
		return nil, gapi.NotFound("Bucket %s does not exist", name)
	}
	return b, nil
}

func (s *Service) GetBucket(_ context.Context, req *lpb.GetBucketRequest) (*lpb.LogBucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(req.Name)
	if err != nil {
		return nil, err
	}
	return proto.Clone(b).(*lpb.LogBucket), nil
}

func (s *Service) CreateBucket(_ context.Context, req *lpb.CreateBucketRequest) (*lpb.LogBucket, error) {
	if !bucketParentRE.MatchString(req.Parent) || !idRE.MatchString(req.BucketId) || strings.HasPrefix(req.BucketId, "_") {
		return nil, gapi.Invalid("Invalid parent %q or bucket_id %q", req.Parent, req.BucketId)
	}
	b := proto.Clone(req.Bucket).(*lpb.LogBucket)
	b.Name, b.LifecycleState = req.Parent+"/buckets/"+req.BucketId, lpb.LifecycleState_ACTIVE
	b.CreateTime, b.UpdateTime = timestamppb.Now(), timestamppb.Now()
	if b.RetentionDays == 0 {
		b.RetentionDays = 30
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.buckets[b.Name]; ok {
		return nil, gapi.Exists("Bucket %s already exists", b.Name)
	}
	s.st.buckets[b.Name] = b
	return proto.Clone(b).(*lpb.LogBucket), nil
}

func (s *Service) UpdateBucket(_ context.Context, req *lpb.UpdateBucketRequest) (*lpb.LogBucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.bucket(req.Name)
	if err != nil {
		return nil, err
	}
	if old.Locked {
		return nil, gapi.Invalid("Bucket %s is locked", req.Name)
	}
	b := proto.Clone(old).(*lpb.LogBucket)
	if err := gapi.Update(b, req.Bucket, req.UpdateMask.GetPaths(), "description", "retention_days", "locked", "analytics_enabled"); err != nil {
		return nil, err
	}
	b.UpdateTime = timestamppb.Now()
	s.st.buckets[b.Name] = b
	return proto.Clone(b).(*lpb.LogBucket), nil
}

// DeleteBucket marks a bucket for deletion; UndeleteBucket brings it back.
// ponytail: it is never purged after the 7 days Google waits.
func (s *Service) DeleteBucket(_ context.Context, req *lpb.DeleteBucketRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(req.Name)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(req.Name, "/_Default") || strings.HasSuffix(req.Name, "/_Required") {
		return nil, gapi.Invalid("The bucket %s cannot be deleted", req.Name)
	}
	b.LifecycleState, b.UpdateTime = lpb.LifecycleState_DELETE_REQUESTED, timestamppb.New(time.Now())
	return &emptypb.Empty{}, nil
}

func (s *Service) UndeleteBucket(_ context.Context, req *lpb.UndeleteBucketRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(req.Name)
	if err != nil {
		return nil, err
	}
	b.LifecycleState, b.UpdateTime = lpb.LifecycleState_ACTIVE, timestamppb.Now()
	return &emptypb.Empty{}, nil
}
