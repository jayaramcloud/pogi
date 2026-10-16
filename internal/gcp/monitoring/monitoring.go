// Package monitoring emulates Google Cloud Monitoring (v3) over gRPC and its
// REST mirror: custom metric descriptors, time series (written with
// CreateTimeSeries, read with filters, alignment and cross-series
// reduction), monitored resource descriptors, and alert policies,
// notification channels and uptime checks, which are only stored.
package monitoring

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	mpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Service is the Cloud Monitoring emulator.
type Service struct {
	mpb.UnimplementedMetricServiceServer
	mpb.UnimplementedAlertPolicyServiceServer
	mpb.UnimplementedNotificationChannelServiceServer
	mpb.UnimplementedUptimeCheckServiceServer
	rest *gapi.REST
	mu   sync.Mutex
	st   state
}

// state is everything that persists. Messages are protos, keyed by name.
type state struct {
	descriptors map[string]*metric.MetricDescriptor // by metric type, per project: "<project>/<type>"
	series      map[string]*mpb.TimeSeries          // points oldest first
	policies    map[string]*mpb.AlertPolicy
	channels    map[string]*mpb.NotificationChannel
	checks      map[string]*mpb.UptimeCheckConfig
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{map[string]*metric.MetricDescriptor{}, map[string]*mpb.TimeSeries{}, map[string]*mpb.AlertPolicy{},
		map[string]*mpb.NotificationChannel{}, map[string]*mpb.UptimeCheckConfig{}}
	s.mu.Unlock()
}

// New creates the Cloud Monitoring service.
func New(*core.Hub) *Service {
	s := &Service{}
	s.Reset()
	alert, chans, up := &mpb.AlertPolicyService_ServiceDesc, &mpb.NotificationChannelService_ServiceDesc, &mpb.UptimeCheckService_ServiceDesc
	s.rest = &gapi.REST{Versions: []string{"v3"}, Desc: &mpb.MetricService_ServiceDesc, Impl: s,
		Collections: []string{"timeSeries", "metricDescriptors", "monitoredResourceDescriptors", "alertPolicies", "notificationChannels",
			"notificationChannelDescriptors", "uptimeCheckConfigs"},
		Routes: map[string]gapi.Route{
			"POST timeSeries":                      {Method: "CreateTimeSeries", Field: "name"},
			"GET timeSeries":                       {Method: "ListTimeSeries", Field: "name"},
			"GET metricDescriptors":                {Method: "ListMetricDescriptors", Field: "name"},
			"POST metricDescriptors":               {Method: "CreateMetricDescriptor", Field: "name", Body: "metricDescriptor"},
			"GET monitoredResourceDescriptors":     {Method: "ListMonitoredResourceDescriptors", Field: "name"},
			"GET monitoredResourceDescriptors/*":   {Method: "GetMonitoredResourceDescriptor", Field: "name"},
			"GET alertPolicies":                    {Method: "ListAlertPolicies", Field: "name", Desc: alert},
			"POST alertPolicies":                   {Method: "CreateAlertPolicy", Field: "name", Body: "alertPolicy", Desc: alert},
			"GET alertPolicies/*":                  {Method: "GetAlertPolicy", Field: "name", Desc: alert},
			"PATCH alertPolicies/*":                {Method: "UpdateAlertPolicy", Field: "alertPolicy.name", Body: "alertPolicy", Desc: alert},
			"DELETE alertPolicies/*":               {Method: "DeleteAlertPolicy", Field: "name", Desc: alert},
			"GET notificationChannels":             {Method: "ListNotificationChannels", Field: "name", Desc: chans},
			"POST notificationChannels":            {Method: "CreateNotificationChannel", Field: "name", Body: "notificationChannel", Desc: chans},
			"GET notificationChannels/*":           {Method: "GetNotificationChannel", Field: "name", Desc: chans},
			"PATCH notificationChannels/*":         {Method: "UpdateNotificationChannel", Field: "notificationChannel.name", Body: "notificationChannel", Desc: chans},
			"DELETE notificationChannels/*":        {Method: "DeleteNotificationChannel", Field: "name", Desc: chans},
			"GET notificationChannelDescriptors":   {Method: "ListNotificationChannelDescriptors", Field: "name", Desc: chans},
			"GET notificationChannelDescriptors/*": {Method: "GetNotificationChannelDescriptor", Field: "name", Desc: chans},
			"GET uptimeCheckConfigs":               {Method: "ListUptimeCheckConfigs", Field: "parent", Desc: up},
			"POST uptimeCheckConfigs":              {Method: "CreateUptimeCheckConfig", Field: "parent", Body: "uptimeCheckConfig", Desc: up},
			"GET uptimeCheckConfigs/*":             {Method: "GetUptimeCheckConfig", Field: "name", Desc: up},
			"PATCH uptimeCheckConfigs/*":           {Method: "UpdateUptimeCheckConfig", Field: "uptimeCheckConfig.name", Body: "uptimeCheckConfig", Desc: up},
			"DELETE uptimeCheckConfigs/*":          {Method: "DeleteUptimeCheckConfig", Field: "name", Desc: up},
		}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "monitoring", Cloud: core.GCP, Title: "Cloud Monitoring", Match: s.rest.Match,
		Targets: []string{"google.monitoring.v3.MetricService", "google.monitoring.v3.AlertPolicyService",
			"google.monitoring.v3.NotificationChannelService", "google.monitoring.v3.UptimeCheckService"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	mpb.RegisterMetricServiceServer(g, s)
	mpb.RegisterAlertPolicyServiceServer(g, s)
	mpb.RegisterNotificationChannelServiceServer(g, s)
	mpb.RegisterUptimeCheckServiceServer(g, s)
}

// ServeHTTP serves the REST mirror. A metric type has slashes
// (metricDescriptors/custom.googleapis.com/x), which the shared router
// cannot name, so those two calls are made here.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, tail, ok := strings.Cut(r.URL.Path, "/metricDescriptors/")
	if !ok || tail == "" || r.Method != http.MethodGet && r.Method != http.MethodDelete {
		s.rest.ServeHTTP(w, r)
		return
	}
	c := core.C(r)
	name := strings.TrimPrefix(r.URL.Path[:len(r.URL.Path)-len(tail)], "/v3/") + tail
	var out proto.Message
	var err error
	if r.Method == http.MethodGet {
		c.Op = "GetMetricDescriptor"
		out, err = s.GetMetricDescriptor(r.Context(), &mpb.GetMetricDescriptorRequest{Name: name})
	} else {
		c.Op = "DeleteMetricDescriptor"
		out, err = s.DeleteMetricDescriptor(r.Context(), &mpb.DeleteMetricDescriptorRequest{Name: name})
	}
	if ierr := c.Intercept(); ierr != nil {
		e := core.AsError(ierr)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	if err != nil {
		gapi.Err(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// ---- persistence ----

type saved struct {
	Descriptors map[string]json.RawMessage `json:"descriptors,omitempty"`
	Series      map[string]json.RawMessage `json:"series,omitempty"`
	Policies    map[string]json.RawMessage `json:"policies,omitempty"`
	Channels    map[string]json.RawMessage `json:"channels,omitempty"`
	Checks      map[string]json.RawMessage `json:"checks,omitempty"`
}

func enc[M proto.Message](in map[string]M) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, m := range in {
		out[k], _ = protojson.Marshal(m)
	}
	return out
}

func dec[M proto.Message](in map[string]json.RawMessage, mk func() M) (map[string]M, error) {
	out := map[string]M{}
	for k, raw := range in {
		m := mk()
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, m); err != nil {
			return nil, err
		}
		out[k] = m
	}
	return out, nil
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(saved{enc(s.st.descriptors), enc(s.st.series), enc(s.st.policies), enc(s.st.channels), enc(s.st.checks)})
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	var st state
	var err error
	for _, f := range []func() error{
		func() (e error) {
			st.descriptors, e = dec(in.Descriptors, func() *metric.MetricDescriptor { return &metric.MetricDescriptor{} })
			return
		},
		func() (e error) {
			st.series, e = dec(in.Series, func() *mpb.TimeSeries { return &mpb.TimeSeries{} })
			return
		},
		func() (e error) {
			st.policies, e = dec(in.Policies, func() *mpb.AlertPolicy { return &mpb.AlertPolicy{} })
			return
		},
		func() (e error) {
			st.channels, e = dec(in.Channels, func() *mpb.NotificationChannel { return &mpb.NotificationChannel{} })
			return
		},
		func() (e error) {
			st.checks, e = dec(in.Checks, func() *mpb.UptimeCheckConfig { return &mpb.UptimeCheckConfig{} })
			return
		},
	} {
		if err = f(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

var projectRE = regexp.MustCompile(`^projects/([^/]+)$`)

// project checks that name is projects/<id> and returns the id.
func project(name string) (string, error) {
	m := projectRE.FindStringSubmatch(name)
	if m == nil {
		return "", gapi.Invalid("Field name has an invalid value %q: it must be projects/<project>", name)
	}
	return m[1], nil
}

func numID() string { return strconv.FormatUint(rand.Uint64N(9e18)+1e18, 10) }

func record() *mpb.MutationRecord {
	return &mpb.MutationRecord{MutateTime: timestamppb.Now(), MutatedBy: "pogi@pogi.iam.gserviceaccount.com"}
}

// byProject returns the messages of one project and collection (alertPolicies, ...), sorted by name.
func byProject[M proto.Message](m map[string]M, parent, coll string) []M {
	var out []M
	for _, n := range core.SortedKeys(m) {
		if strings.HasPrefix(n, parent+"/"+coll+"/") {
			out = append(out, m[n])
		}
	}
	return out
}

func get[M proto.Message](s *Service, m map[string]M, name, what string) (M, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := m[name]; ok {
		return proto.Clone(v).(M), nil
	}
	var zero M
	return zero, gapi.NotFound("%s %s does not exist", what, name)
}

func del[M proto.Message](s *Service, m map[string]M, name, what string) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := m[name]; !ok {
		return nil, gapi.NotFound("%s %s does not exist", what, name)
	}
	delete(m, name)
	return &emptypb.Empty{}, nil
}

// list returns a page of clones.
func list[M proto.Message](s *Service, m map[string]M, parent, coll string, size int32, token string) ([]M, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items, next := gapi.Page(byProject(m, parent, coll), size, token)
	out := make([]M, len(items))
	for i, it := range items {
		out[i] = proto.Clone(it).(M)
	}
	return out, next
}

// ---- metric descriptors ----

var customPrefixes = []string{"custom.googleapis.com/", "external.googleapis.com/", "workload.googleapis.com/", "prometheus.googleapis.com/"}

func isCustom(t string) bool {
	return slices.ContainsFunc(customPrefixes, func(p string) bool { return strings.HasPrefix(t, p) })
}

var descRE = regexp.MustCompile(`^projects/([^/]+)/metricDescriptors/(.+)$`)

func (s *Service) CreateMetricDescriptor(_ context.Context, req *mpb.CreateMetricDescriptorRequest) (*metric.MetricDescriptor, error) {
	p, err := project(req.Name)
	if err != nil {
		return nil, err
	}
	d := proto.Clone(req.MetricDescriptor).(*metric.MetricDescriptor)
	switch {
	case !isCustom(d.Type):
		return nil, gapi.Invalid("Field metricDescriptor.type has an invalid value %q: the type must start with one of %s", d.Type, strings.Join(customPrefixes, ", "))
	case d.ValueType == metric.MetricDescriptor_VALUE_TYPE_UNSPECIFIED:
		d.ValueType = metric.MetricDescriptor_DOUBLE
	}
	if d.MetricKind == metric.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
		d.MetricKind = metric.MetricDescriptor_GAUGE
	}
	d.Name = "projects/" + p + "/metricDescriptors/" + d.Type
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.st.descriptors[d.Name]; ok && (old.MetricKind != d.MetricKind || old.ValueType != d.ValueType) {
		return nil, gapi.Invalid("Metric descriptor %s already exists with a different metric kind or value type", d.Type)
	}
	s.st.descriptors[d.Name] = d
	return proto.Clone(d).(*metric.MetricDescriptor), nil
}

func (s *Service) GetMetricDescriptor(_ context.Context, req *mpb.GetMetricDescriptorRequest) (*metric.MetricDescriptor, error) {
	return get(s, s.st.descriptors, req.Name, "Metric descriptor")
}

func (s *Service) DeleteMetricDescriptor(_ context.Context, req *mpb.DeleteMetricDescriptorRequest) (*emptypb.Empty, error) {
	m := descRE.FindStringSubmatch(req.Name)
	if m == nil {
		return nil, gapi.Invalid("Field name has an invalid value %q", req.Name)
	}
	return del(s, s.st.descriptors, req.Name, "Metric descriptor")
}

func (s *Service) ListMetricDescriptors(_ context.Context, req *mpb.ListMetricDescriptorsRequest) (*mpb.ListMetricDescriptorsResponse, error) {
	if _, err := project(req.Name); err != nil {
		return nil, err
	}
	f, err := gapi.ParseFilter(req.Filter)
	if err != nil {
		return nil, gapi.Invalid("%v", err)
	}
	s.mu.Lock()
	var all []*metric.MetricDescriptor
	for _, d := range byProject(s.st.descriptors, req.Name, "metricDescriptors") {
		if f.Match(func(field, op, v string) bool { return field == "metric.type" && cmpValue(d.Type, op, v) }, func(string) bool { return false }) {
			all = append(all, proto.Clone(d).(*metric.MetricDescriptor))
		}
	}
	s.mu.Unlock()
	items, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &mpb.ListMetricDescriptorsResponse{MetricDescriptors: items, NextPageToken: next}, nil
}

// cmpValue applies a filter operator to a label value.
func cmpValue(got, op, v string) bool {
	switch op {
	case "=":
		return got == v
	case "!=":
		return got != v
	case "starts_with":
		return strings.HasPrefix(got, v)
	case "!starts_with":
		return !strings.HasPrefix(got, v)
	case "ends_with":
		return strings.HasSuffix(got, v)
	case "has_substring", ":":
		return strings.Contains(got, v)
	case "one_of":
		return slices.Contains(strings.Split(v, "\x00"), got)
	case "monitoring.regex.full_match":
		ok, _ := regexp.MatchString("^(?:"+v+")$", got)
		return ok
	}
	return false
}

// ---- monitored resource descriptors ----

type labelDesc = label.LabelDescriptor

func labels(keys ...string) []*labelDesc {
	out := make([]*labelDesc, len(keys))
	for i, k := range keys {
		out[i] = &labelDesc{Key: k, Description: k}
	}
	return out
}

var resourceTypes = map[string][]string{
	"global": {"project_id"}, "gce_instance": {"project_id", "instance_id", "zone"}, "k8s_container": {"project_id", "location", "cluster_name", "namespace_name", "pod_name", "container_name"},
	"k8s_pod": {"project_id", "location", "cluster_name", "namespace_name", "pod_name"}, "cloud_run_revision": {"project_id", "service_name", "revision_name", "location", "configuration_name"},
	"cloud_function": {"project_id", "function_name", "region"}, "cloud_run_job": {"project_id", "job_name", "location"}, "gcs_bucket": {"project_id", "bucket_name", "location"},
	"pubsub_topic": {"project_id", "topic_id"}, "pubsub_subscription": {"project_id", "subscription_id"}, "bigquery_dataset": {"project_id", "dataset_id"},
	"cloudsql_database": {"project_id", "database_id", "region"}, "generic_node": {"project_id", "location", "namespace", "node_id"}, "generic_task": {"project_id", "location", "namespace", "job", "task_id"},
	"gae_app": {"project_id", "module_id", "version_id"}, "uptime_url": {"project_id", "host"},
}

func resourceDescriptors(project string) []*monitoredres.MonitoredResourceDescriptor {
	var out []*monitoredres.MonitoredResourceDescriptor
	for _, t := range core.SortedKeys(resourceTypes) {
		out = append(out, &monitoredres.MonitoredResourceDescriptor{Name: "projects/" + project + "/monitoredResourceDescriptors/" + t,
			Type: t, DisplayName: t, Description: "A " + t + " resource.", Labels: labels(resourceTypes[t]...)})
	}
	return out
}

func (s *Service) ListMonitoredResourceDescriptors(_ context.Context, req *mpb.ListMonitoredResourceDescriptorsRequest) (*mpb.ListMonitoredResourceDescriptorsResponse, error) {
	p, err := project(req.Name)
	if err != nil {
		return nil, err
	}
	items, next := gapi.Page(resourceDescriptors(p), req.PageSize, req.PageToken)
	return &mpb.ListMonitoredResourceDescriptorsResponse{ResourceDescriptors: items, NextPageToken: next}, nil
}

func (s *Service) GetMonitoredResourceDescriptor(_ context.Context, req *mpb.GetMonitoredResourceDescriptorRequest) (*monitoredres.MonitoredResourceDescriptor, error) {
	for _, d := range resourceDescriptors(strings.Split(req.Name, "/")[1]) {
		if d.Name == req.Name {
			return d, nil
		}
	}
	return nil, gapi.NotFound("Monitored resource descriptor %s does not exist", req.Name)
}

// ---- alert policies, notification channels, uptime checks: stored ----

func (s *Service) CreateAlertPolicy(_ context.Context, req *mpb.CreateAlertPolicyRequest) (*mpb.AlertPolicy, error) {
	if _, err := project(req.Name); err != nil {
		return nil, err
	}
	p := proto.Clone(req.AlertPolicy).(*mpb.AlertPolicy)
	if p.DisplayName == "" {
		return nil, gapi.Invalid("Field alert_policy.display_name is required")
	}
	p.Name = req.Name + "/alertPolicies/" + numID()
	for _, c := range p.Conditions {
		c.Name = p.Name + "/conditions/" + numID()
	}
	if p.Enabled == nil {
		p.Enabled = wrapperspb.Bool(true)
	}
	p.CreationRecord, p.MutationRecord = record(), record()
	s.mu.Lock()
	s.st.policies[p.Name] = p
	s.mu.Unlock()
	return proto.Clone(p).(*mpb.AlertPolicy), nil
}

func (s *Service) GetAlertPolicy(_ context.Context, req *mpb.GetAlertPolicyRequest) (*mpb.AlertPolicy, error) {
	return get(s, s.st.policies, req.Name, "Alert policy")
}

func (s *Service) ListAlertPolicies(_ context.Context, req *mpb.ListAlertPoliciesRequest) (*mpb.ListAlertPoliciesResponse, error) {
	if _, err := project(req.Name); err != nil {
		return nil, err
	}
	items, next := list(s, s.st.policies, req.Name, "alertPolicies", req.PageSize, req.PageToken)
	return &mpb.ListAlertPoliciesResponse{AlertPolicies: items, NextPageToken: next}, nil
}

func (s *Service) DeleteAlertPolicy(_ context.Context, req *mpb.DeleteAlertPolicyRequest) (*emptypb.Empty, error) {
	return del(s, s.st.policies, req.Name, "Alert policy")
}

func (s *Service) UpdateAlertPolicy(_ context.Context, req *mpb.UpdateAlertPolicyRequest) (*mpb.AlertPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.st.policies[req.GetAlertPolicy().GetName()]
	if !ok {
		return nil, gapi.NotFound("Alert policy %s does not exist", req.GetAlertPolicy().GetName())
	}
	p := proto.Clone(old).(*mpb.AlertPolicy)
	if err := gapi.Update(p, req.AlertPolicy, req.UpdateMask.GetPaths(), "display_name", "documentation", "user_labels", "conditions",
		"combiner", "enabled", "notification_channels", "alert_strategy"); err != nil {
		return nil, err
	}
	for _, c := range p.Conditions {
		if c.Name == "" {
			c.Name = p.Name + "/conditions/" + numID()
		}
	}
	p.MutationRecord = record()
	s.st.policies[p.Name] = p
	return proto.Clone(p).(*mpb.AlertPolicy), nil
}

var channelTypes = map[string][]string{"email": {"email_address"}, "slack": {"channel_name"}, "webhook_basicauth": {"url", "username"}, "webhook_tokenauth": {"url"},
	"pubsub": {"topic"}, "sms": {"number"}, "pagerduty": {"service_key"}}

func channelDescriptors(project string) []*mpb.NotificationChannelDescriptor {
	var out []*mpb.NotificationChannelDescriptor
	for _, t := range core.SortedKeys(channelTypes) {
		out = append(out, &mpb.NotificationChannelDescriptor{Name: "projects/" + project + "/notificationChannelDescriptors/" + t, Type: t,
			DisplayName: t, Description: "A " + t + " channel.", Labels: labels(channelTypes[t]...)})
	}
	return out
}

func (s *Service) ListNotificationChannelDescriptors(_ context.Context, req *mpb.ListNotificationChannelDescriptorsRequest) (*mpb.ListNotificationChannelDescriptorsResponse, error) {
	p, err := project(req.Name)
	if err != nil {
		return nil, err
	}
	items, next := gapi.Page(channelDescriptors(p), req.PageSize, req.PageToken)
	return &mpb.ListNotificationChannelDescriptorsResponse{ChannelDescriptors: items, NextPageToken: next}, nil
}

func (s *Service) GetNotificationChannelDescriptor(_ context.Context, req *mpb.GetNotificationChannelDescriptorRequest) (*mpb.NotificationChannelDescriptor, error) {
	for _, d := range channelDescriptors(strings.Split(req.Name, "/")[1]) {
		if d.Name == req.Name {
			return d, nil
		}
	}
	return nil, gapi.NotFound("Notification channel descriptor %s does not exist", req.Name)
}

func (s *Service) CreateNotificationChannel(_ context.Context, req *mpb.CreateNotificationChannelRequest) (*mpb.NotificationChannel, error) {
	if _, err := project(req.Name); err != nil {
		return nil, err
	}
	c := proto.Clone(req.NotificationChannel).(*mpb.NotificationChannel)
	if _, ok := channelTypes[c.Type]; !ok {
		return nil, gapi.Invalid("Field notification_channel.type has an invalid value %q", c.Type)
	}
	c.Name = req.Name + "/notificationChannels/" + numID()
	c.VerificationStatus = mpb.NotificationChannel_VERIFIED
	if c.Enabled == nil {
		c.Enabled = wrapperspb.Bool(true)
	}
	c.CreationRecord = record()
	s.mu.Lock()
	s.st.channels[c.Name] = c
	s.mu.Unlock()
	return proto.Clone(c).(*mpb.NotificationChannel), nil
}

func (s *Service) GetNotificationChannel(_ context.Context, req *mpb.GetNotificationChannelRequest) (*mpb.NotificationChannel, error) {
	return get(s, s.st.channels, req.Name, "Notification channel")
}

func (s *Service) ListNotificationChannels(_ context.Context, req *mpb.ListNotificationChannelsRequest) (*mpb.ListNotificationChannelsResponse, error) {
	if _, err := project(req.Name); err != nil {
		return nil, err
	}
	items, next := list(s, s.st.channels, req.Name, "notificationChannels", req.PageSize, req.PageToken)
	return &mpb.ListNotificationChannelsResponse{NotificationChannels: items, NextPageToken: next}, nil
}

func (s *Service) DeleteNotificationChannel(_ context.Context, req *mpb.DeleteNotificationChannelRequest) (*emptypb.Empty, error) {
	return del(s, s.st.channels, req.Name, "Notification channel")
}

func (s *Service) UpdateNotificationChannel(_ context.Context, req *mpb.UpdateNotificationChannelRequest) (*mpb.NotificationChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.st.channels[req.GetNotificationChannel().GetName()]
	if !ok {
		return nil, gapi.NotFound("Notification channel %s does not exist", req.GetNotificationChannel().GetName())
	}
	c := proto.Clone(old).(*mpb.NotificationChannel)
	if err := gapi.Update(c, req.NotificationChannel, req.UpdateMask.GetPaths(), "display_name", "description", "labels", "user_labels", "enabled"); err != nil {
		return nil, err
	}
	c.MutationRecords = append(c.MutationRecords, record())
	s.st.channels[c.Name] = c
	return proto.Clone(c).(*mpb.NotificationChannel), nil
}

func (s *Service) CreateUptimeCheckConfig(_ context.Context, req *mpb.CreateUptimeCheckConfigRequest) (*mpb.UptimeCheckConfig, error) {
	if _, err := project(req.Parent); err != nil {
		return nil, err
	}
	u := proto.Clone(req.UptimeCheckConfig).(*mpb.UptimeCheckConfig)
	if u.DisplayName == "" {
		return nil, gapi.Invalid("Field uptime_check_config.display_name is required")
	}
	id := strings.Trim(regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(u.DisplayName), "-"), "-")
	u.Name = req.Parent + "/uptimeCheckConfigs/" + id + "-" + core.RandHex(3)
	s.mu.Lock()
	s.st.checks[u.Name] = u
	s.mu.Unlock()
	return proto.Clone(u).(*mpb.UptimeCheckConfig), nil
}

func (s *Service) GetUptimeCheckConfig(_ context.Context, req *mpb.GetUptimeCheckConfigRequest) (*mpb.UptimeCheckConfig, error) {
	return get(s, s.st.checks, req.Name, "Uptime check")
}

func (s *Service) ListUptimeCheckConfigs(_ context.Context, req *mpb.ListUptimeCheckConfigsRequest) (*mpb.ListUptimeCheckConfigsResponse, error) {
	if _, err := project(req.Parent); err != nil {
		return nil, err
	}
	items, next := list(s, s.st.checks, req.Parent, "uptimeCheckConfigs", req.PageSize, req.PageToken)
	return &mpb.ListUptimeCheckConfigsResponse{UptimeCheckConfigs: items, NextPageToken: next, TotalSize: int32(len(items))}, nil
}

func (s *Service) DeleteUptimeCheckConfig(_ context.Context, req *mpb.DeleteUptimeCheckConfigRequest) (*emptypb.Empty, error) {
	return del(s, s.st.checks, req.Name, "Uptime check")
}

func (s *Service) UpdateUptimeCheckConfig(_ context.Context, req *mpb.UpdateUptimeCheckConfigRequest) (*mpb.UptimeCheckConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.st.checks[req.GetUptimeCheckConfig().GetName()]
	if !ok {
		return nil, gapi.NotFound("Uptime check %s does not exist", req.GetUptimeCheckConfig().GetName())
	}
	u := proto.Clone(old).(*mpb.UptimeCheckConfig)
	if err := gapi.Update(u, req.UptimeCheckConfig, req.UpdateMask.GetPaths(), "display_name", "monitored_resource", "resource_group", "http_check",
		"tcp_check", "period", "timeout", "content_matchers", "selected_regions", "user_labels"); err != nil {
		return nil, err
	}
	s.st.checks[u.Name] = u
	return proto.Clone(u).(*mpb.UptimeCheckConfig), nil
}

func (s *Service) ListUptimeCheckIps(context.Context, *mpb.ListUptimeCheckIpsRequest) (*mpb.ListUptimeCheckIpsResponse, error) {
	return &mpb.ListUptimeCheckIpsResponse{}, nil
}

// ---- console ----

// Resources lists what is stored.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	desc := core.Table{Name: "Metric descriptors", Columns: []string{"Type", "Project", "Kind", "Value type"}}
	for _, n := range core.SortedKeys(s.st.descriptors) {
		d := s.st.descriptors[n]
		desc.Rows = append(desc.Rows, []string{n, strings.Split(n, "/")[1], d.MetricKind.String(), d.ValueType.String()})
	}
	ts := core.Table{Name: "Time series", Columns: []string{"Metric", "Resource", "Points", "Latest"}}
	for _, k := range core.SortedKeys(s.st.series) {
		t := s.st.series[k]
		last := "-"
		if n := len(t.Points); n > 0 {
			last = t.Points[n-1].Interval.EndTime.AsTime().Format(time.DateTime)
		}
		ts.Rows = append(ts.Rows, []string{k, t.Resource.GetType(), strconv.Itoa(len(t.Points)), last})
	}
	ap := core.Table{Name: "Alert policies", Columns: []string{"Name", "Display name", "Enabled", "Conditions"}}
	for _, n := range core.SortedKeys(s.st.policies) {
		p := s.st.policies[n]
		ap.Rows = append(ap.Rows, []string{n, p.DisplayName, strconv.FormatBool(p.Enabled.GetValue()), strconv.Itoa(len(p.Conditions))})
	}
	nc := core.Table{Name: "Notification channels", Columns: []string{"Name", "Display name", "Type"}}
	for _, n := range core.SortedKeys(s.st.channels) {
		c := s.st.channels[n]
		nc.Rows = append(nc.Rows, []string{n, c.DisplayName, c.Type})
	}
	uc := core.Table{Name: "Uptime checks", Columns: []string{"Name", "Display name", "Period"}}
	for _, n := range core.SortedKeys(s.st.checks) {
		u := s.st.checks[n]
		uc.Rows = append(uc.Rows, []string{n, u.DisplayName, u.Period.AsDuration().String()})
	}
	return []core.Table{desc, ts, ap, nc, uc}
}

// Inspect returns one stored message as JSON.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m proto.Message
	switch table {
	case "Metric descriptors":
		m = s.st.descriptors[id]
	case "Time series":
		m = s.st.series[id]
	case "Alert policies":
		m = s.st.policies[id]
	case "Notification channels":
		m = s.st.channels[id]
	case "Uptime checks":
		m = s.st.checks[id]
	}
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}
	b, _ := protojson.Marshal(m)
	return json.RawMessage(b)
}
