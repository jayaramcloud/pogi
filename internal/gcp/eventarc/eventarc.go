// Package eventarc emulates Google Eventarc (v1) over gRPC and its REST
// mirror: triggers (long-running operations that finish at once), channels,
// channel connections and providers. Triggers route Cloud Storage events
// (delivered by the storage service through the hub) and Pub/Sub messages
// to Cloud Run, Cloud Functions and HTTP endpoints as CloudEvents.
package eventarc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	pubsubType = "google.cloud.pubsub.topic.v1.messagePublished"
	pushPath   = "/_pogi/eventarc/push/" // pogi's own endpoint that Pub/Sub trigger subscriptions push to
)

// Service is the Eventarc emulator.
type Service struct {
	pb.UnimplementedEventarcServer
	hub      *core.Hub
	client   *http.Client
	rest     *gapi.REST
	mu       sync.Mutex
	triggers map[string]*trigger
	channels map[string]*pb.Channel
	conns    map[string]*pb.ChannelConnection
}

type trigger struct {
	p         *pb.Trigger
	autoTopic bool // pogi made the transport topic, so deleting the trigger deletes it
}

// New creates the Eventarc service. It receives events as hub deliveries
// for "//eventarc.googleapis.com/projects/<p>/events".
func New(h *core.Hub) *Service {
	s := &Service{hub: h, client: gapi.Client(h), triggers: map[string]*trigger{}, channels: map[string]*pb.Channel{},
		conns: map[string]*pb.ChannelConnection{}}
	t, ch, cc, pr := "locations/*/triggers", "locations/*/channels", "locations/*/channelConnections", "locations/*/providers"
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"triggers", "channels", "channelConnections", "providers"},
		Desc: &pb.Eventarc_ServiceDesc, Impl: s, Routes: map[string]gapi.Route{
			"GET " + t:            {Method: "ListTriggers", Field: "parent"},
			"POST " + t:           {Method: "CreateTrigger", Field: "parent", Body: "trigger"},
			"GET " + t + "/*":     {Method: "GetTrigger", Field: "name"},
			"PATCH " + t + "/*":   {Method: "UpdateTrigger", Field: "trigger.name", Body: "trigger"},
			"DELETE " + t + "/*":  {Method: "DeleteTrigger", Field: "name"},
			"GET " + ch:           {Method: "ListChannels", Field: "parent"},
			"POST " + ch:          {Method: "CreateChannel", Field: "parent", Body: "channel"},
			"GET " + ch + "/*":    {Method: "GetChannel", Field: "name"},
			"PATCH " + ch + "/*":  {Method: "UpdateChannel", Field: "channel.name", Body: "channel"},
			"DELETE " + ch + "/*": {Method: "DeleteChannel", Field: "name"},
			"GET " + cc:           {Method: "ListChannelConnections", Field: "parent"},
			"POST " + cc:          {Method: "CreateChannelConnection", Field: "parent", Body: "channelConnection"},
			"GET " + cc + "/*":    {Method: "GetChannelConnection", Field: "name"},
			"DELETE " + cc + "/*": {Method: "DeleteChannelConnection", Field: "name"},
			"GET " + pr:           {Method: "ListProviders", Field: "parent"},
			"GET " + pr + "/*":    {Method: "GetProvider", Field: "name"},
		}}
	h.OnDeliver("eventarc", s.onEvent)
	h.Mount(pushPath, http.HandlerFunc(s.push))
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "eventarc", Cloud: core.GCP, Title: "Eventarc", Match: s.rest.Match,
		Targets: []string{"google.cloud.eventarc.v1.Eventarc"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterEventarcServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.triggers, s.channels, s.conns = map[string]*trigger{}, map[string]*pb.Channel{}, map[string]*pb.ChannelConnection{}
	s.mu.Unlock()
}

// ---- persistence ----

type saved struct {
	Triggers []savedTrigger    `json:"triggers"`
	Channels []json.RawMessage `json:"channels,omitempty"`
	Conns    []json.RawMessage `json:"channelConnections,omitempty"`
}

type savedTrigger struct {
	Trigger   json.RawMessage `json:"trigger"`
	AutoTopic bool            `json:"autoTopic,omitempty"`
}

func pj(m proto.Message) json.RawMessage {
	b, _ := protojson.Marshal(m)
	return b
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{Triggers: []savedTrigger{}}
	for _, n := range core.SortedKeys(s.triggers) {
		out.Triggers = append(out.Triggers, savedTrigger{pj(s.triggers[n].p), s.triggers[n].autoTopic})
	}
	for _, n := range core.SortedKeys(s.channels) {
		out.Channels = append(out.Channels, pj(s.channels[n]))
	}
	for _, n := range core.SortedKeys(s.conns) {
		out.Conns = append(out.Conns, pj(s.conns[n]))
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	triggers, channels, conns := map[string]*trigger{}, map[string]*pb.Channel{}, map[string]*pb.ChannelConnection{}
	for _, st := range in.Triggers {
		t := &trigger{p: &pb.Trigger{}, autoTopic: st.AutoTopic}
		if err := protojson.Unmarshal(st.Trigger, t.p); err != nil {
			return err
		}
		triggers[t.p.Name] = t
	}
	for _, raw := range in.Channels {
		c := &pb.Channel{}
		if err := protojson.Unmarshal(raw, c); err != nil {
			return err
		}
		channels[c.Name] = c
	}
	for _, raw := range in.Conns {
		c := &pb.ChannelConnection{}
		if err := protojson.Unmarshal(raw, c); err != nil {
			return err
		}
		conns[c.Name] = c
	}
	s.mu.Lock()
	s.triggers, s.channels, s.conns = triggers, channels, conns
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

var (
	locRE = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
	idRE  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)
)

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

// op records a finished long-running operation under parent.
func (s *Service) op(parent, target, verb string, resp proto.Message) *longrunningpb.Operation {
	now := timestamppb.Now()
	return gapi.Operations(s.hub).Done(parent, &pb.OperationMetadata{CreateTime: now, EndTime: now, Target: target, Verb: verb,
		ApiVersion: "v1"}, resp)
}

// project and location of a resource name.
func split(name string) (project, location string) {
	p := strings.Split(name, "/")
	if len(p) >= 4 {
		return p[1], p[3]
	}
	return "", ""
}

func checkParent(parent, id string) error {
	if !locRE.MatchString(parent) {
		return gapi.Invalid("Invalid parent %q: want projects/<PROJECT_ID>/locations/<LOCATION_ID>", parent)
	}
	if !idRE.MatchString(id) {
		return gapi.Invalid("Invalid ID %q: want 2-63 lowercase letters, digits or hyphens, starting with a letter", id)
	}
	return nil
}

// internal calls a service's REST API in-process; a 409 (already exists) is fine.
func (s *Service) internal(service, method, path string, body any) error {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(method, "http://pogi.internal"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if code, resp := s.hub.DoService(service, req); code/100 != 2 && code != http.StatusConflict {
		return gapi.Invalid("%s: %d %s", path, code, strings.TrimSpace(string(resp)))
	}
	return nil
}

// ---- triggers ----

// validate checks a trigger the way Eventarc does before it accepts it.
func validate(p *pb.Trigger) error {
	typ := ""
	for _, f := range p.EventFilters {
		if f.Attribute == "type" {
			typ = f.Value
		}
	}
	if typ == "" {
		return gapi.Invalid("The request was invalid: an event filter with attribute \"type\" is required")
	}
	switch d := p.GetDestination().GetDescriptor_().(type) {
	case *pb.Destination_CloudRun:
		if d.CloudRun.GetService() == "" {
			return gapi.Invalid("The request was invalid: destination.cloud_run.service is required")
		}
	case *pb.Destination_HttpEndpoint:
		if u, err := url.Parse(d.HttpEndpoint.GetUri()); err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
			return gapi.Invalid("The request was invalid: destination.http_endpoint.uri %q is not an http(s) URL", d.HttpEndpoint.GetUri())
		}
	case *pb.Destination_CloudFunction, *pb.Destination_Workflow, *pb.Destination_Gke:
	default:
		return gapi.Invalid("The request was invalid: destination is required")
	}
	return nil
}

func (s *Service) CreateTrigger(_ context.Context, req *pb.CreateTriggerRequest) (*longrunningpb.Operation, error) {
	if err := checkParent(req.Parent, req.TriggerId); err != nil {
		return nil, err
	}
	p := clone(req.GetTrigger())
	if err := validate(p); err != nil {
		return nil, err
	}
	now := timestamppb.Now()
	p.Name, p.Uid, p.CreateTime, p.UpdateTime, p.Etag = req.Parent+"/triggers/"+req.TriggerId, core.UUID(), now, now, core.RandHex(8)
	if p.EventDataContentType == "" {
		p.EventDataContentType = "application/json"
	}
	if req.ValidateOnly {
		return s.op(req.Parent, p.Name, "create", nil), nil
	}
	s.mu.Lock()
	taken := s.triggers[p.Name] != nil
	s.mu.Unlock()
	if taken {
		return nil, gapi.Exists("Trigger %s already exists.", p.Name)
	}
	t := &trigger{p: p}
	if err := s.subscribe(t); err != nil { // calls Pub/Sub: not under the lock
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggers[p.Name] != nil { // lost a race with another create
		s.unsubscribe(t)
		return nil, gapi.Exists("Trigger %s already exists.", p.Name)
	}
	s.triggers[p.Name] = t
	return s.op(req.Parent, p.Name, "create", clone(p)), nil
}

// subscribe gives a Pub/Sub trigger its transport: the topic (made when absent) and a push
// subscription to pogi's push endpoint. Other event types need no transport.
func (s *Service) subscribe(t *trigger) error {
	p := t.p
	isPubsub := false
	for _, f := range p.EventFilters {
		isPubsub = isPubsub || f.Attribute == "type" && f.Value == pubsubType
	}
	if !isPubsub {
		return nil
	}
	project, loc := split(p.Name)
	id := p.Name[strings.LastIndex(p.Name, "/")+1:]
	topic := p.GetTransport().GetPubsub().GetTopic()
	if topic == "" {
		topic, t.autoTopic = fmt.Sprintf("projects/%s/topics/eventarc-%s-%s-%s", project, loc, id, core.RandHex(3)), true
	}
	sub := fmt.Sprintf("projects/%s/subscriptions/eventarc-%s-%s-sub-%s", project, loc, id, core.RandHex(3))
	if err := s.internal("pubsub", http.MethodPut, "/v1/"+topic, map[string]any{}); err != nil {
		return err
	}
	if err := s.internal("pubsub", http.MethodPut, "/v1/"+sub, map[string]any{"topic": topic,
		"pushConfig": map[string]any{"pushEndpoint": s.hub.URL + pushPath + p.Name}}); err != nil {
		return err
	}
	p.Transport = &pb.Transport{Intermediary: &pb.Transport_Pubsub{Pubsub: &pb.Pubsub{Topic: topic, Subscription: sub}}}
	return nil
}

// unsubscribe removes what subscribe made.
func (s *Service) unsubscribe(t *trigger) {
	ps := t.p.GetTransport().GetPubsub()
	if ps == nil {
		return
	}
	err := s.internal("pubsub", http.MethodDelete, "/v1/"+ps.Subscription, nil)
	if t.autoTopic && err == nil {
		err = s.internal("pubsub", http.MethodDelete, "/v1/"+ps.Topic, nil)
	}
	if err != nil {
		s.hub.Log.Warn("eventarc could not clean up a trigger's transport", "trigger", t.p.Name, "err", err)
	}
}

func (s *Service) GetTrigger(_ context.Context, req *pb.GetTriggerRequest) (*pb.Trigger, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.triggers[req.Name]
	if t == nil {
		return nil, gapi.NotFound("Trigger %s not found.", req.Name)
	}
	return clone(t.p), nil
}

func (s *Service) ListTriggers(_ context.Context, req *pb.ListTriggersRequest) (*pb.ListTriggersResponse, error) {
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<PROJECT_ID>/locations/<LOCATION_ID>", req.Parent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*pb.Trigger
	for _, n := range core.SortedKeys(s.triggers) {
		if strings.HasPrefix(n, req.Parent+"/triggers/") {
			all = append(all, clone(s.triggers[n].p))
		}
	}
	out := &pb.ListTriggersResponse{}
	out.Triggers, out.NextPageToken = gapi.Page(all, req.PageSize, req.PageToken)
	return out, nil
}

func (s *Service) UpdateTrigger(_ context.Context, req *pb.UpdateTriggerRequest) (*longrunningpb.Operation, error) {
	name := req.GetTrigger().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.triggers[name]
	if t == nil {
		return nil, gapi.NotFound("Trigger %s not found.", name)
	}
	p := clone(t.p)
	if err := gapi.Update(p, req.Trigger, req.GetUpdateMask().GetPaths(), "event_filters", "service_account", "destination",
		"labels", "channel", "event_data_content_type", "retry_policy"); err != nil {
		return nil, err
	}
	if err := validate(p); err != nil {
		return nil, err
	}
	parent := name[:strings.LastIndex(name, "/triggers/")]
	if req.ValidateOnly {
		return s.op(parent, name, "update", nil), nil
	}
	p.UpdateTime, p.Etag = timestamppb.Now(), core.RandHex(8)
	t.p = p
	return s.op(parent, name, "update", clone(p)), nil
}

func (s *Service) DeleteTrigger(_ context.Context, req *pb.DeleteTriggerRequest) (*longrunningpb.Operation, error) {
	parent := req.Name[:max(strings.LastIndex(req.Name, "/triggers/"), 0)]
	s.mu.Lock()
	t := s.triggers[req.Name]
	if t != nil && !req.ValidateOnly {
		delete(s.triggers, req.Name)
	}
	s.mu.Unlock()
	switch {
	case t == nil && !req.AllowMissing:
		return nil, gapi.NotFound("Trigger %s not found.", req.Name)
	case t == nil || req.ValidateOnly:
		return s.op(parent, req.Name, "delete", nil), nil
	}
	s.unsubscribe(t)
	return s.op(parent, req.Name, "delete", clone(t.p)), nil
}

// ---- channels, channel connections and providers ----
// ponytail: stored and listed, never connected to a third-party source.

func (s *Service) CreateChannel(_ context.Context, req *pb.CreateChannelRequest) (*longrunningpb.Operation, error) {
	if err := checkParent(req.Parent, req.ChannelId); err != nil {
		return nil, err
	}
	p := clone(req.GetChannel())
	now := timestamppb.Now()
	p.Name, p.Uid, p.CreateTime, p.UpdateTime, p.State = req.Parent+"/channels/"+req.ChannelId, core.UUID(), now, now, pb.Channel_PENDING
	project, loc := split(p.Name)
	p.Transport = &pb.Channel_PubsubTopic{PubsubTopic: fmt.Sprintf("projects/%s/topics/eventarc-channel-%s-%s-%s", project, loc, req.ChannelId, core.RandHex(3))}
	p.ActivationToken = core.RandHex(8)
	if req.ValidateOnly {
		return s.op(req.Parent, p.Name, "create", nil), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channels[p.Name] != nil {
		return nil, gapi.Exists("Channel %s already exists.", p.Name)
	}
	s.channels[p.Name] = p
	return s.op(req.Parent, p.Name, "create", clone(p)), nil
}

func (s *Service) GetChannel(_ context.Context, req *pb.GetChannelRequest) (*pb.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.channels[req.Name]
	if c == nil {
		return nil, gapi.NotFound("Channel %s not found.", req.Name)
	}
	return clone(c), nil
}

func (s *Service) ListChannels(_ context.Context, req *pb.ListChannelsRequest) (*pb.ListChannelsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*pb.Channel
	for _, n := range core.SortedKeys(s.channels) {
		if strings.HasPrefix(n, req.Parent+"/channels/") {
			all = append(all, clone(s.channels[n]))
		}
	}
	out := &pb.ListChannelsResponse{}
	out.Channels, out.NextPageToken = gapi.Page(all, req.PageSize, req.PageToken)
	return out, nil
}

func (s *Service) UpdateChannel(_ context.Context, req *pb.UpdateChannelRequest) (*longrunningpb.Operation, error) {
	name := req.GetChannel().GetName()
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.channels[name]
	if c == nil {
		return nil, gapi.NotFound("Channel %s not found.", name)
	}
	if err := gapi.Update(c, req.Channel, req.GetUpdateMask().GetPaths(), "labels", "crypto_key_name"); err != nil {
		return nil, err
	}
	c.UpdateTime = timestamppb.Now()
	return s.op(name[:strings.LastIndex(name, "/channels/")], name, "update", clone(c)), nil
}

func (s *Service) DeleteChannel(_ context.Context, req *pb.DeleteChannelRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.channels[req.Name]
	if c == nil {
		return nil, gapi.NotFound("Channel %s not found.", req.Name)
	}
	delete(s.channels, req.Name)
	return s.op(req.Name[:strings.LastIndex(req.Name, "/channels/")], req.Name, "delete", clone(c)), nil
}

func (s *Service) CreateChannelConnection(_ context.Context, req *pb.CreateChannelConnectionRequest) (*longrunningpb.Operation, error) {
	if err := checkParent(req.Parent, req.ChannelConnectionId); err != nil {
		return nil, err
	}
	p := clone(req.GetChannelConnection())
	now := timestamppb.Now()
	p.Name, p.Uid, p.CreateTime, p.UpdateTime = req.Parent+"/channelConnections/"+req.ChannelConnectionId, core.UUID(), now, now
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns[p.Name] != nil {
		return nil, gapi.Exists("Channel connection %s already exists.", p.Name)
	}
	s.conns[p.Name] = p
	return s.op(req.Parent, p.Name, "create", clone(p)), nil
}

func (s *Service) GetChannelConnection(_ context.Context, req *pb.GetChannelConnectionRequest) (*pb.ChannelConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.conns[req.Name]
	if c == nil {
		return nil, gapi.NotFound("Channel connection %s not found.", req.Name)
	}
	return clone(c), nil
}

func (s *Service) ListChannelConnections(_ context.Context, req *pb.ListChannelConnectionsRequest) (*pb.ListChannelConnectionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*pb.ChannelConnection
	for _, n := range core.SortedKeys(s.conns) {
		if strings.HasPrefix(n, req.Parent+"/channelConnections/") {
			all = append(all, clone(s.conns[n]))
		}
	}
	out := &pb.ListChannelConnectionsResponse{}
	out.ChannelConnections, out.NextPageToken = gapi.Page(all, req.PageSize, req.PageToken)
	return out, nil
}

func (s *Service) DeleteChannelConnection(_ context.Context, req *pb.DeleteChannelConnectionRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.conns[req.Name]
	if c == nil {
		return nil, gapi.NotFound("Channel connection %s not found.", req.Name)
	}
	delete(s.conns, req.Name)
	return s.op(req.Name[:strings.LastIndex(req.Name, "/channelConnections/")], req.Name, "delete", clone(c)), nil
}

var storageTypes = []string{"finalized", "deleted", "archived", "metadataUpdated"}

// providers are the event sources pogi emits events for.
func providers(parent string) []*pb.Provider {
	storage := &pb.Provider{Name: parent + "/providers/storage.googleapis.com", DisplayName: "Cloud Storage"}
	for _, t := range storageTypes {
		storage.EventTypes = append(storage.EventTypes, &pb.EventType{Type: "google.cloud.storage.object.v1." + t,
			Description: "Cloud Storage object " + t, FilteringAttributes: []*pb.FilteringAttribute{{Attribute: "bucket", Required: true}}})
	}
	return []*pb.Provider{
		{Name: parent + "/providers/pubsub.googleapis.com", DisplayName: "Cloud Pub/Sub", EventTypes: []*pb.EventType{{Type: pubsubType,
			Description: "A message is published to a Pub/Sub topic"}}},
		storage,
	}
}

func (s *Service) GetProvider(_ context.Context, req *pb.GetProviderRequest) (*pb.Provider, error) {
	if i := strings.LastIndex(req.Name, "/providers/"); i > 0 {
		for _, p := range providers(req.Name[:i]) {
			if p.Name == req.Name {
				return p, nil
			}
		}
	}
	return nil, gapi.NotFound("Provider %s not found.", req.Name)
}

func (s *Service) ListProviders(_ context.Context, req *pb.ListProvidersRequest) (*pb.ListProvidersResponse, error) {
	out := &pb.ListProvidersResponse{}
	out.Providers, out.NextPageToken = gapi.Page(providers(req.Parent), req.PageSize, req.PageToken)
	return out, nil
}

// ---- routing events ----

// event is a CloudEvent on its way to a destination.
type event struct {
	id, source, typ, subject, time, data string
	trace, from                          string // the flow that made the event, and the trigger that delivers it
}

// onEvent fans an event delivered through the hub out to the triggers that match it.
// ponytail: any project's trigger can match, and a failed delivery is logged, not retried.
func (s *Service) onEvent(_ string, m *core.Message) error {
	ev := event{id: core.UUID(), source: m.Source, typ: m.DetailType, subject: m.Subject, time: time.Now().UTC().Format(time.RFC3339Nano), data: m.Body, trace: m.Trace}
	s.mu.Lock()
	var hits []*pb.Trigger
	for _, n := range core.SortedKeys(s.triggers) {
		if matches(s.triggers[n].p, ev) {
			hits = append(hits, clone(s.triggers[n].p))
		}
	}
	s.mu.Unlock()
	for _, tr := range hits {
		core.Go("eventarc.onEvent", func() {
			if err := s.deliver(tr, ev); err != nil {
				s.hub.Log.Warn("eventarc delivery failed", "trigger", tr.Name, "type", ev.typ, "err", err)
			}
		})
	}
	return nil
}

// matches applies a trigger's event filters: attributes compare equal, or by
// path pattern (* within a segment, ** across segments) for operator "match-path-pattern".
func matches(tr *pb.Trigger, ev event) bool {
	for _, f := range tr.EventFilters {
		var got string
		switch f.Attribute {
		case "type":
			got = ev.typ
		case "bucket":
			_, got, _ = strings.Cut(ev.source, "/buckets/")
		case "subject":
			got = ev.subject
		case "source":
			got = ev.source
		default:
			return false // audit-log filters (serviceName, methodName, ...) never match: pogi emits no audit logs
		}
		if f.Operator == "match-path-pattern" {
			re := "^" + strings.NewReplacer(`\*\*`, ".*", `\*`, "[^/]*").Replace(regexp.QuoteMeta(f.Value)) + "$"
			if ok, _ := regexp.MatchString(re, got); !ok {
				return false
			}
		} else if got != f.Value {
			return false
		}
	}
	return true
}

// deliver sends the event to the trigger's destination in binary-mode CloudEvents form.
func (s *Service) deliver(tr *pb.Trigger, ev event) error {
	attrs := map[string]string{"ce-id": ev.id, "ce-source": ev.source, "ce-specversion": "1.0", "ce-type": ev.typ, "ce-time": ev.time,
		"content-type": or(tr.EventDataContentType, "application/json")}
	if ev.subject != "" {
		attrs["ce-subject"] = ev.subject
	}
	if _, bucket, ok := strings.Cut(ev.source, "//storage.googleapis.com/projects/_/buckets/"); ok {
		attrs["ce-bucket"] = bucket
	}
	project, loc := split(tr.Name)
	ev.from = "//eventarc.googleapis.com/" + tr.Name
	switch d := tr.GetDestination().GetDescriptor_().(type) {
	case *pb.Destination_CloudRun:
		return s.hub.Deliver(fmt.Sprintf("//run.googleapis.com/projects/%s/locations/%s/services/%s", project, or(d.CloudRun.Region, loc), d.CloudRun.Service),
			message(ev, attrs, or(d.CloudRun.Path, "/")))
	case *pb.Destination_CloudFunction:
		return s.hub.Deliver("//cloudfunctions.googleapis.com/"+d.CloudFunction, message(ev, attrs, "/"))
	case *pb.Destination_HttpEndpoint:
		req, err := http.NewRequest(http.MethodPost, d.HttpEndpoint.Uri, strings.NewReader(ev.data))
		if err != nil {
			return err
		}
		for k, v := range attrs {
			req.Header.Set(k, v)
		}
		if t := core.Traceparent(ev.trace); t != "" {
			req.Header.Set("traceparent", t)
		}
		start := time.Now()
		resp, err := s.client.Do(req)
		if err != nil {
			s.hub.Span(core.SpanNotify, "eventarc", "deliver", ev.from, d.HttpEndpoint.Uri, ev.trace, start, err)
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			err = fmt.Errorf("%s answered %d", d.HttpEndpoint.Uri, resp.StatusCode)
		}
		s.hub.Span(core.SpanNotify, "eventarc", "deliver", ev.from, d.HttpEndpoint.Uri, ev.trace, start, err)
		return err
	}
	return nil // ponytail: workflow and GKE destinations are stored, not delivered
}

func message(ev event, attrs map[string]string, path string) *core.Message {
	m := &core.Message{Body: ev.data, Subject: path, Source: ev.source, DetailType: ev.typ, Trace: ev.trace, From: ev.from, Attributes: map[string]core.MessageAttribute{}}
	for k, v := range attrs {
		m.Attributes[k] = core.MessageAttribute{DataType: "String", StringValue: v}
	}
	return m
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// push receives a Pub/Sub trigger's push deliveries, wraps each message as a
// messagePublished CloudEvent and delivers it; a non-2xx answer makes Pub/Sub retry.
func (s *Service) push(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, pushPath)
	body, _ := io.ReadAll(r.Body)
	var env struct {
		Message struct {
			MessageID   string `json:"messageId"`
			PublishTime string `json:"publishTime"`
		} `json:"message"`
		Subscription string `json:"subscription"`
	}
	s.mu.Lock()
	t := s.triggers[name]
	var tr *pb.Trigger
	if t != nil {
		tr = clone(t.p)
	}
	s.mu.Unlock()
	if tr == nil || json.Unmarshal(body, &env) != nil {
		http.Error(w, "unknown trigger or bad push body", http.StatusNotFound)
		return
	}
	ev := event{id: or(env.Message.MessageID, core.UUID()), source: "//pubsub.googleapis.com/" + tr.GetTransport().GetPubsub().GetTopic(),
		typ: pubsubType, time: or(env.Message.PublishTime, time.Now().UTC().Format(time.RFC3339Nano)), data: string(body), trace: core.C(r).Trace}
	if err := s.deliver(tr, ev); err != nil {
		s.hub.Log.Warn("eventarc delivery failed", "trigger", name, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- console ----

func destination(p *pb.Trigger) string {
	switch d := p.GetDestination().GetDescriptor_().(type) {
	case *pb.Destination_CloudRun:
		return "Cloud Run " + d.CloudRun.Service
	case *pb.Destination_CloudFunction:
		return "Cloud Function " + d.CloudFunction
	case *pb.Destination_HttpEndpoint:
		return d.HttpEndpoint.Uri
	case *pb.Destination_Workflow:
		return "Workflow " + d.Workflow
	}
	return "-"
}

// Resources lists triggers, channels and channel connections.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	tt := core.Table{Name: "Triggers", Columns: []string{"Name", "Event type", "Filters", "Destination", "Created"}}
	for _, n := range core.SortedKeys(s.triggers) {
		p := s.triggers[n].p
		typ, filters := "", []string{}
		for _, f := range p.EventFilters {
			if f.Attribute == "type" {
				typ = f.Value
			} else {
				filters = append(filters, f.Attribute+"="+f.Value)
			}
		}
		tt.Rows = append(tt.Rows, []string{n, typ, strings.Join(filters, ", "), destination(p), p.CreateTime.AsTime().Format(time.DateTime)})
	}
	ct := core.Table{Name: "Channels", Columns: []string{"Name", "Provider", "State", "Created"}}
	for _, n := range core.SortedKeys(s.channels) {
		c := s.channels[n]
		ct.Rows = append(ct.Rows, []string{n, c.Provider, c.State.String(), c.CreateTime.AsTime().Format(time.DateTime)})
	}
	return []core.Table{tt, ct}
}

// Inspect returns a trigger or a channel as the API shows it.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Triggers":
		if t := s.triggers[id]; t != nil {
			return pj(t.p)
		}
	case "Channels":
		if c := s.channels[id]; c != nil {
			return pj(c)
		}
	}
	return nil
}
