// Package pubsub emulates Google Cloud Pub/Sub: the gRPC API the client
// libraries use (PUBSUB_EMULATOR_HOST=localhost:7644) and its REST mirror.
// It covers topics, pull, streaming-pull and push subscriptions with ack
// deadlines, filters, ordering keys, retry backoff and dead-letter topics,
// snapshots and seek, and schemas. GCS bucket notifications arrive through the hub.
package pubsub

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/iam/apiv1/iampb"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const day = 24 * time.Hour

// Service is the Pub/Sub emulator. The gRPC handlers live on the embedded
// server, so the console helper Publish keeps its short signature.
type Service struct{ *server }

type server struct {
	pb.UnimplementedPublisherServer
	pb.UnimplementedSubscriberServer
	pb.UnimplementedSchemaServiceServer
	iampb.UnimplementedIAMPolicyServer

	hub *core.Hub
	mu  sync.Mutex
	// traces is the flow of each message, by message id, kept beside the message so
	// subscribers never see an attribute the publisher did not send.
	// ponytail: bounded to the newest traceKeep ids and not persisted; a restart forgets old flows.
	traces map[string]string
	kick   chan struct{} // wakes the delivery loop early
	done   chan struct{}
	stop   sync.Once
	state
}

type state struct {
	topics    map[string]*pb.Topic
	subs      map[string]*sub
	snaps     map[string]*snap
	schemas   map[string]*pb.Schema
	policies  map[string]*iampb.Policy // IAM policies by resource name
	published map[string]int64         // messages published per topic
	nextID    int64
}

func newState() state {
	return state{topics: map[string]*pb.Topic{}, subs: map[string]*sub{}, snaps: map[string]*snap{},
		schemas: map[string]*pb.Schema{}, policies: map[string]*iampb.Policy{}, published: map[string]int64{}}
}

// sub is a subscription and its backlog in publish order.
type sub struct {
	p     *pb.Subscription
	msgs  []*msg
	match func(map[string]string) bool // compiled filter, nil matches everything
	wake  chan struct{}                // closed and replaced whenever delivery may proceed
}

// msg is a message in one subscription. Leases are not persisted.
type msg struct {
	m         *pb.PubsubMessage // shared between subscriptions, never mutated
	attempts  int32
	acked     bool // kept only with retain_acked_messages
	ackID     string
	deadline  time.Time // lease expiry
	notBefore time.Time // retry backoff
	owner     *stream
}

// stream is one StreamingPull call.
type stream struct{ ackDeadline time.Duration }

// snap is a snapshot: the backlog when it was taken.
type snap struct {
	p       *pb.Snapshot
	msgs    []*pb.PubsubMessage
	created time.Time
}

// New creates the Pub/Sub service and starts its delivery loop.
func New(hub *core.Hub) *Service {
	s := &Service{&server{hub: hub, kick: make(chan struct{}, 1), done: make(chan struct{}), state: newState(), traces: map[string]string{}}}
	hub.OnDeliver("pubsub", func(topic string, m *core.Message) error {
		attrs := map[string]string{}
		for k, v := range m.Attributes {
			attrs[k] = v.StringValue
		}
		_, err := s.publish(topic, m.Trace, []*pb.PubsubMessage{{Data: []byte(m.Body), Attributes: attrs, OrderingKey: m.GroupID}})
		return err
	})
	core.Go("pubsub.loop", func() { s.loop() })
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "pubsub", Cloud: core.GCP, Title: "Pub/Sub", Match: match,
		Targets: []string{"google.pubsub.v1.Publisher", "google.pubsub.v1.Subscriber", "google.pubsub.v1.SchemaService"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	pb.RegisterPublisherServer(g, s.server)
	pb.RegisterSubscriberServer(g, s.server)
	pb.RegisterSchemaServiceServer(g, s.server)
}

// Close stops the delivery loop.
func (s *Service) Close() error {
	s.stop.Do(func() { close(s.done) })
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sb := range s.subs {
		sb.notify() // streams on dropped subscriptions see them gone and end
	}
	s.state = newState()
}

// ---- persistence ----

type saved struct {
	NextID    int64                      `json:"nextId"`
	Published map[string]int64           `json:"published,omitempty"`
	Topics    []json.RawMessage          `json:"topics,omitempty"`
	Subs      []savedSub                 `json:"subscriptions,omitempty"`
	Snapshots []savedSnap                `json:"snapshots,omitempty"`
	Schemas   []json.RawMessage          `json:"schemas,omitempty"`
	Policies  map[string]json.RawMessage `json:"policies,omitempty"`
}

type savedSub struct {
	Sub  json.RawMessage `json:"subscription"`
	Msgs []savedMsg      `json:"messages,omitempty"`
}

type savedMsg struct {
	Msg      json.RawMessage `json:"message"`
	Attempts int32           `json:"attempts,omitempty"`
	Acked    bool            `json:"acked,omitempty"`
}

type savedSnap struct {
	Snap    json.RawMessage   `json:"snapshot"`
	Created time.Time         `json:"created"`
	Msgs    []json.RawMessage `json:"messages,omitempty"`
}

func pj(m proto.Message) json.RawMessage {
	b, _ := protojson.Marshal(m)
	return b
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{NextID: s.nextID, Published: s.published}
	for _, n := range core.SortedKeys(s.topics) {
		out.Topics = append(out.Topics, pj(s.topics[n]))
	}
	for _, n := range core.SortedKeys(s.subs) {
		sb := s.subs[n]
		ss := savedSub{Sub: pj(sb.p)}
		for _, m := range sb.msgs {
			ss.Msgs = append(ss.Msgs, savedMsg{pj(m.m), m.attempts, m.acked})
		}
		out.Subs = append(out.Subs, ss)
	}
	for _, n := range core.SortedKeys(s.snaps) {
		sn := s.snaps[n]
		ss := savedSnap{Snap: pj(sn.p), Created: sn.created}
		for _, m := range sn.msgs {
			ss.Msgs = append(ss.Msgs, pj(m))
		}
		out.Snapshots = append(out.Snapshots, ss)
	}
	for _, n := range core.SortedKeys(s.schemas) {
		out.Schemas = append(out.Schemas, pj(s.schemas[n]))
	}
	out.Policies = savePolicies(s.policies)
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	var err error
	dec := func(raw json.RawMessage, m proto.Message) {
		if err == nil {
			err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
		}
	}
	st := newState()
	st.nextID = in.NextID
	maps.Copy(st.published, in.Published)
	for _, raw := range in.Topics {
		t := &pb.Topic{}
		dec(raw, t)
		st.topics[t.Name] = t
	}
	for _, ss := range in.Subs {
		sb := &sub{p: &pb.Subscription{}, wake: make(chan struct{})}
		dec(ss.Sub, sb.p)
		sb.match, _ = compile(sb.p.Filter)
		for _, sm := range ss.Msgs {
			m := &msg{m: &pb.PubsubMessage{}, attempts: sm.Attempts, acked: sm.Acked}
			dec(sm.Msg, m.m)
			sb.msgs = append(sb.msgs, m)
		}
		st.subs[sb.p.Name] = sb
	}
	for _, ss := range in.Snapshots {
		sn := &snap{p: &pb.Snapshot{}, created: ss.Created}
		dec(ss.Snap, sn.p)
		for _, raw := range ss.Msgs {
			m := &pb.PubsubMessage{}
			dec(raw, m)
			sn.msgs = append(sn.msgs, m)
		}
		st.snaps[sn.p.Name] = sn
	}
	for _, raw := range in.Schemas {
		sc := &pb.Schema{}
		dec(raw, sc)
		st.schemas[sc.Name] = sc
	}
	st.policies = loadPolicies(in.Policies)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sb := range s.subs {
		sb.notify()
	}
	s.state = st
	return nil
}

// ---- names and helpers ----

var (
	resName   = regexp.MustCompile(`^projects/[^/]+/(topics|subscriptions|snapshots|schemas)/([A-Za-z][-A-Za-z0-9_.~+%]{2,254})$`)
	projectRE = regexp.MustCompile(`^projects/[^/]+$`)
)

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

// checkName validates projects/{project}/{kind}/{id}.
func checkName(kind, name string) error {
	m := resName.FindStringSubmatch(name)
	if m == nil || m[1] != kind || strings.HasPrefix(m[2], "goog") {
		return invalid("Invalid [%s] name: (name=%s)", kind, name)
	}
	return nil
}

func checkProject(name string) error {
	if !projectRE.MatchString(name) {
		return invalid("Invalid [projects] name: (name=%s)", name)
	}
	return nil
}

func (s *server) topic(name string) (*pb.Topic, error) {
	if err := checkName("topics", name); err != nil {
		return nil, err
	}
	if t := s.topics[name]; t != nil {
		return t, nil
	}
	return nil, status.Error(codes.NotFound, "Topic not found")
}

func (s *server) sub(name string) (*sub, error) {
	if err := checkName("subscriptions", name); err != nil {
		return nil, err
	}
	if sb := s.subs[name]; sb != nil {
		return sb, nil
	}
	return nil, status.Error(codes.NotFound, "Subscription does not exist")
}

// pullable is a subscription that can still deliver messages.
func (s *server) pullable(name string) (*sub, error) {
	sb, err := s.sub(name)
	if err == nil && sb.p.Detached {
		err = status.Error(codes.FailedPrecondition, "Subscription is detached")
	}
	return sb, err
}

func (s *server) snap(name string) (*snap, error) {
	if err := checkName("snapshots", name); err != nil {
		return nil, err
	}
	if sn := s.snaps[name]; sn != nil {
		return sn, nil
	}
	return nil, status.Error(codes.NotFound, "Snapshot not found")
}

func checkRetention(d *durationpb.Duration) error {
	if d != nil && (d.AsDuration() < 10*time.Minute || d.AsDuration() > 31*day) {
		return invalid("message_retention_duration must be between 10 minutes and 31 days")
	}
	return nil
}

// names returns the sorted keys of m that pass keep.
func names[V any](m map[string]V, keep func(string, V) bool) []string {
	var out []string
	for k, v := range m {
		if keep(k, v) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// page cuts one page from a sorted list; the token is the offset of the next page.
func page[T any](items []T, size int32, token string) ([]T, string) {
	start, _ := strconv.Atoi(token)
	start = min(max(start, 0), len(items))
	end := len(items)
	if size > 0 {
		end = min(end, start+int(size))
	}
	if end < len(items) {
		return items[start:end], strconv.Itoa(end)
	}
	return items[start:end], ""
}

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

// update copies the fields named by an update mask from src to dst.
func update(dst, src proto.Message, paths []string, allowed ...string) error {
	if len(paths) == 0 {
		return invalid("The update_mask in the request must be set")
	}
	src = proto.Clone(src)
	for _, path := range paths {
		parts := strings.Split(path, ".")
		if !slices.Contains(allowed, parts[0]) {
			return invalid("Invalid update_mask provided in the request: %q", path)
		}
		dm, sm := dst.ProtoReflect(), src.ProtoReflect()
		for i, name := range parts {
			fd := dm.Descriptor().Fields().ByName(protoreflect.Name(name))
			last := i == len(parts)-1
			switch {
			case fd == nil || !last && (fd.Message() == nil || fd.IsList() || fd.IsMap()):
				return invalid("Invalid update_mask provided in the request: %q", path)
			case !last:
				dm, sm = dm.Mutable(fd).Message(), sm.Get(fd).Message()
			case sm.Has(fd):
				dm.Set(fd, sm.Get(fd))
			default:
				dm.Clear(fd)
			}
		}
	}
	return nil
}

// ---- topics ----

func (s *server) CreateTopic(_ context.Context, t *pb.Topic) (*pb.Topic, error) {
	if err := checkName("topics", t.Name); err != nil {
		return nil, err
	}
	if err := checkRetention(t.MessageRetentionDuration); err != nil {
		return nil, err
	}
	t.State = pb.Topic_ACTIVE
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSchemaSettings(t.SchemaSettings); err != nil {
		return nil, err
	}
	if s.topics[t.Name] != nil {
		return nil, status.Error(codes.AlreadyExists, "Topic already exists")
	}
	s.topics[t.Name] = clone(t)
	return t, nil
}

func (s *server) GetTopic(_ context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topic(req.Topic)
	if err != nil {
		return nil, err
	}
	return clone(t), nil
}

func (s *server) UpdateTopic(_ context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topic(req.GetTopic().GetName())
	if err != nil {
		return nil, err
	}
	t = clone(t)
	if err := update(t, req.Topic, req.GetUpdateMask().GetPaths(), "labels", "message_storage_policy", "kms_key_name",
		"schema_settings", "message_retention_duration", "ingestion_data_source_settings", "message_transforms"); err != nil {
		return nil, err
	}
	if err := checkRetention(t.MessageRetentionDuration); err != nil {
		return nil, err
	}
	if err := s.checkSchemaSettings(t.SchemaSettings); err != nil {
		return nil, err
	}
	s.topics[t.Name] = t
	return clone(t), nil
}

func (s *server) ListTopics(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	if err := checkProject(req.Project); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list, next := page(names(s.topics, func(n string, _ *pb.Topic) bool { return strings.HasPrefix(n, req.Project+"/") }), req.PageSize, req.PageToken)
	out := &pb.ListTopicsResponse{NextPageToken: next}
	for _, n := range list {
		out.Topics = append(out.Topics, clone(s.topics[n]))
	}
	return out, nil
}

func (s *server) ListTopicSubscriptions(_ context.Context, req *pb.ListTopicSubscriptionsRequest) (*pb.ListTopicSubscriptionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.topic(req.Topic); err != nil {
		return nil, err
	}
	list, next := page(names(s.subs, func(_ string, sb *sub) bool { return sb.p.Topic == req.Topic && !sb.p.Detached }), req.PageSize, req.PageToken)
	return &pb.ListTopicSubscriptionsResponse{Subscriptions: list, NextPageToken: next}, nil
}

func (s *server) ListTopicSnapshots(_ context.Context, req *pb.ListTopicSnapshotsRequest) (*pb.ListTopicSnapshotsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.topic(req.Topic); err != nil {
		return nil, err
	}
	list, next := page(names(s.snaps, func(_ string, sn *snap) bool { return sn.p.Topic == req.Topic }), req.PageSize, req.PageToken)
	return &pb.ListTopicSnapshotsResponse{Snapshots: list, NextPageToken: next}, nil
}

// DeleteTopic keeps the subscriptions; they point at "_deleted-topic_".
func (s *server) DeleteTopic(_ context.Context, req *pb.DeleteTopicRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.topic(req.Topic); err != nil {
		return nil, err
	}
	delete(s.topics, req.Topic)
	delete(s.published, req.Topic)
	for _, sb := range s.subs {
		if sb.p.Topic == req.Topic {
			sb.p.Topic = "_deleted-topic_"
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *server) DetachSubscription(_ context.Context, req *pb.DetachSubscriptionRequest) (*pb.DetachSubscriptionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.Subscription)
	if err != nil {
		return nil, err
	}
	sb.p.Detached, sb.msgs = true, nil
	sb.notify()
	return &pb.DetachSubscriptionResponse{}, nil
}

// restTrace marks a Publish that came in over REST: the request is already a
// step of its flow, so no publish span is recorded for it.
type restTrace struct{}

const traceKeep = 100000 // messages whose flow is remembered

const traceAttr = "googclient_traceparent" // how a message carries its flow, as the client libraries set it

func (s *server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	trace, rest := ctx.Value(restTrace{}).(string)
	if !rest {
		md, _ := metadata.FromIncomingContext(ctx)
		if v := md.Get("traceparent"); len(v) > 0 {
			trace = core.TraceFromTraceparent(v[0])
		}
		if trace == "" {
			trace = core.NewTrace()
		}
	}
	start := time.Now()
	ids, err := s.publish(req.Topic, trace, req.Messages)
	if !rest {
		s.hub.Span(core.SpanDeliver, "pubsub", "publish", "", "//pubsub.googleapis.com/"+req.Topic, trace, start, err)
	}
	if err != nil {
		return nil, err
	}
	return &pb.PublishResponse{MessageIds: ids}, nil
}

// publish queues messages on a topic. A message that does not carry a
// traceparent attribute is tracked under trace, so push and pull deliveries stay in the flow.
func (s *server) publish(topic, trace string, msgs []*pb.PubsubMessage) ([]string, error) {
	if len(msgs) == 0 {
		return nil, invalid("The request contains no messages")
	}
	for _, m := range msgs {
		if len(m.Data) == 0 && len(m.Attributes) == 0 {
			return nil, invalid("Some messages are empty: each message needs data or at least one attribute")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topic(topic)
	if err != nil {
		return nil, err
	}
	if ss := t.GetSchemaSettings(); ss != nil {
		if sc := s.schemas[strings.Split(ss.Schema, "@")[0]]; sc != nil { // a deleted schema leaves the topic unchecked
			for _, m := range msgs {
				if err := messageMatches(sc, ss.Encoding, m.Data); err != nil {
					return nil, err
				}
			}
		}
	}
	return s.fanout(topic, trace, msgs), nil
}

// traceOf is the flow of a message: the traceparent attribute when the client sent one, else the one pogi tracked. Lock held.
func (s *server) traceOf(m *pb.PubsubMessage) string {
	return cmp.Or(core.TraceFromTraceparent(m.GetAttributes()[traceAttr]), s.traces[m.GetMessageId()])
}

// fanout stamps messages and queues them on every attached subscription
// whose filter matches; non-matching messages count as acked. Lock held.
func (s *server) fanout(topic, trace string, msgs []*pb.PubsubMessage) []string {
	now := timestamppb.Now()
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		s.nextID++
		m.MessageId, m.PublishTime = strconv.FormatInt(s.nextID, 10), now
		ids[i] = m.MessageId
		if t := cmp.Or(core.TraceFromTraceparent(m.Attributes[traceAttr]), trace); t != "" {
			s.traces[m.MessageId] = t
		}
		delete(s.traces, strconv.FormatInt(s.nextID-traceKeep, 10))
	}
	s.published[topic] += int64(len(msgs))
	for _, sb := range s.subs {
		if sb.p.Topic != topic || sb.p.Detached {
			continue
		}
		for _, m := range msgs {
			if sb.match == nil || sb.match(m.Attributes) {
				sb.msgs = append(sb.msgs, &msg{m: m})
			}
		}
		sb.notify()
	}
	s.poke()
	return ids
}

// ---- subscriptions ----

// check validates a subscription and fills in defaults; it returns the compiled filter. Lock held.
func (s *server) check(p *pb.Subscription) (func(map[string]string) bool, error) {
	if p.AckDeadlineSeconds == 0 {
		p.AckDeadlineSeconds = 10
	}
	// ponytail: Pub/Sub's minimum is 10s; shorter deadlines are allowed so tests can see redelivery quickly.
	if p.AckDeadlineSeconds < 1 || p.AckDeadlineSeconds > 600 {
		return nil, invalid("Invalid ack_deadline_seconds: %d (must be between 10 and 600)", p.AckDeadlineSeconds)
	}
	if p.MessageRetentionDuration == nil {
		p.MessageRetentionDuration = durationpb.New(7 * day)
	}
	if err := checkRetention(p.MessageRetentionDuration); err != nil {
		return nil, err
	}
	if p.ExpirationPolicy == nil {
		p.ExpirationPolicy = &pb.ExpirationPolicy{Ttl: durationpb.New(31 * day)}
	}
	if p.PushConfig == nil {
		p.PushConfig = &pb.PushConfig{}
	}
	if dl := p.DeadLetterPolicy; dl != nil {
		if dl.MaxDeliveryAttempts == 0 {
			dl.MaxDeliveryAttempts = 5
		}
		if dl.MaxDeliveryAttempts < 5 || dl.MaxDeliveryAttempts > 100 {
			return nil, invalid("max_delivery_attempts must be between 5 and 100")
		}
		if _, err := s.topic(dl.DeadLetterTopic); err != nil {
			return nil, err
		}
	}
	match, err := compile(p.Filter)
	if err != nil {
		return nil, invalid("Invalid filter: %v", err)
	}
	return match, nil
}

func (s *server) CreateSubscription(_ context.Context, p *pb.Subscription) (*pb.Subscription, error) {
	if err := checkName("subscriptions", p.Name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subs[p.Name] != nil {
		return nil, status.Error(codes.AlreadyExists, "Subscription already exists")
	}
	t, err := s.topic(p.Topic)
	if err != nil {
		return nil, err
	}
	match, err := s.check(p)
	if err != nil {
		return nil, err
	}
	p.TopicMessageRetentionDuration, p.State = t.MessageRetentionDuration, pb.Subscription_ACTIVE
	s.subs[p.Name] = &sub{p: clone(p), match: match, wake: make(chan struct{})}
	return p, nil
}

func (s *server) GetSubscription(_ context.Context, req *pb.GetSubscriptionRequest) (*pb.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.Subscription)
	if err != nil {
		return nil, err
	}
	return clone(sb.p), nil
}

func (s *server) UpdateSubscription(_ context.Context, req *pb.UpdateSubscriptionRequest) (*pb.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.GetSubscription().GetName())
	if err != nil {
		return nil, err
	}
	p := clone(sb.p)
	// topic, filter and enable_message_ordering are immutable, as in Pub/Sub.
	if err := update(p, req.Subscription, req.GetUpdateMask().GetPaths(), "push_config", "bigquery_config", "cloud_storage_config",
		"ack_deadline_seconds", "retain_acked_messages", "message_retention_duration", "labels", "expiration_policy",
		"dead_letter_policy", "retry_policy", "enable_exactly_once_delivery", "message_transforms"); err != nil {
		return nil, err
	}
	if _, err := s.check(p); err != nil {
		return nil, err
	}
	sb.p = p
	sb.compact(time.Now())
	sb.notify()
	s.poke()
	return clone(p), nil
}

func (s *server) ListSubscriptions(_ context.Context, req *pb.ListSubscriptionsRequest) (*pb.ListSubscriptionsResponse, error) {
	if err := checkProject(req.Project); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list, next := page(names(s.subs, func(n string, _ *sub) bool { return strings.HasPrefix(n, req.Project+"/") }), req.PageSize, req.PageToken)
	out := &pb.ListSubscriptionsResponse{NextPageToken: next}
	for _, n := range list {
		out.Subscriptions = append(out.Subscriptions, clone(s.subs[n].p))
	}
	return out, nil
}

func (s *server) DeleteSubscription(_ context.Context, req *pb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.Subscription)
	if err != nil {
		return nil, err
	}
	delete(s.subs, req.Subscription)
	sb.notify()
	return &emptypb.Empty{}, nil
}

func (s *server) ModifyPushConfig(_ context.Context, req *pb.ModifyPushConfigRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.Subscription)
	if err != nil {
		return nil, err
	}
	sb.p.PushConfig = req.PushConfig
	if sb.p.PushConfig == nil {
		sb.p.PushConfig = &pb.PushConfig{}
	}
	sb.notify()
	s.poke()
	return &emptypb.Empty{}, nil
}

// ---- delivery ----

func (sb *sub) notify() {
	close(sb.wake)
	sb.wake = make(chan struct{})
}

func (s *server) poke() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// lease hands out up to max deliverable messages in publish order. On ordered
// subscriptions a key's consecutive messages go out as one batch to one
// subscriber, and nothing more of that key until the batch is acked. Lock held.
func (s *server) lease(sb *sub, max int, d time.Duration, owner *stream) []*pb.ReceivedMessage {
	now := time.Now()
	var out []*pb.ReceivedMessage
	size := 0
	key := func(m *msg) string {
		if sb.p.EnableMessageOrdering {
			return m.m.OrderingKey
		}
		return ""
	}
	busy := map[string]bool{}
	for _, m := range sb.msgs {
		if k := key(m); k != "" && m.ackID != "" {
			busy[k] = true
		}
	}
	for _, m := range sb.msgs {
		if len(out) >= max || size > 4<<20 {
			break
		}
		k := key(m)
		if m.acked || m.ackID != "" || k != "" && busy[k] {
			continue
		}
		if now.Before(m.notBefore) {
			if k != "" {
				busy[k] = true // a message in retry backoff holds back the rest of its key
			}
			continue
		}
		m.attempts++
		m.ackID, m.deadline, m.owner = m.m.MessageId+":"+strconv.Itoa(int(m.attempts)), now.Add(d), owner
		rm := &pb.ReceivedMessage{AckId: m.ackID, Message: m.m}
		if sb.p.DeadLetterPolicy != nil {
			rm.DeliveryAttempt = m.attempts
		}
		out = append(out, rm)
		size += len(m.m.Data)
	}
	return out
}

// ack acknowledges messages; an older ack ID of the same message still counts. Lock held.
func (s *server) ack(sb *sub, ackIDs []string) {
	ids := map[string]bool{}
	for _, a := range ackIDs {
		id, _, _ := strings.Cut(a, ":")
		ids[id] = true
	}
	for _, m := range sb.msgs {
		if ids[m.m.MessageId] {
			m.acked, m.ackID, m.owner = true, "", nil
		}
	}
	sb.compact(time.Now())
	sb.notify()
}

// modack moves lease deadlines; 0 seconds nacks. Lock held.
func (s *server) modack(sb *sub, deadlines map[string]int32) {
	now := time.Now()
	for _, m := range sb.msgs {
		secs, ok := deadlines[m.ackID]
		switch {
		case !ok || m.ackID == "":
		case secs > 0:
			m.deadline = now.Add(time.Duration(secs) * time.Second)
		default:
			s.release(sb, m, now)
		}
	}
	sb.compact(now)
	sb.notify()
}

// release ends a lease without an ack: the message comes back after the retry
// backoff, or moves to the dead-letter topic after max attempts. Lock held.
func (s *server) release(sb *sub, m *msg, now time.Time) {
	m.ackID, m.owner = "", nil
	dl := sb.p.DeadLetterPolicy
	if dl == nil || m.attempts < dl.MaxDeliveryAttempts {
		if d := backoff(sb.p, m.attempts); d > 0 {
			m.notBefore = now.Add(d)
		}
		return
	}
	m.acked = true
	if s.topics[dl.DeadLetterTopic] == nil {
		return
	}
	attrs := maps.Clone(m.m.Attributes)
	if attrs == nil {
		attrs = map[string]string{}
	}
	name := strings.Split(sb.p.Name, "/")
	attrs["CloudPubSubDeadLetterSourceDeliveryCount"] = strconv.Itoa(int(m.attempts))
	attrs["CloudPubSubDeadLetterSourceSubscription"] = name[3]
	attrs["CloudPubSubDeadLetterSourceSubscriptionProject"] = name[1]
	attrs["CloudPubSubDeadLetterSourceTopicPublishTime"] = m.m.PublishTime.AsTime().Format(time.RFC3339Nano)
	trace := s.traceOf(m.m)
	s.fanout(dl.DeadLetterTopic, trace, []*pb.PubsubMessage{{Data: m.m.Data, Attributes: attrs, OrderingKey: m.m.OrderingKey}})
	s.hub.Span(core.SpanRedrive, "pubsub", "dead-letter", "//pubsub.googleapis.com/"+sb.p.Name, "//pubsub.googleapis.com/"+dl.DeadLetterTopic, trace, time.Now(), nil)
}

// backoff is the delay before redelivering after a failed attempt: the retry
// policy if set, 100ms..60s for push, immediate for pull.
func backoff(p *pb.Subscription, attempts int32) time.Duration {
	var lo, hi time.Duration
	if p.PushConfig.GetPushEndpoint() != "" {
		lo, hi = 100*time.Millisecond, time.Minute
	}
	if rp := p.RetryPolicy; rp != nil {
		lo, hi = 10*time.Second, 600*time.Second
		if rp.MinimumBackoff != nil {
			lo = rp.MinimumBackoff.AsDuration()
		}
		if rp.MaximumBackoff != nil {
			hi = rp.MaximumBackoff.AsDuration()
		}
	}
	return min(lo<<min(max(attempts-1, 0), 20), hi)
}

// compact drops acked messages (unless retained) and messages past retention. Lock held.
func (sb *sub) compact(now time.Time) {
	keep := 7 * day
	if d := sb.p.MessageRetentionDuration; d != nil {
		keep = d.AsDuration()
	}
	sb.msgs = slices.DeleteFunc(sb.msgs, func(m *msg) bool {
		return m.acked && !sb.p.RetainAckedMessages || now.Sub(m.m.PublishTime.AsTime()) > keep
	})
}

func (s *server) Acknowledge(_ context.Context, req *pb.AcknowledgeRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.Subscription)
	if err != nil {
		return nil, err
	}
	s.ack(sb, req.AckIds)
	return &emptypb.Empty{}, nil
}

func (s *server) ModifyAckDeadline(_ context.Context, req *pb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	if req.AckDeadlineSeconds < 0 || req.AckDeadlineSeconds > 600 {
		return nil, invalid("Invalid ack_deadline_seconds: %d (must be between 0 and 600)", req.AckDeadlineSeconds)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(req.Subscription)
	if err != nil {
		return nil, err
	}
	deadlines := map[string]int32{}
	for _, id := range req.AckIds {
		deadlines[id] = req.AckDeadlineSeconds
	}
	s.modack(sb, deadlines)
	return &emptypb.Empty{}, nil
}

// pullWait bounds how long a Pull without return_immediately waits for messages.
const pullWait = 3 * time.Second

func (s *server) Pull(ctx context.Context, req *pb.PullRequest) (*pb.PullResponse, error) {
	if req.MaxMessages <= 0 {
		return nil, invalid("max_messages must be a positive integer")
	}
	timeout := time.After(pullWait)
	for {
		s.mu.Lock()
		sb, err := s.pullable(req.Subscription)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		msgs := s.lease(sb, int(req.MaxMessages), time.Duration(sb.p.AckDeadlineSeconds)*time.Second, nil)
		wake := sb.wake
		s.mu.Unlock()
		s.pulled(sb, msgs)
		if len(msgs) > 0 || req.ReturnImmediately {
			return &pb.PullResponse{ReceivedMessages: msgs}, nil
		}
		select {
		case <-wake:
		case <-timeout:
			return &pb.PullResponse{}, nil
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

// pulled records the hop of each leased message to its puller, so the flow shows it.
func (s *server) pulled(sb *sub, rms []*pb.ReceivedMessage) {
	s.mu.Lock()
	traces := make([]string, len(rms))
	for i, rm := range rms {
		traces[i] = s.traceOf(rm.Message)
	}
	s.mu.Unlock()
	for _, t := range traces {
		if t != "" {
			s.hub.Span(core.SpanDeliver, "pubsub", "pull", "//pubsub.googleapis.com/"+sb.p.Topic, "//pubsub.googleapis.com/"+sb.p.Name, t, time.Now(), nil)
		}
	}
}

// StreamingPull sends messages as they arrive, up to max_outstanding_messages
// unacked on this stream, and answers client pings with empty responses.
func (s *server) StreamingPull(ss pb.Subscriber_StreamingPullServer) error {
	req, err := ss.Recv()
	if err != nil {
		return err
	}
	if d := req.StreamAckDeadlineSeconds; d < 10 || d > 600 {
		return invalid("stream_ack_deadline_seconds must be between 10 and 600")
	}
	st := &stream{ackDeadline: time.Duration(req.StreamAckDeadlineSeconds) * time.Second}
	s.mu.Lock()
	sb, err := s.pullable(req.Subscription)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	errc, ping := make(chan error, 1), make(chan struct{}, 1)
	core.Go("pubsub.StreamingPull", func() {
		defer func() {
			if r := recover(); r != nil {
				errc <- status.Error(codes.Internal, "internal error") // end the stream, then let core.Go log it
				panic(r)
			}
		}()
		for {
			r, err := ss.Recv()
			if err != nil {
				errc <- err
				return
			}
			s.mu.Lock()
			if r.StreamAckDeadlineSeconds > 0 {
				st.ackDeadline = time.Duration(r.StreamAckDeadlineSeconds) * time.Second
			}
			if len(r.AckIds) > 0 {
				s.ack(sb, r.AckIds)
			}
			if len(r.ModifyDeadlineAckIds) > 0 {
				deadlines := map[string]int32{}
				for i, id := range r.ModifyDeadlineAckIds {
					if i < len(r.ModifyDeadlineSeconds) {
						deadlines[id] = r.ModifyDeadlineSeconds[i]
					}
				}
				s.modack(sb, deadlines)
			}
			s.mu.Unlock()
			if len(r.AckIds)+len(r.ModifyDeadlineAckIds) == 0 {
				select {
				case ping <- struct{}{}:
				default:
				}
			}
		}
	})
	for {
		s.mu.Lock()
		if s.subs[sb.p.Name] != sb {
			s.mu.Unlock()
			return status.Error(codes.NotFound, "Subscription does not exist")
		}
		if sb.p.Detached {
			s.mu.Unlock()
			return status.Error(codes.FailedPrecondition, "Subscription is detached")
		}
		n := 1000
		if limit := req.MaxOutstandingMessages; limit > 0 {
			out := 0
			for _, m := range sb.msgs {
				if m.owner == st && m.ackID != "" {
					out++
				}
			}
			n = min(n, int(limit)-out)
		}
		res := &pb.StreamingPullResponse{ReceivedMessages: s.lease(sb, n, st.ackDeadline, st),
			SubscriptionProperties: &pb.StreamingPullResponse_SubscriptionProperties{
				ExactlyOnceDeliveryEnabled: sb.p.EnableExactlyOnceDelivery, MessageOrderingEnabled: sb.p.EnableMessageOrdering}}
		wake := sb.wake
		s.mu.Unlock()
		s.pulled(sb, res.ReceivedMessages)
		if len(res.ReceivedMessages) > 0 {
			if err := ss.Send(res); err != nil {
				return err
			}
			continue
		}
		select {
		case <-wake:
		case <-ping:
			if err := ss.Send(res); err != nil {
				return err
			}
		case err := <-errc:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-ss.Context().Done():
			return status.FromContextError(ss.Context().Err()).Err()
		}
	}
}

// loop runs expiries, retries and push deliveries.
func (s *server) loop() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		case <-s.kick:
		}
		func() { defer core.Recover("pubsub.tick"); s.tick() }() // a panic skips one tick
	}
}

// tick expires leases, ends retry backoffs, applies retention, expires snapshots and starts pushes.
// ponytail: scans every message 10 times a second; index leases by deadline if backlogs get huge.
func (s *server) tick() {
	now := time.Now()
	var jobs []pushJob
	s.mu.Lock()
	for _, sb := range s.subs {
		changed := false
		for _, m := range sb.msgs {
			if m.ackID != "" && now.After(m.deadline) {
				s.release(sb, m, now)
				changed = true
			}
			if !m.notBefore.IsZero() && !now.Before(m.notBefore) {
				m.notBefore, changed = time.Time{}, true
			}
		}
		sb.compact(now)
		if changed {
			sb.notify()
		}
		if url := sb.p.PushConfig.GetPushEndpoint(); url != "" && !sb.p.Detached {
			jobs = append(jobs, s.pushJobs(sb, url)...)
		}
	}
	for name, sn := range s.snaps {
		if t := sn.p.ExpireTime; t != nil && t.AsTime().Before(now) {
			delete(s.snaps, name)
		}
	}
	s.mu.Unlock()
	for _, j := range jobs {
		core.Go("pubsub.push", func() { s.push(j) })
	}
}

type pushJob struct {
	sb      *sub
	url     string
	ackID   string
	body    []byte
	timeout time.Duration
	trace   string // the flow of the message, from its attribute or the tracked one
}

// pushJobs leases messages of a push subscription, at most 50 in flight. Lock held.
func (s *server) pushJobs(sb *sub, url string) []pushJob {
	inflight := 0
	for _, m := range sb.msgs {
		if m.ackID != "" {
			inflight++
		}
	}
	d := time.Duration(sb.p.AckDeadlineSeconds) * time.Second
	var out []pushJob
	for _, rm := range s.lease(sb, 50-inflight, d, nil) {
		out = append(out, pushJob{sb, url, rm.AckId, pushBody(sb.p, rm), d, s.traceOf(rm.GetMessage())})
	}
	return out
}

// pushBody is what push endpoints receive: the JSON envelope, or the raw data with no_wrapper.
func pushBody(p *pb.Subscription, rm *pb.ReceivedMessage) []byte {
	m := rm.Message
	if p.PushConfig.GetNoWrapper() != nil {
		return m.Data
	}
	type message struct {
		Data         []byte            `json:"data,omitempty"`
		Attributes   map[string]string `json:"attributes,omitempty"`
		MessageID    string            `json:"messageId"`
		MessageID2   string            `json:"message_id"`
		PublishTime  string            `json:"publishTime"`
		PublishTime2 string            `json:"publish_time"`
		OrderingKey  string            `json:"orderingKey,omitempty"`
	}
	t := m.PublishTime.AsTime().Format(time.RFC3339Nano)
	b, _ := json.Marshal(struct {
		Message         message `json:"message"`
		Subscription    string  `json:"subscription"`
		DeliveryAttempt int32   `json:"deliveryAttempt,omitempty"`
	}{message{m.Data, m.Attributes, m.MessageId, m.MessageId, t, t, m.OrderingKey}, p.Name, rm.DeliveryAttempt})
	return b
}

// push POSTs one message; a 2xx (or 102) acks it, anything else nacks it.
func (s *server) push(j pushJob) {
	ctx, cancel := context.WithTimeout(context.Background(), j.timeout)
	defer cancel()
	code, start := 0, time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.url, bytes.NewReader(j.body))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		if t := core.Traceparent(j.trace); t != "" {
			req.Header.Set("traceparent", t)
		}
		var resp *http.Response
		if resp, err = http.DefaultClient.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			code = resp.StatusCode
		}
	}
	if err == nil && code/100 != 2 && code != 102 {
		err = fmt.Errorf("HTTP %d", code)
	}
	s.hub.Span(core.SpanNotify, "pubsub", "push", "//pubsub.googleapis.com/"+j.sb.p.Name, j.url, j.trace, start, err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subs[j.sb.p.Name] != j.sb {
		return
	}
	if code/100 == 2 || code == 102 {
		s.ack(j.sb, []string{j.ackID})
		return
	}
	s.hub.Log.Warn("pubsub push failed", "subscription", j.sb.p.Name, "endpoint", j.url, "status", code, "err", err)
	s.modack(j.sb, map[string]int32{j.ackID: 0})
}

// ---- snapshots and seek ----

func (s *server) CreateSnapshot(_ context.Context, req *pb.CreateSnapshotRequest) (*pb.Snapshot, error) {
	if err := checkName("snapshots", req.Name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snaps[req.Name] != nil {
		return nil, status.Error(codes.AlreadyExists, "Snapshot already exists")
	}
	sb, err := s.pullable(req.Subscription)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	sn := &snap{p: &pb.Snapshot{Name: req.Name, Topic: sb.p.Topic, Labels: req.Labels}, created: now}
	oldest := now
	for _, m := range sb.msgs {
		if !m.acked {
			sn.msgs = append(sn.msgs, m.m)
			if t := m.m.PublishTime.AsTime(); t.Before(oldest) {
				oldest = t
			}
		}
	}
	sn.p.ExpireTime = timestamppb.New(oldest.Add(7 * day))
	s.snaps[req.Name] = sn
	return clone(sn.p), nil
}

func (s *server) GetSnapshot(_ context.Context, req *pb.GetSnapshotRequest) (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snap(req.Snapshot)
	if err != nil {
		return nil, err
	}
	return clone(sn.p), nil
}

func (s *server) ListSnapshots(_ context.Context, req *pb.ListSnapshotsRequest) (*pb.ListSnapshotsResponse, error) {
	if err := checkProject(req.Project); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list, next := page(names(s.snaps, func(n string, _ *snap) bool { return strings.HasPrefix(n, req.Project+"/") }), req.PageSize, req.PageToken)
	out := &pb.ListSnapshotsResponse{NextPageToken: next}
	for _, n := range list {
		out.Snapshots = append(out.Snapshots, clone(s.snaps[n].p))
	}
	return out, nil
}

func (s *server) UpdateSnapshot(_ context.Context, req *pb.UpdateSnapshotRequest) (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snap(req.GetSnapshot().GetName())
	if err != nil {
		return nil, err
	}
	p := clone(sn.p)
	if err := update(p, req.Snapshot, req.GetUpdateMask().GetPaths(), "labels", "expire_time"); err != nil {
		return nil, err
	}
	sn.p = p
	return clone(p), nil
}

func (s *server) DeleteSnapshot(_ context.Context, req *pb.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.snap(req.Snapshot); err != nil {
		return nil, err
	}
	delete(s.snaps, req.Snapshot)
	return &emptypb.Empty{}, nil
}

// Seek marks messages published before a time as acked and the rest as
// unacked, or restores a snapshot's backlog plus everything published since.
// Only retained messages (retain_acked_messages) can be replayed.
func (s *server) Seek(_ context.Context, req *pb.SeekRequest) (*pb.SeekResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.pullable(req.Subscription)
	if err != nil {
		return nil, err
	}
	var unacked func(*pb.PubsubMessage) bool
	switch t := req.Target.(type) {
	case *pb.SeekRequest_Time:
		at := t.Time.AsTime()
		unacked = func(m *pb.PubsubMessage) bool { return !m.PublishTime.AsTime().Before(at) }
	case *pb.SeekRequest_Snapshot:
		sn, err := s.snap(t.Snapshot)
		if err != nil {
			return nil, err
		}
		if sn.p.Topic != sb.p.Topic {
			return nil, invalid("The snapshot and the subscription must belong to the same topic")
		}
		kept, have := map[string]bool{}, map[string]bool{}
		for _, m := range sb.msgs {
			have[m.m.MessageId] = true
		}
		for _, m := range sn.msgs {
			kept[m.MessageId] = true
			if !have[m.MessageId] {
				sb.msgs = append(sb.msgs, &msg{m: m})
			}
		}
		slices.SortFunc(sb.msgs, func(a, b *msg) int {
			return cmp.Or(cmp.Compare(len(a.m.MessageId), len(b.m.MessageId)), strings.Compare(a.m.MessageId, b.m.MessageId))
		})
		unacked = func(m *pb.PubsubMessage) bool { return kept[m.MessageId] || !m.PublishTime.AsTime().Before(sn.created) }
	default:
		return nil, invalid("Seek needs a time or a snapshot")
	}
	for _, m := range sb.msgs {
		m.acked = !unacked(m.m)
		m.ackID, m.owner, m.notBefore = "", nil, time.Time{}
	}
	sb.compact(time.Now())
	sb.notify()
	return &pb.SeekResponse{}, nil
}

// ---- schemas ----

// checkSchemaSettings refuses a topic that names a schema that does not exist. Lock held.
func (s *server) checkSchemaSettings(ss *pb.SchemaSettings) error {
	if ss == nil {
		return nil
	}
	_, err := s.schema(ss.Schema)
	return err
}

func (s *server) CreateSchema(_ context.Context, req *pb.CreateSchemaRequest) (*pb.Schema, error) {
	name := req.Parent + "/schemas/" + req.SchemaId
	if err := checkName("schemas", name); err != nil {
		return nil, err
	}
	sc := req.GetSchema()
	if sc.GetDefinition() == "" || sc.GetType() == pb.Schema_TYPE_UNSPECIFIED {
		return nil, invalid("A schema needs a type and a definition")
	}
	if err := checkSchema(sc); err != nil {
		return nil, err
	}
	sc.Name, sc.RevisionId, sc.RevisionCreateTime = name, core.RandHex(4), timestamppb.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schemas[name] != nil {
		return nil, status.Error(codes.AlreadyExists, "Schema already exists")
	}
	s.schemas[name] = clone(sc)
	return sc, nil
}

func (s *server) schema(name string) (*pb.Schema, error) {
	name, _, _ = strings.Cut(name, "@") // revision suffix
	if sc := s.schemas[name]; sc != nil {
		return sc, nil
	}
	return nil, status.Error(codes.NotFound, "Schema not found")
}

func (s *server) GetSchema(_ context.Context, req *pb.GetSchemaRequest) (*pb.Schema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.schema(req.Name)
	if err != nil {
		return nil, err
	}
	sc = clone(sc)
	if req.View == pb.SchemaView_BASIC {
		sc.Definition = ""
	}
	return sc, nil
}

func (s *server) ListSchemas(_ context.Context, req *pb.ListSchemasRequest) (*pb.ListSchemasResponse, error) {
	if err := checkProject(req.Parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list, next := page(names(s.schemas, func(n string, _ *pb.Schema) bool { return strings.HasPrefix(n, req.Parent+"/") }), req.PageSize, req.PageToken)
	out := &pb.ListSchemasResponse{NextPageToken: next}
	for _, n := range list {
		sc := clone(s.schemas[n])
		if req.View != pb.SchemaView_FULL {
			sc.Definition = ""
		}
		out.Schemas = append(out.Schemas, sc)
	}
	return out, nil
}

func (s *server) DeleteSchema(_ context.Context, req *pb.DeleteSchemaRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.schema(req.Name); err != nil {
		return nil, err
	}
	delete(s.schemas, req.Name)
	return &emptypb.Empty{}, nil
}

func (s *server) ValidateSchema(_ context.Context, req *pb.ValidateSchemaRequest) (*pb.ValidateSchemaResponse, error) {
	if err := checkSchema(req.GetSchema()); err != nil {
		return nil, err
	}
	return &pb.ValidateSchemaResponse{}, nil
}

func (s *server) ValidateMessage(_ context.Context, req *pb.ValidateMessageRequest) (*pb.ValidateMessageResponse, error) {
	sc := req.GetSchema()
	if name := req.GetName(); name != "" {
		if err := checkName("schemas", strings.Split(name, "@")[0]); err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		stored, err := s.schema(name)
		if err != nil {
			return nil, err
		}
		sc = stored
	}
	if sc == nil {
		return nil, invalid("A schema name or a schema is required")
	}
	if err := messageMatches(sc, req.Encoding, req.Message); err != nil {
		return nil, err
	}
	return &pb.ValidateMessageResponse{}, nil
}

// ---- filters ----

type pred = func(map[string]string) bool

var filterTok = regexp.MustCompile(`^(?:"(?:[^"\\]|\\.)*"|!=|[():,=-]|[A-Za-z0-9_.][-A-Za-z0-9_.]*)`)

// compile parses a subscription filter: attributes.k = "v", attributes.k != "v",
// attributes:k, hasPrefix(attributes.k, "p"), NOT (or -), AND, OR and parentheses.
func compile(src string) (pred, error) {
	var p fparser
	for rest := strings.TrimSpace(src); rest != ""; {
		tok := filterTok.FindString(rest)
		if tok == "" {
			return nil, fmt.Errorf("unexpected %q", rest)
		}
		p.toks = append(p.toks, tok)
		rest = strings.TrimSpace(rest[len(tok):])
	}
	if len(p.toks) == 0 {
		return nil, nil
	}
	f, err := p.or()
	if err == nil && p.i < len(p.toks) {
		err = fmt.Errorf("unexpected %q", p.toks[p.i])
	}
	return f, err
}

type fparser struct {
	toks []string
	i    int
}

func (p *fparser) peek() string {
	if p.i < len(p.toks) {
		return p.toks[p.i]
	}
	return ""
}

func (p *fparser) next() string {
	t := p.peek()
	p.i++
	return t
}

func (p *fparser) or() (pred, error) {
	return p.chain("OR", p.and, func(a, b bool) bool { return a || b })
}

func (p *fparser) and() (pred, error) {
	return p.chain("AND", p.unary, func(a, b bool) bool { return a && b })
}

func (p *fparser) chain(op string, operand func() (pred, error), join func(a, b bool) bool) (pred, error) {
	l, err := operand()
	for err == nil && p.peek() == op {
		p.i++
		var r pred
		if r, err = operand(); err == nil {
			a := l
			l = func(m map[string]string) bool { return join(a(m), r(m)) }
		}
	}
	return l, err
}

func (p *fparser) unary() (pred, error) {
	switch tok := p.next(); {
	case tok == "NOT" || tok == "-":
		f, err := p.unary()
		if err != nil {
			return nil, err
		}
		return func(m map[string]string) bool { return !f(m) }, nil
	case tok == "(":
		f, err := p.or()
		if err == nil && p.next() != ")" {
			err = errors.New("missing )")
		}
		return f, err
	case tok == "hasPrefix":
		key, ok := "", p.next() == "("
		if ok {
			key, ok = strings.CutPrefix(p.next(), "attributes.")
		}
		ok = ok && key != "" && p.next() == ","
		prefix, err := strconv.Unquote(p.next())
		if !ok || err != nil || p.next() != ")" {
			return nil, errors.New(`want hasPrefix(attributes.KEY, "PREFIX")`)
		}
		return func(m map[string]string) bool { v, ok := m[key]; return ok && strings.HasPrefix(v, prefix) }, nil
	case tok == "attributes" && p.peek() == ":":
		p.i++
		key := p.next()
		if k, err := strconv.Unquote(key); err == nil {
			key = k
		}
		if key == "" || key == ")" {
			return nil, errors.New("want attributes:KEY")
		}
		return func(m map[string]string) bool { _, ok := m[key]; return ok }, nil
	default:
		key, ok := strings.CutPrefix(tok, "attributes.")
		op := p.next()
		want, err := strconv.Unquote(p.next())
		if !ok || key == "" || err != nil || op != "=" && op != "!=" {
			return nil, fmt.Errorf("unexpected %q", tok)
		}
		if op == "=" {
			return func(m map[string]string) bool { v, ok := m[key]; return ok && v == want }, nil
		}
		return func(m map[string]string) bool { v, ok := m[key]; return !ok || v != want }, nil
	}
}

// ---- REST mirror ----

// match claims /v1/projects/{p}/{topics,subscriptions,snapshots,schemas}…
func match(r *http.Request) bool {
	seg := strings.Split(r.URL.Path, "/")
	if len(seg) < 5 || seg[1] != "v1" || seg[2] != "projects" {
		return false
	}
	coll, _, _ := strings.Cut(seg[4], ":")
	return coll == "topics" || coll == "subscriptions" || coll == "snapshots" || coll == "schemas"
}

// route maps a REST call onto a gRPC method: field takes the resource named
// by the path, body is the request field the JSON body fills ("" = all).
type route struct {
	desc        *grpc.ServiceDesc
	method      string
	field, body string
}

var routes = func() map[string]route {
	pub, subs, sch := &pb.Publisher_ServiceDesc, &pb.Subscriber_ServiceDesc, &pb.SchemaService_ServiceDesc
	iam := &iampb.IAMPolicy_ServiceDesc
	r := map[string]route{
		"GET topics":                             {pub, "ListTopics", "project", ""},
		"PUT topics/*":                           {pub, "CreateTopic", "name", ""},
		"GET topics/*":                           {pub, "GetTopic", "topic", ""},
		"PATCH topics/*":                         {pub, "UpdateTopic", "topic.name", ""},
		"DELETE topics/*":                        {pub, "DeleteTopic", "topic", ""},
		"POST topics/*:publish":                  {pub, "Publish", "topic", ""},
		"GET topics/*/subscriptions":             {pub, "ListTopicSubscriptions", "topic", ""},
		"GET topics/*/snapshots":                 {pub, "ListTopicSnapshots", "topic", ""},
		"POST subscriptions/*:detach":            {pub, "DetachSubscription", "subscription", ""},
		"GET subscriptions":                      {subs, "ListSubscriptions", "project", ""},
		"PUT subscriptions/*":                    {subs, "CreateSubscription", "name", ""},
		"GET subscriptions/*":                    {subs, "GetSubscription", "subscription", ""},
		"PATCH subscriptions/*":                  {subs, "UpdateSubscription", "subscription.name", ""},
		"DELETE subscriptions/*":                 {subs, "DeleteSubscription", "subscription", ""},
		"POST subscriptions/*:pull":              {subs, "Pull", "subscription", ""},
		"POST subscriptions/*:acknowledge":       {subs, "Acknowledge", "subscription", ""},
		"POST subscriptions/*:modifyAckDeadline": {subs, "ModifyAckDeadline", "subscription", ""},
		"POST subscriptions/*:modifyPushConfig":  {subs, "ModifyPushConfig", "subscription", ""},
		"POST subscriptions/*:seek":              {subs, "Seek", "subscription", ""},
		"GET snapshots":                          {subs, "ListSnapshots", "project", ""},
		"PUT snapshots/*":                        {subs, "CreateSnapshot", "name", ""},
		"GET snapshots/*":                        {subs, "GetSnapshot", "snapshot", ""},
		"PATCH snapshots/*":                      {subs, "UpdateSnapshot", "snapshot.name", ""},
		"DELETE snapshots/*":                     {subs, "DeleteSnapshot", "snapshot", ""},
		"GET schemas":                            {sch, "ListSchemas", "parent", ""},
		"POST schemas":                           {sch, "CreateSchema", "parent", "schema"},
		"GET schemas/*":                          {sch, "GetSchema", "name", ""},
		"DELETE schemas/*":                       {sch, "DeleteSchema", "name", ""},
		"POST schemas:validate":                  {sch, "ValidateSchema", "parent", ""},
		"POST schemas:validateMessage":           {sch, "ValidateMessage", "parent", ""},
	}
	for _, c := range []string{"topics", "subscriptions", "snapshots", "schemas"} {
		r["GET "+c+"/*:getIamPolicy"] = route{iam, "GetIamPolicy", "resource", ""}
		r["POST "+c+"/*:setIamPolicy"] = route{iam, "SetIamPolicy", "resource", ""}
		r["POST "+c+"/*:testIamPermissions"] = route{iam, "TestIamPermissions", "resource", ""}
	}
	return r
}()

// ServeHTTP serves the REST mirror by calling the gRPC handlers.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	if !match(r) {
		writeErr(w, c, status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	seg := strings.Split(r.URL.Path, "/")
	parts, verb := seg[4:], ""
	if last, v, ok := strings.Cut(parts[len(parts)-1], ":"); ok {
		parts[len(parts)-1], verb = last, ":"+v
	}
	shape := slices.Clone(parts)
	for i := 1; i < len(shape); i += 2 {
		shape[i] = "*"
	}
	rt, ok := routes[r.Method+" "+strings.Join(shape, "/")+verb]
	c.Op = rt.method
	if !ok {
		writeErr(w, c, status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeJSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	res := "projects/" + seg[3]
	if n := len(parts) &^ 1; n > 0 {
		res += "/" + strings.Join(parts[:n], "/")
	}
	out, err := rest(r.WithContext(context.WithValue(r.Context(), restTrace{}, c.Trace)), s.server, rt, res)
	if err != nil {
		writeErr(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// rest builds the gRPC request from the JSON body, the query parameters and
// the resource name in the path, and calls the handler.
func rest(r *http.Request, srv any, rt route, res string) (proto.Message, error) {
	in, err := gapi.ParseBody(core.Body(r), rt.body)
	if err != nil {
		return nil, err
	}
	for k, v := range r.URL.Query() {
		in[k] = v[0]
	}
	setPath(in, rt.field, res)
	for _, m := range rt.desc.Methods {
		if m.MethodName != rt.method {
			continue
		}
		out, err := m.Handler(srv, r.Context(), func(req any) error {
			return gapi.Decode(req.(proto.Message), in)
		}, nil)
		if err != nil {
			return nil, err
		}
		return out.(proto.Message), nil
	}
	return nil, status.Errorf(codes.Unimplemented, "%s is not implemented", rt.method)
}

// setPath stores v at a dotted path of a JSON object.
func setPath(m map[string]any, path, v string) {
	k, rest, nested := strings.Cut(path, ".")
	if !nested {
		m[k] = v
		return
	}
	child, _ := m[k].(map[string]any)
	if child == nil {
		child = map[string]any{}
		m[k] = child
	}
	setPath(child, rest, v)
}

var httpStatus = map[codes.Code]int{codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.OutOfRange: 400,
	codes.Unauthenticated: 401, codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.Aborted: 409,
	codes.ResourceExhausted: 429, codes.Canceled: 499, codes.Unimplemented: 501, codes.Unavailable: 503, codes.DeadlineExceeded: 504}

// writeErr renders a gRPC status as a Google REST error.
func writeErr(w http.ResponseWriter, c *core.Ctx, err error) {
	st := status.Convert(err)
	code, ok := httpStatus[st.Code()]
	if !ok {
		code = http.StatusInternalServerError
	}
	writeJSONErr(w, c, code, rpccode.Code(st.Code()).String(), st.Message())
}

func writeJSONErr(w http.ResponseWriter, c *core.Ctx, code int, name, msg string) {
	c.ErrCode = name
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg, "status": name}})
}

// ---- console ----

// PeekedMessage is a backlog message as the console shows it.
type PeekedMessage struct {
	ID          string            `json:"messageId"`
	Data        string            `json:"data"` // text, or base64 when not UTF-8
	Base64      bool              `json:"base64,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	OrderingKey string            `json:"orderingKey,omitempty"`
	PublishTime time.Time         `json:"publishTime"`
	Attempts    int32             `json:"deliveryAttempts,omitempty"`
	Leased      bool              `json:"leased,omitempty"`
}

// Publish publishes one message to a topic (projects/p/topics/t) and returns its ID.
func (s *Service) Publish(topic string, data []byte, attrs map[string]string) (string, error) {
	ids, err := s.publish(topic, "", []*pb.PubsubMessage{{Data: data, Attributes: attrs}})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// Peek returns up to max unacked messages of a subscription without leasing them.
func (s *Service) Peek(subscription string, max int) []PeekedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sb := s.subs[subscription]; sb != nil {
		return peek(sb, max)
	}
	return nil
}

func peek(sb *sub, max int) []PeekedMessage {
	out := []PeekedMessage{}
	for _, m := range sb.msgs {
		if len(out) >= max {
			break
		}
		if m.acked {
			continue
		}
		pm := PeekedMessage{ID: m.m.MessageId, Data: string(m.m.Data), Attributes: m.m.Attributes, OrderingKey: m.m.OrderingKey,
			PublishTime: m.m.PublishTime.AsTime(), Attempts: m.attempts, Leased: m.ackID != ""}
		if !utf8.Valid(m.m.Data) {
			pm.Data, pm.Base64 = base64.StdEncoding.EncodeToString(m.m.Data), true
		}
		out = append(out, pm)
	}
	return out
}

// Resources lists topics and subscriptions.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	topics := core.Table{Name: "Topics", Columns: []string{"Name", "Project", "Subscriptions", "Messages"}}
	for _, n := range core.SortedKeys(s.topics) {
		subs := 0
		for _, sb := range s.subs {
			if sb.p.Topic == n && !sb.p.Detached {
				subs++
			}
		}
		topics.Rows = append(topics.Rows, []string{n, strings.Split(n, "/")[1], strconv.Itoa(subs), strconv.FormatInt(s.published[n], 10)})
	}
	subs := core.Table{Name: "Subscriptions", Columns: []string{"Name", "Topic", "Backlog", "Unacked", "Type"}}
	for _, n := range core.SortedKeys(s.subs) {
		sb := s.subs[n]
		backlog, unacked := 0, 0
		for _, m := range sb.msgs {
			switch {
			case m.acked:
			case m.ackID != "":
				unacked++
			default:
				backlog++
			}
		}
		typ := "pull"
		if sb.p.PushConfig.GetPushEndpoint() != "" {
			typ = "push"
		}
		subs.Rows = append(subs.Rows, []string{n, sb.p.Topic, strconv.Itoa(backlog), strconv.Itoa(unacked), typ})
	}
	return []core.Table{topics, subs}
}

// Inspect returns a topic, or a subscription with up to 20 backlog messages.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Topics":
		if t := s.topics[id]; t != nil {
			return map[string]any{"topic": pj(t), "subscriptions": names(s.subs, func(_ string, sb *sub) bool { return sb.p.Topic == id })}
		}
	case "Subscriptions":
		if sb := s.subs[id]; sb != nil {
			return map[string]any{"subscription": pj(sb.p), "messages": peek(sb, 20)}
		}
	}
	return nil
}
