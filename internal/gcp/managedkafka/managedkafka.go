// Package managedkafka emulates Google Cloud Managed Service for Apache Kafka
// (managedkafka.googleapis.com v1) over gRPC and its REST mirror: clusters
// (long-running operations), topics and consumer groups. With a container
// engine every cluster is one Redpanda broker, the runner MSK uses, and
// topic and consumer group calls go to that broker; without one the API
// answers from metadata and clusters are still ACTIVE.
//
// Google's bootstrap address is bootstrap.<cluster>.<region>.managedkafka.<project>.cloud.goog:9092,
// which nothing here can serve. The address of the real broker is the cluster
// label "pogi-bootstrap" (added to every Cluster that has a broker; never
// stored) and "bootstrapAddress" in the console's Inspect.
package managedkafka

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	mkpb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"github.com/nrndev/pogi/internal/aws/kafka"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BootstrapLabel carries a cluster's real bootstrap address.
const BootstrapLabel = "pogi-bootstrap"

const noDocker = "Docker is not available: the API works, but no Kafka broker runs behind the cluster"

var (
	clusterID = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)
	topicID   = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
	parentRE  = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
)

// Service is the Managed Kafka emulator.
type Service struct {
	mkpb.UnimplementedManagedKafkaServer
	hub  *core.Hub
	rest *gapi.REST
	ops  *gapi.Ops
	ctx  context.Context // broker starts; ends at Close
	stop context.CancelFunc

	mu       sync.Mutex
	clusters map[string]*cluster // by full name
}

type cluster struct {
	p         *mkpb.Cluster
	topics    map[string]*mkpb.Topic // created through the API, by id
	container string                 // Redpanda; empty: metadata only
	bootstrap string                 // 127.0.0.1:<port> of its Kafka API
	note      string
	cancel    context.CancelFunc // stops its broker start
}

// New creates the Managed Kafka service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, ops: gapi.Operations(h), clusters: map[string]*cluster{}}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"clusters"}, Desc: &mkpb.ManagedKafka_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"GET locations/*/clusters":                       {Method: "ListClusters", Field: "parent"},
			"POST locations/*/clusters":                      {Method: "CreateCluster", Field: "parent", Body: "cluster"},
			"GET locations/*/clusters/*":                     {Method: "GetCluster", Field: "name"},
			"PATCH locations/*/clusters/*":                   {Method: "UpdateCluster", Field: "cluster.name", Body: "cluster"},
			"DELETE locations/*/clusters/*":                  {Method: "DeleteCluster", Field: "name"},
			"GET locations/*/clusters/*/topics":              {Method: "ListTopics", Field: "parent"},
			"POST locations/*/clusters/*/topics":             {Method: "CreateTopic", Field: "parent", Body: "topic"},
			"GET locations/*/clusters/*/topics/*":            {Method: "GetTopic", Field: "name"},
			"PATCH locations/*/clusters/*/topics/*":          {Method: "UpdateTopic", Field: "topic.name", Body: "topic"},
			"DELETE locations/*/clusters/*/topics/*":         {Method: "DeleteTopic", Field: "name"},
			"GET locations/*/clusters/*/consumerGroups":      {Method: "ListConsumerGroups", Field: "parent"},
			"GET locations/*/clusters/*/consumerGroups/*":    {Method: "GetConsumerGroup", Field: "name"},
			"DELETE locations/*/clusters/*/consumerGroups/*": {Method: "DeleteConsumerGroup", Field: "name"},
			"PATCH locations/*/clusters/*/consumerGroups/*":  {Method: "UpdateConsumerGroup", Field: "consumerGroup.name", Body: "consumerGroup"},
		}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "managedkafka", Cloud: core.GCP, Title: "Managed Kafka", Match: s.rest.Match,
		Targets: []string{"google.cloud.managedkafka.v1.ManagedKafka"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { mkpb.RegisterManagedKafkaServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

// ---- clusters ----

// view is the cluster as clients see it: with the broker's address as a label.
func (cl *cluster) view() *mkpb.Cluster {
	c := proto.Clone(cl.p).(*mkpb.Cluster)
	if cl.bootstrap != "" {
		if c.Labels == nil {
			c.Labels = map[string]string{}
		}
		c.Labels[BootstrapLabel] = cl.bootstrap
	}
	return c
}

func meta(target, verb string) *mkpb.OperationMetadata {
	return &mkpb.OperationMetadata{CreateTime: timestamppb.Now(), Target: target, Verb: verb, ApiVersion: "v1"}
}

// parentOf is projects/p/locations/l of a resource name.
func parentOf(name string) string { return strings.Join(strings.SplitN(name, "/", 5)[:4], "/") }

func (s *Service) CreateCluster(_ context.Context, r *mkpb.CreateClusterRequest) (*longrunningpb.Operation, error) {
	if !parentRE.MatchString(r.Parent) {
		return nil, gapi.Invalid("parent must look like projects/<project>/locations/<location>")
	}
	if !clusterID.MatchString(r.ClusterId) {
		return nil, gapi.Invalid("cluster_id must be 1-63 lowercase letters, digits or hyphens, starting with a letter")
	}
	if r.Cluster == nil {
		return nil, gapi.Invalid("cluster is required")
	}
	name := r.Parent + "/clusters/" + r.ClusterId
	c := proto.Clone(r.Cluster).(*mkpb.Cluster)
	c.Name, c.CreateTime, c.UpdateTime, c.State = name, timestamppb.Now(), timestamppb.Now(), mkpb.Cluster_CREATING
	delete(c.Labels, BootstrapLabel)
	cl := &cluster{p: c, topics: map[string]*mkpb.Topic{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clusters[name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists.", name)
	}
	s.clusters[name] = cl
	if !s.hub.Docker().Available() {
		s.activate(cl, "", noDocker)
		s.hub.Log.Warn(noDocker, "service", "managedkafka", "cluster", name)
		return s.ops.Done(r.Parent, meta(name, "create"), cl.view()), nil
	}
	cl.container = docker.Name("managedkafka", r.ClusterId[:min(len(r.ClusterId), 40)]+"-"+core.NewID()[:8])
	return s.ops.Start(r.Parent, meta(name, "create"), func() (proto.Message, error) { return s.boot(cl) }), nil
}

// activate makes a cluster ACTIVE. Lock held.
func (s *Service) activate(cl *cluster, addr, note string) {
	cl.p.State, cl.p.UpdateTime, cl.bootstrap, cl.note = mkpb.Cluster_ACTIVE, timestamppb.Now(), addr, note
	if addr != "" {
		s.hub.SetEndpoint("managedkafka:"+cl.p.Name, cl.container+":9092") // for containers on pogi's network
	}
}

// boot runs the broker of a cluster and makes it ACTIVE once the broker
// answers; a cluster whose broker fails is removed and the operation fails.
func (s *Service) boot(cl *cluster) (proto.Message, error) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	s.mu.Lock()
	cl.cancel = cancel
	port, _ := strconv.Atoi(cl.bootstrap[strings.LastIndexByte(cl.bootstrap, ':')+1:]) // kept when the container must be made again
	s.mu.Unlock()
	rp := &kafka.Redpanda{Name: cl.container, Service: "managedkafka", Resource: cl.p.Name[strings.LastIndexByte(cl.p.Name, '/')+1:], Port: port}
	addr, err := rp.Run(ctx, s.hub.Docker())
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.clusters[cl.p.Name] != cl: // deleted meanwhile
		return nil, status.Error(codes.Aborted, "the cluster was deleted while it was being created")
	case err != nil:
		delete(s.clusters, cl.p.Name)
		go s.hub.Docker().Remove(context.Background(), cl.container)
		s.hub.Log.Warn("managed kafka cluster failed", "cluster", cl.p.Name, "err", err)
		return nil, status.Errorf(codes.Internal, "the broker did not start: %v", err)
	}
	s.activate(cl, addr, "")
	return cl.view(), nil
}

func (s *Service) GetCluster(_ context.Context, r *mkpb.GetClusterRequest) (*mkpb.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(r.Name)
	if err != nil {
		return nil, err
	}
	return cl.view(), nil
}

// cluster finds a cluster by name. Lock held.
func (s *Service) cluster(name string) (*cluster, error) {
	if cl := s.clusters[name]; cl != nil {
		return cl, nil
	}
	return nil, gapi.NotFound("Resource '%s' was not found", name)
}

func (s *Service) ListClusters(_ context.Context, r *mkpb.ListClustersRequest) (*mkpb.ListClustersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*mkpb.Cluster
	for _, n := range core.SortedKeys(s.clusters) {
		if strings.HasPrefix(n, r.Parent+"/clusters/") {
			all = append(all, s.clusters[n].view())
		}
	}
	items, next, err := page(all, r.PageSize, r.PageToken)
	return &mkpb.ListClustersResponse{Clusters: items, NextPageToken: next}, err
}

func (s *Service) UpdateCluster(_ context.Context, r *mkpb.UpdateClusterRequest) (*longrunningpb.Operation, error) {
	if r.Cluster == nil {
		return nil, gapi.Invalid("cluster is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(r.Cluster.Name)
	if err != nil {
		return nil, err
	}
	in := proto.Clone(r.Cluster).(*mkpb.Cluster)
	delete(in.Labels, BootstrapLabel)
	c := proto.Clone(cl.p).(*mkpb.Cluster)
	if err := applyMask(c, in, r.UpdateMask.GetPaths()); err != nil {
		return nil, err
	}
	c.UpdateTime = timestamppb.Now()
	cl.p = c
	return s.ops.Done(parentOf(c.Name), meta(c.Name, "update"), cl.view()), nil
}

func (s *Service) DeleteCluster(_ context.Context, r *mkpb.DeleteClusterRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(r.Name)
	if err != nil {
		return nil, err
	}
	cl.p.State = mkpb.Cluster_DELETING
	if cl.cancel != nil {
		cl.cancel()
	}
	return s.ops.Start(parentOf(r.Name), meta(r.Name, "delete"), func() (proto.Message, error) {
		s.mu.Lock()
		s.drop(cl)
		delete(s.clusters, r.Name)
		s.mu.Unlock()
		return nil, nil
	}), nil
}

// drop removes the container of a cluster that is gone, unless a current
// cluster runs in it (the same state, loaded again). Lock held.
func (s *Service) drop(cl *cluster) {
	if cl.cancel != nil {
		cl.cancel()
	}
	for _, x := range s.clusters {
		if x != cl && x.container == cl.container {
			return
		}
	}
	if cl.container != "" {
		s.hub.SetEndpoint("managedkafka:"+cl.p.Name, "")
		go s.hub.Docker().Remove(context.Background(), cl.container)
	}
}

// ---- topics ----

// broker returns the Kafka admin client of a cluster, nil with no broker; the
// cluster must be ACTIVE. Close the client once done.
func (s *Service) broker(clusterName string) (*kadm.Client, func(), error) {
	s.mu.Lock()
	cl, err := s.cluster(clusterName)
	var addr string
	if err == nil {
		if cl.p.State != mkpb.Cluster_ACTIVE {
			err = status.Errorf(codes.FailedPrecondition, "cluster %s is %s", clusterName, cl.p.State)
		}
		if cl.container != "" {
			addr = cl.bootstrap
		}
	}
	s.mu.Unlock()
	if err != nil || addr == "" {
		return nil, func() {}, err
	}
	kc, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return nil, func() {}, status.Errorf(codes.Unavailable, "%v", err)
	}
	return kadm.NewClient(kc), kc.Close, nil
}

// split cuts a topic or group name into its cluster and id.
func split(name, kind string) (cluster, id string, err error) {
	i := strings.Index(name, "/clusters/")
	if i >= 0 {
		var rest string
		cluster, rest, _ = strings.Cut(name[i+1:], "/"+kind+"/")
		cluster, id = name[:i+1]+cluster, rest
		if !parentRE.MatchString(parentOf(cluster)) || rest == "" {
			i = -1
		}
	}
	if i < 0 {
		return "", "", gapi.Invalid("name must look like projects/<project>/locations/<location>/clusters/<cluster>/%s/<id>", kind)
	}
	return cluster, id, nil
}

func kerror(err error, what string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kerr.TopicAlreadyExists):
		return gapi.Exists("%s already exists.", what)
	case errors.Is(err, kerr.UnknownTopicOrPartition), errors.Is(err, kerr.GroupIDNotFound):
		return gapi.NotFound("Resource '%s' was not found", what)
	case errors.Is(err, kerr.NonEmptyGroup):
		return status.Errorf(codes.FailedPrecondition, "%s still has members", what)
	case errors.Is(err, kerr.InvalidPartitions), errors.Is(err, kerr.InvalidConfig), errors.Is(err, kerr.InvalidTopicException):
		return gapi.Invalid("%v", err)
	}
	return status.Errorf(codes.Internal, "%v", err)
}

func (s *Service) CreateTopic(ctx context.Context, r *mkpb.CreateTopicRequest) (*mkpb.Topic, error) {
	if !topicID.MatchString(r.TopicId) {
		return nil, gapi.Invalid("topic_id must be 1-249 letters, digits, '.', '_' or '-'")
	}
	t := proto.Clone(r.GetTopic()).(*mkpb.Topic)
	if t == nil || t.PartitionCount < 1 || t.ReplicationFactor < 1 {
		return nil, gapi.Invalid("topic needs a partition_count and a replication_factor of at least 1")
	}
	t.Name = r.Parent + "/topics/" + r.TopicId
	adm, done, err := s.broker(r.Parent)
	defer done()
	if err != nil {
		return nil, err
	}
	if adm != nil {
		cfg := map[string]*string{}
		for k, v := range t.Configs {
			cfg[k] = &v
		}
		// ponytail: one broker holds one replica; the requested replication factor is metadata only
		if _, err := adm.CreateTopic(ctx, t.PartitionCount, 1, cfg, r.TopicId); err != nil {
			return nil, kerror(err, t.Name)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(r.Parent)
	if err != nil {
		return nil, err
	}
	if cl.topics[r.TopicId] != nil && adm == nil {
		return nil, gapi.Exists("%s already exists.", t.Name)
	}
	cl.topics[r.TopicId] = t
	return proto.Clone(t).(*mkpb.Topic), nil
}

// brokerTopic is a topic as the broker has it, with the replication factor
// its creator asked for.
func (s *Service) brokerTopic(ctx context.Context, adm *kadm.Client, clusterName string, d kadm.TopicDetail) *mkpb.Topic {
	t := &mkpb.Topic{Name: clusterName + "/topics/" + d.Topic, PartitionCount: int32(len(d.Partitions)), Configs: map[string]string{}}
	if p, ok := d.Partitions[0]; ok {
		t.ReplicationFactor = int32(len(p.Replicas))
	}
	s.mu.Lock()
	if cl := s.clusters[clusterName]; cl != nil && cl.topics[d.Topic] != nil {
		t.ReplicationFactor = cl.topics[d.Topic].ReplicationFactor
	}
	s.mu.Unlock()
	if rcs, err := adm.DescribeTopicConfigs(ctx, d.Topic); err == nil {
		for _, rc := range rcs {
			for _, c := range rc.Configs {
				if c.Source == kmsg.ConfigSourceDynamicTopicConfig && c.Value != nil {
					t.Configs[c.Key] = *c.Value
				}
			}
		}
	}
	return t
}

func (s *Service) GetTopic(ctx context.Context, r *mkpb.GetTopicRequest) (*mkpb.Topic, error) {
	cname, id, err := split(r.Name, "topics")
	if err != nil {
		return nil, err
	}
	adm, done, err := s.broker(cname)
	defer done()
	if err != nil {
		return nil, err
	}
	if adm != nil {
		ds, err := adm.ListTopics(ctx, id)
		if err != nil {
			return nil, kerror(err, r.Name)
		}
		if d := ds[id]; d.Err == nil {
			return s.brokerTopic(ctx, adm, cname, d), nil
		}
		return nil, gapi.NotFound("Resource '%s' was not found", r.Name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl, err := s.cluster(cname); err != nil {
		return nil, err
	} else if t := cl.topics[id]; t != nil {
		return proto.Clone(t).(*mkpb.Topic), nil
	}
	return nil, gapi.NotFound("Resource '%s' was not found", r.Name)
}

func (s *Service) ListTopics(ctx context.Context, r *mkpb.ListTopicsRequest) (*mkpb.ListTopicsResponse, error) {
	adm, done, err := s.broker(r.Parent)
	defer done()
	if err != nil {
		return nil, err
	}
	var all []*mkpb.Topic
	if adm != nil {
		ds, err := adm.ListTopics(ctx)
		if err != nil {
			return nil, kerror(err, r.Parent)
		}
		for _, n := range ds.Names() {
			all = append(all, s.brokerTopic(ctx, adm, r.Parent, ds[n]))
		}
	} else {
		s.mu.Lock()
		cl, _ := s.cluster(r.Parent)
		for _, n := range core.SortedKeys(cl.topics) {
			all = append(all, proto.Clone(cl.topics[n]).(*mkpb.Topic))
		}
		s.mu.Unlock()
	}
	items, next, err := page(all, r.PageSize, r.PageToken)
	return &mkpb.ListTopicsResponse{Topics: items, NextPageToken: next}, err
}

func (s *Service) UpdateTopic(ctx context.Context, r *mkpb.UpdateTopicRequest) (*mkpb.Topic, error) {
	if r.Topic == nil {
		return nil, gapi.Invalid("topic is required")
	}
	cname, id, err := split(r.Topic.Name, "topics")
	if err != nil {
		return nil, err
	}
	cur, err := s.GetTopic(ctx, &mkpb.GetTopicRequest{Name: r.Topic.Name})
	if err != nil {
		return nil, err
	}
	next := proto.Clone(cur).(*mkpb.Topic)
	if err := applyMask(next, r.Topic, r.UpdateMask.GetPaths()); err != nil {
		return nil, err
	}
	if next.PartitionCount < cur.PartitionCount {
		return nil, gapi.Invalid("partition_count can only grow, from %d", cur.PartitionCount)
	}
	adm, done, err := s.broker(cname)
	defer done()
	if err != nil {
		return nil, err
	}
	if adm != nil {
		if next.PartitionCount > cur.PartitionCount {
			if _, err := adm.UpdatePartitions(ctx, int(next.PartitionCount), id); err != nil {
				return nil, kerror(err, r.Topic.Name)
			}
		}
		var alter []kadm.AlterConfig
		for k, v := range next.Configs {
			alter = append(alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &v})
		}
		for k := range cur.Configs {
			if _, ok := next.Configs[k]; !ok {
				alter = append(alter, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
			}
		}
		if len(alter) > 0 {
			rs, err := adm.AlterTopicConfigs(ctx, alter, id)
			if err == nil {
				for _, x := range rs {
					err = errors.Join(err, x.Err)
				}
			}
			if err != nil {
				return nil, kerror(err, r.Topic.Name)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl, err := s.cluster(cname); err == nil {
		cl.topics[id] = proto.Clone(next).(*mkpb.Topic)
	}
	return next, nil
}

func (s *Service) DeleteTopic(ctx context.Context, r *mkpb.DeleteTopicRequest) (*emptypb.Empty, error) {
	cname, id, err := split(r.Name, "topics")
	if err != nil {
		return nil, err
	}
	adm, done, err := s.broker(cname)
	defer done()
	if err != nil {
		return nil, err
	}
	if adm != nil {
		rs, err := adm.DeleteTopics(ctx, id)
		if err == nil {
			for _, x := range rs {
				err = errors.Join(err, x.Err)
			}
		}
		if err != nil {
			return nil, kerror(err, r.Name)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(cname)
	if err != nil {
		return nil, err
	}
	if cl.topics[id] == nil && adm == nil {
		return nil, gapi.NotFound("Resource '%s' was not found", r.Name)
	}
	delete(cl.topics, id)
	return &emptypb.Empty{}, nil
}

// ---- consumer groups (read from the broker; none without one) ----

func (s *Service) group(ctx context.Context, adm *kadm.Client, cname, id string) (*mkpb.ConsumerGroup, error) {
	os, err := adm.FetchOffsets(ctx, id)
	if err != nil {
		return nil, kerror(err, id)
	}
	g := &mkpb.ConsumerGroup{Name: cname + "/consumerGroups/" + id, Topics: map[string]*mkpb.ConsumerTopicMetadata{}}
	for t, ps := range os {
		m := &mkpb.ConsumerTopicMetadata{Partitions: map[int32]*mkpb.ConsumerPartitionMetadata{}}
		for p, o := range ps {
			m.Partitions[p] = &mkpb.ConsumerPartitionMetadata{Offset: o.At, Metadata: o.Metadata}
		}
		g.Topics[t] = m
	}
	return g, nil
}

func (s *Service) ListConsumerGroups(ctx context.Context, r *mkpb.ListConsumerGroupsRequest) (*mkpb.ListConsumerGroupsResponse, error) {
	adm, done, err := s.broker(r.Parent)
	defer done()
	if err != nil {
		return nil, err
	}
	var all []*mkpb.ConsumerGroup
	if adm != nil {
		gs, err := adm.ListGroups(ctx)
		if err != nil {
			return nil, kerror(err, r.Parent)
		}
		for _, l := range gs.Sorted() {
			g, err := s.group(ctx, adm, r.Parent, l.Group)
			if err != nil {
				return nil, err
			}
			all = append(all, g)
		}
	}
	items, next, err := page(all, r.PageSize, r.PageToken)
	return &mkpb.ListConsumerGroupsResponse{ConsumerGroups: items, NextPageToken: next}, err
}

func (s *Service) GetConsumerGroup(ctx context.Context, r *mkpb.GetConsumerGroupRequest) (*mkpb.ConsumerGroup, error) {
	cname, id, err := split(r.Name, "consumerGroups")
	if err != nil {
		return nil, err
	}
	adm, done, err := s.broker(cname)
	defer done()
	if err != nil {
		return nil, err
	}
	if adm != nil {
		if gs, err := adm.ListGroups(ctx); err != nil {
			return nil, kerror(err, r.Name)
		} else if _, ok := gs[id]; ok {
			return s.group(ctx, adm, cname, id)
		}
	}
	return nil, gapi.NotFound("Resource '%s' was not found", r.Name)
}

func (s *Service) DeleteConsumerGroup(ctx context.Context, r *mkpb.DeleteConsumerGroupRequest) (*emptypb.Empty, error) {
	cname, id, err := split(r.Name, "consumerGroups")
	if err != nil {
		return nil, err
	}
	adm, done, err := s.broker(cname)
	defer done()
	if err != nil {
		return nil, err
	}
	if adm == nil {
		return nil, gapi.NotFound("Resource '%s' was not found", r.Name)
	}
	rs, err := adm.DeleteGroups(ctx, id)
	if err == nil {
		for _, x := range rs {
			err = errors.Join(err, x.Err)
		}
	}
	return &emptypb.Empty{}, kerror(err, r.Name)
}

// ---- helpers ----

// page cuts one page of items; the token is the index of the next item.
func page[T any](items []T, size int32, token string) ([]T, string, error) {
	start := 0
	if token != "" {
		var err error
		if start, err = strconv.Atoi(token); err != nil || start < 0 || start > len(items) {
			return nil, "", gapi.Invalid("invalid page_token")
		}
	}
	end := len(items)
	if size > 0 && start+int(size) < end {
		end = start + int(size)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

// applyMask copies the fields the update mask names, dotted paths included,
// from src to dst. Output-only and identity fields cannot be updated.
func applyMask(dst, src proto.Message, paths []string) error {
	if len(paths) == 0 {
		return gapi.Invalid("update_mask is required")
	}
	for _, p := range paths {
		if slices.Contains([]string{"name", "create_time", "update_time", "state"}, strings.Split(p, ".")[0]) {
			return gapi.Invalid("update_mask path %q cannot be updated", p)
		}
		d, s := dst.ProtoReflect(), src.ProtoReflect()
		parts := strings.Split(p, ".")
		for i, n := range parts {
			fd := d.Descriptor().Fields().ByName(protoreflect.Name(n))
			switch {
			case fd == nil:
				return gapi.Invalid("update_mask path %q is not a field", p)
			case i == len(parts)-1:
				if s.Has(fd) {
					d.Set(fd, s.Get(fd))
				} else {
					d.Clear(fd)
				}
			case fd.Message() == nil || fd.IsList() || fd.IsMap():
				return gapi.Invalid("update_mask path %q is not a field", p)
			default:
				d, s = d.Mutable(fd).Message(), s.Get(fd).Message()
			}
		}
	}
	return nil
}

// ---- persistence, console, shutdown ----

type saved struct {
	Cluster   json.RawMessage   `json:"cluster"`
	Topics    []json.RawMessage `json:"topics,omitempty"`
	Container string            `json:"container,omitempty"`
	Bootstrap string            `json:"bootstrap,omitempty"`
	Note      string            `json:"note,omitempty"`
}

func pj(m proto.Message) json.RawMessage {
	b, _ := protojson.Marshal(m)
	return b
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []saved{}
	for _, n := range core.SortedKeys(s.clusters) {
		cl := s.clusters[n]
		sv := saved{Cluster: pj(cl.p), Container: cl.container, Bootstrap: cl.bootstrap, Note: cl.note}
		for _, id := range core.SortedKeys(cl.topics) {
			sv.Topics = append(sv.Topics, pj(cl.topics[id]))
		}
		out = append(out, sv)
	}
	return json.Marshal(out)
}

// Load restores state. Clusters that ran a broker start their containers
// again (persisted state leaves them behind on shutdown), or fresh ones when
// they are gone; without an engine they stay ACTIVE as metadata.
func (s *Service) Load(b []byte) error {
	var in []saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	clusters := map[string]*cluster{}
	for _, sv := range in {
		cl := &cluster{p: &mkpb.Cluster{}, topics: map[string]*mkpb.Topic{}, container: sv.Container, bootstrap: sv.Bootstrap, note: sv.Note}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(sv.Cluster, cl.p); err != nil {
			return err
		}
		for _, raw := range sv.Topics {
			t := &mkpb.Topic{}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, t); err != nil {
				return err
			}
			cl.topics[t.Name[strings.LastIndexByte(t.Name, '/')+1:]] = t
		}
		clusters[cl.p.Name] = cl
	}
	engine := s.hub.Docker().Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.clusters
	s.clusters = clusters
	for _, cl := range clusters {
		switch {
		case cl.container == "":
		case engine:
			cl.p.State = mkpb.Cluster_CREATING
			core.Go("managedkafka.boot", func() { s.boot(cl) })
		default:
			cl.p.State, cl.bootstrap, cl.note = mkpb.Cluster_ACTIVE, "", noDocker
		}
	}
	for _, cl := range old {
		s.drop(cl)
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("[]")) }

// Close stops broker starts. The hub's container engine client then removes
// the containers, unless state is persisted: the next run starts them again.
func (s *Service) Close() error {
	s.stop()
	return nil
}

// Resources lists clusters and the topics created through the API.
// ponytail: topics that clients made on the broker are not listed here, only through the API
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	cls := core.Table{Name: "Clusters", Columns: []string{"Name", "Project", "Location", "State", "vCPUs", "Bootstrap address"}}
	tps := core.Table{Name: "Topics", Columns: []string{"Name", "Cluster", "Partitions", "Replication factor"}}
	for _, n := range core.SortedKeys(s.clusters) {
		cl := s.clusters[n]
		p := strings.Split(n, "/")
		cls.Rows = append(cls.Rows, []string{n, p[1], p[3], cl.p.State.String(), strconv.FormatInt(cl.p.CapacityConfig.GetVcpuCount(), 10), cl.bootstrap})
		for _, id := range core.SortedKeys(cl.topics) {
			t := cl.topics[id]
			tps.Rows = append(tps.Rows, []string{t.Name, n, strconv.Itoa(int(t.PartitionCount)), strconv.Itoa(int(t.ReplicationFactor))})
		}
	}
	return []core.Table{cls, tps}
}

// Inspect returns a cluster with its topics and pogi's side (container,
// bootstrapAddress, note), or a topic.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if table == "Topics" {
		for _, cl := range s.clusters {
			for _, t := range cl.topics {
				if t.Name == id {
					return pj(t)
				}
			}
		}
		return nil
	}
	cl := s.clusters[id]
	if table != "Clusters" || cl == nil {
		return nil
	}
	topics := []json.RawMessage{}
	for _, tid := range core.SortedKeys(cl.topics) {
		topics = append(topics, pj(cl.topics[tid]))
	}
	return map[string]any{"cluster": pj(cl.view()), "topics": topics, "container": cl.container,
		"bootstrapAddress": cl.bootstrap, "googleBootstrapAddress": googleBootstrap(id), "note": cl.note}
}

// googleBootstrap is the address real Managed Kafka gives a cluster: bootstrap.<cluster>.<region>.managedkafka.<project>.cloud.goog:9092.
func googleBootstrap(name string) string {
	p := strings.Split(name, "/")
	return "bootstrap." + p[5] + "." + p[3] + ".managedkafka." + p[1] + ".cloud.goog:9092"
}
