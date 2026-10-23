// Package memorystore emulates Memorystore for Redis (redis.googleapis.com v1)
// over gRPC and its REST mirror. Every instance is a real Redis container,
// started by the runner ElastiCache uses; "host" and "port" on the instance
// work for a Redis client. Without a container engine the API still answers
// from metadata: the instance is READY, with the address 127.0.0.1:6379 and
// nothing behind it (the console's Inspect says so).
package memorystore

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	instanceID = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`)
	parentRE   = regexp.MustCompile(`^projects/[^/]+/locations/([^/]+)$`)
	nameRE     = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/instances/[^/]+$`)
	versionRE  = regexp.MustCompile(`^REDIS_[0-9]+_([0-9]+|X)$`)
)

// maxSizeGb is the largest instance Memorystore documents.
const maxSizeGb = 300

// configs are the redis_configs Memorystore documents; they reach the container.
// Other names are refused: the values go to the Redis command line.
var configs = []string{"activedefrag", "lfu-decay-time", "lfu-log-factor", "maxmemory-policy",
	"notify-keyspace-events", "stream-node-max-bytes", "stream-node-max-entries"}

// Service is the Memorystore for Redis emulator.
type Service struct {
	redispb.UnimplementedCloudRedisServer
	hub  *core.Hub
	rest *gapi.REST
	ops  *gapi.Ops
	box  *rds.Box
	ctx  context.Context // ends at Close; stops operations that wait for a container
	stop context.CancelFunc

	mu        sync.Mutex
	instances map[string]*instance // by full name
}

// instance is an Instance plus pogi's side of it.
type instance struct {
	p        *redispb.Instance
	auth     string // the AUTH string, when auth_enabled
	srv      rds.Server
	deleting bool
}

// New creates the Memorystore service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, ops: gapi.Operations(h), instances: map[string]*instance{}}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.box = rds.NewBox(h, "memorystore", "failed", &s.mu, s.owns)
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"instances"}, Desc: &redispb.CloudRedis_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"GET locations/*/instances":              {Method: "ListInstances", Field: "parent"},
			"POST locations/*/instances":             {Method: "CreateInstance", Field: "parent", Body: "instance"},
			"GET locations/*/instances/*":            {Method: "GetInstance", Field: "name"},
			"PATCH locations/*/instances/*":          {Method: "UpdateInstance", Field: "instance.name", Body: "instance"},
			"DELETE locations/*/instances/*":         {Method: "DeleteInstance", Field: "name"},
			"GET locations/*/instances/*/authString": {Method: "GetInstanceAuthString", Field: "name"},
		}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "redis", Cloud: core.GCP, Title: "Memorystore for Redis", Match: s.rest.Match,
		Targets: []string{"google.cloud.redis.v1.CloudRedis"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { redispb.RegisterCloudRedisServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

func (s *Service) owns(srv *rds.Server) bool { // lock held
	for _, i := range s.instances {
		if &i.srv == srv {
			return true
		}
	}
	return false
}

// spec is the container: Redis with the size, the configs and the AUTH string
// of the instance on its command line.
func (i *instance) spec() rds.Spec {
	sp := elasticache.Spec("redis", i.srv.HostPort, false)
	sp.Args = []string{"--maxmemory", fmt.Sprintf("%dgb", i.p.MemorySizeGb)}
	for _, k := range core.SortedKeys(i.p.RedisConfigs) {
		sp.Args = append(sp.Args, "--"+k, i.p.RedisConfigs[k])
	}
	if i.auth != "" {
		sp.Args = append(sp.Args, "--requirepass", i.auth)
	}
	return sp
}

// cli is a redis-cli command for the instance's container.
func (i *instance) cli(args ...string) []string {
	cmd := []string{"redis-cli"}
	if i.auth != "" {
		cmd = append(cmd, "-a", i.auth)
	}
	return append(cmd, args...)
}

// view is the instance as clients see it.
func (i *instance) view() *redispb.Instance {
	p := proto.Clone(i.p).(*redispb.Instance)
	switch {
	case i.deleting:
		p.State = redispb.Instance_DELETING
	case i.srv.State == "creating":
		p.State = redispb.Instance_CREATING
	case i.srv.State == "available":
		p.State = redispb.Instance_READY
	default:
		p.State, p.StatusMessage = redispb.Instance_STATE_UNSPECIFIED, i.srv.Note
	}
	if a := i.srv.Addr(); a != nil {
		p.Host, p.Port = "127.0.0.1", int32(a.Port)
	}
	return p
}

func meta(target, verb string) *redispb.OperationMetadata {
	return &redispb.OperationMetadata{CreateTime: timestamppb.Now(), Target: target, Verb: verb, ApiVersion: "v1"}
}

// parentOf is projects/p/locations/l of an instance name.
func parentOf(name string) string { return name[:strings.Index(name, "/instances/")] }

func checkSize(gb int32) error {
	if gb < 1 || gb > maxSizeGb {
		return gapi.Invalid("memory_size_gb must be between 1 and %d", maxSizeGb)
	}
	return nil
}

func checkConfigs(m map[string]string) error {
	for k, v := range m {
		switch {
		case !slices.Contains(configs, k):
			return gapi.Invalid("redis_configs: %q is not supported (supported: %s)", k, strings.Join(configs, ", "))
		case strings.HasPrefix(v, "-"):
			return gapi.Invalid("redis_configs: invalid value %q for %s", v, k)
		}
	}
	return nil
}

func (s *Service) CreateInstance(_ context.Context, r *redispb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	m := parentRE.FindStringSubmatch(r.Parent)
	switch {
	case m == nil:
		return nil, gapi.Invalid("parent must look like projects/<project>/locations/<location>")
	case !instanceID.MatchString(r.InstanceId):
		return nil, gapi.Invalid("instance_id must be 1-40 lowercase letters, digits or hyphens, starting with a letter and ending with a letter or digit")
	case r.Instance == nil:
		return nil, gapi.Invalid("instance is required")
	}
	in := proto.Clone(r.Instance).(*redispb.Instance)
	if err := checkSize(in.MemorySizeGb); err != nil {
		return nil, err
	}
	if in.RedisVersion != "" && !versionRE.MatchString(in.RedisVersion) {
		return nil, gapi.Invalid("redis_version %q is not supported (for example REDIS_7_0)", in.RedisVersion)
	}
	if in.LocationId != "" && !strings.HasPrefix(in.LocationId, m[1]) {
		return nil, gapi.Invalid("location_id %q is not in location %s", in.LocationId, m[1])
	}
	if err := checkConfigs(in.RedisConfigs); err != nil {
		return nil, err
	}
	name := r.Parent + "/instances/" + r.InstanceId
	in.Name, in.CreateTime = name, timestamppb.Now()
	if in.Tier == redispb.Instance_TIER_UNSPECIFIED {
		in.Tier = redispb.Instance_BASIC
	}
	if in.RedisVersion == "" {
		in.RedisVersion = "REDIS_7_0"
	}
	if in.LocationId == "" {
		in.LocationId = m[1] + "-a"
	}
	in.CurrentLocationId = in.LocationId
	if in.ConnectMode == redispb.Instance_CONNECT_MODE_UNSPECIFIED {
		in.ConnectMode = redispb.Instance_DIRECT_PEERING
	}
	if in.AuthorizedNetwork == "" {
		in.AuthorizedNetwork = "projects/" + strings.Split(r.Parent, "/")[1] + "/global/networks/default"
	}
	if in.TransitEncryptionMode == redispb.Instance_TRANSIT_ENCRYPTION_MODE_UNSPECIFIED {
		in.TransitEncryptionMode = redispb.Instance_DISABLED
	}
	if in.ReadReplicasMode == redispb.Instance_READ_REPLICAS_MODE_UNSPECIFIED {
		in.ReadReplicasMode = redispb.Instance_READ_REPLICAS_DISABLED
	}
	i := &instance{p: in}
	if in.AuthEnabled {
		i.auth = rand.Text()
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.instances[name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists.", name)
	}
	s.instances[name] = i
	s.box.Serve(&i.srv, strings.Split(r.Parent, "/")[1], s.hub.Cfg.Region, m[1]+"-"+r.InstanceId, 0, 6379, i.spec, docker) // the project qualifies the container name
	return s.ops.Start(r.Parent, meta(name, "create"), func() (proto.Message, error) { return s.ready(i) }), nil
}

// ready waits until the container of a new instance answers. An instance
// whose container fails is removed and the operation fails.
func (s *Service) ready(i *instance) (proto.Message, error) {
	for {
		s.mu.Lock()
		state, note, gone := i.srv.State, i.srv.Note, s.instances[i.p.Name] != i
		if state == "failed" && !gone {
			delete(s.instances, i.p.Name)
			s.box.Remove(i.srv.Container)
		}
		v := i.view()
		s.mu.Unlock()
		switch {
		case gone:
			return nil, status.Error(codes.Aborted, "the instance was deleted while it was being created")
		case state == "failed":
			return nil, status.Errorf(codes.Internal, "the Redis container did not start: %s", note)
		case state != "creating":
			return v, nil
		}
		select {
		case <-s.ctx.Done():
			return nil, status.Error(codes.Aborted, "pogi is shutting down")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// find returns the instance of a name. Lock held.
func (s *Service) find(name string) (*instance, error) {
	if !nameRE.MatchString(name) {
		return nil, gapi.Invalid("name must look like projects/<project>/locations/<location>/instances/<instance>")
	}
	if i := s.instances[name]; i != nil {
		return i, nil
	}
	return nil, gapi.NotFound("Resource '%s' not found.", name)
}

func (s *Service) GetInstance(_ context.Context, r *redispb.GetInstanceRequest) (*redispb.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	return i.view(), nil
}

func (s *Service) GetInstanceAuthString(_ context.Context, r *redispb.GetInstanceAuthStringRequest) (*redispb.InstanceAuthString, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	if !i.p.AuthEnabled {
		return nil, status.Errorf(codes.FailedPrecondition, "instance %s does not have AUTH enabled", r.Name)
	}
	return &redispb.InstanceAuthString{AuthString: i.auth}, nil
}

func (s *Service) ListInstances(_ context.Context, r *redispb.ListInstancesRequest) (*redispb.ListInstancesResponse, error) {
	m := parentRE.FindStringSubmatch(r.Parent)
	if m == nil {
		return nil, gapi.Invalid("parent must look like projects/<project>/locations/<location>")
	}
	if r.PageSize < 0 {
		return nil, gapi.Invalid("page_size must not be negative")
	}
	prefix := r.Parent + "/instances/"
	if m[1] == "-" { // every location of the project
		prefix = strings.TrimSuffix(r.Parent, "-")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*redispb.Instance
	for _, n := range core.SortedKeys(s.instances) {
		if strings.HasPrefix(n, prefix) {
			all = append(all, s.instances[n].view())
		}
	}
	items, next := gapi.Page(all, r.PageSize, r.PageToken)
	return &redispb.ListInstancesResponse{Instances: items, NextPageToken: next}, nil
}

func (s *Service) UpdateInstance(_ context.Context, r *redispb.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
	if r.Instance == nil {
		return nil, gapi.Invalid("instance is required")
	}
	paths := r.UpdateMask.GetPaths()
	if len(paths) == 0 {
		return nil, gapi.Invalid("update_mask is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.find(r.Instance.Name)
	if err != nil {
		return nil, err
	}
	if i.deleting || i.srv.State == "creating" {
		return nil, status.Errorf(codes.FailedPrecondition, "instance %s is %s", i.p.Name, i.view().State)
	}
	p := proto.Clone(i.p).(*redispb.Instance)
	if err := gapi.Update(p, r.Instance, paths, "display_name", "labels", "memory_size_gb", "redis_configs"); err != nil {
		return nil, err
	}
	if err := checkSize(p.MemorySizeGb); err != nil {
		return nil, err
	}
	if err := checkConfigs(p.RedisConfigs); err != nil {
		return nil, err
	}
	// Running containers get the changes with CONFIG SET.
	if i.srv.Container != "" && i.srv.State == "available" {
		if p.MemorySizeGb != i.p.MemorySizeGb {
			s.box.Exec(&i.srv, i.cli("config", "set", "maxmemory", fmt.Sprintf("%dgb", p.MemorySizeGb))...)
		}
		for _, k := range core.SortedKeys(p.RedisConfigs) { // ponytail: a key dropped from the map keeps its value until the container is made again
			if p.RedisConfigs[k] != i.p.RedisConfigs[k] {
				s.box.Exec(&i.srv, i.cli("config", "set", k, p.RedisConfigs[k])...)
			}
		}
	}
	i.p = p
	s.hub.MarkDirty()
	return s.ops.Done(parentOf(p.Name), meta(p.Name, "update"), i.view()), nil
}

func (s *Service) DeleteInstance(_ context.Context, r *redispb.DeleteInstanceRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.find(r.Name)
	if err != nil {
		return nil, err
	}
	if i.deleting {
		return nil, status.Errorf(codes.FailedPrecondition, "instance %s is already being deleted", r.Name)
	}
	i.deleting = true
	return s.ops.Start(parentOf(r.Name), meta(r.Name, "delete"), func() (proto.Message, error) {
		s.mu.Lock()
		delete(s.instances, r.Name)
		if i.srv.Container != "" {
			s.box.Remove(i.srv.Container)
		}
		s.mu.Unlock()
		s.hub.MarkDirty()
		return nil, nil
	}), nil
}

// ---- persistence, console, shutdown ----

type saved struct {
	Instance json.RawMessage `json:"instance"`
	Auth     string          `json:"auth,omitempty"`
	Pogi     rds.Server      `json:"pogi"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []saved{}
	for _, n := range core.SortedKeys(s.instances) {
		i := s.instances[n]
		b, _ := protojson.Marshal(i.p)
		out = append(out, saved{b, i.auth, i.srv})
	}
	return json.Marshal(out)
}

func (s *Service) containers() map[string]bool { // lock held
	out := map[string]bool{}
	for _, i := range s.instances {
		if i.srv.Container != "" {
			out[i.srv.Container] = true
		}
	}
	return out
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped instances go.
func (s *Service) Load(b []byte) error {
	var in []saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	next := map[string]*instance{}
	for _, sv := range in {
		i := &instance{p: &redispb.Instance{}, auth: sv.Auth, srv: sv.Pogi}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(sv.Instance, i.p); err != nil {
			return err
		}
		next[i.p.Name] = i
	}
	docker := len(next) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.containers()
	s.instances = next
	for _, i := range next {
		s.box.Resume(&i.srv, i.spec(), docker)
	}
	keep := s.containers()
	s.mu.Unlock()
	for n := range old {
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("[]")) }

// Close removes the containers, except those persisted state resumes next run.
func (s *Service) Close() error {
	s.stop()
	s.mu.Lock()
	keep := s.containers()
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

// Resources lists the instances.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Instances", Columns: []string{"Name", "Project", "Location", "Tier", "Memory (GB)", "State", "Address"}}
	for _, n := range core.SortedKeys(s.instances) {
		v := s.instances[n].view()
		p := strings.Split(n, "/")
		addr := ""
		if v.Host != "" {
			addr = v.Host + ":" + strconv.Itoa(int(v.Port))
		}
		t.Rows = append(t.Rows, []string{n, p[1], p[3], v.Tier.String(), strconv.Itoa(int(v.MemorySizeGb)), rds.StatusCell(v.State.String(), &s.instances[n].srv), addr})
	}
	return []core.Table{t}
}

// Inspect returns an instance with pogi's side: container, state and note.
func (s *Service) Inspect(_, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.instances[id]
	if i == nil {
		return nil
	}
	b, _ := protojson.Marshal(i.view())
	var out map[string]any
	json.Unmarshal(b, &out)
	out["pogi"] = i.srv
	return out
}
