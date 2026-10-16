// Package elasticache emulates Amazon ElastiCache over the Query protocol and
// runs the real thing: Redis, Valkey and Memcached cache clusters,
// replication groups and serverless caches start as Docker containers on
// localhost. Without Docker the API still answers and Inspect says so.
package elasticache

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the ElastiCache emulator.
type Service struct {
	hub *core.Hub
	h   http.Handler
	box *rds.Box
	mu  sync.Mutex
	st  state

	applied map[string]string // container → the users script it last got (users.go)
	proxies map[string]*proxy // container → its IAM proxy (proxy.go)
}

type state struct {
	Clusters   map[string]*CacheCluster        `json:"cacheClusters"` // account/region/id
	Groups     map[string]*ReplicationGroup    `json:"replicationGroups"`
	Serverless map[string]*ServerlessCache     `json:"serverlessCaches"`
	Subnets    map[string]*CacheSubnetGroup    `json:"subnetGroups"`
	Params     map[string]*CacheParameterGroup `json:"parameterGroups"`
	Users      map[string]*User                `json:"users"`
	UserGroups map[string]*UserGroup           `json:"userGroups"`
}

func newState() state {
	return state{Clusters: map[string]*CacheCluster{}, Groups: map[string]*ReplicationGroup{}, Serverless: map[string]*ServerlessCache{},
		Subnets: map[string]*CacheSubnetGroup{}, Params: map[string]*CacheParameterGroup{}, Users: map[string]*User{}, UserGroups: map[string]*UserGroup{}}
}

// servers calls f with every server of the state and its container spec.
func (st *state) servers(f func(*rds.Server, func() rds.Spec)) {
	for _, c := range st.Clusters {
		f(&c.Pogi, c.spec)
	}
	for _, g := range st.Groups {
		f(&g.Pogi, g.spec)
	}
	for _, sc := range st.Serverless {
		f(&sc.Pogi, sc.spec)
	}
}

func (st *state) containers() map[string]bool {
	out := map[string]bool{}
	st.servers(func(srv *rds.Server, _ func() rds.Spec) {
		if srv.Container != "" {
			out[srv.Container] = true
		}
	})
	return out
}

// engines pogi runs; versions are newest first and the first is the default.
var engines = map[string]struct {
	image, hello, desc string
	port               int
	versions           []string
}{
	"redis":     {"redis:7-alpine", "PING\r\n", "Redis", 6379, []string{"7.1.0", "7.0.7", "6.2.6"}},
	"valkey":    {"valkey/valkey:8-alpine", "PING\r\n", "Valkey", 6379, []string{"8.1.0", "8.0.1", "7.2.6"}},
	"memcached": {"memcached:1-alpine", "version\r\n", "memcached", 11211, []string{"1.6.22", "1.6.17"}},
}

// items names the list items of ElastiCache responses (see rds.Lists).
var items = map[string]string{
	"CacheClusters": "CacheCluster", "CacheNodes": "CacheNode", "ReplicationGroups": "ReplicationGroup", "MemberClusters": "ClusterId",
	"NodeGroups": "NodeGroup", "NodeGroupMembers": "NodeGroupMember", "CacheSubnetGroups": "CacheSubnetGroup", "Subnets": "Subnet",
	"CacheParameterGroups": "CacheParameterGroup", "Parameters": "Parameter", "CacheEngineVersions": "CacheEngineVersion",
	"TagList": "Tag", "SecurityGroupIds": "SecurityGroupId", "SubnetIds": "SubnetId",
}

// New creates the ElastiCache service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState(), applied: map[string]string{}, proxies: map[string]*proxy{}}
	s.box = rds.NewBox(hub, "elasticache", "create-failed", &s.mu, s.owns)
	s.h = rds.Lists(&core.RPC{XMLNS: "http://elasticache.amazonaws.com/doc/2015-02-02/", Ops: map[string]core.Handler{
		"CreateCacheCluster":           core.H(s.createCacheCluster),
		"DescribeCacheClusters":        core.H(s.describeCacheClusters),
		"ModifyCacheCluster":           core.H(s.modifyCacheCluster),
		"DeleteCacheCluster":           core.H(s.deleteCacheCluster),
		"RebootCacheCluster":           core.H(s.rebootCacheCluster),
		"CreateReplicationGroup":       core.H(s.createReplicationGroup),
		"DescribeReplicationGroups":    core.H(s.describeReplicationGroups),
		"ModifyReplicationGroup":       core.H(s.modifyReplicationGroup),
		"DeleteReplicationGroup":       core.H(s.deleteReplicationGroup),
		"CreateServerlessCache":        core.H(s.createServerlessCache),
		"DescribeServerlessCaches":     core.H(s.describeServerlessCaches),
		"DeleteServerlessCache":        core.H(s.deleteServerlessCache),
		"CreateCacheSubnetGroup":       core.H(s.createCacheSubnetGroup),
		"DescribeCacheSubnetGroups":    core.H(s.describeCacheSubnetGroups),
		"ModifyCacheSubnetGroup":       core.H(s.modifyCacheSubnetGroup),
		"DeleteCacheSubnetGroup":       core.H(s.deleteCacheSubnetGroup),
		"CreateCacheParameterGroup":    core.H(s.createCacheParameterGroup),
		"DescribeCacheParameterGroups": core.H(s.describeCacheParameterGroups),
		"ModifyCacheParameterGroup":    core.H(s.modifyCacheParameterGroup),
		"ResetCacheParameterGroup":     core.H(s.modifyCacheParameterGroup),
		"DeleteCacheParameterGroup":    core.H(s.deleteCacheParameterGroup),
		"DescribeCacheParameters":      core.H(s.describeCacheParameters),
		"DescribeCacheEngineVersions":  core.H(s.describeCacheEngineVersions),
		"AddTagsToResource":            core.H(s.addTags),
		"RemoveTagsFromResource":       core.H(s.removeTags),
		"ListTagsForResource":          core.H(s.listTags),
		"CreateUser":                   core.H(s.createUser),
		"DescribeUsers":                core.H(s.describeUsers),
		"ModifyUser":                   core.H(s.modifyUser),
		"DeleteUser":                   core.H(s.deleteUser),
		"CreateUserGroup":              core.H(s.createUserGroup),
		"DescribeUserGroups":           core.H(s.describeUserGroups),
		"ModifyUserGroup":              core.H(s.modifyUserGroup),
		"DeleteUserGroup":              core.H(s.deleteUserGroup),
	}}, items)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "elasticache", Cloud: core.AWS, Title: "ElastiCache", Signing: []string{"elasticache"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.h.ServeHTTP(w, r) }

// ---- API shapes (stored as is; Pogi and query:"-" fields are pogi's) ----

// CacheCluster is a cache cluster: standalone with its own container, or a
// member of a replication group sharing the group's.
type CacheCluster struct {
	CacheClusterId             string
	ConfigurationEndpoint      *rds.Endpoint `json:",omitempty"`
	CacheNodeType              string
	Engine                     string
	EngineVersion              string
	CacheClusterStatus         string
	NumCacheNodes              int
	PreferredAvailabilityZone  string
	CacheClusterCreateTime     time.Time
	PreferredMaintenanceWindow string
	PendingModifiedValues      struct{}
	CacheParameterGroup        ParameterGroupStatus
	CacheSubnetGroupName       string      `json:",omitempty"`
	CacheNodes                 []CacheNode `json:",omitempty"`
	AutoMinorVersionUpgrade    bool
	SecurityGroups             []SecurityGroupMembership `json:",omitempty"`
	ReplicationGroupId         string                    `json:",omitempty"`
	SnapshotRetentionLimit     int
	SnapshotWindow             string
	AuthTokenEnabled           bool
	TransitEncryptionEnabled   bool
	AtRestEncryptionEnabled    bool
	ARN                        string
	NetworkType                string
	IpDiscovery                string
	Tags                       []rds.Tag  `json:",omitempty" query:"-"`
	Pogi                       rds.Server `json:"pogi" query:"-"`
}

// ParameterGroupStatus is the parameter group of a cache cluster.
type ParameterGroupStatus struct{ CacheParameterGroupName, ParameterApplyStatus string }

// CacheNode is a node of a cache cluster (all nodes share the one container).
type CacheNode struct {
	CacheNodeId              string
	CacheNodeStatus          string
	CacheNodeCreateTime      time.Time
	Endpoint                 *rds.Endpoint `json:",omitempty"`
	ParameterGroupStatus     string
	CustomerAvailabilityZone string
}

// SecurityGroupMembership is a VPC security group of a cache cluster.
type SecurityGroupMembership struct{ SecurityGroupId, Status string }

// ReplicationGroup is a Redis or Valkey replication group; one container
// serves all of its member clusters (and, in cluster mode, all of its shards).
type ReplicationGroup struct {
	ReplicationGroupId         string
	Description                string
	Status                     string
	PendingModifiedValues      struct{}
	MemberClusters             []string
	NodeGroups                 []NodeGroup `json:",omitempty"`
	AutomaticFailover          string
	MultiAZ                    string
	ConfigurationEndpoint      *rds.Endpoint `json:",omitempty"`
	SnapshotRetentionLimit     int
	SnapshotWindow             string
	ClusterEnabled             bool
	CacheNodeType              string
	AuthTokenEnabled           bool
	TransitEncryptionEnabled   bool
	AtRestEncryptionEnabled    bool
	KmsKeyId                   string `json:",omitempty"`
	ARN                        string
	ReplicationGroupCreateTime time.Time
	DataTiering                string
	AutoMinorVersionUpgrade    bool
	NetworkType                string
	IpDiscovery                string
	ClusterMode                string
	Engine                     string
	UserGroupIds               []string
	Shards                     int        `query:"-"`
	Tags                       []rds.Tag  `json:",omitempty" query:"-"`
	Pogi                       rds.Server `json:"pogi" query:"-"`
}

// NodeGroup is a shard of a replication group.
type NodeGroup struct {
	NodeGroupId      string
	Status           string
	PrimaryEndpoint  *rds.Endpoint `json:",omitempty"`
	ReaderEndpoint   *rds.Endpoint `json:",omitempty"`
	Slots            string        `json:",omitempty"`
	NodeGroupMembers []NodeGroupMember
}

// NodeGroupMember is a member cluster of a shard.
type NodeGroupMember struct {
	CacheClusterId            string
	CacheNodeId               string
	ReadEndpoint              *rds.Endpoint `json:",omitempty"`
	PreferredAvailabilityZone string
	CurrentRole               string `json:",omitempty"`
}

// ServerlessCache is a serverless cache.
type ServerlessCache struct {
	ServerlessCacheName    string
	Description            string
	CreateTime             time.Time
	Status                 string
	Engine                 string
	MajorEngineVersion     string
	FullEngineVersion      string
	CacheUsageLimits       *CacheUsageLimits `json:",omitempty"`
	KmsKeyId               string            `json:",omitempty"`
	SecurityGroupIds       []string
	Endpoint               *rds.Endpoint `json:",omitempty"`
	ReaderEndpoint         *rds.Endpoint `json:",omitempty"`
	ARN                    string
	UserGroupId            string `json:",omitempty"`
	SubnetIds              []string
	SnapshotRetentionLimit int
	DailySnapshotTime      string
	Tags                   []rds.Tag  `json:",omitempty" query:"-"`
	Pogi                   rds.Server `json:"pogi" query:"-"`
}

// CacheUsageLimits caps a serverless cache (recorded, not enforced).
type CacheUsageLimits struct {
	DataStorage   *Limit `json:",omitempty"`
	ECPUPerSecond *Limit `json:",omitempty"`
}

// Limit is a usage range.
type Limit struct {
	Maximum, Minimum int
	Unit             string `json:",omitempty"`
}

// CacheSubnetGroup is a cache subnet group.
type CacheSubnetGroup struct {
	CacheSubnetGroupName        string
	CacheSubnetGroupDescription string
	VpcId                       string
	Subnets                     []rds.Subnet
	ARN                         string
	SupportedNetworkTypes       []string
	Tags                        []rds.Tag `json:",omitempty" query:"-"`
}

// CacheParameterGroup is a cache parameter group.
type CacheParameterGroup struct {
	CacheParameterGroupName   string
	CacheParameterGroupFamily string
	Description               string
	IsGlobal                  bool
	ARN                       string
	Params                    map[string]string `json:",omitempty" query:"-"`
	Tags                      []rds.Tag         `json:",omitempty" query:"-"`
}

// Parameter is a parameter of a cache parameter group.
type Parameter struct {
	ParameterName, ParameterValue, Description, Source, DataType string
	IsModifiable                                                 bool
	ChangeType                                                   string
}

// CacheEngineVersion is an engine version pogi offers.
type CacheEngineVersion struct {
	Engine, EngineVersion, CacheParameterGroupFamily, CacheEngineDescription, CacheEngineVersionDescription string
}

// ---- helpers ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValue", format, a...)
}

func combo(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterCombination", format, a...)
}

func key(c *core.Ctx, id string) string { return c.Account + "/" + c.Region + "/" + id }
func mine(c *core.Ctx, k string) bool   { return strings.HasPrefix(k, c.Account+"/"+c.Region+"/") }

func ctxOf(k string) *core.Ctx {
	p := strings.SplitN(k, "/", 3)
	return &core.Ctx{Account: p[0], Region: p[1]}
}

func set[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

func onOff(b *bool, def bool) string {
	if b != nil {
		def = *b
	}
	if def {
		return "enabled"
	}
	return "disabled"
}

func sgs(ids []string) []SecurityGroupMembership {
	out := make([]SecurityGroupMembership, len(ids))
	for i, id := range ids {
		out[i] = SecurityGroupMembership{id, "active"}
	}
	return out
}

// version resolves an empty or partial version ("7") to a full one.
func version(engine, v string) string {
	for _, full := range engines[engine].versions {
		if v == "" || full == v || strings.HasPrefix(full, v+".") {
			return full
		}
	}
	return v
}

// family is the parameter group family of an engine version: redis7, redis6.x, valkey8, memcached1.6.
func family(engine, v string) string {
	p := strings.SplitN(v, ".", 3)
	switch {
	case engine == "memcached" && len(p) > 1:
		return engine + p[0] + "." + p[1]
	case engine == "redis" && p[0] == "6":
		return "redis6.x"
	}
	return engine + p[0]
}

// Spec is the container of an engine. Cluster mode runs a one-node Redis
// cluster that owns every slot and announces the published port, so cluster
// clients on the host follow it.
func Spec(engine string, host int, cluster bool) rds.Spec {
	e := engines[engine]
	sp := rds.Spec{Image: e.image, Port: e.port, Host: host, Hello: e.hello}
	if cluster {
		sp.Args = []string{"--cluster-enabled", "yes", "--cluster-announce-ip", "127.0.0.1", "--cluster-announce-port", "{port}"}
		sp.Init = []string{engine + "-cli", "cluster", "addslotsrange", "0", "16383"}
	}
	return sp
}

func (cl *CacheCluster) spec() rds.Spec      { return Spec(cl.Engine, cl.Pogi.HostPort, false) }
func (g *ReplicationGroup) spec() rds.Spec   { return Spec(g.Engine, g.Pogi.HostPort, g.ClusterEnabled) }
func (sc *ServerlessCache) spec() rds.Spec   { return Spec(sc.Engine, sc.Pogi.HostPort, false) }
func (s *Service) owns(srv *rds.Server) bool { return s.st.has(srv) }

func (st *state) has(target *rds.Server) (found bool) {
	st.servers(func(srv *rds.Server, _ func() rds.Spec) { found = found || srv == target })
	return found
}

// ---- lookups (lock held) ----

func (s *Service) cluster(c *core.Ctx, id string) (*CacheCluster, error) {
	if cl := s.st.Clusters[key(c, strings.ToLower(id))]; cl != nil {
		return cl, nil
	}
	return nil, core.Errorf(404, "CacheClusterNotFound", "CacheCluster not found: %s", id)
}

func (s *Service) group(c *core.Ctx, id string) (*ReplicationGroup, error) {
	if g := s.st.Groups[key(c, strings.ToLower(id))]; g != nil {
		return g, nil
	}
	return nil, core.Errorf(404, "ReplicationGroupNotFoundFault", "ReplicationGroup not found: %s", id)
}

func (s *Service) serverless(c *core.Ctx, name string) (*ServerlessCache, error) {
	if sc := s.st.Serverless[key(c, strings.ToLower(name))]; sc != nil {
		return sc, nil
	}
	return nil, core.Errorf(404, "ServerlessCacheNotFoundFault", "Serverless cache %s not found.", name)
}

func (s *Service) subnetGroup(c *core.Ctx, name string) (*CacheSubnetGroup, error) {
	if g := s.st.Subnets[key(c, strings.ToLower(name))]; g != nil {
		return g, nil
	}
	return nil, core.Errorf(400, "CacheSubnetGroupNotFoundFault", "Cache subnet group %s not found.", name)
}

// paramGroup finds a parameter group; default.<family> groups always exist.
func (s *Service) paramGroup(c *core.Ctx, name string) (*CacheParameterGroup, error) {
	name = strings.ToLower(name)
	if g := s.st.Params[key(c, name)]; g != nil {
		return g, nil
	}
	if fam, ok := strings.CutPrefix(name, "default."); ok && fam != "" {
		return &CacheParameterGroup{CacheParameterGroupName: name, CacheParameterGroupFamily: strings.TrimSuffix(fam, ".cluster.on"),
			Description: "Default parameter group for " + fam, ARN: c.ARN("elasticache", "parametergroup:"+name)}, nil
	}
	return nil, core.Errorf(404, "CacheParameterGroupNotFound", "CacheParameterGroup %s not found.", name)
}

// checkNew validates a new cache cluster id and the groups it refers to; lock held.
func (s *Service) checkNew(c *core.Ctx, id, subnets, params string) error {
	if s.st.Clusters[key(c, id)] != nil {
		return core.Errorf(400, "CacheClusterAlreadyExists", "Cache cluster %s already exists.", id)
	}
	if subnets != "" {
		if _, err := s.subnetGroup(c, subnets); err != nil {
			return err
		}
	}
	if params != "" {
		if _, err := s.paramGroup(c, params); err != nil {
			return err
		}
	}
	return nil
}

// view is the API shape of a cache cluster; members show their group's server. Lock held.
func (s *Service) view(c *core.Ctx, cl *CacheCluster, nodes bool) *CacheCluster {
	v := rds.Clone(cl)
	srv, _ := s.server(c, cl)
	ep := s.endpoint(srv)
	v.CacheClusterStatus = srv.State
	if cl.Engine == "memcached" {
		v.ConfigurationEndpoint = ep
	}
	for n := 0; nodes && n < cl.NumCacheNodes; n++ {
		v.CacheNodes = append(v.CacheNodes, CacheNode{fmt.Sprintf("%04d", n+1), srv.State, cl.CacheClusterCreateTime, ep, "in-sync", cl.PreferredAvailabilityZone})
	}
	return v
}

// server is the server behind a cache cluster: its own, or its replication group's. Lock held.
func (s *Service) server(c *core.Ctx, cl *CacheCluster) (*rds.Server, func() rds.Spec) {
	if g := s.st.Groups[key(c, cl.ReplicationGroupId)]; g != nil {
		return &g.Pogi, g.spec
	}
	return &cl.Pogi, cl.spec
}

// groupView is the API shape of a replication group; lock held.
func (s *Service) groupView(c *core.Ctx, g *ReplicationGroup) *ReplicationGroup {
	v := rds.Clone(g)
	v.Status = g.Pogi.State
	ep, az := s.endpoint(&g.Pogi), c.Region+"a"
	if !g.ClusterEnabled {
		ng := NodeGroup{NodeGroupId: "0001", Status: g.Pogi.State, PrimaryEndpoint: ep, ReaderEndpoint: ep}
		for n, m := range g.MemberClusters {
			role := "replica"
			if n == 0 {
				role = "primary"
			}
			ng.NodeGroupMembers = append(ng.NodeGroupMembers, NodeGroupMember{m, "0001", ep, az, role})
		}
		v.NodeGroups = []NodeGroup{ng}
		return v
	}
	v.ConfigurationEndpoint = ep
	for sh := 1; sh <= g.Shards; sh++ {
		ng := NodeGroup{NodeGroupId: fmt.Sprintf("%04d", sh), Status: g.Pogi.State, Slots: fmt.Sprintf("%d-%d", (sh-1)*16384/g.Shards, sh*16384/g.Shards-1)}
		for _, m := range g.MemberClusters {
			if strings.HasPrefix(m, fmt.Sprintf("%s-%04d-", g.ReplicationGroupId, sh)) {
				ng.NodeGroupMembers = append(ng.NodeGroupMembers, NodeGroupMember{CacheClusterId: m, CacheNodeId: "0001", PreferredAvailabilityZone: az})
			}
		}
		v.NodeGroups = append(v.NodeGroups, ng)
	}
	return v
}

func (s *Service) serverlessView(sc *ServerlessCache) *ServerlessCache {
	v := rds.Clone(sc)
	v.Status, v.Endpoint, v.ReaderEndpoint = sc.Pogi.State, s.endpoint(&sc.Pogi), s.endpoint(&sc.Pogi)
	return v
}

// ---- cache clusters ----

// cacheInput is the input of the cache cluster and replication group operations.
type cacheInput struct {
	CacheClusterId, ReplicationGroupId, ReplicationGroupDescription, PrimaryClusterId, Engine, EngineVersion string
	CacheNodeType, CacheParameterGroupName, CacheSubnetGroupName, PreferredAvailabilityZone                  string
	PreferredMaintenanceWindow, SnapshotWindow, ClusterMode, KmsKeyId, Marker                                string
	NumCacheNodes, NumCacheClusters, NumNodeGroups, ReplicasPerNodeGroup, Port, MaxRecords                   int
	SnapshotRetentionLimit                                                                                   *int
	AutoMinorVersionUpgrade, AutomaticFailoverEnabled, MultiAZEnabled                                        *bool
	TransitEncryptionEnabled, AtRestEncryptionEnabled, ShowCacheNodeInfo                                     bool
	ShowCacheClustersNotInReplicationGroups, RetainPrimaryCluster                                            bool
	SecurityGroupIds, UserGroupIds, UserGroupIdsToAdd, UserGroupIdsToRemove                                  []string
	RemoveUserGroups                                                                                         bool
	NodeGroupConfiguration                                                                                   []struct{ ReplicaCount int }
	Tags                                                                                                     []rds.Tag
}

// newCluster builds a cache cluster from a create request.
func newCluster(c *core.Ctx, id, engine string, in *cacheInput) *CacheCluster {
	ver := version(engine, in.EngineVersion)
	cl := &CacheCluster{CacheClusterId: id, CacheNodeType: cmp.Or(in.CacheNodeType, "cache.t3.micro"), Engine: engine, EngineVersion: ver,
		NumCacheNodes: cmp.Or(in.NumCacheNodes, 1), PreferredAvailabilityZone: cmp.Or(in.PreferredAvailabilityZone, c.Region+"a"),
		CacheClusterCreateTime: time.Now().UTC(), PreferredMaintenanceWindow: cmp.Or(in.PreferredMaintenanceWindow, "sun:05:00-sun:06:00"),
		CacheParameterGroup:     ParameterGroupStatus{cmp.Or(strings.ToLower(in.CacheParameterGroupName), "default."+family(engine, ver)), "in-sync"},
		CacheSubnetGroupName:    strings.ToLower(in.CacheSubnetGroupName),
		AutoMinorVersionUpgrade: in.AutoMinorVersionUpgrade == nil || *in.AutoMinorVersionUpgrade, SecurityGroups: sgs(in.SecurityGroupIds),
		SnapshotWindow: cmp.Or(in.SnapshotWindow, "03:00-04:00"), TransitEncryptionEnabled: in.TransitEncryptionEnabled,
		AtRestEncryptionEnabled: in.AtRestEncryptionEnabled, ARN: c.ARN("elasticache", "cluster:"+id), NetworkType: "ipv4", IpDiscovery: "ipv4",
		Tags: rds.AddTags(nil, in.Tags)}
	set(&cl.SnapshotRetentionLimit, in.SnapshotRetentionLimit)
	return cl
}

func (s *Service) createCacheCluster(c *core.Ctx, in *cacheInput) (any, error) {
	id, engine := strings.ToLower(in.CacheClusterId), cmp.Or(in.Engine, "redis")
	e, known := engines[engine]
	switch err := rds.ValidID("CacheClusterId", id, 50); {
	case err != nil:
		return nil, err
	case in.ReplicationGroupId != "":
		return s.addReplica(c, id, in)
	case !known:
		return nil, invalid("Invalid cache engine: %s", in.Engine)
	case engine != "memcached" && in.NumCacheNodes > 1:
		return nil, invalid("Cannot create a Redis cluster with a NumCacheNodes parameter greater than 1.")
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNew(c, id, in.CacheSubnetGroupName, in.CacheParameterGroupName); err != nil {
		return nil, err
	}
	cl := newCluster(c, id, engine, in)
	s.st.Clusters[key(c, id)] = cl
	s.box.Serve(&cl.Pogi, c.Account, c.Region, id, in.Port, e.port, cl.spec, docker)
	return map[string]any{"CacheCluster": s.view(c, cl, false)}, nil
}

// addReplica adds a member cluster to a replication group (CreateCacheCluster with ReplicationGroupId).
func (s *Service) addReplica(c *core.Ctx, id string, in *cacheInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.ReplicationGroupId)
	if err != nil {
		return nil, err
	}
	if g.ClusterEnabled {
		return nil, combo("Cache clusters cannot be added to the cluster mode enabled replication group %s.", g.ReplicationGroupId)
	}
	if err := s.checkNew(c, id, "", ""); err != nil {
		return nil, err
	}
	first := s.st.Clusters[key(c, g.MemberClusters[0])]
	cl := rds.Clone(first)
	cl.CacheClusterId, cl.ARN, cl.CacheClusterCreateTime, cl.Tags = id, c.ARN("elasticache", "cluster:"+id), time.Now().UTC(), rds.AddTags(nil, in.Tags)
	g.MemberClusters = append(g.MemberClusters, id)
	s.st.Clusters[key(c, id)] = cl
	return map[string]any{"CacheCluster": s.view(c, cl, false)}, nil
}

func (s *Service) describeCacheClusters(c *core.Ctx, in *cacheInput) (any, error) {
	id := strings.ToLower(in.CacheClusterId)
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != "" {
		if _, err := s.cluster(c, id); err != nil {
			return nil, err
		}
	}
	var list []*CacheCluster
	for k, cl := range s.st.Clusters {
		if mine(c, k) && (id == "" || cl.CacheClusterId == id) && !(in.ShowCacheClustersNotInReplicationGroups && cl.ReplicationGroupId != "") {
			list = append(list, cl)
		}
	}
	list, marker := rds.Page(list, func(cl *CacheCluster) string { return cl.CacheClusterId }, in.Marker, in.MaxRecords)
	out := make([]*CacheCluster, len(list))
	for n, cl := range list {
		out[n] = s.view(c, cl, in.ShowCacheNodeInfo)
	}
	return map[string]any{"CacheClusters": out, "Marker": marker}, nil
}

// apply sets the modifiable settings of a cache cluster; lock held.
func apply(cl *CacheCluster, in *cacheInput) {
	cl.CacheNodeType = cmp.Or(in.CacheNodeType, cl.CacheNodeType)
	if in.EngineVersion != "" { // the image stays: one per engine
		cl.EngineVersion = version(cl.Engine, in.EngineVersion)
	}
	if in.CacheParameterGroupName != "" {
		cl.CacheParameterGroup.CacheParameterGroupName = strings.ToLower(in.CacheParameterGroupName)
	}
	if in.SecurityGroupIds != nil {
		cl.SecurityGroups = sgs(in.SecurityGroupIds)
	}
	cl.PreferredMaintenanceWindow = cmp.Or(in.PreferredMaintenanceWindow, cl.PreferredMaintenanceWindow)
	cl.SnapshotWindow = cmp.Or(in.SnapshotWindow, cl.SnapshotWindow)
	set(&cl.SnapshotRetentionLimit, in.SnapshotRetentionLimit)
	set(&cl.AutoMinorVersionUpgrade, in.AutoMinorVersionUpgrade)
}

func (s *Service) modifyCacheCluster(c *core.Ctx, in *cacheInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.CacheClusterId)
	if err == nil && in.CacheParameterGroupName != "" {
		_, err = s.paramGroup(c, in.CacheParameterGroupName)
	}
	switch {
	case err != nil:
		return nil, err
	case in.NumCacheNodes > 1 && cl.Engine != "memcached":
		return nil, invalid("Cannot create a Redis cluster with a NumCacheNodes parameter greater than 1.")
	}
	cl.NumCacheNodes = cmp.Or(in.NumCacheNodes, cl.NumCacheNodes)
	apply(cl, in)
	return map[string]any{"CacheCluster": s.view(c, cl, false)}, nil
}

func (s *Service) deleteCacheCluster(c *core.Ctx, in *cacheInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.CacheClusterId)
	if err != nil {
		return nil, err
	}
	if g := s.st.Groups[key(c, cl.ReplicationGroupId)]; g != nil {
		if len(g.MemberClusters) == 1 {
			return nil, core.Errorf(400, "InvalidCacheClusterState",
				"Cache cluster %s is the last member of replication group %s; delete the replication group instead.", cl.CacheClusterId, g.ReplicationGroupId)
		}
		g.MemberClusters = slices.DeleteFunc(g.MemberClusters, func(m string) bool { return m == cl.CacheClusterId })
	}
	v := s.view(c, cl, false)
	v.CacheClusterStatus = "deleting"
	delete(s.st.Clusters, key(c, cl.CacheClusterId))
	if cl.Pogi.Container != "" {
		s.box.Remove(cl.Pogi.Container)
	}
	return map[string]any{"CacheCluster": v}, nil
}

func (s *Service) rebootCacheCluster(c *core.Ctx, in *cacheInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.CacheClusterId)
	if err != nil {
		return nil, err
	}
	srv, sp := s.server(c, cl)
	if srv.State != "available" {
		return nil, core.Errorf(400, "InvalidCacheClusterState", "Cache cluster %s is not in available state.", cl.CacheClusterId)
	}
	if srv.Container != "" {
		srv.State = "rebooting cache cluster nodes"
		s.box.Up(srv, sp(), "restart")
		delete(s.applied, srv.Container) // its users went with it
		s.sync()
	}
	return map[string]any{"CacheCluster": s.view(c, cl, false)}, nil
}

// ---- replication groups ----

func (s *Service) createReplicationGroup(c *core.Ctx, in *cacheInput) (any, error) {
	id, engine := strings.ToLower(in.ReplicationGroupId), cmp.Or(in.Engine, "redis")
	clustered := in.ClusterMode == "enabled" || in.NumNodeGroups > 1 || len(in.NodeGroupConfiguration) > 1 ||
		strings.Contains(in.CacheParameterGroupName, "cluster.on")
	replicas := in.ReplicasPerNodeGroup
	if len(in.NodeGroupConfiguration) > 0 {
		replicas = cmp.Or(replicas, in.NodeGroupConfiguration[0].ReplicaCount)
	}
	shards, count := cmp.Or(in.NumNodeGroups, len(in.NodeGroupConfiguration), 1), cmp.Or(in.NumCacheClusters, replicas+1)
	switch err := rds.ValidID("ReplicationGroupId", id, 40); {
	case err != nil:
		return nil, err
	case in.ReplicationGroupDescription == "":
		return nil, invalid("The parameter ReplicationGroupDescription must be provided and must not be blank.")
	case in.PrimaryClusterId != "":
		return nil, invalid("pogi does not create replication groups from existing clusters (PrimaryClusterId).")
	case engine != "redis" && engine != "valkey":
		return nil, invalid("Replication groups support the redis and valkey engines, not %s.", engine)
	case !clustered && in.AutomaticFailoverEnabled != nil && *in.AutomaticFailoverEnabled && count < 2:
		return nil, combo("Automatic failover needs at least 2 cache clusters (NumCacheClusters).")
	}
	var ids []string
	for sh := 1; sh <= shards && clustered; sh++ {
		for r := 1; r <= replicas+1; r++ {
			ids = append(ids, fmt.Sprintf("%s-%04d-%03d", id, sh, r))
		}
	}
	for r := 1; r <= count && !clustered; r++ {
		ids = append(ids, fmt.Sprintf("%s-%03d", id, r))
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Groups[key(c, id)] != nil {
		return nil, core.Errorf(400, "ReplicationGroupAlreadyExists", "Replication group %s already exists.", id)
	}
	for _, m := range ids {
		if err := s.checkNew(c, m, in.CacheSubnetGroupName, in.CacheParameterGroupName); err != nil {
			return nil, err
		}
	}
	if err := s.checkGroups(c, in.UserGroupIds); err != nil {
		return nil, err
	}
	if err := s.checkIAM(c, in.UserGroupIds, engine, version(engine, in.EngineVersion)); err != nil {
		return nil, err
	}
	tmpl := newCluster(c, "", engine, &cacheInput{EngineVersion: in.EngineVersion, CacheNodeType: in.CacheNodeType,
		CacheParameterGroupName: in.CacheParameterGroupName, CacheSubnetGroupName: in.CacheSubnetGroupName,
		PreferredMaintenanceWindow: in.PreferredMaintenanceWindow, SnapshotWindow: in.SnapshotWindow, SnapshotRetentionLimit: in.SnapshotRetentionLimit,
		AutoMinorVersionUpgrade: in.AutoMinorVersionUpgrade, TransitEncryptionEnabled: in.TransitEncryptionEnabled,
		AtRestEncryptionEnabled: in.AtRestEncryptionEnabled, SecurityGroupIds: in.SecurityGroupIds})
	for _, m := range ids {
		cl := rds.Clone(tmpl)
		cl.CacheClusterId, cl.ARN, cl.ReplicationGroupId = m, c.ARN("elasticache", "cluster:"+m), id
		s.st.Clusters[key(c, m)] = cl
	}
	g := &ReplicationGroup{ReplicationGroupId: id, Description: in.ReplicationGroupDescription, MemberClusters: ids,
		AutomaticFailover: onOff(in.AutomaticFailoverEnabled, clustered), MultiAZ: onOff(in.MultiAZEnabled, false),
		SnapshotRetentionLimit: tmpl.SnapshotRetentionLimit, SnapshotWindow: tmpl.SnapshotWindow, ClusterEnabled: clustered,
		CacheNodeType: tmpl.CacheNodeType, TransitEncryptionEnabled: in.TransitEncryptionEnabled, AtRestEncryptionEnabled: in.AtRestEncryptionEnabled,
		KmsKeyId: in.KmsKeyId, ARN: c.ARN("elasticache", "replicationgroup:"+id), ReplicationGroupCreateTime: time.Now().UTC(),
		DataTiering: "disabled", AutoMinorVersionUpgrade: tmpl.AutoMinorVersionUpgrade, NetworkType: "ipv4", IpDiscovery: "ipv4",
		ClusterMode: onOff(&clustered, false), Engine: engine, UserGroupIds: lower(in.UserGroupIds), Shards: shards, Tags: rds.AddTags(nil, in.Tags)}
	s.st.Groups[key(c, id)] = g
	s.box.Serve(&g.Pogi, c.Account, c.Region, id, in.Port, engines[engine].port, g.spec, docker)
	s.sync()
	return map[string]any{"ReplicationGroup": s.groupView(c, g)}, nil
}

func (s *Service) describeReplicationGroups(c *core.Ctx, in *cacheInput) (any, error) {
	id := strings.ToLower(in.ReplicationGroupId)
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != "" {
		if _, err := s.group(c, id); err != nil {
			return nil, err
		}
	}
	var list []*ReplicationGroup
	for k, g := range s.st.Groups {
		if mine(c, k) && (id == "" || g.ReplicationGroupId == id) {
			list = append(list, g)
		}
	}
	list, marker := rds.Page(list, func(g *ReplicationGroup) string { return g.ReplicationGroupId }, in.Marker, in.MaxRecords)
	out := make([]*ReplicationGroup, len(list))
	for n, g := range list {
		out[n] = s.groupView(c, g)
	}
	return map[string]any{"ReplicationGroups": out, "Marker": marker}, nil
}

func (s *Service) modifyReplicationGroup(c *core.Ctx, in *cacheInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.ReplicationGroupId)
	if err == nil && in.CacheParameterGroupName != "" {
		_, err = s.paramGroup(c, in.CacheParameterGroupName)
	}
	if err == nil {
		err = s.checkGroups(c, in.UserGroupIdsToAdd)
	}
	if err == nil && len(g.MemberClusters) > 0 {
		if m := s.st.Clusters[key(c, g.MemberClusters[0])]; m != nil {
			err = s.checkIAM(c, in.UserGroupIdsToAdd, g.Engine, m.EngineVersion)
		}
	}
	if err != nil {
		return nil, err
	}
	if in.RemoveUserGroups {
		g.UserGroupIds = nil
	}
	g.UserGroupIds = slices.DeleteFunc(slices.Compact(slices.Sorted(slices.Values(append(g.UserGroupIds, lower(in.UserGroupIdsToAdd)...)))),
		func(id string) bool { return slices.Contains(lower(in.UserGroupIdsToRemove), id) })
	g.Description = cmp.Or(in.ReplicationGroupDescription, g.Description)
	g.CacheNodeType = cmp.Or(in.CacheNodeType, g.CacheNodeType)
	g.SnapshotWindow = cmp.Or(in.SnapshotWindow, g.SnapshotWindow)
	if in.AutomaticFailoverEnabled != nil {
		g.AutomaticFailover = onOff(in.AutomaticFailoverEnabled, false)
	}
	if in.MultiAZEnabled != nil {
		g.MultiAZ = onOff(in.MultiAZEnabled, false)
	}
	set(&g.SnapshotRetentionLimit, in.SnapshotRetentionLimit)
	set(&g.AutoMinorVersionUpgrade, in.AutoMinorVersionUpgrade)
	for _, m := range g.MemberClusters {
		if cl := s.st.Clusters[key(c, m)]; cl != nil {
			apply(cl, in)
		}
	}
	s.sync()
	return map[string]any{"ReplicationGroup": s.groupView(c, g)}, nil
}

func (s *Service) deleteReplicationGroup(c *core.Ctx, in *cacheInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.ReplicationGroupId)
	if err != nil {
		return nil, err
	}
	if in.RetainPrimaryCluster {
		return nil, invalid("pogi does not support RetainPrimaryCluster; delete the replication group without it.")
	}
	v := s.groupView(c, g)
	v.Status = "deleting"
	for _, m := range g.MemberClusters {
		delete(s.st.Clusters, key(c, m))
	}
	delete(s.st.Groups, key(c, g.ReplicationGroupId))
	if g.Pogi.Container != "" {
		s.box.Remove(g.Pogi.Container)
		delete(s.applied, g.Pogi.Container)
	}
	s.sync()
	return map[string]any{"ReplicationGroup": v}, nil
}

// ---- serverless caches ----

type serverlessInput struct {
	ServerlessCacheName, Description, Engine, MajorEngineVersion, KmsKeyId, UserGroupId, DailySnapshotTime, NextToken string
	CacheUsageLimits                                                                                                  *CacheUsageLimits
	SecurityGroupIds, SubnetIds                                                                                       []string
	SnapshotRetentionLimit, MaxResults                                                                                int
	Tags                                                                                                              []rds.Tag
}

func (s *Service) createServerlessCache(c *core.Ctx, in *serverlessInput) (any, error) {
	name := strings.ToLower(in.ServerlessCacheName)
	e, known := engines[in.Engine]
	switch err := rds.ValidID("ServerlessCacheName", name, 40); {
	case err != nil:
		return nil, err
	case !known:
		return nil, invalid("Invalid cache engine: %s", in.Engine)
	}
	ver := version(in.Engine, in.MajorEngineVersion)
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Serverless[key(c, name)] != nil {
		return nil, core.Errorf(400, "ServerlessCacheAlreadyExistsFault", "Serverless cache %s already exists.", name)
	}
	if in.UserGroupId != "" {
		if err := s.checkGroups(c, []string{in.UserGroupId}); err != nil {
			return nil, err
		}
	}
	sc := &ServerlessCache{ServerlessCacheName: name, Description: in.Description, CreateTime: time.Now().UTC(), Engine: in.Engine,
		MajorEngineVersion: strings.TrimSuffix(strings.TrimPrefix(family(in.Engine, ver), in.Engine), ".x"), FullEngineVersion: ver,
		CacheUsageLimits: in.CacheUsageLimits, KmsKeyId: in.KmsKeyId, SecurityGroupIds: in.SecurityGroupIds,
		ARN: c.ARN("elasticache", "serverlesscache:"+name), UserGroupId: strings.ToLower(in.UserGroupId), SubnetIds: in.SubnetIds,
		SnapshotRetentionLimit: in.SnapshotRetentionLimit, DailySnapshotTime: cmp.Or(in.DailySnapshotTime, "04:00"), Tags: rds.AddTags(nil, in.Tags)}
	s.st.Serverless[key(c, name)] = sc
	s.box.Serve(&sc.Pogi, c.Account, c.Region, name, 0, e.port, sc.spec, docker)
	s.sync()
	return map[string]any{"ServerlessCache": s.serverlessView(sc)}, nil
}

func (s *Service) describeServerlessCaches(c *core.Ctx, in *serverlessInput) (any, error) {
	name := strings.ToLower(in.ServerlessCacheName)
	s.mu.Lock()
	defer s.mu.Unlock()
	if name != "" {
		if _, err := s.serverless(c, name); err != nil {
			return nil, err
		}
	}
	var list []*ServerlessCache
	for k, sc := range s.st.Serverless {
		if mine(c, k) && (name == "" || sc.ServerlessCacheName == name) {
			list = append(list, sc)
		}
	}
	list, next := rds.Page(list, func(sc *ServerlessCache) string { return sc.ServerlessCacheName }, in.NextToken, in.MaxResults)
	out := make([]*ServerlessCache, len(list))
	for n, sc := range list {
		out[n] = s.serverlessView(sc)
	}
	return map[string]any{"ServerlessCaches": out, "NextToken": next}, nil
}

func (s *Service) deleteServerlessCache(c *core.Ctx, in *serverlessInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.serverless(c, in.ServerlessCacheName)
	if err != nil {
		return nil, err
	}
	v := s.serverlessView(sc)
	v.Status = "deleting"
	delete(s.st.Serverless, key(c, sc.ServerlessCacheName))
	if sc.Pogi.Container != "" {
		s.box.Remove(sc.Pogi.Container)
		delete(s.applied, sc.Pogi.Container)
	}
	s.sync()
	return map[string]any{"ServerlessCache": v}, nil
}

// ---- subnet groups ----

type subnetInput struct {
	CacheSubnetGroupName, CacheSubnetGroupDescription, Marker string
	SubnetIds                                                 []string
	Tags                                                      []rds.Tag
	MaxRecords                                                int
}

func (s *Service) createCacheSubnetGroup(c *core.Ctx, in *subnetInput) (any, error) {
	name := strings.ToLower(in.CacheSubnetGroupName)
	switch {
	case name == "" || name == "default":
		return nil, invalid("CacheSubnetGroupName must be given and must not be default.")
	case len(in.SubnetIds) == 0:
		return nil, invalid("The parameter SubnetIds must be provided.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Subnets[key(c, name)] != nil {
		return nil, core.Errorf(400, "CacheSubnetGroupAlreadyExists", "Cache subnet group %s already exists.", name)
	}
	g := &CacheSubnetGroup{CacheSubnetGroupName: name, CacheSubnetGroupDescription: in.CacheSubnetGroupDescription, VpcId: rds.DefaultVPC,
		Subnets: rds.Subnets(c.Region, in.SubnetIds), ARN: c.ARN("elasticache", "subnetgroup:"+name), SupportedNetworkTypes: []string{"ipv4"},
		Tags: rds.AddTags(nil, in.Tags)}
	s.st.Subnets[key(c, name)] = g
	return map[string]any{"CacheSubnetGroup": rds.Clone(g)}, nil
}

func (s *Service) describeCacheSubnetGroups(c *core.Ctx, in *subnetInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*CacheSubnetGroup
	if in.CacheSubnetGroupName != "" {
		g, err := s.subnetGroup(c, in.CacheSubnetGroupName)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	} else {
		for k, g := range s.st.Subnets {
			if mine(c, k) {
				list = append(list, g)
			}
		}
	}
	list, marker := rds.Page(list, func(g *CacheSubnetGroup) string { return g.CacheSubnetGroupName }, in.Marker, in.MaxRecords)
	out := make([]*CacheSubnetGroup, len(list))
	for n, g := range list {
		out[n] = rds.Clone(g)
	}
	return map[string]any{"CacheSubnetGroups": out, "Marker": marker}, nil
}

func (s *Service) modifyCacheSubnetGroup(c *core.Ctx, in *subnetInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.subnetGroup(c, in.CacheSubnetGroupName)
	if err != nil {
		return nil, err
	}
	g.CacheSubnetGroupDescription = cmp.Or(in.CacheSubnetGroupDescription, g.CacheSubnetGroupDescription)
	if len(in.SubnetIds) > 0 {
		g.Subnets = rds.Subnets(c.Region, in.SubnetIds)
	}
	return map[string]any{"CacheSubnetGroup": rds.Clone(g)}, nil
}

func (s *Service) deleteCacheSubnetGroup(c *core.Ctx, in *subnetInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.subnetGroup(c, in.CacheSubnetGroupName)
	if err != nil {
		return nil, err
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && cl.CacheSubnetGroupName == g.CacheSubnetGroupName {
			return nil, core.Errorf(400, "CacheSubnetGroupInUse", "Cache subnet group %s is currently in use by a cache cluster.", g.CacheSubnetGroupName)
		}
	}
	delete(s.st.Subnets, key(c, g.CacheSubnetGroupName))
	return nil, nil
}

// ---- parameter groups and engine versions ----

type paramInput struct {
	CacheParameterGroupName, CacheParameterGroupFamily, Description, Source, Engine, EngineVersion, Marker string
	ParameterNameValues                                                                                    []struct{ ParameterName, ParameterValue string }
	Tags                                                                                                   []rds.Tag
	MaxRecords                                                                                             int
	DefaultOnly, ResetAllParameters                                                                        bool
}

func (s *Service) createCacheParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	name := strings.ToLower(in.CacheParameterGroupName)
	switch {
	case name == "" || strings.HasPrefix(name, "default."):
		return nil, invalid("The parameter group name must be given and must not start with default.")
	case in.CacheParameterGroupFamily == "":
		return nil, invalid("The parameter CacheParameterGroupFamily must be provided and must not be blank.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Params[key(c, name)] != nil {
		return nil, core.Errorf(400, "CacheParameterGroupAlreadyExists", "Parameter group %s already exists.", name)
	}
	g := &CacheParameterGroup{CacheParameterGroupName: name, CacheParameterGroupFamily: in.CacheParameterGroupFamily,
		Description: in.Description, ARN: c.ARN("elasticache", "parametergroup:"+name), Tags: rds.AddTags(nil, in.Tags)}
	s.st.Params[key(c, name)] = g
	return map[string]any{"CacheParameterGroup": rds.Clone(g)}, nil
}

func (s *Service) describeCacheParameterGroups(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*CacheParameterGroup
	if in.CacheParameterGroupName != "" {
		g, err := s.paramGroup(c, in.CacheParameterGroupName)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	} else {
		for k, g := range s.st.Params {
			if mine(c, k) {
				list = append(list, g)
			}
		}
	}
	list, marker := rds.Page(list, func(g *CacheParameterGroup) string { return g.CacheParameterGroupName }, in.Marker, in.MaxRecords)
	out := make([]*CacheParameterGroup, len(list))
	for n, g := range list {
		out[n] = rds.Clone(g)
	}
	return map[string]any{"CacheParameterGroups": out, "Marker": marker}, nil
}

// modifyCacheParameterGroup sets parameters, or (Reset) drops them back to the engine defaults.
func (s *Service) modifyCacheParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.CacheParameterGroupName)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(g.CacheParameterGroupName, "default.") {
		return nil, core.Errorf(400, "InvalidCacheParameterGroupState", "Default parameter groups cannot be modified: %s", g.CacheParameterGroupName)
	}
	if g.Params == nil || in.ResetAllParameters {
		g.Params = map[string]string{}
	}
	for _, p := range in.ParameterNameValues {
		if c.Op == "ResetCacheParameterGroup" {
			delete(g.Params, p.ParameterName)
		} else {
			g.Params[p.ParameterName] = p.ParameterValue
		}
	}
	return map[string]string{"CacheParameterGroupName": g.CacheParameterGroupName}, nil
}

func (s *Service) deleteCacheParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.CacheParameterGroupName)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(g.CacheParameterGroupName, "default.") {
		return nil, core.Errorf(400, "InvalidCacheParameterGroupState", "Default parameter groups cannot be deleted: %s", g.CacheParameterGroupName)
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && cl.CacheParameterGroup.CacheParameterGroupName == g.CacheParameterGroupName {
			return nil, core.Errorf(400, "InvalidCacheParameterGroupState", "Parameter group %s is in use by cache cluster %s.", g.CacheParameterGroupName, cl.CacheClusterId)
		}
	}
	delete(s.st.Params, key(c, g.CacheParameterGroupName))
	return nil, nil
}

// describeCacheParameters lists the parameters set by users.
// ponytail: engine defaults are not listed and values do not reach the container.
func (s *Service) describeCacheParameters(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.CacheParameterGroupName)
	if err != nil {
		return nil, err
	}
	var list []Parameter
	if in.Source == "" || in.Source == "user" {
		for n, v := range g.Params {
			list = append(list, Parameter{n, v, n, "user", "string", true, "immediate"})
		}
	}
	list, marker := rds.Page(list, func(p Parameter) string { return p.ParameterName }, in.Marker, in.MaxRecords)
	return map[string]any{"Parameters": list, "Marker": marker}, nil
}

func (s *Service) describeCacheEngineVersions(c *core.Ctx, in *paramInput) (any, error) {
	var list []CacheEngineVersion
	for _, name := range core.SortedKeys(engines) {
		if in.Engine != "" && in.Engine != name {
			continue
		}
		for _, v := range engines[name].versions {
			if in.EngineVersion != "" && v != in.EngineVersion && !strings.HasPrefix(v, in.EngineVersion+".") ||
				in.CacheParameterGroupFamily != "" && family(name, v) != in.CacheParameterGroupFamily {
				continue
			}
			list = append(list, CacheEngineVersion{name, v, family(name, v), engines[name].desc, engines[name].desc + " " + v})
			if in.DefaultOnly {
				break
			}
		}
	}
	list, marker := rds.Page(list, func(v CacheEngineVersion) string { return v.Engine + "/" + v.EngineVersion }, in.Marker, in.MaxRecords)
	return map[string]any{"CacheEngineVersions": list, "Marker": marker}, nil
}

// ---- tags ----

type tagsInput struct {
	ResourceName string
	Tags         []rds.Tag
	TagKeys      []string
}

// tags finds the tag list of the resource an ARN names; lock held.
func (s *Service) tags(arn string) (*[]rds.Tag, error) {
	svc, region, account, res, ok := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, ":")
	c := &core.Ctx{Account: account, Region: region}
	if ok && svc == "elasticache" && name != "" {
		switch kind {
		case "cluster":
			cl, err := s.cluster(c, name)
			if err != nil {
				return nil, err
			}
			return &cl.Tags, nil
		case "replicationgroup":
			g, err := s.group(c, name)
			if err != nil {
				return nil, err
			}
			return &g.Tags, nil
		case "serverlesscache":
			sc, err := s.serverless(c, name)
			if err != nil {
				return nil, err
			}
			return &sc.Tags, nil
		case "subnetgroup":
			g, err := s.subnetGroup(c, name)
			if err != nil {
				return nil, err
			}
			return &g.Tags, nil
		case "parametergroup":
			if g := s.st.Params[key(c, name)]; g != nil {
				return &g.Tags, nil
			}
			return nil, core.Errorf(404, "CacheParameterGroupNotFound", "CacheParameterGroup %s not found.", name)
		case "user":
			if u := s.st.Users[key(c, name)]; u != nil {
				return &u.Tags, nil
			}
			return nil, core.Errorf(404, "UserNotFound", "User %s not found.", name)
		case "usergroup":
			g, err := s.userGroup(c, name)
			if err != nil {
				return nil, err
			}
			return &g.Tags, nil
		}
	}
	return nil, core.Errorf(400, "InvalidARN", "The ARN %s is not valid.", arn)
}

func (s *Service) addTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	*t = rds.AddTags(*t, in.Tags)
	return map[string]any{"TagList": slices.Clone(*t)}, nil
}

func (s *Service) removeTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		if !slices.ContainsFunc(*t, func(tag rds.Tag) bool { return tag.Key == k }) {
			return nil, core.Errorf(404, "TagNotFound", "The tag key %s is not on resource %s.", k, in.ResourceName)
		}
	}
	*t = rds.RemoveTags(*t, in.TagKeys)
	return map[string]any{"TagList": slices.Clone(*t)}, nil
}

func (s *Service) listTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"TagList": slices.Clone(*t)}, nil
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped resources go.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	keep := st.containers()
	docker := len(keep) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	st.servers(func(srv *rds.Server, sp func() rds.Spec) { s.box.Resume(srv, sp(), docker) })
	for name, p := range s.proxies { // the new state's servers get their own
		p.ln.Close()
		delete(s.proxies, name)
	}
	clear(s.applied) // a resumed server lost its users
	if docker {
		s.sync()
	}
	s.mu.Unlock()
	for n := range old.containers() {
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close removes the containers, except those persisted state resumes next run.
func (s *Service) Close() error {
	s.mu.Lock()
	keep := s.st.containers()
	for _, p := range s.proxies {
		p.ln.Close()
	}
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

func lower(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strings.ToLower(id)
	}
	return out
}

func addr(e *rds.Endpoint) string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", e.Address, e.Port)
}

// Resources lists cache clusters, replication groups and serverless caches.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	cols := []string{"Id", "Engine", "Status", "Endpoint", "Region", "Account"}
	cls := core.Table{Name: "Cache clusters", Columns: append(slices.Clone(cols), "Replication group")}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		c, cl := ctxOf(k), s.st.Clusters[k]
		srv, _ := s.server(c, cl)
		cls.Rows = append(cls.Rows, []string{cl.CacheClusterId, cl.Engine + " " + cl.EngineVersion, srv.State, addr(s.endpoint(srv)),
			c.Region, c.Account, cl.ReplicationGroupId})
	}
	rgs := core.Table{Name: "Replication groups", Columns: cols}
	for _, k := range core.SortedKeys(s.st.Groups) {
		c := ctxOf(k)
		v := s.groupView(c, s.st.Groups[k])
		rgs.Rows = append(rgs.Rows, []string{v.ReplicationGroupId, v.Engine, v.Status, addr(s.endpoint(&s.st.Groups[k].Pogi)), c.Region, c.Account})
	}
	sls := core.Table{Name: "Serverless caches", Columns: cols}
	for _, k := range core.SortedKeys(s.st.Serverless) {
		c := ctxOf(k)
		v := s.serverlessView(s.st.Serverless[k])
		sls.Rows = append(sls.Rows, []string{v.ServerlessCacheName, v.Engine + " " + v.FullEngineVersion, v.Status, addr(v.Endpoint), c.Region, c.Account})
	}
	return []core.Table{cls, rgs, sls}
}

// Inspect returns a resource with pogi's side: container, state and pogiNote.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Replication groups":
		for k, g := range s.st.Groups {
			if g.ReplicationGroupId == id {
				return s.groupView(ctxOf(k), g)
			}
		}
	case "Serverless caches":
		for _, sc := range s.st.Serverless {
			if sc.ServerlessCacheName == id {
				return s.serverlessView(sc)
			}
		}
	default:
		for k, cl := range s.st.Clusters {
			if cl.CacheClusterId == id {
				return s.view(ctxOf(k), cl, true)
			}
		}
	}
	return nil
}
