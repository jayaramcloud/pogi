// Package memorydb emulates Amazon MemoryDB over JSON 1.1. Clusters run as
// one-node Valkey (or Redis) clusters in Docker, and the users of their ACL
// are put in the server with ACL SETUSER; subnet groups, parameter groups,
// snapshots and tags are metadata. Without Docker the API still answers.
// ponytail: TLS is recorded, not served; IAM users exist but cannot sign in
// (ElastiCache's token proxy is not wired here).
package memorydb

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the MemoryDB emulator.
type Service struct {
	hub     *core.Hub
	rpc     *core.RPC
	box     *rds.Box
	mu      sync.Mutex
	st      state
	applied map[string]string // container → the users script it last got
}

type state struct {
	Clusters  map[string]*cluster        `json:"clusters"` // account/region/name
	Users     map[string]*user           `json:"users"`
	ACLs      map[string]*ACL            `json:"acls"`
	Subnets   map[string]*SubnetGroup    `json:"subnetGroups"`
	Params    map[string]*ParameterGroup `json:"parameterGroups"`
	Snapshots map[string]*Snapshot       `json:"snapshots"`
	Tags      map[string][]rds.Tag       `json:"tags"` // by ARN
}

func newState() state {
	return state{Clusters: map[string]*cluster{}, Users: map[string]*user{}, ACLs: map[string]*ACL{}, Subnets: map[string]*SubnetGroup{},
		Params: map[string]*ParameterGroup{}, Snapshots: map[string]*Snapshot{}, Tags: map[string][]rds.Tag{}}
}

// engines and their versions, newest first; the first engine and version are the defaults.
var engines = map[string][]string{"valkey": {"7.3", "7.2"}, "redis": {"7.1", "7.0", "6.2"}}

// New creates the MemoryDB service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState(), applied: map[string]string{}}
	s.box = rds.NewBox(hub, "memorydb", "create-failed", &s.mu, s.owns)
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateCluster":           core.H(s.createCluster),
		"DescribeClusters":        core.H(s.describeClusters),
		"UpdateCluster":           core.H(s.updateCluster),
		"DeleteCluster":           core.H(s.deleteCluster),
		"CreateUser":              core.H(s.createUser),
		"DescribeUsers":           core.H(s.describeUsers),
		"UpdateUser":              core.H(s.updateUser),
		"DeleteUser":              core.H(s.deleteUser),
		"CreateACL":               core.H(s.createACL),
		"DescribeACLs":            core.H(s.describeACLs),
		"UpdateACL":               core.H(s.updateACL),
		"DeleteACL":               core.H(s.deleteACL),
		"CreateSubnetGroup":       core.H(s.createSubnetGroup),
		"DescribeSubnetGroups":    core.H(s.describeSubnetGroups),
		"UpdateSubnetGroup":       core.H(s.updateSubnetGroup),
		"DeleteSubnetGroup":       core.H(s.deleteSubnetGroup),
		"CreateParameterGroup":    core.H(s.createParameterGroup),
		"DescribeParameterGroups": core.H(s.describeParameterGroups),
		"UpdateParameterGroup":    core.H(s.updateParameterGroup),
		"ResetParameterGroup":     core.H(s.updateParameterGroup),
		"DeleteParameterGroup":    core.H(s.deleteParameterGroup),
		"DescribeParameters":      core.H(s.describeParameters),
		"CreateSnapshot":          core.H(s.createSnapshot),
		"DescribeSnapshots":       core.H(s.describeSnapshots),
		"DeleteSnapshot":          core.H(s.deleteSnapshot),
		"DescribeEngineVersions":  core.H(s.describeEngineVersions),
		"TagResource":             core.H(s.tagResource),
		"UntagResource":           core.H(s.untagResource),
		"ListTags":                core.H(s.listTags),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "memorydb", Cloud: core.AWS, Title: "MemoryDB", Targets: []string{"AmazonMemoryDB"}, Signing: []string{"memorydb"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// ---- API shapes ----

// Cluster is a MemoryDB cluster.
type Cluster struct {
	Name, Description, Status                  string
	NumberOfShards                             int
	Shards                                     []Shard `json:",omitempty"`
	AvailabilityMode                           string
	ClusterEndpoint                            *rds.Endpoint `json:",omitempty"`
	NodeType, Engine, EngineVersion            string
	EnginePatchVersion                         string
	ParameterGroupName, ParameterGroupStatus   string
	SecurityGroups                             []SecurityGroupMembership `json:",omitempty"`
	SubnetGroupName                            string
	TLSEnabled                                 bool
	KmsKeyId                                   string `json:",omitempty"`
	ARN                                        string
	SnsTopicArn                                string `json:",omitempty"`
	SnsTopicStatus                             string `json:",omitempty"`
	SnapshotRetentionLimit                     int
	MaintenanceWindow, SnapshotWindow, ACLName string
	AutoMinorVersionUpgrade                    bool
	DataTiering, NetworkType, IpDiscovery      string
}

// cluster is a cluster with pogi's side: its replicas per shard, creation time and server.
type cluster struct {
	Cluster
	Replicas int        `json:"replicas"`
	Created  core.Time  `json:"created"`
	Pogi     rds.Server `json:"pogi"`
}

// Shard is a shard of a cluster; all nodes share the cluster's one server.
type Shard struct {
	Name, Status, Slots string
	Nodes               []Node
	NumberOfNodes       int
}

// Node is a node of a shard.
type Node struct {
	Name, Status, AvailabilityZone string
	CreateTime                     core.Time
	Endpoint                       *rds.Endpoint `json:",omitempty"`
}

// SecurityGroupMembership is a security group of a cluster.
type SecurityGroupMembership struct{ SecurityGroupId, Status string }

// User is a MemoryDB user.
type User struct {
	Name, Status, AccessString string
	ACLNames                   []string
	MinimumEngineVersion       string
	Authentication             struct {
		Type          string
		PasswordCount int `json:",omitempty"`
	}
	ARN string
}

// user is a user with its passwords in the server (an IAM user's: a secret of pogi's).
type user struct {
	User
	Passwords []string `json:"passwords,omitempty"`
}

// ACL is an access control list: users a cluster lets in.
type ACL struct {
	Name, Status         string
	UserNames            []string
	MinimumEngineVersion string
	Clusters             []string `json:",omitempty"`
	ARN                  string
}

// SubnetGroup is a subnet group.
type SubnetGroup struct {
	Name, Description, VpcId string
	Subnets                  []Subnet
	ARN                      string
	SupportedNetworkTypes    []string
}

// Subnet is a subnet of a subnet group.
type Subnet struct {
	Identifier            string
	AvailabilityZone      struct{ Name string }
	SupportedNetworkTypes []string
}

// ParameterGroup is a parameter group.
type ParameterGroup struct {
	Name, Family, Description, ARN string
	Params                         map[string]string `json:"params,omitempty"` // pogi's: values set
}

// Snapshot is a snapshot of a cluster.
// ponytail: metadata only; restoring one makes an empty cluster.
type Snapshot struct {
	Name, Status, Source string
	KmsKeyId             string `json:",omitempty"`
	ARN                  string
	ClusterConfiguration struct {
		Name, Description, NodeType, Engine, EngineVersion, MaintenanceWindow string
		TopicArn                                                              string `json:",omitempty"`
		Port                                                                  int
		ParameterGroupName, SubnetGroupName, VpcId                            string
		SnapshotRetentionLimit                                                int
		SnapshotWindow                                                        string
		NumShards                                                             int
	}
	DataTiering string
}

// ---- helpers ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValueException", format, a...)
}

func combo(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterCombinationException", format, a...)
}

func notFound(kind, name string) error {
	return core.Errorf(404, kind+"NotFoundFault", "%s %s not found.", kind, name)
}

func key(c *core.Ctx, name string) string { return c.Account + "/" + c.Region + "/" + name }
func mine(c *core.Ctx, k string) bool     { return strings.HasPrefix(k, c.Account+"/"+c.Region+"/") }

func ctxOf(k string) *core.Ctx {
	p := strings.SplitN(k, "/", 3)
	return &core.Ctx{Account: p[0], Region: p[1]}
}

var nameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]*$`)

// valid checks a resource name: a letter, then letters, digits and single hyphens.
func valid(field, name string, max int) error {
	if len(name) > max || !nameRe.MatchString(name) || strings.HasSuffix(name, "-") || strings.Contains(name, "--") {
		return invalid("%s must be 1 to %d letters, digits or hyphens, start with a letter, and have no two hyphens in a row or at the end.", field, max)
	}
	return nil
}

func set[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

// list returns the values of m in the caller's account and region that keep says to.
func list[T any](c *core.Ctx, m map[string]*T, keep func(*T) bool) []*T {
	var out []*T
	for k, v := range m {
		if mine(c, k) && keep(v) {
			out = append(out, v)
		}
	}
	return out
}

func page[T any](items []*T, name func(*T) string, token string, max int) ([]*T, string) {
	return rds.Page(items, name, token, max)
}

// clones copies records into views that are safe to encode after unlocking.
func clones[T any](items []*T) []*T {
	out := make([]*T, len(items))
	for i, v := range items {
		out[i] = rds.Clone(v)
	}
	return out
}

func onOff(b bool) string { return fmt.Sprint(b) } // DataTiering is "true" or "false"

// ---- lookups (lock held) ----

// openAccess is the ACL every account has: the default user, open to all.
func openAccess(c *core.Ctx) *ACL {
	return &ACL{Name: "open-access", Status: "active", UserNames: []string{"default"}, MinimumEngineVersion: "6.2", ARN: c.ARN("memorydb", "acl/open-access")}
}

func defaultUser(c *core.Ctx) *user {
	u := &user{User: User{Name: "default", Status: "active", AccessString: "on ~* &* +@all", MinimumEngineVersion: "6.2", ARN: c.ARN("memorydb", "user/default")}}
	u.Authentication.Type = "no-password"
	return u
}

func (s *Service) cluster(c *core.Ctx, name string) (*cluster, error) {
	if cl := s.st.Clusters[key(c, strings.ToLower(name))]; cl != nil {
		return cl, nil
	}
	return nil, notFound("Cluster", name)
}

func (s *Service) user(c *core.Ctx, name string) (*user, error) {
	if u := s.st.Users[key(c, name)]; u != nil {
		return u, nil
	}
	if name == "default" {
		return defaultUser(c), nil
	}
	return nil, notFound("User", name)
}

func (s *Service) acl(c *core.Ctx, name string) (*ACL, error) {
	name = strings.ToLower(name)
	if a := s.st.ACLs[key(c, name)]; a != nil {
		return a, nil
	}
	if name == "open-access" {
		return openAccess(c), nil
	}
	return nil, notFound("ACL", name)
}

func (s *Service) subnetGroup(c *core.Ctx, name string) (*SubnetGroup, error) {
	name = strings.ToLower(name)
	if g := s.st.Subnets[key(c, name)]; g != nil {
		return g, nil
	}
	if name == "default" {
		return &SubnetGroup{Name: "default", Description: "Default subnet group", VpcId: rds.DefaultVPC, ARN: c.ARN("memorydb", "subnetgroup/default"),
			Subnets: subnets(c.Region, []string{"subnet-0a1b2c3d", "subnet-1a2b3c4d", "subnet-2a3b4c5d"}), SupportedNetworkTypes: []string{"ipv4"}}, nil
	}
	return nil, notFound("SubnetGroup", name)
}

func subnets(region string, ids []string) []Subnet {
	out := make([]Subnet, len(ids))
	for i, id := range ids {
		out[i] = Subnet{Identifier: id, SupportedNetworkTypes: []string{"ipv4"}}
		out[i].AvailabilityZone.Name = region + string(rune('a'+i%3))
	}
	return out
}

// family is the parameter group family of an engine version: memorydb_valkey7, memorydb_redis6.
func family(engine, version string) string {
	major, _, _ := strings.Cut(version, ".")
	return "memorydb_" + engine + major
}

func (s *Service) paramGroup(c *core.Ctx, name string) (*ParameterGroup, error) {
	name = strings.ToLower(name)
	if g := s.st.Params[key(c, name)]; g != nil {
		return g, nil
	}
	if fam, ok := strings.CutPrefix(name, "default.memorydb-"); ok && fam != "" {
		return &ParameterGroup{Name: name, Family: "memorydb_" + fam, Description: "Default parameter group for memorydb_" + fam,
			ARN: c.ARN("memorydb", "parametergroup/"+name)}, nil
	}
	return nil, notFound("ParameterGroup", name)
}

// view is the API shape of a cluster; lock held.
func (s *Service) view(c *core.Ctx, cl *cluster, shards bool) *Cluster {
	v := cl.Cluster
	ep := cl.Pogi.Addr()
	v.Status, v.ClusterEndpoint = cl.Pogi.State, ep
	v.AvailabilityMode = "singleaz"
	if cl.Replicas > 0 {
		v.AvailabilityMode = "multiaz"
	}
	for sh := 1; shards && sh <= cl.NumberOfShards; sh++ {
		shard := Shard{Name: fmt.Sprintf("%04d", sh), Status: cl.Pogi.State, NumberOfNodes: cl.Replicas + 1,
			Slots: fmt.Sprintf("%d-%d", (sh-1)*16384/cl.NumberOfShards, sh*16384/cl.NumberOfShards-1)}
		for n := 1; n <= cl.Replicas+1; n++ {
			shard.Nodes = append(shard.Nodes, Node{fmt.Sprintf("%s-%04d-%03d", cl.Name, sh, n), cl.Pogi.State,
				c.Region + string(rune('a'+(n-1)%3)), cl.Created, ep})
		}
		v.Shards = append(v.Shards, shard)
	}
	return &v
}

// userView adds the ACLs of a user; lock held.
func (s *Service) userView(c *core.Ctx, u *user) *User {
	v := u.User
	v.ACLNames = nil
	for _, a := range s.acls(c) {
		if slices.Contains(a.UserNames, u.Name) {
			v.ACLNames = append(v.ACLNames, a.Name)
		}
	}
	return &v
}

// acls are the ACLs of an account and region, open-access too; lock held.
func (s *Service) acls(c *core.Ctx) []*ACL {
	out := append(list(c, s.st.ACLs, func(*ACL) bool { return true }), openAccess(c))
	slices.SortFunc(out, func(a, b *ACL) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// aclView adds the clusters of an ACL; lock held.
func (s *Service) aclView(c *core.Ctx, a *ACL) *ACL {
	v := *a
	v.UserNames, v.Clusters = slices.Clone(a.UserNames), nil
	for _, cl := range list(c, s.st.Clusters, func(cl *cluster) bool { return cl.ACLName == a.Name }) {
		v.Clusters = append(v.Clusters, cl.Name)
	}
	slices.Sort(v.Clusters)
	return &v
}

// ---- clusters ----

type clusterInput struct {
	ClusterName, NodeType, ParameterGroupName, Description, SubnetGroupName, MaintenanceWindow string
	SnsTopicArn, SnsTopicStatus, KmsKeyId, SnapshotName, SnapshotWindow, ACLName, Engine       string
	EngineVersion, NetworkType, IpDiscovery, FinalSnapshotName, NextToken                      string
	NumShards, NumReplicasPerShard, Port, SnapshotRetentionLimit                               *int
	TLSEnabled, AutoMinorVersionUpgrade, DataTiering, ShowShardDetails                         *bool
	SecurityGroupIds                                                                           []string
	ReplicaConfiguration                                                                       *struct{ ReplicaCount int }
	ShardConfiguration                                                                         *struct{ ShardCount int }
	Tags                                                                                       []rds.Tag
	MaxResults                                                                                 int
}

func sgs(ids []string) []SecurityGroupMembership {
	out := make([]SecurityGroupMembership, len(ids))
	for i, id := range ids {
		out[i] = SecurityGroupMembership{id, "active"}
	}
	return out
}

func (s *Service) createCluster(c *core.Ctx, in *clusterInput) (any, error) {
	name, engine := strings.ToLower(in.ClusterName), strings.ToLower(cmp.Or(in.Engine, "valkey"))
	versions, known := engines[engine]
	switch err := valid("ClusterName", name, 40); {
	case err != nil:
		return nil, err
	case !strings.HasPrefix(in.NodeType, "db."):
		return nil, invalid("Invalid node type: %s", in.NodeType)
	case in.ACLName == "":
		return nil, invalid("The parameter ACLName must be provided.")
	case !known:
		return nil, invalid("Invalid engine: %s", in.Engine)
	}
	version := versions[0]
	if in.EngineVersion != "" {
		if version = in.EngineVersion; !slices.ContainsFunc(versions, func(v string) bool { return v == version || strings.HasPrefix(v, version+".") }) {
			return nil, invalid("Invalid engine version %s for %s.", in.EngineVersion, engine)
		}
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Clusters[key(c, name)] != nil {
		return nil, core.Errorf(400, "ClusterAlreadyExistsFault", "Cluster %s already exists.", name)
	}
	acl, err := s.acl(c, in.ACLName)
	if err != nil {
		return nil, err
	}
	pg, err := s.paramGroup(c, cmp.Or(in.ParameterGroupName, "default.memorydb-"+strings.TrimPrefix(family(engine, version), "memorydb_")))
	if err != nil {
		return nil, err
	}
	sg, err := s.subnetGroup(c, cmp.Or(in.SubnetGroupName, "default"))
	if err != nil {
		return nil, err
	}
	cl := &cluster{Cluster: Cluster{Name: name, Description: in.Description, NumberOfShards: 1, NodeType: in.NodeType, Engine: engine,
		EngineVersion: version, EnginePatchVersion: version + ".0", ParameterGroupName: pg.Name, ParameterGroupStatus: "in-sync",
		SecurityGroups: sgs(in.SecurityGroupIds), SubnetGroupName: sg.Name, TLSEnabled: true, KmsKeyId: in.KmsKeyId,
		ARN: c.ARN("memorydb", "cluster/"+name), SnsTopicArn: in.SnsTopicArn, MaintenanceWindow: cmp.Or(in.MaintenanceWindow, "wed:08:00-wed:09:00"),
		SnapshotWindow: cmp.Or(in.SnapshotWindow, "05:00-06:00"), ACLName: acl.Name, AutoMinorVersionUpgrade: true, DataTiering: "false",
		NetworkType: cmp.Or(in.NetworkType, "ipv4"), IpDiscovery: cmp.Or(in.IpDiscovery, "ipv4")},
		Replicas: 1, Created: core.Time(time.Now().UTC())}
	if in.SnsTopicArn != "" {
		cl.SnsTopicStatus = "active"
	}
	set(&cl.NumberOfShards, in.NumShards)
	set(&cl.Replicas, in.NumReplicasPerShard)
	set(&cl.SnapshotRetentionLimit, in.SnapshotRetentionLimit)
	set(&cl.TLSEnabled, in.TLSEnabled)
	set(&cl.AutoMinorVersionUpgrade, in.AutoMinorVersionUpgrade)
	if in.DataTiering != nil {
		cl.DataTiering = onOff(*in.DataTiering)
	}
	if cl.NumberOfShards < 1 || cl.Replicas < 0 || cl.Replicas > 5 {
		return nil, invalid("NumShards must be at least 1 and NumReplicasPerShard 0 to 5.")
	}
	s.st.Clusters[key(c, name)] = cl
	s.st.Tags[cl.ARN] = rds.AddTags(nil, in.Tags)
	port := 0 // any free one, like ElastiCache; 6379 without Docker
	set(&port, in.Port)
	s.box.Serve(&cl.Pogi, c.Account, c.Region, name, port, 6379, cl.spec, docker)
	s.sync()
	return map[string]any{"Cluster": s.view(c, cl, false)}, nil
}

func (cl *cluster) spec() rds.Spec { return elasticache.Spec(cl.Engine, cl.Pogi.HostPort, true) }

func (s *Service) owns(srv *rds.Server) bool {
	for _, cl := range s.st.Clusters {
		if &cl.Pogi == srv {
			return true
		}
	}
	return false
}

func (s *Service) describeClusters(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.ClusterName != "" {
		if _, err := s.cluster(c, in.ClusterName); err != nil {
			return nil, err
		}
	}
	all := list(c, s.st.Clusters, func(cl *cluster) bool { return in.ClusterName == "" || cl.Name == strings.ToLower(in.ClusterName) })
	all, next := page(all, func(cl *cluster) string { return cl.Name }, in.NextToken, in.MaxResults)
	out := make([]*Cluster, len(all))
	for i, cl := range all {
		out[i] = s.view(c, cl, in.ShowShardDetails != nil && *in.ShowShardDetails)
	}
	return map[string]any{"Clusters": out, "NextToken": next}, nil
}

func (s *Service) updateCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterName)
	if err != nil {
		return nil, err
	}
	if in.ACLName != "" {
		a, err := s.acl(c, in.ACLName)
		if err != nil {
			return nil, err
		}
		cl.ACLName = a.Name
	}
	if in.ParameterGroupName != "" {
		pg, err := s.paramGroup(c, in.ParameterGroupName)
		if err != nil {
			return nil, err
		}
		cl.ParameterGroupName = pg.Name
	}
	if in.ShardConfiguration != nil && in.ShardConfiguration.ShardCount < 1 || in.ReplicaConfiguration != nil && in.ReplicaConfiguration.ReplicaCount > 5 {
		return nil, invalid("A cluster has at least 1 shard and at most 5 replicas per shard.")
	}
	if in.ShardConfiguration != nil {
		cl.NumberOfShards = in.ShardConfiguration.ShardCount
	}
	if in.ReplicaConfiguration != nil {
		cl.Replicas = in.ReplicaConfiguration.ReplicaCount
	}
	cl.Description = cmp.Or(in.Description, cl.Description)
	cl.MaintenanceWindow = cmp.Or(in.MaintenanceWindow, cl.MaintenanceWindow)
	cl.SnapshotWindow = cmp.Or(in.SnapshotWindow, cl.SnapshotWindow)
	cl.NodeType = cmp.Or(in.NodeType, cl.NodeType)
	cl.EngineVersion = cmp.Or(in.EngineVersion, cl.EngineVersion) // the image stays: one per engine
	cl.IpDiscovery = cmp.Or(in.IpDiscovery, cl.IpDiscovery)
	set(&cl.SnapshotRetentionLimit, in.SnapshotRetentionLimit)
	if in.SecurityGroupIds != nil {
		cl.SecurityGroups = sgs(in.SecurityGroupIds)
	}
	if in.SnsTopicArn != "" || in.SnsTopicStatus != "" {
		cl.SnsTopicArn, cl.SnsTopicStatus = cmp.Or(in.SnsTopicArn, cl.SnsTopicArn), cmp.Or(in.SnsTopicStatus, "active")
	}
	s.sync()
	return map[string]any{"Cluster": s.view(c, cl, false)}, nil
}

func (s *Service) deleteCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterName)
	if err != nil {
		return nil, err
	}
	if in.FinalSnapshotName != "" {
		if _, err := s.addSnapshot(c, cl, in.FinalSnapshotName, "manual", nil); err != nil {
			return nil, err
		}
	}
	v := s.view(c, cl, false)
	v.Status = "deleting"
	delete(s.st.Clusters, key(c, cl.Name))
	delete(s.st.Tags, cl.ARN)
	if cl.Pogi.Container != "" {
		s.box.Remove(cl.Pogi.Container)
		delete(s.applied, cl.Pogi.Container)
	}
	return map[string]any{"Cluster": v}, nil
}

// sync puts the users of their ACL in the servers that run: a restricted
// ACL turns the default user off. Lock held.
func (s *Service) sync() {
	for k, cl := range s.st.Clusters {
		if cl.Pogi.Container == "" {
			continue
		}
		c := ctxOf(k)
		users := []elasticache.ACLUser{{Name: "default", Access: "off"}}
		if a, err := s.acl(c, cl.ACLName); err == nil {
			for _, n := range a.UserNames {
				if u, err := s.user(c, n); err == nil {
					users = append(users, elasticache.ACLUser{Name: u.Name, Access: u.AccessString, Passwords: u.Passwords})
				}
			}
		}
		cl.Pogi.Password = cmp.Or(cl.Pogi.Password, core.RandString(32, core.AlphaNum)) // pogi's admin in the server
		if script := elasticache.ACLScript(cl.Engine, cl.Pogi.Password, true, users); script != s.applied[cl.Pogi.Container] {
			s.applied[cl.Pogi.Container] = script
			s.box.Exec(&cl.Pogi, "sh", "-c", script)
		}
	}
}

// ---- users ----

type userInput struct {
	UserName, AccessString, NextToken string
	AuthenticationMode                *struct {
		Type      string
		Passwords []string
	}
	Tags       []rds.Tag
	MaxResults int
}

// auth checks an authentication mode and returns the server passwords of a
// user (an IAM user's is a secret of pogi's) and how the API shows it.
func (in *userInput) auth(u *user) error {
	m := in.AuthenticationMode
	for _, p := range m.Passwords {
		if len(p) < 16 || len(p) > 128 {
			return invalid("Passwords must be 16 to 128 characters long.")
		}
	}
	switch {
	case m.Type == "password" && len(m.Passwords) > 0 && len(m.Passwords) <= 2:
		u.Passwords = m.Passwords
	case m.Type == "iam" && len(m.Passwords) == 0:
		u.Passwords = []string{core.RandString(32, core.AlphaNum)}
	default:
		return combo("Authentication type %q takes one or two Passwords for password, none for iam.", m.Type)
	}
	u.Authentication.Type, u.Authentication.PasswordCount = m.Type, 0
	if m.Type == "password" {
		u.Authentication.PasswordCount = len(m.Passwords)
	}
	return nil
}

func (s *Service) createUser(c *core.Ctx, in *userInput) (any, error) {
	switch err := valid("UserName", in.UserName, 40); {
	case err != nil:
		return nil, err
	case in.AccessString == "":
		return nil, invalid("The parameter AccessString must be provided.")
	case in.AuthenticationMode == nil:
		return nil, invalid("The parameter AuthenticationMode must be provided.")
	}
	u := &user{User: User{Name: in.UserName, Status: "active", AccessString: in.AccessString, MinimumEngineVersion: "6.2", ARN: c.ARN("memorydb", "user/"+in.UserName)}}
	if err := in.auth(u); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.user(c, in.UserName); err == nil {
		return nil, core.Errorf(400, "UserAlreadyExistsFault", "User %s already exists.", in.UserName)
	}
	s.st.Users[key(c, u.Name)] = u
	s.st.Tags[u.ARN] = rds.AddTags(nil, in.Tags)
	return map[string]any{"User": s.userView(c, u)}, nil
}

func (s *Service) describeUsers(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.UserName != "" {
		if _, err := s.user(c, in.UserName); err != nil {
			return nil, err
		}
	}
	all := append(list(c, s.st.Users, func(*user) bool { return true }), defaultUser(c))
	all = slices.DeleteFunc(all, func(u *user) bool { return in.UserName != "" && u.Name != in.UserName })
	all, next := page(all, func(u *user) string { return u.Name }, in.NextToken, in.MaxResults)
	out := make([]*User, len(all))
	for i, u := range all {
		out[i] = s.userView(c, u)
	}
	return map[string]any{"Users": out, "NextToken": next}, nil
}

func (s *Service) updateUser(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.st.Users[key(c, in.UserName)]
	switch {
	case u == nil && in.UserName == "default":
		return nil, invalid("The default user cannot be modified.")
	case u == nil:
		return nil, notFound("User", in.UserName)
	}
	if in.AuthenticationMode != nil {
		if err := in.auth(u); err != nil {
			return nil, err
		}
	}
	u.AccessString = cmp.Or(in.AccessString, u.AccessString)
	s.sync()
	return map[string]any{"User": s.userView(c, u)}, nil
}

func (s *Service) deleteUser(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.st.Users[key(c, in.UserName)]
	switch {
	case u == nil && in.UserName == "default":
		return nil, invalid("The default user cannot be deleted.")
	case u == nil:
		return nil, notFound("User", in.UserName)
	}
	v := s.userView(c, u)
	v.Status = "deleting"
	for _, a := range list(c, s.st.ACLs, func(*ACL) bool { return true }) { // it leaves its ACLs
		a.UserNames = slices.DeleteFunc(a.UserNames, func(n string) bool { return n == u.Name })
	}
	delete(s.st.Users, key(c, u.Name))
	delete(s.st.Tags, u.ARN)
	s.sync()
	return map[string]any{"User": v}, nil
}

// ---- ACLs ----

type aclInput struct {
	ACLName, NextToken                           string
	UserNames, UserNamesToAdd, UserNamesToRemove []string
	Tags                                         []rds.Tag
	MaxResults                                   int
}

// checkUsers checks that users exist; lock held.
func (s *Service) checkUsers(c *core.Ctx, names []string) error {
	for _, n := range names {
		if _, err := s.user(c, n); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) createACL(c *core.Ctx, in *aclInput) (any, error) {
	name := strings.ToLower(in.ACLName)
	if err := valid("ACLName", name, 40); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.acl(c, name); err == nil {
		return nil, core.Errorf(400, "ACLAlreadyExistsFault", "ACL %s already exists.", name)
	}
	if err := s.checkUsers(c, in.UserNames); err != nil {
		return nil, err
	}
	a := &ACL{Name: name, Status: "active", UserNames: slices.Compact(slices.Sorted(slices.Values(in.UserNames))), MinimumEngineVersion: "6.2",
		ARN: c.ARN("memorydb", "acl/"+name)}
	s.st.ACLs[key(c, name)] = a
	s.st.Tags[a.ARN] = rds.AddTags(nil, in.Tags)
	return map[string]any{"ACL": s.aclView(c, a)}, nil
}

func (s *Service) describeACLs(c *core.Ctx, in *aclInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := s.acls(c)
	if in.ACLName != "" {
		a, err := s.acl(c, in.ACLName)
		if err != nil {
			return nil, err
		}
		all = []*ACL{a}
	}
	all, next := page(all, func(a *ACL) string { return a.Name }, in.NextToken, in.MaxResults)
	out := make([]*ACL, len(all))
	for i, a := range all {
		out[i] = s.aclView(c, a)
	}
	return map[string]any{"ACLs": out, "NextToken": next}, nil
}

func (s *Service) updateACL(c *core.Ctx, in *aclInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.acl(c, in.ACLName)
	switch {
	case err != nil:
		return nil, err
	case a.Name == "open-access":
		return nil, invalid("The open-access ACL cannot be modified.")
	}
	if err := s.checkUsers(c, in.UserNamesToAdd); err != nil {
		return nil, err
	}
	for _, n := range in.UserNamesToRemove {
		if !slices.Contains(a.UserNames, n) {
			return nil, invalid("User %s is not in ACL %s.", n, a.Name)
		}
	}
	names := slices.DeleteFunc(append(slices.Clone(a.UserNames), in.UserNamesToAdd...), func(n string) bool { return slices.Contains(in.UserNamesToRemove, n) })
	a.UserNames = slices.Compact(slices.Sorted(slices.Values(names)))
	s.sync()
	return map[string]any{"ACL": s.aclView(c, a)}, nil
}

func (s *Service) deleteACL(c *core.Ctx, in *aclInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.acl(c, in.ACLName)
	if err != nil {
		return nil, err
	}
	v := s.aclView(c, a)
	switch {
	case a.Name == "open-access":
		return nil, invalid("The open-access ACL cannot be deleted.")
	case len(v.Clusters) > 0:
		return nil, core.Errorf(400, "InvalidACLStateFault", "ACL %s is in use by cluster %s.", a.Name, v.Clusters[0])
	}
	delete(s.st.ACLs, key(c, a.Name))
	delete(s.st.Tags, a.ARN)
	v.Status = "deleting"
	return map[string]any{"ACL": v}, nil
}

// ---- subnet groups ----

type subnetInput struct {
	SubnetGroupName, Description, NextToken string
	SubnetIds                               []string
	Tags                                    []rds.Tag
	MaxResults                              int
}

func (s *Service) createSubnetGroup(c *core.Ctx, in *subnetInput) (any, error) {
	name := strings.ToLower(in.SubnetGroupName)
	switch err := valid("SubnetGroupName", name, 255); {
	case err != nil:
		return nil, err
	case len(in.SubnetIds) == 0:
		return nil, invalid("The parameter SubnetIds must be provided.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.subnetGroup(c, name); err == nil {
		return nil, core.Errorf(400, "SubnetGroupAlreadyExistsFault", "Subnet group %s already exists.", name)
	}
	g := &SubnetGroup{Name: name, Description: in.Description, VpcId: rds.DefaultVPC, Subnets: subnets(c.Region, in.SubnetIds),
		ARN: c.ARN("memorydb", "subnetgroup/"+name), SupportedNetworkTypes: []string{"ipv4"}}
	s.st.Subnets[key(c, name)] = g
	s.st.Tags[g.ARN] = rds.AddTags(nil, in.Tags)
	return map[string]any{"SubnetGroup": rds.Clone(g)}, nil
}

func (s *Service) describeSubnetGroups(c *core.Ctx, in *subnetInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := list(c, s.st.Subnets, func(*SubnetGroup) bool { return true })
	if in.SubnetGroupName != "" {
		g, err := s.subnetGroup(c, in.SubnetGroupName)
		if err != nil {
			return nil, err
		}
		all = []*SubnetGroup{g}
	}
	all, next := page(all, func(g *SubnetGroup) string { return g.Name }, in.NextToken, in.MaxResults)
	return map[string]any{"SubnetGroups": clones(all), "NextToken": next}, nil
}

func (s *Service) updateSubnetGroup(c *core.Ctx, in *subnetInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Subnets[key(c, strings.ToLower(in.SubnetGroupName))]
	if g == nil {
		return nil, notFound("SubnetGroup", in.SubnetGroupName)
	}
	g.Description = cmp.Or(in.Description, g.Description)
	if len(in.SubnetIds) > 0 {
		g.Subnets = subnets(c.Region, in.SubnetIds)
	}
	return map[string]any{"SubnetGroup": rds.Clone(g)}, nil
}

func (s *Service) deleteSubnetGroup(c *core.Ctx, in *subnetInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Subnets[key(c, strings.ToLower(in.SubnetGroupName))]
	if g == nil {
		return nil, notFound("SubnetGroup", in.SubnetGroupName)
	}
	if len(list(c, s.st.Clusters, func(cl *cluster) bool { return cl.SubnetGroupName == g.Name })) > 0 {
		return nil, core.Errorf(400, "SubnetGroupInUseFault", "Subnet group %s is in use by a cluster.", g.Name)
	}
	delete(s.st.Subnets, key(c, g.Name))
	delete(s.st.Tags, g.ARN)
	return map[string]any{"SubnetGroup": g}, nil
}

// ---- parameter groups ----

type paramInput struct {
	ParameterGroupName, Family, Description, NextToken string
	ParameterNameValues                                []struct{ ParameterName, ParameterValue string }
	ParameterNames                                     []string
	AllParameters                                      bool
	Tags                                               []rds.Tag
	MaxResults                                         int
}

// Parameter is a parameter of a parameter group.
type Parameter struct{ Name, Value, Description, DataType, AllowedValues, MinimumEngineVersion string }

func (s *Service) createParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	name := strings.ToLower(in.ParameterGroupName)
	switch err := valid("ParameterGroupName", name, 255); {
	case err != nil:
		return nil, err
	case !strings.HasPrefix(in.Family, "memorydb_"):
		return nil, invalid("Invalid parameter group family: %s", in.Family)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.paramGroup(c, name); err == nil {
		return nil, core.Errorf(400, "ParameterGroupAlreadyExistsFault", "Parameter group %s already exists.", name)
	}
	g := &ParameterGroup{Name: name, Family: in.Family, Description: in.Description, ARN: c.ARN("memorydb", "parametergroup/"+name)}
	s.st.Params[key(c, name)] = g
	s.st.Tags[g.ARN] = rds.AddTags(nil, in.Tags)
	return map[string]any{"ParameterGroup": pgView(g)}, nil
}

func pgView(g *ParameterGroup) *ParameterGroup {
	return &ParameterGroup{Name: g.Name, Family: g.Family, Description: g.Description, ARN: g.ARN}
}

func (s *Service) describeParameterGroups(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := list(c, s.st.Params, func(*ParameterGroup) bool { return true })
	if in.ParameterGroupName != "" {
		g, err := s.paramGroup(c, in.ParameterGroupName)
		if err != nil {
			return nil, err
		}
		all = []*ParameterGroup{g}
	}
	all, next := page(all, func(g *ParameterGroup) string { return g.Name }, in.NextToken, in.MaxResults)
	out := make([]*ParameterGroup, len(all))
	for i, g := range all {
		out[i] = pgView(g)
	}
	return map[string]any{"ParameterGroups": out, "NextToken": next}, nil
}

// updateParameterGroup sets parameters, or (Reset) drops them back to the engine defaults.
// ponytail: values do not reach the server.
func (s *Service) updateParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.ParameterGroupName)
	switch {
	case err != nil:
		return nil, err
	case s.st.Params[key(c, g.Name)] == nil:
		return nil, core.Errorf(400, "InvalidParameterGroupStateFault", "Default parameter groups cannot be modified: %s", g.Name)
	}
	if g.Params == nil || in.AllParameters {
		g.Params = map[string]string{}
	}
	for _, p := range in.ParameterNameValues {
		g.Params[p.ParameterName] = p.ParameterValue
	}
	for _, n := range in.ParameterNames {
		delete(g.Params, n)
	}
	return map[string]any{"ParameterGroup": pgView(g)}, nil
}

func (s *Service) deleteParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.ParameterGroupName)
	switch {
	case err != nil:
		return nil, err
	case s.st.Params[key(c, g.Name)] == nil:
		return nil, core.Errorf(400, "InvalidParameterGroupStateFault", "Default parameter groups cannot be deleted: %s", g.Name)
	case len(list(c, s.st.Clusters, func(cl *cluster) bool { return cl.ParameterGroupName == g.Name })) > 0:
		return nil, core.Errorf(400, "InvalidParameterGroupStateFault", "Parameter group %s is in use by a cluster.", g.Name)
	}
	delete(s.st.Params, key(c, g.Name))
	delete(s.st.Tags, g.ARN)
	return map[string]any{"ParameterGroup": pgView(g)}, nil
}

// describeParameters lists the parameters set by users.
// ponytail: engine defaults are not listed.
func (s *Service) describeParameters(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.ParameterGroupName)
	if err != nil {
		return nil, err
	}
	var all []*Parameter
	for n, v := range g.Params {
		all = append(all, &Parameter{n, v, n, "string", "", "6.2"})
	}
	all, next := page(all, func(p *Parameter) string { return p.Name }, in.NextToken, in.MaxResults)
	return map[string]any{"Parameters": all, "NextToken": next}, nil
}

// ---- snapshots ----

type snapshotInput struct {
	ClusterName, SnapshotName, KmsKeyId, Source, NextToken string
	Tags                                                   []rds.Tag
	MaxResults                                             int
}

// addSnapshot records a snapshot of a cluster; lock held.
func (s *Service) addSnapshot(c *core.Ctx, cl *cluster, name, source string, tags []rds.Tag) (*Snapshot, error) {
	name = strings.ToLower(name)
	if err := valid("SnapshotName", name, 255); err != nil {
		return nil, err
	}
	if s.st.Snapshots[key(c, name)] != nil {
		return nil, core.Errorf(400, "SnapshotAlreadyExistsFault", "Snapshot %s already exists.", name)
	}
	sn := &Snapshot{Name: name, Status: "available", Source: source, KmsKeyId: cl.KmsKeyId, ARN: c.ARN("memorydb", "snapshot/"+name), DataTiering: cl.DataTiering}
	cc := &sn.ClusterConfiguration
	cc.Name, cc.Description, cc.NodeType, cc.Engine, cc.EngineVersion = cl.Name, cl.Description, cl.NodeType, cl.Engine, cl.EngineVersion
	cc.MaintenanceWindow, cc.TopicArn, cc.Port, cc.ParameterGroupName = cl.MaintenanceWindow, cl.SnsTopicArn, cmp.Or(cl.Pogi.HostPort, 6379), cl.ParameterGroupName
	cc.SubnetGroupName, cc.VpcId, cc.SnapshotRetentionLimit, cc.SnapshotWindow, cc.NumShards = cl.SubnetGroupName, rds.DefaultVPC, cl.SnapshotRetentionLimit, cl.SnapshotWindow, cl.NumberOfShards
	s.st.Snapshots[key(c, name)] = sn
	s.st.Tags[sn.ARN] = rds.AddTags(nil, tags)
	return sn, nil
}

func (s *Service) createSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterName)
	if err != nil {
		return nil, err
	}
	sn, err := s.addSnapshot(c, cl, in.SnapshotName, "manual", in.Tags)
	if err != nil {
		return nil, err
	}
	sn.KmsKeyId = cmp.Or(in.KmsKeyId, sn.KmsKeyId)
	return map[string]any{"Snapshot": rds.Clone(sn)}, nil
}

func (s *Service) describeSnapshots(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.SnapshotName != "" && s.st.Snapshots[key(c, strings.ToLower(in.SnapshotName))] == nil {
		return nil, notFound("Snapshot", in.SnapshotName)
	}
	all := list(c, s.st.Snapshots, func(sn *Snapshot) bool {
		return (in.SnapshotName == "" || sn.Name == strings.ToLower(in.SnapshotName)) && (in.Source == "" || sn.Source == in.Source) &&
			(in.ClusterName == "" || sn.ClusterConfiguration.Name == strings.ToLower(in.ClusterName))
	})
	all, next := page(all, func(sn *Snapshot) string { return sn.Name }, in.NextToken, in.MaxResults)
	return map[string]any{"Snapshots": clones(all), "NextToken": next}, nil
}

func (s *Service) deleteSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn := s.st.Snapshots[key(c, strings.ToLower(in.SnapshotName))]
	if sn == nil {
		return nil, notFound("Snapshot", in.SnapshotName)
	}
	delete(s.st.Snapshots, key(c, sn.Name))
	delete(s.st.Tags, sn.ARN)
	v := *sn
	v.Status = "deleting"
	return map[string]any{"Snapshot": &v}, nil
}

// ---- engine versions and tags ----

type versionsInput struct {
	Engine, EngineVersion, ParameterGroupFamily, NextToken string
	DefaultOnly                                            bool
	MaxResults                                             int
}

// EngineVersion is an engine version pogi offers.
type EngineVersion struct{ Engine, EngineVersion, EnginePatchVersion, ParameterGroupFamily string }

func (s *Service) describeEngineVersions(c *core.Ctx, in *versionsInput) (any, error) {
	var all []*EngineVersion
	for _, e := range core.SortedKeys(engines) {
		for _, v := range engines[e] {
			if (in.Engine == "" || in.Engine == e) && (in.EngineVersion == "" || v == in.EngineVersion) &&
				(in.ParameterGroupFamily == "" || family(e, v) == in.ParameterGroupFamily) {
				all = append(all, &EngineVersion{e, v, v + ".0", family(e, v)})
				if in.DefaultOnly {
					break
				}
			}
		}
	}
	all, next := page(all, func(v *EngineVersion) string { return v.Engine + "/" + v.EngineVersion }, in.NextToken, in.MaxResults)
	return map[string]any{"EngineVersions": all, "NextToken": next}, nil
}

type tagsInput struct {
	ResourceArn string
	Tags        []rds.Tag
	TagKeys     []string
}

// tags finds the tags of the resource an ARN names; lock held.
func (s *Service) tags(arn string) ([]rds.Tag, error) {
	svc, region, account, res, ok := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, "/")
	if !ok || svc != "memorydb" || name == "" {
		return nil, core.Errorf(400, "InvalidARNFault", "The ARN %s is not valid.", arn)
	}
	c := &core.Ctx{Account: account, Region: region}
	var err error
	switch kind {
	case "cluster":
		_, err = s.cluster(c, name)
	case "user":
		_, err = s.user(c, name)
	case "acl":
		_, err = s.acl(c, name)
	case "subnetgroup":
		_, err = s.subnetGroup(c, name)
	case "parametergroup":
		_, err = s.paramGroup(c, name)
	case "snapshot":
		if s.st.Snapshots[key(c, name)] == nil {
			err = notFound("Snapshot", name)
		}
	default:
		err = core.Errorf(400, "InvalidARNFault", "The ARN %s is not valid.", arn)
	}
	return s.st.Tags[arn], err
}

func (s *Service) tagResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	s.st.Tags[in.ResourceArn] = rds.AddTags(t, in.Tags)
	return map[string]any{"TagList": slices.Clone(s.st.Tags[in.ResourceArn])}, nil
}

func (s *Service) untagResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		if !slices.ContainsFunc(t, func(tag rds.Tag) bool { return tag.Key == k }) {
			return nil, core.Errorf(404, "TagNotFoundFault", "The tag key %s is not on resource %s.", k, in.ResourceArn)
		}
	}
	s.st.Tags[in.ResourceArn] = rds.RemoveTags(t, in.TagKeys)
	return map[string]any{"TagList": slices.Clone(s.st.Tags[in.ResourceArn])}, nil
}

func (s *Service) listTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"TagList": slices.Clone(t)}, nil
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name; those of
// dropped clusters go.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	keep := containers(st)
	docker := len(keep) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	for _, cl := range st.Clusters {
		s.box.Resume(&cl.Pogi, cl.spec(), docker)
	}
	if docker {
		clear(s.applied) // a resumed server lost its users
		s.sync()
	}
	s.mu.Unlock()
	for n := range containers(old) {
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

func containers(st state) map[string]bool {
	out := map[string]bool{}
	for _, cl := range st.Clusters {
		if cl.Pogi.Container != "" {
			out[cl.Pogi.Container] = true
		}
	}
	return out
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close removes the containers, except those persisted state resumes next run.
func (s *Service) Close() error {
	s.mu.Lock()
	keep := containers(s.st)
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

// Resources lists clusters, users and ACLs for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	cls := core.Table{Name: "Clusters", Columns: []string{"Name", "Engine", "Status", "Endpoint", "ACL", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		c, cl := ctxOf(k), s.st.Clusters[k]
		ep := ""
		if e := cl.Pogi.Addr(); e != nil {
			ep = fmt.Sprintf("%s:%d", e.Address, e.Port)
		}
		cls.Rows = append(cls.Rows, []string{cl.Name, cl.Engine + " " + cl.EngineVersion, rds.StatusCell(cl.Pogi.State, &cl.Pogi), ep, cl.ACLName, c.Region, c.Account})
	}
	users := core.Table{Name: "Users", Columns: []string{"Name", "Authentication", "Access", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Users) {
		c, u := ctxOf(k), s.st.Users[k]
		users.Rows = append(users.Rows, []string{u.Name, u.Authentication.Type, u.AccessString, c.Region, c.Account})
	}
	acls := core.Table{Name: "ACLs", Columns: []string{"Name", "Users", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.ACLs) {
		c, a := ctxOf(k), s.st.ACLs[k]
		acls.Rows = append(acls.Rows, []string{a.Name, strings.Join(a.UserNames, " "), c.Region, c.Account})
	}
	return []core.Table{cls, users, acls}
}

// Inspect returns a cluster with pogi's side: container, state, pogiNote.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, cl := range s.st.Clusters {
		if table == "Clusters" && cl.Name == id {
			v := *cl
			v.Cluster = *s.view(ctxOf(k), cl, true)
			return &v
		}
	}
	return nil
}
