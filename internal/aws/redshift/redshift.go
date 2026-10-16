// Package redshift emulates Amazon Redshift over the Query protocol. Clusters
// run as PostgreSQL containers on localhost (Redshift speaks PostgreSQL's wire
// protocol and most of its SQL); GetClusterCredentials creates real database
// logins. Parameter, subnet and security groups, snapshots and logging are
// metadata. Without Docker the API still answers and Inspect says that
// nothing runs.
package redshift

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Redshift emulator.
type Service struct {
	hub *core.Hub
	h   http.Handler
	box *rds.Box
	mu  sync.Mutex
	st  state
}

type state struct {
	Clusters  map[string]*Cluster       `json:"clusters"` // account/region/identifier
	Params    map[string]*ParamGroup    `json:"parameterGroups"`
	Subnets   map[string]*SubnetGroup   `json:"subnetGroups"`
	SecGroups map[string]*SecurityGroup `json:"securityGroups"`
	Snapshots map[string]*Snapshot      `json:"snapshots"`
	Schedules map[string]*Schedule      `json:"snapshotSchedules,omitempty"`
	Grants    map[string]*CopyGrant     `json:"snapshotCopyGrants,omitempty"`
}

func newState() state {
	return state{Clusters: map[string]*Cluster{}, Params: map[string]*ParamGroup{}, Subnets: map[string]*SubnetGroup{},
		SecGroups: map[string]*SecurityGroup{}, Snapshots: map[string]*Snapshot{}, Schedules: map[string]*Schedule{}, Grants: map[string]*CopyGrant{}}
}

func (st *state) containers() map[string]bool {
	out := map[string]bool{}
	for _, cl := range st.Clusters {
		if cl.Pogi.Container != "" {
			out[cl.Pogi.Container] = true
		}
	}
	return out
}

// items names the list items of Redshift responses (see rds.Lists).
var items = map[string]string{
	"Clusters": "Cluster", "ClusterParameterGroups": "ClusterParameterGroup", "ParameterGroups": "ClusterParameterGroup",
	"Parameters": "Parameter", "ClusterSecurityGroups": "ClusterSecurityGroup", "ClusterSubnetGroups": "ClusterSubnetGroup",
	"Subnets": "Subnet", "VpcSecurityGroups": "VpcSecurityGroup", "IamRoles": "ClusterIamRole", "Tags": "Tag",
	"TaggedResources": "TaggedResource", "Snapshots": "Snapshot", "ScheduleDefinitions": "ScheduleDefinition",
	"SnapshotSchedules": "SnapshotSchedule", "AssociatedClusters": "ClusterAssociatedToSchedule", "SnapshotCopyGrants": "SnapshotCopyGrant",
}

// New creates the Redshift service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.box = rds.NewBox(hub, "redshift", "hardware-failure", &s.mu, s.owns)
	s.h = rds.Lists(&core.RPC{XMLNS: "http://redshift.amazonaws.com/doc/2012-12-01/", Ops: map[string]core.Handler{
		"CreateCluster":                     core.H(s.createCluster),
		"DescribeClusters":                  core.H(s.describeClusters),
		"ModifyCluster":                     core.H(s.modifyCluster),
		"DeleteCluster":                     core.H(s.deleteCluster),
		"PauseCluster":                      core.H(s.pauseCluster),
		"ResumeCluster":                     core.H(s.resumeCluster),
		"RebootCluster":                     core.H(s.rebootCluster),
		"ResizeCluster":                     core.H(s.resizeCluster),
		"GetClusterCredentials":             core.H(s.getClusterCredentials),
		"GetClusterCredentialsWithIAM":      core.H(s.getClusterCredentials),
		"DescribeLoggingStatus":             core.H(s.logging),
		"EnableLogging":                     core.H(s.logging),
		"DisableLogging":                    core.H(s.logging),
		"CreateClusterParameterGroup":       core.H(s.createParamGroup),
		"DescribeClusterParameterGroups":    core.H(s.describeParamGroups),
		"ModifyClusterParameterGroup":       core.H(s.modifyParamGroup),
		"ResetClusterParameterGroup":        core.H(s.modifyParamGroup),
		"DeleteClusterParameterGroup":       core.H(s.deleteParamGroup),
		"DescribeClusterParameters":         core.H(s.describeParameters),
		"CreateClusterSubnetGroup":          core.H(s.createSubnetGroup),
		"DescribeClusterSubnetGroups":       core.H(s.describeSubnetGroups),
		"ModifyClusterSubnetGroup":          core.H(s.modifySubnetGroup),
		"DeleteClusterSubnetGroup":          core.H(s.deleteSubnetGroup),
		"CreateClusterSecurityGroup":        core.H(s.createSecGroup),
		"DescribeClusterSecurityGroups":     core.H(s.describeSecGroups),
		"DeleteClusterSecurityGroup":        core.H(s.deleteSecGroup),
		"CreateClusterSnapshot":             core.H(s.createSnapshot),
		"DescribeClusterSnapshots":          core.H(s.describeSnapshots),
		"DeleteClusterSnapshot":             core.H(s.deleteSnapshot),
		"CreateTags":                        core.H(s.createTags),
		"DeleteTags":                        core.H(s.deleteTags),
		"DescribeTags":                      core.H(s.describeTags),
		"EnableSnapshotCopy":                core.H(s.enableSnapshotCopy),
		"DisableSnapshotCopy":               core.H(s.disableSnapshotCopy),
		"ModifySnapshotCopyRetentionPeriod": core.H(s.modifyCopyRetention),
		"CreateSnapshotCopyGrant":           core.H(s.createCopyGrant),
		"DescribeSnapshotCopyGrants":        core.H(s.describeCopyGrants),
		"DeleteSnapshotCopyGrant":           core.H(s.deleteCopyGrant),
		"CreateSnapshotSchedule":            core.H(s.createSchedule),
		"ModifySnapshotSchedule":            core.H(s.modifySchedule),
		"DescribeSnapshotSchedules":         core.H(s.describeSchedules),
		"DeleteSnapshotSchedule":            core.H(s.deleteSchedule),
		"ModifyClusterSnapshotSchedule":     core.H(s.modifyClusterSchedule),
	}}, items)
	return s
}

func (s *Service) Info() core.Info {
	// The smithy service name is a JSON target for pogi's own in-process calls (hub.Call).
	return core.Info{Name: "redshift", Cloud: core.AWS, Title: "Redshift", Signing: []string{"redshift"},
		Targets: []string{"RedshiftServiceVersion20121201"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.h.ServeHTTP(w, r) }

// ---- API shapes (stored as is; Pogi and query:"-" fields are pogi's) ----

// Cluster is a provisioned cluster; it owns its container.
type Cluster struct {
	ClusterIdentifier                string
	NodeType                         string
	ClusterStatus                    string `json:",omitempty"` // in views only, from Pogi.State
	ClusterAvailabilityStatus        string `json:",omitempty"` // in views only, from Pogi.State
	MasterUsername                   string
	DBName                           string
	Endpoint                         *rds.Endpoint `json:",omitempty"`
	ClusterCreateTime                time.Time
	AutomatedSnapshotRetentionPeriod int
	ManualSnapshotRetentionPeriod    int
	ClusterSecurityGroups            []Membership `json:",omitempty"`
	VpcSecurityGroups                []rds.VpcSecurityGroup
	ClusterParameterGroups           []ParameterGroupStatus
	ClusterSubnetGroupName           string
	VpcId                            string
	AvailabilityZone                 string
	PreferredMaintenanceWindow       string
	ClusterVersion                   string
	AllowVersionUpgrade              bool
	NumberOfNodes                    int
	PubliclyAccessible               bool
	Encrypted                        bool
	KmsKeyId                         string `json:",omitempty"`
	EnhancedVpcRouting               bool
	IamRoles                         []IamRole `json:",omitempty"`
	DefaultIamRoleArn                string    `json:",omitempty"`
	MaintenanceTrackName             string
	ClusterNodes                     []ClusterNode `json:",omitempty"` // in views only
	ClusterNamespaceArn              string
	AvailabilityZoneRelocationStatus string
	MultiAZ                          string
	Tags                             []rds.Tag
	ClusterSnapshotCopyStatus        *CopyStatus `json:",omitempty"`
	Logging                          *Logging    `json:",omitempty" query:"-"`
	ScheduleIdentifier               string      `json:"scheduleIdentifier,omitempty" query:"-"` // snapshot schedule
	Pogi                             rds.Server  `json:"pogi" query:"-"`
}

// Membership is a cluster security group of a cluster.
type Membership struct{ ClusterSecurityGroupName, Status string }

// ParameterGroupStatus is the parameter group of a cluster.
type ParameterGroupStatus struct{ ParameterGroupName, ParameterApplyStatus string }

// IamRole is an IAM role of a cluster.
type IamRole struct{ IamRoleArn, ApplyStatus string }

// ClusterNode is a node of a cluster; one container serves them all.
type ClusterNode struct{ NodeRole, PrivateIPAddress, PublicIPAddress string }

// Logging is a cluster's audit logging setup; pogi writes no logs.
type Logging struct {
	LoggingEnabled                              bool
	BucketName, S3KeyPrefix, LogDestinationType string
	LogExports                                  []string
}

// ParamGroup is a cluster parameter group.
type ParamGroup struct {
	ParameterGroupName   string
	ParameterGroupFamily string
	Description          string
	Tags                 []rds.Tag
	Params               map[string]string `json:",omitempty" query:"-"` // set by users
}

// Parameter is a parameter of a parameter group.
type Parameter struct {
	ParameterName, ParameterValue, Description, Source, DataType, ApplyType string
	IsModifiable                                                            bool
}

// SubnetGroup is a cluster subnet group.
type SubnetGroup struct {
	ClusterSubnetGroupName string
	Description            string
	VpcId                  string
	SubnetGroupStatus      string
	Subnets                []rds.Subnet
	Tags                   []rds.Tag
}

// SecurityGroup is a cluster security group, without ingress rules.
type SecurityGroup struct {
	ClusterSecurityGroupName string
	Description              string
	Tags                     []rds.Tag
}

// Snapshot is a manual snapshot.
// ponytail: metadata only, like RDS's: restoring needs a dump of the container.
type Snapshot struct {
	SnapshotIdentifier            string
	ClusterIdentifier             string
	SnapshotCreateTime            time.Time
	Status                        string
	Port                          int
	AvailabilityZone              string
	ClusterCreateTime             time.Time
	MasterUsername                string
	ClusterVersion                string
	SnapshotType                  string
	NodeType                      string
	NumberOfNodes                 int
	DBName                        string
	VpcId                         string
	Encrypted                     bool
	KmsKeyId                      string `json:",omitempty"`
	EnhancedVpcRouting            bool
	OwnerAccount                  string
	ManualSnapshotRetentionPeriod int
	SnapshotArn                   string
	Tags                          []rds.Tag
}

// ---- helpers ----

const defaultPG = "default.redshift-1.0"

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValue", format, a...)
}
func combo(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterCombination", format, a...)
}
func badState(format string, a ...any) error {
	return core.Errorf(400, "InvalidClusterState", format, a...)
}

func key(c *core.Ctx, id string) string {
	return c.Account + "/" + c.Region + "/" + strings.ToLower(id)
}
func mine(c *core.Ctx, k string) bool { return strings.HasPrefix(k, c.Account+"/"+c.Region+"/") }

// ctxOf rebuilds the account and region of a state key.
func ctxOf(k string) *core.Ctx {
	p := strings.SplitN(k, "/", 3)
	return &core.Ctx{Account: p[0], Region: p[1]}
}

func each[T, U any](in []T, f func(T) U) []U {
	out := make([]U, len(in))
	for i, v := range in {
		out[i] = f(v)
	}
	return out
}

func set[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

var nodeTypeRe = regexp.MustCompile(`^(dc2|ds2|ra3)\.[a-z0-9]+$`)

func checkNodes(nodeType, clusterType string, nodes int) error {
	switch {
	case !nodeTypeRe.MatchString(nodeType):
		return invalid("Invalid node type: %s", nodeType)
	case clusterType != "" && clusterType != "single-node" && clusterType != "multi-node":
		return invalid("Invalid cluster type. Valid values are multi-node and single-node.")
	case clusterType == "multi-node" && nodes < 2:
		return combo("Number of nodes for cluster type multi-node must be greater than or equal to 2")
	case clusterType == "single-node" && nodes != 1:
		return combo("Number of nodes for cluster type single-node must be 1")
	case nodes < 1 || nodes > 128:
		return invalid("Number of nodes must be between 1 and 128.")
	}
	return nil
}

func checkPassword(pw string) error {
	if !ValidPassword(pw) {
		return invalid("The parameter MasterUserPassword is not a valid password. It must be 8 to 64 printable ASCII characters " +
			"with an uppercase letter, a lowercase letter and a number, and none of ' \" \\ / @ or space.")
	}
	return nil
}

func sgs(ids []string) []rds.VpcSecurityGroup {
	return each(ids, func(id string) rds.VpcSecurityGroup {
		return rds.VpcSecurityGroup{VpcSecurityGroupId: id, Status: "active"}
	})
}

func roles(arns []string) []IamRole {
	return each(arns, func(a string) IamRole { return IamRole{a, "in-sync"} })
}

func onOff(on bool, yes, no string) string {
	if on {
		return yes
	}
	return no
}

// owns reports whether srv still belongs to a cluster; lock held.
func (s *Service) owns(srv *rds.Server) bool {
	for _, cl := range s.st.Clusters {
		if &cl.Pogi == srv {
			return true
		}
	}
	return false
}

func (cl *Cluster) spec() rds.Spec {
	return Spec(cl.MasterUsername, cl.Pogi.Password, cl.DBName, cl.Pogi.HostPort)
}

// view is the API shape of a cluster.
func view(cl *Cluster) *Cluster {
	v := rds.Clone(cl)
	v.ClusterStatus, v.Endpoint = status(cl.Pogi.State), cl.Pogi.Addr()
	v.ClusterAvailabilityStatus = cmp.Or(map[string]string{"available": "Available", "hardware-failure": "Failed"}[cl.Pogi.State], "Unavailable")
	v.ClusterNodes = []ClusterNode{{NodeRole: "SHARED", PrivateIPAddress: "10.0.0.10"}}
	if cl.NumberOfNodes > 1 {
		v.ClusterNodes[0].NodeRole = "LEADER"
		for i := range cl.NumberOfNodes {
			v.ClusterNodes = append(v.ClusterNodes, ClusterNode{NodeRole: "COMPUTE-" + strconv.Itoa(i), PrivateIPAddress: "10.0.0." + strconv.Itoa(11+i)})
		}
	}
	return v
}

// ---- lookups (lock held) ----

func (s *Service) cluster(c *core.Ctx, id string) (*Cluster, error) {
	if cl := s.st.Clusters[key(c, id)]; cl != nil {
		return cl, nil
	}
	return nil, core.Errorf(404, "ClusterNotFound", "Cluster %s not found.", id)
}

// paramGroup finds a parameter group; default.redshift-1.0 always exists.
func (s *Service) paramGroup(c *core.Ctx, name string) (*ParamGroup, error) {
	if g := s.st.Params[key(c, name)]; g != nil {
		return g, nil
	}
	if strings.ToLower(name) == defaultPG {
		return &ParamGroup{ParameterGroupName: defaultPG, ParameterGroupFamily: "redshift-1.0", Description: "Default parameter group for redshift-1.0"}, nil
	}
	return nil, core.Errorf(404, "ClusterParameterGroupNotFound", "Parameter group %s not found.", name)
}

func (s *Service) subnetGroup(c *core.Ctx, name string) (*SubnetGroup, error) {
	if g := s.st.Subnets[key(c, name)]; g != nil {
		return g, nil
	}
	return nil, core.Errorf(400, "ClusterSubnetGroupNotFoundFault", "ClusterSubnetGroup %s not found.", name)
}

func (s *Service) secGroup(c *core.Ctx, name string) (*SecurityGroup, error) {
	if g := s.st.SecGroups[key(c, name)]; g != nil {
		return g, nil
	}
	return nil, core.Errorf(404, "ClusterSecurityGroupNotFound", "Cluster security group %s not found.", name)
}

func (s *Service) snapshot(c *core.Ctx, id string) (*Snapshot, error) {
	if sn := s.st.Snapshots[key(c, id)]; sn != nil {
		return sn, nil
	}
	return nil, core.Errorf(404, "ClusterSnapshotNotFound", "Snapshot %s not found.", id)
}

// ---- clusters ----

type createClusterInput struct {
	ClusterIdentifier, NodeType, ClusterType, MasterUsername, MasterUserPassword, DBName            string
	ClusterParameterGroupName, ClusterSubnetGroupName, AvailabilityZone, PreferredMaintenanceWindow string
	ClusterVersion, KmsKeyId, MaintenanceTrackName, DefaultIamRoleArn                               string
	NumberOfNodes, Port                                                                             int
	AutomatedSnapshotRetentionPeriod, ManualSnapshotRetentionPeriod                                 *int
	AllowVersionUpgrade                                                                             *bool
	PubliclyAccessible, Encrypted, EnhancedVpcRouting, AvailabilityZoneRelocation, MultiAZ          bool
	ManageMasterPassword                                                                            bool
	ClusterSecurityGroups, VpcSecurityGroupIds, IamRoles                                            []string
	Tags                                                                                            []rds.Tag
}

func (s *Service) createCluster(c *core.Ctx, in *createClusterInput) (any, error) {
	id, nodes, db := strings.ToLower(in.ClusterIdentifier), cmp.Or(in.NumberOfNodes, 1), cmp.Or(in.DBName, "dev")
	if err := rds.ValidID("ClusterIdentifier", id, 63); err != nil {
		return nil, err
	}
	if err := checkNodes(in.NodeType, in.ClusterType, nodes); err != nil {
		return nil, err
	}
	switch {
	case in.ManageMasterPassword: // ponytail: no Secrets Manager secret yet; add it as RDS does when asked
		return nil, core.Errorf(400, "UnsupportedOperation", "pogi does not manage master passwords yet; pass MasterUserPassword.")
	case !ValidUser(in.MasterUsername):
		return nil, invalid("MasterUsername must be 1 to 128 lowercase letters, digits or _+.@- starting with a letter, and not PUBLIC.")
	case !ValidDB(db):
		return nil, invalid("DBName must be 1 to 64 lowercase letters, digits or underscores.")
	}
	if err := checkPassword(in.MasterUserPassword); err != nil {
		return nil, err
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Clusters[key(c, id)] != nil {
		return nil, core.Errorf(400, "ClusterAlreadyExists", "Cluster already exists")
	}
	pg, err := s.paramGroup(c, cmp.Or(in.ClusterParameterGroupName, defaultPG))
	if err != nil {
		return nil, err
	}
	if in.ClusterSubnetGroupName != "" {
		if _, err := s.subnetGroup(c, in.ClusterSubnetGroupName); err != nil {
			return nil, err
		}
	}
	var csgs []Membership
	for _, name := range in.ClusterSecurityGroups {
		g, err := s.secGroup(c, name)
		if err != nil {
			return nil, err
		}
		csgs = append(csgs, Membership{g.ClusterSecurityGroupName, "active"})
	}
	cl := &Cluster{
		ClusterIdentifier: id, NodeType: in.NodeType, MasterUsername: in.MasterUsername, DBName: db, ClusterCreateTime: time.Now().UTC(),
		AutomatedSnapshotRetentionPeriod: 1, ManualSnapshotRetentionPeriod: -1, ClusterSecurityGroups: csgs, VpcSecurityGroups: sgs(in.VpcSecurityGroupIds),
		ClusterParameterGroups: []ParameterGroupStatus{{pg.ParameterGroupName, "in-sync"}}, ClusterSubnetGroupName: cmp.Or(strings.ToLower(in.ClusterSubnetGroupName), "default"),
		VpcId: rds.DefaultVPC, AvailabilityZone: cmp.Or(in.AvailabilityZone, c.Region+"a"),
		PreferredMaintenanceWindow: strings.ToLower(cmp.Or(in.PreferredMaintenanceWindow, "sun:05:00-sun:05:30")),
		ClusterVersion:             cmp.Or(in.ClusterVersion, "1.0"), AllowVersionUpgrade: in.AllowVersionUpgrade == nil || *in.AllowVersionUpgrade,
		NumberOfNodes: nodes, PubliclyAccessible: in.PubliclyAccessible, Encrypted: in.Encrypted, EnhancedVpcRouting: in.EnhancedVpcRouting,
		IamRoles: roles(in.IamRoles), DefaultIamRoleArn: in.DefaultIamRoleArn, MaintenanceTrackName: cmp.Or(in.MaintenanceTrackName, "current"),
		ClusterNamespaceArn:              c.ARN("redshift", "namespace:"+core.UUID()),
		AvailabilityZoneRelocationStatus: onOff(in.AvailabilityZoneRelocation, "enabled", "disabled"), MultiAZ: onOff(in.MultiAZ, "Enabled", "Disabled"),
		Tags: rds.AddTags(nil, in.Tags),
	}
	set(&cl.AutomatedSnapshotRetentionPeriod, in.AutomatedSnapshotRetentionPeriod)
	set(&cl.ManualSnapshotRetentionPeriod, in.ManualSnapshotRetentionPeriod)
	if cl.Encrypted {
		cl.KmsKeyId = in.KmsKeyId
	}
	cl.Pogi.Password = in.MasterUserPassword
	s.st.Clusters[key(c, id)] = cl
	s.box.Serve(&cl.Pogi, c.Account, c.Region, id, cmp.Or(in.Port, Port), Port, cl.spec, docker)
	return map[string]any{"Cluster": view(cl)}, nil
}

type describeInput struct {
	ClusterIdentifier, ParameterGroupName, ClusterSubnetGroupName, ClusterSecurityGroupName string
	SnapshotIdentifier, SnapshotType, Source, Marker                                        string
	MaxRecords                                                                              int
}

func (s *Service) describeClusters(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Cluster
	if in.ClusterIdentifier != "" {
		cl, err := s.cluster(c, in.ClusterIdentifier)
		if err != nil {
			return nil, err
		}
		list = append(list, cl)
	}
	for k, cl := range s.st.Clusters {
		if in.ClusterIdentifier == "" && mine(c, k) {
			list = append(list, cl)
		}
	}
	list, marker, err := page(list, func(cl *Cluster) string { return cl.ClusterIdentifier }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Clusters": each(list, view), "Marker": marker}, nil
}

type modifyClusterInput struct {
	ClusterIdentifier, NewClusterIdentifier, NodeType, ClusterType, MasterUserPassword string
	ClusterParameterGroupName, PreferredMaintenanceWindow, ClusterVersion, KmsKeyId    string
	MaintenanceTrackName, AvailabilityZone                                             string
	NumberOfNodes, Port                                                                int
	AutomatedSnapshotRetentionPeriod, ManualSnapshotRetentionPeriod                    *int
	AllowVersionUpgrade, PubliclyAccessible, Encrypted, EnhancedVpcRouting             *bool
	AvailabilityZoneRelocation, MultiAZ                                                *bool
	ClusterSecurityGroups, VpcSecurityGroupIds                                         []string
}

func (s *Service) modifyCluster(c *core.Ctx, in *modifyClusterInput) (any, error) {
	if in.MasterUserPassword != "" {
		if err := checkPassword(in.MasterUserPassword); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	nid := strings.ToLower(in.NewClusterIdentifier)
	if nid != "" && nid != cl.ClusterIdentifier {
		if err := rds.ValidID("NewClusterIdentifier", nid, 63); err != nil {
			return nil, err
		}
		if s.st.Clusters[key(c, nid)] != nil {
			return nil, core.Errorf(400, "ClusterAlreadyExists", "Cluster already exists")
		}
	}
	if in.NodeType != "" || in.NumberOfNodes != 0 || in.ClusterType != "" {
		if err := checkNodes(cmp.Or(in.NodeType, cl.NodeType), in.ClusterType, cmp.Or(in.NumberOfNodes, cl.NumberOfNodes)); err != nil {
			return nil, err
		}
	}
	if in.MasterUserPassword != "" && cl.Pogi.Container != "" && cl.Pogi.State != "available" {
		return nil, badState("The cluster %s is %s; its master password can only change while it is available.", cl.ClusterIdentifier, status(cl.Pogi.State))
	}
	if in.ClusterParameterGroupName != "" {
		if _, err := s.paramGroup(c, in.ClusterParameterGroupName); err != nil {
			return nil, err
		}
	}
	var csgs []Membership
	for _, name := range in.ClusterSecurityGroups {
		g, err := s.secGroup(c, name)
		if err != nil {
			return nil, err
		}
		csgs = append(csgs, Membership{g.ClusterSecurityGroupName, "active"})
	}
	if nid != "" && nid != cl.ClusterIdentifier { // the container keeps its name
		delete(s.st.Clusters, key(c, cl.ClusterIdentifier))
		cl.ClusterIdentifier = nid
		s.st.Clusters[key(c, nid)] = cl
	}
	// ponytail: node type and count are metadata, one container serves any size
	cl.NodeType, cl.NumberOfNodes = cmp.Or(in.NodeType, cl.NodeType), cmp.Or(in.NumberOfNodes, cl.NumberOfNodes)
	if in.ClusterType == "single-node" {
		cl.NumberOfNodes = 1
	}
	if in.ClusterParameterGroupName != "" {
		cl.ClusterParameterGroups = []ParameterGroupStatus{{strings.ToLower(in.ClusterParameterGroupName), "in-sync"}}
	}
	if in.ClusterSecurityGroups != nil {
		cl.ClusterSecurityGroups = csgs
	}
	if in.VpcSecurityGroupIds != nil {
		cl.VpcSecurityGroups = sgs(in.VpcSecurityGroupIds)
	}
	cl.PreferredMaintenanceWindow = strings.ToLower(cmp.Or(in.PreferredMaintenanceWindow, cl.PreferredMaintenanceWindow))
	cl.ClusterVersion, cl.KmsKeyId = cmp.Or(in.ClusterVersion, cl.ClusterVersion), cmp.Or(in.KmsKeyId, cl.KmsKeyId)
	cl.MaintenanceTrackName, cl.AvailabilityZone = cmp.Or(in.MaintenanceTrackName, cl.MaintenanceTrackName), cmp.Or(in.AvailabilityZone, cl.AvailabilityZone)
	set(&cl.AutomatedSnapshotRetentionPeriod, in.AutomatedSnapshotRetentionPeriod)
	set(&cl.ManualSnapshotRetentionPeriod, in.ManualSnapshotRetentionPeriod)
	set(&cl.AllowVersionUpgrade, in.AllowVersionUpgrade)
	set(&cl.PubliclyAccessible, in.PubliclyAccessible)
	set(&cl.Encrypted, in.Encrypted)
	set(&cl.EnhancedVpcRouting, in.EnhancedVpcRouting)
	if in.AvailabilityZoneRelocation != nil {
		cl.AvailabilityZoneRelocationStatus = onOff(*in.AvailabilityZoneRelocation, "enabled", "disabled")
	}
	if in.MultiAZ != nil {
		cl.MultiAZ = onOff(*in.MultiAZ, "Enabled", "Disabled")
	}
	if in.Port > 0 && cl.Pogi.Container == "" { // a running container keeps its published port
		cl.Pogi.HostPort = in.Port
	}
	if in.MasterUserPassword != "" {
		cl.Pogi.Password = in.MasterUserPassword
		if cl.Pogi.Container != "" {
			s.box.Exec(&cl.Pogi, Alter(cl.MasterUsername, in.MasterUserPassword)...)
		}
	}
	return map[string]any{"Cluster": view(cl)}, nil
}

type deleteClusterInput struct {
	ClusterIdentifier, FinalClusterSnapshotIdentifier string
	SkipFinalClusterSnapshot                          bool
	FinalClusterSnapshotRetentionPeriod               *int
}

func (s *Service) deleteCluster(c *core.Ctx, in *deleteClusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	switch {
	case err != nil:
		return nil, err
	case in.SkipFinalClusterSnapshot:
	case in.FinalClusterSnapshotIdentifier == "":
		return nil, combo("FinalClusterSnapshotIdentifier is required unless SkipFinalClusterSnapshot is specified.")
	default:
		if _, err := s.addSnapshot(c, in.FinalClusterSnapshotIdentifier, cl, nil, in.FinalClusterSnapshotRetentionPeriod); err != nil {
			return nil, err
		}
	}
	v := view(cl)
	v.ClusterStatus, v.ClusterAvailabilityStatus = "deleting", "Unavailable"
	delete(s.st.Clusters, key(c, cl.ClusterIdentifier))
	if cl.Pogi.Container != "" {
		s.box.Remove(cl.Pogi.Container)
	}
	return map[string]any{"Cluster": v}, nil
}

type clusterInput struct {
	ClusterIdentifier, NodeType, ClusterType string
	NumberOfNodes                            int
}

// pauseCluster stops the container; the cluster is paused once it has.
func (s *Service) pauseCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if cl.Pogi.State != "available" {
		return nil, badState("You cannot pause cluster %s while it is %s.", cl.ClusterIdentifier, status(cl.Pogi.State))
	}
	cl.Pogi.State = "stopped"
	if cl.Pogi.Container != "" {
		cl.Pogi.State = "pausing"
		s.box.Stop(&cl.Pogi)
	}
	return map[string]any{"Cluster": view(cl)}, nil
}

func (s *Service) resumeCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if cl.Pogi.State != "stopped" {
		return nil, badState("You cannot resume cluster %s: it is %s, not paused.", cl.ClusterIdentifier, status(cl.Pogi.State))
	}
	cl.Pogi.State = "available"
	if cl.Pogi.Container != "" {
		cl.Pogi.State = "resuming"
		s.box.Up(&cl.Pogi, cl.spec(), "start")
	}
	return map[string]any{"Cluster": view(cl)}, nil
}

func (s *Service) rebootCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if cl.Pogi.State != "available" {
		return nil, badState("You cannot reboot cluster %s while it is %s.", cl.ClusterIdentifier, status(cl.Pogi.State))
	}
	if cl.Pogi.Container != "" {
		cl.Pogi.State = "rebooting"
		s.box.Up(&cl.Pogi, cl.spec(), "restart")
	}
	return map[string]any{"Cluster": view(cl)}, nil
}

// resizeCluster changes the node type and count, which are metadata.
func (s *Service) resizeCluster(c *core.Ctx, in *clusterInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	nodes := cmp.Or(in.NumberOfNodes, cl.NumberOfNodes)
	if err := checkNodes(cmp.Or(in.NodeType, cl.NodeType), in.ClusterType, nodes); err != nil {
		return nil, err
	}
	if cl.Pogi.State != "available" {
		return nil, badState("You cannot resize cluster %s while it is %s.", cl.ClusterIdentifier, status(cl.Pogi.State))
	}
	cl.NodeType, cl.NumberOfNodes = cmp.Or(in.NodeType, cl.NodeType), nodes
	return map[string]any{"Cluster": view(cl)}, nil
}

// ---- credentials ----

type credentialsInput struct {
	ClusterIdentifier, DbUser, DbName string
	DurationSeconds                   int
	AutoCreate                        bool
	DbGroups                          []string
}

// getClusterCredentials serves GetClusterCredentials and
// GetClusterCredentialsWithIAM: a temporary login, which works on the
// cluster's database right away.
func (s *Service) getClusterCredentials(c *core.Ctx, in *credentialsInput) (any, error) {
	iam, user := c.Op == "GetClusterCredentialsWithIAM", strings.ToLower(in.DbUser)
	login := "IAM:" + user
	switch {
	case iam:
		login, user = IAMUser(c.Principal), ""
	case !ValidRole(user):
		return nil, invalid("DbUser must be 1 to 64 letters, digits or _+.@- starting with a letter.")
	case in.AutoCreate:
		login = "IAMA:" + user
	}
	for _, g := range in.DbGroups {
		if !ValidRole(g) {
			return nil, invalid("DbGroups must be 1 to 64 letters, digits or _+.@- starting with a letter: %s", g)
		}
	}
	dur := cmp.Or(in.DurationSeconds, 900)
	if dur < 900 || dur > 3600 {
		return nil, invalid("The parameter DurationSeconds must be between 900 and 3600.")
	}
	s.mu.Lock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	var db *DB
	if err == nil {
		db = s.db(cl)
		if cl.Pogi.Container != "" && db.Addr == "" {
			err = badState("Cluster %s is %s.", cl.ClusterIdentifier, status(cl.Pogi.State))
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	pw, exp := core.RandString(40, core.AlphaNum), time.Now().Add(time.Duration(dur)*time.Second).UTC()
	if db.Addr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := db.Login(ctx, login, user, each(in.DbGroups, strings.ToLower), in.AutoCreate, pw, exp); err != nil {
			return nil, fmt.Errorf("pogi could not create the database login: %w", err)
		}
	}
	out := map[string]any{"DbUser": login, "DbPassword": pw, "Expiration": exp}
	if iam {
		out["NextRefreshTime"] = exp
	}
	return out, nil
}

// db is where the database of a cluster answers; lock held.
func (s *Service) db(cl *Cluster) *DB {
	return NewDB(s.hub.Docker(), &cl.Pogi, cl.MasterUsername, cl.Pogi.Password, cl.DBName, "Cluster "+cl.ClusterIdentifier)
}

// DB is where the database of a cluster answers, for the Data API; nil when
// there is no such cluster.
func (s *Service) DB(account, region, id string) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl := s.st.Clusters[account+"/"+region+"/"+strings.ToLower(id)]; cl != nil {
		return s.db(cl)
	}
	return nil
}

// ---- logging ----

type loggingInput struct {
	ClusterIdentifier, BucketName, S3KeyPrefix, LogDestinationType string
	LogExports                                                     []string
}

// logging serves DescribeLoggingStatus, EnableLogging and DisableLogging.
func (s *Service) logging(c *core.Ctx, in *loggingInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	switch c.Op {
	case "EnableLogging":
		dest := cmp.Or(in.LogDestinationType, "s3")
		if dest == "s3" && in.BucketName == "" {
			return nil, invalid("BucketName is required when the log destination is s3.")
		}
		cl.Logging = &Logging{true, in.BucketName, in.S3KeyPrefix, dest, in.LogExports}
	case "DisableLogging":
		cl.Logging = nil
	}
	if cl.Logging == nil {
		return &Logging{}, nil
	}
	return rds.Clone(cl.Logging), nil
}

// ---- parameter groups ----

type paramGroupInput struct {
	ParameterGroupName, ParameterGroupFamily, Description, Source, Marker string
	MaxRecords                                                            int
	ResetAllParameters                                                    bool
	Parameters                                                            []Parameter
	Tags                                                                  []rds.Tag
}

func (s *Service) createParamGroup(c *core.Ctx, in *paramGroupInput) (any, error) {
	name := strings.ToLower(in.ParameterGroupName)
	if err := rds.ValidID("ParameterGroupName", name, 255); err != nil {
		return nil, err
	}
	switch {
	case in.ParameterGroupFamily != "redshift-1.0":
		return nil, invalid("Could not find parameter group family %s.", in.ParameterGroupFamily)
	case in.Description == "":
		return nil, invalid("The parameter Description must be provided and must not be blank.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Params[key(c, name)] != nil || name == defaultPG {
		return nil, core.Errorf(400, "ClusterParameterGroupAlreadyExists", "Parameter group %s already exists.", name)
	}
	g := &ParamGroup{ParameterGroupName: name, ParameterGroupFamily: in.ParameterGroupFamily, Description: in.Description, Tags: rds.AddTags(nil, in.Tags)}
	s.st.Params[key(c, name)] = g
	return map[string]any{"ClusterParameterGroup": rds.Clone(g)}, nil
}

func (s *Service) describeParamGroups(c *core.Ctx, in *paramGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*ParamGroup
	if in.ParameterGroupName != "" {
		g, err := s.paramGroup(c, in.ParameterGroupName)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	} else {
		def, _ := s.paramGroup(c, defaultPG)
		list = append(list, def)
		for k, g := range s.st.Params {
			if mine(c, k) {
				list = append(list, g)
			}
		}
	}
	list, marker, err := page(list, func(g *ParamGroup) string { return g.ParameterGroupName }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"ParameterGroups": each(list, rds.Clone[ParamGroup]), "Marker": marker}, nil
}

// modifyParamGroup sets parameters, or (Reset) drops them back to their defaults.
// ponytail: values are kept, not applied to the container's PostgreSQL.
func (s *Service) modifyParamGroup(c *core.Ctx, in *paramGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.ParameterGroupName)
	if err != nil {
		return nil, err
	}
	if g.ParameterGroupName == defaultPG {
		return nil, core.Errorf(400, "InvalidClusterParameterGroupState", "The default parameter group %s cannot be modified.", defaultPG)
	}
	if g.Params == nil || in.ResetAllParameters {
		g.Params = map[string]string{}
	}
	for _, p := range in.Parameters {
		if c.Op == "ResetClusterParameterGroup" {
			delete(g.Params, p.ParameterName)
		} else {
			g.Params[p.ParameterName] = p.ParameterValue
		}
	}
	return map[string]string{"ParameterGroupName": g.ParameterGroupName, "ParameterGroupStatus": "Your parameter group has been updated. " +
		"If you changed only dynamic parameters, associated clusters are being modified now. If you changed static parameters, " +
		"all updates, including dynamic parameters, will be applied when you reboot the associated clusters."}, nil
}

func (s *Service) deleteParamGroup(c *core.Ctx, in *paramGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.ParameterGroupName)
	if err != nil {
		return nil, err
	}
	if g.ParameterGroupName == defaultPG {
		return nil, core.Errorf(400, "InvalidClusterParameterGroupState", "The default parameter group %s cannot be deleted.", defaultPG)
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && cl.ClusterParameterGroups[0].ParameterGroupName == g.ParameterGroupName {
			return nil, core.Errorf(400, "InvalidClusterParameterGroupState", "The parameter group %s is in use by cluster %s.", g.ParameterGroupName, cl.ClusterIdentifier)
		}
	}
	delete(s.st.Params, key(c, g.ParameterGroupName))
	return nil, nil
}

// describeParameters lists the parameters set by users.
// ponytail: engine defaults are not listed.
func (s *Service) describeParameters(c *core.Ctx, in *paramGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.ParameterGroupName)
	if err != nil {
		return nil, err
	}
	var list []Parameter
	if in.Source == "" || in.Source == "user" {
		for n, v := range g.Params {
			list = append(list, Parameter{n, v, n, "user", "string", "dynamic", true})
		}
	}
	list, marker, err := page(list, func(p Parameter) string { return p.ParameterName }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Parameters": list, "Marker": marker}, nil
}

// ---- subnet and security groups ----

type groupInput struct {
	ClusterSubnetGroupName, ClusterSecurityGroupName, Description string
	SubnetIds                                                     []string
	Tags                                                          []rds.Tag
}

func (s *Service) createSubnetGroup(c *core.Ctx, in *groupInput) (any, error) {
	name := strings.ToLower(in.ClusterSubnetGroupName)
	if err := rds.ValidID("ClusterSubnetGroupName", name, 255); err != nil || name == "default" {
		return nil, invalid("ClusterSubnetGroupName must be a valid name other than default.")
	}
	switch {
	case in.Description == "":
		return nil, invalid("The parameter Description must be provided and must not be blank.")
	case len(in.SubnetIds) == 0:
		return nil, invalid("The parameter SubnetIds must be provided.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Subnets[key(c, name)] != nil {
		return nil, core.Errorf(400, "ClusterSubnetGroupAlreadyExists", "The cluster subnet group %s already exists.", name)
	}
	g := &SubnetGroup{ClusterSubnetGroupName: name, Description: in.Description, VpcId: rds.DefaultVPC, SubnetGroupStatus: "Complete",
		Subnets: rds.Subnets(c.Region, in.SubnetIds), Tags: rds.AddTags(nil, in.Tags)}
	s.st.Subnets[key(c, name)] = g
	return map[string]any{"ClusterSubnetGroup": rds.Clone(g)}, nil
}

func (s *Service) describeSubnetGroups(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*SubnetGroup
	if in.ClusterSubnetGroupName != "" {
		g, err := s.subnetGroup(c, in.ClusterSubnetGroupName)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	}
	for k, g := range s.st.Subnets {
		if in.ClusterSubnetGroupName == "" && mine(c, k) {
			list = append(list, g)
		}
	}
	list, marker, err := page(list, func(g *SubnetGroup) string { return g.ClusterSubnetGroupName }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"ClusterSubnetGroups": each(list, rds.Clone[SubnetGroup]), "Marker": marker}, nil
}

func (s *Service) modifySubnetGroup(c *core.Ctx, in *groupInput) (any, error) {
	if len(in.SubnetIds) == 0 {
		return nil, invalid("The parameter SubnetIds must be provided.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.subnetGroup(c, in.ClusterSubnetGroupName)
	if err != nil {
		return nil, err
	}
	g.Description, g.Subnets = cmp.Or(in.Description, g.Description), rds.Subnets(c.Region, in.SubnetIds)
	return map[string]any{"ClusterSubnetGroup": rds.Clone(g)}, nil
}

func (s *Service) deleteSubnetGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.subnetGroup(c, in.ClusterSubnetGroupName)
	if err != nil {
		return nil, err
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && cl.ClusterSubnetGroupName == g.ClusterSubnetGroupName {
			return nil, core.Errorf(400, "InvalidClusterSubnetGroupStateFault", "The cluster subnet group %s is in use by cluster %s.", g.ClusterSubnetGroupName, cl.ClusterIdentifier)
		}
	}
	delete(s.st.Subnets, key(c, g.ClusterSubnetGroupName))
	return nil, nil
}

func (s *Service) createSecGroup(c *core.Ctx, in *groupInput) (any, error) {
	name := strings.ToLower(in.ClusterSecurityGroupName)
	if err := rds.ValidID("ClusterSecurityGroupName", name, 255); err != nil || name == "default" {
		return nil, invalid("ClusterSecurityGroupName must be a valid name other than default.")
	}
	if in.Description == "" {
		return nil, invalid("The parameter Description must be provided and must not be blank.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.SecGroups[key(c, name)] != nil {
		return nil, core.Errorf(400, "ClusterSecurityGroupAlreadyExists", "The cluster security group %s already exists.", name)
	}
	g := &SecurityGroup{ClusterSecurityGroupName: name, Description: in.Description, Tags: rds.AddTags(nil, in.Tags)}
	s.st.SecGroups[key(c, name)] = g
	return map[string]any{"ClusterSecurityGroup": rds.Clone(g)}, nil
}

func (s *Service) describeSecGroups(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*SecurityGroup
	if in.ClusterSecurityGroupName != "" {
		g, err := s.secGroup(c, in.ClusterSecurityGroupName)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	}
	for k, g := range s.st.SecGroups {
		if in.ClusterSecurityGroupName == "" && mine(c, k) {
			list = append(list, g)
		}
	}
	list, marker, err := page(list, func(g *SecurityGroup) string { return g.ClusterSecurityGroupName }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"ClusterSecurityGroups": each(list, rds.Clone[SecurityGroup]), "Marker": marker}, nil
}

func (s *Service) deleteSecGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.secGroup(c, in.ClusterSecurityGroupName)
	if err != nil {
		return nil, err
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && slices.Contains(cl.ClusterSecurityGroups, Membership{g.ClusterSecurityGroupName, "active"}) {
			return nil, core.Errorf(400, "InvalidClusterSecurityGroupState", "The cluster security group %s is in use by cluster %s.", g.ClusterSecurityGroupName, cl.ClusterIdentifier)
		}
	}
	delete(s.st.SecGroups, key(c, g.ClusterSecurityGroupName))
	return nil, nil
}

// ---- snapshots ----

type snapshotInput struct {
	SnapshotIdentifier, ClusterIdentifier string
	ManualSnapshotRetentionPeriod         *int
	Tags                                  []rds.Tag
}

func (s *Service) createSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if cl.Pogi.State != "available" {
		return nil, badState("You cannot snapshot cluster %s while it is %s.", cl.ClusterIdentifier, status(cl.Pogi.State))
	}
	sn, err := s.addSnapshot(c, in.SnapshotIdentifier, cl, in.Tags, in.ManualSnapshotRetentionPeriod)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Snapshot": rds.Clone(sn)}, nil
}

// addSnapshot records a manual snapshot of a cluster; lock held.
func (s *Service) addSnapshot(c *core.Ctx, id string, cl *Cluster, tags []rds.Tag, retention *int) (*Snapshot, error) {
	id = strings.ToLower(id)
	if err := rds.ValidID("SnapshotIdentifier", id, 255); err != nil {
		return nil, err
	}
	if s.st.Snapshots[key(c, id)] != nil {
		return nil, core.Errorf(400, "ClusterSnapshotAlreadyExists", "Cannot create the snapshot because a snapshot with the identifier %s already exists.", id)
	}
	sn := &Snapshot{SnapshotIdentifier: id, ClusterIdentifier: cl.ClusterIdentifier, SnapshotCreateTime: time.Now().UTC(), Status: "available",
		Port: cl.Pogi.HostPort, AvailabilityZone: cl.AvailabilityZone, ClusterCreateTime: cl.ClusterCreateTime, MasterUsername: cl.MasterUsername,
		ClusterVersion: cl.ClusterVersion, SnapshotType: "manual", NodeType: cl.NodeType, NumberOfNodes: cl.NumberOfNodes, DBName: cl.DBName,
		VpcId: cl.VpcId, Encrypted: cl.Encrypted, KmsKeyId: cl.KmsKeyId, EnhancedVpcRouting: cl.EnhancedVpcRouting, OwnerAccount: c.Account,
		ManualSnapshotRetentionPeriod: cl.ManualSnapshotRetentionPeriod, SnapshotArn: c.ARN("redshift", "snapshot:"+cl.ClusterIdentifier+"/"+id),
		Tags: rds.AddTags(nil, tags)}
	set(&sn.ManualSnapshotRetentionPeriod, retention)
	s.st.Snapshots[key(c, id)] = sn
	return sn, nil
}

func (s *Service) describeSnapshots(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.SnapshotIdentifier != "" {
		if _, err := s.snapshot(c, in.SnapshotIdentifier); err != nil {
			return nil, err
		}
	}
	var list []*Snapshot
	for k, sn := range s.st.Snapshots {
		switch {
		case !mine(c, k),
			in.SnapshotIdentifier != "" && sn.SnapshotIdentifier != strings.ToLower(in.SnapshotIdentifier),
			in.ClusterIdentifier != "" && sn.ClusterIdentifier != strings.ToLower(in.ClusterIdentifier),
			in.SnapshotType != "" && sn.SnapshotType != in.SnapshotType:
			continue
		}
		list = append(list, sn)
	}
	list, marker, err := page(list, func(sn *Snapshot) string { return sn.SnapshotIdentifier }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Snapshots": each(list, rds.Clone[Snapshot]), "Marker": marker}, nil
}

func (s *Service) deleteSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in.SnapshotIdentifier)
	if err != nil {
		return nil, err
	}
	delete(s.st.Snapshots, key(c, sn.SnapshotIdentifier))
	v := rds.Clone(sn)
	v.Status = "deleted"
	return map[string]any{"Snapshot": v}, nil
}

// ---- tags ----

type tagsInput struct {
	ResourceName, ResourceType, Marker string
	MaxRecords                         int
	Tags                               []rds.Tag
	TagKeys, TagValues                 []string
}

// tagged is a resource that has tags.
type tagged struct {
	arn, kind string
	tags      *[]rds.Tag
}

// tagged lists the resources of an account and region; lock held.
func (s *Service) tagged(c *core.Ctx) []tagged {
	var out []tagged
	for k, cl := range s.st.Clusters {
		if mine(c, k) {
			out = append(out, tagged{c.ARN("redshift", "cluster:"+cl.ClusterIdentifier), "Cluster", &cl.Tags})
		}
	}
	for k, g := range s.st.Params {
		if mine(c, k) {
			out = append(out, tagged{c.ARN("redshift", "parametergroup:"+g.ParameterGroupName), "Parameter group", &g.Tags})
		}
	}
	for k, g := range s.st.Subnets {
		if mine(c, k) {
			out = append(out, tagged{c.ARN("redshift", "subnetgroup:"+g.ClusterSubnetGroupName), "Subnet group", &g.Tags})
		}
	}
	for k, g := range s.st.SecGroups {
		if mine(c, k) {
			out = append(out, tagged{c.ARN("redshift", "securitygroup:"+g.ClusterSecurityGroupName), "Cluster security group", &g.Tags})
		}
	}
	for k, sn := range s.st.Snapshots {
		if mine(c, k) {
			out = append(out, tagged{sn.SnapshotArn, "Snapshot", &sn.Tags})
		}
	}
	return out
}

// tags finds the tag list of the resource an ARN names; lock held.
func (s *Service) tags(arn string) (*[]rds.Tag, error) {
	_, region, account, _, ok := core.ParseARN(arn)
	if ok {
		for _, t := range s.tagged(&core.Ctx{Account: account, Region: region}) {
			if t.arn == arn {
				return t.tags, nil
			}
		}
	}
	return nil, core.Errorf(404, "ResourceNotFoundFault", "The resource with ARN %s was not found.", arn)
}

func (s *Service) createTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	*t = rds.AddTags(*t, in.Tags)
	return nil, nil
}

func (s *Service) deleteTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	*t = rds.RemoveTags(*t, in.TagKeys)
	return nil, nil
}

// TaggedResource is a tag of a resource, as DescribeTags lists them.
type TaggedResource struct {
	Tag                        rds.Tag
	ResourceName, ResourceType string
}

func (s *Service) describeTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.ResourceName != "" {
		if _, err := s.tags(in.ResourceName); err != nil {
			return nil, err
		}
	}
	var list []TaggedResource
	for _, t := range s.tagged(c) {
		if in.ResourceName != "" && t.arn != in.ResourceName || in.ResourceType != "" && !strings.EqualFold(t.kind, in.ResourceType) {
			continue
		}
		for _, tag := range *t.tags {
			if (len(in.TagKeys) == 0 || slices.Contains(in.TagKeys, tag.Key)) && (len(in.TagValues) == 0 || slices.Contains(in.TagValues, tag.Value)) {
				list = append(list, TaggedResource{tag, t.arn, t.kind})
			}
		}
	}
	list, marker, err := page(list, func(t TaggedResource) string { return t.ResourceName + "\x00" + t.Tag.Key }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"TaggedResources": list, "Marker": marker}, nil
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped clusters go.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	docker := len(st.containers()) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	for _, cl := range st.Clusters {
		s.box.Resume(&cl.Pogi, cl.spec(), docker)
	}
	keep := st.containers()
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
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

// Resources lists clusters for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Clusters", Columns: []string{"Identifier", "Node type", "Nodes", "Status", "Endpoint", "Database", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		c, v := ctxOf(k), view(s.st.Clusters[k])
		ep := ""
		if v.Endpoint != nil {
			ep = fmt.Sprintf("%s:%d", v.Endpoint.Address, v.Endpoint.Port)
		}
		t.Rows = append(t.Rows, []string{v.ClusterIdentifier, v.NodeType, strconv.Itoa(v.NumberOfNodes), v.ClusterStatus, ep, v.DBName, c.Region, c.Account})
	}
	return []core.Table{t}
}

// Inspect returns a cluster with pogi's side: container, master password,
// state and pogiNote.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cl := range s.st.Clusters {
		if cl.ClusterIdentifier == id {
			return view(cl)
		}
	}
	return nil
}
