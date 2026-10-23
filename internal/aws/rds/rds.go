// Package rds emulates Amazon RDS over the Query protocol and runs the real
// thing: PostgreSQL, MySQL and MariaDB instances and Aurora clusters start as
// Docker containers published on localhost. Without Docker the API still
// answers (IaC plans and applies) and Inspect says that nothing runs.
package rds

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the RDS emulator.
type Service struct {
	hub    *core.Hub
	h      http.Handler
	box    *Box
	mu     sync.Mutex
	st     state
	fronts map[*Server]net.Listener // IAM auth proxies (proxy.go)
	dir    string                   // dumps (snapshots.go)
	temp   bool                     // dir is temporary
}

type state struct {
	Instances map[string]*DBInstance    `json:"instances"` // account/region/identifier
	Clusters  map[string]*DBCluster     `json:"clusters"`
	Subnets   map[string]*DBSubnetGroup `json:"subnetGroups"`
	Params    map[string]*ParamGroup    `json:"parameterGroups"` // cluster parameter groups: account/region/cluster:name
	Snapshots map[string]*DBSnapshot    `json:"snapshots"`

	ClusterSnapshots map[string]*DBClusterSnapshot `json:"clusterSnapshots"`
}

func newState() state {
	return state{Instances: map[string]*DBInstance{}, Clusters: map[string]*DBCluster{}, Subnets: map[string]*DBSubnetGroup{},
		Params: map[string]*ParamGroup{}, Snapshots: map[string]*DBSnapshot{}, ClusterSnapshots: map[string]*DBClusterSnapshot{}}
}

// containers names the containers of the state.
func (st *state) containers() map[string]bool {
	out := map[string]bool{}
	for _, i := range st.Instances {
		if i.Pogi.Container != "" {
			out[i.Pogi.Container] = true
		}
	}
	for _, c := range st.Clusters {
		if c.Pogi.Container != "" {
			out[c.Pogi.Container] = true
		}
	}
	return out
}

// engines pogi runs; versions are newest first and the first is the default.
var engines = map[string]struct {
	port     int
	license  string
	desc     string
	versions []string
}{
	"postgres":          {5432, "postgresql-license", "PostgreSQL", []string{"17.4", "16.8", "15.12", "14.17", "13.20"}},
	"mysql":             {3306, "general-public-license", "MySQL Community Edition", []string{"8.4.4", "8.0.41"}},
	"mariadb":           {3306, "general-public-license", "MariaDB Community Edition", []string{"11.4.5", "10.11.11", "10.6.21"}},
	"aurora-postgresql": {5432, "postgresql-license", "Aurora (PostgreSQL)", []string{"16.6", "15.10", "14.15"}},
	"aurora-mysql":      {3306, "general-public-license", "Aurora MySQL", []string{"8.0.mysql_aurora.3.08.0", "8.0.mysql_aurora.3.05.2"}},
	"docdb":             {27017, "na", "Amazon DocumentDB", []string{"5.0.0", "4.0.0"}}, // the rest: engines.go
	"neptune":           {8182, "amazon-license", "Neptune", []string{"1.4.5.0", "1.3.4.0"}},
	"sqlserver-ex":      {1433, "license-included", "SQL Server Express Edition", sqlVersions},
	"sqlserver-web":     {1433, "license-included", "SQL Server Web Edition", sqlVersions},
	"sqlserver-se":      {1433, "license-included", "SQL Server Standard Edition", sqlVersions},
	"sqlserver-ee":      {1433, "license-included", "SQL Server Enterprise Edition", sqlVersions},
}

const hostedZone = "Z2R2ITUGPM61AM"

// items names the list items of RDS responses (see Lists).
var items = map[string]string{
	"DBInstances": "DBInstance", "DBClusters": "DBCluster", "DBSubnetGroups": "DBSubnetGroup", "DBSnapshots": "DBSnapshot",
	"DBParameterGroups": "DBParameterGroup", "DBClusterParameterGroups": "DBClusterParameterGroup", "Parameters": "Parameter",
	"DBEngineVersions": "DBEngineVersion", "OrderableDBInstanceOptions": "OrderableDBInstanceOption", "TagList": "Tag",
	"VpcSecurityGroups": "VpcSecurityGroupMembership", "Subnets": "Subnet", "OptionGroupMemberships": "OptionGroupMembership",
	"DBClusterMembers": "DBClusterMember", "AvailabilityZones": "AvailabilityZone", "DBClusterSnapshots": "DBClusterSnapshot",
	"DBSnapshotAttributes": "DBSnapshotAttribute", "AttributeValues": "AttributeValue",
}

// New creates the RDS service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.box = NewBox(hub, "rds", "failed", &s.mu, s.owns)
	s.h = Lists(&core.RPC{XMLNS: "http://rds.amazonaws.com/doc/2014-10-31/", Ops: map[string]core.Handler{
		"CreateDBInstance":                   core.H(s.createDBInstance),
		"DescribeDBInstances":                core.H(s.describeDBInstances),
		"ModifyDBInstance":                   core.H(s.modifyDBInstance),
		"DeleteDBInstance":                   core.H(s.deleteDBInstance),
		"RebootDBInstance":                   core.H(s.rebootDBInstance),
		"StopDBInstance":                     core.H(s.stopDBInstance),
		"StartDBInstance":                    core.H(s.startDBInstance),
		"CreateDBCluster":                    core.H(s.createDBCluster),
		"DescribeDBClusters":                 core.H(s.describeDBClusters),
		"ModifyDBCluster":                    core.H(s.modifyDBCluster),
		"DeleteDBCluster":                    core.H(s.deleteDBCluster),
		"CreateDBSubnetGroup":                core.H(s.createDBSubnetGroup),
		"DescribeDBSubnetGroups":             core.H(s.describeDBSubnetGroups),
		"ModifyDBSubnetGroup":                core.H(s.modifyDBSubnetGroup),
		"DeleteDBSubnetGroup":                core.H(s.deleteDBSubnetGroup),
		"CreateDBParameterGroup":             core.H(s.createParamGroup),
		"CreateDBClusterParameterGroup":      core.H(s.createParamGroup),
		"DescribeDBParameterGroups":          core.H(s.describeParamGroups),
		"DescribeDBClusterParameterGroups":   core.H(s.describeParamGroups),
		"ModifyDBParameterGroup":             core.H(s.modifyParamGroup),
		"ModifyDBClusterParameterGroup":      core.H(s.modifyParamGroup),
		"ResetDBParameterGroup":              core.H(s.modifyParamGroup),
		"ResetDBClusterParameterGroup":       core.H(s.modifyParamGroup),
		"DeleteDBParameterGroup":             core.H(s.deleteParamGroup),
		"DeleteDBClusterParameterGroup":      core.H(s.deleteParamGroup),
		"DescribeDBParameters":               core.H(s.describeParameters),
		"DescribeDBClusterParameters":        core.H(s.describeParameters),
		"CreateDBSnapshot":                   core.H(s.createDBSnapshot),
		"DescribeDBSnapshots":                core.H(s.describeDBSnapshots),
		"DeleteDBSnapshot":                   core.H(s.deleteDBSnapshot),
		"CopyDBSnapshot":                     core.H(s.copyDBSnapshot),
		"ModifyDBSnapshot":                   core.H(s.modifyDBSnapshot),
		"CreateDBClusterSnapshot":            core.H(s.createDBClusterSnapshot),
		"DescribeDBClusterSnapshots":         core.H(s.describeDBClusterSnapshots),
		"DeleteDBClusterSnapshot":            core.H(s.deleteDBClusterSnapshot),
		"RestoreDBInstanceFromDBSnapshot":    core.H(s.restoreDBInstance),
		"RestoreDBInstanceToPointInTime":     core.H(s.restoreDBInstance),
		"RestoreDBClusterFromSnapshot":       core.H(s.restoreDBCluster),
		"RestoreDBClusterToPointInTime":      core.H(s.restoreDBCluster),
		"EnableHttpEndpoint":                 core.H(s.httpEndpoint),
		"DisableHttpEndpoint":                core.H(s.httpEndpoint),
		"DescribeDBSnapshotAttributes":       core.H(s.describeDBSnapshotAttributes),
		"DescribeGlobalClusters":             core.H(s.describeGlobalClusters),
		"DescribeDBEngineVersions":           core.H(s.describeDBEngineVersions),
		"DescribeOrderableDBInstanceOptions": core.H(s.describeOrderable),
		"AddTagsToResource":                  core.H(s.addTags),
		"RemoveTagsFromResource":             core.H(s.removeTags),
		"ListTagsForResource":                core.H(s.listTags),
	}}, items)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "rds", Cloud: core.AWS, Title: "RDS", Signing: []string{"rds"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.h.ServeHTTP(w, r) }

// ---- API shapes (stored as is; Pogi and query:"-" fields are pogi's) ----

// DBInstance is a DB instance.
type DBInstance struct {
	DBInstanceIdentifier               string
	DBInstanceClass                    string
	Engine                             string
	DBInstanceStatus                   string
	MasterUsername                     string
	DBName                             string    `json:",omitempty"`
	Endpoint                           *Endpoint `json:",omitempty"`
	AllocatedStorage                   int
	InstanceCreateTime                 time.Time
	PreferredBackupWindow              string
	BackupRetentionPeriod              int
	VpcSecurityGroups                  []VpcSecurityGroup
	DBParameterGroups                  []ParameterGroupStatus
	AvailabilityZone                   string
	DBSubnetGroup                      *DBSubnetGroup `json:",omitempty"`
	PreferredMaintenanceWindow         string
	PendingModifiedValues              struct{}
	MultiAZ                            bool
	EngineVersion                      string
	AutoMinorVersionUpgrade            bool
	LicenseModel                       string
	Iops                               int
	OptionGroupMemberships             []OptionGroupMembership
	PubliclyAccessible                 bool
	StorageType                        string
	DBClusterIdentifier                string `json:",omitempty"`
	StorageEncrypted                   bool
	KmsKeyId                           string `json:",omitempty"`
	DbiResourceId                      string
	CACertificateIdentifier            string
	CopyTagsToSnapshot                 bool
	MonitoringInterval                 int
	MonitoringRoleArn                  string `json:",omitempty"`
	DBInstanceArn                      string
	IAMDatabaseAuthenticationEnabled   bool
	PerformanceInsightsEnabled         bool
	PerformanceInsightsRetentionPeriod int      `json:",omitempty"`
	EngineLifecycleSupport             string   `json:",omitempty"`
	MaxAllocatedStorage                int      `json:",omitempty"`
	EnabledCloudwatchLogsExports       []string `json:",omitempty"`
	DeletionProtection                 bool
	TagList                            []Tag
	NetworkType                        string
	BackupTarget                       string
	MasterUserSecret                   *MasterUserSecret `json:",omitempty"`
	DBSubnetGroupName                  string            `json:",omitempty" query:"-"`
	Pogi                               Server            `json:"pogi" query:"-"`
}

// VpcSecurityGroup is a security group membership.
type VpcSecurityGroup struct{ VpcSecurityGroupId, Status string }

// ParameterGroupStatus is the parameter group of an instance.
type ParameterGroupStatus struct{ DBParameterGroupName, ParameterApplyStatus string }

// OptionGroupMembership is the option group of an instance.
type OptionGroupMembership struct{ OptionGroupName, Status string }

// MasterUserSecret is the Secrets Manager secret of a managed master password.
type MasterUserSecret struct{ SecretArn, SecretStatus, KmsKeyId string }

// DBCluster is an Aurora (or Multi-AZ) DB cluster; it owns the container its instances share.
type DBCluster struct {
	AllocatedStorage                 int
	AvailabilityZones                []string
	BackupRetentionPeriod            int
	DatabaseName                     string `json:",omitempty"`
	DBClusterIdentifier              string
	DBClusterParameterGroup          string
	DBSubnetGroup                    string
	Status                           string
	Endpoint                         string `json:",omitempty"`
	ReaderEndpoint                   string `json:",omitempty"`
	MultiAZ                          bool
	Engine                           string
	EngineVersion                    string
	Port                             int
	MasterUsername                   string
	PreferredBackupWindow            string
	PreferredMaintenanceWindow       string
	DBClusterMembers                 []DBClusterMember `json:",omitempty"`
	VpcSecurityGroups                []VpcSecurityGroup
	HostedZoneId                     string
	StorageEncrypted                 bool
	KmsKeyId                         string `json:",omitempty"`
	DbClusterResourceId              string
	DBClusterArn                     string
	IAMDatabaseAuthenticationEnabled bool
	ClusterCreateTime                time.Time
	EnabledCloudwatchLogsExports     []string `json:",omitempty"`
	EngineMode                       string
	DeletionProtection               bool
	HttpEndpointEnabled              bool
	CopyTagsToSnapshot               bool
	CrossAccountClone                bool
	TagList                          []Tag
	MasterUserSecret                 *MasterUserSecret `json:",omitempty"`
	ServerlessV2ScalingConfiguration *ScalingV2        `json:",omitempty"`
	NetworkType                      string
	StorageType                      string
	DBClusterInstanceClass           string `json:",omitempty"`
	Pogi                             Server `json:"pogi" query:"-"`
}

// DBClusterMember is an instance of a cluster.
type DBClusterMember struct {
	DBInstanceIdentifier          string
	IsClusterWriter               bool
	DBClusterParameterGroupStatus string
	PromotionTier                 int
}

// ScalingV2 is a Serverless v2 capacity range.
type ScalingV2 struct{ MinCapacity, MaxCapacity float64 }

// DBSubnetGroup is a DB subnet group.
type DBSubnetGroup struct {
	DBSubnetGroupName        string
	DBSubnetGroupDescription string
	VpcId                    string
	SubnetGroupStatus        string
	Subnets                  []Subnet
	DBSubnetGroupArn         string
	SupportedNetworkTypes    []string
	Tags                     []Tag `json:",omitempty" query:"-"`
}

// ParamGroup is a DB parameter group, or a DB cluster parameter group.
type ParamGroup struct {
	Name, Family, Description, ARN string
	Cluster                        bool              `json:",omitempty"`
	Params                         map[string]string `json:",omitempty"`
	Tags                           []Tag             `json:",omitempty"`
}

type dbParameterGroup struct{ DBParameterGroupName, DBParameterGroupFamily, Description, DBParameterGroupArn string }

type dbClusterParameterGroup struct {
	DBClusterParameterGroupName, DBParameterGroupFamily, Description, DBClusterParameterGroupArn string
}

// out is the API shape of a parameter group and the name of its element.
func (g *ParamGroup) out() (string, any) {
	if g.Cluster {
		return "DBClusterParameterGroup", &dbClusterParameterGroup{g.Name, g.Family, g.Description, g.ARN}
	}
	return "DBParameterGroup", &dbParameterGroup{g.Name, g.Family, g.Description, g.ARN}
}

// Parameter is a parameter of a parameter group.
type Parameter struct {
	ParameterName, ParameterValue, Description, Source, ApplyType, DataType string
	IsModifiable                                                            bool
	ApplyMethod                                                             string
}

// DBSnapshot is a manual snapshot; its data is a dump (snapshots.go).
type DBSnapshot struct {
	DBSnapshotIdentifier       string
	DBInstanceIdentifier       string
	SnapshotCreateTime         time.Time
	Engine                     string
	AllocatedStorage           int
	Status                     string
	Port                       int
	AvailabilityZone           string
	VpcId                      string
	InstanceCreateTime         time.Time
	MasterUsername             string
	EngineVersion              string
	LicenseModel               string
	SnapshotType               string
	PercentProgress            int
	StorageType                string
	Encrypted                  bool
	KmsKeyId                   string `json:",omitempty"`
	DBSnapshotArn              string
	DbiResourceId              string
	TagList                    []Tag
	OriginalSnapshotCreateTime time.Time
	SnapshotTarget             string
	SourceDBSnapshotIdentifier string   `json:",omitempty"` // a copy: the ARN it was copied from
	SourceRegion               string   `json:",omitempty"`
	Pogi                       snapPogi `json:"pogi" query:"-"`
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

// ctxOf rebuilds the account and region of a state key.
func ctxOf(k string) *core.Ctx {
	p := strings.SplitN(k, "/", 3)
	return &core.Ctx{Account: p[0], Region: p[1]}
}

// ident normalizes an identifier argument: case-insensitive, ARNs accepted.
func ident(s string) string {
	if strings.HasPrefix(s, "arn:") {
		s = s[strings.LastIndex(s, ":")+1:]
	}
	return strings.ToLower(s)
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

func sgs(ids []string) []VpcSecurityGroup {
	return each(ids, func(id string) VpcSecurityGroup { return VpcSecurityGroup{id, "active"} })
}

// aurora: the cluster owns the server and instances join it (Aurora, DocumentDB, Neptune).
func aurora(engine string) bool {
	return strings.HasPrefix(engine, "aurora") || engine == "docdb" || engine == "neptune"
}

// major is the version prefix naming images and parameter group families:
// 17 for PostgreSQL, 8.0 for MySQL and MariaDB.
func major(engine, version string) string {
	p := strings.SplitN(version, ".", 3)
	if strings.Contains(engine, "postgres") || len(p) < 2 {
		return p[0]
	}
	return p[0] + "." + p[1]
}

func family(engine, version string) string {
	if strings.HasPrefix(engine, "sqlserver") { // sqlserver-ee-16.0
		return engine + "-" + strings.TrimSuffix(major(engine, version), "0")
	}
	return engine + major(engine, version)
}

// version resolves an empty or partial version ("16") to a full one.
func version(engine, v string) string {
	for _, full := range engines[engine].versions {
		if v == "" || full == v || strings.HasPrefix(full, v+".") {
			return full
		}
	}
	return v
}

// pgData is the data folder of the postgres image: from version 18 the image keeps a folder per major version
// under /var/lib/postgresql.
func pgData(major string) string {
	if n, _ := strconv.Atoi(major); n >= 18 {
		return "/var/lib/postgresql"
	}
	return "/var/lib/postgresql/data"
}

// dbSpec is the container of an engine: the official image of its major version.
func dbSpec(engine, version, user, password, db string, host int) Spec {
	if sp, ok := otherSpec(engine, user, password, host); ok {
		return sp
	}
	m := major(engine, version)
	if strings.Contains(engine, "postgres") {
		sp := Spec{Image: "postgres:" + m + "-alpine", Port: 5432, Host: host, Hello: "\x00\x00\x00\x08\x04\xd2\x16\x2f", // SSLRequest
			Data: pgData(m),
			Env:  []string{"POSTGRES_USER=" + user, "POSTGRES_PASSWORD=" + password},
			Init: []string{"createuser", "-U", user, "--no-login", "rds_iam"}} // the role RDS grants IAM auth users
		if db != "" {
			sp.Env = append(sp.Env, "POSTGRES_DB="+db)
		}
		return sp
	}
	image, pre, cli := "mysql:"+m, "MYSQL_", "mysql"
	if engine == "mariadb" {
		image, pre, cli = "mariadb:"+m, "MARIADB_", "mariadb"
	}
	sp := Spec{Image: image, Port: 3306, Host: host, Data: "/var/lib/mysql", Env: []string{pre + "ROOT_PASSWORD=" + password}}
	if user != "root" { // the image refuses MYSQL_USER=root; the master user gets what RDS gives it
		sp.Env = append(sp.Env, pre+"USER="+user, pre+"PASSWORD="+password)
		sp.Init = []string{"env", "MYSQL_PWD=" + password, cli, "-uroot", "-e", "GRANT ALL PRIVILEGES ON *.* TO '" + user + "'@'%' WITH GRANT OPTION"}
	}
	if db != "" {
		sp.Env = append(sp.Env, pre+"DATABASE="+db)
	}
	return sp
}

// Image is the image an engine runs at a version ("" is the default version): what the service starts,
// so pogi pull cannot disagree with it. It is "" for an engine pogi does not run.
func Image(engine, v string) string {
	if _, ok := engines[engine]; !ok {
		return ""
	}
	return dbSpec(engine, version(engine, v), "u", "p", "", 0).Image
}

// Images are the images of every engine at its default version, sorted and without repeats.
func Images() []string {
	var out []string
	for e := range engines {
		out = append(out, Image(e, ""))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (i *DBInstance) spec() Spec {
	return dbSpec(i.Engine, i.EngineVersion, i.MasterUsername, i.Pogi.Password, i.DBName, i.Pogi.HostPort)
}

func (cl *DBCluster) spec() Spec {
	return dbSpec(cl.Engine, cl.EngineVersion, cl.MasterUsername, cl.Pogi.Password, cl.DatabaseName, cl.Pogi.HostPort)
}

// alter is the command that changes the master password inside a container.
func alter(engine, user, old, pw string) []string {
	if cmd, ok := otherAlter(engine, user, old, pw); ok {
		return cmd
	}
	if strings.Contains(engine, "postgres") { // local connections are trusted in the image
		return []string{"psql", "-U", user, "-d", "postgres", "-c",
			fmt.Sprintf(`ALTER USER "%s" WITH PASSWORD '%s'`, user, strings.ReplaceAll(pw, "'", "''"))}
	}
	cli, q := "mysql", strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(pw)
	if engine == "mariadb" {
		cli = "mariadb"
	}
	sql := ""
	for _, u := range []string{"'root'@'%'", "'root'@'localhost'", "'" + user + "'@'%'"} {
		sql += "ALTER USER " + u + " IDENTIFIED BY '" + q + "';"
	}
	return []string{"env", "MYSQL_PWD=" + old, cli, "-uroot", "-e", sql}
}

func checkPassword(pw string) error {
	switch {
	case len(pw) < 8:
		return invalid("The parameter MasterUserPassword is not a valid password because it is shorter than 8 characters.")
	case strings.ContainsAny(pw, `/@" `):
		return invalid(`The parameter MasterUserPassword is not a valid password. Only printable ASCII characters besides '/', '@', '"', ' ' may be used.`)
	}
	return nil
}

var userRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,62}$`)

func checkMaster(user, pw string, managed bool) error {
	switch {
	case user == "":
		return invalid("The parameter MasterUsername must be provided and must not be blank.")
	case !userRe.MatchString(user): // it also lands in SQL inside the container
		return invalid("MasterUsername must be 1 to 63 letters, digits or underscores and start with a letter.")
	case managed && pw != "":
		return combo("You can't specify MasterUserPassword when ManageMasterUserPassword is turned on.")
	case managed:
		return nil
	case pw == "":
		return invalid("The parameter MasterUserPassword must be provided and must not be blank.")
	}
	return checkPassword(pw)
}

func newPassword() string { return core.RandString(28, core.AlphaNum) }

// newSecret keeps managed master credentials in Secrets Manager, as RDS does;
// without that service the password is only in Inspect. Unlocked.
func (s *Service) newSecret(c *core.Ctx, kind, user, pw string) *MasterUserSecret {
	val, _ := json.Marshal(map[string]string{"username": user, "password": pw})
	var out struct{ ARN string }
	if err := s.hub.Call(c.Account, c.Region, "secretsmanager", "CreateSecret",
		map[string]string{"Name": "rds!" + kind + "-" + core.UUID(), "SecretString": string(val)}, &out); err != nil {
		return nil
	}
	return &MasterUserSecret{SecretArn: out.ARN, SecretStatus: "active", KmsKeyId: c.ARN("kms", "alias/aws/secretsmanager")}
}

// secrets runs the Secrets Manager side of ManageMasterUserPassword: a new
// secret for pw (when set) and the deletion of old (when set). Unlocked.
func (s *Service) secrets(c *core.Ctx, field **MasterUserSecret, kind, user, pw string, old *MasterUserSecret) {
	if pw != "" {
		ms := s.newSecret(c, kind, user, pw)
		s.mu.Lock()
		*field = ms
		s.mu.Unlock()
	}
	if old != nil {
		s.hub.Call(c.Account, c.Region, "secretsmanager", "DeleteSecret",
			map[string]any{"SecretId": old.SecretArn, "ForceDeleteWithoutRecovery": true}, nil)
	}
}

// password sets a new master password, also inside a running container. Lock held.
func (s *Service) password(srv *Server, engine, user, pw string) {
	old := srv.Password
	srv.Password = pw
	if cmd := alter(engine, user, old, pw); srv.Container != "" && cmd != nil {
		s.box.Exec(srv, cmd...)
	}
}

// owns reports whether srv still belongs to an instance or cluster; lock held.
func (s *Service) owns(srv *Server) bool {
	for _, i := range s.st.Instances {
		if &i.Pogi == srv {
			return true
		}
	}
	for _, c := range s.st.Clusters {
		if &c.Pogi == srv {
			return true
		}
	}
	return false
}

// Filter is a Describe filter.
type Filter struct {
	Name   string
	Values []string
}

// filter applies Describe filters: each must match one of the values a resource has for it.
func filter(fs []Filter, vals map[string][]string) (bool, error) {
	for _, f := range fs {
		have, ok := vals[f.Name]
		if !ok {
			return false, invalid("Unrecognized filter name: %s.", f.Name)
		}
		if !slices.ContainsFunc(f.Values, func(v string) bool {
			return slices.ContainsFunc(have, func(h string) bool { return strings.EqualFold(h, v) })
		}) {
			return false, nil
		}
	}
	return true, nil
}

// ---- lookups (lock held) ----

func (s *Service) instance(c *core.Ctx, id string) (*DBInstance, error) {
	if i := s.st.Instances[key(c, ident(id))]; i != nil {
		return i, nil
	}
	return nil, core.Errorf(404, "DBInstanceNotFound", "DBInstance %s not found.", id)
}

func (s *Service) cluster(c *core.Ctx, id string) (*DBCluster, error) {
	if cl := s.st.Clusters[key(c, ident(id))]; cl != nil {
		return cl, nil
	}
	return nil, core.Errorf(404, "DBClusterNotFoundFault", "DBCluster %s not found.", id)
}

func (s *Service) subnetGroup(c *core.Ctx, name string) (*DBSubnetGroup, error) {
	name = strings.ToLower(name)
	if g := s.st.Subnets[key(c, name)]; g != nil {
		return g, nil
	}
	if name == "default" { // the default VPC's
		return &DBSubnetGroup{DBSubnetGroupName: "default", DBSubnetGroupDescription: "default", VpcId: DefaultVPC, SubnetGroupStatus: "Complete",
			Subnets: Subnets(c.Region, []string{"subnet-0a1b2c3d", "subnet-1a2b3c4d", "subnet-2a3b4c5d"}), DBSubnetGroupArn: c.ARN("rds", "subgrp:default"),
			SupportedNetworkTypes: []string{"IPV4"}}, nil
	}
	return nil, core.Errorf(404, "DBSubnetGroupNotFoundFault", "DBSubnetGroup %s not found.", name)
}

func pgKey(c *core.Ctx, name string, cluster bool) string {
	if cluster {
		return key(c, "cluster:"+name)
	}
	return key(c, name)
}

func pgARN(c *core.Ctx, name string, cluster bool) string {
	if cluster {
		return c.ARN("rds", "cluster-pg:"+name)
	}
	return c.ARN("rds", "pg:"+name)
}

// paramGroup finds a parameter group; default.<family> groups always exist.
func (s *Service) paramGroup(c *core.Ctx, name string, cluster bool) (*ParamGroup, error) {
	name = strings.ToLower(name)
	if g := s.st.Params[pgKey(c, name, cluster)]; g != nil {
		return g, nil
	}
	if fam, ok := strings.CutPrefix(name, "default."); ok && fam != "" {
		return &ParamGroup{Name: name, Family: fam, Description: "Default parameter group for " + fam, ARN: pgARN(c, name, cluster), Cluster: cluster}, nil
	}
	return nil, core.Errorf(404, "DBParameterGroupNotFound", "DBParameterGroup not found: %s", name)
}

// paramGroupFor is paramGroup, and the group must be of the engine's family
// (a mysql8.0 group cannot be used by a postgres16 instance).
func (s *Service) paramGroupFor(c *core.Ctx, name, engine, ver string, cluster bool) (*ParamGroup, error) {
	g, err := s.paramGroup(c, name, cluster)
	if err == nil && !strings.EqualFold(g.Family, family(engine, ver)) {
		err = combo("The parameter group %s with DBParameterGroupFamily %s cannot be used for this instance. Please use a parameter group with DBParameterGroupFamily %s.", g.Name, g.Family, family(engine, ver))
	}
	return g, err
}

func (s *Service) snapshot(c *core.Ctx, id string) (*DBSnapshot, error) {
	if sn := s.st.Snapshots[key(c, ident(id))]; sn != nil {
		return sn, nil
	}
	return nil, core.Errorf(404, "DBSnapshotNotFound", "DBSnapshot not found: %s", id)
}

// view is the API shape of an instance; Aurora instances show their cluster's server. Lock held.
func (s *Service) view(c *core.Ctx, i *DBInstance) *DBInstance {
	v := Clone(i)
	srv := &i.Pogi
	if cl := s.st.Clusters[key(c, i.DBClusterIdentifier)]; cl != nil {
		srv = &cl.Pogi
	}
	if v.DBInstanceStatus, v.Endpoint = srv.State, srv.Addr(); v.Endpoint != nil {
		v.Endpoint.HostedZoneId = hostedZone
	}
	if g, err := s.subnetGroup(c, cmp.Or(i.DBSubnetGroupName, "default")); err == nil {
		v.DBSubnetGroup = Clone(g)
	}
	return v
}

// clusterView is the API shape of a cluster; lock held.
func (s *Service) clusterView(c *core.Ctx, cl *DBCluster) *DBCluster {
	v := Clone(cl)
	v.Status, v.Endpoint, v.ReaderEndpoint = cl.Pogi.State, "localhost", "localhost"
	if cl.Pogi.HostPort > 0 {
		v.Port = cmp.Or(cl.Pogi.Proxy, cl.Pogi.HostPort)
	}
	var ms []*DBInstance
	for k, i := range s.st.Instances {
		if mine(c, k) && i.DBClusterIdentifier == cl.DBClusterIdentifier {
			ms = append(ms, i)
		}
	}
	slices.SortFunc(ms, func(a, b *DBInstance) int {
		return cmp.Or(a.InstanceCreateTime.Compare(b.InstanceCreateTime), strings.Compare(a.DBInstanceIdentifier, b.DBInstanceIdentifier))
	})
	for n, i := range ms {
		v.DBClusterMembers = append(v.DBClusterMembers, DBClusterMember{i.DBInstanceIdentifier, n == 0, "in-sync", 1})
	}
	return v
}

// ---- DB instances ----

type createInstanceInput struct {
	DBInstanceIdentifier, DBInstanceClass, Engine, EngineVersion, DBName       string
	MasterUsername, MasterUserPassword, DBClusterIdentifier                    string
	DBSubnetGroupName, DBParameterGroupName, AvailabilityZone, StorageType     string
	PreferredBackupWindow, PreferredMaintenanceWindow, CACertificateIdentifier string
	KmsKeyId, NetworkType, MonitoringRoleArn, EngineLifecycleSupport           string
	Port, AllocatedStorage, Iops, MonitoringInterval                           int
	PerformanceInsightsRetentionPeriod, MaxAllocatedStorage                    int
	BackupRetentionPeriod                                                      *int
	ManageMasterUserPassword, PubliclyAccessible, StorageEncrypted, MultiAZ    bool
	DeletionProtection, CopyTagsToSnapshot, EnableIAMDatabaseAuthentication    bool
	EnablePerformanceInsights                                                  bool
	AutoMinorVersionUpgrade                                                    *bool
	VpcSecurityGroupIds, EnableCloudwatchLogsExports                           []string
	Tags                                                                       []Tag
}

func (s *Service) createDBInstance(c *core.Ctx, in *createInstanceInput) (any, error) {
	id := strings.ToLower(in.DBInstanceIdentifier)
	_, known := engines[in.Engine]
	switch err := ValidID("DBInstanceIdentifier", id, 63); {
	case err != nil:
		return nil, err
	case !known:
		return nil, invalid("Invalid DB engine: %s", in.Engine)
	case !strings.HasPrefix(in.DBInstanceClass, "db."):
		return nil, invalid("Invalid DB Instance class: %s", in.DBInstanceClass)
	case aurora(in.Engine) != (in.DBClusterIdentifier != ""):
		return nil, combo("DBClusterIdentifier is required for Aurora DB instances and not allowed for other engines.")
	}
	pw, managed := in.MasterUserPassword, in.ManageMasterUserPassword && !aurora(in.Engine)
	if !aurora(in.Engine) {
		if err := checkMaster(in.MasterUsername, pw, managed); err != nil {
			return nil, err
		}
		if managed {
			pw = newPassword()
		}
	}
	docker := !aurora(in.Engine) && s.box.Available()
	s.mu.Lock()
	i, err := s.addInstance(c, id, in, pw, docker)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if managed {
		s.secrets(c, &i.MasterUserSecret, "db", in.MasterUsername, pw, nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"DBInstance": s.view(c, i)}, nil
}

// addInstance checks what an instance refers to and stores it; lock held.
func (s *Service) addInstance(c *core.Ctx, id string, in *createInstanceInput, pw string, docker bool) (*DBInstance, error) {
	if s.st.Instances[key(c, id)] != nil {
		return nil, core.Errorf(400, "DBInstanceAlreadyExists", "DB instance already exists")
	}
	ver, user, db := version(in.Engine, in.EngineVersion), in.MasterUsername, in.DBName
	if in.DBClusterIdentifier != "" {
		cl, err := s.cluster(c, in.DBClusterIdentifier)
		if err != nil {
			return nil, err
		}
		if cl.Engine != in.Engine {
			return nil, combo("The engine %s does not match the engine %s of DB cluster %s.", in.Engine, cl.Engine, cl.DBClusterIdentifier)
		}
		ver, user, db = cl.EngineVersion, cl.MasterUsername, cl.DatabaseName
	}
	if in.DBSubnetGroupName != "" {
		if _, err := s.subnetGroup(c, in.DBSubnetGroupName); err != nil {
			return nil, err
		}
	}
	pg, err := s.paramGroupFor(c, cmp.Or(in.DBParameterGroupName, "default."+family(in.Engine, ver)), in.Engine, ver, false)
	if err != nil {
		return nil, err
	}
	og := "default:" + in.Engine + "-" + strings.ReplaceAll(major(in.Engine, ver), ".", "-")
	i := &DBInstance{
		DBInstanceIdentifier: id, DBInstanceClass: in.DBInstanceClass, Engine: in.Engine, EngineVersion: ver, MasterUsername: user, DBName: db,
		AllocatedStorage: cmp.Or(in.AllocatedStorage, 20), InstanceCreateTime: time.Now().UTC(),
		PreferredBackupWindow: cmp.Or(in.PreferredBackupWindow, "03:00-03:30"), BackupRetentionPeriod: 1,
		VpcSecurityGroups: sgs(in.VpcSecurityGroupIds), DBParameterGroups: []ParameterGroupStatus{{pg.Name, "in-sync"}},
		AvailabilityZone: cmp.Or(in.AvailabilityZone, c.Region+"a"), PreferredMaintenanceWindow: cmp.Or(in.PreferredMaintenanceWindow, "sun:05:00-sun:05:30"),
		MultiAZ: in.MultiAZ, AutoMinorVersionUpgrade: in.AutoMinorVersionUpgrade == nil || *in.AutoMinorVersionUpgrade,
		LicenseModel: engines[in.Engine].license, Iops: in.Iops,
		OptionGroupMemberships: []OptionGroupMembership{{og, "in-sync"}}, PubliclyAccessible: in.PubliclyAccessible,
		StorageType: cmp.Or(in.StorageType, "gp2"), DBClusterIdentifier: ident(in.DBClusterIdentifier),
		StorageEncrypted: in.StorageEncrypted, DbiResourceId: "db-" + core.RandString(26, core.UpperNum),
		CACertificateIdentifier: cmp.Or(in.CACertificateIdentifier, "rds-ca-rsa2048-g1"), CopyTagsToSnapshot: in.CopyTagsToSnapshot,
		MonitoringInterval: in.MonitoringInterval, MonitoringRoleArn: in.MonitoringRoleArn, DBInstanceArn: c.ARN("rds", "db:"+id),
		EngineLifecycleSupport: in.EngineLifecycleSupport, MaxAllocatedStorage: in.MaxAllocatedStorage,
		IAMDatabaseAuthenticationEnabled: in.EnableIAMDatabaseAuthentication, PerformanceInsightsEnabled: in.EnablePerformanceInsights,
		EnabledCloudwatchLogsExports: in.EnableCloudwatchLogsExports, DeletionProtection: in.DeletionProtection,
		TagList: AddTags(nil, in.Tags), NetworkType: cmp.Or(in.NetworkType, "IPV4"), BackupTarget: "region",
		DBSubnetGroupName: strings.ToLower(in.DBSubnetGroupName),
	}
	set(&i.BackupRetentionPeriod, in.BackupRetentionPeriod)
	if i.PerformanceInsightsEnabled { // AWS reports the retention only while Performance Insights is on
		i.PerformanceInsightsRetentionPeriod = cmp.Or(in.PerformanceInsightsRetentionPeriod, 7)
	}
	if i.StorageEncrypted {
		i.KmsKeyId = cmp.Or(in.KmsKeyId, c.ARN("kms", "key/"+core.UUID()))
	}
	s.st.Instances[key(c, id)] = i
	if in.DBClusterIdentifier != "" {
		i.AllocatedStorage, i.StorageType = 1, "aurora"
		return i, nil
	}
	i.Pogi.Password = pw
	port := s.front(&i.Pogi, i.Engine, in.Port, i.IAMDatabaseAuthenticationEnabled, docker)
	s.box.Serve(&i.Pogi, c.Account, c.Region, id, port, engines[in.Engine].port, i.spec, docker)
	return i, nil
}

type describeInput struct {
	DBInstanceIdentifier, DBClusterIdentifier, DBSnapshotIdentifier, SnapshotType, DBSubnetGroupName string
	Marker                                                                                           string
	MaxRecords                                                                                       int
	Filters                                                                                          []Filter
}

func (s *Service) describeDBInstances(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.DBInstanceIdentifier != "" {
		if _, err := s.instance(c, in.DBInstanceIdentifier); err != nil {
			return nil, err
		}
	}
	var list []*DBInstance
	for k, i := range s.st.Instances {
		if !mine(c, k) || in.DBInstanceIdentifier != "" && i.DBInstanceIdentifier != ident(in.DBInstanceIdentifier) {
			continue
		}
		ok, err := filter(in.Filters, map[string][]string{"db-instance-id": {i.DBInstanceIdentifier, i.DBInstanceArn},
			"db-cluster-id":   {i.DBClusterIdentifier, c.ARN("rds", "cluster:"+i.DBClusterIdentifier)},
			"dbi-resource-id": {i.DbiResourceId}, "engine": {i.Engine}})
		if err != nil {
			return nil, err
		}
		if ok {
			list = append(list, i)
		}
	}
	list, marker := Page(list, func(i *DBInstance) string { return i.DBInstanceIdentifier }, in.Marker, in.MaxRecords)
	return map[string]any{"DBInstances": each(list, func(i *DBInstance) *DBInstance { return s.view(c, i) }), "Marker": marker}, nil
}

// logExports is a CloudwatchLogsExportConfiguration.
type logExports struct{ EnableLogTypes, DisableLogTypes []string }

func (l *logExports) apply(list []string) []string {
	if l == nil {
		return list
	}
	list = slices.DeleteFunc(slices.Clone(list), func(t string) bool { return slices.Contains(l.DisableLogTypes, t) })
	for _, t := range l.EnableLogTypes {
		if !slices.Contains(list, t) {
			list = append(list, t)
		}
	}
	return list
}

type modifyInstanceInput struct {
	DBInstanceIdentifier, NewDBInstanceIdentifier, DBInstanceClass, EngineVersion, MasterUserPassword string
	StorageType, PreferredBackupWindow, PreferredMaintenanceWindow, CACertificateIdentifier           string
	DBParameterGroupName, DBSubnetGroupName, NetworkType                                              string
	AllocatedStorage, Iops, MonitoringInterval, DBPortNumber                                          int
	MonitoringRoleArn, EngineLifecycleSupport                                                         string
	PerformanceInsightsRetentionPeriod, MaxAllocatedStorage                                           int
	BackupRetentionPeriod                                                                             *int
	ManageMasterUserPassword, PubliclyAccessible, MultiAZ, AutoMinorVersionUpgrade                    *bool
	DeletionProtection, CopyTagsToSnapshot, EnableIAMDatabaseAuthentication                           *bool
	EnablePerformanceInsights                                                                         *bool
	VpcSecurityGroupIds                                                                               []string
	CloudwatchLogsExportConfiguration                                                                 *logExports
}

func (s *Service) modifyDBInstance(c *core.Ctx, in *modifyInstanceInput) (any, error) {
	if in.MasterUserPassword != "" {
		if err := checkPassword(in.MasterUserPassword); err != nil {
			return nil, err
		}
	}
	manage, unmanage := in.ManageMasterUserPassword != nil && *in.ManageMasterUserPassword, in.ManageMasterUserPassword != nil && !*in.ManageMasterUserPassword
	switch {
	case manage && in.MasterUserPassword != "":
		return nil, combo("You can't specify MasterUserPassword when ManageMasterUserPassword is turned on.")
	case unmanage && in.MasterUserPassword == "":
		return nil, combo("Specify MasterUserPassword when you turn off ManageMasterUserPassword.")
	}
	s.mu.Lock()
	i, err := s.instance(c, in.DBInstanceIdentifier)
	if err == nil {
		err = s.checkModify(c, i, in)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if nid := strings.ToLower(in.NewDBInstanceIdentifier); nid != "" && nid != i.DBInstanceIdentifier {
		delete(s.st.Instances, key(c, i.DBInstanceIdentifier))
		i.DBInstanceIdentifier, i.DBInstanceArn = nid, c.ARN("rds", "db:"+nid)
		s.st.Instances[key(c, nid)] = i
	}
	i.DBInstanceClass = cmp.Or(in.DBInstanceClass, i.DBInstanceClass)
	if in.EngineVersion != "" { // the image changes on the next fresh container only
		i.EngineVersion = version(i.Engine, in.EngineVersion)
	}
	i.AllocatedStorage, i.Iops = cmp.Or(in.AllocatedStorage, i.AllocatedStorage), cmp.Or(in.Iops, i.Iops)
	i.StorageType, i.NetworkType = cmp.Or(in.StorageType, i.StorageType), cmp.Or(in.NetworkType, i.NetworkType)
	i.PreferredBackupWindow = cmp.Or(in.PreferredBackupWindow, i.PreferredBackupWindow)
	i.PreferredMaintenanceWindow = cmp.Or(in.PreferredMaintenanceWindow, i.PreferredMaintenanceWindow)
	i.CACertificateIdentifier = cmp.Or(in.CACertificateIdentifier, i.CACertificateIdentifier)
	i.MonitoringInterval = cmp.Or(in.MonitoringInterval, i.MonitoringInterval)
	i.MonitoringRoleArn = cmp.Or(in.MonitoringRoleArn, i.MonitoringRoleArn)
	i.EngineLifecycleSupport = cmp.Or(in.EngineLifecycleSupport, i.EngineLifecycleSupport)
	i.MaxAllocatedStorage = cmp.Or(in.MaxAllocatedStorage, i.MaxAllocatedStorage)
	set(&i.BackupRetentionPeriod, in.BackupRetentionPeriod)
	set(&i.PubliclyAccessible, in.PubliclyAccessible)
	set(&i.MultiAZ, in.MultiAZ)
	set(&i.AutoMinorVersionUpgrade, in.AutoMinorVersionUpgrade)
	set(&i.DeletionProtection, in.DeletionProtection)
	set(&i.CopyTagsToSnapshot, in.CopyTagsToSnapshot)
	set(&i.IAMDatabaseAuthenticationEnabled, in.EnableIAMDatabaseAuthentication)
	set(&i.PerformanceInsightsEnabled, in.EnablePerformanceInsights)
	if !i.PerformanceInsightsEnabled {
		i.PerformanceInsightsRetentionPeriod = 0
	} else {
		i.PerformanceInsightsRetentionPeriod = cmp.Or(in.PerformanceInsightsRetentionPeriod, i.PerformanceInsightsRetentionPeriod, 7)
	}
	i.EnabledCloudwatchLogsExports = in.CloudwatchLogsExportConfiguration.apply(i.EnabledCloudwatchLogsExports)
	if in.VpcSecurityGroupIds != nil {
		i.VpcSecurityGroups = sgs(in.VpcSecurityGroupIds)
	}
	if in.DBParameterGroupName != "" {
		i.DBParameterGroups = []ParameterGroupStatus{{strings.ToLower(in.DBParameterGroupName), "in-sync"}}
	}
	i.DBSubnetGroupName = cmp.Or(strings.ToLower(in.DBSubnetGroupName), i.DBSubnetGroupName)
	if in.DBPortNumber > 0 && i.Pogi.Container == "" { // a running container keeps its published port
		i.Pogi.HostPort = in.DBPortNumber
	}
	var pw string
	var old *MasterUserSecret
	switch {
	case manage && i.MasterUserSecret == nil:
		pw = newPassword()
		s.password(&i.Pogi, i.Engine, i.MasterUsername, pw)
	case unmanage:
		old, i.MasterUserSecret = i.MasterUserSecret, nil
	}
	if in.MasterUserPassword != "" {
		s.password(&i.Pogi, i.Engine, i.MasterUsername, in.MasterUserPassword)
	}
	s.iamAuth(&i.Pogi, i.Engine, i.IAMDatabaseAuthenticationEnabled, i.spec)
	s.mu.Unlock()
	s.secrets(c, &i.MasterUserSecret, "db", i.MasterUsername, pw, old)
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"DBInstance": s.view(c, i)}, nil
}

// checkModify validates a ModifyDBInstance before anything changes; lock held.
func (s *Service) checkModify(c *core.Ctx, i *DBInstance, in *modifyInstanceInput) error {
	if nid := strings.ToLower(in.NewDBInstanceIdentifier); nid != "" && nid != i.DBInstanceIdentifier {
		if err := ValidID("NewDBInstanceIdentifier", nid, 63); err != nil {
			return err
		}
		if s.st.Instances[key(c, nid)] != nil {
			return core.Errorf(400, "DBInstanceAlreadyExists", "DB instance already exists")
		}
	}
	if in.MasterUserPassword != "" || in.ManageMasterUserPassword != nil {
		if i.DBClusterIdentifier != "" {
			return combo("The master user password of an Aurora DB instance is managed by its DB cluster.")
		}
		if i.Pogi.Container != "" && i.Pogi.State != "available" {
			return core.Errorf(400, "InvalidDBInstanceState", "Instance %s is not in available state.", i.DBInstanceIdentifier)
		}
	}
	if in.DBSubnetGroupName != "" {
		if _, err := s.subnetGroup(c, in.DBSubnetGroupName); err != nil {
			return err
		}
	}
	if in.DBParameterGroupName != "" {
		if _, err := s.paramGroupFor(c, in.DBParameterGroupName, i.Engine, i.EngineVersion, false); err != nil {
			return err
		}
	}
	return nil
}

type deleteInput struct {
	DBInstanceIdentifier, DBClusterIdentifier, FinalDBSnapshotIdentifier string
	SkipFinalSnapshot                                                    bool
}

func (s *Service) deleteDBInstance(c *core.Ctx, in *deleteInput) (any, error) {
	s.mu.Lock()
	i, err := s.instance(c, in.DBInstanceIdentifier)
	switch {
	case err != nil:
	case i.DeletionProtection:
		err = combo("Cannot delete protected DB Instance, please disable deletion protection and try again.")
	case i.DBClusterIdentifier != "" || in.SkipFinalSnapshot:
	case in.FinalDBSnapshotIdentifier == "":
		err = combo("FinalDBSnapshotIdentifier is required unless SkipFinalSnapshot is specified.")
	default:
		_, err = s.addSnapshot(c, in.FinalDBSnapshotIdentifier, i, nil)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	v, old := s.view(c, i), i.MasterUserSecret
	v.DBInstanceStatus = "deleting"
	delete(s.st.Instances, key(c, i.DBInstanceIdentifier))
	s.gone(&i.Pogi)
	if i.Pogi.Container != "" {
		s.box.Remove(i.Pogi.Container)
	}
	s.mu.Unlock()
	s.secrets(c, nil, "", "", "", old)
	return map[string]any{"DBInstance": v}, nil
}

type instanceInput struct{ DBInstanceIdentifier string }

func (s *Service) stopDBInstance(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(c, in.DBInstanceIdentifier)
	switch {
	case err != nil:
		return nil, err
	case i.DBClusterIdentifier != "":
		return nil, combo("DB instance %s belongs to DB cluster %s; stop the cluster instead.", i.DBInstanceIdentifier, i.DBClusterIdentifier)
	case i.Pogi.State != "available":
		return nil, core.Errorf(400, "InvalidDBInstanceState", "Instance %s is not in available state.", i.DBInstanceIdentifier)
	}
	i.Pogi.State = "stopped"
	if i.Pogi.Container != "" {
		i.Pogi.State = "stopping"
		s.box.Stop(&i.Pogi)
	}
	return map[string]any{"DBInstance": s.view(c, i)}, nil
}

func (s *Service) startDBInstance(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(c, in.DBInstanceIdentifier)
	if err != nil {
		return nil, err
	}
	if i.Pogi.State != "stopped" {
		return nil, core.Errorf(400, "InvalidDBInstanceState", "Instance %s is not stopped, it is currently in %s.", i.DBInstanceIdentifier, i.Pogi.State)
	}
	i.Pogi.State = "available"
	if i.Pogi.Container != "" {
		i.Pogi.State = "starting"
		s.box.Up(&i.Pogi, i.spec(), "start")
	}
	return map[string]any{"DBInstance": s.view(c, i)}, nil
}

func (s *Service) rebootDBInstance(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(c, in.DBInstanceIdentifier)
	if err != nil {
		return nil, err
	}
	srv, sp := &i.Pogi, i.spec
	if cl := s.st.Clusters[key(c, i.DBClusterIdentifier)]; cl != nil { // Aurora instances share the cluster's server
		srv, sp = &cl.Pogi, cl.spec
	}
	if srv.State != "available" {
		return nil, core.Errorf(400, "InvalidDBInstanceState", "DB instance %s is not in available state.", i.DBInstanceIdentifier)
	}
	if srv.Container != "" {
		srv.State = "rebooting"
		s.box.Up(srv, sp(), "restart")
	}
	return map[string]any{"DBInstance": s.view(c, i)}, nil
}

// ---- DB clusters ----

type createClusterInput struct {
	DBClusterIdentifier, Engine, EngineVersion, EngineMode, DatabaseName, MasterUsername, MasterUserPassword   string
	DBClusterParameterGroupName, DBSubnetGroupName, KmsKeyId, StorageType, NetworkType, DBClusterInstanceClass string
	PreferredBackupWindow, PreferredMaintenanceWindow                                                          string
	Port, AllocatedStorage                                                                                     int
	BackupRetentionPeriod                                                                                      *int
	ManageMasterUserPassword, StorageEncrypted, DeletionProtection, CopyTagsToSnapshot                         bool
	EnableIAMDatabaseAuthentication, EnableHttpEndpoint                                                        bool
	VpcSecurityGroupIds, AvailabilityZones, EnableCloudwatchLogsExports                                        []string
	ServerlessV2ScalingConfiguration                                                                           *ScalingV2
	Tags                                                                                                       []Tag
}

func (s *Service) createDBCluster(c *core.Ctx, in *createClusterInput) (any, error) {
	id := strings.ToLower(in.DBClusterIdentifier)
	_, known := engines[in.Engine]
	switch err := ValidID("DBClusterIdentifier", id, 63); {
	case err != nil:
		return nil, err
	case !known || in.Engine == "mariadb" || strings.HasPrefix(in.Engine, "sqlserver"):
		return nil, invalid("Invalid DB engine: %s", in.Engine)
	}
	if err := checkMaster(in.MasterUsername, in.MasterUserPassword, in.ManageMasterUserPassword); err != nil && in.Engine != "neptune" { // Neptune has no master user
		return nil, err
	}
	pw := in.MasterUserPassword
	if in.ManageMasterUserPassword {
		pw = newPassword()
	}
	docker := s.box.Available()
	s.mu.Lock()
	cl, err := s.addCluster(c, id, in, pw, docker)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if in.ManageMasterUserPassword {
		s.secrets(c, &cl.MasterUserSecret, "cluster", in.MasterUsername, pw, nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"DBCluster": s.clusterView(c, cl)}, nil
}

// addCluster checks what a cluster refers to and stores it; lock held.
func (s *Service) addCluster(c *core.Ctx, id string, in *createClusterInput, pw string, docker bool) (*DBCluster, error) {
	if s.st.Clusters[key(c, id)] != nil {
		return nil, core.Errorf(400, "DBClusterAlreadyExistsFault", "DB Cluster already exists")
	}
	if in.DBSubnetGroupName != "" {
		if _, err := s.subnetGroup(c, in.DBSubnetGroupName); err != nil {
			return nil, err
		}
	}
	ver := version(in.Engine, in.EngineVersion)
	pg, err := s.paramGroup(c, cmp.Or(in.DBClusterParameterGroupName, "default."+family(in.Engine, ver)), true)
	if err != nil {
		return nil, core.Errorf(404, "DBClusterParameterGroupNotFound", "DBClusterParameterGroup not found: %s", in.DBClusterParameterGroupName)
	}
	azs := in.AvailabilityZones
	if len(azs) == 0 {
		azs = []string{c.Region + "a", c.Region + "b", c.Region + "c"}
	}
	port := engines[in.Engine].port
	cl := &DBCluster{
		AllocatedStorage: cmp.Or(in.AllocatedStorage, 1), AvailabilityZones: azs, BackupRetentionPeriod: 1, DatabaseName: in.DatabaseName,
		DBClusterIdentifier: id, DBClusterParameterGroup: pg.Name, DBSubnetGroup: cmp.Or(strings.ToLower(in.DBSubnetGroupName), "default"),
		Engine: in.Engine, EngineVersion: ver, Port: cmp.Or(in.Port, port), MasterUsername: in.MasterUsername,
		PreferredBackupWindow: cmp.Or(in.PreferredBackupWindow, "03:00-03:30"), PreferredMaintenanceWindow: cmp.Or(in.PreferredMaintenanceWindow, "sun:05:00-sun:05:30"),
		VpcSecurityGroups: sgs(in.VpcSecurityGroupIds), HostedZoneId: hostedZone, StorageEncrypted: in.StorageEncrypted,
		DbClusterResourceId: "cluster-" + core.RandString(26, core.UpperNum), DBClusterArn: c.ARN("rds", "cluster:"+id),
		IAMDatabaseAuthenticationEnabled: in.EnableIAMDatabaseAuthentication, ClusterCreateTime: time.Now().UTC(),
		EnabledCloudwatchLogsExports: in.EnableCloudwatchLogsExports, EngineMode: cmp.Or(in.EngineMode, "provisioned"),
		DeletionProtection: in.DeletionProtection, HttpEndpointEnabled: in.EnableHttpEndpoint, CopyTagsToSnapshot: in.CopyTagsToSnapshot,
		TagList: AddTags(nil, in.Tags), ServerlessV2ScalingConfiguration: in.ServerlessV2ScalingConfiguration,
		NetworkType: cmp.Or(in.NetworkType, "IPV4"), StorageType: cmp.Or(in.StorageType, "aurora"), DBClusterInstanceClass: in.DBClusterInstanceClass,
	}
	set(&cl.BackupRetentionPeriod, in.BackupRetentionPeriod)
	if cl.StorageEncrypted {
		cl.KmsKeyId = cmp.Or(in.KmsKeyId, c.ARN("kms", "key/"+core.UUID()))
	}
	if in.Engine == "docdb" || in.Engine == "neptune" { // their standard storage has no type
		cl.StorageType = in.StorageType
	}
	cl.Pogi.Password = pw
	s.st.Clusters[key(c, id)] = cl
	want := s.front(&cl.Pogi, cl.Engine, in.Port, cl.IAMDatabaseAuthenticationEnabled, docker)
	s.box.Serve(&cl.Pogi, c.Account, c.Region, id, want, port, cl.spec, docker)
	return cl, nil
}

func (s *Service) describeDBClusters(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.DBClusterIdentifier != "" {
		if _, err := s.cluster(c, in.DBClusterIdentifier); err != nil {
			return nil, err
		}
	}
	var list []*DBCluster
	for k, cl := range s.st.Clusters {
		if !mine(c, k) || in.DBClusterIdentifier != "" && cl.DBClusterIdentifier != ident(in.DBClusterIdentifier) {
			continue
		}
		ok, err := filter(in.Filters, map[string][]string{"db-cluster-id": {cl.DBClusterIdentifier, cl.DBClusterArn},
			"db-cluster-resource-id": {cl.DbClusterResourceId}, "engine": {cl.Engine}})
		if err != nil {
			return nil, err
		}
		if ok {
			list = append(list, cl)
		}
	}
	list, marker := Page(list, func(cl *DBCluster) string { return cl.DBClusterIdentifier }, in.Marker, in.MaxRecords)
	return map[string]any{"DBClusters": each(list, func(cl *DBCluster) *DBCluster { return s.clusterView(c, cl) }), "Marker": marker}, nil
}

type modifyClusterInput struct {
	DBClusterIdentifier, NewDBClusterIdentifier, MasterUserPassword, DBClusterParameterGroupName, EngineVersion string
	PreferredBackupWindow, PreferredMaintenanceWindow, StorageType, DBClusterInstanceClass                      string
	AllocatedStorage                                                                                            int
	BackupRetentionPeriod                                                                                       *int
	ManageMasterUserPassword, DeletionProtection, CopyTagsToSnapshot                                            *bool
	EnableIAMDatabaseAuthentication, EnableHttpEndpoint                                                         *bool
	VpcSecurityGroupIds                                                                                         []string
	ServerlessV2ScalingConfiguration                                                                            *ScalingV2
	CloudwatchLogsExportConfiguration                                                                           *logExports
}

func (s *Service) modifyDBCluster(c *core.Ctx, in *modifyClusterInput) (any, error) {
	if in.MasterUserPassword != "" {
		if err := checkPassword(in.MasterUserPassword); err != nil {
			return nil, err
		}
	}
	manage, unmanage := in.ManageMasterUserPassword != nil && *in.ManageMasterUserPassword, in.ManageMasterUserPassword != nil && !*in.ManageMasterUserPassword
	switch {
	case manage && in.MasterUserPassword != "":
		return nil, combo("You can't specify MasterUserPassword when ManageMasterUserPassword is turned on.")
	case unmanage && in.MasterUserPassword == "":
		return nil, combo("Specify MasterUserPassword when you turn off ManageMasterUserPassword.")
	}
	s.mu.Lock()
	cl, err := s.cluster(c, in.DBClusterIdentifier)
	nid := strings.ToLower(in.NewDBClusterIdentifier)
	if err == nil && nid != "" && nid != cl.DBClusterIdentifier {
		if err = ValidID("NewDBClusterIdentifier", nid, 63); err == nil && s.st.Clusters[key(c, nid)] != nil {
			err = core.Errorf(400, "DBClusterAlreadyExistsFault", "DB Cluster already exists")
		}
	}
	if err == nil && (in.MasterUserPassword != "" || manage) && cl.Pogi.Container != "" && cl.Pogi.State != "available" {
		err = core.Errorf(400, "InvalidDBClusterStateFault", "DbCluster %s is not in available state.", cl.DBClusterIdentifier)
	}
	if err == nil && in.DBClusterParameterGroupName != "" {
		if _, err = s.paramGroup(c, in.DBClusterParameterGroupName, true); err != nil {
			err = core.Errorf(404, "DBClusterParameterGroupNotFound", "DBClusterParameterGroup not found: %s", in.DBClusterParameterGroupName)
		}
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if nid != "" && nid != cl.DBClusterIdentifier {
		for k, i := range s.st.Instances {
			if mine(c, k) && i.DBClusterIdentifier == cl.DBClusterIdentifier {
				i.DBClusterIdentifier = nid
			}
		}
		delete(s.st.Clusters, key(c, cl.DBClusterIdentifier))
		cl.DBClusterIdentifier, cl.DBClusterArn = nid, c.ARN("rds", "cluster:"+nid)
		s.st.Clusters[key(c, nid)] = cl
	}
	if in.EngineVersion != "" {
		cl.EngineVersion = version(cl.Engine, in.EngineVersion)
	}
	cl.DBClusterParameterGroup = cmp.Or(strings.ToLower(in.DBClusterParameterGroupName), cl.DBClusterParameterGroup)
	cl.PreferredBackupWindow = cmp.Or(in.PreferredBackupWindow, cl.PreferredBackupWindow)
	cl.PreferredMaintenanceWindow = cmp.Or(in.PreferredMaintenanceWindow, cl.PreferredMaintenanceWindow)
	cl.StorageType, cl.AllocatedStorage = cmp.Or(in.StorageType, cl.StorageType), cmp.Or(in.AllocatedStorage, cl.AllocatedStorage)
	cl.DBClusterInstanceClass = cmp.Or(in.DBClusterInstanceClass, cl.DBClusterInstanceClass)
	set(&cl.BackupRetentionPeriod, in.BackupRetentionPeriod)
	set(&cl.DeletionProtection, in.DeletionProtection)
	set(&cl.CopyTagsToSnapshot, in.CopyTagsToSnapshot)
	set(&cl.IAMDatabaseAuthenticationEnabled, in.EnableIAMDatabaseAuthentication)
	set(&cl.HttpEndpointEnabled, in.EnableHttpEndpoint)
	cl.EnabledCloudwatchLogsExports = in.CloudwatchLogsExportConfiguration.apply(cl.EnabledCloudwatchLogsExports)
	if in.ServerlessV2ScalingConfiguration != nil {
		cl.ServerlessV2ScalingConfiguration = in.ServerlessV2ScalingConfiguration
	}
	if in.VpcSecurityGroupIds != nil {
		cl.VpcSecurityGroups = sgs(in.VpcSecurityGroupIds)
	}
	var pw string
	var old *MasterUserSecret
	switch {
	case manage && cl.MasterUserSecret == nil:
		pw = newPassword()
		s.password(&cl.Pogi, cl.Engine, cl.MasterUsername, pw)
	case unmanage:
		old, cl.MasterUserSecret = cl.MasterUserSecret, nil
	}
	if in.MasterUserPassword != "" {
		s.password(&cl.Pogi, cl.Engine, cl.MasterUsername, in.MasterUserPassword)
	}
	s.iamAuth(&cl.Pogi, cl.Engine, cl.IAMDatabaseAuthenticationEnabled, cl.spec)
	s.mu.Unlock()
	s.secrets(c, &cl.MasterUserSecret, "cluster", cl.MasterUsername, pw, old)
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"DBCluster": s.clusterView(c, cl)}, nil
}

func (s *Service) deleteDBCluster(c *core.Ctx, in *deleteInput) (any, error) {
	s.mu.Lock()
	cl, err := s.cluster(c, in.DBClusterIdentifier)
	switch {
	case err != nil:
	case cl.DeletionProtection:
		err = combo("Cannot delete protected Cluster, please disable deletion protection and try again.")
	case len(s.clusterView(c, cl).DBClusterMembers) > 0:
		err = core.Errorf(400, "InvalidDBClusterStateFault", "Cluster cannot be deleted, it still contains DB instances in non-deleting state.")
	case !in.SkipFinalSnapshot && in.FinalDBSnapshotIdentifier == "":
		err = combo("FinalDBSnapshotIdentifier is required unless SkipFinalSnapshot is specified.")
	case !in.SkipFinalSnapshot:
		_, err = s.addClusterSnapshot(c, in.FinalDBSnapshotIdentifier, cl, nil)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	v, old := s.clusterView(c, cl), cl.MasterUserSecret
	v.Status = "deleting"
	delete(s.st.Clusters, key(c, cl.DBClusterIdentifier))
	s.gone(&cl.Pogi)
	if cl.Pogi.Container != "" {
		s.box.Remove(cl.Pogi.Container)
	}
	s.mu.Unlock()
	s.secrets(c, nil, "", "", "", old)
	return map[string]any{"DBCluster": v}, nil
}

// ---- subnet groups ----

type subnetGroupInput struct {
	DBSubnetGroupName, DBSubnetGroupDescription string
	SubnetIds                                   []string
	Tags                                        []Tag
}

func (s *Service) createDBSubnetGroup(c *core.Ctx, in *subnetGroupInput) (any, error) {
	name := strings.ToLower(in.DBSubnetGroupName)
	switch {
	case name == "" || name == "default":
		return nil, invalid("DBSubnetGroupName must be given and must not be default.")
	case len(in.SubnetIds) == 0:
		return nil, invalid("The parameter SubnetIds must be provided.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Subnets[key(c, name)] != nil {
		return nil, core.Errorf(400, "DBSubnetGroupAlreadyExists", "The DB subnet group '%s' already exists.", name)
	}
	g := &DBSubnetGroup{DBSubnetGroupName: name, DBSubnetGroupDescription: in.DBSubnetGroupDescription, VpcId: DefaultVPC,
		SubnetGroupStatus: "Complete", Subnets: Subnets(c.Region, in.SubnetIds), DBSubnetGroupArn: c.ARN("rds", "subgrp:"+name),
		SupportedNetworkTypes: []string{"IPV4"}, Tags: AddTags(nil, in.Tags)}
	s.st.Subnets[key(c, name)] = g
	return map[string]any{"DBSubnetGroup": Clone(g)}, nil
}

func (s *Service) describeDBSubnetGroups(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*DBSubnetGroup
	if in.DBSubnetGroupName != "" {
		g, err := s.subnetGroup(c, in.DBSubnetGroupName)
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
	list, marker := Page(list, func(g *DBSubnetGroup) string { return g.DBSubnetGroupName }, in.Marker, in.MaxRecords)
	return map[string]any{"DBSubnetGroups": each(list, Clone[DBSubnetGroup]), "Marker": marker}, nil
}

func (s *Service) modifyDBSubnetGroup(c *core.Ctx, in *subnetGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Subnets[key(c, strings.ToLower(in.DBSubnetGroupName))]
	if g == nil {
		return nil, core.Errorf(404, "DBSubnetGroupNotFoundFault", "DBSubnetGroup %s not found.", in.DBSubnetGroupName)
	}
	g.DBSubnetGroupDescription = cmp.Or(in.DBSubnetGroupDescription, g.DBSubnetGroupDescription)
	if len(in.SubnetIds) > 0 {
		g.Subnets = Subnets(c.Region, in.SubnetIds)
	}
	return map[string]any{"DBSubnetGroup": Clone(g)}, nil
}

func (s *Service) deleteDBSubnetGroup(c *core.Ctx, in *subnetGroupInput) (any, error) {
	name := strings.ToLower(in.DBSubnetGroupName)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Subnets[key(c, name)] == nil {
		return nil, core.Errorf(404, "DBSubnetGroupNotFoundFault", "DBSubnetGroup %s not found.", name)
	}
	for k, i := range s.st.Instances {
		if mine(c, k) && i.DBSubnetGroupName == name {
			return nil, core.Errorf(400, "InvalidDBSubnetGroupStateFault",
				"Cannot delete the subnet group '%s' because at least one database instance: %s is still using it.", name, i.DBInstanceIdentifier)
		}
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && cl.DBSubnetGroup == name {
			return nil, core.Errorf(400, "InvalidDBSubnetGroupStateFault",
				"Cannot delete the subnet group '%s' because at least one database cluster: %s is still using it.", name, cl.DBClusterIdentifier)
		}
	}
	delete(s.st.Subnets, key(c, name))
	return nil, nil
}

// ---- parameter groups (DB and DB cluster ones share the handlers) ----

type paramGroupInput struct {
	DBParameterGroupName, DBClusterParameterGroupName, DBParameterGroupFamily, Description, Source, Marker string
	MaxRecords                                                                                             int
	ResetAllParameters                                                                                     bool
	Parameters                                                                                             []Parameter
	Tags                                                                                                   []Tag
}

func (in *paramGroupInput) name() string {
	return strings.ToLower(cmp.Or(in.DBParameterGroupName, in.DBClusterParameterGroupName))
}

func (s *Service) createParamGroup(c *core.Ctx, in *paramGroupInput) (any, error) {
	cluster, name := strings.Contains(c.Op, "Cluster"), in.name()
	switch {
	case name == "" || strings.HasPrefix(name, "default."):
		return nil, invalid("The parameter group name must be given and must not start with default.")
	case in.DBParameterGroupFamily == "":
		return nil, invalid("The parameter DBParameterGroupFamily must be provided and must not be blank.")
	case in.Description == "":
		return nil, invalid("The parameter Description must be provided and must not be blank.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Params[pgKey(c, name, cluster)] != nil {
		return nil, core.Errorf(400, "DBParameterGroupAlreadyExists", "Parameter group %s already exists", name)
	}
	g := &ParamGroup{Name: name, Family: in.DBParameterGroupFamily, Description: in.Description, ARN: pgARN(c, name, cluster),
		Cluster: cluster, Tags: AddTags(nil, in.Tags)}
	s.st.Params[pgKey(c, name, cluster)] = g
	elem, out := g.out()
	return map[string]any{elem: out}, nil
}

func (s *Service) describeParamGroups(c *core.Ctx, in *paramGroupInput) (any, error) {
	cluster := strings.Contains(c.Op, "Cluster")
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*ParamGroup
	if in.name() != "" {
		g, err := s.paramGroup(c, in.name(), cluster)
		if err != nil {
			return nil, err
		}
		list = append(list, g)
	} else {
		for k, g := range s.st.Params {
			if mine(c, k) && g.Cluster == cluster {
				list = append(list, g)
			}
		}
	}
	list, marker := Page(list, func(g *ParamGroup) string { return g.Name }, in.Marker, in.MaxRecords)
	elem := "DBParameterGroup"
	if cluster {
		elem = "DBClusterParameterGroup"
	}
	return map[string]any{elem + "s": each(list, func(g *ParamGroup) any { _, out := g.out(); return out }), "Marker": marker}, nil
}

// modifyParamGroup sets parameters, or (Reset*) drops them back to the engine defaults.
func (s *Service) modifyParamGroup(c *core.Ctx, in *paramGroupInput) (any, error) {
	cluster := strings.Contains(c.Op, "Cluster")
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.name(), cluster)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(g.Name, "default.") {
		return nil, core.Errorf(400, "InvalidDBParameterGroupState", "Default parameter groups cannot be modified: %s", g.Name)
	}
	if g.Params == nil || in.ResetAllParameters {
		g.Params = map[string]string{}
	}
	for _, p := range in.Parameters {
		if strings.HasPrefix(c.Op, "Reset") {
			delete(g.Params, p.ParameterName)
		} else {
			g.Params[p.ParameterName] = p.ParameterValue
		}
	}
	elem, _ := g.out()
	return map[string]string{elem + "Name": g.Name}, nil
}

func (s *Service) deleteParamGroup(c *core.Ctx, in *paramGroupInput) (any, error) {
	cluster := strings.Contains(c.Op, "Cluster")
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.name(), cluster)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(g.Name, "default.") {
		return nil, core.Errorf(400, "InvalidDBParameterGroupState", "Default parameter groups cannot be deleted: %s", g.Name)
	}
	for k, i := range s.st.Instances {
		if mine(c, k) && !cluster && i.DBParameterGroups[0].DBParameterGroupName == g.Name {
			return nil, core.Errorf(400, "InvalidDBParameterGroupState", "One or more database instances are still members of this parameter group %s, so the group cannot be deleted", g.Name)
		}
	}
	for k, cl := range s.st.Clusters {
		if mine(c, k) && cluster && cl.DBClusterParameterGroup == g.Name {
			return nil, core.Errorf(400, "InvalidDBParameterGroupState", "One or more database clusters are still members of this parameter group %s, so the group cannot be deleted", g.Name)
		}
	}
	delete(s.st.Params, pgKey(c, g.Name, cluster))
	return nil, nil
}

// describeParameters lists the parameters set by users.
// ponytail: engine defaults are not listed and values do not reach the container.
func (s *Service) describeParameters(c *core.Ctx, in *paramGroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.paramGroup(c, in.name(), strings.Contains(c.Op, "Cluster"))
	if err != nil {
		return nil, err
	}
	var list []Parameter
	if in.Source == "" || in.Source == "user" {
		for n, v := range g.Params {
			list = append(list, Parameter{n, v, n, "user", "dynamic", "string", true, "immediate"})
		}
	}
	list, marker := Page(list, func(p Parameter) string { return p.ParameterName }, in.Marker, in.MaxRecords)
	return map[string]any{"Parameters": list, "Marker": marker}, nil
}

// ---- snapshots ----

type snapshotInput struct {
	DBSnapshotIdentifier, DBInstanceIdentifier string
	Tags                                       []Tag
}

func (s *Service) createDBSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(c, in.DBInstanceIdentifier)
	if err != nil {
		return nil, err
	}
	if i.DBClusterIdentifier != "" {
		return nil, combo("DB instance %s belongs to a DB cluster; snapshot the cluster instead.", i.DBInstanceIdentifier)
	}
	if i.Pogi.State != "available" {
		return nil, core.Errorf(400, "InvalidDBInstanceState", "Instance %s is not in available state.", i.DBInstanceIdentifier)
	}
	sn, err := s.addSnapshot(c, in.DBSnapshotIdentifier, i, in.Tags)
	if err != nil {
		return nil, err
	}
	return map[string]any{"DBSnapshot": Clone(sn)}, nil
}

// addSnapshot records a manual snapshot of an instance; lock held.
func (s *Service) addSnapshot(c *core.Ctx, id string, i *DBInstance, tags []Tag) (*DBSnapshot, error) {
	id = strings.ToLower(id)
	if err := ValidID("DBSnapshotIdentifier", id, 255); err != nil {
		return nil, err
	}
	if s.st.Snapshots[key(c, id)] != nil {
		return nil, core.Errorf(400, "DBSnapshotAlreadyExists", "Cannot create the snapshot because a snapshot with the identifier %s already exists.", id)
	}
	if len(tags) == 0 && i.CopyTagsToSnapshot {
		tags = i.TagList
	}
	now := time.Now().UTC()
	sn := &DBSnapshot{DBSnapshotIdentifier: id, DBInstanceIdentifier: i.DBInstanceIdentifier, SnapshotCreateTime: now, Engine: i.Engine,
		AllocatedStorage: i.AllocatedStorage, Status: "available", Port: cmp.Or(i.Pogi.Proxy, i.Pogi.HostPort), AvailabilityZone: i.AvailabilityZone, VpcId: DefaultVPC,
		InstanceCreateTime: i.InstanceCreateTime, MasterUsername: i.MasterUsername, EngineVersion: i.EngineVersion, LicenseModel: i.LicenseModel,
		SnapshotType: "manual", PercentProgress: 100, StorageType: i.StorageType, Encrypted: i.StorageEncrypted, KmsKeyId: i.KmsKeyId,
		DBSnapshotArn: c.ARN("rds", "snapshot:"+id), DbiResourceId: i.DbiResourceId, TagList: AddTags(nil, tags),
		OriginalSnapshotCreateTime: now, SnapshotTarget: "region", Pogi: snapPogi{i.Pogi.Password, i.DBName, i.DBInstanceClass}}
	k := key(c, id)
	s.st.Snapshots[k] = sn
	s.keep(&i.Pogi, i.Engine, s.snapFile(k, false), &sn.Status, &sn.PercentProgress, func() bool { return s.st.Snapshots[k] == sn })
	return sn, nil
}

func (s *Service) describeDBSnapshots(c *core.Ctx, in *describeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.DBSnapshotIdentifier != "" {
		if _, err := s.snapshot(c, in.DBSnapshotIdentifier); err != nil {
			return nil, err
		}
	}
	var list []*DBSnapshot
	for k, sn := range s.st.Snapshots {
		switch {
		case !mine(c, k),
			in.DBSnapshotIdentifier != "" && sn.DBSnapshotIdentifier != ident(in.DBSnapshotIdentifier),
			in.DBInstanceIdentifier != "" && sn.DBInstanceIdentifier != ident(in.DBInstanceIdentifier),
			in.SnapshotType != "" && sn.SnapshotType != in.SnapshotType:
			continue
		}
		ok, err := filter(in.Filters, map[string][]string{"db-instance-id": {sn.DBInstanceIdentifier}, "db-snapshot-id": {sn.DBSnapshotIdentifier, sn.DBSnapshotArn},
			"dbi-resource-id": {sn.DbiResourceId}, "snapshot-type": {sn.SnapshotType}, "engine": {sn.Engine}})
		if err != nil {
			return nil, err
		}
		if ok {
			list = append(list, sn)
		}
	}
	list, marker := Page(list, func(sn *DBSnapshot) string { return sn.DBSnapshotIdentifier }, in.Marker, in.MaxRecords)
	return map[string]any{"DBSnapshots": each(list, Clone[DBSnapshot]), "Marker": marker}, nil
}

func (s *Service) deleteDBSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in.DBSnapshotIdentifier)
	if err != nil {
		return nil, err
	}
	delete(s.st.Snapshots, key(c, sn.DBSnapshotIdentifier))
	os.Remove(s.snapFile(key(c, sn.DBSnapshotIdentifier), false))
	v := Clone(sn)
	v.Status = "deleted"
	return map[string]any{"DBSnapshot": v}, nil
}

// ---- catalog ----

// DBEngineVersion is an engine version pogi offers.
type DBEngineVersion struct {
	Engine, MajorEngineVersion, EngineVersion, DBParameterGroupFamily string
	DBEngineDescription, DBEngineVersionDescription, Status           string
	SupportedEngineModes                                              []string
	SupportsReadReplica, SupportsLogExportsToCloudwatchLogs           bool
}

type catalogInput struct {
	Engine, EngineVersion, DBParameterGroupFamily, DBInstanceClass, Marker string
	DefaultOnly                                                            bool
	MaxRecords                                                             int
}

func (s *Service) describeDBEngineVersions(c *core.Ctx, in *catalogInput) (any, error) {
	var list []DBEngineVersion
	for _, name := range core.SortedKeys(engines) {
		e := engines[name]
		if in.Engine != "" && in.Engine != name {
			continue
		}
		for _, v := range e.versions {
			if in.EngineVersion != "" && v != in.EngineVersion && !strings.HasPrefix(v, in.EngineVersion+".") ||
				in.DBParameterGroupFamily != "" && family(name, v) != in.DBParameterGroupFamily {
				continue
			}
			list = append(list, DBEngineVersion{name, major(name, v), v, family(name, v), e.desc, e.desc + " " + v, "available",
				[]string{"provisioned"}, true, true})
			if in.DefaultOnly {
				break
			}
		}
	}
	list, marker := Page(list, func(v DBEngineVersion) string { return v.Engine + "/" + v.EngineVersion }, in.Marker, in.MaxRecords)
	return map[string]any{"DBEngineVersions": list, "Marker": marker}, nil
}

// OrderableDBInstanceOption is an instance class pogi accepts for an engine version.
type OrderableDBInstanceOption struct {
	Engine, EngineVersion, DBInstanceClass, LicenseModel                         string
	AvailabilityZones                                                            []struct{ Name string }
	MultiAZCapable, ReadReplicaCapable, Vpc, SupportsStorageEncryption           bool
	StorageType                                                                  string
	SupportsIops, SupportsIAMDatabaseAuthentication, SupportsPerformanceInsights bool
	MinStorageSize, MaxStorageSize                                               int
	SupportsStorageAutoscaling                                                   bool
	SupportedEngineModes                                                         []string
	SupportsClusters                                                             bool
}

func (s *Service) describeOrderable(c *core.Ctx, in *catalogInput) (any, error) {
	classes := []string{"db.t3.micro", "db.t3.small", "db.t3.medium", "db.t4g.micro", "db.t4g.small", "db.t4g.medium", "db.m5.large", "db.m6g.large", "db.r6g.large"}
	storage, minSize := "gp2", 20
	if aurora(in.Engine) {
		classes, storage, minSize = []string{"db.t3.medium", "db.t4g.medium", "db.r6g.large", "db.r7g.large", "db.serverless"}, "aurora", 1
	}
	azs := []struct{ Name string }{{c.Region + "a"}, {c.Region + "b"}, {c.Region + "c"}}
	var list []OrderableDBInstanceOption
	for _, v := range engines[in.Engine].versions {
		if in.EngineVersion != "" && v != in.EngineVersion && !strings.HasPrefix(v, in.EngineVersion+".") {
			continue
		}
		for _, class := range classes {
			if in.DBInstanceClass == "" || in.DBInstanceClass == class {
				list = append(list, OrderableDBInstanceOption{in.Engine, v, class, engines[in.Engine].license, azs, true, true, true, true,
					storage, true, true, true, minSize, 65536, true, []string{"provisioned"}, aurora(in.Engine)})
			}
		}
	}
	list, marker := Page(list, func(o OrderableDBInstanceOption) string { return o.EngineVersion + "/" + o.DBInstanceClass }, in.Marker, in.MaxRecords)
	return map[string]any{"OrderableDBInstanceOptions": list, "Marker": marker}, nil
}

// ---- tags ----

type tagsInput struct {
	ResourceName string
	Tags         []Tag
	TagKeys      []string
}

// tags finds the tag list of the resource an ARN names; lock held.
func (s *Service) tags(arn string) (*[]Tag, error) {
	svc, region, account, res, ok := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, ":")
	if !ok || svc != "rds" {
		return nil, invalid("The specified resource name does not match an RDS resource in this region: %s", arn)
	}
	c := &core.Ctx{Account: account, Region: region}
	switch kind {
	case "db":
		i, err := s.instance(c, name)
		if err != nil {
			return nil, err
		}
		return &i.TagList, nil
	case "cluster":
		cl, err := s.cluster(c, name)
		if err != nil {
			return nil, err
		}
		return &cl.TagList, nil
	case "subgrp":
		if g := s.st.Subnets[key(c, name)]; g != nil {
			return &g.Tags, nil
		}
		return nil, core.Errorf(404, "DBSubnetGroupNotFoundFault", "DBSubnetGroup %s not found.", name)
	case "pg", "cluster-pg":
		if g := s.st.Params[pgKey(c, name, kind == "cluster-pg")]; g != nil {
			return &g.Tags, nil
		}
		return nil, core.Errorf(404, "DBParameterGroupNotFound", "DBParameterGroup not found: %s", name)
	case "snapshot":
		sn, err := s.snapshot(c, name)
		if err != nil {
			return nil, err
		}
		return &sn.TagList, nil
	case "cluster-snapshot":
		sn, err := s.clusterSnapshot(c, name)
		if err != nil {
			return nil, err
		}
		return &sn.TagList, nil
	}
	return nil, invalid("The specified resource name does not match an RDS resource in this region: %s", arn)
}

func (s *Service) addTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	*t = AddTags(*t, in.Tags)
	return nil, nil
}

func (s *Service) removeTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceName)
	if err != nil {
		return nil, err
	}
	*t = RemoveTags(*t, in.TagKeys)
	return nil, nil
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
	docker := len(st.containers()) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	for _, i := range st.Instances {
		s.box.Resume(&i.Pogi, i.spec(), docker)
	}
	for _, cl := range st.Clusters {
		s.box.Resume(&cl.Pogi, cl.spec(), docker)
	}
	s.reload(old)
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
	s.closeFronts()
	s.mu.Lock()
	keep := s.st.containers()
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

func endpoint(e *Endpoint) string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", e.Address, e.Port)
}

// Resources lists instances and clusters for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	inst := core.Table{Name: "DB instances", Columns: []string{"Identifier", "Engine", "Status", "Endpoint", "Class", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Instances) {
		c := ctxOf(k)
		v := s.view(c, s.st.Instances[k])
		inst.Rows = append(inst.Rows, []string{v.DBInstanceIdentifier, v.Engine + " " + v.EngineVersion, StatusCell(v.DBInstanceStatus, &s.st.Instances[k].Pogi), endpoint(v.Endpoint),
			v.DBInstanceClass, c.Region, c.Account})
	}
	cls := core.Table{Name: "DB clusters", Columns: []string{"Identifier", "Engine", "Status", "Endpoint", "Members", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		c := ctxOf(k)
		v := s.clusterView(c, s.st.Clusters[k])
		cls.Rows = append(cls.Rows, []string{v.DBClusterIdentifier, v.Engine + " " + v.EngineVersion, StatusCell(v.Status, &s.st.Clusters[k].Pogi),
			fmt.Sprintf("%s:%d", v.Endpoint, v.Port), fmt.Sprint(len(v.DBClusterMembers)), c.Region, c.Account})
	}
	return []core.Table{inst, cls}
}

// Inspect returns an instance or cluster with pogi's side: container, master
// password, state and pogiNote.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if table == "DB clusters" {
		for k, cl := range s.st.Clusters {
			if cl.DBClusterIdentifier == id {
				return s.clusterView(ctxOf(k), cl)
			}
		}
		return nil
	}
	for k, i := range s.st.Instances {
		if i.DBInstanceIdentifier == id {
			return s.view(ctxOf(k), i)
		}
	}
	return nil
}
