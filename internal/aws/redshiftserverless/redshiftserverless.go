// Package redshiftserverless emulates Amazon Redshift Serverless over JSON
// 1.1. A namespace holds the admin login and the first database; its
// workgroup runs them in a PostgreSQL container on localhost, as a Redshift
// cluster does. Snapshots and usage limits are metadata.
package redshiftserverless

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/aws/redshift"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Redshift Serverless emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	box *rds.Box
	mu  sync.Mutex
	st  state
}

type state struct {
	Namespaces map[string]*Namespace        `json:"namespaces"` // account/region/name
	Workgroups map[string]*Workgroup        `json:"workgroups"`
	Snapshots  map[string]*Snapshot         `json:"snapshots"`
	Limits     map[string]*UsageLimit       `json:"usageLimits"` // account/region/id
	Tags       map[string]map[string]string `json:"tags"`        // resource ARN → key → value
}

func newState() state {
	return state{Namespaces: map[string]*Namespace{}, Workgroups: map[string]*Workgroup{}, Snapshots: map[string]*Snapshot{},
		Limits: map[string]*UsageLimit{}, Tags: map[string]map[string]string{}}
}

func (st *state) containers() map[string]bool {
	out := map[string]bool{}
	for _, w := range st.Workgroups {
		if w.Pogi.Container != "" {
			out[w.Pogi.Container] = true
		}
	}
	return out
}

// New creates the Redshift Serverless service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.box = rds.NewBox(hub, "redshift-serverless", "failed", &s.mu, s.owns)
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateNamespace":     core.H(s.createNamespace),
		"GetNamespace":        core.H(s.getNamespace),
		"ListNamespaces":      core.H(s.listNamespaces),
		"UpdateNamespace":     core.H(s.updateNamespace),
		"DeleteNamespace":     core.H(s.deleteNamespace),
		"CreateWorkgroup":     core.H(s.createWorkgroup),
		"GetWorkgroup":        core.H(s.getWorkgroup),
		"ListWorkgroups":      core.H(s.listWorkgroups),
		"UpdateWorkgroup":     core.H(s.updateWorkgroup),
		"DeleteWorkgroup":     core.H(s.deleteWorkgroup),
		"GetCredentials":      core.H(s.getCredentials),
		"CreateSnapshot":      core.H(s.createSnapshot),
		"GetSnapshot":         core.H(s.getSnapshot),
		"ListSnapshots":       core.H(s.listSnapshots),
		"UpdateSnapshot":      core.H(s.updateSnapshot),
		"DeleteSnapshot":      core.H(s.deleteSnapshot),
		"CreateUsageLimit":    core.H(s.createUsageLimit),
		"GetUsageLimit":       core.H(s.getUsageLimit),
		"ListUsageLimits":     core.H(s.listUsageLimits),
		"UpdateUsageLimit":    core.H(s.updateUsageLimit),
		"DeleteUsageLimit":    core.H(s.deleteUsageLimit),
		"TagResource":         core.H(s.tagResource),
		"UntagResource":       core.H(s.untagResource),
		"ListTagsForResource": core.H(s.listTags),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "redshift-serverless", Cloud: core.AWS, Title: "Redshift Serverless", Targets: []string{"RedshiftServerless"},
		Signing: []string{"redshift-serverless"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// ---- API shapes (stored as is; pogi's fields are cleared in views) ----

// Namespace is a namespace: the admin login and first database.
type Namespace struct {
	NamespaceArn      string    `json:"namespaceArn"`
	NamespaceId       string    `json:"namespaceId"`
	NamespaceName     string    `json:"namespaceName"`
	AdminUsername     string    `json:"adminUsername"`
	DbName            string    `json:"dbName"`
	KmsKeyId          string    `json:"kmsKeyId"`
	DefaultIamRoleArn string    `json:"defaultIamRoleArn,omitempty"`
	IamRoles          []string  `json:"iamRoles,omitempty"`
	LogExports        []string  `json:"logExports,omitempty"`
	Status            string    `json:"status"`
	CreationDate      time.Time `json:"creationDate"`
	Password          string    `json:"pogiAdminPassword,omitempty"` // pogi's
}

// Workgroup is a workgroup; it owns the container of its namespace's database.
type Workgroup struct {
	WorkgroupArn       string            `json:"workgroupArn"`
	WorkgroupId        string            `json:"workgroupId"`
	WorkgroupName      string            `json:"workgroupName"`
	NamespaceName      string            `json:"namespaceName"`
	BaseCapacity       int               `json:"baseCapacity"`
	MaxCapacity        int               `json:"maxCapacity,omitempty"`
	ConfigParameters   []ConfigParameter `json:"configParameters,omitempty"`
	EnhancedVpcRouting bool              `json:"enhancedVpcRouting"`
	PubliclyAccessible bool              `json:"publiclyAccessible"`
	SecurityGroupIds   []string          `json:"securityGroupIds,omitempty"`
	SubnetIds          []string          `json:"subnetIds,omitempty"`
	Port               int               `json:"port"`
	Endpoint           *Endpoint         `json:"endpoint,omitempty"` // in views only
	Status             string            `json:"status,omitempty"`   // in views only, from Pogi
	TrackName          string            `json:"trackName"`
	CreationDate       time.Time         `json:"creationDate"`
	Pogi               *rds.Server       `json:"pogi,omitempty"` // pogi's
}

// ConfigParameter is a database setting of a workgroup.
type ConfigParameter struct {
	ParameterKey   string `json:"parameterKey"`
	ParameterValue string `json:"parameterValue"`
}

// Endpoint is where a workgroup listens.
type Endpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// Snapshot is a snapshot of a namespace.
// ponytail: metadata only, like Redshift's: restoring needs a dump of the container.
type Snapshot struct {
	SnapshotName            string    `json:"snapshotName"`
	SnapshotArn             string    `json:"snapshotArn"`
	NamespaceName           string    `json:"namespaceName"`
	NamespaceArn            string    `json:"namespaceArn"`
	AdminUsername           string    `json:"adminUsername"`
	OwnerAccount            string    `json:"ownerAccount"`
	KmsKeyId                string    `json:"kmsKeyId"`
	SnapshotCreateTime      time.Time `json:"snapshotCreateTime"`
	SnapshotRetentionPeriod int       `json:"snapshotRetentionPeriod"`
	Status                  string    `json:"status"`
}

// UsageLimit is a usage limit of a workgroup; pogi never breaches it.
type UsageLimit struct {
	UsageLimitId  string `json:"usageLimitId"`
	UsageLimitArn string `json:"usageLimitArn"`
	ResourceArn   string `json:"resourceArn"`
	UsageType     string `json:"usageType"`
	Amount        int64  `json:"amount"`
	Period        string `json:"period"`
	BreachAction  string `json:"breachAction"`
}

// Tag is a resource tag.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ---- helpers ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}
func conflict(format string, a ...any) error {
	return core.Errorf(400, "ConflictException", format, a...)
}
func notFound(format string, a ...any) error {
	return core.Errorf(400, "ResourceNotFoundException", format, a...)
}

func key(c *core.Ctx, name string) string { return c.Account + "/" + c.Region + "/" + name }
func mine(c *core.Ctx, k string) bool     { return strings.HasPrefix(k, c.Account+"/"+c.Region+"/") }

// ctxOf rebuilds the account and region of a state key.
func ctxOf(k string) *core.Ctx {
	p := strings.SplitN(k, "/", 3)
	return &core.Ctx{Account: p[0], Region: p[1]}
}

func set[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

var nameRe = regexp.MustCompile(`^[a-z0-9-]{3,64}$`)

func checkName(kind, name string) error {
	if !nameRe.MatchString(name) {
		return invalid("%s name must be 3 to 64 lowercase letters, digits or hyphens: %q", kind, name)
	}
	return nil
}

func checkPort(port int) error {
	if port < 5431 || port > 5455 && port < 8191 || port > 8215 {
		return invalid("The port must be in 5431-5455 or 8191-8215.")
	}
	return nil
}

// page is a List output: name holds the items after token in key order, at
// most max of them (default and cap 100), and nextToken says where more follow.
func page[T any](name string, items []T, key func(T) string, token string, max int) map[string]any {
	items, next := rds.Page(items, key, token, max)
	out := map[string]any{name: items}
	if next != "" {
		out["nextToken"] = next
	}
	return out
}

// owns reports whether srv still belongs to a workgroup; lock held.
func (s *Service) owns(srv *rds.Server) bool {
	for _, w := range s.st.Workgroups {
		if w.Pogi == srv {
			return true
		}
	}
	return false
}

// nsOf is the namespace of a workgroup, which cannot go first; lock held.
func (s *Service) nsOf(c *core.Ctx, w *Workgroup) *Namespace {
	if ns := s.st.Namespaces[key(c, w.NamespaceName)]; ns != nil {
		return ns
	}
	return &Namespace{}
}

// spec is the container of a workgroup, with its namespace's login; lock held.
func (s *Service) spec(c *core.Ctx, w *Workgroup) rds.Spec {
	ns := s.nsOf(c, w)
	return redshift.Spec(ns.AdminUsername, ns.Password, ns.DbName, w.Pogi.HostPort)
}

func nsView(ns *Namespace) *Namespace {
	v := rds.Clone(ns)
	v.Password = ""
	return v
}

func wgView(w *Workgroup) *Workgroup {
	v := rds.Clone(w)
	v.Pogi, v.Status = nil, strings.ToUpper(w.Pogi.State)
	if e := w.Pogi.Addr(); e != nil {
		v.Endpoint, v.Port = &Endpoint{e.Address, e.Port}, e.Port
	}
	return v
}

// ---- lookups (lock held) ----

func (s *Service) namespace(c *core.Ctx, name string) (*Namespace, error) {
	if ns := s.st.Namespaces[key(c, name)]; ns != nil {
		return ns, nil
	}
	return nil, notFound("Namespace %s not found.", name)
}

// workgroup finds a workgroup by name or ARN.
func (s *Service) workgroup(c *core.Ctx, name string) (*Workgroup, error) {
	if w := s.st.Workgroups[key(c, name)]; w != nil {
		return w, nil
	}
	for k, w := range s.st.Workgroups {
		if mine(c, k) && w.WorkgroupArn == name {
			return w, nil
		}
	}
	return nil, notFound("Workgroup %s not found.", name)
}

// usedBy names the workgroup of a namespace, or "".
func (s *Service) usedBy(c *core.Ctx, ns string) string {
	for k, w := range s.st.Workgroups {
		if mine(c, k) && w.NamespaceName == ns {
			return w.WorkgroupName
		}
	}
	return ""
}

// ---- namespaces ----

type namespaceInput struct {
	NamespaceName, AdminUsername, AdminUserPassword, DbName, DefaultIamRoleArn, KmsKeyId string
	FinalSnapshotName, NextToken                                                         string
	FinalSnapshotRetentionPeriod                                                         *int
	MaxResults                                                                           int
	ManageAdminPassword                                                                  bool
	IamRoles, LogExports                                                                 []string
	Tags                                                                                 []Tag
}

func checkAdmin(in *namespaceInput) error {
	switch {
	case in.ManageAdminPassword: // ponytail: no Secrets Manager secret yet
		return invalid("pogi does not manage admin passwords yet; pass adminUsername and adminUserPassword.")
	case (in.AdminUsername == "") != (in.AdminUserPassword == ""):
		return invalid("adminUsername and adminUserPassword must be given together.")
	case in.AdminUsername != "" && !redshift.ValidUser(in.AdminUsername):
		return invalid("adminUsername must be 1 to 128 lowercase letters, digits or _+.@- starting with a letter, and not PUBLIC.")
	case in.AdminUserPassword != "" && !redshift.ValidPassword(in.AdminUserPassword):
		return invalid("adminUserPassword must be 8 to 64 printable ASCII characters with an uppercase letter, a lowercase letter " +
			"and a number, and none of ' \" \\ / @ or space.")
	}
	return nil
}

func (s *Service) createNamespace(c *core.Ctx, in *namespaceInput) (any, error) {
	db := cmp.Or(in.DbName, "dev")
	if err := checkName("Namespace", in.NamespaceName); err != nil {
		return nil, err
	}
	if err := checkAdmin(in); err != nil {
		return nil, err
	}
	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}
	if !redshift.ValidDB(db) {
		return nil, invalid("dbName must be 1 to 64 lowercase letters, digits or underscores.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Namespaces[key(c, in.NamespaceName)] != nil {
		return nil, conflict("Namespace %s already exists.", in.NamespaceName)
	}
	id := core.UUID()
	// Without an admin user the namespace still gets one, so the Data API and
	// GetCredentials have a login to work with.
	ns := &Namespace{NamespaceArn: c.ARN("redshift-serverless", "namespace/"+id), NamespaceId: id, NamespaceName: in.NamespaceName,
		AdminUsername: cmp.Or(in.AdminUsername, "admin"), DbName: db, KmsKeyId: cmp.Or(in.KmsKeyId, "AWS_OWNED_KMS_KEY"),
		DefaultIamRoleArn: in.DefaultIamRoleArn, IamRoles: in.IamRoles, LogExports: in.LogExports, Status: "AVAILABLE",
		CreationDate: time.Now().UTC(), Password: cmp.Or(in.AdminUserPassword, core.RandString(28, core.AlphaNum)+"Aa1")}
	s.st.Namespaces[key(c, ns.NamespaceName)] = ns
	s.tag(ns.NamespaceArn, in.Tags)
	return map[string]any{"namespace": nsView(ns)}, nil
}

func (s *Service) getNamespace(c *core.Ctx, in *namespaceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, in.NamespaceName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"namespace": nsView(ns)}, nil
}

func (s *Service) listNamespaces(c *core.Ctx, in *namespaceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Namespace
	for k, ns := range s.st.Namespaces {
		if mine(c, k) {
			list = append(list, nsView(ns))
		}
	}
	return page("namespaces", list, func(ns *Namespace) string { return ns.NamespaceName }, in.NextToken, in.MaxResults), nil
}

func (s *Service) updateNamespace(c *core.Ctx, in *namespaceInput) (any, error) {
	if err := checkAdmin(in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, in.NamespaceName)
	if err != nil {
		return nil, err
	}
	w := s.st.Workgroups[key(c, s.usedBy(c, ns.NamespaceName))]
	if in.AdminUsername != "" && w != nil && w.Pogi.Container != "" {
		if w.Pogi.State != "available" {
			return nil, conflict("Workgroup %s is %s; the admin login can only change while it is available.", w.WorkgroupName, strings.ToUpper(w.Pogi.State))
		}
		cmd := redshift.Alter(ns.AdminUsername, in.AdminUserPassword)
		if in.AdminUsername != ns.AdminUsername {
			cmd[len(cmd)-1] = `CREATE USER "` + in.AdminUsername + `" SUPERUSER PASSWORD '` + in.AdminUserPassword + `'`
		}
		s.box.Exec(w.Pogi, cmd...)
	}
	if in.AdminUsername != "" {
		ns.AdminUsername, ns.Password = in.AdminUsername, in.AdminUserPassword
	}
	ns.DefaultIamRoleArn, ns.KmsKeyId = cmp.Or(in.DefaultIamRoleArn, ns.DefaultIamRoleArn), cmp.Or(in.KmsKeyId, ns.KmsKeyId)
	if in.IamRoles != nil {
		ns.IamRoles = in.IamRoles
	}
	if in.LogExports != nil {
		ns.LogExports = in.LogExports
	}
	return map[string]any{"namespace": nsView(ns)}, nil
}

func (s *Service) deleteNamespace(c *core.Ctx, in *namespaceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, in.NamespaceName)
	if err != nil {
		return nil, err
	}
	if w := s.usedBy(c, ns.NamespaceName); w != "" {
		return nil, conflict("Namespace %s has workgroup %s; delete the workgroup first.", ns.NamespaceName, w)
	}
	if in.FinalSnapshotName != "" {
		if _, err := s.addSnapshot(c, ns, in.FinalSnapshotName, in.FinalSnapshotRetentionPeriod, nil); err != nil {
			return nil, err
		}
	}
	delete(s.st.Namespaces, key(c, ns.NamespaceName))
	delete(s.st.Tags, ns.NamespaceArn)
	v := nsView(ns)
	v.Status = "DELETING"
	return map[string]any{"namespace": v}, nil
}

// ---- workgroups ----

type workgroupInput struct {
	WorkgroupName, NamespaceName, TrackName, NextToken string
	BaseCapacity, MaxCapacity, Port                    *int
	MaxResults                                         int
	EnhancedVpcRouting, PubliclyAccessible             *bool
	ConfigParameters                                   []ConfigParameter
	SecurityGroupIds, SubnetIds                        []string
	Tags                                               []Tag
}

func (s *Service) createWorkgroup(c *core.Ctx, in *workgroupInput) (any, error) {
	port := redshift.Port
	set(&port, in.Port)
	if err := checkName("Workgroup", in.WorkgroupName); err != nil {
		return nil, err
	}
	if err := checkPort(port); err != nil {
		return nil, err
	}
	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Workgroups[key(c, in.WorkgroupName)] != nil {
		return nil, conflict("Workgroup %s already exists.", in.WorkgroupName)
	}
	ns, err := s.namespace(c, in.NamespaceName)
	if err != nil {
		return nil, err
	}
	if w := s.usedBy(c, ns.NamespaceName); w != "" {
		return nil, conflict("Namespace %s already has workgroup %s.", ns.NamespaceName, w)
	}
	id := core.UUID()
	w := &Workgroup{WorkgroupArn: c.ARN("redshift-serverless", "workgroup/"+id), WorkgroupId: id, WorkgroupName: in.WorkgroupName,
		NamespaceName: ns.NamespaceName, BaseCapacity: 128, ConfigParameters: in.ConfigParameters, SecurityGroupIds: in.SecurityGroupIds,
		SubnetIds: in.SubnetIds, Port: port, TrackName: cmp.Or(in.TrackName, "current"), CreationDate: time.Now().UTC(), Pogi: &rds.Server{}}
	set(&w.BaseCapacity, in.BaseCapacity)
	set(&w.MaxCapacity, in.MaxCapacity)
	set(&w.EnhancedVpcRouting, in.EnhancedVpcRouting)
	set(&w.PubliclyAccessible, in.PubliclyAccessible)
	s.st.Workgroups[key(c, w.WorkgroupName)] = w
	s.tag(w.WorkgroupArn, in.Tags)
	s.box.Serve(w.Pogi, c.Account, c.Region, w.WorkgroupName, port, redshift.Port, func() rds.Spec { return s.spec(c, w) }, docker)
	return map[string]any{"workgroup": wgView(w)}, nil
}

func (s *Service) getWorkgroup(c *core.Ctx, in *workgroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.workgroup(c, in.WorkgroupName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"workgroup": wgView(w)}, nil
}

func (s *Service) listWorkgroups(c *core.Ctx, in *workgroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Workgroup
	for k, w := range s.st.Workgroups {
		if mine(c, k) {
			list = append(list, wgView(w))
		}
	}
	return page("workgroups", list, func(w *Workgroup) string { return w.WorkgroupName }, in.NextToken, in.MaxResults), nil
}

// updateWorkgroup changes settings, which are metadata.
func (s *Service) updateWorkgroup(c *core.Ctx, in *workgroupInput) (any, error) {
	if in.Port != nil {
		if err := checkPort(*in.Port); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.workgroup(c, in.WorkgroupName)
	if err != nil {
		return nil, err
	}
	if w.Pogi.State != "available" {
		return nil, conflict("Workgroup %s is %s; try again once it is AVAILABLE.", w.WorkgroupName, strings.ToUpper(w.Pogi.State))
	}
	set(&w.BaseCapacity, in.BaseCapacity)
	set(&w.MaxCapacity, in.MaxCapacity)
	set(&w.EnhancedVpcRouting, in.EnhancedVpcRouting)
	set(&w.PubliclyAccessible, in.PubliclyAccessible)
	w.TrackName = cmp.Or(in.TrackName, w.TrackName)
	if in.ConfigParameters != nil {
		w.ConfigParameters = in.ConfigParameters
	}
	if in.SecurityGroupIds != nil {
		w.SecurityGroupIds = in.SecurityGroupIds
	}
	if in.SubnetIds != nil {
		w.SubnetIds = in.SubnetIds
	}
	if in.Port != nil && w.Pogi.Container == "" { // a running container keeps its published port
		w.Port, w.Pogi.HostPort = *in.Port, *in.Port
	}
	return map[string]any{"workgroup": wgView(w)}, nil
}

// deleteWorkgroup removes the workgroup with its container (and so, unlike
// on AWS, the namespace's data) and usage limits.
func (s *Service) deleteWorkgroup(c *core.Ctx, in *workgroupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.workgroup(c, in.WorkgroupName)
	if err != nil {
		return nil, err
	}
	v := wgView(w)
	v.Status = "DELETING"
	delete(s.st.Workgroups, key(c, w.WorkgroupName))
	delete(s.st.Tags, w.WorkgroupArn)
	for k, l := range s.st.Limits {
		if l.ResourceArn == w.WorkgroupArn {
			delete(s.st.Limits, k)
			delete(s.st.Tags, l.UsageLimitArn)
		}
	}
	if w.Pogi.Container != "" {
		s.box.Remove(w.Pogi.Container)
	}
	return map[string]any{"workgroup": v}, nil
}

// ---- credentials ----

type credentialsInput struct {
	WorkgroupName, CustomDomainName, DbName string
	DurationSeconds                         int
}

// getCredentials makes a temporary login for the caller's IAM identity,
// which works on the workgroup's database right away.
func (s *Service) getCredentials(c *core.Ctx, in *credentialsInput) (any, error) {
	dur := cmp.Or(in.DurationSeconds, 900)
	if dur < 900 || dur > 3600 {
		return nil, invalid("durationSeconds must be between 900 and 3600.")
	}
	s.mu.Lock()
	w, err := s.workgroup(c, in.WorkgroupName)
	var db *redshift.DB
	if err == nil {
		db = s.db(c, w)
		if w.Pogi.Container != "" && db.Addr == "" {
			err = conflict("%s.", db.Note)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	login, pw, exp := redshift.IAMUser(c.Principal), core.RandString(40, core.AlphaNum), time.Now().Add(time.Duration(dur)*time.Second).UTC()
	if db.Addr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := db.Login(ctx, login, "", nil, false, pw, exp); err != nil {
			return nil, fmt.Errorf("pogi could not create the database login: %w", err)
		}
	}
	return map[string]any{"dbUser": login, "dbPassword": pw, "expiration": core.Time(exp), "nextRefreshTime": core.Time(exp)}, nil
}

// db is where a workgroup's database answers; lock held.
func (s *Service) db(c *core.Ctx, w *Workgroup) *redshift.DB {
	ns := s.nsOf(c, w)
	return redshift.NewDB(s.hub.Docker(), w.Pogi, ns.AdminUsername, ns.Password, ns.DbName, "Workgroup "+w.WorkgroupName)
}

// DB is where the database of a workgroup (by name or ARN) answers, for the
// Data API; nil when there is no such workgroup.
func (s *Service) DB(account, region, id string) *redshift.DB {
	c := &core.Ctx{Account: account, Region: region}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, err := s.workgroup(c, id)
	if err != nil {
		return nil
	}
	return s.db(c, w)
}

// ---- snapshots ----

type snapshotInput struct {
	SnapshotName, SnapshotArn, NamespaceName, NamespaceArn, NextToken string
	RetentionPeriod                                                   *int
	MaxResults                                                        int
	Tags                                                              []Tag
}

func (s *Service) createSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, in.NamespaceName)
	if err != nil {
		return nil, err
	}
	sn, err := s.addSnapshot(c, ns, in.SnapshotName, in.RetentionPeriod, in.Tags)
	if err != nil {
		return nil, err
	}
	return map[string]any{"snapshot": rds.Clone(sn)}, nil
}

// addSnapshot records a snapshot of a namespace; lock held.
func (s *Service) addSnapshot(c *core.Ctx, ns *Namespace, name string, retention *int, tags []Tag) (*Snapshot, error) {
	if name == "" {
		return nil, invalid("snapshotName must be given.")
	}
	if err := checkTags(tags); err != nil {
		return nil, err
	}
	if s.st.Snapshots[key(c, name)] != nil {
		return nil, conflict("Snapshot %s already exists.", name)
	}
	sn := &Snapshot{SnapshotName: name, SnapshotArn: c.ARN("redshift-serverless", "snapshot/"+core.UUID()), NamespaceName: ns.NamespaceName,
		NamespaceArn: ns.NamespaceArn, AdminUsername: ns.AdminUsername, OwnerAccount: c.Account, KmsKeyId: ns.KmsKeyId,
		SnapshotCreateTime: time.Now().UTC(), SnapshotRetentionPeriod: -1, Status: "AVAILABLE"}
	set(&sn.SnapshotRetentionPeriod, retention)
	s.st.Snapshots[key(c, name)] = sn
	s.tag(sn.SnapshotArn, tags)
	return sn, nil
}

// snapshot finds a snapshot by name or ARN; lock held.
func (s *Service) snapshot(c *core.Ctx, in *snapshotInput) (*Snapshot, error) {
	for k, sn := range s.st.Snapshots {
		if mine(c, k) && (sn.SnapshotName == in.SnapshotName || in.SnapshotArn != "" && sn.SnapshotArn == in.SnapshotArn) {
			return sn, nil
		}
	}
	return nil, notFound("Snapshot %s not found.", cmp.Or(in.SnapshotName, in.SnapshotArn))
}

func (s *Service) getSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"snapshot": rds.Clone(sn)}, nil
}

func (s *Service) listSnapshots(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Snapshot
	for k, sn := range s.st.Snapshots {
		if mine(c, k) && (in.NamespaceName == "" || sn.NamespaceName == in.NamespaceName) && (in.NamespaceArn == "" || sn.NamespaceArn == in.NamespaceArn) {
			list = append(list, rds.Clone(sn))
		}
	}
	return page("snapshots", list, func(sn *Snapshot) string { return sn.SnapshotName }, in.NextToken, in.MaxResults), nil
}

func (s *Service) updateSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in)
	if err != nil {
		return nil, err
	}
	set(&sn.SnapshotRetentionPeriod, in.RetentionPeriod)
	return map[string]any{"snapshot": rds.Clone(sn)}, nil
}

func (s *Service) deleteSnapshot(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in)
	if err != nil {
		return nil, err
	}
	delete(s.st.Snapshots, key(c, sn.SnapshotName))
	delete(s.st.Tags, sn.SnapshotArn)
	v := rds.Clone(sn)
	v.Status = "DELETED"
	return map[string]any{"snapshot": v}, nil
}

// ---- usage limits ----

type usageLimitInput struct {
	UsageLimitId, ResourceArn, UsageType, Period, BreachAction, NextToken string
	Amount                                                                *int64
	MaxResults                                                            int
}

func (s *Service) usageLimit(c *core.Ctx, id string) (*UsageLimit, error) {
	if l := s.st.Limits[key(c, id)]; l != nil {
		return l, nil
	}
	return nil, notFound("Usage limit %s not found.", id)
}

func (s *Service) createUsageLimit(c *core.Ctx, in *usageLimitInput) (any, error) {
	switch {
	case in.Amount == nil || *in.Amount < 1:
		return nil, invalid("amount must be a positive number.")
	case !slices.Contains([]string{"serverless-compute", "cross-region-datasharing"}, in.UsageType):
		return nil, invalid("usageType must be serverless-compute or cross-region-datasharing.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(in.ResourceArn) {
		return nil, notFound("Resource %s not found.", in.ResourceArn)
	}
	id := core.UUID()
	l := &UsageLimit{UsageLimitId: id, UsageLimitArn: c.ARN("redshift-serverless", "usagelimit/"+id), ResourceArn: in.ResourceArn,
		UsageType: in.UsageType, Amount: *in.Amount, Period: cmp.Or(in.Period, "monthly"), BreachAction: cmp.Or(in.BreachAction, "log")}
	s.st.Limits[key(c, id)] = l
	return map[string]any{"usageLimit": rds.Clone(l)}, nil
}

func (s *Service) getUsageLimit(c *core.Ctx, in *usageLimitInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.usageLimit(c, in.UsageLimitId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"usageLimit": rds.Clone(l)}, nil
}

func (s *Service) listUsageLimits(c *core.Ctx, in *usageLimitInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*UsageLimit
	for k, l := range s.st.Limits {
		if mine(c, k) && (in.ResourceArn == "" || l.ResourceArn == in.ResourceArn) && (in.UsageType == "" || l.UsageType == in.UsageType) {
			list = append(list, rds.Clone(l))
		}
	}
	return page("usageLimits", list, func(l *UsageLimit) string { return l.UsageLimitId }, in.NextToken, in.MaxResults), nil
}

func (s *Service) updateUsageLimit(c *core.Ctx, in *usageLimitInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.usageLimit(c, in.UsageLimitId)
	if err != nil {
		return nil, err
	}
	set(&l.Amount, in.Amount)
	l.BreachAction = cmp.Or(in.BreachAction, l.BreachAction)
	return map[string]any{"usageLimit": rds.Clone(l)}, nil
}

func (s *Service) deleteUsageLimit(c *core.Ctx, in *usageLimitInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.usageLimit(c, in.UsageLimitId)
	if err != nil {
		return nil, err
	}
	delete(s.st.Limits, key(c, l.UsageLimitId))
	delete(s.st.Tags, l.UsageLimitArn)
	return map[string]any{"usageLimit": rds.Clone(l)}, nil
}

// ---- tags ----

type tagsInput struct {
	ResourceArn string
	Tags        []Tag
	TagKeys     []string
}

// exists reports whether an ARN names a resource; lock held.
func (s *Service) exists(arn string) bool {
	for _, ns := range s.st.Namespaces {
		if ns.NamespaceArn == arn {
			return true
		}
	}
	for _, w := range s.st.Workgroups {
		if w.WorkgroupArn == arn {
			return true
		}
	}
	for _, sn := range s.st.Snapshots {
		if sn.SnapshotArn == arn {
			return true
		}
	}
	for _, l := range s.st.Limits {
		if l.UsageLimitArn == arn {
			return true
		}
	}
	return false
}

// checkTags refuses a tag with no key (AWS: key and value are both required).
func checkTags(tags []Tag) error {
	for _, t := range tags {
		if t.Key == "" {
			return invalid("Each tag needs a key.")
		}
	}
	return nil
}

// tag sets tags on a resource; lock held.
func (s *Service) tag(arn string, tags []Tag) {
	if len(tags) == 0 {
		return
	}
	if s.st.Tags[arn] == nil {
		s.st.Tags[arn] = map[string]string{}
	}
	for _, t := range tags {
		s.st.Tags[arn][t.Key] = t.Value
	}
}

func (s *Service) tagResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(in.ResourceArn) {
		return nil, notFound("Resource %s not found.", in.ResourceArn)
	}
	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}
	s.tag(in.ResourceArn, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(in.ResourceArn) {
		return nil, notFound("Resource %s not found.", in.ResourceArn)
	}
	for _, k := range in.TagKeys {
		delete(s.st.Tags[in.ResourceArn], k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exists(in.ResourceArn) {
		return nil, notFound("Resource %s not found.", in.ResourceArn)
	}
	tags := []Tag{}
	for _, k := range core.SortedKeys(s.st.Tags[in.ResourceArn]) {
		tags = append(tags, Tag{k, s.st.Tags[in.ResourceArn][k]})
	}
	return map[string]any{"tags": tags}, nil
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped workgroups go.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	docker := len(st.containers()) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	for k, w := range st.Workgroups {
		s.box.Resume(w.Pogi, s.spec(ctxOf(k), w), docker)
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

// Resources lists namespaces and workgroups for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	nss := core.Table{Name: "Namespaces", Columns: []string{"Name", "Admin user", "Database", "Workgroup", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Namespaces) {
		c, ns := ctxOf(k), s.st.Namespaces[k]
		nss.Rows = append(nss.Rows, []string{ns.NamespaceName, ns.AdminUsername, ns.DbName, s.usedBy(c, ns.NamespaceName), c.Region, c.Account})
	}
	wgs := core.Table{Name: "Workgroups", Columns: []string{"Name", "Namespace", "Status", "Endpoint", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Workgroups) {
		c, v := ctxOf(k), wgView(s.st.Workgroups[k])
		ep := ""
		if v.Endpoint != nil {
			ep = fmt.Sprintf("%s:%d", v.Endpoint.Address, v.Endpoint.Port)
		}
		wgs.Rows = append(wgs.Rows, []string{v.WorkgroupName, v.NamespaceName, v.Status, ep, c.Region, c.Account})
	}
	return []core.Table{nss, wgs}
}

// Inspect returns a namespace with its admin password, or a workgroup with
// pogi's side: container, state and pogiNote.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if table == "Namespaces" {
		for _, ns := range s.st.Namespaces {
			if ns.NamespaceName == id {
				return rds.Clone(ns)
			}
		}
		return nil
	}
	for _, w := range s.st.Workgroups {
		if w.WorkgroupName == id {
			v := wgView(w)
			v.Pogi = rds.Clone(w.Pogi)
			return v
		}
	}
	return nil
}
