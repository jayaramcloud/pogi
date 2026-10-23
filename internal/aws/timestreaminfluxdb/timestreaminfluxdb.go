// Package timestreaminfluxdb emulates Amazon Timestream for InfluxDB over
// JSON 1.0: DB instances run InfluxDB 2 in Docker, set up with the
// instance's user, organization and bucket, and their auth parameters (with
// the admin token) go to a Secrets Manager secret, as on AWS. Parameter
// groups and tags are metadata. Without Docker the API still answers.
package timestreaminfluxdb

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Timestream for InfluxDB emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	box *rds.Box
	mu  sync.Mutex
	st  state
}

type state struct {
	Instances map[string]*instance         `json:"instances"`       // account/region/id
	Params    map[string]*ParameterGroup   `json:"parameterGroups"` // account/region/id
	Tags      map[string]map[string]string `json:"tags"`            // by ARN
}

func newState() state {
	return state{Instances: map[string]*instance{}, Params: map[string]*ParameterGroup{}, Tags: map[string]map[string]string{}}
}

// New creates the Timestream for InfluxDB service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.box = rds.NewBox(hub, "timestream-influxdb", "FAILED", &s.mu, s.owns)
	s.rpc = &core.RPC{JSONVersion: "1.0", Ops: map[string]core.Handler{
		"CreateDbInstance":       core.H(s.createDbInstance),
		"GetDbInstance":          core.H(s.getDbInstance),
		"ListDbInstances":        core.H(s.listDbInstances),
		"UpdateDbInstance":       core.H(s.updateDbInstance),
		"DeleteDbInstance":       core.H(s.deleteDbInstance),
		"CreateDbParameterGroup": core.H(s.createDbParameterGroup),
		"GetDbParameterGroup":    core.H(s.getDbParameterGroup),
		"ListDbParameterGroups":  core.H(s.listDbParameterGroups),
		"TagResource":            core.H(s.tagResource),
		"UntagResource":          core.H(s.untagResource),
		"ListTagsForResource":    core.H(s.listTagsForResource),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "timestream-influxdb", Cloud: core.AWS, Title: "Timestream for InfluxDB", Targets: []string{"AmazonTimestreamInfluxDB"},
		Signing: []string{"timestream-influxdb"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// DbInstance is the API shape of a DB instance.
type DbInstance struct {
	Id                            string          `json:"id"`
	Name                          string          `json:"name"`
	Arn                           string          `json:"arn"`
	Status                        string          `json:"status"`
	Endpoint                      string          `json:"endpoint,omitempty"`
	Port                          int             `json:"port,omitempty"`
	NetworkType                   string          `json:"networkType"`
	DbInstanceType                string          `json:"dbInstanceType"`
	DbStorageType                 string          `json:"dbStorageType"`
	AllocatedStorage              int             `json:"allocatedStorage"`
	DeploymentType                string          `json:"deploymentType"`
	VpcSubnetIds                  []string        `json:"vpcSubnetIds"`
	PubliclyAccessible            bool            `json:"publiclyAccessible"`
	VpcSecurityGroupIds           []string        `json:"vpcSecurityGroupIds"`
	DbParameterGroupIdentifier    string          `json:"dbParameterGroupIdentifier,omitempty"`
	AvailabilityZone              string          `json:"availabilityZone"`
	SecondaryAvailabilityZone     string          `json:"secondaryAvailabilityZone,omitempty"`
	LogDeliveryConfiguration      json.RawMessage `json:"logDeliveryConfiguration,omitempty"`
	MaintenanceSchedule           json.RawMessage `json:"maintenanceSchedule,omitempty"`
	InfluxAuthParametersSecretArn string          `json:"influxAuthParametersSecretArn,omitempty"`
}

// instance is a DB instance with pogi's side: InfluxDB's setup and the server.
type instance struct {
	DbInstance
	Username     string     `json:"username"`
	Organization string     `json:"organization"`
	Bucket       string     `json:"bucket"`
	Token        string     `json:"token"` // the operator token of the admin user
	Pogi         rds.Server `json:"pogi"`
}

// ParameterGroup is a DB parameter group.
type ParameterGroup struct {
	Id          string          `json:"id"`
	Name        string          `json:"name"`
	Arn         string          `json:"arn"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func notFound(kind, id string) error {
	return core.Errorf(404, "ResourceNotFoundException", "The %s %s was not found.", kind, id)
}

func key(c *core.Ctx, id string) string { return c.Account + "/" + c.Region + "/" + id }
func mine(c *core.Ctx, k string) bool   { return strings.HasPrefix(k, c.Account+"/"+c.Region+"/") }

var (
	nameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]*(-[a-zA-Z0-9]+)*$`)
	userRe = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9]*(-[a-zA-Z0-9]+)*)?$`)
	pwRe   = regexp.MustCompile(`^[a-zA-Z0-9]{8,64}$`)
)

func newID() string { return core.RandString(10, "abcdefghijklmnopqrstuvwxyz0123456789") }

func (i *instance) spec() rds.Spec {
	return rds.Spec{Image: "influxdb:2", Port: 8086, Host: i.Pogi.HostPort, Data: "/var/lib/influxdb2", Hello: "GET /health HTTP/1.0\r\n\r\n",
		Env: []string{"DOCKER_INFLUXDB_INIT_MODE=setup", "DOCKER_INFLUXDB_INIT_USERNAME=" + i.Username, "DOCKER_INFLUXDB_INIT_PASSWORD=" + i.Pogi.Password,
			"DOCKER_INFLUXDB_INIT_ORG=" + i.Organization, "DOCKER_INFLUXDB_INIT_BUCKET=" + i.Bucket, "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=" + i.Token}}
}

func (s *Service) owns(srv *rds.Server) bool {
	for _, i := range s.st.Instances {
		if &i.Pogi == srv {
			return true
		}
	}
	return false
}

// view is the API shape of an instance; lock held.
func view(i *instance) *DbInstance {
	v := i.DbInstance
	v.Status = strings.ToUpper(i.Pogi.State)
	if ep := i.Pogi.Addr(); ep != nil {
		v.Endpoint, v.Port = ep.Address, ep.Port
	}
	return &v
}

func (s *Service) instance(c *core.Ctx, id string) (*instance, error) {
	if i := s.st.Instances[key(c, id)]; i != nil {
		return i, nil
	}
	return nil, notFound("DB instance", id)
}

// ---- DB instances ----

type instanceInput struct {
	Identifier, Name, Username, Password, Organization, Bucket, DbInstanceType string
	DbStorageType, DeploymentType, NetworkType, DbParameterGroupIdentifier     string
	NextToken                                                                  string
	AllocatedStorage, Port, MaxResults                                         int
	PubliclyAccessible                                                         *bool
	VpcSubnetIds, VpcSecurityGroupIds                                          []string
	LogDeliveryConfiguration, MaintenanceSchedule                              json.RawMessage
	Tags                                                                       map[string]string
}

func (in *instanceInput) check() error {
	switch {
	case len(in.Name) < 3 || len(in.Name) > 40 || !nameRe.MatchString(in.Name):
		return invalid("name must be 3 to 40 letters, digits and single hyphens, starting with a letter.")
	case !pwRe.MatchString(in.Password):
		return invalid("password must be 8 to 64 letters and digits.")
	case in.Username != "" && !userRe.MatchString(in.Username):
		return invalid("username must start with a letter and hold letters, digits and single hyphens.")
	case !strings.HasPrefix(in.DbInstanceType, "db.influx."):
		return invalid("Invalid dbInstanceType: %s", in.DbInstanceType)
	case in.AllocatedStorage < 20 || in.AllocatedStorage > 16384:
		return invalid("allocatedStorage must be 20 to 16384 GiB.")
	case len(in.VpcSubnetIds) < 1 || len(in.VpcSubnetIds) > 3 || len(in.VpcSecurityGroupIds) < 1 || len(in.VpcSecurityGroupIds) > 5:
		return invalid("Give 1 to 3 vpcSubnetIds and 1 to 5 vpcSecurityGroupIds.")
	}
	return nil
}

func (s *Service) createDbInstance(c *core.Ctx, in *instanceInput) (any, error) {
	if err := in.check(); err != nil {
		return nil, err
	}
	docker := s.box.Available()
	s.mu.Lock()
	for k, i := range s.st.Instances {
		if mine(c, k) && i.Name == in.Name {
			s.mu.Unlock()
			return nil, core.Errorf(409, "ConflictException", "A DB instance named %s already exists.", in.Name)
		}
	}
	if in.DbParameterGroupIdentifier != "" && s.st.Params[key(c, in.DbParameterGroupIdentifier)] == nil {
		s.mu.Unlock()
		return nil, notFound("DB parameter group", in.DbParameterGroupIdentifier)
	}
	id := newID()
	i := &instance{DbInstance: DbInstance{Id: id, Name: in.Name, Arn: c.ARN("timestream-influxdb", "db-instance/"+id),
		NetworkType: cmp.Or(in.NetworkType, "IPV4"), DbInstanceType: in.DbInstanceType, DbStorageType: cmp.Or(in.DbStorageType, "InfluxIOIncludedT1"),
		AllocatedStorage: in.AllocatedStorage, DeploymentType: cmp.Or(in.DeploymentType, "SINGLE_AZ"), VpcSubnetIds: in.VpcSubnetIds,
		VpcSecurityGroupIds: in.VpcSecurityGroupIds, DbParameterGroupIdentifier: in.DbParameterGroupIdentifier, AvailabilityZone: c.Region + "a",
		LogDeliveryConfiguration: in.LogDeliveryConfiguration, MaintenanceSchedule: in.MaintenanceSchedule},
		Username: cmp.Or(in.Username, "admin"), Organization: cmp.Or(in.Organization, "default"), Bucket: cmp.Or(in.Bucket, "default"),
		Token: core.RandString(64, core.AlphaNum)}
	if in.PubliclyAccessible != nil {
		i.PubliclyAccessible = *in.PubliclyAccessible
	}
	if i.DeploymentType == "WITH_MULTIAZ_STANDBY" {
		i.SecondaryAvailabilityZone = c.Region + "b"
	}
	i.Pogi.Password = in.Password
	s.st.Instances[key(c, id)] = i
	s.st.Tags[i.Arn] = in.Tags
	s.box.Serve(&i.Pogi, c.Account, c.Region, in.Name, in.Port, 8086, i.spec, docker)
	auth, _ := json.Marshal(map[string]string{"username": i.Username, "password": in.Password, "organization": i.Organization, "bucket": i.Bucket, "token": i.Token})
	s.mu.Unlock()
	// as on AWS, the auth parameters land in Secrets Manager (pogi adds the admin token); without it only Inspect has them
	var secret struct{ ARN string }
	if s.hub.Call(c.Account, c.Region, "secretsmanager", "CreateSecret",
		map[string]string{"Name": "READONLY-InfluxDB-auth-parameters-" + id, "SecretString": string(auth)}, &secret) == nil {
		s.mu.Lock()
		i.InfluxAuthParametersSecretArn = secret.ARN
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return view(i), nil
}

func (s *Service) getDbInstance(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(c, in.Identifier)
	if err != nil {
		return nil, err
	}
	return view(i), nil
}

func (s *Service) listDbInstances(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*instance
	for k, i := range s.st.Instances {
		if mine(c, k) {
			all = append(all, i)
		}
	}
	all, next := rds.Page(all, func(i *instance) string { return i.Name }, in.NextToken, in.MaxResults)
	items := make([]map[string]any, len(all))
	for n, i := range all {
		v := view(i)
		items[n] = map[string]any{"id": v.Id, "name": v.Name, "arn": v.Arn, "status": v.Status, "endpoint": v.Endpoint, "port": v.Port,
			"networkType": v.NetworkType, "dbInstanceType": v.DbInstanceType, "dbStorageType": v.DbStorageType,
			"allocatedStorage": v.AllocatedStorage, "deploymentType": v.DeploymentType}
	}
	return map[string]any{"items": items, "nextToken": next}, nil
}

func (s *Service) updateDbInstance(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(c, in.Identifier)
	switch {
	case err != nil:
		return nil, err
	case in.DbInstanceType != "" && !strings.HasPrefix(in.DbInstanceType, "db.influx."):
		return nil, invalid("Invalid dbInstanceType: %s", in.DbInstanceType)
	case in.AllocatedStorage != 0 && in.AllocatedStorage < i.AllocatedStorage:
		return nil, invalid("allocatedStorage can only grow.")
	case in.DbParameterGroupIdentifier != "" && s.st.Params[key(c, in.DbParameterGroupIdentifier)] == nil:
		return nil, notFound("DB parameter group", in.DbParameterGroupIdentifier)
	}
	i.DbInstanceType = cmp.Or(in.DbInstanceType, i.DbInstanceType)
	i.DbStorageType = cmp.Or(in.DbStorageType, i.DbStorageType)
	i.AllocatedStorage = cmp.Or(in.AllocatedStorage, i.AllocatedStorage)
	i.DbParameterGroupIdentifier = cmp.Or(in.DbParameterGroupIdentifier, i.DbParameterGroupIdentifier)
	if in.DeploymentType != "" {
		i.DeploymentType, i.SecondaryAvailabilityZone = in.DeploymentType, ""
		if in.DeploymentType == "WITH_MULTIAZ_STANDBY" {
			i.SecondaryAvailabilityZone = c.Region + "b"
		}
	}
	if in.LogDeliveryConfiguration != nil {
		i.LogDeliveryConfiguration = in.LogDeliveryConfiguration
	}
	if in.MaintenanceSchedule != nil {
		i.MaintenanceSchedule = in.MaintenanceSchedule
	}
	if in.Port > 0 && i.Pogi.Container == "" { // a running container keeps its published port
		i.Pogi.HostPort = in.Port
	}
	return view(i), nil
}

func (s *Service) deleteDbInstance(c *core.Ctx, in *instanceInput) (any, error) {
	s.mu.Lock()
	i, err := s.instance(c, in.Identifier)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	v := view(i)
	v.Status = "DELETING"
	delete(s.st.Instances, key(c, i.Id))
	delete(s.st.Tags, i.Arn)
	if i.Pogi.Container != "" {
		s.box.Remove(i.Pogi.Container)
	}
	s.mu.Unlock()
	if v.InfluxAuthParametersSecretArn != "" {
		s.hub.Call(c.Account, c.Region, "secretsmanager", "DeleteSecret",
			map[string]any{"SecretId": v.InfluxAuthParametersSecretArn, "ForceDeleteWithoutRecovery": true}, nil)
	}
	return v, nil
}

// ---- parameter groups ----

type paramInput struct {
	Identifier, Name, Description, NextToken string
	Parameters                               json.RawMessage
	Tags                                     map[string]string
	MaxResults                               int
}

func (s *Service) createDbParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	if len(in.Name) < 3 || len(in.Name) > 64 || !nameRe.MatchString(in.Name) {
		return nil, invalid("name must be 3 to 64 letters, digits and single hyphens, starting with a letter.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, g := range s.st.Params {
		if mine(c, k) && g.Name == in.Name {
			return nil, core.Errorf(409, "ConflictException", "A DB parameter group named %s already exists.", in.Name)
		}
	}
	id := newID()
	g := &ParameterGroup{Id: id, Name: in.Name, Arn: c.ARN("timestream-influxdb", "db-parameter-group/"+id), Description: in.Description, Parameters: in.Parameters}
	s.st.Params[key(c, id)] = g
	s.st.Tags[g.Arn] = in.Tags
	v := *g
	return &v, nil
}

func (s *Service) getDbParameterGroup(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Params[key(c, in.Identifier)]
	if g == nil {
		return nil, notFound("DB parameter group", in.Identifier)
	}
	v := *g
	return &v, nil
}

func (s *Service) listDbParameterGroups(c *core.Ctx, in *paramInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*ParameterGroup
	for k, g := range s.st.Params {
		if mine(c, k) {
			all = append(all, &ParameterGroup{Id: g.Id, Name: g.Name, Arn: g.Arn, Description: g.Description})
		}
	}
	all, next := rds.Page(all, func(g *ParameterGroup) string { return g.Name }, in.NextToken, in.MaxResults)
	return map[string]any{"items": all, "nextToken": next}, nil
}

// ---- tags ----

type tagsInput struct {
	ResourceArn string
	Tags        map[string]string
	TagKeys     []string
}

// tags checks that an ARN names a resource and returns its tags; lock held.
func (s *Service) tags(arn string) (map[string]string, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	kind, id, _ := strings.Cut(res, "/")
	c := &core.Ctx{Account: account, Region: region}
	switch {
	case !ok:
		return nil, invalid("Invalid resourceArn: %s", arn)
	case kind == "db-instance" && s.st.Instances[key(c, id)] != nil, kind == "db-parameter-group" && s.st.Params[key(c, id)] != nil:
		return s.st.Tags[arn], nil
	}
	return nil, notFound("resource", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	t = maps.Clone(t)
	if t == nil {
		t = map[string]string{}
	}
	maps.Copy(t, in.Tags)
	s.st.Tags[in.ResourceArn] = t
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	t = maps.Clone(t)
	for _, k := range in.TagKeys {
		delete(t, k)
	}
	s.st.Tags[in.ResourceArn] = t
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tags(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"tags": maps.Clone(t)}, nil
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name; those of
// dropped instances go.
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
	for _, i := range st.Instances {
		s.box.Resume(&i.Pogi, i.spec(), docker)
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
	for _, i := range st.Instances {
		if i.Pogi.Container != "" {
			out[i.Pogi.Container] = true
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

// Resources lists the DB instances for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "DB instances", Columns: []string{"Name", "Id", "Status", "Endpoint", "Organization", "Bucket", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Instances) {
		i, p := s.st.Instances[k], strings.SplitN(k, "/", 3)
		v := view(i)
		ep := ""
		if v.Endpoint != "" {
			ep = v.Endpoint + ":" + strconv.Itoa(v.Port)
		}
		t.Rows = append(t.Rows, []string{i.Name, i.Id, rds.StatusCell(v.Status, &i.Pogi), ep, i.Organization, i.Bucket, p[1], p[0]})
	}
	return []core.Table{t}
}

// Inspect returns an instance with pogi's side: the admin user, password and
// token, the container, state and pogiNote.
func (s *Service) Inspect(table, name string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, i := range s.st.Instances {
		if i.Name == name {
			v := *i
			v.DbInstance = *view(i)
			return &v
		}
	}
	return nil
}
