package sql

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

var nameRe = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,95}[a-z0-9])?$`)

// instanceStates maps a container's state to the instance's.
func instanceState(srv string) string {
	switch srv {
	case "available", "stopped":
		return "RUNNABLE"
	case "creating":
		return "PENDING_CREATE"
	case "failed":
		return "FAILED"
	}
	return "MAINTENANCE" // starting, stopping, rebooting
}

// view is the instance as the API shows it; base is the API's URL ("" for none). Lock held.
func (s *Service) view(base string, i *instance) *sqladmin.DatabaseInstance {
	v := clone(i.Spec)
	v.State = instanceState(i.Pogi.State)
	v.Settings.IpConfiguration.ForceSendFields = []string{"Ipv4Enabled"}
	if base != "" {
		v.SelfLink = base + "/projects/" + v.Project + "/instances/" + v.Name
	}
	if a := i.Pogi.Addr(); a != nil {
		v.IpAddresses = []*sqladmin.IpMapping{{Type: "PRIMARY", IpAddress: fmt.Sprintf("127.0.0.1:%d", a.Port)}}
	}
	return v
}

// instance finds the instance a call names. Lock held.
func (s *Service) instance(q *req) (*instance, error) {
	if i := s.st.Instances[q.key()]; i != nil {
		return i, nil
	}
	return nil, notFound("instance")
}

// running is an instance a command can run in: its container answers.
// Without Docker nothing runs and commands are only recorded.
func running(i *instance) error {
	if i.Pogi.Container != "" && i.Pogi.State != "available" {
		return busy
	}
	return nil
}

// ---- operations ----

// newOp records an operation on an instance; wait is the container state it
// lasts for ("" finishes at once), failure says why it failed. Lock held.
func (s *Service) newOp(project, name, typ, wait, failure string) *op {
	now := stamp(time.Now())
	o := &op{Wait: wait, Op: &sqladmin.Operation{Kind: "sql#operation", Name: core.UUID(), OperationType: typ, TargetId: name,
		TargetProject: project, InsertTime: now, StartTime: now, EndTime: now, Status: "DONE"}}
	if failure != "" {
		o.Op.Error = &sqladmin.OperationErrors{Kind: "sql#operationErrors", Errors: []*sqladmin.OperationError{
			{Kind: "sql#operationError", Code: "ERROR_RDBMS", Message: failure}}}
	}
	s.st.Ops[project+"/"+o.Op.Name] = o
	return o
}

// opView is an operation as the API shows it: RUNNING while its instance is
// in the container state it waits for. Lock held.
func (s *Service) opView(base string, o *op) *sqladmin.Operation {
	v := clone(o.Op)
	v.SelfLink = base + "/projects/" + v.TargetProject + "/operations/" + v.Name
	v.TargetLink = base + "/projects/" + v.TargetProject + "/instances/" + v.TargetId
	if i := s.st.Instances[v.TargetProject+"/"+v.TargetId]; i != nil && o.Wait != "" {
		switch i.Pogi.State {
		case o.Wait:
			v.Status, v.EndTime = "RUNNING", ""
		case "failed":
			v.Error = &sqladmin.OperationErrors{Kind: "sql#operationErrors", Errors: []*sqladmin.OperationError{
				{Kind: "sql#operationError", Code: "INTERNAL_ERROR", Message: i.Pogi.Note}}}
		}
	}
	return v
}

// ---- instances ----

func (s *Service) list(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []*sqladmin.DatabaseInstance{}
	for _, k := range core.SortedKeys(s.st.Instances) {
		if i := s.st.Instances[k]; i.Spec.Project == q.project {
			items = append(items, s.view(q.base(), i))
		}
	}
	return &sqladmin.InstancesListResponse{Kind: "sql#instancesList", Items: items}, nil
}

func (s *Service) get(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	return s.view(q.base(), i), nil
}

func (s *Service) insert(q *req) (any, error) {
	in := &sqladmin.DatabaseInstance{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	if !strings.Contains(string(q.body), `"ipv4Enabled"`) { // a bool cannot say "not set": it defaults to true
		defaults(in, cmp.Or(in.Region, "us-central1"))
		in.Settings.IpConfiguration.Ipv4Enabled = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.create(q.project, in, in.RootPassword, "")
	if err != nil {
		return nil, err
	}
	return s.opView(q.base(), o), nil
}

// create adds an instance and starts its container; seed is a dump the
// container loads. Lock held.
func (s *Service) create(project string, in *sqladmin.DatabaseInstance, rootPassword, seed string) (*op, error) {
	if !nameRe.MatchString(in.Name) {
		return nil, fail(400, "invalidRequest", "Invalid request: Invalid instance name: %q. Names are lowercase letters, digits and hyphens, and start with a letter.", in.Name)
	}
	e, ok := engineOf(cmp.Or(in.DatabaseVersion, "MYSQL_8_0"))
	if !ok {
		return nil, fail(400, "invalidRequest", "Invalid request: Invalid value for: databaseVersion %q.", in.DatabaseVersion)
	}
	if s.st.Instances[project+"/"+in.Name] != nil {
		return nil, fail(409, "instanceAlreadyExists", "The Cloud SQL instance already exists.")
	}
	region := cmp.Or(in.Region, "us-central1")
	spec := &sqladmin.DatabaseInstance{Kind: "sql#instance", Name: in.Name, Project: project, Region: region,
		DatabaseVersion: cmp.Or(in.DatabaseVersion, "MYSQL_8_0"), DatabaseInstalledVersion: cmp.Or(in.DatabaseVersion, "MYSQL_8_0"),
		ConnectionName: project + ":" + region + ":" + in.Name, GceZone: region + "-a", InstanceType: "CLOUD_SQL_INSTANCE",
		BackendType: "SECOND_GEN", Etag: core.RandHex(16), CreateTime: stamp(time.Now()), Settings: in.Settings,
		ServiceAccountEmailAddress: "p" + core.RandString(12, "0123456789") + "-" + core.RandString(6, "abcdefghijklmnopqrstuvwxyz0123456789") + "@gcp-sa-cloud-sql.iam.gserviceaccount.com",
		MasterInstanceName:         in.MasterInstanceName}
	spec.SelfLink = ""
	defaults(spec, region)
	authority := newCA()
	spec.ServerCaCert = authority.info(in.Name)
	i := &instance{Spec: spec, CA: authority,
		Users: []*sqladmin.User{{Kind: "sql#user", Name: e.admin, Host: map[bool]string{true: "%"}[e.name == "mysql"], Type: "BUILT_IN", Instance: in.Name, Project: project}}}
	for _, d := range systemDatabases[e.name] {
		i.Databases = append(i.Databases, &sqladmin.Database{Kind: "sql#database", Name: d, Charset: charset(e.name), Collation: collation(e.name), Instance: in.Name, Project: project})
	}
	i.Pogi.Password = cmp.Or(rootPassword, core.RandString(24, core.AlphaNum))
	i.Pogi.Seed = seed
	s.st.Instances[project+"/"+in.Name] = i
	docker := s.box.Available()
	s.box.Serve(&i.Pogi, project, region, in.Name, 0, e.port, s.spec(i), docker)
	wait := ""
	if i.Pogi.Container != "" {
		wait = "creating"
	}
	return s.newOp(project, in.Name, "CREATE", wait, ""), nil
}

func defaults(i *sqladmin.DatabaseInstance, region string) {
	if i.Settings == nil {
		i.Settings = &sqladmin.Settings{}
	}
	st, yes := i.Settings, true
	st.Kind = "sql#settings"
	st.Tier = cmp.Or(st.Tier, "db-custom-1-3840")
	st.ActivationPolicy = cmp.Or(st.ActivationPolicy, "ALWAYS")
	st.AvailabilityType = cmp.Or(st.AvailabilityType, "ZONAL")
	st.DataDiskType = cmp.Or(st.DataDiskType, "PD_SSD")
	st.PricingPlan = cmp.Or(st.PricingPlan, "PER_USE")
	st.ReplicationType = cmp.Or(st.ReplicationType, "SYNCHRONOUS")
	st.Edition = cmp.Or(st.Edition, "ENTERPRISE")
	if st.DataDiskSizeGb == 0 {
		st.DataDiskSizeGb = 10
	}
	if st.StorageAutoResize == nil {
		st.StorageAutoResize = &yes
	}
	if st.SettingsVersion == 0 {
		st.SettingsVersion = 1
	}
	if st.IpConfiguration == nil {
		st.IpConfiguration = &sqladmin.IpConfiguration{}
	}
	if st.BackupConfiguration == nil {
		st.BackupConfiguration = &sqladmin.BackupConfiguration{Kind: "sql#backupConfiguration", StartTime: "23:00"}
	}
	if st.LocationPreference == nil {
		st.LocationPreference = &sqladmin.LocationPreference{Kind: "sql#locationPreference", Zone: region + "-a"}
	}
}

func (s *Service) patch(q *req) (any, error)  { return s.modify(q, false) }
func (s *Service) update(q *req) (any, error) { return s.modify(q, true) }

// modify merges a JSON patch into an instance (a PUT replaces its settings),
// bumps settingsVersion and acts on a change of activationPolicy: NEVER stops
// the container and ALWAYS starts it. A rootPassword is set in the engine.
// ponytail: a new tier, disk size or database flag is stored but does not reach the container.
func (s *Service) modify(q *req, replace bool) (any, error) {
	var patch map[string]any
	if err := q.into(&patch); err != nil {
		return nil, err
	}
	s.mu.Lock()
	i, err := s.instance(q)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	cur := map[string]any{}
	b, _ := json.Marshal(i.Spec)
	json.Unmarshal(b, &cur)
	if st, ok := patch["settings"]; ok && replace {
		cur["settings"] = st
		delete(patch, "settings")
	}
	merge(cur, patch)
	b, _ = json.Marshal(cur)
	next := &sqladmin.DatabaseInstance{}
	if err := json.Unmarshal(b, next); err != nil {
		s.mu.Unlock()
		return nil, fail(400, "invalidRequest", "Invalid request: %v", err)
	}
	old := i.Spec
	if e, ok := engineOf(next.DatabaseVersion); !ok || e.name != i.engine().name {
		s.mu.Unlock()
		return nil, fail(400, "invalidRequest", "Invalid request: databaseVersion %q cannot replace %q.", next.DatabaseVersion, old.DatabaseVersion)
	}
	next.Name, next.Project, next.Region, next.ConnectionName, next.CreateTime, next.ServerCaCert, next.Kind =
		old.Name, old.Project, old.Region, old.ConnectionName, old.CreateTime, old.ServerCaCert, old.Kind
	next.ServiceAccountEmailAddress, next.GceZone, next.InstanceType, next.BackendType = old.ServiceAccountEmailAddress, old.GceZone, old.InstanceType, old.BackendType
	next.RootPassword, next.State, next.IpAddresses, next.SelfLink = "", "", nil, ""
	defaults(next, old.Region)
	next.Settings.SettingsVersion = old.Settings.SettingsVersion + 1
	next.Etag = core.RandHex(16)
	i.Spec = next
	wait := ""
	switch policy := next.Settings.ActivationPolicy; {
	case policy == "NEVER" && old.Settings.ActivationPolicy != "NEVER" && i.Pogi.Container != "" && i.Pogi.State == "available":
		i.Pogi.State, wait = "stopping", "stopping"
		s.box.Stop(&i.Pogi)
	case policy != "NEVER" && old.Settings.ActivationPolicy == "NEVER" && i.Pogi.Container != "" && i.Pogi.State == "stopped":
		i.Pogi.State, wait = "starting", "starting"
		s.box.Up(&i.Pogi, s.spec(i)(), "start")
	}
	o := s.newOp(i.Spec.Project, i.Spec.Name, "UPDATE", wait, "")
	view := s.opView(q.base(), o)
	pw, _ := patch["rootPassword"].(string)
	s.mu.Unlock()
	if pw != "" {
		if err := s.setPassword(q, i.Spec.Project, i.Spec.Name, "", "", pw); err != nil {
			return nil, err
		}
	}
	return view, nil
}

// merge copies src into dst: objects merge, anything else replaces.
func merge(dst, src map[string]any) {
	for k, v := range src {
		if sm, ok := v.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				merge(dm, sm)
				continue
			}
		}
		dst[k] = v
	}
}

func (s *Service) delete(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	if i.Spec.Settings.DeletionProtectionEnabled {
		return nil, fail(400, "invalidRequest", "The instance or operation is not in an appropriate state to handle the request: deletion protection is enabled.")
	}
	delete(s.st.Instances, q.key())
	if i.Pogi.Container != "" {
		s.box.Remove(i.Pogi.Container)
	}
	return s.opView(q.base(), s.newOp(q.project, q.inst, "DELETE", "", "")), nil
}

func (s *Service) restart(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	if i.Pogi.State != "available" {
		return nil, busy
	}
	wait := ""
	if i.Pogi.Container != "" {
		i.Pogi.State, wait = "rebooting", "rebooting"
		s.box.Up(&i.Pogi, s.spec(i)(), "restart")
	}
	return s.opView(q.base(), s.newOp(q.project, q.inst, "RESTART", wait, "")), nil
}

// dir is where dumps of clones wait for their container to load them. Lock held.
func (s *Service) files() string {
	if s.dir == "" {
		s.dir, _ = os.MkdirTemp("", "pogi-cloudsql-")
	}
	return s.dir
}

// cloneInstance makes a new instance from a running one, with its databases
// and, for PostgreSQL and MySQL, their data.
// ponytail: no point-in-time clones, and a SQL Server clone starts empty.
func (s *Service) cloneInstance(q *req) (any, error) {
	var in sqladmin.InstancesCloneRequest
	if err := q.into(&in); err != nil {
		return nil, err
	}
	if in.CloneContext == nil || in.CloneContext.DestinationInstanceName == "" {
		return nil, fail(400, "invalidRequest", "Invalid request: Missing parameter: cloneContext.destinationInstanceName.")
	}
	dest := in.CloneContext.DestinationInstanceName
	// Check the name before it is used in a file path.
	if !nameRe.MatchString(dest) {
		return nil, fail(400, "invalidRequest", "Invalid request: Invalid instance name: %q. Names are lowercase letters, digits and hyphens, and start with a letter.", dest)
	}
	s.mu.Lock()
	i, err := s.instance(q)
	if err == nil {
		err = running(i)
	}
	if err == nil && s.st.Instances[q.project+"/"+dest] != nil {
		err = fail(409, "instanceAlreadyExists", "The Cloud SQL instance already exists.")
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	e, container, pw := i.engine(), i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	var dump []byte
	if container != "" && e.name != "sqlserver-ee" {
		if dump, err = s.box.Dump(container, e.name, pw); err != nil {
			return nil, fail(500, "backendError", "The source instance could not be dumped: %v", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err = s.instance(q)
	if err != nil {
		return nil, err
	}
	seed := ""
	if dump != nil {
		seed = filepath.Join(s.files(), dest+".sql")
		if err := os.WriteFile(seed, dump, 0o600); err != nil {
			return nil, err
		}
	}
	spec := clone(i.Spec)
	spec.Name, spec.RootPassword = dest, ""
	o, err := s.create(q.project, spec, pw, seed)
	if err != nil {
		return nil, err
	}
	n := s.st.Instances[q.project+"/"+dest]
	n.Databases, n.Users = *clone(&i.Databases), *clone(&i.Users)
	for _, d := range n.Databases {
		d.Instance = dest
	}
	for _, u := range n.Users {
		u.Instance = dest
	}
	o.Op.OperationType = "CLONE"
	return s.opView(q.base(), o), nil
}

// ---- export and import through GCS ----

// gcs splits a gs:// URI.
func gcs(uri string) (bucket, object string, ok bool) {
	rest, ok := strings.CutPrefix(uri, "gs://")
	bucket, object, _ = strings.Cut(rest, "/")
	return bucket, object, ok && bucket != "" && object != ""
}

func (s *Service) gcsPut(bucket, object string, data []byte) error {
	r, _ := newReq("POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+url.QueryEscape(object), data)
	r.Header.Set("Content-Type", "application/sql")
	if code, body := s.hub.DoService("gcs", r); code/100 != 2 {
		return fmt.Errorf("writing gs://%s/%s: %d %s", bucket, object, code, strings.TrimSpace(string(body)))
	}
	return nil
}

func (s *Service) gcsGet(bucket, object string) ([]byte, error) {
	r, _ := newReq("GET", "/storage/v1/b/"+bucket+"/o/"+url.PathEscape(object)+"?alt=media", nil)
	code, body := s.hub.DoService("gcs", r)
	if code/100 != 2 {
		return nil, fmt.Errorf("reading gs://%s/%s: %d %s", bucket, object, code, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// export writes a SQL dump to a GCS object (the Cloud Storage emulator
// serves it): every database, or those named. An error lands in the operation.
// ponytail: SQL only; no CSV or BAK exports.
func (s *Service) export(q *req) (any, error) {
	var in sqladmin.InstancesExportRequest
	if err := q.into(&in); err != nil {
		return nil, err
	}
	ec := in.ExportContext
	if ec == nil {
		return nil, fail(400, "invalidRequest", "Invalid request: Missing parameter: exportContext.")
	}
	bucket, object, ok := gcs(ec.Uri)
	if !ok {
		return nil, fail(400, "invalidRequest", "Invalid request: exportContext.uri must be gs://<bucket>/<object>.")
	}
	s.mu.Lock()
	i, err := s.instance(q)
	if err == nil {
		err = running(i)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	e, container, pw := i.engine(), i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	var failure string
	if data, err := s.dump(e, container, pw, ec.FileType, ec.Databases); err != nil {
		failure = err.Error()
	} else if err := s.gcsPut(bucket, object, data); err != nil {
		failure = err.Error()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.newOp(q.project, q.inst, "EXPORT", "", failure)
	o.Op.ExportContext = ec
	return s.opView(q.base(), o), nil
}

func (s *Service) dump(e engine, container, pw, fileType string, dbs []string) ([]byte, error) {
	switch {
	case container == "":
		return nil, fmt.Errorf("nothing runs behind this instance: %s", "Docker is not available")
	case fileType != "" && fileType != "SQL":
		return nil, fmt.Errorf("only SQL exports are supported, not %s", fileType)
	case e.name == "sqlserver-ee":
		return nil, fmt.Errorf("SQL Server exports are not supported")
	case len(dbs) == 0:
		return s.box.Dump(container, e.name, pw)
	}
	for _, d := range dbs {
		if !dbRe.MatchString(d) {
			return nil, fmt.Errorf("invalid database name %q", d)
		}
	}
	if e.name == "postgres" {
		return s.box.Out(container, "pg_dump", "-U", e.admin, dbs[0])
	}
	return s.box.Out(container, append([]string{"env", "MYSQL_PWD=" + pw, "mysqldump", "-uroot", "--single-transaction", "--routines", "--databases"}, dbs...)...)
}

// importSQL loads a SQL file from a GCS object into the engine. An error
// lands in the operation. ponytail: SQL only; no CSV imports.
func (s *Service) importSQL(q *req) (any, error) {
	var in sqladmin.InstancesImportRequest
	if err := q.into(&in); err != nil {
		return nil, err
	}
	ic := in.ImportContext
	if ic == nil {
		return nil, fail(400, "invalidRequest", "Invalid request: Missing parameter: importContext.")
	}
	bucket, object, ok := gcs(ic.Uri)
	if !ok {
		return nil, fail(400, "invalidRequest", "Invalid request: importContext.uri must be gs://<bucket>/<object>.")
	}
	if ic.Database != "" && !dbRe.MatchString(ic.Database) {
		return nil, fail(400, "invalidRequest", "Invalid request: Invalid database name %q.", ic.Database)
	}
	s.mu.Lock()
	i, err := s.instance(q)
	if err == nil {
		err = running(i)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	e, container, pw := i.engine(), i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	failure := ""
	if err := s.load(e, container, pw, ic, bucket, object); err != nil {
		failure = err.Error()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := s.st.Instances[q.key()]; i != nil && failure == "" && container != "" {
		s.mu.Unlock() // the engine answers while the lock is off
		names := s.listDatabaseNames(e, container, pw)
		s.mu.Lock()
		if i = s.st.Instances[q.key()]; i != nil {
			for _, n := range names {
				if !hasDatabase(i, n) {
					i.Databases = append(i.Databases, &sqladmin.Database{Kind: "sql#database", Name: n, Charset: charset(e.name),
						Collation: collation(e.name), Instance: q.inst, Project: q.project})
				}
			}
		}
	}
	o := s.newOp(q.project, q.inst, "IMPORT", "", failure)
	o.Op.ImportContext = ic
	return s.opView(q.base(), o), nil
}

func (s *Service) load(e engine, container, pw string, ic *sqladmin.ImportContext, bucket, object string) error {
	switch {
	case container == "":
		return fmt.Errorf("nothing runs behind this instance: Docker is not available")
	case ic.FileType != "" && ic.FileType != "SQL":
		return fmt.Errorf("only SQL imports are supported, not %s", ic.FileType)
	case e.name == "sqlserver-ee":
		return fmt.Errorf("SQL Server imports are not supported")
	}
	data, err := s.gcsGet(bucket, object)
	if err != nil {
		return err
	}
	const file = "/tmp/pogi-import.sql"
	if err := s.box.Put(container, file, data); err != nil {
		return err
	}
	cmd := []string{"psql", "-U", e.admin, "-d", cmp.Or(ic.Database, "postgres"), "-v", "ON_ERROR_STOP=1", "-f", file}
	if e.name == "mysql" {
		cmd = []string{"env", "MYSQL_PWD=" + pw, "sh", "-c", "mysql -uroot $1 < " + file, "sh", ic.Database}
	}
	_, err = s.box.Out(container, cmd...)
	s.box.Out(container, "rm", "-f", file)
	return err
}

// listDatabaseNames asks a PostgreSQL or MySQL server for its user databases.
func (s *Service) listDatabaseNames(e engine, container, pw string) []string {
	cmd := []string{"psql", "-U", e.admin, "-d", "postgres", "-Atc", "select datname from pg_database where not datistemplate"}
	if e.name == "mysql" {
		cmd = []string{"env", "MYSQL_PWD=" + pw, "mysql", "-uroot", "-N", "-e", "select schema_name from information_schema.schemata"}
	}
	out, err := s.box.Out(container, cmd...)
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}

func newReq(method, target string, body []byte) (*http.Request, error) {
	return http.NewRequest(method, target, bytes.NewReader(body))
}
