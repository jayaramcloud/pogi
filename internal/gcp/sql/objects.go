package sql

import (
	"cmp"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

var (
	dbRe   = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_$-]{0,62}$`)
	userRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.$-]{0,62}$`)
	hostRe = regexp.MustCompile(`^[A-Za-z0-9_.%:-]{0,60}$`)
	wordRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`) // charsets and collations
)

// systemDatabases are the databases every server of an engine starts with.
var systemDatabases = map[string][]string{
	"postgres":     {"postgres"},
	"mysql":        {"information_schema", "mysql", "performance_schema", "sys"},
	"sqlserver-ee": {"master", "model", "msdb", "tempdb"},
}

func charset(engine string) string {
	return map[string]string{"postgres": "UTF8", "mysql": "utf8mb4", "sqlserver-ee": "SQL_Latin1_General_CP1_CI_AS"}[engine]
}

func collation(engine string) string {
	return map[string]string{"postgres": "en_US.UTF8", "mysql": "utf8mb4_0900_ai_ci", "sqlserver-ee": "SQL_Latin1_General_CP1_CI_AS"}[engine]
}

func hasDatabase(i *instance, name string) bool {
	return slices.ContainsFunc(i.Databases, func(d *sqladmin.Database) bool { return d.Name == name })
}

// ---- running SQL in the engine ----

// step is one statement, run in database db ("" is the default one).
type step struct{ db, sql string }

// adminCmd is the command that runs sql in the engine as its administrator.
// The server's own client is used: local connections are trusted in the
// PostgreSQL image, and MySQL and SQL Server take the root password.
func adminCmd(e engine, pw string, st step) []string {
	switch e.name {
	case "postgres":
		return []string{"psql", "-U", e.admin, "-d", cmp.Or(st.db, "postgres"), "-v", "ON_ERROR_STOP=1", "-c", st.sql}
	case "mysql":
		return []string{"env", "MYSQL_PWD=" + pw, "mysql", "-uroot", "-e", st.sql}
	}
	return []string{"/opt/mssql-tools18/bin/sqlcmd", "-C", "-b", "-S", "localhost", "-U", "sa", "-P", rds.SAPassword(pw), "-Q", st.sql}
}

// inEngine runs statements in an instance's container, without the lock; the
// first failure is the error. Without a container there is nothing to run.
func (s *Service) inEngine(e engine, container, pw string, steps ...step) error {
	if container == "" {
		return nil
	}
	for _, st := range steps {
		if out, err := s.box.Out(container, adminCmd(e, pw, st)...); err != nil {
			msg := strings.TrimSpace(string(out))
			if msg == "" {
				msg = err.Error()
			}
			return fail(400, "invalidRequest", "Invalid request: %s", firstLines(msg))
		}
	}
	return nil
}

// firstLines keeps an engine's error short: its first two lines.
func firstLines(s string) string {
	l := strings.SplitN(s, "\n", 3)
	return strings.Join(l[:min(len(l), 2)], " ")
}

func pgQ(s string) string    { return strings.ReplaceAll(s, "'", "''") }
func mysqlQ(s string) string { return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) }

// target is what runs commands of an instance: its engine, container and root password. Lock held.
func (s *Service) target(q *req) (*instance, engine, error) {
	i, err := s.instance(q)
	if err != nil {
		return nil, engine{}, err
	}
	return i, i.engine(), running(i)
}

// ---- databases ----

func (s *Service) listDatabases(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	items := make([]*sqladmin.Database, len(i.Databases))
	for n, d := range i.Databases {
		items[n] = s.dbView(q, d)
	}
	return &sqladmin.DatabasesListResponse{Kind: "sql#databasesList", Items: items}, nil
}

func (s *Service) dbView(q *req, d *sqladmin.Database) *sqladmin.Database {
	v := clone(d)
	v.SelfLink = q.base() + "/projects/" + d.Project + "/instances/" + d.Instance + "/databases/" + d.Name
	return v
}

func (s *Service) database(q *req, i *instance) (*sqladmin.Database, error) {
	for _, d := range i.Databases {
		if d.Name == q.item {
			return d, nil
		}
	}
	return nil, notFound("database")
}

func (s *Service) getDatabase(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	d, err := s.database(q, i)
	if err != nil {
		return nil, err
	}
	return s.dbView(q, d), nil
}

func (s *Service) insertDatabase(q *req) (any, error) {
	in := &sqladmin.Database{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	if !dbRe.MatchString(in.Name) || !wordRe.MatchString(cmp.Or(in.Charset, "x")) || !wordRe.MatchString(cmp.Or(in.Collation, "x")) {
		return nil, fail(400, "invalidRequest", "Invalid request: Invalid database name, charset or collation.")
	}
	s.mu.Lock()
	i, e, err := s.target(q)
	if err == nil && hasDatabase(i, in.Name) {
		err = fail(409, "invalidRequest", "Invalid request: failed to create database %s. Detail: database already exists.", in.Name)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	d := &sqladmin.Database{Kind: "sql#database", Name: in.Name, Charset: cmp.Or(in.Charset, charset(e.name)),
		Collation: cmp.Or(in.Collation, collation(e.name)), Instance: q.inst, Project: q.project, Etag: core.RandHex(8)}
	i.Databases = append(i.Databases, d) // reserved while the engine works
	container, pw := i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	steps := []step{{sql: `CREATE DATABASE "` + in.Name + `"`}}
	switch {
	case e.name == "postgres": // PostgreSQL 15 keeps non-owners out of the public schema
		steps = append(steps, step{in.Name, `GRANT ALL ON SCHEMA public TO PUBLIC`})
	case e.name == "mysql":
		steps[0].sql = "CREATE DATABASE `" + in.Name + "`"
		if in.Charset != "" {
			steps[0].sql += " CHARACTER SET " + in.Charset
		}
		if in.Collation != "" {
			steps[0].sql += " COLLATE " + in.Collation
		}
	default:
		steps[0].sql = "CREATE DATABASE [" + in.Name + "]"
	}
	err = s.inEngine(e, container, pw, steps...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		i.Databases = slices.DeleteFunc(i.Databases, func(x *sqladmin.Database) bool { return x == d })
		return nil, err
	}
	return s.opView(q.base(), s.newOp(q.project, q.inst, "CREATE_DATABASE", "", "")), nil
}

// updateDatabase changes the charset and collation in pogi's record only.
// ponytail: the engine's database keeps what it was created with.
func (s *Service) updateDatabase(q *req) (any, error) {
	in := &sqladmin.Database{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	d, err := s.database(q, i)
	if err != nil {
		return nil, err
	}
	d.Charset, d.Collation = cmp.Or(in.Charset, d.Charset), cmp.Or(in.Collation, d.Collation)
	return s.opView(q.base(), s.newOp(q.project, q.inst, "UPDATE_DATABASE", "", "")), nil
}

func (s *Service) deleteDatabase(q *req) (any, error) {
	s.mu.Lock()
	i, e, err := s.target(q)
	var d *sqladmin.Database
	if err == nil {
		d, err = s.database(q, i)
	}
	if err == nil && slices.Contains(systemDatabases[e.name], d.Name) {
		err = fail(400, "invalidRequest", "Invalid request: %s is a system database and cannot be deleted.", d.Name)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	container, pw := i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	drop := map[string]string{"postgres": `DROP DATABASE "` + d.Name + `" WITH (FORCE)`, "mysql": "DROP DATABASE `" + d.Name + "`"}[e.name]
	if e.name == "sqlserver-ee" {
		drop = "DROP DATABASE [" + d.Name + "]"
	}
	if err := s.inEngine(e, container, pw, step{sql: drop}); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i.Databases = slices.DeleteFunc(i.Databases, func(x *sqladmin.Database) bool { return x == d })
	return s.opView(q.base(), s.newOp(q.project, q.inst, "DELETE_DATABASE", "", "")), nil
}

// ---- users ----

// userKey is a MySQL account's host (default %); PostgreSQL and SQL Server accounts have none.
func hostOf(e engine, host string) string {
	if e.name != "mysql" {
		return ""
	}
	return cmp.Or(host, "%")
}

func (s *Service) user(i *instance, name, host string) *sqladmin.User {
	for _, u := range i.Users {
		if u.Name == name && (host == "" || u.Host == host) {
			return u
		}
	}
	return nil
}

func userView(u *sqladmin.User) *sqladmin.User { v := clone(u); v.Password = ""; return v }

func (s *Service) listUsers(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	items := make([]*sqladmin.User, len(i.Users))
	for n, u := range i.Users {
		items[n] = userView(u)
	}
	return &sqladmin.UsersListResponse{Kind: "sql#usersList", Items: items}, nil
}

func (s *Service) getUser(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	if u := s.user(i, q.item, q.r.URL.Query().Get("host")); u != nil {
		return userView(u), nil
	}
	return nil, notFound("user")
}

func (s *Service) insertUser(q *req) (any, error) {
	in := &sqladmin.User{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	builtIn := in.Type == "" || in.Type == "BUILT_IN"
	if in.Name == "" || builtIn && !userRe.MatchString(in.Name) || !hostRe.MatchString(in.Host) {
		return nil, fail(400, "invalidRequest", "Invalid request: Invalid user name or host.")
	}
	s.mu.Lock()
	i, e, err := s.target(q)
	host := hostOf(e, in.Host)
	if err == nil && s.user(i, in.Name, host) != nil {
		err = fail(409, "invalidRequest", "Invalid request: failed to create user %s. Detail: the user already exists.", in.Name)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	u := &sqladmin.User{Kind: "sql#user", Name: in.Name, Host: host, Type: cmp.Or(in.Type, "BUILT_IN"), Instance: q.inst,
		Project: q.project, Etag: core.RandHex(8)}
	i.Users = append(i.Users, u)
	container, pw := i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	// ponytail: IAM users are only recorded, and the database does not check IAM tokens
	if builtIn {
		err = s.inEngine(e, container, pw, createUser(e, in.Name, host, in.Password)...)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		i.Users = slices.DeleteFunc(i.Users, func(x *sqladmin.User) bool { return x == u })
		return nil, err
	}
	return s.opView(q.base(), s.newOp(q.project, q.inst, "CREATE_USER", "", "")), nil
}

// createUser is the SQL of a new account. It may do most things in its
// databases, like Cloud SQL's users, but is no superuser.
func createUser(e engine, name, host, pw string) []step {
	switch e.name {
	case "postgres":
		sql := `CREATE ROLE "` + name + `" LOGIN CREATEDB CREATEROLE`
		if pw != "" {
			sql += ` PASSWORD '` + pgQ(pw) + `'`
		}
		return []step{{sql: sql}}
	case "mysql":
		acct := "'" + name + "'@'" + host + "'"
		return []step{{sql: "CREATE USER " + acct + " IDENTIFIED BY '" + mysqlQ(pw) + "'; GRANT ALL PRIVILEGES ON *.* TO " + acct + " WITH GRANT OPTION"}}
	}
	return []step{{sql: "CREATE LOGIN [" + name + "] WITH PASSWORD = N'" + pgQ(pw) + "', CHECK_POLICY = OFF; ALTER SERVER ROLE sysadmin ADD MEMBER [" + name + "]"}}
}

// passwordSQL changes an account's password; the administrator's also
// changes where pogi connects as (MySQL's root@localhost, SQL Server's sa).
func passwordSQL(e engine, user, host, pw string, admin bool) string {
	switch e.name {
	case "postgres":
		return `ALTER ROLE "` + user + `" PASSWORD '` + pgQ(pw) + `'`
	case "mysql":
		sql := "ALTER USER '" + user + "'@'" + host + "' IDENTIFIED BY '" + mysqlQ(pw) + "'"
		if admin {
			sql += "; ALTER USER 'root'@'localhost' IDENTIFIED BY '" + mysqlQ(pw) + "'"
		}
		return sql
	}
	sql := "ALTER LOGIN [" + user + "] WITH PASSWORD = N'" + pgQ(pw) + "', CHECK_POLICY = OFF"
	if admin {
		sql += "; ALTER LOGIN [sa] WITH PASSWORD = N'" + pgQ(rds.SAPassword(pw)) + "'"
	}
	return sql
}

func (s *Service) updateUser(q *req) (any, error) {
	in := &sqladmin.User{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	name, host := q.r.URL.Query().Get("name"), q.r.URL.Query().Get("host")
	if name == "" {
		return nil, fail(400, "invalidRequest", "Invalid request: Missing parameter: name.")
	}
	if err := s.setPassword(q, q.project, q.inst, name, host, in.Password); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opView(q.base(), s.newOp(q.project, q.inst, "UPDATE_USER", "", "")), nil
}

// setPassword sets an account's password in the engine; user "" is the
// administrator (a patched rootPassword). An empty password only checks the user exists.
func (s *Service) setPassword(q *req, project, inst, user, host, pw string) error {
	q2 := &req{r: q.r, project: project, inst: inst}
	s.mu.Lock()
	i, e, err := s.target(q2)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	user = cmp.Or(user, e.admin)
	u := s.user(i, user, hostOf(e, host))
	if u == nil {
		s.mu.Unlock()
		return notFound("user")
	}
	host = u.Host
	admin := user == e.admin
	container, old := i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	if pw == "" || u.Type != "BUILT_IN" {
		return nil
	}
	if err := s.inEngine(e, container, old, step{sql: passwordSQL(e, user, host, pw, admin)}); err != nil {
		return err
	}
	if admin {
		s.mu.Lock()
		i.Pogi.Password = pw
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) deleteUser(q *req) (any, error) {
	name, host := q.r.URL.Query().Get("name"), q.r.URL.Query().Get("host")
	s.mu.Lock()
	i, e, err := s.target(q)
	var u *sqladmin.User
	if err == nil {
		if u = s.user(i, name, hostOf(e, host)); u == nil {
			err = notFound("user")
		} else if name == e.admin {
			err = fail(400, "invalidRequest", "Invalid request: %s is the built-in administrator and cannot be deleted.", name)
		}
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	container, pw := i.Pogi.Container, i.Pogi.Password
	s.mu.Unlock()
	drop := map[string]string{"postgres": `DROP ROLE "` + u.Name + `"`, "mysql": "DROP USER '" + u.Name + "'@'" + u.Host + "'"}[e.name]
	if e.name == "sqlserver-ee" {
		drop = "DROP LOGIN [" + u.Name + "]"
	}
	if u.Type == "BUILT_IN" {
		if err := s.inEngine(e, container, pw, step{sql: drop}); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i.Users = slices.DeleteFunc(i.Users, func(x *sqladmin.User) bool { return x == u })
	return s.opView(q.base(), s.newOp(q.project, q.inst, "DELETE_USER", "", "")), nil
}

// ---- SSL certificates, connect settings ----

func (s *Service) listCerts(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	return &sqladmin.SslCertsListResponse{Kind: "sql#sslCertsList", Items: *clone(&i.Certs)}, nil
}

func (s *Service) cert(i *instance, sha string) (*sqladmin.SslCert, error) {
	for _, c := range i.Certs {
		if c.Sha1Fingerprint == sha {
			return c, nil
		}
	}
	return nil, notFound("SSL certificate")
}

func (s *Service) getCert(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	return s.cert(i, q.item)
}

// insertCert makes a client certificate and its private key, signed by the instance's CA.
func (s *Service) insertCert(q *req) (any, error) {
	in := &sqladmin.SslCertsInsertRequest{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	if in.CommonName == "" {
		return nil, fail(400, "invalidRequest", "Invalid request: Missing parameter: commonName.")
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	cert, err := i.CA.issue(&key.PublicKey, in.CommonName, 10*365*24*time.Hour)
	if err != nil {
		return nil, err
	}
	cert.Instance = q.inst
	i.Certs = append(i.Certs, cert)
	kb, _ := x509.MarshalECPrivateKey(key)
	return &sqladmin.SslCertsInsertResponse{Kind: "sql#sslCertsInsert", ServerCaCert: i.Spec.ServerCaCert,
		ClientCert: &sqladmin.SslCertDetail{CertInfo: cert, CertPrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))},
		Operation:  s.opView(q.base(), s.newOp(q.project, q.inst, "CREATE_CLIENT_CERTIFICATE", "", ""))}, nil
}

func (s *Service) deleteCert(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	c, err := s.cert(i, q.item)
	if err != nil {
		return nil, err
	}
	i.Certs = slices.DeleteFunc(i.Certs, func(x *sqladmin.SslCert) bool { return x == c })
	return s.opView(q.base(), s.newOp(q.project, q.inst, "DELETE_CLIENT_CERTIFICATE", "", "")), nil
}

func (s *Service) connectSettings(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	v := s.view(q.base(), i)
	return &sqladmin.ConnectSettings{Kind: "sql#connectSettings", ConnectionName: v.ConnectionName, DatabaseVersion: v.DatabaseVersion,
		BackendType: v.BackendType, Region: v.Region, IpAddresses: v.IpAddresses, ServerCaCert: v.ServerCaCert,
		ServerCaMode: "GOOGLE_MANAGED_INTERNAL_CA"}, nil
}

// ephemeral signs the public key of a connector for an hour (or validDuration).
func (s *Service) ephemeral(q *req, pubKey, valid string) (*sqladmin.SslCert, error) {
	pub, err := publicKey(pubKey)
	if err != nil {
		return nil, err
	}
	ttl := time.Hour
	if d, err := time.ParseDuration(valid); err == nil && d > 0 {
		ttl = d
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.instance(q)
	if err != nil {
		return nil, err
	}
	cert, err := i.CA.issue(pub, "pogi-ephemeral", ttl)
	if err != nil {
		return nil, fail(400, "invalidRequest", "Invalid request: %v", err)
	}
	cert.Instance = q.inst
	return cert, nil
}

func (s *Service) generateEphemeralCert(q *req) (any, error) {
	in := &sqladmin.GenerateEphemeralCertRequest{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	cert, err := s.ephemeral(q, in.PublicKey, in.ValidDuration)
	if err != nil {
		return nil, err
	}
	return &sqladmin.GenerateEphemeralCertResponse{EphemeralCert: cert}, nil
}

func (s *Service) createEphemeral(q *req) (any, error) {
	in := &struct {
		PublicKey string `json:"public_key"`
	}{}
	if err := q.into(in); err != nil {
		return nil, err
	}
	return s.ephemeral(q, in.PublicKey, "")
}

// ---- operations, tiers, flags ----

func (s *Service) listOps(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []*sqladmin.Operation{}
	for _, k := range core.SortedKeys(s.st.Ops) {
		o := s.st.Ops[k]
		if o.Op.TargetProject == q.project && (q.r.URL.Query().Get("instance") == "" || o.Op.TargetId == q.r.URL.Query().Get("instance")) {
			items = append(items, s.opView(q.base(), o))
		}
	}
	return &sqladmin.OperationsListResponse{Kind: "sql#operationsList", Items: items}, nil
}

func (s *Service) getOp(q *req) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.st.Ops[q.project+"/"+q.item]
	if o == nil {
		return nil, notFound("operation")
	}
	return s.opView(q.base(), o), nil
}

func (s *Service) tiers(q *req) (any, error) {
	out := &sqladmin.TiersListResponse{Kind: "sql#tiersList"}
	for _, t := range []struct {
		name      string
		ram, disk int64
	}{{"db-f1-micro", 629145600, 3 << 40}, {"db-g1-small", 1717986918, 3 << 40}, {"db-custom-1-3840", 3840 << 20, 3 << 40},
		{"db-custom-2-7680", 7680 << 20, 3 << 40}, {"db-custom-4-15360", 15360 << 20, 3 << 40}, {"db-perf-optimized-N-2", 16 << 30, 3 << 40}} {
		out.Items = append(out.Items, &sqladmin.Tier{Kind: "sql#tier", Tier: t.name, RAM: t.ram, DiskQuota: t.disk,
			Region: []string{"us-central1", "us-east1", "europe-west1", "asia-east1"}})
	}
	return out, nil
}

// flags is a short list of the database flags people set most.
// ponytail: the list is not the full one, and a flag is stored but not applied.
func (s *Service) flags(q *req) (any, error) {
	want := q.r.URL.Query().Get("databaseVersion")
	pg, my, ms := []string{"POSTGRES_13", "POSTGRES_14", "POSTGRES_15", "POSTGRES_16", "POSTGRES_17"}, []string{"MYSQL_5_7", "MYSQL_8_0", "MYSQL_8_4"},
		[]string{"SQLSERVER_2019_STANDARD", "SQLSERVER_2022_STANDARD"}
	out := &sqladmin.FlagsListResponse{Kind: "sql#flagsList", Items: []*sqladmin.Flag{}}
	for _, f := range []struct {
		name, typ string
		applies   []string
		min, max  int64
		restart   bool
	}{{"max_connections", "INTEGER", pg, 14, 262143, true}, {"work_mem", "INTEGER", pg, 64, 2147483647, false},
		{"log_min_duration_statement", "INTEGER", pg, -1, 2147483647, false}, {"max_connections", "INTEGER", my, 1, 100000, false},
		{"slow_query_log", "BOOLEAN", my, 0, 0, false}, {"long_query_time", "INTEGER", my, 0, 31536000, false},
		{"character_set_server", "STRING", my, 0, 0, true}, {"1204", "NONE", ms, 0, 0, false}} {
		if want != "" && !slices.Contains(f.applies, want) {
			continue
		}
		out.Items = append(out.Items, &sqladmin.Flag{Kind: "sql#flag", Name: f.name, Type: f.typ, AppliesTo: f.applies, MinValue: f.min,
			MaxValue: f.max, RequiresRestart: f.restart})
	}
	return out, nil
}
