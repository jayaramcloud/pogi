// Package databases is the Azure Resource Manager management plane of
// Azure SQL, PostgreSQL, MySQL and MariaDB servers and Cache for Redis. The
// servers are real: each runs as a container next to pogi (the same
// containers RDS and ElastiCache start, through rds.Box), with the admin
// login and password from the PUT body. Databases are created in the engine.
// Without Docker the resources are metadata only.
//
// The HTTP surface is internal/azure/arm's; this package registers one
// arm.Provider per resource type.
package databases

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// kind is one family of servers.
type kind struct {
	key     string // short name, in container names
	typ     string // ARM resource type
	engine  string // RDS engine of the container; "" for Redis
	port    int    // port inside the container
	state   string // the property that reports the server's state
	version string // default engine version
	login   bool   // has an admin login and password
	dbs     bool   // has databases
	cfgs    bool   // has configurations
	// props and sku are what a new server gets where the body has none.
	props func() map[string]any
	sku   any
}

var kinds = []*kind{
	{key: "sql", typ: "Microsoft.Sql/servers", engine: "sqlserver-ex", port: 1433, state: "state", login: true, dbs: true,
		props: func() map[string]any {
			return map[string]any{"version": "12.0", "minimalTlsVersion": "1.2", "publicNetworkAccess": "Enabled",
				"restrictOutboundNetworkAccess": "Disabled", "privateEndpointConnections": []any{}}
		}},
	{key: "pg", typ: "Microsoft.DBforPostgreSQL/flexibleServers", engine: "postgres", port: 5432, state: "state", version: "16", login: true, dbs: true, cfgs: true,
		sku: map[string]any{"name": "Standard_B1ms", "tier": "Burstable"},
		props: func() map[string]any {
			return map[string]any{"version": "16", "minorVersion": "0", "availabilityZone": "1", "storage": map[string]any{"storageSizeGB": 32, "autoGrow": "Disabled", "tier": "P4"},
				"backup":           map[string]any{"backupRetentionDays": 7, "geoRedundantBackup": "Disabled"},
				"highAvailability": map[string]any{"mode": "Disabled"}, "network": map[string]any{"publicNetworkAccess": "Enabled"},
				"authConfig": map[string]any{"activeDirectoryAuth": "Disabled", "passwordAuth": "Enabled"}, "replicationRole": "Primary"}
		}},
	{key: "mysql", typ: "Microsoft.DBforMySQL/flexibleServers", engine: "mysql", port: 3306, state: "state", version: "8.0.21", login: true, dbs: true, cfgs: true,
		sku: map[string]any{"name": "Standard_B1ms", "tier": "Burstable"},
		props: func() map[string]any {
			return map[string]any{"version": "8.0.21", "availabilityZone": "1", "storage": map[string]any{"storageSizeGB": 20, "iops": 360, "autoGrow": "Enabled"},
				"backup":           map[string]any{"backupRetentionDays": 7, "geoRedundantBackup": "Disabled"},
				"highAvailability": map[string]any{"mode": "Disabled"}, "network": map[string]any{"publicNetworkAccess": "Enabled"}, "replicationRole": "None"}
		}},
	{key: "mariadb", typ: "Microsoft.DBforMariaDB/servers", engine: "mariadb", port: 3306, state: "userVisibleState", version: "10.3", login: true, dbs: true,
		sku: map[string]any{"name": "B_Gen5_1", "tier": "Basic", "capacity": 1, "family": "Gen5"},
		props: func() map[string]any {
			return map[string]any{"version": "10.3", "sslEnforcement": "Enabled", "minimalTlsVersion": "TLSEnforcementDisabled", "publicNetworkAccess": "Enabled",
				"storageProfile": map[string]any{"storageMB": 5120, "backupRetentionDays": 7, "geoRedundantBackup": "Disabled", "storageAutogrow": "Enabled"}}
		}},
	{key: "redis", typ: "Microsoft.Cache/redis", port: 6379, state: "provisioningState",
		props: func() map[string]any {
			return map[string]any{"redisVersion": "7.2", "enableNonSslPort": false, "minimumTlsVersion": "1.2", "publicNetworkAccess": "Enabled",
				"sku": map[string]any{"name": "Basic", "family": "C", "capacity": 0}, "redisConfiguration": map[string]any{}, "linkedServers": []any{}}
		}},
}

// configDefaults are the configurations a PostgreSQL or MySQL server starts with: name to value and type.
var configDefaults = map[string]map[string][2]string{
	"pg": {"max_connections": {"50", "Integer"}, "work_mem": {"4096", "Integer"}, "timezone": {"UTC", "Enumeration"},
		"azure.extensions": {"", "Set"}, "log_min_duration_statement": {"-1", "Integer"}},
	"mysql": {"max_connections": {"171", "Integer"}, "sql_mode": {"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION", "Set"},
		"time_zone": {"SYSTEM", "String"}, "require_secure_transport": {"ON", "Enumeration"}, "character_set_server": {"utf8mb4", "Enumeration"}},
}

// Child resource types, by the last segment of their type.
const (
	databases     = "databases"
	firewallRules = "firewallRules"
	configs       = "configurations"
)

// res is a server, or a database, firewall rule or configuration of one.
type res struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Kind     string         `json:"kind"` // kind.key of the server (of its parent for children)
	Location string         `json:"location,omitempty"`
	Tags     any            `json:"tags,omitempty"`
	Sku      any            `json:"sku,omitempty"`
	Props    map[string]any `json:"properties"`
	Keys     []string       `json:"keys,omitempty"` // Redis access keys, primary and secondary
	// Pogi is the container of a server. Its Password is the admin password.
	Pogi rds.Server `json:"pogi" query:"-"`
}

type state struct {
	Res map[string]*res `json:"resources"` // by lower-cased resource ID
}

// Service holds the resources of every kind of server.
type Service struct {
	hub    *core.Hub
	box    *rds.Box
	mu     sync.Mutex
	st     state
	ctx    context.Context
	cancel context.CancelFunc
}

func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: state{Res: map[string]*res{}}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.box = rds.NewBox(hub, "azdatabases", "failed", &s.mu, s.owns)
	for _, k := range kinds {
		p := arm.Provider{Type: k.typ, Put: s.putServer(k), Get: s.get, Delete: s.deleteServer, List: s.list(k.typ)}
		if k.key == "redis" {
			p.Actions = map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": s.listKeys, "regenerateKey": s.regenerateKey}
		}
		arm.Register(hub, p)
		var children []string
		if k.dbs {
			children = append(children, databases)
		}
		if k.cfgs {
			children = append(children, configs)
		}
		for _, ch := range append(children, firewallRules) {
			arm.Register(hub, arm.Provider{Type: k.typ + "/" + ch, Put: s.putChild(k, ch), Get: s.get, Delete: s.deleteChild(k, ch), List: s.list(k.typ + "/" + ch)})
		}
	}
	return s
}

func (s *Service) Info() core.Info {
	// The ARM service routes to the providers; this one has no HTTP routes.
	return core.Info{Name: "azdatabases", Cloud: core.Azure, Title: "Azure SQL, PostgreSQL, MySQL, MariaDB and Redis", Protocol: "azure-json",
		Match: func(*http.Request) bool { return false }}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }

func (s *Service) Close() error {
	s.cancel()
	s.mu.Lock()
	keep := s.containers()
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

func key(id arm.ID) string { return strings.ToLower(id.String()) }

func (st *state) servers(f func(*res)) {
	for _, r := range st.Res {
		if r.Pogi.State != "" {
			f(r)
		}
	}
}

func (s *Service) owns(srv *rds.Server) (found bool) {
	s.st.servers(func(r *res) { found = found || &r.Pogi == srv })
	return found
}

func (s *Service) containers() map[string]bool {
	keep := map[string]bool{}
	s.st.servers(func(r *res) {
		if r.Pogi.Container != "" {
			keep[r.Pogi.Container] = true
		}
	})
	return keep
}

func kindOf(key string) *kind {
	for _, k := range kinds {
		if k.key == key {
			return k
		}
	}
	return nil
}

// ---- servers ----

var loginRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,62}$`)

func invalid(format string, a ...any) *core.Error {
	return core.Errorf(http.StatusBadRequest, "InvalidParameterValue", format, a...)
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func randKey() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// container is the name part that tells servers of different kinds, groups
// and subscriptions apart.
func container(k *kind, id arm.ID) string {
	n := k.key + "-" + id.Name()
	if !strings.EqualFold(id.ResourceGroup, core.AzureResourceGroup) {
		n += "-" + id.ResourceGroup
	}
	return n
}

// spec is the container of a server.
func (r *res) spec() rds.Spec {
	k := kindOf(r.Kind)
	if k.key == "redis" {
		// Both access keys are passwords of the default user: the first through requirepass, the second as an ACL password.
		return rds.Spec{Image: "redis:7-alpine", Port: 6379, Host: r.Pogi.HostPort, Hello: "PING\r\n", Args: []string{"--requirepass", r.Keys[0]},
			Init: []string{"redis-cli", "-a", r.Keys[0], "--no-auth-warning", "ACL", "SETUSER", "default", ">" + r.Keys[1]}}
	}
	v := str(r.Props["version"])
	if k.key == "sql" {
		v = ""
	} else if k.key == "mariadb" {
		v = "10.11" // ponytail: Azure MariaDB runs 10.2 and 10.3, which have no current images; every server gets 10.11
	}
	return rds.DBSpec(k.engine, v, str(r.Props["administratorLogin"]), r.Pogi.Password, "", r.Pogi.HostPort)
}

// putServer creates or updates a server and starts its container.
func (s *Service) putServer(k *kind) func(*core.Ctx, arm.ID, map[string]any) (map[string]any, error) {
	return func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
		props := maps.Clone(obj(body["properties"]))
		if props == nil {
			props = map[string]any{}
		}
		pw := str(props["administratorLoginPassword"])
		delete(props, "administratorLoginPassword")
		docker := s.box.Available()

		s.mu.Lock()
		defer s.mu.Unlock()
		r := s.st.Res[key(id)]
		if r == nil {
			login := str(props["administratorLogin"])
			if k.login && !loginRe.MatchString(login) { // it also lands in SQL inside the container
				return nil, invalid("The administrator login must be 1 to 63 letters, digits or underscores and start with a letter.")
			}
			r = &res{ID: id.String(), Type: k.typ, Kind: k.key, Props: k.props()}
			if k.sku != nil {
				r.Sku = k.sku
			}
			if k.key == "redis" {
				r.Keys = []string{randKey(), randKey()}
			} else {
				// ponytail: a missing password (Entra-only authentication on Azure) gets a random one, which Inspect shows.
				r.Pogi.Password = cmp.Or(pw, rand.Text())
			}
			s.fill(r, body, props)
			s.st.Res[key(id)] = r
			if k.key == "sql" { // every SQL server has its master database
				m := id.Child("databases", "master")
				s.st.Res[key(m)] = &res{ID: m.String(), Type: k.typ + "/databases", Kind: k.key, Location: r.Location, Props: sqlDB(nil)}
			}
			for name, c := range configDefaults[k.key] {
				cid := id.Child("configurations", name)
				s.st.Res[key(cid)] = &res{ID: cid.String(), Type: k.typ + "/configurations", Kind: k.key, Props: map[string]any{"value": c[0], "defaultValue": c[0],
					"source": "system-default", "dataType": c[1], "isReadOnly": false, "isDynamicConfig": true, "isConfigPendingRestart": false}}
			}
			s.box.Serve(&r.Pogi, core.DefaultAccount, s.hub.Cfg.Region, container(k, id), 0, k.port, r.spec, docker)
		} else {
			if v, ok := props["administratorLogin"]; ok && v != r.Props["administratorLogin"] {
				return nil, invalid("The administrator login of a server cannot be changed.")
			}
			s.fill(r, body, props)
			if k.login && pw != "" && pw != r.Pogi.Password && r.Pogi.Container != "" && r.Pogi.State == "available" {
				// ponytail: a password change while the server is still being created is not applied.
				s.box.Exec(&r.Pogi, rds.Alter(k.engine, str(r.Props["administratorLogin"]), r.Pogi.Password, pw)...)
			}
			if k.login && pw != "" {
				r.Pogi.Password = pw
			}
		}
		s.hub.MarkDirty()
		return s.view(r), nil
	}
}

// fill applies a PUT body to a server.
func (s *Service) fill(r *res, body, props map[string]any) {
	if v, ok := body["location"].(string); ok || r.Location == "" {
		r.Location = cmp.Or(v, "eastus")
	}
	if v, ok := body["tags"]; ok {
		r.Tags = v
	}
	if v, ok := body["sku"]; ok && v != nil {
		r.Sku = v
	}
	maps.Copy(r.Props, props)
}

func (s *Service) stateOf(k *kind, r *res) string {
	switch r.Pogi.State {
	case "creating":
		return "Creating"
	case "failed":
		return "Failed"
	case "stopped":
		return "Stopped"
	}
	if k.key == "redis" {
		return "Succeeded"
	}
	return "Ready"
}

// view renders a resource as ARM returns it. Lock held.
func (s *Service) view(r *res) map[string]any {
	id, _ := arm.ParseID(r.ID)
	props := maps.Clone(r.Props)
	if r.Pogi.State == "" { // a child
		out := arm.Resource(id, r.Location, r.Tags, props)
		if r.Sku != nil {
			out["sku"] = r.Sku
		}
		return out
	}
	k := kindOf(r.Kind)
	props[k.state] = s.stateOf(k, r)
	props["fullyQualifiedDomainName"] = "localhost"
	if k.key == "redis" {
		delete(props, "fullyQualifiedDomainName")
		props["hostName"], props["port"], props["sslPort"] = "localhost", cmp.Or(r.Pogi.HostPort, 6379), 6380
	}
	// ponytail: pogi serves the plain protocol on pogiPort only; there is no TLS port and no 1433, 5432, 3306 or 6379.
	if r.Pogi.State != "creating" {
		props["pogiPort"] = r.Pogi.HostPort
	}
	if r.Pogi.Note != "" {
		props["pogiNote"] = r.Pogi.Note
	}
	out := arm.Resource(id, r.Location, r.Tags, props)
	if r.Sku != nil {
		out["sku"] = r.Sku
	}
	if k.key == "sql" {
		out["kind"] = "v12.0"
	}
	return out
}

func (s *Service) get(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.st.Res[key(id)]
	if r == nil {
		return nil, arm.NotFound(id)
	}
	return s.view(r), nil
}

func (s *Service) list(typ string) func(*core.Ctx, arm.ID) ([]map[string]any, error) {
	return func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var keys []string
		for k, r := range s.st.Res {
			id, _ := arm.ParseID(r.ID)
			switch {
			case !strings.EqualFold(r.Type, typ):
			case len(id.Types) > 1: // a child: of this parent
				if strings.HasPrefix(k, key(parent)+"/") {
					keys = append(keys, k)
				}
			case parent.ResourceGroup == "" || strings.EqualFold(id.ResourceGroup, parent.ResourceGroup):
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		out := make([]map[string]any, len(keys))
		for i, k := range keys {
			out[i] = s.view(s.st.Res[k])
		}
		return out, nil
	}
}

func (s *Service) deleteServer(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	k := key(id)
	r := s.st.Res[k]
	for name := range s.st.Res {
		if strings.HasPrefix(name, k+"/") {
			delete(s.st.Res, name)
		}
	}
	delete(s.st.Res, k)
	s.mu.Unlock()
	if r != nil && r.Pogi.Container != "" {
		s.box.Remove(r.Pogi.Container)
	}
	s.hub.MarkDirty()
	return nil
}

// ---- Redis keys ----

func (s *Service) redis(id arm.ID) (*res, error) {
	r := s.st.Res[key(id)]
	if r == nil {
		return nil, arm.NotFound(id)
	}
	return r, nil
}

func keys(r *res) map[string]any {
	return map[string]any{"primaryKey": r.Keys[0], "secondaryKey": r.Keys[1]}
}

func (s *Service) listKeys(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.redis(id)
	if err != nil {
		return nil, err
	}
	return keys(r), nil
}

func (s *Service) regenerateKey(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
	which := strings.ToLower(str(body["keyType"]))
	if which != "primary" && which != "secondary" {
		return nil, core.Errorf(http.StatusBadRequest, "BadRequest", "keyType must be Primary or Secondary.")
	}
	s.mu.Lock()
	r, err := s.redis(id)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	next := slices.Clone(r.Keys)
	next[map[string]int{"primary": 0, "secondary": 1}[which]] = randKey()
	name, live := r.Pogi.Container, r.Pogi.State == "available"
	s.mu.Unlock()
	if name != "" && live { // the running Redis takes the new pair, authenticating with an old key
		err = s.run(name, "redis-cli", "-a", r.Keys[0], "--no-auth-warning", "ACL", "SETUSER", "default", "resetpass", ">"+next[0], ">"+next[1])
		if err != nil {
			return nil, core.Errorf(http.StatusInternalServerError, "InternalServerError", "%v", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r.Keys = next
	s.hub.MarkDirty()
	return keys(r), nil
}

// ---- children: databases, firewall rules and configurations ----

var dbNameRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$`)

// sqlDB is a new SQL database's properties.
func sqlDB(in map[string]any) map[string]any {
	p := map[string]any{"collation": "SQL_Latin1_General_CP1_CI_AS", "maxSizeBytes": 2147483648, "status": "Online",
		"databaseId": randUUID(), "creationDate": time.Now().UTC().Format(time.RFC3339), "zoneRedundant": false,
		"currentServiceObjectiveName": "Basic", "requestedServiceObjectiveName": "Basic"}
	maps.Copy(p, in)
	return p
}

func randUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

var dbDefaults = map[string]map[string]any{
	"pg":      {"charset": "UTF8", "collation": "en_US.utf8"},
	"mysql":   {"charset": "utf8mb4", "collation": "utf8mb4_0900_ai_ci"},
	"mariadb": {"charset": "utf8", "collation": "utf8_general_ci"},
}

func (s *Service) putChild(k *kind, child string) func(*core.Ctx, arm.ID, map[string]any) (map[string]any, error) {
	return func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
		props := maps.Clone(obj(body["properties"]))
		if props == nil {
			props = map[string]any{}
		}
		s.mu.Lock()
		srv := s.st.Res[key(id.Parent())]
		if srv == nil {
			s.mu.Unlock()
			return nil, arm.NotFound(id.Parent())
		}
		r := s.st.Res[key(id)]
		isNew := r == nil
		switch {
		case child == databases && !dbNameRe.MatchString(id.Name()):
			s.mu.Unlock()
			return nil, invalid("The database name %q is not valid: use letters, digits, underscores and hyphens.", id.Name())
		case isNew:
			r = &res{ID: id.String(), Type: k.typ + "/" + child, Kind: k.key, Location: srv.Location, Props: map[string]any{}}
			switch child {
			case databases:
				if k.key == "sql" {
					r.Props = sqlDB(nil)
					if v, ok := body["sku"]; ok {
						r.Sku = v
					}
				} else {
					maps.Copy(r.Props, dbDefaults[k.key])
				}
			case configs:
				r.Props = map[string]any{"source": "system-default", "isReadOnly": false, "isDynamicConfig": true, "isConfigPendingRestart": false}
			}
			s.st.Res[key(id)] = r
		}
		maps.Copy(r.Props, props)
		if child == configs {
			if _, ok := props["value"]; ok {
				r.Props["source"] = "user-override" // ponytail: configurations are stored and reported, never applied to the engine
			}
		}
		if v, ok := body["tags"]; ok {
			r.Tags = v
		}
		out := s.view(r)
		name, ready := srv.Pogi.Container, srv.Pogi.State == "available"
		cmd := dbCmd(srv, "create", id.Name())
		s.hub.MarkDirty()
		s.mu.Unlock()

		if isNew && child == databases && name != "" {
			if err := s.createDB(srv, id.Name(), name, cmd, ready); err != nil {
				s.mu.Lock()
				delete(s.st.Res, key(id))
				s.mu.Unlock()
				return nil, core.Errorf(http.StatusInternalServerError, "InternalServerError", "creating database %s: %v", id.Name(), err)
			}
		}
		return out, nil
	}
}

// createDB creates a database in the engine: now when the server answers,
// else once it does. Called without the lock.
func (s *Service) createDB(srv *res, db, container string, cmd []string, ready bool) error {
	if ready {
		return s.run(container, cmd...)
	}
	// ponytail: a database put while its server is still starting is created in the background; failures only reach the log.
	core.Go("databases.createDB", func() {
		for s.ctx.Err() == nil {
			s.mu.Lock()
			st := srv.Pogi.State
			s.mu.Unlock()
			switch st {
			case "available":
				if err := s.run(container, cmd...); err != nil {
					s.hub.Log.Warn("creating database failed", "database", db, "err", err)
				}
				return
			case "creating":
				time.Sleep(250 * time.Millisecond)
			default:
				return
			}
		}
	})
	return nil
}

// dbCmd is the command that creates or drops a database inside the server's container.
func dbCmd(srv *res, op, db string) []string {
	pw := srv.Pogi.Password
	create := op == "create"
	switch srv.Kind {
	case "pg":
		sql := fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, db) // ponytail: no FORCE, so open connections block the drop
		if create {
			sql = fmt.Sprintf(`CREATE DATABASE "%s"`, db)
		}
		return []string{"psql", "-U", str(srv.Props["administratorLogin"]), "-d", "postgres", "-c", sql}
	case "sql":
		sql := fmt.Sprintf("IF DB_ID(N'%[1]s') IS NOT NULL DROP DATABASE [%[1]s]", db)
		if create {
			sql = fmt.Sprintf("IF DB_ID(N'%[1]s') IS NULL CREATE DATABASE [%[1]s]", db)
		}
		return rds.TSQL(rds.SAPassword(pw), sql)
	}
	cli, sql := "mysql", "DROP DATABASE IF EXISTS `"+db+"`"
	if srv.Kind == "mariadb" {
		cli = "mariadb"
	}
	if create {
		sql = "CREATE DATABASE IF NOT EXISTS `" + db + "`"
	}
	return []string{"env", "MYSQL_PWD=" + pw, cli, "-uroot", "-e", sql}
}

func (s *Service) deleteChild(k *kind, child string) func(*core.Ctx, arm.ID) error {
	return func(c *core.Ctx, id arm.ID) error {
		s.mu.Lock()
		r, srv := s.st.Res[key(id)], s.st.Res[key(id.Parent())]
		delete(s.st.Res, key(id))
		var name string
		var cmd []string
		if srv != nil && r != nil && child == databases && srv.Pogi.State == "available" && id.Name() != "master" {
			name, cmd = srv.Pogi.Container, dbCmd(srv, "drop", id.Name())
		}
		s.hub.MarkDirty()
		s.mu.Unlock()
		if name != "" {
			if err := s.run(name, cmd...); err != nil {
				return core.Errorf(http.StatusInternalServerError, "InternalServerError", "dropping database %s: %v", id.Name(), err)
			}
		}
		return nil
	}
}

// run executes a command in a container.
func (s *Service) run(container string, cmd ...string) error {
	out, code, err := s.hub.Docker().Exec(s.ctx, container, cmd...)
	if err == nil && code != 0 {
		err = fmt.Errorf("%s exited with %d: %s", cmd[0], code, strings.TrimSpace(string(out)))
	}
	return err
}

// ---- persistence, console ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped servers go.
func (s *Service) Load(b []byte) error {
	st := state{}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Res == nil {
		st.Res = map[string]*res{}
	}
	s.mu.Lock()
	old := s.containers()
	s.st = st
	keep := s.containers()
	docker := len(keep) > 0 && s.box.Available()
	s.st.servers(func(r *res) { s.box.Resume(&r.Pogi, r.spec(), docker) })
	s.mu.Unlock()
	for n := range old {
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Resources lists servers and databases for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	srv := core.Table{Name: "Servers", Columns: []string{"Name", "Type", "State", "Endpoint", "Resource group"}}
	db := core.Table{Name: "Databases", Columns: []string{"Name", "Server", "Type"}}
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		id, _ := arm.ParseID(r.ID)
		switch {
		case r.Pogi.State != "":
			srv.Rows = append(srv.Rows, []string{id.Name(), r.Type, s.stateOf(kindOf(r.Kind), r), fmt.Sprintf("localhost:%d", r.Pogi.HostPort), id.ResourceGroup})
		case strings.HasSuffix(strings.ToLower(r.Type), "/databases"):
			db.Rows = append(db.Rows, []string{id.Name(), id.Parent().Name(), r.Type})
		}
	}
	return []core.Table{srv, db}
}

// Inspect returns a server with pogi's side (container, admin password, note).
func (s *Service) Inspect(table, name string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		if id, _ := arm.ParseID(r.ID); id.Name() == name && (r.Pogi.State != "") == (table == "Servers") {
			return map[string]any{"resource": s.view(r), "pogi": r.Pogi, "keys": r.Keys}
		}
	}
	return nil
}
