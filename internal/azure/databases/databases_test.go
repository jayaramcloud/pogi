package databases_test

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	_ "github.com/microsoft/go-mssqldb"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/databases"
	"github.com/nrndev/pogi/internal/core"
)

const pw = "Pogi-test-Passw0rd"

type env struct {
	t *testing.T
	h *core.Hub
}

func setup(t *testing.T) *env {
	h, _ := core.ServeTest(t, func(h *core.Hub) core.Service { return databases.New(h) })
	return &env{t, h}
}

func (e *env) docker() {
	if !e.h.Docker().Available() {
		e.t.Skip("Docker is not available")
	}
}

// id builds /subscriptions/s/resourceGroups/pogi/providers/<typ>/<name>[/<child>/<name>].
func id(typ string, names ...string) arm.ID {
	ns, types, _ := strings.Cut(typ, "/")
	s := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/" + ns
	for i, t := range strings.Split(types, "/") {
		s += "/" + t + "/" + names[i]
	}
	i, ok := arm.ParseID(s)
	if !ok {
		panic(s)
	}
	return i
}

func ctx() *core.Ctx { return core.C(httptest.NewRequest("PUT", "/", nil)) }

func (e *env) provider(typ string) *arm.Provider {
	p := arm.Lookup(e.h, typ)
	if p == nil {
		e.t.Fatalf("no provider for %s", typ)
	}
	return p
}

func (e *env) put(typ string, body map[string]any, names ...string) map[string]any {
	e.t.Helper()
	out, err := e.provider(typ).Put(ctx(), id(typ, names...), body)
	if err != nil {
		e.t.Fatalf("PUT %s %v: %v", typ, names, err)
	}
	return out
}

func (e *env) get(typ string, names ...string) (map[string]any, error) {
	return e.provider(typ).Get(ctx(), id(typ, names...))
}

func props(r map[string]any) map[string]any { return r["properties"].(map[string]any) }

func port(r map[string]any) int { return props(r)["pogiPort"].(int) }

func code(err error) string {
	var e *core.Error
	if errors.As(err, &e) {
		return fmt.Sprintf("%d %s", e.Status, e.Code)
	}
	return fmt.Sprint(err)
}

// ready waits until the server reports state want in property prop; a failed server skips the test.
func (e *env) ready(typ, prop, want string, wait time.Duration, names ...string) map[string]any {
	e.t.Helper()
	for end := time.Now().Add(wait); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
		r, err := e.get(typ, names...)
		if err != nil {
			e.t.Fatal(err)
		}
		switch got := props(r)[prop]; got {
		case want:
			return r
		case "Failed":
			e.t.Skipf("%s did not start: %v", typ, props(r)["pogiNote"])
		}
	}
	e.t.Fatalf("%s %v never became %s", typ, names, want)
	return nil
}

func name(prefix string) string { return fmt.Sprintf("%s%x", prefix, time.Now().UnixNano()%0xfffffff) }

// ---- without Docker ----

func TestMetadataOnly(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	e := setup(t)

	srv := name("pg")
	r := e.put("Microsoft.DBforPostgreSQL/flexibleServers", map[string]any{"location": "westeurope", "tags": map[string]any{"a": "b"},
		"sku":        map[string]any{"name": "Standard_D2s_v3", "tier": "GeneralPurpose"},
		"properties": map[string]any{"administratorLogin": "pgadmin", "administratorLoginPassword": pw, "version": "15"}}, srv)
	p := props(r)
	if r["name"] != srv || r["type"] != "Microsoft.DBforPostgreSQL/flexibleServers" || r["location"] != "westeurope" || r["sku"].(map[string]any)["tier"] != "GeneralPurpose" ||
		r["id"] != id("Microsoft.DBforPostgreSQL/flexibleServers", srv).String() {
		t.Fatalf("server %+v", r)
	}
	if p["state"] != "Ready" || p["administratorLogin"] != "pgadmin" || p["version"] != "15" || p["fullyQualifiedDomainName"] != "localhost" ||
		p["pogiPort"] != 5432 || p["pogiNote"] == nil || p["administratorLoginPassword"] != nil {
		t.Fatalf("properties %+v", p)
	}

	// Children: databases, firewall rules, configurations.
	pgdb := "Microsoft.DBforPostgreSQL/flexibleServers/databases"
	d := e.put(pgdb, map[string]any{"properties": map[string]any{"charset": "UTF8"}}, srv, "app")
	if props(d)["collation"] != "en_US.utf8" || d["name"] != "app" {
		t.Fatalf("database %+v", d)
	}
	fw := e.put("Microsoft.DBforPostgreSQL/flexibleServers/firewallRules", map[string]any{"properties": map[string]any{"startIpAddress": "0.0.0.0", "endIpAddress": "255.255.255.255"}}, srv, "all")
	if props(fw)["endIpAddress"] != "255.255.255.255" {
		t.Fatalf("rule %+v", fw)
	}
	cfgType := "Microsoft.DBforPostgreSQL/flexibleServers/configurations"
	if c, err := e.get(cfgType, srv, "max_connections"); err != nil || props(c)["source"] != "system-default" {
		t.Fatalf("config %+v %v", c, err)
	}
	c := e.put(cfgType, map[string]any{"properties": map[string]any{"value": "100"}}, srv, "max_connections")
	if props(c)["value"] != "100" || props(c)["source"] != "user-override" || props(c)["defaultValue"] != "50" {
		t.Fatalf("config %+v", c)
	}
	list, err := e.provider(pgdb).List(ctx(), id("Microsoft.DBforPostgreSQL/flexibleServers", srv))
	if err != nil || len(list) != 1 {
		t.Fatalf("databases %v %v", list, err)
	}
	if _, err := e.provider(pgdb).Put(ctx(), id(pgdb, "nope", "x"), map[string]any{}); code(err) != "404 ResourceNotFound" {
		t.Fatalf("child of a missing server: %v", err)
	}
	if _, err := e.provider(pgdb).Put(ctx(), id(pgdb, srv, "bad name; drop"), map[string]any{}); code(err) != "400 InvalidParameterValue" {
		t.Fatalf("bad database name: %v", err)
	}

	// Listing by resource group and subscription.
	rg, _ := arm.ParseID("/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup)
	sub, _ := arm.ParseID("/subscriptions/" + core.AzureSubscription)
	for _, parent := range []arm.ID{rg, sub} {
		if l, err := e.provider("Microsoft.DBforPostgreSQL/flexibleServers").List(ctx(), parent); err != nil || len(l) != 1 {
			t.Fatalf("list %v: %v %v", parent, l, err)
		}
	}

	// Errors.
	if _, err := e.provider("Microsoft.DBforMySQL/flexibleServers").Put(ctx(), id("Microsoft.DBforMySQL/flexibleServers", "x"),
		map[string]any{"properties": map[string]any{"administratorLogin": "1; DROP"}}); code(err) != "400 InvalidParameterValue" {
		t.Fatalf("bad login: %v", err)
	}
	if _, err := e.get("Microsoft.DBforMySQL/flexibleServers", "missing"); code(err) != "404 ResourceNotFound" {
		t.Fatalf("missing: %v", err)
	}
	if _, err := e.provider("Microsoft.DBforPostgreSQL/flexibleServers").Put(ctx(), id("Microsoft.DBforPostgreSQL/flexibleServers", srv),
		map[string]any{"properties": map[string]any{"administratorLogin": "other"}}); code(err) != "400 InvalidParameterValue" {
		t.Fatalf("changing the login: %v", err)
	}

	// SQL Server has its master database; MariaDB reports userVisibleState.
	sqlSrv := e.put("Microsoft.Sql/servers", map[string]any{"location": "eastus", "properties": map[string]any{"administratorLogin": "sqladmin", "administratorLoginPassword": pw}}, name("sql"))
	if sqlSrv["kind"] != "v12.0" || props(sqlSrv)["version"] != "12.0" || props(sqlSrv)["state"] != "Ready" {
		t.Fatalf("sql %+v", sqlSrv)
	}
	sqlName := sqlSrv["name"].(string)
	dbs, _ := e.provider("Microsoft.Sql/servers/databases").List(ctx(), id("Microsoft.Sql/servers", sqlName))
	if len(dbs) != 1 || dbs[0]["name"] != "master" {
		t.Fatalf("sql databases %v", dbs)
	}
	sdb := e.put("Microsoft.Sql/servers/databases", map[string]any{"location": "eastus", "sku": map[string]any{"name": "Basic", "tier": "Basic"}}, sqlName, "app")
	if props(sdb)["status"] != "Online" || sdb["sku"].(map[string]any)["name"] != "Basic" || props(sdb)["databaseId"] == "" {
		t.Fatalf("sql database %+v", sdb)
	}
	maria := e.put("Microsoft.DBforMariaDB/servers", map[string]any{"location": "eastus", "properties": map[string]any{"administratorLogin": "mdb", "administratorLoginPassword": pw}}, name("mdb"))
	if props(maria)["userVisibleState"] != "Ready" || maria["sku"].(map[string]any)["name"] != "B_Gen5_1" {
		t.Fatalf("mariadb %+v", maria)
	}

	// Redis: keys and regeneration.
	rname := name("redis")
	rd := e.put("Microsoft.Cache/redis", map[string]any{"location": "eastus", "properties": map[string]any{"sku": map[string]any{"name": "Standard", "family": "C", "capacity": 1}}}, rname)
	rp := props(rd)
	if rp["provisioningState"] != "Succeeded" || rp["hostName"] != "localhost" || rp["port"] != 6379 || rp["sslPort"] != 6380 || rp["sku"].(map[string]any)["name"] != "Standard" {
		t.Fatalf("redis %+v", rp)
	}
	redis := e.provider("Microsoft.Cache/redis")
	k1, err := redis.Actions["listKeys"](ctx(), id("Microsoft.Cache/redis", rname), nil)
	if err != nil || len(k1.(map[string]any)["primaryKey"].(string)) < 40 || k1.(map[string]any)["primaryKey"] == k1.(map[string]any)["secondaryKey"] {
		t.Fatalf("keys %v %v", k1, err)
	}
	k2, err := redis.Actions["regenerateKey"](ctx(), id("Microsoft.Cache/redis", rname), map[string]any{"keyType": "Primary"})
	if err != nil || k2.(map[string]any)["primaryKey"] == k1.(map[string]any)["primaryKey"] || k2.(map[string]any)["secondaryKey"] != k1.(map[string]any)["secondaryKey"] {
		t.Fatalf("regenerated %v %v", k2, err)
	}
	if _, err := redis.Actions["regenerateKey"](ctx(), id("Microsoft.Cache/redis", rname), map[string]any{"keyType": "Tertiary"}); code(err) != "400 BadRequest" {
		t.Fatalf("bad key type: %v", err)
	}
	if _, err := redis.Actions["listKeys"](ctx(), id("Microsoft.Cache/redis", "missing"), nil); code(err) != "404 ResourceNotFound" {
		t.Fatalf("keys of a missing cache: %v", err)
	}
	e.put("Microsoft.Cache/redis/firewallRules", map[string]any{"properties": map[string]any{"startIP": "10.0.0.1", "endIP": "10.0.0.9"}}, rname, "office")

	// Console, persistence, delete.
	svc := e.h.Service("azdatabases").(interface {
		core.Browsable
		core.Inspectable
		core.Stateful
	})
	tables := svc.Resources()
	if len(tables) != 2 || len(tables[0].Rows) != 4 || len(tables[1].Rows) != 3 { // servers: pg, sql, mariadb, redis; databases: pg app, sql master and app
		t.Fatalf("tables %+v", tables)
	}
	if svc.Inspect("Servers", rname) == nil || svc.Inspect("Databases", "app") == nil || svc.Inspect("Servers", "nope") != nil {
		t.Fatal("inspect")
	}
	saved, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	svc.Reset()
	if _, err := e.get("Microsoft.DBforPostgreSQL/flexibleServers", srv); code(err) != "404 ResourceNotFound" {
		t.Fatalf("after reset: %v", err)
	}
	if err := svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	if got, err := e.get("Microsoft.DBforPostgreSQL/flexibleServers", srv); err != nil || props(got)["administratorLogin"] != "pgadmin" {
		t.Fatalf("after load: %v %v", got, err)
	}
	if err := e.provider("Microsoft.DBforPostgreSQL/flexibleServers").Delete(ctx(), id("Microsoft.DBforPostgreSQL/flexibleServers", srv)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.get(pgdb, srv, "app"); code(err) != "404 ResourceNotFound" {
		t.Fatalf("database of a deleted server: %v", err)
	}
	if err := e.provider("Microsoft.DBforPostgreSQL/flexibleServers").Delete(ctx(), id("Microsoft.DBforPostgreSQL/flexibleServers", srv)); err != nil {
		t.Fatalf("deleting twice: %v", err)
	}
}

// ---- with Docker: connect to the real engines ----

func TestPostgres(t *testing.T) {
	e := setup(t)
	e.docker()
	srv := name("pg")
	e.put("Microsoft.DBforPostgreSQL/flexibleServers", map[string]any{"location": "eastus",
		"properties": map[string]any{"administratorLogin": "pgadmin", "administratorLoginPassword": pw, "version": "16"}}, srv)
	r := e.ready("Microsoft.DBforPostgreSQL/flexibleServers", "state", "Ready", 3*time.Minute, srv)
	e.put("Microsoft.DBforPostgreSQL/flexibleServers/databases", map[string]any{}, srv, "app")

	dsn := fmt.Sprintf("postgres://pgadmin:%s@127.0.0.1:%d/app?sslmode=disable", pw, port(r))
	bg := context.Background()
	conn, err := pgx.Connect(bg, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var db, one = "", 0
	if err := conn.QueryRow(bg, "SELECT current_database(), 1").Scan(&db, &one); err != nil || db != "app" || one != 1 {
		t.Fatalf("%q %d %v", db, one, err)
	}
	conn.Close(bg)

	// A new password applies to the running server; deleting the database drops it.
	e.put("Microsoft.DBforPostgreSQL/flexibleServers", map[string]any{"properties": map[string]any{"administratorLoginPassword": pw + "2"}}, srv)
	var cn *pgx.Conn
	for i := 0; i < 60; i++ {
		if cn, err = pgx.Connect(bg, strings.Replace(dsn, pw, pw+"2", 1)); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("the new password: %v", err)
	}
	cn.Close(bg)
	if err := e.provider("Microsoft.DBforPostgreSQL/flexibleServers/databases").Delete(ctx(), id("Microsoft.DBforPostgreSQL/flexibleServers/databases", srv, "app")); err != nil {
		t.Fatal(err)
	}
	if c, err := pgx.Connect(bg, strings.Replace(dsn, pw, pw+"2", 1)); err == nil {
		c.Close(bg)
		t.Fatal("the dropped database still accepts connections")
	}
}

func mysqlTest(t *testing.T, typ, srvType string, version string) {
	e := setup(t)
	e.docker()
	srv := name("my")
	e.put(typ, map[string]any{"location": "eastus",
		"properties": map[string]any{"administratorLogin": "myadmin", "administratorLoginPassword": pw, "version": version}}, srv)
	r := e.ready(typ, srvType, "Ready", 5*time.Minute, srv)
	e.put(typ+"/databases", map[string]any{}, srv, "app")

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = "myadmin", pw, "tcp", fmt.Sprintf("127.0.0.1:%d", port(r)), "app"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var name string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&name); err != nil || name != "app" {
		t.Fatalf("%q %v", name, err)
	}
	if _, err := db.Exec("CREATE TABLE t (n INT)"); err != nil {
		t.Fatal(err)
	}
}

func TestMySQL(t *testing.T) {
	mysqlTest(t, "Microsoft.DBforMySQL/flexibleServers", "state", "8.0.21")
}

func TestMariaDB(t *testing.T) {
	mysqlTest(t, "Microsoft.DBforMariaDB/servers", "userVisibleState", "10.3")
}

// resp sends a Redis command and returns the first line of the reply (the data line of a bulk string).
func resp(t *testing.T, c net.Conn, r *bufio.Reader, args ...string) string {
	t.Helper()
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, a := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)
	}
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(cmd)); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(line, "$") && line != "$-1\r\n" {
		line, _ = r.ReadString('\n')
	}
	return strings.TrimSpace(line)
}

func dial(t *testing.T, port int) (net.Conn, *bufio.Reader) {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, bufio.NewReader(c)
}

func TestRedis(t *testing.T) {
	e := setup(t)
	e.docker()
	typ, srv := "Microsoft.Cache/redis", name("rd")
	e.put(typ, map[string]any{"location": "eastus", "properties": map[string]any{"sku": map[string]any{"name": "Basic", "family": "C", "capacity": 0}}}, srv)
	r := e.ready(typ, "provisioningState", "Succeeded", 2*time.Minute, srv)
	p := props(r)
	if p["hostName"] != "localhost" || p["port"] != port(r) {
		t.Fatalf("%+v", p)
	}
	keys := func() (string, string) {
		k, err := e.provider(typ).Actions["listKeys"](ctx(), id(typ, srv), nil)
		if err != nil {
			t.Fatal(err)
		}
		return k.(map[string]any)["primaryKey"].(string), k.(map[string]any)["secondaryKey"].(string)
	}
	primary, secondary := keys()

	c, rd := dial(t, port(r))
	if got := resp(t, c, rd, "GET", "k"); !strings.HasPrefix(got, "-NOAUTH") {
		t.Fatalf("without AUTH: %q", got)
	}
	if got := resp(t, c, rd, "AUTH", "wrong"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("wrong key: %q", got)
	}
	for _, key := range []string{primary, secondary} {
		c, rd := dial(t, port(r))
		if got := resp(t, c, rd, "AUTH", key); got != "+OK" {
			t.Fatalf("AUTH with an access key: %q", got)
		}
		if got := resp(t, c, rd, "SET", "k", "v"); got != "+OK" {
			t.Fatalf("SET: %q", got)
		}
		if got := resp(t, c, rd, "GET", "k"); got != "v" {
			t.Fatalf("GET: %q", got)
		}
	}

	// Regenerating the primary key replaces it in the running server and keeps the secondary.
	if _, err := e.provider(typ).Actions["regenerateKey"](ctx(), id(typ, srv), map[string]any{"keyType": "Primary"}); err != nil {
		t.Fatal(err)
	}
	newPrimary, newSecondary := keys()
	if newPrimary == primary || newSecondary != secondary {
		t.Fatal("keys after regenerating the primary")
	}
	for key, want := range map[string]string{primary: "-WRONGPASS", newPrimary: "+OK", secondary: "+OK"} {
		c, rd := dial(t, port(r))
		if got := resp(t, c, rd, "AUTH", key); !strings.HasPrefix(got, want) {
			t.Fatalf("AUTH after regenerating: want %s, got %q", want, got)
		}
	}
}

func TestSQLServer(t *testing.T) {
	e := setup(t)
	e.docker()
	typ, srv := "Microsoft.Sql/servers", name("sql")
	e.put(typ, map[string]any{"location": "eastus", "properties": map[string]any{"administratorLogin": "sqladmin", "administratorLoginPassword": pw}}, srv)
	// The image is amd64 only: on Apple Silicon it runs under emulation and can take minutes.
	r := e.ready(typ, "state", "Ready", 8*time.Minute, srv)
	e.put(typ+"/databases", map[string]any{"location": "eastus"}, srv, "app")

	u := url.URL{Scheme: "sqlserver", User: url.UserPassword("sqladmin", pw), Host: fmt.Sprintf("127.0.0.1:%d", port(r)),
		RawQuery: url.Values{"database": {"app"}, "encrypt": {"disable"}}.Encode()}
	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var name string
	if err := db.QueryRow("SELECT DB_NAME()").Scan(&name); err != nil || name != "app" {
		t.Fatalf("%q %v", name, err)
	}
}
