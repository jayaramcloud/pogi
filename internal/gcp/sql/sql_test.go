package sql_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	dbsql "database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	sqlv1 "google.golang.org/api/sqladmin/v1"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// start runs pogi with Cloud SQL and Cloud Storage; an empty dockerHost finds the container engine.
func start(t *testing.T, dockerHost string) (*pogi.Running, *sqladmin.Service) {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"sqladmin", "gcs"}, DockerHost: dockerHost})
	return srv, must(sqladmin.NewService(ctx, option.WithEndpoint(srv.URL), option.WithoutAuthentication()))
}

func name(prefix string) string { return prefix + "-" + strings.ToLower(core.RandHex(4)) }

// wait polls an operation until it is DONE, and fails the test if it ended in an error.
func wait(t *testing.T, svc *sqladmin.Service, project string, op *sqladmin.Operation, err error) *sqladmin.Operation {
	t.Helper()
	if err != nil {
		t.Fatalf("operation: %v", err)
	}
	for end := time.Now().Add(4 * time.Minute); op.Status != "DONE"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("operation %s %s is still %s", op.OperationType, op.Name, op.Status)
		}
		var err error
		if op, err = svc.Operations.Get(project, op.Name).Do(); err != nil {
			t.Fatal(err)
		}
	}
	if op.Error != nil {
		t.Fatalf("operation %s failed: %v", op.OperationType, op.Error.Errors[0])
	}
	return op
}

func code(err error) int {
	if e, ok := err.(*googleapi.Error); ok {
		return e.Code
	}
	return 0
}

func reason(err error) string {
	if e, ok := err.(*googleapi.Error); ok && len(e.Errors) > 0 {
		return e.Errors[0].Reason
	}
	return ""
}

func primary(t *testing.T, i *sqladmin.DatabaseInstance) string {
	t.Helper()
	for _, ip := range i.IpAddresses {
		if ip.Type == "PRIMARY" {
			return ip.IpAddress
		}
	}
	t.Fatalf("no PRIMARY address: %+v", i.IpAddresses)
	return ""
}

// create makes an instance and waits until it runs.
func create(t *testing.T, svc *sqladmin.Service, p, id, version, pw string) *sqladmin.DatabaseInstance {
	t.Helper()
	op, err := svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: id, Region: "us-central1", DatabaseVersion: version, RootPassword: pw,
		Settings: &sqladmin.Settings{Tier: "db-f1-micro"}}).Do()
	wait(t, svc, p, op, err)
	return must(svc.Instances.Get(p, id).Do())
}

// ---- the API, without a container engine ----

func TestInstances(t *testing.T) {
	_, svc := start(t, "/nonexistent/docker.sock")
	p, id := "demo", name("pg")

	op := must(svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: id, Region: "europe-west1", DatabaseVersion: "POSTGRES_16",
		RootPassword: "root-pw", Settings: &sqladmin.Settings{Tier: "db-f1-micro", UserLabels: map[string]string{"env": "test"}}}).Do())
	if op.OperationType != "CREATE" || op.TargetId != id || op.Status != "DONE" || !strings.HasSuffix(op.TargetLink, "/projects/demo/instances/"+id) {
		t.Fatalf("create operation: %+v", op)
	}
	i := must(svc.Instances.Get(p, id).Do())
	if i.State != "RUNNABLE" || i.ConnectionName != "demo:europe-west1:"+id || i.Settings.Tier != "db-f1-micro" || i.Settings.SettingsVersion != 1 ||
		i.DatabaseVersion != "POSTGRES_16" || i.Kind != "sql#instance" || i.BackendType != "SECOND_GEN" || i.RootPassword != "" ||
		i.Settings.DataDiskSizeGb != 10 || i.Settings.ActivationPolicy != "ALWAYS" || i.GceZone != "europe-west1-a" {
		t.Fatalf("instance: %+v settings %+v", i, i.Settings)
	}
	if a := primary(t, i); a != "127.0.0.1:5432" {
		t.Errorf("without Docker the address is the engine's own port: %s", a)
	}
	if i.Settings.IpConfiguration == nil || !i.Settings.IpConfiguration.Ipv4Enabled {
		t.Errorf("ipv4 defaults to enabled: %+v", i.Settings.IpConfiguration)
	}
	if ca := i.ServerCaCert; ca == nil || !strings.Contains(ca.Cert, "BEGIN CERTIFICATE") || ca.Sha1Fingerprint == "" {
		t.Errorf("server CA: %+v", ca)
	}
	if l := must(svc.Instances.List(p).Do()); len(l.Items) != 1 || l.Items[0].Name != id || l.Kind != "sql#instancesList" {
		t.Errorf("list: %+v", l)
	}
	if l := must(svc.Instances.List("other").Do()); len(l.Items) != 0 {
		t.Errorf("another project lists %d instances", len(l.Items))
	}

	// errors, with the reasons the SDKs and Terraform branch on
	_, err := svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: id, DatabaseVersion: "POSTGRES_16"}).Do()
	if code(err) != 409 || reason(err) != "instanceAlreadyExists" {
		t.Errorf("duplicate: %v", err)
	}
	_, err = svc.Instances.Get(p, "nope").Do()
	if code(err) != 404 || reason(err) != "instanceDoesNotExist" {
		t.Errorf("missing: %v", err)
	}
	if _, err = svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: name("x"), DatabaseVersion: "ORACLE_19"}).Do(); code(err) != 400 {
		t.Errorf("unknown version: %v", err)
	}
	if _, err = svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: "Bad_Name", DatabaseVersion: "POSTGRES_16"}).Do(); code(err) != 400 {
		t.Errorf("bad name: %v", err)
	}

	// patch merges, update replaces the settings, and both bump settingsVersion
	op = must(svc.Instances.Patch(p, id, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{Tier: "db-g1-small",
		DatabaseFlags: []*sqladmin.DatabaseFlags{{Name: "max_connections", Value: "50"}}}}).Do())
	if op.OperationType != "UPDATE" || op.Status != "DONE" {
		t.Errorf("patch operation: %+v", op)
	}
	i = must(svc.Instances.Get(p, id).Do())
	if i.Settings.Tier != "db-g1-small" || i.Settings.SettingsVersion != 2 || i.Settings.UserLabels["env"] != "test" ||
		len(i.Settings.DatabaseFlags) != 1 || i.DatabaseVersion != "POSTGRES_16" || i.Region != "europe-west1" {
		t.Errorf("patched: %+v %+v", i, i.Settings)
	}
	must(svc.Instances.Update(p, id, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{Tier: "db-custom-2-7680"}}).Do())
	i = must(svc.Instances.Get(p, id).Do())
	if i.Settings.Tier != "db-custom-2-7680" || i.Settings.SettingsVersion != 3 || len(i.Settings.UserLabels) != 0 {
		t.Errorf("updated: %+v", i.Settings)
	}

	// operations
	if l := must(svc.Operations.List(p).Instance(id).Do()); len(l.Items) != 3 || l.Items[0].Kind != "sql#operation" {
		t.Errorf("operations of the instance: %d", len(l.Items))
	}
	if _, err := svc.Operations.Get(p, "nope").Do(); code(err) != 404 {
		t.Errorf("missing operation: %v", err)
	}

	// tiers and flags
	if tl := must(svc.Tiers.List(p).Do()); !slices.ContainsFunc(tl.Items, func(x *sqladmin.Tier) bool { return x.Tier == "db-f1-micro" }) {
		t.Errorf("tiers: %+v", tl.Items)
	}
	fl := must(svc.Flags.List().DatabaseVersion("MYSQL_8_0").Do())
	if len(fl.Items) == 0 || slices.ContainsFunc(fl.Items, func(f *sqladmin.Flag) bool { return !slices.Contains(f.AppliesTo, "MYSQL_8_0") }) {
		t.Errorf("flags for MySQL: %+v", fl.Items)
	}

	// deletion protection
	must(svc.Instances.Patch(p, id, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{DeletionProtectionEnabled: true}}).Do())
	if _, err := svc.Instances.Delete(p, id).Do(); code(err) != 400 {
		t.Errorf("a protected instance is deleted: %v", err)
	}
	must(svc.Instances.Patch(p, id, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{DeletionProtectionEnabled: false,
		ForceSendFields: []string{"DeletionProtectionEnabled"}}}).Do())
	if op = must(svc.Instances.Delete(p, id).Do()); op.OperationType != "DELETE" {
		t.Errorf("delete operation: %+v", op)
	}
	if _, err = svc.Instances.Get(p, id).Do(); code(err) != 404 {
		t.Errorf("deleted instance: %v", err)
	}
}

func TestDatabasesAndUsers(t *testing.T) {
	_, svc := start(t, "/nonexistent/docker.sock")
	p, id := "demo", name("my")
	wait(t, svc, p, must(svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: id, DatabaseVersion: "MYSQL_8_0"}).Do()), nil)

	dbs := must(svc.Databases.List(p, id).Do())
	if len(dbs.Items) != 4 || dbs.Items[0].Kind != "sql#database" || dbs.Kind != "sql#databasesList" {
		t.Errorf("MySQL starts with its system databases: %+v", dbs.Items)
	}
	wait(t, svc, p, must(svc.Databases.Insert(p, id, &sqladmin.Database{Name: "shop", Charset: "utf8mb4", Collation: "utf8mb4_bin"}).Do()), nil)
	d := must(svc.Databases.Get(p, id, "shop").Do())
	if d.Charset != "utf8mb4" || d.Collation != "utf8mb4_bin" || d.Instance != id || !strings.HasSuffix(d.SelfLink, "/instances/"+id+"/databases/shop") {
		t.Errorf("database: %+v", d)
	}
	if _, err := svc.Databases.Insert(p, id, &sqladmin.Database{Name: "shop"}).Do(); code(err) != 409 {
		t.Errorf("duplicate database: %v", err)
	}
	if _, err := svc.Databases.Insert(p, id, &sqladmin.Database{Name: "a b; drop"}).Do(); code(err) != 400 {
		t.Errorf("bad database name: %v", err)
	}
	must(svc.Databases.Patch(p, id, "shop", &sqladmin.Database{Collation: "utf8mb4_general_ci"}).Do())
	if d = must(svc.Databases.Get(p, id, "shop").Do()); d.Collation != "utf8mb4_general_ci" || d.Charset != "utf8mb4" {
		t.Errorf("patched database: %+v", d)
	}
	if _, err := svc.Databases.Delete(p, id, "mysql").Do(); code(err) != 400 {
		t.Errorf("a system database is deleted: %v", err)
	}
	wait(t, svc, p, must(svc.Databases.Delete(p, id, "shop").Do()), nil)
	if _, err := svc.Databases.Get(p, id, "shop").Do(); code(err) != 404 {
		t.Errorf("deleted database: %v", err)
	}
	if _, err := svc.Databases.List(p, "nope").Do(); code(err) != 404 {
		t.Errorf("databases of a missing instance: %v", err)
	}

	us := must(svc.Users.List(p, id).Do())
	if len(us.Items) != 1 || us.Items[0].Name != "root" || us.Items[0].Host != "%" || us.Kind != "sql#usersList" {
		t.Errorf("MySQL starts with root: %+v", us.Items)
	}
	wait(t, svc, p, must(svc.Users.Insert(p, id, &sqladmin.User{Name: "app", Password: "s3cret"}).Do()), nil)
	us = must(svc.Users.List(p, id).Do())
	if len(us.Items) != 2 || us.Items[1].Name != "app" || us.Items[1].Host != "%" || us.Items[1].Password != "" || us.Items[1].Type != "BUILT_IN" {
		t.Errorf("users: %+v", us.Items)
	}
	if _, err := svc.Users.Insert(p, id, &sqladmin.User{Name: "app", Password: "x"}).Do(); code(err) != 409 {
		t.Errorf("duplicate user: %v", err)
	}
	wait(t, svc, p, must(svc.Users.Insert(p, id, &sqladmin.User{Name: "app", Host: "10.0.0.1", Password: "x"}).Do()), nil) // another host is another account
	wait(t, svc, p, must(svc.Users.Update(p, id, &sqladmin.User{Password: "new"}).Name("app").Host("10.0.0.1").Do()), nil)
	if _, err := svc.Users.Update(p, id, &sqladmin.User{Password: "new"}).Name("ghost").Do(); code(err) != 404 {
		t.Errorf("update of a missing user: %v", err)
	}
	if _, err := svc.Users.Delete(p, id).Name("root").Host("%").Do(); code(err) != 400 {
		t.Errorf("the built-in administrator is deleted: %v", err)
	}
	wait(t, svc, p, must(svc.Users.Delete(p, id).Name("app").Host("10.0.0.1").Do()), nil)
	if us = must(svc.Users.List(p, id).Do()); len(us.Items) != 2 {
		t.Errorf("users after delete: %+v", us.Items)
	}
	if u := must(svc.Users.Get(p, id, "app").Do()); u.Name != "app" {
		t.Errorf("get user: %+v", u)
	}
	if _, err := svc.Users.Get(p, id, "ghost").Do(); code(err) != 404 {
		t.Errorf("missing user: %v", err)
	}
	// IAM users are recorded, not created in the engine
	wait(t, svc, p, must(svc.Users.Insert(p, id, &sqladmin.User{Name: "dev@example.com", Type: "CLOUD_IAM_USER"}).Do()), nil)
}

func TestCertificates(t *testing.T) {
	_, svc := start(t, "/nonexistent/docker.sock")
	p, id := "demo", name("pg")
	wait(t, svc, p, must(svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: id, DatabaseVersion: "POSTGRES_15"}).Do()), nil)
	i := must(svc.Instances.Get(p, id).Do())
	caBlock, _ := pem.Decode([]byte(i.ServerCaCert.Cert))
	caCert := must(x509.ParseCertificate(caBlock.Bytes))
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	// connectSettings: what the connectors read
	cs := must(svc.Connect.Get(p, id).Do())
	if cs.ConnectionName != i.ConnectionName || cs.DatabaseVersion != "POSTGRES_15" || cs.Region != "us-central1" || cs.ServerCaCert.Cert != i.ServerCaCert.Cert ||
		len(cs.IpAddresses) != 1 || cs.IpAddresses[0].IpAddress != primary(t, i) || cs.Kind != "sql#connectSettings" {
		t.Errorf("connect settings: %+v", cs)
	}

	// generateEphemeralCert signs the connector's key with the instance's CA
	key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	der := must(x509.MarshalPKIXPublicKey(&key.PublicKey))
	pub := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	res := must(svc.Connect.GenerateEphemeralCert(p, id, &sqladmin.GenerateEphemeralCertRequest{PublicKey: pub}).Do())
	b, _ := pem.Decode([]byte(res.EphemeralCert.Cert))
	leaf := must(x509.ParseCertificate(b.Bytes))
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("the ephemeral certificate does not verify against the instance CA: %v", err)
	}
	if !leaf.PublicKey.(*ecdsa.PublicKey).Equal(&key.PublicKey) || time.Until(leaf.NotAfter) > 61*time.Minute || time.Until(leaf.NotAfter) < 30*time.Minute {
		t.Errorf("ephemeral certificate: key matches %v, expires %v", leaf.PublicKey.(*ecdsa.PublicKey).Equal(&key.PublicKey), leaf.NotAfter)
	}
	if _, err := svc.Connect.GenerateEphemeralCert(p, id, &sqladmin.GenerateEphemeralCertRequest{PublicKey: "junk"}).Do(); code(err) != 400 {
		t.Errorf("a bad public key: %v", err)
	}
	if _, err := svc.Connect.Get(p, "nope").Do(); code(err) != 404 {
		t.Errorf("connect settings of a missing instance: %v", err)
	}

	// sslCerts: a client certificate with its private key
	ins := must(svc.SslCerts.Insert(p, id, &sqladmin.SslCertsInsertRequest{CommonName: "client-1"}).Do())
	ci := ins.ClientCert.CertInfo
	if ci.CommonName != "client-1" || ci.Sha1Fingerprint == "" || ins.ClientCert.CertPrivateKey == "" || ins.ServerCaCert.Cert != i.ServerCaCert.Cert {
		t.Fatalf("client certificate: %+v", ins)
	}
	cb, _ := pem.Decode([]byte(ci.Cert))
	if _, err := must(x509.ParseCertificate(cb.Bytes)).Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("the client certificate does not verify: %v", err)
	}
	if l := must(svc.SslCerts.List(p, id).Do()); len(l.Items) != 1 || l.Items[0].Sha1Fingerprint != ci.Sha1Fingerprint {
		t.Errorf("list: %+v", l.Items)
	}
	if g := must(svc.SslCerts.Get(p, id, ci.Sha1Fingerprint).Do()); g.CommonName != "client-1" {
		t.Errorf("get: %+v", g)
	}
	must(svc.SslCerts.Delete(p, id, ci.Sha1Fingerprint).Do())
	if _, err := svc.SslCerts.Get(p, id, ci.Sha1Fingerprint).Do(); code(err) != 404 {
		t.Errorf("deleted certificate: %v", err)
	}
}

// TestV1 serves the v1 API too, whose paths (/v1/projects/...) are shared with other
// Google APIs: the operations of Cloud SQL still come to it, not to the shared operations API.
func TestV1(t *testing.T) {
	srv, _ := start(t, "/nonexistent/docker.sock")
	svc := must(sqlv1.NewService(ctx, option.WithEndpoint(srv.URL), option.WithoutAuthentication()))
	id := name("v1")
	op := must(svc.Instances.Insert("demo", &sqlv1.DatabaseInstance{Name: id, DatabaseVersion: "POSTGRES_17"}).Do())
	if op = must(svc.Operations.Get("demo", op.Name).Do()); op.Status != "DONE" || op.TargetId != id {
		t.Errorf("operation: %+v", op)
	}
	if i := must(svc.Instances.Get("demo", id).Do()); i.State != "RUNNABLE" {
		t.Errorf("instance: %+v", i)
	}
	if _, err := svc.Operations.Get("demo", "someone-elses").Do(); code(err) != 404 {
		t.Errorf("an operation of another API: %v", err)
	}
	if tl := must(svc.Tiers.List("demo").Do()); len(tl.Items) == 0 {
		t.Error("no tiers")
	}
}

func TestPersistence(t *testing.T) {
	srv, svc := start(t, "/nonexistent/docker.sock")
	id := name("pg")
	wait(t, svc, "demo", must(svc.Instances.Insert("demo", &sqladmin.DatabaseInstance{Name: id, DatabaseVersion: "POSTGRES_16"}).Do()), nil)
	wait(t, svc, "demo", must(svc.Users.Insert("demo", id, &sqladmin.User{Name: "app", Password: "pw"}).Do()), nil)
	b, err := srv.Hub.Service("sqladmin").(core.Stateful).Save()
	if err != nil {
		t.Fatal(err)
	}
	srv.Reset("sqladmin")
	if _, err := svc.Instances.Get("demo", id).Do(); code(err) != 404 {
		t.Fatalf("reset: %v", err)
	}
	if err := srv.Hub.Service("sqladmin").(core.Stateful).Load(b); err != nil {
		t.Fatal(err)
	}
	if us := must(svc.Users.List("demo", id).Do()); len(us.Items) != 2 {
		t.Errorf("users after load: %+v", us.Items)
	}
	if i := must(svc.Instances.Get("demo", id).Do()); i.ServerCaCert == nil || i.State != "RUNNABLE" {
		t.Errorf("instance after load: %+v", i)
	}
	// the console lists and inspects it
	rows := srv.Hub.Service("sqladmin").(core.Browsable).Resources()[0].Rows
	if len(rows) != 1 || rows[0][0] != id || srv.Hub.Service("sqladmin").(core.Inspectable).Inspect("Instances", "demo/"+id) == nil {
		t.Errorf("console: %v", rows)
	}
}

// ---- the container engine, faked: what Cloud SQL asks of it ----

func TestEngineCalls(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() { // anything that answers is a ready server
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				c.Write([]byte("N"))
				c.SetReadDeadline(time.Now().Add(time.Second))
				io.Copy(io.Discard, c)
			}()
		}
	}()
	eng := dockertest.Start(t, ln.Addr().(*net.TCPAddr).Port, false)
	_, svc := start(t, "")
	p := "demo"

	pg := name("pg")
	op, err := svc.Instances.Insert(p, &sqladmin.DatabaseInstance{Name: pg, DatabaseVersion: "POSTGRES_16", RootPassword: "root-pw"}).Do()
	wait(t, svc, p, op, err)
	prefix := "pogi-cloudsql-" + pg
	creates := eng.Called("create --name " + prefix)
	if len(creates) != 1 || !strings.HasSuffix(creates[0], " postgres:16-alpine") || !strings.Contains(creates[0], " -e POSTGRES_PASSWORD=root-pw") ||
		!strings.Contains(creates[0], " -e POSTGRES_USER=postgres") || !strings.Contains(creates[0], ":5432 ") {
		t.Fatalf("container: %v", creates)
	}
	i := must(svc.Instances.Get(p, pg).Do())
	if i.State != "RUNNABLE" || primary(t, i) != fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port) {
		t.Errorf("instance: %s %+v", i.State, i.IpAddresses)
	}

	// users and databases are made in the engine
	wait(t, svc, p, must(svc.Databases.Insert(p, pg, &sqladmin.Database{Name: "shop"}).Do()), nil)
	wait(t, svc, p, must(svc.Users.Insert(p, pg, &sqladmin.User{Name: "app", Password: "it's"}).Do()), nil)
	wait(t, svc, p, must(svc.Users.Update(p, pg, &sqladmin.User{Password: "p2"}).Name("app").Do()), nil)
	wait(t, svc, p, must(svc.Users.Delete(p, pg).Name("app").Do()), nil)
	wait(t, svc, p, must(svc.Databases.Delete(p, pg, "shop").Do()), nil)
	exec := strings.Join(eng.Called("exec "+prefix), "\n")
	for _, want := range []string{`CREATE DATABASE "shop"`, `GRANT ALL ON SCHEMA public TO PUBLIC`, `CREATE ROLE "app" LOGIN CREATEDB CREATEROLE PASSWORD 'it''s'`,
		`ALTER ROLE "app" PASSWORD 'p2'`, `DROP ROLE "app"`, `DROP DATABASE "shop" WITH (FORCE)`} {
		if !strings.Contains(exec, want) {
			t.Errorf("no %q in the commands run in the container:\n%s", want, exec)
		}
	}
	// the root password reaches the engine and the next commands
	must(svc.Instances.Patch(p, pg, &sqladmin.DatabaseInstance{RootPassword: "root-2"}).Do())
	if !strings.Contains(strings.Join(eng.Called("exec "+prefix), "\n"), `ALTER ROLE "postgres" PASSWORD 'root-2'`) {
		t.Error("the root password was not changed in the engine")
	}

	// activationPolicy stops and starts the container; restart restarts it
	op = must(svc.Instances.Patch(p, pg, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{ActivationPolicy: "NEVER"}}).Do())
	wait(t, svc, p, op, nil)
	if len(eng.Called("stop "+prefix)) != 1 {
		t.Error("NEVER does not stop the container")
	}
	if _, err := svc.Users.Insert(p, pg, &sqladmin.User{Name: "late"}).Do(); code(err) != 409 || reason(err) != "operationInProgress" {
		t.Errorf("a user on a stopped instance: %v", err)
	}
	starts := len(eng.Called("start " + prefix)) // creating the container started it once
	wait(t, svc, p, must(svc.Instances.Patch(p, pg, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{ActivationPolicy: "ALWAYS"}}).Do()), nil)
	if len(eng.Called("start "+prefix)) != starts+1 {
		t.Error("ALWAYS does not start the container")
	}
	wait(t, svc, p, must(svc.Instances.Restart(p, pg).Do()), nil)
	if len(eng.Called("restart "+prefix)) != 1 {
		t.Error("restart does not restart the container")
	}
	wait(t, svc, p, must(svc.Instances.Delete(p, pg).Do()), nil)
	eventually(t, "the container to be removed", func() bool { return len(eng.Called("rm -f -v "+prefix)) > 0 })

	// MySQL and SQL Server run their own images, and their own SQL
	my := name("my")
	create(t, svc, p, my, "MYSQL_8_4", "mysql-pw")
	wait(t, svc, p, must(svc.Users.Insert(p, my, &sqladmin.User{Name: "app", Password: "pw"}).Do()), nil)
	if c := eng.Called("create --name pogi-cloudsql-" + my); len(c) != 1 || !strings.HasSuffix(c[0], " mysql:8.4") || !strings.Contains(c[0], " -e MYSQL_ROOT_PASSWORD=mysql-pw") {
		t.Errorf("MySQL container: %v", c)
	}
	if e := strings.Join(eng.Called("exec pogi-cloudsql-"+my), "\n"); !strings.Contains(e, "CREATE USER 'app'@'%' IDENTIFIED BY 'pw'; GRANT ALL PRIVILEGES ON *.* TO 'app'@'%'") ||
		!strings.Contains(e, "MYSQL_PWD=mysql-pw") {
		t.Errorf("MySQL commands:\n%s", e)
	}
	ms := name("ms")
	create(t, svc, p, ms, "SQLSERVER_2022_STANDARD", "Sql-pw-1")
	wait(t, svc, p, must(svc.Users.Insert(p, ms, &sqladmin.User{Name: "app", Password: "Sql-pw-2"}).Do()), nil)
	if c := eng.Called("create --name pogi-cloudsql-" + ms); len(c) != 1 || !strings.Contains(c[0], "mcr.microsoft.com/mssql/server:2022-latest") || !strings.Contains(c[0], ":1433 ") {
		t.Errorf("SQL Server container: %v", c)
	}
	if e := strings.Join(eng.Called("exec pogi-cloudsql-"+ms), "\n"); !strings.Contains(e, "CREATE LOGIN [app] WITH PASSWORD = N'Sql-pw-2'") {
		t.Errorf("SQL Server commands:\n%s", e)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// ---- real engines ----

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
}

func gone(t *testing.T, srv *pogi.Running, container string) {
	t.Helper()
	eventually(t, container+" to be removed", func() bool { _, err := srv.Hub.Docker().Inspect(ctx, container); return err != nil })
}

func pgConnect(t *testing.T, addr, user, pw, db string) *pgx.Conn {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	cfg := must(pgx.ParseConfig(fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable connect_timeout=15", host, port, user, pw, db)))
	return must(pgx.ConnectConfig(ctx, cfg))
}

func TestPostgres(t *testing.T) {
	needDocker(t)
	srv, svc := start(t, "")
	p, id := "demo", name("pg")

	i := create(t, svc, p, id, "POSTGRES_16", "root-pw")
	addr := primary(t, i)

	wait(t, svc, p, must(svc.Databases.Insert(p, id, &sqladmin.Database{Name: "shop"}).Do()), nil)
	wait(t, svc, p, must(svc.Users.Insert(p, id, &sqladmin.User{Name: "app", Password: "app-pw"}).Do()), nil)

	// the reported address takes the user that was created
	conn := pgConnect(t, addr, "app", "app-pw", "shop")
	var who, db string
	if err := conn.QueryRow(ctx, "select current_user, current_database()").Scan(&who, &db); err != nil || who != "app" || db != "shop" {
		t.Fatalf("query: %q %q %v", who, db, err)
	}
	if _, err := conn.Exec(ctx, "create table t (n int); insert into t values (7)"); err != nil {
		t.Errorf("the user cannot use its database: %v", err)
	}
	conn.Exec(ctx, "drop table t") // as in PostgreSQL, a role that owns objects cannot be dropped
	conn.Close(ctx)
	if c, err := pgx.ConnectConfig(ctx, must(pgx.ParseConfig("postgres://app:wrong@"+addr+"/shop?sslmode=disable"))); err == nil {
		c.Close(ctx)
		t.Error("a wrong password connects")
	}
	// the root password is the one given at insert, and changes with the API
	pgConnect(t, addr, "postgres", "root-pw", "postgres").Close(ctx)
	wait(t, svc, p, must(svc.Users.Update(p, id, &sqladmin.User{Password: "root-2"}).Name("postgres").Do()), nil)
	pgConnect(t, addr, "postgres", "root-2", "postgres").Close(ctx)
	wait(t, svc, p, must(svc.Users.Update(p, id, &sqladmin.User{Password: "app-2"}).Name("app").Do()), nil)
	pgConnect(t, addr, "app", "app-2", "shop").Close(ctx)

	// a deleted user and database are gone from the engine
	wait(t, svc, p, must(svc.Users.Delete(p, id).Name("app").Do()), nil)
	if c, err := pgx.ConnectConfig(ctx, must(pgx.ParseConfig("postgres://app:app-2@"+addr+"/shop?sslmode=disable"))); err == nil {
		c.Close(ctx)
		t.Error("a deleted user connects")
	}
	wait(t, svc, p, must(svc.Databases.Delete(p, id, "shop").Do()), nil)
	root := pgConnect(t, addr, "postgres", "root-2", "postgres")
	var n int
	if err := root.QueryRow(ctx, "select count(*) from pg_database where datname = 'shop'").Scan(&n); err != nil || n != 0 {
		t.Errorf("shop is still in the engine: %d %v", n, err)
	}
	root.Close(ctx)

	// stopping takes the address away, starting brings the same data back
	wait(t, svc, p, must(svc.Instances.Patch(p, id, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{ActivationPolicy: "NEVER"}}).Do()), nil)
	// Docker's port proxy can accept for a moment after the container stops, so allow a few seconds.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			break
		}
		c.Close()
		if time.Now().After(deadline) {
			t.Error("a stopped instance answers")
			break
		}
	}
	wait(t, svc, p, must(svc.Instances.Patch(p, id, &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{ActivationPolicy: "ALWAYS"}}).Do()), nil)
	pgConnect(t, primary(t, must(svc.Instances.Get(p, id).Do())), "postgres", "root-2", "postgres").Close(ctx)

	// deleting the instance removes the container
	container := "pogi-cloudsql-" + id + "-demo-us-central1"
	if _, err := srv.Hub.Docker().Inspect(ctx, container); err != nil {
		t.Fatalf("no container %s: %v", container, err)
	}
	wait(t, svc, p, must(svc.Instances.Delete(p, id).Do()), nil)
	gone(t, srv, container)
}

func TestMySQL(t *testing.T) {
	needDocker(t)
	srv, svc := start(t, "")
	p, id := "demo", name("my")
	i := create(t, svc, p, id, "MYSQL_8_0", "root-pw")
	addr := primary(t, i)
	wait(t, svc, p, must(svc.Databases.Insert(p, id, &sqladmin.Database{Name: "shop"}).Do()), nil)
	wait(t, svc, p, must(svc.Users.Insert(p, id, &sqladmin.User{Name: "app", Password: "app-pw"}).Do()), nil)

	db := must(dbsql.Open("mysql", "app:app-pw@tcp("+addr+")/shop?timeout=15s"))
	defer db.Close()
	var who, name string
	if err := db.QueryRow("select substring_index(current_user(), '@', 1), database()").Scan(&who, &name); err != nil || who != "app" || name != "shop" {
		t.Fatalf("query: %q %q %v", who, name, err)
	}
	root := must(dbsql.Open("mysql", "root:root-pw@tcp("+addr+")/?timeout=15s"))
	defer root.Close()
	if err := root.Ping(); err != nil {
		t.Errorf("root: %v", err)
	}
	container := "pogi-cloudsql-" + id + "-demo-us-central1"
	wait(t, svc, p, must(svc.Instances.Delete(p, id).Do()), nil)
	gone(t, srv, container)
}

// TestCloneExportImport moves data: a clone has the source's databases and rows, an export
// lands in Cloud Storage and an import loads it into another instance.
func TestCloneExportImport(t *testing.T) {
	needDocker(t)
	srv, svc := start(t, "")
	p, a := "demo", name("src")
	i := create(t, svc, p, a, "POSTGRES_16", "root-pw")
	wait(t, svc, p, must(svc.Databases.Insert(p, a, &sqladmin.Database{Name: "shop"}).Do()), nil)
	conn := pgConnect(t, primary(t, i), "postgres", "root-pw", "shop")
	for _, q := range []string{"create table orders (id int primary key, item text)", "insert into orders values (1, 'book'), (2, 'pen')"} {
		must(conn.Exec(ctx, q))
	}
	conn.Close(ctx)

	rows := func(addr string) []string {
		c := pgConnect(t, addr, "postgres", "root-pw", "shop")
		defer c.Close(ctx)
		r := must(c.Query(ctx, "select item from orders order by id"))
		var out []string
		for r.Next() {
			var s string
			r.Scan(&s)
			out = append(out, s)
		}
		return out
	}

	// clone
	b := name("clone")
	wait(t, svc, p, must(svc.Instances.Clone(p, a, &sqladmin.InstancesCloneRequest{CloneContext: &sqladmin.CloneContext{DestinationInstanceName: b}}).Do()), nil)
	ci := must(svc.Instances.Get(p, b).Do())
	for end := time.Now().Add(2 * time.Minute); ci.State != "RUNNABLE"; ci = must(svc.Instances.Get(p, b).Do()) {
		if time.Now().After(end) {
			t.Fatalf("the clone is %s", ci.State)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := rows(primary(t, ci)); !slices.Equal(got, []string{"book", "pen"}) {
		t.Errorf("the clone has %v", got)
	}
	if dl := must(svc.Databases.List(p, b).Do()); !slices.ContainsFunc(dl.Items, func(d *sqladmin.Database) bool { return d.Name == "shop" }) {
		t.Errorf("the clone's databases: %+v", dl.Items)
	}
	if _, err := svc.Instances.Clone(p, a, &sqladmin.InstancesCloneRequest{CloneContext: &sqladmin.CloneContext{DestinationInstanceName: b}}).Do(); code(err) != 409 {
		t.Errorf("clone onto an existing instance: %v", err)
	}

	// export to Cloud Storage
	bucket := name("dumps")
	if r, _ := http.NewRequest("POST", "/storage/v1/b?project=demo", strings.NewReader(`{"name":"`+bucket+`"}`)); true {
		if code, body := srv.Hub.DoService("gcs", r); code != 200 {
			t.Fatalf("bucket: %d %s", code, body)
		}
	}
	uri := "gs://" + bucket + "/shop.sql"
	wait(t, svc, p, must(svc.Instances.Export(p, a, &sqladmin.InstancesExportRequest{ExportContext: &sqladmin.ExportContext{
		FileType: "SQL", Uri: uri, Databases: []string{"shop"}}}).Do()), nil)
	r, _ := http.NewRequest("GET", "/storage/v1/b/"+bucket+"/o/shop.sql?alt=media", nil)
	if code, body := srv.Hub.DoService("gcs", r); code != 200 || !strings.Contains(string(body), "CREATE TABLE public.orders") || !strings.Contains(string(body), "book") {
		t.Fatalf("the exported object: %d %.200s", code, body)
	}
	// a missing bucket fails the operation, not the request
	op := must(svc.Instances.Export(p, a, &sqladmin.InstancesExportRequest{ExportContext: &sqladmin.ExportContext{
		FileType: "SQL", Uri: "gs://nope-" + bucket + "/x.sql"}}).Do())
	if op.Error == nil {
		t.Errorf("exporting to a missing bucket: %+v", op)
	}

	// import into another instance
	c := name("dst")
	i = create(t, svc, p, c, "POSTGRES_16", "root-pw")
	wait(t, svc, p, must(svc.Databases.Insert(p, c, &sqladmin.Database{Name: "shop"}).Do()), nil)
	wait(t, svc, p, must(svc.Instances.Import(p, c, &sqladmin.InstancesImportRequest{ImportContext: &sqladmin.ImportContext{
		FileType: "SQL", Uri: uri, Database: "shop"}}).Do()), nil)
	if got := rows(primary(t, i)); !slices.Equal(got, []string{"book", "pen"}) {
		t.Errorf("the import has %v", got)
	}
	if op := must(svc.Instances.Import(p, c, &sqladmin.InstancesImportRequest{ImportContext: &sqladmin.ImportContext{
		FileType: "CSV", Uri: uri, Database: "shop"}}).Do()); op.Error == nil {
		t.Errorf("CSV imports are not supported, but: %+v", op)
	}
}

// A clone name with path pieces is refused before anything is written.
func TestCloneBadName(t *testing.T) {
	_, svc := start(t, "/nonexistent/docker.sock")
	for _, bad := range []string{"../x", "a/b", "UPPER"} {
		_, err := svc.Instances.Clone("demo", "src", &sqladmin.InstancesCloneRequest{CloneContext: &sqladmin.CloneContext{DestinationInstanceName: bad}}).Do()
		if code(err) != 400 {
			t.Errorf("clone to %q: %v", bad, err)
		}
	}
}
