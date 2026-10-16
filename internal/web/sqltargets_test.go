package web_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

// TestSQLTargets checks, against a fake container engine, that the editor
// finds the container and the master credentials of an RDS MySQL instance, a
// Cloud SQL PostgreSQL instance and an Azure PostgreSQL server, and runs the
// right client in each. TestSQLEditor runs a statement for real.
func TestSQLTargets(t *testing.T) {
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
	srv := pogi.Start(t, pogi.Config{Services: []string{"rds", "sqladmin", "azdatabases"}})
	ctx := context.Background()

	if _, err := rds.NewFromConfig(awstest.Config(t, srv.URL)).CreateDBInstance(ctx, &rds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String("shop"), Engine: aws.String("mysql"),
		DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("rds-secret1"), DBName: aws.String("orders"), AllocatedStorage: aws.Int32(20)}); err != nil {
		t.Fatal(err)
	}
	send(t, srv, "POST", "/sql/v1beta4/projects/pogi/instances", `{"name":"ledger","databaseVersion":"POSTGRES_16","rootPassword":"gcp-pw"}`)
	rid, _ := arm.ParseID("/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/Microsoft.DBforPostgreSQL/flexibleServers/books")
	if _, err := arm.Lookup(srv.Hub, "Microsoft.DBforPostgreSQL/flexibleServers").Put(&core.Ctx{}, rid, map[string]any{"location": "westeurope",
		"properties": map[string]any{"administratorLogin": "pgadmin", "administratorLoginPassword": "az-pw", "version": "16"}}); err != nil {
		t.Fatal(err)
	}

	p := page(t, srv, "/_pogi/sql")
	has(t, p, `value="rds|shop"`, `value="cloudsql|pogi/ledger"`, `value="azure|books"`)

	run := func(target, db, sql string) string {
		t.Helper()
		var p string
		for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) { // the container is made in the background
			p = form(t, srv, "/_pogi/sql", url.Values{"target": {target}, "db": {db}, "sql": {sql}})
			if !strings.Contains(p, "no running container") {
				break
			}
		}
		return p
	}
	exec := func(container string) string {
		t.Helper()
		var last string
		for _, c := range eng.Called("exec " + container) { // the client's call, not the engine setup
			if strings.Contains(c, " psql ") || strings.Contains(c, "command -v mysql") {
				last = c
			}
		}
		if last == "" {
			t.Fatalf("no client was run in %s: %v", container, eng.Called("exec"))
		}
		return last
	}

	shows(t, run("rds|shop", "", "SELECT 1"), "OK, no rows returned") // the fake engine prints nothing
	if c := exec("pogi-rds-shop"); !strings.Contains(c, "MYSQL_PWD=rds-secret1") || !strings.Contains(c, " -u admin -D orders -e SELECT 1") || !strings.Contains(c, "mariadb") {
		t.Errorf("RDS MySQL: %s", c)
	}
	run("cloudsql|pogi/ledger", "", "SELECT 2")
	if c := exec("pogi-cloudsql-ledger"); !strings.Contains(c, "PGPASSWORD=gcp-pw psql") || !strings.Contains(c, " -U postgres -d postgres -c SELECT 2") {
		t.Errorf("Cloud SQL: %s", c)
	}
	run("azure|books", "app", "SELECT 3")
	if c := exec("pogi-azdatabases-pg-books"); !strings.Contains(c, "PGPASSWORD=az-pw psql") || !strings.Contains(c, " -U pgadmin -d app -c SELECT 3") {
		t.Errorf("Azure PostgreSQL: %s", c)
	}
	// a server the editor cannot speak to is refused
	shows(t, form(t, srv, "/_pogi/sql", url.Values{"target": {"cloudsql|pogi/none"}, "sql": {"SELECT 1"}}), `class="flash"`)
}
