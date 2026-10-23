package rds_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	doctypes "github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/smithy-go"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

func code(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return fmt.Sprint(err)
}

// TestOtherEnginesMetadata runs DocumentDB, Neptune and SQL Server without a
// container engine, through their own SDKs where they have one.
func TestOtherEnginesMetadata(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return rds.New(h) })
	ctx := context.Background()
	doc := docdb.NewFromConfig(awstest.Config(t, base))

	cl, err := doc.CreateDBCluster(ctx, &docdb.CreateDBClusterInput{DBClusterIdentifier: aws.String("Docs"), Engine: aws.String("docdb"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("secret123")})
	if err != nil {
		t.Fatal(err)
	}
	if c := cl.DBCluster; *c.Status != "available" || *c.Port != 27017 || *c.Endpoint != "localhost" || *c.ReaderEndpoint != "localhost" ||
		*c.EngineVersion != "5.0.0" || *c.DBClusterParameterGroup != "default.docdb5.0" || c.StorageType != nil {
		t.Fatalf("cluster %+v", c)
	}
	in, err := doc.CreateDBInstance(ctx, &docdb.CreateDBInstanceInput{DBInstanceIdentifier: aws.String("docs-1"), DBClusterIdentifier: aws.String("docs"),
		Engine: aws.String("docdb"), DBInstanceClass: aws.String("db.r6g.large")})
	if err != nil {
		t.Fatal(err)
	}
	if i := in.DBInstance; *i.DBInstanceStatus != "available" || *i.Endpoint.Port != 27017 || *i.DBClusterIdentifier != "docs" {
		t.Fatalf("instance %+v", i)
	}
	cls, err := doc.DescribeDBClusters(ctx, &docdb.DescribeDBClustersInput{Filters: []doctypes.Filter{{Name: aws.String("engine"), Values: []string{"docdb"}}}})
	if err != nil || len(cls.DBClusters) != 1 || len(cls.DBClusters[0].DBClusterMembers) != 1 || !*cls.DBClusters[0].DBClusterMembers[0].IsClusterWriter {
		t.Fatalf("describe %+v %v", cls, err)
	}
	vs, err := doc.DescribeDBEngineVersions(ctx, &docdb.DescribeDBEngineVersionsInput{Engine: aws.String("docdb"), DefaultOnly: aws.Bool(true)})
	if err != nil || len(vs.DBEngineVersions) != 1 || *vs.DBEngineVersions[0].EngineVersion != "5.0.0" || *vs.DBEngineVersions[0].DBParameterGroupFamily != "docdb5.0" {
		t.Fatalf("versions %+v %v", vs, err)
	}
	if g, err := doc.DescribeGlobalClusters(ctx, &docdb.DescribeGlobalClustersInput{}); err != nil || len(g.GlobalClusters) != 0 {
		t.Fatalf("global clusters %+v %v", g, err)
	}
	for want, err := range map[string]error{
		"DBClusterAlreadyExistsFault": second(doc.CreateDBCluster(ctx, &docdb.CreateDBClusterInput{DBClusterIdentifier: aws.String("docs"), Engine: aws.String("docdb"),
			MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("secret123")})),
		"InvalidParameterValue": second(doc.CreateDBCluster(ctx, &docdb.CreateDBClusterInput{DBClusterIdentifier: aws.String("weak"), Engine: aws.String("docdb"),
			MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("short")})),
		"InvalidDBClusterStateFault": second(doc.DeleteDBCluster(ctx, &docdb.DeleteDBClusterInput{DBClusterIdentifier: aws.String("docs"), SkipFinalSnapshot: aws.Bool(true)})),
		"GlobalClusterNotFoundFault": second(doc.DescribeGlobalClusters(ctx, &docdb.DescribeGlobalClustersInput{GlobalClusterIdentifier: aws.String("nope")})),
	} {
		if code(err) != want {
			t.Fatalf("want %s, got %v", want, err)
		}
	}
	fails(t, base, 400, "InvalidParameterCombination", "Action", "CreateDBInstance", "DBInstanceIdentifier", "lonely", "Engine", "docdb", "DBInstanceClass", "db.r6g.large")
	if _, err := doc.DeleteDBInstance(ctx, &docdb.DeleteDBInstanceInput{DBInstanceIdentifier: aws.String("docs-1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := doc.DeleteDBCluster(ctx, &docdb.DeleteDBClusterInput{DBClusterIdentifier: aws.String("docs"), SkipFinalSnapshot: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doc.DescribeDBClusters(ctx, &docdb.DescribeDBClustersInput{DBClusterIdentifier: aws.String("docs")}); code(err) != "DBClusterNotFoundFault" {
		t.Fatalf("deleted cluster: %v", err)
	}

	nep := neptune.NewFromConfig(awstest.Config(t, base))
	ncl, err := nep.CreateDBCluster(ctx, &neptune.CreateDBClusterInput{DBClusterIdentifier: aws.String("graph"), Engine: aws.String("neptune")})
	if err != nil {
		t.Fatal(err)
	}
	if c := ncl.DBCluster; *c.Status != "available" || *c.Port != 8182 || *c.EngineVersion != "1.4.5.0" || *c.DBClusterParameterGroup != "default.neptune1.4" {
		t.Fatalf("neptune cluster %+v", c)
	}
	ni, err := nep.CreateDBInstance(ctx, &neptune.CreateDBInstanceInput{DBInstanceIdentifier: aws.String("graph-1"), DBClusterIdentifier: aws.String("graph"),
		Engine: aws.String("neptune"), DBInstanceClass: aws.String("db.r6g.large")})
	if err != nil || *ni.DBInstance.Endpoint.Port != 8182 {
		t.Fatalf("neptune instance %+v %v", ni, err)
	}

	var r res
	ok(t, base, &r, "Action", "CreateDBInstance", "DBInstanceIdentifier", "mssql", "Engine", "sqlserver-ex", "DBInstanceClass", "db.t3.micro",
		"MasterUsername", "admin", "MasterUserPassword", "secret123")
	if i := r.R.Instance; i.DBInstanceStatus != "available" || i.Endpoint.Port != 1433 || i.EngineVersion != "16.00.4185.3.v1" ||
		!slices.Equal(i.ParameterGroups, []string{"default.sqlserver-ex-16.0"}) {
		t.Fatalf("sql server %+v", i)
	}
	fails(t, base, 400, "InvalidParameterValue", "Action", "CreateDBCluster", "DBClusterIdentifier", "mssql", "Engine", "sqlserver-ee",
		"MasterUsername", "admin", "MasterUserPassword", "secret123")
	ok(t, base, &r, "Action", "DescribeDBEngineVersions", "Engine", "sqlserver-se")
	if v := r.R.Versions; len(v) != 2 || v[0].DBParameterGroupFamily != "sqlserver-se-15.0" || v[1].DBParameterGroupFamily != "sqlserver-se-16.0" {
		t.Fatalf("sql server versions %+v", v)
	}
}

func second[T any](_ T, err error) error { return err }

// TestOtherEngineContainers checks the containers of the fake engine.
func TestOtherEngineContainers(t *testing.T) {
	f, _, base := setup(t, false)
	ok(t, base, nil, "Action", "CreateDBCluster", "DBClusterIdentifier", "docs", "Engine", "docdb", "MasterUsername", "admin", "MasterUserPassword", "secret123")
	ok(t, base, nil, "Action", "CreateDBCluster", "DBClusterIdentifier", "graph", "Engine", "neptune")
	create(t, base, "mssql", "Engine", "sqlserver-web")
	await(t, base, "mssql", "available")
	run := strings.Join(f.Called("create"), "\n") + "\n"
	for _, want := range []string{
		"-e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=secret123 mongo:7\n",
		":8182 tinkerpop/gremlin-server:3.7\n",
		":1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Pogi-secret123 mcr.microsoft.com/mssql/server:2022-latest\n",
	} {
		if !strings.Contains(run, want) {
			t.Fatalf("missing %q in\n%s", want, run)
		}
	}
	if g := f.Called("exec pogi-rds-mssql sh -c"); len(g) != 1 || !strings.Contains(g[0], " sh Pogi-secret123 IF SUSER_ID(N'admin') IS NULL CREATE LOGIN [admin] WITH PASSWORD = N'secret123'") {
		t.Fatalf("the master user gets a login: %q", g)
	}
	eventually(t, "docdb available", func() bool {
		var r res
		ok(t, base, &r, "Action", "DescribeDBClusters", "DBClusterIdentifier", "docs")
		return r.R.Clusters[0].Status == "available"
	})
	ok(t, base, nil, "Action", "ModifyDBCluster", "DBClusterIdentifier", "docs", "MasterUserPassword", "n3w'secret", "ApplyImmediately", "true")
	eventually(t, "changeUserPassword", func() bool {
		return slices.Contains(f.Called("exec pogi-rds-docs"), `exec pogi-rds-docs mongosh admin --quiet -u admin -p secret123 --eval db.changeUserPassword("admin","n3w'secret")`)
	})
	ok(t, base, nil, "Action", "ModifyDBInstance", "DBInstanceIdentifier", "mssql", "MasterUserPassword", "other-secret", "ApplyImmediately", "true")
	eventually(t, "ALTER LOGIN", func() bool {
		g := f.Called("exec pogi-rds-mssql sh -c")
		return len(g) == 2 && strings.Contains(g[1], " sh Pogi-secret123 ALTER LOGIN [admin] WITH PASSWORD = N'other-secret', CHECK_POLICY = OFF; ALTER LOGIN [sa] WITH PASSWORD = N'Pogi-other-secret'")
	})
}

// TestOtherEnginesInDocker runs MongoDB, Gremlin Server and SQL Server for
// real when a container engine answers.
func TestOtherEnginesInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	var svc *rds.Service
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { svc = rds.New(h); return svc })
	ctx := context.Background()
	id := func(kind string) string { return fmt.Sprintf("it-%s-%d", kind, os.Getpid()) }
	// why says what pogi and the engine know of a resource that failed: its note, and its container's state and
	// last log lines (the docdb cluster of a gate run on 2026-10-21 failed within 3 seconds, saying nothing)
	why := func(table, id string) string {
		b, _ := json.Marshal(svc.Inspect(table, id))
		name := "pogi-rds-" + id
		var lines []string
		if in, err := d.Inspect(ctx, name); err != nil {
			lines = append(lines, fmt.Sprintf("inspect %s: %v", name, err))
		} else {
			lines = append(lines, fmt.Sprintf("container %s: %s, exit=%d, out of memory=%v, mounts=%+v", name, in.State.Status, in.State.ExitCode, in.State.OOMKilled, in.Mounts))
		}
		d.Logs(ctx, name, false, func(l string) { lines = append(lines, l) })
		if len(lines) > 40 {
			lines = append(lines[:1], lines[len(lines)-39:]...)
		}
		return string(b) + "\n" + strings.Join(lines, "\n")
	}
	cluster := func(t *testing.T, id string) int {
		var r res
		eventually5m(t, id+" to start (the first run pulls the image)", func() bool {
			ok(t, base, &r, "Action", "DescribeDBClusters", "DBClusterIdentifier", id)
			if r.R.Clusters[0].Status == "failed" {
				t.Fatalf("container failed: %+v\n%s", r.R.Clusters[0], why("DB clusters", id))
			}
			return r.R.Clusters[0].Status == "available"
		})
		return r.R.Clusters[0].Port
	}

	t.Run("docdb", func(t *testing.T) {
		t.Parallel()
		ok(t, base, nil, "Action", "CreateDBCluster", "DBClusterIdentifier", id("docdb"), "Engine", "docdb", "MasterUsername", "admin", "MasterUserPassword", "secret123")
		cluster(t, id("docdb"))
		out, code, err := d.Exec(ctx, "pogi-rds-"+id("docdb"), "mongosh", "mongodb://admin:secret123@localhost:27017/shop?authSource=admin", "--quiet", "--eval",
			`db.items.insertOne({sku: "a1"}); print(db.items.findOne({sku: "a1"}).sku)`)
		if err != nil || code != 0 || strings.TrimSpace(string(out)) != "a1" {
			t.Fatalf("mongosh: %d %v %s", code, err, out)
		}
		ok(t, base, nil, "Action", "DeleteDBCluster", "DBClusterIdentifier", id("docdb"), "SkipFinalSnapshot", "true")
	})
	t.Run("neptune", func(t *testing.T) {
		t.Parallel()
		ok(t, base, nil, "Action", "CreateDBCluster", "DBClusterIdentifier", id("neptune"), "Engine", "neptune")
		port := cluster(t, id("neptune"))
		gremlin := func(q string) string {
			resp, err := http.Post("http://localhost:"+strconv.Itoa(port)+"/gremlin", "application/json", strings.NewReader(`{"gremlin":"`+q+`"}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				t.Fatalf("%s: %d %s", q, resp.StatusCode, b)
			}
			return string(b)
		}
		gremlin(`g.addV('x')`)
		if out := gremlin(`g.V().count()`); !strings.Contains(out, `{"@type":"g:Int64","@value":1}`) {
			t.Fatalf("count: %s", out)
		}
		ok(t, base, nil, "Action", "DeleteDBCluster", "DBClusterIdentifier", id("neptune"), "SkipFinalSnapshot", "true")
	})
	t.Run("sqlserver", func(t *testing.T) {
		t.Parallel()
		create(t, base, id("mssql"), "Engine", "sqlserver-ex")
		eventually5m(t, "sql server to start", func() bool {
			i := describe(t, base, id("mssql"))
			if i.DBInstanceStatus == "failed" {
				t.Fatalf("container failed: %+v\n%s", i, why("DB instances", id("mssql")))
			}
			return i.DBInstanceStatus == "available"
		})
		var out []byte
		eventually5m(t, "SELECT 1 as the master user", func() bool { // the login comes a moment after the server answers
			var code int
			var err error
			out, code, err = d.Exec(ctx, "pogi-rds-"+id("mssql"), "/opt/mssql-tools18/bin/sqlcmd", "-C", "-b", "-h", "-1", "-S", "localhost",
				"-U", "admin", "-P", "secret123", "-Q", "SET NOCOUNT ON; SELECT 1")
			return err == nil && code == 0
		})
		if strings.TrimSpace(string(out)) != "1" {
			t.Fatalf("SELECT 1 = %q", out)
		}
		ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", id("mssql"), "SkipFinalSnapshot", "true")
	})
}
