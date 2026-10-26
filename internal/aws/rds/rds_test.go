package rds_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/aws/secretsmanager"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

// fake makes services created afterwards use a fake engine whose containers
// all publish a listener that answers like a database server.
func fake(t *testing.T, down bool) *dockertest.Engine {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { // greet, then read until the client hangs up
				defer c.Close()
				c.Write([]byte("N"))
				c.SetReadDeadline(time.Now().Add(time.Second))
				io.Copy(io.Discard, c)
			}()
		}
	}()
	return dockertest.Start(t, ln.Addr().(*net.TCPAddr).Port, down)
}

func setup(t *testing.T, down bool) (*dockertest.Engine, *rds.Service, string) {
	f := fake(t, down)
	var svc *rds.Service
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { svc = rds.New(h); return svc },
		func(h *core.Hub) core.Service { return secretsmanager.New(h) })
	return f, svc, base
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// call sends a Query request the way the SDKs and the CLI do.
func call(t *testing.T, base string, params ...string) (int, string) {
	t.Helper()
	form := url.Values{"Version": {"2014-10-31"}}
	for i := 0; i+1 < len(params); i += 2 {
		form.Set(params[i], params[i+1]) // later pairs override defaults
	}
	req, _ := http.NewRequest(http.MethodPost, base, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	awstest.Sign(t, req, "rds")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func ok(t *testing.T, base string, v any, params ...string) string {
	t.Helper()
	st, body := call(t, base, params...)
	if st != 200 {
		t.Fatalf("%s: %d %s", params[1], st, body)
	}
	if v != nil {
		reflect.ValueOf(v).Elem().SetZero() // xml.Unmarshal appends to slices
		if err := xml.Unmarshal([]byte(body), v); err != nil {
			t.Fatalf("%s: %v in %s", params[1], err, body)
		}
	}
	return body
}

func fails(t *testing.T, base string, status int, code string, params ...string) {
	t.Helper()
	st, body := call(t, base, params...)
	var e struct {
		Code string `xml:"Error>Code"`
	}
	xml.Unmarshal([]byte(body), &e)
	if st != status || e.Code != code {
		t.Fatalf("%s: want %d %s, got %d %s", params[1], status, code, st, body)
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

type tag struct{ Key, Value string }

type instance struct {
	DBInstanceIdentifier, DBInstanceStatus, Engine, EngineVersion, DBInstanceClass, MasterUsername, DBName string
	DBInstanceArn, DbiResourceId, DBClusterIdentifier                                                      string
	Endpoint                                                                                               struct {
		Address string
		Port    int
	}
	SecurityGroups  []string `xml:"VpcSecurityGroups>VpcSecurityGroupMembership>VpcSecurityGroupId"`
	ParameterGroups []string `xml:"DBParameterGroups>DBParameterGroup>DBParameterGroupName"`
	SubnetGroup     string   `xml:"DBSubnetGroup>DBSubnetGroupName"`
	Subnets         []string `xml:"DBSubnetGroup>Subnets>Subnet>SubnetIdentifier"`
	Tags            []tag    `xml:"TagList>Tag"`
	SecretArn       string   `xml:"MasterUserSecret>SecretArn"`
	LogExports      []string `xml:"EnabledCloudwatchLogsExports>member"`
}

type cluster struct {
	DBClusterIdentifier, Status, Endpoint, ReaderEndpoint, Engine, EngineVersion string
	Port                                                                         int
	Members                                                                      []struct {
		DBInstanceIdentifier string
		IsClusterWriter      bool
	} `xml:"DBClusterMembers>DBClusterMember"`
	Zones []string `xml:"AvailabilityZones>AvailabilityZone"`
}

// res is any RDS response: <OpResponse><OpResult>…</OpResult>…</OpResponse>.
type res struct {
	R struct {
		Instance     instance   `xml:"DBInstance"`
		Instances    []instance `xml:"DBInstances>DBInstance"`
		Cluster      cluster    `xml:"DBCluster"`
		Clusters     []cluster  `xml:"DBClusters>DBCluster"`
		SubnetGroups []struct {
			DBSubnetGroupName string
			Subnets           []string `xml:"Subnets>Subnet>SubnetIdentifier"`
		} `xml:"DBSubnetGroups>DBSubnetGroup"`
		ParamGroups        []string                                                              `xml:"DBParameterGroups>DBParameterGroup>DBParameterGroupName"`
		ClusterParamGroups []string                                                              `xml:"DBClusterParameterGroups>DBClusterParameterGroup>DBClusterParameterGroupName"`
		Parameters         []struct{ ParameterName, ParameterValue, Source string }              `xml:"Parameters>Parameter"`
		Snapshots          []struct{ DBSnapshotIdentifier, DBInstanceIdentifier, Status string } `xml:"DBSnapshots>DBSnapshot"`
		Snapshot           struct{ DBSnapshotIdentifier, Status string }                         `xml:"DBSnapshot"`
		Tags               []tag                                                                 `xml:"TagList>Tag"`
		Versions           []struct{ Engine, EngineVersion, DBParameterGroupFamily string }      `xml:"DBEngineVersions>DBEngineVersion"`
		Options            []struct {
			EngineVersion, DBInstanceClass string
			Zones                          []string `xml:"AvailabilityZones>AvailabilityZone>Name"`
		} `xml:"OrderableDBInstanceOptions>OrderableDBInstanceOption"`
		Marker string
	} `xml:",any"`
}

func describe(t *testing.T, base, id string) instance {
	t.Helper()
	var r res
	ok(t, base, &r, "Action", "DescribeDBInstances", "DBInstanceIdentifier", id)
	if len(r.R.Instances) != 1 {
		t.Fatalf("describe %s: %+v", id, r.R.Instances)
	}
	return r.R.Instances[0]
}

func await(t *testing.T, base, id, status string) instance {
	t.Helper()
	var i instance
	eventually(t, id+" "+status, func() bool { i = describe(t, base, id); return i.DBInstanceStatus == status })
	return i
}

func create(t *testing.T, base, id string, more ...string) instance {
	t.Helper()
	var r res
	ok(t, base, &r, append([]string{"Action", "CreateDBInstance", "DBInstanceIdentifier", id, "Engine", "postgres",
		"DBInstanceClass", "db.t3.micro", "MasterUsername", "admin", "MasterUserPassword", "secret123"}, more...)...)
	return r.R.Instance
}

func TestInstanceLifecycle(t *testing.T) {
	f, _, base := setup(t, false)
	want := freePort(t)
	i := create(t, base, "Shop", "EngineVersion", "16", "DBName", "shop", "Port", strconv.Itoa(want),
		"VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-1", "Tags.Tag.1.Key", "env", "Tags.Tag.1.Value", "dev")
	if i.DBInstanceIdentifier != "shop" || i.DBInstanceStatus != "creating" || i.EngineVersion != "16.8" || i.Endpoint.Port != 0 ||
		!slices.Equal(i.SecurityGroups, []string{"sg-1"}) || !slices.Equal(i.Tags, []tag{{"env", "dev"}}) ||
		!slices.Equal(i.ParameterGroups, []string{"default.postgres16"}) || i.SubnetGroup != "default" || len(i.Subnets) != 3 ||
		!strings.HasPrefix(i.DbiResourceId, "db-") || i.DBInstanceArn != "arn:aws:rds:us-east-1:000000000000:db:shop" {
		t.Fatalf("created %+v", i)
	}
	i = await(t, base, "shop", "available")
	if i.Endpoint.Address != "localhost" || i.Endpoint.Port != f.Port {
		t.Fatalf("endpoint %+v, want localhost:%d", i.Endpoint, f.Port)
	}
	run := f.Called("create")
	if len(run) != 1 || !strings.Contains(run[0], "--name pogi-rds-shop -p 127.0.0.1:"+strconv.Itoa(want)+":5432 ") ||
		!strings.HasSuffix(run[0], "-e POSTGRES_USER=admin -e POSTGRES_PASSWORD=secret123 -e POSTGRES_DB=shop postgres:16-alpine") {
		t.Fatalf("docker create = %q", run)
	}

	fails(t, base, 400, "DBInstanceAlreadyExists", "Action", "CreateDBInstance", "DBInstanceIdentifier", "shop", "Engine", "postgres",
		"DBInstanceClass", "db.t3.micro", "MasterUsername", "admin", "MasterUserPassword", "secret123")
	fails(t, base, 404, "DBInstanceNotFound", "Action", "DescribeDBInstances", "DBInstanceIdentifier", "nope")
	fails(t, base, 400, "InvalidParameterValue", "Action", "DescribeDBInstances", "Filters.Filter.1.Name", "colour", "Filters.Filter.1.Values.Value.1", "x")
	var r res
	ok(t, base, &r, "Action", "DescribeDBInstances", "Filters.Filter.1.Name", "dbi-resource-id", "Filters.Filter.1.Values.Value.1", i.DbiResourceId)
	if len(r.R.Instances) != 1 || describe(t, base, i.DBInstanceArn).DBInstanceIdentifier != "shop" {
		t.Fatalf("lookup by resource id or ARN failed: %+v", r.R.Instances)
	}

	ok(t, base, &r, "Action", "ModifyDBInstance", "DBInstanceIdentifier", "shop", "DBInstanceClass", "db.t3.small",
		"MasterUserPassword", "n3w'secret", "ApplyImmediately", "true")
	if r.R.Instance.DBInstanceClass != "db.t3.small" {
		t.Fatalf("modify: %+v", r.R.Instance)
	}
	eventually(t, "ALTER USER", func() bool {
		return slices.Contains(f.Called("exec"), `exec pogi-rds-shop psql -U admin -d postgres -c ALTER USER "admin" WITH PASSWORD 'n3w''secret'`)
	})

	ok(t, base, &r, "Action", "StopDBInstance", "DBInstanceIdentifier", "shop")
	await(t, base, "shop", "stopped")
	fails(t, base, 400, "InvalidDBInstanceState", "Action", "RebootDBInstance", "DBInstanceIdentifier", "shop")
	ok(t, base, &r, "Action", "StartDBInstance", "DBInstanceIdentifier", "shop")
	await(t, base, "shop", "available")
	ok(t, base, &r, "Action", "RebootDBInstance", "DBInstanceIdentifier", "shop")
	await(t, base, "shop", "available")
	for cmd, n := range map[string]int{"stop pogi-rds-shop": 1, "start pogi-rds-shop": 2, "restart pogi-rds-shop": 1} { // started on create too
		if len(f.Called(cmd)) != n {
			t.Fatalf("want %d %q in %q", n, cmd, f.Called(""))
		}
	}

	ok(t, base, nil, "Action", "CreateDBSnapshot", "DBInstanceIdentifier", "shop", "DBSnapshotIdentifier", "before")
	fails(t, base, 400, "DBSnapshotAlreadyExists", "Action", "CreateDBSnapshot", "DBInstanceIdentifier", "shop", "DBSnapshotIdentifier", "before")
	fails(t, base, 400, "InvalidParameterCombination", "Action", "DeleteDBInstance", "DBInstanceIdentifier", "shop")
	ok(t, base, &r, "Action", "DeleteDBInstance", "DBInstanceIdentifier", "shop", "FinalDBSnapshotIdentifier", "final")
	if r.R.Instance.DBInstanceStatus != "deleting" {
		t.Fatalf("delete: %+v", r.R.Instance)
	}
	fails(t, base, 404, "DBInstanceNotFound", "Action", "DescribeDBInstances", "DBInstanceIdentifier", "shop")
	eventually(t, "docker rm", func() bool { return len(f.Called("rm -f -v pogi-rds-shop")) == 1 }) // no leftover before run, so only now
	ok(t, base, &r, "Action", "DescribeDBSnapshots", "DBInstanceIdentifier", "shop")
	if len(r.R.Snapshots) != 2 || r.R.Snapshots[1].DBSnapshotIdentifier != "final" || r.R.Snapshots[1].Status != "available" {
		t.Fatalf("snapshots %+v", r.R.Snapshots)
	}
	ok(t, base, &r, "Action", "DeleteDBSnapshot", "DBSnapshotIdentifier", "before")
	fails(t, base, 404, "DBSnapshotNotFound", "Action", "DescribeDBSnapshots", "DBSnapshotIdentifier", "before")
}

func TestQueryShape(t *testing.T) {
	_, _, base := setup(t, true)
	create(t, base, "shape", "VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-1", "Tags.Tag.1.Key", "k", "Tags.Tag.1.Value", "v")
	body := ok(t, base, nil, "Action", "DescribeDBInstances")
	for _, want := range []string{
		`<DescribeDBInstancesResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/"><DescribeDBInstancesResult><DBInstances><DBInstance>`,
		`<VpcSecurityGroups><VpcSecurityGroupMembership><VpcSecurityGroupId>sg-1</VpcSecurityGroupId><Status>active</Status></VpcSecurityGroupMembership></VpcSecurityGroups>`,
		`<TagList><Tag><Key>k</Key><Value>v</Value></Tag></TagList>`,
		`<SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>`,
		`<DBParameterGroups><DBParameterGroup><DBParameterGroupName>default.postgres17</DBParameterGroupName>`,
		`</DBInstance></DBInstances></DescribeDBInstancesResult><ResponseMetadata><RequestId>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in\n%s", want, body)
		}
	}
	for _, internal := range []string{"<pogi", "Password", "secret123", "Docker is not available"} {
		if strings.Contains(body, internal) {
			t.Fatalf("pogi-only field %s leaked into\n%s", internal, body)
		}
	}
	fails(t, base, 400, "InvalidAction", "Action", "PurchaseReservedDBInstancesOffering")
}

func TestNoDockerAndManagedPassword(t *testing.T) {
	f, svc, base := setup(t, true)
	var r res
	ok(t, base, &r, "Action", "CreateDBInstance", "DBInstanceIdentifier", "legacy", "Engine", "mysql", "DBInstanceClass", "db.t3.micro",
		"MasterUsername", "admin", "ManageMasterUserPassword", "true", "Port", "3307")
	i := r.R.Instance
	if i.DBInstanceStatus != "available" || i.Endpoint.Port != 3307 || !strings.Contains(i.SecretArn, ":secret:rds!db-") {
		t.Fatalf("created %+v", i)
	}
	if len(f.Called("create")) != 0 {
		t.Fatalf("no container without Docker: %q", f.Called(""))
	}
	v := svc.Inspect("DB instances", "legacy").(*rds.DBInstance)
	b, _ := json.Marshal(v)
	if v.Pogi.Note != rds.NoDocker || len(v.Pogi.Password) != 28 || !strings.Contains(string(b), `"pogiNote":"Docker is not available`) {
		t.Fatalf("inspect %s", b)
	}
	secret := func() (int, string) {
		req, _ := http.NewRequest(http.MethodPost, base, strings.NewReader(`{"SecretId":"`+i.SecretArn+`"}`))
		req.Header.Set("X-Amz-Target", "secretsmanager.GetSecretValue")
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct{ SecretString string }
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out.SecretString
	}
	if st, s := secret(); st != 200 || s != `{"password":"`+v.Pogi.Password+`","username":"admin"}` {
		t.Fatalf("secret %d %s", st, s)
	}
	ok(t, base, &r, "Action", "DeleteDBInstance", "DBInstanceIdentifier", "legacy", "SkipFinalSnapshot", "true")
	if st, _ := secret(); st != 400 {
		t.Fatalf("the managed secret must go with the instance, got %d", st)
	}
}

func TestAuroraCluster(t *testing.T) {
	f, _, base := setup(t, false)
	var r res
	ok(t, base, &r, "Action", "CreateDBCluster", "DBClusterIdentifier", "Orders", "Engine", "aurora-postgresql",
		"MasterUsername", "admin", "MasterUserPassword", "secret123", "DatabaseName", "orders")
	if c := r.R.Cluster; c.DBClusterIdentifier != "orders" || c.Status != "creating" || c.Endpoint != "localhost" || c.Port != 5432 ||
		c.EngineVersion != "16.6" || len(c.Zones) != 3 {
		t.Fatalf("created %+v", c)
	}
	eventually(t, "cluster available", func() bool {
		ok(t, base, &r, "Action", "DescribeDBClusters", "DBClusterIdentifier", "orders")
		return r.R.Clusters[0].Status == "available" && r.R.Clusters[0].Port == f.Port
	})
	more := []string{"Engine", "aurora-postgresql", "DBClusterIdentifier", "orders", "DBInstanceClass", "db.r6g.large"}
	for _, id := range []string{"orders-1", "orders-2"} {
		ok(t, base, &r, append([]string{"Action", "CreateDBInstance", "DBInstanceIdentifier", id}, more...)...)
		if i := r.R.Instance; i.DBInstanceStatus != "available" || i.Endpoint.Port != f.Port || i.DBClusterIdentifier != "orders" {
			t.Fatalf("aurora instance %+v", i)
		}
	}
	ok(t, base, &r, "Action", "DescribeDBClusters")
	if m := r.R.Clusters[0].Members; len(m) != 2 || !m[0].IsClusterWriter || m[0].DBInstanceIdentifier != "orders-1" || m[1].IsClusterWriter {
		t.Fatalf("members %+v", m)
	}
	if run := f.Called("create"); len(run) != 1 || !strings.HasSuffix(run[0], "-e POSTGRES_DB=orders postgres:16-alpine") {
		t.Fatalf("one container per cluster, got %q", run)
	}
	fails(t, base, 400, "InvalidParameterCombination", "Action", "CreateDBInstance", "DBInstanceIdentifier", "lonely",
		"Engine", "aurora-mysql", "DBInstanceClass", "db.r6g.large")
	fails(t, base, 400, "InvalidParameterCombination", append([]string{"Action", "CreateDBInstance", "DBInstanceIdentifier", "wrong"},
		"Engine", "aurora-mysql", "DBClusterIdentifier", "orders", "DBInstanceClass", "db.r6g.large")...)
	fails(t, base, 404, "DBClusterNotFoundFault", append([]string{"Action", "CreateDBInstance", "DBInstanceIdentifier", "x"},
		"Engine", "aurora-postgresql", "DBClusterIdentifier", "nope", "DBInstanceClass", "db.r6g.large")...)
	fails(t, base, 400, "InvalidDBClusterStateFault", "Action", "DeleteDBCluster", "DBClusterIdentifier", "orders", "SkipFinalSnapshot", "true")

	ok(t, base, nil, "Action", "RebootDBInstance", "DBInstanceIdentifier", "orders-2")
	eventually(t, "cluster restart", func() bool { return len(f.Called("restart pogi-rds-orders")) == 1 })
	await(t, base, "orders-2", "available")
	ok(t, base, &r, "Action", "ModifyDBCluster", "DBClusterIdentifier", "orders", "MasterUserPassword", "other-secret", "DeletionProtection", "true")
	eventually(t, "cluster password", func() bool { return len(f.Called("exec pogi-rds-orders psql -U admin")) == 1 })

	for _, id := range []string{"orders-1", "orders-2"} {
		ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", id)
	}
	fails(t, base, 400, "InvalidParameterCombination", "Action", "DeleteDBCluster", "DBClusterIdentifier", "orders", "SkipFinalSnapshot", "true")
	ok(t, base, nil, "Action", "ModifyDBCluster", "DBClusterIdentifier", "orders", "DeletionProtection", "false")
	fails(t, base, 400, "InvalidParameterCombination", "Action", "DeleteDBCluster", "DBClusterIdentifier", "orders")
	ok(t, base, &r, "Action", "DeleteDBCluster", "DBClusterIdentifier", "orders", "SkipFinalSnapshot", "true")
	if r.R.Cluster.Status != "deleting" {
		t.Fatalf("delete %+v", r.R.Cluster)
	}
	fails(t, base, 404, "DBClusterNotFoundFault", "Action", "DescribeDBClusters", "DBClusterIdentifier", "orders")
	eventually(t, "cluster container removed", func() bool { return len(f.Called("rm -f -v pogi-rds-orders")) == 1 })
}

func TestEngineContainers(t *testing.T) {
	f, _, base := setup(t, false)
	create(t, base, "app", "Engine", "mysql", "DBName", "app")
	create(t, base, "maria", "Engine", "mariadb", "MasterUsername", "root", "EngineVersion", "10.11")
	await(t, base, "app", "available")
	await(t, base, "maria", "available")
	run := strings.Join(f.Called("create"), "\n")
	for _, want := range []string{
		"-e MYSQL_ROOT_PASSWORD=secret123 -e MYSQL_USER=admin -e MYSQL_PASSWORD=secret123 -e MYSQL_DATABASE=app mysql:8.4\n",
		"-e MARIADB_ROOT_PASSWORD=secret123 mariadb:10.11", // root is the image's own user
	} {
		if !strings.Contains(run+"\n", want) {
			t.Fatalf("missing %q in\n%s", want, run)
		}
	}
	if g := f.Called("exec pogi-rds-app"); len(g) != 1 || !strings.HasSuffix(g[0], "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION") {
		t.Fatalf("the master user gets every privilege: %q", g)
	}
	if len(f.Called("exec pogi-rds-maria")) != 0 {
		t.Fatal("root needs no grant")
	}
}

func TestValidation(t *testing.T) {
	_, _, base := setup(t, true)
	base4 := []string{"Action", "CreateDBInstance", "DBInstanceClass", "db.t3.micro", "MasterUsername", "admin"}
	for _, c := range []struct {
		code string
		args []string
	}{
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "1bad", "Engine", "postgres", "MasterUserPassword", "secret123"}},
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "bad--id", "Engine", "postgres", "MasterUserPassword", "secret123"}},
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "db", "Engine", "oracle-ee", "MasterUserPassword", "secret123"}},
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "db", "Engine", "postgres"}},
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "db", "Engine", "postgres", "MasterUserPassword", "short"}},
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "db", "Engine", "postgres", "MasterUserPassword", "with@sign"}},
		{"InvalidParameterValue", []string{"DBInstanceIdentifier", "db", "Engine", "postgres", "MasterUserPassword", "secret123", "MasterUsername", "x'; drop"}},
		{"InvalidParameterCombination", []string{"DBInstanceIdentifier", "db", "Engine", "postgres", "MasterUserPassword", "secret123", "ManageMasterUserPassword", "true"}},
	} {
		fails(t, base, 400, c.code, append(slices.Clone(base4), c.args...)...)
	}
	fails(t, base, 404, "DBSubnetGroupNotFoundFault", append(slices.Clone(base4), "DBInstanceIdentifier", "db", "Engine", "postgres",
		"MasterUserPassword", "secret123", "DBSubnetGroupName", "nope")...)
	fails(t, base, 404, "DBParameterGroupNotFound", append(slices.Clone(base4), "DBInstanceIdentifier", "db", "Engine", "postgres",
		"MasterUserPassword", "secret123", "DBParameterGroupName", "nope")...)
	// floci #1278: a parameter group of another engine family is refused
	ok(t, base, nil, "Action", "CreateDBParameterGroup", "DBParameterGroupName", "mysqlpg", "DBParameterGroupFamily", "mysql8.0", "Description", "d")
	fails(t, base, 400, "InvalidParameterCombination", append(slices.Clone(base4), "DBInstanceIdentifier", "db", "Engine", "postgres",
		"MasterUserPassword", "secret123", "DBParameterGroupName", "mysqlpg")...)
	// floci #3665: monitoring, Performance Insights, lifecycle and storage-limit settings are described
	create(t, base, "fidelity", "MonitoringInterval", "30", "MonitoringRoleArn", "arn:aws:iam::000000000000:role/mon", "EnablePerformanceInsights", "true",
		"PerformanceInsightsRetentionPeriod", "93", "EngineLifecycleSupport", "open-source-rds-extended-support-disabled", "MaxAllocatedStorage", "100")
	for _, want := range []string{"<MonitoringRoleArn>arn:aws:iam::000000000000:role/mon<", "<PerformanceInsightsRetentionPeriod>93<",
		"<EngineLifecycleSupport>open-source-rds-extended-support-disabled<", "<MaxAllocatedStorage>100<"} {
		if _, body := call(t, base, "Action", "DescribeDBInstances", "DBInstanceIdentifier", "fidelity"); !strings.Contains(body, want) {
			t.Fatalf("describe lacks %s: %s", want, body)
		}
	}
	create(t, base, "kept", "DeletionProtection", "true")
	fails(t, base, 400, "InvalidParameterCombination", "Action", "DeleteDBInstance", "DBInstanceIdentifier", "kept", "SkipFinalSnapshot", "true")
	var r res
	ok(t, base, &r, "Action", "ModifyDBInstance", "DBInstanceIdentifier", "kept", "DeletionProtection", "false", "NewDBInstanceIdentifier", "renamed",
		"CloudwatchLogsExportConfiguration.EnableLogTypes.member.1", "postgresql", "CloudwatchLogsExportConfiguration.EnableLogTypes.member.2", "upgrade")
	if i := r.R.Instance; i.DBInstanceIdentifier != "renamed" || !slices.Equal(i.LogExports, []string{"postgresql", "upgrade"}) {
		t.Fatalf("modify %+v", i)
	}
	ok(t, base, &r, "Action", "ModifyDBInstance", "DBInstanceIdentifier", "renamed", "CloudwatchLogsExportConfiguration.DisableLogTypes.member.1", "upgrade")
	if !slices.Equal(r.R.Instance.LogExports, []string{"postgresql"}) {
		t.Fatalf("log exports %v", r.R.Instance.LogExports)
	}
	ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", "renamed", "SkipFinalSnapshot", "true")
}

func TestGroupsTagsAndCatalog(t *testing.T) {
	_, _, base := setup(t, true)
	var r res
	subnets := []string{"Action", "CreateDBSubnetGroup", "DBSubnetGroupName", "App-Subnets", "DBSubnetGroupDescription", "app",
		"SubnetIds.SubnetIdentifier.1", "subnet-1", "SubnetIds.SubnetIdentifier.2", "subnet-2"}
	ok(t, base, nil, subnets...)
	fails(t, base, 400, "DBSubnetGroupAlreadyExists", subnets...)
	ok(t, base, &r, "Action", "DescribeDBSubnetGroups", "DBSubnetGroupName", "app-subnets")
	if g := r.R.SubnetGroups; len(g) != 1 || g[0].DBSubnetGroupName != "app-subnets" || !slices.Equal(g[0].Subnets, []string{"subnet-1", "subnet-2"}) {
		t.Fatalf("subnet groups %+v", g)
	}
	fails(t, base, 404, "DBSubnetGroupNotFoundFault", "Action", "DescribeDBSubnetGroups", "DBSubnetGroupName", "nope")

	params := []string{"Action", "CreateDBParameterGroup", "DBParameterGroupName", "app-pg", "DBParameterGroupFamily", "postgres17", "Description", "app"}
	ok(t, base, nil, params...)
	fails(t, base, 400, "DBParameterGroupAlreadyExists", params...)
	ok(t, base, nil, "Action", "CreateDBClusterParameterGroup", "DBClusterParameterGroupName", "app-pg", "DBParameterGroupFamily", "aurora-postgresql16", "Description", "app")
	ok(t, base, nil, "Action", "ModifyDBParameterGroup", "DBParameterGroupName", "app-pg",
		"Parameters.Parameter.1.ParameterName", "max_connections", "Parameters.Parameter.1.ParameterValue", "200", "Parameters.Parameter.1.ApplyMethod", "immediate")
	ok(t, base, &r, "Action", "DescribeDBParameters", "DBParameterGroupName", "app-pg", "Source", "user")
	if p := r.R.Parameters; len(p) != 1 || p[0].ParameterName != "max_connections" || p[0].ParameterValue != "200" || p[0].Source != "user" {
		t.Fatalf("parameters %+v", p)
	}
	ok(t, base, nil, "Action", "ResetDBParameterGroup", "DBParameterGroupName", "app-pg", "Parameters.Parameter.1.ParameterName", "max_connections")
	if ok(t, base, &r, "Action", "DescribeDBParameters", "DBParameterGroupName", "app-pg"); len(r.R.Parameters) != 0 {
		t.Fatalf("reset left %+v", r.R.Parameters)
	}
	ok(t, base, &r, "Action", "DescribeDBParameterGroups")
	if !slices.Equal(r.R.ParamGroups, []string{"app-pg"}) {
		t.Fatalf("parameter groups %v", r.R.ParamGroups)
	}
	ok(t, base, &r, "Action", "DescribeDBClusterParameterGroups", "DBClusterParameterGroupName", "app-pg")
	if !slices.Equal(r.R.ClusterParamGroups, []string{"app-pg"}) {
		t.Fatalf("cluster parameter groups %v", r.R.ClusterParamGroups)
	}
	ok(t, base, &r, "Action", "DescribeDBParameterGroups", "DBParameterGroupName", "default.mysql8.4")
	fails(t, base, 400, "InvalidDBParameterGroupState", "Action", "DeleteDBParameterGroup", "DBParameterGroupName", "default.mysql8.4")

	i := create(t, base, "tagged", "DBSubnetGroupName", "app-subnets", "DBParameterGroupName", "app-pg")
	if i.SubnetGroup != "app-subnets" || !slices.Equal(i.ParameterGroups, []string{"app-pg"}) {
		t.Fatalf("instance groups %+v", i)
	}
	fails(t, base, 400, "InvalidDBSubnetGroupStateFault", "Action", "DeleteDBSubnetGroup", "DBSubnetGroupName", "app-subnets")
	fails(t, base, 400, "InvalidDBParameterGroupState", "Action", "DeleteDBParameterGroup", "DBParameterGroupName", "app-pg")

	ok(t, base, nil, "Action", "AddTagsToResource", "ResourceName", i.DBInstanceArn, "Tags.Tag.1.Key", "team", "Tags.Tag.1.Value", "core",
		"Tags.Tag.2.Key", "env", "Tags.Tag.2.Value", "dev")
	ok(t, base, nil, "Action", "RemoveTagsFromResource", "ResourceName", i.DBInstanceArn, "TagKeys.member.1", "env")
	ok(t, base, &r, "Action", "ListTagsForResource", "ResourceName", i.DBInstanceArn)
	if !slices.Equal(r.R.Tags, []tag{{"team", "core"}}) {
		t.Fatalf("tags %+v", r.R.Tags)
	}
	ok(t, base, nil, "Action", "AddTagsToResource", "ResourceName", "arn:aws:rds:us-east-1:000000000000:subgrp:app-subnets", "Tags.Tag.1.Key", "a", "Tags.Tag.1.Value", "b")
	fails(t, base, 404, "DBInstanceNotFound", "Action", "ListTagsForResource", "ResourceName", "arn:aws:rds:us-east-1:000000000000:db:nope")
	fails(t, base, 400, "InvalidParameterValue", "Action", "ListTagsForResource", "ResourceName", "not-an-arn")

	ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", "tagged", "SkipFinalSnapshot", "true")
	ok(t, base, nil, "Action", "DeleteDBSubnetGroup", "DBSubnetGroupName", "app-subnets")
	ok(t, base, nil, "Action", "DeleteDBParameterGroup", "DBParameterGroupName", "app-pg")
	fails(t, base, 404, "DBParameterGroupNotFound", "Action", "DeleteDBParameterGroup", "DBParameterGroupName", "app-pg")
	ok(t, base, nil, "Action", "DeleteDBClusterParameterGroup", "DBClusterParameterGroupName", "app-pg")

	ok(t, base, &r, "Action", "DescribeDBEngineVersions", "Engine", "postgres", "DefaultOnly", "true")
	if v := r.R.Versions; len(v) != 1 || v[0].EngineVersion != "17.4" || v[0].DBParameterGroupFamily != "postgres17" {
		t.Fatalf("versions %+v", v)
	}
	ok(t, base, &r, "Action", "DescribeDBEngineVersions", "Engine", "aurora-mysql")
	if v := r.R.Versions; len(v) != 2 || v[0].DBParameterGroupFamily != "aurora-mysql8.0" {
		t.Fatalf("versions %+v", v)
	}
	ok(t, base, &r, "Action", "DescribeOrderableDBInstanceOptions", "Engine", "mysql", "DBInstanceClass", "db.t3.micro")
	if o := r.R.Options; len(o) != 2 || o[0].DBInstanceClass != "db.t3.micro" || len(o[0].Zones) != 3 {
		t.Fatalf("options %+v", o)
	}

	for _, n := range []string{"c", "a", "b"} {
		ok(t, base, nil, "Action", "CreateDBSubnetGroup", "DBSubnetGroupName", n, "DBSubnetGroupDescription", n, "SubnetIds.SubnetIdentifier.1", "s")
	}
	ok(t, base, &r, "Action", "DescribeDBSubnetGroups", "MaxRecords", "2")
	marker := r.R.Marker
	if len(r.R.SubnetGroups) != 2 || r.R.SubnetGroups[0].DBSubnetGroupName != "a" || marker == "" {
		t.Fatalf("page 1 %+v %q", r.R.SubnetGroups, marker)
	}
	ok(t, base, &r, "Action", "DescribeDBSubnetGroups", "MaxRecords", "2", "Marker", marker)
	if len(r.R.SubnetGroups) != 1 || r.R.SubnetGroups[0].DBSubnetGroupName != "c" || r.R.Marker != "" {
		t.Fatalf("page 2 %+v", r.R.SubnetGroups)
	}
}

// TestPersist checks that --persist keeps containers on shutdown and
// reconnects by name on load, and that without it they go.
func TestPersist(t *testing.T) {
	f := fake(t, false)
	h := core.NewHub(core.Config{Persist: true, DataDir: t.TempDir()}, nil)
	svc := rds.New(h)
	h.Register(svc)
	base := core.Listen(t, h)
	create(t, base, "keep")
	await(t, base, "keep", "available")
	state, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	svc.Close()
	if n := len(f.Called("rm")); n != 0 {
		t.Fatalf("--persist must keep containers: %q", f.Called("rm"))
	}

	again := rds.New(core.NewHub(core.Config{Persist: true}, nil))
	if err := again.Load(state); err != nil {
		t.Fatal(err)
	}
	eventually(t, "reconnect", func() bool {
		v, _ := again.Inspect("DB instances", "keep").(*rds.DBInstance)
		return v != nil && v.DBInstanceStatus == "available" && v.Endpoint.Port == f.Port && len(f.Called("start pogi-rds-keep")) == 2
	})
	again.Close()

	gone := rds.New(core.NewHub(core.Config{}, nil))
	gone.Load(state)
	gone.Close()
	if rm := f.Called("rm -f -v pogi-rds-keep"); len(rm) != 1 {
		t.Fatalf("without --persist Close removes the containers: %q", rm)
	}
}

// TestPostgresInDocker runs a real PostgreSQL when a container engine answers.
func TestPostgresInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return rds.New(h) })
	id := fmt.Sprintf("it-%d", os.Getpid())
	create(t, base, id, "EngineVersion", "16")
	var i instance
	eventually5m(t, "postgres to start (the first run pulls the image)", func() bool {
		if i = describe(t, base, id); i.DBInstanceStatus == "failed" {
			t.Fatalf("container failed: %+v", i)
		}
		return i.DBInstanceStatus == "available"
	})
	ctx := context.Background()
	name := "pogi-rds-" + id
	info, err := d.Inspect(ctx, name)
	if err != nil || info.HostPort(5432) != i.Endpoint.Port || info.Config.Labels[docker.LabelService] != "rds" ||
		info.Config.Labels[docker.LabelResource] != id || info.NetworkSettings.Networks["pogi"].IPAddress == "" {
		t.Fatalf("inspect %+v, %v (endpoint %+v)", info, err, i.Endpoint)
	}
	ok(t, base, nil, "Action", "ModifyDBInstance", "DBInstanceIdentifier", id, "MasterUserPassword", "changed-pw", "ApplyImmediately", "true")
	eventually5m(t, "the new password", func() bool {
		// over the pogi network, not loopback (which the image trusts), so the password counts
		ip := info.NetworkSettings.Networks["pogi"].IPAddress
		out, code, err := d.Exec(ctx, name, "env", "PGPASSWORD=changed-pw", "psql", "-h", ip, "-U", "admin", "-d", "postgres", "-tAc", "select 41+1")
		return err == nil && code == 0 && strings.TrimSpace(string(out)) == "42"
	})
	ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", id, "SkipFinalSnapshot", "true")
	eventually5m(t, "container removal", func() bool {
		list, err := d.List(ctx, docker.LabelResource+"="+id, docker.LabelService+"=rds")
		return err == nil && len(list) == 0
	})
}

func eventually5m(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Minute); !cond(); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
