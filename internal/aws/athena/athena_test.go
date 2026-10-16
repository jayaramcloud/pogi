package athena_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	athenasvc "github.com/nrndev/pogi/internal/aws/athena"
	gluesvc "github.com/nrndev/pogi/internal/aws/glue"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// isErr asserts err is the API error E and returns it.
func isErr[E error](t *testing.T, err error) E {
	t.Helper()
	var e E
	if !errors.As(err, &e) {
		t.Fatalf("got %v, want %T", err, e)
	}
	return e
}

type env struct {
	a  *athena.Client
	g  *glue.Client
	s3 *s3.Client
}

// setup serves Athena with Glue and S3, and the buckets lake and results;
// without engine, Docker is unreachable.
func setup(t *testing.T, engine bool) env {
	if !engine {
		t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	}
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return gluesvc.New(h) },
		func(h *core.Hub) core.Service { return athenasvc.New(h) })
	cfg := awstest.Config(t, url)
	e := env{athena.NewFromConfig(cfg), glue.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })}
	for _, b := range []string{"lake", "results"} {
		must(e.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	return e
}

func (e env) put(key, body string) {
	must(e.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("lake"), Key: aws.String(key), Body: strings.NewReader(body)}))
}

// start starts a query in database db, with results under s3://results/q/.
func (e env) start(db, sql string, params ...string) *string {
	in := &athena.StartQueryExecutionInput{QueryString: aws.String(sql), ExecutionParameters: params,
		ResultConfiguration: &types.ResultConfiguration{OutputLocation: aws.String("s3://results/q/")}}
	if db != "" {
		in.QueryExecutionContext = &types.QueryExecutionContext{Database: aws.String(db)}
	}
	return must(e.a.StartQueryExecution(ctx, in)).QueryExecutionId
}

// wait waits until a query finishes.
func (e env) wait(t *testing.T, id *string) *types.QueryExecution {
	t.Helper()
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(50 * time.Millisecond) {
		q := must(e.a.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: id})).QueryExecution
		if s := q.Status.State; s != types.QueryExecutionStateQueued && s != types.QueryExecutionStateRunning {
			return q
		}
		if time.Now().After(end) {
			t.Fatalf("query %s did not finish", *q.Query)
		}
	}
}

// ok runs a query that must succeed and returns its rows, NULL as "NULL".
func (e env) ok(t *testing.T, db, sql string, params ...string) [][]string {
	t.Helper()
	q := e.wait(t, e.start(db, sql, params...))
	if q.Status.State != types.QueryExecutionStateSucceeded {
		t.Fatalf("%s: %s: %s", sql, q.Status.State, aws.ToString(q.Status.StateChangeReason))
	}
	return e.rows(q.QueryExecutionId, 0)
}

// failed runs a query that must fail and returns why.
func (e env) failed(t *testing.T, db, sql string) string {
	t.Helper()
	q := e.wait(t, e.start(db, sql))
	if q.Status.State != types.QueryExecutionStateFailed || q.Status.AthenaError == nil {
		t.Fatalf("%s: %s, want FAILED", sql, q.Status.State)
	}
	return aws.ToString(q.Status.StateChangeReason)
}

// rows reads every page of a query's results.
func (e env) rows(id *string, size int32) (out [][]string) {
	in := &athena.GetQueryResultsInput{QueryExecutionId: id}
	if size > 0 {
		in.MaxResults = &size
	}
	for p := athena.NewGetQueryResultsPaginator(e.a, in); p.HasMorePages(); {
		for _, r := range must(p.NextPage(ctx)).ResultSet.Rows {
			var row []string
			for _, d := range r.Data {
				row = append(row, aws.ToString(d.VarCharValue))
				if d.VarCharValue == nil {
					row[len(row)-1] = "NULL"
				}
			}
			out = append(out, row)
		}
	}
	return out
}

func (e env) object(t *testing.T, bucket, key string) string {
	t.Helper()
	o, err := e.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("s3://%s/%s: %v", bucket, key, err)
	}
	defer o.Body.Close()
	return string(must(io.ReadAll(o.Body)))
}

func TestWorkGroupsAndCatalogs(t *testing.T) {
	a := setup(t, false).a
	if wg := must(a.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String("primary")})).WorkGroup; wg.State != types.WorkGroupStateEnabled {
		t.Fatalf("primary %+v", wg)
	}
	must(a.CreateWorkGroup(ctx, &athena.CreateWorkGroupInput{Name: aws.String("etl"), Description: aws.String("nightly"),
		Configuration: &types.WorkGroupConfiguration{ResultConfiguration: &types.ResultConfiguration{OutputLocation: aws.String("s3://results/etl/")}},
		Tags:          []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}}}))
	_, err := a.CreateWorkGroup(ctx, &athena.CreateWorkGroupInput{Name: aws.String("etl")})
	isErr[*types.InvalidRequestException](t, err)
	wg := must(a.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String("etl")})).WorkGroup
	cfg := wg.Configuration
	if *wg.Description != "nightly" || !*cfg.EnforceWorkGroupConfiguration || !*cfg.PublishCloudWatchMetricsEnabled ||
		*cfg.ResultConfiguration.OutputLocation != "s3://results/etl/" || *cfg.EngineVersion.SelectedEngineVersion != "AUTO" ||
		*cfg.EngineVersion.EffectiveEngineVersion != "Athena engine version 3" {
		t.Fatalf("workgroup %+v %+v", wg, cfg)
	}
	_, err = a.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String("nope")})
	if e := isErr[*types.InvalidRequestException](t, err); aws.ToString(e.AthenaErrorCode) != "INVALID_INPUT" || !strings.Contains(e.ErrorMessage(), "is not found") {
		t.Fatalf("missing workgroup: %v (%v)", err, aws.ToString(e.AthenaErrorCode))
	}
	var names []string
	for _, w := range must(a.ListWorkGroups(ctx, &athena.ListWorkGroupsInput{})).WorkGroups {
		names = append(names, *w.Name)
	}
	if !slices.Equal(names, []string{"etl", "primary"}) {
		t.Fatalf("workgroups %v", names)
	}
	must(a.UpdateWorkGroup(ctx, &athena.UpdateWorkGroupInput{WorkGroup: aws.String("etl"), Description: aws.String("hourly"),
		ConfigurationUpdates: &types.WorkGroupConfigurationUpdates{ResultConfigurationUpdates: &types.ResultConfigurationUpdates{RemoveOutputLocation: aws.Bool(true)}}}))
	wg = must(a.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String("etl")})).WorkGroup
	if *wg.Description != "hourly" || wg.Configuration.ResultConfiguration.OutputLocation != nil || !*wg.Configuration.EnforceWorkGroupConfiguration {
		t.Fatalf("updated workgroup %+v", wg.Configuration.ResultConfiguration)
	}

	arn := "arn:aws:athena:us-east-1::workgroup/etl" // as Terraform builds it without an account id
	must(a.TagResource(ctx, &athena.TagResourceInput{ResourceARN: &arn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	tags := must(a.ListTagsForResource(ctx, &athena.ListTagsForResourceInput{ResourceARN: aws.String("arn:aws:athena:us-east-1:000000000000:workgroup/etl")})).Tags
	if len(tags) != 2 || *tags[0].Key != "env" || *tags[1].Value != "data" {
		t.Fatalf("tags %+v", tags)
	}
	_, err = a.ListTagsForResource(ctx, &athena.ListTagsForResourceInput{ResourceARN: aws.String("arn:aws:athena:us-east-1:000000000000:workgroup/nope")})
	isErr[*types.ResourceNotFoundException](t, err)

	nq := must(a.CreateNamedQuery(ctx, &athena.CreateNamedQueryInput{Name: aws.String("top"), Database: aws.String("shop"),
		QueryString: aws.String("SELECT 1"), WorkGroup: aws.String("etl")})).NamedQueryId
	if got := must(a.GetNamedQuery(ctx, &athena.GetNamedQueryInput{NamedQueryId: nq})).NamedQuery; *got.Name != "top" || *got.WorkGroup != "etl" || *got.QueryString != "SELECT 1" {
		t.Fatalf("named query %+v", got)
	}
	if ids := must(a.ListNamedQueries(ctx, &athena.ListNamedQueriesInput{WorkGroup: aws.String("etl")})).NamedQueryIds; !slices.Equal(ids, []string{*nq}) {
		t.Fatalf("named queries %v", ids)
	}
	if ids := must(a.ListNamedQueries(ctx, &athena.ListNamedQueriesInput{})).NamedQueryIds; len(ids) != 0 {
		t.Fatalf("primary's named queries %v", ids)
	}
	missing := "11111111-2222-3333-4444-555555555555"
	b := must(a.BatchGetNamedQuery(ctx, &athena.BatchGetNamedQueryInput{NamedQueryIds: []string{*nq, missing}}))
	if len(b.NamedQueries) != 1 || len(b.UnprocessedNamedQueryIds) != 1 || *b.UnprocessedNamedQueryIds[0].NamedQueryId != missing {
		t.Fatalf("batch %+v", b)
	}
	_, err = a.GetNamedQuery(ctx, &athena.GetNamedQueryInput{NamedQueryId: &missing})
	if e := isErr[*types.InvalidRequestException](t, err); !strings.Contains(e.ErrorMessage(), "does not exist") {
		t.Fatalf("missing named query: %v", err)
	}
	_, err = a.DeleteWorkGroup(ctx, &athena.DeleteWorkGroupInput{WorkGroup: aws.String("etl")})
	isErr[*types.InvalidRequestException](t, err) // not empty
	must(a.DeleteWorkGroup(ctx, &athena.DeleteWorkGroupInput{WorkGroup: aws.String("etl"), RecursiveDeleteOption: aws.Bool(true)}))
	_, err = a.GetNamedQuery(ctx, &athena.GetNamedQueryInput{NamedQueryId: nq})
	isErr[*types.InvalidRequestException](t, err)

	must(a.CreatePreparedStatement(ctx, &athena.CreatePreparedStatementInput{StatementName: aws.String("by_item"), WorkGroup: aws.String("primary"),
		QueryStatement: aws.String("SELECT * FROM sales WHERE item = ?")}))
	ps := must(a.GetPreparedStatement(ctx, &athena.GetPreparedStatementInput{StatementName: aws.String("by_item"), WorkGroup: aws.String("primary")})).PreparedStatement
	if *ps.QueryStatement != "SELECT * FROM sales WHERE item = ?" || *ps.WorkGroupName != "primary" || ps.LastModifiedTime == nil {
		t.Fatalf("prepared statement %+v", ps)
	}
	if l := must(a.ListPreparedStatements(ctx, &athena.ListPreparedStatementsInput{WorkGroup: aws.String("primary")})).PreparedStatements; len(l) != 1 {
		t.Fatalf("prepared statements %+v", l)
	}
	must(a.DeletePreparedStatement(ctx, &athena.DeletePreparedStatementInput{StatementName: aws.String("by_item"), WorkGroup: aws.String("primary")}))
	_, err = a.GetPreparedStatement(ctx, &athena.GetPreparedStatementInput{StatementName: aws.String("by_item"), WorkGroup: aws.String("primary")})
	isErr[*types.ResourceNotFoundException](t, err)

	dcs := must(a.ListDataCatalogs(ctx, &athena.ListDataCatalogsInput{})).DataCatalogsSummary
	if len(dcs) != 1 || *dcs[0].CatalogName != "AwsDataCatalog" || dcs[0].Type != types.DataCatalogTypeGlue {
		t.Fatalf("catalogs %+v", dcs)
	}
	must(a.CreateDataCatalog(ctx, &athena.CreateDataCatalogInput{Name: aws.String("hms"), Type: types.DataCatalogTypeHive,
		Parameters: map[string]string{"metadata-function": "arn:aws:lambda:us-east-1:000000000000:function:hms"}}))
	if dc := must(a.GetDataCatalog(ctx, &athena.GetDataCatalogInput{Name: aws.String("hms")})).DataCatalog; dc.Type != types.DataCatalogTypeHive {
		t.Fatalf("catalog %+v", dc)
	}
	must(a.DeleteDataCatalog(ctx, &athena.DeleteDataCatalogInput{Name: aws.String("hms")}))
	_, err = a.GetDataCatalog(ctx, &athena.GetDataCatalogInput{Name: aws.String("hms")})
	isErr[*types.InvalidRequestException](t, err)
}

const salesDDL = `CREATE EXTERNAL TABLE IF NOT EXISTS shop.sales (
  id bigint,
  item string COMMENT 'what was sold',
  price double)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://lake/sales/'
TBLPROPERTIES ('skip.header.line.count'='1')`

// TestDDL needs no Docker: DDL and metadata go to Glue.
func TestDDL(t *testing.T) {
	e := setup(t, false)
	e.put("sales/dt=2026-01-01/part-0.csv", "id,item,price\n1,apple,1.5\n")
	e.put("sales/dt=2026-01-02/part-0.csv", "id,item,price\n2,pear,2\n")
	e.ok(t, "", "CREATE DATABASE IF NOT EXISTS shop COMMENT 'the shop'")
	if r := e.failed(t, "", "CREATE DATABASE shop"); !strings.Contains(r, "already exists") {
		t.Fatalf("duplicate database: %s", r)
	}
	q := e.wait(t, e.start("", salesDDL))
	if q.Status.State != types.QueryExecutionStateSucceeded || q.StatementType != types.StatementTypeDdl || q.Statistics == nil ||
		!strings.HasSuffix(*q.ResultConfiguration.OutputLocation, *q.QueryExecutionId+".txt") {
		t.Fatalf("create table %+v %+v", q.Status, q.ResultConfiguration)
	}
	tb := must(e.g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("shop"), Name: aws.String("sales")})).Table
	sd := tb.StorageDescriptor
	if len(sd.Columns) != 3 || *sd.Columns[1].Comment != "what was sold" || *sd.Columns[2].Type != "double" || *tb.PartitionKeys[0].Name != "dt" ||
		*sd.Location != "s3://lake/sales/" || sd.SerdeInfo.Parameters["field.delim"] != "," || tb.Parameters["skip.header.line.count"] != "1" ||
		*sd.InputFormat != "org.apache.hadoop.mapred.TextInputFormat" || *tb.TableType != "EXTERNAL_TABLE" {
		t.Fatalf("table %+v", tb)
	}
	if r := e.failed(t, "shop", "CREATE EXTERNAL TABLE sales (id int) LOCATION 's3://lake/x/'"); !strings.Contains(r, "already exists") {
		t.Fatalf("duplicate table: %s", r)
	}
	if r := e.failed(t, "", "CREATE EXTERNAL TABLE nope.t (id int) LOCATION 's3://lake/x/'"); !strings.Contains(r, "Database does not exist: nope") {
		t.Fatalf("table in a missing database: %s", r)
	}
	if r := e.failed(t, "", "CREATE EXTERNAL TABL shop.t (id int)"); !strings.Contains(r, "line 1:") {
		t.Fatalf("syntax error: %s", r)
	}

	e.ok(t, "shop", "MSCK REPAIR TABLE sales")
	e.ok(t, "shop", `ALTER TABLE sales ADD IF NOT EXISTS PARTITION (dt='2026-01-01') PARTITION (dt = '2026-01-03') LOCATION 's3://lake/extra/'`)
	parts := must(e.g.GetPartitions(ctx, &glue.GetPartitionsInput{DatabaseName: aws.String("shop"), TableName: aws.String("sales")})).Partitions
	if len(parts) != 3 || *parts[0].StorageDescriptor.Location != "s3://lake/sales/dt=2026-01-01/" || *parts[2].StorageDescriptor.Location != "s3://lake/extra/" ||
		parts[1].StorageDescriptor.SerdeInfo.Parameters["field.delim"] != "," {
		t.Fatalf("partitions %+v", parts)
	}
	if got := e.ok(t, "shop", "SHOW PARTITIONS sales"); !slices.Equal(flat(got), []string{"dt=2026-01-01", "dt=2026-01-02", "dt=2026-01-03"}) {
		t.Fatalf("partitions %v", got)
	}
	e.ok(t, "shop", "ALTER TABLE sales DROP PARTITION (dt='2026-01-03')")
	id := e.start("", "SHOW TABLES IN shop")
	e.wait(t, id)
	if got := e.rows(id, 0); !slices.Equal(flat(got), []string{"sales"}) {
		t.Fatalf("show tables %v", got)
	}
	if got := e.object(t, "results", "q/"+*id+".txt"); got != "sales\n" {
		t.Fatalf("show tables output %q", got)
	}
	if got := e.ok(t, "", "SHOW DATABASES LIKE 'sh*'"); !slices.Equal(flat(got), []string{"shop"}) {
		t.Fatalf("show databases %v", got)
	}
	if got := e.ok(t, "shop", "SHOW COLUMNS IN sales"); !slices.Equal(flat(got), []string{"id", "item", "price", "dt"}) {
		t.Fatalf("show columns %v", got)
	}
	if got := e.ok(t, "shop", "DESCRIBE sales"); len(got) != 9 || !strings.HasPrefix(got[1][0], "item ") || got[5][0] != "# Partition Information\t \t " {
		t.Fatalf("describe %q", got)
	}

	db := must(e.a.GetDatabase(ctx, &athena.GetDatabaseInput{CatalogName: aws.String("AwsDataCatalog"), DatabaseName: aws.String("shop")})).Database
	if *db.Name != "shop" || *db.Description != "the shop" {
		t.Fatalf("database %+v", db)
	}
	if dbs := must(e.a.ListDatabases(ctx, &athena.ListDatabasesInput{CatalogName: aws.String("AwsDataCatalog")})).DatabaseList; len(dbs) != 1 {
		t.Fatalf("databases %+v", dbs)
	}
	tm := must(e.a.GetTableMetadata(ctx, &athena.GetTableMetadataInput{CatalogName: aws.String("AwsDataCatalog"), DatabaseName: aws.String("shop"),
		TableName: aws.String("sales")})).TableMetadata
	if len(tm.Columns) != 3 || *tm.PartitionKeys[0].Name != "dt" || tm.Parameters["location"] != "s3://lake/sales/" ||
		tm.Parameters["serde.param.field.delim"] != "," || *tm.TableType != "EXTERNAL_TABLE" {
		t.Fatalf("table metadata %+v", tm)
	}
	if l := must(e.a.ListTableMetadata(ctx, &athena.ListTableMetadataInput{CatalogName: aws.String("AwsDataCatalog"), DatabaseName: aws.String("shop")})).TableMetadataList; len(l) != 1 {
		t.Fatalf("table metadata list %+v", l)
	}
	_, err := e.a.GetTableMetadata(ctx, &athena.GetTableMetadataInput{CatalogName: aws.String("AwsDataCatalog"), DatabaseName: aws.String("shop"), TableName: aws.String("nope")})
	isErr[*types.MetadataException](t, err)

	q = e.wait(t, e.start("shop", "SELECT * FROM sales"))
	if q.Status.State != types.QueryExecutionStateFailed || *q.Status.StateChangeReason != "pogi: Docker is not available, so Athena cannot run queries" ||
		*q.Status.AthenaError.ErrorCategory != 1 || q.StatementType != types.StatementTypeDml {
		t.Fatalf("select without Docker %+v", q.Status)
	}
	_, err = e.a.GetQueryResults(ctx, &athena.GetQueryResultsInput{QueryExecutionId: q.QueryExecutionId})
	isErr[*types.InvalidRequestException](t, err)
	_, err = e.a.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{QueryString: aws.String("SELECT 1")})
	if e := isErr[*types.InvalidRequestException](t, err); !strings.Contains(e.ErrorMessage(), "No output location provided") {
		t.Fatalf("no output location: %v", err)
	}
	in := &athena.StartQueryExecutionInput{QueryString: aws.String("SHOW TABLES IN shop"), ClientRequestToken: aws.String("token-0123456789abcdef0123456789abcdef"),
		ResultConfiguration: &types.ResultConfiguration{OutputLocation: aws.String("s3://results/")}}
	first, again := must(e.a.StartQueryExecution(ctx, in)).QueryExecutionId, must(e.a.StartQueryExecution(ctx, in)).QueryExecutionId
	if *first != *again {
		t.Fatalf("the same client request token started %s and %s", *first, *again)
	}
	ids := must(e.a.ListQueryExecutions(ctx, &athena.ListQueryExecutionsInput{})).QueryExecutionIds
	if len(ids) < 10 || ids[0] != *first {
		t.Fatalf("query executions %v", ids)
	}
	b := must(e.a.BatchGetQueryExecution(ctx, &athena.BatchGetQueryExecutionInput{QueryExecutionIds: []string{*first, "nope"}}))
	if len(b.QueryExecutions) != 1 || len(b.UnprocessedQueryExecutionIds) != 1 {
		t.Fatalf("batch %+v", b)
	}
	e.wait(t, first)

	if r := e.failed(t, "shop", "DROP DATABASE shop"); !strings.Contains(r, "is not empty") {
		t.Fatalf("drop a database with tables: %s", r)
	}
	e.ok(t, "shop", "DROP TABLE sales")
	e.ok(t, "shop", "DROP TABLE IF EXISTS sales")
	if r := e.failed(t, "shop", "DROP TABLE sales"); !strings.Contains(r, "Table not found") {
		t.Fatalf("drop a missing table: %s", r)
	}
	_, err = e.g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("shop"), Name: aws.String("sales")})
	isErr[*gluetypes.EntityNotFoundException](t, err)
	e.ok(t, "", "DROP DATABASE shop")
}

// TestQueriesInDocker runs SELECTs on DuckDB when a container engine answers;
// the first run builds the engine image, which needs the network.
func TestQueriesInDocker(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	e := setup(t, true)
	e.put("sales/dt=2026-01-01/part-0.csv", "id,item,price\n1,apple,1.5\n2,pear,2\n3,apple,1.5\n")
	e.put("sales/dt=2026-01-02/part-0.csv", "id,item,price\n4,apple,2.5\n5,plum,4\n6,pear,1\n")
	e.put("sales/dt=2026-01-03/part-0.csv", "id,item,price\n7,fig,9\n") // its partition goes again below: not read
	e.put("events/2026/01/events.json", `{"name": "ann", "score": 10, "ts": "2026-01-01T10:00:00Z", "tags": ["a", "b"]}
{"name": "bob", "score": 5, "ts": "2026-01-02T11:30:00Z", "tags": []}
{"name": "ann", "score": 7, "ts": "2026-01-03T12:00:00Z", "tags": ["c"]}
`)
	e.ok(t, "", "CREATE DATABASE shop")
	e.ok(t, "", salesDDL)
	e.ok(t, "shop", "ALTER TABLE sales ADD PARTITION (dt='2026-01-01')")
	e.ok(t, "shop", "MSCK REPAIR TABLE sales")
	must(e.g.DeletePartition(ctx, &glue.DeletePartitionInput{DatabaseName: aws.String("shop"), TableName: aws.String("sales"), PartitionValues: []string{"2026-01-03"}}))

	sql := "SELECT item, count(*) AS n, sum(price) AS total FROM sales WHERE price >= 1.5 GROUP BY item ORDER BY total DESC"
	id := e.start("shop", sql)
	q := e.wait(t, id)
	if q.Status.State != types.QueryExecutionStateSucceeded {
		t.Fatalf("%s: %s", q.Status.State, aws.ToString(q.Status.StateChangeReason))
	}
	want := [][]string{{"item", "n", "total"}, {"apple", "3", "5.5"}, {"plum", "1", "4.0"}, {"pear", "1", "2.0"}}
	if got := e.rows(id, 0); !slices.Equal(flat(got), flat(want)) {
		t.Fatalf("rows %v, want %v", got, want)
	}
	if got := e.rows(id, 3); !slices.Equal(flat(got), flat(want)) { // two pages
		t.Fatalf("paged rows %v", got)
	}
	res := must(e.a.GetQueryResults(ctx, &athena.GetQueryResultsInput{QueryExecutionId: id}))
	var types_ []string
	for _, c := range res.ResultSet.ResultSetMetadata.ColumnInfo {
		types_ = append(types_, *c.Name+":"+*c.Type)
	}
	if !slices.Equal(types_, []string{"item:varchar", "n:bigint", "total:double"}) {
		t.Fatalf("columns %v", types_)
	}
	if *q.Statistics.DataScannedInBytes == 0 || !strings.HasSuffix(*q.ResultConfiguration.OutputLocation, *id+".csv") {
		t.Fatalf("execution %+v %+v", q.Statistics, q.ResultConfiguration)
	}
	csv := "\"item\",\"n\",\"total\"\n\"apple\",\"3\",\"5.5\"\n\"plum\",\"1\",\"4.0\"\n\"pear\",\"1\",\"2.0\"\n"
	if got := e.object(t, "results", "q/"+*id+".csv"); got != csv {
		t.Fatalf("results in S3: %q", got)
	}
	if meta := e.object(t, "results", "q/"+*id+".csv.metadata"); !strings.Contains(meta, `"Type":"bigint"`) {
		t.Fatalf("metadata %s", meta)
	}

	if got := e.ok(t, "", `SELECT id, dt FROM "AwsDataCatalog"."shop"."sales" WHERE dt = '2026-01-02' AND item <> 'plum' ORDER BY id`); !slices.Equal(flat(got),
		flat([][]string{{"id", "dt"}, {"4", "2026-01-02"}, {"6", "2026-01-02"}})) {
		t.Fatalf("partition filter %v", got)
	}
	e.ok(t, "shop", `CREATE EXTERNAL TABLE events (name string, score int, ts string, tags array<string>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://lake/events/'`)
	got := e.ok(t, "shop", `SELECT name, sum(score) AS score, approx_distinct(ts) AS days, max(cardinality(tags)) AS tags,
  date_format(min(from_iso8601_timestamp(ts)), '%Y-%m-%d %H:%i') AS first_at,
  min(date_parse(substr(ts, 1, 10), '%Y-%m-%d')) AS first_day
FROM events GROUP BY name ORDER BY name`)
	want = [][]string{{"name", "score", "days", "tags", "first_at", "first_day"},
		{"ann", "17", "2", "2", "2026-01-01 10:00", "2026-01-01 00:00:00.000"}, {"bob", "5", "1", "0", "2026-01-02 11:30", "2026-01-02 00:00:00.000"}}
	if !slices.Equal(flat(got), flat(want)) {
		t.Fatalf("json rows %v, want %v", got, want)
	}
	if got := e.ok(t, "shop", "SELECT count(*) AS n FROM sales WHERE item = ?", "'pear'"); !slices.Equal(flat(got), []string{"n", "2"}) {
		t.Fatalf("execution parameters %v", got)
	}
	must(e.a.CreatePreparedStatement(ctx, &athena.CreatePreparedStatementInput{StatementName: aws.String("by_item"), WorkGroup: aws.String("primary"),
		QueryStatement: aws.String("SELECT count(*) AS n FROM sales WHERE item = ? AND price > ?")}))
	if got := e.ok(t, "shop", "EXECUTE by_item USING 'apple', 1.5"); !slices.Equal(flat(got), []string{"n", "1"}) {
		t.Fatalf("execute %v", got)
	}
	if r := e.failed(t, "shop", "SELECT nope FROM sales"); !strings.Contains(r, "nope") {
		t.Fatalf("bad column: %s", r)
	}
	if got := e.ok(t, "shop", "SELECT * FROM sales WHERE false"); !slices.Equal(flat(got), []string{"id", "item", "price", "dt"}) {
		t.Fatalf("no rows %v", got)
	}
}

func flat(rows [][]string) (out []string) {
	for _, r := range rows {
		out = append(out, r...)
	}
	return out
}

func TestRewrite(t *testing.T) {
	for in, want := range map[string]string{
		"SELECT approx_distinct(x), cardinality(a) FROM t":                     "SELECT approx_count_distinct(x), len(a) FROM t",
		"SELECT from_iso8601_timestamp(s), from_iso8601_date('2026-01-01')":    "SELECT CAST(s AS TIMESTAMPTZ), CAST('2026-01-01' AS DATE)",
		"SELECT date_parse(s, '%Y-%m-%d %H:%i:%s')":                            "SELECT strptime(s, '%Y-%m-%d %H:%M:%S')",
		"SELECT date_format(date_parse(s, '%T'), '%M %e')":                     "SELECT strftime(strptime(s, '%H:%M:%S'), '%B %d')",
		"SELECT 'cardinality(a)', \"approx_distinct\" -- approx_distinct(x)\n": "SELECT 'cardinality(a)', \"approx_distinct\" -- approx_distinct(x)\n",
		"SELECT json_extract_scalar(j, '$.a'), regexp_like(s, 'a(b)')":         "SELECT json_extract_string(j, '$.a'), regexp_matches(s, 'a(b)')",
		"SELECT approx_distinct(x, 0.01)":                                      "SELECT approx_distinct(x, 0.01)",
	} {
		if got := athenasvc.Rewrite(in); got != want {
			t.Errorf("Rewrite(%q) = %q, want %q", in, got, want)
		}
	}
	if got, err := athenasvc.Bind("SELECT ? WHERE a = '?' AND b = ?", []string{"1", "'x'"}); err != nil || got != "SELECT 1 WHERE a = '?' AND b = 'x'" {
		t.Errorf("Bind = %q, %v", got, err)
	}
	if _, err := athenasvc.Bind("SELECT ?", nil); err == nil {
		t.Error("Bind without parameters succeeded")
	}
	for in, want := range map[string]string{"string": "VARCHAR", "int": "INTEGER", "decimal(10,2)": "DECIMAL(10,2)",
		"array<struct<a:int,b:map<string,double>>>": `STRUCT("a" INTEGER, "b" MAP(VARCHAR, DOUBLE))[]`} {
		if got := athenasvc.DuckType(in); got != want {
			t.Errorf("DuckType(%q) = %q, want %q", in, got, want)
		}
	}
}
