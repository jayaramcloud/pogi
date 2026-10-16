package glue_test

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	gluesvc "github.com/nrndev/pogi/internal/aws/glue"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func setup(t *testing.T) (*glue.Client, *s3.Client) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return gluesvc.New(h) })
	cfg := awstest.Config(t, url)
	return glue.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
}

// isErr asserts err is the API error E.
func isErr[E error](t *testing.T, err error) {
	t.Helper()
	var e E
	if !errors.As(err, &e) {
		t.Fatalf("got %v, want %T", err, e)
	}
}

func csvTable(name, location string) *types.TableInput {
	return &types.TableInput{Name: aws.String(name), TableType: aws.String("EXTERNAL_TABLE"),
		Parameters:    map[string]string{"classification": "csv", "skip.header.line.count": "1"},
		PartitionKeys: []types.Column{{Name: aws.String("dt"), Type: aws.String("string")}},
		StorageDescriptor: &types.StorageDescriptor{Location: aws.String(location),
			Columns:      []types.Column{{Name: aws.String("id"), Type: aws.String("bigint")}, {Name: aws.String("name"), Type: aws.String("string"), Comment: aws.String("who")}},
			InputFormat:  aws.String("org.apache.hadoop.mapred.TextInputFormat"),
			OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
			SerdeInfo: &types.SerDeInfo{SerializationLibrary: aws.String("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
				Parameters: map[string]string{"field.delim": ","}}}}
}

func TestDatabasesAndTables(t *testing.T) {
	g, _ := setup(t)
	must(g.CreateDatabase(ctx, &glue.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("Sales"), Description: aws.String("d")},
		Tags: map[string]string{"team": "data"}}))
	_, err := g.CreateDatabase(ctx, &glue.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("sales")}})
	isErr[*types.AlreadyExistsException](t, err)
	db := must(g.GetDatabase(ctx, &glue.GetDatabaseInput{Name: aws.String("sales")})).Database
	if *db.Name != "sales" || *db.Description != "d" || *db.CatalogId != "000000000000" || db.CreateTime == nil ||
		*db.CreateTableDefaultPermissions[0].Principal.DataLakePrincipalIdentifier != "IAM_ALLOWED_PRINCIPALS" {
		t.Fatalf("database %+v", db)
	}
	must(g.UpdateDatabase(ctx, &glue.UpdateDatabaseInput{Name: aws.String("sales"), DatabaseInput: &types.DatabaseInput{Name: aws.String("sales"),
		LocationUri: aws.String("s3://lake/sales/")}}))
	if dbs := must(g.GetDatabases(ctx, &glue.GetDatabasesInput{})).DatabaseList; len(dbs) != 1 || *dbs[0].LocationUri != "s3://lake/sales/" || dbs[0].Description != nil {
		t.Fatalf("databases %+v", dbs)
	}
	arn := "arn:aws:glue:us-east-1:000000000000:database/sales"
	if tags := must(g.GetTags(ctx, &glue.GetTagsInput{ResourceArn: &arn})).Tags; tags["team"] != "data" {
		t.Fatalf("tags %v", tags)
	}
	must(g.TagResource(ctx, &glue.TagResourceInput{ResourceArn: &arn, TagsToAdd: map[string]string{"env": "dev"}}))
	must(g.UntagResource(ctx, &glue.UntagResourceInput{ResourceArn: &arn, TagsToRemove: []string{"team"}}))
	if tags := must(g.GetTags(ctx, &glue.GetTagsInput{ResourceArn: &arn})).Tags; len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags %v", tags)
	}

	_, err = g.CreateTable(ctx, &glue.CreateTableInput{DatabaseName: aws.String("nope"), TableInput: csvTable("orders", "s3://lake/orders/")})
	isErr[*types.EntityNotFoundException](t, err)
	for _, name := range []string{"orders", "order_items", "customers"} {
		must(g.CreateTable(ctx, &glue.CreateTableInput{DatabaseName: aws.String("sales"), TableInput: csvTable(name, "s3://lake/"+name+"/")}))
	}
	_, err = g.CreateTable(ctx, &glue.CreateTableInput{DatabaseName: aws.String("sales"), TableInput: csvTable("orders", "s3://x/")})
	isErr[*types.AlreadyExistsException](t, err)
	tb := must(g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("sales"), Name: aws.String("ORDERS")})).Table
	sd := tb.StorageDescriptor
	if *tb.Name != "orders" || *tb.DatabaseName != "sales" || *tb.VersionId != "0" || tb.Parameters["skip.header.line.count"] != "1" ||
		*sd.Location != "s3://lake/orders/" || len(sd.Columns) != 2 || *sd.Columns[1].Comment != "who" ||
		sd.SerdeInfo.Parameters["field.delim"] != "," || *tb.PartitionKeys[0].Name != "dt" || tb.CreateTime == nil {
		t.Fatalf("table %+v", tb)
	}
	_, err = g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("sales"), Name: aws.String("nope")})
	isErr[*types.EntityNotFoundException](t, err)

	names := func(expr string) (out []string) {
		for _, t := range must(g.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String("sales"), Expression: aws.String(expr)})).TableList {
			out = append(out, *t.Name)
		}
		return out
	}
	if got := names("order.*"); !slices.Equal(got, []string{"order_items", "orders"}) {
		t.Fatalf("order.* matched %v", got)
	}
	if got := names("cust*|orders"); !slices.Equal(got, []string{"customers", "orders"}) {
		t.Fatalf("cust*|orders matched %v", got)
	}

	in := csvTable("orders", "s3://lake/orders-v2/")
	must(g.UpdateTable(ctx, &glue.UpdateTableInput{DatabaseName: aws.String("sales"), TableInput: in}))
	vs := must(g.GetTableVersions(ctx, &glue.GetTableVersionsInput{DatabaseName: aws.String("sales"), TableName: aws.String("orders")})).TableVersions
	if len(vs) != 2 || *vs[0].VersionId != "1" || *vs[0].Table.StorageDescriptor.Location != "s3://lake/orders-v2/" || *vs[1].VersionId != "0" {
		t.Fatalf("versions %+v", vs)
	}
	// floci #1183: UpdateTable with a stale VersionId fails
	_, err = g.UpdateTable(ctx, &glue.UpdateTableInput{DatabaseName: aws.String("sales"), TableInput: in, VersionId: aws.String("0")})
	isErr[*types.ConcurrentModificationException](t, err)
	must(g.UpdateTable(ctx, &glue.UpdateTableInput{DatabaseName: aws.String("sales"), TableInput: in, VersionId: aws.String("1")}))
	errs := must(g.BatchDeleteTable(ctx, &glue.BatchDeleteTableInput{DatabaseName: aws.String("sales"), TablesToDelete: []string{"customers", "nope"}})).Errors
	if len(errs) != 1 || *errs[0].TableName != "nope" || *errs[0].ErrorDetail.ErrorCode != "EntityNotFoundException" {
		t.Fatalf("batch delete errors %+v", errs)
	}
	must(g.DeleteDatabase(ctx, &glue.DeleteDatabaseInput{Name: aws.String("sales")}))
	_, err = g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("sales"), Name: aws.String("orders")})
	isErr[*types.EntityNotFoundException](t, err)
}

func TestPartitions(t *testing.T) {
	g, _ := setup(t)
	must(g.CreateDatabase(ctx, &glue.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("logs")}}))
	tbl := csvTable("events", "s3://lake/events/")
	tbl.PartitionKeys = append(tbl.PartitionKeys, types.Column{Name: aws.String("region"), Type: aws.String("string")})
	must(g.CreateTable(ctx, &glue.CreateTableInput{DatabaseName: aws.String("logs"), TableInput: tbl}))
	part := func(dt, region string) types.PartitionInput {
		return types.PartitionInput{Values: []string{dt, region},
			StorageDescriptor: &types.StorageDescriptor{Location: aws.String("s3://lake/events/dt=" + dt + "/region=" + region + "/")}}
	}
	p := part("2026-01-01", "eu")
	must(g.CreatePartition(ctx, &glue.CreatePartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"), PartitionInput: &p}))
	errs := must(g.BatchCreatePartition(ctx, &glue.BatchCreatePartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionInputList: []types.PartitionInput{part("2026-01-01", "eu"), part("2026-01-01", "us"), part("2026-01-02", "eu"), part("2026-01-10", "eu")}})).Errors
	if len(errs) != 1 || errs[0].PartitionValues[1] != "eu" || *errs[0].ErrorDetail.ErrorCode != "AlreadyExistsException" {
		t.Fatalf("batch create errors %+v", errs)
	}
	bad := types.PartitionInput{Values: []string{"2026-01-01"}}
	_, err := g.CreatePartition(ctx, &glue.CreatePartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"), PartitionInput: &bad})
	isErr[*types.InvalidInputException](t, err)

	got := must(g.GetPartition(ctx, &glue.GetPartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionValues: []string{"2026-01-01", "us"}})).Partition
	if *got.StorageDescriptor.Location != "s3://lake/events/dt=2026-01-01/region=us/" || *got.TableName != "events" || got.CreationTime == nil {
		t.Fatalf("partition %+v", got)
	}
	filter := func(expr string) (out []string) {
		for _, p := range must(g.GetPartitions(ctx, &glue.GetPartitionsInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
			Expression: aws.String(expr)})).Partitions {
			out = append(out, strings.Join(p.Values, "/"))
		}
		return out
	}
	for expr, want := range map[string][]string{
		"":                                     {"2026-01-01/eu", "2026-01-01/us", "2026-01-02/eu", "2026-01-10/eu"},
		"dt = '2026-01-01'":                    {"2026-01-01/eu", "2026-01-01/us"},
		"dt='2026-01-01' AND region='eu'":      {"2026-01-01/eu"},
		"region IN ('us', 'xx')":               {"2026-01-01/us"},
		"dt >= '2026-01-02' and region = 'eu'": {"2026-01-02/eu", "2026-01-10/eu"},
	} {
		if got := filter(expr); !slices.Equal(got, want) {
			t.Errorf("%q matched %v, want %v", expr, got, want)
		}
	}
	_, err = g.GetPartitions(ctx, &glue.GetPartitionsInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"), Expression: aws.String("nope = 1")})
	isErr[*types.InvalidInputException](t, err)

	moved := part("2026-01-01", "us")
	moved.StorageDescriptor.Location = aws.String("s3://elsewhere/")
	must(g.UpdatePartition(ctx, &glue.UpdatePartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionValueList: []string{"2026-01-01", "us"}, PartitionInput: &moved}))
	got = must(g.GetPartition(ctx, &glue.GetPartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionValues: []string{"2026-01-01", "us"}})).Partition
	if *got.StorageDescriptor.Location != "s3://elsewhere/" {
		t.Fatalf("updated partition %+v", got)
	}
	must(g.DeletePartition(ctx, &glue.DeletePartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionValues: []string{"2026-01-01", "us"}}))
	_, err = g.GetPartition(ctx, &glue.GetPartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionValues: []string{"2026-01-01", "us"}})
	isErr[*types.EntityNotFoundException](t, err)
	errs = must(g.BatchDeletePartition(ctx, &glue.BatchDeletePartitionInput{DatabaseName: aws.String("logs"), TableName: aws.String("events"),
		PartitionsToDelete: []types.PartitionValueList{{Values: []string{"2026-01-02", "eu"}}, {Values: []string{"2027-01-01", "eu"}}}})).Errors
	if len(errs) != 1 || errs[0].PartitionValues[0] != "2027-01-01" {
		t.Fatalf("batch delete errors %+v", errs)
	}
	if got := filter(""); !slices.Equal(got, []string{"2026-01-01/eu", "2026-01-10/eu"}) {
		t.Fatalf("left %v", got)
	}
}

func TestCrawler(t *testing.T) {
	g, s := setup(t)
	must(s.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("lake")}))
	put := func(key, body string) {
		must(s.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("lake"), Key: aws.String(key), Body: strings.NewReader(body)}))
	}
	put("raw/sales/dt=2026-01-01/a.csv", "id,name,amount\n1,ann,1.5\n2,bob,2\n")
	put("raw/sales/dt=2026-01-02/b.csv", "id,name,amount\n3,cid,3.25\n")
	put("raw/sales/_SUCCESS", "")
	put("raw/events/part-0.json", `{"id": 1, "user": "ann", "tags": ["a"], "geo": {"lat": 1.5}, "ok": true}`+"\n"+`{"id": 2, "user": null, "extra": 2.5}`+"\n")
	must(g.CreateCrawler(ctx, &glue.CreateCrawlerInput{Name: aws.String("lake"), Role: aws.String("arn:aws:iam::000000000000:role/glue"),
		DatabaseName: aws.String("lake"), TablePrefix: aws.String("raw_"),
		Targets: &types.CrawlerTargets{S3Targets: []types.S3Target{{Path: aws.String("s3://lake/raw/")}}}}))
	_, err := g.StopCrawler(ctx, &glue.StopCrawlerInput{Name: aws.String("lake")})
	isErr[*types.CrawlerNotRunningException](t, err)
	must(g.StartCrawler(ctx, &glue.StartCrawlerInput{Name: aws.String("lake")}))
	var cr *types.Crawler
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		cr = must(g.GetCrawler(ctx, &glue.GetCrawlerInput{Name: aws.String("lake")})).Crawler
		if cr.State == types.CrawlerStateReady && cr.LastCrawl != nil {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("crawler did not finish: %+v", cr)
		}
	}
	if cr.LastCrawl.Status != types.LastCrawlStatusSucceeded {
		t.Fatalf("crawl %+v: %s", cr.LastCrawl, aws.ToString(cr.LastCrawl.ErrorMessage))
	}
	m := must(g.GetCrawlerMetrics(ctx, &glue.GetCrawlerMetricsInput{CrawlerNameList: []string{"lake"}})).CrawlerMetricsList
	if len(m) != 1 || m[0].TablesCreated != 2 {
		t.Fatalf("metrics %+v", m)
	}

	sales := must(g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("lake"), Name: aws.String("raw_sales")})).Table
	var cols []string
	for _, c := range sales.StorageDescriptor.Columns {
		cols = append(cols, *c.Name+" "+*c.Type)
	}
	if !slices.Equal(cols, []string{"id bigint", "name string", "amount double"}) || *sales.PartitionKeys[0].Name != "dt" ||
		sales.Parameters["classification"] != "csv" || sales.Parameters["skip.header.line.count"] != "1" ||
		*sales.StorageDescriptor.Location != "s3://lake/raw/sales/" || sales.StorageDescriptor.SerdeInfo.Parameters["field.delim"] != "," {
		t.Fatalf("sales %v %+v", cols, sales)
	}
	parts := must(g.GetPartitions(ctx, &glue.GetPartitionsInput{DatabaseName: aws.String("lake"), TableName: aws.String("raw_sales")})).Partitions
	if len(parts) != 2 || parts[1].Values[0] != "2026-01-02" || *parts[1].StorageDescriptor.Location != "s3://lake/raw/sales/dt=2026-01-02/" {
		t.Fatalf("partitions %+v", parts)
	}
	events := must(g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("lake"), Name: aws.String("raw_events")})).Table
	cols = nil
	for _, c := range events.StorageDescriptor.Columns {
		cols = append(cols, *c.Name+" "+*c.Type)
	}
	if !slices.Equal(cols, []string{"id bigint", "user string", "tags array<string>", "geo struct<lat:double>", "ok boolean", "extra double"}) ||
		events.Parameters["classification"] != "json" || len(events.PartitionKeys) != 0 {
		t.Fatalf("events %v %+v", cols, events)
	}

	must(g.StartCrawler(ctx, &glue.StartCrawlerInput{Name: aws.String("lake")})) // again: nothing changed
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if cr = must(g.GetCrawler(ctx, &glue.GetCrawlerInput{Name: aws.String("lake")})).Crawler; cr.State == types.CrawlerStateReady {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("crawler did not finish: %+v", cr)
		}
	}
	vs := must(g.GetTableVersions(ctx, &glue.GetTableVersionsInput{DatabaseName: aws.String("lake"), TableName: aws.String("raw_sales")})).TableVersions
	if len(vs) != 1 {
		t.Fatalf("a crawl without changes made %d versions", len(vs))
	}
	must(g.DeleteCrawler(ctx, &glue.DeleteCrawlerInput{Name: aws.String("lake")}))
	_, err = g.GetCrawler(ctx, &glue.GetCrawlerInput{Name: aws.String("lake")})
	isErr[*types.EntityNotFoundException](t, err)
}

func TestMetadataOnly(t *testing.T) {
	g, _ := setup(t)
	must(g.CreateDatabase(ctx, &glue.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("db")}}))
	must(g.CreateJob(ctx, &glue.CreateJobInput{Name: aws.String("etl"), Role: aws.String("arn:aws:iam::000000000000:role/glue"),
		Command: &types.JobCommand{Name: aws.String("glueetl"), ScriptLocation: aws.String("s3://code/etl.py")}, DefaultArguments: map[string]string{"--x": "1"}}))
	_, err := g.CreateJob(ctx, &glue.CreateJobInput{Name: aws.String("etl"), Role: aws.String("r"), Command: &types.JobCommand{Name: aws.String("glueetl")}})
	isErr[*types.AlreadyExistsException](t, err)
	if j := must(g.GetJob(ctx, &glue.GetJobInput{JobName: aws.String("etl")})).Job; *j.Command.ScriptLocation != "s3://code/etl.py" || *j.Timeout != 2880 {
		t.Fatalf("job %+v", j)
	}
	id := must(g.StartJobRun(ctx, &glue.StartJobRunInput{JobName: aws.String("etl")})).JobRunId
	run := must(g.GetJobRun(ctx, &glue.GetJobRunInput{JobName: aws.String("etl"), RunId: id})).JobRun
	if run.JobRunState != types.JobRunStateSucceeded || run.Arguments["--x"] != "1" || run.CompletedOn == nil {
		t.Fatalf("run %+v", run)
	}
	if runs := must(g.GetJobRuns(ctx, &glue.GetJobRunsInput{JobName: aws.String("etl")})).JobRuns; len(runs) != 1 || *runs[0].Id != *id {
		t.Fatalf("runs %+v", runs)
	}
	_, err = g.StartJobRun(ctx, &glue.StartJobRunInput{JobName: aws.String("nope")})
	isErr[*types.EntityNotFoundException](t, err)

	must(g.CreateConnection(ctx, &glue.CreateConnectionInput{ConnectionInput: &types.ConnectionInput{Name: aws.String("pg"),
		ConnectionType: types.ConnectionTypeJdbc, ConnectionProperties: map[string]string{"JDBC_CONNECTION_URL": "jdbc:postgresql://db/x"}}}))
	if c := must(g.GetConnection(ctx, &glue.GetConnectionInput{Name: aws.String("pg")})).Connection; c.ConnectionProperties["JDBC_CONNECTION_URL"] == "" {
		t.Fatalf("connection %+v", c)
	}
	must(g.CreateTrigger(ctx, &glue.CreateTriggerInput{Name: aws.String("nightly"), Type: types.TriggerTypeScheduled, Schedule: aws.String("cron(0 1 * * ? *)"),
		Actions: []types.Action{{JobName: aws.String("etl")}}}))
	must(g.StartTrigger(ctx, &glue.StartTriggerInput{Name: aws.String("nightly")}))
	if tr := must(g.GetTrigger(ctx, &glue.GetTriggerInput{Name: aws.String("nightly")})).Trigger; tr.State != types.TriggerStateActivated || *tr.Actions[0].JobName != "etl" {
		t.Fatalf("trigger %+v", tr)
	}
	must(g.CreateUserDefinedFunction(ctx, &glue.CreateUserDefinedFunctionInput{DatabaseName: aws.String("db"),
		FunctionInput: &types.UserDefinedFunctionInput{FunctionName: aws.String("geo"), ClassName: aws.String("com.example.Geo"),
			OwnerName: aws.String("me"), OwnerType: types.PrincipalTypeUser}}))
	if f := must(g.GetUserDefinedFunction(ctx, &glue.GetUserDefinedFunctionInput{DatabaseName: aws.String("db"), FunctionName: aws.String("geo")})).UserDefinedFunction; *f.ClassName != "com.example.Geo" {
		t.Fatalf("function %+v", f)
	}
	if fs := must(g.GetUserDefinedFunctions(ctx, &glue.GetUserDefinedFunctionsInput{Pattern: aws.String("ge*")})).UserDefinedFunctions; len(fs) != 1 {
		t.Fatalf("functions %+v", fs)
	}
	if es := must(g.GetDataCatalogEncryptionSettings(ctx, &glue.GetDataCatalogEncryptionSettingsInput{})).DataCatalogEncryptionSettings; es.EncryptionAtRest.CatalogEncryptionMode != types.CatalogEncryptionModeDisabled {
		t.Fatalf("default encryption %+v", es.EncryptionAtRest)
	}
	must(g.PutDataCatalogEncryptionSettings(ctx, &glue.PutDataCatalogEncryptionSettingsInput{DataCatalogEncryptionSettings: &types.DataCatalogEncryptionSettings{
		EncryptionAtRest: &types.EncryptionAtRest{CatalogEncryptionMode: types.CatalogEncryptionModeSsekms, SseAwsKmsKeyId: aws.String("alias/glue")}}}))
	if es := must(g.GetDataCatalogEncryptionSettings(ctx, &glue.GetDataCatalogEncryptionSettingsInput{})).DataCatalogEncryptionSettings; *es.EncryptionAtRest.SseAwsKmsKeyId != "alias/glue" {
		t.Fatalf("encryption %+v", es.EncryptionAtRest)
	}
}

// floci #4676: the schema registry, with versions that ignore custom Avro attributes, and metadata.
func TestSchemaRegistry(t *testing.T) {
	c, _ := setup(t)
	reg := must(c.CreateRegistry(ctx, &glue.CreateRegistryInput{RegistryName: aws.String("reg"), Description: aws.String("d"), Tags: map[string]string{"env": "dev"}}))
	if !strings.HasSuffix(aws.ToString(reg.RegistryArn), ":registry/reg") {
		t.Fatalf("registry = %+v", reg)
	}
	_, err := c.CreateRegistry(ctx, &glue.CreateRegistryInput{RegistryName: aws.String("reg")})
	isErr[*types.AlreadyExistsException](t, err)
	if tags := must(c.GetTags(ctx, &glue.GetTagsInput{ResourceArn: reg.RegistryArn})).Tags; tags["env"] != "dev" {
		t.Fatalf("tags = %v", tags)
	}
	def := func(extra string) *string {
		return aws.String(`{"type":"record","name":"R","fields":[{"name":"a","type":"string"` + extra + `}]}`)
	}
	rid := &types.RegistryId{RegistryName: aws.String("reg")}
	sid := &types.SchemaId{SchemaName: aws.String("sch"), RegistryName: aws.String("reg")}
	s := must(c.CreateSchema(ctx, &glue.CreateSchemaInput{SchemaName: aws.String("sch"), RegistryId: rid, DataFormat: types.DataFormatAvro, Compatibility: types.CompatibilityNone, SchemaDefinition: def("")}))
	if aws.ToInt64(s.LatestSchemaVersion) != 1 || aws.ToInt64(s.NextSchemaVersion) != 2 || s.SchemaVersionStatus != types.SchemaVersionStatusAvailable {
		t.Fatalf("schema = %+v", s)
	}
	_, err = c.CreateSchema(ctx, &glue.CreateSchemaInput{SchemaName: aws.String("bad"), RegistryId: rid, DataFormat: types.DataFormatAvro, SchemaDefinition: aws.String("{not json")})
	isErr[*types.InvalidInputException](t, err)

	same := must(c.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{SchemaId: sid, SchemaDefinition: def(`,"x-custom":"foo"`)}))
	if aws.ToString(same.SchemaVersionId) != aws.ToString(s.SchemaVersionId) || aws.ToInt64(same.VersionNumber) != 1 {
		t.Fatalf("a custom attribute made version %d", aws.ToInt64(same.VersionNumber))
	}
	v2 := must(c.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{SchemaId: sid, SchemaDefinition: aws.String(`{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":"int"}]}`)}))
	if aws.ToInt64(v2.VersionNumber) != 2 {
		t.Fatalf("version = %d", aws.ToInt64(v2.VersionNumber))
	}
	g := must(c.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{SchemaId: sid, SchemaVersionNumber: &types.SchemaVersionNumber{LatestVersion: true}}))
	if aws.ToInt64(g.VersionNumber) != 2 || !strings.Contains(aws.ToString(g.SchemaDefinition), `"b"`) {
		t.Fatalf("latest = %+v", g)
	}
	byID := must(c.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{SchemaVersionId: s.SchemaVersionId}))
	if aws.ToInt64(byID.VersionNumber) != 1 {
		t.Fatalf("by id = %+v", byID)
	}
	if l := must(c.ListSchemaVersions(ctx, &glue.ListSchemaVersionsInput{SchemaId: sid})).Schemas; len(l) != 2 {
		t.Fatalf("versions = %+v", l)
	}
	if d := must(c.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{SchemaId: sid, SchemaDefinition: def("")})); aws.ToString(d.SchemaVersionId) != aws.ToString(s.SchemaVersionId) {
		t.Fatalf("by definition = %+v", d)
	}
	_, err = c.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{SchemaId: sid, SchemaDefinition: aws.String(`{"type":"record","name":"R","fields":[{"name":"zz","type":"int"}]}`)})
	isErr[*types.EntityNotFoundException](t, err)

	// metadata
	kv := func(k, v string) *types.MetadataKeyValuePair {
		return &types.MetadataKeyValuePair{MetadataKey: aws.String(k), MetadataValue: aws.String(v)}
	}
	must(c.PutSchemaVersionMetadata(ctx, &glue.PutSchemaVersionMetadataInput{SchemaVersionId: s.SchemaVersionId, MetadataKeyValue: kv("owner", "team-a")}))
	_, err = c.PutSchemaVersionMetadata(ctx, &glue.PutSchemaVersionMetadataInput{SchemaVersionId: s.SchemaVersionId, MetadataKeyValue: kv("owner", "team-a")})
	isErr[*types.AlreadyExistsException](t, err)
	must(c.PutSchemaVersionMetadata(ctx, &glue.PutSchemaVersionMetadataInput{SchemaVersionId: s.SchemaVersionId, MetadataKeyValue: kv("owner", "team-b")}))
	q := must(c.QuerySchemaVersionMetadata(ctx, &glue.QuerySchemaVersionMetadataInput{SchemaVersionId: s.SchemaVersionId}))
	if e, ok := q.MetadataInfoMap["owner"]; !ok || aws.ToString(e.MetadataValue) != "team-b" || len(e.OtherMetadataValueList) != 1 {
		t.Fatalf("metadata = %+v", q.MetadataInfoMap)
	}
	must(c.RemoveSchemaVersionMetadata(ctx, &glue.RemoveSchemaVersionMetadataInput{SchemaVersionId: s.SchemaVersionId, MetadataKeyValue: kv("owner", "team-b")}))
	if q := must(c.QuerySchemaVersionMetadata(ctx, &glue.QuerySchemaVersionMetadataInput{SchemaVersionId: s.SchemaVersionId})); len(q.MetadataInfoMap) != 0 {
		t.Fatalf("metadata after remove = %+v", q.MetadataInfoMap)
	}

	// versions can go, except the checkpoint
	del := must(c.DeleteSchemaVersions(ctx, &glue.DeleteSchemaVersionsInput{SchemaId: sid, Versions: aws.String("1-2")}))
	if len(del.SchemaVersionErrors) != 1 || aws.ToInt64(del.SchemaVersionErrors[0].VersionNumber) != 1 {
		t.Fatalf("delete errors = %+v", del.SchemaVersionErrors)
	}
	must(c.UpdateSchema(ctx, &glue.UpdateSchemaInput{SchemaId: sid, Description: aws.String("new"), Compatibility: types.CompatibilityDisabled}))
	_, err = c.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{SchemaId: sid, SchemaDefinition: aws.String(`{"type":"record","name":"R","fields":[{"name":"c","type":"int"}]}`)})
	isErr[*types.InvalidInputException](t, err)

	// default registry, lists, deletes
	must(c.CreateSchema(ctx, &glue.CreateSchemaInput{SchemaName: aws.String("loose"), DataFormat: types.DataFormatJson, SchemaDefinition: aws.String(`{"type":"object"}`)}))
	if l := must(c.ListRegistries(ctx, &glue.ListRegistriesInput{})).Registries; len(l) != 2 {
		t.Fatalf("registries = %+v", l)
	}
	if l := must(c.ListSchemas(ctx, &glue.ListSchemasInput{RegistryId: rid})).Schemas; len(l) != 1 || aws.ToString(l[0].SchemaName) != "sch" {
		t.Fatalf("schemas = %+v", l)
	}
	if gs := must(c.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: sid})); aws.ToString(gs.Description) != "new" || gs.Compatibility != types.CompatibilityDisabled {
		t.Fatalf("GetSchema = %+v", gs)
	}
	must(c.DeleteSchema(ctx, &glue.DeleteSchemaInput{SchemaId: sid}))
	_, err = c.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: sid})
	isErr[*types.EntityNotFoundException](t, err)
	must(c.DeleteRegistry(ctx, &glue.DeleteRegistryInput{RegistryId: rid}))
	_, err = c.GetRegistry(ctx, &glue.GetRegistryInput{RegistryId: rid})
	isErr[*types.EntityNotFoundException](t, err)
}

// floci #1130: tagged resources are listed for the tagging API.
func TestTaggedResources(t *testing.T) {
	var svc *gluesvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = gluesvc.New(h); return svc })
	c := glue.NewFromConfig(awstest.Config(t, url))
	must(c.CreateDatabase(ctx, &glue.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("sample")}, Tags: map[string]string{"team": "data"}}))
	got := svc.TaggedResources("000000000000", "us-east-1")
	if got["arn:aws:glue:us-east-1:000000000000:database/sample"]["team"] != "data" || len(got) != 1 {
		t.Fatalf("tagged %v", got)
	}
	if len(svc.TaggedResources("000000000000", "eu-west-1")) != 0 {
		t.Fatal("other region lists it")
	}
}

// A create or update without its input structure is an error, not a panic.
func TestMissingInputStructure(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return gluesvc.New(h) })
	for _, op := range []string{"CreateDatabase", "UpdateDatabase", "CreateConnection", "UpdateConnection"} {
		req, _ := http.NewRequest("POST", url+"/", strings.NewReader(`{"Name":"x"}`))
		req.Header.Set("X-Amz-Target", "AWSGlue."+op)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		awstest.Sign(t, req, "glue")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", op, resp.StatusCode)
		}
	}
}
