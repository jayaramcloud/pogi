package rdsdata_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	sm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"

	"github.com/nrndev/pogi/internal/aws/rds"
	data "github.com/nrndev/pogi/internal/aws/rdsdata"
	"github.com/nrndev/pogi/internal/aws/secretsmanager"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

var ctx = context.Background()

type clients struct {
	rds  *awsrds.Client
	sm   *sm.Client
	data *rdsdata.Client
}

func serve(t *testing.T) clients {
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return rds.New(h) },
		func(h *core.Hub) core.Service { return data.New(h) },
		func(h *core.Hub) core.Service { return secretsmanager.New(h) })
	cfg := awstest.Config(t, base)
	return clients{awsrds.NewFromConfig(cfg), sm.NewFromConfig(cfg), rdsdata.NewFromConfig(cfg)}
}

func withDocker(t *testing.T) clients {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	return serve(t)
}

// cluster makes an Aurora cluster with a secret of its master credentials
// and waits for it.
func cluster(t *testing.T, c clients, engine, id string, http bool) (arn, secret string) {
	t.Helper()
	out, err := c.rds.CreateDBCluster(ctx, &awsrds.CreateDBClusterInput{DBClusterIdentifier: aws.String(id), Engine: aws.String(engine),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("secret123"), DatabaseName: aws.String("shop"),
		EnableHttpEndpoint: aws.Bool(http)})
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.sm.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String(id), SecretString: aws.String(`{"username":"admin","password":"secret123"}`)})
	if err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		d, err := c.rds.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{DBClusterIdentifier: aws.String(id)})
		if err != nil || aws.ToString(d.DBClusters[0].Status) == "failed" || time.Now().After(end) {
			t.Fatalf("cluster %s: %v %+v", id, err, d)
		}
		if aws.ToString(d.DBClusters[0].Status) == "available" {
			return aws.ToString(out.DBCluster.DBClusterArn), aws.ToString(s.ARN)
		}
	}
}

// value unwraps a Field.
func value(f types.Field) any {
	switch f := f.(type) {
	case *types.FieldMemberLongValue:
		return f.Value
	case *types.FieldMemberStringValue:
		return f.Value
	case *types.FieldMemberBooleanValue:
		return f.Value
	case *types.FieldMemberDoubleValue:
		return f.Value
	case *types.FieldMemberBlobValue:
		return string(f.Value)
	case *types.FieldMemberIsNull:
		return nil
	}
	return fmt.Sprintf("%T", f)
}

func values(rec []types.Field) []any {
	out := make([]any, len(rec))
	for i, f := range rec {
		out[i] = value(f)
	}
	return out
}

func p(name string, v types.Field, hint ...types.TypeHint) types.SqlParameter {
	sp := types.SqlParameter{Name: aws.String(name), Value: v}
	if len(hint) > 0 {
		sp.TypeHint = hint[0]
	}
	return sp
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return fmt.Sprint(err)
}

// TestPostgres runs statements, transactions and batches on an
// aurora-postgresql cluster, then reads them back from a restored snapshot.
func TestPostgres(t *testing.T) {
	c := withDocker(t)
	id := fmt.Sprintf("data-pg-%d", os.Getpid())
	arn, secret := cluster(t, c, "aurora-postgresql", id, true)
	run := func(sql string, params ...types.SqlParameter) *rdsdata.ExecuteStatementOutput {
		t.Helper()
		out, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &sql,
			Parameters: params, IncludeResultMetadata: true})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return out
	}
	run(`CREATE TABLE items (id serial PRIMARY KEY, name text, price numeric(10,2), qty int, ok bool, born date, data bytea, meta jsonb, note text)`)
	ins := run(`INSERT INTO items (name, price, qty, ok, born, data, meta, note) VALUES (:name, :price, :qty, :ok, :born, :data, :meta, :note)`,
		p("name", &types.FieldMemberStringValue{Value: "it's :not a param"}), p("price", &types.FieldMemberStringValue{Value: "9.50"}, types.TypeHintDecimal),
		p("qty", &types.FieldMemberLongValue{Value: 3}), p("ok", &types.FieldMemberBooleanValue{Value: true}),
		p("born", &types.FieldMemberStringValue{Value: "2024-01-02"}, types.TypeHintDate), p("data", &types.FieldMemberBlobValue{Value: []byte{1, 2}}),
		p("meta", &types.FieldMemberStringValue{Value: `{"a": 1}`}, types.TypeHintJson), p("note", &types.FieldMemberIsNull{Value: true}))
	if ins.NumberOfRecordsUpdated != 1 || ins.Records != nil {
		t.Fatalf("insert %+v", ins)
	}
	ret := run(`INSERT INTO items (name, qty) VALUES (:name, :qty) RETURNING id`, p("name", &types.FieldMemberStringValue{Value: "two"}),
		p("qty", &types.FieldMemberLongValue{Value: 4}))
	if ret.NumberOfRecordsUpdated != 1 || len(ret.Records) != 1 || value(ret.Records[0][0]) != int64(2) {
		t.Fatalf("returning %+v", ret)
	}

	sel := run(`SELECT id, name, price, qty, ok, born, data, meta, note, 1.5::float8 AS d, 'a :b' /* :c */ AS lit FROM items WHERE id = :id`,
		p("id", &types.FieldMemberLongValue{Value: 1}), p("b", &types.FieldMemberLongValue{Value: 0}), p("c", &types.FieldMemberLongValue{Value: 0}))
	want := []any{int64(1), "it's :not a param", "9.50", int64(3), true, "2024-01-02", "\x01\x02", `{"a": 1}`, nil, 1.5, "a :b"}
	if len(sel.Records) != 1 || !reflect.DeepEqual(values(sel.Records[0]), want) {
		t.Fatalf("select %v, want %v", values(sel.Records[0]), want)
	}
	m := sel.ColumnMetadata
	if len(m) != 11 || aws.ToString(m[0].Name) != "id" || aws.ToString(m[0].TypeName) != "int4" || m[0].Type != 4 ||
		aws.ToString(m[1].TypeName) != "text" || m[1].Type != 12 || m[4].Type != -7 || m[5].Type != 91 || !m[2].IsSigned {
		t.Fatalf("metadata %+v", m)
	}

	// floci #5112 and #5117: numeric parameter names, and array columns as arrayValue
	arr := run(`SELECT CAST(:1 AS int), ARRAY[1,2,3], ARRAY['a','b']`, p("1", &types.FieldMemberLongValue{Value: 5}))
	if a, ok := arr.Records[0][1].(*types.FieldMemberArrayValue); value(arr.Records[0][0]) != int64(5) || !ok || len(a.Value.(*types.ArrayValueMemberLongValues).Value) != 3 {
		t.Fatalf("numeric name and array: %+v", arr.Records)
	}
	if _, ok := arr.Records[0][2].(*types.FieldMemberArrayValue); !ok {
		t.Fatalf("text array: %T", arr.Records[0][2])
	}

	sql := "SELECT id, name, note FROM items ORDER BY id"
	js, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &sql,
		FormatRecordsAs: types.RecordsFormatTypeJson})
	if err != nil || js.Records != nil {
		t.Fatalf("json: %v %+v", err, js)
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(js.FormattedRecords)), &rows); err != nil || len(rows) != 2 ||
		rows[0]["name"] != "it's :not a param" || rows[1]["id"] != 2.0 || rows[0]["note"] != nil {
		t.Fatalf("formatted %v: %s", err, aws.ToString(js.FormattedRecords))
	}

	count := func() any { return value(run("SELECT count(*) FROM items").Records[0][0]) }
	for _, commit := range []bool{true, false} {
		before := count()
		tx, err := c.data.BeginTransaction(ctx, &rdsdata.BeginTransactionInput{ResourceArn: &arn, SecretArn: &secret})
		if err != nil {
			t.Fatal(err)
		}
		insert := "INSERT INTO items (name) VALUES ('in a transaction')"
		if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &insert,
			TransactionId: tx.TransactionId}); err != nil {
			t.Fatal(err)
		}
		if n := count(); n != before {
			t.Fatalf("outside the transaction: %v rows", n)
		}
		if commit {
			out, err := c.data.CommitTransaction(ctx, &rdsdata.CommitTransactionInput{ResourceArn: &arn, SecretArn: &secret, TransactionId: tx.TransactionId})
			if err != nil || aws.ToString(out.TransactionStatus) != "Transaction Committed" || count() != before.(int64)+1 {
				t.Fatalf("commit: %v %+v", err, out)
			}
		} else {
			out, err := c.data.RollbackTransaction(ctx, &rdsdata.RollbackTransactionInput{ResourceArn: &arn, SecretArn: &secret, TransactionId: tx.TransactionId})
			if err != nil || aws.ToString(out.TransactionStatus) != "Rollback Complete" || count() != before {
				t.Fatalf("rollback: %v %+v", err, out)
			}
		}
		if _, err := c.data.CommitTransaction(ctx, &rdsdata.CommitTransactionInput{ResourceArn: &arn, SecretArn: &secret,
			TransactionId: tx.TransactionId}); code(err) != "TransactionNotFoundException" {
			t.Fatalf("ended transaction: %v", err)
		}
	}

	batch := "INSERT INTO items (name, qty) VALUES (:name, :qty)"
	var sets [][]types.SqlParameter
	for i := range 3 {
		sets = append(sets, []types.SqlParameter{p("name", &types.FieldMemberStringValue{Value: fmt.Sprint("b", i)}), p("qty", &types.FieldMemberLongValue{Value: int64(i)})})
	}
	b, err := c.data.BatchExecuteStatement(ctx, &rdsdata.BatchExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &batch, ParameterSets: sets})
	if err != nil || len(b.UpdateResults) != 3 || count() != int64(6) {
		t.Fatalf("batch: %v %+v", err, b)
	}
	sets[1][1] = p("qty", &types.FieldMemberStringValue{Value: "not a number"})
	if _, err := c.data.BatchExecuteStatement(ctx, &rdsdata.BatchExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &batch,
		ParameterSets: sets}); code(err) != "DatabaseErrorException" || count() != int64(6) {
		t.Fatalf("a failed batch changes nothing: %v, %v rows", err, count())
	}

	bad := "SELECT * FROM nope"
	if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &bad}); code(err) != "DatabaseErrorException" {
		t.Fatalf("missing table: %v", err)
	}
	wrong, _ := c.sm.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String(id + "-wrong"), SecretString: aws.String(`{"username":"admin","password":"nope-nope"}`)})
	if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: wrong.ARN, Sql: &bad}); code(err) != "ForbiddenException" {
		t.Fatalf("wrong password: %v", err)
	}

	// floci #5113 and #5114: a row over 64 KB and a result over 1 MiB are refused
	for _, sql := range []string{"SELECT repeat('x', 100000)", "SELECT repeat('x', 20000) FROM generate_series(1, 100)"} {
		if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &sql}); code(err) != "BadRequestException" {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if o := run("SELECT repeat('x', 60000)"); len(o.Records) != 1 {
		t.Fatalf("a 60 KB row is fine: %+v", o)
	}

	// the cluster's data survives a snapshot
	if _, err := c.rds.CreateDBClusterSnapshot(ctx, &awsrds.CreateDBClusterSnapshotInput{DBClusterIdentifier: aws.String(id),
		DBClusterSnapshotIdentifier: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(time.Minute); ; time.Sleep(100 * time.Millisecond) {
		d, err := c.rds.DescribeDBClusterSnapshots(ctx, &awsrds.DescribeDBClusterSnapshotsInput{DBClusterSnapshotIdentifier: aws.String(id)})
		if err != nil || time.Now().After(end) {
			t.Fatalf("cluster snapshot: %v %+v", err, d)
		}
		if aws.ToString(d.DBClusterSnapshots[0].Status) == "available" {
			break
		}
	}
	r, err := c.rds.RestoreDBClusterFromSnapshot(ctx, &awsrds.RestoreDBClusterFromSnapshotInput{DBClusterIdentifier: aws.String(id + "-r"),
		SnapshotIdentifier: aws.String(id), Engine: aws.String("aurora-postgresql")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.rds.EnableHttpEndpoint(ctx, &awsrds.EnableHttpEndpointInput{ResourceArn: r.DBCluster.DBClusterArn}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		out, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: r.DBCluster.DBClusterArn, SecretArn: &secret,
			Sql: aws.String("SELECT count(*) FROM items")})
		if err == nil {
			if n := value(out.Records[0][0]); n != int64(6) {
				t.Fatalf("restored %v rows", n)
			}
			break
		}
		if time.Now().After(end) {
			t.Fatalf("restored cluster: %v", err)
		}
	}
}

// TestMySQL covers the basics on aurora-mysql: insert ids, typed rows, a transaction, a batch.
func TestMySQL(t *testing.T) {
	c := withDocker(t)
	arn, secret := cluster(t, c, "aurora-mysql", fmt.Sprintf("data-my-%d", os.Getpid()), true)
	run := func(sql string, params ...types.SqlParameter) *rdsdata.ExecuteStatementOutput {
		t.Helper()
		out, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &sql,
			Parameters: params, IncludeResultMetadata: true})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return out
	}
	run("CREATE TABLE items (id int AUTO_INCREMENT PRIMARY KEY, name varchar(20), price decimal(10,2), at datetime, n bigint unsigned)")
	ins := run("INSERT INTO items (name, price, at, n) VALUES (:name, :price, :at, :n)", p("name", &types.FieldMemberStringValue{Value: "one"}),
		p("price", &types.FieldMemberDoubleValue{Value: 2.5}), p("at", &types.FieldMemberStringValue{Value: "2024-01-02 03:04:05"}, types.TypeHintTimestamp),
		p("n", &types.FieldMemberIsNull{Value: true}))
	if ins.NumberOfRecordsUpdated != 1 || len(ins.GeneratedFields) != 1 || value(ins.GeneratedFields[0]) != int64(1) {
		t.Fatalf("insert %+v", ins)
	}
	sel := run("SELECT id, name, price, at, n FROM items")
	if want := []any{int64(1), "one", "2.50", "2024-01-02 03:04:05", nil}; !reflect.DeepEqual(values(sel.Records[0]), want) {
		t.Fatalf("select %v, want %v", values(sel.Records[0]), want)
	}
	if m := sel.ColumnMetadata; aws.ToString(m[0].TypeName) != "INT" || m[0].Type != 4 || aws.ToString(m[1].TypeName) != "VARCHAR" || m[3].Type != 93 {
		t.Fatalf("metadata %+v", m)
	}
	tx, err := c.data.BeginTransaction(ctx, &rdsdata.BeginTransactionInput{ResourceArn: &arn, SecretArn: &secret})
	if err != nil {
		t.Fatal(err)
	}
	insert := "INSERT INTO items (name) VALUES ('rolled back')"
	if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: &insert, TransactionId: tx.TransactionId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.data.RollbackTransaction(ctx, &rdsdata.RollbackTransactionInput{ResourceArn: &arn, SecretArn: &secret, TransactionId: tx.TransactionId}); err != nil {
		t.Fatal(err)
	}
	b, err := c.data.BatchExecuteStatement(ctx, &rdsdata.BatchExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: aws.String("INSERT INTO items (name) VALUES (:name)"),
		ParameterSets: [][]types.SqlParameter{{p("name", &types.FieldMemberStringValue{Value: "a"})}, {p("name", &types.FieldMemberStringValue{Value: "b"})}}})
	if err != nil || len(b.UpdateResults) != 2 || value(b.UpdateResults[1].GeneratedFields[0]) != int64(4) { // 2 went to the rollback
		t.Fatalf("batch: %v %+v", err, b)
	}
	if n := value(run("SELECT count(*) FROM items").Records[0][0]); n != int64(3) {
		t.Fatalf("%v rows", n)
	}
	if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: aws.String("SELECT * FROM nope")}); code(err) != "DatabaseErrorException" {
		t.Fatalf("missing table: %v", err)
	}
}

// TestErrors needs no container: a cluster without the HTTP endpoint, one
// with nothing behind it, unknown clusters, secrets and transactions.
func TestErrors(t *testing.T) {
	dockertest.Start(t, 1, true) // an engine that is down: clusters are API-only
	c := serve(t)
	arn, secret := cluster(t, c, "aurora-postgresql", "plain", false)
	sql := aws.String("SELECT 1")
	_, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: sql})
	var br *types.BadRequestException
	if !errors.As(err, &br) || !strings.HasPrefix(aws.ToString(br.Message), "HttpEndpoint is not enabled for cluster plain.") {
		t.Fatalf("no HTTP endpoint: %v", err)
	}
	if _, err := c.rds.EnableHttpEndpoint(ctx, &awsrds.EnableHttpEndpointInput{ResourceArn: &arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: sql}); !errors.As(err, &br) {
		t.Fatalf("nothing runs: %v", err)
	}
	for name, in := range map[string]*rdsdata.ExecuteStatementInput{
		"cluster": {ResourceArn: aws.String("arn:aws:rds:us-east-1:000000000000:cluster:nope"), SecretArn: &secret, Sql: sql},
		"secret":  {ResourceArn: &arn, SecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:nope"), Sql: sql},
	} {
		if _, err := c.data.ExecuteStatement(ctx, in); code(err) != "NotFoundException" {
			t.Fatalf("unknown %s: %v", name, err)
		}
	}
	if _, err := c.data.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{ResourceArn: &arn, SecretArn: &secret, Sql: sql,
		TransactionId: aws.String("nope")}); code(err) != "TransactionNotFoundException" {
		t.Fatalf("unknown transaction: %v", err)
	}
}
