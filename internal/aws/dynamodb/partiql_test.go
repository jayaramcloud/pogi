package dynamodb_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

func run(t *testing.T, c *dynamodb.Client, stmt string, params ...types.AttributeValue) *dynamodb.ExecuteStatementOutput {
	t.Helper()
	out, err := c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(stmt), Parameters: params})
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	return out
}

func ids(out *dynamodb.ExecuteStatementOutput) string {
	var s []string
	for _, it := range out.Items {
		s = append(s, str(it["id"])+str(it["n"]))
	}
	return strings.Join(s, ",")
}

func TestPartiQLStatements(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("mmm", "id", "n")))
	run(t, c, `INSERT INTO mmm VALUE {'id': 'a', 'n': 1, 'tags': <<'x','y'>>, 'l': [1, 2], 'o': {'k': 'v'}, 'age': 5}`)
	run(t, c, `INSERT INTO "mmm" VALUE ?`, &types.AttributeValueMemberM{Value: av{"id": S("a"), "n": N("2"), "age": N("7")}})
	run(t, c, `insert into mmm value {'id': 'b', 'n': 1, 'age': 9}`)

	// Duplicate key.
	_, err := c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`INSERT INTO mmm VALUE {'id': 'a', 'n': 1}`)})
	if code, _ := apiErr(err); code != "DuplicateItemException" {
		t.Fatalf("duplicate: %v", err)
	}

	for _, tc := range []struct{ stmt, want string }{
		{`SELECT * FROM mmm WHERE id = 'a'`, "a1,a2"},
		{`SELECT * FROM mmm WHERE id = 'a' ORDER BY n DESC`, "a2,a1"},
		{`SELECT * FROM mmm WHERE id IN ['a', 'b'] AND n = 1`, "a1,b1"},
		{`SELECT * FROM mmm WHERE age > 5 AND age <= 9`, "a2,b1"},
		{`SELECT * FROM mmm WHERE age BETWEEN 5 AND 7`, "a1,a2"},
		{`SELECT * FROM mmm WHERE NOT (age = 5 OR age = 7)`, "b1"},
		{`SELECT * FROM mmm WHERE tags IS NOT MISSING`, "a1"},
		{`SELECT * FROM mmm WHERE tags IS MISSING AND id <> 'b'`, "a2"},
		{`SELECT * FROM mmm WHERE contains(tags, 'x')`, "a1"},
		{`SELECT * FROM mmm WHERE begins_with(id, 'b')`, "b1"},
		{`SELECT * FROM mmm WHERE attribute_type(age, 'N') AND size(l) = 2`, "a1"},
		{`SELECT * FROM mmm WHERE o.k = 'v' AND l[1] = 2`, "a1"},
		{`SELECT * FROM mmm WHERE "age" != 5 AND id = 'a'`, "a2"},
	} {
		if got := ids(run(t, c, tc.stmt)); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.stmt, got, tc.want)
		}
	}
	if it := run(t, c, `SELECT id, o.k, l[0] FROM mmm WHERE id = ? AND n = ?`, S("a"), N("1")).Items[0]; len(it) != 3 || str(it["id"]) != "a" {
		t.Fatalf("projection = %v", it)
	}

	// Paging: Limit counts items evaluated; the NextToken carries on.
	var seen []string
	var tok *string
	for i := 0; i < 5; i++ {
		out := must(c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`SELECT * FROM mmm`), Limit: aws.Int32(1), NextToken: tok}))
		seen = append(seen, ids(out))
		if tok = out.NextToken; tok == nil {
			break
		}
		if out.LastEvaluatedKey["id"] == nil {
			t.Fatalf("a page with a NextToken has no LastEvaluatedKey: %+v", out.LastEvaluatedKey)
		}
	}
	if got := strings.Join(seen, "|"); got != "a1|a2|b1|" {
		t.Fatalf("pages = %q", got)
	}
	// A token is for the statement that made it.
	out := must(c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`SELECT * FROM mmm`), Limit: aws.Int32(1)}))
	for _, in := range []*dynamodb.ExecuteStatementInput{
		{Statement: aws.String(`SELECT * FROM mmm WHERE id = 'a'`), NextToken: out.NextToken},
		{Statement: aws.String(`SELECT * FROM mmm`), NextToken: aws.String("bogus")},
	} {
		_, err := c.ExecuteStatement(ctx, in)
		wantValidation(t, err, "NextToken")
	}

	// UPDATE with RETURNING and extra conditions.
	got := run(t, c, `UPDATE mmm SET age = age + 1 SET tags = set_add(tags, <<'z'>>) REMOVE o.k WHERE id = 'a' AND n = 1 RETURNING ALL NEW *`)
	if len(got.Items) != 1 || str(got.Items[0]["age"]) != "6" || len(got.Items[0]["tags"].(*types.AttributeValueMemberSS).Value) != 3 {
		t.Fatalf("update = %v", got.Items)
	}
	got = run(t, c, `UPDATE mmm SET l = list_append(l, [3]) SET age = 1 WHERE id = 'a' AND n = 1 AND age = 6 RETURNING MODIFIED OLD *`)
	if len(got.Items[0]) != 2 || str(got.Items[0]["age"]) != "6" {
		t.Fatalf("modified old = %v", got.Items)
	}
	_, err = c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`UPDATE mmm SET age = 2 WHERE id = 'a' AND n = 1 AND age = 99`),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld})
	var cf *types.ConditionalCheckFailedException
	if !asErr(err, &cf) || str(cf.Item["age"]) != "1" {
		t.Fatalf("condition: %v", err)
	}
	// UPDATE does not create an item.
	_, err = c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`UPDATE mmm SET age = 2 WHERE id = 'nope' AND n = 1`)})
	if !asErr(err, &cf) {
		t.Fatalf("update of a missing item: %v", err)
	}

	// DELETE.
	if got := run(t, c, `DELETE FROM mmm WHERE id = 'b' AND n = 1 RETURNING ALL OLD *`); ids(got) != "b1" {
		t.Fatalf("delete = %v", got.Items)
	}
	run(t, c, `DELETE FROM mmm WHERE id = 'b' AND n = 1`) // already gone: nothing happens
	if got := ids(run(t, c, `SELECT * FROM mmm`)); got != "a1,a2" {
		t.Fatalf("after delete: %s", got)
	}
}

func asErr[E error](err error, target *E) bool { return errors.As(err, target) }

func TestPartiQLErrors(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("mmm", "id", "n")))
	long := "SELECT * FROM mmm WHERE id = '" + strings.Repeat("x", 8200) + "'"
	for _, tc := range []struct {
		stmt   string
		params []types.AttributeValue
		want   string
	}{
		{"", nil, "greater than or equal to 1"},
		{long, nil, "less than or equal to 8192"},
		{"SELECT FROM mmm", nil, "wasn't well formed"},
		{"SELECT * FROM mmm WHERE id = 'a", nil, "unterminated"},
		{"SELECT * FROM mmm WHERE id = 'a' garbage", nil, "wasn't well formed"},
		{"SELECT * FROM mmm WHERE id = ?", nil, "Number of parameters"},
		{"SELECT * FROM mmm WHERE id = 'a'", []types.AttributeValue{S("x")}, "Number of parameters"},
		{"DROP TABLE m", nil, "wasn't well formed"},
		{"UPDATE mmm SET WHERE id = 'a'", nil, "wasn't well formed"},
		{"UPDATE mmm SET n = 1 WHERE id = 'a' AND n = 1", nil, "part of the key"},
		{"UPDATE mmm SET a = 1, a = 2 WHERE id = 'a' AND n = 1", nil, "conflict"},
		{"UPDATE mmm SET a = 1 WHERE id = 'a'", nil, "mandatory equality on all key attributes"},
		{"UPDATE mmm SET a = 1 WHERE id = 'a' OR n = 1", nil, "mandatory equality on all key attributes"},
		{"DELETE FROM mmm WHERE n = 1", nil, "mandatory equality on all key attributes"},
		{"DELETE FROM mmm WHERE id = 'a' AND n = 1 RETURNING ALL NEW *", nil, "not supported"},
		{"INSERT INTO mmm VALUE {'id': 'a'}", nil, "Missing the key n"},
		{"INSERT INTO mmm VALUE {'id': 'a', 'n': 1, 'x': <<>>}", nil, "set"},
		{"INSERT INTO mmm VALUE 5", nil, "VALUE must be a map"},
		{"SELECT * FROM mmm ORDER BY n", nil, "WHERE clause"},
		{"SELECT * FROM mmm WHERE n = 1 ORDER BY n", nil, "hash key condition"},
		{"SELECT * FROM mmm WHERE id = 'a' ORDER BY id", nil, "sort key"},
		{"SELECT * FROM mmm.nope", nil, "does not have the specified index"},
		{"SELECT * FROM mmm WHERE id = 'a' AND n = 1 AND begins_with(id)", nil, "wasn't well formed"},
		{"EXISTS(SELECT * FROM mmm WHERE id = 'a' AND n = 1)", nil, "ExecuteTransaction"},
	} {
		_, err := c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(tc.stmt), Parameters: tc.params})
		wantValidation(t, err, tc.want)
	}
	if _, err := c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String("SELECT * FROM nope")}); !asErr(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("unknown table: %v", err)
	}
	_, err := c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String("SELECT * FROM mmm"), Limit: aws.Int32(0)})
	wantValidation(t, err, "limit")
}

func TestPartiQLIndexes(t *testing.T) {
	c, _, _ := setup(t)
	in := def("ggg", "id", "")
	in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String("city"), AttributeType: types.ScalarAttributeTypeS})
	in.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{{IndexName: aws.String("by_city"), Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("city"), KeyType: types.KeyTypeHash}}}}
	must(c.CreateTable(ctx, in))
	run(t, c, `INSERT INTO ggg VALUE {'id': '1', 'city': 'x', 'v': 1}`)
	run(t, c, `INSERT INTO ggg VALUE {'id': '2', 'city': 'y', 'v': 2}`)
	got := run(t, c, `SELECT * FROM ggg."by_city" WHERE city = 'x'`)
	if len(got.Items) != 1 || len(got.Items[0]) != 2 {
		t.Fatalf("index items = %v", got.Items)
	}
	_, err := c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`SELECT * FROM ggg."by_city"`), ConsistentRead: aws.Bool(true)})
	wantValidation(t, err, "Consistent reads")
	_, err = c.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{Statement: aws.String(`UPDATE ggg."by_city" SET v = 1 WHERE id = '1'`)})
	wantValidation(t, err, "wasn't well formed")
}

func TestPartiQLBatch(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("mmm", "id", "n")))
	st := func(s string, p ...types.AttributeValue) types.BatchStatementRequest {
		return types.BatchStatementRequest{Statement: aws.String(s), Parameters: p}
	}
	out := must(c.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{Statements: []types.BatchStatementRequest{
		st(`INSERT INTO mmm VALUE {'id': 'a', 'n': 1}`),
		st(`INSERT INTO mmm VALUE {'id': 'a', 'n': 1}`),
		st(`INSERT INTO nope VALUE {'id': 'a', 'n': 1}`),
		st(`UPDATE mmm SET v = 1 WHERE id = 'a' AND n = 1 AND v = 5`),
		st(`SELECT nonsense`),
		st(`DELETE FROM mmm WHERE id = ? AND n = 2`, S("a")),
	}}))
	var codes []string
	for _, r := range out.Responses {
		code := ""
		if r.Error != nil {
			code = string(r.Error.Code)
		}
		codes = append(codes, code)
	}
	if got := strings.Join(codes, ","); got != ",DuplicateItem,ResourceNotFound,ConditionalCheckFailed,ValidationError," {
		t.Fatalf("codes = %s", got)
	}
	reads := must(c.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{Statements: []types.BatchStatementRequest{
		st(`SELECT * FROM mmm WHERE id = 'a' AND n = 1`), st(`SELECT * FROM mmm WHERE id = 'a' AND n = 9`), st(`SELECT * FROM mmm WHERE id = 'a'`)}}))
	if r := reads.Responses; r[0].Item == nil || r[1].Item != nil || r[1].Error != nil || r[2].Error == nil {
		t.Fatalf("reads = %+v", r)
	}
	for _, bad := range [][]types.BatchStatementRequest{
		{}, make([]types.BatchStatementRequest, 26),
		{st(`SELECT * FROM mmm WHERE id = 'a' AND n = 1`), st(`DELETE FROM mmm WHERE id = 'a' AND n = 1`)},
	} {
		if len(bad) == 26 {
			for i := range bad {
				bad[i] = st(`SELECT * FROM mmm WHERE id = 'a' AND n = 1`)
			}
		}
		_, err := c.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{Statements: bad})
		wantValidation(t, err, "")
	}
}

func TestPartiQLTransaction(t *testing.T) {
	c, _, cfg := setup(t)
	in := def("mmm", "id", "n")
	in.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages}
	stream := must(c.CreateTable(ctx, in)).TableDescription.LatestStreamArn
	st := func(s string) types.ParameterizedStatement {
		return types.ParameterizedStatement{Statement: aws.String(s)}
	}
	run(t, c, `INSERT INTO mmm VALUE {'id': 'a', 'n': 1, 'v': 1}`)
	run(t, c, `INSERT INTO mmm VALUE {'id': 'c', 'n': 1, 'v': 1}`)

	// One failure cancels everything, with a reason for each statement.
	_, err := c.ExecuteTransaction(ctx, &dynamodb.ExecuteTransactionInput{TransactStatements: []types.ParameterizedStatement{
		st(`INSERT INTO mmm VALUE {'id': 'b', 'n': 1}`),
		st(`INSERT INTO mmm VALUE {'id': 'a', 'n': 1}`),
		st(`EXISTS(SELECT * FROM mmm WHERE id = 'zz' AND n = 1)`),
	}})
	var tc *types.TransactionCanceledException
	if !asErr(err, &tc) || len(tc.CancellationReasons) != 3 || *tc.CancellationReasons[0].Code != "None" ||
		*tc.CancellationReasons[1].Code != "DuplicateItem" || *tc.CancellationReasons[2].Code != "ConditionalCheckFailed" {
		t.Fatalf("cancel: %v", err)
	}
	if n := len(run(t, c, `SELECT * FROM mmm WHERE id = 'b'`).Items); n != 0 {
		t.Fatalf("a cancelled transaction wrote %d items", n)
	}

	token := aws.String("tok-1")
	good := &dynamodb.ExecuteTransactionInput{ClientRequestToken: token, ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal, TransactStatements: []types.ParameterizedStatement{
		st(`INSERT INTO mmm VALUE {'id': 'b', 'n': 1}`),
		st(`UPDATE mmm SET v = v + 1 WHERE id = 'a' AND n = 1`),
		st(`EXISTS(SELECT * FROM mmm WHERE id = 'c' AND n = 1 AND v = 1)`),
	}}
	if out := must(c.ExecuteTransaction(ctx, good)); len(out.ConsumedCapacity) != 1 {
		t.Fatalf("capacity = %v", out.ConsumedCapacity)
	}
	must(c.ExecuteTransaction(ctx, good)) // a retry changes nothing
	if got := str(run(t, c, `SELECT * FROM mmm WHERE id = 'a'`).Items[0]["v"]); got != "2" {
		t.Fatalf("v = %s after a retry", got)
	}
	other := *good
	other.TransactStatements = good.TransactStatements[:1]
	if _, err := c.ExecuteTransaction(ctx, &other); err == nil || !strings.Contains(err.Error(), "IdempotentParameterMismatch") {
		t.Fatalf("same token, other statements: %v", err)
	}

	reads := must(c.ExecuteTransaction(ctx, &dynamodb.ExecuteTransactionInput{TransactStatements: []types.ParameterizedStatement{
		st(`SELECT * FROM mmm WHERE id = 'a' AND n = 1`), st(`SELECT * FROM mmm WHERE id = 'x' AND n = 1`)}}))
	if len(reads.Responses) != 2 || reads.Responses[0].Item == nil || reads.Responses[1].Item != nil {
		t.Fatalf("reads = %+v", reads.Responses)
	}
	for _, bad := range [][]types.ParameterizedStatement{
		{}, {st(`SELECT * FROM mmm WHERE id = 'a' AND n = 1`), st(`DELETE FROM mmm WHERE id = 'b' AND n = 1`)},
		{st(`DELETE FROM mmm WHERE id = 'b' AND n = 1`), st(`DELETE FROM mmm WHERE id = 'b' AND n = 1`)}, {st(`nonsense`)},
	} {
		_, err := c.ExecuteTransaction(ctx, &dynamodb.ExecuteTransactionInput{TransactStatements: bad})
		wantValidation(t, err, "")
	}

	// The writes reached the stream.
	ss := dynamodbstreams.NewFromConfig(cfg)
	desc := must(ss.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: stream}))
	it := must(ss.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{StreamArn: stream, ShardId: desc.StreamDescription.Shards[0].ShardId, ShardIteratorType: "TRIM_HORIZON"}))
	recs := must(ss.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it.ShardIterator})).Records
	var names []string
	for _, r := range recs {
		names = append(names, string(r.EventName))
	}
	if got := strings.Join(names, ","); got != "INSERT,INSERT,INSERT,MODIFY" {
		t.Fatalf("stream = %s", got)
	}
}
