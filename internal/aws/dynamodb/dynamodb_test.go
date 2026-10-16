package dynamodb_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/smithy-go"
	ddb "github.com/nrndev/pogi/internal/aws/dynamodb"
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

func setup(t *testing.T) (*dynamodb.Client, *ddb.Service, aws.Config) {
	var svc *ddb.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = ddb.New(h); return svc })
	cfg := awstest.Config(t, url)
	return dynamodb.NewFromConfig(cfg), svc, cfg
}

type av = map[string]types.AttributeValue

func S(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }
func N(s string) types.AttributeValue { return &types.AttributeValueMemberN{Value: s} }

func str(v types.AttributeValue) string {
	switch v := v.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	}
	return ""
}

// def is an on-demand table with a string hash key and an optional number range key.
func def(name, hash, rng string) *dynamodb.CreateTableInput {
	in := &dynamodb.CreateTableInput{TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String(hash), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String(hash), KeyType: types.KeyTypeHash}}}
	if rng != "" {
		in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String(rng), AttributeType: types.ScalarAttributeTypeN})
		in.KeySchema = append(in.KeySchema, types.KeySchemaElement{AttributeName: aws.String(rng), KeyType: types.KeyTypeRange})
	}
	return in
}

// apiErr returns the error code and message an SDK sees.
func apiErr(err error) (code, msg string) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode(), ae.ErrorMessage()
	}
	if err != nil {
		return "", err.Error()
	}
	return "", ""
}

func wantValidation(t *testing.T, err error, substr string) {
	t.Helper()
	if code, msg := apiErr(err); code != "ValidationException" || !strings.Contains(msg, substr) {
		t.Fatalf("want ValidationException containing %q, got %s: %s", substr, code, msg)
	}
}

func TestTables(t *testing.T) {
	c, _, _ := setup(t)
	in := def("users", "id", "")
	in.Tags = []types.Tag{{Key: aws.String("team"), Value: aws.String("core")}}
	created := must(c.CreateTable(ctx, in)).TableDescription
	if created.TableStatus != types.TableStatusActive || *created.TableArn != "arn:aws:dynamodb:us-east-1:000000000000:table/users" ||
		created.BillingModeSummary.BillingMode != types.BillingModePayPerRequest || created.TableId == nil || created.CreationDateTime == nil {
		t.Fatalf("created = %+v", created)
	}
	if _, err := c.CreateTable(ctx, def("users", "id", "")); !errors.As(err, new(*types.ResourceInUseException)) {
		t.Fatalf("duplicate table: %v", err)
	}
	if _, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("nope")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("missing table: %v", err)
	}
	bad := def("bad", "id", "")
	bad.KeySchema[0].AttributeName = aws.String("other")
	_, err := c.CreateTable(ctx, bad)
	wantValidation(t, err, "AttributeDefinitions")

	prov := def("orders", "id", "n")
	prov.BillingMode = ""
	_, err = c.CreateTable(ctx, prov)
	wantValidation(t, err, "ReadCapacityUnits and WriteCapacityUnits must both be specified")
	prov.ProvisionedThroughput = &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(7)}
	if d := must(c.CreateTable(ctx, prov)).TableDescription; d.BillingModeSummary != nil || *d.ProvisionedThroughput.WriteCapacityUnits != 7 {
		t.Fatalf("provisioned table = %+v", d)
	}
	var names []string
	for p := dynamodb.NewListTablesPaginator(c, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}); p.HasMorePages(); {
		names = append(names, must(p.NextPage(ctx)).TableNames...)
	}
	if strings.Join(names, ",") != "orders,users" {
		t.Fatalf("tables = %v", names)
	}

	// What Terraform reads after every apply.
	users := aws.String("users")
	ttl := must(c.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: users})).TimeToLiveDescription
	if ttl.TimeToLiveStatus != types.TimeToLiveStatusDisabled {
		t.Fatalf("ttl = %+v", ttl)
	}
	must(c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: users,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("exp"), Enabled: aws.Bool(true)}}))
	if ttl := must(c.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: users})).TimeToLiveDescription; ttl.TimeToLiveStatus != types.TimeToLiveStatusEnabled || *ttl.AttributeName != "exp" {
		t.Fatalf("ttl = %+v", ttl)
	}
	cb := must(c.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: users})).ContinuousBackupsDescription
	if cb.ContinuousBackupsStatus != types.ContinuousBackupsStatusEnabled || cb.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != types.PointInTimeRecoveryStatusDisabled {
		t.Fatalf("backups = %+v", cb)
	}
	cb = must(c.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{TableName: users,
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)}})).ContinuousBackupsDescription
	if cb.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != types.PointInTimeRecoveryStatusEnabled {
		t.Fatalf("pitr = %+v", cb.PointInTimeRecoveryDescription)
	}
	if _, err := c.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("nope")}); !errors.As(err, new(*types.TableNotFoundException)) {
		t.Fatalf("backups of a missing table: %v", err)
	}
	_, err = c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: users,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("exp"), Enabled: aws.Bool(true)}})
	wantValidation(t, err, "TimeToLive is already enabled")
	if k := must(c.DescribeKinesisStreamingDestination(ctx, &dynamodb.DescribeKinesisStreamingDestinationInput{TableName: users})); *k.TableName != "users" || len(k.KinesisDataStreamDestinations) != 0 {
		t.Fatalf("kinesis = %+v", k)
	}
	if l := must(c.DescribeLimits(ctx, &dynamodb.DescribeLimitsInput{})); *l.TableMaxReadCapacityUnits == 0 {
		t.Fatalf("limits = %+v", l)
	}
	if e := must(c.DescribeEndpoints(ctx, &dynamodb.DescribeEndpointsInput{})); len(e.Endpoints) != 1 {
		t.Fatalf("endpoints = %+v", e)
	}
	must(c.TagResource(ctx, &dynamodb.TagResourceInput{ResourceArn: created.TableArn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(c.UntagResource(ctx, &dynamodb.UntagResourceInput{ResourceArn: created.TableArn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: created.TableArn})).Tags; len(tags) != 1 || *tags[0].Key != "env" {
		t.Fatalf("tags = %+v", tags)
	}

	upd := must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: users,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("email"), AttributeType: types.ScalarAttributeTypeS}},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Create: &types.CreateGlobalSecondaryIndexAction{IndexName: aws.String("byEmail"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("email"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly}}}},
		StreamSpecification: &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeKeysOnly},
	})).TableDescription
	if g := upd.GlobalSecondaryIndexes; len(g) != 1 || g[0].IndexStatus != types.IndexStatusActive || upd.LatestStreamArn == nil || len(upd.AttributeDefinitions) != 2 {
		t.Fatalf("updated = %+v", upd)
	}
	upd = must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: users,
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("byEmail")}}}})).TableDescription
	if len(upd.GlobalSecondaryIndexes) != 0 || len(upd.AttributeDefinitions) != 1 {
		t.Fatalf("after index delete = %+v", upd)
	}

	must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: users, DeletionProtectionEnabled: aws.Bool(true)}))
	_, err = c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: users})
	wantValidation(t, err, "protected against manual deletion")
	must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: users, DeletionProtectionEnabled: aws.Bool(false)}))
	if del := must(c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: users})).TableDescription; del.TableStatus != types.TableStatusDeleting {
		t.Fatalf("deleted = %+v", del)
	}
	if _, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: users}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("deleted table: %v", err)
	}
}

func TestItems(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("users", "id", "")))
	tbl, key := aws.String("users"), av{"id": S("u1")}
	item := av{"id": S("u1"), "age": N("30"), "nick": S("al")}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: item, ConditionExpression: aws.String("attribute_not_exists(id)")}))

	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: item, ConditionExpression: aws.String("attribute_not_exists(id)"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld})
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) || str(ccf.Item["nick"]) != "al" {
		t.Fatalf("create-only put: %v", err)
	}
	old := must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"id": S("u1"), "age": N("031.0")}, ReturnValues: types.ReturnValueAllOld}))
	if str(old.Attributes["nick"]) != "al" {
		t.Fatalf("ALL_OLD = %v", old.Attributes)
	}
	got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: key, ConsistentRead: aws.Bool(true)})).Item
	if str(got["age"]) != "31" || got["nick"] != nil {
		t.Fatalf("get = %v (numbers come back canonical)", got)
	}
	nums := av{"id": S("nums"), "a": N("1E2"), "b": N("-0.50"), "c": N("0.000"), "d": N("1e-130"), "e": N("99999999999999999999999999999999999999")}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: nums}))
	got = must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: av{"id": S("nums")}})).Item
	if str(got["a"]) != "100" || str(got["b"]) != "-0.5" || str(got["c"]) != "0" || str(got["d"]) != "0."+strings.Repeat("0", 129)+"1" ||
		str(got["e"]) != "99999999999999999999999999999999999999" {
		t.Fatalf("canonical numbers = %v", got)
	}

	upd := func(expr string, rv types.ReturnValue, vals av) av {
		t.Helper()
		return must(c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: key, UpdateExpression: aws.String(expr),
			ExpressionAttributeValues: vals, ReturnValues: rv})).Attributes
	}
	if a := upd("SET age = age + :one", types.ReturnValueUpdatedNew, av{":one": N("1")}); len(a) != 1 || str(a["age"]) != "32" {
		t.Fatalf("UPDATED_NEW = %v", a)
	}
	if a := upd("SET nick = :n", types.ReturnValueUpdatedOld, av{":n": S("bo")}); len(a) != 0 {
		t.Fatalf("UPDATED_OLD of a new attribute = %v", a)
	}
	if a := upd("SET nick = :n", types.ReturnValueAllOld, av{":n": S("cy")}); str(a["nick"]) != "bo" || str(a["age"]) != "32" {
		t.Fatalf("ALL_OLD = %v", a)
	}
	if a := upd("REMOVE nick", types.ReturnValueAllNew, nil); a["nick"] != nil || str(a["id"]) != "u1" || str(a["age"]) != "32" {
		t.Fatalf("ALL_NEW = %v", a)
	}
	if a := upd("SET age = :a", types.ReturnValueNone, av{":a": N("40")}); len(a) != 0 {
		t.Fatalf("NONE = %v", a)
	}
	_, err = c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: key, UpdateExpression: aws.String("SET age = :a"),
		ConditionExpression: aws.String("age < :max AND #n = :n"), ExpressionAttributeNames: map[string]string{"#n": "nick"},
		ExpressionAttributeValues: av{":a": N("1"), ":max": N("100"), ":n": S("cy")}})
	if !errors.As(err, &ccf) || ccf.Item != nil {
		t.Fatalf("condition on a removed attribute: %v", err)
	}
	_, err = c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: tbl, Key: key, ConditionExpression: aws.String("age = :a"), ExpressionAttributeValues: av{":a": N("41")}})
	if !errors.As(err, &ccf) {
		t.Fatalf("conditional delete: %v", err)
	}
	del := must(c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: tbl, Key: key, ReturnValues: types.ReturnValueAllOld,
		ConditionExpression: aws.String("age = :a"), ExpressionAttributeValues: av{":a": N("40")}}))
	if str(del.Attributes["age"]) != "40" {
		t.Fatalf("deleted = %v", del.Attributes)
	}
	if got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: key})); got.Item != nil {
		t.Fatalf("item survived delete: %v", got.Item)
	}

	// The validation errors SDK users run into.
	for _, tc := range []struct {
		in   *dynamodb.PutItemInput
		want string
	}{
		{&dynamodb.PutItemInput{Item: av{"id": N("1")}}, "Type mismatch for key id"},
		{&dynamodb.PutItemInput{Item: av{"x": S("1")}}, "Missing the key id"},
		{&dynamodb.PutItemInput{Item: av{"id": S("")}}, "cannot contain an empty string value"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "n": N("12abc")}}, "cannot be converted into a number"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "n": N("1e999999999")}}, "cannot be converted into a number"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "n": N("1e126")}}, "Number overflow"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "n": N("1e-131")}}, "Number underflow"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "n": N("1234567890123456789012345678901234567890")}}, "38 significant digits"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "big": S(strings.Repeat("x", 401<<10))}}, "Item size has exceeded the maximum allowed size"},
		{&dynamodb.PutItemInput{Item: av{"id": S("1"), "s": &types.AttributeValueMemberSS{Value: []string{"a", "a"}}}}, "contains duplicates"},
		{&dynamodb.PutItemInput{Item: item, ConditionExpression: aws.String("attribute_exists(id)"), ExpressionAttributeValues: av{":x": S("1")}},
			"Value provided in ExpressionAttributeValues unused in expressions: keys: {:x}"},
		{&dynamodb.PutItemInput{Item: item, ConditionExpression: aws.String("age = :nope")}, "attribute value: :nope"},
		{&dynamodb.PutItemInput{Item: item, ConditionExpression: aws.String("#a = :a"), ExpressionAttributeValues: av{":a": S("1")}}, "attribute name: #a"},
		{&dynamodb.PutItemInput{Item: item, ConditionExpression: aws.String("attribute_exists(name)")}, "reserved keyword: name"},
		{&dynamodb.PutItemInput{Item: item, ConditionExpression: aws.String("age = = :a"), ExpressionAttributeValues: av{":a": N("1")}}, "Syntax error"},
		{&dynamodb.PutItemInput{Item: item, ConditionExpression: aws.String("age BETWEEN :hi AND :lo"), ExpressionAttributeValues: av{":lo": N("1"), ":hi": N("9")}}, "BETWEEN"},
		{&dynamodb.PutItemInput{Item: item, ReturnValues: types.ReturnValueAllNew}, "Return values set to invalid value"},
	} {
		tc.in.TableName = tbl
		_, err := c.PutItem(ctx, tc.in)
		wantValidation(t, err, tc.want)
	}
	_, err = c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: av{"id": S("u1"), "extra": S("x")}})
	wantValidation(t, err, "The provided key element does not match the schema")
	if _, err := c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("nope"), Key: key}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("missing table: %v", err)
	}
}

func TestUpdateExpressions(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("docs", "id", "")))
	tbl, key := aws.String("docs"), av{"id": S("d1")}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{
		"id":     S("d1"),
		"count":  N("1"),
		"tags":   &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"colors": &types.AttributeValueMemberSS{Value: []string{"red", "blue"}},
		"list":   &types.AttributeValueMemberL{Value: []types.AttributeValue{N("1"), N("2"), N("3")}},
		"profile": &types.AttributeValueMemberM{Value: av{"name": S("x"),
			"address": &types.AttributeValueMemberM{Value: av{"city": S("Paris")}}}},
		"a": N("1"), "b": N("2"),
	}}))
	out := must(c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: key, ReturnValues: types.ReturnValueAllNew,
		UpdateExpression: aws.String("SET #c = #c + :one, profile.address.city = :city, profile.address.zip = :zip, a = b, b = a, " +
			"later = list_append(if_not_exists(later, :empty), :items), visits = if_not_exists(visits, :zero) - :one, #l[9] = :one " +
			"REMOVE #l[0], profile.#n ADD tags :more, points :five DELETE colors :less"),
		ExpressionAttributeNames: map[string]string{"#c": "count", "#l": "list", "#n": "name"},
		ExpressionAttributeValues: av{":one": N("1"), ":city": S("Lyon"), ":zip": S("69001"), ":zero": N("0"), ":five": N("5"),
			":empty": &types.AttributeValueMemberL{Value: []types.AttributeValue{}}, ":items": &types.AttributeValueMemberL{Value: []types.AttributeValue{S("x")}},
			":more": &types.AttributeValueMemberSS{Value: []string{"c"}}, ":less": &types.AttributeValueMemberSS{Value: []string{"red"}}},
	}))
	var doc map[string]any
	if err := attributevalue.UnmarshalMap(out.Attributes, &doc); err != nil {
		t.Fatal(err)
	}
	tags := doc["tags"].([]string)
	slices.Sort(tags)
	want := map[string]any{"id": "d1", "count": 2.0, "tags": []string{"a", "b", "c"}, "colors": []string{"blue"}, "list": []any{2.0, 3.0, 1.0},
		"profile": map[string]any{"address": map[string]any{"city": "Lyon", "zip": "69001"}},
		"a":       2.0, "b": 1.0, "later": []any{"x"}, "visits": -1.0, "points": 5.0}
	if !reflect.DeepEqual(doc, want) {
		t.Fatalf("updated doc\n got %v\nwant %v", doc, want)
	}

	for expr, msg := range map[string]string{
		"SET #c = #c + :s":            "incorrect data type",
		"SET a = :s, a.b = :s":        "Two document paths overlap",
		"SET absent.child = :s":       "document path provided in the update expression is invalid",
		"SET id = :s":                 "Cannot update attribute id",
		"SET x = nope + :one":         "does not exist in the item",
		"ADD tags :one":               "incorrect data type",
		"SET x = :s SET y = :s":       "can only be used once",
		"SET x = :s REMOVE #c, x":     "Two document paths conflict",
		"SET x = list_append(:s, :s)": "list_append",
	} {
		names := map[string]string{"#c": "count"}
		if !strings.Contains(expr, "#c") {
			names = nil
		}
		vals := av{}
		for _, k := range []string{":s", ":one"} {
			if strings.Contains(expr, k) {
				vals[k] = map[string]types.AttributeValue{":s": S("str"), ":one": N("1")}[k]
			}
		}
		_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: key, UpdateExpression: aws.String(expr),
			ExpressionAttributeNames: names, ExpressionAttributeValues: vals})
		wantValidation(t, err, msg)
	}

	// Updating a missing item creates it.
	must(c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: av{"id": S("d2")}, UpdateExpression: aws.String("ADD hits :one"),
		ExpressionAttributeValues: av{":one": N("1")}}))
	if got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: av{"id": S("d2")}})).Item; str(got["hits"]) != "1" {
		t.Fatalf("upsert = %v", got)
	}
	// Projections pick nested paths and list elements.
	got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: key, ProjectionExpression: aws.String("profile.address.city, #l[1], nope"),
		ExpressionAttributeNames: map[string]string{"#l": "list"}})).Item
	var picked map[string]any
	if err := attributevalue.UnmarshalMap(got, &picked); err != nil || !reflect.DeepEqual(picked, map[string]any{
		"profile": map[string]any{"address": map[string]any{"city": "Lyon"}}, "list": []any{3.0}}) {
		t.Fatalf("projection = %v %v", picked, err)
	}
}

func TestQueryScan(t *testing.T) {
	c, _, _ := setup(t)
	in := def("events", "device", "ts")
	in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String("kind"), AttributeType: types.ScalarAttributeTypeS})
	in.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{{IndexName: aws.String("byKind"),
		KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("kind"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("ts"), KeyType: types.KeyTypeRange}},
		Projection: &types.Projection{ProjectionType: types.ProjectionTypeInclude, NonKeyAttributes: []string{"level"}}}}
	must(c.CreateTable(ctx, in))
	tbl := aws.String("events")
	for i := 1; i <= 10; i++ {
		kind := map[bool]string{true: "even", false: "odd"}[i%2 == 0]
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"device": S("d1"), "ts": N(strconv.Itoa(i)),
			"level": N(strconv.Itoa(i * 10)), "kind": S(kind), "payload": S("p")}}))
	}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"device": S("d2"), "ts": N("5")}})) // not in the index
	lv := map[string]string{"#lv": "level"}                                                                 // LEVEL is a reserved word

	q := must(c.Query(ctx, &dynamodb.QueryInput{TableName: tbl, KeyConditionExpression: aws.String("device = :d AND ts BETWEEN :lo AND :hi"),
		FilterExpression: aws.String("#lv > :min"), ExpressionAttributeNames: lv,
		ExpressionAttributeValues: av{":d": S("d1"), ":lo": N("3"), ":hi": N("8"), ":min": N("50")}}))
	if q.Count != 3 || q.ScannedCount != 6 || str(q.Items[0]["ts"]) != "6" {
		t.Fatalf("query = %d/%d %v", q.Count, q.ScannedCount, q.Items)
	}
	var seen []string
	pages := 0
	for p := dynamodb.NewQueryPaginator(c, &dynamodb.QueryInput{TableName: tbl, KeyConditionExpression: aws.String("device = :d"),
		ExpressionAttributeValues: av{":d": S("d1")}, Limit: aws.Int32(3), ScanIndexForward: aws.Bool(false)}); p.HasMorePages(); pages++ {
		for _, it := range must(p.NextPage(ctx)).Items {
			seen = append(seen, str(it["ts"]))
		}
	}
	if strings.Join(seen, ",") != "10,9,8,7,6,5,4,3,2,1" || pages != 4 {
		t.Fatalf("descending pages = %d %v", pages, seen)
	}
	g := must(c.Query(ctx, &dynamodb.QueryInput{TableName: tbl, IndexName: aws.String("byKind"), KeyConditionExpression: aws.String("kind = :k AND ts > :t"),
		ExpressionAttributeValues: av{":k": S("even"), ":t": N("4")}}))
	if g.Count != 3 || str(g.Items[0]["ts"]) != "6" || g.Items[0]["payload"] != nil || str(g.Items[0]["level"]) != "60" || str(g.Items[0]["device"]) != "d1" {
		t.Fatalf("index query = %v", g.Items)
	}
	_, err := c.Query(ctx, &dynamodb.QueryInput{TableName: tbl, IndexName: aws.String("byKind"), ConsistentRead: aws.Bool(true),
		KeyConditionExpression: aws.String("kind = :k"), ExpressionAttributeValues: av{":k": S("even")}})
	wantValidation(t, err, "Consistent reads are not supported on global secondary indexes")
	_, err = c.Query(ctx, &dynamodb.QueryInput{TableName: tbl, KeyConditionExpression: aws.String("ts = :t"), ExpressionAttributeValues: av{":t": N("1")}})
	wantValidation(t, err, "Query condition missed key schema element: device")
	_, err = c.Query(ctx, &dynamodb.QueryInput{TableName: tbl, KeyConditionExpression: aws.String("device = :d OR ts = :t"),
		ExpressionAttributeValues: av{":d": S("d1"), ":t": N("1")}})
	wantValidation(t, err, "Query key condition not supported")

	s := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, FilterExpression: aws.String("attribute_exists(kind) AND #lv >= :x"),
		ExpressionAttributeNames: lv, ExpressionAttributeValues: av{":x": N("80")}}))
	if s.Count != 3 || s.ScannedCount != 11 {
		t.Fatalf("scan = %d/%d", s.Count, s.ScannedCount)
	}
	if n := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, Select: types.SelectCount})); n.Count != 11 || n.Items != nil {
		t.Fatalf("count = %d %v", n.Count, n.Items)
	}
	total := int32(0)
	for seg := range int32(3) {
		total += must(c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, Segment: aws.Int32(seg), TotalSegments: aws.Int32(3)})).Count
	}
	if total != 11 {
		t.Fatalf("segments saw %d items", total)
	}
	if n := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, IndexName: aws.String("byKind")})).Count; n != 10 {
		t.Fatalf("index scan = %d", n)
	}
	f := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, ExpressionAttributeNames: lv,
		FilterExpression:          aws.String("NOT contains(kind, :o) AND #lv IN (:a, :b, :c) AND attribute_type(payload, :s) AND size(payload) = :one"),
		ExpressionAttributeValues: av{":o": S("o"), ":a": N("20"), ":b": N("40"), ":c": N("999"), ":s": S("S"), ":one": N("1")}}))
	if f.Count != 2 {
		t.Fatalf("functions filter = %d", f.Count)
	}

	notes := def("notes", "user", "")
	notes.AttributeDefinitions = append(notes.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS})
	notes.KeySchema = append(notes.KeySchema, types.KeySchemaElement{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange})
	must(c.CreateTable(ctx, notes))
	for _, sk := range []string{"task#1", "note#2", "note#1", "notebook"} {
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("notes"), Item: av{"user": S("u"), "sk": S(sk)}}))
	}
	n := must(c.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("notes"), KeyConditionExpression: aws.String("#u = :u AND begins_with(sk, :p)"),
		ExpressionAttributeNames: map[string]string{"#u": "user"}, ExpressionAttributeValues: av{":u": S("u"), ":p": S("note#")}}))
	if n.Count != 2 || str(n.Items[0]["sk"]) != "note#1" || str(n.Items[1]["sk"]) != "note#2" {
		t.Fatalf("begins_with query = %v", n.Items)
	}
}

// Lambda event source mappings poll streams in-process through hub.Call.
func TestHubCall(t *testing.T) {
	h, _ := core.ServeTest(t, func(h *core.Hub) core.Service { return ddb.New(h) })
	const acct, region = "000000000000", "us-east-1"
	var created struct {
		TableDescription struct{ LatestStreamArn string }
	}
	if err := h.Call(acct, region, "DynamoDB_20120810", "CreateTable", map[string]any{"TableName": "tbl", "BillingMode": "PAY_PER_REQUEST",
		"AttributeDefinitions": []map[string]string{{"AttributeName": "id", "AttributeType": "S"}},
		"KeySchema":            []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
		"StreamSpecification":  map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}}, &created); err != nil {
		t.Fatal(err)
	}
	if err := h.Call(acct, region, "DynamoDB_20120810", "PutItem", map[string]any{"TableName": "tbl", "Item": map[string]any{"id": map[string]string{"S": "1"}}}, nil); err != nil {
		t.Fatal(err)
	}
	var desc struct {
		StreamDescription struct{ Shards []struct{ ShardId string } }
	}
	var it struct{ ShardIterator string }
	var recs struct {
		Records []struct {
			EventName string `json:"eventName"`
			Dynamodb  struct{ NewImage map[string]map[string]string }
		}
		NextShardIterator string
	}
	arn := created.TableDescription.LatestStreamArn
	if err := errors.Join(
		h.Call(acct, region, "DynamoDBStreams_20120810", "DescribeStream", map[string]string{"StreamArn": arn}, &desc),
		h.Call(acct, region, "DynamoDBStreams_20120810", "GetShardIterator", map[string]string{"StreamArn": arn,
			"ShardId": desc.StreamDescription.Shards[0].ShardId, "ShardIteratorType": "TRIM_HORIZON"}, &it),
		h.Call(acct, region, "DynamoDBStreams_20120810", "GetRecords", map[string]string{"ShardIterator": it.ShardIterator}, &recs),
	); err != nil {
		t.Fatal(err)
	}
	if len(recs.Records) != 1 || recs.Records[0].EventName != "INSERT" || recs.Records[0].Dynamodb.NewImage["id"]["S"] != "1" || recs.NextShardIterator == "" {
		t.Fatalf("records = %+v", recs)
	}
	err := h.Call(acct, region, "DynamoDBStreams_20120810", "GetRecords", map[string]string{"ShardIterator": "garbage"}, nil)
	if core.AsError(err).Code != "ValidationException" {
		t.Fatalf("bad iterator: %v", err)
	}
}

func TestBatchAndTransactions(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("accounts", "id", "")))
	tbl := "accounts"
	put := func(id, balance string) types.WriteRequest {
		return types.WriteRequest{PutRequest: &types.PutRequest{Item: av{"id": S(id), "balance": N(balance)}}}
	}
	if out := must(c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		tbl: {put("a", "100"), put("b", "50"), put("c", "0")}}})); len(out.UnprocessedItems) != 0 {
		t.Fatalf("unprocessed = %v", out.UnprocessedItems)
	}
	must(c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		tbl: {{DeleteRequest: &types.DeleteRequest{Key: av{"id": S("c")}}}}}}))
	bg := must(c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		tbl: {Keys: []map[string]types.AttributeValue{{"id": S("a")}, {"id": S("b")}, {"id": S("c")}}, ProjectionExpression: aws.String("id")}}}))
	if len(bg.Responses[tbl]) != 2 || len(bg.Responses[tbl][0]) != 1 || len(bg.UnprocessedKeys) != 0 {
		t.Fatalf("batch get = %v", bg.Responses)
	}
	_, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		tbl: {Keys: []map[string]types.AttributeValue{{"id": S("a")}, {"id": S("a")}}}}})
	wantValidation(t, err, "duplicates")

	transfer := func(amount, ref string) error {
		_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Update: &types.Update{TableName: aws.String(tbl), Key: av{"id": S("a")}, UpdateExpression: aws.String("SET balance = balance - :amt"),
				ConditionExpression: aws.String("balance >= :amt"), ExpressionAttributeValues: av{":amt": N(amount)},
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld}},
			{Update: &types.Update{TableName: aws.String(tbl), Key: av{"id": S("b")}, UpdateExpression: aws.String("SET balance = balance + :amt"),
				ExpressionAttributeValues: av{":amt": N(amount)}}},
			{ConditionCheck: &types.ConditionCheck{TableName: aws.String(tbl), Key: av{"id": S("c")}, ConditionExpression: aws.String("attribute_not_exists(id)")}},
			{Put: &types.Put{TableName: aws.String(tbl), Item: av{"id": S(ref), "amount": N(amount)}}},
		}})
		return err
	}
	if err := transfer("30", "t1"); err != nil {
		t.Fatal(err)
	}
	err = transfer("500", "t2")
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) || len(tce.CancellationReasons) != 4 || *tce.CancellationReasons[0].Code != "ConditionalCheckFailed" ||
		*tce.CancellationReasons[1].Code != "None" || str(tce.CancellationReasons[0].Item["balance"]) != "70" ||
		!strings.Contains(tce.ErrorMessage(), "[ConditionalCheckFailed, None, None, None]") {
		t.Fatalf("cancelled transfer: %v", err)
	}
	tg := must(c.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: []types.TransactGetItem{
		{Get: &types.Get{TableName: aws.String(tbl), Key: av{"id": S("a")}}},
		{Get: &types.Get{TableName: aws.String(tbl), Key: av{"id": S("b")}}},
		{Get: &types.Get{TableName: aws.String(tbl), Key: av{"id": S("t2")}}},
	}}))
	if len(tg.Responses) != 3 || str(tg.Responses[0].Item["balance"]) != "70" || str(tg.Responses[1].Item["balance"]) != "80" || tg.Responses[2].Item != nil {
		t.Fatalf("after transfers = %+v", tg.Responses)
	}
	_, err = c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Delete: &types.Delete{TableName: aws.String(tbl), Key: av{"id": S("a")}}},
		{Put: &types.Put{TableName: aws.String(tbl), Item: av{"id": S("a")}}},
	}})
	wantValidation(t, err, "multiple operations on one item")
	tok := aws.String("same-token")
	twice := &dynamodb.TransactWriteItemsInput{ClientRequestToken: tok, TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String(tbl), Key: av{"id": S("a")}, UpdateExpression: aws.String("ADD balance :one"), ExpressionAttributeValues: av{":one": N("1")}}}}}
	must(c.TransactWriteItems(ctx, twice))
	must(c.TransactWriteItems(ctx, twice)) // an idempotent retry
	if got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tbl), Key: av{"id": S("a")}})).Item; str(got["balance"]) != "71" {
		t.Fatalf("retried transaction applied twice: %v", got)
	}
}

func TestAttributeValueRoundTrip(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("things", "ID", "")))
	type Dims struct{ W, H float64 }
	type Thing struct {
		ID      string
		Count   int64
		Price   float64
		Active  bool
		Tags    []string `dynamodbav:",stringset"`
		Scores  []int    `dynamodbav:",numberset"`
		Notes   []string
		Attrs   map[string]string
		Empty   map[string]int
		Dims    Dims
		Blob    []byte
		Missing *string
		Created time.Time
	}
	in := Thing{ID: "t1", Count: math.MaxInt64, Price: 19.99, Active: true, Tags: []string{"x", "y"}, Scores: []int{1, 3},
		Notes: []string{"n1", ""}, Attrs: map[string]string{"k": "v"}, Empty: map[string]int{}, Dims: Dims{1.5, 2}, Blob: []byte{0, 1, 2, 255},
		Created: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("things"), Item: must(attributevalue.MarshalMap(in))}))
	got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("things"), Key: av{"ID": S("t1")}})).Item
	var out Thing
	if err := attributevalue.UnmarshalMap(got, &out); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out.Tags)
	slices.Sort(out.Scores)
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip\n got %+v\nwant %+v", out, in)
	}
}

func TestStreamsAndTTL(t *testing.T) {
	c, svc, cfg := setup(t)
	in := def("orders", "id", "")
	in.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages}
	desc := must(c.CreateTable(ctx, in)).TableDescription
	if desc.LatestStreamArn == nil || !*desc.StreamSpecification.StreamEnabled || desc.StreamSpecification.StreamViewType != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("stream spec = %+v", desc)
	}
	tbl, key := aws.String("orders"), av{"id": S("o1")}
	setStatus := &dynamodb.UpdateItemInput{TableName: tbl, Key: key, UpdateExpression: aws.String("SET #s = :s"),
		ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: av{":s": S("paid")}}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"id": S("o1"), "status": S("new")}}))
	must(c.UpdateItem(ctx, setStatus))
	must(c.UpdateItem(ctx, setStatus)) // changes nothing, so no record
	must(c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: tbl, Key: key}))

	sc := dynamodbstreams.NewFromConfig(cfg)
	streams := must(sc.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{TableName: tbl})).Streams
	if len(streams) != 1 || *streams[0].StreamArn != *desc.LatestStreamArn {
		t.Fatalf("streams = %+v", streams)
	}
	sd := must(sc.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: desc.LatestStreamArn})).StreamDescription
	if sd.StreamStatus != streamtypes.StreamStatusEnabled || len(sd.Shards) != 1 || len(sd.KeySchema) != 1 || *sd.TableName != "orders" {
		t.Fatalf("stream = %+v", sd)
	}
	iter := func(typ streamtypes.ShardIteratorType, seq *string) *string {
		return must(sc.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{StreamArn: desc.LatestStreamArn,
			ShardId: sd.Shards[0].ShardId, ShardIteratorType: typ, SequenceNumber: seq})).ShardIterator
	}
	get := func(it *string) *dynamodbstreams.GetRecordsOutput {
		return must(sc.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it}))
	}
	recs := get(iter(streamtypes.ShardIteratorTypeTrimHorizon, nil))
	var names []string
	for _, r := range recs.Records {
		names = append(names, string(r.EventName))
	}
	if strings.Join(names, ",") != "INSERT,MODIFY,REMOVE" {
		t.Fatalf("events = %v", names)
	}
	s := func(m map[string]streamtypes.AttributeValue, k string) string {
		v, _ := m[k].(*streamtypes.AttributeValueMemberS)
		if v == nil {
			return ""
		}
		return v.Value
	}
	r0 := recs.Records[0]
	ins, mod, rem := r0.Dynamodb, recs.Records[1].Dynamodb, recs.Records[2].Dynamodb
	if ins.OldImage != nil || s(ins.NewImage, "status") != "new" || s(mod.OldImage, "status") != "new" || s(mod.NewImage, "status") != "paid" ||
		rem.NewImage != nil || s(rem.OldImage, "status") != "paid" || s(rem.Keys, "id") != "o1" || r0.UserIdentity != nil {
		t.Fatalf("images = %+v %+v %+v", ins, mod, rem)
	}
	if *r0.EventSource != "aws:dynamodb" || *r0.EventVersion != "1.1" || *r0.AwsRegion != "us-east-1" || *r0.EventID == "" ||
		ins.ApproximateCreationDateTime == nil || *ins.SizeBytes == 0 || ins.StreamViewType != streamtypes.StreamViewTypeNewAndOldImages ||
		!(*ins.SequenceNumber < *mod.SequenceNumber && *mod.SequenceNumber < *rem.SequenceNumber) {
		t.Fatalf("record = %+v", r0)
	}
	next := get(recs.NextShardIterator)
	if len(next.Records) != 0 || next.NextShardIterator == nil {
		t.Fatalf("open shard: %d records, next %v", len(next.Records), next.NextShardIterator)
	}
	if n := len(get(iter(streamtypes.ShardIteratorTypeAfterSequenceNumber, ins.SequenceNumber)).Records); n != 2 {
		t.Fatalf("after first record: %d", n)
	}
	if n := len(get(iter(streamtypes.ShardIteratorTypeAtSequenceNumber, mod.SequenceNumber)).Records); n != 2 {
		t.Fatalf("at second record: %d", n)
	}
	if n := len(get(iter(streamtypes.ShardIteratorTypeLatest, nil)).Records); n != 0 {
		t.Fatalf("latest: %d", n)
	}

	// TTL deletions are REMOVE records made by the service.
	must(c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: tbl,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("expires"), Enabled: aws.Bool(true)}}))
	now := time.Now()
	for id, at := range map[string]time.Time{"stale": now.Add(-time.Minute), "fresh": now.Add(time.Hour), "ancient": now.AddDate(-6, 0, 0)} {
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"id": S(id), "expires": N(strconv.FormatInt(at.Unix(), 10))}}))
	}
	svc.ExpireTTL(now)
	for id, alive := range map[string]bool{"stale": false, "fresh": true, "ancient": true} {
		if got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: tbl, Key: av{"id": S(id)}})).Item; (got != nil) != alive {
			t.Fatalf("%s alive = %v", id, got != nil)
		}
	}
	ttl := get(next.NextShardIterator).Records
	i := slices.IndexFunc(ttl, func(r streamtypes.Record) bool { return r.EventName == streamtypes.OperationTypeRemove })
	if len(ttl) != 4 || i < 0 || s(ttl[i].Dynamodb.Keys, "id") != "stale" || ttl[i].UserIdentity == nil ||
		*ttl[i].UserIdentity.Type != "Service" || *ttl[i].UserIdentity.PrincipalId != "dynamodb.amazonaws.com" {
		t.Fatalf("ttl records = %+v", ttl)
	}

	must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: tbl, StreamSpecification: &types.StreamSpecification{StreamEnabled: aws.Bool(false)}}))
	sd = must(sc.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: desc.LatestStreamArn})).StreamDescription
	if sd.StreamStatus != streamtypes.StreamStatusDisabled || sd.Shards[0].SequenceNumberRange.EndingSequenceNumber == nil {
		t.Fatalf("disabled stream = %+v", sd)
	}
	if _, err := sc.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(*desc.LatestStreamArn + "x")}); !errors.As(err, new(*streamtypes.ResourceNotFoundException)) {
		t.Fatalf("missing stream: %v", err)
	}
}

func TestConsoleAndPersistence(t *testing.T) {
	c, svc, _ := setup(t)
	in := def("blobs", "id", "")
	in.AttributeDefinitions[0].AttributeType = types.ScalarAttributeTypeB
	must(c.CreateTable(ctx, in))
	key := av{"id": &types.AttributeValueMemberB{Value: []byte{0xff, 0}}}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("blobs"), Item: av{"id": key["id"], "m": &types.AttributeValueMemberM{Value: av{}}}}))
	const acct, region = "000000000000", "us-east-1"
	if err := svc.PutItemJSON(acct, region, "blobs", map[string]any{"id": map[string]any{"B": "AQI="}, "n": map[string]any{"N": "1.50"}}); err != nil {
		t.Fatal(err)
	}
	items := must(svc.Items(acct, region, "blobs", 10))
	if len(items) != 2 || !reflect.DeepEqual(items[0]["n"], map[string]any{"N": "1.5"}) || !reflect.DeepEqual(items[1]["m"], map[string]any{"M": map[string]any{}}) {
		t.Fatalf("console items = %v", items)
	}

	b := must(svc.Save())
	svc.Reset()
	if got := must(c.ListTables(ctx, &dynamodb.ListTablesInput{})).TableNames; len(got) != 0 {
		t.Fatalf("after reset = %v", got)
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	got := must(c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("blobs"), Key: key})).Item
	if m, ok := got["m"].(*types.AttributeValueMemberM); !ok || len(m.Value) != 0 {
		t.Fatalf("loaded item = %v", got)
	}
	if err := svc.DeleteItemJSON(acct, region, "blobs", map[string]any{"id": map[string]any{"B": "AQI="}}); err != nil {
		t.Fatal(err)
	}
	if rows := svc.Resources()[0].Rows; len(rows) != 1 || rows[0][0] != "blobs" || rows[0][3] != "1" || rows[0][5] != "id (B)" {
		t.Fatalf("resources = %v", rows)
	}
	v, ok := svc.Inspect("Tables", "blobs").(map[string]any)
	if !ok || len(v["Items"].([]ddb.Item)) != 1 || v["Table"].(map[string]any)["TableName"] != "blobs" {
		t.Fatalf("inspect = %v", v)
	}
	if _, err := svc.Items(acct, region, "nope", 0); err == nil {
		t.Fatal("items of a missing table")
	}
}

func TestAccountsAndRegions(t *testing.T) {
	c, _, cfg := setup(t)
	must(c.CreateTable(ctx, def("shared", "id", "")))
	other := dynamodb.NewFromConfig(awstest.ConfigAs(t, *cfg.BaseEndpoint, "111111111111", "test", ""))
	west := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.Region = "eu-west-1" })
	for _, cl := range []*dynamodb.Client{other, west} {
		if _, err := cl.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("shared")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
			t.Fatalf("table leaked across accounts or regions: %v", err)
		}
	}
	d := must(other.CreateTable(ctx, def("shared", "id", ""))).TableDescription
	if *d.TableArn != "arn:aws:dynamodb:us-east-1:111111111111:table/shared" {
		t.Fatalf("arn = %s", *d.TableArn)
	}
	// A table ARN works as TableName, as in AWS.
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: d.TableArn, Item: av{"id": S("x")}}))
	if n := must(other.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("shared")})).Count; n != 1 {
		t.Fatalf("scan by ARN owner = %d", n)
	}
}

// Pre-expression parameters still show up in old code and in DynamoDB mappers.
func TestLegacyParameters(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("legacy", "pk", "sk")))
	tbl := aws.String("legacy")
	for i := 1; i <= 4; i++ {
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"pk": S("p"), "sk": N(strconv.Itoa(i)), "color": S([]string{"red", "blue"}[i%2])},
			Expected: map[string]types.ExpectedAttributeValue{"pk": {Exists: aws.Bool(false)}}}))
	}
	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"pk": S("p"), "sk": N("1")},
		Expected: map[string]types.ExpectedAttributeValue{"pk": {Exists: aws.Bool(false)}}})
	if !errors.As(err, new(*types.ConditionalCheckFailedException)) {
		t.Fatalf("Expected Exists=false: %v", err)
	}
	up := must(c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: av{"pk": S("p"), "sk": N("1")}, ReturnValues: types.ReturnValueAllNew,
		AttributeUpdates: map[string]types.AttributeValueUpdate{
			"hits":  {Action: types.AttributeActionAdd, Value: N("2")},
			"color": {Action: types.AttributeActionDelete},
			"tag":   {Action: types.AttributeActionPut, Value: S("x")},
		},
		Expected: map[string]types.ExpectedAttributeValue{"color": {ComparisonOperator: types.ComparisonOperatorEq, AttributeValueList: []types.AttributeValue{S("blue")}}},
	})).Attributes
	if str(up["hits"]) != "2" || up["color"] != nil || str(up["tag"]) != "x" {
		t.Fatalf("AttributeUpdates = %v", up)
	}
	q := must(c.Query(ctx, &dynamodb.QueryInput{TableName: tbl, AttributesToGet: []string{"sk"},
		KeyConditions: map[string]types.Condition{
			"pk": {ComparisonOperator: types.ComparisonOperatorEq, AttributeValueList: []types.AttributeValue{S("p")}},
			"sk": {ComparisonOperator: types.ComparisonOperatorBetween, AttributeValueList: []types.AttributeValue{N("2"), N("4")}},
		},
		QueryFilter: map[string]types.Condition{"color": {ComparisonOperator: types.ComparisonOperatorEq, AttributeValueList: []types.AttributeValue{S("red")}}},
	}))
	if q.Count != 2 || q.ScannedCount != 3 || len(q.Items[0]) != 1 || str(q.Items[0]["sk"]) != "2" {
		t.Fatalf("legacy query = %d/%d %v", q.Count, q.ScannedCount, q.Items)
	}
	sc := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, ConditionalOperator: types.ConditionalOperatorOr, ScanFilter: map[string]types.Condition{
		"color": {ComparisonOperator: types.ComparisonOperatorNull},
		"sk":    {ComparisonOperator: types.ComparisonOperatorGt, AttributeValueList: []types.AttributeValue{N("3")}},
	}}))
	if sc.Count != 2 {
		t.Fatalf("legacy scan = %d", sc.Count)
	}
	tw := must(c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TransactItems: []types.TransactWriteItem{{Put: &types.Put{TableName: tbl, Item: av{"pk": S("q"), "sk": N("1")}}}}}))
	if len(tw.ConsumedCapacity) != 1 || *tw.ConsumedCapacity[0].CapacityUnits != 2 {
		t.Fatalf("transaction capacity = %+v", tw.ConsumedCapacity)
	}
}

func TestDocumentsAndSets(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("docs", "id", "")))
	tbl := aws.String("docs")
	l := func(v ...types.AttributeValue) types.AttributeValue { return &types.AttributeValueMemberL{Value: v} }
	m := &types.AttributeValueMemberM{Value: av{"a": N("1"), "b": l(N("1"), S("x"))}}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tbl, Item: av{"id": S("1"), "m": m, "l": l(N("1"), S("x")),
		"ss": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}}, "ns": &types.AttributeValueMemberNS{Value: []string{"1", "2.5"}},
		"bs": &types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}}, "flag": &types.AttributeValueMemberBOOL{Value: true},
		"nothing": &types.AttributeValueMemberNULL{Value: true}}}))
	vals := av{":m": m, ":l": l(N("1"), S("x")), ":l2": l(S("x"), N("1")), ":ss": &types.AttributeValueMemberSS{Value: []string{"c", "a", "b"}},
		":ns": &types.AttributeValueMemberNS{Value: []string{"2.50", "1.0"}}, ":t": &types.AttributeValueMemberBOOL{Value: true},
		":null": &types.AttributeValueMemberNULL{Value: true}, ":x": S("x"), ":one": N("1"), ":b": &types.AttributeValueMemberB{Value: []byte{2}},
		":two": N("2"), ":three": N("3"), ":M": S("M")}
	for cond, want := range map[string]bool{
		"m = :m": true, "l = :l": true, "l = :l2": false, "l <> :l2": true, "ss = :ss": true, "ns = :ns": true, "flag = :t": true,
		"#n = :null": true, "contains(l, :x)": true, "contains(ns, :one)": true, "contains(bs, :b)": true, "contains(ss, :x)": false,
		"size(m) = :two": true, "size(ss) = :three": true, "m.b[1] = :x": true, "m.b[5] = :x": false, "attribute_type(m, :M)": true,
		"ss > :ss": false, "nope <> :x": true,
	} {
		used := av{}
		for _, k := range regexp.MustCompile(`:\w+`).FindAllString(cond, -1) {
			used[k] = vals[k]
		}
		var names map[string]string
		if strings.Contains(cond, "#n") {
			names = map[string]string{"#n": "nothing"}
		}
		out, err := c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, FilterExpression: aws.String(cond), ExpressionAttributeNames: names, ExpressionAttributeValues: used})
		if err != nil || (out.Count == 1) != want {
			t.Fatalf("%s: want %v, got %v %v", cond, want, out, err)
		}
	}
	up := must(c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: tbl, Key: av{"id": S("1")}, ReturnValues: types.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String("ADD ns :ns DELETE bs :bs"),
		ExpressionAttributeValues: av{":ns": &types.AttributeValueMemberNS{Value: []string{"3", "1.00"}}, ":bs": &types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}}},
	})).Attributes
	ns := up["ns"].(*types.AttributeValueMemberNS).Value
	slices.Sort(ns)
	if len(up) != 1 || !reflect.DeepEqual(ns, []string{"1", "2.5", "3"}) {
		t.Fatalf("set update = %v (bs is gone once empty)", up)
	}
}

// floci #4572: UpdateTable changes at most one GSI and needs an index name of 3 or more characters.
// floci #3322: a Query start key from another partition is refused.
func TestUpdateTableIndexLimitsAndQueryStartKey(t *testing.T) {
	c, _, _ := setup(t)
	in := def("gsit", "pk", "")
	in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String("a"), AttributeType: types.ScalarAttributeTypeS},
		types.AttributeDefinition{AttributeName: aws.String("b"), AttributeType: types.ScalarAttributeTypeS})
	gsi := func(name, key string) types.GlobalSecondaryIndex {
		return types.GlobalSecondaryIndex{IndexName: aws.String(name), Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			KeySchema: []types.KeySchemaElement{{AttributeName: aws.String(key), KeyType: types.KeyTypeHash}}}
	}
	in.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{gsi("gsi_a", "a"), gsi("gsi_b", "b")}
	must(c.CreateTable(ctx, in))
	del := func(name string) types.GlobalSecondaryIndexUpdate {
		return types.GlobalSecondaryIndexUpdate{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(name)}}
	}
	_, err := c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("gsit"), GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{del("gsi_a"), del("gsi_b")}})
	if code, _ := apiErr(err); code != "LimitExceededException" {
		t.Fatalf("two deletes: %v", err)
	}
	_, err = c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("gsit"), GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{del("ab")}})
	wantValidation(t, err, "length greater than or equal to 3")
	must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("gsit"), GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{del("gsi_a")}}))

	must(c.CreateTable(ctx, def("qtab", "pk", "sk")))
	for _, pk := range []string{"a", "b"} {
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("qtab"), Item: av{"pk": S(pk), "sk": N("1")}}))
	}
	_, err = c.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("qtab"), KeyConditionExpression: aws.String("pk = :p"),
		ExpressionAttributeValues: av{":p": S("a")}, ExclusiveStartKey: av{"pk": S("b"), "sk": N("1")}})
	wantValidation(t, err, "outside query boundaries")
}

// floci #4521: on-demand backups and restore, also to a point in time.
func TestBackupsAndRestore(t *testing.T) {
	c, svc, _ := setup(t)
	must(c.CreateTable(ctx, def("src", "id", "")))
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("src"), Item: av{"id": S("a"), "v": S("1")}}))
	b := must(c.CreateBackup(ctx, &dynamodb.CreateBackupInput{TableName: aws.String("src"), BackupName: aws.String("backup1")})).BackupDetails
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("src"), Item: av{"id": S("b")}}))
	if _, err := c.CreateBackup(ctx, &dynamodb.CreateBackupInput{TableName: aws.String("nope"), BackupName: aws.String("backup1")}); !errors.As(err, new(*types.TableNotFoundException)) {
		t.Fatalf("backup of a missing table: %v", err)
	}
	// The backup survives a save and load, as in the storage modes.
	snap := must(svc.Save())
	if err := svc.Load(snap); err != nil {
		t.Fatal(err)
	}
	if l := must(c.ListBackups(ctx, &dynamodb.ListBackupsInput{TableName: aws.String("src")})).BackupSummaries; len(l) != 1 || *l[0].BackupArn != *b.BackupArn || l[0].BackupStatus != types.BackupStatusAvailable {
		t.Fatalf("ListBackups = %+v", l)
	}
	d := must(c.DescribeBackup(ctx, &dynamodb.DescribeBackupInput{BackupArn: b.BackupArn})).BackupDescription
	if *d.SourceTableDetails.ItemCount != 1 || *d.SourceTableDetails.TableName != "src" {
		t.Fatalf("DescribeBackup = %+v", d.SourceTableDetails)
	}
	r := must(c.RestoreTableFromBackup(ctx, &dynamodb.RestoreTableFromBackupInput{TargetTableName: aws.String("copy"), BackupArn: b.BackupArn})).TableDescription
	if r.RestoreSummary == nil || *r.RestoreSummary.SourceBackupArn != *b.BackupArn {
		t.Fatalf("RestoreSummary = %+v", r.RestoreSummary)
	}
	if n := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("copy")})).Count; n != 1 {
		t.Fatalf("restored items = %d", n)
	}
	if _, err := c.RestoreTableFromBackup(ctx, &dynamodb.RestoreTableFromBackupInput{TargetTableName: aws.String("copy"), BackupArn: b.BackupArn}); !errors.As(err, new(*types.TableAlreadyExistsException)) {
		t.Fatalf("restore over a table: %v", err)
	}

	// Point in time needs PITR on.
	pit := &dynamodb.RestoreTableToPointInTimeInput{SourceTableName: aws.String("src"), TargetTableName: aws.String("pit"), UseLatestRestorableTime: aws.Bool(true)}
	if _, err := c.RestoreTableToPointInTime(ctx, pit); !errors.As(err, new(*types.PointInTimeRecoveryUnavailableException)) {
		t.Fatalf("restore without PITR: %v", err)
	}
	must(c.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{TableName: aws.String("src"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)}}))
	must(c.RestoreTableToPointInTime(ctx, pit))
	if n := must(c.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("pit")})).Count; n != 2 {
		t.Fatalf("point in time items = %d", n)
	}

	must(c.DeleteBackup(ctx, &dynamodb.DeleteBackupInput{BackupArn: b.BackupArn}))
	if _, err := c.DescribeBackup(ctx, &dynamodb.DescribeBackupInput{BackupArn: b.BackupArn}); !errors.As(err, new(*types.BackupNotFoundException)) {
		t.Fatalf("deleted backup: %v", err)
	}
}

// floci #4177: replicas appear in DescribeTable, are real tables, and copy writes both ways.
func TestGlobalTableReplicas(t *testing.T) {
	c, _, cfg := setup(t)
	in := def("gtab", "id", "")
	in.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages}
	must(c.CreateTable(ctx, in))
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("gtab"), Item: av{"id": S("a")}}))
	up := func(u types.ReplicationGroupUpdate) error {
		_, err := c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("gtab"), ReplicaUpdates: []types.ReplicationGroupUpdate{u}})
		return err
	}
	wantValidation(t, up(types.ReplicationGroupUpdate{Create: &types.CreateReplicationGroupMemberAction{RegionName: aws.String("us-east-1")}}), "already exists")
	if err := up(types.ReplicationGroupUpdate{Create: &types.CreateReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")}}); err != nil {
		t.Fatal(err)
	}
	d := must(c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("gtab")})).Table
	var regs []string
	for _, r := range d.Replicas {
		regs = append(regs, *r.RegionName)
	}
	if strings.Join(regs, ",") != "us-east-1,eu-west-1" || *d.GlobalTableVersion != "2019.11.21" {
		t.Fatalf("Replicas = %v, version %v", regs, d.GlobalTableVersion)
	}
	eu := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.Region = "eu-west-1" })
	if g := must(eu.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("gtab"), Key: av{"id": S("a")}})); g.Item == nil {
		t.Fatal("existing item missing in the replica")
	}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("gtab"), Item: av{"id": S("b")}}))
	must(eu.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("gtab"), Item: av{"id": S("c")}}))
	for _, cl := range []*dynamodb.Client{c, eu} {
		if n := must(cl.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("gtab")})).Count; n != 3 {
			t.Fatalf("items = %d, want 3 in both regions", n)
		}
	}
	if err := up(types.ReplicationGroupUpdate{Delete: &types.DeleteReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")}}); err != nil {
		t.Fatal(err)
	}
	if d := must(c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("gtab")})).Table; len(d.Replicas) != 0 {
		t.Fatalf("Replicas after delete = %+v", d.Replicas)
	}
	if _, err := eu.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("gtab")}); err == nil {
		t.Fatal("replica table still exists")
	}
}

// floci #2460, #2461, #2462: a global secondary index can have up to four HASH and four RANGE attributes.
func TestMultiAttributeGSI(t *testing.T) {
	c, _, _ := setup(t)
	ks := func(kt types.KeyType, names ...string) (out []types.KeySchemaElement) {
		for _, n := range names {
			out = append(out, types.KeySchemaElement{AttributeName: aws.String(n), KeyType: kt})
		}
		return
	}
	mk := func(name string, k []types.KeySchemaElement) error {
		attrs := []types.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS}}
		for _, e := range k {
			attrs = append(attrs, types.AttributeDefinition{AttributeName: e.AttributeName, AttributeType: types.ScalarAttributeTypeS})
		}
		_, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: attrs, KeySchema: ks(types.KeyTypeHash, "id"),
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{IndexName: aws.String("gsi"), KeySchema: k, Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}}}})
		return err
	}
	wantValidation(t, mk("toomanyhash", ks(types.KeyTypeHash, "tenant", "region", "state", "created", "h5")), "Invalid KeySchema")
	wantValidation(t, mk("toomanyrange", append(ks(types.KeyTypeHash, "tenant"), ks(types.KeyTypeRange, "region", "state", "created", "h5", "h6")...)), "Invalid KeySchema")
	wantValidation(t, mk("rangefirst", append(ks(types.KeyTypeRange, "state"), ks(types.KeyTypeHash, "tenant")...)), "Invalid KeySchema")
	if err := mk("multikey", append(ks(types.KeyTypeHash, "tenant", "region"), ks(types.KeyTypeRange, "state", "created")...)); err != nil {
		t.Fatal(err)
	}
	put := func(id, tenant, region, state, created string) {
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("multikey"), Item: av{"id": S(id), "tenant": S(tenant), "region": S(region), "state": S(state), "created": S(created)}}))
	}
	put("1", "acme", "us", "A", "2026-01-01")
	put("2", "acme", "eu", "D", "2026-02-01")
	put("3", "acme", "us", "B", "2026-03-01")
	q := func(expr string, vals av) (*dynamodb.QueryOutput, error) {
		vals[":t"] = S("acme")
		names := map[string]string{}
		for k, v := range map[string]string{"#r": "region", "#s": "state"} {
			if strings.Contains(expr, k) {
				names[k] = v
			}
		}
		if len(names) == 0 {
			names = nil
		}
		return c.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("multikey"), IndexName: aws.String("gsi"), KeyConditionExpression: aws.String(expr),
			ExpressionAttributeNames: names, ExpressionAttributeValues: vals})
	}
	_, err := q("tenant = :t", av{})
	wantValidation(t, err, "region")
	if o := must(q("tenant = :t AND #r = :r", av{":r": S("us")})); o.Count != 2 || str(o.Items[0]["id"]) != "1" || str(o.Items[1]["id"]) != "3" {
		t.Fatalf("full partition key: %+v", o.Items)
	}
	if o := must(q("tenant = :t AND #r = :r AND #s > :s", av{":r": S("us"), ":s": S("A")})); o.Count != 1 || str(o.Items[0]["id"]) != "3" {
		t.Fatalf("sort key range: %+v", o.Items)
	}
	_, err = q("tenant = :t AND #r = :r AND created = :c", av{":r": S("us"), ":c": S("x")})
	wantValidation(t, err, "state")
	_, err = q("tenant = :t AND #r = :r AND #s > :s AND created = :c", av{":r": S("us"), ":s": S("A"), ":c": S("x")})
	wantValidation(t, err, "range")
}

// floci #3323: a partition key over 2048 bytes and a sort key over 1024 bytes are refused.
// floci #2894: Query cannot mix QueryFilter with KeyConditionExpression.
// floci #2468: an ExclusiveStartKey of the wrong key type is refused.
// floci #2276: Query refuses a FilterExpression on a key attribute.
func TestKeyLimitsAndQueryChecks(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateTable(ctx, def("lim", "id", "n")))
	tn := aws.String("lim")
	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tn, Item: av{"id": S(strings.Repeat("a", 2049)), "n": N("1")}})
	wantValidation(t, err, "hashkey")
	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{TableName: tn, Item: av{"id": S(strings.Repeat("a", 2048)), "n": N("1")}})
	if err != nil {
		t.Fatalf("2048-byte partition key: %v", err)
	}
	// A sort key is a number here; the sort key limit is covered through a string-keyed table.
	sk := def("lim2", "id", "")
	sk.AttributeDefinitions = append(sk.AttributeDefinitions, types.AttributeDefinition{AttributeName: aws.String("s"), AttributeType: types.ScalarAttributeTypeS})
	sk.KeySchema = append(sk.KeySchema, types.KeySchemaElement{AttributeName: aws.String("s"), KeyType: types.KeyTypeRange})
	must(c.CreateTable(ctx, sk))
	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("lim2"), Item: av{"id": S("a"), "s": S(strings.Repeat("a", 1025))}})
	wantValidation(t, err, "range keys")

	_, err = c.Query(ctx, &dynamodb.QueryInput{TableName: tn, KeyConditionExpression: aws.String("id = :p"),
		ExpressionAttributeValues: av{":p": S("a")},
		QueryFilter:               map[string]types.Condition{"x": {ComparisonOperator: types.ComparisonOperatorEq, AttributeValueList: []types.AttributeValue{S("1")}}}})
	wantValidation(t, err, "Can not use both expression and non-expression")

	_, err = c.Scan(ctx, &dynamodb.ScanInput{TableName: tn, ExclusiveStartKey: av{"id": N("1"), "n": N("1")}})
	wantValidation(t, err, "starting key")
	_, err = c.Query(ctx, &dynamodb.QueryInput{TableName: tn, KeyConditionExpression: aws.String("id = :p"), ExpressionAttributeValues: av{":p": S("a")},
		ExclusiveStartKey: av{"id": S("a"), "n": &types.AttributeValueMemberBOOL{Value: true}}})
	wantValidation(t, err, "starting key")

	_, err = c.Query(ctx, &dynamodb.QueryInput{TableName: tn, KeyConditionExpression: aws.String("id = :p"), FilterExpression: aws.String("n > :z"),
		ExpressionAttributeValues: av{":p": S("a"), ":z": N("0")}})
	wantValidation(t, err, "non-primary key")
}
