package dynamodb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ddb "github.com/nrndev/pogi/internal/aws/dynamodb"
	"github.com/nrndev/pogi/internal/core"
)

// TestJournalReplay replays the wal-mode journal into a fresh service: the
// state must come out the same, stream records and tokens included.
func TestJournalReplay(t *testing.T) {
	c, svc, _ := setup(t)
	fresh := ddb.New(nil)
	t.Cleanup(func() { fresh.Close() })
	check := core.JournalTest(t, svc, fresh)

	for _, name := range []string{"items", "quiet"} {
		in := def(name, "id", "")
		in.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages}
		must(c.CreateTable(ctx, in))
	}
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("quiet"), Item: av{"id": S("q")}}))
	must(c.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("quiet"),
		StreamSpecification: &types.StreamSpecification{StreamEnabled: aws.Bool(false)}}))
	must(c.CreateTable(ctx, def("gone", "id", "")))
	must(c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("gone")}))
	for _, id := range []string{"a", "b", "c"} {
		must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("items"), Item: av{"id": S(id), "n": N("1")}}))
	}
	must(c.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String("items"), Key: av{"id": S("a")},
		UpdateExpression: aws.String("ADD n :one"), ExpressionAttributeValues: av{":one": N("1")}}))
	must(c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("items"), Key: av{"id": S("b")}}))
	must(c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{ClientRequestToken: aws.String("tok"), TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("items"), Item: av{"id": S("d")}}}}}))
	must(c.TagResource(ctx, &dynamodb.TagResourceInput{ResourceArn: aws.String("arn:aws:dynamodb:us-east-1:000000000000:table/items"),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("core")}}}))
	must(c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: aws.String("items"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("exp"), Enabled: aws.Bool(true)}}))
	past := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
	must(c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("items"), Item: av{"id": S("old"), "exp": N(past)}}))
	svc.ExpireTTL(time.Now())
	check()
}
