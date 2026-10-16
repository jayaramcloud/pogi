package web_test

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/nrndev/pogi/internal/awstest"
)

// Cost prices a seeded bucket, queue and table from resources plus traffic, lists a topic as not
// priced, and feeds the sidebar badge.
func TestCostPage(t *testing.T) {
	ctx := context.Background()
	c := start(t, "s3", "sqs", "dynamodb", "sns")
	want(t, c.get("/_pogi/cost"), "No resources yet", "$0.00")

	cfg := awstest.Config(t, c.base)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("uploads")}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("uploads"), Key: aws.String("a"), Body: strings.NewReader("hello")}); err != nil {
			t.Fatal(err)
		}
	}
	q, err := sqs.NewFromConfig(cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})
	if err != nil {
		t.Fatal(err)
	}
	sqs.NewFromConfig(cfg).SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("x")})
	ddb := dynamodb.NewFromConfig(cfg)
	if _, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("orders"), BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}}); err != nil {
		t.Fatal(err)
	}
	ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders"), Item: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}}})
	if _, err := sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("events")}); err != nil {
		t.Fatal(err)
	}

	page := c.get("/_pogi/cost")
	want(t, page, "Estimated monthly cost", ">uploads<", ">jobs<", ">orders<", ">events<", "not priced", "writes,", "requests counted in", "as a rate over 30 days")
	wantNot(t, page, "No resources yet", `bigcost">$0.00`)
	// the same estimate in another region costs more (eu-west-1 is +10%)
	total := func(p string) float64 {
		_, rest, _ := strings.Cut(p, `bigcost">$`)
		v, _, _ := strings.Cut(rest, "<")
		f, _ := strconv.ParseFloat(v, 64)
		return f
	}
	us, eu := total(page), total(c.get("/_pogi/cost?region=eu-west-1"))
	if us <= 0 || eu <= us {
		t.Fatalf("monthly totals: us-east-1 %v, eu-west-1 %v", us, eu)
	}
	if !strings.Contains(c.get("/_pogi/"), "/mo") {
		t.Fatal("the sidebar has no cost badge")
	}
}

// do sends a request to the test server, with a Host header when the service routes by host.
func do(t *testing.T, c *console, method, path, host, body string) {
	t.Helper()
	req, _ := http.NewRequest(method, c.base+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if host != "" {
		req.Host = host
	}
	res, err := c.c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("%s %s = %d", method, path, res.StatusCode)
	}
}

// Google Cloud and Azure resources are priced from the static table, with their own cloud dot.
func TestCostOtherClouds(t *testing.T) {
	c := start(t, "gcs", "azblob")
	do(t, c, "POST", "/storage/v1/b?project=pogi", "", `{"name":"assets"}`)
	do(t, c, "PUT", "/files?restype=container", "dev.blob.localhost", "")
	page := c.get("/_pogi/cost")
	want(t, page, `dot gcp"></span> <span class="faint">Cloud Storage`, ">assets<", `dot azure"></span> <span class="faint">Blob Storage`, ">files<")
	wantNot(t, page, ">not priced<")
}

// AWS SNS, KMS and Secrets Manager rows are priced; an RDS instance adds its allocated storage.
func TestCostAWSStatic(t *testing.T) {
	ctx := context.Background()
	c := start(t, "sns", "kms", "secretsmanager")
	cfg := awstest.Config(t, c.base)
	if _, err := sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("events")}); err != nil {
		t.Fatal(err)
	}
	if _, err := kms.NewFromConfig(cfg).CreateKey(ctx, &kms.CreateKeyInput{}); err != nil {
		t.Fatal(err)
	}
	want(t, c.get("/_pogi/cost"), ">events<", "1 key", "publishes", "$1.1")
}

// An RDS instance is priced for its hours plus its allocated storage.
func TestCostRDSStorage(t *testing.T) {
	c := start(t, "rds")
	cfg := awstest.Config(t, c.base)
	if _, err := rds.NewFromConfig(cfg).CreateDBInstance(context.Background(), &rds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String("db1"), Engine: aws.String("postgres"),
		DBInstanceClass: aws.String("db.t3.micro"), AllocatedStorage: aws.Int32(100), StorageType: aws.String("gp3"), MasterUsername: aws.String("u"), MasterUserPassword: aws.String("password1")}); err != nil {
		t.Fatal(err)
	}
	want(t, c.get("/_pogi/cost"), ">db1<", "100 GB gp3")
}
