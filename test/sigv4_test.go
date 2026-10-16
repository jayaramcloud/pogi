package test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// With signature checking on, the SDKs work as usual (S3 presigning and POST
// policies, STS sessions, IAM keys, Lambda role credentials) and a wrong
// secret is rejected in every protocol family.
func TestSignatures(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t, pogi.Config{ValidateSigs: true})
	cfg := awstest.Config(t, srv.URL)
	ok := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	want := func(what string, err error, code string) {
		t.Helper()
		var ae smithy.APIError
		if !errors.As(err, &ae) || ae.ErrorCode() != code {
			t.Errorf("%s: want %s, got %v", what, code, err)
		}
	}
	whoami := func(cfg aws.Config) string {
		t.Helper()
		id, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		ok("GetCallerIdentity", err)
		return *id.Arn
	}

	// S3: put, get, multipart, a presigned GET and a browser POST upload
	pathStyle := func(o *s3.Options) { o.UsePathStyle = true }
	s := s3.NewFromConfig(cfg, pathStyle)
	b, key := aws.String("sigs"), aws.String("dir/a b+(1)ü.txt")
	_, err := s.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: b})
	ok("CreateBucket", err)
	_, err = s.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: key, Body: strings.NewReader("hello")})
	ok("PutObject", err)
	get, err := s.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: key})
	ok("GetObject", err)
	if body, _ := io.ReadAll(get.Body); string(body) != "hello" {
		t.Fatalf("GetObject = %q", body)
	}
	up, err := s.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("big")})
	ok("CreateMultipartUpload", err)
	part, err := s.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: aws.String("big"), UploadId: up.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("part one")})
	ok("UploadPart", err)
	_, err = s.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b, Key: aws.String("big"), UploadId: up.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}}}})
	ok("CompleteMultipartUpload", err)
	ps := s3.NewPresignClient(s)
	pg, err := ps.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: key})
	ok("PresignGetObject", err)
	if st, body := call(t, "GET", pg.URL); st != 200 || body != "hello" {
		t.Fatalf("presigned GET = %d %s", st, body)
	}
	if st, body := call(t, "GET", strings.Replace(pg.URL, "X-Amz-Signature=", "X-Amz-Signature=0", 1)); st != 403 {
		t.Fatalf("tampered presigned GET = %d %s", st, body)
	}
	pp, err := ps.PresignPostObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("posted.txt")})
	ok("PresignPostObject", err)
	post := func(fields map[string]string) (int, string) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		for k, v := range fields {
			mw.WriteField(k, v)
		}
		fw, _ := mw.CreateFormFile("file", "posted.txt")
		io.WriteString(fw, "from a form")
		mw.Close()
		res, err := http.Post(pp.URL, mw.FormDataContentType(), &buf)
		ok("POST upload", err)
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}
	if st, body := post(pp.Values); st != 204 {
		t.Fatalf("POST upload = %d %s", st, body)
	}
	pp.Values["X-Amz-Signature"] = strings.Repeat("0", 64)
	if st, body := post(pp.Values); st != 403 || !strings.Contains(body, "SignatureDoesNotMatch") {
		t.Fatalf("POST upload with a bad signature = %d %s", st, body)
	}

	// SQS over JSON (the SDK) and Query (a signed form)
	q := sqs.NewFromConfig(cfg)
	qu, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("sigs")})
	ok("CreateQueue", err)
	_, err = q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: qu.QueueUrl, MessageBody: aws.String("json")})
	ok("SendMessage", err)
	req, _ := http.NewRequest("POST", *qu.QueueUrl, strings.NewReader(url.Values{"Action": {"SendMessage"}, "MessageBody": {"query"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	awstest.Sign(t, req, "sqs")
	res, err := http.DefaultClient.Do(req)
	ok("Query SendMessage", err)
	res.Body.Close()
	msgs, err := q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qu.QueueUrl, MaxNumberOfMessages: 10})
	ok("ReceiveMessage", err)
	if res.StatusCode != 200 || len(msgs.Messages) != 2 {
		t.Fatalf("Query SendMessage = %d, received %d", res.StatusCode, len(msgs.Messages))
	}

	// DynamoDB
	d := dynamodb.NewFromConfig(cfg)
	_, err = d.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("sigs"), BillingMode: ddb.BillingModePayPerRequest,
		AttributeDefinitions: []ddb.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddb.ScalarAttributeTypeS}},
		KeySchema:            []ddb.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddb.KeyTypeHash}}})
	ok("CreateTable", err)
	_, err = d.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("sigs"), Item: map[string]ddb.AttributeValue{"id": &ddb.AttributeValueMemberS{Value: "1"}}})
	ok("PutItem", err)

	// STS session credentials sign the next calls
	_, err = iam.NewFromConfig(awstest.ConfigAs(t, srv.URL, "111111111111", "test", "")).CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("app"),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}`)})
	ok("CreateRole", err)
	ar, err := sts.NewFromConfig(cfg).AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: aws.String("arn:aws:iam::111111111111:role/app"), RoleSessionName: aws.String("ss")})
	ok("AssumeRole", err)
	session := awstest.ConfigAs(t, srv.URL, *ar.Credentials.AccessKeyId, *ar.Credentials.SecretAccessKey, *ar.Credentials.SessionToken)
	if arn := whoami(session); arn != "arn:aws:sts::111111111111:assumed-role/app/ss" {
		t.Fatalf("assumed role = %s", arn)
	}
	_, err = sqs.NewFromConfig(session).ListQueues(ctx, &sqs.ListQueuesInput{})
	ok("ListQueues with the session", err)

	// IAM access keys sign as their user, survive a restore and stop when deactivated
	i := iam.NewFromConfig(cfg)
	_, err = i.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String("bob")})
	ok("CreateUser", err)
	k, err := i.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String("bob")})
	ok("CreateAccessKey", err)
	bob := awstest.ConfigAs(t, srv.URL, *k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, "")
	if arn := whoami(bob); arn != "arn:aws:iam::000000000000:user/bob" {
		t.Fatalf("bob = %s", arn)
	}
	if v, _ := json.Marshal(srv.Hub.Service("iam").(core.Inspectable).Inspect("Users", "bob")); strings.Contains(string(v), *k.AccessKey.SecretAccessKey) {
		t.Fatalf("the console shows the secret: %s", v)
	}
	snap, err := srv.Hub.Snapshot()
	ok("Snapshot", err)
	again := pogi.Start(t, pogi.Config{ValidateSigs: true})
	ok("Restore", again.Hub.Restore(snap))
	if arn := whoami(awstest.ConfigAs(t, again.URL, *k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, "")); arn != "arn:aws:iam::000000000000:user/bob" {
		t.Fatalf("bob after restore = %s", arn)
	}
	_, err = i.UpdateAccessKey(ctx, &iam.UpdateAccessKeyInput{UserName: aws.String("bob"), AccessKeyId: k.AccessKey.AccessKeyId, Status: iamtypes.StatusTypeInactive})
	ok("UpdateAccessKey", err)
	_, err = sts.NewFromConfig(bob).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	want("inactive key", err, "InvalidClientTokenId")

	// Lambda role credentials, as a function sees them
	if _, err := exec.LookPath("python3"); err == nil {
		var zbuf bytes.Buffer
		zw := zip.NewWriter(&zbuf)
		w, _ := zw.Create("app.py")
		io.WriteString(w, "import os\ndef handler(event, context):\n    return {k: os.environ[k] for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')}\n")
		zw.Close()
		l := lambda.NewFromConfig(cfg)
		_, err = l.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("creds"), Runtime: lambdatypes.RuntimePython312,
			Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/fn"), Code: &lambdatypes.FunctionCode{ZipFile: zbuf.Bytes()}})
		ok("CreateFunction", err)
		out, err := l.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("creds")})
		ok("Invoke", err)
		var env map[string]string
		json.Unmarshal(out.Payload, &env)
		role := awstest.ConfigAs(t, srv.URL, env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"], env["AWS_SESSION_TOKEN"])
		if arn := whoami(role); arn != "arn:aws:sts::000000000000:assumed-role/fn/creds" {
			t.Fatalf("function role = %s (payload %s)", arn, out.Payload)
		}
	}

	// a wrong secret, per protocol family, and an unknown key
	bad := awstest.ConfigAs(t, srv.URL, "test", "wrong", "")
	_, err = dynamodb.NewFromConfig(bad).ListTables(ctx, &dynamodb.ListTablesInput{})
	want("JSON", err, "InvalidSignatureException")
	_, err = sts.NewFromConfig(bad).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	want("Query", err, "SignatureDoesNotMatch")
	_, err = s3.NewFromConfig(bad, pathStyle).ListBuckets(ctx, &s3.ListBucketsInput{})
	want("REST-XML", err, "SignatureDoesNotMatch")
	_, err = lambda.NewFromConfig(bad).ListFunctions(ctx, &lambda.ListFunctionsInput{})
	want("REST-JSON", err, "InvalidSignatureException")
	_, err = cloudwatch.NewFromConfig(bad).ListMetrics(ctx, &cloudwatch.ListMetricsInput{})
	want("CBOR", err, "InvalidSignatureException")
	_, err = dynamodb.NewFromConfig(awstest.ConfigAs(t, srv.URL, "AKIANOBODY", "test", "")).ListTables(ctx, &dynamodb.ListTablesInput{})
	want("unknown key", err, "InvalidClientTokenId")
}
