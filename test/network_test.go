package test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apitypes "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// orFail returns v and fails the test (by panicking) on err.
func orFail[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// pyZip is a deployment package with index.py.
func pyZip(code string) []byte {
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	w, _ := zw.Create("index.py")
	io.WriteString(w, code)
	zw.Close()
	return b.Bytes()
}

// whoAmI returns the ARN STS GetCallerIdentity gives the function's credentials.
const whoAmI = `import datetime, hashlib, hmac, os, urllib.request

def handler(event, context):
    key, secret, token, region, url = (os.environ[k] for k in
        ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_ENDPOINT_URL"))
    body = b"Action=GetCallerIdentity&Version=2011-06-15"
    now = datetime.datetime.utcnow()
    stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    h = {"content-type": "application/x-www-form-urlencoded", "host": url.split("://")[1].rstrip("/"),
         "x-amz-date": stamp, "x-amz-security-token": token}
    signed = ";".join(sorted(h))
    canonical = "POST\n/\n\n" + "".join(k + ":" + h[k] + "\n" for k in sorted(h)) + "\n" + signed + "\n" + hashlib.sha256(body).hexdigest()
    scope = day + "/" + region + "/sts/aws4_request"
    k = ("AWS4" + secret).encode()
    for part in (day, region, "sts", "aws4_request"):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, ("AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()).encode(), hashlib.sha256).hexdigest()
    h["authorization"] = "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s" % (key, scope, signed, sig)
    body = urllib.request.urlopen(urllib.request.Request(url, data=body, headers=h)).read().decode()
    return body[body.index("<Arn>") + 5:body.index("</Arn>")]
`

// TestPartitionARNs: in GovCloud and China, ARNs follow the region's partition.
func TestPartitionARNs(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t)
	cfg := awstest.Config(t, srv.URL)
	cfg.Region = "us-gov-west-1"
	expect := func(what, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %s, want %s", what, got, want)
		}
	}
	stsc, iamc := sts.NewFromConfig(cfg), iam.NewFromConfig(cfg)
	expect("caller", *orFail(stsc.GetCallerIdentity(ctx, nil)).Arn, "arn:aws-us-gov:iam::000000000000:root")
	role := orFail(iamc.CreateRole(ctx, &iam.CreateRoleInput{RoleName: aws.String("app"),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`)})).Role
	expect("role", *role.Arn, "arn:aws-us-gov:iam::000000000000:role/app")
	readOnly := "arn:aws-us-gov:iam::aws:policy/ReadOnlyAccess" // AWS managed policies exist in every partition
	orFail(iamc.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{RoleName: role.RoleName, PolicyArn: &readOnly}))
	attached := orFail(iamc.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: role.RoleName})).AttachedPolicies
	if len(attached) != 1 || *attached[0].PolicyArn != readOnly {
		t.Errorf("attached = %+v", attached)
	}
	for _, p := range orFail(iamc.ListPolicies(ctx, &iam.ListPoliciesInput{Scope: iamtypes.PolicyScopeTypeAws})).Policies {
		if !strings.HasPrefix(*p.Arn, "arn:aws-us-gov:iam::aws:policy/") {
			t.Errorf("managed policy %s", *p.Arn)
		}
	}
	assumed := orFail(stsc.AssumeRole(ctx, &sts.AssumeRoleInput{RoleArn: role.Arn, RoleSessionName: aws.String("ss")}))
	expect("assumed role", *assumed.AssumedRoleUser.Arn, "arn:aws-us-gov:sts::000000000000:assumed-role/app/ss")

	sqsc := sqs.NewFromConfig(cfg)
	q := orFail(sqsc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")}))
	attrs := orFail(sqsc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: q.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}}))
	expect("queue", attrs.Attributes["QueueArn"], "arn:aws-us-gov:sqs:us-gov-west-1:000000000000:jobs")
	topic := orFail(sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("events")}))
	expect("topic", *topic.TopicArn, "arn:aws-us-gov:sns:us-gov-west-1:000000000000:events")
	lc := lambda.NewFromConfig(cfg)
	fn := orFail(lc.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("fn"), Role: role.Arn,
		Runtime: lambdatypes.RuntimePython312, Handler: aws.String("index.handler"), Code: &lambdatypes.FunctionCode{ZipFile: pyZip(whoAmI)}}))
	expect("function", *fn.FunctionArn, "arn:aws-us-gov:lambda:us-gov-west-1:000000000000:function:fn")
	if _, err := exec.LookPath("python3"); err == nil { // the function runs as its execution role
		out := orFail(lc.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("fn")}))
		expect("execution role", string(out.Payload), `"arn:aws-us-gov:sts::000000000000:assumed-role/app/fn"`)
	}

	cfg.Region = "cn-north-1"
	expect("caller in China", *orFail(sts.NewFromConfig(cfg).GetCallerIdentity(ctx, nil)).Arn, "arn:aws-cn:iam::000000000000:root")
	cf := cloudformation.NewFromConfig(cfg)
	orFail(cf.CreateStack(ctx, &cloudformation.CreateStackInput{StackName: aws.String("part"), TemplateBody: aws.String(`{"Resources": {"T": {"Type": "AWS::SNS::Topic"}},
		"Outputs": {"P": {"Value": {"Fn::Sub": "${AWS::Partition} ${AWS::URLSuffix}"}}, "T": {"Value": {"Ref": "T"}}}}`)}))
	st := orFail(cf.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String("part")})).Stacks[0]
	for i := 0; i < 100 && strings.HasSuffix(string(st.StackStatus), "_IN_PROGRESS"); i++ {
		time.Sleep(20 * time.Millisecond)
		st = orFail(cf.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String("part")})).Stacks[0]
	}
	out := map[string]string{}
	for _, o := range st.Outputs {
		out[*o.OutputKey] = *o.OutputValue
	}
	if out["P"] != "aws-cn amazonaws.com.cn" || !strings.HasPrefix(out["T"], "arn:aws-cn:sns:cn-north-1:000000000000:") {
		t.Errorf("stack %s, outputs %v", st.StackStatus, out)
	}
}

// TestLocalhostHostnames reaches pogi on *.localhost names: S3 in virtual-host
// style, an SDK host prefix, and API Gateway and Lambda function URL hosts.
func TestLocalhostHostnames(t *testing.T) {
	if _, err := net.LookupHost("probe.aws.localhost"); err != nil {
		t.Skipf("*.localhost names do not resolve here: %v", err)
	}
	ctx := context.Background()
	srv := pogi.Start(t, pogi.Config{Hostname: "aws.localhost"})
	port := srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	cfg := awstest.Config(t, "http://aws.localhost:"+port)

	s3c := s3.NewFromConfig(cfg) // UsePathStyle stays false: <bucket>.aws.localhost
	orFail(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("vhost")}))
	orFail(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("vhost"), Key: aws.String("a.txt"), Body: strings.NewReader("hi")}))
	obj := orFail(s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("vhost"), Key: aws.String("a.txt")}))
	if b, _ := io.ReadAll(obj.Body); string(b) != "hi" {
		t.Fatalf("object = %q", b)
	}
	if l := orFail(s3c.ListBuckets(ctx, nil)); len(l.Buckets) != 1 || *l.Buckets[0].Name != "vhost" { // aws.localhost itself
		t.Fatalf("buckets = %+v", l.Buckets)
	}
	if e := srv.Hub.Requests.Query(0, 0, "s3", "PutObject", false); len(e) != 1 || !strings.HasPrefix(e[0].Path, "/a.txt") {
		t.Fatalf("PutObject was not virtual-host style: %+v", e)
	}

	// StartSyncExecution goes to sync-aws.localhost (the SDK's host prefix).
	sf := sfn.NewFromConfig(cfg)
	m := orFail(sf.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("quick"), Type: sfntypes.StateMachineTypeExpress,
		RoleArn: aws.String("arn:aws:iam::000000000000:role/sfn"), Definition: aws.String(`{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`)}))
	if run := orFail(sf.StartSyncExecution(ctx, &sfn.StartSyncExecutionInput{StateMachineArn: m.StateMachineArn})); run.Status != sfntypes.SyncExecutionStatusSucceeded || *run.Output != `{"ok":true}` {
		t.Fatalf("sync execution = %s %v", run.Status, aws.ToString(run.Output))
	}

	// API Gateway invoke through <api>.execute-api.localhost
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "backend "+r.URL.Path) }))
	defer backend.Close()
	gw := apigatewayv2.NewFromConfig(cfg)
	api := orFail(gw.CreateApi(ctx, &apigatewayv2.CreateApiInput{Name: aws.String("web"), ProtocolType: apitypes.ProtocolTypeHttp}))
	in := orFail(gw.CreateIntegration(ctx, &apigatewayv2.CreateIntegrationInput{ApiId: api.ApiId, IntegrationType: apitypes.IntegrationTypeHttpProxy,
		IntegrationUri: aws.String(backend.URL + "/{proxy}"), IntegrationMethod: aws.String("ANY"), PayloadFormatVersion: aws.String("1.0")}))
	orFail(gw.CreateRoute(ctx, &apigatewayv2.CreateRouteInput{ApiId: api.ApiId, RouteKey: aws.String("ANY /{proxy+}"), Target: aws.String("integrations/" + *in.IntegrationId)}))
	orFail(gw.CreateStage(ctx, &apigatewayv2.CreateStageInput{ApiId: api.ApiId, StageName: aws.String("$default"), AutoDeploy: aws.Bool(true)}))
	if want := "http://" + *api.ApiId + ".execute-api.localhost:" + port; *api.ApiEndpoint != want {
		t.Fatalf("endpoint = %s, want %s", *api.ApiEndpoint, want)
	}
	if code, body := call(t, "GET", *api.ApiEndpoint+"/hello"); code != 200 || body != "backend /hello" {
		t.Fatalf("invoke = %d %q", code, body)
	}

	// Lambda function URLs through <url-id>.lambda-url.localhost
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	lc := lambda.NewFromConfig(cfg)
	orFail(lc.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("web"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("index.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &lambdatypes.FunctionCode{ZipFile: pyZip("def handler(event, context):\n    return {'statusCode': 200, 'body': 'url ' + event['rawPath']}\n")}}))
	u := orFail(lc.CreateFunctionUrlConfig(ctx, &lambda.CreateFunctionUrlConfigInput{FunctionName: aws.String("web"), AuthType: lambdatypes.FunctionUrlAuthTypeNone}))
	id, _, _ := strings.Cut(strings.TrimPrefix(*u.FunctionUrl, "http://"), ".")
	if code, body := call(t, "GET", "http://"+id+".lambda-url.localhost:"+port+"/hi"); code != 200 || body != "url /hi" {
		t.Fatalf("function URL = %d %q", code, body)
	}
}

// TestBucketCORSAndBodyLimit: a bucket's CORS rules decide for it, pogi's
// policy for everything else, and bodies over maxUploadMB get 413.
func TestBucketCORSAndBodyLimit(t *testing.T) {
	ctx := context.Background()
	// The front door lets the bucket's origin in first: a foreign origin needs server.cors.origins.
	srv := pogi.Start(t, pogi.Config{MaxUploadMB: 1, CORSOrigins: []string{"https://app.example"}})
	s3c := s3.NewFromConfig(awstest.Config(t, srv.URL), func(o *s3.Options) { o.UsePathStyle = true })
	orFail(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("web")}))
	orFail(s3c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String("web"), CORSConfiguration: &s3types.CORSConfiguration{
		CORSRules: []s3types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"https://app.example"}}}}}))
	for _, c := range []struct {
		path, origin string
		status       int
		allow        string
	}{
		{"/web/a.txt", "https://app.example", 200, "https://app.example"},       // the bucket's rule
		{"/web/a.txt", "http://localhost:3000", 403, ""},                        // pogi's defaults do not widen it
		{"/plain/a.txt", "http://localhost:3000", 204, "http://localhost:3000"}, // no bucket CORS: pogi's policy
		{"/", "http://localhost:3000", 204, "http://localhost:3000"},            // preflights of the other APIs land on S3
		{"/", "https://evil.example", 403, ""},
	} {
		req, _ := http.NewRequest("OPTIONS", srv.URL+c.path, nil)
		req.Header.Set("Origin", c.origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		res := orFail(http.DefaultClient.Do(req))
		res.Body.Close()
		if res.StatusCode != c.status || res.Header.Get("Access-Control-Allow-Origin") != c.allow {
			t.Errorf("OPTIONS %s from %s = %d %q", c.path, c.origin, res.StatusCode, res.Header.Get("Access-Control-Allow-Origin"))
		}
	}

	_, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("web"), Key: aws.String("big"), Body: bytes.NewReader(make([]byte, 3<<20))})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "EntityTooLarge" {
		t.Fatalf("3 MiB with maxUploadMB 1: %v", err)
	}
}

// TestBindGuard: pogi refuses to listen beyond loopback unless allowed.
func TestBindGuard(t *testing.T) {
	for _, f := range []string{"/.dockerenv", "/run/.containerenv"} {
		if _, err := os.Stat(f); err == nil {
			t.Skip("inside a container pogi may listen on any address")
		}
	}
	err := pogi.New(pogi.Config{Host: "0.0.0.0", DataDir: t.TempDir()}, nil).Run(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "--allow-external") {
		t.Fatalf("listening on 0.0.0.0: %v", err)
	}
	ln := orFail(net.Listen("tcp", "127.0.0.1:0"))
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a loopback name passes; the cancelled context then stops the server
	if err := pogi.New(pogi.Config{Host: "localhost", Port: port, NoPorts: true, DataDir: t.TempDir()}, nil).Run(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

// TestSMTPPort: SES's SMTP port opens in tests only when asked for, and a
// relay that is not smtp:// stops pogi from starting.
func TestSMTPPort(t *testing.T) {
	if srv := pogi.Start(t); srv.Ports["ses"] != "" {
		t.Fatalf("SMTP is on by default in tests: %s", srv.Ports["ses"])
	}
	ln := orFail(net.Listen("tcp", "127.0.0.1:0"))
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	srv := pogi.Start(t, pogi.Config{SMTPPort: port})
	c := orFail(smtp.Dial(strings.TrimPrefix(srv.Ports["ses"], "smtp://")))
	defer c.Close()
	if err := c.Hello("test"); err != nil {
		t.Fatal(err)
	}
	err := pogi.New(pogi.Config{SMTPRelay: "mail:25", DataDir: t.TempDir()}, nil).Run(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "SMTP relay") {
		t.Fatalf("a relay that is not smtp://: %v", err)
	}
}
