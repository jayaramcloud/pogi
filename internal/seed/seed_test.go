package seed_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ps "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	gcs "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/seed"
	"go.yaml.in/yaml/v3"
)

var ctx = context.Background()

const pogiYAML = `identities:
  aws:
    users:
      - {name: dev, accessKey: AKIADEV0000000000001, secretKey: devsecret, groups: [devs],
         policies: ["arn:aws:iam::aws:policy/ReadOnlyAccess"],
         inline: {logs: {Version: "2012-10-17", Statement: [{Effect: Allow, Action: "logs:*", Resource: "*"}]}}}
    roles:
      - {name: app, trust: lambda.amazonaws.com, policies: ["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"]}
resources:
  aws:
    s3: {buckets: [uploads, {name: logs, versioning: true, tags: {env: dev}}]}
    sqs: {queues: [jobs-dlq, {name: jobs, visibilityTimeout: 45, deadLetter: {queue: jobs-dlq, maxReceiveCount: 3}}, events.fifo]}
    sns: {topics: [{name: orders, subscriptions: [{protocol: sqs, endpoint: jobs}]}]}
    dynamodb: {tables: [{name: users, key: [pk, "sk:N"], indexes: [{name: byEmail, key: email}], ttl: expires, stream: true}]}
    lambda: {functions: [{name: hello, runtime: python3.12, handler: app.handler, code: ./fn, environment: {MODE: test}}]}
    secretsmanager: {secrets: {db-password: hunter2}}
    ssm: {parameters: {"/app/url": "http://localhost"}}
    kms: {keys: [app]}
    logs: {groups: [{name: /app/api, retentionDays: 7}]}
    kinesis: {streams: [{name: clicks, shards: 2}]}
    events: {buses: [shop]}
    stepfunctions: {stateMachines: [{name: flow, definition: {StartAt: Done, States: {Done: {Type: Succeed}}}}]}
  gcp:
    pubsub: {topics: [orders], subscriptions: [{name: worker, topic: orders, ackDeadline: 30}]}
    storage: {buckets: [assets]}
    secretmanager: {secrets: {api-key: abc123}}
    firestore: {documents: {users/alice: {name: Alice, age: 30}}}
  azure:
    blob: {containers: [images]}
    queue: {queues: [tasks]}
    table: {tables: [events]}
    keyvault: {secrets: {vault-password: hunter2}}
    appconfig: {stores: [config], keyvalues: {color: {value: blue, store: config, label: prod, contentType: text/plain, tags: {team: core}}, plain: hello}}
    cosmos: {databases: [{name: app, containers: [{name: items, partitionKey: /pk}]}]}
  typed:
    - {type: "AWS::SNS::Topic", properties: {TopicName: alerts}}
    - {type: "AWS::SQS::Queue"}
stacks:
  - {name: base, template: ./stack.yaml, parameters: {Env: dev}}
`

const stackYAML = `Parameters:
  Env: {Type: String}
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "base-${Env}"
`

// load parses src as pogi.yaml in a new directory that also holds a
// function, a stack template and a broken one (bad.yaml).
func load(t *testing.T, src string) *config.File {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"fn/app.py": "def handler(event, context):\n    return 'hi'\n", "stack.yaml": stackYAML,
		"bad.yaml": strings.Replace(stackYAML, "base-${Env}", "not a queue name!", 1)} {
		os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	f := &config.File{Path: filepath.Join(dir, "pogi.yaml")}
	if err := yaml.Unmarshal([]byte(src), f); err != nil {
		t.Fatal(err)
	}
	return f
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// has reports whether a service lists a resource whose row mentions name.
func has(h *core.Hub, svc, name string) bool {
	for _, tb := range h.Service(svc).(core.Browsable).Resources() {
		for _, row := range tb.Rows {
			if strings.Contains(strings.Join(row, " "), name) {
				return true
			}
		}
	}
	return false
}

// inventory counts the rows of every resource table, to spot duplicates.
func inventory(srv *pogi.Running) string {
	var b strings.Builder
	for _, svc := range srv.Hub.Services() {
		if br, ok := svc.(core.Browsable); ok {
			for _, tb := range br.Resources() {
				fmt.Fprintf(&b, "%s.%s=%d ", svc.Info().Name, tb.Name, len(tb.Rows))
			}
		}
	}
	return b.String()
}

func get(t *testing.T, u string) string {
	t.Helper()
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("Authorization", "Bearer x")
	res := must(http.DefaultClient.Do(req))
	defer res.Body.Close()
	b := must(io.ReadAll(res.Body))
	if res.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s", u, res.StatusCode, b)
	}
	return string(b)
}

func TestSeed(t *testing.T) {
	f := load(t, pogiYAML)
	srv := pogi.Start(t, pogi.Config{File: f})
	srv.Setenv(t)
	cfg := awstest.Config(t, srv.URL)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	sqsc, snsc, iamc := sqs.NewFromConfig(cfg), sns.NewFromConfig(cfg), iam.NewFromConfig(cfg)
	psc := must(ps.NewClient(ctx, "pogi"))
	t.Cleanup(func() { psc.Close() })
	gcsc := must(gcs.NewClient(ctx))
	blobc := must(azblob.NewClientFromConnectionString(os.Getenv("AZURE_STORAGE_CONNECTION_STRING"), nil))
	insecure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	cosmosc := must(azcosmos.NewClientWithKey(os.Getenv("COSMOS_ENDPOINT"), must(azcosmos.NewKeyCredential(os.Getenv("COSMOS_KEY"))),
		&azcosmos.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: insecure}}))
	queueURL := func(name string) string {
		return *must(sqsc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})).QueueUrl
	}

	check := func(when string) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s: %v", when, r)
			}
		}()
		// AWS, through the SDKs
		must(s3c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("uploads")}))
		if v := must(s3c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String("logs")})); v.Status != "Enabled" {
			t.Errorf("%s: logs versioning = %q", when, v.Status)
		}
		attrs := must(sqsc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL("jobs")),
			AttributeNames: []sqstypes.QueueAttributeName{"All"}})).Attributes
		if !strings.Contains(attrs["RedrivePolicy"], ":jobs-dlq") || attrs["VisibilityTimeout"] != "45" {
			t.Errorf("%s: jobs attributes = %v", when, attrs)
		}
		queueURL("events.fifo")
		queueURL("pogi-seed-Queue2") // an unnamed typed resource gets a stable name
		subs := must(snsc.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:orders")}))
		if len(subs.Subscriptions) != 1 || *subs.Subscriptions[0].Endpoint != "arn:aws:sqs:us-east-1:000000000000:jobs" {
			t.Errorf("%s: orders subscriptions = %+v", when, subs.Subscriptions)
		}
		must(snsc.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:alerts")}))
		tbl := must(dynamodb.NewFromConfig(cfg).DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("users")})).Table
		if len(tbl.KeySchema) != 2 || *tbl.KeySchema[1].AttributeName != "sk" || len(tbl.GlobalSecondaryIndexes) != 1 ||
			tbl.StreamSpecification == nil || tbl.BillingModeSummary.BillingMode != "PAY_PER_REQUEST" {
			t.Errorf("%s: users table = %+v", when, tbl)
		}
		fn := must(lambda.NewFromConfig(cfg).GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String("hello")})).Configuration
		if *fn.Role != "arn:aws:iam::000000000000:role/pogi-seed-lambda" || fn.Environment.Variables["MODE"] != "test" || fn.CodeSize == 0 {
			t.Errorf("%s: hello = %+v", when, fn)
		}
		st := must(cloudformation.NewFromConfig(cfg).DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String("base")})).Stacks[0]
		if st.StackStatus != "CREATE_COMPLETE" {
			t.Errorf("%s: stack base = %s %s", when, st.StackStatus, aws.ToString(st.StackStatusReason))
		}
		queueURL("base-dev")
		role := must(iamc.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String("app")})).Role
		if doc, _ := url.QueryUnescape(*role.AssumeRolePolicyDocument); !strings.Contains(doc, `"Service":"lambda.amazonaws.com"`) {
			t.Errorf("%s: app trust = %s", when, doc)
		}
		if g := must(iamc.ListGroupsForUser(ctx, &iam.ListGroupsForUserInput{UserName: aws.String("dev")})).Groups; len(g) != 1 || *g[0].GroupName != "devs" {
			t.Errorf("%s: dev groups = %+v", when, g)
		}
		// the fixed access key is dev's
		id := must(sts.NewFromConfig(awstest.ConfigAs(t, srv.URL, "AKIADEV0000000000001", "devsecret", "")).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}))
		if *id.Arn != "arn:aws:iam::000000000000:user/dev" {
			t.Errorf("%s: caller = %s", when, *id.Arn)
		}
		for svc, name := range map[string]string{"secretsmanager": "db-password", "ssm": "/app/url", "kms": "alias/app", "logs": "/app/api",
			"kinesis": "clicks", "events": "shop", "stepfunctions": "flow", "azqueue": "tasks", "aztable": "events", "keyvault": "vault-password",
			"azappconfig": "config",
			"firestore":   "users/alice", "secretmanager": "api-key"} {
			if !has(srv.Hub, svc, name) {
				t.Errorf("%s: %s has no %s", when, svc, name)
			}
		}

		// Google Cloud and Azure, through theirs
		sub := must(psc.SubscriptionAdminClient.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: "projects/pogi/subscriptions/worker"}))
		if sub.Topic != "projects/pogi/topics/orders" || sub.AckDeadlineSeconds != 30 {
			t.Errorf("%s: worker = %v", when, sub)
		}
		must(gcsc.Bucket("assets").Attrs(ctx))
		if b := get(t, srv.URL+"/v1/projects/pogi/secrets/api-key/versions/latest:access"); !strings.Contains(b, "YWJjMTIz") {
			t.Errorf("%s: api-key = %s", when, b)
		}
		must(blobc.ServiceClient().NewContainerClient("images").GetProperties(ctx, nil))
		ctr := must(must(cosmosc.NewContainer("app", "items")).Read(ctx, nil))
		if p := ctr.ContainerProperties.PartitionKeyDefinition.Paths; len(p) != 1 || p[0] != "/pk" {
			t.Errorf("%s: items partition key = %v", when, p)
		}
		if b := get(t, srv.URL+"/secrets/vault-password?api-version=7.4"); !strings.Contains(b, "hunter2") {
			t.Errorf("%s: vault-password = %s", when, b)
		}
		if b := get(t, srv.URL+"/_azure/appconfig/config/kv/color?label=prod&api-version=1.0"); !strings.Contains(b, `"value":"blue"`) || !strings.Contains(b, `"team":"core"`) {
			t.Errorf("%s: app configuration color = %s", when, b)
		}
		if b := get(t, srv.URL+"/_azure/appconfig/default/kv/plain?api-version=1.0"); !strings.Contains(b, `"value":"hello"`) {
			t.Errorf("%s: app configuration plain = %s", when, b)
		}
	}
	check("seeded")

	// seeding again changes nothing
	before := inventory(srv)
	if err := seed.Apply(srv.Hub, f); err != nil {
		t.Fatal(err)
	}
	if after := inventory(srv); after != before {
		t.Fatalf("applying twice changed resources:\n%s\n%s", before, after)
	}
	if n := len(must(iamc.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: aws.String("dev")})).AccessKeyMetadata); n != 1 {
		t.Errorf("dev has %d access keys", n)
	}

	// a partial reset brings back that service's resources only
	must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("extra")}))
	srv.Reset("s3", "iam")
	if _, err := s3c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("extra")}); err == nil {
		t.Error("extra survived the s3 reset")
	}
	check("after reset s3, iam")
	srv.Reset()
	check("after a full reset")

	// MCP's seed tool takes a resources fragment
	if err := srv.Hub.Seed([]byte("aws:\n  s3: {buckets: [fragment]}\n")); err != nil {
		t.Fatal(err)
	}
	must(s3c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("fragment")}))
	if err := srv.Hub.Seed([]byte("aws:\n  s3: {buckets: [{name: x, colour: red}]}\n")); err == nil || !strings.HasPrefix(err.Error(), `seed:2: aws.s3.buckets x: unknown property "colour"`) {
		t.Errorf("bad fragment: %v", err)
	}
	if err := srv.Hub.Seed([]byte("resources: {aws: {}}")); err == nil {
		t.Error("a fragment with resources: on top was accepted")
	}
}

// Run seeds before it reports ready, and stops with file:line on a bad file.
func TestRunSeedsBeforeReady(t *testing.T) {
	run := func(src string) (seeded bool, err error) {
		ln := must(net.Listen("tcp", "127.0.0.1:0"))
		port := ln.Addr().(*net.TCPAddr).Port
		ln.Close()
		srv := pogi.New(pogi.Config{Port: port, NoPorts: true, NoTLS: true, DataDir: t.TempDir(), File: load(t, src)}, nil)
		c, cancel := context.WithCancel(ctx)
		defer cancel()
		err = srv.Run(c, func(string) { seeded = has(srv.Hub, "s3", "early"); cancel() })
		return seeded, err
	}
	if seeded, err := run("resources: {aws: {s3: {buckets: [early]}}}\n"); err != nil || !seeded {
		t.Errorf("good file: seeded %v, err %v", seeded, err)
	}
	if _, err := run("resources:\n  aws:\n    s3: {buckets: [{name: b, bad: 1}]}\n"); err == nil || !strings.Contains(err.Error(), "pogi.yaml:3: ") {
		t.Errorf("bad file: %v", err)
	}
}

func TestErrorsNameTheLine(t *testing.T) {
	srv := pogi.Start(t)
	for _, tc := range []struct{ src, want string }{
		{"resources:\n  aws:\n    s3:\n      buckets:\n        - {name: b, versoning: true}\n", `pogi.yaml:5: aws.s3.buckets b: unknown property "versoning" (allowed: versioning, tags)`},
		{"resources:\n  aws:\n    dynamodb: {tables: [{name: t, key: [a, b, c]}]}\n", "pogi.yaml:3: aws.dynamodb.tables t: key must be a name or [hash, range]"},
		{"resources:\n  aws:\n    s3: {tubs: [x]}\n", "pogi.yaml:3: pogi cannot seed aws.s3.tubs"},
		{"resources:\n  typed:\n    - {type: \"Google::Storage::Bucket\"}\n", "pogi.yaml:3: Google::Storage::Bucket: typed google/azure resources arrive with the generic engine"},
		{"\nstacks:\n  - {name: bad, template: ./bad.yaml, parameters: {Env: x}}\n", "pogi.yaml:3: stack bad: ROLLBACK_COMPLETE: Queue: InvalidParameterValueException"},
	} {
		if err := seed.Apply(srv.Hub, load(t, tc.src)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("got %v\nwant …%s…", err, tc.want)
		}
	}
}
