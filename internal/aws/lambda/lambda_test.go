package lambda_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
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

func zipFiles(files map[string]string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range files {
		w, _ := zw.Create(name)
		io.WriteString(w, body)
	}
	zw.Close()
	return buf.Bytes()
}

func setup(t *testing.T) (*lambda.Client, *sqs.Client, string) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	return lambda.NewFromConfig(cfg), sqs.NewFromConfig(cfg), url
}

func need(t *testing.T, bin string) {
	if _, err := exec.LookPath(bin); err != nil {
		t.Skipf("%s not installed", bin)
	}
}

const pyApp = `
import json, os, time, urllib.request

def handler(event, context):
    if event.get("fail"):
        raise ValueError("boom")
    if event.get("sleep"):
        time.sleep(event["sleep"])
    if event.get("health"):
        return json.loads(urllib.request.urlopen(os.environ["AWS_ENDPOINT_URL"] + "/_localstack/health").read())
    print("hello from python", json.dumps(event))
    return {"echo": event, "fn": context.function_name, "greeting": os.environ.get("GREETING"),
            "region": os.environ["AWS_REGION"], "left": context.get_remaining_time_in_millis() > 0}
`

func TestPythonFunction(t *testing.T) {
	need(t, "python3")
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("py"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": pyApp})}}))

	out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("py"), Payload: []byte(`{"n":1}`), LogType: types.LogTypeTail}))
	var res map[string]any
	json.Unmarshal(out.Payload, &res)
	if out.FunctionError != nil || res["fn"] != "py" || res["region"] != "us-east-1" || res["left"] != true {
		t.Fatalf("invoke = %s (error %v)", out.Payload, aws.ToString(out.FunctionError))
	}
	logs, _ := base64.StdEncoding.DecodeString(aws.ToString(out.LogResult))
	if !strings.Contains(string(logs), "hello from python") || !strings.Contains(string(logs), "REPORT RequestId") {
		t.Fatalf("logs = %s", logs)
	}

	fail := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("py"), Payload: []byte(`{"fail":true}`)}))
	if aws.ToString(fail.FunctionError) != "Unhandled" || !strings.Contains(string(fail.Payload), "boom") || !strings.Contains(string(fail.Payload), "ValueError") {
		t.Fatalf("error invoke = %s %v", fail.Payload, fail.FunctionError)
	}

	// The function's own AWS calls come back to pogi.
	health := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("py"), Payload: []byte(`{"health":true}`)}))
	if !strings.Contains(string(health.Payload), `"lambda": "running"`) && !strings.Contains(string(health.Payload), `"lambda":"running"`) {
		t.Fatalf("health from inside the function = %s", health.Payload)
	}

	must(c.UpdateFunctionConfiguration(ctx, &lambda.UpdateFunctionConfigurationInput{FunctionName: aws.String("py"), Timeout: aws.Int32(1),
		Environment: &types.Environment{Variables: map[string]string{"GREETING": "hi"}}}))
	out = must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("py"), Payload: []byte(`{}`)}))
	if !strings.Contains(string(out.Payload), `"greeting": "hi"`) {
		t.Fatalf("env not applied: %s", out.Payload)
	}
	slow := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("py"), Payload: []byte(`{"sleep":3}`)}))
	if aws.ToString(slow.FunctionError) == "" || !strings.Contains(string(slow.Payload), "Task timed out after 1.00 seconds") {
		t.Fatalf("timeout = %s", slow.Payload)
	}

	v := must(c.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("py")}))
	must(c.CreateAlias(ctx, &lambda.CreateAliasInput{FunctionName: aws.String("py"), Name: aws.String("live"), FunctionVersion: v.Version}))
	viaAlias := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("py"), Qualifier: aws.String("live"), Payload: []byte(`{"a":2}`)}))
	if aws.ToString(viaAlias.ExecutedVersion) != "1" {
		t.Fatalf("alias executed version = %v", aws.ToString(viaAlias.ExecutedVersion))
	}
	must(c.AddPermission(ctx, &lambda.AddPermissionInput{FunctionName: aws.String("py"), StatementId: aws.String("s3"),
		Action: aws.String("lambda:InvokeFunction"), Principal: aws.String("s3.amazonaws.com")}))
	if p := must(c.GetPolicy(ctx, &lambda.GetPolicyInput{FunctionName: aws.String("py")})); !strings.Contains(*p.Policy, "s3.amazonaws.com") {
		t.Fatalf("policy = %s", *p.Policy)
	}
	must(c.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String("py")}))
	_, err := c.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String("py")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("want ResourceNotFoundException, got %v", err)
	}
}

func TestNodeFunctions(t *testing.T) {
	need(t, "node")
	c, _, _ := setup(t)
	for name, files := range map[string]map[string]string{
		"esm":      {"index.mjs": `export const handler = async (event, context) => ({ hello: event.who, id: context.awsRequestId.length > 0 });`},
		"callback": {"lib/main.js": `exports.handler = (event, context, cb) => { console.log("cjs"); cb(null, { hello: event.who }); };`},
	} {
		handler := "index.handler"
		if name == "callback" {
			handler = "lib/main.handler"
		}
		must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: types.RuntimeNodejs20x,
			Handler: aws.String(handler), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipFiles(files)}}))
		out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String(name), Payload: []byte(`{"who":"pogi"}`)}))
		if !strings.Contains(string(out.Payload), `"hello":"pogi"`) || out.FunctionError != nil {
			t.Fatalf("%s: %s %v", name, out.Payload, aws.ToString(out.FunctionError))
		}
	}
}

// The function forwards each SQS record to another queue with a raw JSON-protocol call.
const forwarder = `
import json, os, urllib.request

def handler(event, context):
    for r in event["Records"]:
        req = urllib.request.Request(os.environ["AWS_ENDPOINT_URL"],
            data=json.dumps({"QueueUrl": "http://localhost/000000000000/out", "MessageBody": "got:" + r["body"]}).encode(),
            headers={"X-Amz-Target": "AmazonSQS.SendMessage", "Content-Type": "application/x-amz-json-1.0"})
        urllib.request.urlopen(req).read()
    return {}
`

func TestSQSEventSourceAndDestinations(t *testing.T) {
	need(t, "python3")
	c, q, _ := setup(t)
	in := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("in")}))
	out := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("out")}))
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("fwd"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": forwarder})}}))
	must(c.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{FunctionName: aws.String("fwd"),
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:in"), BatchSize: aws.Int32(5),
		FilterCriteria: &types.FilterCriteria{Filters: []types.Filter{{Pattern: aws.String(`{"body":{"keep":[true]}}`)}}}}))
	must(q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: in.QueueUrl, MessageBody: aws.String(`{"keep":false}`)}))
	must(q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: in.QueueUrl, MessageBody: aws.String(`{"keep":true}`)}))
	got := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: out.QueueUrl, WaitTimeSeconds: 10}))
	if len(got.Messages) != 1 || *got.Messages[0].Body != `got:{"keep":true}` {
		t.Fatalf("forwarded = %+v", got.Messages)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a := must(q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: in.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}}))
		if a.Attributes["ApproximateNumberOfMessages"] == "0" && a.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Async failures go to the OnFailure destination once retries are exhausted.
	failures := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("failures")}))
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("bad"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "def handler(e, c):\n    raise RuntimeError('nope')\n"})}}))
	must(c.PutFunctionEventInvokeConfig(ctx, &lambda.PutFunctionEventInvokeConfigInput{FunctionName: aws.String("bad"), MaximumRetryAttempts: aws.Int32(0),
		DestinationConfig: &types.DestinationConfig{OnFailure: &types.OnFailure{Destination: aws.String("arn:aws:sqs:us-east-1:000000000000:failures")}}}))
	res := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("bad"), InvocationType: types.InvocationTypeEvent, Payload: []byte(`{"x":1}`)}))
	if res.StatusCode != 202 {
		t.Fatalf("async status = %d", res.StatusCode)
	}
	f := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: failures.QueueUrl, WaitTimeSeconds: 10}))
	if len(f.Messages) != 1 || !strings.Contains(*f.Messages[0].Body, "RetriesExhausted") || !strings.Contains(*f.Messages[0].Body, "nope") {
		t.Fatalf("failure destination = %+v", f.Messages)
	}
}

func TestFunctionURLAndHotReload(t *testing.T) {
	need(t, "python3")
	c, _, url := setup(t)
	dir := t.TempDir()
	write := func(msg string) {
		code := "import json\ndef handler(event, context):\n    return {'statusCode': 201, 'headers': {'x-pogi': 'yes'}, 'body': '" + msg + " ' + event['rawPath']}\n"
		os.WriteFile(filepath.Join(dir, "app.py"), []byte(code), 0o644)
	}
	write("v1")
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("web"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{S3Bucket: aws.String("hot-reload"), S3Key: aws.String(dir)}}))
	cfg := must(c.CreateFunctionUrlConfig(ctx, &lambda.CreateFunctionUrlConfigInput{FunctionName: aws.String("web"), AuthType: types.FunctionUrlAuthTypeNone}))
	call := func() (int, string, string) {
		req, _ := http.NewRequest("GET", url+"/hello?x=1", nil)
		req.Host = strings.TrimSuffix(strings.TrimPrefix(*cfg.FunctionUrl, "http://"), "/")
		res := must(http.DefaultClient.Do(req))
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b), res.Header.Get("x-pogi")
	}
	if st, body, hdr := call(); st != 201 || body != "v1 /hello" || hdr != "yes" {
		t.Fatalf("url call = %d %q %q", st, body, hdr)
	}
	time.Sleep(10 * time.Millisecond)
	write("v2")
	os.Chtimes(filepath.Join(dir, "app.py"), time.Now().Add(time.Second), time.Now().Add(time.Second))
	if _, body, _ := call(); body != "v2 /hello" {
		t.Fatalf("hot reload = %q", body)
	}
}

// A custom runtime in shell: the provided.al2023 base image has bash and curl.
const shBootstrap = `#!/bin/sh
set -eu
api="http://$AWS_LAMBDA_RUNTIME_API/2018-06-01/runtime/invocation"
while true; do
  headers=$(mktemp)
  event=$(curl -sS -D "$headers" "$api/next")
  id=$(grep -i '^lambda-runtime-aws-request-id:' "$headers" | tr -d '\r' | cut -d' ' -f2)
  echo "hello from docker $event"
  health=$(curl -sS "$AWS_ENDPOINT_URL/_localstack/health")
  curl -sS -X POST "$api/$id/response" -d "{\"echo\":$event,\"region\":\"$AWS_REGION\",\"health\":$health}" >/dev/null
done
`

// TestDockerFunction runs a function in its AWS base image through the
// container engine: the Runtime API, pogi's endpoint and the logs.
func TestDockerFunction(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("sh"), Runtime: types.RuntimeProvidedal2023,
		Handler: aws.String("handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"), Timeout: aws.Int32(300),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"bootstrap": shBootstrap})}}))
	out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("sh"), Payload: []byte(`{"n":1}`), LogType: types.LogTypeTail}))
	var res struct {
		Echo   map[string]int
		Region string
		Health struct{ Services map[string]string }
	}
	if err := json.Unmarshal(out.Payload, &res); err != nil || out.FunctionError != nil || res.Echo["n"] != 1 || res.Region != "us-east-1" ||
		res.Health.Services["lambda"] != "running" {
		t.Fatalf("invoke = %s (error %v, %v)", out.Payload, aws.ToString(out.FunctionError), err)
	}
	logs, _ := base64.StdEncoding.DecodeString(aws.ToString(out.LogResult))
	if !strings.Contains(string(logs), `hello from docker {"n":1}`) || !strings.Contains(string(logs), "REPORT RequestId") {
		t.Fatalf("logs = %s", logs)
	}
	must(c.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String("sh")}))
	list, err := docker.New(docker.Options{}).List(ctx, docker.LabelService+"=lambda", docker.LabelResource+"=sh")
	if err != nil || len(list) != 0 {
		t.Fatalf("containers left after delete: %+v %v", list, err)
	}
}

// imageBootstrap answers with the handler the image's entrypoint passed on and its working directory.
const imageBootstrap = `#!/bin/sh
set -eu
api="http://$AWS_LAMBDA_RUNTIME_API/2018-06-01/runtime/invocation"
while true; do
  headers=$(mktemp)
  event=$(curl -sS -D "$headers" "$api/next")
  id=$(grep -i '^lambda-runtime-aws-request-id:' "$headers" | tr -d '\r' | cut -d' ' -f2)
  curl -sS -X POST "$api/$id/response" -d "{\"echo\":$event,\"handler\":\"$_HANDLER\",\"dir\":\"$(pwd)\"}" >/dev/null
done
`

// TestImageFunction builds an image on the provided.al2023 base image and
// runs it as a container image function, then with ImageConfig overrides.
func TestImageFunction(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	need(t, "docker")
	dir, image := t.TempDir(), "pogi-lambda-test:"+core.RandHex(4)
	os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM public.ecr.aws/lambda/provided:al2023\nCOPY bootstrap ${LAMBDA_RUNTIME_DIR}\nCMD [\"function.handler\"]\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "bootstrap"), []byte(imageBootstrap), 0o755)
	if out, err := exec.Command("docker", "build", "-q", "-t", image, dir).CombinedOutput(); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}
	t.Cleanup(func() { exec.Command("docker", "rmi", "-f", image).Run() })

	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("img"), PackageType: types.PackageTypeImage,
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Timeout: aws.Int32(60), Code: &types.FunctionCode{ImageUri: aws.String(image)}}))
	if fn := must(c.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String("img")})); aws.ToString(fn.Code.ImageUri) != image ||
		fn.Configuration.PackageType != types.PackageTypeImage {
		t.Fatalf("GetFunction = %+v %+v", fn.Code, fn.Configuration)
	}
	type result struct {
		Echo         map[string]int
		Handler, Dir string
	}
	invoke := func() (res result) {
		out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("img"), Payload: []byte(`{"n":1}`)}))
		if err := json.Unmarshal(out.Payload, &res); err != nil || out.FunctionError != nil || res.Echo["n"] != 1 {
			t.Fatalf("invoke = %s (error %v, %v)", out.Payload, aws.ToString(out.FunctionError), err)
		}
		return res
	}
	if res := invoke(); res.Handler != "function.handler" || res.Dir != "/var/task" {
		t.Fatalf("image defaults = %+v", res)
	}

	must(c.UpdateFunctionConfiguration(ctx, &lambda.UpdateFunctionConfigurationInput{FunctionName: aws.String("img"),
		ImageConfig: &types.ImageConfig{Command: []string{"other.handler"}, WorkingDirectory: aws.String("/tmp")}}))
	cfg := must(c.GetFunctionConfiguration(ctx, &lambda.GetFunctionConfigurationInput{FunctionName: aws.String("img")}))
	if ic := cfg.ImageConfigResponse; ic == nil || ic.ImageConfig == nil || strings.Join(ic.ImageConfig.Command, " ") != "other.handler" ||
		aws.ToString(ic.ImageConfig.WorkingDirectory) != "/tmp" {
		t.Fatalf("ImageConfigResponse = %+v", ic)
	}
	if res := invoke(); res.Handler != "other.handler" || res.Dir != "/tmp" {
		t.Fatalf("with ImageConfig = %+v", res)
	}
	must(c.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String("img")}))
	list, err := docker.New(docker.Options{}).List(ctx, docker.LabelService+"=lambda", docker.LabelResource+"=img")
	if err != nil || len(list) != 0 {
		t.Fatalf("containers left after delete: %+v %v", list, err)
	}
}

// Without a container engine, image functions (and runtimes pogi cannot run
// natively) are still created and described; invoking them is a clear 502.
func TestImageFunctionWithoutDocker(t *testing.T) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return lambdasvc.New(h) })
	h.Cfg.DockerHost = "unix://" + filepath.Join(t.TempDir(), "no-engine.sock")
	c := lambda.NewFromConfig(awstest.Config(t, url))
	role, image := aws.String("arn:aws:iam::000000000000:role/r"), "localhost:4510/app:latest"
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("img"), PackageType: types.PackageTypeImage, Role: role,
		Code: &types.FunctionCode{ImageUri: aws.String(image)}, ImageConfig: &types.ImageConfig{Command: []string{"app.handler"}}}))
	fn := must(c.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String("img")}))
	if ic := fn.Configuration.ImageConfigResponse; aws.ToString(fn.Code.ImageUri) != image || aws.ToString(fn.Code.ResolvedImageUri) == "" ||
		ic == nil || ic.ImageConfig == nil || strings.Join(ic.ImageConfig.Command, " ") != "app.handler" {
		t.Fatalf("GetFunction = %+v %+v", fn.Code, fn.Configuration)
	}
	var runtimeErr *types.InvalidRuntimeException
	if _, err := c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("img")}); !errors.As(err, &runtimeErr) || !strings.Contains(err.Error(), "Docker") {
		t.Fatalf("invoking an image without Docker: %v", err)
	}
	var bad *types.InvalidParameterValueException
	if _, err := c.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{FunctionName: aws.String("img"),
		ZipFile: zipFiles(map[string]string{"app.py": pyApp})}); !errors.As(err, &bad) {
		t.Fatalf("zip code for an image function: %v", err)
	}

	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("java"), Runtime: types.RuntimeJava21, Role: role,
		Handler: aws.String("example.Handler::handleRequest"), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"example/Handler.class": ""})}}))
	if _, err := c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("java")}); !errors.As(err, &runtimeErr) || !strings.Contains(err.Error(), "needs Docker") {
		t.Fatalf("invoking java21 without Docker: %v", err)
	}
}

// floci #2124: a permission is stored under its qualifier, and GetPolicy and RemovePermission read that scope.
// floci #4627: an account-id principal is stored as its root ARN, and PrincipalOrgID is kept.
func TestPermissionScopeAndPrincipal(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "def handler(e, c): pass\n"})}}))
	must(c.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("f")}))
	must(c.CreateAlias(ctx, &lambda.CreateAliasInput{FunctionName: aws.String("f"), Name: aws.String("live"), FunctionVersion: aws.String("1")}))
	add := &lambda.AddPermissionInput{FunctionName: aws.String("f"), Qualifier: aws.String("live"), StatementId: aws.String("s"), Action: aws.String("lambda:InvokeFunction"),
		Principal: aws.String("222222222222"), PrincipalOrgID: aws.String("o-1234567890")}
	out := must(c.AddPermission(ctx, add))
	for _, want := range []string{"function:f:live", "arn:aws:iam::222222222222:root", "o-1234567890"} {
		if !strings.Contains(aws.ToString(out.Statement), want) {
			t.Errorf("statement lacks %q: %s", want, aws.ToString(out.Statement))
		}
	}
	must(c.GetPolicy(ctx, &lambda.GetPolicyInput{FunctionName: aws.String("f"), Qualifier: aws.String("live")}))
	var notFound *types.ResourceNotFoundException
	if _, err := c.GetPolicy(ctx, &lambda.GetPolicyInput{FunctionName: aws.String("f")}); !errors.As(err, &notFound) {
		t.Errorf("unqualified GetPolicy = %v, want ResourceNotFoundException", err)
	}
	var conflict *types.ResourceConflictException
	if _, err := c.AddPermission(ctx, add); !errors.As(err, &conflict) {
		t.Errorf("same sid, same alias = %v, want ResourceConflictException", err)
	}
	add.Qualifier = nil
	must(c.AddPermission(ctx, add)) // the same sid on the unqualified function is another policy
	must(c.RemovePermission(ctx, &lambda.RemovePermissionInput{FunctionName: aws.String("f"), Qualifier: aws.String("live"), StatementId: aws.String("s")}))
	must(c.GetPolicy(ctx, &lambda.GetPolicyInput{FunctionName: aws.String("f")}))
}

// floci #2808: ListLayers pages with MaxItems and Marker and filters by runtime and architecture.
func TestListLayersFilters(t *testing.T) {
	c, _, _ := setup(t)
	for name, rt := range map[string]types.Runtime{"l1": types.RuntimePython313, "l2": types.RuntimeNodejs22x} {
		arch := types.ArchitectureArm64
		if name == "l2" {
			arch = types.ArchitectureX8664
		}
		must(c.PublishLayerVersion(ctx, &lambda.PublishLayerVersionInput{LayerName: aws.String(name), CompatibleRuntimes: []types.Runtime{rt},
			CompatibleArchitectures: []types.Architecture{arch}, Content: &types.LayerVersionContentInput{ZipFile: zipFiles(map[string]string{"python/a.py": ""})}}))
	}
	first := must(c.ListLayers(ctx, &lambda.ListLayersInput{MaxItems: aws.Int32(1)}))
	if len(first.Layers) != 1 || first.NextMarker == nil {
		t.Fatalf("page 1 = %d layers, marker %v", len(first.Layers), first.NextMarker)
	}
	second := must(c.ListLayers(ctx, &lambda.ListLayersInput{MaxItems: aws.Int32(1), Marker: first.NextMarker}))
	if len(second.Layers) != 1 || second.NextMarker != nil || aws.ToString(second.Layers[0].LayerName) == aws.ToString(first.Layers[0].LayerName) {
		t.Errorf("page 2 = %+v", second)
	}
	if o := must(c.ListLayers(ctx, &lambda.ListLayersInput{CompatibleRuntime: types.RuntimePython313})); len(o.Layers) != 1 || aws.ToString(o.Layers[0].LayerName) != "l1" {
		t.Errorf("CompatibleRuntime = %+v", o.Layers)
	}
	if o := must(c.ListLayers(ctx, &lambda.ListLayersInput{CompatibleArchitecture: types.ArchitectureX8664})); len(o.Layers) != 1 || aws.ToString(o.Layers[0].LayerName) != "l2" {
		t.Errorf("CompatibleArchitecture = %+v", o.Layers)
	}
	var bad *types.InvalidParameterValueException
	if _, err := c.ListLayers(ctx, &lambda.ListLayersInput{CompatibleRuntime: types.Runtime("bogus9.x")}); !errors.As(err, &bad) {
		t.Errorf("unknown CompatibleRuntime = %v, want InvalidParameterValueException", err)
	}
}

// floci #4732: a qualifier in the function name must agree with the Qualifier parameter.
func TestInvokeQualifierMismatch(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "def handler(e, c): return 1\n"})}}))
	must(c.PublishVersion(ctx, &lambda.PublishVersionInput{FunctionName: aws.String("f")}))
	var bad *types.InvalidParameterValueException
	if _, err := c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("f:1"), Qualifier: aws.String("2")}); !errors.As(err, &bad) {
		t.Errorf("f:1 with Qualifier=2 = %v, want InvalidParameterValueException", err)
	}
}

// floci #3314: the Python runtime names load errors like AWS does.
func TestPythonInitErrorTypes(t *testing.T) {
	need(t, "python3")
	c, _, _ := setup(t)
	for name, tc := range map[string]struct{ handler, code, want string }{
		"nomod":   {"nomod.handler", "def handler(e, c): return 1", "Runtime.ImportModuleError"},
		"nope":    {"app.nope", "def handler(e, c): return 1", "Runtime.HandlerNotFound"},
		"syntax":  {"app.handler", "def handler(e, c:\n  return 1", "Runtime.UserCodeSyntaxError"},
		"owntype": {"app.handler", "raise ValueError('x')", "ValueError"},
	} {
		must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f-" + name), Runtime: types.RuntimePython312, Handler: aws.String(tc.handler),
			Role: aws.String("arn:aws:iam::000000000000:role/r"), Timeout: aws.Int32(5), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": tc.code})}}))
		out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("f-" + name)}))
		var body struct {
			ErrorType string `json:"errorType"`
		}
		json.Unmarshal(out.Payload, &body)
		if body.ErrorType != tc.want {
			t.Errorf("%s: errorType = %q, want %q (%s)", name, body.ErrorType, tc.want, out.Payload)
		}
	}
}

// floci #525: after an invoke times out, the next one gets a fresh runtime, not the one that was killed.
func TestInvokeAfterTimeout(t *testing.T) {
	need(t, "python3")
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("slow"), Runtime: types.RuntimePython312, Handler: aws.String("index.handler"), Timeout: aws.Int32(1),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"index.py": "import time\nstate = []\ndef handler(e, c):\n    state.append(1)\n    if e.get('sleep'):\n        time.sleep(4)\n    return {'calls': len(state)}\n"})}}))
	for range 3 { // the failure was a race, so try a few times
		must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("slow"), Payload: []byte(`{"sleep":true}`)}))
		o := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("slow"), Payload: []byte(`{}`)}))
		if o.FunctionError != nil || !strings.Contains(string(o.Payload), `"calls": 1`) {
			t.Fatalf("invoke after a timeout: %v %s", aws.ToString(o.FunctionError), o.Payload)
		}
	}
}

// floci #632: ListFunctionEventInvokeConfigs lists the config that Put made.
func TestListEventInvokeConfigs(t *testing.T) {
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "def handler(e, c):\n    return e\n"})}}))
	if l := must(c.ListFunctionEventInvokeConfigs(ctx, &lambda.ListFunctionEventInvokeConfigsInput{FunctionName: aws.String("f")})); len(l.FunctionEventInvokeConfigs) != 0 {
		t.Fatalf("before put: %v", l.FunctionEventInvokeConfigs)
	}
	must(c.PutFunctionEventInvokeConfig(ctx, &lambda.PutFunctionEventInvokeConfigInput{FunctionName: aws.String("f"), MaximumRetryAttempts: aws.Int32(1)}))
	l := must(c.ListFunctionEventInvokeConfigs(ctx, &lambda.ListFunctionEventInvokeConfigsInput{FunctionName: aws.String("f")}))
	if len(l.FunctionEventInvokeConfigs) != 1 || aws.ToInt32(l.FunctionEventInvokeConfigs[0].MaximumRetryAttempts) != 1 ||
		!strings.HasSuffix(aws.ToString(l.FunctionEventInvokeConfigs[0].FunctionArn), ":function:f") {
		t.Fatalf("after put: %+v", l.FunctionEventInvokeConfigs)
	}
	must(c.DeleteFunctionEventInvokeConfig(ctx, &lambda.DeleteFunctionEventInvokeConfigInput{FunctionName: aws.String("f")}))
	if l := must(c.ListFunctionEventInvokeConfigs(ctx, &lambda.ListFunctionEventInvokeConfigsInput{FunctionName: aws.String("f")})); len(l.FunctionEventInvokeConfigs) != 0 {
		t.Fatalf("after delete: %v", l.FunctionEventInvokeConfigs)
	}
}

// floci #2813: a missing layer of the caller's own account is refused; another account's layer is taken on trust.
func TestMissingOwnLayer(t *testing.T) {
	c, _, _ := setup(t)
	mk := func(name, layer string) error {
		_, err := c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: types.RuntimePython312,
			Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
			Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": ""})}, Layers: []string{layer}})
		return err
	}
	var bad *types.InvalidParameterValueException
	if err := mk("own", "arn:aws:lambda:us-east-1:000000000000:layer:nope:1"); !errors.As(err, &bad) {
		t.Errorf("own missing layer: %v", err)
	}
	if err := mk("foreign", "arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV3:1"); err != nil {
		t.Errorf("foreign layer: %v", err)
	}
}
