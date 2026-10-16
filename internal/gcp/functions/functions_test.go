package functions_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	functionsv2 "cloud.google.com/go/functions/apiv2"
	"cloud.google.com/go/functions/apiv2/functionspb"
	ps "cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ok(err error) {
	if err != nil {
		panic(err)
	}
}

const parent = "projects/proj/locations/us-central1"

const mainPy = `import base64, json, os, urllib.request
import functions_framework

def put(name, body):
    req = urllib.request.Request(os.environ["STORAGE_EMULATOR_HOST"] + "/upload/storage/v1/b/results/o?uploadType=media&name=" + name,
                                 data=body.encode(), method="POST", headers={"Content-Type": "text/plain"})
    urllib.request.urlopen(req).read()

@functions_framework.http
def hello(request):
    if request.args.get("fail"):
        return ("nope", 418)
    return "hello %s from %s at %s" % (request.args.get("name", "world"), os.environ.get("GREETING", ""), request.path)

@functions_framework.cloud_event
def on_message(event):
    m = event.data["message"]
    put("pubsub-last", json.dumps({"type": event["type"], "source": event["source"],
        "data": base64.b64decode(m["data"]).decode(), "attributes": m.get("attributes", {})}))

@functions_framework.cloud_event
def on_object(event):
    put("storage-" + event.data["name"], event["type"] + " " + event.data["bucket"])
`

var (
	ffOnce sync.Once
	ffDir  string
)

// installed pip-installs the Functions Framework once, the way `pogi lambda-sdks` does.
func installed() bool {
	ffOnce.Do(func() {
		py, err := exec.LookPath("python3")
		if err != nil {
			return
		}
		dir, err := os.MkdirTemp("", "pogi-ff-")
		if err != nil {
			return
		}
		if out, err := exec.Command(py, "-m", "pip", "install", "-q", "--disable-pip-version-check", "--target", filepath.Join(dir, "python"), "functions-framework").CombinedOutput(); err != nil {
			os.RemoveAll(dir)
			println("pip install functions-framework failed:", string(out))
			return
		}
		ffDir = dir
	})
	return ffDir != ""
}

func TestMain(m *testing.M) {
	code := m.Run()
	if ffDir != "" {
		os.RemoveAll(ffDir)
	}
	os.Exit(code)
}

// start runs pogi with the Functions Framework installed natively when pip can
// install it, else with Docker; without either the test is skipped.
func start(t *testing.T) *pogi.Running {
	t.Helper()
	cfg := pogi.Config{Services: []string{"cloudfunctions", "operations", "pubsub", "gcs"}, DataDir: t.TempDir()}
	if os.Getenv("POGI_TEST_FUNCTIONS") != "docker" && installed() { // POGI_TEST_FUNCTIONS=docker tests the container path
		cfg.Lambda = "native"
		os.MkdirAll(filepath.Join(cfg.DataDir, "functions"), 0o755)
		if err := os.Symlink(filepath.Join(ffDir, "python"), filepath.Join(cfg.DataDir, "functions", "python")); err != nil {
			t.Fatal(err)
		}
	} else {
		cfg.Lambda = "docker"
		if _, err := exec.LookPath("docker"); err != nil {
			t.Skip("no Functions Framework (pip) and no docker")
		}
	}
	srv := pogi.Start(t, cfg)
	if cfg.Lambda == "docker" && !srv.Hub.Docker().Available() {
		t.Skip("no Functions Framework (pip) and no container engine")
	}
	srv.Setenv(t)
	return srv
}

func opts(srv *pogi.Running) []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(strings.TrimPrefix(srv.URL, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
}

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for n, c := range files {
		io.WriteString(must(zw.Create(n)), c)
	}
	zw.Close()
	return buf.Bytes()
}

// upload stores a source zip the way gcloud does: through generateUploadUrl.
func upload(t *testing.T, c *functionsv2.FunctionClient, files map[string]string) *functionspb.StorageSource {
	t.Helper()
	up := must(c.GenerateUploadUrl(ctx, &functionspb.GenerateUploadUrlRequest{Parent: parent}))
	req, _ := http.NewRequest(http.MethodPut, up.UploadUrl, bytes.NewReader(zipOf(t, files)))
	req.Header.Set("Content-Type", "application/zip")
	resp := must(http.DefaultClient.Do(req))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("upload = %d", resp.StatusCode)
	}
	return up.StorageSource
}

func source(ss *functionspb.StorageSource) *functionspb.Source {
	return &functionspb.Source{Source: &functionspb.Source_StorageSource{StorageSource: ss}}
}

func create(c *functionsv2.FunctionClient, id string, f *functionspb.Function) (*functionspb.Function, error) {
	op, err := c.CreateFunction(ctx, &functionspb.CreateFunctionRequest{Parent: parent, FunctionId: id, Function: f})
	if err != nil {
		return nil, err
	}
	return op.Wait(ctx)
}

func get(srv *pogi.Running, host, path string) (int, string) {
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, n, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, n, strings.TrimPrefix(srv.URL, "http://"))
	}}}
	resp, err := c.Get("http://" + host + ":" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:] + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// object reads the side effect a function left in pogi's Cloud Storage, waiting for it.
func object(t *testing.T, srv *pogi.Running, name string) string {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		resp, err := http.Get(srv.URL + "/storage/v1/b/results/o/" + url.PathEscape(name) + "?alt=media")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			return string(b)
		}
	}
	t.Fatalf("object %s never appeared", name)
	return ""
}

func bucket(t *testing.T, srv *pogi.Running) {
	t.Helper()
	resp := must(http.Post(srv.URL+"/storage/v1/b?project=proj", "application/json", strings.NewReader(`{"name":"results"}`)))
	resp.Body.Close()
}

func TestHTTPFunction(t *testing.T) {
	srv := start(t)
	c := must(functionsv2.NewFunctionClient(ctx, opts(srv)...))
	defer c.Close()
	src := upload(t, c, map[string]string{"main.py": mainPy, "requirements.txt": "# none\n"})

	f, err := create(c, "hello", &functionspb.Function{
		BuildConfig:   &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "hello", Source: source(src)},
		ServiceConfig: &functionspb.ServiceConfig{EnvironmentVariables: map[string]string{"GREETING": "hi"}, TimeoutSeconds: 30},
		Labels:        map[string]string{"team": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	port := srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	if f.Name != parent+"/functions/hello" || f.State != functionspb.Function_ACTIVE || f.Environment != functionspb.Environment_GEN_2 ||
		f.Url != "http://hello.cloudfunctions.localhost:"+port+"/" || f.ServiceConfig.Uri != f.Url || f.ServiceConfig.TimeoutSeconds != 30 ||
		f.ServiceConfig.EnvironmentVariables["GREETING"] != "hi" || f.BuildConfig.EntryPoint != "hello" || f.Labels["team"] != "x" ||
		!strings.HasPrefix(f.ServiceConfig.Revision, "hello-00001-") || f.ServiceConfig.Service != parent+"/services/hello" {
		t.Fatalf("created = %v", f)
	}
	if _, err := create(c, "hello", &functionspb.Function{BuildConfig: f.BuildConfig}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate = %v", err)
	}
	if _, err := create(c, "badruntime", &functionspb.Function{BuildConfig: &functionspb.BuildConfig{Runtime: "cobol", EntryPoint: "x", Source: source(src)}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("runtime = %v", err)
	}

	// by host and by path; the framework answers (a 418 from the function passes through)
	if code, body := get(srv, "hello.cloudfunctions.localhost", "/?name=ada"); code != 200 || body != "hello ada from hi at /" {
		t.Fatalf("host = %d %q", code, body)
	}
	if code, body := get(srv, "127.0.0.1", "/_gcp/functions/proj/us-central1/hello/sub/path?name=bob"); code != 200 || body != "hello bob from hi at /sub/path" {
		t.Fatalf("path = %d %q", code, body)
	}
	if code, _ := get(srv, "hello.cloudfunctions.localhost", "/?fail=1"); code != 418 {
		t.Fatalf("function status = %d", code)
	}
	if code, _ := get(srv, "nothere.cloudfunctions.localhost", "/"); code != 404 {
		t.Fatalf("unknown function = %d", code)
	}

	// update: new variables, a new revision, the function restarts
	f.ServiceConfig.EnvironmentVariables["GREETING"] = "hola"
	up, err := c.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{Function: f})
	if err != nil {
		t.Fatal(err)
	}
	f2 := must(up.Wait(ctx))
	if !strings.HasPrefix(f2.ServiceConfig.Revision, "hello-00002-") || f2.State != functionspb.Function_ACTIVE {
		t.Fatalf("updated = %v", f2)
	}
	if _, body := get(srv, "hello.cloudfunctions.localhost", "/"); body != "hello world from hola at /" {
		t.Fatalf("after update: %q", body)
	}
	l := c.ListFunctions(ctx, &functionspb.ListFunctionsRequest{Parent: parent})
	if got, err := l.Next(); err != nil || got.Name != f.Name {
		t.Fatalf("list = %v %v", got, err)
	}
	if rt := must(c.ListRuntimes(ctx, &functionspb.ListRuntimesRequest{Parent: parent})); len(rt.Runtimes) == 0 {
		t.Fatal("no runtimes")
	}
	dl := must(c.GenerateDownloadUrl(ctx, &functionspb.GenerateDownloadUrlRequest{Name: f.Name}))
	if resp := must(http.Get(dl.DownloadUrl)); resp.StatusCode != 200 {
		t.Fatalf("download = %d", resp.StatusCode)
	}
	if tbl := srv.Hub.Service("cloudfunctions").(core.Browsable).Resources(); len(tbl[0].Rows) != 1 || tbl[0].Rows[0][2] != "ACTIVE" {
		t.Fatalf("console = %v", tbl)
	}
	insp, _ := json.Marshal(srv.Hub.Service("cloudfunctions").(core.Inspectable).Inspect("functions", f.Name))
	if !strings.Contains(string(insp), `"logs"`) || !strings.Contains(string(insp), `"runtime":"python312"`) {
		t.Fatalf("inspect = %s", insp)
	}

	// a source on disk works too
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "main.py"), []byte(mainPy), 0o644)
	if _, err := create(c, "local", &functionspb.Function{BuildConfig: &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "hello",
		Source: source(&functionspb.StorageSource{Object: dir})}}); err != nil {
		t.Fatal(err)
	}
	if _, body := get(srv, "local.cloudfunctions.localhost", "/?name=disk"); body != "hello disk from  at /" {
		t.Fatalf("local source: %q", body)
	}

	// an entry point that does not exist fails the deployment, and the function says so
	_, err = create(c, "broken", &functionspb.Function{BuildConfig: &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "nothing", Source: source(src)}})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "failed to deploy") {
		t.Fatalf("broken = %v", err)
	}
	if b := must(c.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: parent + "/functions/broken"})); b.State != functionspb.Function_FAILED || len(b.StateMessages) == 0 {
		t.Fatalf("broken function = %v", b)
	}
	if code, _ := get(srv, "broken.cloudfunctions.localhost", "/"); code != 503 {
		t.Fatalf("broken answers %d", code)
	}

	ok(must(c.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: f.Name})).Wait(ctx))
	if _, err := c.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: f.Name}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete = %v", err)
	}
	if code, _ := get(srv, "hello.cloudfunctions.localhost", "/"); code != 404 {
		t.Fatalf("data plane after delete = %d", code)
	}
}

func TestPubsubTrigger(t *testing.T) {
	srv := start(t)
	bucket(t, srv)
	fc := must(functionsv2.NewFunctionClient(ctx, opts(srv)...))
	defer fc.Close()
	pc := must(ps.NewClient(ctx, "proj"))
	defer pc.Close()
	topic := "projects/proj/topics/orders"
	must(pc.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: topic}))
	src := upload(t, fc, map[string]string{"main.py": mainPy})

	if _, err := create(fc, "missing-topic", &functionspb.Function{
		BuildConfig:  &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "on_message", Source: source(src)},
		EventTrigger: &functionspb.EventTrigger{EventType: "google.cloud.pubsub.topic.v1.messagePublished", PubsubTopic: "projects/proj/topics/nope"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing topic = %v", err)
	}
	if _, err := create(fc, "no-topic", &functionspb.Function{
		BuildConfig:  &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "on_message", Source: source(src)},
		EventTrigger: &functionspb.EventTrigger{EventType: "google.cloud.pubsub.topic.v1.messagePublished"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no topic = %v", err)
	}

	f, err := create(fc, "on-order", &functionspb.Function{
		BuildConfig:  &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "on_message", Source: source(src)},
		EventTrigger: &functionspb.EventTrigger{EventType: "google.cloud.pubsub.topic.v1.messagePublished", PubsubTopic: topic}})
	if err != nil {
		t.Fatal(err)
	}
	et := f.EventTrigger
	if et.PubsubTopic != topic || et.TriggerRegion != "us-central1" || !strings.HasPrefix(et.Trigger, parent+"/triggers/on-order-") || et.RetryPolicy != functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY {
		t.Fatalf("trigger = %v", et)
	}
	subs := pc.TopicAdminClient.ListTopicSubscriptions(ctx, &pb.ListTopicSubscriptionsRequest{Topic: topic})
	name, err := subs.Next()
	if err != nil || !strings.Contains(name, "gcf-on-order-us-central1-orders") {
		t.Fatalf("subscriptions = %q %v", name, err)
	}
	sub := must(pc.SubscriptionAdminClient.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: name}))
	if !strings.Contains(sub.PushConfig.GetPushEndpoint(), "/_gcp/functions/_pubsub/proj/us-central1/on-order") {
		t.Fatalf("push config = %v", sub.PushConfig)
	}

	// publishing with the Pub/Sub client reaches the function, which leaves an object behind
	res := pc.Publisher("orders").Publish(ctx, &ps.Message{Data: []byte("order 17"), Attributes: map[string]string{"shop": "north"}})
	if _, err := res.Get(ctx); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Type, Source, Data string
		Attributes         map[string]string
	}
	if err := json.Unmarshal([]byte(object(t, srv, "pubsub-last")), &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "google.cloud.pubsub.topic.v1.messagePublished" || got.Source != "//pubsub.googleapis.com/"+topic || got.Data != "order 17" || got.Attributes["shop"] != "north" {
		t.Fatalf("function saw %+v", got)
	}

	// deleting the function deletes its subscription
	ok(must(fc.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: f.Name})).Wait(ctx))
	if left, err := pc.TopicAdminClient.ListTopicSubscriptions(ctx, &pb.ListTopicSubscriptionsRequest{Topic: topic}).Next(); err == nil {
		t.Fatalf("subscription %s left", left)
	}
}

// Eventarc delivers Cloud Storage events with Hub.Deliver.
func TestDeliver(t *testing.T) {
	srv := start(t)
	bucket(t, srv)
	fc := must(functionsv2.NewFunctionClient(ctx, opts(srv)...))
	defer fc.Close()
	src := upload(t, fc, map[string]string{"main.py": mainPy})
	f, err := create(fc, "on-upload", &functionspb.Function{
		BuildConfig: &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "on_object", Source: source(src)},
		EventTrigger: &functionspb.EventTrigger{EventType: "google.cloud.storage.object.v1.finalized",
			EventFilters: []*functionspb.EventFilter{{Attribute: "bucket", Value: "uploads"}}}})
	if err != nil {
		t.Fatal(err)
	}
	attr := func(v string) core.MessageAttribute { return core.MessageAttribute{DataType: "String", StringValue: v} }
	err = srv.Hub.Deliver("//cloudfunctions.googleapis.com/"+f.Name, &core.Message{Body: `{"bucket":"uploads","name":"a.txt"}`, Attributes: map[string]core.MessageAttribute{
		"ce-specversion": attr("1.0"), "ce-id": attr("1"), "ce-source": attr("//storage.googleapis.com/projects/_/buckets/uploads"),
		"ce-type": attr("google.cloud.storage.object.v1.finalized"), "content-type": attr("application/json")}})
	if err != nil {
		t.Fatal(err)
	}
	if got := object(t, srv, "storage-a.txt"); got != "google.cloud.storage.object.v1.finalized uploads" {
		t.Fatalf("function saw %q", got)
	}
	if err := srv.Hub.Deliver("//cloudfunctions.googleapis.com/"+parent+"/functions/missing", &core.Message{}); err == nil {
		t.Fatal("delivering to a missing function should fail")
	}
	if _, err := create(fc, "no-bucket", &functionspb.Function{BuildConfig: f.BuildConfig,
		EventTrigger: &functionspb.EventTrigger{EventType: "google.cloud.storage.object.v1.finalized"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("storage trigger without bucket = %v", err)
	}
}

// The REST mirror: create, get and poll the operation with plain net/http.
func TestREST(t *testing.T) {
	srv := start(t)
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "main.py"), []byte(mainPy), 0o644)
	do := func(method, path, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, op := do("POST", "/v2/"+parent+"/functions?functionId=rest", `{"buildConfig":{"runtime":"python312","entryPoint":"hello","source":{"storageSource":{"object":"`+dir+`"}}},
		"serviceConfig":{"environmentVariables":{"GREETING":"rest"}}}`)
	name, _ := op["name"].(string)
	if code != 200 || !strings.HasPrefix(name, parent+"/operations/") {
		t.Fatalf("create = %d %v", code, op)
	}
	var done map[string]any
	for range 300 {
		if _, done = do("GET", "/v2/"+name, ""); done["done"] == true {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	resp, _ := done["response"].(map[string]any)
	if done["done"] != true || resp["name"] != parent+"/functions/rest" || resp["state"] != "ACTIVE" || resp["@type"] != "type.googleapis.com/google.cloud.functions.v2.Function" {
		t.Fatalf("operation = %v", done)
	}
	code, fn := do("GET", "/v2/"+parent+"/functions/rest", "")
	if code != 200 || fn["environment"] != "GEN_2" || fn["url"] == "" || fn["buildConfig"].(map[string]any)["entryPoint"] != "hello" {
		t.Fatalf("get = %d %v", code, fn)
	}
	if code, list := do("GET", "/v2/"+parent+"/functions", ""); code != 200 || len(list["functions"].([]any)) != 1 {
		t.Fatalf("list = %d %v", code, list)
	}
	if code, up := do("POST", "/v2/"+parent+"/functions:generateUploadUrl", `{}`); code != 200 || !strings.Contains(up["uploadUrl"].(string), "gcf-v2-uploads-proj-us-central1") {
		t.Fatalf("generateUploadUrl = %d %v", code, up)
	}
	if code, e := do("GET", "/v2/"+parent+"/functions/missing", ""); code != 404 || e["error"].(map[string]any)["status"] != "NOT_FOUND" {
		t.Fatalf("missing = %d %v", code, e)
	}
	if code, _ := do("DELETE", "/v2/"+parent+"/functions/rest", ""); code != 200 {
		t.Fatalf("delete = %d", code)
	}
	if code, _ := do("GET", "/v2/"+parent+"/functions/rest", ""); code != 404 {
		t.Fatalf("get deleted = %d", code)
	}
}

// Without the Functions Framework and without Docker a deployment fails clearly.
func TestNoRuntime(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // no functions-framework, no python
	srv := pogi.Start(t, pogi.Config{Services: []string{"cloudfunctions", "operations"}, DockerHost: "tcp://127.0.0.1:1", DataDir: t.TempDir()})
	fc := must(functionsv2.NewFunctionClient(ctx, opts(srv)...))
	defer fc.Close()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "main.py"), []byte(mainPy), 0o644)
	_, err := create(fc, "f", &functionspb.Function{BuildConfig: &functionspb.BuildConfig{Runtime: "python312", EntryPoint: "hello", Source: source(&functionspb.StorageSource{Object: dir})}})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "container engine") {
		t.Fatalf("deploy = %v", err)
	}
	// the API still lists the function, as FAILED
	f := must(fc.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: parent + "/functions/f"}))
	if f.State != functionspb.Function_FAILED {
		t.Fatalf("state = %v", f.State)
	}
	st := srv.Hub.Service("cloudfunctions").(core.Stateful)
	b := must(st.Save())
	st.Reset()
	if err := st.Load(b); err != nil {
		t.Fatal(err)
	}
	if f := must(fc.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: parent + "/functions/f"})); f.State != functionspb.Function_FAILED {
		t.Fatalf("after load = %v", f)
	}
}
