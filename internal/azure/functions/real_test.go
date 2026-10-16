package functions_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/functions"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// These tests run the real Functions host: Core Tools when func is on PATH,
// else mcr.microsoft.com/azure-functions/python in Docker (pulled on first
// use, about 1 GB). They skip when neither is available.

const hostJSON = `{"version":"2.0","extensionBundle":{"id":"Microsoft.Azure.Functions.ExtensionBundle","version":"[4.*, 5.0.0)"}}`

// queuePut is shared by the Python test apps: the function reaches pogi's
// queue endpoint through the AzureWebJobsStorage setting that pogi injected.
const queuePut = `
import os, re, urllib.request
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def put(queue, text):
    q = re.search(r"QueueEndpoint=([^;]+)", os.environ["AzureWebJobsStorage"]).group(1)
    body = "<QueueMessage><MessageText>%s</MessageText></QueueMessage>" % text
    urllib.request.urlopen(urllib.request.Request(q + "/" + queue + "/messages", data=body.encode(), method="POST")).read()
`

func needHost(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("func"); err == nil {
		return
	}
	if !docker.New(docker.Options{}).Available() {
		t.Skip("neither Azure Functions Core Tools (func) nor Docker is available")
	}
}

type app struct {
	t   *testing.T
	srv *pogi.Running
	svc *functions.Service
	q   *azqueue.ServiceClient
}

// start runs pogi with Azure Storage and Functions, and creates the app.
func start(t *testing.T, name, code string, queues ...string) *app {
	t.Helper()
	needHost(t)
	srv := pogi.Start(t, pogi.Config{Services: []string{"azblob", "azqueue", "aztable", "azfunctions"}})
	dir := t.TempDir()
	for f, content := range map[string]string{"function_app.py": code, "host.json": hostJSON} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	conn := ""
	for _, kv := range srv.Env() {
		conn = cmpCut(kv, "AZURE_STORAGE_CONNECTION_STRING=", conn)
	}
	q, err := azqueue.NewServiceClientFromConnectionString(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range queues {
		if _, err := q.CreateQueue(context.Background(), name, nil); err != nil {
			t.Fatal(err)
		}
	}
	svc := srv.Hub.Service("azfunctions").(*functions.Service)
	if err := svc.CreateApp(name, "python", dir, nil); err != nil {
		t.Fatal(err)
	}
	return &app{t, srv, svc, q}
}

func cmpCut(kv, prefix, def string) string {
	if v, ok := strings.CutPrefix(kv, prefix); ok {
		return v
	}
	return def
}

// get calls the app (starting its host on the first call); it skips the test
// when the host cannot run here, as when its image has no build for this CPU.
func (a *app) get(path, host string) (int, string) {
	a.t.Helper()
	req, _ := http.NewRequest("GET", a.srv.URL+path, nil)
	req.Host = host
	resp, err := (&http.Client{Timeout: 6 * time.Minute}).Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(b), "manifest") {
		a.t.Skipf("the Functions image is not available for this machine: %s", b)
	}
	return resp.StatusCode, string(b)
}

func (a *app) messages(queue string, want int, within time.Duration) []string {
	a.t.Helper()
	qc := a.q.NewQueueClient(queue)
	var got []string
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(time.Second) {
		resp, err := qc.DequeueMessages(context.Background(), &azqueue.DequeueMessagesOptions{NumberOfMessages: to.Ptr(int32(32))})
		if err != nil {
			a.t.Fatal(err)
		}
		for _, m := range resp.Messages {
			got = append(got, *m.MessageText)
		}
		if len(got) >= want {
			return got
		}
	}
	a.t.Fatalf("queue %s: %d messages after %s, want %d\nhost log: %s", queue, len(got), within, want, a.log())
	return nil
}

func (a *app) log() string {
	m, _ := a.svc.Inspect("Function apps", "hello").(map[string]any)
	l, _ := m["logs"].([]string)
	return strings.Join(l[max(0, len(l)-40):], "\n")
}

// A Python v2 HTTP function answers through pogi in both URL forms; any function key will do.
func TestPythonHTTP(t *testing.T) {
	a := start(t, "hello", queuePut+`
@app.route(route="hello")
def hello(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("hello " + req.params.get("name", "world"))
`)
	if code, body := a.get("/_azure/functions/hello/api/hello?name=x&code=anything", ""); code != 200 || body != "hello x" {
		t.Fatalf("path form: %d %q\n%s", code, body, a.log())
	}
	if code, body := a.get("/api/hello?name=y", "hello.azurewebsites.localhost"); code != 200 || body != "hello y" {
		t.Fatalf("host form: %d %q", code, body)
	}
	if code, _ := a.get("/api/missing", "hello.azurewebsites.localhost"); code != 404 {
		t.Fatalf("unknown route: %d", code)
	}
	if a.log() == "" {
		t.Fatal("the host's output is not captured")
	}
}

// A timer function with a six-field cron schedule runs by itself, with no
// request to start the host, and its side effect shows in pogi's queue.
func TestPythonTimer(t *testing.T) {
	a := start(t, "hello", queuePut+`
@app.timer_trigger(schedule="*/5 * * * * *", arg_name="t", run_on_startup=False)
def tick(t: func.TimerRequest) -> None:
    put("ticks", "tick")
`, "ticks")
	if got := a.messages("ticks", 2, 90*time.Second); got[0] != "tick" {
		t.Fatalf("messages: %v", got)
	}
}

// The admin endpoint runs a non-HTTP function on demand, through the path form.
func TestAdminInvoke(t *testing.T) {
	a := start(t, "hello", queuePut+`
@app.timer_trigger(schedule="0 0 0 1 1 *", arg_name="t", run_on_startup=False)
def yearly(t: func.TimerRequest) -> None:
    put("manual", "ran")
`, "manual")
	req, _ := http.NewRequest("POST", a.srv.URL+"/_azure/functions/hello/admin/functions/yearly", strings.NewReader(`{"input":""}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 6 * time.Minute}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(b), "manifest") {
		t.Skipf("the Functions image is not available for this machine: %s", b)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("admin invoke: %d %s\n%s", resp.StatusCode, b, a.log())
	}
	a.messages("manual", 1, time.Minute)
}

// The Event Grid delivery hook invokes an Event Grid-triggered function.
func TestEventGridTrigger(t *testing.T) {
	a := start(t, "hello", queuePut+`
@app.event_grid_trigger(arg_name="event")
def on_event(event: func.EventGridEvent) -> None:
    put("events", event.id + " " + event.event_type)
`, "events")
	if code, _ := a.get("/_azure/functions/hello/admin/host/status", ""); code == 0 { // start the host
		t.Fatal("no answer")
	}
	id := "/subscriptions/s/resourceGroups/g/providers/Microsoft.Web/sites/hello/functions/on_event"
	event := `[{"id":"evt-1","eventType":"Pogi.Test","subject":"/x","data":{"a":1},"dataVersion":"1.0","metadataVersion":"1","eventTime":"2026-01-01T00:00:00Z","topic":"/t"}]`
	var err error
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		if err = a.srv.Hub.Deliver(id, &core.Message{Body: event}); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("deliver: %v\n%s", err, a.log())
	}
	if got := a.messages("events", 1, time.Minute); got[0] != "evt-1 Pogi.Test" {
		t.Fatalf("messages: %v", got)
	}
}
