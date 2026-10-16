package lambda_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nrndev/pogi/internal/docker"
)

// floci #2573: a function (or an extension in its process) registers with the Extensions API and gets an identifier.
func TestExtensionsRegister(t *testing.T) {
	need(t, "python3")
	c, _, _ := setup(t)
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": `
import os, json, urllib.request, urllib.error
base = "http://" + os.environ["AWS_LAMBDA_RUNTIME_API"] + "/2020-01-01/extension"
def call(path, name, body, method="POST", ident=None):
    h = {"Content-Type": "application/json"}
    if name: h["Lambda-Extension-Name"] = name
    if ident: h["Lambda-Extension-Identifier"] = ident
    try:
        r = urllib.request.urlopen(urllib.request.Request(base + path, data=body, headers=h, method=method))
        return r.status, r.headers.get("Lambda-Extension-Identifier"), json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, None, json.loads(e.read())
def handler(e, c):
    st, ident, info = call("/register", "ext", b'{"events":["INVOKE","SHUTDOWN"]}')
    return {"status": st, "id": ident, "fn": info.get("functionName"),
            "noName": call("/register", "", b'{"events":["INVOKE"]}')[0],
            "badEvent": call("/register", "x", b'{"events":["NOPE"]}')[2].get("errorType"),
            "badId": call("/event/next", None, None, "GET", "nope")[0]}
`})}}))
	out := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("f")}))
	p := string(out.Payload)
	for _, want := range []string{`"status": 200`, `"fn": "f"`, `"noName": 400`, `"badEvent": "Extension.InvalidEventType"`, `"badId": 403`} {
		if !strings.Contains(strings.ReplaceAll(p, `":"`, `": "`), want) && !strings.Contains(strings.ReplaceAll(p, `":`, `": `), want) {
			t.Errorf("payload %s lacks %s", p, want)
		}
	}
	if strings.Contains(p, `"id":null`) {
		t.Errorf("no identifier: %s", p)
	}
}

// An external extension in a layer's extensions folder starts with the runtime, gets INVOKE for each
// invocation and SHUTDOWN when the instance stops.
func TestExternalExtension(t *testing.T) {
	need(t, "python3")
	c, _, _ := setup(t)
	out := t.TempDir()
	const ext = `#!/usr/bin/env python3
import os, json, urllib.request
base = "http://" + os.environ["AWS_LAMBDA_RUNTIME_API"] + "/2020-01-01/extension"
r = urllib.request.urlopen(urllib.request.Request(base + "/register", data=b'{"events":["INVOKE","SHUTDOWN"]}',
    headers={"Lambda-Extension-Name": "recorder", "Content-Type": "application/json"}))
ident = r.headers["Lambda-Extension-Identifier"]
log = open(os.path.join(os.environ["OUT"], "events"), "a", buffering=1)
log.write("registered\n")
while True:
    ev = json.loads(urllib.request.urlopen(urllib.request.Request(base + "/event/next", headers={"Lambda-Extension-Identifier": ident})).read())
    log.write(ev["eventType"] + " " + ev.get("requestId", ev.get("shutdownReason", "")) + "\n")
    if ev["eventType"] == "SHUTDOWN":
        break
`
	layer := must(c.PublishLayerVersion(ctx, &lambda.PublishLayerVersionInput{LayerName: aws.String("ext"), Content: &types.LayerVersionContentInput{ZipFile: zipFiles(map[string]string{"extensions/recorder": ext})}}))
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("g"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Layers: []string{*layer.LayerVersionArn}, Environment: &types.Environment{Variables: map[string]string{"OUT": out}},
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "def handler(e, c):\n    return c.aws_request_id\n"})}}))
	var ids []string
	for range 2 {
		r := must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("g")}))
		ids = append(ids, strings.Trim(string(r.Payload), `"`))
	}
	read := func() []string {
		b, _ := os.ReadFile(filepath.Join(out, "events"))
		return strings.Fields(strings.ReplaceAll(string(b), "\n", " "))
	}
	waitFor := func(n int) []string {
		t.Helper()
		for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
			if got := read(); len(got) >= n {
				return got
			}
		}
		t.Fatalf("extension events = %v", read())
		return nil
	}
	got := waitFor(5) // registered, then INVOKE and the request id, twice
	want := []string{"registered", "INVOKE", ids[0], "INVOKE", ids[1]}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("events = %v, want %v", got, want)
	}
	must(c.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String("g")}))
	if got = waitFor(7); got[5] != "SHUTDOWN" || got[6] != "spindown" {
		t.Fatalf("after delete: %v", got)
	}
}

// shExtension is an extension for the provided runtimes that need no interpreter: it prints what it gets.
const shExtension = `#!/bin/sh
base="http://$AWS_LAMBDA_RUNTIME_API/2020-01-01/extension"
hdr=$(mktemp)
curl -sS -D "$hdr" -X POST "$base/register" -H "Lambda-Extension-Name: recorder" -d '{"events":["INVOKE","SHUTDOWN"]}' >/dev/null
id=$(grep -i '^lambda-extension-identifier:' "$hdr" | tr -d '\r' | cut -d' ' -f2)
echo "EXT registered"
while true; do
  ev=$(curl -sS "$base/event/next" -H "Lambda-Extension-Identifier: $id")
  echo "EXT $ev"
  case "$ev" in *SHUTDOWN*) exit 0;; esac
done
`

// The same, in a container: the layer's extensions start in the function's container.
func TestExternalExtensionDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	c, _, _ := setup(t)
	layer := must(c.PublishLayerVersion(ctx, &lambda.PublishLayerVersionInput{LayerName: aws.String("shext"), Content: &types.LayerVersionContentInput{ZipFile: zipFiles(map[string]string{"extensions/recorder": shExtension})}}))
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("dx"), Runtime: types.RuntimeProvidedal2023, Handler: aws.String("handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Timeout: aws.Int32(300), Layers: []string{*layer.LayerVersionArn},
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"bootstrap": shBootstrap})}}))
	t.Cleanup(func() { c.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String("dx")}) })
	must(c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("dx"), Payload: []byte(`{"n":1}`)}))
	var lines []string
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		list, err := d.List(ctx, docker.LabelService+"=lambda", docker.LabelResource+"=dx")
		if err != nil || len(list) != 1 {
			t.Fatalf("containers: %+v %v", list, err)
		}
		lines = nil
		d.Logs(ctx, list[0].ID, false, func(l string) { lines = append(lines, l) })
		if strings.Contains(strings.Join(lines, "\n"), `"eventType":"INVOKE"`) {
			return
		}
	}
	t.Fatalf("the extension did not get INVOKE; container log:\n%s", strings.Join(lines, "\n"))
}
