package test

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	dk "github.com/nrndev/pogi/internal/docker"
)

// silentEngine is a Docker engine on a unix socket that answers the ping and version, then reads every other
// request and never answers. release lets go of the waiting requests.
func silentEngine(t *testing.T) (host string, release func()) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pgfe") // short: unix socket paths are limited
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", dir+"/d.sock")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	hs := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_ping"):
			io.WriteString(w, "OK")
		case strings.HasSuffix(r.URL.Path, "/version"):
			io.WriteString(w, `{"Version":"26.0.0","ApiVersion":"1.45","Arch":"arm64","Platform":{"Name":"fake"}}`)
		default:
			io.Copy(io.Discard, r.Body) // reading the body is how the server notices the client left
			select {
			case <-stop:
			case <-r.Context().Done():
			}
		}
	})}
	go hs.Serve(ln)
	var once sync.Once
	release = func() { once.Do(func() { close(stop); hs.Close(); os.RemoveAll(dir) }) }
	t.Cleanup(release)
	return "unix://" + dir + "/d.sock", release
}

func awsPost(t *testing.T, base, path, ctype, sign string, body []byte) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", base+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", ctype)
	awstest.Sign(t, req, sign)
	res, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, string(b)
}

// An engine that accepts a call and never answers must not leave a database "creating" for ever, or a Lambda
// invoke waiting for ever. The time limit is shortened here (it is two minutes).
func TestSilentDockerEngineEndsInFailure(t *testing.T) {
	dk.CallLimit = 3 * time.Second
	t.Cleanup(func() { dk.CallLimit = 2 * time.Minute })
	host, release := silentEngine(t)
	srv := pogi.Start(t, pogi.Config{DockerHost: host, Lambda: "docker"})
	defer release() // let go of the silent engine before pogi shuts down
	const form = "application/x-www-form-urlencoded"
	rds := func(v url.Values) string {
		v.Set("Version", "2014-10-31")
		_, body := awsPost(t, srv.URL, "/", form, "rds", []byte(v.Encode()))
		return body
	}
	t0 := time.Now()
	rds(url.Values{"Action": {"CreateDBInstance"}, "DBInstanceIdentifier": {"hung1"}, "Engine": {"postgres"}, "DBInstanceClass": {"db.t3.micro"},
		"MasterUsername": {"u"}, "MasterUserPassword": {"password1"}, "AllocatedStorage": {"20"}})
	if time.Since(t0) > 5*time.Second {
		t.Errorf("the create call took %s", time.Since(t0))
	}
	t.Logf("create call returned after %s", time.Since(t0))
	failed := false
	for end := time.Now().Add(15 * time.Second); !failed && time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		failed = strings.Contains(rds(url.Values{"Action": {"DescribeDBInstances"}, "DBInstanceIdentifier": {"hung1"}}), "<DBInstanceStatus>failed<")
	}
	t.Logf("failed seen after %s", time.Since(t0))
	if !failed {
		t.Error("the database is not failed 15s after its create stopped getting answers")
	}
	// Lambda in docker mode
	b, _ := json.Marshal(map[string]any{"FunctionName": "dk", "Runtime": "python3.12", "Handler": "index.handler", "Timeout": 3,
		"Role": "arn:aws:iam::000000000000:role/r", "Code": map[string][]byte{"ZipFile": pyZip("def handler(e, c):\n    return 1\n")}})
	awsPost(t, srv.URL, "/2015-03-31/functions", "application/json", "lambda", b)
	t0 = time.Now()
	code, body := awsPost(t, srv.URL, "/2015-03-31/functions/dk/invocations", "application/json", "lambda", []byte("{}"))
	t.Logf("invoke: %d after %s", code, time.Since(t0))
	if code < 500 || time.Since(t0) > 20*time.Second {
		t.Errorf("invoke against a silent engine: %d after %s: %.200q", code, time.Since(t0), body)
	}
}
