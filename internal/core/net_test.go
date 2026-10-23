package core

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPartitions(t *testing.T) {
	for _, c := range []struct{ region, partition, suffix, arn, root string }{
		{"us-east-1", "aws", "amazonaws.com", "arn:aws:sqs:us-east-1:000000000000:q", "arn:aws:iam::000000000000:root"},
		{"us-gov-west-1", "aws-us-gov", "amazonaws.com", "arn:aws-us-gov:sqs:us-gov-west-1:000000000000:q", "arn:aws-us-gov:iam::000000000000:root"},
		{"cn-north-1", "aws-cn", "amazonaws.com.cn", "arn:aws-cn:sqs:cn-north-1:000000000000:q", "arn:aws-cn:iam::000000000000:root"},
	} {
		if p, s := Partition(c.region), URLSuffix(c.region); p != c.partition || s != c.suffix {
			t.Errorf("%s: partition %s, suffix %s", c.region, p, s)
		}
		if arn := ARN("sqs", c.region, DefaultAccount, "q"); arn != c.arn {
			t.Errorf("%s: ARN %s", c.region, arn)
		}
		if svc, region, account, res, ok := ParseARN(c.arn); !ok || svc != "sqs" || region != c.region || account != DefaultAccount || res != "q" {
			t.Errorf("ParseARN(%s) = %s %s %s %s %v", c.arn, svc, region, account, res, ok)
		}
		// the default identity of an unknown access key, a global ARN
		if _, principal := (&Hub{}).accountFor("test", c.region); principal != c.root {
			t.Errorf("%s: principal %s", c.region, principal)
		}
	}
	if arn := GlobalARN("s3", "cn-north-1", "", "logs"); arn != "arn:aws-cn:s3:::logs" {
		t.Errorf("bucket ARN %s", arn)
	}
}

func TestSplitHost(t *testing.T) {
	for _, c := range []struct{ host, hostname, label, base string }{
		{"my-bucket.aws.localhost:7644", "aws.localhost", "my-bucket", "aws.localhost"},
		{"aws.localhost:7644", "aws.localhost", "", "aws.localhost"},
		{"sync-aws.localhost:7644", "aws.localhost", "", "aws.localhost"}, // Step Functions' host prefix
		{"my-bucket.localhost", "aws.localhost", "my-bucket", "localhost"},
		{"my-bucket.localhost:7644", "localhost", "my-bucket", "localhost"},
		{"localhost:7644", "localhost", "", "localhost"},
		{"Abc12.execute-api.localhost:7644", "", "Abc12", "execute-api.localhost"},
		{"u1.lambda-url.us-east-1.localhost:7644", "localhost", "u1", "lambda-url.us-east-1.localhost"},
		{"127.0.0.1:7644", "localhost", "", "127.0.0.1"},
		{"[::1]:7644", "localhost", "", "::1"},
	} {
		if label, base := SplitHost(c.host, c.hostname); label != c.label || base != c.base {
			t.Errorf("SplitHost(%q, %q) = %q, %q", c.host, c.hostname, label, base)
		}
	}
}

// echo is a JSON-protocol service that answers with its request body.
type echo struct{}

func (echo) Info() Info { return Info{Name: "echo", Cloud: AWS, Targets: []string{"Echo"}} }
func (echo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := C(r).Intercept(); err != nil {
		w.WriteHeader(AsError(err).Status)
		io.WriteString(w, AsError(err).Code)
		return
	}
	w.Write(Body(r))
}

func serve(h *Hub, method, path string, body io.Reader, hdr ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, body)
	r.Host = "localhost:7644"
	for i := 0; i+1 < len(hdr); i += 2 {
		if hdr[i] == "Host" {
			r.Host = hdr[i+1]
			continue
		}
		r.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCORS(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir(), CORSOrigins: []string{"https://app.example"}, CORSHeaders: []string{"X-Trace"}}, nil)
	h.Register(echo{})

	// A preflight carries nothing to route by; with S3 (which gets them) off, the gateway answers.
	w := serve(h, "OPTIONS", "/", nil, "Origin", "http://localhost:3000", "Access-Control-Request-Method", "POST",
		"Access-Control-Request-Headers", "amz-sdk-request,authorization,content-type,x-amz-date,x-amz-target,x-evil,x-trace")
	if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" ||
		w.Header().Get("Access-Control-Allow-Headers") != "amz-sdk-request, authorization, content-type, x-amz-date, x-amz-target, x-trace" {
		t.Fatalf("preflight = %d %v", w.Code, w.Header())
	}
	if w := serve(h, "OPTIONS", "/", nil, "Origin", "https://evil.example", "Access-Control-Request-Method", "POST"); w.Code != 403 ||
		w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("foreign preflight = %d %v", w.Code, w.Header())
	}
	// API responses carry the headers for allowed origins only.
	for origin, want := range map[string]string{"https://app.example": "https://app.example", "http://127.0.0.1:5173": "http://127.0.0.1:5173",
		"https://web.localhost": "https://web.localhost", "http://[::1]:8080": "http://[::1]:8080", "https://evil.example": "", "http://localhost.evil.example": ""} {
		w := serve(h, "POST", "/", strings.NewReader("{}"), "Origin", origin, "X-Amz-Target", "Echo.Op")
		// the front door turns foreign origins away before any service runs
		if wantCode := map[bool]int{true: 200, false: 403}[want != ""]; w.Code != wantCode || w.Header().Get("Access-Control-Allow-Origin") != want {
			t.Errorf("%s: %d %q", origin, w.Code, w.Header().Get("Access-Control-Allow-Origin"))
		}
	}
	// The admin API keeps its cross-origin protection, even for allowed origins.
	if w := serve(h, "POST", "/_pogi/api/reset", nil, "Origin", "http://localhost:3000", "Sec-Fetch-Site", "cross-site"); w.Code != 403 {
		t.Fatalf("cross-site admin call = %d", w.Code)
	}
}

func TestBrowserGate(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(echo{})
	call := func(hub *Hub, hdr ...string) int {
		return serve(hub, "POST", "/", strings.NewReader("{}"), append(hdr, "X-Amz-Target", "Echo.Op")...).Code
	}
	for _, c := range []struct {
		name string
		hdr  []string
		want int
	}{
		{"cross-site text/plain POST", []string{"Origin", "https://evil.example", "Sec-Fetch-Site", "cross-site", "Content-Type", "text/plain"}, 403},
		{"origin null", []string{"Origin", "null"}, 403},
		{"rebound host with its own origin", []string{"Host", "evil.example:7644", "Origin", "http://evil.example:7644"}, 403},
		{"rebound host, plain GET", []string{"Host", "evil.example:7644", "Sec-Fetch-Site", "same-origin"}, 403},
		{"localhost app", []string{"Origin", "http://localhost:3000", "Sec-Fetch-Site", "same-site"}, 200},
		{"cross-site GET, no Origin, loopback Referer", []string{"Sec-Fetch-Site", "cross-site", "Referer", "http://127.0.0.1:5173/"}, 200},
		{"cross-site GET, no Origin, foreign Referer", []string{"Sec-Fetch-Site", "cross-site", "Referer", "https://evil.example/"}, 403},
		{"cross-site GET, no Origin, no Referer", []string{"Sec-Fetch-Site", "cross-site"}, 403},
		{"rebound same-origin GET from a browser", []string{"User-Agent", "Mozilla/5.0", "Host", "evil.example:7644"}, 403},
		{"browser on localhost, no Fetch headers", []string{"User-Agent", "Mozilla/5.0", "Host", "localhost:7644"}, 200},
		{"SDK with a foreign Host", []string{"User-Agent", "aws-sdk-go/1.55", "Host", "foo.example"}, 200},
		{"SDK in a container", []string{"Host", "pogi:7644"}, 200},
		{"SDK with a real AWS name", []string{"Host", "sqs.us-east-1.amazonaws.com"}, 200},
		{"console from a LAN address", []string{"Host", "192.168.1.5:7644", "Origin", "http://192.168.1.5:7644"}, 200},
	} {
		if got := call(h, c.hdr...); got != c.want {
			t.Errorf("%s = %d, want %d", c.name, got, c.want)
		}
	}
	// a link into the console is let in; the same link to an API path is not
	nav := func(path string) int {
		return serve(h, "GET", path, nil, "Sec-Fetch-Site", "cross-site", "Sec-Fetch-Mode", "navigate").Code
	}
	if got := nav("/_pogi/"); got != 200 && got != 404 { // the console is not mounted in this test hub; the gate let it through
		t.Errorf("cross-site navigate to /_pogi/ = %d", got)
	}
	if got := nav("/?Action=ListQueues"); got != 403 {
		t.Errorf("cross-site navigate to /?Action=ListQueues = %d", got)
	}
	if got := nav("/_pogi/../?Action=DeleteQueue"); got != 403 {
		t.Errorf("cross-site navigate with dot segments = %d", got)
	}
	// the console is behind the gate too, and a refused call reaches no service
	if w := serve(h, "POST", "/_pogi/api/reset", nil, "Origin", "https://evil.example"); w.Code != 403 || !strings.Contains(w.Body.String(), "server.cors.origins") {
		t.Errorf("console from a foreign origin = %d %q", w.Code, w.Body)
	}
	// the opt-out: "*" lets every origin and host in
	open := NewHub(Config{DataDir: t.TempDir(), CORSOrigins: []string{"*"}}, nil)
	open.Register(echo{})
	if got := call(open, "Host", "evil.example:7644", "Origin", "https://evil.example"); got != 200 {
		t.Errorf("with cors \"*\" = %d", got)
	}
}

func TestBodyLimit(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir(), MaxUploadMB: 1}, nil)
	h.Register(echo{})
	small, big := bytes.Repeat([]byte("a"), 100<<10), bytes.Repeat([]byte("a"), 1<<20+1)
	if w := serve(h, "POST", "/", bytes.NewReader(small), "X-Amz-Target", "Echo.Op"); w.Code != 200 || w.Body.Len() != len(small) {
		t.Fatalf("small = %d, %d bytes", w.Code, w.Body.Len())
	}
	if w := serve(h, "POST", "/", bytes.NewReader(big), "X-Amz-Target", "Echo.Op"); w.Code != 413 || w.Body.String() != "EntityTooLarge" {
		t.Fatalf("big = %d %s", w.Code, w.Body)
	}
	want := "pogi is a local emulator for tests, so uploads are limited to 1 MB (this one is 2 MB). " +
		"Raise the limit with --max-upload-mb or server.maxUploadMB in pogi.yaml."
	if got := Caution(1<<20+1, 1<<20); got != want {
		t.Fatalf("caution = %q", got)
	}
	// without a Content-Length (chunked) the limit still holds
	r := httptest.NewRequest("POST", "/", io.MultiReader(bytes.NewReader(big)))
	r.ContentLength = -1
	r.Header.Set("X-Amz-Target", "Echo.Op")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatalf("chunked = %d", w.Code)
	}
	if e := h.Requests.Query(0, 0, "", "", true); len(e) != 2 || e[0].Status != 413 {
		t.Fatalf("request log = %+v", e)
	}
}

// upload is a service for the upload tests: it can raise its own body limit and answer slowly.
type upload struct {
	limit int64
	wait  time.Duration
}

func (upload) Info() Info                      { return Info{Name: "upload", Cloud: AWS, Targets: []string{"Upload"}} }
func (u upload) BodyLimit(*http.Request) int64 { return u.limit }
func (u upload) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := C(r).Intercept(); err != nil {
		w.WriteHeader(AsError(err).Status)
		io.WriteString(w, AsError(err).Msg)
		return
	}
	time.Sleep(u.wait)
	io.WriteString(w, "ok")
}

// A service can raise the limit for its own requests (function code), and the default stays 10 MiB.
func TestBodyLimiter(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(upload{limit: h.CodeLimit(false)})
	body := bytes.NewReader(make([]byte, 11<<20))
	if w := serve(h, "POST", "/", body, "X-Amz-Target", "Upload.Op"); w.Code != 200 {
		t.Fatalf("11 MiB with a raised limit = %d %s", w.Code, w.Body)
	}
	// over a raised (code) limit the message counts the zip
	w := serve(h, "POST", "/", bytes.NewReader(make([]byte, 60<<20)), "X-Amz-Target", "Upload.Op")
	if w.Code != 413 || !strings.Contains(w.Body.String(), "Function code is limited to 50 MB zipped (this one is 60 MB zipped)") {
		t.Fatalf("60 MiB with a code limit = %d %s", w.Code, w.Body)
	}
	h = NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(upload{})
	w = serve(h, "POST", "/", bytes.NewReader(make([]byte, 23<<20)), "X-Amz-Target", "Upload.Op")
	if w.Code != 413 || !strings.Contains(w.Body.String(), "limited to 10 MB (this one is 23 MB)") {
		t.Fatalf("23 MiB with the default limit = %d %s", w.Code, w.Body)
	}
}

// A body that stalls gets 408; a slow handler after a fully read body is not cancelled.
func TestStalledBody(t *testing.T) {
	defer func(d time.Duration) { stallTimeout = d }(stallTimeout)
	stallTimeout = 200 * time.Millisecond
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(upload{wait: 600 * time.Millisecond})
	srv := httptest.NewServer(h)
	defer srv.Close()

	pr, pw := io.Pipe()
	defer pw.Close()
	go pw.Write([]byte("x")) // one byte, then nothing
	req, _ := http.NewRequest("POST", srv.URL, pr)
	req.ContentLength = 1000
	req.Header.Set("X-Amz-Target", "Upload.Op")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 408 || !strings.Contains(string(b), "stalled") || time.Since(start) > 2*time.Second {
		t.Fatalf("stalled body = %d %q after %v", resp.StatusCode, b, time.Since(start))
	}

	req, _ = http.NewRequest("POST", srv.URL, strings.NewReader("hello"))
	req.Header.Set("X-Amz-Target", "Upload.Op")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("slow handler after a full body = %d %q", resp.StatusCode, b)
	}
}

func TestCustomTLS(t *testing.T) {
	dir := t.TempDir()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"pogi.test"}, NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	kb, _ := x509.MarshalECPrivateKey(key)
	cert, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writePEM(cert, "CERTIFICATE", der)
	writePEM(keyPath, "EC PRIVATE KEY", kb)

	h := NewHub(Config{DataDir: dir, TLSCert: cert, TLSKey: keyPath}, nil)
	cfg, err := h.TLS()
	if err != nil || !slices.Equal(cfg.Certificates[0].Leaf.DNSNames, []string{"pogi.test"}) {
		t.Fatalf("custom certificate not served: %v", err)
	}
	h.Cfg.TLSKey = filepath.Join(dir, "missing.pem")
	if _, err := h.TLS(); err == nil {
		t.Fatal("a missing key must fail, not fall back to pogi's CA")
	}
}

// A reset that names services must name enabled ones. An empty or unknown name is refused, so it is
// neither read as "every service" nor reported as done.
func TestResetNames(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(echo{})
	for path, want := range map[string]int{
		"/_pogi/api/reset":                200,
		"/_pogi/api/reset?service=echo":   200,
		"/_pogi/api/reset?service=":       400,
		"/_pogi/api/reset?service=nosuch": 400,
		"/_pogi/api/reset?service=echo,":  400,
	} {
		if w := serve(h, "POST", path, nil); w.Code != want {
			t.Errorf("%s = %d %s, want %d", path, w.Code, w.Body, want)
		}
	}
}

// A ?service= filter must name a running service: an unknown one, or one that is off, is a 404 and
// not an empty answer that looks like "nothing there".
func TestServiceFilterNamesARunningService(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(echo{})
	for path, want := range map[string]int{
		"/_pogi/api/resources":                200,
		"/_pogi/api/resources?service=echo":   200,
		"/_pogi/api/resources?service=nosuch": 404,
		"/_pogi/api/requests?service=":        200,
		"/_pogi/api/requests?service=echo":    200,
		"/_pogi/api/requests?service=nosuch":  404,
	} {
		if w := serve(h, "GET", path, nil); w.Code != want {
			t.Errorf("%s = %d %s, want %d", path, w.Code, w.Body, want)
		}
	}
}

// An unknown snapshot name is a 404 that names the snapshot, not the server's file path, for load,
// rm and export alike.
func TestUnknownSnapshot(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	for _, c := range [][2]string{{"POST", "/_pogi/api/snapshots/nope/load"}, {"DELETE", "/_pogi/api/snapshots/nope"}, {"GET", "/_pogi/api/snapshots/nope"}} {
		w := serve(h, c[0], c[1], nil)
		if want := `no snapshot named \"nope\" (pogi snapshot ls lists them)`; w.Code != 404 || !strings.Contains(w.Body.String(), want) {
			t.Errorf("%s %s = %d %s, want 404 %s", c[0], c[1], w.Code, w.Body, want)
		}
	}
}

// A chaos rule from the API must inject something, with an error status, a latency that is not
// negative and a rate between 0 and 1. The console and MCP use the same Rule.Check.
func TestChaosRuleChecked(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	for body, want := range map[string]int{
		`{"status":503}`:                      200,
		`{"latencyMs":300,"probability":0.5}`: 200,
		`{}`:                                  400,
		`{"status":999}`:                      400,
		`{"status":200}`:                      400,
		`{"latencyMs":-3000}`:                 400,
		`{"status":500,"probability":5}`:      400,
		`{"status":500,"probability":-0.1}`:   400,
	} {
		if w := serve(h, "POST", "/_pogi/api/chaos", strings.NewReader(body)); w.Code != want {
			t.Errorf("%s = %d %s, want %d", body, w.Code, w.Body, want)
		}
	}
	if n := len(h.Chaos.List()); n != 2 {
		t.Errorf("%d rules stored, want the 2 good ones", n)
	}
}
