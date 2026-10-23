package core

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TB is the subset of testing.TB that test helpers need (no testing import in the binary).
type TB interface {
	Helper()
	Cleanup(func())
	Fatalf(format string, args ...any)
	TempDir() string
}

// Listen serves h on a random loopback port until the test ends and returns the base URL.
func Listen(tb TB, h http.Handler) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: h, Protocols: Protocols()}
	go srv.Serve(ln)
	tb.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String()
}

// ServeTest starts a hub with only the given services, for package tests.
func ServeTest(tb TB, build ...func(*Hub) Service) (*Hub, string) {
	tb.Helper()
	h := NewHub(Config{Version: "test", DataDir: tb.TempDir(), ValidateSigs: testSigs(), LambdaInit: TestLambdaInit}, nil)
	for _, b := range build {
		h.Register(b(h))
	}
	h.URL = Listen(tb, h)
	tb.Cleanup(h.Close)
	return h, h.URL
}

// TestLambdaInit is the Lambda start limit in pogi's own tests. AWS's 10 seconds is too tight for a cold start
// while a full test run loads the machine (TestExternalExtension failed so, 3 runs in about 22); a start that is
// truly stuck still fails.
const TestLambdaInit = time.Minute

// testSigs turns on signature checking for a whole test run
// (POGI_TEST_VALIDATE_SIGNATURES=1 go test ./...).
func testSigs() bool { return os.Getenv("POGI_TEST_VALIDATE_SIGNATURES") == "1" }

// ServePort serves one service on its own random port for tests (Azure Storage).
func ServePort(tb TB, build func(*Hub) Service) (*Hub, string) {
	tb.Helper()
	h := NewHub(Config{Version: "test", DataDir: tb.TempDir(), ValidateSigs: testSigs(), LambdaInit: TestLambdaInit}, nil)
	svc := build(h)
	h.Register(svc)
	h.URL = Listen(tb, h)
	tb.Cleanup(h.Close)
	return h, Listen(tb, h.Handler(svc))
}

// Protocols enables HTTP/1.1 and cleartext HTTP/2 (gRPC clients use h2c).
func Protocols() *http.Protocols {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	return &p
}

// AWSEnv is the environment that points AWS SDKs and CLIs at pogi.
func AWSEnv(endpoint, region string) []string {
	return []string{
		"AWS_ENDPOINT_URL=" + endpoint,
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_DEFAULT_REGION=" + region,
		"AWS_REGION=" + region,
	}
}

// Well-known emulator keys (Azurite and the Cosmos DB emulator); pogi accepts any key.
const (
	AzuriteKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	CosmosKey  = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
)

// Env is AWSEnv plus the variables Google Cloud and Azure clients read.
// storage overrides the blob, queue and table base URLs (default: the
// Azurite ports 10000-10002 on the endpoint's host).
func Env(endpoint, region string, storage ...string) []string {
	hp := Hostport(endpoint)
	host, _, _ := strings.Cut(hp, ":")
	azurite := func(i int, kind string) string {
		base := "http://" + host + ":" + strconv.Itoa(10000+i)
		if i < len(storage) && storage[i] != "" {
			base = storage[i]
		}
		return kind + "Endpoint=" + base + "/devstoreaccount1;"
	}
	return append(append(AWSEnv(endpoint, region),
		"STORAGE_EMULATOR_HOST="+endpoint,
		"PUBSUB_EMULATOR_HOST="+hp,
		"FIRESTORE_EMULATOR_HOST="+hp,
		"FIREBASE_AUTH_EMULATOR_HOST="+hp,
		"DATASTORE_EMULATOR_HOST="+hp,
		"SPANNER_EMULATOR_HOST="+hp,
		"BIGTABLE_EMULATOR_HOST="+hp,
		"AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey="+AzuriteKey+";"+
			azurite(0, "Blob")+azurite(1, "Queue")+azurite(2, "Table"),
		"COSMOS_ENDPOINT=https://"+hp+"/",
		"COSMOS_KEY="+CosmosKey,
		"AZURE_KEYVAULT_URL=https://"+hp+"/",
		"IDENTITY_ENDPOINT="+endpoint+"/msi/token", // managed identity, App Service style: DefaultAzureCredential finds it
		"IDENTITY_HEADER=pogi",
		"AZURE_AUTHORITY_HOST=https://"+hp+"/", // azidentity wants https, and DisableInstanceDiscovery for a host that isn't Microsoft's
		"ARM_METADATA_HOSTNAME="+hp,            // azurerm: metadata_host
	), GcloudEnv(endpoint)...)
}

// DefaultProject is the Google Cloud project `pogi env` names for gcloud; pogi
// serves any project, and seeds without one use it too.
const DefaultProject = "pogi"

// gcloudAPIs are the Google APIs pogi serves, by the name of their gcloud
// property api_endpoint_overrides/<name>.
var gcloudAPIs = []string{"pubsub", "storage", "secretmanager", "firestore", "run", "cloudfunctions", "cloudtasks",
	"cloudscheduler", "eventarc", "managedkafka", "cloudkms", "sql", "datastore", "iam", "iamcredentials",
	"cloudresourcemanager", "serviceusage", "logging", "monitoring", "bigquery", "compute"}

// GcloudEnv points the gcloud CLI at pogi: one CLOUDSDK_API_ENDPOINT_OVERRIDES_<API>
// per served API, the default project and a dummy access token. The override
// is the bare endpoint with a "/" path, so that gcloud keeps each API's own
// version path (/v1, /v2).
// ponytail: gcloud itself reads its token from a file (CLOUDSDK_AUTH_ACCESS_TOKEN_FILE, which
// `pogi env` adds); CLOUDSDK_AUTH_ACCESS_TOKEN is for other tooling and ignored by gcloud.
func GcloudEnv(endpoint string) []string {
	var out []string
	for _, api := range gcloudAPIs {
		out = append(out, "CLOUDSDK_API_ENDPOINT_OVERRIDES_"+strings.ToUpper(api)+"="+strings.TrimSuffix(endpoint, "/")+"/")
	}
	for _, api := range SortedKeys(googlePrefixed) { // the APIs served under /<api>/ (googlePrefix)
		path := "/" + api + "/"
		if api == "dns" { // its own paths already start with /dns/v1/
			path = "/"
		}
		out = append(out, "CLOUDSDK_API_ENDPOINT_OVERRIDES_"+strings.ToUpper(api)+"="+strings.TrimSuffix(endpoint, "/")+path)
	}
	return append(out, "CLOUDSDK_CORE_PROJECT="+DefaultProject, "CLOUDSDK_AUTH_ACCESS_TOKEN=pogi-token", "CLOUDSDK_CORE_DISABLE_PROMPTS=1")
}

// Hostport strips the scheme from a URL.
func Hostport(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
}

const internalHeader = "X-Pogi-Internal"

// ConsoleHeader marks an in-process call as a write the console made for the user. Such calls are
// logged (marked "console"); other internal calls are not. Like internalHeader, it only counts
// for in-process calls.
const ConsoleHeader = "X-Pogi-Console"

// Call invokes a JSON-protocol operation in-process as account/region, the
// way Lambda pollers, Step Functions and CloudFormation reach other services.
// Internal calls skip the request log and chaos rules.
func (h *Hub) Call(account, region, target, op string, in, out any) error {
	return h.CallTrace("", account, region, target, op, in, out)
}

// CallTrace is Call inside a flow: the called service sees the trace on X-Amzn-Trace-Id,
// so what it sends on stays in the same flow.
func (h *Hub) CallTrace(trace, account, region, target, op string, in, out any) error {
	return h.call(false, trace, account, region, target, op, in, out)
}

// ConsoleCall is Call for a write the console makes: it shows in Activity, marked "console".
func (h *Hub) ConsoleCall(account, region, target, op string, in, out any) error {
	return h.call(true, "", account, region, target, op, in, out)
}

func (h *Hub) call(console bool, trace, account, region, target, op string, in, out any) error {
	if h.targets[target] == nil {
		return &Error{http.StatusBadRequest, "ServiceNotEnabled", "no enabled service handles " + target}
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Host = "localhost:" + strconv.Itoa(h.Cfg.Port)
	req.Header.Set("X-Amz-Target", target+"."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+account+"/20260101/"+region+"/pogi/aws4_request")
	req.Header.Set(internalHeader, "1")
	if console {
		req.Header.Set(ConsoleHeader, "1")
	}
	if trace != "" {
		req.Header.Set("X-Amzn-Trace-Id", TraceHeader(trace))
	}
	rec := &bufferWriter{header: http.Header{}}
	h.ServeHTTP(rec, req)
	if rec.status >= 300 {
		var e struct {
			Type    string `json:"__type"`
			Message string `json:"message"`
		}
		json.Unmarshal(rec.buf.Bytes(), &e)
		return &Error{rec.status, e.Type, e.Message}
	}
	if out != nil && rec.buf.Len() > 0 {
		return json.Unmarshal(rec.buf.Bytes(), out)
	}
	return nil
}

// bufferWriter is a minimal in-memory http.ResponseWriter.
type bufferWriter struct {
	header http.Header
	status int
	buf    bytes.Buffer
}

func (b *bufferWriter) Header() http.Header { return b.header }
func (b *bufferWriter) WriteHeader(s int) {
	if b.status == 0 {
		b.status = s
	}
}
func (b *bufferWriter) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.buf.Write(p)
}

// Every calls f on a ticker in its own goroutine until the returned stop is
// called; services use it for clocks (schedules, TTLs, buffers) and stop it in Close.
func Every(d time.Duration, f func(now time.Time)) (stop func()) {
	t := time.NewTicker(d)
	done := make(chan struct{})
	var once sync.Once
	Go("core.Every", func() {
		for {
			select {
			case now := <-t.C:
				func() { defer Recover("core.Every"); f(now) }() // a panic skips this tick, not all later ones
			case <-done:
				return
			}
		}
	})
	return func() { once.Do(func() { t.Stop(); close(done) }) }
}

// ListenTLS serves h over HTTPS (pogi's local CA) for tests of SDKs that
// insist on TLS (Key Vault, Cosmos DB); it returns https://localhost:port and
// a pool that trusts the CA.
func ListenTLS(tb TB, hub *Hub, h http.Handler) (string, *x509.CertPool) {
	tb.Helper()
	cfg, err := hub.TLS()
	if err != nil {
		tb.Fatalf("tls: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: h, Protocols: Protocols()}
	go srv.Serve(SniffTLS(ln, cfg))
	tb.Cleanup(func() { srv.Close() })
	caPath, _, _ := hub.CertPaths()
	pool := x509.NewCertPool()
	pem, _ := os.ReadFile(caPath)
	pool.AppendCertsFromPEM(pem)
	return "https://localhost:" + strconv.Itoa(ln.Addr().(*net.TCPAddr).Port), pool
}

// Do sends an HTTP request through the gateway in-process as account/region
// (signing name chooses the service), for REST services such as S3, Lambda
// and API Gateway. It returns the status, headers and body.
func (h *Hub) Do(account, region, signing, method, path string, header http.Header, body []byte) (int, http.Header, []byte) {
	req, _ := http.NewRequest(method, path, bytes.NewReader(body))
	req.Host = "localhost:" + strconv.Itoa(h.Cfg.Port)
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+account+"/20260101/"+region+"/"+signing+"/aws4_request")
	req.Header.Set(internalHeader, "1")
	rec := &bufferWriter{header: http.Header{}}
	h.ServeHTTP(rec, req)
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return rec.status, rec.header, rec.buf.Bytes()
}

// DoService serves req straight to the named service as an internal call
// (no routing, request log, chaos or IAM), the way seeding reaches the Google
// Cloud and Azure APIs. It returns the status and body.
func (h *Hub) DoService(name string, req *http.Request) (int, []byte) {
	return h.DoServiceAs(name, DefaultAccount, h.Cfg.Region, req)
}

// DoServiceAs is DoService as the given account and region.
func (h *Hub) DoServiceAs(name, account, region string, req *http.Request) (int, []byte) {
	svc := h.byName[name]
	if svc == nil {
		return http.StatusNotFound, []byte("service " + name + " is not enabled")
	}
	req.Header.Set(internalHeader, "1")
	if req.Body == nil { // handlers expect a server request: never nil
		req.Body = http.NoBody
	}
	rec := &bufferWriter{header: http.Header{}}
	h.serve(svc, &Ctx{Hub: h, RequestID: NewID(), Host: req.Host, Path: req.URL.Path, Region: region, Account: account}, rec, req)
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return rec.status, rec.buf.Bytes()
}
