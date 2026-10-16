package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// sigEcho stands in for a service: "ok" once Intercept passes, else the error code.
type sigEcho struct{ name, protocol string }

func (s sigEcho) Info() Info {
	return Info{Name: s.name, Cloud: AWS, Signing: []string{s.name}, Protocol: s.protocol}
}

func (s sigEcho) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := C(r).Intercept(); err != nil {
		http.Error(w, AsError(err).Code, AsError(err).Status)
		return
	}
	io.WriteString(w, "ok")
}

// sdkSign signs req like the AWS SDK for Go does, at the given time.
func sdkSign(t *testing.T, req *http.Request, body, key, secret, service string, at time.Time) {
	t.Helper()
	hash := req.Header.Get("X-Amz-Content-Sha256")
	if hash == "" {
		sum := sha256.Sum256([]byte(body))
		hash = hex.EncodeToString(sum[:])
	}
	err := v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: key, SecretAccessKey: secret}, req, hash,
		service, "us-east-1", at, func(o *v4.SignerOptions) { o.DisableURIPathEscaping = service == "s3" })
	if err != nil {
		t.Fatal(err)
	}
}

func do(t *testing.T, req *http.Request) string {
	t.Helper()
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return strings.TrimSpace(string(b))
}

func TestSigV4(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir(), ValidateSigs: true}, nil)
	h.Register(sigEcho{"s3", "rest-xml"})
	h.Register(sigEcho{"lambda", "rest-json"})
	h.Register(sigEcho{"sts", "query"})
	base := Listen(t, h)
	h.AddIdentity("AKIAKNOWN", Identity{Account: "111111111111", Secret: "s3cret"})
	now := time.Now()

	// send signs a request (after edit, if any, changes it) and returns the answer.
	send := func(method, path, body, service, key, secret string, at time.Time, edit func(*http.Request)) string {
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		req.Header.Set("X-Amz-Meta-Who", "po   gi") // inner spaces collapse
		// the S3 SDKs send the payload hash
		if service == "s3" {
			sum := sha256.Sum256([]byte(body))
			req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
		}
		sdkSign(t, req, body, key, secret, service, at)
		if edit != nil {
			edit(req)
		}
		return do(t, req)
	}
	const invoke = "/2015-03-31/functions/fn/invocations"
	for _, tc := range []struct {
		name, method, path, body, service, key, secret string
		at                                             time.Time
		edit                                           func(*http.Request)
		want                                           string
	}{
		{"good", "POST", invoke, `{"a":1}`, "lambda", "test", "test", now, nil, "ok"},
		{"account id key", "POST", invoke, `{"a":1}`, "lambda", "111122223333", "test", now, nil, "ok"},
		{"registered key", "POST", invoke, `{"a":1}`, "lambda", "AKIAKNOWN", "s3cret", now, nil, "ok"},
		{"wrong secret", "POST", invoke, `{"a":1}`, "lambda", "test", "nope", now, nil, "InvalidSignatureException"},
		{"wrong secret, query", "POST", "/", "Action=GetCallerIdentity", "sts", "AKIAKNOWN", "test", now, nil, "SignatureDoesNotMatch"},
		{"unknown key", "POST", invoke, `{"a":1}`, "lambda", "AKIAUNKNOWN", "test", now, nil, "InvalidClientTokenId"},
		{"unknown key, s3", "GET", "/b/k", "", "s3", "AKIAUNKNOWN", "test", now, nil, "InvalidAccessKeyId"},
		{"tampered header", "POST", invoke, `{"a":1}`, "lambda", "test", "test", now,
			func(r *http.Request) { r.Header.Set("X-Amz-Meta-Who", "eve") }, "InvalidSignatureException"},
		{"tampered body", "POST", invoke, `{"a":1}`, "lambda", "test", "test", now,
			func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader(`{"a":2}`)) }, "InvalidSignatureException"},
		{"tampered body, signed hash", "PUT", "/b/k", "hello", "s3", "test", "test", now, func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader("jello"))
		}, "SignatureDoesNotMatch"},
		{"in-process header from the network", "POST", invoke, `{"a":1}`, "lambda", "test", "nope", now,
			func(r *http.Request) { r.Header.Set("X-Pogi-Internal", "1") }, "InvalidSignatureException"},
		{"skewed", "POST", invoke, `{"a":1}`, "lambda", "test", "test", now.Add(-20 * time.Minute), nil, "InvalidSignatureException"},
		{"skewed, s3", "GET", "/b/k", "", "s3", "test", "test", now.Add(20 * time.Minute), nil, "RequestTimeTooSkewed"},
		{"within skew", "GET", "/b/k", "", "s3", "test", "test", now.Add(-10 * time.Minute), nil, "ok"},
		// S3 signs the path as sent (encoded once), the others encode it again.
		{"s3 special characters", "GET", "/b/a%20b/c%2Bd%21%27%28%29%2A%26%24%40%3D%3B%3A%2C%C3%BC~%25.txt?prefix=a%20b%2Bc&list-type=2&uploads",
			"", "s3", "test", "test", now, nil, "ok"},
		{"double-encoded path", "POST", "/2015-03-31/functions/arn%3Aaws%3Alambda%3Aus-east-1%3A000000000000%3Afunction%3Afn/invocations",
			"{}", "lambda", "test", "test", now, nil, "ok"},
	} {
		if got := send(tc.method, tc.path, tc.body, tc.service, tc.key, tc.secret, tc.at, tc.edit); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}

	// aws-chunked and unsigned payloads: only the seed signature counts.
	for _, hash := range []string{"STREAMING-UNSIGNED-PAYLOAD-TRAILER", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "UNSIGNED-PAYLOAD"} {
		if got := send("PUT", "/b/k", "5\r\nhello\r\n0\r\n\r\n", "s3", "test", "test", now, func(r *http.Request) {
			r.Header.Set("X-Amz-Content-Sha256", hash) // after signing: must be signed with it
		}); got != "SignatureDoesNotMatch" {
			t.Errorf("%s set after signing: %q", hash, got)
		}
		req, _ := http.NewRequest("PUT", base+"/b/k", strings.NewReader("anything"))
		req.Header.Set("X-Amz-Content-Sha256", hash)
		sdkSign(t, req, "", "test", "test", "s3", now)
		if got := do(t, req); got != "ok" {
			t.Errorf("%s: %q", hash, got)
		}
	}

	// presigned URLs
	presign := func(secret string, at time.Time) string {
		req, _ := http.NewRequest("GET", base+"/b/k%20%2B.txt?X-Amz-Expires=60&versionId=1", nil)
		u, _, err := v4.NewSigner().PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test", SecretAccessKey: secret},
			req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", at, func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
		if err != nil {
			t.Fatal(err)
		}
		req, _ = http.NewRequest("GET", u, nil)
		return do(t, req)
	}
	if got := presign("test", now.Add(-30*time.Second)); got != "ok" {
		t.Errorf("presigned: %q", got)
	}
	if got := presign("nope", now); got != "SignatureDoesNotMatch" {
		t.Errorf("presigned, wrong secret: %q", got)
	}
	if got := presign("test", now.Add(-2*time.Minute)); got != "AccessDenied" {
		t.Errorf("presigned, expired: %q", got)
	}

	// floci #4150: an x-amz-* header the presigned URL does not sign is refused, a browser's x-amz-user-agent is not
	for header, want := range map[string]string{"X-Amz-Checksum-Crc32": "AccessDenied", "X-Amz-User-Agent": "ok"} {
		req, _ := http.NewRequest("PUT", base+"/b/k?X-Amz-Expires=60", nil)
		u, _, err := v4.NewSigner().PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"},
			req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", now, func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
		if err != nil {
			t.Fatal(err)
		}
		req, _ = http.NewRequest("PUT", u, nil)
		req.Header.Set(header, "x")
		if got := do(t, req); got != want {
			t.Errorf("presigned with unsigned %s: %q, want %q", header, got, want)
		}
	}

	// skipped: anonymous, SigV2 and other schemes
	for _, auth := range []string{"", "AWS test:c2lnbmF0dXJl", "Bearer token"} {
		req, _ := http.NewRequest("GET", base+"/b/k", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		if got := do(t, req); got != "ok" {
			t.Errorf("auth %q: %q", auth, got)
		}
	}

	// off by default
	off := NewHub(Config{DataDir: t.TempDir()}, nil)
	off.Register(sigEcho{"lambda", "rest-json"})
	req, _ := http.NewRequest("POST", Listen(t, off)+invoke, strings.NewReader("{}"))
	sdkSign(t, req, "{}", "test", "nope", "lambda", now)
	if got := do(t, req); got != "ok" {
		t.Errorf("validation off: %q", got)
	}
}

func TestVerifyPolicy(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir(), ValidateSigs: true}, nil)
	post := func(secret string, expires time.Duration) map[string]string {
		c := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://localhost"), UsePathStyle: true,
			Credentials: aws.NewCredentialsCache(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: secret}, nil
			}))})
		p, err := s3.NewPresignClient(c).PresignPostObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String("k")},
			func(o *s3.PresignPostOptions) { o.Expires = expires })
		if err != nil {
			t.Fatal(err)
		}
		return p.Values
	}
	code := func(f map[string]string) string {
		if e := h.VerifyPolicy(f["X-Amz-Credential"], f["policy"], f["X-Amz-Signature"]); e != nil {
			return e.Code
		}
		return "ok"
	}
	good := post("test", time.Hour)
	if got := code(good); got != "ok" {
		t.Errorf("good policy: %s", got)
	}
	if got := code(post("nope", time.Hour)); got != "SignatureDoesNotMatch" {
		t.Errorf("wrong secret: %s", got)
	}
	tampered := map[string]string{"X-Amz-Credential": good["X-Amz-Credential"], "X-Amz-Signature": good["X-Amz-Signature"], "policy": good["policy"] + "e30="}
	if got := code(tampered); got != "SignatureDoesNotMatch" {
		t.Errorf("tampered policy: %s", got)
	}
	if got := code(post("test", -time.Minute)); got != "AccessDenied" {
		t.Errorf("expired policy: %s", got)
	}
	if got := code(map[string]string{"policy": good["policy"]}); got != "ok" {
		t.Errorf("unsigned upload: %s", got)
	}
}
