// Package awstest builds AWS SDK configs pointed at a test pogi server.
// It is imported only by tests, so the SDK never ends up in the binary.
package awstest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Config returns an SDK config for url with test credentials and no retries, so a test sees exactly what pogi
// answered.
func Config(t testing.TB, url string) aws.Config {
	return ConfigAs(t, url, "test", "test", "")
}

// ConfigAs is Config with explicit credentials.
func ConfigAs(t testing.TB, url, key, secret, token string) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, token)),
		config.WithBaseEndpoint(url),
		config.WithRetryMaxAttempts(1),
		config.WithHTTPClient(testClient()),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// Sign signs a hand-built request with the test credentials the way the SDKs
// do (SigV4 for service in us-east-1). The payload hash is the
// X-Amz-Content-Sha256 header when set, else the hash of the body.
func Sign(t testing.TB, req *http.Request, service string) {
	t.Helper()
	hash := req.Header.Get("X-Amz-Content-Sha256")
	if hash == "" {
		var body []byte
		if req.GetBody != nil {
			rc, _ := req.GetBody()
			body, _ = io.ReadAll(rc)
		}
		sum := sha256.Sum256(body)
		hash = hex.EncodeToString(sum[:])
	}
	err := v4.NewSigner().SignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"},
		req, hash, service, "us-east-1", time.Now(), func(o *v4.SignerOptions) { o.DisableURIPathEscaping = service == "s3" })
	if err != nil {
		t.Fatal(err)
	}
}

// testClient is the SDK's own transport with one change: each request body of known length is handed to Go's
// transport as an in-memory copy. The SDK closes its request body as soon as the response headers arrive; on a
// busy machine Go's transport may still be finishing the write, reads EOF from the closed body, and drops the
// connection under the response being read ("use of closed network connection", seen on CI runners). The
// copy is never closed by the SDK, so that race cannot happen. Bodies of unknown length (event streams) pass
// through untouched. Redirects follow the SDK's rule: only 307 and 308.
func testClient() *http.Client {
	return &http.Client{
		Transport: bodyCopy{awshttp.NewBuildableClient().GetTransport()},
		CheckRedirect: func(r *http.Request, _ []*http.Request) error {
			if c := r.Response.StatusCode; c == http.StatusTemporaryRedirect || c == http.StatusPermanentRedirect {
				return nil
			}
			return http.ErrUseLastResponse
		},
	}
}

type bodyCopy struct{ base http.RoundTripper }

func (t bodyCopy) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength >= 0 {
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return nil, err
		}
		r = r.Clone(r.Context())
		r.Body = io.NopCloser(bytes.NewReader(b))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	}
	return t.base.RoundTrip(r)
}
