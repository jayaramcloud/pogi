package core

import (
	"net/http"
	"strings"
	"testing"
)

// gcpPubsub claims /v1/projects/... like a Google service that routes by path.
type gcpPubsub struct{}

func (gcpPubsub) Info() Info {
	return Info{Name: "pubsub", Cloud: GCP, Match: func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/v1/projects/") }}
}
func (gcpPubsub) ServeHTTP(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pubsub")) }

// awsKafka is signed for "kafka" and serves /v1/operations/..., which a Google service claims by path.
type awsKafka struct{}

func (awsKafka) Info() Info                                       { return Info{Name: "kafka", Cloud: AWS, Signing: []string{"kafka"}} }
func (awsKafka) ServeHTTP(w http.ResponseWriter, r *http.Request) { w.Write([]byte("kafka")) }

type gcpOps struct{}

func (gcpOps) Info() Info {
	return Info{Name: "operations", Cloud: GCP, Match: func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/v1/operations/") }}
}
func (gcpOps) ServeHTTP(w http.ResponseWriter, r *http.Request) { w.Write([]byte("operations")) }

// azureARM claims /subscriptions paths like Resource Manager, and awsS3 is S3's catch-all.
type azureARM struct{}

func (azureARM) Info() Info {
	return Info{Name: "arm", Cloud: Azure, Match: func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/subscriptions") }}
}
func (azureARM) ServeHTTP(w http.ResponseWriter, r *http.Request) { w.Write([]byte("arm")) }

type awsS3 struct{}

func (awsS3) Info() Info                                       { return Info{Name: "s3", Cloud: AWS, Signing: []string{"s3"}} }
func (awsS3) ServeHTTP(w http.ResponseWriter, r *http.Request) { w.Write([]byte("s3")) }

// A request no service claims is answered in the error shape of its cloud, never by S3, and
// a Google service only takes its own host and no SigV4 traffic.
func TestUnknownAPI(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	h.Register(gcpPubsub{})
	h.Register(gcpOps{})
	h.Register(awsKafka{})
	h.Register(azureARM{})
	h.Register(awsS3{})
	sig := func(name string) string {
		return "AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/" + name + "/aws4_request, Signature=x"
	}
	for _, c := range []struct {
		name, path string
		hdr        []string
		code       int
		want       string
	}{
		{"Google path on its own host", "/v1/projects/p/topics", []string{"Host", "pubsub.googleapis.com"}, 200, "pubsub"},
		{"regional Google host", "/v1/projects/p/topics", []string{"Host", "us-central1-pubsub.googleapis.com"}, 200, "pubsub"},
		{"Google path on another API's host", "/v1/projects/p/topics", []string{"Host", "frobnicate.googleapis.com"}, 404, `"status":"NOT_FOUND"`},
		{"unknown Google path", "/v9/projects/p/topics", nil, 501, `"status":"UNIMPLEMENTED"`},
		{"MSK operation path, SigV4", "/v1/operations/arn", []string{"Authorization", sig("kafka")}, 200, "kafka"},
		{"same path, no SigV4", "/v1/operations/arn", nil, 200, "operations"},
		{"unknown AWS JSON target", "/", []string{"X-Amz-Target", "Nosuch_2099.Foo", "Authorization", sig("nosuch")}, 400, "UnknownOperationException"},
		{"unknown AWS query action", "/", []string{"Content-Type", "application/x-www-form-urlencoded", "Authorization", sig("nosuch")}, 400, "InvalidAction"},
		{"S3 bucket named subscriptions, signed", "/subscriptions/key", []string{"Authorization", sig("s3")}, 200, "s3"},
		{"S3 bucket named subscriptions, presigned", "/subscriptions/key?X-Amz-Credential=AKID%2F20260101%2Fus-east-1%2Fs3%2Faws4_request", nil, 200, "s3"},
		{"unknown Azure path with an api-version", "/madeUp?api-version=2022-09-01", nil, 404, `"code":"NotFound"`},
		{"unknown Graph path", "/v2.0/users", nil, 400, "Request_BadRequest"},
		{"unknown Entra path", "/00000000-0000-0000-0000-000000000000/madeUp", nil, 400, "invalid_request"},
		{"bad Azure credentials", "/secrets", []string{"Authorization", "Basic x"}, 404, `"code":"NotFound"`},
		{"anonymous S3 request", "/bucket/key", nil, 200, "s3"},
		{"unknown AWS REST service", "/v1/things", []string{"Authorization", sig("nosuch")}, 404, "UnknownOperationException"},
	} {
		method := "GET"
		if c.path == "/" {
			method = "POST"
		}
		w := serve(h, method, c.path, nil, c.hdr...)
		if w.Code != c.code || !strings.Contains(w.Body.String(), c.want) {
			t.Errorf("%s: %d %s, want %d %s", c.name, w.Code, w.Body, c.code, c.want)
		}
	}
}

// A /<label>/ prefix picks the API by host, so two APIs with the same paths stay apart.
func TestGooglePrefix(t *testing.T) {
	for _, tc := range []struct{ path, wantPath, wantHost string }{
		{"/redis/v1/projects/p/locations/l/instances", "/v1/projects/p/locations/l/instances", "redis.googleapis.com"},
		{"/workflowexecutions/v1/projects/p/locations/l/workflows/w/executions", "/v1/projects/p/locations/l/workflows/w/executions", "workflowexecutions.googleapis.com"},
		{"/redis/notaversion/x", "/redis/notaversion/x", "localhost:7644"}, // not an API path: maybe an S3 bucket's
		{"/storage/v1/b/bucket", "/storage/v1/b/bucket", "localhost:7644"}, // GCS's own path, not a prefix
		{"/redis", "/redis", "localhost:7644"},
	} {
		r, _ := http.NewRequest("GET", "http://localhost:7644"+tc.path, nil)
		googlePrefix(r)
		if r.URL.Path != tc.wantPath || r.Host != tc.wantHost {
			t.Errorf("%s: got %s on %s, want %s on %s", tc.path, r.URL.Path, r.Host, tc.wantPath, tc.wantHost)
		}
	}
}
