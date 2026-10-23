package test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func call(t *testing.T, method, url string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return res.StatusCode, string(b)
}

func TestSpansAPI(t *testing.T) {
	srv := pogi.Start(t)
	if code, b := call(t, "GET", srv.URL+"/_pogi/api/spans"); code != 200 || strings.TrimSpace(b) != "[]" {
		t.Fatalf("empty: %d %q", code, b)
	}
	// one delivery step, as an SNS message handed to a queue would leave
	from, to := "arn:aws:sns:us-east-1:000000000000:alerts", "arn:aws:sqs:us-east-1:000000000000:orders"
	srv.Hub.OnDeliver("sqs", func(string, *core.Message) error { return nil })
	srv.Hub.Deliver(to, &core.Message{Body: "x", Trace: "1-a-b", From: from})
	srv.Hub.Deliver(to, &core.Message{Body: "y", Trace: "1-c-d", From: from})
	code, b := call(t, "GET", srv.URL+"/_pogi/api/spans?limit=1")
	var spans []core.Span
	if err := json.Unmarshal([]byte(b), &spans); err != nil || code != 200 || len(spans) != 1 || spans[0].Trace != "1-c-d" || spans[0].From != from || spans[0].To != to {
		t.Fatalf("limit=1: %d %s", code, b)
	}
	for _, q := range []string{"0", "5001", "-1", "abc", "1.5"} {
		if code, b := call(t, "GET", srv.URL+"/_pogi/api/spans?limit="+q); code != 400 || !strings.Contains(b, "limit must be a whole number from 1 to 5000") {
			t.Errorf("limit=%s: %d %s", q, code, b)
		}
	}
}

func TestLibraryModeAdminAndConsole(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t)
	c := s3.NewFromConfig(awstest.Config(t, srv.URL), func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("keep")}); err != nil {
		t.Fatal(err)
	}

	// Admin API: info lists services with resource counts.
	_, body := call(t, "GET", srv.URL+"/_pogi/api/info")
	var st core.Status
	json.Unmarshal([]byte(body), &st)
	found := false
	for _, s := range st.Services {
		found = found || s.Name == "s3" && s.Resources == 1 && s.Requests == 1
	}
	if !found {
		t.Fatalf("info = %s", body)
	}

	// Snapshots round-trip through reset.
	if code, b := call(t, "POST", srv.URL+"/_pogi/api/snapshots/one"); code != 200 {
		t.Fatalf("save: %d %s", code, b)
	}
	call(t, "POST", srv.URL+"/_pogi/api/reset")
	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("keep")}); err == nil {
		t.Fatal("bucket should be gone after reset")
	}
	if code, b := call(t, "POST", srv.URL+"/_pogi/api/snapshots/one/load"); code != 200 {
		t.Fatalf("load: %d %s", code, b)
	}
	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("keep")}); err != nil {
		t.Fatalf("bucket should be back after load: %v", err)
	}

	// Chaos: inject a 503 on HeadBucket, then remove it.
	req, _ := http.NewRequest("POST", srv.URL+"/_pogi/api/chaos", strings.NewReader(`{"service":"s3","op":"HeadBucket","status":503,"code":"SlowDown"}`))
	res, _ := http.DefaultClient.Do(req)
	var rule core.Rule
	json.NewDecoder(res.Body).Decode(&rule)
	_, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("keep")})
	var re interface{ HTTPStatusCode() int }
	if !errors.As(err, &re) || re.HTTPStatusCode() != 503 {
		t.Fatalf("chaos should inject 503, got %v", err)
	}
	call(t, "DELETE", srv.URL+"/_pogi/api/chaos/"+rule.ID)
	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("keep")}); err != nil {
		t.Fatalf("after removing the rule: %v", err)
	}

	// Errors carry real AWS codes through the SDK.
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("nope"), Key: aws.String("x")})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "NoSuchBucket" {
		t.Fatalf("want NoSuchBucket, got %v", err)
	}

	// Console pages render, LocalStack health answers, and the request log saw it all.
	for _, p := range []string{"/_pogi/", "/_pogi/requests", "/_pogi/resources", "/_pogi/objects/s3/keep", "/_pogi/inbox",
		"/_pogi/resources/sns", "/_pogi/resources/events", "/_pogi/resources/lambda", "/_pogi/chaos", "/_pogi/snapshots", "/_pogi/connect"} {
		if code, b := call(t, "GET", srv.URL+p); code != 200 || !strings.Contains(b, "pogi") {
			t.Fatalf("%s: %d", p, code)
		}
	}
	if code, b := call(t, "GET", srv.URL+"/_localstack/health"); code != 200 || !strings.Contains(b, `"s3":"running"`) {
		t.Fatalf("health: %d %s", code, b)
	}
	if _, b := call(t, "GET", srv.URL+"/_pogi/api/requests?errors=1"); !strings.Contains(b, "NoSuchBucket") {
		t.Fatalf("request log should hold the error: %s", b)
	}

	// A malicious page cannot wipe state with a cross-site form post.
	req, _ = http.NewRequest("POST", srv.URL+"/_pogi/api/reset", nil)
	req.Header.Set("Origin", "https://evil.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	if res, _ := http.DefaultClient.Do(req); res.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-site reset should be forbidden, got %d", res.StatusCode)
	}
}

func TestServiceFilter(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"sqs"}})
	if _, b := call(t, "GET", srv.URL+"/_localstack/health"); strings.Contains(b, `"s3"`) || !strings.Contains(b, `"sqs"`) {
		t.Fatalf("only sqs should run: %s", b)
	}
}

func TestLibraryModeAllClouds(t *testing.T) {
	srv := pogi.Start(t)
	env := map[string]string{}
	for _, kv := range srv.Env() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	// Azure Storage gets its own port, and the connection string names it
	if !strings.Contains(env["AZURE_STORAGE_CONNECTION_STRING"], "BlobEndpoint="+srv.Ports["azblob"]+"/devstoreaccount1;") {
		t.Fatalf("connection string %q, ports %v", env["AZURE_STORAGE_CONNECTION_STRING"], srv.Ports)
	}
	res, err := http.Get(srv.Ports["azqueue"] + "/devstoreaccount1?comp=list")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("queue port: %v %v", err, res)
	}
	// the main port also speaks TLS (Cosmos DB and Key Vault clients need it)
	insecure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	res, err = insecure.Get(env["AZURE_KEYVAULT_URL"] + "secrets?api-version=7.4")
	if err != nil || res.StatusCode != http.StatusUnauthorized { // the bearer challenge comes first
		t.Fatalf("https key vault: %v %v", err, res)
	}
	if env["PUBSUB_EMULATOR_HOST"] != strings.TrimPrefix(srv.URL, "http://") || env["STORAGE_EMULATOR_HOST"] != srv.URL {
		t.Fatalf("google env %v", env)
	}
}
