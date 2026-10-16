package generic_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// raw sends one signed request and returns the status and the body.
func raw(t *testing.T, url, service, method, path string, hdr map[string]string, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	awstest.Sign(t, req, service)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// Bad input found by the bad-input probe gets a 4xx in AWS's shape, not a panic and not a 200.
func TestBadInput(t *testing.T) {
	url := pogi.Start(t, pogi.Config{}).URL
	for _, c := range []struct {
		name, service, method, path, body string
		hdr                               map[string]string
		status                            int
		want                              string
	}{
		{"CloudTrail's long target is checked", "cloudtrail", "POST", "/", `{"Name":"x","RetentionPeriod":-1}`,
			map[string]string{"X-Amz-Target": "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.CreateEventDataStore", "Content-Type": "application/x-amz-json-1.1"},
			400, "ValidationException"},
		{"an empty header is checked", "sagemaker", "POST", "/endpoints/ep/async-invocations", "{}",
			map[string]string{"X-Amzn-SageMaker-InputLocation": ""}, 400, "validation error detected"},
		{"StartIngestionJob on nothing", "bedrock", "PUT", "/knowledgebases/kb/datasources/ds/ingestionjobs/", "{}", nil, 404, "ResourceNotFoundException"},
	} {
		status, body := raw(t, url, c.service, c.method, c.path, c.hdr, c.body)
		if status != c.status || !strings.Contains(body, c.want) {
			t.Errorf("%s: %d %s", c.name, status, body)
		}
	}
	// the model lists a required member (examples) its output does not define: the answer is still built
	status, body := raw(t, url, "bedrock-agentcore", "POST", "/datasets", nil, `{"datasetName":"d","schemaType":"THIRD_PARTY_EVALUATION_V1","source":{}}`)
	var made struct{ DatasetId string }
	if err := json.Unmarshal([]byte(body), &made); err != nil || made.DatasetId == "" {
		t.Fatalf("create dataset: %d %s", status, body)
	}
	if status, body := raw(t, url, "bedrock-agentcore", "GET", "/datasets/"+made.DatasetId+"/examples", nil, ""); status != 200 {
		t.Errorf("list examples: %d %s", status, body)
	}
}
