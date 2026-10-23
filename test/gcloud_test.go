package test

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
)

func TestEnvGcloud(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"gcp"}})
	env := map[string]string{}
	for _, kv := range srv.Env() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	for _, api := range []string{"PUBSUB", "STORAGE", "SECRETMANAGER", "FIRESTORE", "RUN", "CLOUDFUNCTIONS", "CLOUDTASKS",
		"CLOUDSCHEDULER", "EVENTARC", "MANAGEDKAFKA", "CLOUDKMS"} {
		if got := env["CLOUDSDK_API_ENDPOINT_OVERRIDES_"+api]; got != srv.URL+"/" {
			t.Errorf("%s override %q, want %q", api, got, srv.URL+"/")
		}
	}
	for api, path := range map[string]string{"REDIS": "/redis/", "SPANNER": "/spanner/", "WORKFLOWEXECUTIONS": "/workflowexecutions/",
		"FILE": "/file/", "DNS": "/"} { // APIs that share paths are told apart by a prefix; DNS's own paths have one
		if got := env["CLOUDSDK_API_ENDPOINT_OVERRIDES_"+api]; got != srv.URL+path {
			t.Errorf("%s override %q, want %q", api, got, srv.URL+path)
		}
	}
	if env["CLOUDSDK_CORE_PROJECT"] != "pogi" || env["CLOUDSDK_AUTH_ACCESS_TOKEN"] == "" {
		t.Errorf("project and token: %v", env)
	}
	if env["FIREBASE_AUTH_EMULATOR_HOST"] != strings.TrimPrefix(srv.URL, "http://") {
		t.Errorf("FIREBASE_AUTH_EMULATOR_HOST = %q", env["FIREBASE_AUTH_EMULATOR_HOST"])
	}
	if env["PUBSUB_EMULATOR_HOST"] == "" || env["STORAGE_EMULATOR_HOST"] == "" || env["AWS_ENDPOINT_URL"] == "" {
		t.Errorf("the existing variables are kept: %v", env)
	}
}

// TestGcloud runs the real gcloud CLI against pogi, offline: pogi env's
// variables are all it needs. Skipped without gcloud.
func TestGcloud(t *testing.T) {
	gcloud, err := exec.LookPath("gcloud")
	if err != nil {
		t.Skip("gcloud is not installed")
	}
	srv := pogi.Start(t, pogi.Config{Services: []string{"gcp"}, DockerHost: "/nonexistent/docker.sock"}) // no broker for the cluster
	for _, req := range [][3]string{
		{"PUT", "/v1/projects/pogi/topics/orders", "{}"},
		{"POST", "/v1/projects/pogi/secrets?secretId=api-key", `{"replication":{"automatic":{}}}`},
		{"POST", "/v1/projects/pogi/locations/us-central1/clusters?clusterId=events", `{"capacityConfig":{"vcpuCount":"3","memoryBytes":"3221225472"}}`},
	} {
		r, _ := http.NewRequest(req[0], srv.URL+req[1], strings.NewReader(req[2]))
		if resp, err := http.DefaultClient.Do(r); err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s %s: %v %v", req[0], req[1], resp, err)
		}
	}
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	os.WriteFile(token, []byte("pogi-token"), 0o600)
	run := func(want string, args ...string) {
		t.Helper()
		cmd := exec.Command(gcloud, args...)
		cmd.Env = append(os.Environ(), srv.Env()...)
		cmd.Env = append(cmd.Env, "CLOUDSDK_CONFIG="+filepath.Join(dir, "config"), "CLOUDSDK_AUTH_ACCESS_TOKEN_FILE="+token,
			"CLOUDSDK_CORE_DISABLE_USAGE_REPORTING=true", "CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK=true")
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), want) {
			t.Fatalf("gcloud %v: %v\n%s", args, err, out)
		}
	}
	run("projects/pogi/topics/orders", "pubsub", "topics", "list")
	run("api-key", "secrets", "list")
	run("events", "managed-kafka", "clusters", "list", "--location=us-central1")
}
