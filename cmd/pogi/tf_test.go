package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

// stdout runs f and returns what it printed.
func stdout(t *testing.T, f func() error) string {
	t.Helper()
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	err := f()
	os.Stdout = old
	w.Close()
	out, _ := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestTerraformGoogle(t *testing.T) {
	out := stdout(t, func() error {
		return terraform([]string{"--cloud", "gcp", "--endpoint", "http://localhost:4571", "--project", "demo"})
	})
	for _, want := range []string{`provider "google"`, `project               = "demo"`, `region                = "us-central1"`,
		`access_token          = "pogi"`, `user_project_override = false`,
		`storage_custom_endpoint`, `"http://localhost:4571/storage/v1/"`, `cloud_run_v2_custom_endpoint`, `"http://localhost:4571/v2/"`,
		`secret_manager_custom_endpoint`, `managed_kafka_custom_endpoint`, `kms_custom_endpoint`, `sql_custom_endpoint`, `"http://localhost:4571/sql/v1beta4/"`,
		`big_query_custom_endpoint`, `monitoring_custom_endpoint`, `iam_beta_custom_endpoint`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in\n%s", want, out)
		}
	}
}

func TestTerraformAzure(t *testing.T) {
	out := stdout(t, func() error {
		return terraform([]string{"--cloud", "azure", "--endpoint", "http://localhost:4571"})
	})
	for _, want := range []string{`provider "azurerm"`, `features {}`, `metadata_host                   = "host.docker.internal:4571"`,
		`resource_provider_registrations = "none"`, `subscription_id                 = "00000000-0000-0000-0000-000000000000"`,
		`SSL_CERT_FILE=/pogi-ca.pem`, `docker run`, `certs/ca.pem`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in\n%s", want, out)
		}
	}
	custom := stdout(t, func() error {
		return terraform([]string{"--cloud", "azure", "--endpoint", "http://localhost:4571", "--metadata-host", "pogi.test:9"})
	})
	if !strings.Contains(custom, `"pogi.test:9"`) {
		t.Errorf("--metadata-host is ignored:\n%s", custom)
	}
}
