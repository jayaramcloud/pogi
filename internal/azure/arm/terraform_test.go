package arm_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// The real azurerm provider, through Terraform: apply, a plan with no changes,
// destroy, on a resource group and a user-assigned identity.
//
// It needs `terraform` on the PATH, and the provider: set POGI_AZURERM_PROVIDER_DIR
// to a directory that holds the provider binary (dev_overrides, no download),
// or leave it unset and `terraform init` fetches hashicorp/azurerm.
//
// The provider must trust pogi's CA. On Linux the test hands it the CA in
// SSL_CERT_FILE. Go on macOS ignores that variable and asks the keychain, so
// there the test runs only when pogi's CA is already trusted: set
// POGI_TEST_DATA_DIR to a directory to keep the CA in across runs, and trust
// <dir>/certs/ca.pem once (`pogi cert` prints how). It skips otherwise.
func TestTerraformAzurerm(t *testing.T) {
	tf, err := exec.LookPath("terraform")
	if err != nil {
		t.Skip("terraform is not installed")
	}
	srv := pogi.Start(t, pogi.Config{Services: []string{"arm", "entra", "graph"}, DataDir: dataDir(t)})
	port := srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	env := append(os.Environ(), "TF_IN_AUTOMATION=1")
	if runtime.GOOS == "darwin" {
		if _, err := http.Get("https://localhost:" + port + "/metadata/endpoints?api-version=2022-09-01"); err != nil {
			t.Skipf("the system does not trust pogi's CA (%v); set POGI_TEST_DATA_DIR and trust %s", err, ca)
		}
	} else {
		env = append(env, "SSL_CERT_FILE="+ca)
	}

	dir := t.TempDir()
	cli := filepath.Join(dir, "terraform.rc")
	rc := ""
	if p := os.Getenv("POGI_AZURERM_PROVIDER_DIR"); p != "" {
		rc = fmt.Sprintf("provider_installation {\n  dev_overrides { \"hashicorp/azurerm\" = %q }\n  direct {}\n}\n", p)
	}
	os.WriteFile(cli, []byte(rc), 0o644)
	env = append(env, "TF_CLI_CONFIG_FILE="+cli)
	os.WriteFile(filepath.Join(dir, "main.tf"), []byte(fmt.Sprintf(`
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

provider "azurerm" {
  features {}
  metadata_host                   = "localhost:%s"
  subscription_id                 = %q
  tenant_id                       = %q
  client_id                       = "tf-app"
  client_secret                   = "tf-secret"
  resource_provider_registrations = "none"
}

resource "azurerm_resource_group" "rg" {
  name     = "tf-rg"
  location = "eastus"
  tags     = { env = "test" }
}

resource "azurerm_user_assigned_identity" "id" {
  name                = "tf-id"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}
`, port, core.AzureSubscription, "00000000-0000-0000-0000-000000000000")), 0o644)

	cx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	run := func(want int, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(cx, tf, append(args, "-no-color", "-input=false")...)
		cmd.Dir, cmd.Env = dir, env
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != want {
			t.Fatalf("terraform %s: exit %d, want %d\n%s", strings.Join(args, " "), code, want, out)
		}
	}
	if os.Getenv("POGI_AZURERM_PROVIDER_DIR") == "" {
		run(0, "init")
	}
	run(0, "apply", "-auto-approve")

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	status := func(path string) int {
		t.Helper()
		req, _ := http.NewRequest("GET", "https://localhost:"+port+path, nil)
		req.Header.Set("Authorization", "Bearer x")
		res, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	rg := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/tf-rg"
	if st := status(rg + "?api-version=2021-04-01"); st != 200 {
		t.Fatalf("resource group: %d", st)
	}
	if st := status(rg + "/providers/Microsoft.ManagedIdentity/userAssignedIdentities/tf-id?api-version=2023-01-31"); st != 200 {
		t.Fatalf("identity: %d", st)
	}
	run(0, "plan", "-detailed-exitcode") // exit 2 would be pending changes: a property the provider reads back differs
	run(0, "destroy", "-auto-approve")
	if st := status(rg + "?api-version=2021-04-01"); st != 404 {
		t.Fatalf("resource group after destroy: %d", st)
	}
}

func dataDir(t *testing.T) string {
	if d := os.Getenv("POGI_TEST_DATA_DIR"); d != "" {
		return d
	}
	return t.TempDir()
}
