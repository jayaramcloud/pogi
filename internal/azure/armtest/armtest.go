// Package armtest is what tests of Azure management planes share: a pogi
// with the Resource Manager and the sign-in services, and client options that
// trust its CA and sign in with client credentials.
package armtest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azarm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
)

// Ctx is the context of the calls tests make.
var Ctx = context.Background()

// Fast makes the SDK poll long-running operations at once, not every second.
var Fast = &runtime.PollUntilDoneOptions{Frequency: 10 * time.Millisecond}

// Must returns v, or panics on err.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Env is a running pogi and how to reach its Resource Manager.
type Env struct {
	Srv  *pogi.Running
	Base string // https://localhost:port
	Cred azcore.TokenCredential
	Opts *azarm.ClientOptions
}

// Start runs pogi with the Resource Manager, the sign-in services and the services named; cfg
// is the rest of the configuration (its Services are replaced).
func Start(t *testing.T, cfg pogi.Config, services ...string) *Env {
	t.Helper()
	cfg.Services = append([]string{"arm", "entra", "graph"}, services...)
	srv := pogi.Start(t, cfg)
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(Must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	cred := Must(azidentity.NewClientSecretCredential(arm.TenantID, "app", "secret",
		&azidentity.ClientSecretCredentialOptions{ClientOptions: co, DisableInstanceDiscovery: true}))
	return &Env{Srv: srv, Base: base, Cred: cred, Opts: &azarm.ClientOptions{ClientOptions: co}}
}

// WantErr fails the test unless err is the ARM error with this status and code.
func WantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || re.ErrorCode != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
}
