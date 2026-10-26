package core

import (
	"slices"
	"testing"
)

// `az login` against pogi fails without this: MSAL first asks login.microsoftonline.com about the authority.
func TestEnvTurnsOffAzureCLIInstanceDiscovery(t *testing.T) {
	if !slices.Contains(Env("http://localhost:7644", "us-east-1"), "AZURE_CORE_INSTANCE_DISCOVERY=false") {
		t.Fatal("Env does not set AZURE_CORE_INSTANCE_DISCOVERY=false")
	}
}
