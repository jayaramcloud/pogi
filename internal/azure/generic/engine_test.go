package generic_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/azure/generic"
	"github.com/nrndev/pogi/internal/core"
)

// drain reads every page of a pager and returns how many items there were.
func drain[R any](t *testing.T, p *runtime.Pager[R], size func(R) int) int {
	t.Helper()
	n := 0
	for p.More() {
		n += size(armtest.Must(p.NextPage(ctx)))
	}
	return n
}

// Types that no service lists are still refused, in the two ways ARM has.
func TestUnlistedTypes(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdns", "azpolicy", "azlogic")
	c := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	body := armresources.GenericResource{Location: to.Ptr("eastus")}
	_, err := c.BeginCreateOrUpdate(ctx, rg, "Microsoft.Network", "", "trafficManagerProfiles", "tm", "2022-04-01", body, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceType")
	_, err = c.BeginCreateOrUpdate(ctx, rg, "Microsoft.Network", "", "dnsZonesNope", "z", "2018-05-01", body, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceType")
	_, err = c.BeginCreateOrUpdate(ctx, rg, "Microsoft.Bogus", "", "things", "t", "2022-01-01", body, nil)
	armtest.WantErr(t, err, 400, "NoRegisteredProviderFound")
	// a listed type of a namespace is served, through the generic client too
	armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "Microsoft.Logic", "", "workflows", "viaclient", "2019-05-01", body, nil))
	if _, err := c.Get(ctx, rg, "Microsoft.Logic", "", "workflows", "viaclient", "2019-05-01", nil); err != nil {
		t.Fatal(err)
	}
}

// A type that a hand-written Provider serves never reaches the engine, whichever way round they come.
func TestHandWrittenWins(t *testing.T) {
	e := armtest.Start(t, pogi.Config{})
	hits := 0
	var stored map[string]any
	arm.Register(e.Srv.Hub, arm.Provider{Type: "Microsoft.Logic/workflows",
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			hits++
			stored = map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": "eastus", "properties": map[string]any{"by": "hand"}}
			return stored, nil
		},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			if stored == nil {
				return nil, arm.NotFound(id)
			}
			return stored, nil
		}})
	generic.New(e.Srv.Hub, "azlogic")
	c := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	got := armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "Microsoft.Logic", "", "workflows", "w", "2019-05-01",
		armresources.GenericResource{Location: to.Ptr("eastus")}, nil)).PollUntilDone(ctx, armtest.Fast))
	if hits != 1 || got.Properties.(map[string]any)["by"] != "hand" {
		t.Fatalf("the hand-written Provider was not used: %d %+v", hits, got)
	}
}

// A long-running PATCH answers 202 with an operation where its spec lists 202 (Redis Enterprise), and stays a
// plain call where the spec lists only 200 (IoT Hub): SDKs generated from a spec accept only its statuses.
func TestLongRunningPatch(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azmanagedredis", "aziothub")
	for typ, want := range map[string]bool{"Microsoft.Cache/redisEnterprise": true, "Microsoft.Devices/IotHubs": false} {
		if p := arm.Lookup(e.Srv.Hub, typ); p == nil || p.AsyncPatch != want {
			t.Fatal(typ, p)
		}
	}
	c := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	const v = "2025-04-01"
	armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "Microsoft.Cache", "", "redisEnterprise", "re1", v,
		armresources.GenericResource{Location: to.Ptr("eastus"), SKU: &armresources.SKU{Name: to.Ptr("Balanced_B0")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	got := armtest.Must(armtest.Must(c.BeginUpdate(ctx, rg, "Microsoft.Cache", "", "redisEnterprise", "re1", v,
		armresources.GenericResource{Tags: map[string]*string{"env": to.Ptr("dev")}}, nil)).PollUntilDone(ctx, armtest.Fast))
	if got.Tags["env"] == nil || *got.Tags["env"] != "dev" {
		t.Fatalf("%+v", got)
	}
}
