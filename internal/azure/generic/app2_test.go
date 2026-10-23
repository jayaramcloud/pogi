package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/automation/armautomation"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/iothub/armiothub"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/maps/armmaps"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/recoveryservices/armrecoveryservices/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

func TestIoTHub(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "aziothub")
	c := armtest.Must(armiothub.NewResourceClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "sensors", armiothub.Description{Location: to.Ptr("eastus"),
		SKU:        &armiothub.SKUInfo{Name: to.Ptr(armiothub.IotHubSKUS1), Capacity: to.Ptr[int64](1)},
		Properties: &armiothub.Properties{EventHubEndpoints: map[string]*armiothub.EventHubProperties{"events": {RetentionTimeInDays: to.Ptr[int64](3), PartitionCount: to.Ptr[int32](2)}}}}, nil))
	hub := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Description
	ev := hub.Properties.EventHubEndpoints["events"]
	if *hub.Properties.HostName != "sensors.azure-devices.net" || *hub.SKU.Tier != armiothub.IotHubSKUTierStandard || *hub.Properties.State != "Active" {
		t.Fatalf("hub: %+v %+v", hub.Properties, hub.SKU)
	}
	// the Event Hubs-compatible endpoint: what the client asked for, and the names the service makes
	if *ev.RetentionTimeInDays != 3 || len(ev.PartitionIDs) != 2 || *ev.Path != "sensors" || !strings.HasPrefix(*ev.Endpoint, "sb://iothub-ns-sensors-") || !strings.HasSuffix(*ev.Endpoint, ".servicebus.windows.net/") {
		t.Fatalf("event hub endpoint: %+v", ev)
	}

	// the five default policies, with keys that stay the same
	keys := func() map[string]*armiothub.SharedAccessSignatureAuthorizationRule {
		out := map[string]*armiothub.SharedAccessSignatureAuthorizationRule{}
		for p := c.NewListKeysPager(rg, "sensors", nil); p.More(); {
			for _, r := range armtest.Must(p.NextPage(ctx)).Value {
				out[*r.KeyName] = r
			}
		}
		return out
	}
	k := keys()
	if len(k) != 5 || k["iothubowner"] == nil || *k["iothubowner"].Rights != "RegistryWrite, ServiceConnect, DeviceConnect" || *k["service"].PrimaryKey == *k["device"].PrimaryKey {
		t.Fatalf("policies: %v", k)
	}
	one := armtest.Must(c.GetKeysForKeyName(ctx, rg, "sensors", "service", nil)).SharedAccessSignatureAuthorizationRule
	if *one.PrimaryKey != *k["service"].PrimaryKey || *keys()["service"].PrimaryKey != *k["service"].PrimaryKey {
		t.Fatal("the keys change between reads")
	}

	// a consumer group: $Default is there already; listing them is by event hub endpoint
	armtest.Must(c.CreateEventHubConsumerGroup(ctx, rg, "sensors", "events", "reports", armiothub.EventHubConsumerGroupBodyDescription{Properties: &armiothub.EventHubConsumerGroupName{Name: to.Ptr("reports")}}, nil))
	names := map[string]bool{}
	for p := c.NewListEventHubConsumerGroupsPager(rg, "sensors", "events", nil); p.More(); {
		for _, g := range armtest.Must(p.NextPage(ctx)).Value {
			names[*g.Name] = true
		}
	}
	if len(names) != 2 || !names["$Default"] || !names["reports"] {
		t.Fatalf("consumer groups: %v", names)
	}

	// policies the client sends replace the defaults; an update that names none keeps them
	desc := armtest.Must(c.Get(ctx, rg, "sensors", nil)).Description
	desc.Properties.AuthorizationPolicies = []*armiothub.SharedAccessSignatureAuthorizationRule{{KeyName: to.Ptr("ingest"), Rights: to.Ptr(armiothub.AccessRightsServiceConnect)}}
	armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "sensors", desc, nil)).PollUntilDone(ctx, armtest.Fast))
	if k := keys(); len(k) != 1 || *k["ingest"].PrimaryKey == "" {
		t.Fatalf("policies after the update: %v", k)
	}
}

func TestIoTHubFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "aziothub")
	c := armtest.Must(armiothub.NewResourceClient(sub, e.Cred, e.Opts))
	_, err := c.BeginCreateOrUpdate(ctx, rg, "nolocation", armiothub.Description{SKU: &armiothub.SKUInfo{Name: to.Ptr(armiothub.IotHubSKUF1)}}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.BeginCreateOrUpdate(ctx, rg, "nosku", armiothub.Description{Location: to.Ptr("eastus")}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = c.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.GetKeysForKeyName(ctx, rg, "missing", "service", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.CreateEventHubConsumerGroup(ctx, rg, "missing", "events", "g", armiothub.EventHubConsumerGroupBodyDescription{Properties: &armiothub.EventHubConsumerGroupName{Name: to.Ptr("g")}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "here", armiothub.Description{Location: to.Ptr("eastus"), SKU: &armiothub.SKUInfo{Name: to.Ptr(armiothub.IotHubSKUF1)}}, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err = c.GetKeysForKeyName(ctx, rg, "here", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.GetEventHubConsumerGroup(ctx, rg, "here", "events", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.CreateEventHubConsumerGroup(ctx, rg, "here", "other", "g", armiothub.EventHubConsumerGroupBodyDescription{Properties: &armiothub.EventHubConsumerGroupName{Name: to.Ptr("g")}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
}

func TestMaps(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azmaps")
	c := armtest.Must(armmaps.NewAccountsClient(sub, e.Cred, e.Opts))
	a := armtest.Must(c.CreateOrUpdate(ctx, rg, "atlas", armmaps.Account{Location: to.Ptr("global"), SKU: &armmaps.SKU{Name: to.Ptr(armmaps.NameG2)},
		Kind: to.Ptr(armmaps.KindGen2), Tags: map[string]*string{"env": to.Ptr("dev")}}, nil)).Account
	if len(*a.Properties.UniqueID) != 36 || *a.Properties.UniqueID != *armtest.Must(c.Get(ctx, rg, "atlas", nil)).Properties.UniqueID || *a.SKU.Name != armmaps.NameG2 {
		t.Fatalf("account: %+v", a.Properties)
	}
	k := armtest.Must(c.ListKeys(ctx, rg, "atlas", nil)).AccountKeys
	if *k.PrimaryKey == "" || *k.PrimaryKey == *k.SecondaryKey || *armtest.Must(c.ListKeys(ctx, rg, "atlas", nil)).PrimaryKey != *k.PrimaryKey {
		t.Fatalf("keys: %+v", k)
	}
	if n := drain(t, c.NewListByResourceGroupPager(rg, nil), func(r armmaps.AccountsClientListByResourceGroupResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("accounts: %d", n)
	}
	armtest.Must(c.Delete(ctx, rg, "atlas", nil))
	_, err := c.Get(ctx, rg, "atlas", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestMapsFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azmaps")
	c := armtest.Must(armmaps.NewAccountsClient(sub, e.Cred, e.Opts))
	_, err := c.CreateOrUpdate(ctx, rg, "nolocation", armmaps.Account{SKU: &armmaps.SKU{Name: to.Ptr(armmaps.NameG2)}}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.CreateOrUpdate(ctx, rg, "nosku", armmaps.Account{Location: to.Ptr("global")}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = c.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.ListKeys(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestAutomation(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azautomation")
	acc := armtest.Must(armautomation.NewAccountClient(sub, e.Cred, e.Opts))
	rb := armtest.Must(armautomation.NewRunbookClient(sub, e.Cred, e.Opts))
	draft := armtest.Must(armautomation.NewRunbookDraftClient(sub, e.Cred, e.Opts))

	a := armtest.Must(acc.CreateOrUpdate(ctx, rg, "ops", armautomation.AccountCreateOrUpdateParameters{Location: to.Ptr("eastus"),
		Properties: &armautomation.AccountCreateOrUpdateProperties{SKU: &armautomation.SKU{Name: to.Ptr(armautomation.SKUNameEnumBasic)}}}, nil)).Account
	if *a.Properties.State != armautomation.AutomationAccountStateOk || *a.Properties.SKU.Name != armautomation.SKUNameEnumBasic {
		t.Fatalf("account: %+v", a.Properties)
	}

	r := armtest.Must(rb.CreateOrUpdate(ctx, rg, "ops", "hello", armautomation.RunbookCreateOrUpdateParameters{Location: to.Ptr("eastus"),
		Properties: &armautomation.RunbookCreateOrUpdateProperties{RunbookType: to.Ptr(armautomation.RunbookTypeEnumPowerShell), LogVerbose: to.Ptr(true)}}, nil)).Runbook
	if *r.Properties.State != armautomation.RunbookStateNew || !*r.Properties.LogVerbose {
		t.Fatalf("runbook: %+v", r.Properties)
	}
	// the script is text: put as the draft, then published, then read back
	const script = "Write-Output 'hello from pogi'\n"
	armtest.Must(armtest.Must(draft.BeginReplaceContent(ctx, rg, "ops", "hello", script, nil)).PollUntilDone(ctx, armtest.Fast))
	armtest.Must(armtest.Must(rb.BeginPublish(ctx, rg, "ops", "hello", nil)).PollUntilDone(ctx, armtest.Fast))
	if got := *armtest.Must(rb.GetContent(ctx, rg, "ops", "hello", nil)).Value; got != script {
		t.Fatalf("content: %q", got)
	}
	if st := armtest.Must(rb.Get(ctx, rg, "ops", "hello", nil)).Properties.State; *st != armautomation.RunbookStatePublished {
		t.Fatalf("state after publish: %s", *st)
	}
	// an update keeps the published state
	armtest.Must(rb.CreateOrUpdate(ctx, rg, "ops", "hello", armautomation.RunbookCreateOrUpdateParameters{Location: to.Ptr("eastus"),
		Properties: &armautomation.RunbookCreateOrUpdateProperties{RunbookType: to.Ptr(armautomation.RunbookTypeEnumPowerShell), Description: to.Ptr("greets")}}, nil))
	if g := armtest.Must(rb.Get(ctx, rg, "ops", "hello", nil)); *g.Properties.State != armautomation.RunbookStatePublished || *g.Properties.Description != "greets" {
		t.Fatalf("after update: %+v", g.Properties)
	}
	armtest.Must(rb.Delete(ctx, rg, "ops", "hello", nil))
	_, err := rb.Get(ctx, rg, "ops", "hello", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestAutomationFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azautomation")
	acc := armtest.Must(armautomation.NewAccountClient(sub, e.Cred, e.Opts))
	rb := armtest.Must(armautomation.NewRunbookClient(sub, e.Cred, e.Opts))
	draft := armtest.Must(armautomation.NewRunbookDraftClient(sub, e.Cred, e.Opts))
	params := armautomation.RunbookCreateOrUpdateParameters{Location: to.Ptr("eastus"), Properties: &armautomation.RunbookCreateOrUpdateProperties{RunbookType: to.Ptr(armautomation.RunbookTypeEnumPowerShell)}}

	_, err := acc.CreateOrUpdate(ctx, rg, "nolocation", armautomation.AccountCreateOrUpdateParameters{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = acc.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = rb.CreateOrUpdate(ctx, rg, "missing", "hello", params, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = rb.Get(ctx, rg, "missing", "hello", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(acc.CreateOrUpdate(ctx, rg, "here", armautomation.AccountCreateOrUpdateParameters{Location: to.Ptr("eastus")}, nil))
	_, err = rb.CreateOrUpdate(ctx, rg, "here", "1bad", params, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = rb.CreateOrUpdate(ctx, rg, "here", "norunbooktype", armautomation.RunbookCreateOrUpdateParameters{Location: to.Ptr("eastus"), Properties: &armautomation.RunbookCreateOrUpdateProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = rb.BeginPublish(ctx, rg, "here", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = draft.BeginReplaceContent(ctx, rg, "here", "nope", "x", nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = rb.GetContent(ctx, rg, "here", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestRecoveryServices(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azrecoveryservices")
	c := armtest.Must(armrecoveryservices.NewVaultsClient(sub, e.Cred, e.Opts))
	res := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "vault1", armrecoveryservices.Vault{Location: to.Ptr("eastus"),
		SKU: &armrecoveryservices.SKU{Name: to.Ptr(armrecoveryservices.SKUNameStandard)}, Properties: &armrecoveryservices.VaultProperties{}}, nil))
	v := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Vault
	if *v.SKU.Name != armrecoveryservices.SKUNameStandard || *v.Properties.ProvisioningState != "Succeeded" {
		t.Fatalf("vault: %+v", v.Properties)
	}
	// the settings that Terraform reads come with the vault
	for _, p := range []struct{ typ, name, field string }{
		{"backupconfig", "vaultconfig", "softDeleteFeatureState"}, {"backupstorageconfig", "vaultstorageconfig", "storageModelType"},
	} {
		g := armtest.Must(res.Get(ctx, rg, "Microsoft.RecoveryServices", "vaults/vault1", p.typ, p.name, "2023-02-01", nil))
		if props, _ := g.Properties.(map[string]any); props[p.field] == nil {
			t.Fatalf("%s: %v", p.typ, g.Properties)
		}
	}

	// a backup policy (the Backup client is not in go.mod, so a generic resource stands in for it)
	pol := armtest.Must(res.BeginCreateOrUpdate(ctx, rg, "Microsoft.RecoveryServices", "vaults/vault1", "backupPolicies", "daily", "2023-02-01", armresources.GenericResource{
		Location: to.Ptr("eastus"), Properties: map[string]any{"backupManagementType": "AzureIaasVM", "instantRpRetentionRangeInDays": 2,
			"schedulePolicy": map[string]any{"scheduleRunTimes": []any{"23:00:00Z"}}}}, nil))
	armtest.Must(pol.PollUntilDone(ctx, armtest.Fast))
	g := armtest.Must(res.Get(ctx, rg, "Microsoft.RecoveryServices", "vaults/vault1", "backupPolicies", "daily", "2023-02-01", nil))
	if props, _ := g.Properties.(map[string]any); props["backupManagementType"] != "AzureIaasVM" || props["instantRpRetentionRangeInDays"] != float64(2) {
		t.Fatalf("policy: %v", g.Properties)
	} else if times := props["schedulePolicy"].(map[string]any)["scheduleRunTimes"].([]any); times[0] != "2018-01-01T23:00:00Z" { // Terraform reads the time from a date-time
		t.Fatalf("schedule times: %v", times)
	}
	if n := drain(t, c.NewListByResourceGroupPager(rg, nil), func(r armrecoveryservices.VaultsClientListByResourceGroupResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("vaults: %d", n)
	}
	armtest.Must(armtest.Must(c.BeginDelete(ctx, rg, "vault1", nil)).PollUntilDone(ctx, armtest.Fast))
	_, err := c.Get(ctx, rg, "vault1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestRecoveryServicesFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azrecoveryservices")
	c := armtest.Must(armrecoveryservices.NewVaultsClient(sub, e.Cred, e.Opts))
	res := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	policy := armresources.GenericResource{Location: to.Ptr("eastus"), Properties: map[string]any{"backupManagementType": "AzureIaasVM"}}

	_, err := c.BeginCreateOrUpdate(ctx, rg, "nolocation", armrecoveryservices.Vault{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = res.BeginCreateOrUpdate(ctx, rg, "Microsoft.RecoveryServices", "vaults/missing", "backupPolicies", "daily", "2023-02-01", policy, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = res.Get(ctx, rg, "Microsoft.RecoveryServices", "vaults/missing", "backupPolicies", "daily", "2023-02-01", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "here", armrecoveryservices.Vault{Location: to.Ptr("eastus")}, nil)).PollUntilDone(ctx, armtest.Fast))
	_, err = res.BeginCreateOrUpdate(ctx, rg, "Microsoft.RecoveryServices", "vaults/here", "backupPolicies", "daily", "2023-02-01",
		armresources.GenericResource{Location: to.Ptr("eastus"), Properties: map[string]any{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = res.Get(ctx, rg, "Microsoft.RecoveryServices", "vaults/here", "backupPolicies", "nope", "2023-02-01", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}
