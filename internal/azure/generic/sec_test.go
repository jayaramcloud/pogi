package generic_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/advisor/armadvisor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/chaos/armchaos/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/confidentialledger/armconfidentialledger"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/consumption/armconsumption/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/costmanagement/armcostmanagement/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/domainservices/armdomainservices"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/managedservices/armmanagedservices"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/purview/armpurview"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/security/armsecurity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/securityinsights/armsecurityinsights"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/solutions/armmanagedapplications/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/trustedsigning/armtrustedsigning"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

var secServices = []string{"azdomainservices", "azsecurity", "azsentinel", "azconfidentialledger", "aztrustedsigning",
	"azlighthouse", "azadvisor", "azcostmanagement", "azmanagedapps", "azpurview", "azchaos", "azmonitor"}

func TestLedgerSigningPurview(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)

	var err error
	led := armtest.Must(armconfidentialledger.NewLedgerClient(sub, e.Cred, e.Opts))
	pl := armtest.Must(led.BeginCreate(ctx, rg, "ledger1", armconfidentialledger.ConfidentialLedger{Location: to.Ptr(eus),
		Properties: &armconfidentialledger.LedgerProperties{LedgerType: to.Ptr(armconfidentialledger.LedgerTypePublic)}}, nil))
	l := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).ConfidentialLedger
	if *l.Properties.LedgerURI != "https://ledger1.confidential-ledger.azure.com" || *l.Properties.LedgerName != "ledger1" {
		t.Fatalf("ledger: %+v", l.Properties)
	}
	_, err = led.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	sg := armtest.Must(armtrustedsigning.NewCodeSigningAccountsClient(sub, e.Cred, e.Opts))
	ps := armtest.Must(sg.BeginCreate(ctx, rg, "sign1", armtrustedsigning.CodeSigningAccount{Location: to.Ptr(eus),
		Properties: &armtrustedsigning.CodeSigningAccountProperties{SKU: &armtrustedsigning.AccountSKU{Name: to.Ptr(armtrustedsigning.SKUNameBasic)}}}, nil))
	a := armtest.Must(ps.PollUntilDone(ctx, armtest.Fast)).CodeSigningAccount
	if *a.Properties.AccountURI != "https://eus.codesigning.azure.net" {
		t.Fatalf("signing account: %+v", a.Properties)
	}
	profiles := armtest.Must(armtrustedsigning.NewCertificateProfilesClient(sub, e.Cred, e.Opts))
	_, err = profiles.BeginCreate(ctx, rg, "nosuchaccount", "profile1", armtrustedsigning.CertificateProfile{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")

	pv := armtest.Must(armpurview.NewAccountsClient(sub, e.Cred, e.Opts))
	pp := armtest.Must(pv.BeginCreateOrUpdate(ctx, rg, "purview1", armpurview.Account{Location: to.Ptr(eus)}, nil))
	acc := armtest.Must(pp.PollUntilDone(ctx, armtest.Fast)).Account
	if !strings.HasPrefix(*acc.Properties.Endpoints.Catalog, "https://purview1.purview.azure.com/") || *acc.SKU.Name != armpurview.NameStandard {
		t.Fatalf("purview: %+v", acc.Properties)
	}
	_, err = pv.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestDefenderForCloud(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)
	scope := "subscriptions/" + sub
	pr := armtest.Must(armsecurity.NewPricingsClient(e.Cred, e.Opts))
	st := armtest.Must(armsecurity.NewSettingsClient(sub, e.Cred, e.Opts))
	ct := armtest.Must(armsecurity.NewContactsClient(sub, e.Cred, e.Opts))

	armtest.Must(pr.Update(ctx, scope, "VirtualMachines", armsecurity.Pricing{Properties: &armsecurity.PricingProperties{PricingTier: to.Ptr(armsecurity.PricingTierStandard)}}, nil))
	got := armtest.Must(pr.Get(ctx, scope, "VirtualMachines", nil))
	if *got.Properties.PricingTier != armsecurity.PricingTierStandard || *got.Name != "VirtualMachines" {
		t.Fatalf("pricing: %+v", got.Properties)
	}
	_, err := pr.Update(ctx, scope, "Dns", armsecurity.Pricing{Properties: &armsecurity.PricingProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = pr.Get(ctx, scope, "NoSuchPlan", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// the settings that every subscription has exist from the start, switched off
	s := armtest.Must(st.Get(ctx, armsecurity.SettingNameMCAS, nil))
	if d := s.SettingClassification.(*armsecurity.DataExportSettings); *d.Properties.Enabled {
		t.Fatalf("MCAS is on at the start: %+v", d.Properties)
	}
	armtest.Must(st.Update(ctx, armsecurity.SettingNameMCAS, &armsecurity.DataExportSettings{Kind: to.Ptr(armsecurity.SettingKindDataExportSettings),
		Properties: &armsecurity.DataExportSettingProperties{Enabled: to.Ptr(true)}}, nil))
	s = armtest.Must(st.Get(ctx, armsecurity.SettingNameMCAS, nil))
	if d := s.SettingClassification.(*armsecurity.DataExportSettings); !*d.Properties.Enabled {
		t.Fatal("MCAS stays on after it is switched on")
	}
	_, err = st.Get(ctx, armsecurity.SettingName("NoSuchSetting"), nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	c := armtest.Must(ct.Create(ctx, armsecurity.SecurityContactNameDefault, armsecurity.Contact{Properties: &armsecurity.ContactProperties{
		Emails: to.Ptr("security@example.com"), IsEnabled: to.Ptr(true)}}, nil))
	if *c.Properties.Emails != "security@example.com" {
		t.Fatalf("contact: %+v", c.Properties)
	}
}

func TestSentinel(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)
	res := armtest.Must(armresources.NewClient(sub, e.Cred, e.Opts))
	wsID := "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.OperationalInsights/workspaces/sent1"
	pl := armtest.Must(res.BeginCreateOrUpdateByID(ctx, wsID, "2022-10-01", armresources.GenericResource{Location: to.Ptr(eus)}, nil))
	armtest.Must(pl.PollUntilDone(ctx, armtest.Fast))

	rules := armtest.Must(armsecurityinsights.NewAlertRulesClient(sub, e.Cred, e.Opts))
	on := armtest.Must(armsecurityinsights.NewSentinelOnboardingStatesClient(sub, e.Cred, e.Opts))
	wl := armtest.Must(armsecurityinsights.NewWatchlistsClient(sub, e.Cred, e.Opts))
	armtest.Must(on.Create(ctx, rg, "sent1", "default", nil))
	r := armtest.Must(rules.CreateOrUpdate(ctx, rg, "sent1", "rule1", &armsecurityinsights.ScheduledAlertRule{Kind: to.Ptr(armsecurityinsights.AlertRuleKindScheduled),
		Properties: &armsecurityinsights.ScheduledAlertRuleProperties{DisplayName: to.Ptr("Failed sign-ins"), Enabled: to.Ptr(true), Severity: to.Ptr(armsecurityinsights.AlertSeverityLow),
			Query: to.Ptr("SigninLogs"), QueryFrequency: to.Ptr("PT1H"), QueryPeriod: to.Ptr("PT1H"), TriggerOperator: to.Ptr(armsecurityinsights.TriggerOperatorGreaterThan),
			TriggerThreshold: to.Ptr(int32(0)), SuppressionDuration: to.Ptr("PT1H"), SuppressionEnabled: to.Ptr(false)}}, nil))
	if got := r.AlertRuleClassification.(*armsecurityinsights.ScheduledAlertRule); *got.Properties.DisplayName != "Failed sign-ins" || *got.Name != "rule1" {
		t.Fatalf("rule: %+v", got.Properties)
	}
	armtest.Must(wl.CreateOrUpdate(ctx, rg, "sent1", "hosts", armsecurityinsights.Watchlist{Properties: &armsecurityinsights.WatchlistProperties{
		DisplayName: to.Ptr("Hosts"), Provider: to.Ptr("pogi"), Source: to.Ptr(armsecurityinsights.SourceLocalFile), ItemsSearchKey: to.Ptr("host")}}, nil))

	_, err := rules.Get(ctx, rg, "sent1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = wl.Get(ctx, rg, "sent1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(rules.Delete(ctx, rg, "sent1", "rule1", nil))
	_, err = rules.Get(ctx, rg, "sent1", "rule1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestLighthouse(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)
	scope := "subscriptions/" + sub
	defs := armtest.Must(armmanagedservices.NewRegistrationDefinitionsClient(e.Cred, e.Opts))
	asg := armtest.Must(armmanagedservices.NewRegistrationAssignmentsClient(e.Cred, e.Opts))
	const defID = "9d0a2c1e-1a2b-4c3d-8e4f-0123456789ab"
	pl := armtest.Must(defs.BeginCreateOrUpdate(ctx, defID, scope, armmanagedservices.RegistrationDefinition{Properties: &armmanagedservices.RegistrationDefinitionProperties{
		ManagedByTenantID: to.Ptr("22222222-3333-4444-5555-666666666666"), RegistrationDefinitionName: to.Ptr("Read access"),
		Authorizations: []*armmanagedservices.Authorization{{PrincipalID: to.Ptr("33333333-4444-5555-6666-777777777777"), RoleDefinitionID: to.Ptr("acdd72a7-3385-48ef-bd42-f606fba81ae7")}}}}, nil))
	d := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).RegistrationDefinition
	if *d.Name != defID || *d.Properties.RegistrationDefinitionName != "Read access" {
		t.Fatalf("definition: %+v", d.Properties)
	}
	pa := armtest.Must(asg.BeginCreateOrUpdate(ctx, scope, "8c1d9b2a-0000-4000-8000-000000000001", armmanagedservices.RegistrationAssignment{
		Properties: &armmanagedservices.RegistrationAssignmentProperties{RegistrationDefinitionID: d.ID}}, nil))
	a := armtest.Must(pa.PollUntilDone(ctx, armtest.Fast)).RegistrationAssignment
	if *a.Properties.RegistrationDefinitionID != *d.ID {
		t.Fatalf("assignment: %+v", a.Properties)
	}
	_, err := defs.Get(ctx, scope, "00000000-0000-4000-8000-00000000dead", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = asg.Get(ctx, scope, "00000000-0000-4000-8000-00000000dead", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestAdvisorCostManagedApps(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)

	cfg := armtest.Must(armadvisor.NewConfigurationsClient(sub, e.Cred, e.Opts))
	armtest.Must(cfg.CreateInSubscription(ctx, armadvisor.ConfigurationNameDefault, armadvisor.ConfigData{Properties: &armadvisor.ConfigDataProperties{
		Exclude: to.Ptr(true), LowCPUThreshold: to.Ptr(armadvisor.CPUThreshold("10"))}}, nil))
	if n := drain(t, cfg.NewListBySubscriptionPager(nil), func(r armadvisor.ConfigurationsClientListBySubscriptionResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("configurations: %d", n)
	}
	recs := armtest.Must(armadvisor.NewRecommendationsClient(sub, e.Cred, e.Opts))
	if n := drain(t, recs.NewListPager(nil), func(r armadvisor.RecommendationsClientListResponse) int { return len(r.Value) }); n != 0 {
		t.Fatalf("pogi makes no recommendations, got %d", n)
	}
	_, err := recs.Get(ctx, "subscriptions/"+sub+"/resourceGroups/"+rg, "no-such-recommendation", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	scope := "/subscriptions/" + sub
	bud := armtest.Must(armconsumption.NewBudgetsClient(e.Cred, e.Opts))
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	b := armtest.Must(bud.CreateOrUpdate(ctx, scope, "monthly", armconsumption.Budget{Properties: &armconsumption.BudgetProperties{
		Amount: to.Ptr(100.0), Category: to.Ptr(armconsumption.CategoryTypeCost), TimeGrain: to.Ptr(armconsumption.TimeGrainTypeMonthly),
		TimePeriod: &armconsumption.BudgetTimePeriod{StartDate: &start}}}, nil))
	if *b.Properties.Amount != 100 || *b.Name != "monthly" {
		t.Fatalf("budget: %+v", b.Properties)
	}
	_, err = bud.CreateOrUpdate(ctx, scope, "nodates", armconsumption.Budget{Properties: &armconsumption.BudgetProperties{Amount: to.Ptr(1.0)}}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = bud.Get(ctx, scope, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	ex := armtest.Must(armcostmanagement.NewExportsClient(e.Cred, e.Opts))
	end := start.AddDate(1, 0, 0)
	x := armtest.Must(ex.CreateOrUpdate(ctx, scope, "monthly", armcostmanagement.Export{Properties: &armcostmanagement.ExportProperties{
		Format:     to.Ptr(armcostmanagement.FormatTypeCSV),
		Definition: &armcostmanagement.ExportDefinition{Type: to.Ptr(armcostmanagement.ExportTypeUsage), Timeframe: to.Ptr(armcostmanagement.TimeframeTypeMonthToDate)},
		DeliveryInfo: &armcostmanagement.ExportDeliveryInfo{Destination: &armcostmanagement.ExportDeliveryDestination{
			ResourceID: to.Ptr(scope + "/resourceGroups/" + rg + "/providers/Microsoft.Storage/storageAccounts/exports"), Container: to.Ptr("exports"), RootFolderPath: to.Ptr("costs")}},
		Schedule: &armcostmanagement.ExportSchedule{Recurrence: to.Ptr(armcostmanagement.RecurrenceTypeMonthly), Status: to.Ptr(armcostmanagement.StatusTypeActive),
			RecurrencePeriod: &armcostmanagement.ExportRecurrencePeriod{From: &start, To: &end}}}}, nil))
	if *x.Name != "monthly" || *x.Properties.DeliveryInfo.Destination.Container != "exports" {
		t.Fatalf("export: %+v", x.Properties)
	}
	_, err = ex.Get(ctx, scope, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	defs := armtest.Must(armmanagedapplications.NewApplicationDefinitionsClient(sub, e.Cred, e.Opts))
	apps := armtest.Must(armmanagedapplications.NewApplicationsClient(sub, e.Cred, e.Opts))
	d := armtest.Must(defs.CreateOrUpdate(ctx, rg, "appdef", armmanagedapplications.ApplicationDefinition{Location: to.Ptr(eus), Properties: &armmanagedapplications.ApplicationDefinitionProperties{
		LockLevel: to.Ptr(armmanagedapplications.ApplicationLockLevelReadOnly), DisplayName: to.Ptr("Pogi app"), PackageFileURI: to.Ptr("https://example.com/app.zip"),
		Authorizations: []*armmanagedapplications.ApplicationAuthorization{{PrincipalID: to.Ptr("44444444-5555-6666-7777-888888888888"), RoleDefinitionID: to.Ptr("acdd72a7-3385-48ef-bd42-f606fba81ae7")}}}}, nil))
	pa := armtest.Must(apps.BeginCreateOrUpdate(ctx, rg, "app1", armmanagedapplications.Application{Location: to.Ptr(eus), Kind: to.Ptr("ServiceCatalog"),
		Properties: &armmanagedapplications.ApplicationProperties{ApplicationDefinitionID: d.ID, ManagedResourceGroupID: to.Ptr("/subscriptions/" + sub + "/resourceGroups/app1-managed")}}, nil))
	a := armtest.Must(pa.PollUntilDone(ctx, armtest.Fast)).Application
	if *a.Properties.ApplicationDefinitionID != *d.ID {
		t.Fatalf("application: %+v", a.Properties)
	}
	// this client reads a 404 as a success with nothing in it
	if r := armtest.Must(apps.Get(ctx, rg, "nope", nil)); r.ID != nil {
		t.Fatalf("an application that does not exist: %+v", r)
	}
	_, err = apps.BeginCreateOrUpdate(ctx, rg, "app2", armmanagedapplications.Application{Kind: to.Ptr("ServiceCatalog")}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	armtest.Must(defs.Delete(ctx, rg, "appdef", nil))
}

func TestChaosStudio(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)
	targets := armtest.Must(armchaos.NewTargetsClient(sub, e.Cred, e.Opts))
	tt := armtest.Must(armchaos.NewTargetTypesClient(sub, e.Cred, e.Opts))
	exps := armtest.Must(armchaos.NewExperimentsClient(sub, e.Cred, e.Opts))

	got := armtest.Must(tt.Get(ctx, eus, "Microsoft-Agent", nil))
	if *got.Name != "Microsoft-Agent" {
		t.Fatalf("target type: %+v", got)
	}
	_, err := tt.Get(ctx, eus, "Microsoft-NoSuchThing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	// a target is made on any resource, here a virtual machine that does not have to exist
	tg := armtest.Must(targets.CreateOrUpdate(ctx, rg, "Microsoft.Compute", "virtualMachines", "vm1", "Microsoft-Agent", armchaos.Target{Properties: map[string]any{}}, nil)).Target
	if *tg.Name != "Microsoft-Agent" || !strings.HasSuffix(*tg.ID, "/virtualMachines/vm1/providers/Microsoft.Chaos/targets/Microsoft-Agent") {
		t.Fatalf("target: %+v", tg)
	}
	armtest.Must(targets.Get(ctx, rg, "Microsoft.Compute", "virtualMachines", "vm1", "Microsoft-Agent", nil))
	_, err = targets.Get(ctx, rg, "Microsoft.Compute", "virtualMachines", "vm1", "Microsoft-KeyVault", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	pl := armtest.Must(exps.BeginCreateOrUpdate(ctx, rg, "exp1", armchaos.Experiment{Location: to.Ptr(eus),
		Identity: &armchaos.ManagedServiceIdentity{Type: to.Ptr(armchaos.ManagedServiceIdentityTypeSystemAssigned)},
		Properties: &armchaos.ExperimentProperties{
			Selectors: []armchaos.TargetSelectorClassification{&armchaos.TargetListSelector{ID: to.Ptr("sel1"), Type: to.Ptr(armchaos.SelectorTypeList),
				Targets: []*armchaos.TargetReference{{ID: tg.ID, Type: to.Ptr(armchaos.TargetReferenceTypeChaosTarget)}}}},
			Steps: []*armchaos.ExperimentStep{{Name: to.Ptr("step1"), Branches: []*armchaos.ExperimentBranch{{Name: to.Ptr("b1"), Actions: []armchaos.ExperimentActionClassification{
				&armchaos.ContinuousAction{Name: to.Ptr("urn:csci:microsoft:agent:cpuPressure/1.0"), Type: to.Ptr(armchaos.ExperimentActionTypeContinuous), Duration: to.Ptr("PT10M"), SelectorID: to.Ptr("sel1"),
					Parameters: []*armchaos.KeyValuePair{{Key: to.Ptr("pCpuUtilization"), Value: to.Ptr("95")}}}}}}}}}}, nil))
	x := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Experiment
	if *x.Name != "exp1" || *x.Identity.PrincipalID == "" {
		t.Fatalf("experiment: %+v", x)
	}
	_, err = exps.BeginCreateOrUpdate(ctx, rg, "exp2", armchaos.Experiment{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = exps.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestEntraDomainServices(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, secServices...)
	ds := armtest.Must(armdomainservices.NewClient(sub, e.Cred, e.Opts))
	subnet := "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Network/virtualNetworks/vnet/subnets/aadds"
	pl := armtest.Must(ds.BeginCreateOrUpdate(ctx, rg, "aadds1", armdomainservices.DomainService{Location: to.Ptr(eus), Properties: &armdomainservices.DomainServiceProperties{
		DomainName: to.Ptr("pogi.example.com"), ReplicaSets: []*armdomainservices.ReplicaSet{{Location: to.Ptr(eus), SubnetID: to.Ptr(subnet)}}}}, nil))
	d := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).DomainService
	rs := d.Properties.ReplicaSets
	if len(rs) != 1 || *rs[0].ServiceStatus != "Running" || *rs[0].SubnetID != subnet || len(rs[0].DomainControllerIPAddress) != 2 || *rs[0].ReplicaSetID == "" {
		t.Fatalf("replica sets: %+v", rs)
	}
	again := armtest.Must(ds.Get(ctx, rg, "aadds1", nil)).DomainService
	if *again.Properties.ReplicaSets[0].ReplicaSetID != *rs[0].ReplicaSetID {
		t.Fatal("a replica set keeps its id")
	}
	_, err := ds.BeginCreateOrUpdate(ctx, rg, "aadds2", armdomainservices.DomainService{Properties: &armdomainservices.DomainServiceProperties{}}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = ds.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	pd := armtest.Must(ds.BeginDelete(ctx, rg, "aadds1", nil))
	armtest.Must(pd.PollUntilDone(ctx, armtest.Fast))
	_, err = ds.Get(ctx, rg, "aadds1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}
