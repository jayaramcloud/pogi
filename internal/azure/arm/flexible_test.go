package arm_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/nrndev/pogi/internal/core"
)

// The databases service's Providers, through the ARM surface and the official
// PostgreSQL client: a server, a database and a firewall rule, metadata only.
func TestPostgresFlexibleServer(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	e := start(t, "azdatabases")
	must(e.factory(core.AzureSubscription).NewResourceGroupsClient().CreateOrUpdate(ctx, "db", armresources.ResourceGroup{Location: to.Ptr("eastus")}, nil))
	f := must(armpostgresqlflexibleservers.NewClientFactory(core.AzureSubscription, e.cred, e.opts))

	servers := f.NewServersClient()
	poller := must(servers.BeginCreate(ctx, "db", "pg1", armpostgresqlflexibleservers.Server{
		Location: to.Ptr("eastus"),
		SKU:      &armpostgresqlflexibleservers.SKU{Name: to.Ptr("Standard_D2s_v3"), Tier: to.Ptr(armpostgresqlflexibleservers.SKUTierGeneralPurpose)},
		Properties: &armpostgresqlflexibleservers.ServerProperties{AdministratorLogin: to.Ptr("pgadmin"), AdministratorLoginPassword: to.Ptr("Pogi-test-Passw0rd"),
			Version: to.Ptr(armpostgresqlflexibleservers.ServerVersion("15"))}}, nil))
	srv := must(poller.PollUntilDone(ctx, &azruntimePoll))
	if *srv.Name != "pg1" || *srv.Type != "Microsoft.DBforPostgreSQL/flexibleServers" || *srv.Properties.AdministratorLogin != "pgadmin" || *srv.Properties.State != armpostgresqlflexibleservers.ServerStateReady {
		t.Fatalf("%+v", srv.Properties)
	}
	if got := must(servers.Get(ctx, "db", "pg1", nil)); *got.SKU.Name != "Standard_D2s_v3" || got.Properties.AdministratorLoginPassword != nil {
		t.Fatalf("%+v", got)
	}
	must(must(f.NewDatabasesClient().BeginCreate(ctx, "db", "pg1", "app", armpostgresqlflexibleservers.Database{}, nil)).PollUntilDone(ctx, &azruntimePoll))
	must(must(f.NewFirewallRulesClient().BeginCreateOrUpdate(ctx, "db", "pg1", "all", armpostgresqlflexibleservers.FirewallRule{
		Properties: &armpostgresqlflexibleservers.FirewallRuleProperties{StartIPAddress: to.Ptr("0.0.0.0"), EndIPAddress: to.Ptr("255.255.255.255")}}, nil)).PollUntilDone(ctx, &azruntimePoll))
	var dbs []string
	for p := f.NewDatabasesClient().NewListByServerPager("db", "pg1", nil); p.More(); {
		for _, d := range must(p.NextPage(ctx)).Value {
			dbs = append(dbs, *d.Name)
		}
	}
	if !strings.Contains(strings.Join(dbs, ","), "app") {
		t.Fatal(dbs)
	}
	var names []string
	for p := servers.NewListByResourceGroupPager("db", nil); p.More(); {
		for _, s := range must(p.NextPage(ctx)).Value {
			names = append(names, *s.Name)
		}
	}
	if len(names) != 1 || names[0] != "pg1" {
		t.Fatal(names)
	}
	// deleting the group deletes the server with its children
	must(must(e.factory(core.AzureSubscription).NewResourceGroupsClient().BeginDelete(ctx, "db", nil)).PollUntilDone(ctx, &azruntimePoll))
	if st, _, _ := e.do(t, "GET", "/subscriptions/"+core.AzureSubscription+"/resourceGroups/db/providers/Microsoft.DBforPostgreSQL/flexibleServers/pg1?api-version=2023-06-01-preview", ""); st != 404 {
		t.Fatal(st)
	}
}
