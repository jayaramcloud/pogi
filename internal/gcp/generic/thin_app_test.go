package generic_test

import (
	"encoding/json"
	"testing"

	"github.com/nrndev/pogi"
	"google.golang.org/api/apigee/v1"
	"google.golang.org/api/apihub/v1"
	"google.golang.org/api/apphub/v1"
	"google.golang.org/api/billingbudgets/v1"
	"google.golang.org/api/blockchainnodeengine/v1"
	"google.golang.org/api/connectors/v1"
	"google.golang.org/api/developerconnect/v1"
	"google.golang.org/api/firebaseappcheck/v1"
	"google.golang.org/api/firebaseapphosting/v1"
	"google.golang.org/api/firebasedataconnect/v1"
	"google.golang.org/api/integrations/v1"
	"google.golang.org/api/migrationcenter/v1"
	"google.golang.org/api/parametermanager/v1"
	"google.golang.org/api/securesourcemanager/v1"
	"google.golang.org/api/servicemanagement/v1"

	licensemanager "cloud.google.com/go/licensemanager/apiv1"
	"cloud.google.com/go/licensemanager/apiv1/licensemanagerpb"
	"google.golang.org/api/option"
)

func startApp(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"apigee", "apihub", "apphub", "integrations", "connectors",
		"servicemanagement", "developerconnect", "securesourcemanager", "parametermanager", "billingbudgets",
		"migrationcenter", "blockchainnodeengine", "licensemanager", "firebaseappcheck", "firebaseapphosting",
		"firebasedataconnect", "operations"}})
}

func TestApigee(t *testing.T) {
	srv := startApp(t)
	svc := must(apigee.NewService(ctx, opts(srv, "/apigee/")...))
	orgs := svc.Organizations
	// an organization is named after its project
	op := must(orgs.Create(&apigee.GoogleCloudApigeeV1Organization{AnalyticsRegion: "us-central1", RuntimeType: "HYBRID"}).Parent("projects/pg").Do())
	if !op.Done || op.Name == "" {
		t.Fatalf("%+v", op)
	}
	if o := must(orgs.Get("organizations/pg").Do()); o.Name != "pg" || o.ProjectId != "pg" || o.AnalyticsRegion != "us-central1" {
		t.Fatalf("%+v", o)
	}
	_, err := orgs.Create(&apigee.GoogleCloudApigeeV1Organization{}).Parent("projects/pg").Do()
	wantCode(t, err, 409, "already exists")
	_, err = orgs.Create(&apigee.GoogleCloudApigeeV1Organization{}).Parent("pg").Do()
	wantCode(t, err, 400, "Invalid parent")
	_, err = orgs.Get("organizations/nope").Do()
	wantCode(t, err, 404, "nope")

	// an environment keeps its short name
	envs := orgs.Environments
	must(envs.Create("organizations/pg", &apigee.GoogleCloudApigeeV1Environment{Name: "test", Description: "d"}).Do())
	if e := must(envs.Get("organizations/pg/environments/test").Do()); e.Name != "test" || e.Description != "d" {
		t.Fatalf("%+v", e)
	}
	_, err = envs.Create("organizations/pg", &apigee.GoogleCloudApigeeV1Environment{Name: "test"}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = envs.Create("organizations/pg", &apigee.GoogleCloudApigeeV1Environment{}).Do()
	wantCode(t, err, 400, `"name"`)
	_, err = envs.Create("organizations/nope", &apigee.GoogleCloudApigeeV1Environment{Name: "x"}).Do()
	wantCode(t, err, 404, "organizations/nope")
	must(envs.Delete("organizations/pg/environments/test").Do())
	_, err = envs.Delete("organizations/pg/environments/test").Do()
	wantCode(t, err, 404, "test")

	// a developer is named by its email
	dev := must(orgs.Developers.Create("organizations/pg", &apigee.GoogleCloudApigeeV1Developer{Email: "a@example.com", FirstName: "A", LastName: "B", UserName: "ab"}).Do())
	if dev.DeveloperId == "" {
		t.Fatalf("%+v", dev)
	}
	if d := must(orgs.Developers.Get("organizations/pg/developers/a@example.com").Do()); d.UserName != "ab" {
		t.Fatalf("%+v", d)
	}
	_, err = orgs.Developers.Create("organizations/pg", &apigee.GoogleCloudApigeeV1Developer{}).Do()
	wantCode(t, err, 400, `"email"`)
}

func TestAPIHub(t *testing.T) {
	svc := must(apihub.NewService(ctx, opts(startApp(t), "/apihub/")...))
	apis := svc.Projects.Locations.Apis
	a := must(apis.Create(parent, &apihub.GoogleCloudApihubV1Api{DisplayName: "Pets"}).ApiId("pets").Do())
	if a.Name != parent+"/apis/pets" || a.CreateTime == "" || a.DisplayName != "Pets" {
		t.Fatalf("%+v", a)
	}
	v := must(apis.Versions.Create(a.Name, &apihub.GoogleCloudApihubV1Version{DisplayName: "v1"}).VersionId("v1").Do())
	if v.Name != a.Name+"/versions/v1" {
		t.Fatalf("%+v", v)
	}
	if l := must(apis.List(parent).Do()); len(l.Apis) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := apis.Create(parent, &apihub.GoogleCloudApihubV1Api{}).ApiId("pets").Do()
	wantCode(t, err, 409, "already exists")
	_, err = apis.Create(parent, &apihub.GoogleCloudApihubV1Api{}).Do()
	wantCode(t, err, 400, "apiId")
	_, err = apis.Create(parent, &apihub.GoogleCloudApihubV1Api{}).ApiId("Bad Id").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = apis.Versions.Create(parent+"/apis/nope", &apihub.GoogleCloudApihubV1Version{}).VersionId("v1").Do()
	wantCode(t, err, 404, "nope")
	_, err = apis.Get(parent + "/apis/nope").Do()
	wantCode(t, err, 404, "nope")
	must(apis.Delete(a.Name).Do())
	_, err = apis.Delete(a.Name).Do()
	wantCode(t, err, 404, "pets")
}

func TestAppHub(t *testing.T) {
	svc := must(apphub.NewService(ctx, opts(startApp(t), "/apphub/")...))
	apps := svc.Projects.Locations.Applications
	op := must(apps.Create(parent, &apphub.Application{DisplayName: "shop", Scope: &apphub.Scope{Type: "REGIONAL"}}).ApplicationId("shop").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(apps.Get(parent + "/applications/shop").Do())
	if got.State != "ACTIVE" || got.Scope.Type != "REGIONAL" || got.Uid == "" {
		t.Fatalf("%+v", got)
	}
	must(apps.Services.Create(got.Name, &apphub.Service{DiscoveredService: "projects/p/locations/l/serviceProjectAttachments/x/discoveredServices/y"}).ServiceId("web").Do())
	_, err := apps.Create(parent, &apphub.Application{}).ApplicationId("shop").Do()
	wantCode(t, err, 409, "already exists")
	_, err = apps.Create(parent, &apphub.Application{}).Do()
	wantCode(t, err, 400, "applicationId")
	_, err = apps.Services.Create(parent+"/applications/nope", &apphub.Service{}).ServiceId("web").Do()
	wantCode(t, err, 404, "nope")
	_, err = apps.Get(parent + "/applications/nope").Do()
	wantCode(t, err, 404, "nope")
	must(apps.Delete(got.Name).Do())
	_, err = apps.Delete(got.Name).Do()
	wantCode(t, err, 404, "shop")
}

func TestApplicationIntegration(t *testing.T) {
	svc := must(integrations.NewService(ctx, opts(startApp(t), "/integrations/")...))
	cfgs := svc.Projects.Locations.AuthConfigs
	a := must(cfgs.Create(parent, &integrations.GoogleCloudIntegrationsV1alphaAuthConfig{DisplayName: "basic", Description: "d"}).Do())
	if a.Name == "" || len(a.Name) <= len(parent+"/authConfigs/") || a.DisplayName != "basic" || a.CreateTime == "" {
		t.Fatalf("%+v", a)
	}
	b := must(cfgs.Create(parent, &integrations.GoogleCloudIntegrationsV1alphaAuthConfig{DisplayName: "basic"}).Do()) // a second one is a second config
	if b.Name == a.Name {
		t.Fatalf("%+v", b)
	}
	if got := must(cfgs.Get(a.Name).Do()); got.Description != "d" {
		t.Fatalf("%+v", got)
	}
	if l := must(cfgs.List(parent).Do()); len(l.AuthConfigs) != 2 {
		t.Fatalf("%+v", l)
	}
	_, err := cfgs.Get(parent + "/authConfigs/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = cfgs.Delete(parent + "/authConfigs/nope").Do()
	wantCode(t, err, 404, "nope")
	must(cfgs.Delete(a.Name).Do())
	_, err = cfgs.Get(a.Name).Do()
	wantCode(t, err, 404, "authConfigs")
}

func TestIntegrationConnectors(t *testing.T) {
	svc := must(connectors.NewService(ctx, opts(startApp(t), "/connectors/")...))
	conns := svc.Projects.Locations.Connections
	op := must(conns.Create(parent, &connectors.Connection{ConnectorVersion: "projects/p/locations/global/providers/gcp/connectors/pubsub/versions/1"}).ConnectionId("pubsub").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	c := must(conns.Get(parent + "/connections/pubsub").Do())
	if c.Status == nil || c.Status.State != "ACTIVE" || c.Name != parent+"/connections/pubsub" {
		t.Fatalf("%+v", c)
	}
	_, err := conns.Create(parent, &connectors.Connection{}).ConnectionId("pubsub").Do()
	wantCode(t, err, 409, "already exists")
	_, err = conns.Create(parent, &connectors.Connection{}).Do()
	wantCode(t, err, 400, "connectionId")
	_, err = conns.Create(parent, &connectors.Connection{}).ConnectionId("-bad").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = conns.Get(parent + "/connections/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = conns.Delete(parent + "/connections/nope").Do()
	wantCode(t, err, 404, "nope")
	must(conns.Delete(c.Name).Do())
}

// Cloud Endpoints: a service, a config submitted for it, and a rollout of that config.
func TestCloudEndpoints(t *testing.T) {
	svc := must(servicemanagement.NewService(ctx, opts(startApp(t), "/servicemanagement/")...))
	const name = "api.endpoints.pg.cloud.goog"
	must(svc.Services.Create(&servicemanagement.ManagedService{ServiceName: name, ProducerProjectId: "pg"}).Do())
	if s := must(svc.Services.Get(name).Do()); s.ProducerProjectId != "pg" {
		t.Fatalf("%+v", s)
	}
	_, err := svc.Services.Create(&servicemanagement.ManagedService{ServiceName: name}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = svc.Services.Create(&servicemanagement.ManagedService{}).Do()
	wantCode(t, err, 400, "serviceName")

	files := &servicemanagement.ConfigSource{Files: []*servicemanagement.ConfigFile{{FilePath: "openapi.yaml", FileType: "OPEN_API_YAML", FileContents: "c3dhZ2dlcjogIjIuMCI="}}}
	op := must(svc.Services.Configs.Submit(name, &servicemanagement.SubmitConfigSourceRequest{ConfigSource: files}).Do())
	var resp struct{ ServiceConfig struct{ Id, Name string } }
	if !op.Done || json.Unmarshal(op.Response, &resp) != nil || resp.ServiceConfig.Id == "" || resp.ServiceConfig.Name != name {
		t.Fatalf("%+v %s", op, op.Response)
	}
	cfg := resp.ServiceConfig.Id
	if got := must(svc.Services.Configs.Get(name, cfg).Do()); got.Id != cfg {
		t.Fatalf("%+v", got)
	}
	_, err = svc.Services.Configs.Submit(name, &servicemanagement.SubmitConfigSourceRequest{}).Do()
	wantCode(t, err, 400, "configSource.files")
	_, err = svc.Services.Configs.Submit("nope.example.com", &servicemanagement.SubmitConfigSourceRequest{ConfigSource: files}).Do()
	wantCode(t, err, 404, "nope.example.com")

	roll := func(id string) error {
		_, err := svc.Services.Rollouts.Create(name, &servicemanagement.Rollout{ServiceName: name,
			TrafficPercentStrategy: &servicemanagement.TrafficPercentStrategy{Percentages: map[string]float64{id: 100}}}).Do()
		return err
	}
	if err := roll(cfg); err != nil {
		t.Fatal(err)
	}
	wantCode(t, roll("2020-01-01r0"), 404, "2020-01-01r0")
	if l := must(svc.Services.Rollouts.List(name).Do()); len(l.Rollouts) != 1 || l.Rollouts[0].Status != "SUCCESS" {
		t.Fatalf("%+v", l)
	}
	if got := must(svc.Services.GetConfig(name).Do()); got.Id != cfg { // the newest config
		t.Fatalf("%+v", got)
	}
	_, err = svc.Services.GetConfig(name).ConfigId("nope").Do()
	wantCode(t, err, 404, "nope")
	must(svc.Services.Delete(name).Do())
	_, err = svc.Services.Get(name).Do()
	wantCode(t, err, 404, name)
	_, err = svc.Services.Delete(name).Do()
	wantCode(t, err, 404, name)
}

func TestDeveloperConnect(t *testing.T) {
	svc := must(developerconnect.NewService(ctx, opts(startApp(t), "/developerconnect/")...))
	conns := svc.Projects.Locations.Connections
	must(conns.Create(parent, &developerconnect.Connection{GithubConfig: &developerconnect.GitHubConfig{GithubApp: "FIREBASE"}}).ConnectionId("gh").Do())
	c := must(conns.Get(parent + "/connections/gh").Do())
	if c.GithubConfig.GithubApp != "FIREBASE" || c.Uid == "" || c.Etag != "" { // the provider takes the etag as an input
		t.Fatalf("%+v", c)
	}
	_, err := conns.Create(parent, &developerconnect.Connection{}).ConnectionId("gh").Do()
	wantCode(t, err, 409, "already exists")
	_, err = conns.Create(parent, &developerconnect.Connection{}).Do()
	wantCode(t, err, 400, "connectionId")
	_, err = conns.Get(parent + "/connections/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = conns.Delete(parent + "/connections/nope").Do()
	wantCode(t, err, 404, "nope")
	must(conns.Delete(c.Name).Do())
}

func TestSecureSourceManager(t *testing.T) {
	svc := must(securesourcemanager.NewService(ctx, opts(startApp(t), "/securesourcemanager/")...))
	insts, repos := svc.Projects.Locations.Instances, svc.Projects.Locations.Repositories
	must(insts.Create(parent, &securesourcemanager.Instance{}).InstanceId("ssm").Do())
	if i := must(insts.Get(parent + "/instances/ssm").Do()); i.State != "ACTIVE" || i.Name != parent+"/instances/ssm" {
		t.Fatalf("%+v", i)
	}
	must(repos.Create(parent, &securesourcemanager.Repository{Instance: parent + "/instances/ssm", Description: "d"}).RepositoryId("r").Do())
	if r := must(repos.Get(parent + "/repositories/r").Do()); r.Description != "d" || r.Uid == "" {
		t.Fatalf("%+v", r)
	}
	_, err := repos.Create(parent, &securesourcemanager.Repository{}).RepositoryId("r").Do()
	wantCode(t, err, 409, "already exists")
	_, err = repos.Create(parent, &securesourcemanager.Repository{}).Do()
	wantCode(t, err, 400, "repositoryId")
	_, err = insts.Get(parent + "/instances/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = repos.Delete(parent + "/repositories/nope").Do()
	wantCode(t, err, 404, "nope")
	must(repos.Delete(parent + "/repositories/r").Do())
}

func TestParameterManager(t *testing.T) {
	svc := must(parametermanager.NewService(ctx, opts(startApp(t), "/parametermanager/")...))
	const loc = "projects/p/locations/global"
	params := svc.Projects.Locations.Parameters
	p := must(params.Create(loc, &parametermanager.Parameter{Format: "JSON"}).ParameterId("cfg").Do())
	if p.Name != loc+"/parameters/cfg" || p.Format != "JSON" || p.CreateTime == "" {
		t.Fatalf("%+v", p)
	}
	v := must(params.Versions.Create(p.Name, &parametermanager.ParameterVersion{Payload: &parametermanager.ParameterVersionPayload{Data: "eyJhIjoxfQ=="}}).ParameterVersionId("v1").Do())
	if v.Name != p.Name+"/versions/v1" || v.Payload.Data != "eyJhIjoxfQ==" {
		t.Fatalf("%+v", v)
	}
	_, err := params.Create(loc, &parametermanager.Parameter{}).ParameterId("cfg").Do()
	wantCode(t, err, 409, "already exists")
	_, err = params.Create(loc, &parametermanager.Parameter{}).Do()
	wantCode(t, err, 400, "parameterId")
	_, err = params.Versions.Create(loc+"/parameters/nope", &parametermanager.ParameterVersion{}).ParameterVersionId("v1").Do()
	wantCode(t, err, 404, "nope")
	_, err = params.Get(loc + "/parameters/nope").Do()
	wantCode(t, err, 404, "nope")
	must(params.Delete(p.Name).Do())
	_, err = params.Versions.Get(v.Name).Do() // a parameter takes its versions with it
	wantCode(t, err, 404, "v1")
}

func TestBillingBudgets(t *testing.T) {
	svc := must(billingbudgets.NewService(ctx, opts(startApp(t), "/billingbudgets/")...))
	const acct = "billingAccounts/000000-000000-000000"
	budgets := svc.BillingAccounts.Budgets
	b := must(budgets.Create(acct, &billingbudgets.GoogleCloudBillingBudgetsV1Budget{DisplayName: "monthly",
		Amount: &billingbudgets.GoogleCloudBillingBudgetsV1BudgetAmount{SpecifiedAmount: &billingbudgets.GoogleTypeMoney{CurrencyCode: "USD", Units: 100}}}).Do())
	if len(b.Name) <= len(acct+"/budgets/") || b.DisplayName != "monthly" || b.Amount.SpecifiedAmount.Units != 100 {
		t.Fatalf("%+v", b)
	}
	b2 := must(budgets.Create(acct, &billingbudgets.GoogleCloudBillingBudgetsV1Budget{DisplayName: "monthly"}).Do()) // names may repeat
	if b2.Name == b.Name {
		t.Fatalf("%+v", b2)
	}
	if l := must(budgets.List(acct).Do()); len(l.Budgets) != 2 {
		t.Fatalf("%+v", l)
	}
	if got := must(budgets.Patch(b.Name, &billingbudgets.GoogleCloudBillingBudgetsV1Budget{DisplayName: "yearly"}).UpdateMask("displayName").Do()); got.DisplayName != "yearly" {
		t.Fatalf("%+v", got)
	}
	_, err := budgets.Get(acct + "/budgets/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = budgets.Delete(acct + "/budgets/nope").Do()
	wantCode(t, err, 404, "nope")
	must(budgets.Delete(b.Name).Do())
	_, err = budgets.Get(b.Name).Do()
	wantCode(t, err, 404, b.Name)
}

func TestMigrationCenter(t *testing.T) {
	svc := must(migrationcenter.NewService(ctx, opts(startApp(t), "/migrationcenter/")...))
	groups := svc.Projects.Locations.Groups
	must(groups.Create(parent, &migrationcenter.Group{DisplayName: "web", Labels: map[string]string{"env": "dev"}}).GroupId("web").Do())
	g := must(groups.Get(parent + "/groups/web").Do())
	if g.Name != parent+"/groups/web" || g.DisplayName != "web" || g.Labels["env"] != "dev" || g.CreateTime == "" {
		t.Fatalf("%+v", g)
	}
	must(svc.Projects.Locations.Sources.Create(parent, &migrationcenter.Source{DisplayName: "vcenter", Type: "SOURCE_TYPE_UPLOAD"}).SourceId("src").Do())
	_, err := groups.Create(parent, &migrationcenter.Group{}).GroupId("web").Do()
	wantCode(t, err, 409, "already exists")
	_, err = groups.Create(parent, &migrationcenter.Group{}).Do()
	wantCode(t, err, 400, "groupId")
	_, err = groups.Get(parent + "/groups/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = groups.Delete(parent + "/groups/nope").Do()
	wantCode(t, err, 404, "nope")
	must(groups.Delete(g.Name).Do())
}

func TestBlockchainNodeEngine(t *testing.T) {
	svc := must(blockchainnodeengine.NewService(ctx, opts(startApp(t), "/blockchainnodeengine/")...))
	nodes := svc.Projects.Locations.BlockchainNodes
	must(nodes.Create(parent, &blockchainnodeengine.BlockchainNode{BlockchainType: "ETHEREUM",
		EthereumDetails: &blockchainnodeengine.EthereumDetails{Network: "MAINNET", NodeType: "FULL"}}).BlockchainNodeId("eth").Do())
	n := must(nodes.Get(parent + "/blockchainNodes/eth").Do())
	if n.State != "RUNNING" || n.EthereumDetails.Network != "MAINNET" || n.Name != parent+"/blockchainNodes/eth" {
		t.Fatalf("%+v", n)
	}
	_, err := nodes.Create(parent, &blockchainnodeengine.BlockchainNode{}).BlockchainNodeId("eth").Do()
	wantCode(t, err, 409, "already exists")
	_, err = nodes.Create(parent, &blockchainnodeengine.BlockchainNode{}).Do()
	wantCode(t, err, 400, "blockchainNodeId")
	_, err = nodes.Get(parent + "/blockchainNodes/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = nodes.Delete(parent + "/blockchainNodes/nope").Do()
	wantCode(t, err, 404, "nope")
	must(nodes.Delete(n.Name).Do())
}

// License Manager has no client in google.golang.org/api: this uses the REST client of cloud.google.com/go.
func TestLicenseManager(t *testing.T) {
	srv := startApp(t)
	c := must(licensemanager.NewRESTClient(ctx, option.WithEndpoint(srv.URL+"/licensemanager"), option.WithoutAuthentication()))
	defer c.Close()
	create := func(id string, cfg *licensemanagerpb.Configuration) error {
		op, err := c.CreateConfiguration(ctx, &licensemanagerpb.CreateConfigurationRequest{Parent: parent, ConfigurationId: id, Configuration: cfg})
		if err == nil {
			_, err = op.Wait(ctx)
		}
		return err
	}
	if err := create("win", &licensemanagerpb.Configuration{DisplayName: "win", Product: "windows-server", LicenseType: licensemanagerpb.LicenseType_LICENSE_TYPE_BRING_YOUR_OWN_LICENSE}); err != nil {
		t.Fatal(err)
	}
	got := must(c.GetConfiguration(ctx, &licensemanagerpb.GetConfigurationRequest{Name: parent + "/configurations/win"}))
	if got.Name != parent+"/configurations/win" || got.State != licensemanagerpb.Configuration_STATE_ACTIVE || got.Product != "windows-server" || got.CreateTime == nil {
		t.Fatalf("%+v", got)
	}
	if err := create("win", &licensemanagerpb.Configuration{}); err == nil {
		t.Fatal("a second create of the same id passed")
	}
	if err := create("", &licensemanagerpb.Configuration{}); err == nil {
		t.Fatal("a create without an id passed")
	}
	if _, err := c.GetConfiguration(ctx, &licensemanagerpb.GetConfigurationRequest{Name: parent + "/configurations/nope"}); err == nil {
		t.Fatal("get of an unknown configuration passed")
	}
	op := must(c.DeleteConfiguration(ctx, &licensemanagerpb.DeleteConfigurationRequest{Name: got.Name}))
	if err := op.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetConfiguration(ctx, &licensemanagerpb.GetConfigurationRequest{Name: got.Name}); err == nil {
		t.Fatal("get after delete passed")
	}
}

func TestFirebaseAppCheck(t *testing.T) {
	svc := must(firebaseappcheck.NewService(ctx, opts(startApp(t), "/firebaseappcheck/")...))
	const app = "projects/p/apps/1:123:web:abc"
	tokens := svc.Projects.Apps.DebugTokens
	d := must(tokens.Create(app, &firebaseappcheck.GoogleFirebaseAppcheckV1DebugToken{DisplayName: "ci", Token: "11111111-2222-3333-4444-555555555555"}).Do())
	if len(d.Name) <= len(app+"/debugTokens/") || d.DisplayName != "ci" {
		t.Fatalf("%+v", d)
	}
	if l := must(tokens.List(app).Do()); len(l.DebugTokens) != 1 {
		t.Fatalf("%+v", l)
	}
	must(tokens.Patch(d.Name, &firebaseappcheck.GoogleFirebaseAppcheckV1DebugToken{DisplayName: "ci2"}).UpdateMask("displayName").Do())
	if got := must(tokens.Get(d.Name).Do()); got.DisplayName != "ci2" {
		t.Fatalf("%+v", got)
	}
	_, err := tokens.Get(app + "/debugTokens/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = tokens.Delete(app + "/debugTokens/nope").Do()
	wantCode(t, err, 404, "nope")
	must(tokens.Delete(d.Name).Do())
	_, err = tokens.Get(d.Name).Do()
	wantCode(t, err, 404, "debugTokens")
}

func TestFirebaseAppHosting(t *testing.T) {
	svc := must(firebaseapphosting.NewService(ctx, opts(startApp(t), "/firebaseapphosting/")...))
	backends := svc.Projects.Locations.Backends
	must(backends.Create(parent, &firebaseapphosting.Backend{AppId: "1:123:web:abc", ServingLocality: "GLOBAL_ACCESS", ServiceAccount: "sa@p.iam.gserviceaccount.com"}).BackendId("web").Do())
	b := must(backends.Get(parent + "/backends/web").Do())
	if b.Name != parent+"/backends/web" || b.ServingLocality != "GLOBAL_ACCESS" || b.Uid == "" {
		t.Fatalf("%+v", b)
	}
	_, err := backends.Create(parent, &firebaseapphosting.Backend{}).BackendId("web").Do()
	wantCode(t, err, 409, "already exists")
	_, err = backends.Create(parent, &firebaseapphosting.Backend{}).Do()
	wantCode(t, err, 400, "backendId")
	_, err = backends.Get(parent + "/backends/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = backends.Delete(parent + "/backends/nope").Do()
	wantCode(t, err, 404, "nope")
	must(backends.Delete(b.Name).Do())
}

func TestFirebaseDataConnect(t *testing.T) {
	svc := must(firebasedataconnect.NewService(ctx, opts(startApp(t), "/firebasedataconnect/")...))
	services := svc.Projects.Locations.Services
	must(services.Create(parent, &firebasedataconnect.Service{DisplayName: "movies"}).ServiceId("movies").Do())
	s := must(services.Get(parent + "/services/movies").Do())
	if s.Name != parent+"/services/movies" || s.DisplayName != "movies" || s.Uid == "" {
		t.Fatalf("%+v", s)
	}
	must(services.Schemas.Create(s.Name, &firebasedataconnect.Schema{}).SchemaId("main").Do())
	_, err := services.Create(parent, &firebasedataconnect.Service{}).ServiceId("movies").Do()
	wantCode(t, err, 409, "already exists")
	_, err = services.Create(parent, &firebasedataconnect.Service{}).Do()
	wantCode(t, err, 400, "serviceId")
	_, err = services.Schemas.Create(parent+"/services/nope", &firebasedataconnect.Schema{}).SchemaId("main").Do()
	wantCode(t, err, 404, "nope")
	_, err = services.Delete(parent + "/services/nope").Do()
	wantCode(t, err, 404, "nope")
	must(services.Delete(s.Name).Do())
}
