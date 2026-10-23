package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/batch/armbatch/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/cognitiveservices/armcognitiveservices/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/dashboard/armdashboard/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/databricks/armdatabricks/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/datafactory/armdatafactory/v11"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/kusto/armkusto/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/search/armsearch"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/streamanalytics/armstreamanalytics"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/synapse/armsynapse"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

var dataServices = []string{"azdatafactory", "azbatch", "azgrafana", "azcognitive", "azdatabricks", "azsearch", "azsynapse", "azkusto", "azstreamanalytics"}

const eus = "eastus"

func TestDataFactory(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, dataServices...)
	fs := armtest.Must(armdatafactory.NewFactoriesClient(sub, e.Cred, e.Opts))
	ls := armtest.Must(armdatafactory.NewLinkedServicesClient(sub, e.Cred, e.Opts))
	ds := armtest.Must(armdatafactory.NewDatasetsClient(sub, e.Cred, e.Opts))
	ps := armtest.Must(armdatafactory.NewPipelinesClient(sub, e.Cred, e.Opts))

	f := armtest.Must(fs.CreateOrUpdate(ctx, rg, "adf1", armdatafactory.Factory{Location: to.Ptr(eus)}, nil)).Factory
	if *f.Name != "adf1" || *f.Properties.ProvisioningState != "Succeeded" {
		t.Fatalf("factory: %+v", f)
	}
	armtest.Must(ls.CreateOrUpdate(ctx, rg, "adf1", "web", armdatafactory.LinkedServiceResource{Properties: &armdatafactory.WebLinkedService{
		Type: to.Ptr("Web"), TypeProperties: &armdatafactory.WebAnonymousAuthentication{AuthenticationType: to.Ptr(armdatafactory.WebAuthenticationTypeAnonymous), URL: "https://example.test"}}}, nil))
	armtest.Must(ds.CreateOrUpdate(ctx, rg, "adf1", "data", armdatafactory.DatasetResource{Properties: &armdatafactory.HTTPDataset{
		Type: to.Ptr("HttpFile"), LinkedServiceName: &armdatafactory.LinkedServiceReference{Type: to.Ptr(armdatafactory.LinkedServiceReferenceTypeLinkedServiceReference), ReferenceName: to.Ptr("web")}}}, nil))
	armtest.Must(ps.CreateOrUpdate(ctx, rg, "adf1", "pipe", armdatafactory.PipelineResource{Properties: &armdatafactory.Pipeline{Description: to.Ptr("copy")}}, nil))
	p := armtest.Must(ps.Get(ctx, rg, "adf1", "pipe", nil)).PipelineResource
	if *p.Type != "Microsoft.DataFactory/factories/pipelines" || *p.Properties.Description != "copy" {
		t.Fatalf("pipeline: %+v", p)
	}
	if n := drain(t, ls.NewListByFactoryPager(rg, "adf1", nil), func(r armdatafactory.LinkedServicesClientListByFactoryResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("linked services: %d", n)
	}

	// failures
	_, err := fs.CreateOrUpdate(ctx, rg, "adf2", armdatafactory.Factory{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = fs.CreateOrUpdate(ctx, rg, "-bad-", armdatafactory.Factory{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = ps.CreateOrUpdate(ctx, rg, "nofactory", "pipe", armdatafactory.PipelineResource{Properties: &armdatafactory.Pipeline{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = ds.CreateOrUpdate(ctx, rg, "adf1", "nolinked", armdatafactory.DatasetResource{}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = fs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = ps.Get(ctx, rg, "adf1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(ps.Delete(ctx, rg, "adf1", "nope", nil)) // deleting what is gone is fine
}

func TestBatch(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, dataServices...)
	as := armtest.Must(armbatch.NewAccountClient(sub, e.Cred, e.Opts))
	pools := armtest.Must(armbatch.NewPoolClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(as.BeginCreate(ctx, rg, "batch1", armbatch.AccountCreateParameters{Location: to.Ptr(eus)}, nil))
	a := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Account
	if *a.Properties.AccountEndpoint != "batch1.eastus.batch.azure.com" {
		t.Fatalf("endpoint: %s", *a.Properties.AccountEndpoint)
	}
	k1 := armtest.Must(as.GetKeys(ctx, rg, "batch1", nil))
	k2 := armtest.Must(as.GetKeys(ctx, rg, "batch1", nil))
	if *k1.Primary == "" || *k1.Primary == *k1.Secondary || *k1.Primary != *k2.Primary {
		t.Fatal("keys must be two different values that stay the same across calls")
	}
	pool := armtest.Must(pools.Create(ctx, rg, "batch1", "pool1", armbatch.Pool{Properties: &armbatch.PoolProperties{VMSize: to.Ptr("Standard_A1")}}, nil)).Pool
	if *pool.Properties.AllocationState != armbatch.AllocationStateSteady {
		t.Fatalf("pool: %+v", pool.Properties)
	}

	_, err := as.BeginCreate(ctx, rg, "batch2", armbatch.AccountCreateParameters{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = as.BeginCreate(ctx, rg, "bad_name", armbatch.AccountCreateParameters{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = pools.Create(ctx, rg, "nobatch", "pool1", armbatch.Pool{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = as.Get(ctx, rg, "nobatch", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = as.GetKeys(ctx, rg, "nobatch", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	pd := armtest.Must(as.BeginDelete(ctx, rg, "batch1", nil))
	armtest.Must(pd.PollUntilDone(ctx, armtest.Fast))
	_, err = pools.Get(ctx, rg, "batch1", "pool1", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestGrafanaDatabricksStreamAnalytics(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, dataServices...)
	g := armtest.Must(armdashboard.NewGrafanaClient(sub, e.Cred, e.Opts))
	pl := armtest.Must(g.BeginCreate(ctx, rg, "graf1", armdashboard.ManagedGrafana{Location: to.Ptr(eus),
		Identity: &armdashboard.ManagedServiceIdentity{Type: to.Ptr(armdashboard.ManagedServiceIdentityTypeSystemAssigned)}}, nil))
	m := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).ManagedGrafana
	if !strings.HasPrefix(*m.Properties.Endpoint, "https://graf1-") || *m.Identity.PrincipalID == "" || *m.Identity.TenantID == "" {
		t.Fatalf("grafana: %s %+v", *m.Properties.Endpoint, m.Identity)
	}
	again := armtest.Must(g.Get(ctx, rg, "graf1", nil)).ManagedGrafana
	if *again.Identity.PrincipalID != *m.Identity.PrincipalID {
		t.Fatal("the principal of an identity stays the same")
	}
	_, err := g.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = g.BeginCreate(ctx, rg, "graf2", armdashboard.ManagedGrafana{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")

	w := armtest.Must(armdatabricks.NewWorkspacesClient(sub, e.Cred, e.Opts))
	wp := armtest.Must(w.BeginCreateOrUpdate(ctx, rg, "dbx1", armdatabricks.Workspace{Location: to.Ptr(eus), SKU: &armdatabricks.SKU{Name: to.Ptr("standard")},
		Properties: &armdatabricks.WorkspaceProperties{ManagedResourceGroupID: to.Ptr("/subscriptions/" + sub + "/resourceGroups/dbx-managed")}}, nil))
	ws := armtest.Must(wp.PollUntilDone(ctx, armtest.Fast)).Workspace
	if !strings.HasSuffix(*ws.Properties.WorkspaceURL, ".azuredatabricks.net") || *ws.Properties.WorkspaceID == "" || !strings.HasSuffix(*ws.Properties.ManagedResourceGroupID, "dbx-managed") {
		t.Fatalf("workspace: %+v", ws.Properties)
	}
	_, err = w.BeginCreateOrUpdate(ctx, rg, "dbx2", armdatabricks.Workspace{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent") // managedResourceGroupId is required
	_, err = w.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	jobs := armtest.Must(armstreamanalytics.NewStreamingJobsClient(sub, e.Cred, e.Opts))
	jp := armtest.Must(jobs.BeginCreateOrReplace(ctx, rg, "asa1", armstreamanalytics.StreamingJob{Location: to.Ptr(eus),
		Properties: &armstreamanalytics.StreamingJobProperties{SKU: &armstreamanalytics.SKU{Name: to.Ptr(armstreamanalytics.SKUNameStandard)}}}, nil))
	j := armtest.Must(jp.PollUntilDone(ctx, armtest.Fast)).StreamingJob
	if *j.Properties.ProvisioningState != "Succeeded" {
		t.Fatalf("job: %+v", j.Properties)
	}
	_, err = jobs.BeginCreateOrReplace(ctx, rg, "asa2", armstreamanalytics.StreamingJob{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = jobs.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestCognitiveAndSearch(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, dataServices...)
	acc := armtest.Must(armcognitiveservices.NewAccountsClient(sub, e.Cred, e.Opts))
	mk := func(name, kind string) error {
		_, err := acc.BeginCreate(ctx, rg, name, armcognitiveservices.Account{Location: to.Ptr(eus), Kind: to.Ptr(kind),
			SKU: &armcognitiveservices.SKU{Name: to.Ptr("S0")}, Properties: &armcognitiveservices.AccountProperties{CustomSubDomainName: to.Ptr(name)}}, nil)
		return err
	}
	if err := mk("textapi", "TextAnalytics"); err != nil {
		t.Fatal(err)
	}
	a := armtest.Must(acc.Get(ctx, rg, "textapi", nil)).Account
	if *a.Properties.Endpoint != "https://textapi.cognitiveservices.azure.com/" {
		t.Fatalf("endpoint: %s", *a.Properties.Endpoint)
	}
	k := armtest.Must(acc.ListKeys(ctx, rg, "textapi", nil))
	if *k.Key1 == "" || *k.Key1 == *k.Key2 || *k.Key1 != *armtest.Must(acc.ListKeys(ctx, rg, "textapi", nil)).Key1 {
		t.Fatal("keys must be two different values that stay the same across calls")
	}
	// by the owner's decision pogi does not emulate Azure OpenAI
	for _, kind := range []string{"OpenAI", "AIServices", "openai"} {
		err := mk("refused"+strings.ToLower(kind), kind)
		armtest.WantErr(t, err, 400, "InvalidRequestContent")
		if !strings.Contains(err.Error(), "does not emulate Azure OpenAI") {
			t.Fatalf("the refusal must say why: %v", err)
		}
	}
	if _, err := acc.Get(ctx, rg, "refusedopenai", nil); err == nil {
		t.Fatal("a refused account must not be stored")
	}
	armtest.WantErr(t, mk("a", "TextAnalytics"), 400, "InvalidResourceName")
	_, err := acc.ListKeys(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = acc.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")

	svc := armtest.Must(armsearch.NewServicesClient(sub, e.Cred, e.Opts))
	sp := armtest.Must(svc.BeginCreateOrUpdate(ctx, rg, "srch1", armsearch.Service{Location: to.Ptr(eus), SKU: &armsearch.SKU{Name: to.Ptr(armsearch.SKUNameStandard)}}, nil, nil))
	s := armtest.Must(sp.PollUntilDone(ctx, armtest.Fast)).Service
	if *s.Properties.Status != armsearch.SearchServiceStatusRunning || *s.Properties.ReplicaCount != 1 {
		t.Fatalf("search service: %+v", s.Properties)
	}
	admin := armtest.Must(armtest.Must(armsearch.NewAdminKeysClient(sub, e.Cred, e.Opts)).Get(ctx, rg, "srch1", nil, nil))
	qk := armtest.Must(armsearch.NewQueryKeysClient(sub, e.Cred, e.Opts))
	if *admin.PrimaryKey == "" || *admin.PrimaryKey == *admin.SecondaryKey {
		t.Fatal("admin keys")
	}
	if n := drain(t, qk.NewListBySearchServicePager(rg, "srch1", nil, nil), func(r armsearch.QueryKeysClientListBySearchServiceResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("query keys: %d", n)
	}
	_, err = armtest.Must(armsearch.NewAdminKeysClient(sub, e.Cred, e.Opts)).Get(ctx, rg, "nope", nil, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = svc.BeginCreateOrUpdate(ctx, rg, "srch2", armsearch.Service{}, nil, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
}

func TestSynapseAndKusto(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, dataServices...)
	ws := armtest.Must(armsynapse.NewWorkspacesClient(sub, e.Cred, e.Opts))
	pools := armtest.Must(armsynapse.NewSQLPoolsClient(sub, e.Cred, e.Opts))
	fw := armtest.Must(armsynapse.NewIPFirewallRulesClient(sub, e.Cred, e.Opts))

	wp := armtest.Must(ws.BeginCreateOrUpdate(ctx, rg, "syn1", armsynapse.Workspace{Location: to.Ptr(eus)}, nil))
	armtest.Must(wp.PollUntilDone(ctx, armtest.Fast))
	pp := armtest.Must(pools.BeginCreate(ctx, rg, "syn1", "pool1", armsynapse.SQLPool{Location: to.Ptr(eus), SKU: &armsynapse.SKU{Name: to.Ptr("DW100c")}}, nil))
	pool := armtest.Must(pp.PollUntilDone(ctx, armtest.Fast)).SQLPool
	if *pool.Properties.Status != "Online" {
		t.Fatalf("pool: %+v", pool.Properties)
	}
	fp := armtest.Must(fw.BeginCreateOrUpdate(ctx, rg, "syn1", "all", armsynapse.IPFirewallRuleInfo{Properties: &armsynapse.IPFirewallRuleProperties{StartIPAddress: to.Ptr("0.0.0.0"), EndIPAddress: to.Ptr("255.255.255.255")}}, nil))
	armtest.Must(fp.PollUntilDone(ctx, armtest.Fast))
	_, err := pools.BeginCreate(ctx, rg, "nosyn", "pool1", armsynapse.SQLPool{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = ws.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = pools.Get(ctx, rg, "syn1", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = ws.BeginCreateOrUpdate(ctx, rg, "syn2", armsynapse.Workspace{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")

	cl := armtest.Must(armkusto.NewClustersClient(sub, e.Cred, e.Opts))
	dbs := armtest.Must(armkusto.NewDatabasesClient(sub, e.Cred, e.Opts))
	cp := armtest.Must(cl.BeginCreateOrUpdate(ctx, rg, "kusto1", armkusto.Cluster{Location: to.Ptr(eus),
		SKU: &armkusto.AzureSKU{Name: to.Ptr(armkusto.AzureSKUNameStandardD11V2), Tier: to.Ptr(armkusto.AzureSKUTierStandard), Capacity: to.Ptr[int32](2)}}, nil))
	c := armtest.Must(cp.PollUntilDone(ctx, armtest.Fast)).Cluster
	if !strings.HasPrefix(*c.Properties.URI, "https://kusto1.") || *c.Properties.State != armkusto.StateRunning {
		t.Fatalf("cluster: %+v", c.Properties)
	}
	dp := armtest.Must(dbs.BeginCreateOrUpdate(ctx, rg, "kusto1", "db1", &armkusto.ReadWriteDatabase{Kind: to.Ptr(armkusto.KindReadWrite), Location: to.Ptr(eus)}, nil))
	armtest.Must(dp.PollUntilDone(ctx, armtest.Fast))
	_, err = cl.BeginCreateOrUpdate(ctx, rg, "kusto2", armkusto.Cluster{Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent") // sku is required
	_, err = dbs.BeginCreateOrUpdate(ctx, rg, "nokusto", "db1", &armkusto.ReadWriteDatabase{Kind: to.Ptr(armkusto.KindReadWrite), Location: to.Ptr(eus)}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = cl.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}
