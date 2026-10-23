package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/notificationhubs/armnotificationhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/relay/armrelay/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/webpubsub/armwebpubsub"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

// Tests of the application services on the engine, each with the service's own client. Nothing runs behind them.

func TestStaticWebApps(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azstaticwebapps")
	c := armtest.Must(armappservice.NewStaticSitesClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(c.BeginCreateOrUpdateStaticSite(ctx, rg, "docs", armappservice.StaticSiteARMResource{
		Location: to.Ptr("westus2"), SKU: &armappservice.SKUDescription{Name: to.Ptr("Free"), Tier: to.Ptr("Free")},
		Properties: &armappservice.StaticSite{RepositoryURL: to.Ptr("https://example.test/org/docs"), Branch: to.Ptr("main")},
		Tags:       map[string]*string{"env": to.Ptr("dev")}}, nil))
	site := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).StaticSiteARMResource
	if !strings.HasPrefix(*site.Properties.DefaultHostname, "docs-") || !strings.HasSuffix(*site.Properties.DefaultHostname, ".azurestaticapps.net") ||
		*site.Properties.Branch != "main" || *site.Tags["env"] != "dev" {
		t.Fatalf("site: %+v", site.Properties)
	}
	again := armtest.Must(c.GetStaticSite(ctx, rg, "docs", nil))
	if *again.Properties.DefaultHostname != *site.Properties.DefaultHostname {
		t.Fatal("the host name changes between reads")
	}

	// the API key (deployment token) stays the same
	k1 := armtest.Must(c.ListStaticSiteSecrets(ctx, rg, "docs", nil)).Properties["apiKey"]
	k2 := armtest.Must(c.ListStaticSiteSecrets(ctx, rg, "docs", nil)).Properties["apiKey"]
	if k1 == nil || *k1 == "" || *k1 != *k2 {
		t.Fatalf("api key: %v %v", k1, k2)
	}

	// app settings hold the user's pairs and nothing of ours
	armtest.Must(c.CreateOrUpdateStaticSiteAppSettings(ctx, rg, "docs", armappservice.StringDictionary{Properties: map[string]*string{"API_URL": to.Ptr("https://api.example.test")}}, nil))
	for _, got := range []map[string]*string{
		armtest.Must(c.ListStaticSiteAppSettings(ctx, rg, "docs", nil)).Properties,
		armtest.Must(c.ListStaticSiteFunctionAppSettings(ctx, rg, "docs", nil)).Properties,
	} {
		if len(got) > 1 || (len(got) == 1 && *got["API_URL"] != "https://api.example.test") {
			t.Fatalf("app settings: %v", got)
		}
	}

	if n := drain(t, c.NewListPager(nil), func(r armappservice.StaticSitesClientListResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("sites: %d", n)
	}
	del := armtest.Must(c.BeginDeleteStaticSite(ctx, rg, "docs", nil))
	armtest.Must(del.PollUntilDone(ctx, armtest.Fast))
	_, err := c.GetStaticSite(ctx, rg, "docs", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestStaticWebAppsFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azstaticwebapps")
	c := armtest.Must(armappservice.NewStaticSitesClient(sub, e.Cred, e.Opts))
	_, err := c.BeginCreateOrUpdateStaticSite(ctx, rg, "nolocation", armappservice.StaticSiteARMResource{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = c.GetStaticSite(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.ListStaticSiteSecrets(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.CreateOrUpdateStaticSiteAppSettings(ctx, rg, "missing", armappservice.StringDictionary{}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = c.BeginDeleteStaticSite(ctx, rg, "missing", nil) // gone already: ARM answers 204, which this client does not take as a success
	if err == nil {
		t.Fatal("deleting a missing site should not look like a started delete")
	}
}

func TestWebPubSub(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azwebpubsub")
	c := armtest.Must(armwebpubsub.NewClient(sub, e.Cred, e.Opts))
	hubs := armtest.Must(armwebpubsub.NewHubsClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "chatsvc", armwebpubsub.ResourceInfo{Location: to.Ptr("eastus"),
		SKU: &armwebpubsub.ResourceSKU{Name: to.Ptr("Free_F1"), Capacity: to.Ptr[int32](1)}}, nil))
	svc := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).ResourceInfo
	if *svc.Properties.HostName != "chatsvc.webpubsub.azure.com" || *svc.SKU.Name != "Free_F1" {
		t.Fatalf("service: %+v", svc.Properties)
	}
	keys := armtest.Must(c.ListKeys(ctx, rg, "chatsvc", nil))
	if *keys.PrimaryKey == "" || *keys.PrimaryKey == *keys.SecondaryKey ||
		!strings.Contains(*keys.PrimaryConnectionString, "AccessKey="+*keys.PrimaryKey+";") || !strings.HasPrefix(*keys.PrimaryConnectionString, "Endpoint=https://chatsvc.webpubsub.azure.com;") {
		t.Fatalf("keys: %+v", keys)
	}
	if *armtest.Must(c.ListKeys(ctx, rg, "chatsvc", nil)).PrimaryKey != *keys.PrimaryKey {
		t.Fatal("the keys change between reads")
	}

	hp := armtest.Must(hubs.BeginCreateOrUpdate(ctx, "chat", rg, "chatsvc", armwebpubsub.Hub{Properties: &armwebpubsub.HubProperties{AnonymousConnectPolicy: to.Ptr("allow")}}, nil))
	hub := armtest.Must(hp.PollUntilDone(ctx, armtest.Fast)).Hub
	if *hub.Properties.AnonymousConnectPolicy != "allow" || *hub.Name != "chat" {
		t.Fatalf("hub: %+v", hub)
	}
	got := armtest.Must(hubs.Get(ctx, "chat", rg, "chatsvc", nil)).Hub
	if *got.Properties.AnonymousConnectPolicy != "allow" {
		t.Fatalf("hub read back: %+v", got.Properties)
	}
	if n := drain(t, c.NewListByResourceGroupPager(rg, nil), func(r armwebpubsub.ClientListByResourceGroupResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("services: %d", n)
	}
}

func TestWebPubSubFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azwebpubsub")
	c := armtest.Must(armwebpubsub.NewClient(sub, e.Cred, e.Opts))
	hubs := armtest.Must(armwebpubsub.NewHubsClient(sub, e.Cred, e.Opts))
	_, err := c.BeginCreateOrUpdate(ctx, rg, "nolocation", armwebpubsub.ResourceInfo{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = hubs.BeginCreateOrUpdate(ctx, "chat", rg, "missing", armwebpubsub.Hub{Properties: &armwebpubsub.HubProperties{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	pl := armtest.Must(c.BeginCreateOrUpdate(ctx, rg, "here", armwebpubsub.ResourceInfo{Location: to.Ptr("eastus")}, nil))
	armtest.Must(pl.PollUntilDone(ctx, armtest.Fast))
	_, err = hubs.BeginCreateOrUpdate(ctx, "chat", rg, "here", armwebpubsub.Hub{}, nil)
	armtest.WantErr(t, err, 400, "InvalidRequestContent")
	_, err = hubs.Get(ctx, "chat", rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = c.ListKeys(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestNotificationHubs(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "aznotificationhubs")
	ns := armtest.Must(armnotificationhubs.NewNamespacesClient(sub, e.Cred, e.Opts))
	hubs := armtest.Must(armnotificationhubs.NewClient(sub, e.Cred, e.Opts))

	n := armtest.Must(ns.CreateOrUpdate(ctx, rg, "orders-ns", armnotificationhubs.NamespaceCreateOrUpdateParameters{Location: to.Ptr("eastus"),
		SKU:        &armnotificationhubs.SKU{Name: to.Ptr(armnotificationhubs.SKUNameStandard)},
		Properties: &armnotificationhubs.NamespaceProperties{NamespaceType: to.Ptr(armnotificationhubs.NamespaceTypeNotificationHub)}}, nil)).NamespaceResource
	if *n.Properties.ServiceBusEndpoint != "https://orders-ns.servicebus.windows.net:443/" || *n.Properties.Status != "Created" || *n.Properties.Name != "orders-ns" {
		t.Fatalf("namespace: %+v", n.Properties)
	}

	// the namespace starts with RootManageSharedAccessKey; a rule's keys stay the same
	root := armtest.Must(ns.ListKeys(ctx, rg, "orders-ns", "RootManageSharedAccessKey", nil)).ResourceListKeys
	if *root.PrimaryKey == "" || *root.PrimaryKey == *root.SecondaryKey || !strings.HasPrefix(*root.PrimaryConnectionString, "Endpoint=sb://orders-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;") {
		t.Fatalf("root keys: %+v", root)
	}
	armtest.Must(hubs.CreateOrUpdate(ctx, rg, "orders-ns", "orders", armnotificationhubs.NotificationHubCreateOrUpdateParameters{
		Location: to.Ptr("eastus"), Properties: &armnotificationhubs.NotificationHubProperties{}}, nil))
	armtest.Must(hubs.CreateOrUpdateAuthorizationRule(ctx, rg, "orders-ns", "orders", "send", armnotificationhubs.SharedAccessAuthorizationRuleCreateOrUpdateParameters{
		Properties: &armnotificationhubs.SharedAccessAuthorizationRuleProperties{Rights: []*armnotificationhubs.AccessRights{to.Ptr(armnotificationhubs.AccessRightsSend)}}}, nil))
	k := armtest.Must(hubs.ListKeys(ctx, rg, "orders-ns", "orders", "send", nil)).ResourceListKeys
	if *k.KeyName != "send" || *k.PrimaryKey == *root.PrimaryKey || *k.PrimaryKey != *armtest.Must(hubs.ListKeys(ctx, rg, "orders-ns", "orders", "send", nil)).PrimaryKey {
		t.Fatalf("hub rule keys: %+v", k)
	}
	r := armtest.Must(hubs.GetAuthorizationRule(ctx, rg, "orders-ns", "orders", "send", nil)).SharedAccessAuthorizationRuleResource
	if len(r.Properties.Rights) != 1 || *r.Properties.Rights[0] != armnotificationhubs.AccessRightsSend {
		t.Fatalf("rule: %+v", r.Properties)
	}
}

func TestNotificationHubsFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "aznotificationhubs")
	ns := armtest.Must(armnotificationhubs.NewNamespacesClient(sub, e.Cred, e.Opts))
	hubs := armtest.Must(armnotificationhubs.NewClient(sub, e.Cred, e.Opts))
	_, err := ns.CreateOrUpdate(ctx, rg, "nolocation", armnotificationhubs.NamespaceCreateOrUpdateParameters{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = hubs.CreateOrUpdate(ctx, rg, "missing", "orders", armnotificationhubs.NotificationHubCreateOrUpdateParameters{Location: to.Ptr("eastus"), Properties: &armnotificationhubs.NotificationHubProperties{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = ns.Get(ctx, rg, "missing", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = ns.ListKeys(ctx, rg, "missing", "RootManageSharedAccessKey", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(ns.CreateOrUpdate(ctx, rg, "here", armnotificationhubs.NamespaceCreateOrUpdateParameters{Location: to.Ptr("eastus"), SKU: &armnotificationhubs.SKU{Name: to.Ptr(armnotificationhubs.SKUNameFree)}}, nil))
	_, err = hubs.CreateOrUpdate(ctx, rg, "here", "orders", armnotificationhubs.NotificationHubCreateOrUpdateParameters{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = ns.ListKeys(ctx, rg, "here", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = hubs.Get(ctx, rg, "here", "nohub", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestRelay(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azrelay")
	ns := armtest.Must(armrelay.NewNamespacesClient(sub, e.Cred, e.Opts))
	hc := armtest.Must(armrelay.NewHybridConnectionsClient(sub, e.Cred, e.Opts))

	pl := armtest.Must(ns.BeginCreateOrUpdate(ctx, rg, "bridge", armrelay.Namespace{Location: to.Ptr("eastus"),
		SKU: &armrelay.SKU{Name: to.Ptr(armrelay.SKUNameStandard), Tier: to.Ptr(armrelay.SKUTierStandard)}}, nil))
	n := armtest.Must(pl.PollUntilDone(ctx, armtest.Fast)).Namespace
	if *n.Properties.ServiceBusEndpoint != "https://bridge.servicebus.windows.net:443/" || *n.Properties.Status != "Active" || *n.Properties.MetricID != sub+":bridge" {
		t.Fatalf("namespace: %+v", n.Properties)
	}
	root := armtest.Must(ns.ListKeys(ctx, rg, "bridge", "RootManageSharedAccessKey", nil)).AccessKeys
	if *root.PrimaryKey == "" || !strings.Contains(*root.SecondaryConnectionString, "SharedAccessKey="+*root.SecondaryKey) {
		t.Fatalf("root keys: %+v", root)
	}

	h := armtest.Must(hc.CreateOrUpdate(ctx, rg, "bridge", "db", armrelay.HybridConnection{Properties: &armrelay.HybridConnectionProperties{
		RequiresClientAuthorization: to.Ptr(false), UserMetadata: to.Ptr("db")}}, nil)).HybridConnection
	if *h.Properties.RequiresClientAuthorization || *h.Properties.UserMetadata != "db" || *h.Properties.ListenerCount != 0 {
		t.Fatalf("hybrid connection: %+v", h.Properties)
	}
	armtest.Must(hc.CreateOrUpdateAuthorizationRule(ctx, rg, "bridge", "db", "listen", armrelay.AuthorizationRule{
		Properties: &armrelay.AuthorizationRuleProperties{Rights: []*armrelay.AccessRights{to.Ptr(armrelay.AccessRightsListen)}}}, nil))
	k := armtest.Must(hc.ListKeys(ctx, rg, "bridge", "db", "listen", nil)).AccessKeys
	if *k.KeyName != "listen" || *k.PrimaryKey == *root.PrimaryKey {
		t.Fatalf("hybrid connection rule keys: %+v", k)
	}
}

func TestRelayFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azrelay")
	ns := armtest.Must(armrelay.NewNamespacesClient(sub, e.Cred, e.Opts))
	hc := armtest.Must(armrelay.NewHybridConnectionsClient(sub, e.Cred, e.Opts))
	_, err := ns.BeginCreateOrUpdate(ctx, rg, "nolocation", armrelay.Namespace{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = hc.CreateOrUpdate(ctx, rg, "missing", "db", armrelay.HybridConnection{Properties: &armrelay.HybridConnectionProperties{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = hc.Get(ctx, rg, "missing", "db", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = ns.ListKeys(ctx, rg, "missing", "RootManageSharedAccessKey", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	pl := armtest.Must(ns.BeginCreateOrUpdate(ctx, rg, "here", armrelay.Namespace{Location: to.Ptr("eastus")}, nil))
	armtest.Must(pl.PollUntilDone(ctx, armtest.Fast))
	_, err = hc.ListKeys(ctx, rg, "here", "nohc", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = hc.CreateOrUpdate(ctx, rg, "here", "bad name!", armrelay.HybridConnection{Properties: &armrelay.HybridConnectionProperties{}}, nil)
	if err == nil {
		t.Log("a hybrid connection name is not checked: the spec has no pattern") // ponytail: names without a pattern in the spec are accepted
	}
}
