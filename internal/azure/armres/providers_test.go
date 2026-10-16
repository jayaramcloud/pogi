package armres_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// The Providers are called the way the resource manager's HTTP layer does,
// and each resource is then checked through the service's own data plane
// with the official SDK.

var bg = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type rm struct {
	t *testing.T
	h *core.Hub
	c *core.Ctx
}

func start(t *testing.T) (*rm, *pogi.Running) {
	srv := pogi.Start(t)
	return &rm{t, srv.Hub, &core.Ctx{Hub: srv.Hub, Account: core.DefaultAccount, Region: "eastus", RequestID: "t"}}, srv
}

const rg = "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/"

func (r *rm) provider(id arm.ID) *arm.Provider {
	typ := id.Type()
	if i := slices.Index(id.Types, "providers"); i >= 0 { // a scoped resource: the type is the one after the link
		typ = id.Names[i] + "/" + id.Types[len(id.Types)-1]
	}
	p := arm.Lookup(r.h, typ)
	if p == nil {
		r.t.Fatalf("no provider for %s", id.Type())
	}
	return p
}

func (r *rm) id(path string) arm.ID {
	id, ok := arm.ParseID(rg + path)
	if !ok {
		r.t.Fatalf("bad ID %s", path)
	}
	return id
}

// json is what the HTTP layer would send: typed values become plain JSON ones.
func json2(v any) map[string]any {
	b, _ := json.Marshal(v)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

func (r *rm) put(path string, body map[string]any) map[string]any {
	r.t.Helper()
	id := r.id(path)
	out, err := r.provider(id).Put(r.c, id, json2(body))
	if err != nil {
		r.t.Fatalf("PUT %s: %v", path, err)
	}
	return json2(out)
}

func (r *rm) get(path string) (map[string]any, error) {
	id := r.id(path)
	out, err := r.provider(id).Get(r.c, id)
	return json2(out), err
}

func (r *rm) mustGet(path string) map[string]any {
	r.t.Helper()
	out, err := r.get(path)
	if err != nil {
		r.t.Fatalf("GET %s: %v", path, err)
	}
	return out
}

func (r *rm) delete(path string) {
	r.t.Helper()
	id := r.id(path)
	if err := r.provider(id).Delete(r.c, id); err != nil {
		r.t.Fatalf("DELETE %s: %v", path, err)
	}
}

func (r *rm) list(path string) []map[string]any {
	r.t.Helper()
	id := r.id(path)
	out, err := r.provider(id).List(r.c, id.Parent())
	if err != nil {
		r.t.Fatalf("LIST %s: %v", path, err)
	}
	for i := range out {
		out[i] = json2(out[i])
	}
	return out
}

func (r *rm) action(path, name string) map[string]any {
	r.t.Helper()
	id := r.id(path)
	out, err := r.provider(id).Actions[name](r.c, id, nil)
	if err != nil {
		r.t.Fatalf("POST %s/%s: %v", path, name, err)
	}
	return json2(out)
}

func (r *rm) notFound(path string) {
	r.t.Helper()
	if _, err := r.get(path); err == nil || core.AsError(err).Status != http.StatusNotFound || core.AsError(err).Code != "ResourceNotFound" {
		r.t.Fatalf("GET %s: %v, want ResourceNotFound", path, err)
	}
}

func props(m map[string]any) map[string]any { p, _ := m["properties"].(map[string]any); return p }

func str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

// hostClient reaches the *.localhost names the resource manager hands out as the host would, whatever the
// system resolver says: it dials pogi's main port and trusts pogi's CA.
func hostClient(t *testing.T, srv *pogi.Running) *http.Client {
	t.Helper()
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	addr := srv.URL[len("http://"):]
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}}
}

// ---- Storage ----

func TestStorageAccount(t *testing.T) {
	r, _ := start(t)
	acct := r.put("Microsoft.Storage/storageAccounts/pogistore", map[string]any{"location": "westus", "kind": "StorageV2",
		"sku": map[string]any{"name": "Standard_GRS"}, "tags": map[string]any{"env": "dev"},
		"properties": map[string]any{"accessTier": "Cool", "minimumTlsVersion": "TLS1_0"}})
	p := props(acct)
	if acct["location"] != "westus" || acct["sku"].(map[string]any)["tier"] != "Standard" || p["accessTier"] != "Cool" || p["minimumTlsVersion"] != "TLS1_0" ||
		p["provisioningState"] != "Succeeded" || p["supportsHttpsTrafficOnly"] != true {
		t.Fatalf("account = %v", acct)
	}
	if again := r.mustGet("Microsoft.Storage/storageAccounts/pogistore"); props(again)["creationTime"] != p["creationTime"] || again["tags"].(map[string]any)["env"] != "dev" {
		t.Fatalf("GET after PUT = %v", again)
	}
	if l := r.list("Microsoft.Storage/storageAccounts"); len(l) != 1 {
		t.Fatalf("list = %v", l)
	}
	keys := r.action("Microsoft.Storage/storageAccounts/pogistore", "listKeys")["keys"].([]any)
	if len(keys) != 2 || keys[0].(map[string]any)["keyName"] != "key1" || len(str(keys[0].(map[string]any), "value")) != 88 {
		t.Fatalf("keys = %v", keys)
	}
	r.notFound("Microsoft.Storage/storageAccounts/nope")
	if svc := r.mustGet("Microsoft.Storage/storageAccounts/pogistore/blobServices/default"); props(svc)["isVersioningEnabled"] != false {
		t.Fatalf("blob service = %v", svc)
	}
	r.delete("Microsoft.Storage/storageAccounts/pogistore")
	r.notFound("Microsoft.Storage/storageAccounts/pogistore")
	r.delete("Microsoft.Storage/storageAccounts/pogistore") // a missing resource is not an error
}

// A container, queue and table made through the resource manager are the ones the data-plane SDKs see.
func TestStorageDataPlane(t *testing.T) {
	r, srv := start(t)
	hc := hostClient(t, srv)
	const acct = "Microsoft.Storage/storageAccounts/dataacct"
	p := props(r.put(acct, map[string]any{"location": "eastus"}))
	ep := p["primaryEndpoints"].(map[string]any)
	key := r.action(acct, "listKeys")["keys"].([]any)[0].(map[string]any)["value"].(string)

	ct := r.put(acct+"/blobServices/default/containers/photos", map[string]any{"properties": map[string]any{
		"publicAccess": "Blob", "metadata": map[string]any{"owner": "ana"}}})
	if cp := props(ct); cp["publicAccess"] != "Blob" || cp["metadata"].(map[string]any)["owner"] != "ana" || ct["type"] != "Microsoft.Storage/storageAccounts/blobServices/containers" {
		t.Fatalf("container = %v", ct)
	}
	q := r.put(acct+"/queueServices/default/queues/jobs", map[string]any{"properties": map[string]any{"metadata": map[string]any{"k": "v"}}})
	if props(q)["metadata"].(map[string]any)["k"] != "v" {
		t.Fatalf("queue = %v", q)
	}
	r.put(acct+"/tableServices/default/tables/people", map[string]any{})

	name := "dataacct"
	cred := must(azblob.NewSharedKeyCredential(name, key))
	blobs := must(azblob.NewClientWithSharedKeyCredential(str(ep, "blob"), cred, &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}}))
	var seen []string
	pager := blobs.NewListContainersPager(&azblob.ListContainersOptions{Include: azblob.ListContainersInclude{Metadata: true}})
	for pager.More() {
		page := must(pager.NextPage(bg))
		for _, c := range page.ContainerItems {
			seen = append(seen, *c.Name+":"+*c.Metadata["owner"])
		}
	}
	if len(seen) != 1 || seen[0] != "photos:ana" {
		t.Fatalf("containers by the SDK = %v", seen)
	}
	must(blobs.UploadBuffer(bg, "photos", "a.txt", []byte("hello"), nil))
	if l := r.list(acct + "/blobServices/default/containers"); len(l) != 1 || str(l[0], "name") != "photos" {
		t.Fatalf("ARM container list = %v", l)
	}

	qc := must(azqueue.NewQueueClientWithSharedKeyCredential(str(ep, "queue")+"jobs", must(azqueue.NewSharedKeyCredential(name, key)), &azqueue.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}}))
	must(qc.EnqueueMessage(bg, "ping", nil))
	if m := must(qc.GetProperties(bg, nil)); *m.ApproximateMessagesCount != 1 || m.Metadata["K"] == nil || *m.Metadata["K"] != "v" {
		t.Fatalf("queue properties by the SDK = %+v", m)
	}
	if got := props(r.mustGet(acct + "/queueServices/default/queues/jobs"))["approximateMessageCount"]; got != float64(1) && got != 1 {
		t.Fatalf("ARM queue message count = %v", got)
	}

	tc := must(aztables.NewServiceClientWithSharedKey(str(ep, "table"), must(aztables.NewSharedKeyCredential(name, key)), &aztables.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}}))
	var tables []string
	for tp := tc.NewListTablesPager(nil); tp.More(); {
		for _, tb := range must(tp.NextPage(bg)).Tables {
			tables = append(tables, *tb.Name)
		}
	}
	if len(tables) != 1 || tables[0] != "people" {
		t.Fatalf("tables by the SDK = %v", tables)
	}

	r.delete(acct + "/blobServices/default/containers/photos")
	r.notFound(acct + "/blobServices/default/containers/photos")
	// deleting the account takes its queues and tables with it
	r.delete(acct)
	r.notFound(acct + "/queueServices/default/queues/jobs")
	r.notFound(acct + "/tableServices/default/tables/people")
	if _, err := r.provider(r.id(acct+"/blobServices/default/containers/x")).Put(r.c, r.id(acct+"/blobServices/default/containers/X"), nil); core.AsError(err).Code != "InvalidResourceName" {
		t.Fatalf("a bad container name: %v", err)
	}
}

// ---- Key Vault ----

func TestKeyVault(t *testing.T) {
	r, srv := start(t)
	v := r.put("Microsoft.KeyVault/vaults/pogivault", map[string]any{"location": "eastus", "properties": map[string]any{
		"tenantId": "t1", "sku": map[string]any{"family": "A", "name": "premium"}, "enableRbacAuthorization": true,
		"accessPolicies": []any{map[string]any{"tenantId": "t1", "objectId": "o1", "permissions": map[string]any{"secrets": []any{"Get"}}}}}})
	p := props(v)
	uri := str(p, "vaultUri")
	if !strings.HasPrefix(uri, "https://pogivault.vault.localhost:") || p["enableRbacAuthorization"] != true || p["sku"].(map[string]any)["name"] != "premium" || p["provisioningState"] != "Succeeded" {
		t.Fatalf("vault = %v", v)
	}

	// access policies: add merges permissions, remove drops the policy
	pol := func(op string, policies ...any) map[string]any {
		return r.put("Microsoft.KeyVault/vaults/pogivault/accessPolicies/"+op, map[string]any{"properties": map[string]any{"accessPolicies": policies}})
	}
	pol("add", map[string]any{"tenantId": "t1", "objectId": "o1", "permissions": map[string]any{"secrets": []any{"Set"}, "keys": []any{"Get"}}},
		map[string]any{"tenantId": "t1", "objectId": "o2", "permissions": map[string]any{"secrets": []any{"List"}}})
	pols := props(r.mustGet("Microsoft.KeyVault/vaults/pogivault"))["accessPolicies"].([]any)
	if len(pols) != 2 {
		t.Fatalf("policies = %v", pols)
	}
	if perms := pols[0].(map[string]any)["permissions"].(map[string]any); len(perms["secrets"].([]any)) != 2 || len(perms["keys"].([]any)) != 1 {
		t.Fatalf("merged permissions = %v", perms)
	}
	pol("remove", map[string]any{"tenantId": "t1", "objectId": "o2"})
	if n := len(props(r.mustGet("Microsoft.KeyVault/vaults/pogivault"))["accessPolicies"].([]any)); n != 1 {
		t.Fatalf("policies after remove = %d", n)
	}

	// a secret made through the resource manager is in the vault the data plane serves
	s := r.put("Microsoft.KeyVault/vaults/pogivault/secrets/db-password", map[string]any{"tags": map[string]any{"a": "b"},
		"properties": map[string]any{"value": "hunter2", "contentType": "text/plain"}})
	if sp := props(s); !strings.HasPrefix(str(sp, "secretUri"), "http") || sp["contentType"] != "text/plain" || s["tags"].(map[string]any)["a"] != "b" {
		t.Fatalf("secret = %v", s)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/secrets/db-password?api-version=7.4", nil)
	req.Host = "pogivault.vault.localhost"
	req.Header.Set("Authorization", "Bearer x")
	res := must(http.DefaultClient.Do(req))
	body := must(io.ReadAll(res.Body))
	res.Body.Close()
	if !strings.Contains(string(body), `"value":"hunter2"`) {
		t.Fatalf("data plane: %d %s", res.StatusCode, body)
	}
	if l := r.list("Microsoft.KeyVault/vaults/pogivault/secrets"); len(l) != 1 || str(l[0], "name") != "db-password" {
		t.Fatalf("secrets = %v", l)
	}
	r.delete("Microsoft.KeyVault/vaults/pogivault/secrets/db-password")
	r.notFound("Microsoft.KeyVault/vaults/pogivault/secrets/db-password")

	r.delete("Microsoft.KeyVault/vaults/pogivault")
	r.notFound("Microsoft.KeyVault/vaults/pogivault")
}

// ---- Cosmos DB ----

func TestCosmos(t *testing.T) {
	r, srv := start(t)
	const acct = "Microsoft.DocumentDB/databaseAccounts/pogicosmos"
	a := r.put(acct, map[string]any{"location": "westus", "kind": "GlobalDocumentDB", "properties": map[string]any{
		"databaseAccountOfferType": "Standard", "consistencyPolicy": map[string]any{"defaultConsistencyLevel": "Eventual"},
		"geoLocations": []any{map[string]any{"locationName": "West US", "failoverPriority": 0}}}})
	p := props(a)
	if p["documentEndpoint"] != srv.URL+"/" || p["consistencyPolicy"].(map[string]any)["defaultConsistencyLevel"] != "Eventual" ||
		len(p["writeLocations"].([]any)) != 1 || p["provisioningState"] != "Succeeded" || p["geoLocations"] != nil {
		t.Fatalf("account = %v", a)
	}
	keys := r.action(acct, "listKeys")
	if len(str(keys, "primaryMasterKey")) != 88 || r.action(acct, "listReadOnlyKeys")["primaryMasterKey"] != nil {
		t.Fatalf("keys = %v", keys)
	}
	cs := r.action(acct, "listConnectionStrings")["connectionStrings"].([]any)
	if len(cs) != 4 || !strings.Contains(cs[0].(map[string]any)["connectionString"].(string), "AccountKey="+str(keys, "primaryMasterKey")) {
		t.Fatalf("connection strings = %v", cs)
	}

	db := r.put(acct+"/sqlDatabases/shop", map[string]any{"properties": map[string]any{
		"resource": map[string]any{"id": "shop"}, "options": map[string]any{"throughput": 800}}})
	if res := props(db)["resource"].(map[string]any); res["id"] != "shop" || res["_rid"] == nil || res["_etag"] == nil {
		t.Fatalf("database = %v", db)
	}
	ct := r.put(acct+"/sqlDatabases/shop/containers/orders", map[string]any{"properties": map[string]any{
		"resource": map[string]any{"id": "orders", "partitionKey": map[string]any{"paths": []any{"/customer"}, "kind": "Hash"}, "defaultTtl": 60,
			"uniqueKeyPolicy": map[string]any{"uniqueKeys": []any{map[string]any{"paths": []any{"/sku"}}}}},
		"options": map[string]any{"autoscaleSettings": map[string]any{"maxThroughput": 4000}}}})
	res := props(ct)["resource"].(map[string]any)
	if res["partitionKey"].(map[string]any)["paths"].([]any)[0] != "/customer" || res["defaultTtl"] != float64(60) || res["indexingPolicy"] == nil {
		t.Fatalf("container = %v", ct)
	}
	if _, err := r.get(acct + "/sqlDatabases/shop/containers/none"); core.AsError(err).Code != "ResourceNotFound" {
		t.Fatalf("missing container: %v", err)
	}
	if l := r.list(acct + "/sqlDatabases"); len(l) != 1 {
		t.Fatalf("databases = %v", l)
	}
	if l := r.list(acct + "/sqlDatabases/shop/containers"); len(l) != 1 {
		t.Fatalf("containers = %v", l)
	}

	tp := props(r.mustGet(acct + "/sqlDatabases/shop/throughputSettings/default"))["resource"].(map[string]any)
	if tp["throughput"] != float64(800) {
		t.Fatalf("database throughput = %v", tp)
	}
	if tp := props(r.mustGet(acct + "/sqlDatabases/shop/containers/orders/throughputSettings/default"))["resource"].(map[string]any); tp["autoscaleSettings"].(map[string]any)["maxThroughput"] != float64(4000) {
		t.Fatalf("container throughput = %v", tp)
	}
	r.put(acct+"/sqlDatabases/shop/throughputSettings/default", map[string]any{"properties": map[string]any{"resource": map[string]any{"throughput": 1200}}})
	if tp := props(r.mustGet(acct + "/sqlDatabases/shop/throughputSettings/default"))["resource"].(map[string]any); tp["throughput"] != float64(1200) {
		t.Fatalf("replaced throughput = %v", tp)
	}

	// the official SDK reads what ARM made, and writes to it
	insecure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	cc := must(azcosmos.NewClientWithKey(str(p, "documentEndpoint"), must(azcosmos.NewKeyCredential(str(keys, "primaryMasterKey"))),
		&azcosmos.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: insecure}}))
	ctr := must(must(cc.NewContainer("shop", "orders")).Read(bg, nil))
	if ctr.ContainerProperties.PartitionKeyDefinition.Paths[0] != "/customer" {
		t.Fatalf("container read by the SDK = %+v", ctr.ContainerProperties)
	}
	client := must(cc.NewContainer("shop", "orders"))
	must(client.UpsertItem(bg, azcosmos.NewPartitionKeyString("ana"), []byte(`{"id":"1","customer":"ana"}`), nil))

	r.delete(acct + "/sqlDatabases/shop/containers/orders")
	r.notFound(acct + "/sqlDatabases/shop/containers/orders")
	r.delete(acct + "/sqlDatabases/shop")
	r.notFound(acct + "/sqlDatabases/shop")
	r.delete(acct)
	r.notFound(acct)
}

// ---- Service Bus ----

func amqpConn(t *testing.T, srv *pogi.Running, conn string) string {
	t.Helper()
	// the namespace is the first label of the host the client dials: keep it, and use this server's broker port
	i := strings.Index(conn, "Endpoint=sb://")
	if i < 0 || !strings.Contains(conn, "UseDevelopmentEmulator=true") {
		t.Fatalf("connection string = %q", conn)
	}
	end := strings.Index(conn[i:], ";") + i
	host, _, _ := strings.Cut(conn[i+len("Endpoint=sb://"):end], ":")
	port := srv.Ports["amqp"][strings.LastIndexByte(srv.Ports["amqp"], ':')+1:]
	return conn[:i] + "Endpoint=sb://" + host + ":" + port + conn[end:]
}

func TestServiceBus(t *testing.T) {
	r, srv := start(t)
	const ns = "Microsoft.ServiceBus/namespaces/pogibus"
	n := r.put(ns, map[string]any{"location": "eastus", "sku": map[string]any{"name": "Premium", "capacity": 1}, "properties": map[string]any{"zoneRedundant": true}})
	if np := props(n); n["sku"].(map[string]any)["tier"] != "Premium" || np["zoneRedundant"] != true || np["provisioningState"] != "Succeeded" ||
		!strings.HasPrefix(str(np, "serviceBusEndpoint"), "https://pogibus.") {
		t.Fatalf("namespace = %v", n)
	}
	q := r.put(ns+"/queues/orders", map[string]any{"properties": map[string]any{"lockDuration": "PT30S", "maxDeliveryCount": 3, "requiresSession": false,
		"deadLetteringOnMessageExpiration": true, "defaultMessageTimeToLive": "P1D"}})
	if qp := props(q); qp["lockDuration"] != "PT30S" || qp["maxDeliveryCount"] != float64(3) || qp["deadLetteringOnMessageExpiration"] != true ||
		qp["defaultMessageTimeToLive"] != "P1D" || qp["status"] != "Active" || qp["maxSizeInMegabytes"] != float64(1024) {
		t.Fatalf("queue = %v", q)
	}
	r.put(ns+"/queues/orders", map[string]any{"properties": map[string]any{"maxDeliveryCount": 7}}) // PUT again updates
	if props(r.mustGet(ns + "/queues/orders"))["maxDeliveryCount"] != float64(7) {
		t.Fatal("queue was not updated")
	}
	r.put(ns+"/topics/events", map[string]any{"properties": map[string]any{"requiresDuplicateDetection": true}})
	sub := r.put(ns+"/topics/events/subscriptions/audit", map[string]any{"properties": map[string]any{"maxDeliveryCount": 5}})
	if props(sub)["maxDeliveryCount"] != float64(5) || props(r.mustGet(ns + "/topics/events"))["subscriptionCount"] != float64(1) {
		t.Fatalf("subscription = %v", sub)
	}
	if l := r.list(ns + "/queues"); len(l) != 1 {
		t.Fatalf("queues = %v", l)
	}
	if l := r.list(ns + "/topics/events/subscriptions"); len(l) != 1 {
		t.Fatalf("subscriptions = %v", l)
	}
	r.notFound(ns + "/queues/none")

	rules := r.list(ns + "/authorizationRules")
	if len(rules) != 1 || str(rules[0], "name") != "RootManageSharedAccessKey" {
		t.Fatalf("rules = %v", rules)
	}
	keys := r.action(ns+"/authorizationRules/RootManageSharedAccessKey", "listKeys")
	conn := str(keys, "primaryConnectionString")
	if !strings.Contains(conn, "UseDevelopmentEmulator=true") || !strings.Contains(conn, "Endpoint=sb://pogibus.") || !strings.Contains(conn, "SharedAccessKeyName=RootManageSharedAccessKey") {
		t.Fatalf("connection string = %q", conn)
	}
	r.put(ns+"/queues/orders/authorizationRules/send-only", map[string]any{"properties": map[string]any{"rights": []any{"Send"}}})
	if c := str(r.action(ns+"/queues/orders/authorizationRules/send-only", "listKeys"), "primaryConnectionString"); !strings.HasSuffix(c, ";EntityPath=orders") {
		t.Fatalf("entity rule connection string = %q", c)
	}

	// messages flow through what the resource manager made
	cli := must(azservicebus.NewClientFromConnectionString(amqpConn(t, srv, conn), nil))
	defer cli.Close(bg)
	snd := must(cli.NewSender("orders", nil))
	must(0, snd.SendMessage(bg, &azservicebus.Message{Body: []byte("o-1")}, nil))
	rcv := must(cli.NewReceiverForQueue("orders", nil))
	msgs := must(rcv.ReceiveMessages(bg, 1, nil))
	if len(msgs) != 1 || string(msgs[0].Body) != "o-1" {
		t.Fatalf("received %v", msgs)
	}
	must(0, rcv.CompleteMessage(bg, msgs[0], nil))
	tsnd := must(cli.NewSender("events", nil))
	must(0, tsnd.SendMessage(bg, &azservicebus.Message{Body: []byte("e-1")}, nil))
	srcv := must(cli.NewReceiverForSubscription("events", "audit", nil))
	if m := must(srcv.ReceiveMessages(bg, 1, nil)); len(m) != 1 || string(m[0].Body) != "e-1" {
		t.Fatalf("subscription received %v", m)
	}

	r.delete(ns + "/topics/events/subscriptions/audit")
	r.delete(ns + "/queues/orders")
	r.notFound(ns + "/queues/orders")
	r.delete(ns)
	r.notFound(ns)
}

// ---- App Configuration ----

func TestAppConfiguration(t *testing.T) {
	r, _ := start(t)
	const st = "Microsoft.AppConfiguration/configurationStores/pogiconf"
	s := r.put(st, map[string]any{"location": "eastus", "sku": map[string]any{"name": "free"}})
	if sp := props(s); !strings.HasSuffix(str(sp, "endpoint"), "/_azure/appconfig/pogiconf") || s["sku"].(map[string]any)["name"] != "free" || sp["provisioningState"] != "Succeeded" {
		t.Fatalf("store = %v", s)
	}
	keys := r.action(st, "listKeys")["value"].([]any)
	if len(keys) != 4 {
		t.Fatalf("keys = %v", keys)
	}
	cs := keys[0].(map[string]any)["connectionString"].(string)
	cl := must(azappconfig.NewClientFromConnectionString(cs, nil))
	must(cl.AddSetting(bg, "color", to.Ptr("blue"), nil))
	if v := must(cl.GetSetting(bg, "color", nil)); *v.Value != "blue" {
		t.Fatalf("setting = %v", *v.Value)
	}
	r.delete(st)
	r.notFound(st)
}
