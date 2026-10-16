package armres_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/nrndev/pogi/internal/azure/email"
	"github.com/nrndev/pogi/internal/core"
)

// ---- Event Hubs ----

func TestEventHubs(t *testing.T) {
	r, srv := start(t)
	const ns = "Microsoft.EventHub/namespaces/pogihubs"
	n := r.put(ns, map[string]any{"location": "eastus", "sku": map[string]any{"name": "Standard", "capacity": 2}, "properties": map[string]any{"kafkaEnabled": false}})
	if np := props(n); np["kafkaEnabled"] != false || np["status"] != "Active" || n["sku"].(map[string]any)["capacity"] != float64(2) {
		t.Fatalf("namespace = %v", n)
	}
	hub := r.put(ns+"/eventhubs/telemetry", map[string]any{"properties": map[string]any{"partitionCount": 3, "messageRetentionInDays": 2}})
	hp := props(hub)
	if hp["partitionCount"] != float64(3) || len(hp["partitionIds"].([]any)) != 3 || hp["messageRetentionInDays"] != float64(2) || hp["status"] != "Active" {
		t.Fatalf("event hub = %v", hub)
	}
	r.put(ns+"/eventhubs/telemetry", map[string]any{"properties": map[string]any{"partitionCount": 9}}) // the partition count is fixed for life
	if props(r.mustGet(ns + "/eventhubs/telemetry"))["partitionCount"] != float64(3) {
		t.Fatal("PUT changed the partition count")
	}
	r.put(ns+"/eventhubs/telemetry/consumergroups/archiver", map[string]any{})
	if l := r.list(ns + "/eventhubs/telemetry/consumergroups"); len(l) != 2 || str(l[0], "name") != "$Default" {
		t.Fatalf("consumer groups = %v", l)
	}

	rule := ns + "/eventhubs/telemetry/authorizationRules/sender"
	r.put(rule, map[string]any{"properties": map[string]any{"rights": []any{"Send"}}})
	conn := str(r.action(rule, "listKeys"), "primaryConnectionString")
	if !strings.HasSuffix(conn, ";EntityPath=telemetry") || !strings.Contains(conn, "Endpoint=sb://pogihubs.") {
		t.Fatalf("connection string = %q", conn)
	}
	root := str(r.action(ns+"/authorizationRules/RootManageSharedAccessKey", "listKeys"), "primaryConnectionString")
	if strings.Contains(root, "EntityPath") {
		t.Fatalf("namespace connection string = %q", root)
	}

	p := must(azeventhubs.NewProducerClientFromConnectionString(amqpConn(t, srv, conn), "", nil))
	defer p.Close(bg)
	if got := must(p.GetEventHubProperties(bg, nil)); got.Name != "telemetry" || len(got.PartitionIDs) != 3 {
		t.Fatalf("hub seen by the SDK = %+v", got)
	}
	r.delete(ns + "/eventhubs/telemetry/consumergroups/archiver")
	r.notFound(ns + "/eventhubs/telemetry/consumergroups/archiver")
	r.delete(ns + "/eventhubs/telemetry")
	r.notFound(ns + "/eventhubs/telemetry")
	r.delete(ns)
	r.notFound(ns)
}

// ---- Event Grid ----

func TestEventGrid(t *testing.T) {
	r, srv := start(t)
	const sb = "Microsoft.ServiceBus/namespaces/egbus"
	r.put(sb, map[string]any{})
	r.put(sb+"/queues/events", map[string]any{})
	conn := str(r.action(sb+"/authorizationRules/RootManageSharedAccessKey", "listKeys"), "primaryConnectionString")

	tp := r.put("Microsoft.EventGrid/topics/orders", map[string]any{"location": "eastus", "properties": map[string]any{"inputSchema": "CloudEventSchemaV1_0"}})
	if p := props(tp); !strings.HasSuffix(str(p, "endpoint"), "/api/events") || !strings.Contains(str(p, "endpoint"), "orders.eventgrid.") || p["inputSchema"] != "CloudEventSchemaV1_0" {
		t.Fatalf("topic = %v", tp)
	}
	if k := r.action("Microsoft.EventGrid/topics/orders", "listKeys"); str(k, "key1") == "" || str(k, "key1") == str(k, "key2") {
		t.Fatalf("keys = %v", k)
	}
	if l := r.list("Microsoft.EventGrid/topics"); len(l) != 1 {
		t.Fatalf("topics = %v", l)
	}

	sbQueue := rg + sb + "/queues/events"
	subPath := "Microsoft.EventGrid/topics/orders/providers/Microsoft.EventGrid/eventSubscriptions/to-bus"
	sub := r.put(subPath, map[string]any{"properties": map[string]any{
		"destination":         map[string]any{"endpointType": "ServiceBusQueue", "properties": map[string]any{"resourceId": sbQueue}},
		"filter":              map[string]any{"includedEventTypes": []any{"order.placed"}, "subjectBeginsWith": "/eu/"},
		"eventDeliverySchema": "CloudEventSchemaV1_0", "labels": []any{"a"}}})
	sp := props(sub)
	if sp["destination"].(map[string]any)["endpointType"] != "ServiceBusQueue" || sp["topic"] == nil || sp["provisioningState"] != "Succeeded" ||
		sp["filter"].(map[string]any)["subjectBeginsWith"] != "/eu/" || sp["retryPolicy"].(map[string]any)["maxDeliveryAttempts"] != float64(30) {
		t.Fatalf("event subscription = %v", sub)
	}
	if l := r.list("Microsoft.EventGrid/topics/orders/providers/Microsoft.EventGrid/eventSubscriptions"); len(l) != 1 {
		t.Fatalf("event subscriptions = %v", l)
	}
	r.put(subPath, map[string]any{"properties": map[string]any{ // a PUT replaces the subscription
		"destination": map[string]any{"endpointType": "ServiceBusQueue", "properties": map[string]any{"resourceId": sbQueue}},
		"filter":      map[string]any{"includedEventTypes": []any{"order.placed"}}, "eventDeliverySchema": "CloudEventSchemaV1_0"}})
	if f := props(r.mustGet(subPath))["filter"].(map[string]any); f["subjectBeginsWith"] != nil {
		t.Fatalf("filter after replace = %v", f)
	}

	// an event published to the topic reaches the queue the subscription names
	ev := `{"specversion":"1.0","id":"e1","source":"/orders","type":"order.placed","subject":"/eu/1","data":{"n":1}}`
	req, _ := http.NewRequest("POST", srv.URL+"/_azure/eventgrid/orders/api/events", strings.NewReader("["+ev+"]"))
	req.Header.Set("Content-Type", "application/cloudevents-batch+json")
	req.Header.Set("aeg-sas-key", "k")
	res := must(http.DefaultClient.Do(req))
	body := must(io.ReadAll(res.Body))
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("publish: %d %s", res.StatusCode, body)
	}
	cli := must(azservicebus.NewClientFromConnectionString(amqpConn(t, srv, conn), nil))
	defer cli.Close(bg)
	rcv := must(cli.NewReceiverForQueue("events", nil))
	ctx, cancel := context.WithTimeout(bg, 20*time.Second)
	defer cancel()
	msgs := must(rcv.ReceiveMessages(ctx, 1, nil))
	if len(msgs) != 1 || !strings.Contains(string(msgs[0].Body), `"order.placed"`) {
		t.Fatalf("queue received %v", msgs)
	}

	r.delete(subPath)
	r.notFound(subPath)
	r.delete("Microsoft.EventGrid/topics/orders")
	r.notFound("Microsoft.EventGrid/topics/orders")

	st := r.put("Microsoft.EventGrid/systemTopics/blobs", map[string]any{"location": "global", "properties": map[string]any{
		"source": rg + "Microsoft.Storage/storageAccounts/acct", "topicType": "Microsoft.Storage.StorageAccounts"}})
	if props(st)["topicType"] != "Microsoft.Storage.StorageAccounts" {
		t.Fatalf("system topic = %v", st)
	}
	r.put("Microsoft.EventGrid/systemTopics/blobs/eventSubscriptions/s1", map[string]any{"properties": map[string]any{
		"destination": map[string]any{"endpointType": "ServiceBusQueue", "properties": map[string]any{"resourceId": sbQueue}}}})
	if l := r.list("Microsoft.EventGrid/systemTopics/blobs/eventSubscriptions"); len(l) != 1 || str(l[0], "name") != "s1" {
		t.Fatalf("system topic subscriptions = %v", l)
	}
}

// ---- Functions ----

func TestFunctionApps(t *testing.T) {
	r, srv := start(t)
	plan := r.put("Microsoft.Web/serverfarms/plan1", map[string]any{"location": "eastus", "kind": "functionapp", "sku": map[string]any{"name": "Y1", "tier": "Dynamic"},
		"properties": map[string]any{"reserved": true}})
	if props(plan)["reserved"] != true || props(plan)["provisioningState"] != "Succeeded" || plan["sku"].(map[string]any)["name"] != "Y1" {
		t.Fatalf("plan = %v", plan)
	}
	site := r.put("Microsoft.Web/sites/pogifn", map[string]any{"location": "eastus", "kind": "functionapp,linux", "properties": map[string]any{
		"serverFarmId": rg + "Microsoft.Web/serverfarms/plan1", "httpsOnly": true,
		"siteConfig": map[string]any{"linuxFxVersion": "Python|3.11", "appSettings": []any{map[string]any{"name": "GREETING", "value": "hi"}}}}})
	sp := props(site)
	if site["kind"] != "functionapp,linux" || sp["httpsOnly"] != true || sp["state"] != "Running" || !strings.HasPrefix(str(sp, "defaultHostName"), "pogifn.azurewebsites.") || sp["siteConfig"] != nil {
		t.Fatalf("site = %v", site)
	}
	found := func(name string) bool {
		for _, tb := range srv.Hub.Service("azfunctions").(core.Browsable).Resources() {
			for _, row := range tb.Rows {
				if row[0] == name {
					return true
				}
			}
		}
		return false
	}
	if !found("pogifn") {
		t.Fatal("the function app is not in the Functions service")
	}
	r.put("Microsoft.Web/sites/pogifn", map[string]any{"location": "eastus", "kind": "functionapp,linux", "properties": map[string]any{ // update
		"siteConfig": map[string]any{"appSettings": []any{map[string]any{"name": "FUNCTIONS_WORKER_RUNTIME", "value": "node"}}}}})
	r.put("Microsoft.Web/sites/webapp", map[string]any{"location": "eastus", "kind": "app,linux"})
	if found("webapp") {
		t.Fatal("a plain web app became a function app")
	}
	if l := r.list("Microsoft.Web/sites"); len(l) != 2 {
		t.Fatalf("sites = %v", l)
	}
	r.delete("Microsoft.Web/sites/pogifn")
	r.notFound("Microsoft.Web/sites/pogifn")
	if found("pogifn") {
		t.Fatal("the function app outlived its site")
	}
	r.delete("Microsoft.Web/serverfarms/plan1")
	r.notFound("Microsoft.Web/serverfarms/plan1")
}

// ---- SignalR ----

func TestSignalR(t *testing.T) {
	r, srv := start(t)
	const sr = "Microsoft.SignalRService/signalR/pogisignal"
	s := r.put(sr, map[string]any{"location": "eastus", "kind": "SignalR", "sku": map[string]any{"name": "Free_F1", "tier": "Free", "capacity": 1}})
	if p := props(s); !strings.HasPrefix(str(p, "hostName"), "pogisignal.service.signalr.") || s["sku"].(map[string]any)["name"] != "Free_F1" || p["version"] != "1.0" {
		t.Fatalf("signalr = %v", s)
	}
	keys := r.action(sr, "listKeys")
	key := str(keys, "primaryKey")
	if key == "" || key == "pogi-signalr-development-access-key" || !strings.Contains(str(keys, "primaryConnectionString"), "AccessKey="+key+";Version=1.0;") {
		t.Fatalf("keys = %v", keys)
	}
	// negotiation tokens are signed with the key the resource manager handed out
	req, _ := http.NewRequest("POST", srv.URL+"/_azure/signalr/pogisignal/client/negotiate?hub=chat", nil)
	res := must(http.DefaultClient.Do(req))
	var neg struct {
		AccessToken string `json:"accessToken"`
	}
	b := must(io.ReadAll(res.Body))
	res.Body.Close()
	if err := json.Unmarshal(b, &neg); err != nil || neg.AccessToken == "" {
		t.Fatalf("negotiate: %d %s", res.StatusCode, b)
	}
	parts := strings.Split(neg.AccessToken, ".")
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil)); parts[2] != want {
		t.Fatalf("token signature %s, want %s", parts[2], want)
	}
	r.delete(sr)
	r.notFound(sr)
}

// ---- Communication Services ----

func TestCommunicationServices(t *testing.T) {
	r, srv := start(t)
	const cs = "Microsoft.Communication/communicationServices/pogicomm"
	c := r.put(cs, map[string]any{"location": "global", "properties": map[string]any{"dataLocation": "Europe"}})
	if p := props(c); p["dataLocation"] != "Europe" || !strings.HasPrefix(str(p, "hostName"), "pogicomm.communication.") || p["provisioningState"] != "Succeeded" {
		t.Fatalf("communication service = %v", c)
	}
	keys := r.action(cs, "listKeys")
	if !strings.HasPrefix(str(keys, "primaryConnectionString"), "endpoint=https://pogicomm.communication.") || !strings.HasSuffix(str(keys, "primaryConnectionString"), "accesskey="+str(keys, "primaryKey")) {
		t.Fatalf("keys = %v", keys)
	}
	// mail sent to the resource shows up in the Email service
	mail := `{"senderAddress":"a@x.test","content":{"subject":"hi","plainText":"yo"},"recipients":{"to":[{"address":"b@x.test"}]}}`
	req, _ := http.NewRequest("POST", srv.URL+"/_azure/communication/pogicomm/emails:send?api-version=2023-03-31", strings.NewReader(mail))
	req.Header.Set("Content-Type", "application/json")
	res := must(http.DefaultClient.Do(req))
	res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("send: %d", res.StatusCode)
	}
	if out := srv.Hub.Service("azemail").(interface{ Outbox() []email.Message }).Outbox(); len(out) != 1 || out[0].Resource != "pogicomm" {
		t.Fatalf("outbox = %+v", out)
	}
	r.delete(cs)
	r.notFound(cs)
}

// ---- through the resource manager's HTTP surface ----

// What the azurerm provider needs of the status codes and the scoped event subscription path.
func TestResourceManagerHTTP(t *testing.T) {
	_, srv := start(t)
	call := func(method, path string, body any, want ...int) (int, http.Header, map[string]any) {
		t.Helper()
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = strings.NewReader(string(b))
		}
		req, _ := http.NewRequest(method, srv.URL+path+"?api-version=2023-01-01", rd)
		req.Header.Set("Authorization", "Bearer x")
		req.Header.Set("Content-Type", "application/json")
		res := must(http.DefaultClient.Do(req))
		defer res.Body.Close()
		var out map[string]any
		json.Unmarshal(must(io.ReadAll(res.Body)), &out)
		if !slices.Contains(want, res.StatusCode) {
			t.Fatalf("%s %s: %d %v, want %v", method, path, res.StatusCode, out, want)
		}
		return res.StatusCode, res.Header, out
	}
	sub := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/"
	// the SDKs accept only the statuses of the real service
	call("PUT", sub+"Microsoft.Storage/storageAccounts/httpacct", map[string]any{"location": "eastus"}, 202)
	call("PUT", sub+"Microsoft.Storage/storageAccounts/httpacct/queueServices/default/queues/queue1", map[string]any{}, 200)
	call("DELETE", sub+"Microsoft.Storage/storageAccounts/httpacct/queueServices/default/queues/queue1", nil, 204)
	call("PUT", sub+"Microsoft.ServiceBus/namespaces/httpbus", map[string]any{"location": "eastus"}, 201)
	call("PUT", sub+"Microsoft.ServiceBus/namespaces/httpbus/queues/q", map[string]any{"properties": map[string]any{}}, 200)
	call("DELETE", sub+"Microsoft.Storage/storageAccounts/httpacct", nil, 200)

	// a Key Vault is listed by type, which azurerm does to find the vault a URL belongs to
	call("PUT", sub+"Microsoft.KeyVault/vaults/httpvault", map[string]any{"location": "eastus"}, 201)
	_, _, l := call("GET", "/subscriptions/"+core.AzureSubscription+"/resources", nil, 200)
	if n := len(l["value"].([]any)); n < 2 {
		t.Fatalf("resources = %v", l)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/subscriptions/"+core.AzureSubscription+"/resources?api-version=2021-04-01&$filter="+
		"resourceType%20eq%20'Microsoft.KeyVault/vaults'", nil)
	req.Header.Set("Authorization", "Bearer x")
	res := must(http.DefaultClient.Do(req))
	var typed struct{ Value []map[string]any }
	json.Unmarshal(must(io.ReadAll(res.Body)), &typed)
	res.Body.Close()
	if len(typed.Value) != 1 || typed.Value[0]["name"] != "httpvault" {
		t.Fatalf("vaults by resourceType = %v", typed.Value)
	}
	// a soft-deleted vault leaves a record to purge; a purge answers like an operation and forgets it
	call("DELETE", sub+"Microsoft.KeyVault/vaults/httpvault", nil, 200)
	deleted := "/subscriptions/" + core.AzureSubscription + "/providers/Microsoft.KeyVault/locations/eastus/deletedVaults/httpvault"
	call("GET", deleted, nil, 200)
	call("POST", deleted+"/purge", nil, 202)
	call("GET", deleted, nil, 404)

	// event subscriptions are scoped by the topic they are on
	call("PUT", sub+"Microsoft.EventGrid/topics/httptopic", map[string]any{"location": "eastus"}, 201, 202)
	scoped := sub + "Microsoft.EventGrid/topics/httptopic/providers/Microsoft.EventGrid/eventSubscriptions/s1"
	body := map[string]any{"properties": map[string]any{"destination": map[string]any{"endpointType": "ServiceBusQueue",
		"properties": map[string]any{"resourceId": sub + "Microsoft.ServiceBus/namespaces/httpbus/queues/q"}}}}
	call("PUT", scoped, body, 201)
	_, _, got := call("GET", scoped, nil, 200)
	if props(got)["provisioningState"] != "Succeeded" || got["type"] != "Microsoft.EventGrid/eventSubscriptions" {
		t.Fatalf("scoped subscription = %v", got)
	}
	_, _, all := call("GET", sub+"Microsoft.EventGrid/topics/httptopic/providers/Microsoft.EventGrid/eventSubscriptions", nil, 200)
	if len(all["value"].([]any)) != 1 {
		t.Fatalf("subscriptions of the topic = %v", all)
	}
	call("DELETE", scoped, nil, 200)
	call("GET", scoped, nil, 404)
}
