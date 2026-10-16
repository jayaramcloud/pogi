package eventgrid_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/messaging"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/eventgrid"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// Azurite's well-known development account key.
const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

const account = "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup +
	"/providers/Microsoft.Storage/storageAccounts/devstoreaccount1"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	srv  *pogi.Running
	eg   *eventgrid.Service
	blob *azblob.Client
	tls  string                     // https://localhost:<port>: the SDKs refuse to send keys over plain HTTP
	opts *azeventgrid.ClientOptions // trusting pogi's CA
}

func setup(t *testing.T) *env {
	t.Helper()
	srv := pogi.Start(t)
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + key + ";BlobEndpoint=" + srv.Ports["azblob"] + "/devstoreaccount1;"
	e := &env{srv: srv, eg: srv.Hub.Service("eventgrid").(*eventgrid.Service), blob: must(azblob.NewClientFromConnectionString(conn, nil))}
	e.eg.Backoff = func(int) time.Duration { return time.Millisecond }
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	e.tls = "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	e.opts = &azeventgrid.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}}}
	return e
}

// delivery is what a webhook received.
type delivery struct {
	header http.Header
	body   []byte
}

// webhook is an endpoint that passes the validation handshake (unless refuse) and answers events with
// reply(n), n counting from 1.
type hook struct {
	*httptest.Server
	events chan delivery
	n      atomic.Int32
}

func webhook(t *testing.T, refuse bool, reply func(n int) int) *hook {
	h := &hook{events: make(chan delivery, 100)}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case refuse:
			w.WriteHeader(http.StatusOK) // 200, but no validationResponse
		case r.Method == http.MethodOptions:
			w.Header().Set("WebHook-Allowed-Origin", r.Header.Get("WebHook-Request-Origin"))
		case r.Header.Get("aeg-event-type") == "SubscriptionValidation":
			var evs []struct {
				EventType string
				Data      struct{ ValidationCode string }
			}
			json.Unmarshal(body, &evs)
			if len(evs) != 1 || evs[0].EventType != "Microsoft.EventGrid.SubscriptionValidationEvent" {
				t.Errorf("validation event = %s", body)
			}
			json.NewEncoder(w).Encode(map[string]string{"validationResponse": evs[0].Data.ValidationCode})
		default:
			h.events <- delivery{r.Header, body}
			w.WriteHeader(reply(int(h.n.Add(1))))
		}
	}))
	t.Cleanup(h.Close)
	return h
}

func ok(int) int { return http.StatusOK }

func (h *hook) next(t *testing.T) delivery {
	t.Helper()
	select {
	case d := <-h.events:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no event was delivered")
		return delivery{}
	}
}

func (h *hook) none(t *testing.T) {
	t.Helper()
	select {
	case d := <-h.events:
		t.Fatalf("unexpected event %s", d.body)
	case <-time.After(300 * time.Millisecond):
	}
}

func webhookSub(name, url string) eventgrid.Subscription {
	return eventgrid.Subscription{Name: name, Destination: eventgrid.Destination{Type: "WebHook", Endpoint: url}}
}

func busSub(name, queue string) eventgrid.Subscription {
	return eventgrid.Subscription{Name: name, Destination: eventgrid.Destination{Type: "ServiceBusQueue", ResourceID: "/subscriptions/" +
		core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/Microsoft.ServiceBus/namespaces/ns/queues/" + queue}}
}

func TestBlobEventsToWebhookAndServiceBus(t *testing.T) {
	e := setup(t)
	hook := webhook(t, false, ok)
	var bus = make(chan *core.Message, 10)
	e.srv.Hub.OnDeliver("microsoft.servicebus", func(id string, m *core.Message) error {
		if !strings.HasSuffix(id, "/namespaces/ns/queues/q") {
			t.Errorf("service bus target = %s", id)
		}
		bus <- m
		return nil
	})
	if err := e.eg.CreateSystemTopic("storage", account, "Microsoft.Storage.StorageAccounts"); err != nil {
		t.Fatal(err)
	}
	// The handshake: an endpoint that does not echo the code fails the creation.
	refuser := webhook(t, true, ok)
	err := e.eg.CreateSubscription("systemTopics/storage", webhookSub("nope", refuser.URL))
	if got := core.AsError(err); err == nil || got.Code != "Url validation" || got.Status != 400 {
		t.Fatalf("a refused handshake = %v", err)
	}
	if subs := e.eg.Subscriptions("systemTopics/storage"); len(subs) != 0 {
		t.Fatalf("a failed handshake left %v", subs)
	}
	web := webhookSub("web", hook.URL)
	web.Filter.IncludedEventTypes = []string{"Microsoft.Storage.BlobCreated"}
	web.Filter.SubjectBeginsWith = "/blobServices/default/containers/photos/"
	if err := e.eg.CreateSubscription("systemTopics/storage", web); err != nil {
		t.Fatal(err)
	}
	if err := e.eg.CreateSubscription("systemTopics/storage", busSub("bus", "q")); err != nil {
		t.Fatal(err)
	}
	if err := e.eg.CreateSubscription("systemTopics/storage", busSub("bus", "q")); core.AsError(err).Status != 409 {
		t.Fatalf("a duplicate subscription = %v", err)
	}

	must(e.blob.CreateContainer(ctx, "photos", nil))
	must(e.blob.CreateContainer(ctx, "other", nil))
	must(e.blob.UploadBuffer(ctx, "photos", "cat.txt", []byte("meow"), nil))
	must(e.blob.UploadBuffer(ctx, "other", "x.txt", []byte("x"), nil)) // filtered out of the webhook

	d := hook.next(t)
	var evs []map[string]any
	if err := json.Unmarshal(d.body, &evs); err != nil || len(evs) != 1 {
		t.Fatalf("webhook body = %s", d.body)
	}
	ev := evs[0]
	data := ev["data"].(map[string]any)
	if ev["eventType"] != "Microsoft.Storage.BlobCreated" || ev["subject"] != "/blobServices/default/containers/photos/blobs/cat.txt" ||
		ev["topic"] != account || ev["id"] == "" || ev["metadataVersion"] != "1" ||
		data["api"] != "PutBlob" || data["contentLength"] != 4.0 || data["blobType"] != "BlockBlob" ||
		!strings.HasSuffix(data["url"].(string), "/photos/cat.txt") {
		t.Fatalf("webhook event = %s", d.body)
	}
	if d.header.Get("aeg-event-type") != "Notification" || d.header.Get("aeg-subscription-name") != "WEB" {
		t.Fatalf("webhook headers = %v", d.header)
	}
	hook.none(t) // the upload to "other" did not match the subject filter

	// Service Bus got both uploads, as one event each.
	var subjects []string
	for range 2 {
		select {
		case m := <-bus:
			var ev map[string]any
			if err := json.Unmarshal([]byte(m.Body), &ev); err != nil {
				t.Fatalf("service bus body = %q", m.Body)
			}
			subjects = append(subjects, ev["subject"].(string))
		case <-time.After(5 * time.Second):
			t.Fatal("service bus got no event")
		}
	}
	if len(subjects) != 2 || !strings.Contains(strings.Join(subjects, " "), "/other/blobs/x.txt") {
		t.Fatalf("service bus subjects = %v", subjects)
	}

	// Block lists and copies are BlobCreated too; deleting is BlobDeleted, which the webhook did not ask for.
	big := e.blob.ServiceClient().NewContainerClient("photos").NewBlockBlobClient("big.bin")
	id := base64.StdEncoding.EncodeToString([]byte("block-0001"))
	must(big.StageBlock(ctx, id, streaming.NopCloser(bytes.NewReader([]byte("staged"))), nil))
	must(big.CommitBlockList(ctx, []string{id}, nil))
	if d := hook.next(t); !strings.Contains(string(d.body), `"api":"PutBlockList"`) {
		t.Fatalf("block list event = %s", d.body)
	}
	must(e.blob.ServiceClient().NewContainerClient("photos").NewBlobClient("copy.txt").StartCopyFromURL(ctx,
		e.blob.ServiceClient().NewContainerClient("photos").NewBlobClient("cat.txt").URL(), nil))
	if d := hook.next(t); !strings.Contains(string(d.body), `"api":"CopyBlob"`) {
		t.Fatalf("copy event = %s", d.body)
	}
	must(e.blob.DeleteBlob(ctx, "photos", "cat.txt", nil))
	hook.none(t)
	for {
		m := <-bus
		if strings.Contains(m.Body, `"eventType":"Microsoft.Storage.BlobDeleted"`) && strings.Contains(m.Body, "cat.txt") {
			break
		}
	}
}

func TestPublishWithSDK(t *testing.T) {
	e := setup(t)
	hook := webhook(t, false, ok)
	if err := e.eg.CreateTopic("orders", ""); err != nil {
		t.Fatal(err)
	}
	if err := e.eg.CreateSubscription("topics/orders", webhookSub("all", hook.URL)); err != nil {
		t.Fatal(err)
	}
	// Path form and host form reach the same topic.
	client := must(azeventgrid.NewClientWithSharedKeyCredential(e.tls+"/_azure/eventgrid/orders/api/events", azcore.NewKeyCredential("any"), e.opts))
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	must(client.PublishEvents(ctx, []azeventgrid.Event{
		{ID: to.Ptr("e1"), Subject: to.Ptr("orders/1"), EventType: to.Ptr("Order.Placed"), EventTime: &when, DataVersion: to.Ptr("1.0"), Data: map[string]any{"total": 12.5}},
		{ID: to.Ptr("e2"), Subject: to.Ptr("orders/2"), EventType: to.Ptr("Order.Placed"), EventTime: &when, DataVersion: to.Ptr("1.0"), Data: "plain"},
	}, nil))
	sas := must(azeventgrid.NewClientWithSAS(e.tls+"/_azure/eventgrid/orders/api/events", azcore.NewSASCredential("sig"), e.opts))
	must(sas.PublishEvents(ctx, []azeventgrid.Event{{ID: to.Ptr("e3"), Subject: to.Ptr("s"), EventType: to.Ptr("t"), EventTime: &when, DataVersion: to.Ptr("1"), Data: 1}}, nil))
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/api/events?api-version=2018-01-01", strings.NewReader(
		`[{"id":"e4","subject":"s","eventType":"t","eventTime":"2026-01-02T03:04:05Z","dataVersion":"1","data":{}}]`))
	req.Host = "orders.eventgrid.localhost"
	req.Header.Set("aeg-sas-key", "k")
	if res := must(http.DefaultClient.Do(req)); res.StatusCode != 200 {
		t.Fatalf("host-routed publish = %d", res.StatusCode)
	}

	got := map[string]map[string]any{}
	for range 4 {
		var evs []map[string]any
		json.Unmarshal(hook.next(t).body, &evs)
		got[evs[0]["id"].(string)] = evs[0]
	}
	e1 := got["e1"]
	if e1["subject"] != "orders/1" || e1["eventType"] != "Order.Placed" || e1["dataVersion"] != "1.0" || e1["data"].(map[string]any)["total"] != 12.5 ||
		!strings.HasSuffix(e1["topic"].(string), "/providers/Microsoft.EventGrid/topics/orders") || !strings.HasPrefix(e1["eventTime"].(string), "2026-01-02T03:04:05") {
		t.Fatalf("event e1 = %v", e1)
	}
	if got["e2"]["data"] != "plain" || got["e3"] == nil || got["e4"] == nil {
		t.Fatalf("events = %v", got)
	}

	// Bad requests: a missing property, an unknown topic.
	_, err := client.PublishEvents(ctx, []azeventgrid.Event{{ID: to.Ptr("x"), EventType: to.Ptr("t"), EventTime: &when, DataVersion: to.Ptr("1")}}, nil)
	var rerr *azcore.ResponseError
	if !asResponse(err, &rerr) || rerr.StatusCode != 400 {
		t.Fatalf("an event without a subject = %v", err)
	}
	missing := must(azeventgrid.NewClientWithSharedKeyCredential(e.tls+"/_azure/eventgrid/nobody/api/events", azcore.NewKeyCredential("any"), e.opts))
	_, err = missing.PublishEvents(ctx, []azeventgrid.Event{{ID: to.Ptr("x"), Subject: to.Ptr("s"), EventType: to.Ptr("t"), EventTime: &when, DataVersion: to.Ptr("1")}}, nil)
	if !asResponse(err, &rerr) || rerr.StatusCode != 404 {
		t.Fatalf("an unknown topic = %v", err)
	}
}

func asResponse(err error, target **azcore.ResponseError) bool {
	for err != nil {
		if r, ok := err.(*azcore.ResponseError); ok {
			*target = r
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestCloudEventsSchema(t *testing.T) {
	e := setup(t)
	hook := webhook(t, false, ok)
	if err := e.eg.CreateTopic("ce", "CloudEventSchemaV1_0"); err != nil {
		t.Fatal(err)
	}
	// CloudEvents deliveries are validated with the OPTIONS handshake.
	sub := webhookSub("ce", hook.URL)
	sub.Schema = "CloudEventSchemaV1_0"
	if err := e.eg.CreateSubscription("topics/ce", sub); err != nil {
		t.Fatal(err)
	}
	if err := e.eg.CreateSubscription("topics/ce", func() eventgrid.Subscription {
		s := webhookSub("refused", webhook(t, true, ok).URL)
		s.Schema = "CloudEventSchemaV1_0"
		return s
	}()); core.AsError(err).Code != "Url validation" {
		t.Fatalf("a refused OPTIONS handshake = %v", err)
	}
	// The same topic also feeds a subscription that wants the Event Grid schema.
	eg := webhook(t, false, ok)
	if err := e.eg.CreateSubscription("topics/ce", webhookSub("eg", eg.URL)); err != nil {
		t.Fatal(err)
	}

	client := must(azeventgrid.NewClientWithSharedKeyCredential(e.tls+"/_azure/eventgrid/ce/api/events", azcore.NewKeyCredential("any"), e.opts))
	ev := must(messaging.NewCloudEvent("/sources/shop", "shop.order.placed", map[string]any{"id": 7}, &messaging.CloudEventOptions{Subject: to.Ptr("orders/7")}))
	must(client.PublishCloudEvents(ctx, []messaging.CloudEvent{ev}, nil))

	d := hook.next(t)
	if ct := d.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/cloudevents+json") {
		t.Fatalf("content type = %q", ct)
	}
	var ce map[string]any
	if err := json.Unmarshal(d.body, &ce); err != nil {
		t.Fatalf("body = %s", d.body)
	}
	if ce["specversion"] != "1.0" || ce["type"] != "shop.order.placed" || ce["source"] != "/sources/shop" || ce["subject"] != "orders/7" ||
		ce["id"] != ev.ID || ce["datacontenttype"] != "application/json" || ce["data"].(map[string]any)["id"] != 7.0 || ce["time"] == nil {
		t.Fatalf("cloud event = %s", d.body)
	}
	var evs []map[string]any
	if err := json.Unmarshal(eg.next(t).body, &evs); err != nil || evs[0]["eventType"] != "shop.order.placed" || evs[0]["subject"] != "orders/7" {
		t.Fatalf("the same event in the Event Grid schema = %v", evs)
	}
}

func TestFiltersEndToEnd(t *testing.T) {
	e := setup(t)
	byType, byData, bySubject := webhook(t, false, ok), webhook(t, false, ok), webhook(t, false, ok)
	e.eg.CreateTopic("t", "")
	a := webhookSub("types", byType.URL)
	a.Filter.IncludedEventTypes = []string{"Kind.B"}
	b := webhookSub("data", byData.URL)
	b.Filter.Advanced = []eventgrid.AdvancedFilter{
		{Operator: "StringIn", Key: "data.color", Values: []any{"red", "blue"}}, {Operator: "NumberGreaterThan", Key: "data.size", Value: 10.0}}
	c := webhookSub("subject", bySubject.URL)
	c.Filter.SubjectBeginsWith, c.Filter.SubjectEndsWith, c.Filter.IsSubjectCaseSensitive = "/Photos/", ".png", true
	for _, s := range []eventgrid.Subscription{a, b, c} {
		if err := e.eg.CreateSubscription("topics/t", s); err != nil {
			t.Fatal(err)
		}
	}
	pub := func(id, typ, subject, data string) {
		body := `[{"id":"` + id + `","subject":"` + subject + `","eventType":"` + typ + `","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1","data":` + data + `}]`
		res := must(http.Post(e.srv.URL+"/_azure/eventgrid/t/api/events", "application/json", strings.NewReader(body)))
		if res.StatusCode != 200 {
			t.Fatalf("publish = %d", res.StatusCode)
		}
	}
	pub("1", "Kind.A", "/Photos/a.png", `{"color":"red","size":11}`)
	pub("2", "Kind.B", "/photos/b.png", `{"color":"green","size":99}`)
	pub("3", "Kind.A", "/Photos/c.jpg", `{"color":"blue","size":5}`)
	ids := func(h *hook) string {
		var out []string
		for {
			select {
			case d := <-h.events:
				var evs []struct{ ID string }
				json.Unmarshal(d.body, &evs)
				out = append(out, evs[0].ID)
			case <-time.After(400 * time.Millisecond):
				return strings.Join(out, ",")
			}
		}
	}
	if got := ids(byType); got != "2" {
		t.Errorf("by type: %q", got)
	}
	if got := ids(byData); got != "1" {
		t.Errorf("by data: %q", got)
	}
	if got := ids(bySubject); got != "1" {
		t.Errorf("by subject (case sensitive): %q", got)
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	e := setup(t)
	e.eg.CreateTopic("t", "")
	flaky := webhook(t, false, func(n int) int { // fails twice, then works
		if n < 3 {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	})
	dead := webhook(t, false, func(int) int { return http.StatusServiceUnavailable })
	a := webhookSub("flaky", flaky.URL)
	b := webhookSub("dead", dead.URL)
	b.Retry.MaxAttempts = 3
	b.DeadLetter = &eventgrid.DeadLetter{ResourceID: account, Container: "deadletters"}
	for _, s := range []eventgrid.Subscription{a, b} {
		if err := e.eg.CreateSubscription("topics/t", s); err != nil {
			t.Fatal(err)
		}
	}
	res := must(http.Post(e.srv.URL+"/_azure/eventgrid/t/api/events", "application/json", strings.NewReader(
		`[{"id":"ev1","subject":"s","eventType":"t","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1","data":{"k":"v"}}]`)))
	if res.StatusCode != 200 {
		t.Fatalf("publish = %d", res.StatusCode)
	}

	// The test drives the retry clock instead of waiting for the ticker.
	var blobs []string
	for deadline := time.Now().Add(10 * time.Second); len(blobs) == 0 || flaky.n.Load() < 3; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("flaky saw %d attempts, dead-letter blobs = %v", flaky.n.Load(), blobs)
		}
		e.eg.Tick(time.Now().Add(time.Second))
		blobs = blobs[:0]
		pager := e.blob.NewListBlobsFlatPager("deadletters", nil)
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				break // the container is not there yet
			}
			for _, it := range page.Segment.BlobItems {
				blobs = append(blobs, *it.Name)
			}
		}
	}
	if dead.n.Load() != 3 || flaky.n.Load() != 3 {
		t.Fatalf("attempts: dead %d (want 3, then the dead letter), flaky %d (want 3: two failures, one success)", dead.n.Load(), flaky.n.Load())
	}
	if len(blobs) != 1 || !strings.HasPrefix(blobs[0], "t/dead/") || !strings.HasSuffix(blobs[0], "/ev1.json") {
		t.Fatalf("dead-letter blobs = %v", blobs)
	}
	stream := must(e.blob.DownloadStream(ctx, "deadletters", blobs[0], nil))
	raw, _ := io.ReadAll(stream.Body)
	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || len(out) != 1 {
		t.Fatalf("dead letter = %s", raw)
	}
	if out[0]["id"] != "ev1" || out[0]["deadLetterReason"] != "MaxDeliveryAttemptsExceeded" || out[0]["deliveryAttempts"] != 3.0 ||
		out[0]["lastHttpStatusCode"] != 503.0 || out[0]["data"].(map[string]any)["k"] != "v" {
		t.Fatalf("dead letter = %s", raw)
	}
	// Both endpoints saw the delivery count rise from 0.
	var counts []string
	for range 3 {
		counts = append(counts, flaky.next(t).header.Get("aeg-delivery-count"))
	}
	if strings.Join(counts, ",") != "0,1,2" {
		t.Fatalf("delivery counts = %v", counts)
	}
	// Nothing is left to retry.
	e.eg.Tick(time.Now().Add(time.Hour))
	if dead.n.Load() != 3 {
		t.Fatalf("an exhausted event was tried again: %d", dead.n.Load())
	}
}

func TestExpiredEventIsDeadLettered(t *testing.T) {
	e := setup(t)
	e.eg.CreateTopic("t", "")
	e.eg.Backoff = func(int) time.Duration { return time.Hour } // a retry would be after the 1 minute time to live
	h := webhook(t, false, func(int) int { return http.StatusInternalServerError })
	s := webhookSub("w", h.URL)
	s.Retry.TTLMinutes = 1
	s.DeadLetter = &eventgrid.DeadLetter{ResourceID: account, Container: "late-events"}
	if err := e.eg.CreateSubscription("topics/t", s); err != nil {
		t.Fatal(err)
	}
	http.Post(e.srv.URL+"/_azure/eventgrid/t/api/events", "application/json", strings.NewReader(
		`[{"id":"late","subject":"s","eventType":"t","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1"}]`))
	h.next(t)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		pager := e.blob.NewListBlobsFlatPager("late-events", nil)
		if page, err := pager.NextPage(ctx); err == nil && len(page.Segment.BlobItems) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no dead letter")
		}
	}
}

func TestDestinations(t *testing.T) {
	e := setup(t)
	e.eg.CreateTopic("t", "")
	prefix := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/"
	got := make(chan string, 10)
	for _, p := range []string{"microsoft.servicebus", "microsoft.eventhub", "microsoft.web"} {
		e.srv.Hub.OnDeliver(p, func(id string, m *core.Message) error { got <- p + " " + id + " " + m.Body[:1]; return nil })
	}
	svcs := map[string]eventgrid.Destination{
		"queue":    {Type: "ServiceBusQueue", ResourceID: prefix + "Microsoft.ServiceBus/namespaces/n/queues/q"},
		"topic":    {Type: "ServiceBusTopic", ResourceID: prefix + "Microsoft.ServiceBus/namespaces/n/topics/t"},
		"hub":      {Type: "EventHub", ResourceID: prefix + "Microsoft.EventHub/namespaces/n/eventhubs/h"},
		"function": {Type: "AzureFunction", ResourceID: prefix + "Microsoft.Web/sites/app/functions/fn"},
		"queue2":   {Type: "StorageQueue", ResourceID: prefix + "Microsoft.Storage/storageAccounts/devstoreaccount1/queueServices/default/queues/events"},
	}
	for name, d := range svcs {
		if err := e.eg.CreateSubscription("topics/t", eventgrid.Subscription{Name: name, Destination: d}); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []eventgrid.Destination{{Type: "HybridConnection", ResourceID: "x"}, {Type: "WebHook", Endpoint: "ftp://x"}, {Type: "EventHub", ResourceID: "nope"}} {
		if err := e.eg.CreateSubscription("topics/t", eventgrid.Subscription{Name: "bad", Destination: bad}); core.AsError(err).Status != 400 {
			t.Fatalf("destination %+v = %v", bad, err)
		}
	}
	if err := e.eg.CreateSubscription("topics/missing", busSub("x", "q")); core.AsError(err).Status != 404 {
		t.Fatalf("a subscription on a missing topic = %v", err)
	}

	qc := must(azqueue.NewQueueClientFromConnectionString("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey="+key+
		";QueueEndpoint="+e.srv.Ports["azqueue"]+"/devstoreaccount1;", "events", nil))
	must(qc.Create(ctx, nil))
	http.Post(e.srv.URL+"/_azure/eventgrid/t/api/events", "application/json", strings.NewReader(
		`[{"id":"1","subject":"s","eventType":"t","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1","data":{}}]`))

	seen := map[string]bool{}
	for range 4 {
		select {
		case m := <-got:
			seen[m] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("deliveries so far: %v", seen)
		}
	}
	for _, want := range []string{"microsoft.servicebus " + prefix + "Microsoft.ServiceBus/namespaces/n/queues/q {", "microsoft.servicebus " + prefix + "Microsoft.ServiceBus/namespaces/n/topics/t {",
		"microsoft.eventhub " + prefix + "Microsoft.EventHub/namespaces/n/eventhubs/h [", "microsoft.web " + prefix + "Microsoft.Web/sites/app/functions/fn {"} {
		if !seen[want] {
			t.Errorf("missing delivery %q in %v", want, seen)
		}
	}
	// The storage queue holds the event, base64 encoded.
	var msg string
	for deadline := time.Now().Add(5 * time.Second); msg == "" && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if res, err := qc.DequeueMessage(ctx, nil); err == nil && len(res.Messages) == 1 {
			msg = *res.Messages[0].MessageText
		}
	}
	raw, err := base64.StdEncoding.DecodeString(msg)
	if err != nil || !strings.Contains(string(raw), `"eventType":"t"`) {
		t.Fatalf("storage queue message = %q (%v)", msg, err)
	}
}

func TestDomains(t *testing.T) {
	e := setup(t)
	if err := e.eg.CreateDomain("shop", ""); err != nil {
		t.Fatal(err)
	}
	all, one := webhook(t, false, ok), webhook(t, false, ok)
	if err := e.eg.CreateSubscription("domains/shop", webhookSub("all", all.URL)); err != nil {
		t.Fatal(err)
	}
	if err := e.eg.CreateSubscription("domains/shop/topics/books", webhookSub("one", one.URL)); err != nil { // creates the domain topic
		t.Fatal(err)
	}
	pub := func(topic string) {
		res := must(http.Post(e.srv.URL+"/_azure/eventgrid/shop/api/events", "application/json", strings.NewReader(
			`[{"id":"`+topic+`","topic":"`+topic+`","subject":"s","eventType":"t","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1"}]`)))
		if res.StatusCode != 200 {
			t.Fatalf("publish = %d", res.StatusCode)
		}
	}
	pub("books")
	pub("toys")
	if d := all.next(t); !strings.Contains(string(d.body), "/domains/shop/topics/books") {
		t.Fatalf("domain event = %s", d.body)
	}
	all.next(t)
	one.next(t)
	one.none(t) // toys is somebody else's topic
	res := must(http.Post(e.srv.URL+"/_azure/eventgrid/shop/api/events", "application/json", strings.NewReader(
		`[{"id":"x","subject":"s","eventType":"t","eventTime":"2026-01-01T00:00:00Z","dataVersion":"1"}]`)))
	if res.StatusCode != 400 {
		t.Fatalf("a domain event without a topic = %d", res.StatusCode)
	}
	if err := e.eg.DeleteDomain("shop"); err != nil {
		t.Fatal(err)
	}
	if e.eg.Subscriptions("domains/shop") != nil || e.eg.Subscriptions("domains/shop/topics/books") != nil {
		t.Fatal("deleting a domain left its subscriptions")
	}
}

func TestSeedStateAndConsole(t *testing.T) {
	e := setup(t)
	hook := webhook(t, false, ok)
	// Seeded webhooks are not validated: this endpoint is not running.
	yaml := `
azure:
  eventgrid:
    topics:
      orders: {inputSchema: EventGridSchema}
    systemTopics:
      storage: {source: ` + account + `}
    subscriptions:
      to-web: {topic: orders, endpoint: ` + hook.URL + `, eventTypes: [Order.Placed], maxAttempts: 5}
      refused: {topic: orders, endpoint: http://127.0.0.1:1/never}
      blobs: {topic: systemTopics/storage, storageQueue: /subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/devstoreaccount1/queueServices/default/queues/q}
`
	if err := e.srv.Hub.Seed([]byte(yaml)); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.Hub.Seed([]byte(yaml)); err != nil { // again: what exists is skipped
		t.Fatal(err)
	}
	subs := e.eg.Subscriptions("topics/orders")
	if len(subs) != 2 || subs[1].Name != "to-web" || subs[1].Retry.MaxAttempts != 5 || subs[1].Filter.IncludedEventTypes[0] != "Order.Placed" {
		t.Fatalf("seeded subscriptions = %+v", subs)
	}

	rows := map[string]int{}
	for _, tb := range e.eg.Resources() {
		rows[tb.Name] = len(tb.Rows)
	}
	if rows["Topics"] != 1 || rows["System topics"] != 1 || rows["Subscriptions"] != 3 {
		t.Fatalf("console tables = %v", rows)
	}
	if got := e.eg.Inspect("Subscriptions", "topics/orders/eventSubscriptions/to-web"); got == nil {
		t.Fatal("Inspect found no subscription")
	}

	// Save, reset and load bring everything back.
	saved := must(e.eg.Save())
	e.srv.Reset("eventgrid")
	if e.eg.Subscriptions("topics/orders") != nil { // the seed ran again after the reset, but without a file there is none
		t.Fatal("Reset kept subscriptions")
	}
	if err := e.eg.Load(saved); err != nil {
		t.Fatal(err)
	}
	if got := e.eg.Subscriptions("topics/orders"); len(got) != 2 {
		t.Fatalf("after Load: %+v", got)
	}
	if err := e.eg.DeleteSubscription("topics/orders", "refused"); err != nil {
		t.Fatal(err)
	}
	if err := e.eg.DeleteTopic("orders"); err != nil {
		t.Fatal(err)
	}
	if e.eg.Subscriptions("topics/orders") != nil || core.AsError(e.eg.DeleteTopic("orders")).Status != 404 {
		t.Fatal("DeleteTopic left subscriptions, or deleted twice")
	}
}
