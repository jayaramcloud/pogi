package eventarc_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	eventarc "cloud.google.com/go/eventarc/apiv1"
	pb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	ps "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	gcs "cloud.google.com/go/storage"
	"github.com/nrndev/pogi/internal/core"
	ea "github.com/nrndev/pogi/internal/gcp/eventarc"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/pubsub"
	gcsvc "github.com/nrndev/pogi/internal/gcp/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

const parent = "projects/proj/locations/us-central1"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

type env struct {
	hub *core.Hub
	svc *ea.Service
	c   *eventarc.Client
	url string
}

func setup(t *testing.T) *env {
	t.Helper()
	e := &env{}
	e.hub, e.url = core.ServeTest(t,
		func(h *core.Hub) core.Service { return gapi.NewOps(h) },
		func(h *core.Hub) core.Service { return pubsub.New(h) },
		func(h *core.Hub) core.Service { return gcsvc.New(h) },
		func(h *core.Hub) core.Service { e.svc = ea.New(h); return e.svc })
	e.c = must(eventarc.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(e.url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { e.c.Close() })
	t.Setenv("STORAGE_EMULATOR_HOST", e.url)
	t.Setenv("PUBSUB_EMULATOR_HOST", strings.TrimPrefix(e.url, "http://"))
	return e
}

// delivered is one message a destination service received through the hub.
type delivered struct {
	target string
	msg    *core.Message
}

// capture stands in for a destination service ("run", "cloudfunctions") that is built elsewhere.
type capture struct {
	mu   sync.Mutex
	list []delivered
}

func (e *env) capture(service string) *capture {
	c := &capture{}
	e.hub.OnDeliver(service, func(arn string, m *core.Message) error {
		c.mu.Lock()
		c.list = append(c.list, delivered{arn, m})
		c.mu.Unlock()
		return nil
	})
	return c
}

func (c *capture) all() []delivered {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]delivered(nil), c.list...)
}

// wait returns the nth delivery (1-based), failing if it does not arrive.
func (c *capture) wait(t *testing.T, n int) delivered {
	t.Helper()
	for i := 0; i < 500; i++ {
		if l := c.all(); len(l) >= n {
			return l[n-1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for delivery %d (have %d)", n, len(c.all()))
	return delivered{}
}

// quiet fails if anything arrives beyond n deliveries.
func (c *capture) quiet(t *testing.T, n int) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	if l := c.all(); len(l) != n {
		t.Fatalf("got %d deliveries, want %d: %+v", len(l), n, l)
	}
}

func attr(m *core.Message, k string) string { return m.Attributes[k].StringValue }

func filters(kv ...string) []*pb.EventFilter {
	var out []*pb.EventFilter
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, &pb.EventFilter{Attribute: kv[i], Value: kv[i+1]})
	}
	return out
}

func toRun(service, path string) *pb.Destination {
	return &pb.Destination{Descriptor_: &pb.Destination_CloudRun{CloudRun: &pb.CloudRun{Service: service, Region: "us-central1", Path: path}}}
}

func create(t *testing.T, e *env, id string, tr *pb.Trigger) *pb.Trigger {
	t.Helper()
	op := must(e.c.CreateTrigger(ctx, &pb.CreateTriggerRequest{Parent: parent, TriggerId: id, Trigger: tr}))
	return must(op.Wait(ctx))
}

func TestStorageEventsReachCloudRun(t *testing.T) {
	e := setup(t)
	run := e.capture("run")
	tr := create(t, e, "on-upload", &pb.Trigger{
		EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "uploads"),
		Destination:  toRun("thumbnailer", "/events"), ServiceAccount: "sa@proj.iam.gserviceaccount.com"})
	if tr.Name != parent+"/triggers/on-upload" || tr.Uid == "" || tr.CreateTime == nil || tr.EventDataContentType != "application/json" {
		t.Fatalf("trigger = %v", tr)
	}

	sc := must(gcs.NewClient(ctx))
	defer sc.Close()
	for _, b := range []string{"uploads", "other"} {
		if err := sc.Bucket(b).Create(ctx, "proj", nil); err != nil {
			t.Fatal(err)
		}
	}
	w := sc.Bucket("uploads").Object("photos/cat.txt").NewWriter(ctx)
	w.ContentType = "text/plain"
	w.Write([]byte("meow"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	d := run.wait(t, 1)
	if d.target != "projects/proj/locations/us-central1/services/thumbnailer" {
		t.Fatalf("target = %q", d.target)
	}
	m := d.msg
	if m.Subject != "/events" {
		t.Fatalf("path = %q", m.Subject)
	}
	for k, want := range map[string]string{"ce-specversion": "1.0", "ce-type": "google.cloud.storage.object.v1.finalized",
		"ce-source": "//storage.googleapis.com/projects/_/buckets/uploads", "ce-subject": "objects/photos/cat.txt",
		"content-type": "application/json", "ce-bucket": "uploads"} {
		if got := attr(m, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if attr(m, "ce-id") == "" || must(time.Parse(time.RFC3339Nano, attr(m, "ce-time"))).IsZero() {
		t.Errorf("ce-id %q ce-time %q", attr(m, "ce-id"), attr(m, "ce-time"))
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(m.Body), &obj); err != nil || obj["name"] != "photos/cat.txt" || obj["bucket"] != "uploads" ||
		obj["contentType"] != "text/plain" || obj["size"] != "4" || obj["generation"] == nil {
		t.Fatalf("data = %s (%v)", m.Body, err)
	}

	// Other buckets and other event types do not match this trigger.
	w = sc.Bucket("other").Object("x.txt").NewWriter(ctx)
	w.Write([]byte("x"))
	w.Close()
	must(0, sc.Bucket("uploads").Object("photos/cat.txt").Delete(ctx))
	run.quiet(t, 1)

	// A deleted trigger stops delivering.
	must(e.c.DeleteTrigger(ctx, &pb.DeleteTriggerRequest{Name: tr.Name}))
	w = sc.Bucket("uploads").Object("again.txt").NewWriter(ctx)
	w.Write([]byte("x"))
	w.Close()
	run.quiet(t, 1)
}

func TestStorageEventTypes(t *testing.T) {
	e := setup(t)
	run := e.capture("run")
	create(t, e, "deletes", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.deleted", "bucket", "bkt"), Destination: toRun("s", "")})
	create(t, e, "meta", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.metadataUpdated", "bucket", "bkt"), Destination: toRun("s", "/meta")})
	sc := must(gcs.NewClient(ctx))
	defer sc.Close()
	bkt := sc.Bucket("bkt")
	if err := bkt.Create(ctx, "proj", nil); err != nil {
		t.Fatal(err)
	}
	obj := bkt.Object("o.txt")
	w := obj.NewWriter(ctx)
	w.Write([]byte("v1"))
	w.Close()
	run.quiet(t, 0) // finalized has no trigger
	must(obj.Update(ctx, gcs.ObjectAttrsToUpdate{Metadata: map[string]string{"k": "v"}}))
	d := run.wait(t, 1)
	if attr(d.msg, "ce-type") != "google.cloud.storage.object.v1.metadataUpdated" || d.msg.Subject != "/meta" {
		t.Fatalf("metadata event = %v %q", d.msg.Attributes, d.msg.Subject)
	}
	must(0, obj.Delete(ctx))
	d = run.wait(t, 2)
	if attr(d.msg, "ce-type") != "google.cloud.storage.object.v1.deleted" || d.msg.Subject != "/" {
		t.Fatalf("delete event = %v %q", d.msg.Attributes, d.msg.Subject)
	}
}

func TestPubsubTriggerDeliversMessagePublished(t *testing.T) {
	e := setup(t)
	run := e.capture("run")
	pc := must(ps.NewClient(ctx, "proj"))
	defer pc.Close()

	// With a transport topic that does not exist yet: Eventarc creates it, plus a push subscription.
	tr := create(t, e, "on-msg", &pb.Trigger{EventFilters: filters("type", "google.cloud.pubsub.topic.v1.messagePublished"),
		Destination: toRun("worker", "/pubsub"), Transport: &pb.Transport{Intermediary: &pb.Transport_Pubsub{Pubsub: &pb.Pubsub{Topic: "projects/proj/topics/orders"}}}})
	topic, sub := tr.Transport.GetPubsub().Topic, tr.Transport.GetPubsub().Subscription
	if topic != "projects/proj/topics/orders" || !strings.HasPrefix(sub, "projects/proj/subscriptions/eventarc-us-central1-on-msg") {
		t.Fatalf("transport = %v", tr.Transport)
	}
	got := must(pc.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub}))
	if got.Topic != topic || !strings.HasSuffix(got.PushConfig.GetPushEndpoint(), "/_pogi/eventarc/push/"+tr.Name) {
		t.Fatalf("subscription = %v", got)
	}

	pub := pc.Publisher(topic)
	defer pub.Stop()
	id := must(pub.Publish(ctx, &ps.Message{Data: []byte("hello"), Attributes: map[string]string{"k": "v"}}).Get(ctx))

	d := run.wait(t, 1)
	if d.target != "projects/proj/locations/us-central1/services/worker" || d.msg.Subject != "/pubsub" {
		t.Fatalf("target %q path %q", d.target, d.msg.Subject)
	}
	for k, want := range map[string]string{"ce-specversion": "1.0", "ce-type": "google.cloud.pubsub.topic.v1.messagePublished",
		"ce-source": "//pubsub.googleapis.com/" + topic, "ce-id": id, "content-type": "application/json"} {
		if got := attr(d.msg, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	var data struct {
		Message struct {
			Data       string
			Attributes map[string]string
			MessageID  string `json:"messageId"`
		}
		Subscription string
	}
	if err := json.Unmarshal([]byte(d.msg.Body), &data); err != nil {
		t.Fatal(err)
	}
	if b, _ := base64.StdEncoding.DecodeString(data.Message.Data); string(b) != "hello" || data.Message.Attributes["k"] != "v" ||
		data.Message.MessageID != id || data.Subscription != sub {
		t.Fatalf("data = %s", d.msg.Body)
	}

	// Without a transport topic Eventarc makes one, and deleting the trigger removes it and the subscription.
	auto := create(t, e, "auto-topic", &pb.Trigger{EventFilters: filters("type", "google.cloud.pubsub.topic.v1.messagePublished"), Destination: toRun("w", "")})
	autoTopic := auto.Transport.GetPubsub().Topic
	must(pc.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: autoTopic}))
	must(e.c.DeleteTrigger(ctx, &pb.DeleteTriggerRequest{Name: auto.Name}))
	_, err := pc.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: autoTopic})
	wantCode(t, err, codes.NotFound)
	_, err = pc.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: auto.Transport.GetPubsub().Subscription})
	wantCode(t, err, codes.NotFound)
}

func TestHTTPEndpointAndFunctionDestinations(t *testing.T) {
	e := setup(t)
	fn := e.capture("cloudfunctions")
	var mu sync.Mutex
	var gotHdr http.Header
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotHdr, gotBody = r.Header.Clone(), string(b)
		mu.Unlock()
	}))
	defer srv.Close()
	create(t, e, "to-http", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "bkt"),
		Destination: &pb.Destination{Descriptor_: &pb.Destination_HttpEndpoint{HttpEndpoint: &pb.HttpEndpoint{Uri: srv.URL + "/hook"}}}})
	create(t, e, "to-fn", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "bkt"),
		Destination: &pb.Destination{Descriptor_: &pb.Destination_CloudFunction{CloudFunction: "projects/proj/locations/us-central1/functions/handler"}}})
	// Workflow destinations are stored.
	wf := create(t, e, "to-wf", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "bkt"),
		Destination: &pb.Destination{Descriptor_: &pb.Destination_Workflow{Workflow: "projects/proj/locations/us-central1/workflows/w"}}})
	if wf.Destination.GetWorkflow() == "" {
		t.Fatalf("workflow destination lost: %v", wf)
	}

	sc := must(gcs.NewClient(ctx))
	defer sc.Close()
	if err := sc.Bucket("bkt").Create(ctx, "proj", nil); err != nil {
		t.Fatal(err)
	}
	w := sc.Bucket("bkt").Object("f.txt").NewWriter(ctx)
	w.Write([]byte("data"))
	w.Close()

	d := fn.wait(t, 1)
	if d.target != "projects/proj/locations/us-central1/functions/handler" || attr(d.msg, "ce-type") != "google.cloud.storage.object.v1.finalized" {
		t.Fatalf("function delivery = %q %v", d.target, d.msg.Attributes)
	}
	for i := 0; ; i++ {
		mu.Lock()
		hdr, body := gotHdr, gotBody
		mu.Unlock()
		if hdr != nil {
			if hdr.Get("Ce-Type") != "google.cloud.storage.object.v1.finalized" || hdr.Get("Ce-Subject") != "objects/f.txt" ||
				hdr.Get("Ce-Specversion") != "1.0" || hdr.Get("Content-Type") != "application/json" || !strings.Contains(body, `"name":"f.txt"`) {
				t.Fatalf("http delivery: %v %s", hdr, body)
			}
			break
		}
		if i > 300 {
			t.Fatal("the HTTP endpoint never got the event")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAuditLogTriggerIsStoredNotFired(t *testing.T) {
	e := setup(t)
	run := e.capture("run")
	tr := create(t, e, "audit", &pb.Trigger{
		EventFilters: filters("type", "google.cloud.audit.log.v1.written", "serviceName", "storage.googleapis.com", "methodName", "storage.objects.create"),
		Destination:  toRun("s", "")})
	if len(tr.EventFilters) != 3 {
		t.Fatalf("filters = %v", tr.EventFilters)
	}
	sc := must(gcs.NewClient(ctx))
	defer sc.Close()
	sc.Bucket("bkt").Create(ctx, "proj", nil)
	w := sc.Bucket("bkt").Object("x").NewWriter(ctx)
	w.Write([]byte("x"))
	w.Close()
	run.quiet(t, 0)
}

func TestTriggerCRUD(t *testing.T) {
	e := setup(t)
	base := &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "bkt"), Destination: toRun("s", "/a")}
	_, err := e.c.CreateTrigger(ctx, &pb.CreateTriggerRequest{Parent: parent, TriggerId: "no-type", Trigger: &pb.Trigger{Destination: toRun("s", "")}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = e.c.CreateTrigger(ctx, &pb.CreateTriggerRequest{Parent: parent, TriggerId: "no-dest", Trigger: &pb.Trigger{EventFilters: base.EventFilters}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = e.c.CreateTrigger(ctx, &pb.CreateTriggerRequest{Parent: parent, TriggerId: "Bad_ID", Trigger: base})
	wantCode(t, err, codes.InvalidArgument)
	create(t, e, "one", base)
	_, err = e.c.CreateTrigger(ctx, &pb.CreateTriggerRequest{Parent: parent, TriggerId: "one", Trigger: base})
	wantCode(t, err, codes.AlreadyExists)
	create(t, e, "two", base)

	op := must(e.c.UpdateTrigger(ctx, &pb.UpdateTriggerRequest{
		Trigger:    &pb.Trigger{Name: parent + "/triggers/one", Destination: toRun("s", "/b"), Labels: map[string]string{"x": "y"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"destination"}}}))
	up := must(op.Wait(ctx))
	if up.Destination.GetCloudRun().Path != "/b" || len(up.Labels) != 0 || up.UpdateTime == nil || up.Etag == "" {
		t.Fatalf("update = %v", up)
	}
	if g := must(e.c.GetTrigger(ctx, &pb.GetTriggerRequest{Name: up.Name})); g.Destination.GetCloudRun().Path != "/b" {
		t.Fatalf("get = %v", g)
	}
	_, err = e.c.GetTrigger(ctx, &pb.GetTriggerRequest{Name: parent + "/triggers/missing"})
	wantCode(t, err, codes.NotFound)

	var names []string
	for it := e.c.ListTriggers(ctx, &pb.ListTriggersRequest{Parent: parent}); ; {
		x, err := it.Next()
		if err == iterator.Done {
			break
		}
		names = append(names, must(x, err).Name)
	}
	if len(names) != 2 || !strings.HasSuffix(names[0], "one") {
		t.Fatalf("list = %v", names)
	}
	del := must(e.c.DeleteTrigger(ctx, &pb.DeleteTriggerRequest{Name: up.Name}))
	if r := must(del.Wait(ctx)); r.Name != up.Name {
		t.Fatalf("delete result = %v", r)
	}
	_, err = e.c.DeleteTrigger(ctx, &pb.DeleteTriggerRequest{Name: up.Name})
	wantCode(t, err, codes.NotFound)
	must(e.c.DeleteTrigger(ctx, &pb.DeleteTriggerRequest{Name: up.Name, AllowMissing: true}))
}

func TestChannelsAndProviders(t *testing.T) {
	e := setup(t)
	op := must(e.c.CreateChannel(ctx, &pb.CreateChannelRequest{Parent: parent, ChannelId: "partner", Channel: &pb.Channel{Provider: parent + "/providers/acme"}}))
	ch := must(op.Wait(ctx))
	if ch.Name != parent+"/channels/partner" || ch.State != pb.Channel_PENDING || ch.ActivationToken == "" || ch.GetPubsubTopic() == "" {
		t.Fatalf("channel = %v", ch)
	}
	cc := must(must(e.c.CreateChannelConnection(ctx, &pb.CreateChannelConnectionRequest{Parent: parent, ChannelConnectionId: "conn",
		ChannelConnection: &pb.ChannelConnection{Channel: ch.Name}})).Wait(ctx))
	if g := must(e.c.GetChannelConnection(ctx, &pb.GetChannelConnectionRequest{Name: cc.Name})); g.Channel != ch.Name {
		t.Fatalf("connection = %v", g)
	}
	must(must(e.c.DeleteChannel(ctx, &pb.DeleteChannelRequest{Name: ch.Name})).Wait(ctx))
	_, err := e.c.GetChannel(ctx, &pb.GetChannelRequest{Name: ch.Name})
	wantCode(t, err, codes.NotFound)

	var types []string
	for it := e.c.ListProviders(ctx, &pb.ListProvidersRequest{Parent: parent}); ; {
		p, err := it.Next()
		if err == iterator.Done {
			break
		}
		for _, et := range must(p, err).EventTypes {
			types = append(types, et.Type)
		}
	}
	if len(types) != 5 || !strings.Contains(strings.Join(types, " "), "google.cloud.storage.object.v1.finalized") {
		t.Fatalf("provider event types = %v", types)
	}
	p := must(e.c.GetProvider(ctx, &pb.GetProviderRequest{Name: parent + "/providers/storage.googleapis.com"}))
	if p.DisplayName != "Cloud Storage" {
		t.Fatalf("provider = %v", p)
	}
}

func TestREST(t *testing.T) {
	e := setup(t)
	run := e.capture("run")
	do := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, e.url+path, strings.NewReader(body))
		resp := must(http.DefaultClient.Do(req))
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, op := do("POST", "/v1/"+parent+"/triggers?triggerId=rest-trigger", `{"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},`+
		`{"attribute":"bucket","value":"rest-bucket"}],"destination":{"cloudRun":{"service":"rest-svc","region":"us-central1"}}}`)
	if code != 200 || op["done"] != true || !strings.Contains(op["name"].(string), parent+"/operations/") {
		t.Fatalf("create: %d %v", code, op)
	}
	resp := op["response"].(map[string]any)
	if resp["name"] != parent+"/triggers/rest-trigger" || resp["uid"] == nil {
		t.Fatalf("response = %v", resp)
	}
	if code, o := do("GET", "/v1/"+op["name"].(string), ""); code != 200 || o["done"] != true {
		t.Fatalf("operation: %d %v", code, o)
	}
	if code, g := do("GET", "/v1/"+parent+"/triggers/rest-trigger", ""); code != 200 || g["name"] != resp["name"] {
		t.Fatalf("get: %d %v", code, g)
	}
	if code, l := do("GET", "/v1/"+parent+"/triggers", ""); code != 200 || len(l["triggers"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, l)
	}
	if code, er := do("POST", "/v1/"+parent+"/triggers?triggerId=rest-trigger", `{"eventFilters":[{"attribute":"type","value":"x"}],"destination":{"cloudRun":{"service":"s"}}}`); code != 409 ||
		er["error"].(map[string]any)["status"] != "ALREADY_EXISTS" {
		t.Fatalf("duplicate: %d %v", code, er)
	}
	if code, l := do("GET", "/v1/"+parent+"/providers", ""); code != 200 || len(l["providers"].([]any)) != 2 {
		t.Fatalf("providers: %d %v", code, l)
	}
	sc := must(gcs.NewClient(ctx))
	defer sc.Close()
	sc.Bucket("rest-bucket").Create(ctx, "proj", nil)
	w := sc.Bucket("rest-bucket").Object("r.txt").NewWriter(ctx)
	w.Write([]byte("r"))
	w.Close()
	if d := run.wait(t, 1); d.target != "projects/proj/locations/us-central1/services/rest-svc" {
		t.Fatalf("target = %q", d.target)
	}
	if code, _ := do("DELETE", "/v1/"+parent+"/triggers/rest-trigger", ""); code != 200 {
		t.Fatalf("delete: %d", code)
	}
}

func TestPersistence(t *testing.T) {
	e := setup(t)
	tr := create(t, e, "keep", &pb.Trigger{EventFilters: filters("type", "google.cloud.storage.object.v1.finalized", "bucket", "bkt"), Destination: toRun("s", "/k")})
	b := must(e.svc.Save())
	e.svc.Reset()
	_, err := e.c.GetTrigger(ctx, &pb.GetTriggerRequest{Name: tr.Name})
	wantCode(t, err, codes.NotFound)
	if err := e.svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if g := must(e.c.GetTrigger(ctx, &pb.GetTriggerRequest{Name: tr.Name})); g.Destination.GetCloudRun().Path != "/k" || g.Uid != tr.Uid {
		t.Fatalf("loaded = %v", g)
	}
	if rows := e.svc.Resources(); len(rows) != 2 || len(rows[0].Rows) != 1 || e.svc.Inspect("Triggers", tr.Name) == nil {
		t.Fatalf("resources = %v", rows)
	}
}
