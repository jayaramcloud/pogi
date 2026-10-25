package servicebus_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/azure/servicebus"
	"github.com/nrndev/pogi/internal/core"
)

var bg = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// adminClient is the official admin client dialling pogi's main port as the SDK does with
// UseDevelopmentEmulator=true: https://<host:port>/ (AMQP is the part that goes without TLS).
func adminClient(t *testing.T) (*admin.Client, *servicebus.Service) {
	t.Helper()
	h := core.NewHub(core.Config{DataDir: t.TempDir()}, nil)
	svc := servicebus.New(h)
	h.Register(svc)
	t.Cleanup(h.Close)
	u, pool := core.ListenTLS(t, h, h)
	conn := "Endpoint=sb://" + strings.TrimPrefix(u, "https://") + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c2VjcmV0;UseDevelopmentEmulator=true"
	c, err := admin.NewClientFromConnectionString(conn, &admin.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return c, svc
}

func TestAdminQueues(t *testing.T) {
	c, svc := adminClient(t)

	ns := must(c.GetNamespaceProperties(bg, nil))
	if ns.Name != "localhost" || ns.SKU != "Standard" || ns.CreatedTime.IsZero() {
		t.Fatalf("namespace = %+v", ns)
	}

	created := must(c.CreateQueue(bg, "orders", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{
		LockDuration:                     to.Ptr("PT30S"),
		MaxDeliveryCount:                 to.Ptr(int32(3)),
		RequiresSession:                  to.Ptr(true),
		DeadLetteringOnMessageExpiration: to.Ptr(true),
		DefaultMessageTimeToLive:         to.Ptr("PT1H"),
		RequiresDuplicateDetection:       to.Ptr(true),
		UserMetadata:                     to.Ptr("meta"),
		AutoDeleteOnIdle:                 to.Ptr("P1D"),
	}}))
	p := created.QueueProperties
	if *p.LockDuration != "PT30S" || *p.MaxDeliveryCount != 3 || !*p.RequiresSession || !*p.DeadLetteringOnMessageExpiration ||
		*p.DefaultMessageTimeToLive != "PT1H" || !*p.RequiresDuplicateDetection || *p.UserMetadata != "meta" || *p.AutoDeleteOnIdle != "P1D" ||
		*p.Status != admin.EntityStatusActive {
		t.Fatalf("created = %+v", p)
	}
	// the engine has the settings, not just the echo
	if e, ok := svc.GetQueue("localhost", "orders"); !ok || time.Duration(e.LockDuration) != 30*time.Second || e.MaxDeliveryCount != 3 || !e.RequiresSession {
		t.Fatalf("engine queue = %+v", e)
	}

	// defaults, and the conflict on a second create
	plain := must(c.CreateQueue(bg, "plain", nil))
	if *plain.LockDuration != "PT1M" || *plain.MaxDeliveryCount != 10 || *plain.MaxSizeInMegabytes != 1024 || *plain.DuplicateDetectionHistoryTimeWindow != "PT10M" {
		t.Fatalf("defaults = %+v", plain.QueueProperties)
	}
	if _, err := c.CreateQueue(bg, "plain", nil); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("duplicate create = %v", err)
	}

	got := must(c.GetQueue(bg, "orders", nil))
	if got == nil || *got.MaxDeliveryCount != 3 {
		t.Fatalf("get = %+v", got)
	}
	if missing := must(c.GetQueue(bg, "nope", nil)); missing != nil {
		t.Fatalf("missing queue = %+v", missing)
	}

	// update changes what can change and keeps what cannot
	upd := p
	upd.MaxDeliveryCount = to.Ptr(int32(7))
	upd.RequiresSession = to.Ptr(false)
	up := must(c.UpdateQueue(bg, "orders", upd, nil))
	if *up.MaxDeliveryCount != 7 || !*up.RequiresSession {
		t.Fatalf("updated = %+v", up.QueueProperties)
	}
	if _, err := c.UpdateQueue(bg, "nope", upd, nil); err == nil {
		t.Fatal("updated a missing queue")
	}

	// runtime properties follow the engine's messages
	m := &amqp.Message{Data: [][]byte{[]byte("hello")}, Properties: &amqp.Properties{MessageID: "1"}}
	for range 3 {
		check(svc.Send("localhost", "plain", m))
	}
	rt := must(c.GetQueueRuntimeProperties(bg, "plain", nil))
	if rt.ActiveMessageCount != 3 || rt.TotalMessageCount != 3 || rt.SizeInBytes != 15 || rt.DeadLetterMessageCount != 0 || rt.CreatedAt.IsZero() {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}

	// listing pages by name
	var names []string
	for pg := c.NewListQueuesPager(&admin.ListQueuesOptions{MaxPageSize: 1}); pg.More(); {
		for _, q := range must(pg.NextPage(bg)).Queues {
			names = append(names, q.QueueName)
		}
	}
	if strings.Join(names, ",") != "orders,plain" {
		t.Fatalf("queues = %v", names)
	}
	var counts []int32
	for pg := c.NewListQueuesRuntimePropertiesPager(nil); pg.More(); {
		for _, q := range must(pg.NextPage(bg)).QueueRuntimeProperties {
			counts = append(counts, q.ActiveMessageCount)
		}
	}
	if len(counts) != 2 || counts[1] != 3 {
		t.Fatalf("runtime list = %v", counts)
	}

	must(c.DeleteQueue(bg, "plain", nil))
	if g := must(c.GetQueue(bg, "plain", nil)); g != nil {
		t.Fatal("queue survived its deletion")
	}
	if _, err := c.DeleteQueue(bg, "plain", nil); err == nil {
		t.Fatal("deleted a missing queue")
	}

	// forwarding is stored
	must(c.CreateQueue(bg, "sink", nil))
	must(c.CreateQueue(bg, "fwd", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{
		ForwardTo: to.Ptr("https://localhost/sink"), ForwardDeadLetteredMessagesTo: to.Ptr("sink")}}))
	check(svc.Send("localhost", "fwd", m))
	if e, _ := svc.GetQueue("localhost", "sink"); e.Active != 1 {
		t.Fatalf("forwarded to sink = %+v", e)
	}
}

func TestAdminTopicsSubscriptionsRules(t *testing.T) {
	c, svc := adminClient(t)

	top := must(c.CreateTopic(bg, "events", &admin.CreateTopicOptions{Properties: &admin.TopicProperties{
		DefaultMessageTimeToLive: to.Ptr("PT2H"), RequiresDuplicateDetection: to.Ptr(true), SupportOrdering: to.Ptr(true)}}))
	if *top.DefaultMessageTimeToLive != "PT2H" || !*top.RequiresDuplicateDetection {
		t.Fatalf("topic = %+v", top.TopicProperties)
	}
	if _, err := c.CreateTopic(bg, "events", nil); err == nil {
		t.Fatal("duplicate topic")
	}
	if g := must(c.GetTopic(bg, "events", nil)); g == nil || !*g.SupportOrdering {
		t.Fatalf("get topic = %+v", g)
	}
	upd := must(c.UpdateTopic(bg, "events", admin.TopicProperties{DefaultMessageTimeToLive: to.Ptr("PT3H")}, nil))
	if *upd.DefaultMessageTimeToLive != "PT3H" {
		t.Fatalf("update topic = %+v", upd.TopicProperties)
	}

	// subscriptions, with a default rule
	must(c.CreateSubscription(bg, "events", "audit", nil))
	must(c.CreateSubscription(bg, "events", "big", &admin.CreateSubscriptionOptions{Properties: &admin.SubscriptionProperties{
		LockDuration: to.Ptr("PT45S"), MaxDeliveryCount: to.Ptr(int32(2)), RequiresSession: to.Ptr(false),
		DefaultRule: &admin.RuleProperties{Name: "big", Filter: &admin.SQLFilter{Expression: "price > @min", Parameters: map[string]any{"min": int64(100)}},
			Action: &admin.SQLAction{Expression: "SET sys.Label = 'big'"}}}}))
	if _, err := c.CreateSubscription(bg, "events", "audit", nil); err == nil {
		t.Fatal("duplicate subscription")
	}
	if _, err := c.CreateSubscription(bg, "nope", "s", nil); err == nil {
		t.Fatal("subscription on a missing topic")
	}
	if g := must(c.GetSubscription(bg, "events", "big", nil)); g == nil || *g.LockDuration != "PT45S" || *g.MaxDeliveryCount != 2 {
		t.Fatalf("get subscription = %+v", g)
	}
	if g := must(c.GetSubscription(bg, "events", "none", nil)); g != nil {
		t.Fatal("missing subscription found")
	}
	us := must(c.UpdateSubscription(bg, "events", "big", admin.SubscriptionProperties{MaxDeliveryCount: to.Ptr(int32(5))}, nil))
	if *us.MaxDeliveryCount != 5 {
		t.Fatalf("updated subscription = %+v", us.SubscriptionProperties)
	}

	// rules: every filter and action kind, create, get, update, list, delete
	must(c.CreateRule(bg, "events", "audit", &admin.CreateRuleOptions{Name: to.Ptr("corr"), Filter: &admin.CorrelationFilter{
		CorrelationID: to.Ptr("c1"), Subject: to.Ptr("subj"), ApplicationProperties: map[string]any{"region": "eu", "n": int64(3), "ok": true}}}))
	must(c.CreateRule(bg, "events", "audit", &admin.CreateRuleOptions{Name: to.Ptr("false"), Filter: &admin.FalseFilter{}}))
	r := must(c.CreateRule(bg, "events", "audit", &admin.CreateRuleOptions{Name: to.Ptr("sql"),
		Filter: &admin.SQLFilter{Expression: "a = 'x' AND b IN ('1','2') AND c > @c", Parameters: map[string]any{"c": 2.5}},
		Action: &admin.SQLAction{Expression: "SET d = 1; REMOVE e"}}))
	if f, ok := r.Filter.(*admin.SQLFilter); !ok || f.Parameters["c"] != 2.5 || r.Action.(*admin.SQLAction).Expression != "SET d = 1; REMOVE e" {
		t.Fatalf("created rule = %+v", r)
	}
	if _, err := c.CreateRule(bg, "events", "audit", &admin.CreateRuleOptions{Name: to.Ptr("sql"), Filter: &admin.TrueFilter{}}); err == nil {
		t.Fatal("duplicate rule")
	}
	if _, err := c.CreateRule(bg, "events", "audit", &admin.CreateRuleOptions{Name: to.Ptr("bad"), Filter: &admin.SQLFilter{Expression: "a >"}}); err == nil {
		t.Fatal("invalid SQL accepted")
	}
	corr := must(c.GetRule(bg, "events", "audit", "corr", nil))
	cf, ok := corr.Filter.(*admin.CorrelationFilter)
	if !ok || *cf.CorrelationID != "c1" || *cf.Subject != "subj" || cf.ApplicationProperties["region"] != "eu" || cf.ApplicationProperties["n"] != int64(3) || cf.ApplicationProperties["ok"] != true {
		t.Fatalf("correlation rule = %+v %+v", corr, cf)
	}
	if _, ok := must(c.GetRule(bg, "events", "audit", "false", nil)).Filter.(*admin.FalseFilter); !ok {
		t.Fatal("false rule")
	}
	if g := must(c.GetRule(bg, "events", "audit", "gone", nil)); g != nil {
		t.Fatal("missing rule found")
	}
	var rules []string
	for pg := c.NewListRulesPager("events", "audit", &admin.ListRulesOptions{MaxPageSize: 2}); pg.More(); {
		for _, r := range must(pg.NextPage(bg)).Rules {
			rules = append(rules, r.Name)
		}
	}
	if strings.Join(rules, ",") != "$Default,corr,false,sql" {
		t.Fatalf("rules = %v", rules)
	}
	must(c.UpdateRule(bg, "events", "audit", admin.RuleProperties{Name: "sql", Filter: &admin.TrueFilter{}}))
	if _, ok := must(c.GetRule(bg, "events", "audit", "sql", nil)).Filter.(*admin.TrueFilter); !ok {
		t.Fatal("updated rule")
	}
	must(c.DeleteRule(bg, "events", "audit", "false", nil))
	must(c.DeleteRule(bg, "events", "audit", "sql", nil))
	if rs, _ := svc.ListRules("localhost", "events", "audit"); len(rs) != 2 {
		t.Fatalf("engine rules = %+v", rs)
	}

	// the default rule given at creation, and its action, run on publish
	m := &amqp.Message{Data: [][]byte{[]byte("x")}, Properties: &amqp.Properties{MessageID: "1"}, ApplicationProperties: map[string]any{"price": int64(150)}}
	check(svc.Send("localhost", "events", m))
	small := &amqp.Message{Data: [][]byte{[]byte("y")}, Properties: &amqp.Properties{MessageID: "2"}, ApplicationProperties: map[string]any{"price": int64(5)}}
	check(svc.Send("localhost", "events", small))

	rt := must(c.GetSubscriptionRuntimeProperties(bg, "events", "big", nil))
	if rt.ActiveMessageCount != 1 || rt.TotalMessageCount != 1 {
		t.Fatalf("subscription runtime = %+v", rt.SubscriptionRuntimeProperties)
	}
	trt := must(c.GetTopicRuntimeProperties(bg, "events", nil))
	if trt.SubscriptionCount != 2 || trt.SQLFilterCount != 1 || trt.CorrelationFilterCount != 1 {
		t.Fatalf("topic runtime = %+v", trt.TopicRuntimeProperties)
	}
	var subs []string
	for pg := c.NewListSubscriptionsPager("events", &admin.ListSubscriptionsOptions{MaxPageSize: 1}); pg.More(); {
		for _, s := range must(pg.NextPage(bg)).Subscriptions {
			subs = append(subs, s.SubscriptionName)
		}
	}
	var rts []int32
	for pg := c.NewListSubscriptionsRuntimePropertiesPager("events", nil); pg.More(); {
		for _, s := range must(pg.NextPage(bg)).SubscriptionRuntimeProperties {
			rts = append(rts, s.ActiveMessageCount)
		}
	}
	var topics []string
	for pg := c.NewListTopicsPager(nil); pg.More(); {
		for _, tp := range must(pg.NextPage(bg)).Topics {
			topics = append(topics, tp.TopicName)
		}
	}
	for pg := c.NewListTopicsRuntimePropertiesPager(nil); pg.More(); {
		must(pg.NextPage(bg))
	}
	if strings.Join(subs, ",") != "audit,big" || len(rts) != 2 || rts[0] != 2 || rts[1] != 1 || strings.Join(topics, ",") != "events" {
		t.Fatalf("lists: subs=%v runtimes=%v topics=%v", subs, rts, topics)
	}

	must(c.DeleteSubscription(bg, "events", "audit", nil))
	if g := must(c.GetSubscription(bg, "events", "audit", nil)); g != nil {
		t.Fatal("subscription survived its deletion")
	}
	must(c.DeleteTopic(bg, "events", nil))
	if g := must(c.GetTopic(bg, "events", nil)); g != nil {
		t.Fatal("topic survived its deletion")
	}
}

// TestRouting: the matcher claims Service Bus management traffic and nothing that
// belongs to Azure Storage, Key Vault, Cosmos DB, S3 or the Google APIs.
func TestRouting(t *testing.T) {
	info := servicebus.New(core.NewHub(core.Config{DataDir: t.TempDir()}, nil)).Info()
	req := func(method, target string, h map[string]string) *http.Request {
		r := httptest.NewRequest(method, target, nil)
		for k, v := range h {
			r.Header.Set(k, v)
		}
		return r
	}
	sas := "SharedAccessSignature sr=x&sig=y&se=1&skn=k"
	for name, c := range map[string]struct {
		r    *http.Request
		want bool
	}{
		"list queues":         {req("GET", "/$Resources/Queues?api-version=2024-05", map[string]string{"Authorization": sas}), true},
		"namespace info":      {req("GET", "/$namespaceinfo?api-version=2024-05", nil), true},
		"get queue":           {req("GET", "/orders?api-version=2024-05", map[string]string{"Authorization": sas}), true},
		"create queue":        {req("PUT", "/orders", map[string]string{"Content-Type": "application/atom+xml;type=entry;charset=utf-8"}), true},
		"subscription rules":  {req("GET", "/t/Subscriptions/s/Rules/?api-version=2024-05", map[string]string{"Authorization": sas}), true},
		"host":                {httptest.NewRequest("GET", "http://ns.servicebus.localhost/x", nil), true},
		"blob SharedKey":      {req("GET", "/devstoreaccount1/c?restype=container", map[string]string{"Authorization": "SharedKey acct:sig", "x-ms-version": "2023-11-03"}), false},
		"blob SAS":            {req("GET", "/devstoreaccount1/c/b?sv=2023&sig=abc&api-version=1", nil), false},
		"key vault secrets":   {req("GET", "/secrets/x?api-version=7.4", map[string]string{"Authorization": "Bearer t"}), false},
		"key vault anonymous": {req("GET", "/secrets/x?api-version=7.4", nil), false},
		"cosmos":              {req("GET", "/dbs", map[string]string{"Authorization": "type%3Dmaster%26ver%3D1.0%26sig%3Dx", "x-ms-version": "2018-12-31"}), false},
		"s3":                  {req("GET", "/bucket/key", map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=a/20250101/us-east-1/s3/aws4_request"}), false},
		"google storage":      {req("GET", "/storage/v1/b?project=p", map[string]string{"Authorization": "Bearer t"}), false},
		"aws json":            {req("POST", "/", map[string]string{"X-Amz-Target": "DynamoDB_20120810.ListTables", "Content-Type": "application/x-amz-json-1.0"}), false},
		"bearer without atom": {req("GET", "/orders?api-version=2024-05", map[string]string{"Authorization": "Bearer t"}), false},
	} {
		if got := info.Match(c.r); got != c.want {
			t.Errorf("%s: match = %v, want %v", name, got, c.want)
		}
	}
}

// A negative $skip or $top is a 400, not a crash.
func TestNegativePaging(t *testing.T) {
	h := core.NewHub(core.Config{DataDir: t.TempDir()}, nil)
	h.Register(servicebus.New(h))
	t.Cleanup(h.Close)
	u, pool := core.ListenTLS(t, h, h)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	for _, q := range []string{"$skip=-1", "$top=-5"} {
		r, _ := http.NewRequest("GET", u+"/$Resources/Queues?api-version=2024-05&"+q, nil)
		r.Header.Set("Authorization", "SharedAccessSignature sr=x&sig=y&se=1&skn=k")
		resp := must(hc.Do(r))
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d, want 400", q, resp.StatusCode)
		}
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// A number in a queue description that is not a number, or does not fit, is refused with 400. It used to be stored
// as 0 (so the default) or wrapped: a MaxDeliveryCount of 4294967297 became 1.
func TestQueueNumbersChecked(t *testing.T) {
	h := core.NewHub(core.Config{DataDir: t.TempDir()}, nil)
	h.Register(servicebus.New(h))
	t.Cleanup(h.Close)
	u, pool := core.ListenTLS(t, h, h)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	put := func(name, field, v string) int {
		body := `<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><` +
			field + `>` + v + `</` + field + `></QueueDescription></content></entry>`
		r, _ := http.NewRequest("PUT", u+"/"+name+"?api-version=2024-05", strings.NewReader(body))
		r.Header.Set("Authorization", "SharedAccessSignature sr=x&sig=y&se=1&skn=k")
		r.Header.Set("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
		resp := must(hc.Do(r))
		resp.Body.Close()
		return resp.StatusCode
	}
	for i, c := range []struct{ field, v string }{{"MaxDeliveryCount", "4294967297"}, {"MaxDeliveryCount", "abc"},
		{"MaxSizeInMegabytes", "99999999999"}, {"MaxMessageSizeInKilobytes", "1e3"}} {
		if code := put(fmt.Sprintf("bad%d", i), c.field, c.v); code != 400 {
			t.Errorf("%s=%s: status %d, want 400", c.field, c.v, code)
		}
	}
	if code := put("good", "MaxDeliveryCount", "7"); code != 201 {
		t.Fatalf("a valid MaxDeliveryCount: status %d, want 201", code)
	}
}
