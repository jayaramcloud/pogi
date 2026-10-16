package servicebus_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// sb is the official SDK pointed at pogi: the admin client over HTTPS on the main port, messaging
// over plain AMQP (UseDevelopmentEmulator=true) on the broker's.
type sb struct {
	t     *testing.T
	srv   *pogi.Running
	admin *admin.Client
	cli   *azservicebus.Client
}

func connString(addr string) string {
	return "Endpoint=sb://" + addr + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c2VjcmV0;UseDevelopmentEmulator=true"
}

func startSB(t *testing.T) *sb {
	t.Helper()
	srv := pogi.Start(t)
	mainPort := srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	caPath, _, _ := srv.Hub.CertPaths()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	ac, err := admin.NewClientFromConnectionString(connString("localhost:"+mainPort), &admin.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}}})
	if err != nil {
		t.Fatal(err)
	}
	amqpAddr := strings.TrimPrefix(srv.Ports["amqp"], "amqp://")
	if amqpAddr == "" {
		t.Fatal("pogi.Start has no AMQP port")
	}
	cli, err := azservicebus.NewClientFromConnectionString(connString(amqpAddr), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close(bg) })
	return &sb{t: t, srv: srv, admin: ac, cli: cli}
}

func (s *sb) sender(entity string) *azservicebus.Sender {
	s.t.Helper()
	snd := must(s.cli.NewSender(entity, nil))
	s.t.Cleanup(func() { snd.Close(bg) })
	return snd
}

func (s *sb) receiver(queue string, o *azservicebus.ReceiverOptions) *azservicebus.Receiver {
	s.t.Helper()
	r := must(s.cli.NewReceiverForQueue(queue, o))
	s.t.Cleanup(func() { r.Close(bg) })
	return r
}

func (s *sb) subReceiver(topic, sub string, o *azservicebus.ReceiverOptions) *azservicebus.Receiver {
	s.t.Helper()
	r := must(s.cli.NewReceiverForSubscription(topic, sub, o))
	s.t.Cleanup(func() { r.Close(bg) })
	return r
}

func sbody(m *azservicebus.ReceivedMessage) string { return string(m.Body) }

func bodies(ms []*azservicebus.ReceivedMessage) string {
	var out []string
	for _, m := range ms {
		out = append(out, string(m.Body))
	}
	return strings.Join(out, ",")
}

// recv takes n messages within a few seconds.
func recv(t *testing.T, r *azservicebus.Receiver, n int) []*azservicebus.ReceivedMessage {
	t.Helper()
	ctx, cancel := bgTimeout(5 * time.Second)
	defer cancel()
	var out []*azservicebus.ReceivedMessage
	for len(out) < n {
		ms, err := r.ReceiveMessages(ctx, n-len(out), nil)
		if err != nil {
			t.Fatalf("received %d of %d: %v", len(out), n, err)
		}
		out = append(out, ms...)
	}
	return out
}

// expectNone checks that nothing arrives soon.
func expectNone(t *testing.T, r *azservicebus.Receiver) {
	t.Helper()
	ctx, cancel := bgTimeout(500 * time.Millisecond)
	defer cancel()
	ms, _ := r.ReceiveMessages(ctx, 1, nil)
	if len(ms) > 0 {
		t.Fatalf("unexpected message %q", sbody(ms[0]))
	}
}

func TestSDKSendReceiveSettle(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "orders", nil))
	snd := s.sender("orders")

	// single messages with their properties, and a batch
	check(snd.SendMessage(bg, &azservicebus.Message{
		Body: []byte("one"), MessageID: to.Ptr("m1"), Subject: to.Ptr("greeting"), ContentType: to.Ptr("text/plain"),
		CorrelationID: to.Ptr("c1"), To: to.Ptr("dest"), ReplyTo: to.Ptr("back"), TimeToLive: to.Ptr(time.Hour),
		ApplicationProperties: map[string]any{"n": int64(5), "s": "x", "b": true},
	}, nil))
	batch := must(snd.NewMessageBatch(bg, nil))
	for _, b := range []string{"two", "three", "four", "five"} {
		check(batch.AddMessage(&azservicebus.Message{Body: []byte(b)}, nil))
	}
	check(snd.SendMessageBatch(bg, batch, nil))
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "orders", nil)); rt.ActiveMessageCount != 5 {
		t.Fatalf("active = %d", rt.ActiveMessageCount)
	}

	r := s.receiver("orders", nil)
	first := recv(t, r, 1)[0]
	if sbody(first) != "one" || first.MessageID != "m1" || *first.Subject != "greeting" || *first.ContentType != "text/plain" ||
		*first.CorrelationID != "c1" || *first.To != "dest" || *first.ReplyTo != "back" || first.DeliveryCount != 1 ||
		first.ApplicationProperties["n"] != int64(5) || first.ApplicationProperties["s"] != "x" || first.ApplicationProperties["b"] != true ||
		first.SequenceNumber == nil || *first.SequenceNumber != 1 || first.EnqueuedTime == nil || first.LockedUntil == nil ||
		first.LockToken == [16]byte{} || first.TimeToLive == nil {
		t.Fatalf("first = %+v", first)
	}
	if !first.LockedUntil.After(time.Now().Add(30 * time.Second)) {
		t.Fatalf("locked until %v", first.LockedUntil)
	}

	// complete
	check(r.CompleteMessage(bg, first, nil))

	// abandon: redelivered with the count up and the properties changed
	m2 := recv(t, r, 1)[0]
	check(r.AbandonMessage(bg, m2, &azservicebus.AbandonMessageOptions{PropertiesToModify: map[string]any{"retry": int64(1)}}))
	m2 = recv(t, r, 1)[0]
	if sbody(m2) != "two" || m2.DeliveryCount != 2 || m2.ApplicationProperties["retry"] != int64(1) {
		t.Fatalf("redelivered = %+v", m2)
	}

	// dead-letter, then read it from the DLQ with its reason
	check(r.DeadLetterMessage(bg, m2, &azservicebus.DeadLetterOptions{Reason: to.Ptr("bad"), ErrorDescription: to.Ptr("very bad")}))
	dlq := s.receiver("orders", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	dm := recv(t, dlq, 1)[0]
	if sbody(dm) != "two" || *dm.DeadLetterReason != "bad" || *dm.DeadLetterErrorDescription != "very bad" || *dm.DeadLetterSource != "orders" {
		t.Fatalf("dead letter = %+v", dm)
	}
	check(dlq.CompleteMessage(bg, dm, nil))
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "orders", nil)); rt.DeadLetterMessageCount != 0 || rt.ActiveMessageCount != 3 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}

	// defer, then receive the deferred message by sequence number and complete it
	m3 := recv(t, r, 1)[0]
	seq := *m3.SequenceNumber
	check(r.DeferMessage(bg, m3, nil))
	deferred := must(r.ReceiveDeferredMessages(bg, []int64{seq}, nil))
	if len(deferred) != 1 || sbody(deferred[0]) != "three" || deferred[0].State != azservicebus.MessageStateDeferred {
		t.Fatalf("deferred = %+v", deferred)
	}
	check(r.CompleteMessage(bg, deferred[0], nil))

	// the rest, then nothing
	rest := recv(t, r, 2)
	if bodies(rest) != "four,five" {
		t.Fatalf("rest = %s", bodies(rest))
	}
	for _, m := range rest {
		check(r.CompleteMessage(bg, m, nil))
	}
	expectNone(t, r)
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "orders", nil)); rt.TotalMessageCount != 0 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}
}

func TestSDKMaxDeliveryCount(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "q", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{MaxDeliveryCount: to.Ptr(int32(2))}}))
	check(s.sender("q").SendMessage(bg, &azservicebus.Message{Body: []byte("poison")}, nil))
	r := s.receiver("q", nil)
	for i := 1; i <= 2; i++ {
		m := recv(t, r, 1)[0]
		if m.DeliveryCount != uint32(i) {
			t.Fatalf("delivery %d count = %d", i, m.DeliveryCount)
		}
		check(r.AbandonMessage(bg, m, nil))
	}
	expectNone(t, r)
	dlq := s.receiver("q", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	dm := recv(t, dlq, 1)[0]
	if sbody(dm) != "poison" || *dm.DeadLetterReason != "MaxDeliveryCountExceeded" {
		t.Fatalf("dead letter = %+v", dm)
	}
}

func TestSDKLockExpiryAndRenewal(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "q", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{LockDuration: to.Ptr("PT2S")}}))
	check(s.sender("q").SendMessage(bg, &azservicebus.Message{Body: []byte("m")}, nil))
	r := s.receiver("q", nil)
	m := recv(t, r, 1)[0]
	before := *m.LockedUntil
	time.Sleep(time.Second)
	check(r.RenewMessageLock(bg, m, nil))
	if !m.LockedUntil.After(before) {
		t.Fatalf("lock not extended: %v -> %v", before, m.LockedUntil)
	}
	time.Sleep(1500 * time.Millisecond) // past the original expiry; the renewal holds
	expectNone(t, r)
	check(r.CompleteMessage(bg, m, nil))

	// an unrenewed lock lapses: the message comes back with a higher count
	check(s.sender("q").SendMessage(bg, &azservicebus.Message{Body: []byte("again")}, nil))
	m = recv(t, r, 1)[0]
	time.Sleep(2500 * time.Millisecond)
	m2 := recv(t, r, 1)[0]
	if sbody(m2) != "again" || m2.DeliveryCount < 2 { // 3 when the redelivered lock also lapses under load
		t.Fatalf("after expiry = %+v", m2)
	}
	// settling with the lost lock must not take the message away from its current holder
	// (Source.Settle cannot report the failure over the link, as Service Bus does)
	r.CompleteMessage(bg, m, nil)
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "q", nil)); rt.TotalMessageCount != 1 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}
	check(r.CompleteMessage(bg, m2, nil))
}

func TestSDKReceiveAndDelete(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "q", nil))
	snd := s.sender("q")
	for _, b := range []string{"a", "b", "c"} {
		check(snd.SendMessage(bg, &azservicebus.Message{Body: []byte(b)}, nil))
	}
	r := s.receiver("q", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if got := bodies(recv(t, r, 3)); got != "a,b,c" {
		t.Fatalf("received %s", got)
	}
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "q", nil)); rt.TotalMessageCount != 0 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}
}

func TestSDKPeekAndSchedule(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "q", nil))
	snd := s.sender("q")
	for _, b := range []string{"a", "b", "c"} {
		check(snd.SendMessage(bg, &azservicebus.Message{Body: []byte(b), ApplicationProperties: map[string]any{"k": b}}, nil))
	}
	r := s.receiver("q", nil)
	peeked := must(r.PeekMessages(bg, 2, nil))
	if bodies(peeked) != "a,b" || peeked[0].ApplicationProperties["k"] != "a" {
		t.Fatalf("peeked = %s", bodies(peeked))
	}
	// the next peek continues after the last one; peeking locks nothing
	if got := bodies(must(r.PeekMessages(bg, 5, nil))); got != "c" {
		t.Fatalf("second peek = %s", got)
	}
	from := *peeked[1].SequenceNumber
	if got := bodies(must(r.PeekMessages(bg, 5, &azservicebus.PeekMessagesOptions{FromSequenceNumber: &from}))); got != "b,c" {
		t.Fatalf("peek from %d = %s", from, got)
	}
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "q", nil)); rt.ActiveMessageCount != 3 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}

	// scheduled messages stay hidden until their time; one is cancelled
	at := time.Now().Add(1500 * time.Millisecond)
	seqs := must(snd.ScheduleMessages(bg, []*azservicebus.Message{{Body: []byte("s1")}, {Body: []byte("s2")}}, at, nil))
	if len(seqs) != 2 || seqs[0] == seqs[1] {
		t.Fatalf("sequence numbers = %v", seqs)
	}
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "q", nil)); rt.ScheduledMessageCount != 2 || rt.ActiveMessageCount != 3 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}
	check(snd.CancelScheduledMessages(bg, seqs[:1], nil))
	if err := snd.CancelScheduledMessages(bg, seqs[:1], nil); err == nil {
		t.Fatal("cancelled the same message twice")
	}
	for _, m := range recv(t, r, 3) {
		check(r.CompleteMessage(bg, m, nil))
	}
	expectNone(t, r)
	time.Sleep(time.Until(at.Add(600 * time.Millisecond)))
	m := recv(t, r, 1)[0]
	if sbody(m) != "s2" || m.ScheduledEnqueueTime == nil || m.ScheduledEnqueueTime.Sub(at).Abs() > time.Second {
		t.Fatalf("scheduled = %+v", m)
	}
	check(r.CompleteMessage(bg, m, nil))
	expectNone(t, r)

	// a message sent with ScheduledEnqueueTime set
	later := time.Now().Add(time.Hour)
	check(snd.SendMessage(bg, &azservicebus.Message{Body: []byte("far"), ScheduledEnqueueTime: &later}, nil))
	expectNone(t, r)
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "q", nil)); rt.ScheduledMessageCount != 1 {
		t.Fatalf("runtime = %+v", rt.QueueRuntimeProperties)
	}
}

func TestSDKDuplicateDetectionAndTTL(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "dd", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{RequiresDuplicateDetection: to.Ptr(true)}}))
	snd := s.sender("dd")
	for _, id := range []string{"a", "a", "b", "a"} {
		check(snd.SendMessage(bg, &azservicebus.Message{Body: []byte("x"), MessageID: to.Ptr(id)}, nil))
	}
	if rt := must(s.admin.GetQueueRuntimeProperties(bg, "dd", nil)); rt.ActiveMessageCount != 2 {
		t.Fatalf("active = %d, want 2 after duplicates", rt.ActiveMessageCount)
	}

	// expired messages go to the DLQ when the queue says so
	must(s.admin.CreateQueue(bg, "ttl", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{DeadLetteringOnMessageExpiration: to.Ptr(true)}}))
	check(s.sender("ttl").SendMessage(bg, &azservicebus.Message{Body: []byte("short"), TimeToLive: to.Ptr(time.Second)}, nil))
	time.Sleep(1600 * time.Millisecond)
	dlq := s.receiver("ttl", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	dm := recv(t, dlq, 1)[0]
	if *dm.DeadLetterReason != "TTLExpiredException" {
		t.Fatalf("expired = %+v", dm)
	}
}

func TestSDKTopicFanOut(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateTopic(bg, "events", nil))
	for _, sub := range []string{"all", "red", "big", "corr", "none"} {
		must(s.admin.CreateSubscription(bg, "events", sub, &admin.CreateSubscriptionOptions{Properties: &admin.SubscriptionProperties{DefaultRule: ruleFor(sub)}}))
	}
	// an extra rule added after creation: "red" also gets the large ones
	must(s.admin.CreateRule(bg, "events", "red", &admin.CreateRuleOptions{Name: to.Ptr("large"),
		Filter: &admin.SQLFilter{Expression: "price >= @p", Parameters: map[string]any{"p": int64(100)}}, Action: &admin.SQLAction{Expression: "SET tag = 'large'"}}))

	snd := s.sender("events")
	send := func(body string, m azservicebus.Message) {
		m.Body = []byte(body)
		check(snd.SendMessage(bg, &m, nil))
	}
	send("red-cheap", azservicebus.Message{ApplicationProperties: map[string]any{"color": "red", "price": int64(5)}})
	send("blue-big", azservicebus.Message{ApplicationProperties: map[string]any{"color": "blue", "price": int64(500)}})
	send("order", azservicebus.Message{CorrelationID: to.Ptr("order-7"), Subject: to.Ptr("order")})

	got := func(sub string, n int) string {
		r := s.subReceiver("events", sub, &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
		return bodies(recv(t, r, n))
	}
	if g := got("all", 3); g != "red-cheap,blue-big,order" {
		t.Fatalf("all = %s", g)
	}
	if g := got("red", 2); g != "red-cheap,blue-big" {
		t.Fatalf("red = %s", g)
	}
	if g := got("big", 1); g != "blue-big" {
		t.Fatalf("big = %s", g)
	}
	if g := got("corr", 1); g != "order" {
		t.Fatalf("corr = %s", g)
	}
	r := s.subReceiver("events", "none", nil)
	expectNone(t, r)
	// the rule action tagged the copy blue-big in "red"; delivered copies were consumed above, so check a new one
	send("red-big", azservicebus.Message{ApplicationProperties: map[string]any{"color": "red", "price": int64(300)}})
	rr := s.subReceiver("events", "red", nil)
	ms := recv(t, rr, 2) // two rules match: a copy per rule
	tags := 0
	for _, m := range ms {
		if m.ApplicationProperties["tag"] == "large" {
			tags++
		}
	}
	if len(ms) != 2 || tags != 1 {
		t.Fatalf("red copies = %d with %d tagged", len(ms), tags)
	}

	if trt := must(s.admin.GetTopicRuntimeProperties(bg, "events", nil)); trt.SubscriptionCount != 5 || trt.SQLFilterCount != 3 || trt.CorrelationFilterCount != 1 {
		t.Fatalf("topic runtime = %+v", trt.TopicRuntimeProperties)
	}
}

func ruleFor(sub string) *admin.RuleProperties {
	switch sub {
	case "red":
		return &admin.RuleProperties{Name: "red", Filter: &admin.SQLFilter{Expression: "color = 'red'"}}
	case "big":
		return &admin.RuleProperties{Name: "big", Filter: &admin.SQLFilter{Expression: "price > 100 AND color <> 'red'"}}
	case "corr":
		return &admin.RuleProperties{Name: "corr", Filter: &admin.CorrelationFilter{CorrelationID: to.Ptr("order-7"), Subject: to.Ptr("order")}}
	case "none":
		return &admin.RuleProperties{Name: "none", Filter: &admin.FalseFilter{}}
	}
	return nil
}

func TestSDKSessions(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "sq", &admin.CreateQueueOptions{Properties: &admin.QueueProperties{RequiresSession: to.Ptr(true)}}))
	snd := s.sender("sq")
	for _, m := range [][2]string{{"A", "a1"}, {"B", "b1"}, {"A", "a2"}} {
		check(snd.SendMessage(bg, &azservicebus.Message{Body: []byte(m[1]), SessionID: to.Ptr(m[0])}, nil))
	}
	// a session-less send to a session queue is refused
	if err := snd.SendMessage(bg, &azservicebus.Message{Body: []byte("x")}, nil); err == nil {
		t.Fatal("sent without a session id")
	}
	// a plain receiver is refused too
	if r, err := s.cli.NewReceiverForQueue("sq", nil); err == nil {
		short, cancel := bgTimeout(2 * time.Second) // the SDK keeps retrying the refusal
		defer cancel()
		_, err = r.ReceiveMessages(short, 1, nil)
		r.Close(short)
		if err == nil {
			t.Fatal("plain receiver on a session queue")
		}
	}

	// the next available session is A; its messages come in order
	ctx, cancel := bgTimeout(5 * time.Second)
	defer cancel()
	sa := must(s.cli.AcceptNextSessionForQueue(ctx, "sq", nil))
	if sa.SessionID() != "A" {
		t.Fatalf("session = %s", sa.SessionID())
	}
	ms := must(sa.ReceiveMessages(ctx, 2, nil))
	for len(ms) < 2 {
		ms = append(ms, must(sa.ReceiveMessages(ctx, 1, nil))...)
	}
	if bodies(ms) != "a1,a2" || *ms[0].SessionID != "A" {
		t.Fatalf("session A = %s", bodies(ms))
	}
	for _, m := range ms {
		check(sa.CompleteMessage(ctx, m, nil))
	}

	// session state and lock renewal
	if st := must(sa.GetSessionState(ctx, nil)); st != nil {
		t.Fatalf("initial state = %q", st)
	}
	check(sa.SetSessionState(ctx, []byte("cursor=2"), nil))
	if st := must(sa.GetSessionState(ctx, nil)); string(st) != "cursor=2" {
		t.Fatalf("state = %q", st)
	}
	before := sa.LockedUntil()
	time.Sleep(50 * time.Millisecond)
	check(sa.RenewSessionLock(ctx, nil))
	if !sa.LockedUntil().After(before) {
		t.Fatalf("session lock %v -> %v", before, sa.LockedUntil())
	}

	// a locked session cannot be accepted twice
	if _, err := s.cli.AcceptSessionForQueue(ctx, "sq", "A", nil); err == nil {
		t.Fatal("accepted a locked session")
	}
	// the next session is B, and a specific one can be named
	sbq := must(s.cli.AcceptNextSessionForQueue(ctx, "sq", nil))
	if sbq.SessionID() != "B" {
		t.Fatalf("next session = %s", sbq.SessionID())
	}
	b1 := must(sbq.ReceiveMessages(ctx, 1, nil))
	if sbody(b1[0]) != "b1" {
		t.Fatalf("B = %s", bodies(b1))
	}
	check(sbq.CompleteMessage(ctx, b1[0], nil))
	check(sbq.Close(ctx))

	// closing A releases it, and its state is still there
	check(sa.Close(ctx))
	again := must(s.cli.AcceptSessionForQueue(ctx, "sq", "A", nil))
	if st := must(again.GetSessionState(ctx, nil)); string(st) != "cursor=2" {
		t.Fatalf("state after reaccept = %q", st)
	}
	check(again.Close(ctx))

	// with no sessions holding messages the wait for "next" gives up
	short, c2 := bgTimeout(600 * time.Millisecond)
	defer c2()
	if _, err := s.cli.AcceptNextSessionForQueue(short, "sq", nil); err == nil {
		t.Fatal("got a session from an empty queue")
	}
}

func TestSDKDeadLetterThenResubmit(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "q", nil))
	snd := s.sender("q")
	for i := range 3 {
		check(snd.SendMessage(bg, &azservicebus.Message{Body: []byte(fmt.Sprint("m", i))}, nil))
	}
	r := s.receiver("q", nil)
	for _, m := range recv(t, r, 3) {
		check(r.DeadLetterMessage(bg, m, &azservicebus.DeadLetterOptions{Reason: to.Ptr("r"), PropertiesToModify: map[string]any{"why": "test"}}))
	}
	dlq := s.receiver("q", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	got := recv(t, dlq, 3)
	if bodies(got) != "m0,m1,m2" || got[0].ApplicationProperties["why"] != "test" {
		t.Fatalf("dlq = %s %v", bodies(got), got[0].ApplicationProperties)
	}
	for _, m := range got {
		check(snd.SendMessage(bg, &azservicebus.Message{Body: m.Body}, nil))
		check(dlq.CompleteMessage(bg, m, nil))
	}
	if got := bodies(recv(t, r, 3)); got != "m0,m1,m2" {
		t.Fatalf("resubmitted = %s", got)
	}
}

func bgTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// TestSDKEventGridDelivery: what Event Grid hands the hub for a Service Bus destination lands on the
// queue or topic subscription and is received over AMQP.
func TestSDKEventGridDelivery(t *testing.T) {
	s := startSB(t)
	must(s.admin.CreateQueue(bg, "events", nil))
	must(s.admin.CreateTopic(bg, "fanout", nil))
	must(s.admin.CreateSubscription(bg, "fanout", "all", nil))
	id := func(kind, name string) string {
		return "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup +
			"/providers/Microsoft.ServiceBus/namespaces/pogi/" + kind + "/" + name
	}
	event := `[{"id":"1","eventType":"Microsoft.Storage.BlobCreated","subject":"/blobServices/default/containers/c/blobs/b"}]`
	check(s.srv.Hub.Deliver(id("queues", "events"), &core.Message{Body: event}))
	check(s.srv.Hub.Deliver(id("topics", "fanout"), &core.Message{Body: event, Subject: "blob"}))

	m := recv(t, s.receiver("events", nil), 1)[0]
	if sbody(m) != event || *m.ContentType != "application/json" {
		t.Fatalf("queue message = %q %v", sbody(m), m.ContentType)
	}
	m = recv(t, s.subReceiver("fanout", "all", nil), 1)[0]
	if sbody(m) != event || *m.Subject != "blob" {
		t.Fatalf("topic message = %q", sbody(m))
	}
}
