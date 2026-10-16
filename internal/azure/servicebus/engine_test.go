package servicebus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/azure/servicebus"
	"github.com/nrndev/pogi/internal/core"
)

const ns = "ns1" // from the host name ns1.servicebus.windows.net

// clock is the service's time in tests, moved by hand.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

type env struct {
	t   *testing.T
	h   *core.Hub
	svc *servicebus.Service
	clk *clock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	h := core.NewHub(core.Config{DataDir: t.TempDir()}, nil)
	svc := servicebus.New(h)
	h.Register(svc)
	t.Cleanup(h.Close)
	clk := &clock{t: time.Now().UTC().Truncate(time.Millisecond)}
	svc.Now = clk.now
	return &env{t: t, h: h, svc: svc, clk: clk}
}

func (e *env) link(addr string, receiving bool) *amqp.Link {
	return &amqp.Link{Conn: &amqp.Conn{Host: ns + ".servicebus.windows.net"}, Address: addr, Receiving: receiving}
}

func (e *env) sender(addr string) (amqp.Sink, *amqp.Link) {
	e.t.Helper()
	l := e.link(addr, false)
	n, err := e.h.AMQP().Resolve(l)
	if err != nil {
		e.t.Fatalf("attach sender %s: %v", addr, err)
	}
	return n.(amqp.Sink), l
}

func (e *env) receiver(addr string, mod ...func(*amqp.Link)) (amqp.Source, *amqp.Link, error) {
	e.t.Helper()
	l := e.link(addr, true)
	for _, m := range mod {
		m(l)
	}
	n, err := e.h.AMQP().Resolve(l)
	if err != nil {
		return nil, l, err
	}
	return n.(amqp.Source), l, nil
}

func (e *env) mustReceiver(addr string, mod ...func(*amqp.Link)) (amqp.Source, *amqp.Link) {
	e.t.Helper()
	src, l, err := e.receiver(addr, mod...)
	if err != nil {
		e.t.Fatalf("attach receiver %s: %v", addr, err)
	}
	return src, l
}

func (e *env) send(addr string, ms ...*amqp.Message) {
	e.t.Helper()
	k, l := e.sender(addr)
	for _, m := range ms {
		if err := k.Accept(context.Background(), l, m); err != nil {
			e.t.Fatalf("send to %s: %v", addr, err)
		}
	}
}

func msg(body, id string) *amqp.Message {
	return &amqp.Message{Data: [][]byte{[]byte(body)}, Properties: &amqp.Properties{MessageID: id}}
}

func (e *env) next(src amqp.Source, l *amqp.Link) *amqp.Message {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m, err := src.Next(ctx, l)
	if err != nil {
		e.t.Fatalf("Next: %v", err)
	}
	return m
}

func (e *env) none(src amqp.Source, l *amqp.Link) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if m, err := src.Next(ctx, l); err == nil {
		e.t.Fatalf("Next returned %v, want nothing", string(m.Data[0]))
	}
}

func body(m *amqp.Message) string { return string(m.Data[0]) }

func (e *env) queue(name string, p servicebus.Props) {
	e.t.Helper()
	if _, err := e.svc.CreateQueue(ns, name, p); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) counts(name string) servicebus.Entity {
	e.t.Helper()
	q, ok := e.svc.GetQueue(ns, name)
	if !ok {
		e.t.Fatalf("no queue %s", name)
	}
	return q
}

var (
	accept  = amqp.Outcome{State: "accepted"}
	abandon = amqp.Outcome{State: "modified"}
	deferO  = amqp.Outcome{State: "modified", UndeliverableHere: true}
)

func deadLetter(reason, desc string) amqp.Outcome {
	return amqp.Outcome{State: "rejected", Error: &amqp.Error{Condition: "com.microsoft:dead-letter",
		Info: map[string]any{"DeadLetterReason": reason, "DeadLetterErrorDescription": desc}}}
}

func mgmt(e *env, path, op string, v map[string]any) *amqp.Message {
	e.t.Helper()
	return e.svc.Manage(ns, path, &amqp.Message{ApplicationProperties: map[string]any{"operation": "com.microsoft:" + op}, Value: v})
}

func status(m *amqp.Message) int { return int(m.ApplicationProperties["status-code"].(int32)) }

func TestPeekLockSettlement(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{})
	app := map[string]any{"k": int64(7)}
	m1, m2, m3 := msg("one", "id1"), msg("two", "id2"), msg("three", "id3")
	m1.ApplicationProperties = app
	e.send("q", m1, m2, m3)
	src, l := e.mustReceiver("q")

	// delivery: annotations, lock token, delivery count
	d1 := e.next(src, l)
	if body(d1) != "one" || d1.ApplicationProperties["k"] != int64(7) {
		t.Fatalf("first = %+v", d1)
	}
	a := d1.MessageAnnotations
	if a["x-opt-sequence-number"] != int64(1) || a["x-opt-enqueued-time"] != e.clk.now() || a["x-opt-locked-until"] != e.clk.now().Add(time.Minute) {
		t.Fatalf("annotations = %v", a)
	}
	tok, ok := d1.DeliveryAnnotations["x-opt-lock-token"].(amqp.UUID)
	if !ok || len(d1.DeliveryTag) != 16 || d1.DeliveryTag[0] != tok[3] || d1.DeliveryTag[3] != tok[0] || d1.DeliveryTag[8] != tok[8] {
		t.Fatalf("lock token %v, tag %v", d1.DeliveryAnnotations, d1.DeliveryTag)
	}
	if d1.Header.DeliveryCount != 0 {
		t.Fatalf("delivery count = %d", d1.Header.DeliveryCount)
	}

	// complete
	src.Settle(l, d1, accept)
	if c := e.counts("q"); c.Active != 2 {
		t.Fatalf("after complete: %+v", c)
	}

	// abandon: back at the front, delivery count 1, properties merged
	d2 := e.next(src, l)
	src.Settle(l, d2, amqp.Outcome{State: "modified", Annotations: map[string]any{"retry": int64(1)}})
	d2 = e.next(src, l)
	if body(d2) != "two" || d2.Header.DeliveryCount != 1 || d2.ApplicationProperties["retry"] != int64(1) {
		t.Fatalf("redelivered = %+v header %+v", d2.ApplicationProperties, d2.Header)
	}

	// dead-letter: reason in the application properties, source recorded
	src.Settle(l, d2, deadLetter("bad", "why"))
	dlq, dl := e.mustReceiver("q/$DeadLetterQueue")
	dd := e.next(dlq, dl)
	if body(dd) != "two" || dd.ApplicationProperties["DeadLetterReason"] != "bad" || dd.ApplicationProperties["DeadLetterErrorDescription"] != "why" ||
		dd.MessageAnnotations["x-opt-deadletter-source"] != "q" || dd.MessageAnnotations["x-opt-enqueue-sequence-number"] != int64(2) {
		t.Fatalf("dead letter = %+v %v", dd.ApplicationProperties, dd.MessageAnnotations)
	}
	dlq.Settle(dl, dd, accept)
	if c := e.counts("q"); c.Dead != 0 || c.Active != 1 {
		t.Fatalf("counts = %+v", c)
	}

	// defer, then receive it by sequence number and settle it by lock token
	d3 := e.next(src, l)
	src.Settle(l, d3, deferO)
	e.none(src, l)
	if c := e.counts("q"); c.Deferred != 1 || c.Active != 0 {
		t.Fatalf("counts = %+v", c)
	}
	resp := mgmt(e, "q", "receive-by-sequence-number", map[string]any{"sequence-numbers": []int64{3}, "receiver-settle-mode": uint32(1)})
	if status(resp) != 200 {
		t.Fatalf("receive deferred = %v", resp.ApplicationProperties)
	}
	items := resp.Value.(map[string]any)["messages"].([]any)
	raw, _ := items[0].(map[string]any)["message"].([]byte)
	got, err := servicebus.Decode(raw)
	if err != nil || body(got) != "three" || got.MessageAnnotations["x-opt-message-state"] != int32(1) {
		t.Fatalf("deferred message = %+v %v", got, err)
	}
	dtok := got.DeliveryAnnotations["x-opt-lock-token"].(amqp.UUID)
	if r := mgmt(e, "q", "update-disposition", map[string]any{"disposition-status": "completed", "lock-tokens": []amqp.UUID{dtok}}); status(r) != 200 {
		t.Fatalf("disposition = %v", r.ApplicationProperties)
	}
	if c := e.counts("q"); c.Deferred != 0 || c.Active != 0 {
		t.Fatalf("counts = %+v", c)
	}
	// the same token twice: the lock is gone
	if r := mgmt(e, "q", "update-disposition", map[string]any{"disposition-status": "completed", "lock-tokens": []amqp.UUID{dtok}}); status(r) != 410 {
		t.Fatalf("second disposition = %v", r.ApplicationProperties)
	}
}

func TestLockExpiryAndMaxDelivery(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{LockDuration: servicebus.Dur(30 * time.Second), MaxDeliveryCount: 2})
	e.send("q", msg("poison", "p"))
	src, l := e.mustReceiver("q")

	d := e.next(src, l)
	if d.Header.DeliveryCount != 0 {
		t.Fatal("first delivery count")
	}
	e.none(src, l) // locked
	e.svc.Tick(e.clk.add(29 * time.Second))
	e.none(src, l)

	// the lock expires: delivered again, count one higher
	e.svc.Tick(e.clk.add(2 * time.Second))
	d = e.next(src, l)
	if d.Header.DeliveryCount != 1 {
		t.Fatalf("redelivery count = %d", d.Header.DeliveryCount)
	}

	// renew the lock: expiry moves
	tok := d.DeliveryAnnotations["x-opt-lock-token"].(amqp.UUID)
	e.clk.add(20 * time.Second)
	r := mgmt(e, "q", "renew-lock", map[string]any{"lock-tokens": []amqp.UUID{tok}})
	exp := r.Value.(map[string]any)["expirations"].([]time.Time)
	if status(r) != 200 || !exp[0].Equal(e.clk.now().Add(30*time.Second)) {
		t.Fatalf("renew = %v %v", r.ApplicationProperties, r.Value)
	}
	e.svc.Tick(e.clk.add(25 * time.Second))
	e.none(src, l) // still locked thanks to the renewal

	// second expiry reaches MaxDeliveryCount: dead-lettered
	e.svc.Tick(e.clk.add(10 * time.Second))
	e.none(src, l)
	dlq, dl := e.mustReceiver("q/$DeadLetterQueue")
	dd := e.next(dlq, dl)
	if dd.ApplicationProperties["DeadLetterReason"] != "MaxDeliveryCountExceeded" || body(dd) != "poison" {
		t.Fatalf("dead letter = %+v", dd.ApplicationProperties)
	}
	// the lost lock cannot be renewed
	if r := mgmt(e, "q", "renew-lock", map[string]any{"lock-tokens": []amqp.UUID{tok}}); status(r) != 410 {
		t.Fatalf("renew after loss = %v", r.ApplicationProperties)
	}
}

func TestReceiveAndDelete(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{})
	e.send("q", msg("a", "1"), msg("b", "2"))
	src, l := e.mustReceiver("q", func(l *amqp.Link) { l.Settled = true })
	if m := e.next(src, l); body(m) != "a" || len(m.DeliveryTag) != 0 {
		t.Fatalf("got %+v", m)
	}
	if c := e.counts("q"); c.Active != 1 {
		t.Fatalf("counts = %+v", c)
	}
}

func TestTimeToLive(t *testing.T) {
	e := newEnv(t)
	e.queue("plain", servicebus.Props{DefaultMessageTimeToLive: servicebus.Dur(time.Minute)})
	e.queue("dlq", servicebus.Props{DefaultMessageTimeToLive: servicebus.Dur(time.Minute), DeadLetteringOnMessageExpiration: true})
	short := msg("short", "s")
	short.Header = &amqp.Header{TTL: 10 * time.Second}
	long := msg("long", "l")
	long.Header = &amqp.Header{TTL: time.Hour} // cannot outlive the queue's default
	for _, q := range []string{"plain", "dlq"} {
		e.send(q, short, long)
	}
	e.svc.Tick(e.clk.add(11 * time.Second))
	if c := e.counts("plain"); c.Active != 1 || c.Dead != 0 {
		t.Fatalf("plain = %+v", c)
	}
	e.svc.Tick(e.clk.add(time.Minute))
	if c := e.counts("plain"); c.Active != 0 || c.Dead != 0 {
		t.Fatalf("plain after default ttl = %+v", c)
	}
	c := e.counts("dlq")
	if c.Active != 0 || c.Dead != 2 {
		t.Fatalf("dlq = %+v", c)
	}
	src, l := e.mustReceiver("dlq/$DeadLetterQueue")
	if m := e.next(src, l); m.ApplicationProperties["DeadLetterReason"] != "TTLExpiredException" {
		t.Fatalf("reason = %v", m.ApplicationProperties)
	}
}

func TestScheduledMessages(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{})
	at := e.clk.now().Add(time.Hour)
	m := msg("later", "x")
	m.MessageAnnotations = map[string]any{"x-opt-scheduled-enqueue-time": at}
	e.send("q", m)
	src, l := e.mustReceiver("q")
	e.none(src, l)
	if c := e.counts("q"); c.Scheduled != 1 || c.Active != 0 {
		t.Fatalf("counts = %+v", c)
	}
	// visible to peek as scheduled
	r := mgmt(e, "q", "peek-message", map[string]any{"from-sequence-number": int64(0), "message-count": int32(10)})
	pm, _ := servicebus.Decode(r.Value.(map[string]any)["messages"].([]any)[0].(map[string]any)["message"].([]byte))
	if pm.MessageAnnotations["x-opt-message-state"] != int32(2) {
		t.Fatalf("peeked = %v", pm.MessageAnnotations)
	}
	e.svc.Tick(e.clk.add(time.Hour + time.Second))
	d := e.next(src, l)
	if body(d) != "later" || d.MessageAnnotations["x-opt-scheduled-enqueue-time"] != at {
		t.Fatalf("delivered = %v", d.MessageAnnotations)
	}
	src.Settle(l, d, accept)

	// schedule and cancel through $management
	enc := func(b string) []byte {
		bs, _ := servicebus.Encode(&amqp.Message{Data: [][]byte{[]byte(b)}, Properties: &amqp.Properties{MessageID: b},
			MessageAnnotations: map[string]any{"x-opt-scheduled-enqueue-time": e.clk.now().Add(time.Hour)}})
		return bs
	}
	r = mgmt(e, "q", "schedule-message", map[string]any{"messages": []any{
		map[string]any{"message-id": "a", "message": enc("a")}, map[string]any{"message-id": "b", "message": enc("b")}}})
	seqs, _ := r.Value.(map[string]any)["sequence-numbers"].([]int64)
	if status(r) != 200 || len(seqs) != 2 || e.counts("q").Scheduled != 2 {
		t.Fatalf("schedule = %v %v", r.ApplicationProperties, r.Value)
	}
	if r := mgmt(e, "q", "cancel-scheduled-message", map[string]any{"sequence-numbers": []int64{seqs[0]}}); status(r) != 200 {
		t.Fatalf("cancel = %v", r.ApplicationProperties)
	}
	if r := mgmt(e, "q", "cancel-scheduled-message", map[string]any{"sequence-numbers": []int64{seqs[0]}}); status(r) != 404 {
		t.Fatalf("second cancel = %v", r.ApplicationProperties)
	}
	e.svc.Tick(e.clk.add(2 * time.Hour))
	if d := e.next(src, l); body(d) != "b" {
		t.Fatalf("after cancel got %q", body(d))
	}
	e.none(src, l)
}

func TestDuplicateDetection(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{RequiresDuplicateDetection: true, DuplicateDetectionHistoryWindow: servicebus.Dur(time.Minute)})
	e.send("q", msg("a", "same"), msg("again", "same"), msg("b", "other"))
	if c := e.counts("q"); c.Active != 2 {
		t.Fatalf("counts = %+v", c)
	}
	e.svc.Tick(e.clk.add(2 * time.Minute)) // history forgotten
	e.send("q", msg("a2", "same"))
	if c := e.counts("q"); c.Active != 3 {
		t.Fatalf("after window = %+v", c)
	}
}

func TestForwarding(t *testing.T) {
	e := newEnv(t)
	e.queue("target", servicebus.Props{})
	e.queue("dead", servicebus.Props{})
	e.queue("hop", servicebus.Props{ForwardTo: "sb://" + ns + ".servicebus.windows.net/target"})
	e.queue("src", servicebus.Props{ForwardTo: "hop", ForwardDeadLetteredMessagesTo: "dead", MaxDeliveryCount: 1})
	e.send("src", msg("fwd", "1"))
	if c := e.counts("target"); c.Active != 1 || e.counts("src").Active != 0 || e.counts("hop").Active != 0 {
		t.Fatalf("forwarded = %+v", c)
	}
	// dead letters of a queue go to its ForwardDeadLetteredMessagesTo
	e.queue("work", servicebus.Props{MaxDeliveryCount: 1, ForwardDeadLetteredMessagesTo: "dead"})
	e.send("work", msg("bad", "2"))
	src, l := e.mustReceiver("work")
	d := e.next(src, l)
	src.Settle(l, d, abandon)
	if c := e.counts("dead"); c.Active != 1 || e.counts("work").Dead != 0 {
		t.Fatalf("dead = %+v", c)
	}
}

func TestEntityStatusAndErrors(t *testing.T) {
	e := newEnv(t)
	e.queue("off", servicebus.Props{Status: "Disabled"})
	k, l := e.sender("off")
	var ae *amqp.Error
	if err := k.Accept(context.Background(), l, msg("x", "1")); !errors.As(err, &ae) || ae.Condition != "com.microsoft:entity-disabled" {
		t.Fatalf("send to disabled = %v", err)
	}
	if _, _, err := e.receiver("off"); !errors.As(err, &ae) || ae.Condition != "com.microsoft:entity-disabled" {
		t.Fatalf("receive from disabled = %v", err)
	}
	// unknown entities are left to other resolvers, then not-found
	if _, err := e.h.AMQP().Resolve(e.link("nope", false)); !errors.As(err, &ae) || ae.Condition != amqp.NotFound {
		t.Fatalf("unknown = %v", err)
	}
	e.queue("sess", servicebus.Props{RequiresSession: true})
	k, l = e.sender("sess")
	if err := k.Accept(context.Background(), l, msg("x", "1")); !errors.As(err, &ae) || ae.Condition != amqp.NotAllowed {
		t.Fatalf("no session id = %v", err)
	}
	fs, fl, err := e.receiver("sess")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Next(context.Background(), fl); !errors.As(err, &ae) || ae.Condition != amqp.NotAllowed {
		t.Fatalf("plain receiver on session queue = %v", err)
	}
	if _, err := e.svc.CreateQueue(ns, "off", servicebus.Props{}); err == nil || core.AsError(err).Status != 409 {
		t.Fatalf("duplicate = %v", err)
	}
	if _, err := e.svc.CreateTopic(ns, "off", servicebus.Props{}); err == nil || core.AsError(err).Status != 409 {
		t.Fatalf("queue and topic share a namespace of names = %v", err)
	}
}

func TestSessions(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{RequiresSession: true})
	mk := func(b, id, session string) *amqp.Message {
		m := msg(b, id)
		m.Properties.GroupID = session
		return m
	}
	e.send("q", mk("a1", "1", "A"), mk("b1", "2", "B"), mk("a2", "3", "A"))

	next := func(mod ...func(*amqp.Link)) (amqp.Source, *amqp.Link, error) {
		return e.receiver("q", append(mod, func(l *amqp.Link) { l.Filter = map[string]any{"com.microsoft:session-filter": nil} })...)
	}
	srcA, la, err := next()
	if err != nil {
		t.Fatal(err)
	}
	if la.ReplySourceFilter["com.microsoft:session-filter"] != "A" || la.ReplyProperties["com.microsoft:locked-until-utc"] == nil {
		t.Fatalf("reply = %v %v", la.ReplySourceFilter, la.ReplyProperties)
	}
	srcB, lb, err := next()
	if err != nil || lb.ReplySourceFilter["com.microsoft:session-filter"] != "B" {
		t.Fatalf("second next session = %v %v", lb.ReplySourceFilter, err)
	}
	// session A delivers only its own messages, in order
	d1 := e.next(srcA, la)
	if body(d1) != "a1" {
		t.Fatal("a1")
	}
	srcA.Settle(la, d1, accept)
	if m := e.next(srcB, lb); body(m) != "b1" {
		t.Fatal("b1")
	}

	// no session left: the attach times out
	var ae *amqp.Error
	_, _, err = next(func(l *amqp.Link) { l.Properties = map[string]any{"com.microsoft:timeout": uint32(100)} })
	if !errors.As(err, &ae) || ae.Condition != "com.microsoft:timeout" {
		t.Fatalf("third next session = %v", err)
	}
	// a locked session cannot be taken
	_, _, err = e.receiver("q", func(l *amqp.Link) { l.Filter = map[string]any{"com.microsoft:session-filter": "A"} })
	if !errors.As(err, &ae) || ae.Condition != "com.microsoft:session-cannot-be-locked" {
		t.Fatalf("locked session = %v", err)
	}

	// session state and lock renewal through $management
	if r := mgmt(e, "q", "set-session-state", map[string]any{"session-id": "A", "session-state": []byte("cursor=7")}); status(r) != 200 {
		t.Fatalf("set state = %v", r.ApplicationProperties)
	}
	r := mgmt(e, "q", "get-session-state", map[string]any{"session-id": "A"})
	if string(r.Value.(map[string]any)["session-state"].([]byte)) != "cursor=7" {
		t.Fatalf("get state = %v", r.Value)
	}
	e.clk.add(10 * time.Second)
	r = mgmt(e, "q", "renew-session-lock", map[string]any{"session-id": "A"})
	if exp := r.Value.(map[string]any)["expiration"].(time.Time); !exp.Equal(e.clk.now().Add(time.Minute)) {
		t.Fatalf("renew session = %v", r.Value)
	}
	if r := mgmt(e, "q", "get-session-state", map[string]any{"session-id": "nobody"}); status(r) != 410 {
		t.Fatalf("state of unlocked session = %v", r.ApplicationProperties)
	}

	// closing the receiver releases the session, state kept
	srcA.(amqp.Detacher).Detached(la)
	src2, l2, err := e.receiver("q", func(l *amqp.Link) { l.Filter = map[string]any{"com.microsoft:session-filter": "A"} })
	if err != nil {
		t.Fatalf("reaccept A: %v", err)
	}
	if m := e.next(src2, l2); body(m) != "a2" {
		t.Fatalf("reaccepted session gave %q", body(m))
	}
	if r := mgmt(e, "q", "get-session-state", map[string]any{"session-id": "A"}); string(r.Value.(map[string]any)["session-state"].([]byte)) != "cursor=7" {
		t.Fatalf("state after reaccept = %v", r.Value)
	}

	// a session lock that runs out is lost
	e.svc.Tick(e.clk.add(2 * time.Minute))
	if _, err := srcB.Next(context.Background(), lb); !errors.As(err, &ae) || ae.Condition != "com.microsoft:session-lock-lost" {
		t.Fatalf("expired session = %v", err)
	}
}

func TestTopicFiltersAndActions(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.CreateTopic(ns, "t", servicebus.Props{}); err != nil {
		t.Fatal(err)
	}
	sub := func(name string, def *servicebus.Rule) {
		if _, err := e.svc.CreateSubscription(ns, "t", name, servicebus.Props{}, def); err != nil {
			t.Fatal(err)
		}
	}
	sub("all", nil)
	sub("none", &servicebus.Rule{Name: "r", Filter: servicebus.Filter{Type: "FalseFilter"}})
	sub("sql", &servicebus.Rule{Name: "big", Filter: servicebus.Filter{Type: "SqlFilter", SQL: "price > 10 AND color = 'red'"},
		Action: "SET sys.Label = 'big'; SET price = price * 2; REMOVE color"})
	sub("corr", &servicebus.Rule{Name: "c", Filter: servicebus.Filter{Type: "CorrelationFilter", CorrelationID: "order-1", Properties: map[string]any{"region": "eu"}}})
	sub("two", &servicebus.Rule{Name: "r1", Filter: servicebus.Filter{Type: "SqlFilter", SQL: "region IN ('eu','us')"}})
	if err := e.svc.AddRule(ns, "t", "two", servicebus.Rule{Name: "r2", Filter: servicebus.Filter{Type: "SqlFilter", SQL: "sys.Label LIKE 'o%'"}}); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.AddRule(ns, "t", "two", servicebus.Rule{Name: "bad", Filter: servicebus.Filter{Type: "SqlFilter", SQL: "price >> 1"}}); err == nil {
		t.Fatal("invalid SQL accepted")
	}

	m := msg("hello", "m1")
	m.Properties.CorrelationID = "order-1"
	m.Properties.Subject = "order"
	m.ApplicationProperties = map[string]any{"price": int64(15), "color": "red", "region": "eu"}
	e.send("t", m)

	count := func(sub string) servicebus.Entity {
		s, _ := e.svc.GetSubscription(ns, "t", sub)
		return s
	}
	if count("all").Active != 1 || count("none").Active != 0 || count("sql").Active != 1 || count("corr").Active != 1 {
		t.Fatalf("counts all=%d none=%d sql=%d corr=%d", count("all").Active, count("none").Active, count("sql").Active, count("corr").Active)
	}
	if count("two").Active != 2 { // both rules match: a copy each
		t.Fatalf("two = %d", count("two").Active)
	}
	// the action edited the sql subscription's copy only
	src, l := e.mustReceiver("t/Subscriptions/sql")
	d := e.next(src, l)
	if d.Properties.Subject != "big" || d.ApplicationProperties["price"] != int64(30) || d.ApplicationProperties["color"] != nil {
		t.Fatalf("sql copy = %+v %v", d.Properties, d.ApplicationProperties)
	}
	src, l = e.mustReceiver("t/Subscriptions/all")
	if d := e.next(src, l); d.Properties.Subject != "order" || d.ApplicationProperties["price"] != int64(15) {
		t.Fatalf("all copy = %+v", d.ApplicationProperties)
	}

	// a message that matches nothing is dropped; rules can be listed and removed
	other := msg("x", "m2")
	other.ApplicationProperties = map[string]any{"price": int64(1)}
	e.send("t", other)
	if count("all").Active != 2 || count("sql").Active != 1 || count("none").Active != 0 || count("corr").Active != 1 {
		t.Fatalf("after second: all=%d sql=%d none=%d corr=%d", count("all").Active, count("sql").Active, count("none").Active, count("corr").Active)
	}
	rules, err := e.svc.ListRules(ns, "t", "two")
	if err != nil || len(rules) != 2 {
		t.Fatalf("rules = %v %v", rules, err)
	}
	if err := e.svc.RemoveRule(ns, "t", "two", "r2"); err != nil {
		t.Fatal(err)
	}
	if topic, _ := e.svc.GetTopic(ns, "t"); topic.Subscriptions != 5 || topic.SQLFilters != 2 || topic.CorrelationFilters != 1 {
		t.Fatalf("topic = %+v", topic)
	}
	// receiving from a topic is refused
	var ae *amqp.Error
	if _, _, err := e.receiver("t"); !errors.As(err, &ae) || ae.Condition != amqp.NotAllowed {
		t.Fatalf("receive from topic = %v", err)
	}
}

func TestManagementRules(t *testing.T) {
	e := newEnv(t)
	e.svc.CreateTopic(ns, "t", servicebus.Props{})
	e.svc.CreateSubscription(ns, "t", "s", servicebus.Props{}, nil)
	path := "t/Subscriptions/s"
	r := mgmt(e, path, "add-rule", map[string]any{"rule-name": "sql", "rule-description": map[string]any{
		"sql-filter": map[string]any{"expression": "a = 1"}, "sql-rule-action": map[string]any{"expression": "SET b = 2"}}})
	if status(r) != 200 {
		t.Fatalf("add = %v", r.ApplicationProperties)
	}
	if r := mgmt(e, path, "add-rule", map[string]any{"rule-name": "sql", "rule-description": map[string]any{}}); status(r) != 409 {
		t.Fatalf("duplicate = %v", r.ApplicationProperties)
	}
	r = mgmt(e, path, "enumerate-rules", map[string]any{"top": int32(10), "skip": int32(0)})
	if rules := r.Value.(map[string]any)["rules"].([]any); len(rules) != 2 {
		t.Fatalf("rules = %v", rules)
	}
	if status(mgmt(e, path, "remove-rule", map[string]any{"rule-name": "$Default"})) != 200 || status(mgmt(e, path, "remove-rule", map[string]any{"rule-name": "nope"})) != 404 {
		t.Fatal("remove")
	}
	e.send("t", func() *amqp.Message {
		m := msg("x", "1")
		m.ApplicationProperties = map[string]any{"a": int64(1)}
		return m
	}())
	src, l := e.mustReceiver(path)
	if d := e.next(src, l); d.ApplicationProperties["b"] != int64(2) {
		t.Fatalf("action via mgmt rule = %v", d.ApplicationProperties)
	}
}

func TestPeekAndSessionsListing(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{})
	e.send("q", msg("a", "1"), msg("b", "2"), msg("c", "3"))
	r := mgmt(e, "q", "peek-message", map[string]any{"from-sequence-number": int64(2), "message-count": int32(5)})
	items := r.Value.(map[string]any)["messages"].([]any)
	if len(items) != 2 {
		t.Fatalf("peeked %d", len(items))
	}
	if pm, _ := servicebus.Decode(items[0].(map[string]any)["message"].([]byte)); body(pm) != "b" || pm.DeliveryTag != nil {
		t.Fatalf("peeked = %+v", pm)
	}
	// peeking locks nothing
	if c := e.counts("q"); c.Active != 3 {
		t.Fatalf("counts = %+v", c)
	}
	if r := mgmt(e, "q", "peek-message", map[string]any{"from-sequence-number": int64(10), "message-count": int32(5)}); status(r) != 204 {
		t.Fatalf("empty peek = %v", r.ApplicationProperties)
	}
	if r := mgmt(e, "nope", "peek-message", nil); status(r) != 404 {
		t.Fatalf("missing entity = %v", r.ApplicationProperties)
	}
}

func TestDeliverFromEventGrid(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{})
	e.svc.CreateTopic("localhost", "t", servicebus.Props{})
	e.svc.CreateSubscription("localhost", "t", "s", servicebus.Props{}, nil)
	id := "/subscriptions/s/resourceGroups/g/providers/Microsoft.ServiceBus/namespaces/%s/%s"
	err := e.h.Deliver(fmt.Sprintf(id, ns, "queues/q"), &core.Message{Body: `{"a":1}`, Subject: "evt",
		Attributes: map[string]core.MessageAttribute{"n": {DataType: "Number", StringValue: "5"}, "s": {DataType: "String", StringValue: "x"}}})
	if err != nil {
		t.Fatal(err)
	}
	// a resource ID names an ARM namespace; the AMQP side may know it as "localhost"
	if err := e.h.Deliver(fmt.Sprintf(id, "elsewhere", "topics/t"), &core.Message{Body: "hi"}); err != nil {
		t.Fatalf("topic delivery: %v", err)
	}
	src, l := e.mustReceiver("q")
	d := e.next(src, l)
	if body(d) != `{"a":1}` || d.Properties.Subject != "evt" || d.ApplicationProperties["n"] != int64(5) || d.ApplicationProperties["s"] != "x" || d.Properties.ContentType != "application/json" {
		t.Fatalf("delivered = %+v %v", d.Properties, d.ApplicationProperties)
	}
	if s, _ := e.svc.GetSubscription("localhost", "t", "s"); s.Active != 1 {
		t.Fatalf("topic delivery = %+v", s)
	}
	if err := e.h.Deliver(fmt.Sprintf(id, ns, "queues/missing"), &core.Message{Body: "x"}); err == nil {
		t.Fatal("delivered to a missing queue")
	}
}

func TestSaveLoadAndConsole(t *testing.T) {
	e := newEnv(t)
	e.queue("q", servicebus.Props{RequiresDuplicateDetection: true})
	e.svc.CreateTopic(ns, "t", servicebus.Props{})
	e.svc.CreateSubscription(ns, "t", "s", servicebus.Props{}, &servicebus.Rule{Name: "r", Filter: servicebus.Filter{Type: "SqlFilter", SQL: "a = 1"}})
	e.send("q", msg("kept", "1"))
	b, err := e.svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	e.svc.Reset()
	if _, ok := e.svc.GetQueue(ns, "q"); ok {
		t.Fatal("reset kept the queue")
	}
	if err := e.svc.Load(b); err != nil {
		t.Fatal(err)
	}
	src, l := e.mustReceiver("q")
	if d := e.next(src, l); body(d) != "kept" {
		t.Fatalf("after load = %+v", d)
	}
	// loaded rules compile on first use
	m := msg("x", "2")
	m.ApplicationProperties = map[string]any{"a": int64(1)}
	e.send("t", m)
	if s, _ := e.svc.GetSubscription(ns, "t", "s"); s.Active != 1 {
		t.Fatalf("rule after load = %+v", s)
	}
	var rows []string
	for _, tb := range e.svc.Resources() {
		for _, r := range tb.Rows {
			rows = append(rows, tb.Name+":"+r[0])
		}
	}
	if got := strings.Join(rows, " "); got != "Namespaces:ns1 Queues:ns1/q Topics:ns1/t Subscriptions:ns1/t/s" {
		t.Fatalf("resources = %s", got)
	}
	if v, _ := e.svc.Inspect("Queues", "ns1/q").(map[string]any); v["messages"] == nil {
		t.Fatalf("inspect queue = %v", v)
	}
	if e.svc.Inspect("Subscriptions", "ns1/t/s") == nil || e.svc.Inspect("Queues", "ns1/none") != nil {
		t.Fatal("inspect")
	}
}

func TestSQL(t *testing.T) {
	m := func() *amqp.Message {
		return &amqp.Message{
			Properties:            &amqp.Properties{MessageID: "m1", Subject: "Order", ContentType: "text/plain", GroupID: "g"},
			ApplicationProperties: map[string]any{"price": int32(20), "name": "widget", "rate": 1.5, "flag": true, "x-y": "dash"},
		}
	}
	for _, c := range []struct {
		sql  string
		want bool
	}{
		{"price = 20", true}, {"price == 20", true}, {"price <> 20", false}, {"price != 21", true}, {"price > 19 AND price < 21", true},
		{"price >= 20 AND price <= 20", true}, {"price > 20 OR name = 'widget'", true}, {"NOT price > 20", true},
		{"(price > 100 OR price < 5) AND name = 'widget'", false},
		{"price + 5 = 25", true}, {"price - 5 = 15", true}, {"price * 2 = 40", true}, {"price / 4 = 5", true}, {"price % 7 = 6", true}, {"-price = -20", true},
		{"rate * 2 = 3", true}, {"rate > 1 AND rate < 2", true},
		{"name IN ('a', 'widget')", true}, {"name NOT IN ('a', 'b')", true}, {"price IN (1, 2)", false},
		{"name LIKE 'wid%'", true}, {"name LIKE '_idget'", true}, {"name LIKE 'w%t'", true}, {"name NOT LIKE 'x%'", true}, {"name LIKE 'wid'", false},
		{"name LIKE 'a!%b' ESCAPE '!'", false},
		{"missing IS NULL", true}, {"name IS NOT NULL", true}, {"missing = 1", false}, {"NOT missing = 1", false},
		{"EXISTS(price)", true}, {"EXISTS(missing)", false}, {"EXISTS(sys.Label)", true},
		{"sys.Label = 'Order'", true}, {"sys.MessageId = 'm1'", true}, {"sys.ContentType LIKE 'text/%'", true}, {"sys.SessionId = 'g'", true},
		{"sys.To IS NULL", true}, {"flag = TRUE", true}, {"flag AND price = 20", true}, {"flag = FALSE", false},
		{"user.price = 20", true}, {"[x-y] = 'dash'", true}, {"@p = name", true}, {"price > @n", true}, {"name = 'it''s'", false},
		{"1 = 1", true}, {"1 = 0", false}, {"price = '20'", false}, {"name = 5", false},
		{"property('name') = 'widget'", true}, {"p('price') = 20", true},
	} {
		got, err := servicebus.Eval(c.sql, m(), map[string]any{"p": "widget", "n": int64(10)})
		if err != nil || got != c.want {
			t.Errorf("%s = %v, %v; want %v", c.sql, got, err, c.want)
		}
	}
	for _, bad := range []string{"price >", "price = = 1", "name IN 'a'", "AND", "price 5", "'open", "price = 1)", "[open",
		strings.Repeat("(", 100000) + "1=1" + strings.Repeat(")", 100000), strings.Repeat("NOT ", 100000) + "1=1", "a = " + strings.Repeat("-", 100000) + "1"} {
		if _, err := servicebus.Eval(bad, m(), nil); err == nil {
			t.Errorf("%q compiled", bad)
		}
	}
	x := m()
	if err := servicebus.Act("SET a = 1; SET b = price + 1, REMOVE name; SET sys.Label = 'L'; SET sys.To = 'dest'", x, nil); err != nil {
		t.Fatal(err)
	}
	if x.ApplicationProperties["a"] != int64(1) || x.ApplicationProperties["b"] != int64(21) || x.ApplicationProperties["name"] != nil ||
		x.Properties.Subject != "L" || x.Properties.To != "dest" {
		t.Fatalf("action = %v %+v", x.ApplicationProperties, x.Properties)
	}
	for _, bad := range []string{"", "SET", "SET a", "x = 1", "SET a = ", "REMOVE"} {
		if err := servicebus.Act(bad, m(), nil); err == nil {
			t.Errorf("action %q compiled", bad)
		}
	}
}
