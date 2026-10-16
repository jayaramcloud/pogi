package amqp_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"

	"github.com/nrndev/pogi/internal/amqp"
)

// --- toy nodes

type settlement struct {
	m *amqp.Message
	o amqp.Outcome
}

// queue is an in-memory node: a Sink that stores what it accepts and a Source
// that hands it out, recording every Next call and settlement.
type queue struct {
	msgs     chan *amqp.Message
	nexts    atomic.Int32
	settled  chan settlement
	accepted chan *amqp.Message
	reject   func(*amqp.Message) error

	mu       sync.Mutex
	nextCtxs []context.Context
}

func newQueue() *queue {
	return &queue{msgs: make(chan *amqp.Message, 64), settled: make(chan settlement, 1024), accepted: make(chan *amqp.Message, 1024)}
}

func (q *queue) Accept(ctx context.Context, l *amqp.Link, m *amqp.Message) error {
	if q.reject != nil {
		if err := q.reject(m); err != nil {
			return err
		}
	}
	q.accepted <- m
	return nil
}

func (q *queue) Next(ctx context.Context, l *amqp.Link) (*amqp.Message, error) {
	q.nexts.Add(1)
	q.mu.Lock()
	q.nextCtxs = append(q.nextCtxs, ctx)
	q.mu.Unlock()
	select {
	case m := <-q.msgs:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *queue) Settle(l *amqp.Link, m *amqp.Message, o amqp.Outcome) { q.settled <- settlement{m, o} }

func (q *queue) settlement(t *testing.T) settlement {
	t.Helper()
	select {
	case s := <-q.settled:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no settlement")
		return settlement{}
	}
}

func (q *queue) noSettlement(t *testing.T) {
	t.Helper()
	select {
	case s := <-q.settled:
		t.Fatalf("unexpected settlement %+v", s.o)
	case <-time.After(150 * time.Millisecond):
	}
}

// harness is a Server on a test listener with toy resolvers.
type harness struct {
	t     *testing.T
	srv   *amqp.Server
	addr  string
	q     *queue
	nodes map[string]any

	mu    sync.Mutex
	conns []*amqp.Conn
	links []*amqp.Link
}

func start(t *testing.T, opt amqp.Options) *harness {
	t.Helper()
	h := &harness{t: t, srv: amqp.NewServer(opt), q: newQueue(), nodes: map[string]any{}}
	h.nodes["q"] = h.q
	h.srv.Add(func(l *amqp.Link) (any, error) {
		h.mu.Lock()
		h.conns = append(h.conns, l.Conn)
		h.links = append(h.links, l)
		h.mu.Unlock()
		if l.Address == "denied" {
			return nil, amqp.Errorf(amqp.Unauthorized, "no access to %s", l.Address)
		}
		return h.nodes[l.Address], nil
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go h.srv.Serve(ln)
	t.Cleanup(h.srv.Close)
	h.addr = "amqp://" + ln.Addr().String()
	return h
}

func (h *harness) lastLink(addr string) *amqp.Link {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		for i := len(h.links) - 1; i >= 0; i-- {
			if h.links[i].Address == addr {
				l := h.links[i]
				h.mu.Unlock()
				return l
			}
		}
		h.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("no link at %s", addr)
	return nil
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func (h *harness) dial(t *testing.T, o *goamqp.ConnOptions) (*goamqp.Conn, *goamqp.Session) {
	t.Helper()
	if o == nil {
		o = &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous()}
	}
	c, err := goamqp.Dial(ctx(t), h.addr, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	s, err := c.NewSession(ctx(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

func sender(t *testing.T, s *goamqp.Session, addr string) *goamqp.Sender {
	t.Helper()
	snd, err := s.NewSender(ctx(t), addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	return snd
}

func receiver(t *testing.T, s *goamqp.Session, addr string, o *goamqp.ReceiverOptions) *goamqp.Receiver {
	t.Helper()
	r, err := s.NewReceiver(ctx(t), addr, o)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func body(m *goamqp.Message) []byte { return bytes.Join(m.Data, nil) }

// --- tests

func TestSendReceive(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	snd := sender(t, s, "q")
	msg := goamqp.NewMessage([]byte("hello"))
	subject, ct := "subj", "text/plain"
	msg.Properties = &goamqp.MessageProperties{MessageID: "id-1", Subject: &subject, ContentType: &ct}
	msg.ApplicationProperties = map[string]any{"n": int64(7), "s": "x"}
	msg.Annotations = goamqp.Annotations{"x-opt-partition-key": "pk"}
	if err := snd.Send(ctx(t), msg, nil); err != nil {
		t.Fatal(err)
	}
	got := <-h.q.accepted
	if string(bytes.Join(got.Data, nil)) != "hello" || got.Properties.MessageID != "id-1" || got.Properties.Subject != "subj" ||
		got.Properties.ContentType != "text/plain" || got.ApplicationProperties["n"] != int64(7) || got.MessageAnnotations["x-opt-partition-key"] != "pk" {
		t.Fatalf("sink got %+v %+v", got, got.Properties)
	}

	// and back out of the Source
	h.q.msgs <- &amqp.Message{Properties: &amqp.Properties{MessageID: "out-1"}, Data: [][]byte{[]byte("world")},
		MessageAnnotations: map[string]any{"x-opt-sequence-number": int64(42)}}
	r := receiver(t, s, "q", nil)
	m, err := r.Receive(ctx(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(body(m)) != "world" || m.Properties.MessageID != "out-1" || m.Annotations["x-opt-sequence-number"] != int64(42) {
		t.Fatalf("receiver got %+v", m)
	}
	if err := r.AcceptMessage(ctx(t), m); err != nil {
		t.Fatal(err)
	}
	if st := h.q.settlement(t); st.o.State != "accepted" || string(st.m.Data[0]) != "world" {
		t.Fatalf("settle %+v", st)
	}
}

func TestCreditLimit(t *testing.T) {
	h := start(t, amqp.Options{})
	for i := range 5 {
		h.q.msgs <- &amqp.Message{Data: [][]byte{{byte(i)}}}
	}
	_, s := h.dial(t, nil)
	r := receiver(t, s, "q", &goamqp.ReceiverOptions{Credit: -1})
	time.Sleep(150 * time.Millisecond)
	if n := h.q.nexts.Load(); n != 0 {
		t.Fatalf("Next called %d times without credit", n)
	}
	if err := r.IssueCredit(2); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := r.Receive(ctx(t), nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond)
	if n := h.q.nexts.Load(); n != 2 {
		t.Fatalf("Next called %d times for 2 credits", n)
	}
	if err := r.IssueCredit(1); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Receive(ctx(t), nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := h.q.nexts.Load(); n != 3 {
		t.Fatalf("Next called %d times for 3 credits", n)
	}
}

func TestDrain(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	r := receiver(t, s, "q", &goamqp.ReceiverOptions{Credit: -1})
	h.q.msgs <- &amqp.Message{Data: [][]byte{[]byte("a")}}
	if err := r.IssueCredit(10); err != nil {
		t.Fatal(err)
	}
	m, err := r.Receive(ctx(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	r.AcceptMessage(ctx(t), m)
	// the remaining 9 credits are drained though the queue is empty
	dctx, cancel := context.WithTimeout(ctx(t), 5*time.Second)
	defer cancel()
	if err := r.DrainCredit(dctx, nil); err != nil {
		t.Fatalf("drain: %v", err)
	}
	// credit is back to zero: a new message isn't pulled until credit is issued
	h.q.msgs <- &amqp.Message{Data: [][]byte{[]byte("b")}}
	n := h.q.nexts.Load()
	time.Sleep(150 * time.Millisecond)
	if h.q.nexts.Load() != n {
		t.Fatal("Next called after drain without credit")
	}
	r.IssueCredit(1)
	if m, err = r.Receive(ctx(t), nil); err != nil || string(body(m)) != "b" {
		t.Fatalf("after drain: %v %v", m, err)
	}
}

func TestOutcomes(t *testing.T) {
	h := start(t, amqp.Options{})
	for i := range 4 {
		h.q.msgs <- &amqp.Message{Data: [][]byte{{byte(i)}}}
	}
	_, s := h.dial(t, nil)
	r := receiver(t, s, "q", &goamqp.ReceiverOptions{Credit: 10})
	var ms []*goamqp.Message
	for range 4 {
		m, err := r.Receive(ctx(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		ms = append(ms, m)
	}
	check := func(i int, f func() error, want settlement) {
		t.Helper()
		if err := f(); err != nil {
			t.Fatal(err)
		}
		got := h.q.settlement(t)
		if got.m.Data[0][0] != byte(i) || got.o.State != want.o.State || got.o.DeliveryFailed != want.o.DeliveryFailed ||
			got.o.UndeliverableHere != want.o.UndeliverableHere || (want.o.Error != nil && (got.o.Error == nil || got.o.Error.Condition != want.o.Error.Condition)) {
			t.Fatalf("message %d settled %+v, want %+v", i, got.o, want.o)
		}
	}
	check(0, func() error { return r.AcceptMessage(ctx(t), ms[0]) }, settlement{o: amqp.Outcome{State: "accepted"}})
	check(1, func() error {
		return r.RejectMessage(ctx(t), ms[1], &goamqp.Error{Condition: "com.microsoft:dead-letter", Description: "bad"})
	}, settlement{o: amqp.Outcome{State: "rejected", Error: &amqp.Error{Condition: "com.microsoft:dead-letter"}}})
	check(2, func() error { return r.ReleaseMessage(ctx(t), ms[2]) }, settlement{o: amqp.Outcome{State: "released"}})
	check(3, func() error {
		return r.ModifyMessage(ctx(t), ms[3], &goamqp.ModifyMessageOptions{DeliveryFailed: true, UndeliverableHere: true})
	}, settlement{o: amqp.Outcome{State: "modified", DeliveryFailed: true, UndeliverableHere: true}})
}

func TestReceiveSecondSettleMode(t *testing.T) {
	h := start(t, amqp.Options{})
	h.q.msgs <- &amqp.Message{Data: [][]byte{[]byte("x")}}
	_, s := h.dial(t, nil)
	r := receiver(t, s, "q", &goamqp.ReceiverOptions{SettlementMode: goamqp.ReceiverSettleModeSecond.Ptr()})
	m, err := r.Receive(ctx(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AcceptMessage(ctx(t), m); err != nil { // returns when the broker confirmed
		t.Fatal(err)
	}
	if st := h.q.settlement(t); st.o.State != "accepted" {
		t.Fatalf("settle %+v", st)
	}
}

func TestPreSettledReceive(t *testing.T) {
	h := start(t, amqp.Options{})
	h.q.msgs <- &amqp.Message{Data: [][]byte{[]byte("fire-and-forget")}}
	_, s := h.dial(t, nil)
	r := receiver(t, s, "q", &goamqp.ReceiverOptions{
		SettlementMode: goamqp.ReceiverSettleModeFirst.Ptr(), RequestedSenderSettleMode: goamqp.SenderSettleModeSettled.Ptr()})
	m, err := r.Receive(ctx(t), nil)
	if err != nil || string(body(m)) != "fire-and-forget" {
		t.Fatalf("%v %v", m, err)
	}
	if !h.lastLink("q").Settled {
		t.Fatal("Link.Settled is false for a pre-settled receiver")
	}
	h.q.noSettlement(t)
}

func TestLargeMessage(t *testing.T) {
	h := start(t, amqp.Options{})
	big := make([]byte, 1<<20)
	rand.Read(big)
	_, s := h.dial(t, &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous(), MaxFrameSize: 16384})
	if err := sender(t, s, "q").Send(ctx(t), goamqp.NewMessage(big), nil); err != nil {
		t.Fatal(err)
	}
	got := <-h.q.accepted
	if !bytes.Equal(bytes.Join(got.Data, nil), big) {
		t.Fatal("1 MiB message arrived changed")
	}
	// out of a Source too, over many frames
	h.q.msgs <- &amqp.Message{Data: [][]byte{big}}
	r := receiver(t, s, "q", nil)
	m, err := r.Receive(ctx(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body(m), big) {
		t.Fatal("1 MiB message came out changed")
	}
	r.AcceptMessage(ctx(t), m)
	h.q.settlement(t)
}

func batch(t *testing.T, msgs ...*goamqp.Message) *goamqp.Message {
	var data [][]byte
	for _, m := range msgs {
		b, err := m.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, b)
	}
	return &goamqp.Message{Data: data, Format: 0x80013700}
}

func TestBatch(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	snd := sender(t, s, "q")
	var in []*goamqp.Message
	for i := range 5 {
		m := goamqp.NewMessage([]byte(fmt.Sprintf("m%d", i)))
		m.ApplicationProperties = map[string]any{"i": int64(i)}
		in = append(in, m)
	}
	b := batch(t, in...)
	b.Annotations = goamqp.Annotations{"x-opt-partition-key": "pk"}
	if err := snd.Send(ctx(t), b, nil); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		got := <-h.q.accepted
		if string(got.Data[0]) != fmt.Sprintf("m%d", i) || got.ApplicationProperties["i"] != int64(i) || got.MessageAnnotations["x-opt-partition-key"] != "pk" {
			t.Fatalf("inner message %d: %+v", i, got)
		}
		if got.Format != 0 {
			t.Fatalf("inner format %x", got.Format)
		}
	}
	select {
	case m := <-h.q.accepted:
		t.Fatalf("extra message %+v", m)
	default:
	}

	// one rejected inner message rejects the whole delivery
	var n int
	h.q.reject = func(m *amqp.Message) error {
		n++
		if n == 2 {
			return amqp.Errorf("com.microsoft:too-big", "no")
		}
		return nil
	}
	err := snd.Send(ctx(t), batch(t, in[:3]...), nil)
	var ae *goamqp.Error
	if !errors.As(err, &ae) || ae.Condition != "com.microsoft:too-big" {
		t.Fatalf("batch send error %v, want rejection", err)
	}
}

func TestRefusedAttach(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	for name, want := range map[string]string{"nosuch": amqp.NotFound, "denied": amqp.Unauthorized} {
		_, err := s.NewSender(ctx(t), name, nil)
		if c := condOf(err); c != want {
			t.Fatalf("send to %s: %#v", name, err)
		}
		_, err = s.NewReceiver(ctx(t), name, nil)
		if c := condOf(err); c != want {
			t.Fatalf("receive from %s: %#v", name, err)
		}
	}
	// the session survives
	sender(t, s, "q")
}

func rpc(t *testing.T, s *goamqp.Session, addr, replyTo string, req *goamqp.Message) *goamqp.Message {
	t.Helper()
	snd := sender(t, s, addr)
	rcv := receiver(t, s, addr, rpcOpts(replyTo))
	if req.Properties == nil {
		req.Properties = &goamqp.MessageProperties{}
	}
	req.Properties.ReplyTo = &replyTo
	if err := snd.Send(ctx(t), req, nil); err != nil {
		t.Fatal(err)
	}
	m, err := rcv.Receive(ctx(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCBS(t *testing.T) {
	var tokens []string
	h := start(t, amqp.Options{Authorize: func(typ, aud, token string) error {
		tokens = append(tokens, typ+"|"+aud+"|"+token)
		if token == "bad" {
			return errors.New("bad token")
		}
		return nil
	}})
	h.nodes["probe"] = h.q
	_, s := h.dial(t, nil)
	put := func(name, token string) *goamqp.Message {
		return rpc(t, s, "$cbs", "cbs-reply", &goamqp.Message{
			Value:                 token,
			Properties:            &goamqp.MessageProperties{MessageID: "m-" + token},
			ApplicationProperties: map[string]any{"operation": "put-token", "type": "servicebus.windows.net:sastoken", "name": name},
		})
	}
	snd := sender(t, s, "probe")
	snd.Send(ctx(t), goamqp.NewMessage([]byte("x")), nil)
	conn := h.lastLink("probe").Conn
	if conn.Authorized("sb://ns/q") {
		t.Fatal("authorized before any token")
	}
	exp := time.Now().Add(time.Hour).Unix()
	resp := put("sb://ns/q", fmt.Sprintf("SharedAccessSignature sr=x&sig=y&se=%d&skn=k", exp))
	if resp.ApplicationProperties["status-code"] != int32(202) || resp.ApplicationProperties["status-description"] == nil || resp.Properties.CorrelationID != "m-"+fmt.Sprintf("SharedAccessSignature sr=x&sig=y&se=%d&skn=k", exp) {
		t.Fatalf("put-token reply %+v %+v", resp.ApplicationProperties, resp.Properties)
	}
	if !conn.Authorized("sb://ns/q") || !conn.Authorized("sb://ns/q/$management") || conn.Authorized("sb://ns/other") {
		t.Fatal("claim not recorded for the audience")
	}
	if len(tokens) != 1 || tokens[0][:24] != "servicebus.windows.net:s" {
		t.Fatalf("Authorize calls %v", tokens)
	}
	// a refused token
	resp = put("sb://ns/z", "bad")
	if resp.ApplicationProperties["status-code"] != int32(401) || conn.Authorized("sb://ns/z") {
		t.Fatalf("bad token reply %+v", resp.ApplicationProperties)
	}
	// an expired one counts for nothing
	resp = put("sb://ns/old", "SharedAccessSignature sr=x&sig=y&se=1000&skn=k")
	if resp.ApplicationProperties["status-code"] != int32(202) || conn.Authorized("sb://ns/old") {
		t.Fatalf("expired token: %+v authorized=%v", resp.ApplicationProperties, conn.Authorized("sb://ns/old"))
	}
}

func TestManagement(t *testing.T) {
	h := start(t, amqp.Options{})
	h.nodes["q/$management"] = amqp.Management(func(ctx context.Context, l *amqp.Link, req *amqp.Message) (*amqp.Message, error) {
		switch op := req.ApplicationProperties["operation"]; op {
		case "echo":
			return &amqp.Message{Value: req.Value, ApplicationProperties: map[string]any{"statusCode": int32(200), "statusDescription": "OK"}}, nil
		case "missing":
			return nil, amqp.Errorf(amqp.NotFound, "no such thing")
		case "boom":
			return nil, errors.New("kaput")
		}
		return nil, amqp.Errorf(amqp.InvalidField, "unknown op")
	})
	_, s := h.dial(t, nil)
	snd := sender(t, s, "q/$management")
	rcv := receiver(t, s, "q/$management", rpcOpts("q-reply-1"))
	call := func(id, op string, val any) *goamqp.Message {
		t.Helper()
		reply := "q-reply-1"
		err := snd.Send(ctx(t), &goamqp.Message{Value: val, Properties: &goamqp.MessageProperties{MessageID: id, ReplyTo: &reply},
			ApplicationProperties: map[string]any{"operation": op}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		m, err := rcv.Receive(ctx(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		if m.Properties.CorrelationID != id {
			t.Fatalf("correlation-id %v, want %s", m.Properties.CorrelationID, id)
		}
		return m
	}
	m := call("id-a", "echo", map[string]any{"k": "v"})
	if !reflect.DeepEqual(m.Value, map[string]any{"k": "v"}) || m.ApplicationProperties["status-code"] != int32(200) ||
		m.ApplicationProperties["statusCode"] != int32(200) || m.ApplicationProperties["status-description"] != "OK" || m.ApplicationProperties["statusDescription"] != "OK" {
		t.Fatalf("echo reply %+v %+v", m.Value, m.ApplicationProperties)
	}
	m = call("id-b", "missing", nil)
	if m.ApplicationProperties["statusCode"] != int32(404) || m.ApplicationProperties["status-code"] != int32(404) || m.ApplicationProperties["statusDescription"] != "no such thing" {
		t.Fatalf("not-found reply %+v", m.ApplicationProperties)
	}
	if m = call("id-c", "boom", nil); m.ApplicationProperties["status-code"] != int32(500) {
		t.Fatalf("error reply %+v", m.ApplicationProperties)
	}
	// a second pair of links, on another reply address, is paired independently
	snd2 := sender(t, s, "q/$management")
	rcv2 := receiver(t, s, "q/$management", rpcOpts("q-reply-2"))
	reply2 := "q-reply-2"
	snd2.Send(ctx(t), &goamqp.Message{Value: "two", Properties: &goamqp.MessageProperties{MessageID: "id-d", ReplyTo: &reply2},
		ApplicationProperties: map[string]any{"operation": "echo"}}, nil)
	m2, err := rcv2.Receive(ctx(t), nil)
	if err != nil || m2.Properties.CorrelationID != "id-d" || m2.Value != "two" {
		t.Fatalf("second pair: %v %v", m2, err)
	}
}

func TestLinkCloseStolen(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	r := receiver(t, s, "q", nil)
	l := h.lastLink("q")
	errc := make(chan error, 1)
	go func() { _, err := r.Receive(ctx(t), nil); errc <- err }()
	time.Sleep(100 * time.Millisecond)
	l.Close(amqp.Errorf(amqp.Stolen, "a newer receiver attached"))
	select {
	case err := <-errc:
		var le *goamqp.LinkError
		if !errors.As(err, &le) || le.RemoteErr == nil || le.RemoteErr.Condition != goamqp.ErrCondStolen {
			t.Fatalf("Receive error %#v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not return")
	}
	// a sending link closes the same way
	snd := sender(t, s, "q")
	h.lastLink("q").Close(amqp.Errorf(amqp.Stolen, "gone"))
	var err error
	for range 50 {
		if err = snd.Send(ctx(t), goamqp.NewMessage([]byte("x")), nil); err != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var le *goamqp.LinkError
	if !errors.As(err, &le) || le.RemoteErr == nil || le.RemoteErr.Condition != goamqp.ErrCondStolen {
		t.Fatalf("Send error %#v", err)
	}
}

func TestDetachAndConnCloseRelease(t *testing.T) {
	h := start(t, amqp.Options{})
	for i := range 2 {
		h.q.msgs <- &amqp.Message{Data: [][]byte{{byte(i)}}}
	}
	conn, s := h.dial(t, nil)
	r := receiver(t, s, "q", &goamqp.ReceiverOptions{Credit: 10})
	for range 2 {
		if _, err := r.Receive(ctx(t), nil); err != nil { // never settled
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond) // the third Next is blocked now
	h.q.mu.Lock()
	blocked := h.q.nextCtxs[len(h.q.nextCtxs)-1]
	h.q.mu.Unlock()
	if blocked.Err() != nil {
		t.Fatal("ctx of Next done too early")
	}
	// detach: the link's deliveries are released and Next's ctx ends
	if err := r.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		st := h.q.settlement(t)
		if st.o.State != "released" || st.m.Data[0][0] != byte(i) {
			t.Fatalf("detach settlement %d: %+v", i, st.o)
		}
	}
	select {
	case <-blocked.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Next ctx not cancelled on detach")
	}

	// connection close does the same
	for i := range 2 {
		h.q.msgs <- &amqp.Message{Data: [][]byte{{byte(10 + i)}}}
	}
	r2 := receiver(t, s, "q", &goamqp.ReceiverOptions{Credit: 10})
	for range 2 {
		if _, err := r2.Receive(ctx(t), nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	h.q.mu.Lock()
	blocked = h.q.nextCtxs[len(h.q.nextCtxs)-1]
	h.q.mu.Unlock()
	conn.Close()
	for i := range 2 {
		st := h.q.settlement(t)
		if st.o.State != "released" || st.m.Data[0][0] != byte(10+i) {
			t.Fatalf("conn close settlement %d: %+v", i, st.o)
		}
	}
	select {
	case <-blocked.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Next ctx not cancelled on connection close")
	}
}

func TestIdleTimeoutHeartbeat(t *testing.T) {
	h := start(t, amqp.Options{})
	// the client drops the connection if the broker is silent for 600ms
	conn, s := h.dial(t, &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous(), IdleTimeout: 600 * time.Millisecond})
	time.Sleep(1800 * time.Millisecond)
	select {
	case <-conn.Done():
		t.Fatalf("client closed the connection: %v", conn.Err())
	default:
	}
	if err := sender(t, s, "q").Send(ctx(t), goamqp.NewMessage([]byte("still here")), nil); err != nil {
		t.Fatal(err)
	}
}

func TestIdlePeerClosed(t *testing.T) {
	h := start(t, amqp.Options{IdleTimeout: 300 * time.Millisecond})
	nc, err := net.Dial("tcp", h.addr[len("amqp://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.Write([]byte("AMQP\x00\x01\x00\x00"))
	open, _ := amqp.Encode(nil, amqp.Described{Descriptor: uint64(0x10), Value: []any{"raw-client"}})
	frame := append([]byte{0, 0, 0, byte(8 + len(open)), 2, 0, 0, 0}, open...)
	nc.Write(frame)
	start := time.Now()
	nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadAll(nc); err != nil { // header, open, then a close frame, then EOF
		t.Fatalf("read: %v", err)
	}
	if d := time.Since(start); d < 250*time.Millisecond || d > 3*time.Second {
		t.Fatalf("idle client dropped after %v", d)
	}
}

func TestSASL(t *testing.T) {
	h := start(t, amqp.Options{Login: func(u, p string) bool { return u == "u" && p == "p" }})
	if _, err := goamqp.Dial(ctx(t), h.addr, &goamqp.ConnOptions{SASLType: goamqp.SASLTypePlain("u", "wrong")}); err == nil {
		t.Fatal("wrong password accepted")
	}
	c, err := goamqp.Dial(ctx(t), h.addr, &goamqp.ConnOptions{SASLType: goamqp.SASLTypePlain("u", "p")})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, _ := c.NewSession(ctx(t), nil)
	sender(t, s, "q")
	if h.conns[0].User != "u" {
		t.Fatalf("Conn.User %q", h.conns[0].User)
	}
	// ANONYMOUS is always offered
	c2, err := goamqp.Dial(ctx(t), h.addr, &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous()})
	if err != nil {
		t.Fatal(err)
	}
	c2.Close()
}

func TestManyConcurrentSenders(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snd := sender(t, s, "q")
			for j := range 100 { // more than one credit window
				if err := snd.Send(ctx(t), goamqp.NewMessage([]byte(fmt.Sprintf("%d-%d", i, j))), nil); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n := len(h.q.accepted); n != 400 {
		t.Fatalf("%d accepted, want 400", n)
	}
}

func asLinkErr(err error, le **goamqp.LinkError) bool { return errors.As(err, le) }

// rpcOpts are the options the Azure SDKs give a response link.
func rpcOpts(replyTo string) *goamqp.ReceiverOptions {
	return &goamqp.ReceiverOptions{TargetAddress: replyTo, Credit: 100, SettlementMode: goamqp.ReceiverSettleModeFirst.Ptr(),
		RequestedSenderSettleMode: goamqp.SenderSettleModeSettled.Ptr()}
}

// condOf is the AMQP condition the peer sent in err.
func condOf(err error) string {
	var le *goamqp.LinkError
	var ae *goamqp.Error
	switch {
	case errors.As(err, &le) && le.RemoteErr != nil:
		return string(le.RemoteErr.Condition)
	case errors.As(err, &ae):
		return string(ae.Condition)
	}
	return ""
}
