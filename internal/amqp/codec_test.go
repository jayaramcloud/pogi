package amqp_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"

	"github.com/nrndev/pogi/internal/amqp"
)

func big(n int) []any {
	l := make([]any, n)
	for i := range l {
		l[i] = int64(i * 1000)
	}
	return l
}

func TestCodecRoundTrip(t *testing.T) {
	ts := time.UnixMilli(1700000000123).UTC()
	bigMap := map[string]any{}
	for i := range 60 {
		bigMap[strings.Repeat("k", i+1)] = int32(i)
	}
	cases := []struct {
		name string
		in   any
	}{
		{"null", nil}, {"true", true}, {"false", false},
		{"ubyte", uint8(200)},
		{"ushort", uint16(60000)},
		{"uint0", uint32(0)}, {"smalluint", uint32(7)}, {"uint", uint32(70000)},
		{"ulong0", uint64(0)}, {"smallulong", uint64(7)}, {"ulong", uint64(1 << 40)},
		{"byte", int8(-5)},
		{"short", int16(-300)},
		{"smallint", int32(-7)}, {"int", int32(-70000)},
		{"smalllong", int64(100)}, {"long", int64(-1 << 40)},
		{"float", float32(1.5)}, {"double", 2.25},
		{"char", amqp.Char('λ')},
		{"timestamp", ts},
		{"uuid", amqp.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"vbin8", []byte("abc")}, {"vbin32", []byte(strings.Repeat("b", 300))}, {"empty binary", []byte{}},
		{"str8", "héllo"}, {"str32", strings.Repeat("s", 300)}, {"empty string", ""},
		{"sym8", amqp.Symbol("x-opt-offset")}, {"sym32", amqp.Symbol(strings.Repeat("y", 300))},
		{"decimal32", amqp.Decimal{1, 2, 3, 4}}, {"decimal64", amqp.Decimal{1, 2, 3, 4, 5, 6, 7, 8}},
		{"decimal128", amqp.Decimal(make([]byte, 16))},
		{"described ulong", amqp.Described{Descriptor: uint64(0x468C00000004), Value: "amqp.annotation.x-opt-offset > '12'"}},
		{"described symbol", amqp.Described{Descriptor: amqp.Symbol("apache.org:selector-filter:string"), Value: []any{int64(1), "a"}}},
		{"list0", []any{}}, {"list8", []any{int32(1), "two", true, nil, []any{amqp.Symbol("n")}}}, {"list32", big(200)},
		{"map8", map[string]any{"a": int64(1), "b": []byte("x")}},
		{"map32", bigMap},
		{"map any keys", map[any]any{int64(1): "one", true: "yes"}},
		{"nested", map[string]any{"l": []any{map[string]any{"deep": ts}}, "m": map[any]any{uint32(2): amqp.UUID{9}}}},
		{"array of strings", []string{"0", "1", "2"}},
		{"array of symbols", []amqp.Symbol{"a", "bb"}},
		{"array of longs", []int64{1, -2, 1 << 40}},
		{"array of ints", []int32{1, -2, 70000}},
		{"array of uints", []uint32{1, 2}},
		{"array of ulongs", []uint64{1, 2}},
		{"array of bools", []bool{true, false}},
		{"array of doubles", []float64{1.5, 2}},
		{"array of shorts", []int16{1, -2}},
		{"array of uuids", []amqp.UUID{{1}, {2}}},
		{"array of timestamps", []time.Time{ts, ts.Add(time.Second)}},
		{"array of binary", amqp.Array{[]byte("a"), []byte("bcd")}},
		{"empty string array", []string{}},
		{"big string array", func() any {
			s := make([]string, 100)
			for i := range s {
				s[i] = strings.Repeat("p", i%7+1)
			}
			return s
		}()},
	}
	for _, c := range cases {
		b, err := amqp.Encode(nil, c.in)
		if err != nil {
			t.Fatalf("%s: encode: %v", c.name, err)
		}
		got, rest, err := amqp.Decode(append(b, 0xde))
		if err != nil {
			t.Fatalf("%s: decode: %v", c.name, err)
		}
		if len(rest) != 1 {
			t.Fatalf("%s: %d bytes left, want 1", c.name, len(rest))
		}
		if !reflect.DeepEqual(got, c.in) {
			t.Errorf("%s: got %#v, want %#v", c.name, got, c.in)
		}
	}
	// symbol-keyed maps come back with string keys; wire widths
	b, _ := amqp.Encode(nil, map[amqp.Symbol]any{"k": amqp.Symbol("v")})
	if got, _, _ := amqp.Decode(b); !reflect.DeepEqual(got, map[string]any{"k": amqp.Symbol("v")}) {
		t.Errorf("symbol keys: %#v", got)
	}
	for _, c := range []struct {
		in   any
		code byte
	}{{int32(200), 0x71}, {int32(5), 0x54}, {int64(5), 0x55}, {int64(500), 0x81}, {uint32(500), 0x70}, {uint64(500), 0x80}, {int(5), 0x55}, {[]string{"a"}, 0xe0}} {
		if b, _ := amqp.Encode(nil, c.in); b[0] != c.code {
			t.Errorf("%T(%v) encoded with constructor 0x%02x, want 0x%02x", c.in, c.in, b[0], c.code)
		}
	}
	if v, _, _ := amqp.Decode(mustEncode(t, int(7))); v != int64(7) {
		t.Errorf("int decodes to %T", v)
	}
	// garbage doesn't panic
	for _, b := range [][]byte{{}, {0xa1}, {0xa1, 5, 'a'}, {0xd0, 0xff, 0xff, 0xff, 0xff}, {0xc1, 3, 1, 0x40}, {0xff}, {0x00, 0x40}, {0xe0, 2, 1, 0x71}} {
		if _, _, err := amqp.Decode(b); err == nil {
			t.Errorf("decoded % x", b)
		}
	}
}

func mustEncode(t *testing.T, v any) []byte {
	b, err := amqp.Encode(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMessageRoundTrip(t *testing.T) {
	ts := time.UnixMilli(1700000000000).UTC()
	id := amqp.UUID{1, 2, 3}
	in := &amqp.Message{
		Header:              &amqp.Header{Durable: true, Priority: 7, TTL: 90 * time.Second, FirstAcquirer: true, DeliveryCount: 3},
		DeliveryAnnotations: map[string]any{"x-opt-delivery": "d"},
		MessageAnnotations: map[string]any{"x-opt-sequence-number": int64(9), "x-opt-enqueued-time": ts,
			"x-opt-locked-until": ts.Add(time.Minute), "x-opt-partition-key": "pk", "x-opt-offset": "128"},
		Properties: &amqp.Properties{MessageID: "m1", UserID: []byte("u"), To: "to", Subject: "s", ReplyTo: "rt", CorrelationID: id,
			ContentType: "application/json", ContentEncoding: "gzip", AbsoluteExpiryTime: ts, CreationTime: ts, GroupID: "g",
			GroupSequence: 4, ReplyToGroupID: "rg"},
		ApplicationProperties: map[string]any{"i32": int32(1), "i64": int64(2), "u32": uint32(3), "b": true, "s": "x", "bin": []byte("y")},
		Data:                  [][]byte{[]byte("one"), []byte("two")},
		Footer:                map[string]any{"f": "v"},
	}
	b, err := in.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	out, err := amqp.Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("\n got %+v %+v %+v\nwant %+v %+v %+v", out, out.Header, out.Properties, in, in.Header, in.Properties)
	}
	for _, m := range []*amqp.Message{
		{Value: map[string]any{"a": []any{int64(1)}}},
		{Value: "text"},
		{Sequence: [][]any{{int64(1), "a"}, {true}}},
		{Value: nil},
	} {
		b, _ := m.Marshal()
		got, err := amqp.Unmarshal(b)
		if err != nil || !reflect.DeepEqual(got, m) {
			t.Fatalf("body round trip: %+v %v", got, err)
		}
	}
	// an unset header priority is the default, 4
	b, _ = (&amqp.Message{Header: &amqp.Header{Priority: 4}, Value: "x"}).Marshal()
	if m, _ := amqp.Unmarshal(b); m.Header.Priority != 4 {
		t.Fatal("priority")
	}
	if _, err := amqp.Unmarshal([]byte{0x00, 0x53, 0x75}); err == nil {
		t.Fatal("truncated section decoded")
	}
}

// detachNode is a queue that counts Detached calls.
type detachNode struct {
	*queue
	detached chan *amqp.Link
}

func (d *detachNode) Detached(l *amqp.Link) { d.detached <- l }

func TestDetacher(t *testing.T) {
	h := start(t, amqp.Options{})
	d := &detachNode{queue: newQueue(), detached: make(chan *amqp.Link, 8)}
	h.nodes["d"] = d
	h.q.msgs <- &amqp.Message{Data: [][]byte{[]byte("x")}}
	d.msgs <- &amqp.Message{Data: [][]byte{[]byte("x")}}

	wait := func(what string) {
		t.Helper()
		select {
		case <-d.detached:
		case <-time.After(3 * time.Second):
			t.Fatalf("Detached not called after %s", what)
		}
		select {
		case <-d.detached:
			t.Fatalf("Detached called twice after %s", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
	conn, s := h.dial(t, nil)
	// client detach, of a Source link with an unsettled delivery: released before Detached
	r := receiver(t, s, "d", nil)
	if _, err := r.Receive(ctx(t), nil); err != nil {
		t.Fatal(err)
	}
	r.Close(ctx(t))
	select {
	case st := <-d.settled:
		if st.o.State != "released" {
			t.Fatalf("settled %+v", st.o)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("not released")
	}
	wait("client detach of a Source")
	// client detach of a Sink
	snd := sender(t, s, "d")
	snd.Close(ctx(t))
	wait("client detach of a Sink")
	// broker-side Link.Close
	receiver(t, s, "d", nil)
	h.lastLink("d").Close(amqp.Errorf(amqp.Stolen, "x"))
	wait("Link.Close")
	// connection close
	receiver(t, s, "d", nil)
	sender(t, s, "d")
	conn.Close()
	for range 2 {
		select {
		case <-d.detached:
		case <-time.After(3 * time.Second):
			t.Fatal("Detached not called on connection close")
		}
	}
}

// A Resolver may close links of any connection, including one still being
// attached, without deadlocking.
func TestCloseFromResolver(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s1 := h.dial(t, nil)
	r1 := receiver(t, s1, "q", nil)
	victim := h.lastLink("q")
	h.srv.Add(func(l *amqp.Link) (any, error) {
		if l.Address == "kick" {
			victim.Close(amqp.Errorf(amqp.Stolen, "kicked"))
			l.Close(nil) // its own, not yet attached
			return h.q, nil
		}
		return nil, nil
	})
	_, s2 := h.dial(t, nil)
	done := make(chan struct{})
	go func() { receiver(t, s2, "kick", nil); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock")
	}
	_, err := r1.Receive(ctx(t), nil)
	var le *goamqp.LinkError
	if !asLinkErr(err, &le) || le.RemoteErr == nil || le.RemoteErr.Condition != goamqp.ErrCondStolen {
		t.Fatalf("victim: %v", err)
	}
}

func TestAttachPropertiesAndBatchAnnotations(t *testing.T) {
	h := start(t, amqp.Options{})
	_, s := h.dial(t, nil)
	snd, err := s.NewSender(ctx(t), "q", &goamqp.SenderOptions{Properties: map[string]any{"com.microsoft:epoch": int64(5), "n": int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	p := h.lastLink("q").Properties
	if p["com.microsoft:epoch"] != int64(5) || p["n"] != int32(1) {
		t.Fatalf("attach properties %#v", p)
	}
	// each inner message keeps its own annotations; the envelope's fill gaps
	a, b := goamqp.NewMessage([]byte("a")), goamqp.NewMessage([]byte("b"))
	a.Annotations = goamqp.Annotations{"x-opt-partition-key": "inner-a"}
	env := batch(t, a, b)
	env.Annotations = goamqp.Annotations{"x-opt-partition-key": "outer", "x-opt-other": "o"}
	if err := snd.Send(ctx(t), env, nil); err != nil {
		t.Fatal(err)
	}
	ga, gb := <-h.q.accepted, <-h.q.accepted
	if ga.MessageAnnotations["x-opt-partition-key"] != "inner-a" || ga.MessageAnnotations["x-opt-other"] != "o" ||
		gb.MessageAnnotations["x-opt-partition-key"] != "outer" {
		t.Fatalf("annotations: %v / %v", ga.MessageAnnotations, gb.MessageAnnotations)
	}
}

func TestWireTypesToClient(t *testing.T) {
	h := start(t, amqp.Options{})
	h.nodes["q/$management"] = amqp.Management(func(ctx context.Context, l *amqp.Link, req *amqp.Message) (*amqp.Message, error) {
		return &amqp.Message{Value: map[string]any{"partition_ids": []string{"0", "1"}},
			ApplicationProperties: map[string]any{"status-code": int32(200), "u": uint32(4)}}, nil
	})
	_, s := h.dial(t, nil)
	m := rpc(t, s, "q/$management", "tr", &goamqp.Message{Value: "x", Properties: &goamqp.MessageProperties{MessageID: "1"}})
	ids, _ := m.Value.(map[string]any)["partition_ids"].([]string)
	if !reflect.DeepEqual(ids, []string{"0", "1"}) || m.ApplicationProperties["status-code"] != int32(200) || m.ApplicationProperties["u"] != uint32(4) {
		t.Fatalf("%#v %#v", m.Value, m.ApplicationProperties)
	}
}
