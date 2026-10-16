package eventhubs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

func start(t *testing.T) (*core.Hub, *Service) {
	t.Helper()
	var s *Service
	h, _ := core.ServeTest(t, func(h *core.Hub) core.Service { s = New(h); return s })
	return h, s
}

func link(addr string, receiving bool, props map[string]any, filter string) *amqp.Link {
	l := &amqp.Link{Conn: &amqp.Conn{Host: "127.0.0.1"}, Name: "link-" + addr, Address: addr, Receiving: receiving, Settled: true, Properties: props}
	if filter != "" {
		l.Filter = map[string]any{"apache.org:selector-filter:string": filter}
	}
	return l
}

func sink(t *testing.T, h *core.Hub, addr string) amqp.Sink {
	t.Helper()
	n, err := h.AMQP().Resolve(link(addr, false, nil, ""))
	if err != nil {
		t.Fatalf("send %s: %v", addr, err)
	}
	return n.(amqp.Sink)
}

func source(t *testing.T, h *core.Hub, addr string, props map[string]any, filter string) (amqp.Source, *amqp.Link) {
	t.Helper()
	l := link(addr, true, props, filter)
	n, err := h.AMQP().Resolve(l)
	if err != nil {
		t.Fatalf("receive %s: %v", addr, err)
	}
	return n.(amqp.Source), l
}

func send(t *testing.T, snk amqp.Sink, body string, key string) {
	t.Helper()
	m := &amqp.Message{Data: [][]byte{[]byte(body)}, Properties: &amqp.Properties{MessageID: body}}
	if key != "" {
		m.MessageAnnotations = map[string]any{"x-opt-partition-key": key}
	}
	if err := snk.Accept(context.Background(), nil, m); err != nil {
		t.Fatal(err)
	}
}

func next(t *testing.T, src amqp.Source, l *amqp.Link) *amqp.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := src.Next(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// drain reads what is there and returns the bodies.
func drain(src amqp.Source, l *amqp.Link) (out []string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		m, err := src.Next(ctx, l)
		cancel()
		if err != nil {
			return out
		}
		out = append(out, string(m.Data[0]))
	}
}

func wantCond(t *testing.T, err error, cond string) {
	t.Helper()
	var ae *amqp.Error
	if !errors.As(err, &ae) || ae.Condition != cond {
		t.Fatalf("want %s, got %v", cond, err)
	}
}

func TestControlPlane(t *testing.T) {
	_, s := start(t)
	if got := s.Namespaces(); len(got) != 1 || got[0] != "localhost" {
		t.Fatalf("namespaces %v", got)
	}
	for _, c := range []struct {
		err  error
		want error
	}{
		{s.CreateNamespace("bad name"), ErrInvalid},
		{s.CreateNamespace("localhost"), ErrExists},
		{s.CreateHub("nope", "h", 0, 0), ErrNotFound},
		{s.CreateHub("localhost", "h", 33, 0), ErrInvalid},
		{s.DeleteNamespace("nope"), ErrNotFound},
	} {
		if !errors.Is(c.err, c.want) {
			t.Errorf("got %v, want %v", c.err, c.want)
		}
	}
	if err := s.CreateNamespace("Orders"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateHub("orders", "Events", 8, 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateHub("orders", "events", 0, 0); !errors.Is(err, ErrExists) {
		t.Fatalf("names are case-insensitive: %v", err)
	}
	hi, err := s.GetHub("orders", "EVENTS")
	if err != nil || hi.Name != "Events" || len(hi.PartitionIDs) != 8 || hi.Retention != 48*time.Hour || hi.ConsumerGroups[0] != DefaultConsumerGroup {
		t.Fatalf("%+v %v", hi, err)
	}
	if err := s.CreateHub("orders", "defaults", 0, 0); err != nil {
		t.Fatal(err)
	}
	if hi, _ := s.GetHub("orders", "defaults"); len(hi.PartitionIDs) != 4 || hi.Retention != 24*time.Hour {
		t.Fatalf("defaults: %+v", hi)
	}
	if err := s.CreateConsumerGroup("orders", "events", "billing"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateConsumerGroup("orders", "events", "BILLING"); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	if err := s.CreateConsumerGroup("orders", "events", "$default"); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	if err := s.DeleteConsumerGroup("orders", "events", "$Default"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("$Default stays: %v", err)
	}
	if err := s.DeleteConsumerGroup("orders", "events", "billing"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteConsumerGroup("orders", "events", "billing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if hubs, _ := s.Hubs("orders"); len(hubs) != 2 {
		t.Fatalf("hubs %v", hubs)
	}
	if err := s.DeleteHub("orders", "events"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetHub("orders", "events"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.DeleteNamespace("orders"); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceOf(t *testing.T) {
	for host, want := range map[string]string{"localhost": "localhost", "127.0.0.1": "localhost", "127.0.0.1:5671": "localhost", "[::1]": "localhost",
		"Orders.servicebus.windows.net": "orders", "orders.eventhubs.localhost:7644": "orders", "": "localhost"} {
		if got := namespaceOf(host); got != want {
			t.Errorf("%q: %q, want %q", host, got, want)
		}
	}
}

func TestSendPartitioning(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 4, 0)
	snk := sink(t, h, "hub")
	for i := range 40 {
		send(t, snk, "same-"+strconv.Itoa(i), "key-A")
	}
	for i := range 40 {
		send(t, snk, "rr-"+strconv.Itoa(i), "")
	}
	send(t, sink(t, h, "hub/Partitions/3"), "explicit", "")
	counts := map[string]int{}
	keyed := map[string]bool{}
	s.mu.Lock()
	for _, p := range s.st.Namespaces["localhost"].Hubs["hub"].Partitions {
		counts[p.ID] = len(p.Events)
		for _, e := range p.Events {
			if strings.HasPrefix(string(e.Msg.Data[0]), "same-") {
				keyed[p.ID] = true
				if e.Key != "key-A" {
					t.Errorf("key %q", e.Key)
				}
			}
		}
	}
	s.mu.Unlock()
	if len(keyed) != 1 {
		t.Fatalf("one key must stay on one partition: %v", keyed)
	}
	sum := 0
	for id, n := range counts {
		sum += n
		if n < 10 {
			t.Errorf("partition %s got only %d events: %v", id, n, counts) // 40 round-robin over 4, plus the key's 40 on one
		}
	}
	if sum != 81 {
		t.Fatalf("sum %d", sum)
	}
	// the same key on a partition-bound link keeps its explicit partition
	send(t, sink(t, h, "hub/Partitions/0"), "pinned", "key-A")
	_, err := h.AMQP().Resolve(link("hub/Partitions/9", false, nil, ""))
	if err != nil {
		t.Fatalf("a bad partition is refused when sending: %v", err) // the link attaches, the first send fails
	}
	if err := sink(t, h, "hub/Partitions/9").Accept(context.Background(), nil, &amqp.Message{Data: [][]byte{{1}}}); err == nil {
		t.Fatal("sent to a partition that does not exist")
	} else {
		wantCond(t, err, argRange)
	}
}

func TestReceivePositions(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 2, 0)
	snk := sink(t, h, "hub/Partitions/1")
	for i := range 5 {
		send(t, snk, "e"+strconv.Itoa(i), "pk")
		time.Sleep(3 * time.Millisecond)
	}
	addr := "hub/ConsumerGroups/$Default/Partitions/1"
	read := func(filter string) []string {
		src, l := source(t, h, addr, nil, filter)
		defer src.(amqp.Detacher).Detached(l)
		return drain(src, l)
	}
	if got := read(""); strings.Join(got, ",") != "e0,e1,e2,e3,e4" {
		t.Fatalf("no filter: %v", got)
	}
	if got := read("amqp.annotation.x-opt-offset > '-1'"); len(got) != 5 {
		t.Fatalf("earliest: %v", got)
	}
	if got := read("amqp.annotation.x-opt-offset > '@latest'"); len(got) != 0 {
		t.Fatalf("latest: %v", got)
	}
	// first message: annotations and their types
	src, l := source(t, h, addr, nil, "amqp.annotation.x-opt-offset > '-1'")
	var msgs []*amqp.Message
	for range 5 {
		msgs = append(msgs, next(t, src, l))
	}
	for i, m := range msgs {
		a := m.MessageAnnotations
		if a["x-opt-sequence-number"] != int64(i) || a["x-opt-partition-key"] != "pk" {
			t.Fatalf("%d: %v", i, a)
		}
		if _, ok := a["x-opt-offset"].(string); !ok {
			t.Fatalf("offset must be a string: %T", a["x-opt-offset"])
		}
		if tm, ok := a["x-opt-enqueued-time"].(time.Time); !ok || time.Since(tm) > time.Minute {
			t.Fatalf("enqueued time %v", a["x-opt-enqueued-time"])
		}
		if m.Properties.MessageID != "e"+strconv.Itoa(i) {
			t.Fatalf("properties lost: %+v", m.Properties)
		}
	}
	off2 := msgs[2].MessageAnnotations["x-opt-offset"].(string)
	if msgs[2].MessageAnnotations["x-opt-offset"] == msgs[3].MessageAnnotations["x-opt-offset"] {
		t.Fatal("offsets must differ")
	}
	for filter, want := range map[string]string{
		"amqp.annotation.x-opt-offset > '" + off2 + "'":                "e3,e4",
		"amqp.annotation.x-opt-offset >= '" + off2 + "'":               "e2,e3,e4",
		"amqp.annotation.x-opt-sequence-number > '1'":                  "e2,e3,e4",
		"amqp.annotation.x-opt-sequence-number >= '1'":                 "e1,e2,e3,e4",
		"amqp.annotation.x-opt-sequence-number > '4'":                  "",
		"amqp.annotation.x-opt-sequence-number >= '0'":                 "e0,e1,e2,e3,e4",
		"amqp.annotation.x-opt-sequence-number > '-1'":                 "e0,e1,e2,e3,e4",
		"amqp.annotation.x-opt-enqueued-time > '0'":                    "e0,e1,e2,e3,e4",
		"amqp.annotation.x-opt-enqueued-time >= '" + ms(msgs[3]) + "'": "e3,e4",
		"amqp.annotation.x-opt-enqueued-time > '" + ms(msgs[4]) + "'":  "",
		"amqp.annotation.x-opt-enqueued-time > '99999999999999'":       "",
	} {
		if got := strings.Join(read(filter), ","); got != want {
			t.Errorf("%s: got %q, want %q", filter, got, want)
		}
	}
	if _, err := h.AMQP().Resolve(link(addr, true, nil, "amqp.annotation.x-opt-offset > 'abc'")); err == nil {
		t.Fatal("a bad offset is refused")
	}
	// another partition is empty
	if got := read2(t, h, "hub/ConsumerGroups/$Default/Partitions/0"); len(got) != 0 {
		t.Fatalf("partition 0: %v", got)
	}
}

func read2(t *testing.T, h *core.Hub, addr string) []string {
	src, l := source(t, h, addr, nil, "")
	defer src.(amqp.Detacher).Detached(l)
	return drain(src, l)
}

func ms(m *amqp.Message) string {
	return strconv.FormatInt(m.MessageAnnotations["x-opt-enqueued-time"].(time.Time).UnixMilli(), 10)
}

func TestNextBlocks(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 1, 0)
	src, l := source(t, h, "hub/ConsumerGroups/$Default/Partitions/0", nil, "amqp.annotation.x-opt-offset > '@latest'")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if _, err := src.Next(ctx, l); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("an empty partition blocks: %v", err)
	}
	cancel()
	got := make(chan *amqp.Message)
	go func() { got <- next(t, src, l) }()
	time.Sleep(50 * time.Millisecond)
	send(t, sink(t, h, "hub"), "wake", "")
	select {
	case m := <-got:
		if string(m.Data[0]) != "wake" {
			t.Fatal(m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not wake")
	}
}

func TestResolve(t *testing.T) {
	_, s := start(t)
	s.CreateHub("localhost", "hub", 2, 0)
	for _, c := range []struct {
		addr      string
		receiving bool
		cond      string // "" ok, "none" unresolved
	}{
		{"hub", false, ""},
		{"sb://localhost/hub/Partitions/1", false, ""},
		{"hub/$management", true, ""},
		{"$management", false, ""},
		{"queue1", false, "none"},
		{"queue1/ConsumerGroups/$Default/Partitions/0", true, "none"},
		{"hub/ConsumerGroups/nope/Partitions/0", true, amqp.NotFound},
		{"hub/ConsumerGroups/$Default/Partitions/7", true, argRange},
		{"hub/ConsumerGroups/$Default", true, amqp.NotFound},
		{"hub/ConsumerGroups/$default/Partitions/1", true, ""},
	} {
		n, err := s.resolve(link(c.addr, c.receiving, nil, ""))
		switch c.cond {
		case "":
			if err != nil || n == nil {
				t.Errorf("%s: %v %v", c.addr, n, err)
			}
		case "none":
			if err != nil || n != nil {
				t.Errorf("%s: %v %v", c.addr, n, err)
			}
		default:
			wantCond(t, err, c.cond)
		}
	}
	// another namespace
	l := link("hub", false, nil, "")
	l.Conn.Host = "other.servicebus.windows.net"
	if n, err := s.resolve(l); n != nil || err != nil {
		t.Fatalf("other namespace: %v %v", n, err)
	}
}

func TestEpochs(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 2, 0)
	addr := "hub/ConsumerGroups/$Default/Partitions/0"
	epoch := func(e int64, name string) map[string]any {
		return map[string]any{"com.microsoft:epoch": e, "com.microsoft:receiver-name": name}
	}
	attach := func(props map[string]any) (amqp.Source, error) {
		n, err := h.AMQP().Resolve(link(addr, true, props, ""))
		if err != nil {
			return nil, err
		}
		if r := n.(*receiver); r.gone { // refused: accepted, then detached with the error on first use
			_, err := r.Next(context.Background(), nil)
			return nil, err
		}
		return n.(amqp.Source), nil
	}
	gone := func(src amqp.Source) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return src.(*receiver).gone
	}

	a, err := attach(epoch(5, "A"))
	if err != nil {
		t.Fatal(err)
	}
	// lower epoch is refused, with the condition the SDK reads as ownership lost
	_, err = attach(epoch(4, "B"))
	wantCond(t, err, amqp.Stolen)
	if !strings.Contains(err.Error(), "increasing epoch") {
		t.Fatalf("message: %v", err)
	}
	// a non-epoch receiver is refused while an epoch receiver holds the partition
	_, err = attach(map[string]any{"com.microsoft:receiver-name": "C"})
	wantCond(t, err, amqp.Stolen)
	// another partition is separate
	if _, err := h.AMQP().Resolve(link("hub/ConsumerGroups/$Default/Partitions/1", true, nil, "")); err != nil {
		t.Fatal(err)
	}
	// another consumer group is separate
	s.CreateConsumerGroup("localhost", "hub", "other")
	if _, err := h.AMQP().Resolve(link("hub/ConsumerGroups/other/Partitions/0", true, epoch(0, "D"), "")); err != nil {
		t.Fatal(err)
	}
	if gone(a) {
		t.Fatal("A still owns the partition")
	}
	// a higher epoch steals it, and A's Next now fails
	b, err := attach(epoch(6, "B"))
	if err != nil {
		t.Fatal(err)
	}
	if !gone(a) || gone(b) {
		t.Fatal("B must own the partition")
	}
	if _, err := a.Next(context.Background(), nil); err == nil {
		t.Fatal("A reads after losing the partition")
	}
	// the same epoch also takes over (a restarted receiver)
	b2, err := attach(epoch(6, "B2"))
	if err != nil {
		t.Fatal(err)
	}
	if !gone(b) || gone(b2) {
		t.Fatal("equal epoch takes over")
	}
	// when the owner's link goes away, plain receivers can attach again
	b2.(*receiver).Detached(nil)
	var readers []amqp.Source
	for i := range maxReaders {
		r, err := attach(map[string]any{"com.microsoft:receiver-name": fmt.Sprint("r", i)})
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
	}
	_, err = attach(nil)
	wantCond(t, err, amqp.ResourceLimit)
	// and an epoch receiver takes the partition from all of them
	e, err := attach(epoch(0, "E"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range readers {
		if !gone(r) {
			t.Fatal("a non-epoch receiver survived the epoch receiver")
		}
	}
	// a detached link frees the partition
	e.(amqp.Detacher).Detached(nil)
	if _, err := attach(nil); err != nil {
		t.Fatalf("a receiver whose link ended must not hold the partition: %v", err)
	}
}

func TestManagement(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 3, 0)
	s.CreateHub("localhost", "second", 1, 0)
	l := link("$management", false, nil, "")
	call := func(props map[string]any) (int32, map[string]any) {
		resp, err := s.mgmt(context.Background(), l, &amqp.Message{ApplicationProperties: props})
		if err != nil {
			t.Fatal(err)
		}
		v, _ := resp.Value.(map[string]any)
		return resp.ApplicationProperties["statusCode"].(int32), v
	}
	code, v := call(map[string]any{"operation": "READ", "type": "com.microsoft:eventhub", "name": "hub"})
	if code != 200 || v["name"] != "hub" || fmt.Sprint(v["partition_ids"]) != "[0 1 2]" {
		t.Fatalf("%d %v", code, v)
	}
	if _, ok := v["partition_ids"].([]string); !ok {
		t.Fatalf("partition_ids is an AMQP array of strings, got %T", v["partition_ids"])
	}
	if _, ok := v["created_at"].(time.Time); !ok {
		t.Fatalf("created_at %T", v["created_at"])
	}
	if v["georeplication_factor"] != int64(1) {
		t.Fatalf("%v", v)
	}
	part := func(id string) (int32, map[string]any) {
		return call(map[string]any{"operation": "READ", "type": "com.microsoft:partition", "name": "hub", "partition": id})
	}
	code, v = part("1")
	if code != 200 || v["is_partition_empty"] != true || v["begin_sequence_number"] != int64(-1) ||
		v["last_enqueued_sequence_number"] != int64(-1) || v["last_enqueued_offset"] != "-1" || v["partition"] != "1" || v["name"] != "hub" {
		t.Fatalf("empty: %d %v", code, v)
	}
	snk := sink(t, h, "hub/Partitions/1")
	send(t, snk, "a", "")
	send(t, snk, "bb", "")
	send(t, snk, "ccc", "")
	code, v = part("1")
	last := v["last_enqueued_offset"].(string)
	if code != 200 || v["is_partition_empty"] != false || v["begin_sequence_number"] != int64(0) ||
		v["last_enqueued_sequence_number"] != int64(2) || last == "-1" {
		t.Fatalf("%d %v", code, v)
	}
	if tm, ok := v["last_enqueued_time_utc"].(time.Time); !ok || time.Since(tm) > time.Minute {
		t.Fatalf("%v", v["last_enqueued_time_utc"])
	}
	// the last offset is what a receiver sees on the last event
	src, rl := source(t, h, "hub/ConsumerGroups/$Default/Partitions/1", nil, "amqp.annotation.x-opt-sequence-number > '1'")
	if m := next(t, src, rl); m.MessageAnnotations["x-opt-offset"] != last {
		t.Fatalf("offset %v vs %v", m.MessageAnnotations["x-opt-offset"], last)
	}
	if code, _ = part("9"); code != 404 {
		t.Fatalf("bad partition: %d", code)
	}
	if code, _ = call(map[string]any{"operation": "READ", "type": "com.microsoft:eventhub", "name": "nope"}); code != 404 {
		t.Fatalf("unknown hub: %d", code)
	}
	// retention trimming keeps the sequence of what was there
	s.Tick(time.Now().Add(48 * time.Hour))
	code, v = part("1")
	if v["is_partition_empty"] != true || v["last_enqueued_sequence_number"] != int64(2) || v["begin_sequence_number"] != int64(3) || v["last_enqueued_offset"] != last {
		t.Fatalf("trimmed: %d %v", code, v)
	}
}

func TestRetention(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 1, time.Hour)
	snk := sink(t, h, "hub")
	for i := range 3 {
		send(t, snk, "old"+strconv.Itoa(i), "")
	}
	src, l := source(t, h, "hub/ConsumerGroups/$Default/Partitions/0", nil, "")
	next(t, src, l) // a receiver in the middle of the log
	s.Tick(time.Now().Add(30 * time.Minute))
	if got := len(drain(src, l)); got != 2 {
		t.Fatalf("nothing is old enough yet: %d", got)
	}
	send(t, snk, "mid", "")
	s.Tick(time.Now().Add(90 * time.Minute)) // everything so far is past retention
	send(t, snk, "new", "")
	if got := read2(t, h, "hub/ConsumerGroups/$Default/Partitions/0"); strings.Join(got, ",") != "new" {
		t.Fatalf("after trimming: %v", got)
	}
	// a receiver whose position was trimmed away starts at the oldest event left
	src2, l2 := source(t, h, "hub/ConsumerGroups/$Default/Partitions/0", nil, "")
	if m := next(t, src2, l2); m.MessageAnnotations["x-opt-sequence-number"].(int64) < 3 {
		t.Fatalf("trimmed events came back: %v", m.MessageAnnotations)
	}
}

func TestSaveLoadReset(t *testing.T) {
	h, s := start(t)
	s.CreateNamespace("ns1")
	s.CreateHub("ns1", "hub", 2, 3*time.Hour)
	s.CreateConsumerGroup("ns1", "hub", "cg")
	l := link("hub", false, nil, "")
	l.Conn.Host = "ns1.servicebus.windows.net"
	n, err := h.AMQP().Resolve(l)
	if err != nil {
		t.Fatal(err)
	}
	m := &amqp.Message{Data: [][]byte{[]byte("persist")}, ApplicationProperties: map[string]any{"n": int64(7), "s": "x"},
		MessageAnnotations: map[string]any{"x-opt-partition-key": "pk"}, Properties: &amqp.Properties{MessageID: "id1", ContentType: "text/plain"}}
	if err := n.(amqp.Sink).Accept(context.Background(), nil, m); err != nil {
		t.Fatal(err)
	}
	b, err := s.Save()
	if err != nil {
		t.Fatal(err)
	}
	// a live receiver is detached by Load
	rl := link("hub/ConsumerGroups/cg/Partitions/0", true, nil, "")
	rl.Conn.Host = "ns1.x"
	src, _ := h.AMQP().Resolve(rl)

	_, s2 := start(t)
	if err := s2.Load(b); err != nil {
		t.Fatal(err)
	}
	hi, err := s2.GetHub("ns1", "hub")
	if err != nil || len(hi.PartitionIDs) != 2 || hi.Retention != 3*time.Hour || len(hi.ConsumerGroups) != 2 {
		t.Fatalf("%+v %v", hi, err)
	}
	s2.mu.Lock()
	var got *Event
	for _, p := range s2.st.Namespaces["ns1"].Hubs["hub"].Partitions {
		if len(p.Events) > 0 {
			got = p.Events[0]
		}
	}
	s2.mu.Unlock()
	if got == nil || string(got.Msg.Data[0]) != "persist" || got.Msg.ApplicationProperties["n"] != int64(7) || got.Key != "pk" ||
		got.Msg.Properties.ContentType != "text/plain" {
		t.Fatalf("%+v %+v", got, got.Msg)
	}
	// loaded logs go on from where they were
	if _, err := s2.Send("ns1", "hub", "", "pk", []byte("two")); err != nil {
		t.Fatal(err)
	}
	s2.mu.Lock()
	seqs := 0
	for _, p := range s2.st.Namespaces["ns1"].Hubs["hub"].Partitions {
		seqs += int(p.Next)
	}
	s2.mu.Unlock()
	if seqs != 2 {
		t.Fatalf("sequence numbers continue: %d", seqs)
	}

	s.Reset()
	if got := s.Namespaces(); len(got) != 1 || got[0] != "localhost" {
		t.Fatalf("reset: %v", got)
	}
	if !src.(*receiver).gone {
		t.Fatal("reset detaches receivers")
	}
}

func TestDeliver(t *testing.T) {
	h, s := start(t)
	s.CreateNamespace("ns2")
	s.CreateHub("ns2", "hub", 1, 0)
	id := "/subscriptions/s/resourceGroups/g/providers/Microsoft.EventHub/namespaces/ns2/eventhubs/hub"
	if err := h.Deliver(id, &core.Message{Body: `{"a":1}`}); err != nil {
		t.Fatal(err)
	}
	l := link("hub/ConsumerGroups/$Default/Partitions/0", true, nil, "")
	l.Conn.Host = "ns2.servicebus.windows.net"
	n, _ := h.AMQP().Resolve(l)
	if m := next(t, n.(amqp.Source), l); string(m.Data[0]) != `{"a":1}` {
		t.Fatalf("%s", m.Data[0])
	}
	if err := h.Deliver(strings.Replace(id, "/hub", "/gone", 1), &core.Message{Body: "x"}); err == nil {
		t.Fatal("delivered to a missing hub")
	}
}

func TestConsole(t *testing.T) {
	_, s := start(t)
	s.CreateHub("localhost", "hub", 2, 0)
	s.CreateConsumerGroup("localhost", "hub", "cg")
	s.Send("localhost", "hub", "0", "", []byte("x"))
	rows := map[string][][]string{}
	for _, tb := range s.Resources() {
		rows[tb.Name] = tb.Rows
	}
	if len(rows["Namespaces"]) != 1 || len(rows["Event hubs"]) != 1 || len(rows["Consumer groups"]) != 2 {
		t.Fatalf("%v", rows)
	}
	if r := rows["Event hubs"][0]; r[0] != "localhost/hub" || r[2] != "2" || r[4] != "1" {
		t.Fatalf("%v", r)
	}
	for _, id := range []string{"localhost", "localhost/hub", "localhost/hub/cg"} {
		if s.Inspect("", id) == nil {
			t.Errorf("Inspect(%s)", id)
		}
	}
	if s.Inspect("", "localhost/nope") != nil || s.Inspect("", "nope") != nil || s.Inspect("", "localhost/hub/nope") != nil {
		t.Fatal("inspected something that is not there")
	}
}

func TestDeleteDetaches(t *testing.T) {
	h, s := start(t)
	s.CreateHub("localhost", "hub", 1, 0)
	src, _ := source(t, h, "hub/ConsumerGroups/$Default/Partitions/0", nil, "")
	s.DeleteHub("localhost", "hub")
	if !src.(*receiver).gone {
		t.Fatal("deleting the hub leaves receivers attached")
	}
	if _, err := h.AMQP().Resolve(link("hub", false, nil, "")); err == nil {
		t.Fatal("a deleted hub still resolves")
	}
}
