package eventhubs

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/amqp"
)

const (
	detachForced = "amqp:link:detach-forced"
	argRange     = "com.microsoft:argument-out-of-range"
	maxReaders   = 5 // non-epoch receivers per consumer group partition
)

// resolve maps an AMQP address to its node:
//
//	<hub>                                         send, the partition is picked per event
//	<hub>/Partitions/<id>                         send to one partition
//	<hub>/ConsumerGroups/<cg>/Partitions/<id>     receive
//	$management, <hub>/$management                the management node
//
// Addresses of hubs that don't exist are not ours (Service Bus may own them).
func (s *Service) resolve(l *amqp.Link) (any, error) {
	ns := namespaceOf(l.Conn.Host)
	addr := l.Address
	if _, rest, ok := strings.Cut(addr, "://"); ok { // sb://host/hub/...
		_, addr, _ = strings.Cut(rest, "/")
	}
	p := strings.Split(strings.Trim(addr, "/"), "/")
	if p[0] == "$management" {
		return amqp.Management(s.mgmt), nil
	}
	s.mu.Lock()
	_, err := s.hub(ns, p[0])
	s.mu.Unlock()
	if err != nil {
		return nil, nil
	}
	switch {
	case len(p) == 2 && p[1] == "$management":
		return amqp.Management(s.mgmt), nil
	case !l.Receiving && len(p) == 1:
		l.ReplyMaxMessageSize = maxMessageSize
		return &sender{s, ns, p[0], ""}, nil
	case !l.Receiving && len(p) == 3 && strings.EqualFold(p[1], "Partitions"):
		l.ReplyMaxMessageSize = maxMessageSize
		return &sender{s, ns, p[0], p[2]}, nil
	case l.Receiving && len(p) == 5 && strings.EqualFold(p[1], "ConsumerGroups") && strings.EqualFold(p[3], "Partitions"):
		return s.attach(l, ns, p[0], p[2], p[4])
	}
	return nil, amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", l.Address)
}

// ---- send ----

type sender struct {
	s              *Service
	ns, hub, parti string // parti "": any
}

func (k *sender) Accept(ctx context.Context, l *amqp.Link, m *amqp.Message) error {
	key, _ := m.MessageAnnotations["x-opt-partition-key"].(string)
	_, err := k.s.enqueue(k.ns, k.hub, k.parti, key, m)
	return err
}

// ---- receive ----

// receiver is one attached link reading a partition. A consumer group
// partition has up to maxReaders of them, or one exclusive epoch receiver.
type receiver struct {
	s     *Service
	key   string // namespace/hub/group/partition, lower-cased
	part  *Partition
	next  int64 // sequence number to deliver next
	epoch *int64
	name  string
	link  *amqp.Link
	gone  bool
	err   *amqp.Error // what Next reports once the receiver lost the partition, or never had it
}

func (r *receiver) String() string {
	if r.epoch == nil {
		return fmt.Sprintf("'%s' (no epoch)", r.name)
	}
	return fmt.Sprintf("'%s' with epoch '%d'", r.name, *r.epoch)
}

// closing lists links to detach once the lock is released.
type closing []detach

type detach struct {
	l *amqp.Link
	e *amqp.Error
}

func (c closing) detach() {
	for _, x := range c {
		x.l.Close(x.e)
	}
}

// take removes the receivers whose key starts with prefix. Lock held.
func (s *Service) take(prefix string) (out closing) {
	for key, rs := range s.recv {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, r := range rs {
			r.gone = true
			out = append(out, detach{r.link, amqp.Errorf(detachForced, "The entity of the link was deleted.")})
		}
		delete(s.recv, key)
	}
	return out
}

func (s *Service) takeAll() closing {
	c := s.take("")
	s.recv = map[string][]*receiver{}
	return c
}

func epochOf(props map[string]any) *int64 {
	switch v := props["com.microsoft:epoch"].(type) {
	case int64:
		return &v
	case int32:
		e := int64(v)
		return &e
	case int:
		e := int64(v)
		return &e
	case uint64:
		e := int64(v)
		return &e
	}
	return nil
}

func (s *Service) attach(l *amqp.Link, ns, hubName, cg, pid string) (any, error) {
	s.mu.Lock()
	h, err := s.hub(ns, hubName)
	if err != nil {
		s.mu.Unlock()
		return nil, amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", l.Address)
	}
	if !h.hasGroup(cg) {
		s.mu.Unlock()
		return nil, amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", l.Address)
	}
	var part *Partition
	for _, p := range h.Partitions {
		if p.ID == pid {
			part = p
		}
	}
	if part == nil {
		s.mu.Unlock()
		return nil, amqp.Errorf(argRange, "The specified partition '%s' is invalid for EventHub '%s'.", pid, h.Name)
	}
	start, err := part.start(fmt.Sprint(l.Filter["apache.org:selector-filter:string"]))
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	name, _ := l.Properties["com.microsoft:receiver-name"].(string)
	if name == "" {
		name = l.Name
	}
	r := &receiver{s: s, key: k(ns) + "/" + k(hubName) + "/" + k(cg) + "/" + pid, part: part, next: start, epoch: epochOf(l.Properties), name: name, link: l}

	var live []*receiver
	for _, x := range s.recv[r.key] {
		if !x.gone {
			live = append(live, x)
		}
	}
	// A refused epoch attach is accepted and then detached with the error, as Event Hubs does:
	// the SDK reads a refused attach as retryable and only a detach as lost ownership.
	refuse := func(e *amqp.Error) (any, error) {
		s.mu.Unlock()
		r.gone, r.err = true, e
		return r, nil
	}
	var out closing
	switch {
	case r.epoch == nil:
		for _, x := range live {
			if x.epoch != nil {
				return refuse(amqp.Errorf(amqp.Stolen, "Receiver %s cannot be created because a receiver %s already exists. "+
					"Make sure you are creating receiver with epoch, or ensure all epoch receivers are closed or disconnected.", r, x))
			}
		}
		if len(live) >= maxReaders {
			return refuse(amqp.Errorf(amqp.ResourceLimit, "Exceeding the maximum number of allowed receivers per partition in a consumer group which is %d. "+
				"List of connected receivers - %d.", maxReaders, len(live)))
		}
	default:
		for _, x := range live {
			if x.epoch != nil && *x.epoch > *r.epoch {
				return refuse(amqp.Errorf(amqp.Stolen, "Receiver %s cannot be created because a receiver %s already exists. "+
					"Make sure you are creating receiver with increasing epoch value to ensure connectivity, "+
					"or ensure all old epoch receivers are closed or disconnected.", r, x))
			}
		}
		for _, x := range live { // the new owner takes the partition from everybody
			x.gone, x.err = true, amqp.Errorf(amqp.Stolen, "New receiver %s with higher epoch of '%d' is created hence current receiver %s is getting disconnected. "+
				"If you are recreating the receiver, make sure a higher epoch is used.", r.name, *r.epoch, x)
			out = append(out, detach{x.link, x.err})
		}
		live = nil
	}
	s.recv[r.key] = append(live, r)
	s.mu.Unlock()
	out.detach()
	return r, nil
}

var _ amqp.Detacher = (*receiver)(nil)

// Detached is called by the broker when the link goes away.
func (r *receiver) Detached(*amqp.Link) { r.release() }

func (r *receiver) release() {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.gone = true
	rs := r.s.recv[r.key]
	for i, x := range rs {
		if x == r {
			r.s.recv[r.key] = append(rs[:i:i], rs[i+1:]...)
			break
		}
	}
}

// Next blocks until the partition has an event past the receiver's position.
func (r *receiver) Next(ctx context.Context, l *amqp.Link) (*amqp.Message, error) {
	s := r.s
	for {
		s.mu.Lock()
		if r.gone {
			s.mu.Unlock()
			if r.err != nil {
				return nil, r.err
			}
			return nil, amqp.Errorf(detachForced, "The link was closed.")
		}
		p := r.part
		if i := p.begin(); r.next < i { // trimmed away meanwhile
			r.next = i
		}
		if i := r.next - p.begin(); i < int64(len(p.Events)) {
			e := p.Events[i]
			r.next++
			s.mu.Unlock()
			return e.message(), nil
		}
		w := p.waiter()
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w:
		}
	}
}

// Settle is never called: Event Hubs receivers are pre-settled.
func (r *receiver) Settle(*amqp.Link, *amqp.Message, amqp.Outcome) {}

// message is what a receiver sees: the stored message with the system annotations.
func (e *Event) message() *amqp.Message {
	m := *e.Msg
	m.Format, m.DeliveryTag = 0, nil
	m.MessageAnnotations = make(map[string]any, len(e.Msg.MessageAnnotations)+3)
	for k, v := range e.Msg.MessageAnnotations {
		m.MessageAnnotations[k] = v
	}
	m.MessageAnnotations["x-opt-sequence-number"] = e.Seq
	m.MessageAnnotations["x-opt-offset"] = strconv.FormatInt(e.Offset, 10)
	m.MessageAnnotations["x-opt-enqueued-time"] = e.Enqueued
	return &m
}

// ---- start position ----

// selector is the filter azeventhubs sends, for example
// "amqp.annotation.x-opt-offset > '@latest'" or
// "amqp.annotation.x-opt-sequence-number >= '42'" (enqueued time in epoch milliseconds).
var selector = regexp.MustCompile(`amqp\.annotation\.x-opt-(offset|sequence-number|enqueued-time)\s*(>=|>)\s*'([^']*)'`)

// start returns the sequence number of the first event a receiver with
// filter expression expr gets; no filter means the beginning. Lock held.
func (p *Partition) start(expr string) (int64, error) {
	m := selector.FindStringSubmatch(expr)
	if m == nil {
		return p.begin(), nil
	}
	field, inclusive, val := m[1], m[2] == ">=", m[3]
	if field == "offset" && val == "@latest" {
		return p.Next, nil
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, amqp.Errorf(amqp.InvalidField, "The filter '%s' is not valid.", expr)
	}
	key := func(e *Event) int64 {
		switch field {
		case "offset":
			return e.Offset
		case "enqueued-time":
			return e.Enqueued.UnixMilli()
		}
		return e.Seq
	}
	i := sort.Search(len(p.Events), func(i int) bool { // keys only grow along the log
		return key(p.Events[i]) > n || inclusive && key(p.Events[i]) == n
	})
	if i == len(p.Events) {
		return p.Next, nil
	}
	return p.Events[i].Seq, nil
}

// ---- $management ----

// mgmt answers the READs azeventhubs makes: com.microsoft:eventhub and com.microsoft:partition.
func (s *Service) mgmt(ctx context.Context, l *amqp.Link, req *amqp.Message) (*amqp.Message, error) {
	prop := func(k string) string { v, _ := req.ApplicationProperties[k].(string); return v }
	reply := func(code int32, desc string, v any) (*amqp.Message, error) {
		return &amqp.Message{ApplicationProperties: map[string]any{"statusCode": code, "statusDescription": desc}, Value: v}, nil
	}
	name := prop("name")
	s.mu.Lock()
	defer s.mu.Unlock()
	h, err := s.hub(namespaceOf(l.Conn.Host), name)
	if err != nil {
		return reply(404, fmt.Sprintf("The messaging entity '%s' could not be found.", name), nil)
	}
	if prop("operation") != "READ" {
		return reply(501, "Operation "+prop("operation")+" is not supported.", nil)
	}
	switch typ := prop("type"); typ {
	case "com.microsoft:eventhub":
		ids := []string{} // an AMQP array of strings, which is what the SDK type-asserts
		for _, p := range h.Partitions {
			ids = append(ids, p.ID)
		}
		return reply(200, "OK", map[string]any{"name": h.Name, "type": typ, "created_at": h.Created, "partition_ids": ids,
			"georeplication_factor": int64(1)})
	case "com.microsoft:partition":
		for _, p := range h.Partitions {
			if p.ID != prop("partition") {
				continue
			}
			v := map[string]any{"name": h.Name, "type": typ, "partition": p.ID, "begin_sequence_number": int64(-1),
				"last_enqueued_sequence_number": int64(-1), "last_enqueued_offset": "-1", "last_enqueued_time_utc": p.LastTime,
				"is_partition_empty": len(p.Events) == 0}
			if p.Next > 0 { // something was enqueued once, even if retention trimmed it all
				v["begin_sequence_number"], v["last_enqueued_sequence_number"] = p.begin(), p.Next-1
				v["last_enqueued_offset"] = strconv.FormatInt(p.LastOffset, 10)
			}
			return reply(200, "OK", v)
		}
		return reply(404, fmt.Sprintf("The specified partition '%s' is invalid for EventHub '%s'.", prop("partition"), h.Name), nil)
	}
	return reply(501, "Type "+prop("type")+" is not supported.", nil)
}
