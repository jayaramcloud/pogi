package servicebus

import (
	"crypto/rand"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

// Store is the message storage of a queue or subscription (a topic uses it
// for scheduled messages and duplicate detection only).
type Store struct {
	Seq      int64                `json:"seq"`                // last sequence number given out
	Msgs     []*Msg               `json:"msgs,omitempty"`     // active, locked and deferred, in arrival order
	Dead     []*Msg               `json:"dead,omitempty"`     // the $DeadLetterQueue
	Sched    []*Msg               `json:"sched,omitempty"`    // waiting for their enqueue time
	Seen     map[string]time.Time `json:"seen,omitempty"`     // duplicate detection: message id -> first seen
	Sessions map[string]*Session  `json:"sessions,omitempty"` // session id -> state and lock

	next time.Time // earliest moment sweep has work; zero: unknown
}

// Session is a message session: its state blob and who holds its lock.
type Session struct {
	State []byte    `json:"state,omitempty"`
	Until time.Time `json:"-"` // lock expiry; locks do not survive a restart
	owner *source
}

// Msg is a stored message and its broker-side bookkeeping.
type Msg struct {
	Seq        int64         `json:"seq"`
	EnqSeq     int64         `json:"enqSeq,omitempty"` // sequence number before dead-lettering
	M          *amqp.Message `json:"m"`
	Enqueued   time.Time     `json:"enqueued"`
	Expires    time.Time     `json:"expires,omitzero"`
	Sched      time.Time     `json:"sched,omitzero"` // x-opt-scheduled-enqueue-time
	Deferred   bool          `json:"deferred,omitempty"`
	Deliveries uint32        `json:"deliveries,omitempty"`
	Locked     bool          `json:"locked,omitempty"`
	Released   bool          `json:"released,omitempty"` // unlocked by a "released" outcome; its token still settles via $management
	Token      [16]byte      `json:"token"`
	Until      time.Time     `json:"until,omitzero"`
	Src        string        `json:"src,omitempty"`   // dead-letter source entity
	Trace      string        `json:"trace,omitempty"` // the flow of the message, kept beside it so no property is injected
}

func (m *Msg) session() string {
	if m.M.Properties == nil {
		return ""
	}
	return m.M.Properties.GroupID
}

func (m *Msg) size() int {
	n := 0
	for _, d := range m.M.Data {
		n += len(d)
	}
	return n
}

func (store *Store) counts() (active, dead, sched, deferred int, size int64) {
	for _, m := range store.Msgs {
		if m.Deferred {
			deferred++
		} else {
			active++
		}
		size += int64(m.size())
	}
	for _, m := range store.Dead {
		size += int64(m.size())
	}
	return active, len(store.Dead), len(store.Sched), deferred, size
}

func (sub *Sub) size() int64 { _, _, _, _, n := sub.counts(); return n }

// touch records that sweep has work at t.
func (store *Store) touch(t time.Time) {
	if !t.IsZero() && (store.next.IsZero() || t.Before(store.next)) {
		store.next = t
	}
}

// Rule routes topic messages into a subscription: a filter, and optionally
// an action that edits the copy.
type Rule struct {
	Name         string         `json:"name"`
	Filter       Filter         `json:"filter"`
	Action       string         `json:"action,omitempty"` // SQL: SET a = 1; REMOVE b
	ActionParams map[string]any `json:"actionParams,omitempty"`
	Created      time.Time      `json:"created,omitzero"`

	f compiled
	a action
}

// Filter is one of TrueFilter, FalseFilter, SqlFilter or CorrelationFilter.
type Filter struct {
	Type             string         `json:"type"`
	SQL              string         `json:"sql,omitempty"`
	Params           map[string]any `json:"params,omitempty"`
	CorrelationID    string         `json:"correlationId,omitempty"`
	MessageID        string         `json:"messageId,omitempty"`
	To               string         `json:"to,omitempty"`
	ReplyTo          string         `json:"replyTo,omitempty"`
	Label            string         `json:"label,omitempty"`
	SessionID        string         `json:"sessionId,omitempty"`
	ReplyToSessionID string         `json:"replyToSessionId,omitempty"`
	ContentType      string         `json:"contentType,omitempty"`
	Properties       map[string]any `json:"properties,omitempty"`
}

// compile validates the rule and prepares its filter and action.
func (r *Rule) compile() error {
	if r.Name == "" {
		r.Name = "$Default"
	}
	switch r.Filter.Type {
	case "", "TrueFilter":
		r.Filter.Type = "TrueFilter"
	case "FalseFilter", "CorrelationFilter":
	case "SqlFilter":
		f, err := compileFilter(r.Filter.SQL)
		if err != nil {
			return invalid("The SQL filter '%s' is not valid: %v.", r.Filter.SQL, err)
		}
		r.f = f
	default:
		return invalid("Unknown filter type '%s'.", r.Filter.Type)
	}
	r.a = nil
	if r.Action != "" {
		a, err := compileAction(r.Action)
		if err != nil {
			return invalid("The SQL action '%s' is not valid: %v.", r.Action, err)
		}
		r.a = a
	}
	return nil
}

// matches reports whether the filter selects the message.
func (r *Rule) matches(v *view) bool {
	switch r.Filter.Type {
	case "TrueFilter":
		return true
	case "SqlFilter":
		if r.f == nil && r.compile() != nil {
			return false
		}
		return r.f(v, r.Filter.Params) == true
	case "CorrelationFilter":
		f := r.Filter
		p := v.props()
		eq := func(want string, got any) bool { return want == "" || fmt.Sprint(str(got)) == want }
		if !eq(f.CorrelationID, p.CorrelationID) || !eq(f.MessageID, p.MessageID) || !eq(f.To, p.To) ||
			!eq(f.ReplyTo, p.ReplyTo) || !eq(f.Label, p.Subject) || !eq(f.SessionID, p.GroupID) ||
			!eq(f.ReplyToSessionID, p.ReplyToGroupID) || !eq(f.ContentType, p.ContentType) {
			return false
		}
		for k, want := range f.Properties {
			if got, ok := v.m.ApplicationProperties[k]; !ok || compare("=", got, want) != true {
				return false
			}
		}
		return true
	}
	return false
}

// apply runs the rule's action on a copy of the message.
func (r *Rule) apply(v *view) {
	if r.Action == "" {
		return
	}
	if r.a == nil && r.compile() != nil {
		return
	}
	r.a.apply(v, r.ActionParams)
}

// ---- nodes ----

// node is a message-holding address: a queue, a topic or a subscription,
// or the dead-letter queue or $management node of one.
type node struct {
	kind  string // "queue", "topic", "subscription"
	path  string // address without the $DeadLetterQueue/$management suffix
	name  string // entity name, as stored
	q     *Queue
	t     *Topic
	sub   *Sub
	dlq   bool
	mgmt  bool
	nsKey string
}

func (n *node) props() *Props {
	switch {
	case n.q != nil:
		return &n.q.Props
	case n.sub != nil:
		return &n.sub.Props
	}
	return &n.t.Props
}

func (n *node) store() *Store {
	switch {
	case n.q != nil:
		return &n.q.Store
	case n.sub != nil:
		return &n.sub.Store
	}
	return &n.t.Store
}

func (n *node) list() *[]*Msg {
	if n.dlq {
		return &n.store().Dead
	}
	return &n.store().Msgs
}

// cleanAddr turns a link address into "a/b/c": scheme and host dropped, no slashes at the ends.
func cleanAddr(a string) string {
	if _, rest, ok := strings.Cut(a, "://"); ok {
		_, a, _ = strings.Cut(rest, "/")
	}
	return strings.Trim(a, "/")
}

// find resolves an address in a namespace to a node, or nil; the caller holds the lock.
func (s *Service) find(nsName, addr string) *node {
	ns := s.st.Namespaces[nsName]
	if ns == nil {
		return nil
	}
	p := strings.Split(cleanAddr(addr), "/")
	n := &node{nsKey: nsName}
	rest := []string(nil)
	switch {
	case ns.Queues[key(p[0])] != nil:
		n.kind, n.q = "queue", ns.Queues[key(p[0])]
		n.name, rest = n.q.Name, p[1:]
	case ns.Topics[key(p[0])] != nil:
		n.t = ns.Topics[key(p[0])]
		n.kind, n.name, rest = "topic", n.t.Name, p[1:]
		if len(rest) >= 2 && strings.EqualFold(rest[0], "Subscriptions") {
			if n.sub = n.t.Subs[key(rest[1])]; n.sub == nil {
				return nil
			}
			n.kind, rest = "subscription", rest[2:]
		}
	default:
		return nil
	}
	n.path = n.name
	if n.kind == "subscription" {
		n.path = n.t.Name + "/Subscriptions/" + n.sub.Name
	}
	if len(rest) > 0 && strings.EqualFold(rest[0], "$DeadLetterQueue") && n.kind != "topic" {
		n.dlq, rest = true, rest[1:]
	}
	switch {
	case len(rest) == 0:
	case len(rest) == 1 && strings.EqualFold(rest[0], "$management"):
		n.mgmt = true
	default:
		return nil
	}
	return n
}

// ---- sending ----

const maxForward = 4

// ingest takes ownership of a copy of a client's message and prepares it for storage.
func ingest(src *amqp.Message) *Msg {
	m := *src
	m.DeliveryTag, m.DeliveryAnnotations, m.Format = nil, nil, 0
	m.MessageAnnotations = maps.Clone(src.MessageAnnotations)
	for _, k := range []string{"x-opt-sequence-number", "x-opt-enqueued-time", "x-opt-locked-until", "x-opt-message-state",
		"x-opt-deadletter-source", "x-opt-enqueue-sequence-number", "x-opt-lock-token"} {
		delete(m.MessageAnnotations, k)
	}
	m.ApplicationProperties = maps.Clone(src.ApplicationProperties)
	if src.Header != nil {
		h := *src.Header
		m.Header = &h
	}
	if src.Properties != nil {
		p := *src.Properties
		m.Properties = &p
	}
	out := &Msg{M: &m, Trace: traceOf(&m)}
	switch t := m.MessageAnnotations["x-opt-scheduled-enqueue-time"].(type) {
	case time.Time:
		out.Sched = t.UTC()
	case *time.Time:
		if t != nil {
			out.Sched = t.UTC()
		}
	}
	return out
}

// Send puts a message into a queue or topic the way an AMQP sender would.
func (s *Service) Send(ns, entity string, m *amqp.Message) error {
	return s.sendMsg(ns, entity, ingest(m))
}

func (s *Service) sendMsg(ns, entity string, m *Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.find(ns, entity)
	if n == nil || n.dlq || n.mgmt || n.kind == "subscription" {
		return notFound("Entity '" + entity + "'")
	}
	_, err := s.send(n, m, s.now(), 0)
	return err
}

// send stores a message in a queue or fans it out from a topic; the caller holds the lock.
// It returns the sequence number the message got in this entity.
func (s *Service) send(n *node, m *Msg, now time.Time, depth int) (int64, error) {
	p, st := n.props(), n.store()
	if p.Status == "Disabled" || p.Status == "SendDisabled" {
		return 0, amqp.Errorf("com.microsoft:entity-disabled", "The messaging entity '%s' is disabled for sending.", n.path)
	}
	if n.kind == "queue" && p.RequiresSession && m.session() == "" {
		return 0, amqp.Errorf(amqp.NotAllowed, "The SessionId was not set on a message, and it cannot be sent to the entity '%s', because it requires session. Make sure the SessionId is set on the message.", n.path)
	}
	if id, ok := str(msgID(m.M)).(string); ok && p.RequiresDuplicateDetection {
		if seen, dup := st.Seen[id]; dup && now.Sub(seen) < time.Duration(p.DuplicateDetectionHistoryWindow) {
			return 0, nil // dropped, as Service Bus does
		}
		if st.Seen == nil {
			st.Seen = map[string]time.Time{}
		}
		st.Seen[id] = now
		st.touch(now.Add(time.Duration(p.DuplicateDetectionHistoryWindow)))
	}
	m.limitTTL(p)
	st.Seq++
	m.Seq = st.Seq
	if m.Trace == "" {
		m.Trace = core.NewTrace() // a message that arrives untraced gets a flow, so its hops show
	}
	s.span(core.SpanDeliver, "send", "", n.id(), m.Trace)
	if m.Sched.After(now) {
		m.Enqueued = m.Sched
		st.Sched = append(st.Sched, m)
		st.touch(m.Sched)
		return m.Seq, nil
	}
	m.Sched = time.Time{}
	return m.Seq, s.place(n, m, now, depth)
}

// limitTTL applies the entity's default time to live: a message's own can only be shorter.
func (m *Msg) limitTTL(p *Props) {
	def := time.Duration(p.DefaultMessageTimeToLive)
	var ttl time.Duration
	if m.M.Header != nil {
		ttl = m.M.Header.TTL
	}
	if def > 0 && (ttl <= 0 || ttl > def) {
		ttl = def
	}
	if ttl > 0 {
		if m.M.Header == nil {
			m.M.Header = &amqp.Header{}
		}
		m.M.Header.TTL = ttl
	}
}

// place makes a message (its sequence number already set) available: topics
// copy it into subscriptions, entities with ForwardTo pass it on, others store it.
func (s *Service) place(n *node, m *Msg, now time.Time, depth int) error {
	m.Enqueued = now
	if m.M.Header != nil && m.M.Header.TTL > 0 {
		m.Expires = m.Enqueued.Add(m.M.Header.TTL)
	}
	st := n.store()
	switch n.kind {
	case "topic":
		for _, k := range core.SortedKeys(n.t.Subs) {
			s.route(n, n.t.Subs[k], m, now, depth)
		}
	default:
		if to := n.props().ForwardTo; to != "" {
			return s.forward(n, to, m, now, depth)
		}
		*n.list() = append(*n.list(), m)
		st.touch(m.Expires)
	}
	s.notify()
	return nil
}

// route copies a topic message into a subscription once per matching rule.
func (s *Service) route(t *node, sub *Sub, m *Msg, now time.Time, depth int) {
	n := &node{kind: "subscription", sub: sub, t: t.t, nsKey: t.nsKey, name: sub.Name, path: t.t.Name + "/Subscriptions/" + sub.Name}
	for _, r := range sub.Rules {
		v := &view{m: cloneMessage(m.M), enqueued: now, seq: m.Seq}
		if !r.matches(v) {
			continue
		}
		r.apply(v)
		cp := &Msg{M: v.m, Trace: m.Trace}
		s.span(core.SpanDeliver, "fan-out", t.id(), n.id(), m.Trace)
		cp.limitTTL(&sub.Props)
		n.store().Seq++
		cp.Seq = n.store().Seq
		s.place(n, cp, now, depth)
	}
}

func cloneMessage(m *amqp.Message) *amqp.Message {
	c := *m
	c.MessageAnnotations = maps.Clone(m.MessageAnnotations)
	c.ApplicationProperties = maps.Clone(m.ApplicationProperties)
	if m.Header != nil {
		h := *m.Header
		c.Header = &h
	}
	if m.Properties != nil {
		p := *m.Properties
		c.Properties = &p
	}
	return &c
}

// forward passes a message to the queue or topic named by an entity's ForwardTo.
func (s *Service) forward(from *node, to string, m *Msg, now time.Time, depth int) error {
	if depth >= maxForward {
		return amqp.Errorf(amqp.NotAllowed, "Forwarding from '%s' goes through too many entities.", from.path)
	}
	target := s.find(from.nsKey, lastSegments(to))
	if target == nil || target.dlq || target.mgmt || target.kind == "subscription" {
		// like Service Bus: a destination that is gone dead-letters the message
		return nil
	}
	cp := &Msg{M: cloneMessage(m.M), Enqueued: m.Enqueued, EnqSeq: m.Seq, Deliveries: 0, Trace: m.Trace}
	s.span(core.SpanDeliver, "forward", from.id(), target.id(), m.Trace)
	cp.limitTTL(target.props())
	st := target.store()
	st.Seq++
	cp.Seq = st.Seq
	return s.place(target, cp, now, depth+1)
}

// lastSegments reduces "sb://ns.servicebus.windows.net/topic" and "topic" to the entity path.
func lastSegments(to string) string { return cleanAddr(to) }

// ---- receiving ----

func newToken() (t [16]byte) {
	rand.Read(t[:])
	t[6], t[8] = t[6]&0x0f|0x40, t[8]&0x3f|0x80
	return t
}

// msgID is a message's id, nil without properties.
func msgID(m *amqp.Message) any {
	if m.Properties == nil {
		return nil
	}
	return m.Properties.MessageID
}

// dotnetTag is the delivery tag azservicebus expects: the lock token as a .NET GUID (the first
// three fields byte-swapped).
func dotnetTag(t [16]byte) []byte {
	g := t
	g[0], g[1], g[2], g[3] = t[3], t[2], t[1], t[0]
	g[4], g[5] = t[5], t[4]
	g[6], g[7] = t[7], t[6]
	return g[:]
}

// tokenOf reads the lock token off a delivery the broker hands back.
func tokenOf(m *amqp.Message) (t [16]byte, ok bool) {
	if u, isUUID := m.DeliveryAnnotations["x-opt-lock-token"].(amqp.UUID); isUUID {
		return u, true
	}
	if len(m.DeliveryTag) != 16 {
		return t, false
	}
	copy(t[:], m.DeliveryTag)
	return [16]byte(dotnetTag(t)), true // the swap is its own inverse
}

// render builds the AMQP message a receiver sees.
func (n *node) render(m *Msg, locked bool) *amqp.Message {
	out := cloneMessage(m.M)
	h := amqp.Header{}
	if out.Header != nil {
		h = *out.Header
	}
	h.DeliveryCount = m.Deliveries
	if locked && h.DeliveryCount > 0 {
		h.DeliveryCount-- // the SDK adds one
	}
	out.Header = &h
	a := out.MessageAnnotations
	if a == nil {
		a = map[string]any{}
	}
	a["x-opt-sequence-number"] = m.Seq
	a["x-opt-enqueued-time"] = m.Enqueued.UTC()
	if !m.Sched.IsZero() {
		a["x-opt-scheduled-enqueue-time"] = m.Sched.UTC()
	}
	if m.EnqSeq != 0 {
		a["x-opt-enqueue-sequence-number"] = m.EnqSeq
	}
	if m.Src != "" {
		a["x-opt-deadletter-source"] = m.Src
	}
	if m.Deferred {
		a["x-opt-message-state"] = int32(1)
	}
	if locked {
		a["x-opt-locked-until"] = m.Until.UTC()
		out.DeliveryAnnotations = map[string]any{"x-opt-lock-token": amqp.UUID(m.Token)}
		out.DeliveryTag = dotnetTag(m.Token)
	}
	out.MessageAnnotations = a
	return out
}

// lock takes a message for a receiver in peek-lock mode.
func (s *Service) lock(n *node, m *Msg, now time.Time) {
	m.Locked, m.Released, m.Token, m.Deliveries = true, false, newToken(), m.Deliveries+1
	m.Until = now.Add(time.Duration(n.props().LockDuration))
	n.store().touch(m.Until)
}

// take picks the next available message for a receiver, locks it (or, with
// settled, removes it) and returns it rendered. sess nil: not a session receiver.
func (s *Service) take(n *node, sess *string, settled bool, now time.Time) *amqp.Message {
	list := n.list()
	for i, m := range *list {
		if m.Locked || m.Deferred || sess != nil && m.session() != *sess {
			continue
		}
		s.span(core.SpanDeliver, "receive", n.id(), n.id(), m.Trace)
		if settled {
			*list = slices.Delete(*list, i, i+1)
			m.Deliveries++
			return n.render(m, false)
		}
		s.lock(n, m, now)
		return n.render(m, true)
	}
	return nil
}

// byToken finds the locked message holding a lock token in the node's list.
func (n *node) byToken(t [16]byte) (int, *Msg) {
	for i, m := range *n.list() {
		if (m.Locked || m.Released) && m.Token == t {
			return i, m
		}
	}
	return -1, nil
}

// bySeq finds a message by sequence number.
func bySeq(list []*Msg, seq int64) *Msg {
	for _, m := range list {
		if m.Seq == seq {
			return m
		}
	}
	return nil
}

// complete removes a settled message.
func (n *node) remove(m *Msg) {
	l := n.list()
	if i := slices.Index(*l, m); i >= 0 {
		*l = slices.Delete(*l, i, i+1)
	}
}

// deadLetter moves a message to the dead-letter queue, or forwards it when the
// entity has ForwardDeadLetteredMessagesTo.
func (s *Service) deadLetter(n *node, m *Msg, reason, desc string, now time.Time) {
	if n.dlq {
		m.Locked = false // nowhere further to go
		return
	}
	n.remove(m)
	cp := &Msg{M: cloneMessage(m.M), Enqueued: m.Enqueued, EnqSeq: m.Seq, Deliveries: m.Deliveries, Src: n.path, Trace: m.Trace}
	if cp.M.ApplicationProperties == nil {
		cp.M.ApplicationProperties = map[string]any{}
	}
	cp.M.ApplicationProperties["DeadLetterReason"] = reason
	cp.M.ApplicationProperties["DeadLetterErrorDescription"] = desc
	st := n.store()
	s.span(core.SpanRedrive, "dead-letter", n.id(), n.id()+"/$deadletterqueue", m.Trace)
	if to := n.props().ForwardDeadLetteredMessagesTo; to != "" {
		cp.Src, cp.Seq = "", m.Seq
		s.forward(n, to, cp, now, 0)
		return
	}
	st.Seq++
	cp.Seq = st.Seq
	st.Dead = append(st.Dead, cp)
}

// settle applies a receiver's outcome to a locked message.
func (s *Service) settle(n *node, m *Msg, o amqp.Outcome, now time.Time) {
	switch o.State {
	case "accepted":
		n.remove(m)
	case "released":
		// A receiver that closes releases what it holds, then may settle it over $management with the
		// token it still has (azservicebus does): the message is free for others, the token stays good
		// until the next lock.
		m.Locked, m.Released = false, true
	case "rejected":
		reason, desc := "", ""
		props := map[string]any{}
		if o.Error != nil {
			for k, v := range o.Error.Info {
				switch k {
				case "DeadLetterReason":
					reason, _ = v.(string)
				case "DeadLetterErrorDescription":
					desc, _ = v.(string)
				default:
					props[k] = v
				}
			}
			if reason == "" {
				reason, desc = o.Error.Condition, o.Error.Description
			}
		}
		m.modify(props)
		s.deadLetter(n, m, reason, desc, now)
	default: // modified: abandon, or defer when undeliverable here
		m.modify(o.Annotations)
		s.unlock(n, m, o.UndeliverableHere, now)
	}
	s.notify()
}

// unlock returns a locked message to the queue (abandon, lock expiry), parks it
// as deferred, or dead-letters it once it has been delivered MaxDeliveryCount times.
func (s *Service) unlock(n *node, m *Msg, deferred bool, now time.Time) {
	m.Locked, m.Released = false, false
	switch {
	case deferred:
		m.Deferred = true
	case !n.dlq && int32(m.Deliveries) >= n.props().MaxDeliveryCount:
		s.deadLetter(n, m, "MaxDeliveryCountExceeded", fmt.Sprintf("Message could not be consumed after %d delivery attempts.", m.Deliveries), now)
	}
}

// modify merges client-supplied properties into the stored message.
func (m *Msg) modify(props map[string]any) {
	if len(props) == 0 {
		return
	}
	if m.M.ApplicationProperties == nil {
		m.M.ApplicationProperties = map[string]any{}
	}
	maps.Copy(m.M.ApplicationProperties, props)
}

// ---- time ----

// Tick runs everything that depends on the clock: scheduled messages become
// available, expired locks and TTLs take effect, duplicate history and session
// locks age out.
func (s *Service) Tick(now time.Time) {
	now = now.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for nsName, ns := range s.st.Namespaces {
		for _, q := range ns.Queues {
			n := &node{kind: "queue", q: q, name: q.Name, path: q.Name, nsKey: nsName}
			changed = s.sweep(n, now) || changed
		}
		for _, t := range ns.Topics {
			n := &node{kind: "topic", t: t, name: t.Name, path: t.Name, nsKey: nsName}
			changed = s.sweep(n, now) || changed
			for _, sub := range t.Subs {
				n := &node{kind: "subscription", t: t, sub: sub, name: sub.Name, path: t.Name + "/Subscriptions/" + sub.Name, nsKey: nsName}
				changed = s.sweep(n, now) || changed
			}
		}
	}
	if changed {
		s.notify()
	}
}

// sweep does the clock-driven work of one entity; the caller holds the lock.
func (s *Service) sweep(n *node, now time.Time) (changed bool) {
	st := n.store()
	if !st.next.IsZero() && now.Before(st.next) {
		return false
	}
	next := now.Add(time.Minute)
	soon := func(t time.Time) {
		if !t.IsZero() && t.Before(next) {
			next = t
		}
	}
	p := n.props()
	// scheduled messages that are due
	for _, m := range slices.Clone(st.Sched) {
		if m.Sched.After(now) {
			soon(m.Sched)
			continue
		}
		st.Sched = slices.DeleteFunc(st.Sched, func(x *Msg) bool { return x == m })
		m.Sched = time.Time{}
		s.place(n, m, now, 0)
		changed = true
	}
	// duplicate-detection history
	for id, seen := range st.Seen {
		if until := seen.Add(time.Duration(p.DuplicateDetectionHistoryWindow)); !until.After(now) {
			delete(st.Seen, id)
		} else {
			soon(until)
		}
	}
	for id, sess := range st.Sessions {
		if sess.owner != nil && !sess.Until.After(now) {
			sess.owner, sess.Until = nil, time.Time{}
			changed = true
		} else if sess.owner != nil {
			soon(sess.Until)
		}
		if sess.owner == nil && len(sess.State) == 0 {
			delete(st.Sessions, id)
		}
	}
	if n.kind != "topic" {
		for _, dead := range []bool{false, true} {
			n.dlq = dead
			for _, m := range slices.Clone(*n.list()) {
				switch {
				case m.Locked && !m.Until.After(now):
					s.unlock(n, m, false, now)
					changed = true
				case m.Locked:
					soon(m.Until)
				case !dead && !m.Expires.IsZero() && !m.Expires.After(now):
					if p.DeadLetteringOnMessageExpiration {
						s.deadLetter(n, m, "TTLExpiredException", "The message expired and was dead lettered.", now)
					} else {
						n.remove(m)
					}
					changed = true
				case !dead:
					soon(m.Expires)
				}
			}
		}
		n.dlq = false
	}
	st.next = next
	return changed
}

// ---- sessions ----

// sessionLock locks a session for a receiver, creating it on first use.
// It fails when another receiver holds an unexpired lock.
func (s *Service) sessionLock(n *node, id string, src *source, now time.Time) error {
	st := n.store()
	if st.Sessions == nil {
		st.Sessions = map[string]*Session{}
	}
	sess := st.Sessions[id]
	if sess == nil {
		sess = &Session{}
		st.Sessions[id] = sess
	}
	if sess.owner != nil && sess.owner != src && sess.Until.After(now) {
		return amqp.Errorf("com.microsoft:session-cannot-be-locked", "The requested session '%s' cannot be accepted. It may be locked by another receiver.", id)
	}
	sess.owner, sess.Until = src, now.Add(time.Duration(n.props().LockDuration))
	st.touch(sess.Until)
	return nil
}

// nextSession finds a session with messages nobody holds, in order of arrival.
func (s *Service) nextSession(n *node, now time.Time) (string, bool) {
	st := n.store()
	for _, m := range st.Msgs {
		if m.Locked || m.Deferred {
			continue
		}
		if sess := st.Sessions[m.session()]; sess != nil && sess.owner != nil && sess.Until.After(now) {
			continue
		}
		return m.session(), true
	}
	return "", false
}
