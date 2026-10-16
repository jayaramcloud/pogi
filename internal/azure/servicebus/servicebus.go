// Package servicebus emulates Azure Service Bus: queues, topics and
// subscriptions with SQL and correlation rules, peek-lock and
// receive-and-delete, settlement, dead-lettering, sessions, scheduled and
// deferred messages, duplicate detection, forwarding, lock renewal, and the
// ATOM management API that azservicebus/admin speaks.
//
// Messages travel over AMQP 1.0, which internal/amqp serves: this package
// registers a Resolver that answers for queue, topic, subscription and
// <entity>/$management addresses. Namespaces are the first label of the
// connection's host name ("localhost" or an IP is the namespace "localhost")
// and exist from first use.
//
// Admin calls are served on pogi's main port; Phase 14's resource manager
// reaches the same engine through the exported methods.
package servicebus

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Service Bus emulator.
//
// ponytail: one lock guards every namespace, and every receiver waits on one
// shared wake channel; per-entity locks and conds if that ever shows.
type Service struct {
	hub  *core.Hub
	mu   sync.Mutex
	st   state
	wake chan struct{}
	stop func()
	// Now is the clock; tests replace it and call Tick.
	Now func() time.Time
}

type state struct {
	Namespaces map[string]*Namespace `json:"namespaces"`
	ARM        armres.Store          `json:"arm,omitempty"` // namespaces and authorization rules as the resource manager has them
}

// Namespace holds the entities of one Service Bus namespace.
type Namespace struct {
	Created time.Time         `json:"created"`
	Queues  map[string]*Queue `json:"queues,omitempty"` // by lower-cased name
	Topics  map[string]*Topic `json:"topics,omitempty"`
}

// Queue is a queue and its messages.
type Queue struct {
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	Props
	Store
}

// Topic fans messages out to its subscriptions. Its own store keeps only
// scheduled messages and the duplicate-detection history.
type Topic struct {
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	Props
	Store
	Subs map[string]*Sub `json:"subs,omitempty"`
}

// Sub is a topic subscription: a rule set and a message store.
type Sub struct {
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	Props
	Rules []*Rule `json:"rules"`
	Store
}

// Entity is a point-in-time view of a queue, topic or subscription for
// callers outside the package.
type Entity struct {
	Kind    string // "queue", "topic" or "subscription"
	Name    string
	Topic   string // subscriptions: the topic
	Created time.Time
	Updated time.Time
	Props
	Active, Dead, Scheduled, Deferred int
	Size                              int64
	Subscriptions                     int // topics
	SQLFilters, CorrelationFilters    int // topics
	Rules                             int // subscriptions
}

// New creates the Service Bus service and puts its AMQP resolver on the broker.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, Now: time.Now}
	s.Reset()
	h.AMQP().Add(s.resolve)
	h.OnDeliver("microsoft.servicebus", s.deliver)
	s.registerARM()
	s.stop = core.Every(250*time.Millisecond, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "servicebus", Cloud: core.Azure, Title: "Service Bus", Protocol: "azure-xml", Match: match}
}

func (s *Service) Close() error {
	s.stop()
	return nil
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Namespaces: map[string]*Namespace{}}
	s.notify()
	s.mu.Unlock()
}

// ponytail: messages are saved as JSON, so typed application properties and message ids other than
// strings, bools and numbers come back as generic JSON values after a restart; a typed encoding if that bites.
func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Namespaces == nil {
		st.Namespaces = map[string]*Namespace{}
	}
	s.mu.Lock()
	s.st = st
	s.notify()
	s.mu.Unlock()
	return nil
}

// notify wakes every blocked receiver; the caller holds the lock.
func (s *Service) notify() {
	if s.wake != nil {
		close(s.wake)
		s.wake = nil
	}
}

// waiter returns the channel the next notify closes; the caller holds the lock.
func (s *Service) waiter() <-chan struct{} {
	if s.wake == nil {
		s.wake = make(chan struct{})
	}
	return s.wake
}

// namespaceOf names the namespace a host belongs to: the first label of
// "<ns>.servicebus.windows.net"; "localhost" for localhost and IPs.
func namespaceOf(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	first, _, _ := strings.Cut(host, ".")
	if host == "" || net.ParseIP(host) != nil || first == "localhost" {
		return "localhost"
	}
	return first
}

func key(name string) string { return strings.ToLower(name) }

// ns returns the namespace, creating it on first use; the caller holds the lock.
func (s *Service) ns(name string) *Namespace {
	n := s.st.Namespaces[name]
	if n == nil {
		n = &Namespace{Created: s.now(), Queues: map[string]*Queue{}, Topics: map[string]*Topic{}}
		s.st.Namespaces[name] = n
	}
	return n
}

func notFound(what string) error {
	return core.Errorf(404, "NotFound", "%s could not be found.", what)
}

func conflict(what string) error {
	return core.Errorf(409, "Conflict", "The messaging entity '%s' already exists.", what)
}

func invalid(format string, a ...any) error { return core.Errorf(400, "BadRequest", format, a...) }

// ---- namespaces ----

// CreateNamespace makes the namespace if it is not there; it reports whether it was made.
func (s *Service) CreateNamespace(name string) (created bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.st.Namespaces[name]
	s.ns(name)
	return !ok
}

// GetNamespace returns when a namespace was created.
func (s *Service) GetNamespace(name string) (created time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.st.Namespaces[name]
	if !ok {
		return time.Time{}, false
	}
	return n.Created, true
}

// DeleteNamespace removes a namespace with everything in it.
func (s *Service) DeleteNamespace(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.st.Namespaces[name]
	delete(s.st.Namespaces, name)
	s.notify()
	return ok
}

// Namespaces lists the namespace names.
func (s *Service) Namespaces() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return core.SortedKeys(s.st.Namespaces)
}

// ---- entities ----

func (s *Service) validate(kind string, p *Props) error {
	p.defaults(kind)
	switch p.Status {
	case "Active", "Disabled", "SendDisabled", "ReceiveDisabled":
	default:
		return invalid("Invalid entity status '%s'.", p.Status)
	}
	if p.MaxDeliveryCount < 1 && kind != "topic" {
		return invalid("MaxDeliveryCount must be at least 1.")
	}
	return nil
}

// CreateQueue adds a queue; it conflicts with an existing queue or topic of that name.
func (s *Service) CreateQueue(ns, name string, p Props) (Entity, error) {
	if name == "" || strings.Contains(name, "/") {
		return Entity{}, invalid("Invalid queue name '%s'.", name)
	}
	if err := s.validate("queue", &p); err != nil {
		return Entity{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.ns(ns)
	if n.Queues[key(name)] != nil || n.Topics[key(name)] != nil {
		return Entity{}, conflict(name)
	}
	now := s.now()
	q := &Queue{Name: name, Created: now, Updated: now, Props: p}
	n.Queues[key(name)] = q
	return q.entity(now), nil
}

// UpdateQueue replaces a queue's settings; RequiresSession and
// RequiresDuplicateDetection cannot change.
func (s *Service) UpdateQueue(ns, name string, p Props) (Entity, error) {
	if err := s.validate("queue", &p); err != nil {
		return Entity{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.ns(ns).Queues[key(name)]
	if q == nil {
		return Entity{}, notFound("Queue '" + name + "'")
	}
	p.RequiresSession, p.RequiresDuplicateDetection = q.RequiresSession, q.RequiresDuplicateDetection
	q.Props, q.Updated = p, s.now()
	s.notify()
	return q.entity(s.now()), nil
}

// GetQueue returns a queue.
func (s *Service) GetQueue(ns, name string) (Entity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.ns(ns).Queues[key(name)]
	if q == nil {
		return Entity{}, false
	}
	return q.entity(s.now()), true
}

// DeleteQueue removes a queue and its messages.
func (s *Service) DeleteQueue(ns, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.ns(ns)
	_, ok := n.Queues[key(name)]
	delete(n.Queues, key(name))
	s.notify()
	return ok
}

// ListQueues lists a namespace's queues by name.
func (s *Service) ListQueues(ns string) []Entity {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entity
	for _, k := range core.SortedKeys(s.ns(ns).Queues) {
		out = append(out, s.ns(ns).Queues[k].entity(s.now()))
	}
	return out
}

// CreateTopic adds a topic.
func (s *Service) CreateTopic(ns, name string, p Props) (Entity, error) {
	if name == "" || strings.Contains(name, "/") {
		return Entity{}, invalid("Invalid topic name '%s'.", name)
	}
	if err := s.validate("topic", &p); err != nil {
		return Entity{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.ns(ns)
	if n.Queues[key(name)] != nil || n.Topics[key(name)] != nil {
		return Entity{}, conflict(name)
	}
	now := s.now()
	t := &Topic{Name: name, Created: now, Updated: now, Props: p, Subs: map[string]*Sub{}}
	n.Topics[key(name)] = t
	return t.entity(now), nil
}

// UpdateTopic replaces a topic's settings.
func (s *Service) UpdateTopic(ns, name string, p Props) (Entity, error) {
	if err := s.validate("topic", &p); err != nil {
		return Entity{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ns(ns).Topics[key(name)]
	if t == nil {
		return Entity{}, notFound("Topic '" + name + "'")
	}
	p.RequiresDuplicateDetection = t.RequiresDuplicateDetection
	t.Props, t.Updated = p, s.now()
	return t.entity(s.now()), nil
}

// GetTopic returns a topic.
func (s *Service) GetTopic(ns, name string) (Entity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ns(ns).Topics[key(name)]
	if t == nil {
		return Entity{}, false
	}
	return t.entity(s.now()), true
}

// DeleteTopic removes a topic, its subscriptions and their messages.
func (s *Service) DeleteTopic(ns, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.ns(ns)
	_, ok := n.Topics[key(name)]
	delete(n.Topics, key(name))
	s.notify()
	return ok
}

// ListTopics lists a namespace's topics by name.
func (s *Service) ListTopics(ns string) []Entity {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entity
	for _, k := range core.SortedKeys(s.ns(ns).Topics) {
		out = append(out, s.ns(ns).Topics[k].entity(s.now()))
	}
	return out
}

// CreateSubscription adds a subscription to a topic with its default rule:
// def, or $Default with a TrueFilter when def is nil.
func (s *Service) CreateSubscription(ns, topic, name string, p Props, def *Rule) (Entity, error) {
	if name == "" || strings.Contains(name, "/") {
		return Entity{}, invalid("Invalid subscription name '%s'.", name)
	}
	if err := s.validate("subscription", &p); err != nil {
		return Entity{}, err
	}
	if def == nil {
		def = &Rule{Name: "$Default", Filter: Filter{Type: "TrueFilter"}}
	}
	if err := def.compile(); err != nil {
		return Entity{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ns(ns).Topics[key(topic)]
	if t == nil {
		return Entity{}, notFound("Topic '" + topic + "'")
	}
	if t.Subs[key(name)] != nil {
		return Entity{}, conflict(name)
	}
	now := s.now()
	r := *def
	r.Created = now
	sub := &Sub{Name: name, Created: now, Updated: now, Props: p, Rules: []*Rule{&r}}
	t.Subs[key(name)] = sub
	return sub.entity(topic, now), nil
}

// UpdateSubscription replaces a subscription's settings; RequiresSession cannot change.
func (s *Service) UpdateSubscription(ns, topic, name string, p Props) (Entity, error) {
	if err := s.validate("subscription", &p); err != nil {
		return Entity{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, err := s.sub(ns, topic, name)
	if err != nil {
		return Entity{}, err
	}
	p.RequiresSession = sub.RequiresSession
	sub.Props, sub.Updated = p, s.now()
	s.notify()
	return sub.entity(topic, s.now()), nil
}

// sub finds a subscription; the caller holds the lock.
func (s *Service) sub(ns, topic, name string) (*Sub, error) {
	t := s.ns(ns).Topics[key(topic)]
	if t == nil {
		return nil, notFound("Topic '" + topic + "'")
	}
	sub := t.Subs[key(name)]
	if sub == nil {
		return nil, notFound("Subscription '" + name + "'")
	}
	return sub, nil
}

// GetSubscription returns a subscription.
func (s *Service) GetSubscription(ns, topic, name string) (Entity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, err := s.sub(ns, topic, name)
	if err != nil {
		return Entity{}, false
	}
	return sub.entity(topic, s.now()), true
}

// DeleteSubscription removes a subscription and its messages.
func (s *Service) DeleteSubscription(ns, topic, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ns(ns).Topics[key(topic)]
	if t == nil || t.Subs[key(name)] == nil {
		return false
	}
	delete(t.Subs, key(name))
	s.notify()
	return true
}

// ListSubscriptions lists a topic's subscriptions by name.
func (s *Service) ListSubscriptions(ns, topic string) ([]Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ns(ns).Topics[key(topic)]
	if t == nil {
		return nil, notFound("Topic '" + topic + "'")
	}
	var out []Entity
	for _, k := range core.SortedKeys(t.Subs) {
		out = append(out, t.Subs[k].entity(topic, s.now()))
	}
	return out, nil
}

func (q *Queue) entity(now time.Time) Entity {
	e := Entity{Kind: "queue", Name: q.Name, Created: q.Created, Updated: q.Updated, Props: q.Props}
	e.Active, e.Dead, e.Scheduled, e.Deferred, e.Size = q.counts()
	return e
}

func (t *Topic) entity(now time.Time) Entity {
	e := Entity{Kind: "topic", Name: t.Name, Created: t.Created, Updated: t.Updated, Props: t.Props, Subscriptions: len(t.Subs)}
	e.Scheduled = len(t.Sched)
	for _, sub := range t.Subs {
		e.Size += sub.size()
		for _, r := range sub.Rules {
			switch r.Filter.Type {
			case "SqlFilter":
				e.SQLFilters++
			case "CorrelationFilter":
				e.CorrelationFilters++
			}
		}
	}
	return e
}

func (sub *Sub) entity(topic string, now time.Time) Entity {
	e := Entity{Kind: "subscription", Name: sub.Name, Topic: topic, Created: sub.Created, Updated: sub.Updated, Props: sub.Props, Rules: len(sub.Rules)}
	e.Active, e.Dead, e.Scheduled, e.Deferred, e.Size = sub.counts()
	return e
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	nss := core.Table{Name: "Namespaces", Columns: []string{"Name", "Queues", "Topics", "Created"}}
	qs := core.Table{Name: "Queues", Columns: []string{"Queue", "Active", "Dead-letter", "Scheduled", "Deferred", "Sessions"}}
	ts := core.Table{Name: "Topics", Columns: []string{"Topic", "Subscriptions", "Scheduled"}}
	ss := core.Table{Name: "Subscriptions", Columns: []string{"Subscription", "Active", "Dead-letter", "Rules"}}
	now := s.now()
	for _, name := range core.SortedKeys(s.st.Namespaces) {
		n := s.st.Namespaces[name]
		nss.Rows = append(nss.Rows, []string{name, itoa(len(n.Queues)), itoa(len(n.Topics)), n.Created.Format(time.RFC3339)})
		for _, qk := range core.SortedKeys(n.Queues) {
			e := n.Queues[qk].entity(now)
			qs.Rows = append(qs.Rows, []string{name + "/" + e.Name, itoa(e.Active), itoa(e.Dead), itoa(e.Scheduled), itoa(e.Deferred), yes(e.RequiresSession)})
		}
		for _, tk := range core.SortedKeys(n.Topics) {
			t := n.Topics[tk]
			ts.Rows = append(ts.Rows, []string{name + "/" + t.Name, itoa(len(t.Subs)), itoa(len(t.Sched))})
			for _, sk := range core.SortedKeys(t.Subs) {
				e := t.Subs[sk].entity(t.Name, now)
				ss.Rows = append(ss.Rows, []string{name + "/" + t.Name + "/" + e.Name, itoa(e.Active), itoa(e.Dead), itoa(e.Rules)})
			}
		}
	}
	return []core.Table{nss, qs, ts, ss}
}

func yes(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// Inspect returns the full view of one row: settings, counts, rules and the
// first messages (id is "<namespace>/<queue>" or ".../<topic>/<subscription>").
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.Split(id, "/")
	n := s.st.Namespaces[p[0]]
	if n == nil {
		return nil
	}
	type msgView struct {
		Seq        int64     `json:"sequenceNumber"`
		MessageID  string    `json:"messageId,omitempty"`
		Session    string    `json:"sessionId,omitempty"`
		State      string    `json:"state"`
		Deliveries uint32    `json:"deliveryCount"`
		Bytes      int       `json:"bytes"`
		LockedTill time.Time `json:"lockedUntil,omitzero"`
	}
	peek := func(st *Store) map[string][]msgView {
		out := map[string][]msgView{}
		for name, list := range map[string][]*Msg{"messages": st.Msgs, "deadLetter": st.Dead, "scheduled": st.Sched} {
			for i, m := range list {
				if i == 20 {
					break
				}
				state := "active"
				switch {
				case m.Deferred:
					state = "deferred"
				case m.Locked:
					state = "locked"
				case name == "scheduled":
					state = "scheduled"
				}
				mv := msgView{Seq: m.Seq, Session: m.session(), State: state, Deliveries: m.Deliveries, Bytes: m.size()}
				if id, ok := str(msgID(m.M)).(string); ok {
					mv.MessageID = id
				}
				if m.Locked {
					mv.LockedTill = m.Until
				}
				out[name] = append(out[name], mv)
			}
		}
		return out
	}
	now := s.now()
	switch {
	case table == "Namespaces" && len(p) == 1:
		return map[string]any{"name": p[0], "created": n.Created, "queues": core.SortedKeys(n.Queues), "topics": core.SortedKeys(n.Topics)}
	case table == "Queues" && len(p) == 2:
		if q := n.Queues[key(p[1])]; q != nil {
			return map[string]any{"properties": q.entity(now), "messages": peek(&q.Store)}
		}
	case table == "Topics" && len(p) == 2:
		if t := n.Topics[key(p[1])]; t != nil {
			return map[string]any{"properties": t.entity(now), "scheduled": peek(&t.Store)["scheduled"], "subscriptions": core.SortedKeys(t.Subs)}
		}
	case table == "Subscriptions" && len(p) == 3:
		if t := n.Topics[key(p[1])]; t != nil && t.Subs[key(p[2])] != nil {
			sub := t.Subs[key(p[2])]
			return map[string]any{"properties": sub.entity(t.Name, now), "rules": sub.Rules, "messages": peek(&sub.Store)}
		}
	}
	return nil
}

func itoa(n int) string { return strconv.Itoa(n) }
