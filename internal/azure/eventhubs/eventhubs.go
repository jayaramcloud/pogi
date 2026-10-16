// Package eventhubs emulates Azure Event Hubs: namespaces, event hubs with
// partitions, consumer groups and retention, over AMQP 1.0 (the official
// azeventhubs SDK) with an optional Kafka endpoint on Redpanda.
//
// Namespaces are named by the first label of the host the client connects to
// (<ns>.servicebus.windows.net); localhost and IP addresses mean "localhost",
// which always exists. The control plane is Go methods (the Azure resource
// manager and pogi.yaml seeding call them through h.Service("eventhubs")); the
// data plane is AMQP (amqp.go) and Kafka (kafka.go).
package eventhubs

import (
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Errors of the control-plane methods, wrapped with the name concerned.
var (
	ErrNotFound = errors.New("not found")
	ErrExists   = errors.New("already exists")
	ErrInvalid  = errors.New("invalid")
)

// DefaultConsumerGroup always exists in every event hub.
const DefaultConsumerGroup = "$Default"

const (
	defaultPartitions = 4
	maxPartitions     = 32
	defaultRetention  = 24 * time.Hour
	maxMessageSize    = 1 << 20 // Standard tier
	eventOverhead     = 64      // bytes an event adds to the offset besides its body
)

// Service is the Event Hubs emulator.
type Service struct {
	h    *core.Hub
	mu   sync.Mutex
	st   state
	recv map[string][]*receiver // live receivers by consumer group partition, see amqp.go
	stop func()

	kmu   sync.Mutex        // serialises starting Kafka brokers (minutes); never held with mu
	kafka map[string]string // namespace → bootstrap address, once its broker runs; under mu
}

type state struct {
	Namespaces map[string]*Namespace `json:"namespaces"`
	ARM        armres.Store          `json:"arm,omitempty"` // namespaces and authorization rules as the resource manager has them
}

// Namespace holds event hubs, keyed by lower-cased name.
type Namespace struct {
	Name      string          `json:"name"`
	Created   time.Time       `json:"created"`
	Hubs      map[string]*Hub `json:"hubs"`
	Container string          `json:"container,omitempty"` // Redpanda container of the Kafka endpoint, once needed
}

// Hub is an event hub: a fixed set of partitions and its consumer groups.
type Hub struct {
	Name           string        `json:"name"`
	Created        time.Time     `json:"created"`
	Retention      time.Duration `json:"retention"`
	Partitions     []*Partition  `json:"partitions"`
	ConsumerGroups []string      `json:"consumerGroups"`
	rr             int           // round-robin cursor of sends without a key
}

// Partition is an append-only log. Sequence numbers count from 0; an offset
// is the bytes enqueued before the event, so both only ever grow.
type Partition struct {
	ID         string    `json:"id"`
	Events     []*Event  `json:"events"` // retained events, contiguous sequence numbers
	Next       int64     `json:"next"`   // sequence number of the next event
	NextOffset int64     `json:"nextOffset"`
	LastOffset int64     `json:"lastOffset"` // offset of the last event ever enqueued
	LastTime   time.Time `json:"lastTime"`   // enqueued time of the last event ever enqueued
	wake       chan struct{}
}

// Event is one stored message.
type Event struct {
	Seq      int64         `json:"seq"`
	Offset   int64         `json:"offset"`
	Enqueued time.Time     `json:"enqueued"`
	Key      string        `json:"key,omitempty"`
	Msg      *amqp.Message `json:"msg"`
}

// New creates the Event Hubs service and registers its AMQP resolver.
func New(h *core.Hub) *Service {
	s := &Service{h: h}
	s.Reset()
	h.AMQP().Add(s.resolve)
	h.OnDeliver("microsoft.eventhub", s.deliver)
	s.registerARM()
	s.stop = core.Every(time.Minute, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "eventhubs", Cloud: core.Azure, Title: "Event Hubs", Protocol: "azure-json"}
}

// ServeHTTP is never routed to (Info has no Match); the data plane is AMQP.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }

// Close stops retention trimming.
func (s *Service) Close() error { s.stop(); return nil }

// k is the map key of a name: Azure resource names are case-insensitive.
func k(name string) string { return strings.ToLower(name) }

// namespaceOf names the namespace a connection to host reaches.
func namespaceOf(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	first, _, _ := strings.Cut(host, ".")
	if net.ParseIP(host) != nil || first == "" || first == "localhost" {
		return "localhost"
	}
	return strings.ToLower(first)
}

func newHub(name string, partitions int, retention time.Duration) *Hub {
	if partitions == 0 {
		partitions = defaultPartitions
	}
	if retention == 0 {
		retention = defaultRetention
	}
	h := &Hub{Name: name, Created: time.Now().UTC(), Retention: retention, ConsumerGroups: []string{DefaultConsumerGroup}}
	for i := range partitions {
		h.Partitions = append(h.Partitions, &Partition{ID: strconv.Itoa(i), LastTime: h.Created})
	}
	return h
}

func (s *Service) Reset() {
	s.mu.Lock()
	closing := s.takeAll()
	var containers []string
	for _, n := range s.st.Namespaces {
		containers = append(containers, n.Container)
	}
	now := time.Now().UTC()
	s.st = state{Namespaces: map[string]*Namespace{"localhost": {Name: "localhost", Created: now, Hubs: map[string]*Hub{}}}}
	s.kafka = map[string]string{}
	s.mu.Unlock()
	closing.detach()
	s.removeContainers(containers)
}

// ---- namespaces ----

// CreateNamespace creates an empty namespace.
func (s *Service) CreateNamespace(name string) error {
	if !validName(name) {
		return fmt.Errorf("namespace %q: %w", name, ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.Namespaces[k(name)]; ok {
		return fmt.Errorf("namespace %q: %w", name, ErrExists)
	}
	s.st.Namespaces[k(name)] = &Namespace{Name: name, Created: time.Now().UTC(), Hubs: map[string]*Hub{}}
	return nil
}

// DeleteNamespace deletes a namespace with its hubs and detaches their links.
func (s *Service) DeleteNamespace(name string) error {
	s.mu.Lock()
	n, ok := s.st.Namespaces[k(name)]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("namespace %q: %w", name, ErrNotFound)
	}
	delete(s.st.Namespaces, k(name))
	closing := s.take(k(name) + "/")
	s.mu.Unlock()
	closing.detach()
	s.removeContainers([]string{n.Container})
	return nil
}

// Namespaces lists namespace names.
func (s *Service) Namespaces() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, key := range core.SortedKeys(s.st.Namespaces) {
		out = append(out, s.st.Namespaces[key].Name)
	}
	return out
}

// ---- event hubs ----

// HubInfo describes an event hub.
type HubInfo struct {
	Name           string
	Created        time.Time
	Retention      time.Duration
	PartitionIDs   []string
	ConsumerGroups []string
}

func (h *Hub) info() HubInfo {
	i := HubInfo{Name: h.Name, Created: h.Created, Retention: h.Retention, ConsumerGroups: append([]string(nil), h.ConsumerGroups...)}
	for _, p := range h.Partitions {
		i.PartitionIDs = append(i.PartitionIDs, p.ID)
	}
	return i
}

// CreateHub creates an event hub; zero partitions and retention get the
// defaults (4 partitions, 24 hours). The partition count is fixed for life.
func (s *Service) CreateHub(ns, name string, partitions int, retention time.Duration) error {
	if !validName(name) || partitions < 0 || partitions > maxPartitions || retention < 0 {
		return fmt.Errorf("event hub %q: %w: 1-%d partitions, retention in the future", name, ErrInvalid, maxPartitions)
	}
	s.mu.Lock()
	n, ok := s.st.Namespaces[k(ns)]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("namespace %q: %w", ns, ErrNotFound)
	}
	if _, ok := n.Hubs[k(name)]; ok {
		s.mu.Unlock()
		return fmt.Errorf("event hub %q: %w", name, ErrExists)
	}
	hub := newHub(name, partitions, retention)
	n.Hubs[k(name)] = hub
	s.mu.Unlock()
	s.kafkaTopic(ns, hub.Name, len(hub.Partitions), hub.Retention, false)
	return nil
}

// GetHub describes an event hub.
func (s *Service) GetHub(ns, name string) (HubInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hub, err := s.hub(ns, name)
	if err != nil {
		return HubInfo{}, err
	}
	return hub.info(), nil
}

// Hubs lists the event hubs of a namespace.
func (s *Service) Hubs(ns string) ([]HubInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.st.Namespaces[k(ns)]
	if !ok {
		return nil, fmt.Errorf("namespace %q: %w", ns, ErrNotFound)
	}
	var out []HubInfo
	for _, key := range core.SortedKeys(n.Hubs) {
		out = append(out, n.Hubs[key].info())
	}
	return out, nil
}

// DeleteHub deletes an event hub and its events, and detaches its links.
func (s *Service) DeleteHub(ns, name string) error {
	s.mu.Lock()
	if _, err := s.hub(ns, name); err != nil {
		s.mu.Unlock()
		return err
	}
	delete(s.st.Namespaces[k(ns)].Hubs, k(name))
	closing := s.take(k(ns) + "/" + k(name) + "/")
	s.mu.Unlock()
	closing.detach()
	s.kafkaTopic(ns, name, 0, 0, true)
	return nil
}

// hub finds an event hub. Lock held.
func (s *Service) hub(ns, name string) (*Hub, error) {
	n, ok := s.st.Namespaces[k(ns)]
	if !ok {
		return nil, fmt.Errorf("namespace %q: %w", ns, ErrNotFound)
	}
	h, ok := n.Hubs[k(name)]
	if !ok {
		return nil, fmt.Errorf("event hub %q: %w", name, ErrNotFound)
	}
	return h, nil
}

// ---- consumer groups ----

// CreateConsumerGroup adds a consumer group to an event hub.
func (s *Service) CreateConsumerGroup(ns, hub, name string) error {
	if !validName(strings.TrimPrefix(name, "$")) {
		return fmt.Errorf("consumer group %q: %w", name, ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, err := s.hub(ns, hub)
	if err != nil {
		return err
	}
	if h.hasGroup(name) {
		return fmt.Errorf("consumer group %q: %w", name, ErrExists)
	}
	h.ConsumerGroups = append(h.ConsumerGroups, name)
	return nil
}

// DeleteConsumerGroup removes a consumer group ($Default stays) and detaches its receivers.
func (s *Service) DeleteConsumerGroup(ns, hub, name string) error {
	s.mu.Lock()
	h, err := s.hub(ns, hub)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if k(name) == k(DefaultConsumerGroup) {
		s.mu.Unlock()
		return fmt.Errorf("consumer group %q: %w: it cannot be deleted", name, ErrInvalid)
	}
	i := indexFold(h.ConsumerGroups, name)
	if i < 0 {
		s.mu.Unlock()
		return fmt.Errorf("consumer group %q: %w", name, ErrNotFound)
	}
	h.ConsumerGroups = append(h.ConsumerGroups[:i], h.ConsumerGroups[i+1:]...)
	closing := s.take(k(ns) + "/" + k(hub) + "/" + k(name) + "/")
	s.mu.Unlock()
	closing.detach()
	return nil
}

func (h *Hub) hasGroup(name string) bool { return indexFold(h.ConsumerGroups, name) >= 0 }

func indexFold(list []string, s string) int {
	for i, x := range list {
		if strings.EqualFold(x, s) {
			return i
		}
	}
	return -1
}

// validName: 1-50 letters, digits, '.', '-' and '_'.
func validName(s string) bool {
	if s == "" || len(s) > 50 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune(".-_", c)) {
			return false
		}
	}
	return true
}

// ---- the log ----

// pick chooses the partition of a send: the named one, the hash of key, or
// the next in turn. Lock held.
func (h *Hub) pick(id, key string) (*Partition, error) {
	if id != "" {
		for _, p := range h.Partitions {
			if p.ID == id {
				return p, nil
			}
		}
		return nil, amqp.Errorf("com.microsoft:argument-out-of-range", "The specified partition '%s' is invalid for EventHub '%s'.", id, h.Name)
	}
	if key != "" {
		f := fnv.New32a()
		f.Write([]byte(key))
		return h.Partitions[int(f.Sum32()%uint32(len(h.Partitions)))], nil
	}
	h.rr++
	return h.Partitions[h.rr%len(h.Partitions)], nil
}

// append stores m as the next event. Lock held.
func (p *Partition) append(m *amqp.Message, key string) {
	now := time.Now().UTC()
	p.Events = append(p.Events, &Event{Seq: p.Next, Offset: p.NextOffset, Enqueued: now, Key: key, Msg: m})
	p.Next++
	p.LastOffset = p.NextOffset
	p.NextOffset += int64(msgSize(m) + eventOverhead)
	p.LastTime = now
	if p.wake != nil {
		close(p.wake)
		p.wake = nil
	}
}

// waiter returns a channel that closes when the next event arrives. Lock held.
func (p *Partition) waiter() <-chan struct{} {
	if p.wake == nil {
		p.wake = make(chan struct{})
	}
	return p.wake
}

func msgSize(m *amqp.Message) int {
	n := 0
	for _, d := range m.Data {
		n += len(d)
	}
	if m.Value != nil {
		n += len(fmt.Sprint(m.Value))
	}
	return n
}

// Send enqueues an event from inside pogi, to the partition named or else
// by key hash or round-robin. It returns the partition used.
func (s *Service) Send(ns, hub, partition, key string, body []byte) (string, error) {
	m := &amqp.Message{Data: [][]byte{body}}
	if key != "" {
		m.MessageAnnotations = map[string]any{"x-opt-partition-key": key}
	}
	return s.enqueue(ns, hub, partition, key, m)
}

func (s *Service) enqueue(ns, hub, partition, key string, m *amqp.Message) (string, error) {
	if msgSize(m) > maxMessageSize {
		return "", amqp.Errorf(amqp.MessageSizeExceed, "The received message (delivery-id:0, size:%d bytes) exceeds the limit (%d bytes) currently allowed on the link.", msgSize(m), maxMessageSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, err := s.hub(ns, hub)
	if err != nil {
		return "", amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", hub)
	}
	p, err := h.pick(partition, key)
	if err != nil {
		return "", err
	}
	p.append(m, key)
	return p.ID, nil
}

// deliver takes events from other services: target is
// /subscriptions/s/resourceGroups/g/providers/Microsoft.EventHub/namespaces/<ns>/eventhubs/<hub>.
func (s *Service) deliver(target string, m *core.Message) error {
	parts := strings.Split(target, "/")
	for i := range parts {
		if strings.EqualFold(parts[i], "namespaces") && i+3 < len(parts) && strings.EqualFold(parts[i+2], "eventhubs") {
			_, err := s.Send(parts[i+1], parts[i+3], "", "", []byte(m.Body))
			return err
		}
	}
	return fmt.Errorf("invalid event hub %q", target)
}

// Tick trims events older than each hub's retention.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.st.Namespaces {
		for _, h := range n.Hubs {
			cut := now.Add(-h.Retention)
			for _, p := range h.Partitions {
				i := 0
				for i < len(p.Events) && p.Events[i].Enqueued.Before(cut) {
					i++
				}
				// ponytail: re-slicing keeps the trimmed events' memory until the next growth; copy if logs get huge
				p.Events = p.Events[i:]
			}
		}
	}
}

// begin is the first retained sequence number, or the next one when there are none.
func (p *Partition) begin() int64 {
	if len(p.Events) > 0 {
		return p.Events[0].Seq
	}
	return p.Next
}
