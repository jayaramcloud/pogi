// Package eventgrid emulates Azure Event Grid: custom topics, domains (with domain topics) and system
// topics, event subscriptions with filters, retries and dead-lettering, and the data plane that
// publishes events (POST /api/events, in the Event Grid or CloudEvents schema).
//
// A topic is reached at https://<topic>.eventgrid.localhost:7644/api/events, or on any host at
// /_azure/eventgrid/<topic>/api/events; any aeg-sas-key or aeg-sas-token is accepted. Subscriptions
// deliver to webhooks (after the validation handshake), Service Bus queues and topics, Event Hubs,
// storage queues and Azure Functions, through the hub. Storage account events (Blob) reach system
// topics through OnDeliver("microsoft.eventgrid"). The management plane is Go methods
// (CreateTopic, CreateSubscription ...) that the Azure resource manager calls.
package eventgrid

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Event Grid emulator.
type Service struct {
	hub  *core.Hub
	mu   sync.Mutex
	st   state
	stop func()
	http *http.Client

	// Backoff is how long to wait before delivery attempt n+1 after attempt n failed; tests shorten it.
	Backoff func(attempt int) time.Duration
}

type state struct {
	Topics  map[string]*Topic        `json:"topics"`
	Domains map[string]*Domain       `json:"domains"`
	System  map[string]*SystemTopic  `json:"systemTopics"`
	Subs    map[string]*Subscription `json:"subscriptions"` // by scope/eventSubscriptions/name
	Pending []*Delivery              `json:"pending,omitempty"`
	ARM     armres.Store             `json:"arm,omitempty"` // topics as the resource manager has them (location, tags, ...)
}

// Topic is a custom topic.
type Topic struct {
	Name        string    `json:"name"`
	InputSchema string    `json:"inputSchema"` // EventGridSchema | CloudEventSchemaV1_0 | CustomEventSchema; not enforced
	Created     time.Time `json:"created"`
}

// Domain groups domain topics behind one endpoint.
type Domain struct {
	Name        string    `json:"name"`
	InputSchema string    `json:"inputSchema"`
	Topics      []string  `json:"topics"`
	Created     time.Time `json:"created"`
}

// SystemTopic carries the events of an Azure resource, such as a storage account.
type SystemTopic struct {
	Name      string    `json:"name"`
	Source    string    `json:"source"` // resource ID of the publisher
	TopicType string    `json:"topicType"`
	Created   time.Time `json:"created"`
}

// Subscription delivers the events of one topic to a destination.
type Subscription struct {
	Name          string      `json:"name"`
	Scope         string      `json:"scope"` // topics/t, domains/d, domains/d/topics/t or systemTopics/s
	Destination   Destination `json:"destination"`
	Filter        Filter      `json:"filter"`
	Schema        string      `json:"eventDeliverySchema"` // EventGridSchema (default) | CloudEventSchemaV1_0
	Retry         Retry       `json:"retryPolicy"`
	DeadLetter    *DeadLetter `json:"deadLetterDestination,omitempty"`
	Labels        []string    `json:"labels,omitempty"`
	Created       time.Time   `json:"created"`
	SkipHandshake bool        `json:"-"` // create a webhook without validating it (seeding)
}

// Destination is where a subscription delivers. Type is an ARM endpointType: WebHook, ServiceBusQueue,
// ServiceBusTopic, EventHub, StorageQueue or AzureFunction.
type Destination struct {
	Type       string `json:"endpointType"`
	Endpoint   string `json:"endpointUrl,omitempty"` // WebHook
	ResourceID string `json:"resourceId,omitempty"`  // everything else
}

// Filter selects the events of a topic a subscription receives.
type Filter struct {
	IncludedEventTypes     []string         `json:"includedEventTypes,omitempty"`
	SubjectBeginsWith      string           `json:"subjectBeginsWith,omitempty"`
	SubjectEndsWith        string           `json:"subjectEndsWith,omitempty"`
	IsSubjectCaseSensitive bool             `json:"isSubjectCaseSensitive,omitempty"`
	Advanced               []AdvancedFilter `json:"advancedFilters,omitempty"`
}

// AdvancedFilter compares one event field (id, topic, subject, eventType, eventTime, dataVersion or data.a.b).
type AdvancedFilter struct {
	Operator string `json:"operatorType"`
	Key      string `json:"key"`
	Value    any    `json:"value,omitempty"`
	Values   []any  `json:"values,omitempty"`
}

// Retry is the retry policy.
type Retry struct {
	MaxAttempts int `json:"maxDeliveryAttempts"`      // default 30
	TTLMinutes  int `json:"eventTimeToLiveInMinutes"` // default 1440
}

// DeadLetter is a blob container that receives events that could not be delivered.
type DeadLetter struct {
	ResourceID string `json:"resourceId"` // the storage account
	Container  string `json:"blobContainerName"`
}

// Event is one published event, in either schema.
type Event struct {
	ID              string          `json:"id"`
	Topic           string          `json:"topic"`            // resource ID of the topic it was published to
	Source          string          `json:"source,omitempty"` // CloudEvents source, when it differs from the topic
	Subject         string          `json:"subject"`
	EventType       string          `json:"eventType"`
	EventTime       string          `json:"eventTime"`
	DataVersion     string          `json:"dataVersion"`
	Data            json.RawMessage `json:"data,omitempty"`
	DataContentType string          `json:"dataContentType,omitempty"`
}

// Delivery is an event waiting for its next attempt at one subscription.
type Delivery struct {
	Sub      string    `json:"sub"`
	Event    Event     `json:"event"`
	Attempts int       `json:"attempts"`
	Due      time.Time `json:"due"`
	Expires  time.Time `json:"expires"`
	Reason   string    `json:"reason,omitempty"` // of the last failure
	Status   int       `json:"status,omitempty"` // last HTTP status
	Trace    string    `json:"trace,omitempty"`  // the flow that published the event
}

// New creates the Event Grid service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, http: &http.Client{Timeout: 30 * time.Second}, Backoff: backoff}
	s.Reset()
	hub.OnDeliver("microsoft.eventgrid", s.system)
	s.registerARM()
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "eventgrid", Cloud: core.Azure, Title: "Event Grid", Protocol: "azure-json", Match: func(r *http.Request) bool {
		_, _, ok := target(r)
		return ok
	}}
}

// Close stops the retry clock.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Topics: map[string]*Topic{}, Domains: map[string]*Domain{}, System: map[string]*SystemTopic{}, Subs: map[string]*Subscription{}}
	s.mu.Unlock()
}

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
	if st.Topics == nil {
		st.Topics = map[string]*Topic{}
	}
	if st.Domains == nil {
		st.Domains = map[string]*Domain{}
	}
	if st.System == nil {
		st.System = map[string]*SystemTopic{}
	}
	if st.Subs == nil {
		st.Subs = map[string]*Subscription{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- management ----

// resourceID is the ARM ID of a scope (topics/t, domains/d/topics/t, systemTopics/s).
func resourceID(scope string) string {
	return "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/Microsoft.EventGrid/" + scope
}

// scopeOf accepts a scope or a full resource ID of a topic, domain, domain topic or system topic.
func scopeOf(s string) string {
	if _, rest, ok := strings.Cut(s, "/providers/Microsoft.EventGrid/"); ok {
		s = rest
	}
	return strings.Trim(s, "/")
}

var (
	errExists = func(what string) error {
		return core.Errorf(http.StatusConflict, "Conflict", "%s already exists.", what)
	}
	errNotFound = func(what string) error {
		return core.Errorf(http.StatusNotFound, "ResourceNotFound", "The %s was not found.", what)
	}
)

func orStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// CreateTopic adds a custom topic; inputSchema defaults to EventGridSchema.
func (s *Service) CreateTopic(name, inputSchema string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Topics[name] != nil {
		return errExists("topic " + name)
	}
	s.st.Topics[name] = &Topic{Name: name, InputSchema: orStr(inputSchema, "EventGridSchema"), Created: time.Now().UTC()}
	return nil
}

// DeleteTopic removes a custom topic and its subscriptions.
func (s *Service) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Topics[name] == nil {
		return errNotFound("topic " + name)
	}
	delete(s.st.Topics, name)
	s.dropScope("topics/" + name)
	return nil
}

// CreateDomain adds a domain; inputSchema defaults to EventGridSchema.
func (s *Service) CreateDomain(name, inputSchema string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Domains[name] != nil {
		return errExists("domain " + name)
	}
	s.st.Domains[name] = &Domain{Name: name, InputSchema: orStr(inputSchema, "EventGridSchema"), Created: time.Now().UTC()}
	return nil
}

// DeleteDomain removes a domain, its topics and all their subscriptions.
func (s *Service) DeleteDomain(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Domains[name] == nil {
		return errNotFound("domain " + name)
	}
	delete(s.st.Domains, name)
	s.dropScope("domains/" + name)
	return nil
}

// CreateDomainTopic adds a topic to a domain.
func (s *Service) CreateDomainTopic(domain, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.st.Domains[domain]
	if d == nil {
		return errNotFound("domain " + domain)
	}
	if slices.Contains(d.Topics, name) {
		return errExists("domain topic " + name)
	}
	d.Topics = append(d.Topics, name)
	return nil
}

// DeleteDomainTopic removes a domain topic and its subscriptions.
func (s *Service) DeleteDomainTopic(domain, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.st.Domains[domain]
	if d == nil || !slices.Contains(d.Topics, name) {
		return errNotFound("domain topic " + name)
	}
	d.Topics = slices.DeleteFunc(d.Topics, func(t string) bool { return t == name })
	s.dropScope("domains/" + domain + "/topics/" + name)
	return nil
}

// CreateSystemTopic adds a system topic for the Azure resource source (for example a storage account's
// resource ID, whose Blob events arrive from the Blob service); topicType is informational.
func (s *Service) CreateSystemTopic(name, source, topicType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.System[name] != nil {
		return errExists("system topic " + name)
	}
	s.st.System[name] = &SystemTopic{Name: name, Source: source, TopicType: topicType, Created: time.Now().UTC()}
	return nil
}

// DeleteSystemTopic removes a system topic and its subscriptions.
func (s *Service) DeleteSystemTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.System[name] == nil {
		return errNotFound("system topic " + name)
	}
	delete(s.st.System, name)
	s.dropScope("systemTopics/" + name)
	return nil
}

// dropScope removes the subscriptions of a scope (and of the topics under it), and their pending deliveries.
func (s *Service) dropScope(scope string) {
	for k, sub := range s.st.Subs {
		if sub.Scope == scope || strings.HasPrefix(sub.Scope, scope+"/") {
			delete(s.st.Subs, k)
		}
	}
	s.st.Pending = slices.DeleteFunc(s.st.Pending, func(d *Delivery) bool { return s.st.Subs[d.Sub] == nil })
}

// hasScope reports whether the scope exists; a domain topic that does not is created on the spot, as
// Event Grid does with the first subscription.
func (s *Service) hasScope(scope string) bool {
	p := strings.Split(scope, "/")
	switch {
	case len(p) == 2 && p[0] == "topics":
		return s.st.Topics[p[1]] != nil
	case len(p) == 2 && p[0] == "domains":
		return s.st.Domains[p[1]] != nil
	case len(p) == 2 && p[0] == "systemTopics":
		return s.st.System[p[1]] != nil
	case len(p) == 4 && p[0] == "domains" && p[2] == "topics":
		d := s.st.Domains[p[1]]
		if d == nil {
			return false
		}
		if !slices.Contains(d.Topics, p[3]) {
			d.Topics = append(d.Topics, p[3])
		}
		return true
	}
	return false
}

func subKey(scope, name string) string { return scope + "/eventSubscriptions/" + name }

// CreateSubscription adds an event subscription to a topic, domain, domain topic or system topic (a scope
// or full resource ID). A webhook must pass the validation handshake first, or creation fails.
func (s *Service) CreateSubscription(scope string, sub Subscription) error {
	scope = scopeOf(scope)
	sub.Scope, sub.Created = scope, time.Now().UTC()
	sub.Schema = orStr(sub.Schema, "EventGridSchema")
	if sub.Retry.MaxAttempts == 0 {
		sub.Retry.MaxAttempts = 30
	}
	if sub.Retry.TTLMinutes == 0 {
		sub.Retry.TTLMinutes = 1440
	}
	if sub.Name == "" {
		return core.Errorf(http.StatusBadRequest, "BadRequest", "The event subscription needs a name.")
	}
	if err := sub.Destination.check(); err != nil {
		return err
	}
	s.mu.Lock()
	ok, dup := s.hasScope(scope), s.st.Subs[subKey(scope, sub.Name)] != nil
	s.mu.Unlock()
	switch {
	case !ok:
		return errNotFound("topic " + scope)
	case dup:
		return errExists("event subscription " + sub.Name)
	}
	if sub.Destination.Type == "WebHook" && !sub.SkipHandshake {
		if err := s.handshake(&sub); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Subs[subKey(scope, sub.Name)] != nil {
		return errExists("event subscription " + sub.Name)
	}
	s.st.Subs[subKey(scope, sub.Name)] = &sub
	return nil
}

// DeleteSubscription removes a subscription and its pending deliveries.
func (s *Service) DeleteSubscription(scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := subKey(scopeOf(scope), name)
	if s.st.Subs[k] == nil {
		return errNotFound("event subscription " + name)
	}
	delete(s.st.Subs, k)
	s.st.Pending = slices.DeleteFunc(s.st.Pending, func(d *Delivery) bool { return d.Sub == k })
	return nil
}

// Subscriptions lists the subscriptions of a scope, by name.
func (s *Service) Subscriptions(scope string) []Subscription {
	scope = scopeOf(scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Subscription
	for _, k := range core.SortedKeys(s.st.Subs) {
		if s.st.Subs[k].Scope == scope {
			out = append(out, *s.st.Subs[k])
		}
	}
	return out
}

// check validates a destination at creation.
func (d Destination) check() error {
	bad := func(format string, a ...any) error {
		return core.Errorf(http.StatusBadRequest, "InvalidRequest", format, a...)
	}
	switch d.Type {
	case "WebHook":
		if !strings.HasPrefix(d.Endpoint, "http://") && !strings.HasPrefix(d.Endpoint, "https://") {
			return bad("The webhook endpoint %q is not an HTTP URL.", d.Endpoint)
		}
	case "ServiceBusQueue", "ServiceBusTopic", "EventHub", "StorageQueue", "AzureFunction":
		if svc, ok := core.DeliverTarget(d.ResourceID); !ok || svc == "" {
			return bad("The resource ID %q of the %s destination is not valid.", d.ResourceID, d.Type)
		}
	default:
		return bad("The endpoint type %q is not supported.", d.Type)
	}
	return nil
}

// ---- console ----

// Resources lists topics, domains, system topics, subscriptions and deliveries waiting for a retry.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := func(scope string) string {
		c := 0
		for _, sub := range s.st.Subs {
			if sub.Scope == scope {
				c++
			}
		}
		return itoa(c)
	}
	topics := core.Table{Name: "Topics", Columns: []string{"Name", "Schema", "Subscriptions"}}
	for _, k := range core.SortedKeys(s.st.Topics) {
		topics.Rows = append(topics.Rows, []string{k, s.st.Topics[k].InputSchema, n("topics/" + k)})
	}
	domains := core.Table{Name: "Domains", Columns: []string{"Name", "Schema", "Topics"}}
	for _, k := range core.SortedKeys(s.st.Domains) {
		domains.Rows = append(domains.Rows, []string{k, s.st.Domains[k].InputSchema, strings.Join(s.st.Domains[k].Topics, ", ")})
	}
	system := core.Table{Name: "System topics", Columns: []string{"Name", "Source", "Type", "Subscriptions"}}
	for _, k := range core.SortedKeys(s.st.System) {
		t := s.st.System[k]
		system.Rows = append(system.Rows, []string{k, t.Source, t.TopicType, n("systemTopics/" + k)})
	}
	subs := core.Table{Name: "Subscriptions", Columns: []string{"Subscription", "Destination", "Schema"}}
	for _, k := range core.SortedKeys(s.st.Subs) {
		d := s.st.Subs[k].Destination
		subs.Rows = append(subs.Rows, []string{k, d.Type + " " + orStr(d.Endpoint, d.ResourceID), s.st.Subs[k].Schema})
	}
	pending := core.Table{Name: "Retries", Columns: []string{"Subscription", "Event", "Attempts", "Next attempt", "Last failure"}}
	for _, d := range s.st.Pending {
		pending.Rows = append(pending.Rows, []string{d.Sub, d.Event.ID, itoa(d.Attempts), d.Due.Format(time.RFC3339), d.Reason})
	}
	return []core.Table{topics, domains, system, subs, pending}
}

// AddSubscription creates a subscription from its JSON form, without the webhook handshake, for seeding:
// an endpoint that is not running yet must not stop pogi from starting.
func (s *Service) AddSubscription(scope string, raw []byte) error {
	var sub Subscription
	if err := json.Unmarshal(raw, &sub); err != nil {
		return core.Errorf(http.StatusBadRequest, "BadRequest", "%v", err)
	}
	sub.SkipHandshake = true
	return s.CreateSubscription(scope, sub)
}

// Inspect returns one topic, domain, system topic or subscription.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Topics":
		if t := s.st.Topics[id]; t != nil {
			return map[string]any{"topic": t, "id": resourceID("topics/" + id)}
		}
	case "Domains":
		if d := s.st.Domains[id]; d != nil {
			return map[string]any{"domain": d, "id": resourceID("domains/" + id)}
		}
	case "System topics":
		if t := s.st.System[id]; t != nil {
			return map[string]any{"systemTopic": t, "id": resourceID("systemTopics/" + id)}
		}
	case "Subscriptions":
		if sub := s.st.Subs[id]; sub != nil {
			return map[string]any{"subscription": sub, "id": resourceID(id)}
		}
	}
	return nil
}
