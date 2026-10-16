// Package queue emulates Azure Queue Storage on Azurite's port (10001):
// queues with metadata and access policies, and messages with visibility
// timeouts, TTLs, pop receipts, peek, update and clear. Connect with a
// connection string whose QueueEndpoint is
// http://127.0.0.1:10001/devstoreaccount1. Shared Key and SAS are accepted
// without signature checks.
package queue

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Queue Storage emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
}

type state struct {
	Accounts map[string]*Account `json:"accounts"`
	ARM      armres.Store        `json:"arm,omitempty"` // service settings the resource manager holds
}

// Account is a storage account (devstoreaccount1 by default).
type Account struct {
	Queues map[string]*Queue `json:"queues"`
	Props  string            `json:"props,omitempty"` // service properties XML
}

// Queue holds messages in arrival order.
// ponytail: operations scan the slice; index by id if queues get deep.
type Queue struct {
	Name     string            `json:"name"`
	Meta     map[string]string `json:"meta,omitempty"`
	ACL      string            `json:"acl,omitempty"` // SignedIdentifiers XML
	Messages []*Message        `json:"messages"`
}

// Message is a queue message.
type Message struct {
	ID           string    `json:"id"`
	Text         string    `json:"text"`
	Inserted     time.Time `json:"inserted"`
	Expires      time.Time `json:"expires"`
	NextVisible  time.Time `json:"nextVisible"`
	DequeueCount int64     `json:"dequeueCount"`
	PopReceipt   string    `json:"popReceipt"`
}

// Port is Azurite's queue port.
const Port = 10001

const week = 7 * 24 * 3600 // default TTL and longest visibility timeout, in seconds

var never = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC) // messagettl=-1

// New creates the Queue Storage service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.registerARM()
	hub.OnDeliver("microsoft.storage", s.deliver)
	return s
}

// deliver enqueues the body, base64 as Event Grid does, on a queue named by its resource ID:
// .../storageAccounts/<account>/queueServices/default/queues/<queue>.
func (s *Service) deliver(id string, m *core.Message) error {
	_, rest, _ := strings.Cut(strings.ToLower(id), "/storageaccounts/")
	account, rest, _ := strings.Cut(rest, "/")
	_, queue, ok := strings.Cut(rest, "/queues/")
	if !ok {
		return fmt.Errorf("%s is not a storage queue", id)
	}
	// the IDs are matched lower-cased; queue names are lower-case already, account names too
	return s.Send(account, queue, base64.StdEncoding.EncodeToString([]byte(m.Body)))
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azqueue", Cloud: core.Azure, Title: "Queue Storage", Protocol: "azure-xml", Match: armres.HostMatch("queue")}
}

func (s *Service) Port() int { return Port }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Accounts: map[string]*Account{}}
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
	if st.Accounts == nil {
		st.Accounts = map[string]*Account{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

var (
	errNoQueue   = func() error { return core.Errorf(404, "QueueNotFound", "The specified queue does not exist.") }
	errNoMessage = func() error { return core.Errorf(404, "MessageNotFound", "The specified message does not exist.") }
	errXML       = func() error {
		return core.Errorf(400, "InvalidXmlDocument", "XML specified is not syntactically valid.")
	}
)

// azError writes the Azure Storage XML error with x-ms-error-code.
func azError(w http.ResponseWriter, r *http.Request, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("x-ms-error-code", e.Code)
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status) // no Content-Type: the SDK would parse the empty body as XML
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.Status)
	io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>`+e.Code+`</Code><Message>`+
		esc(e.Msg+"\nRequestId:"+c.RequestID+"\nTime:"+time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z"))+`</Message></Error>`)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = armres.AccountPath(r, "queue")
	c := core.C(r)
	p := append(strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 4), "", "", "")
	h := w.Header()
	h.Set("x-ms-request-id", c.RequestID)
	h.Set("x-ms-version", or(r.Header.Get("x-ms-version"), "2025-01-05"))
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if id := r.Header.Get("x-ms-client-request-id"); id != "" {
		h.Set("x-ms-client-request-id", id)
	}
	op, fn := s.route(w, r, p[0], p[1], p[2], p[3])
	c.Op = op
	if fn == nil {
		azError(w, r, c, core.Errorf(400, "UnsupportedHttpVerb", "pogi does not implement %s %s", r.Method, r.URL.RequestURI()))
		return
	}
	if err := c.Intercept(); err != nil {
		azError(w, r, c, err)
		return
	}
	if err := fn(); err != nil {
		azError(w, r, c, err)
	}
}

func (s *Service) route(w http.ResponseWriter, r *http.Request, account, queue, sub, id string) (string, func() error) {
	q := r.URL.Query()
	comp, m := q.Get("comp"), r.Method
	get := m == http.MethodGet || m == http.MethodHead
	switch {
	case queue == "":
		svc := q.Get("restype") == "service"
		switch {
		case get && comp == "list":
			return "ListQueues", func() error { return s.list(w, r, account) }
		case svc && comp == "properties" && get:
			return "GetServiceProperties", func() error { return s.serviceProps(w, account, nil) }
		case svc && comp == "properties" && m == http.MethodPut:
			return "SetServiceProperties", func() error { return s.serviceProps(w, account, core.Body(r)) }
		case svc && comp == "stats":
			return "GetServiceStats", func() error {
				writeXML(w, http.StatusOK, `<StorageServiceStats><GeoReplication><Status>live</Status><LastSyncTime>`+
					gmt(time.Now())+`</LastSyncTime></GeoReplication></StorageServiceStats>`)
				return nil
			}
		}
	case sub == "":
		switch {
		case m == http.MethodPut && comp == "":
			return "CreateQueue", func() error { return s.create(w, r, account, queue) }
		case m == http.MethodDelete && comp == "":
			return "DeleteQueue", func() error { return s.remove(w, account, queue) }
		case get && comp == "metadata":
			return "GetQueueMetadata", func() error {
				return s.withQueue(account, queue, func(qu *Queue, _ time.Time) error {
					w.Header().Set("x-ms-approximate-messages-count", strconv.Itoa(len(qu.Messages)))
					for k, v := range qu.Meta {
						w.Header().Set("x-ms-meta-"+k, v)
					}
					w.WriteHeader(http.StatusOK)
					return nil
				})
			}
		case m == http.MethodPut && comp == "metadata":
			return "SetQueueMetadata", func() error {
				return s.withQueue(account, queue, func(qu *Queue, _ time.Time) error {
					qu.Meta = meta(r.Header)
					w.WriteHeader(http.StatusNoContent)
					return nil
				})
			}
		case get && comp == "acl":
			return "GetQueueACL", func() error {
				return s.withQueue(account, queue, func(qu *Queue, _ time.Time) error {
					w.Header().Set("Content-Type", "application/xml")
					io.WriteString(w, or(qu.ACL, `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers />`))
					return nil
				})
			}
		case m == http.MethodPut && comp == "acl":
			return "SetQueueACL", func() error {
				body := core.Body(r)
				if err := checkACL(body); err != nil {
					return err
				}
				return s.withQueue(account, queue, func(qu *Queue, _ time.Time) error {
					qu.ACL = string(body)
					w.WriteHeader(http.StatusNoContent)
					return nil
				})
			}
		}
	case sub != "messages":
	case id == "":
		switch {
		case m == http.MethodPost:
			return "PutMessage", func() error { return s.put(w, r, account, queue) }
		case m == http.MethodGet && strings.EqualFold(q.Get("peekonly"), "true"):
			return "PeekMessages", func() error { return s.get(w, r, account, queue, true) }
		case m == http.MethodGet:
			return "GetMessages", func() error { return s.get(w, r, account, queue, false) }
		case m == http.MethodDelete:
			return "ClearMessages", func() error {
				return s.withQueue(account, queue, func(qu *Queue, _ time.Time) error {
					qu.Messages = nil
					w.WriteHeader(http.StatusNoContent)
					return nil
				})
			}
		}
	case m == http.MethodPut:
		return "UpdateMessage", func() error { return s.update(w, r, account, queue, id) }
	case m == http.MethodDelete:
		return "DeleteMessage", func() error {
			return s.withMessage(r, account, queue, id, func(qu *Queue, i int, _ time.Time) error {
				qu.Messages = slices.Delete(qu.Messages, i, i+1)
				w.WriteHeader(http.StatusNoContent)
				return nil
			})
		}
	}
	return "", nil
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func esc(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func gmt(t time.Time) string { return t.UTC().Format(http.TimeFormat) }

func secs(n int) time.Duration { return time.Duration(n) * time.Second }

func writeXML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?>`+body)
}

// intParam reads an integer query parameter in [lo, hi]; absent means def.
func intParam(q url.Values, name string, def, lo, hi int) (int, error) {
	v := q.Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, core.Errorf(400, "InvalidQueryParameterValue", "Value for one of the query parameters specified in the request URI is invalid.")
	}
	if n < lo || n > hi {
		return 0, core.Errorf(400, "OutOfRangeQueryParameterValue", "One of the query parameters specified in the request URI is outside the permissible range.")
	}
	return n, nil
}

// ---- queues ----

func (s *Service) account(name string) *Account {
	a := s.st.Accounts[name]
	if a == nil {
		a = &Account{Queues: map[string]*Queue{}}
		s.st.Accounts[name] = a
	}
	return a
}

// withQueue runs f on a queue under the lock, after dropping expired messages.
func (s *Service) withQueue(account, name string, f func(*Queue, time.Time) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.account(account).Queues[name]
	if q == nil {
		return errNoQueue()
	}
	now := time.Now().UTC()
	q.Messages = slices.DeleteFunc(q.Messages, func(m *Message) bool { return !now.Before(m.Expires) })
	return f(q, now)
}

func meta(h http.Header) map[string]string {
	var m map[string]string
	for k, v := range h {
		if name, ok := strings.CutPrefix(strings.ToLower(k), "x-ms-meta-"); ok {
			if m == nil {
				m = map[string]string{}
			}
			m[name] = v[0]
		}
	}
	return m
}

var queueName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

func (s *Service) create(w http.ResponseWriter, r *http.Request, account, name string) error {
	if len(name) < 3 || len(name) > 63 {
		return core.Errorf(400, "OutOfRangeInput", "The specified resource name length is not within the permissible limits.")
	}
	if !queueName.MatchString(name) {
		return core.Errorf(400, "InvalidResourceName", "The specified resource name contains invalid characters.")
	}
	md := meta(r.Header)
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if q := a.Queues[name]; q != nil {
		if !maps.Equal(q.Meta, md) {
			return core.Errorf(409, "QueueAlreadyExists", "The specified queue already exists.")
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	a.Queues[name] = &Queue{Name: name, Meta: md}
	w.WriteHeader(http.StatusCreated)
	return nil
}

func (s *Service) remove(w http.ResponseWriter, account, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Queues[name] == nil {
		return errNoQueue()
	}
	delete(a.Queues, name)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// checkACL enforces the service limits: at most 5 identifiers of up to 64 characters.
func checkACL(body []byte) error {
	var in struct {
		IDs []string `xml:"SignedIdentifier>Id"`
	}
	if len(bytes.TrimSpace(body)) > 0 && xml.Unmarshal(body, &in) != nil || len(in.IDs) > 5 ||
		slices.ContainsFunc(in.IDs, func(id string) bool { return len(id) > 64 }) {
		return errXML()
	}
	return nil
}

func (s *Service) list(w http.ResponseWriter, r *http.Request, account string) error {
	q := r.URL.Query()
	prefix, marker := q.Get("prefix"), q.Get("marker")
	max := 5000
	if n, err := strconv.Atoi(q.Get("maxresults")); err == nil && n > 0 {
		max = min(n, max)
	}
	withMeta := strings.Contains(q.Get("include"), "metadata")
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	var b strings.Builder
	fmt.Fprintf(&b, `<EnumerationResults ServiceEndpoint="http://%s/%s/"><Prefix>%s</Prefix>`, esc(r.Host), esc(account), esc(prefix))
	if marker != "" {
		b.WriteString("<Marker>" + esc(marker) + "</Marker>")
	}
	b.WriteString("<MaxResults>" + strconv.Itoa(max) + "</MaxResults><Queues>")
	next, n := "", 0
	for _, name := range core.SortedKeys(a.Queues) {
		if !strings.HasPrefix(name, prefix) || name < marker {
			continue
		}
		if n == max {
			next = name
			break
		}
		b.WriteString("<Queue><Name>" + name + "</Name>")
		if withMeta {
			b.WriteString("<Metadata>")
			for _, k := range core.SortedKeys(a.Queues[name].Meta) {
				b.WriteString("<" + k + ">" + esc(a.Queues[name].Meta[k]) + "</" + k + ">")
			}
			b.WriteString("</Metadata>")
		}
		b.WriteString("</Queue>")
		n++
	}
	b.WriteString("</Queues><NextMarker>" + next + "</NextMarker></EnumerationResults>")
	writeXML(w, http.StatusOK, b.String())
	return nil
}

func (s *Service) serviceProps(w http.ResponseWriter, account string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if body != nil {
		a.Props = string(body)
		w.WriteHeader(http.StatusAccepted)
		return nil
	}
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, or(a.Props, `<?xml version="1.0" encoding="utf-8"?><StorageServiceProperties><Logging><Version>1.0</Version><Delete>false</Delete><Read>false</Read><Write>false</Write><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging><HourMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics><MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics><Cors /></StorageServiceProperties>`))
	return nil
}

// ---- messages ----

func newMessage(text string, now time.Time, vt, ttl int) *Message {
	m := &Message{ID: core.UUID(), Text: text, Inserted: now, Expires: never, NextVisible: now.Add(secs(vt)), PopReceipt: core.NewID()}
	if ttl != -1 {
		m.Expires = now.Add(secs(ttl))
	}
	return m
}

func messageText(body []byte) (string, error) {
	var in struct {
		Text *string `xml:"MessageText"`
	}
	if xml.Unmarshal(body, &in) != nil || in.Text == nil {
		return "", errXML()
	}
	if len(*in.Text) > 64<<10 {
		return "", core.Errorf(400, "MessageTooLarge", "The message exceeds the maximum allowed size.")
	}
	return *in.Text, nil
}

// msgXML renders a message; pop adds the receipt and next visibility, body the count and text.
func msgXML(m *Message, pop, body bool) string {
	s := "<QueueMessage><MessageId>" + m.ID + "</MessageId><InsertionTime>" + gmt(m.Inserted) +
		"</InsertionTime><ExpirationTime>" + gmt(m.Expires) + "</ExpirationTime>"
	if pop {
		s += "<PopReceipt>" + m.PopReceipt + "</PopReceipt><TimeNextVisible>" + gmt(m.NextVisible) + "</TimeNextVisible>"
	}
	if body {
		s += "<DequeueCount>" + strconv.FormatInt(m.DequeueCount, 10) + "</DequeueCount><MessageText>" + esc(m.Text) + "</MessageText>"
	}
	return s + "</QueueMessage>"
}

// visible returns up to n live messages that are visible at now, oldest first.
func visible(q *Queue, now time.Time, n int) []*Message {
	var out []*Message
	for _, m := range q.Messages {
		if len(out) == n {
			break
		}
		if !m.NextVisible.After(now) && now.Before(m.Expires) {
			out = append(out, m)
		}
	}
	return out
}

func (s *Service) put(w http.ResponseWriter, r *http.Request, account, name string) error {
	q := r.URL.Query()
	vt, err := intParam(q, "visibilitytimeout", 0, 0, week)
	if err != nil {
		return err
	}
	ttl, err := intParam(q, "messagettl", week, -1, math.MaxInt32)
	if err == nil && ttl == 0 {
		err = core.Errorf(400, "OutOfRangeQueryParameterValue", "messagettl must be -1 or a positive number of seconds.")
	}
	if err != nil {
		return err
	}
	if ttl != -1 && vt >= ttl {
		return core.Errorf(400, "InvalidQueryParameterValue", "The visibility timeout must be smaller than the message time-to-live.")
	}
	text, err := messageText(core.Body(r))
	if err != nil {
		return err
	}
	return s.withQueue(account, name, func(qu *Queue, now time.Time) error {
		m := newMessage(text, now, vt, ttl)
		qu.Messages = append(qu.Messages, m)
		writeXML(w, http.StatusCreated, "<QueueMessagesList>"+msgXML(m, true, false)+"</QueueMessagesList>")
		return nil
	})
}

// get dequeues (or peeks) up to numofmessages visible messages.
func (s *Service) get(w http.ResponseWriter, r *http.Request, account, name string, peek bool) error {
	q := r.URL.Query()
	n, err := intParam(q, "numofmessages", 1, 1, 32)
	if err != nil {
		return err
	}
	vt, err := intParam(q, "visibilitytimeout", 30, 1, week)
	if err != nil && !peek {
		return err
	}
	return s.withQueue(account, name, func(qu *Queue, now time.Time) error {
		var b strings.Builder
		for _, m := range visible(qu, now, n) {
			if !peek {
				m.DequeueCount++
				m.PopReceipt, m.NextVisible = core.NewID(), now.Add(secs(vt))
			}
			b.WriteString(msgXML(m, !peek, true))
		}
		if !peek && s.hub != nil {
			s.hub.MarkDirty() // a GET that changes state
		}
		writeXML(w, http.StatusOK, "<QueueMessagesList>"+b.String()+"</QueueMessagesList>")
		return nil
	})
}

// withMessage finds a message by id and checks the request's pop receipt.
func (s *Service) withMessage(r *http.Request, account, name, id string, f func(*Queue, int, time.Time) error) error {
	pr := r.URL.Query().Get("popreceipt")
	if pr == "" {
		return core.Errorf(400, "MissingRequiredQueryParameter", "A query parameter that's mandatory for this request is not specified.")
	}
	return s.withQueue(account, name, func(qu *Queue, now time.Time) error {
		i := slices.IndexFunc(qu.Messages, func(m *Message) bool { return m.ID == id })
		switch {
		case i < 0:
			return errNoMessage()
		case qu.Messages[i].PopReceipt != pr:
			return core.Errorf(400, "PopReceiptMismatch", "The specified pop receipt did not match the pop receipt for a dequeued message.")
		}
		return f(qu, i, now)
	})
}

func (s *Service) update(w http.ResponseWriter, r *http.Request, account, name, id string) error {
	vt, err := intParam(r.URL.Query(), "visibilitytimeout", 0, 0, week)
	if err != nil {
		return err
	}
	var text *string
	if body := core.Body(r); len(bytes.TrimSpace(body)) > 0 {
		t, err := messageText(body)
		if err != nil {
			return err
		}
		text = &t
	}
	return s.withMessage(r, account, name, id, func(qu *Queue, i int, now time.Time) error {
		m := qu.Messages[i]
		if text != nil {
			m.Text = *text
		}
		m.PopReceipt, m.NextVisible = core.NewID(), now.Add(secs(vt))
		w.Header().Set("x-ms-popreceipt", m.PopReceipt)
		w.Header().Set("x-ms-time-next-visible", gmt(m.NextVisible))
		w.WriteHeader(http.StatusNoContent)
		return nil
	})
}

// ---- console ----

// Peek returns up to max visible messages without changing them.
func (s *Service) Peek(account, queue string, max int) []Message {
	out := []Message{}
	s.withQueue(account, queue, func(q *Queue, now time.Time) error {
		for _, m := range visible(q, now, max) {
			out = append(out, *m)
		}
		return nil
	})
	return out
}

// Send enqueues a message that is visible now and lives for 7 days.
func (s *Service) Send(account, queue, text string) error {
	err := s.withQueue(account, queue, func(q *Queue, now time.Time) error {
		q.Messages = append(q.Messages, newMessage(text, now, 0, week))
		return nil
	})
	if err == nil && s.hub != nil {
		s.hub.MarkDirty()
	}
	return err
}

// Resources lists queues across accounts.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	t := core.Table{Name: "Queues", Columns: []string{"Name", "Account", "Messages", "Visible"}}
	for _, acct := range core.SortedKeys(s.st.Accounts) {
		a := s.st.Accounts[acct]
		for _, name := range core.SortedKeys(a.Queues) {
			live := 0
			for _, m := range a.Queues[name].Messages {
				if now.Before(m.Expires) {
					live++
				}
			}
			vis := len(visible(a.Queues[name], now, -1))
			t.Rows = append(t.Rows, []string{name, acct, strconv.Itoa(live), strconv.Itoa(vis)})
		}
	}
	return []core.Table{t}
}

// Inspect returns a queue's metadata and a peek at its first 20 visible messages.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, acct := range core.SortedKeys(s.st.Accounts) {
		if q := s.st.Accounts[acct].Queues[id]; q != nil {
			msgs := []Message{}
			for _, m := range visible(q, time.Now(), 20) {
				msgs = append(msgs, *m)
			}
			return map[string]any{"account": acct, "name": q.Name, "metadata": q.Meta, "messages": msgs}
		}
	}
	return nil
}
