// Package email emulates Azure Communication Services Email: POST /emails:send takes an EmailMessage and
// answers 202 with an operation-location; GET /emails/operations/<id> reports Succeeded. Sent messages
// are captured in the Outbox (the console inbox shows them) and nothing leaves pogi.
//
// A resource is reached at https://<name>.communication.localhost:7644, or on any host under
// /_azure/communication/<name>. The plain paths on any host belong to the resource "default" when the
// request carries an api-version and no AWS signature. HMAC-SHA256 authentication is accepted without
// a check, or verified when Config.ValidateSigs is on and the resource's access key is known (SetKey).
package email

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

const maxOutbox = 1000

// Service is the Communication Services Email emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
}

type state struct {
	Keys   map[string]string `json:"keys"`          // resource name → base64 access key, for HMAC checks
	Outbox []Message         `json:"outbox"`        // the last maxOutbox messages
	ARM    armres.Store      `json:"arm,omitempty"` // resources as the resource manager has them
}

// Message is one captured email.
type Message struct {
	ID          string            `json:"id"`
	Resource    string            `json:"resource"`
	From        string            `json:"senderAddress"`
	To          []Address         `json:"to,omitempty"`
	Cc          []Address         `json:"cc,omitempty"`
	Bcc         []Address         `json:"bcc,omitempty"`
	ReplyTo     []Address         `json:"replyTo,omitempty"`
	Subject     string            `json:"subject"`
	Text        string            `json:"plainText,omitempty"`
	HTML        string            `json:"html,omitempty"`
	Attachments []Attachment      `json:"attachments,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Time        time.Time         `json:"time"`
}

// Address is a recipient.
type Address struct {
	Address     string `json:"address"`
	DisplayName string `json:"displayName,omitempty"`
}

// Attachment is a file sent with a message; Content is base64.
type Attachment struct {
	Name        string `json:"name"`
	ContentType string `json:"contentType"`
	Content     string `json:"contentInBase64"`
}

// New creates the Email service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azemail", Cloud: core.Azure, Title: "Communication Email", Protocol: "azure-json", Match: func(r *http.Request) bool {
		_, _, ok := target(r)
		return ok
	}}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Keys: map[string]string{}}
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
	if st.Keys == nil {
		st.Keys = map[string]string{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// SetKey gives a resource an access key (base64, as in its connection string), so that its HMAC
// signatures are checked when Config.ValidateSigs is on.
func (s *Service) SetKey(resource, key string) {
	s.mu.Lock()
	s.st.Keys[resource] = key
	s.mu.Unlock()
}

// Outbox returns the captured messages, oldest first.
func (s *Service) Outbox() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.st.Outbox)
}

// ClearOutbox forgets the captured messages (the console's "Clear inbox").
func (s *Service) ClearOutbox() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Outbox = nil
}

// target finds the resource a request is for, and the path that is left.
func target(r *http.Request) (name, path string, ok bool) {
	if rest, ok := strings.CutPrefix(r.URL.Path, "/_azure/communication/"); ok {
		name, path, _ = strings.Cut(rest, "/")
		return name, "/" + path, name != ""
	}
	label, base := core.SplitHost(r.Host, "")
	if label != "" && (base == "communication.localhost" || strings.HasSuffix(base, ".communication.localhost")) {
		return label, r.URL.Path, true
	}
	// "emails" is also a possible S3 bucket: the plain paths need an api-version and no AWS signature
	q, p := r.URL.Query(), r.URL.Path
	if a := r.Header.Get("Authorization"); strings.HasPrefix(a, "AWS") || q.Has("X-Amz-Algorithm") || !q.Has("api-version") {
		return "", "", false
	}
	if p == "/emails:send" || strings.HasPrefix(p, "/emails/operations/") {
		return "default", p, true
	}
	return "", "", false
}

func writeError(w http.ResponseWriter, c *core.Ctx, status int, code, msg string) {
	c.ErrCode = code
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	name, path, _ := target(r)
	w.Header().Set("x-ms-request-id", c.RequestID)
	switch {
	case path == "/emails:send" && r.Method == http.MethodPost:
		c.Op = "SendEmail"
		s.send(w, r, c, name)
	case strings.HasPrefix(path, "/emails/operations/") && r.Method == http.MethodGet:
		c.Op = "GetSendResult"
		s.operation(w, r, c, name, strings.TrimPrefix(path, "/emails/operations/"))
	default:
		writeError(w, c, http.StatusNotFound, "ResourceNotFound", "pogi serves POST /emails:send and GET /emails/operations/<id>.")
	}
}

type recipients struct {
	To  []Address `json:"to"`
	Cc  []Address `json:"cc"`
	Bcc []Address `json:"bcc"`
}

func (s *Service) send(w http.ResponseWriter, r *http.Request, c *core.Ctx, name string) {
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeError(w, c, e.Status, e.Code, e.Msg)
		return
	}
	body := core.Body(r)
	if err := s.checkHMAC(r, body, name); err != nil {
		writeError(w, c, http.StatusUnauthorized, "Unauthorized", err.Error())
		return
	}
	var in struct {
		Sender     string `json:"senderAddress"`
		Recipients recipients
		Content    struct{ Subject, PlainText, HTML string } `json:"content"`
		ReplyTo    []Address                                 `json:"replyTo"`
		Headers    map[string]string                         `json:"headers"`
		Attachs    []Attachment                              `json:"attachments"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		writeError(w, c, http.StatusBadRequest, "InvalidRequestBody", "The request body is not a valid EmailMessage: "+err.Error())
		return
	}
	if err := validate(in.Sender, in.Recipients, in.Content.Subject, in.Content.PlainText, in.Content.HTML, in.Attachs); err != nil {
		writeError(w, c, http.StatusBadRequest, "InvalidRequestBody", err.Error())
		return
	}
	m := Message{ID: core.UUID(), Resource: name, From: in.Sender, To: in.Recipients.To, Cc: in.Recipients.Cc, Bcc: in.Recipients.Bcc,
		ReplyTo: in.ReplyTo, Subject: in.Content.Subject, Text: in.Content.PlainText, HTML: in.Content.HTML, Attachments: in.Attachs,
		Headers: in.Headers, Time: time.Now().UTC()}
	s.mu.Lock()
	s.st.Outbox = append(s.st.Outbox, m)
	if n := len(s.st.Outbox) - maxOutbox; n > 0 {
		s.st.Outbox = s.st.Outbox[n:]
	}
	s.mu.Unlock()

	scheme, prefix := "http", ""
	if r.TLS != nil {
		scheme = "https"
	}
	if strings.HasPrefix(r.URL.Path, "/_azure/communication/") {
		prefix = "/_azure/communication/" + name
	}
	w.Header().Set("operation-location", scheme+"://"+r.Host+prefix+"/emails/operations/"+m.ID+"?api-version="+url.QueryEscape(r.URL.Query().Get("api-version")))
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{"id": m.ID, "status": "NotStarted", "error": nil})
}

func validate(sender string, rc recipients, subject, text, html string, atts []Attachment) error {
	if !strings.Contains(sender, "@") {
		return fmt.Errorf("senderAddress must be an email address.")
	}
	if len(rc.To)+len(rc.Cc)+len(rc.Bcc) == 0 {
		return fmt.Errorf("recipients needs at least one address in to, cc or bcc.")
	}
	for _, a := range slices.Concat(rc.To, rc.Cc, rc.Bcc) {
		if !strings.Contains(a.Address, "@") {
			return fmt.Errorf("%q is not a recipient email address.", a.Address)
		}
	}
	if subject == "" {
		return fmt.Errorf("content.subject is required.")
	}
	if text == "" && html == "" {
		return fmt.Errorf("content needs plainText or html.")
	}
	for _, a := range atts {
		if _, err := base64.StdEncoding.DecodeString(a.Content); err != nil || a.Name == "" || a.ContentType == "" {
			return fmt.Errorf("attachment %q needs a name, a contentType and contentInBase64.", a.Name)
		}
	}
	return nil
}

// operation reports a send as Succeeded: delivery is the capture itself.
func (s *Service) operation(w http.ResponseWriter, r *http.Request, c *core.Ctx, name, id string) {
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeError(w, c, e.Status, e.Code, e.Msg)
		return
	}
	if err := s.checkHMAC(r, core.Body(r), name); err != nil {
		writeError(w, c, http.StatusUnauthorized, "Unauthorized", err.Error())
		return
	}
	s.mu.Lock()
	found := slices.ContainsFunc(s.st.Outbox, func(m Message) bool { return m.ID == id })
	s.mu.Unlock()
	if !found {
		writeError(w, c, http.StatusNotFound, "ResourceNotFound", "The email operation "+id+" does not exist.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": id, "status": "Succeeded", "error": nil})
}

// checkHMAC verifies "Authorization: HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature=..."
// when signatures are checked and the resource has a key; otherwise anything goes.
func (s *Service) checkHMAC(r *http.Request, body []byte, resource string) error {
	s.mu.Lock()
	key, known := s.st.Keys[resource]
	s.mu.Unlock()
	if !s.hub.Cfg.ValidateSigs || !known {
		return nil
	}
	rest, ok := strings.CutPrefix(r.Header.Get("Authorization"), "HMAC-SHA256 ")
	if !ok {
		return fmt.Errorf("The request needs an HMAC-SHA256 Authorization header.")
	}
	params := map[string]string{}
	for _, kv := range strings.Split(rest, "&") {
		k, v, _ := strings.Cut(kv, "=")
		params[k] = v
	}
	sum := sha256.Sum256(body)
	if got := r.Header.Get("x-ms-content-sha256"); got != base64.StdEncoding.EncodeToString(sum[:]) {
		return fmt.Errorf("x-ms-content-sha256 is not the SHA-256 of the body.")
	}
	var values []string
	for _, h := range strings.Split(params["SignedHeaders"], ";") {
		if h == "host" {
			values = append(values, r.Host)
		} else {
			values = append(values, r.Header.Get(h))
		}
	}
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		raw = []byte(key) // not base64: use it as it is
	}
	m := hmac.New(sha256.New, raw)
	m.Write([]byte(r.Method + "\n" + r.URL.RequestURI() + "\n" + strings.Join(values, ";")))
	if !hmac.Equal([]byte(params["Signature"]), []byte(base64.StdEncoding.EncodeToString(m.Sum(nil)))) {
		return fmt.Errorf("The HMAC signature does not match.")
	}
	return nil
}

// ---- console ----

// Resources lists the captured messages, newest first.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Outbox", Columns: []string{"Id", "From", "To", "Subject", "Sent"}}
	for _, m := range slices.Backward(s.st.Outbox) {
		t.Rows = append(t.Rows, []string{m.ID, m.From, strings.Join(addresses(slices.Concat(m.To, m.Cc, m.Bcc)), ", "), m.Subject, m.Time.Format(time.RFC3339)})
	}
	return []core.Table{t}
}

// Inspect returns one message, with the size of each attachment instead of its content.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.st.Outbox {
		if table != "Outbox" || m.ID != id {
			continue
		}
		var files []map[string]any
		for _, a := range m.Attachments {
			raw, _ := base64.StdEncoding.DecodeString(a.Content)
			files = append(files, map[string]any{"name": a.Name, "contentType": a.ContentType, "size": len(raw)})
		}
		m.Attachments = nil
		return map[string]any{"message": m, "attachments": files}
	}
	return nil
}

func addresses(list []Address) []string {
	out := make([]string, len(list))
	for i, a := range list {
		out[i] = a.Address
	}
	return out
}
