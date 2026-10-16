// Package ses emulates Amazon SES v1 (Query) and SES v2 (REST-JSON under
// /v2/email/) in one service. Every sent message lands in an outbox, served
// LocalStack-style at /_aws/ses.
//
// Like LocalStack (and unlike the SES sandbox), pogi does not require verified
// identities: any sender and recipient is accepted, and identities verify at once.
package ses

import (
	"bytes"
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"mime"
	"net"
	"net/http"
	"net/mail"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const maxOutbox = 1000

// Service is the SES emulator.
type Service struct {
	hub       *core.Hub
	rpc       *core.RPC
	mux       *http.ServeMux
	mu        sync.Mutex
	st        state
	listeners []net.Listener // SMTP
}

type state struct {
	Identities    map[string]*Identity  `json:"identities"`              // account/region/identity
	Templates     map[string]*Template  `json:"templates"`               // account/region/name
	ConfigSets    map[string]time.Time  `json:"configSets"`              // account/region/name → created
	ConfigDetails map[string]*ConfigSet `json:"configDetails,omitempty"` // account/region/name → destinations and options
	Outbox        []Email               `json:"outbox"`                  // the last maxOutbox messages
}

// Identity is an email address or domain; pogi verifies it at once.
type Identity struct {
	Name       string            `json:"name"`
	Domain     bool              `json:"domain"`
	Token      string            `json:"token,omitempty"` // domain verification TXT record
	DkimTokens []string          `json:"dkimTokens,omitempty"`
	Topics     map[string]string `json:"topics,omitempty"` // Bounce|Complaint|Delivery → SNS topic
	Tags       map[string]string `json:"tags,omitempty"`
	Created    time.Time         `json:"created"`
}

// Template is an email template, shared by SES v1 and v2.
type Template struct {
	Name    string    `json:"name"`
	Subject string    `json:"subject,omitempty"`
	Text    string    `json:"text,omitempty"`
	Html    string    `json:"html,omitempty"`
	Created time.Time `json:"created"`
}

// Email is one sent message, in the shape of LocalStack's /_aws/ses.
type Email struct {
	Id           string
	Region       string
	Account      string `json:",omitempty"`
	Source       string
	Destination  *Destination `json:",omitempty"`
	Subject      string       `json:",omitempty"`
	Body         *Body        `json:",omitempty"`
	RawData      string       `json:",omitempty"`
	Template     string       `json:",omitempty"`
	TemplateData string       `json:",omitempty"`
	Timestamp    time.Time
}

// Destination holds the recipients of a message.
type Destination struct {
	ToAddresses  []string `json:",omitempty"`
	CcAddresses  []string `json:",omitempty"`
	BccAddresses []string `json:",omitempty"`
}

// Body holds the text and HTML parts of a message.
type Body struct {
	TextPart string `json:"text_part"`
	HtmlPart string `json:"html_part"`
}

// New creates the SES service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	s.rpc = &core.RPC{XMLNS: "http://ses.amazonaws.com/doc/2010-12-01/", Ops: map[string]core.Handler{
		"SendEmail":                              core.H(s.sendEmail),
		"SendRawEmail":                           core.H(s.sendRawEmail),
		"SendTemplatedEmail":                     core.H(s.sendTemplatedEmail),
		"SendBulkTemplatedEmail":                 core.H(s.sendBulkTemplatedEmail),
		"VerifyEmailIdentity":                    core.H(s.verifyEmail),
		"VerifyEmailAddress":                     core.H(s.verifyEmail),
		"VerifyDomainIdentity":                   core.H(s.verifyDomain),
		"VerifyDomainDkim":                       core.H(s.verifyDomainDkim),
		"ListIdentities":                         core.H(s.listIdentities),
		"ListVerifiedEmailAddresses":             core.H(s.listVerifiedEmailAddresses),
		"GetIdentityVerificationAttributes":      core.H(s.getVerificationAttributes),
		"GetIdentityDkimAttributes":              core.H(s.getDkimAttributes),
		"DeleteIdentity":                         core.H(s.deleteIdentity),
		"DeleteVerifiedEmailAddress":             core.H(s.deleteIdentity),
		"GetSendQuota":                           core.H(s.getSendQuota),
		"GetSendStatistics":                      core.H(s.getSendStatistics),
		"GetAccountSendingEnabled":               core.H(s.getAccountSendingEnabled),
		"CreateTemplate":                         core.H(s.createTemplate),
		"UpdateTemplate":                         core.H(s.updateTemplate),
		"GetTemplate":                            core.H(s.getTemplate),
		"DeleteTemplate":                         core.H(s.deleteTemplate),
		"ListTemplates":                          core.H(s.listTemplates),
		"CreateConfigurationSet":                 core.H(s.createConfigurationSet),
		"DeleteConfigurationSet":                 core.H(s.deleteConfigurationSet),
		"DescribeConfigurationSet":               core.H(s.describeConfigurationSet),
		"ListConfigurationSets":                  core.H(s.listConfigurationSets),
		"CreateConfigurationSetEventDestination": core.H(s.createEventDestination),
		"UpdateConfigurationSetEventDestination": core.H(s.updateEventDestination),
		"DeleteConfigurationSetEventDestination": core.H(s.deleteEventDestination),
		"PutConfigurationSetDeliveryOptions":     core.H(s.putDeliveryOptionsV1),
		"SetIdentityNotificationTopic":           core.H(s.setNotificationTopic),
		"GetIdentityNotificationAttributes":      core.H(s.getNotificationAttributes),
	}}
	s.routesV2()
	if hub != nil {
		hub.Mount("/_aws/ses", http.HandlerFunc(s.outboxHandler))
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "ses", Cloud: core.AWS, Title: "SES", Signing: []string{"ses", "email"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v2/email/") {
		s.mux.ServeHTTP(w, r)
		return
	}
	if r.URL.Path != "/" { // v1 is only POST /; any other path is a v2 (rest-json) call pogi does not serve
		core.WriteJSONError(w, core.C(r), "1.0", core.Errorf(404, "UnknownOperationException", "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Identities: map[string]*Identity{}, Templates: map[string]*Template{}, ConfigSets: map[string]time.Time{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Identities: map[string]*Identity{}, Templates: map[string]*Template{}, ConfigSets: map[string]time.Time{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func scope(c *core.Ctx) string { return c.Account + "/" + c.Region + "/" }

// empty is the output of operations that return nothing ("{}" in REST-JSON).
var empty = struct{}{}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValue", format, a...)
}

// ---- sending (shared by v1 and v2) ----

// send stores a message in the outbox, relays it when Config.SMTPRelay is set
// and publishes a Delivery notification when the sender's identity has a
// topic for it.
func (s *Service) send(c *core.Ctx, e Email, configSet string) (string, error) {
	if err := e.refuseLineBreaks(); err != nil {
		return "", err
	}
	e.Id = core.RandHex(8) + "-" + core.UUID() + "-000000"
	e.Region, e.Account, e.Timestamp = c.Region, c.Account, time.Now().UTC()
	if e.Destination == nil {
		e.Destination = &Destination{}
	}
	s.mu.Lock()
	s.st.Outbox = append(s.st.Outbox, e)
	if n := len(s.st.Outbox) - maxOutbox; n > 0 {
		s.st.Outbox = s.st.Outbox[n:]
	}
	topic := s.topic(c, e.Source, "Delivery")
	s.mu.Unlock()
	if s.hub != nil && s.hub.Cfg.SMTPRelay != "" {
		core.Go("ses.relay", func() { s.relay(e) })
	}
	if configSet != "" {
		s.publish(c, e, configSet)
	}
	if topic != "" && s.hub != nil {
		to := slices.Concat(e.Destination.ToAddresses, e.Destination.CcAddresses, e.Destination.BccAddresses)
		ts := e.Timestamp.Format(core.ISO)
		b, _ := json.Marshal(map[string]any{"notificationType": "Delivery",
			"mail": map[string]any{"timestamp": ts, "source": e.Source, "sendingAccountId": c.Account, "messageId": e.Id, "destination": to},
			"delivery": map[string]any{"timestamp": ts, "processingTimeMillis": 0, "recipients": to, "smtpResponse": "250 2.0.0 OK",
				"reportingMTA": "pogi"}})
		if err := s.hub.Deliver(topic, &core.Message{Body: string(b)}); err != nil {
			s.hub.Log.Warn("ses notification failed", "topic", topic, "err", err)
		}
	}
	return e.Id, nil
}

// refuseLineBreaks stops header injection: a CR or LF in the sender, a recipient
// or the subject could add headers (a hidden Bcc) when the message is relayed.
func (e Email) refuseLineBreaks() error {
	d := cmp.Or(e.Destination, &Destination{})
	fields := slices.Concat([]string{e.Source, e.Subject}, d.ToAddresses, d.CcAddresses, d.BccAddresses)
	if slices.ContainsFunc(fields, func(f string) bool { return strings.ContainsAny(f, "\r\n") }) {
		return invalid("Source, recipients and Subject must not contain line breaks.")
	}
	return nil
}

// topic returns the notification topic of the sender's address or domain; lock held.
func (s *Service) topic(c *core.Ctx, source, kind string) string {
	addr := source
	if a, err := mail.ParseAddress(source); err == nil {
		addr = a.Address
	}
	for _, id := range []string{addr, addr[strings.LastIndex(addr, "@")+1:]} {
		if i := s.st.Identities[scope(c)+id]; i != nil && i.Topics[kind] != "" {
			return i.Topics[kind]
		}
	}
	return ""
}

// parseRaw fills subject, sender and recipients from a MIME message.
func parseRaw(data []byte, e *Email) bool {
	m, err := mail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		return false
	}
	e.RawData, e.Subject = string(data), m.Header.Get("Subject")
	if dec, err := new(mime.WordDecoder).DecodeHeader(e.Subject); err == nil {
		e.Subject = dec
	}
	if e.Source == "" {
		e.Source = m.Header.Get("From")
	}
	if e.Destination == nil {
		e.Destination = &Destination{}
		for h, list := range map[string]*[]string{"To": &e.Destination.ToAddresses, "Cc": &e.Destination.CcAddresses, "Bcc": &e.Destination.BccAddresses} {
			addrs, _ := m.Header.AddressList(h)
			for _, a := range addrs {
				*list = append(*list, a.Address)
			}
		}
	}
	return e.Source != ""
}

var placeholder = regexp.MustCompile(`\{\{\s*([^{}\s]+)\s*\}\}`)

// render substitutes {{name}} and {{a.b}} placeholders.
// ponytail: plain substitution, no Handlebars blocks ({{#if}}, {{#each}}).
func render(text string, vars map[string]any) string {
	return placeholder.ReplaceAllStringFunc(text, func(m string) string {
		var v any = vars
		for _, k := range strings.Split(placeholder.FindStringSubmatch(m)[1], ".") {
			obj, _ := v.(map[string]any)
			v = obj[k]
		}
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	})
}

// fill renders a template into e; false when the data is not a JSON object.
func fill(e *Email, t Template, data string) bool {
	var vars map[string]any
	if data != "" && json.Unmarshal([]byte(data), &vars) != nil {
		return false
	}
	e.Template, e.TemplateData = t.Name, data
	e.Subject, e.Body = render(t.Subject, vars), &Body{render(t.Text, vars), render(t.Html, vars)}
	return true
}

func (s *Service) template(c *core.Ctx, name string) (Template, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.st.Templates[scope(c)+name]; t != nil {
		return *t, true
	}
	return Template{}, false
}

func (s *Service) hasConfigSet(c *core.Ctx, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.st.ConfigSets[scope(c)+name]
	return ok || name == ""
}

func (s *Service) sentLast24h(c *core.Ctx) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.st.Outbox {
		if e.Account == c.Account && e.Region == c.Region && time.Since(e.Timestamp) < 24*time.Hour {
			n++
		}
	}
	return float64(n)
}

// ---- SES v1 (Query) ----

type content struct{ Data, Charset string }

type message struct {
	Subject content
	Body    struct{ Text, Html *content }
}

func (m message) body() *Body {
	b := &Body{}
	if m.Body.Text != nil {
		b.TextPart = m.Body.Text.Data
	}
	if m.Body.Html != nil {
		b.HtmlPart = m.Body.Html.Data
	}
	return b
}

func noConfigSet(name string) error {
	return core.Errorf(400, "ConfigurationSetDoesNotExist", "Configuration set <%s> does not exist.", name)
}

func noTemplate(name string) error {
	return core.Errorf(400, "TemplateDoesNotExist", "Template %s does not exist.", name)
}

func (s *Service) sendEmail(c *core.Ctx, in *struct {
	Source               string
	Destination          Destination
	Message              message
	ReplyToAddresses     []string
	ReturnPath           string
	ConfigurationSetName string
}) (any, error) {
	if !s.hasConfigSet(c, in.ConfigurationSetName) {
		return nil, noConfigSet(in.ConfigurationSetName)
	}
	id, err := s.send(c, Email{Source: in.Source, Destination: &in.Destination, Subject: in.Message.Subject.Data, Body: in.Message.body()}, in.ConfigurationSetName)
	if err != nil {
		return nil, err
	}
	return map[string]string{"MessageId": id}, nil
}

func (s *Service) sendRawEmail(c *core.Ctx, in *struct {
	Source               string
	Destinations         []string
	RawMessage           struct{ Data []byte }
	ConfigurationSetName string
}) (any, error) {
	if !s.hasConfigSet(c, in.ConfigurationSetName) {
		return nil, noConfigSet(in.ConfigurationSetName)
	}
	e := Email{Source: in.Source}
	if len(in.Destinations) > 0 {
		e.Destination = &Destination{ToAddresses: in.Destinations}
	}
	if !parseRaw(in.RawMessage.Data, &e) {
		return nil, invalid("Missing required header 'From'.")
	}
	id, err := s.send(c, e, in.ConfigurationSetName)
	if err != nil {
		return nil, err
	}
	return map[string]string{"MessageId": id}, nil
}

func (s *Service) sendTemplatedEmail(c *core.Ctx, in *struct {
	Source               string
	Destination          Destination
	Template             string
	TemplateData         string
	ConfigurationSetName string
}) (any, error) {
	if !s.hasConfigSet(c, in.ConfigurationSetName) {
		return nil, noConfigSet(in.ConfigurationSetName)
	}
	t, ok := s.template(c, in.Template)
	if !ok {
		return nil, noTemplate(in.Template)
	}
	e := Email{Source: in.Source, Destination: &in.Destination}
	if !fill(&e, t, in.TemplateData) {
		return nil, invalid("TemplateData must be a JSON object.")
	}
	id, err := s.send(c, e, in.ConfigurationSetName)
	if err != nil {
		return nil, err
	}
	return map[string]string{"MessageId": id}, nil
}

type bulkStatus struct {
	Status    string
	Error     string `json:",omitempty"`
	MessageId string `json:",omitempty"`
}

func (s *Service) sendBulkTemplatedEmail(c *core.Ctx, in *struct {
	Source               string
	Template             string
	DefaultTemplateData  string
	ConfigurationSetName string
	Destinations         []struct {
		Destination             Destination
		ReplacementTemplateData string
	}
}) (any, error) {
	if !s.hasConfigSet(c, in.ConfigurationSetName) {
		return nil, noConfigSet(in.ConfigurationSetName)
	}
	t, ok := s.template(c, in.Template)
	if !ok {
		return nil, noTemplate(in.Template)
	}
	out := &struct{ Status []bulkStatus }{}
	for _, d := range in.Destinations {
		vars := map[string]any{}
		json.Unmarshal([]byte(in.DefaultTemplateData), &vars)
		repl := map[string]any{}
		json.Unmarshal([]byte(d.ReplacementTemplateData), &repl)
		maps.Copy(vars, repl)
		data, _ := json.Marshal(vars)
		e := Email{Source: in.Source, Destination: &d.Destination}
		if !fill(&e, t, string(data)) {
			out.Status = append(out.Status, bulkStatus{Status: "InvalidParameterValue", Error: "invalid template data"})
			continue
		}
		id, err := s.send(c, e, in.ConfigurationSetName)
		if err != nil {
			out.Status = append(out.Status, bulkStatus{Status: "InvalidParameterValue", Error: err.Error()})
			continue
		}
		out.Status = append(out.Status, bulkStatus{Status: "Success", MessageId: id})
	}
	return out, nil
}

// verify registers (or returns) an identity; lock held.
func (s *Service) verify(c *core.Ctx, name string) *Identity {
	k := scope(c) + name
	if i := s.st.Identities[k]; i != nil {
		return i
	}
	i := &Identity{Name: name, Domain: !strings.Contains(name, "@"), Created: time.Now().UTC()}
	if i.Domain {
		b := make([]byte, 32)
		rand.Read(b)
		i.Token = base64.StdEncoding.EncodeToString(b)
	}
	s.st.Identities[k] = i
	return i
}

func dkimTokens() []string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	return []string{core.RandString(32, alphabet), core.RandString(32, alphabet), core.RandString(32, alphabet)}
}

func (s *Service) verifyEmail(c *core.Ctx, in *struct{ EmailAddress string }) (any, error) {
	if !strings.Contains(in.EmailAddress, "@") {
		return nil, invalid("Invalid email address <%s>.", in.EmailAddress)
	}
	s.mu.Lock()
	s.verify(c, in.EmailAddress)
	s.mu.Unlock()
	return empty, nil
}

func (s *Service) verifyDomain(c *core.Ctx, in *struct{ Domain string }) (any, error) {
	if in.Domain == "" || strings.Contains(in.Domain, "@") {
		return nil, invalid("Invalid domain name <%s>.", in.Domain)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]string{"VerificationToken": s.verify(c, in.Domain).Token}, nil
}

func (s *Service) verifyDomainDkim(c *core.Ctx, in *struct{ Domain string }) (any, error) {
	if in.Domain == "" || strings.Contains(in.Domain, "@") {
		return nil, invalid("Invalid domain name <%s>.", in.Domain)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.verify(c, in.Domain)
	if i.DkimTokens == nil {
		i.DkimTokens = dkimTokens()
	}
	return map[string][]string{"DkimTokens": i.DkimTokens}, nil
}

// identities returns the caller's identities, sorted; lock held.
func (s *Service) identities(c *core.Ctx) []*Identity {
	var out []*Identity
	for _, k := range core.SortedKeys(s.st.Identities) {
		if strings.HasPrefix(k, scope(c)) {
			out = append(out, s.st.Identities[k])
		}
	}
	return out
}

func (s *Service) listIdentities(c *core.Ctx, in *struct {
	IdentityType string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &struct{ Identities []string }{[]string{}}
	for _, i := range s.identities(c) {
		if in.IdentityType == "" || in.IdentityType == "Domain" == i.Domain {
			out.Identities = append(out.Identities, i.Name)
		}
	}
	return out, nil
}

func (s *Service) listVerifiedEmailAddresses(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &struct{ VerifiedEmailAddresses []string }{[]string{}}
	for _, i := range s.identities(c) {
		if !i.Domain {
			out.VerifiedEmailAddresses = append(out.VerifiedEmailAddresses, i.Name)
		}
	}
	return out, nil
}

type verification struct {
	VerificationStatus string
	VerificationToken  string `json:",omitempty"`
}

// getVerificationAttributes omits unknown identities, as SES does.
func (s *Service) getVerificationAttributes(c *core.Ctx, in *struct{ Identities []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]verification{}
	for _, n := range in.Identities {
		if i := s.st.Identities[scope(c)+n]; i != nil {
			out[n] = verification{"Success", i.Token}
		}
	}
	return map[string]any{"VerificationAttributes": out}, nil
}

type dkimAttributes struct {
	DkimEnabled            bool
	DkimVerificationStatus string
	DkimTokens             []string `json:",omitempty"`
}

func (s *Service) getDkimAttributes(c *core.Ctx, in *struct{ Identities []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]dkimAttributes{}
	for _, n := range in.Identities {
		if i := s.st.Identities[scope(c)+n]; i != nil {
			a := dkimAttributes{DkimVerificationStatus: "NotStarted"}
			if i.DkimTokens != nil {
				a = dkimAttributes{true, "Success", i.DkimTokens}
			}
			out[n] = a
		}
	}
	return map[string]any{"DkimAttributes": out}, nil
}

func (s *Service) deleteIdentity(c *core.Ctx, in *struct{ Identity, EmailAddress string }) (any, error) {
	s.mu.Lock()
	delete(s.st.Identities, scope(c)+in.Identity+in.EmailAddress)
	s.mu.Unlock()
	return empty, nil
}

func (s *Service) getSendQuota(c *core.Ctx, _ *struct{}) (any, error) {
	return map[string]float64{"Max24HourSend": 200, "MaxSendRate": 1, "SentLast24Hours": s.sentLast24h(c)}, nil
}

type sendDataPoint struct {
	Timestamp                                      core.Time
	DeliveryAttempts, Bounces, Complaints, Rejects int
}

// getSendStatistics counts sent messages in 15-minute buckets over two weeks.
func (s *Service) getSendStatistics(c *core.Ctx, _ *struct{}) (any, error) {
	counts := map[int64]int{}
	s.mu.Lock()
	for _, e := range s.st.Outbox {
		if e.Account == c.Account && e.Region == c.Region && time.Since(e.Timestamp) < 14*24*time.Hour {
			counts[e.Timestamp.Unix()/900*900]++
		}
	}
	s.mu.Unlock()
	out := &struct{ SendDataPoints []sendDataPoint }{[]sendDataPoint{}}
	for _, t := range slices.Sorted(maps.Keys(counts)) {
		out.SendDataPoints = append(out.SendDataPoints, sendDataPoint{Timestamp: core.Time(time.Unix(t, 0).UTC()), DeliveryAttempts: counts[t]})
	}
	return out, nil
}

func (s *Service) getAccountSendingEnabled(c *core.Ctx, _ *struct{}) (any, error) {
	return map[string]bool{"Enabled": true}, nil
}

type templateV1 struct {
	TemplateName string
	SubjectPart  string `json:",omitempty"`
	TextPart     string `json:",omitempty"`
	HtmlPart     string `json:",omitempty"`
}

func (s *Service) createTemplate(c *core.Ctx, in *struct{ Template templateV1 }) (any, error) {
	t := in.Template
	if t.TemplateName == "" {
		return nil, core.Errorf(400, "InvalidTemplate", "Template name is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := scope(c) + t.TemplateName
	if s.st.Templates[k] != nil {
		return nil, core.Errorf(400, "AlreadyExists", "Template %s already exists for account id %s", t.TemplateName, c.Account)
	}
	s.st.Templates[k] = &Template{t.TemplateName, t.SubjectPart, t.TextPart, t.HtmlPart, time.Now().UTC()}
	return empty, nil
}

func (s *Service) updateTemplate(c *core.Ctx, in *struct{ Template templateV1 }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.st.Templates[scope(c)+in.Template.TemplateName]
	if t == nil {
		return nil, noTemplate(in.Template.TemplateName)
	}
	t.Subject, t.Text, t.Html = in.Template.SubjectPart, in.Template.TextPart, in.Template.HtmlPart
	return empty, nil
}

func (s *Service) getTemplate(c *core.Ctx, in *struct{ TemplateName string }) (any, error) {
	t, ok := s.template(c, in.TemplateName)
	if !ok {
		return nil, noTemplate(in.TemplateName)
	}
	return map[string]templateV1{"Template": {t.Name, t.Subject, t.Text, t.Html}}, nil
}

func (s *Service) deleteTemplate(c *core.Ctx, in *struct{ TemplateName string }) (any, error) {
	s.mu.Lock()
	delete(s.st.Templates, scope(c)+in.TemplateName)
	s.mu.Unlock()
	return empty, nil
}

type templateMeta struct {
	Name             string
	CreatedTimestamp core.Time
}

func (s *Service) listTemplates(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &struct{ TemplatesMetadata []templateMeta }{[]templateMeta{}}
	for _, k := range core.SortedKeys(s.st.Templates) {
		if t := s.st.Templates[k]; strings.HasPrefix(k, scope(c)) {
			out.TemplatesMetadata = append(out.TemplatesMetadata, templateMeta{t.Name, core.Time(t.Created)})
		}
	}
	return out, nil
}

type configSet struct{ Name string }

func (s *Service) createConfigurationSet(c *core.Ctx, in *struct{ ConfigurationSet configSet }) (any, error) {
	name := in.ConfigurationSet.Name
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+name]; ok {
		return nil, core.Errorf(400, "ConfigurationSetAlreadyExists", "Configuration set <%s> already exists.", name)
	}
	s.st.ConfigSets[scope(c)+name] = time.Now().UTC()
	return empty, nil
}

func (s *Service) deleteConfigurationSet(c *core.Ctx, in *struct{ ConfigurationSetName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+in.ConfigurationSetName]; !ok {
		return nil, noConfigSet(in.ConfigurationSetName)
	}
	delete(s.st.ConfigSets, scope(c)+in.ConfigurationSetName)
	delete(s.st.ConfigDetails, scope(c)+in.ConfigurationSetName)
	return empty, nil
}

func (s *Service) listConfigurationSets(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &struct{ ConfigurationSets []configSet }{[]configSet{}}
	for _, k := range core.SortedKeys(s.st.ConfigSets) {
		if strings.HasPrefix(k, scope(c)) {
			out.ConfigurationSets = append(out.ConfigurationSets, configSet{strings.TrimPrefix(k, scope(c))})
		}
	}
	return out, nil
}

func (s *Service) setNotificationTopic(c *core.Ctx, in *struct{ Identity, NotificationType, SnsTopic string }) (any, error) {
	if !slices.Contains([]string{"Bounce", "Complaint", "Delivery"}, in.NotificationType) {
		return nil, invalid("Invalid notification type <%s>.", in.NotificationType)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.st.Identities[scope(c)+in.Identity]
	if i == nil {
		return nil, invalid("Identity <%s> is invalid. Must be a verified email address or domain.", in.Identity)
	}
	if i.Topics == nil {
		i.Topics = map[string]string{}
	}
	i.Topics[in.NotificationType] = in.SnsTopic
	if in.SnsTopic == "" {
		delete(i.Topics, in.NotificationType)
	}
	return empty, nil
}

type notificationAttributes struct {
	BounceTopic, ComplaintTopic, DeliveryTopic                                                                         string `json:",omitempty"`
	ForwardingEnabled                                                                                                  bool
	HeadersInBounceNotificationsEnabled, HeadersInComplaintNotificationsEnabled, HeadersInDeliveryNotificationsEnabled bool
}

func (s *Service) getNotificationAttributes(c *core.Ctx, in *struct{ Identities []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]notificationAttributes{}
	for _, n := range in.Identities {
		if i := s.st.Identities[scope(c)+n]; i != nil {
			out[n] = notificationAttributes{BounceTopic: i.Topics["Bounce"], ComplaintTopic: i.Topics["Complaint"],
				DeliveryTopic: i.Topics["Delivery"], ForwardingEnabled: true}
		}
	}
	return map[string]any{"NotificationAttributes": out}, nil
}

// ---- SES v2 (REST-JSON) ----

func (s *Service) routesV2() {
	route := func(pattern, op string, h func(*core.Ctx, *http.Request) (any, error)) {
		s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			c := core.C(r)
			c.Op = op
			out, err := any(nil), c.Intercept()
			if err == nil {
				out, err = h(c, r)
			}
			if err != nil {
				core.RESTJSONError(w, c, err)
				return
			}
			core.RESTJSON(w, c, http.StatusOK, out)
		})
	}
	route("POST /v2/email/outbound-emails", "SendEmail", s.sendEmailV2)
	route("POST /v2/email/identities", "CreateEmailIdentity", s.createIdentityV2)
	route("GET /v2/email/identities", "ListEmailIdentities", s.listIdentitiesV2)
	route("POST /v2/email/list-identities", "ListEmailIdentities", s.listIdentitiesV2) // newer SDKs
	route("GET /v2/email/identities/{id}", "GetEmailIdentity", s.getIdentityV2)
	route("DELETE /v2/email/identities/{id}", "DeleteEmailIdentity", s.deleteIdentityV2)
	route("POST /v2/email/templates", "CreateEmailTemplate", s.putTemplateV2(true))
	route("PUT /v2/email/templates/{name}", "UpdateEmailTemplate", s.putTemplateV2(false))
	route("GET /v2/email/templates", "ListEmailTemplates", s.listTemplatesV2)
	route("GET /v2/email/templates/{name}", "GetEmailTemplate", s.getTemplateV2)
	route("DELETE /v2/email/templates/{name}", "DeleteEmailTemplate", s.deleteTemplateV2)
	route("GET /v2/email/account", "GetAccount", s.getAccountV2)
	route("POST /v2/email/configuration-sets", "CreateConfigurationSet", s.createConfigurationSetV2)
	route("GET /v2/email/configuration-sets", "ListConfigurationSets", s.listConfigurationSetsV2)
	route("GET /v2/email/configuration-sets/{name}", "GetConfigurationSet", s.getConfigurationSetV2)
	route("DELETE /v2/email/configuration-sets/{name}", "DeleteConfigurationSet", s.deleteConfigurationSetV2)
	for path, key := range options {
		route("PUT /v2/email/configuration-sets/{name}/"+path, "PutConfigurationSet"+key, s.putOptionV2(key))
	}
	route("POST /v2/email/configuration-sets/{name}/event-destinations", "CreateConfigurationSetEventDestination", s.createEventDestinationV2)
	route("GET /v2/email/configuration-sets/{name}/event-destinations", "GetConfigurationSetEventDestinations", s.getEventDestinationsV2)
	route("PUT /v2/email/configuration-sets/{name}/event-destinations/{dest}", "UpdateConfigurationSetEventDestination", s.updateEventDestinationV2)
	route("DELETE /v2/email/configuration-sets/{name}/event-destinations/{dest}", "DeleteConfigurationSetEventDestination", s.deleteEventDestinationV2)
	route("/", "Unknown", func(c *core.Ctx, r *http.Request) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement SES v2 %s %s", r.Method, r.URL.Path)
	})
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, v) != nil {
		return core.Errorf(400, "BadRequestException", "Could not parse the request body as JSON.")
	}
	return nil
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "NotFoundException", format, a...)
}

func badRequest(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

type templateContent struct {
	Subject string `json:",omitempty"`
	Text    string `json:",omitempty"`
	Html    string `json:",omitempty"`
}

func (s *Service) sendEmailV2(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		FromEmailAddress     string
		Destination          *Destination
		ConfigurationSetName string
		Content              struct {
			Simple   *message
			Raw      *struct{ Data []byte }
			Template *struct {
				TemplateName    string
				TemplateData    string
				TemplateContent *templateContent
			}
		}
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if !s.hasConfigSet(c, in.ConfigurationSetName) {
		return nil, notFound("Configuration set <%s> does not exist.", in.ConfigurationSetName)
	}
	e := Email{Source: in.FromEmailAddress, Destination: in.Destination}
	switch ct := in.Content; {
	case ct.Raw != nil:
		if !parseRaw(ct.Raw.Data, &e) {
			return nil, badRequest("Missing required header 'From'.")
		}
	case in.FromEmailAddress == "":
		return nil, badRequest("FromEmailAddress is required.")
	case ct.Simple != nil:
		e.Subject, e.Body = ct.Simple.Subject.Data, ct.Simple.body()
	case ct.Template != nil:
		t, ok := s.template(c, ct.Template.TemplateName)
		if tc := ct.Template.TemplateContent; tc != nil {
			t, ok = Template{Subject: tc.Subject, Text: tc.Text, Html: tc.Html}, true
		}
		if !ok {
			return nil, notFound("Template %s does not exist.", ct.Template.TemplateName)
		}
		if !fill(&e, t, ct.Template.TemplateData) {
			return nil, badRequest("TemplateData must be a JSON object.")
		}
	default:
		return nil, badRequest("Content must contain Simple, Raw or Template.")
	}
	id, err := s.send(c, e, in.ConfigurationSetName)
	if err != nil {
		return nil, err
	}
	return map[string]string{"MessageId": id}, nil
}

type dkimV2 struct {
	SigningEnabled          bool
	Status                  string
	Tokens                  []string `json:",omitempty"`
	SigningAttributesOrigin string
}

func identityType(i *Identity) string {
	if i.Domain {
		return "DOMAIN"
	}
	return "EMAIL_ADDRESS"
}

func dkim(i *Identity) dkimV2 {
	if i.DkimTokens == nil {
		return dkimV2{Status: "NOT_STARTED", SigningAttributesOrigin: "AWS_SES"}
	}
	return dkimV2{true, "SUCCESS", i.DkimTokens, "AWS_SES"}
}

type tagV2 struct{ Key, Value string }

func (s *Service) createIdentityV2(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		EmailIdentity string
		Tags          []tagV2
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if in.EmailIdentity == "" {
		return nil, badRequest("EmailIdentity is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Identities[scope(c)+in.EmailIdentity] != nil {
		return nil, core.Errorf(400, "AlreadyExistsException", "Email identity <%s> already exist.", in.EmailIdentity)
	}
	i := s.verify(c, in.EmailIdentity)
	if i.Domain {
		i.DkimTokens = dkimTokens() // Easy DKIM
	}
	for _, t := range in.Tags {
		if i.Tags == nil {
			i.Tags = map[string]string{}
		}
		i.Tags[t.Key] = t.Value
	}
	return map[string]any{"IdentityType": identityType(i), "VerifiedForSendingStatus": true, "DkimAttributes": dkim(i)}, nil
}

type identitySummary struct {
	IdentityType       string
	IdentityName       string
	SendingEnabled     bool
	VerificationStatus string
}

func (s *Service) listIdentitiesV2(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &struct{ EmailIdentities []identitySummary }{[]identitySummary{}}
	for _, i := range s.identities(c) {
		out.EmailIdentities = append(out.EmailIdentities, identitySummary{identityType(i), i.Name, true, "SUCCESS"})
	}
	return out, nil
}

func (s *Service) getIdentityV2(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.st.Identities[scope(c)+r.PathValue("id")]
	if i == nil {
		return nil, notFound("Email identity <%s> does not exist.", r.PathValue("id"))
	}
	tags := []tagV2{}
	for _, k := range core.SortedKeys(i.Tags) {
		tags = append(tags, tagV2{k, i.Tags[k]})
	}
	return map[string]any{"IdentityType": identityType(i), "FeedbackForwardingStatus": true, "VerifiedForSendingStatus": true,
		"VerificationStatus": "SUCCESS", "DkimAttributes": dkim(i), "Policies": map[string]string{}, "Tags": tags}, nil
}

func (s *Service) deleteIdentityV2(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := scope(c) + r.PathValue("id")
	if s.st.Identities[k] == nil {
		return nil, notFound("Email identity <%s> does not exist.", r.PathValue("id"))
	}
	delete(s.st.Identities, k)
	return empty, nil
}

func (s *Service) putTemplateV2(create bool) func(*core.Ctx, *http.Request) (any, error) {
	return func(c *core.Ctx, r *http.Request) (any, error) {
		var in struct {
			TemplateName    string
			TemplateContent templateContent
		}
		if err := decode(r, &in); err != nil {
			return nil, err
		}
		if !create {
			in.TemplateName = r.PathValue("name")
		}
		if in.TemplateName == "" {
			return nil, badRequest("TemplateName is required.")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		k, tc := scope(c)+in.TemplateName, in.TemplateContent
		switch t := s.st.Templates[k]; {
		case create && t != nil:
			return nil, core.Errorf(400, "AlreadyExistsException", "Template %s already exists.", in.TemplateName)
		case !create && t == nil:
			return nil, notFound("Template %s does not exist.", in.TemplateName)
		case create:
			s.st.Templates[k] = &Template{in.TemplateName, tc.Subject, tc.Text, tc.Html, time.Now().UTC()}
		default:
			t.Subject, t.Text, t.Html = tc.Subject, tc.Text, tc.Html
		}
		return empty, nil
	}
}

type templateMetaV2 struct {
	TemplateName     string
	CreatedTimestamp core.Time
}

func (s *Service) listTemplatesV2(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &struct{ TemplatesMetadata []templateMetaV2 }{[]templateMetaV2{}}
	for _, k := range core.SortedKeys(s.st.Templates) {
		if t := s.st.Templates[k]; strings.HasPrefix(k, scope(c)) {
			out.TemplatesMetadata = append(out.TemplatesMetadata, templateMetaV2{t.Name, core.Time(t.Created)})
		}
	}
	return out, nil
}

func (s *Service) getTemplateV2(c *core.Ctx, r *http.Request) (any, error) {
	t, ok := s.template(c, r.PathValue("name"))
	if !ok {
		return nil, notFound("Template %s does not exist.", r.PathValue("name"))
	}
	return map[string]any{"TemplateName": t.Name, "TemplateContent": templateContent{t.Subject, t.Text, t.Html}}, nil
}

func (s *Service) deleteTemplateV2(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := scope(c) + r.PathValue("name")
	if s.st.Templates[k] == nil {
		return nil, notFound("Template %s does not exist.", r.PathValue("name"))
	}
	delete(s.st.Templates, k)
	return empty, nil
}

func (s *Service) getAccountV2(c *core.Ctx, r *http.Request) (any, error) {
	return map[string]any{"SendingEnabled": true, "ProductionAccessEnabled": true, "EnforcementStatus": "HEALTHY",
		"DedicatedIpAutoWarmupEnabled": false,
		"SendQuota":                    map[string]float64{"Max24HourSend": 200, "MaxSendRate": 1, "SentLast24Hours": s.sentLast24h(c)}}, nil
}

// ---- outbox, console ----

// Outbox returns the sent messages, oldest first.
func (s *Service) Outbox() []Email {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.st.Outbox)
}

// ClearOutbox forgets the sent messages (the console's "Clear inbox").
func (s *Service) ClearOutbox() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Outbox = nil
}

// outboxHandler serves LocalStack's GET/DELETE /_aws/ses[?id=…][&email=…].
func (s *Service) outboxHandler(w http.ResponseWriter, r *http.Request) {
	id, email := r.URL.Query().Get("id"), r.URL.Query().Get("email")
	switch r.Method {
	case http.MethodGet:
		out := []Email{}
		for _, e := range s.Outbox() {
			if (id == "" || e.Id == id) && (email == "" || e.Source == email) {
				out = append(out, e)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"messages": out})
	case http.MethodDelete:
		s.mu.Lock()
		s.st.Outbox = slices.DeleteFunc(s.st.Outbox, func(e Email) bool { return id == "" || e.Id == id })
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// Resources lists identities, templates and the outbox.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := core.Table{Name: "Identities", Columns: []string{"Identity", "Type", "Region"}}
	for _, k := range core.SortedKeys(s.st.Identities) {
		i := s.st.Identities[k]
		ids.Rows = append(ids.Rows, []string{i.Name, identityType(i), strings.Split(k, "/")[1]})
	}
	ts := core.Table{Name: "Templates", Columns: []string{"Name", "Subject", "Region"}}
	for _, k := range core.SortedKeys(s.st.Templates) {
		t := s.st.Templates[k]
		ts.Rows = append(ts.Rows, []string{t.Name, t.Subject, strings.Split(k, "/")[1]})
	}
	ob := core.Table{Name: "Outbox", Columns: []string{"Id", "From", "To", "Subject", "Sent"}}
	for _, e := range slices.Backward(s.st.Outbox) {
		ob.Rows = append(ob.Rows, []string{e.Id, e.Source, strings.Join(e.Destination.ToAddresses, ", "), e.Subject, e.Timestamp.Format(time.RFC3339)})
	}
	return []core.Table{ids, ts, ob}
}

// Inspect returns an identity, a template or a sent message.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Identities":
		for _, i := range s.st.Identities {
			if i.Name == id {
				cp := *i
				cp.Topics, cp.Tags = maps.Clone(i.Topics), maps.Clone(i.Tags)
				return cp
			}
		}
	case "Templates":
		for _, t := range s.st.Templates {
			if t.Name == id {
				return *t
			}
		}
	case "Outbox":
		for _, e := range s.st.Outbox {
			if e.Id == id {
				return e
			}
		}
	}
	return nil
}
