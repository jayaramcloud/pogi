package sns

import (
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/core"
)

// ---- publishing ----

type publishIn struct {
	Id                     string // PublishBatch entries
	TopicArn               string
	TargetArn              string
	PhoneNumber            string
	Message                string
	Subject                string
	MessageStructure       string
	MessageAttributes      map[string]core.MessageAttribute
	MessageGroupId         string
	MessageDeduplicationId string
	auto                   bool   // sent by pogi itself (other services, the console): FIFO ids get defaults
	trace                  string // the publisher's flow
}

type published struct {
	Id             string // PublishBatch entries
	MessageId      string
	SequenceNumber string
}

func (s *Service) publish(c *core.Ctx, in *publishIn) (any, error) {
	in.trace = c.Trace
	return s.publishTo(c.Account, c.Region, c.Host, in)
}

type batchError struct {
	Id, Code, Message string
	SenderFault       bool
}

var batchID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

func (s *Service) publishBatch(c *core.Ctx, in *struct {
	TopicArn                   string
	PublishBatchRequestEntries []publishIn
}) (any, error) {
	entries := in.PublishBatchRequestEntries
	switch {
	case len(entries) == 0:
		return nil, core.Errorf(400, "EmptyBatchRequest", "The batch request doesn't contain any entries.")
	case len(entries) > 10:
		return nil, core.Errorf(400, "TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.")
	}
	ids, total := map[string]bool{}, 0
	for _, e := range entries {
		if !batchID.MatchString(e.Id) {
			return nil, core.Errorf(400, "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.")
		}
		if ids[e.Id] {
			return nil, core.Errorf(400, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.")
		}
		ids[e.Id] = true
		total += len(e.Message)
	}
	if total > 256<<10 {
		return nil, core.Errorf(400, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.")
	}
	s.mu.Lock()
	_, err := s.topicByARN(in.TopicArn)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	out := struct {
		Successful []*published
		Failed     []batchError
	}{}
	for _, e := range entries {
		e.TopicArn, e.TargetArn, e.PhoneNumber, e.trace = in.TopicArn, "", "", c.Trace
		res, err := s.publishTo(c.Account, c.Region, c.Host, &e)
		if err != nil {
			ae := core.AsError(err)
			out.Failed = append(out.Failed, batchError{e.Id, ae.Code, ae.Msg, true})
			continue
		}
		res.Id = e.Id
		out.Successful = append(out.Successful, res)
	}
	return &out, nil
}

// receive publishes what other services send to a topic: S3 events,
// EventBridge targets, CloudWatch alarms.
func (s *Service) receive(arn string, m *core.Message) error {
	_, region, account, _, _ := core.ParseARN(arn)
	_, err := s.publishTo(account, region, "", &publishIn{TopicArn: arn, Message: m.Body, Subject: m.Subject,
		MessageAttributes: maps.Clone(m.Attributes), MessageGroupId: m.GroupID, MessageDeduplicationId: m.DedupID, auto: true, trace: m.Trace})
	return err
}

// publishTo validates one message and sends it to a topic, a phone number or
// a platform endpoint. Deliveries happen after the lock is released.
func (s *Service) publishTo(account, region, host string, in *publishIn) (*published, error) {
	structured, err := checkMessage(in)
	if err != nil {
		return nil, err
	}
	n := note{id: core.UUID(), account: account, region: region, host: s.host(host), subject: in.Subject, attrs: in.MessageAttributes, trace: in.trace, at: time.Now().UTC()}
	arn := cmp.Or(in.TopicArn, in.TargetArn)
	switch {
	case in.PhoneNumber != "":
		if !phone.MatchString(in.PhoneNumber) {
			return nil, invalid("PhoneNumber Reason: %s is not valid to publish to", in.PhoneNumber)
		}
		n.protocol, n.endpoint, n.msg = "sms", in.PhoneNumber, pick(structured, "sms", in.Message)
	case arn == "":
		return nil, invalid("TopicArn or TargetArn Reason: no value for required parameter")
	case strings.Contains(arn, ":endpoint/"):
		s.mu.Lock()
		e := s.st.Endpoints[arn]
		disabled := e != nil && strings.EqualFold(e.Attrs["Enabled"], "false")
		s.mu.Unlock()
		switch {
		case e == nil:
			return nil, notFound("Endpoint")
		case disabled:
			return nil, core.Errorf(400, "EndpointDisabled", "Endpoint is disabled")
		}
		_, n.region, n.account, _, _ = core.ParseARN(arn)
		n.protocol, n.endpoint, n.msg = "application", arn, pick(structured, platformOf(arn), in.Message)
	default:
		s.mu.Lock()
		notes, out, err := s.fanout(arn, in, structured, n)
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		for _, n := range notes {
			s.send(n)
		}
		return out, nil
	}
	s.send(n)
	return &published{MessageId: n.id}, nil
}

// fanout makes one note per confirmed subscription whose filter policy
// accepts the message, after FIFO checks and deduplication; lock held.
func (s *Service) fanout(arn string, in *publishIn, structured map[string]string, n note) ([]note, *published, error) {
	t, err := s.topicByARN(arn)
	if err != nil {
		return nil, nil, err
	}
	n.topic, n.account, n.region = t.ARN, t.Account, t.Region
	out := &published{MessageId: n.id}
	switch {
	case t.fifo():
		if in.auto && in.MessageGroupId == "" {
			in.MessageGroupId = "default"
		}
		dedup := in.MessageDeduplicationId
		switch {
		case in.MessageGroupId == "":
			return nil, nil, invalid("The MessageGroupId parameter is required for FIFO topics")
		case dedup != "":
		case t.Attrs["ContentBasedDeduplication"] == "true":
			dedup = sha256Hex(in.Message)
		case in.auto:
			dedup = core.NewID()
		default:
			return nil, nil, invalid("The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")
		}
		for k, d := range t.Dedup {
			if n.at.Sub(d.At) >= 5*time.Minute {
				delete(t.Dedup, k)
			}
		}
		if d, ok := t.Dedup[dedup]; ok { // accepted, not delivered again
			return nil, &published{MessageId: d.ID, SequenceNumber: d.Seq}, nil
		}
		t.Seq++
		out.SequenceNumber = fmt.Sprintf("%020d", t.Seq)
		if t.Dedup == nil {
			t.Dedup = map[string]seen{}
		}
		t.Dedup[dedup] = seen{out.MessageId, out.SequenceNumber, n.at}
		n.group, n.dedup, n.seq = in.MessageGroupId, dedup, out.SequenceNumber
	case in.MessageDeduplicationId != "" && !in.auto:
		return nil, nil, invalid("MessageDeduplicationId Reason: The request includes MessageDeduplicationId parameter that is not valid for this topic type")
	default:
		n.group = in.MessageGroupId // standard topics pass it on to SQS fair queues
	}
	var notes []note
	for _, sub := range t.Subs {
		key := sub.Protocol
		if key == "application" {
			key = platformOf(sub.Endpoint)
		}
		msg := pick(structured, key, in.Message)
		if sub.Pending || !sub.accepts(msg, in.MessageAttributes) {
			continue
		}
		sn := n
		sn.subArn, sn.protocol, sn.endpoint, sn.msg = sub.ARN, sub.Protocol, sub.Endpoint, msg
		sn.raw = strings.EqualFold(sub.Attrs["RawMessageDelivery"], "true")
		sn.dlq = dlqOf(sub.Attrs["RedrivePolicy"])
		notes = append(notes, sn)
	}
	return notes, out, nil
}

// subjectRe is a valid Subject: printable ASCII that starts with a letter, number or punctuation
// mark (not a space), under 100 characters. Messages other services send skip it.
var subjectRe = regexp.MustCompile(`^[!-~][ -~]{0,98}$`)

// checkMessage validates a message and decodes MessageStructure=json into
// its per-protocol messages.
func checkMessage(in *publishIn) (map[string]string, error) {
	switch {
	case in.Message == "":
		return nil, invalid("Empty message")
	case len(in.Message) > 256<<10:
		return nil, invalid("Message too long")
	case in.Subject != "" && !in.auto && !subjectRe.MatchString(in.Subject):
		return nil, invalid("Subject")
	}
	for k, a := range in.MessageAttributes {
		kind, _, _ := strings.Cut(a.DataType, ".")
		switch {
		case kind != "String" && kind != "Number" && kind != "Binary":
			return nil, core.Errorf(400, "ParameterValueInvalid", "The message attribute '%s' has an invalid message attribute type, the set of supported type prefixes is Binary, Number, and String.", k)
		case kind == "Binary" && len(a.BinaryValue) == 0, kind != "Binary" && a.StringValue == "":
			return nil, core.Errorf(400, "ParameterValueInvalid", "The message attribute '%s' must contain non-empty message attribute value for message attribute type '%s'.", k, kind)
		case kind == "Number":
			if _, err := strconv.ParseFloat(a.StringValue, 64); err != nil {
				return nil, core.Errorf(400, "ParameterValueInvalid", "Could not cast message attribute '%s' value to number.", k)
			}
		}
	}
	switch in.MessageStructure {
	case "":
		return nil, nil
	case "json":
		var m map[string]string
		if json.Unmarshal([]byte(in.Message), &m) != nil {
			return nil, invalid("Message Structure - JSON message body failed to parse")
		}
		if _, ok := m["default"]; !ok {
			return nil, invalid("Message Structure - No default entry in JSON message body")
		}
		return m, nil
	}
	return nil, invalid("MessageStructure")
}

// pick chooses the message for a protocol (or push platform) under MessageStructure=json.
func pick(structured map[string]string, key, message string) string {
	if structured == nil {
		return message
	}
	if m, ok := structured[key]; ok {
		return m
	}
	return structured["default"]
}

// platformOf returns the push platform of an endpoint ARN (…:endpoint/GCM/app/id).
func platformOf(arn string) string {
	_, _, _, res, _ := core.ParseARN(arn)
	p := strings.Split(res, "/")
	if len(p) < 2 {
		return ""
	}
	return p[1]
}

// accepts applies the subscription's filter policy; lock held.
// ponytail: compiles the policy on every publish; cache it on the subscription if that ever shows up in a profile.
func (sub *subscription) accepts(msg string, attrs map[string]core.MessageAttribute) bool {
	policy := sub.Attrs["FilterPolicy"]
	if policy == "" {
		return true
	}
	p, err := pattern.Compile(policy)
	if err != nil {
		return false
	}
	if sub.Attrs["FilterPolicyScope"] == "MessageBody" {
		return p.MatchJSON([]byte(msg))
	}
	return p.Match(attrEvent(attrs))
}

// attrEvent turns message attributes into what attribute filter policies
// match: strings, numbers and String.Array lists. Binary attributes are ignored.
func attrEvent(attrs map[string]core.MessageAttribute) map[string]any {
	ev := map[string]any{}
	for k, a := range attrs {
		switch kind, _, _ := strings.Cut(a.DataType, "."); kind {
		case "String":
			var list []any
			if a.DataType == "String.Array" && json.Unmarshal([]byte(a.StringValue), &list) == nil {
				ev[k] = list
			} else {
				ev[k] = a.StringValue
			}
		case "Number":
			ev[k], _ = strconv.ParseFloat(a.StringValue, 64)
		}
	}
	return ev
}

// ---- delivery ----

// note is one message on its way to one destination. It holds copies only,
// so it is safe to use without the lock.
type note struct {
	id, account, region, host              string
	topic, subArn, protocol, endpoint, dlq string
	subject, msg, seq, group, dedup        string
	trace                                  string // the publisher's flow
	attrs                                  map[string]core.MessageAttribute
	raw                                    bool
	at                                     time.Time
}

// envelope is the JSON document SNS wraps notifications in.
type envelope struct {
	Type              string
	MessageId         string
	Token             string `json:",omitempty"`
	SequenceNumber    string `json:",omitempty"`
	TopicArn          string
	Subject           string `json:",omitempty"`
	Message           string
	SubscribeURL      string `json:",omitempty"`
	Timestamp         string
	SignatureVersion  string
	Signature         string
	SigningCertURL    string
	UnsubscribeURL    string               `json:",omitempty"`
	MessageAttributes map[string]attrValue `json:",omitempty"`
}

type attrValue struct{ Type, Value string }

func (n note) attributes() map[string]attrValue {
	out := map[string]attrValue{}
	for k, a := range n.attrs {
		v := a.StringValue
		if strings.HasPrefix(a.DataType, "Binary") {
			v = base64.StdEncoding.EncodeToString(a.BinaryValue)
		}
		out[k] = attrValue{a.DataType, v}
	}
	return out
}

// signature is a stand-in: pogi has no signing key, so nothing could verify a real one.
func (n note) signature() string {
	h := sha256.Sum256([]byte(n.id + n.msg))
	return base64.StdEncoding.EncodeToString(h[:])
}

func (n note) certURL() string {
	return "https://sns." + n.region + ".amazonaws.com/SimpleNotificationService-pogi.pem"
}

func (n note) unsubscribeURL() string {
	return "http://" + n.host + "/?Action=Unsubscribe&SubscriptionArn=" + n.subArn
}

func (n note) envelope() string {
	return jsonString(envelope{Type: "Notification", MessageId: n.id, SequenceNumber: n.seq, TopicArn: n.topic, Subject: n.subject,
		Message: n.msg, Timestamp: n.at.Format(core.ISO), SignatureVersion: "1", Signature: n.signature(),
		SigningCertURL: n.certURL(), UnsubscribeURL: n.unsubscribeURL(), MessageAttributes: n.attributes()})
}

func (n note) confirmation(token string) string {
	return jsonString(envelope{Type: "SubscriptionConfirmation", MessageId: n.id, Token: token, TopicArn: n.topic,
		Message:      "You have chosen to subscribe to the topic " + n.topic + ".\nTo confirm the subscription, visit the SubscribeURL included in this message.",
		SubscribeURL: "http://" + n.host + "/?Action=ConfirmSubscription&TopicArn=" + n.topic + "&Token=" + token,
		Timestamp:    n.at.Format(core.ISO), SignatureVersion: "1", Signature: n.signature(), SigningCertURL: n.certURL()})
}

// body is what queues, Firehose and HTTP endpoints get: the raw message or the envelope.
func (n note) body() string {
	if n.raw {
		return n.msg
	}
	return n.envelope()
}

// lambdaEvent is the event a Lambda function subscribed to a topic receives.
func (n note) lambdaEvent() string {
	var subject any // null when absent, as in Lambda
	if n.subject != "" {
		subject = n.subject
	}
	return jsonString(map[string]any{"Records": []any{map[string]any{
		"EventSource": "aws:sns", "EventVersion": "1.0", "EventSubscriptionArn": n.subArn,
		"Sns": map[string]any{"Type": "Notification", "MessageId": n.id, "TopicArn": n.topic, "Subject": subject, "Message": n.msg,
			"Timestamp": n.at.Format(core.ISO), "SignatureVersion": "1", "Signature": n.signature(),
			"SigningCertUrl": n.certURL(), "UnsubscribeUrl": n.unsubscribeURL(), "MessageAttributes": n.attributes()},
	}}})
}

// send delivers one note; call it without the lock.
func (s *Service) send(n note) {
	switch n.protocol {
	case "sqs", "firehose":
		m := &core.Message{Body: n.body(), GroupID: n.group, DedupID: n.dedup, Trace: n.trace, From: n.topic}
		if n.raw {
			m.Attributes = n.attrs
		}
		s.failed(n, s.hub.Deliver(n.endpoint, m))
	case "lambda": // Lambda runs it asynchronously; Deliver only fails for unknown functions
		s.failed(n, s.hub.Deliver(n.endpoint, &core.Message{Body: n.lambdaEvent(), Trace: n.trace, From: n.topic}))
	case "http", "https":
		body := n.body()
		core.Go("sns.post", func() { s.post(n, "Notification", body) })
	default: // email, email-json, sms, application: nothing to reach locally
		s.record(n)
	}
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

// post delivers to an HTTP(S) endpoint, retrying after 1, 2 and 4 seconds.
// ponytail: fixed backoff; honour the subscription's DeliveryPolicy if someone needs it.
func (s *Service) post(n note, kind, body string) {
	var err error
	start := time.Now()
	if kind == "Notification" {
		defer func() { s.hub.Span(core.SpanNotify, "sns", "notify", n.topic, n.endpoint, n.trace, start, err) }()
	}
	for i := range 4 {
		if i > 0 {
			time.Sleep(time.Second << (i - 1))
		}
		if err = postOnce(n, kind, body); err == nil {
			return
		}
	}
	s.failed(n, err)
}

func postOnce(n note, kind, body string) error {
	req, err := http.NewRequest(http.MethodPost, n.endpoint, strings.NewReader(body))
	if err != nil {
		return err
	}
	h := req.Header
	h.Set("Content-Type", "text/plain; charset=UTF-8")
	h.Set("User-Agent", "Amazon Simple Notification Service Agent")
	h.Set("x-amz-sns-message-type", kind)
	h.Set("x-amz-sns-message-id", n.id)
	if n.trace != "" {
		h.Set("X-Amzn-Trace-Id", core.TraceHeader(n.trace))
	}
	h.Set("x-amz-sns-topic-arn", n.topic)
	if kind == "Notification" {
		h.Set("x-amz-sns-subscription-arn", n.subArn)
	}
	if n.raw {
		h.Set("x-amz-sns-rawdelivery", "true")
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d", res.StatusCode)
	}
	return nil
}

// failed logs a delivery error and redrives the message to the subscription's dead-letter queue.
func (s *Service) failed(n note, err error) {
	if err == nil {
		return
	}
	s.hub.Log.Warn("sns delivery failed", "subscription", n.subArn, "endpoint", n.endpoint, "err", err)
	if n.dlq == "" {
		return
	}
	if err := s.hub.Deliver(n.dlq, &core.Message{Body: n.envelope(), GroupID: n.group, DedupID: n.dedup, Trace: n.trace, From: n.topic}); err != nil {
		s.hub.Log.Warn("sns dead-letter delivery failed", "queue", n.dlq, "err", err)
	}
}

// host is where SubscribeURL and UnsubscribeURL point: the caller's host, or
// pogi itself for messages that did not come from a request.
func (s *Service) host(h string) string {
	if h != "" {
		return h
	}
	if s.hub != nil && s.hub.Cfg.Port != 0 {
		return "localhost:" + strconv.Itoa(s.hub.Cfg.Port)
	}
	return "localhost:7644"
}

// jsonString encodes v without HTML escaping, as SNS does.
func jsonString(v any) string {
	var b strings.Builder
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	e.Encode(v)
	return strings.TrimSuffix(b.String(), "\n")
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// ---- outbox: email, SMS and mobile push ----

const outboxMax = 1000

// OutboxMessage is a notification pogi keeps instead of sending it: email,
// SMS and mobile push have no local destination.
type OutboxMessage struct {
	Timestamp         time.Time
	MessageId         string
	AccountId         string
	Region            string
	TopicArn          string `json:",omitempty"`
	SubscriptionArn   string `json:",omitempty"`
	Protocol          string // email, email-json, sms or application
	Destination       string // email address, phone number or platform endpoint ARN
	Subject           string `json:",omitempty"`
	Message           string
	MessageAttributes map[string]core.MessageAttribute `json:",omitempty"`
}

func (s *Service) record(n note) {
	msg := n.msg
	if n.protocol == "email-json" {
		msg = n.envelope()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Outbox = append(s.st.Outbox, OutboxMessage{n.at, n.id, n.account, n.region, n.topic, n.subArn, n.protocol, n.endpoint, n.subject, msg, n.attrs})
	if len(s.st.Outbox) > outboxMax {
		s.st.Outbox = s.st.Outbox[len(s.st.Outbox)-outboxMax:]
	}
}

// Outbox returns the last notifications kept for email, SMS and mobile push, oldest first.
func (s *Service) Outbox() []OutboxMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.st.Outbox)
}

// ClearOutbox forgets the kept notifications (the console's "Clear inbox").
func (s *Service) ClearOutbox() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Outbox = nil
}

// outboxHandler serves LocalStack's /_aws/sns/sms-messages and
// /_aws/sns/platform-endpoint-messages: GET lists, DELETE clears.
// accountId, region and phoneNumber/endpointArn filter both.
func (s *Service) outboxHandler(w http.ResponseWriter, r *http.Request) {
	protocol, key, filter := "sms", "sms_messages", "phoneNumber"
	switch strings.TrimSuffix(r.URL.Path, "/") {
	case "/_aws/sns/sms-messages":
	case "/_aws/sns/platform-endpoint-messages":
		protocol, key, filter = "application", "platform_endpoint_messages", "endpointArn"
	default:
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	match := func(m OutboxMessage) bool {
		return m.Protocol == protocol && (q.Get(filter) == "" || m.Destination == q.Get(filter)) &&
			(q.Get("accountId") == "" || m.AccountId == q.Get("accountId")) && (q.Get("region") == "" || m.Region == q.Get("region"))
	}
	switch r.Method {
	case http.MethodGet:
		out := map[string][]OutboxMessage{}
		s.mu.Lock()
		for _, m := range s.st.Outbox {
			if match(m) {
				out[m.Destination] = append(out[m.Destination], m)
			}
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{key: out})
	case http.MethodDelete:
		s.mu.Lock()
		s.st.Outbox = slices.DeleteFunc(s.st.Outbox, match)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
