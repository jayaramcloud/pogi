package servicebus

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The ATOM management API azservicebus/admin speaks: queues, topics,
// subscriptions and rules (create, get, update, list, delete) and their
// runtime properties. admin.Client always dials https://<endpoint host:port>/
// (even with UseDevelopmentEmulator=true, which only turns TLS off for AMQP),
// so pogi's main port, which takes HTTP and HTTPS alike, serves it. Any
// SharedAccessSignature or bearer token is accepted.

const (
	nsAtom = "http://www.w3.org/2005/Atom"
	nsSB   = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
	nsI    = "http://www.w3.org/2001/XMLSchema-instance"
)

// match claims Service Bus management traffic and nothing else: ATOM
// content, the $Resources and $namespaceinfo paths, requests signed with a
// Service Bus SharedAccessSignature, and *.servicebus.localhost hosts. Azure
// Storage signs with SharedKey or a sig= query, Key Vault and Cosmos have their
// own matchers, and AWS and Google clients send none of these.
//
// ponytail: a bearer-token (Entra ID) GET /<queue> with no ATOM content type looks like any other
// REST call and is not claimed; admin clients using TokenCredentials need a *.servicebus.localhost host.
func match(r *http.Request) bool {
	host, _, _ := strings.Cut(strings.ToLower(r.Host), ":")
	p := r.URL.Path
	switch {
	case strings.HasSuffix(host, ".servicebus.localhost"),
		strings.HasPrefix(p, "/$Resources/"), p == "/$namespaceinfo",
		strings.HasPrefix(r.Header.Get("Content-Type"), "application/atom+xml"):
		return true
	}
	return strings.HasPrefix(r.Header.Get("Authorization"), "SharedAccessSignature ") && r.URL.Query().Has("api-version") &&
		!r.URL.Query().Has("sig")
}

// ---- request bodies ----

type putBody struct {
	Content struct {
		Queue *descXML `xml:"QueueDescription"`
		Topic *descXML `xml:"TopicDescription"`
		Sub   *descXML `xml:"SubscriptionDescription"`
		Rule  *ruleXML `xml:"RuleDescription"`
	} `xml:"content"`
}

// descXML reads queue, topic and subscription descriptions; every field is text, "" when absent.
type descXML struct {
	LockDuration                      string   `xml:"LockDuration"`
	MaxSizeInMegabytes                string   `xml:"MaxSizeInMegabytes"`
	RequiresDuplicateDetection        string   `xml:"RequiresDuplicateDetection"`
	RequiresSession                   string   `xml:"RequiresSession"`
	DefaultMessageTimeToLive          string   `xml:"DefaultMessageTimeToLive"`
	DeadLetteringOnMessageExpiration  string   `xml:"DeadLetteringOnMessageExpiration"`
	DeadLetteringOnFilterEvalFailures string   `xml:"DeadLetteringOnFilterEvaluationExceptions"`
	DuplicateDetectionHistoryWindow   string   `xml:"DuplicateDetectionHistoryTimeWindow"`
	MaxDeliveryCount                  string   `xml:"MaxDeliveryCount"`
	EnableBatchedOperations           string   `xml:"EnableBatchedOperations"`
	Status                            string   `xml:"Status"`
	AutoDeleteOnIdle                  string   `xml:"AutoDeleteOnIdle"`
	EnablePartitioning                string   `xml:"EnablePartitioning"`
	ForwardTo                         string   `xml:"ForwardTo"`
	ForwardDeadLetteredMessagesTo     string   `xml:"ForwardDeadLetteredMessagesTo"`
	UserMetadata                      string   `xml:"UserMetadata"`
	SupportOrdering                   string   `xml:"SupportOrdering"`
	MaxMessageSizeInKilobytes         string   `xml:"MaxMessageSizeInKilobytes"`
	DefaultRule                       *ruleXML `xml:"DefaultRuleDescription"`
}

type ruleXML struct {
	Name   string     `xml:"Name"`
	Filter *filterXML `xml:"Filter"`
	Action *struct {
		Type       string  `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
		SQL        string  `xml:"SqlExpression"`
		Parameters *kvList `xml:"Parameters"`
	} `xml:"Action"`
}

type filterXML struct {
	Type             string  `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	SQL              string  `xml:"SqlExpression"`
	CorrelationID    string  `xml:"CorrelationId"`
	MessageID        string  `xml:"MessageId"`
	To               string  `xml:"To"`
	ReplyTo          string  `xml:"ReplyTo"`
	Label            string  `xml:"Label"`
	SessionID        string  `xml:"SessionId"`
	ReplyToSessionID string  `xml:"ReplyToSessionId"`
	ContentType      string  `xml:"ContentType"`
	Parameters       *kvList `xml:"Parameters"`
	Properties       *kvList `xml:"Properties"`
}

type kvList struct {
	KV []struct {
		Key   string `xml:"Key"`
		Value struct {
			Type string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
			Text string `xml:",chardata"`
		} `xml:"Value"`
	} `xml:"KeyValueOfstringanyType"`
}

func (l *kvList) values() map[string]any {
	if l == nil || len(l.KV) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, kv := range l.KV {
		_, typ, _ := strings.Cut(kv.Value.Type, ":")
		if typ == "" {
			typ = kv.Value.Type
		}
		t := kv.Value.Text
		var v any = t
		switch typ {
		case "boolean":
			v = t == "true"
		case "int", "long", "short", "byte":
			if n, err := strconv.ParseInt(t, 10, 64); err == nil {
				v = n
			}
		case "double", "float", "decimal":
			if f, err := strconv.ParseFloat(t, 64); err == nil {
				v = f
			}
		case "dateTime":
			if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
				v = tm.UTC()
			}
		}
		out[kv.Key] = v
	}
	return out
}

// check rejects a number in a description that is not a number or does not fit its field, as Service Bus does,
// instead of storing 0 (and so a default) or a wrapped value: a MaxDeliveryCount of 4294967297 must not become 1.
func (b *putBody) check() error {
	for _, d := range []*descXML{b.Content.Queue, b.Content.Topic, b.Content.Sub} {
		if d == nil {
			continue
		}
		for _, f := range []struct {
			name, v string
			bits    int
		}{{"MaxSizeInMegabytes", d.MaxSizeInMegabytes, 32}, {"MaxDeliveryCount", d.MaxDeliveryCount, 32}, {"MaxMessageSizeInKilobytes", d.MaxMessageSizeInKilobytes, 64}} {
			if _, err := strconv.ParseInt(f.v, 10, f.bits); f.v != "" && err != nil {
				return invalid("The value '%s' of %s is not a valid number.", f.v, f.name)
			}
		}
	}
	return nil
}

func (d *descXML) props() Props {
	b := func(s string) bool { return s == "true" }
	dur := func(s string) Dur { v, _ := ParseDur(s); return v }
	n := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
	n32 := func(s string) int32 { v, _ := strconv.ParseInt(s, 10, 32); return int32(v) } // check() rejected what does not fit
	return Props{
		LockDuration: dur(d.LockDuration), MaxSizeInMegabytes: n32(d.MaxSizeInMegabytes),
		RequiresDuplicateDetection: b(d.RequiresDuplicateDetection), RequiresSession: b(d.RequiresSession),
		DefaultMessageTimeToLive: dur(d.DefaultMessageTimeToLive), DeadLetteringOnMessageExpiration: b(d.DeadLetteringOnMessageExpiration),
		DeadLetteringOnFilterEvalFailures: b(d.DeadLetteringOnFilterEvalFailures), DuplicateDetectionHistoryWindow: dur(d.DuplicateDetectionHistoryWindow),
		MaxDeliveryCount: n32(d.MaxDeliveryCount), EnableBatchedOperations: b(d.EnableBatchedOperations), Status: d.Status,
		AutoDeleteOnIdle: dur(d.AutoDeleteOnIdle), EnablePartitioning: b(d.EnablePartitioning), ForwardTo: d.ForwardTo,
		ForwardDeadLetteredMessagesTo: d.ForwardDeadLetteredMessagesTo, UserMetadata: d.UserMetadata, SupportOrdering: b(d.SupportOrdering),
		MaxMessageSizeInKilobytes: n(d.MaxMessageSizeInKilobytes),
	}
}

func (r *ruleXML) rule() *Rule {
	out := &Rule{Name: r.Name, Filter: Filter{Type: "TrueFilter"}}
	if f := r.Filter; f != nil {
		out.Filter = Filter{Type: f.Type, SQL: f.SQL, Params: f.Parameters.values(), CorrelationID: f.CorrelationID, MessageID: f.MessageID,
			To: f.To, ReplyTo: f.ReplyTo, Label: f.Label, SessionID: f.SessionID, ReplyToSessionID: f.ReplyToSessionID,
			ContentType: f.ContentType, Properties: f.Properties.values()}
	}
	if a := r.Action; a != nil && a.Type == "SqlRuleAction" {
		out.Action, out.ActionParams = a.SQL, a.Parameters.values()
	}
	return out
}

// ---- responses ----

func esc(s string) string {
	var b bytes.Buffer
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func ts(t time.Time) string {
	if t.IsZero() {
		return "0001-01-01T00:00:00"
	}
	return t.UTC().Format("2006-01-02T15:04:05.0000000Z")
}

// el writes <name>text</name>, or nothing for an empty text.
func el(b *strings.Builder, name, text string) {
	if text != "" {
		fmt.Fprintf(b, "<%s>%s</%s>", name, esc(text), name)
	}
}

func boolS(v bool) string { return strconv.FormatBool(v) }

func (e Entity) countDetails(b *strings.Builder) {
	fmt.Fprintf(b, `<CountDetails xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus">`+
		`<d2p1:ActiveMessageCount>%d</d2p1:ActiveMessageCount><d2p1:DeadLetterMessageCount>%d</d2p1:DeadLetterMessageCount>`+
		`<d2p1:ScheduledMessageCount>%d</d2p1:ScheduledMessageCount><d2p1:TransferMessageCount>0</d2p1:TransferMessageCount>`+
		`<d2p1:TransferDeadLetterMessageCount>0</d2p1:TransferDeadLetterMessageCount></CountDetails>`, e.Active+e.Deferred, e.Dead, e.Scheduled)
}

// description renders the *Description element of an entity.
func (e Entity) description() string {
	var b strings.Builder
	root := map[string]string{"queue": "QueueDescription", "topic": "TopicDescription", "subscription": "SubscriptionDescription"}[e.Kind]
	fmt.Fprintf(&b, `<%s xmlns="%s" xmlns:i="%s">`, root, nsSB, nsI)
	switch e.Kind {
	case "queue":
		el(&b, "LockDuration", e.LockDuration.ISO())
		el(&b, "MaxSizeInMegabytes", strconv.Itoa(int(e.MaxSizeInMegabytes)))
		el(&b, "RequiresDuplicateDetection", boolS(e.RequiresDuplicateDetection))
		el(&b, "RequiresSession", boolS(e.RequiresSession))
		el(&b, "DefaultMessageTimeToLive", e.DefaultMessageTimeToLive.ISO())
		el(&b, "DeadLetteringOnMessageExpiration", boolS(e.DeadLetteringOnMessageExpiration))
		el(&b, "DuplicateDetectionHistoryTimeWindow", e.DuplicateDetectionHistoryWindow.ISO())
		el(&b, "MaxDeliveryCount", strconv.Itoa(int(e.MaxDeliveryCount)))
		el(&b, "EnableBatchedOperations", boolS(e.EnableBatchedOperations))
		el(&b, "SizeInBytes", strconv.FormatInt(e.Size, 10))
		el(&b, "MessageCount", strconv.Itoa(e.Active+e.Dead+e.Scheduled+e.Deferred))
		el(&b, "IsAnonymousAccessible", "false")
		el(&b, "Status", e.Status)
		el(&b, "CreatedAt", ts(e.Created))
		el(&b, "UpdatedAt", ts(e.Updated))
		el(&b, "AccessedAt", ts(time.Time{}))
		el(&b, "SupportOrdering", "true")
		el(&b, "AutoDeleteOnIdle", e.AutoDeleteOnIdle.ISO())
		el(&b, "EnablePartitioning", boolS(e.EnablePartitioning))
		el(&b, "EnableExpress", "false")
		e.countDetails(&b)
		el(&b, "ForwardTo", e.ForwardTo)
		el(&b, "ForwardDeadLetteredMessagesTo", e.ForwardDeadLetteredMessagesTo)
		el(&b, "UserMetadata", e.UserMetadata)
		el(&b, "MaxMessageSizeInKilobytes", strconv.FormatInt(e.MaxMessageSizeInKilobytes, 10))
	case "topic":
		el(&b, "DefaultMessageTimeToLive", e.DefaultMessageTimeToLive.ISO())
		el(&b, "MaxSizeInMegabytes", strconv.Itoa(int(e.MaxSizeInMegabytes)))
		el(&b, "RequiresDuplicateDetection", boolS(e.RequiresDuplicateDetection))
		el(&b, "DuplicateDetectionHistoryTimeWindow", e.DuplicateDetectionHistoryWindow.ISO())
		el(&b, "EnableBatchedOperations", boolS(e.EnableBatchedOperations))
		el(&b, "SizeInBytes", strconv.FormatInt(e.Size, 10))
		el(&b, "FilteringMessagesBeforePublishing", "false")
		el(&b, "IsAnonymousAccessible", "false")
		el(&b, "Status", e.Status)
		el(&b, "UserMetadata", e.UserMetadata)
		el(&b, "CreatedAt", ts(e.Created))
		el(&b, "UpdatedAt", ts(e.Updated))
		el(&b, "AccessedAt", ts(time.Time{}))
		el(&b, "SupportOrdering", boolS(e.SupportOrdering))
		el(&b, "AutoDeleteOnIdle", e.AutoDeleteOnIdle.ISO())
		el(&b, "EnablePartitioning", boolS(e.EnablePartitioning))
		el(&b, "EnableSubscriptionPartitioning", "false")
		el(&b, "EnableExpress", "false")
		e.countDetails(&b)
		el(&b, "SubscriptionCount", strconv.Itoa(e.Subscriptions))
		el(&b, "SqlFilterCount", strconv.Itoa(e.SQLFilters))
		el(&b, "CorrelationFilterCount", strconv.Itoa(e.CorrelationFilters))
		el(&b, "MaxMessageSizeInKilobytes", strconv.FormatInt(e.MaxMessageSizeInKilobytes, 10))
	default:
		el(&b, "LockDuration", e.LockDuration.ISO())
		el(&b, "RequiresSession", boolS(e.RequiresSession))
		el(&b, "DefaultMessageTimeToLive", e.DefaultMessageTimeToLive.ISO())
		el(&b, "DeadLetteringOnMessageExpiration", boolS(e.DeadLetteringOnMessageExpiration))
		el(&b, "DeadLetteringOnFilterEvaluationExceptions", boolS(e.DeadLetteringOnFilterEvalFailures))
		el(&b, "MessageCount", strconv.Itoa(e.Active+e.Dead+e.Deferred))
		el(&b, "MaxDeliveryCount", strconv.Itoa(int(e.MaxDeliveryCount)))
		el(&b, "EnableBatchedOperations", boolS(e.EnableBatchedOperations))
		el(&b, "Status", e.Status)
		el(&b, "CreatedAt", ts(e.Created))
		el(&b, "UpdatedAt", ts(e.Updated))
		el(&b, "AccessedAt", ts(time.Time{}))
		e.countDetails(&b)
		el(&b, "AutoDeleteOnIdle", e.AutoDeleteOnIdle.ISO())
		el(&b, "ForwardTo", e.ForwardTo)
		el(&b, "ForwardDeadLetteredMessagesTo", e.ForwardDeadLetteredMessagesTo)
		el(&b, "UserMetadata", e.UserMetadata)
	}
	fmt.Fprintf(&b, "</%s>", root)
	return b.String()
}

func kvXML(name string, m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<%s xmlns:d2p1="http://schemas.datacontract.org/2004/07/System.Collections.Generic">`, name)
	for _, k := range core.SortedKeys(m) {
		typ, text := "string", ""
		switch v := norm(m[k]).(type) {
		case bool:
			typ, text = "boolean", boolS(v)
		case int64:
			typ, text = "int", strconv.FormatInt(v, 10)
		case float64:
			typ, text = "double", strconv.FormatFloat(v, 'f', -1, 64)
		case time.Time:
			typ, text = "dateTime", v.UTC().Format(time.RFC3339Nano)
		default:
			text = fmt.Sprint(v)
		}
		fmt.Fprintf(&b, `<KeyValueOfstringanyType><Key>%s</Key><Value i:type="l28:%s" xmlns:l28="http://www.w3.org/2001/XMLSchema">%s</Value></KeyValueOfstringanyType>`,
			esc(k), typ, esc(text))
	}
	fmt.Fprintf(&b, "</%s>", name)
	return b.String()
}

func ruleDescription(r *Rule) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<RuleDescription xmlns="%s" xmlns:i="%s">`, nsSB, nsI)
	f := r.Filter
	switch f.Type {
	case "SqlFilter":
		fmt.Fprintf(&b, `<Filter i:type="SqlFilter"><SqlExpression>%s</SqlExpression><CompatibilityLevel>20</CompatibilityLevel>%s</Filter>`, esc(f.SQL), kvXML("Parameters", f.Params))
	case "FalseFilter":
		b.WriteString(`<Filter i:type="FalseFilter"><SqlExpression>1=0</SqlExpression><CompatibilityLevel>20</CompatibilityLevel></Filter>`)
	case "CorrelationFilter":
		b.WriteString(`<Filter i:type="CorrelationFilter">`)
		el(&b, "CorrelationId", f.CorrelationID)
		el(&b, "MessageId", f.MessageID)
		el(&b, "To", f.To)
		el(&b, "ReplyTo", f.ReplyTo)
		el(&b, "Label", f.Label)
		el(&b, "SessionId", f.SessionID)
		el(&b, "ReplyToSessionId", f.ReplyToSessionID)
		el(&b, "ContentType", f.ContentType)
		b.WriteString(kvXML("Properties", f.Properties) + `</Filter>`)
	default:
		b.WriteString(`<Filter i:type="TrueFilter"><SqlExpression>1=1</SqlExpression><CompatibilityLevel>20</CompatibilityLevel></Filter>`)
	}
	if r.Action == "" {
		b.WriteString(`<Action i:type="EmptyRuleAction"/>`)
	} else {
		fmt.Fprintf(&b, `<Action i:type="SqlRuleAction"><SqlExpression>%s</SqlExpression><CompatibilityLevel>20</CompatibilityLevel>%s</Action>`, esc(r.Action), kvXML("Parameters", r.ActionParams))
	}
	fmt.Fprintf(&b, `<CreatedAt>%s</CreatedAt><Name>%s</Name></RuleDescription>`, ts(r.Created), esc(r.Name))
	return b.String()
}

// entryXML wraps a description in an ATOM entry; root adds the xmlns for a document, not a feed item.
func entryXML(base, path, title, desc string, root bool) string {
	ns := ""
	if root {
		ns = ` xmlns="` + nsAtom + `"`
	}
	id := base + path
	return fmt.Sprintf(`<entry%s><id>%s</id><title type="text">%s</title><updated>%s</updated><author><name>%s</name></author>`+
		`<link rel="self" href="%s"/><content type="application/xml">%s</content></entry>`,
		ns, esc(id), esc(title), ts(time.Now()), esc(title), esc(id), desc)
}

func feedXML(base, path, title string, entries []string) string {
	return fmt.Sprintf(`<feed xmlns="%s"><title type="text">%s</title><id>%s</id><updated>%s</updated><link rel="self" href="%s"/>%s</feed>`,
		nsAtom, esc(title), esc(base+path), ts(time.Now()), esc(base+path), strings.Join(entries, ""))
}

// page applies $skip and $top.
func page[T any](items []T, q url.Values) []T {
	skip, _ := strconv.Atoi(q.Get("$skip"))
	top, _ := strconv.Atoi(q.Get("$top"))
	if skip > len(items) {
		skip = len(items)
	}
	items = items[skip:]
	if top > 0 && top < len(items) {
		items = items[:top]
	}
	return items
}

// ---- handler ----

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for i := range p {
		p[i], _ = url.PathUnescape(p[i])
	}
	nsName := namespaceOf(r.Host)
	base := "https://" + r.Host + "/"
	if r.TLS == nil {
		base = "http://" + r.Host + "/"
	}
	c.Op = opName(r.Method, p, r.Header.Get("If-Match") != "")
	err := c.Intercept()
	status, body := 0, ""
	if err == nil {
		_, status, body, err = s.atom(r, p, nsName, base)
	}
	w.Header().Set("x-ms-request-id", c.RequestID)
	if err != nil {
		e := core.AsError(err)
		c.ErrCode = e.Code
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(e.Status)
		fmt.Fprintf(w, `<Error><Code>%d</Code><Detail>%s TrackingId:%s</Detail></Error>`, e.Status, esc(e.Msg), c.RequestID)
		return
	}
	w.Header().Set("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write([]byte(`<?xml version="1.0" encoding="utf-8" ?>` + body))
	}
}

// opName names an ATOM request for the request log.
func opName(method string, p []string, update bool) string {
	verb := map[string]string{http.MethodGet: "Get", http.MethodHead: "Get", http.MethodDelete: "Delete", http.MethodPut: "Create"}[method]
	if update {
		verb = "Update"
	}
	switch {
	case len(p) == 1 && p[0] == "$namespaceinfo":
		return "GetNamespace"
	case len(p) == 2 && p[0] == "$Resources":
		return "List" + p[1]
	case len(p) == 1:
		return verb + "Entity"
	case len(p) == 2:
		return "ListSubscriptions"
	case len(p) == 3:
		return verb + "Subscription"
	case len(p) == 4:
		return "ListRules"
	}
	return verb + "Rule"
}

// atom does one request: the operation name, status and body, or an error.
func (s *Service) atom(r *http.Request, p []string, ns, base string) (op string, status int, body string, err error) {
	read := r.Method == http.MethodGet || r.Method == http.MethodHead
	del := r.Method == http.MethodDelete
	put := r.Method == http.MethodPut
	update := r.Header.Get("If-Match") != ""
	q := r.URL.Query()
	for _, k := range []string{"$skip", "$top"} {
		if n, err := strconv.Atoi(q.Get(k)); err == nil && n < 0 {
			return "List", 0, "", invalid("The value of %s must not be negative.", k)
		}
	}
	switch {
	case !read && !del && !put:
		return "Unsupported", 0, "", core.Errorf(405, "MethodNotAllowed", "Method %s is not allowed.", r.Method)

	case len(p) == 1 && p[0] == "$namespaceinfo":
		s.mu.Lock()
		n := s.ns(ns)
		created := n.Created
		s.mu.Unlock()
		return "GetNamespace", 200, fmt.Sprintf(`<entry xmlns="%s"><id>%s</id><title type="text">%s</title><updated>%s</updated>`+
			`<content type="application/xml"><NamespaceInfo xmlns="%s" xmlns:i="%s"><CreatedTime>%s</CreatedTime>`+
			`<MessagingSKU>Standard</MessagingSKU><ModifiedTime>%s</ModifiedTime><Name>%s</Name><NamespaceType>Messaging</NamespaceType></NamespaceInfo></content></entry>`,
			nsAtom, esc(base+"$namespaceinfo"), esc(ns), ts(time.Now()), nsSB, nsI, ts(created), ts(created), esc(ns)), nil

	case len(p) == 2 && p[0] == "$Resources" && read:
		s.mu.Lock()
		defer s.mu.Unlock()
		var entries []string
		switch strings.ToLower(p[1]) {
		case "queues":
			op = "ListQueues"
			for _, e := range s.listQueues(ns) {
				entries = append(entries, entryXML(base, e.Name, e.Name, e.description(), false))
			}
		case "topics":
			op = "ListTopics"
			for _, e := range s.listTopics(ns) {
				entries = append(entries, entryXML(base, e.Name, e.Name, e.description(), false))
			}
		default:
			return "Unsupported", 0, "", notFound("Resource '" + p[1] + "'")
		}
		return op, 200, feedXML(base, "$Resources/"+p[1], p[1], page(entries, q)), nil

	case len(p) == 1 && read:
		s.mu.Lock()
		defer s.mu.Unlock()
		n := s.ns(ns)
		if e := n.Queues[key(p[0])]; e != nil {
			return "GetQueue", 200, entryXML(base, e.Name, e.Name, e.entity(s.now()).description(), true), nil
		}
		if e := n.Topics[key(p[0])]; e != nil {
			return "GetTopic", 200, entryXML(base, e.Name, e.Name, e.entity(s.now()).description(), true), nil
		}
		return "GetQueue", 0, "", notFound("Entity '" + p[0] + "'")

	case len(p) == 1 && del:
		if !s.DeleteQueue(ns, p[0]) && !s.DeleteTopic(ns, p[0]) {
			return "DeleteQueue", 0, "", notFound("Entity '" + p[0] + "'")
		}
		return "DeleteQueue", 200, "", nil

	case len(p) == 1 && put:
		var in putBody
		if xml.Unmarshal(core.Body(r), &in) != nil {
			return "Create", 0, "", invalid("The request body is not a valid ATOM entry.")
		}
		if err := in.check(); err != nil {
			return "Create", 0, "", err
		}
		var e Entity
		switch {
		case in.Content.Queue != nil:
			op = "CreateQueue"
			if update {
				op = "UpdateQueue"
				e, err = s.UpdateQueue(ns, p[0], in.Content.Queue.props())
			} else {
				e, err = s.CreateQueue(ns, p[0], in.Content.Queue.props())
			}
		case in.Content.Topic != nil:
			op = "CreateTopic"
			if update {
				op = "UpdateTopic"
				e, err = s.UpdateTopic(ns, p[0], in.Content.Topic.props())
			} else {
				e, err = s.CreateTopic(ns, p[0], in.Content.Topic.props())
			}
		default:
			return "Create", 0, "", invalid("The entry holds neither a QueueDescription nor a TopicDescription.")
		}
		if err != nil {
			return op, 0, "", err
		}
		return op, created(update), entryXML(base, e.Name, e.Name, e.description(), true), nil

	case len(p) == 2 && strings.EqualFold(p[1], "Subscriptions") && read:
		s.mu.Lock()
		defer s.mu.Unlock()
		t := s.ns(ns).Topics[key(p[0])]
		if t == nil {
			return "ListSubscriptions", 0, "", notFound("Topic '" + p[0] + "'")
		}
		var entries []string
		for _, k := range core.SortedKeys(t.Subs) {
			e := t.Subs[k].entity(t.Name, s.now())
			entries = append(entries, entryXML(base, p[0]+"/Subscriptions/"+e.Name, e.Name, e.description(), false))
		}
		return "ListSubscriptions", 200, feedXML(base, p[0]+"/Subscriptions", "Subscriptions", page(entries, q)), nil

	case len(p) == 3 && strings.EqualFold(p[1], "Subscriptions"):
		path := p[0] + "/Subscriptions/" + p[2]
		switch {
		case read:
			e, ok := s.GetSubscription(ns, p[0], p[2])
			if !ok {
				return "GetSubscription", 0, "", notFound("Subscription '" + p[2] + "'")
			}
			return "GetSubscription", 200, entryXML(base, path, e.Name, e.description(), true), nil
		case del:
			if !s.DeleteSubscription(ns, p[0], p[2]) {
				return "DeleteSubscription", 0, "", notFound("Subscription '" + p[2] + "'")
			}
			return "DeleteSubscription", 200, "", nil
		}
		var in putBody
		if xml.Unmarshal(core.Body(r), &in) != nil || in.Content.Sub == nil {
			return "CreateSubscription", 0, "", invalid("The entry holds no SubscriptionDescription.")
		}
		if err := in.check(); err != nil {
			return "CreateSubscription", 0, "", err
		}
		var e Entity
		if update {
			op = "UpdateSubscription"
			e, err = s.UpdateSubscription(ns, p[0], p[2], in.Content.Sub.props())
		} else {
			op = "CreateSubscription"
			var def *Rule
			if d := in.Content.Sub.DefaultRule; d != nil {
				def = d.rule()
			}
			e, err = s.CreateSubscription(ns, p[0], p[2], in.Content.Sub.props(), def)
		}
		if err != nil {
			return op, 0, "", err
		}
		return op, created(update), entryXML(base, path, e.Name, e.description(), true), nil

	case len(p) == 4 && strings.EqualFold(p[1], "Subscriptions") && strings.EqualFold(p[3], "Rules") && read:
		rules, err := s.ListRules(ns, p[0], p[2])
		if err != nil {
			return "ListRules", 0, "", err
		}
		var entries []string
		for _, rl := range rules {
			entries = append(entries, entryXML(base, p[0]+"/Subscriptions/"+p[2]+"/Rules/"+rl.Name, rl.Name, ruleDescription(&rl), false))
		}
		return "ListRules", 200, feedXML(base, p[0]+"/Subscriptions/"+p[2]+"/Rules", "Rules", page(entries, q)), nil

	case len(p) == 5 && strings.EqualFold(p[1], "Subscriptions") && strings.EqualFold(p[3], "Rules"):
		path := strings.Join(p, "/")
		switch {
		case read:
			rl, err := s.GetRule(ns, p[0], p[2], p[4])
			if err != nil {
				return "GetRule", 0, "", err
			}
			return "GetRule", 200, entryXML(base, path, rl.Name, ruleDescription(&rl), true), nil
		case del:
			if err := s.RemoveRule(ns, p[0], p[2], p[4]); err != nil {
				return "DeleteRule", 0, "", err
			}
			return "DeleteRule", 200, "", nil
		}
		var in putBody
		if xml.Unmarshal(core.Body(r), &in) != nil || in.Content.Rule == nil {
			return "CreateRule", 0, "", invalid("The entry holds no RuleDescription.")
		}
		rl := in.Content.Rule.rule()
		rl.Name = p[4]
		if update {
			op, err = "UpdateRule", s.UpdateRule(ns, p[0], p[2], *rl)
		} else {
			op, err = "CreateRule", s.AddRule(ns, p[0], p[2], *rl)
		}
		if err != nil {
			return op, 0, "", err
		}
		got, _ := s.GetRule(ns, p[0], p[2], p[4])
		return op, created(update), entryXML(base, path, got.Name, ruleDescription(&got), true), nil
	}
	return "Unsupported", 0, "", notFound("Resource '" + r.URL.Path + "'")
}

func created(update bool) int {
	if update {
		return http.StatusOK
	}
	return http.StatusCreated
}

// listQueues and listTopics are ListQueues and ListTopics for callers already holding the lock.
func (s *Service) listQueues(ns string) []Entity {
	var out []Entity
	for _, k := range core.SortedKeys(s.ns(ns).Queues) {
		out = append(out, s.ns(ns).Queues[k].entity(s.now()))
	}
	return out
}

func (s *Service) listTopics(ns string) []Entity {
	var out []Entity
	for _, k := range core.SortedKeys(s.ns(ns).Topics) {
		out = append(out, s.ns(ns).Topics[k].entity(s.now()))
	}
	return out
}

// ---- rules ----

// AddRule adds a rule to a subscription; the name must be new.
func (s *Service) AddRule(ns, topic, sub string, r Rule) error {
	if err := r.compile(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(ns, topic, sub)
	if err != nil {
		return err
	}
	for _, x := range sb.Rules {
		if strings.EqualFold(x.Name, r.Name) {
			return conflict(r.Name)
		}
	}
	r.Created = s.now()
	sb.Rules = append(sb.Rules, &r)
	return nil
}

// UpdateRule replaces an existing rule.
func (s *Service) UpdateRule(ns, topic, sub string, r Rule) error {
	if err := r.compile(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(ns, topic, sub)
	if err != nil {
		return err
	}
	for i, x := range sb.Rules {
		if strings.EqualFold(x.Name, r.Name) {
			r.Created = x.Created
			sb.Rules[i] = &r
			return nil
		}
	}
	return notFound("Rule '" + r.Name + "'")
}

// GetRule returns one rule of a subscription.
func (s *Service) GetRule(ns, topic, sub, name string) (Rule, error) {
	rules, err := s.ListRules(ns, topic, sub)
	if err != nil {
		return Rule{}, err
	}
	for _, r := range rules {
		if strings.EqualFold(r.Name, name) {
			return r, nil
		}
	}
	return Rule{}, notFound("Rule '" + name + "'")
}

// ListRules returns a subscription's rules in the order they were added.
func (s *Service) ListRules(ns, topic, sub string) ([]Rule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(ns, topic, sub)
	if err != nil {
		return nil, err
	}
	out := make([]Rule, len(sb.Rules))
	for i, r := range sb.Rules {
		out[i] = Rule{Name: r.Name, Filter: r.Filter, Action: r.Action, ActionParams: r.ActionParams, Created: r.Created}
	}
	return out, nil
}

// RemoveRule deletes a rule; a subscription without rules gets no messages.
func (s *Service) RemoveRule(ns, topic, sub, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, err := s.sub(ns, topic, sub)
	if err != nil {
		return err
	}
	for i, r := range sb.Rules {
		if strings.EqualFold(r.Name, name) {
			sb.Rules = append(sb.Rules[:i], sb.Rules[i+1:]...)
			return nil
		}
	}
	return notFound("Rule '" + name + "'")
}
