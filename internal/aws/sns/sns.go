// Package sns emulates Amazon SNS over the Query protocol: standard and FIFO
// topics, subscriptions with filter policies and raw delivery, fan-out to SQS,
// Lambda, Firehose and HTTP(S) endpoints, and an outbox for email, SMS and
// mobile push, which have no local destination.
package sns

import (
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the SNS emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Topics    map[string]*topic            `json:"topics"`    // by ARN
	Apps      map[string]*platformApp      `json:"apps"`      // platform applications by ARN
	Endpoints map[string]*platformEndpoint `json:"endpoints"` // platform endpoints by ARN
	SMS       map[string]map[string]string `json:"sms"`       // account/region → SMS attributes
	Outbox    []OutboxMessage              `json:"outbox"`
}

func newState() state {
	return state{Topics: map[string]*topic{}, Apps: map[string]*platformApp{}, Endpoints: map[string]*platformEndpoint{}, SMS: map[string]map[string]string{}}
}

type topic struct {
	ARN     string                   `json:"arn"`
	Name    string                   `json:"name"`
	Account string                   `json:"account"`
	Region  string                   `json:"region"`
	Attrs   map[string]string        `json:"attrs"`
	Tags    map[string]string        `json:"tags,omitempty"`
	Subs    map[string]*subscription `json:"subs"`
	Deleted int                      `json:"deleted,omitempty"`
	Seq     int64                    `json:"seq,omitempty"`
	Dedup   map[string]seen          `json:"dedup,omitempty"` // FIFO deduplication id → first publish
}

type seen struct {
	ID  string    `json:"id"`
	Seq string    `json:"seq"`
	At  time.Time `json:"at"`
}

type subscription struct {
	ARN      string            `json:"arn"`
	Topic    string            `json:"topic"`
	Owner    string            `json:"owner"`
	Protocol string            `json:"protocol"`
	Endpoint string            `json:"endpoint"`
	Attrs    map[string]string `json:"attrs,omitempty"`
	Token    string            `json:"token,omitempty"`
	Pending  bool              `json:"pending,omitempty"`
}

type platformApp struct {
	ARN      string            `json:"arn"`
	Name     string            `json:"name"`
	Platform string            `json:"platform"`
	Attrs    map[string]string `json:"attrs"`
}

type platformEndpoint struct {
	ARN   string            `json:"arn"`
	App   string            `json:"app"`
	Attrs map[string]string `json:"attrs"`
}

// New creates the SNS service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{XMLNS: "http://sns.amazonaws.com/doc/2010-03-31/", Ops: map[string]core.Handler{
		"CreateTopic":                        core.H(s.createTopic),
		"DeleteTopic":                        core.H(s.deleteTopic),
		"ListTopics":                         core.H(s.listTopics),
		"GetTopicAttributes":                 core.H(s.getTopicAttributes),
		"SetTopicAttributes":                 core.H(s.setTopicAttributes),
		"TagResource":                        core.H(s.tagResource),
		"UntagResource":                      core.H(s.untagResource),
		"ListTagsForResource":                core.H(s.listTags),
		"AddPermission":                      core.H(s.addPermission),
		"RemovePermission":                   core.H(s.removePermission),
		"Subscribe":                          core.H(s.subscribe),
		"ConfirmSubscription":                core.H(s.confirmSubscription),
		"Unsubscribe":                        core.H(s.unsubscribe),
		"ListSubscriptions":                  core.H(s.listSubscriptions),
		"ListSubscriptionsByTopic":           core.H(s.listSubscriptions),
		"GetSubscriptionAttributes":          core.H(s.getSubscriptionAttributes),
		"SetSubscriptionAttributes":          core.H(s.setSubscriptionAttributes),
		"Publish":                            core.H(s.publish),
		"PublishBatch":                       core.H(s.publishBatch),
		"CheckIfPhoneNumberIsOptedOut":       core.H(s.checkOptedOut),
		"ListPhoneNumbersOptedOut":           core.H(s.listOptedOut),
		"GetSMSAttributes":                   core.H(s.getSMSAttributes),
		"SetSMSAttributes":                   core.H(s.setSMSAttributes),
		"CreatePlatformApplication":          core.H(s.createPlatformApplication),
		"DeletePlatformApplication":          core.H(s.deletePlatform),
		"ListPlatformApplications":           core.H(s.listPlatformApplications),
		"GetPlatformApplicationAttributes":   core.H(s.getPlatformAttributes),
		"SetPlatformApplicationAttributes":   core.H(s.setPlatformAttributes),
		"CreatePlatformEndpoint":             core.H(s.createPlatformEndpoint),
		"DeleteEndpoint":                     core.H(s.deletePlatform),
		"ListEndpointsByPlatformApplication": core.H(s.listEndpoints),
		"GetEndpointAttributes":              core.H(s.getPlatformAttributes),
		"SetEndpointAttributes":              core.H(s.setPlatformAttributes),
	}}
	if hub != nil {
		hub.OnDeliver("sns", s.receive)
		hub.Mount("/_aws/sns/", http.HandlerFunc(s.outboxHandler))
	}
	return s
}

func (s *Service) Info() core.Info {
	// The smithy service name is a JSON target for pogi's own in-process calls (hub.Call).
	return core.Info{Name: "sns", Cloud: core.AWS, Title: "SNS", Signing: []string{"sns"}, Targets: []string{"AmazonSimpleNotificationService"}, Match: isLink, Protocol: "query"}
}

// isLink claims the SubscribeURL and UnsubscribeURL links SNS hands out: they
// arrive as unsigned GETs, which the gateway would otherwise give to S3.
func isLink(r *http.Request) bool {
	if r.URL.Path != "/" || r.URL.RawQuery == "" {
		return false
	}
	q := r.URL.Query()
	switch q.Get("Action") {
	case "ConfirmSubscription":
		return q.Get("TopicArn") != "" && q.Get("Token") != ""
	case "Unsubscribe":
		return q.Get("SubscriptionArn") != ""
	}
	return false
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameter", "Invalid parameter: "+format, a...)
}

func notFound(what string) error { return core.Errorf(404, "NotFound", "%s does not exist", what) }

// topicByARN finds a topic; lock held.
func (s *Service) topicByARN(arn string) (*topic, error) {
	if t := s.st.Topics[arn]; t != nil {
		return t, nil
	}
	if _, _, _, _, ok := core.ParseARN(arn); !ok {
		return nil, invalid("TopicArn")
	}
	return nil, notFound("Topic")
}

// subByARN finds a subscription: its ARN is the topic ARN plus ":id"; lock held.
func (s *Service) subByARN(arn string) (*topic, *subscription, error) {
	if t := s.st.Topics[arn[:max(strings.LastIndex(arn, ":"), 0)]]; t != nil && t.Subs[arn] != nil {
		return t, t.Subs[arn], nil
	}
	if _, _, _, res, ok := core.ParseARN(arn); !ok || !strings.Contains(res, ":") {
		return nil, nil, invalid("SubscriptionArn")
	}
	return nil, nil, notFound("Subscription")
}

// page returns up to 100 sorted keys after token, and the next page's token.
func page(keys []string, token string) ([]string, string) {
	slices.Sort(keys)
	var out []string
	for _, k := range keys {
		if k <= token {
			continue
		}
		if len(out) == 100 {
			return out, out[99]
		}
		out = append(out, k)
	}
	return out, ""
}

// ---- topics ----

func (t *topic) fifo() bool { return t.Attrs["FifoTopic"] == "true" }

const defaultDeliveryPolicy = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false,"defaultRequestPolicy":{"headerContentType":"text/plain; charset=UTF-8"}}}`

// attributes returns set, default and computed attributes; lock held.
func (t *topic) attributes() map[string]string {
	confirmed, pending := 0, 0
	for _, sub := range t.Subs {
		if sub.Pending {
			pending++
		} else {
			confirmed++
		}
	}
	out := map[string]string{
		"TopicArn": t.ARN, "Owner": t.Account, "DisplayName": "", "EffectiveDeliveryPolicy": defaultDeliveryPolicy,
		"Policy": `{"Version":"2008-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"__default_statement_ID","Effect":"Allow","Principal":{"AWS":"*"},` +
			`"Action":["SNS:GetTopicAttributes","SNS:SetTopicAttributes","SNS:AddPermission","SNS:RemovePermission","SNS:DeleteTopic","SNS:Subscribe","SNS:ListSubscriptionsByTopic","SNS:Publish"],` +
			`"Resource":"` + t.ARN + `","Condition":{"StringEquals":{"AWS:SourceOwner":"` + t.Account + `"}}}]}`,
		"SubscriptionsConfirmed": strconv.Itoa(confirmed), "SubscriptionsPending": strconv.Itoa(pending), "SubscriptionsDeleted": strconv.Itoa(t.Deleted),
	}
	if t.fifo() {
		out["ContentBasedDeduplication"] = "false"
	}
	maps.Copy(out, t.Attrs)
	if p := t.Attrs["DeliveryPolicy"]; p != "" {
		out["EffectiveDeliveryPolicy"] = p
	}
	return out
}

// checkTopicAttr validates one attribute for CreateTopic (create) or SetTopicAttributes.
func checkTopicAttr(k, v string, fifo, create bool) error {
	switch k {
	case "FifoTopic":
		if !create {
			return invalid("AttributeName")
		}
	case "ContentBasedDeduplication":
		if !fifo {
			return invalid("ContentBasedDeduplication Reason: Content-based deduplication can only be set for FIFO topics")
		}
	case "Policy", "DeliveryPolicy", "ArchivePolicy":
		if v != "" && !json.Valid([]byte(v)) {
			return invalid("%s: the value is not valid JSON", k)
		}
	case "DisplayName", "KmsMasterKeyId", "SignatureVersion", "TracingConfig", "FifoThroughputScope":
	default:
		if !strings.HasSuffix(k, "FeedbackRoleArn") && !strings.HasSuffix(k, "FeedbackSampleRate") {
			return invalid("AttributeName")
		}
	}
	return nil
}

var topicName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

func (s *Service) createTopic(c *core.Ctx, in *struct {
	Name       string
	Attributes map[string]string
	Tags       map[string]string
}) (any, error) {
	fifo := strings.EqualFold(in.Attributes["FifoTopic"], "true")
	name := in.Name
	if fifo {
		if !strings.HasSuffix(name, ".fifo") {
			return nil, invalid("Fifo Topic names must end with .fifo and must be made up of only uppercase and lowercase ASCII letters, numbers, underscores, and hyphens, and must be between 1 and 256 characters long.")
		}
		name = strings.TrimSuffix(name, ".fifo")
	}
	if !topicName.MatchString(name) {
		return nil, invalid("Topic Name")
	}
	for k, v := range in.Attributes {
		if err := checkTopicAttr(k, v, fifo, true); err != nil {
			return nil, err
		}
	}
	arn := c.ARN("sns", in.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.st.Topics[arn]; t != nil {
		have := t.attributes()
		for k, v := range in.Attributes {
			if have[k] != v {
				return nil, invalid("Attributes Reason: Topic already exists with different attributes")
			}
		}
		return map[string]string{"TopicArn": arn}, nil
	}
	attrs := map[string]string{}
	maps.Copy(attrs, in.Attributes)
	if fifo {
		attrs["FifoTopic"] = "true"
	}
	s.st.Topics[arn] = &topic{ARN: arn, Name: in.Name, Account: c.Account, Region: c.Region, Attrs: attrs, Tags: in.Tags, Subs: map[string]*subscription{}}
	return map[string]string{"TopicArn": arn}, nil
}

// deleteTopic is idempotent, as in SNS; the topic's subscriptions go with it.
func (s *Service) deleteTopic(c *core.Ctx, in *struct{ TopicArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Topics, in.TopicArn)
	return nil, nil
}

type topicRef struct{ TopicArn string }

func (s *Service) listTopics(c *core.Ctx, in *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var arns []string
	for arn, t := range s.st.Topics {
		if t.Account == c.Account && t.Region == c.Region {
			arns = append(arns, arn)
		}
	}
	arns, next := page(arns, in.NextToken)
	out := struct {
		Topics    []topicRef
		NextToken string
	}{NextToken: next}
	for _, arn := range arns {
		out.Topics = append(out.Topics, topicRef{arn})
	}
	return &out, nil
}

func (s *Service) getTopicAttributes(c *core.Ctx, in *struct{ TopicArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topicByARN(in.TopicArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Attributes": t.attributes()}, nil
}

func (s *Service) setTopicAttributes(c *core.Ctx, in *struct{ TopicArn, AttributeName, AttributeValue string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topicByARN(in.TopicArn)
	if err != nil {
		return nil, err
	}
	if err := checkTopicAttr(in.AttributeName, in.AttributeValue, t.fifo(), false); err != nil {
		return nil, err
	}
	if in.AttributeValue == "" {
		delete(t.Attrs, in.AttributeName) // back to the default
	} else {
		t.Attrs[in.AttributeName] = in.AttributeValue
	}
	return nil, nil
}

// ---- tags and permissions ----

func (s *Service) tagged(arn string) (*topic, error) {
	if t := s.st.Topics[arn]; t != nil {
		return t, nil
	}
	return nil, core.Errorf(404, "ResourceNotFound", "Resource does not exist")
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        map[string]string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	if t.Tags == nil {
		t.Tags = map[string]string{}
	}
	maps.Copy(t.Tags, in.Tags)
	return struct{}{}, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(t.Tags, k)
	}
	return struct{}{}, nil
}

type tag struct{ Key, Value string }

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	out := struct{ Tags []tag }{}
	for _, k := range core.SortedKeys(t.Tags) {
		out.Tags = append(out.Tags, tag{k, t.Tags[k]})
	}
	return &out, nil
}

// policy decodes the topic's access policy; lock held.
func (t *topic) policy() (map[string]any, []any) {
	var p map[string]any
	if json.Unmarshal([]byte(t.attributes()["Policy"]), &p) != nil || p == nil {
		p = map[string]any{"Version": "2008-10-17"}
	}
	stmts, _ := p["Statement"].([]any)
	return p, stmts
}

func sid(stmt any) any {
	m, _ := stmt.(map[string]any)
	return m["Sid"]
}

func (s *Service) addPermission(c *core.Ctx, in *struct {
	TopicArn, Label string
	AWSAccountId    []string
	ActionName      []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topicByARN(in.TopicArn)
	if err != nil {
		return nil, err
	}
	p, stmts := t.policy()
	if slices.ContainsFunc(stmts, func(st any) bool { return sid(st) == in.Label }) {
		return nil, invalid("Statement already exists")
	}
	var who, what []string
	for _, a := range in.AWSAccountId {
		who = append(who, core.GlobalARN("iam", c.Region, a, "root"))
	}
	for _, a := range in.ActionName {
		what = append(what, "SNS:"+a)
	}
	p["Statement"] = append(stmts, map[string]any{"Sid": in.Label, "Effect": "Allow", "Principal": map[string]any{"AWS": who}, "Action": what, "Resource": t.ARN})
	t.Attrs["Policy"] = jsonString(p)
	return nil, nil
}

func (s *Service) removePermission(c *core.Ctx, in *struct{ TopicArn, Label string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topicByARN(in.TopicArn)
	if err != nil {
		return nil, err
	}
	p, stmts := t.policy()
	p["Statement"] = slices.DeleteFunc(stmts, func(st any) bool { return sid(st) == in.Label })
	t.Attrs["Policy"] = jsonString(p)
	return nil, nil
}

// ---- subscriptions ----

// protocols maps each protocol to the service its endpoint ARN belongs to ("" when the endpoint is not an ARN).
var protocols = map[string]string{"sqs": "sqs", "lambda": "lambda", "firehose": "firehose", "application": "sns",
	"http": "", "https": "", "email": "", "email-json": "", "sms": ""}

var phone = regexp.MustCompile(`^\+?[0-9]{5,15}$`)

func checkEndpoint(protocol, ep string) error {
	ok := false
	switch svc := protocols[protocol]; {
	case svc != "":
		got, _, _, res, isARN := core.ParseARN(ep)
		ok = isARN && got == svc && (protocol != "application" || strings.HasPrefix(res, "endpoint/"))
	case protocol == "http" || protocol == "https":
		ok = strings.HasPrefix(ep, protocol+"://")
	case protocol == "sms":
		ok = phone.MatchString(ep)
	default: // email, email-json
		ok = strings.Contains(ep, "@")
	}
	if !ok {
		return invalid("Endpoint Reason: %q is not a valid %s endpoint", ep, protocol)
	}
	return nil
}

func checkSubAttr(protocol, k, v string) error {
	switch k {
	case "FilterPolicy":
		if v != "" {
			if _, err := pattern.Compile(v); err != nil {
				return invalid("FilterPolicy: %v", err)
			}
		}
	case "FilterPolicyScope":
		if v != "MessageAttributes" && v != "MessageBody" {
			return invalid("FilterPolicyScope: Invalid value [%s]. Please use either MessageBody or MessageAttributes", v)
		}
	case "RawMessageDelivery":
		if !strings.EqualFold(v, "true") && !strings.EqualFold(v, "false") {
			return invalid("RawMessageDelivery: Invalid value [%s]. Must be true or false.", v)
		}
		if strings.EqualFold(v, "true") && !slices.Contains([]string{"sqs", "http", "https", "firehose"}, protocol) {
			return invalid("Attributes Reason: Delivery protocol [%s] does not support raw message delivery.", protocol)
		}
	case "RedrivePolicy":
		if v != "" && dlqOf(v) == "" {
			return invalid("RedrivePolicy: must be JSON with a deadLetterTargetArn")
		}
	case "DeliveryPolicy", "ReplayPolicy":
		if v != "" && !json.Valid([]byte(v)) {
			return invalid("%s: the value is not valid JSON", k)
		}
	case "SubscriptionRoleArn":
	default:
		return invalid("AttributeName")
	}
	return nil
}

// dlqOf returns the dead-letter queue of a RedrivePolicy.
func dlqOf(policy string) string {
	var rp struct {
		Target string `json:"deadLetterTargetArn"`
	}
	json.Unmarshal([]byte(policy), &rp)
	return rp.Target
}

// attributes returns set, default and computed attributes; lock held.
func (sub *subscription) attributes() map[string]string {
	out := map[string]string{
		"SubscriptionArn": sub.ARN, "TopicArn": sub.Topic, "Owner": sub.Owner, "Protocol": sub.Protocol, "Endpoint": sub.Endpoint,
		"PendingConfirmation": strconv.FormatBool(sub.Pending), "ConfirmationWasAuthenticated": strconv.FormatBool(!sub.Pending),
		"RawMessageDelivery": "false",
	}
	if sub.Protocol == "http" || sub.Protocol == "https" {
		out["EffectiveDeliveryPolicy"] = defaultDeliveryPolicy
	}
	maps.Copy(out, sub.Attrs)
	if out["FilterPolicy"] != "" && out["FilterPolicyScope"] == "" {
		out["FilterPolicyScope"] = "MessageAttributes"
	}
	return out
}

func (s *Service) subscribe(c *core.Ctx, in *struct {
	TopicArn, Protocol, Endpoint string
	Attributes                   map[string]string
	ReturnSubscriptionArn        bool
}) (any, error) {
	if _, ok := protocols[in.Protocol]; !ok {
		return nil, invalid("Amazon SNS does not support this protocol string: %s", in.Protocol)
	}
	if err := checkEndpoint(in.Protocol, in.Endpoint); err != nil {
		return nil, err
	}
	for k, v := range in.Attributes {
		if err := checkSubAttr(in.Protocol, k, v); err != nil {
			return nil, err
		}
	}
	if in.Protocol == "firehose" && in.Attributes["SubscriptionRoleArn"] == "" {
		return nil, invalid("Attributes Reason: Firehose subscriptions require a SubscriptionRoleArn")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topicByARN(in.TopicArn)
	if err != nil {
		return nil, err
	}
	switch fifoQueue := strings.HasSuffix(in.Endpoint, ".fifo"); {
	case t.fifo() && in.Protocol != "sqs":
		return nil, invalid("Protocol Reason: FIFO topics only deliver to SQS queues, not %s", in.Protocol)
	case !t.fifo() && in.Protocol == "sqs" && fifoQueue:
		return nil, invalid("Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics")
	}
	var sub *subscription
	for _, old := range t.Subs {
		if old.Protocol == in.Protocol && old.Endpoint == in.Endpoint {
			sub = old
		}
	}
	if sub == nil {
		sub = &subscription{ARN: t.ARN + ":" + core.UUID(), Topic: t.ARN, Owner: c.Account, Protocol: in.Protocol, Endpoint: in.Endpoint,
			Attrs: in.Attributes, Pending: in.Protocol == "http" || in.Protocol == "https"}
		if sub.Pending {
			sub.Token = core.RandHex(64)
		}
		t.Subs[sub.ARN] = sub
	} else {
		for k, v := range in.Attributes {
			if sub.Attrs[k] != v {
				return nil, invalid("Attributes Reason: Subscription already exists with different attributes")
			}
		}
	}
	if sub.Pending { // (re)send the confirmation; the goroutine never takes the lock
		n := note{id: core.UUID(), region: t.Region, host: s.host(c.Host), topic: t.ARN, subArn: sub.ARN,
			protocol: sub.Protocol, endpoint: sub.Endpoint, at: time.Now().UTC()}
		body := n.confirmation(sub.Token)
		core.Go("sns.post", func() { s.post(n, "SubscriptionConfirmation", body) })
	}
	arn := sub.ARN
	if sub.Pending && !in.ReturnSubscriptionArn {
		arn = "pending confirmation"
	}
	return map[string]string{"SubscriptionArn": arn}, nil
}

func (s *Service) confirmSubscription(c *core.Ctx, in *struct{ TopicArn, Token, AuthenticateOnUnsubscribe string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.topicByARN(in.TopicArn)
	if err != nil {
		return nil, err
	}
	for _, sub := range t.Subs {
		if in.Token != "" && sub.Token == in.Token {
			sub.Pending = false
			return map[string]string{"SubscriptionArn": sub.ARN}, nil
		}
	}
	return nil, invalid("Token")
}

func (s *Service) unsubscribe(c *core.Ctx, in *struct{ SubscriptionArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, sub, err := s.subByARN(in.SubscriptionArn)
	if err != nil {
		return nil, err
	}
	delete(t.Subs, sub.ARN)
	t.Deleted++
	return nil, nil
}

type subRef struct{ SubscriptionArn, Owner, Protocol, Endpoint, TopicArn string }

// listSubscriptions serves ListSubscriptions and ListSubscriptionsByTopic.
func (s *Service) listSubscriptions(c *core.Ctx, in *struct{ TopicArn, NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := map[string]*subscription{}
	if c.Op == "ListSubscriptionsByTopic" {
		t, err := s.topicByARN(in.TopicArn)
		if err != nil {
			return nil, err
		}
		subs = t.Subs
	} else {
		for _, t := range s.st.Topics {
			if t.Account == c.Account && t.Region == c.Region {
				maps.Copy(subs, t.Subs)
			}
		}
	}
	keys, next := page(slices.Collect(maps.Keys(subs)), in.NextToken)
	out := struct {
		Subscriptions []subRef
		NextToken     string
	}{NextToken: next}
	for _, k := range keys {
		sub := subs[k]
		arn := sub.ARN
		if sub.Pending {
			arn = "PendingConfirmation"
		}
		out.Subscriptions = append(out.Subscriptions, subRef{arn, sub.Owner, sub.Protocol, sub.Endpoint, sub.Topic})
	}
	return &out, nil
}

func (s *Service) getSubscriptionAttributes(c *core.Ctx, in *struct{ SubscriptionArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, sub, err := s.subByARN(in.SubscriptionArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Attributes": sub.attributes()}, nil
}

func (s *Service) setSubscriptionAttributes(c *core.Ctx, in *struct{ SubscriptionArn, AttributeName, AttributeValue string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, sub, err := s.subByARN(in.SubscriptionArn)
	if err != nil {
		return nil, err
	}
	if err := checkSubAttr(sub.Protocol, in.AttributeName, in.AttributeValue); err != nil {
		return nil, err
	}
	if in.AttributeValue == "" {
		delete(sub.Attrs, in.AttributeName)
		return nil, nil
	}
	if sub.Attrs == nil {
		sub.Attrs = map[string]string{}
	}
	sub.Attrs[in.AttributeName] = in.AttributeValue
	return nil, nil
}

// ---- SMS ----

func (s *Service) checkOptedOut(c *core.Ctx, in *struct {
	PhoneNumber string `json:"phoneNumber"`
}) (any, error) {
	if !phone.MatchString(in.PhoneNumber) {
		return nil, invalid("PhoneNumber")
	}
	return map[string]any{"isOptedOut": false}, nil
}

func (s *Service) listOptedOut(c *core.Ctx, _ *struct{}) (any, error) { return struct{}{}, nil }

func (s *Service) getSMSAttributes(c *core.Ctx, in *struct {
	Attributes []string `json:"attributes"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for k, v := range s.st.SMS[c.Account+"/"+c.Region] {
		if len(in.Attributes) == 0 || slices.Contains(in.Attributes, k) {
			out[k] = v
		}
	}
	return map[string]any{"attributes": out}, nil
}

func (s *Service) setSMSAttributes(c *core.Ctx, in *struct {
	Attributes map[string]string `json:"attributes"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := c.Account + "/" + c.Region
	if s.st.SMS[k] == nil {
		s.st.SMS[k] = map[string]string{}
	}
	maps.Copy(s.st.SMS[k], in.Attributes)
	return struct{}{}, nil
}

// ---- mobile push: platform applications and endpoints ----

var platforms = []string{"ADM", "APNS", "APNS_SANDBOX", "BAIDU", "GCM", "MPNS", "WNS"}

type platformIn struct {
	PlatformApplicationArn string
	EndpointArn            string
	Attributes             map[string]string
}

type platformItem struct {
	PlatformApplicationArn string
	EndpointArn            string
	Attributes             map[string]string
}

// platformAttrs returns the live attributes of the endpoint, or else the application, named in in; lock held.
func (s *Service) platformAttrs(in *platformIn) (map[string]string, error) {
	if in.EndpointArn != "" {
		if e := s.st.Endpoints[in.EndpointArn]; e != nil {
			return e.Attrs, nil
		}
		return nil, notFound("Endpoint")
	}
	if a := s.st.Apps[in.PlatformApplicationArn]; a != nil {
		return a.Attrs, nil
	}
	return nil, notFound("PlatformApplication")
}

func (s *Service) createPlatformApplication(c *core.Ctx, in *struct {
	Name, Platform string
	Attributes     map[string]string
}) (any, error) {
	if !topicName.MatchString(in.Name) {
		return nil, invalid("Name")
	}
	if !slices.Contains(platforms, in.Platform) {
		return nil, invalid("Platform")
	}
	arn := c.ARN("sns", "app/"+in.Platform+"/"+in.Name)
	attrs := map[string]string{"Enabled": "true"}
	maps.Copy(attrs, in.Attributes)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Apps[arn] = &platformApp{ARN: arn, Name: in.Name, Platform: in.Platform, Attrs: attrs}
	return map[string]string{"PlatformApplicationArn": arn}, nil
}

// deletePlatform serves DeleteEndpoint and DeletePlatformApplication (which
// also drops the application's endpoints). Both are idempotent.
func (s *Service) deletePlatform(c *core.Ctx, in *platformIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Endpoints, in.EndpointArn)
	if app := in.PlatformApplicationArn; app != "" {
		delete(s.st.Apps, app)
		maps.DeleteFunc(s.st.Endpoints, func(_ string, e *platformEndpoint) bool { return e.App == app })
	}
	return nil, nil
}

func (s *Service) listPlatformApplications(c *core.Ctx, in *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := c.ARN("sns", "app/")
	var arns []string
	for arn := range s.st.Apps {
		if strings.HasPrefix(arn, prefix) {
			arns = append(arns, arn)
		}
	}
	arns, next := page(arns, in.NextToken)
	out := struct {
		PlatformApplications []platformItem
		NextToken            string
	}{NextToken: next}
	for _, arn := range arns {
		out.PlatformApplications = append(out.PlatformApplications, platformItem{PlatformApplicationArn: arn, Attributes: public(s.st.Apps[arn].Attrs)})
	}
	return &out, nil
}

// public copies platform attributes for output; PlatformCredential is write-only, as in SNS.
func public(attrs map[string]string) map[string]string {
	out := maps.Clone(attrs)
	delete(out, "PlatformCredential")
	return out
}

// getPlatformAttributes serves GetEndpointAttributes and GetPlatformApplicationAttributes.
func (s *Service) getPlatformAttributes(c *core.Ctx, in *platformIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attrs, err := s.platformAttrs(in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Attributes": public(attrs)}, nil
}

// setPlatformAttributes serves SetEndpointAttributes and SetPlatformApplicationAttributes.
func (s *Service) setPlatformAttributes(c *core.Ctx, in *platformIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attrs, err := s.platformAttrs(in)
	if err != nil {
		return nil, err
	}
	maps.Copy(attrs, in.Attributes)
	return nil, nil
}

func (s *Service) createPlatformEndpoint(c *core.Ctx, in *struct {
	PlatformApplicationArn, Token, CustomUserData string
	Attributes                                    map[string]string
}) (any, error) {
	if in.Token == "" {
		return nil, invalid("Token")
	}
	asked := map[string]string{}
	maps.Copy(asked, in.Attributes)
	if in.CustomUserData != "" {
		asked["CustomUserData"] = in.CustomUserData
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	app := s.st.Apps[in.PlatformApplicationArn]
	if app == nil {
		return nil, notFound("PlatformApplication")
	}
	for _, e := range s.st.Endpoints { // idempotent per device token
		if e.App == app.ARN && e.Attrs["Token"] == in.Token {
			for k, v := range asked {
				if e.Attrs[k] != v {
					return nil, invalid("Token Reason: Endpoint %s already exists with the same Token, but different attributes.", e.ARN)
				}
			}
			return map[string]string{"EndpointArn": e.ARN}, nil
		}
	}
	_, region, account, _, _ := core.ParseARN(app.ARN)
	arn := core.ARN("sns", region, account, "endpoint/"+app.Platform+"/"+app.Name+"/"+core.UUID())
	attrs := map[string]string{"Enabled": "true", "Token": in.Token}
	maps.Copy(attrs, asked)
	s.st.Endpoints[arn] = &platformEndpoint{ARN: arn, App: app.ARN, Attrs: attrs}
	return map[string]string{"EndpointArn": arn}, nil
}

func (s *Service) listEndpoints(c *core.Ctx, in *struct{ PlatformApplicationArn, NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Apps[in.PlatformApplicationArn] == nil {
		return nil, notFound("PlatformApplication")
	}
	var arns []string
	for arn, e := range s.st.Endpoints {
		if e.App == in.PlatformApplicationArn {
			arns = append(arns, arn)
		}
	}
	arns, next := page(arns, in.NextToken)
	out := struct {
		Endpoints []platformItem
		NextToken string
	}{NextToken: next}
	for _, arn := range arns {
		out.Endpoints = append(out.Endpoints, platformItem{EndpointArn: arn, Attributes: maps.Clone(s.st.Endpoints[arn].Attrs)})
	}
	return &out, nil
}

// ---- console ----

// TaggedResources lists topics (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, t := range s.st.Topics {
		if t.Account == account && t.Region == region {
			out[t.ARN] = maps.Clone(t.Tags)
		}
	}
	return out
}

// Resources lists topics and subscriptions for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	topics := core.Table{Name: "Topics", Columns: []string{"Name", "Region", "Account", "Subscriptions", "Type"}}
	subs := core.Table{Name: "Subscriptions", Columns: []string{"ARN", "Protocol", "Endpoint", "Status"}}
	for _, arn := range core.SortedKeys(s.st.Topics) {
		t := s.st.Topics[arn]
		topics.Rows = append(topics.Rows, []string{t.Name, t.Region, t.Account, strconv.Itoa(len(t.Subs)), map[bool]string{true: "FIFO", false: "Standard"}[t.fifo()]})
		for _, k := range core.SortedKeys(t.Subs) {
			sub := t.Subs[k]
			subs.Rows = append(subs.Rows, []string{sub.ARN, sub.Protocol, sub.Endpoint, map[bool]string{true: "PendingConfirmation", false: "Confirmed"}[sub.Pending]})
		}
	}
	return []core.Table{topics, subs}
}

// Inspect returns a topic (by name or ARN) with its attributes, tags and
// subscriptions, or a subscription's attributes.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, arn := range core.SortedKeys(s.st.Topics) {
		t := s.st.Topics[arn]
		if sub := t.Subs[id]; table == "Subscriptions" && sub != nil {
			return sub.attributes()
		}
		if table == "Topics" && (t.Name == id || t.ARN == id) {
			subs := []map[string]string{}
			for _, k := range core.SortedKeys(t.Subs) {
				subs = append(subs, t.Subs[k].attributes())
			}
			return map[string]any{"name": t.Name, "arn": t.ARN, "account": t.Account, "region": t.Region,
				"attributes": t.attributes(), "tags": maps.Clone(t.Tags), "subscriptions": subs}
		}
	}
	return nil
}

// PublishConsole publishes a message to a topic from the console and returns its message ID.
func (s *Service) PublishConsole(account, region, topicArn, subject, message string) (string, error) {
	out, err := s.publishTo(account, region, "", &publishIn{TopicArn: topicArn, Subject: subject, Message: message, auto: true, trace: core.NewTrace()})
	if err != nil {
		return "", err
	}
	return out.MessageId, nil
}
